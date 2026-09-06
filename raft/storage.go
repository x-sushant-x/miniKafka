package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var enc = binary.BigEndian

const (
	indexEntrySize = 8
	lengthSize     = 4
)

// RaftLog is a durable append-only Raft log.
//
// log.store:
//
//	[4-byte length][gob encoded LogEntry]
//	[4-byte length][gob encoded LogEntry]
//	...
//
// log.index:
//
//	[8-byte store position]
//	[8-byte store position]
//	...
//
// Raft index N maps to the Nth entry in log.index.
type RaftLog struct {
	mu sync.Mutex

	sFile *os.File
	iFile *os.File

	sSize uint64
	count uint64
}

func newRaftLog(sFile, iFile *os.File) (*RaftLog, error) {
	r := &RaftLog{
		sFile: sFile,
		iFile: iFile,
	}

	if err := r.recover(); err != nil {
		return nil, err
	}

	if r.count == 0 {
		dummy := LogEntry{
			Term: 0,
		}

		if _, err := r.Append(dummy); err != nil {
			r.Close()
			return nil, err
		}

		if err := r.Sync(); err != nil {
			r.Close()
			return nil, err
		}
	}

	return r, nil
}

func openRaftLog(dir string) (*RaftLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sFile, err := os.OpenFile(
		dir+"/log.store",
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}

	iFile, err := os.OpenFile(
		dir+"/log.index",
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		_ = sFile.Close()
		return nil, err
	}

	log, err := newRaftLog(sFile, iFile)
	if err != nil {
		_ = sFile.Close()
		_ = iFile.Close()
		return nil, err
	}

	return log, nil
}

func (r *RaftLog) Append(logEntry LogEntry) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var payload bytes.Buffer
	err := gob.NewEncoder(&payload).Encode(&logEntry)
	if err != nil {
		return 0, err
	}

	data := payload.Bytes()

	// Record starts at the current end of the store.
	position := r.sSize

	// Build:
	// [4 byte length][payload]
	record := make([]byte, lengthSize+len(data))

	enc.PutUint32(record[:lengthSize], uint32(len(data)))
	copy(record[lengthSize:], data)

	// Write the store record first.
	_, err = r.sFile.Write(record)
	if err != nil {
		return 0, err
	}

	// Then write the position into the index.
	indexPosition := make([]byte, indexEntrySize)
	enc.PutUint64(indexPosition, position)

	_, err = r.iFile.Write(indexPosition)
	if err != nil {
		// The store contains an orphaned record.
		// Roll it back because the index was not written.
		_ = r.sFile.Truncate(int64(position))
		_, _ = r.sFile.Seek(int64(position), io.SeekStart)
		return 0, err
	}

	r.sSize += uint64(len(record))

	index := r.count
	r.count++

	return int(index), nil
}

func (r *RaftLog) Get(index int) (LogEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index < 0 || uint64(index) >= r.count {
		return LogEntry{}, fmt.Errorf("raft log index %d out of range", index)
	}

	// Find store position from the index file.
	var positionBytes [indexEntrySize]byte

	_, err := r.iFile.ReadAt(positionBytes[:], int64(index)*indexEntrySize)
	if err != nil {
		return LogEntry{}, err
	}

	position := enc.Uint64(positionBytes[:])

	// Read record length.
	var lengthBytes [lengthSize]byte
	_, err = r.sFile.ReadAt(lengthBytes[:], int64(position))
	if err != nil {
		return LogEntry{}, err
	}

	length := enc.Uint32(lengthBytes[:])
	data := make([]byte, length)

	_, err = r.sFile.ReadAt(data, int64(position)+lengthSize)
	if err != nil {
		return LogEntry{}, err
	}

	var entry LogEntry
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&entry)
	if err != nil {
		return LogEntry{}, err
	}

	return entry, nil
}

func (r *RaftLog) LastIndex() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return -1
	}
	return int64(r.count - 1)
}

func (r *RaftLog) LastTerm() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return 0
	}

	// Avoid calling Get() because it tries to acquire the same mutex.
	index := int(r.count - 1)

	position, err := r.getPosition(index)
	if err != nil {
		return 0
	}

	entry, err := r.readAtPosition(position)
	if err != nil {
		return 0
	}

	return entry.Term
}

func (r *RaftLog) getPosition(index int) (uint64, error) {
	if index < 0 || uint64(index) >= r.count {
		return 0, fmt.Errorf("index %d out of range", index)
	}

	var buf [indexEntrySize]byte

	_, err := r.iFile.ReadAt(buf[:], int64(index)*indexEntrySize)
	if err != nil {
		return 0, err
	}

	return enc.Uint64(buf[:]), nil
}

func (r *RaftLog) readAtPosition(position uint64) (LogEntry, error) {
	var lengthBuf [lengthSize]byte

	_, err := r.sFile.ReadAt(lengthBuf[:], int64(position))
	if err != nil {
		return LogEntry{}, err
	}

	length := enc.Uint32(lengthBuf[:])

	data := make([]byte, length)

	_, err = r.sFile.ReadAt(data, int64(position)+lengthSize)
	if err != nil {
		return LogEntry{}, err
	}

	var entry LogEntry

	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&entry)
	if err != nil {
		return LogEntry{}, err
	}

	return entry, nil
}

func (r *RaftLog) AppendBatch(entries []LogEntry) error {
	for _, entry := range entries {
		_, err := r.Append(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RaftLog) GetRange(start, end int) ([]LogEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if start < 0 || end < start || uint64(end) > r.count {
		return nil, fmt.Errorf("invalid log range [%d,%d)", start, end)
	}

	entries := make([]LogEntry, 0, end-start)

	for i := start; i < end; i++ {
		position, err := r.getPosition(i)
		if err != nil {
			return nil, err
		}

		entry, err := r.readAtPosition(position)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (r *RaftLog) TruncateFrom(index int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index < 0 {
		return errors.New("invalid truncate index")
	}

	if uint64(index) > r.count {
		return fmt.Errorf("truncate index %d beyond log size %d", index, r.count)
	}

	// Nothing to truncate.
	if uint64(index) == r.count {
		return nil
	}

	var newStoreSize uint64

	if index > 0 {
		position, err := r.getPosition(index)
		if err != nil {
			return err
		}

		newStoreSize = position
	}

	err := r.sFile.Truncate(int64(newStoreSize))
	if err != nil {
		return err
	}

	newIndexSize := int64(index * indexEntrySize)

	err = r.iFile.Truncate(newIndexSize)
	if err != nil {
		return err
	}

	_, err = r.sFile.Seek(int64(newStoreSize), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = r.iFile.Seek(newIndexSize, io.SeekStart)
	if err != nil {
		return err
	}

	r.sSize = newStoreSize
	r.count = uint64(index)

	return nil
}

func (r *RaftLog) Sync() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.sFile.Sync()
	if err != nil {
		return err
	}

	err = r.iFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (r *RaftLog) recover() error {
	storeInfo, err := r.sFile.Stat()
	if err != nil {
		return err
	}

	indexInfo, err := r.iFile.Stat()
	if err != nil {
		return err
	}

	storeSize := uint64(storeInfo.Size())
	indexSize := uint64(indexInfo.Size())

	// Every index entry must be exactly 8 bytes.
	validIndexSize := indexSize - (indexSize % indexEntrySize)

	if validIndexSize != indexSize {
		err = r.iFile.Truncate(int64(validIndexSize))
		if err != nil {
			return err
		}

		indexSize = validIndexSize
	}

	count := indexSize / indexEntrySize

	// Validate every indexed record.
	var lastValidStoreEnd uint64

	for i := uint64(0); i < count; i++ {
		var positionBuf [indexEntrySize]byte

		_, err := r.iFile.ReadAt(positionBuf[:], int64(i*indexEntrySize))
		if err != nil {
			return err
		}

		position := enc.Uint64(positionBuf[:])

		if position >= storeSize {
			// Index points beyond the store.
			count = i
			break
		}

		end, err := r.validateRecord(position, storeSize)
		if err != nil {
			// Ignore corrupt/partial tail.
			count = i
			break
		}

		lastValidStoreEnd = end
	}

	// Remove invalid index entries.
	err = r.iFile.Truncate(int64(count * indexEntrySize))
	if err != nil {
		return err
	}

	// Remove orphaned/corrupt tail from store.
	err = r.sFile.Truncate(int64(lastValidStoreEnd))
	if err != nil {
		return err
	}

	r.count = count
	r.sSize = lastValidStoreEnd

	_, err = r.sFile.Seek(int64(r.sSize), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = r.iFile.Seek(int64(r.count*indexEntrySize), io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

func (r *RaftLog) validateRecord(position uint64, storeSize uint64) (uint64, error) {
	if position+lengthSize > storeSize {
		return 0, io.ErrUnexpectedEOF
	}

	var lengthBuf [lengthSize]byte

	_, err := r.sFile.ReadAt(lengthBuf[:], int64(position))
	if err != nil {
		return 0, err
	}

	length := uint64(enc.Uint32(lengthBuf[:]))

	end := position + lengthSize + length

	if end > storeSize {
		return 0, io.ErrUnexpectedEOF
	}

	data := make([]byte, length)

	_, err = r.sFile.ReadAt(data, int64(position)+lengthSize)
	if err != nil {
		return 0, err
	}

	var entry LogEntry

	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&entry)
	if err != nil {
		return 0, err
	}

	return end, nil
}

func (r *RaftLog) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.sFile.Close(); err != nil {
		_ = r.iFile.Close()
		return err
	}

	return r.iFile.Close()
}
