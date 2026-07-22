/*
 * This code is responsible for writing data to log files in following format:
 * [length][checksum][timestamp][offset][data]
 *
 * Checksum ensures data integrity. It is made by combining msg + msg length.
 *
 * Timestamp will be used to implement the functionality where client request is to give all messages after given timestamp.
 * Introducing timestamp based access will also require to maintain another file .timeindex
 */

package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"os"
	"sync"

	"github.com/x-sushant-x/miniKafka/models"
)

const (
	lenWidth       = 4       // Bytes
	checksumWidth  = 4       // Bytes
	timestampWidth = 8       // Bytes
	offsetWidth    = 8       // Bytes
	messageMaxSize = 1000000 // Bytes
)

var (
	// BigEndian is standard for network oriented applications
	enc                       = binary.BigEndian
	errMessageMaxSizeBreached = errors.New("message max size limit crossed")
)

type logStore struct {
	mu     sync.RWMutex
	f      *os.File
	buf    *bufio.Writer
	size   uint64
	crc    hash.Hash32
	header [24]byte
}

func newLogStore(file *os.File) (*logStore, error) {
	if file == nil {
		return nil, errors.New("nil file for store")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.New("invalid file for store")
	}

	return &logStore{
		f:    file,
		buf:  bufio.NewWriter(file),
		size: uint64(fileInfo.Size()),
		crc:  crc32.NewIEEE(),
	}, nil
}

/*
 * This function perform following steps:
 * 1. Calculate message length and convert it into 8 byte big endian format.
 * 2. Generate and store a checksum from the combination of msgLen + msg.
 * 3. Store data into log file in following format: [length][checksum][data]
 */
func (store *logStore) Append(record *models.Record) (totalBytesWritten int, pos uint64, err error) {
	msgLen := uint32(len(record.Value))

	if msgLen > messageMaxSize {
		return 0, 0, errMessageMaxSizeBreached
	}

	store.mu.Lock()

	/*
	 * pos tells the position at which current entry is being appended in log file.
	 * This is later sent to index module which maintain the indexing of each entry for optimized lookup while reading.
	 */
	pos = store.size

	h := store.header

	// lenBuf := make([]byte, lenWidth)
	// var lenBuf [lenWidth]byte
	enc.PutUint32(h[0:4], msgLen)

	/*
	 * We are using CRC32 for checksum because:
	 * 1. It is exactly designed for detecting corruption in streaming and storage systems.
	 * 2. It is extremely fst.
	 * 3. It take only 4 byte of space.
	 */
	store.crc.Write(h[0:4])
	store.crc.Write(record.Value)
	checksum := store.crc.Sum32()
	store.crc.Reset()

	// checksumBuf := make([]byte, checksumWidth)
	// var checksumBuf [checksumWidth]byte
	// binary.BigEndian.PutUint32(checksumBuf[:], checksum)
	enc.PutUint32(h[4:8], checksum)

	// timestampBuf := make([]byte, timestampWidth)
	// var timestampBuf [timestampWidth]byte
	enc.PutUint64(h[8:16], record.Timestamp)

	// offsetBuf := make([]byte, offsetWidth)
	// var offsetBuf [offsetWidth]byte
	enc.PutUint64(h[16:24], record.Offset)

	// recordLen := lenWidth + checksumWidth + timestampWidth + offsetWidth + msgLen
	// r := make([]byte, recordLen)
	// copy(r[0:], lenBuf)
	// copy(r[lenWidth:], checksumBuf)
	// copy(r[lenWidth+checksumWidth:], timestampBuf)
	// copy(r[lenWidth+checksumWidth+timestampWidth:], offsetBuf)
	// copy(r[lenWidth+checksumWidth+timestampWidth+offsetWidth:], record.Value)

	// store.buf.Write(lenBuf[:])
	// store.buf.Write(checksumBuf[:])
	// store.buf.Write(timestampBuf[:])
	// store.buf.Write(offsetBuf[:])

	// TODO - Find how can we handle error here.
	store.buf.Write(h[:])
	store.buf.Write(record.Value)

	// bytesWritten, err := utils.WriteFull(store.buf, r)
	// if err != nil {
	// 	return 0, 0, err
	// }

	// if uint64(bytesWritten) != uint64(recordLen) {
	// 	err = errors.New("unable to write to wal")
	// 	return
	// }

	totalBytesWritten = lenWidth + checksumWidth + timestampWidth + offsetWidth + len(record.Value)
	store.size += uint64(totalBytesWritten)

	store.mu.Unlock()
	return
}

/*
 * This function perform following steps:
 * 1. Flush data to make sure everything is written in disk before reading. Sometime data is buffered but not written in disk.
 * 2. Ask index module for the position where data is stored in log file for a particular message offset.
 * 3. Read the checksum.
 * 4. Read the message.
 * 5. Generate a checksum with length + message.
 * 6. Compare if generated checksum and stored checksum is equal or not.
 */
func (store *logStore) Read(posToRead uint64) (*models.Record, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if err := store.buf.Flush(); err != nil {
		return nil, err
	}

	// lenBuf := make([]byte, lenWidth)
	// checksumBuf := make([]byte, checksumWidth)
	// timestampBuf := make([]byte, timestampWidth)
	// offsetBuf := make([]byte, offsetWidth)

	header := make([]byte, 24)
	startPos := int64(posToRead)

	// _, err := store.f.ReadAt(lenBuf, startPos)
	// if err != nil {
	// 	return nil, err
	// }

	// _, err = store.f.ReadAt(checksumBuf, startPos+lenWidth)
	// if err != nil {
	// 	return nil, err
	// }

	// expectedChecksum := enc.Uint32(checksumBuf)

	// _, err = store.f.ReadAt(timestampBuf, startPos+lenWidth+checksumWidth)
	// if err != nil {
	// 	return nil, err
	// }

	// _, err = store.f.ReadAt(offsetBuf, startPos+lenWidth+checksumWidth+timestampWidth)
	// if err != nil {
	// 	return nil, err
	// }

	_, err := store.f.ReadAt(header[:], startPos)
	if err != nil {
		return nil, err
	}

	// dataLen := enc.Uint32(lenBuf)

	dataLen := enc.Uint32(header[0:4])
	expectedChecksum := enc.Uint32(header[4:8])
	timestamp := enc.Uint64(header[8:16])
	offset := enc.Uint64(header[16:24])

	data := make([]byte, dataLen)

	_, err = store.f.ReadAt(data, startPos+24)
	if err != nil {
		return nil, err
	}

	crc := crc32.NewIEEE()
	crc.Write(header[0:4])
	crc.Write(data)

	actualChecksum := crc.Sum32()

	if actualChecksum != expectedChecksum {
		return nil, errors.New("corrupted WAL entry: checksum mismatch")
	}

	return &models.Record{
		Value:     data,
		Timestamp: timestamp,
		Offset:    offset,
	}, err
}

func (store *logStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if err := store.buf.Flush(); err != nil {
		return err
	}

	return store.f.Close()
}

func (store *logStore) Delete() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	return os.Remove(store.f.Name())
}
