/*
 * This code is responsible for maintaining an index data structure that hold following content:
 * Message Offset : Position in log file
 *
 * Whenever a read request comes WAL can query index and index will give exact position where the requested message is stored.
 * This elimiate the need of traversing the whole log file.
 */

package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
)

const (
	/*
	 * Every entry in index will be of 12 bytes. This simple mathematics will give us huge advantage.
	 * Let's say we want to query index for message with 5th offset.
	 * We know that each entry in index will take 12 bytes so information for 5th offset can be found at:
	 * 5 * totWidth = 5 * 12 = 60
	 * This same functionality is implemented in Read function.
	 */
	offWidth = 4
	posWidth = 8
	totWidth = offWidth + posWidth
)

var indexEnc = binary.BigEndian

type index struct {
	f    *os.File
	buf  *bufio.Writer
	size uint64
	/*
	 * There is no mutex locking in index because as of now it will only be called from wal.go which already have mutex locking.
	 * Another reason was to prevent unnecessary performance overhead with 2 locking mechanisms.
	 * This also have drawback that index can only be called from synchronized peice of code and synchronization must be handled by caller.
	 */
}

func newIndex(file *os.File) (*index, error) {
	if file == nil {
		return nil, errors.New("nil file for store")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.New("invalid file for store")
	}

	return &index{
		f:    file,
		buf:  bufio.NewWriter(file),
		size: uint64(fileInfo.Size()),
	}, nil
}

func (i *index) Name() string {
	return i.f.Name()
}

func (i *index) Close() error {
	if err := i.buf.Flush(); err != nil {
		return err
	}

	if err := i.f.Close(); err != nil {
		return err
	}

	return nil
}

func (i *index) Write(off uint32, pos uint64) error {
	err := binary.Write(i.buf, indexEnc, uint32(off))
	if err != nil {
		return err
	}

	err = binary.Write(i.buf, indexEnc, uint64(pos))
	if err != nil {
		return err
	}

	i.size += totWidth

	return err
}

func (i *index) Read(off uint32) (pos uint64, err error) {
	if err := i.buf.Flush(); err != nil {
		return 0, err
	}

	posInIndex := off * totWidth
	posInIndex += offWidth

	data := make([]byte, 8)

	n, err := i.f.ReadAt(data, int64(posInIndex))
	if err != nil {
		return 0, err
	}

	if n < 8 {
		return 0, errors.New("unable to read position bytes")
	}

	pos = indexEnc.Uint64(data)
	return pos, err
}

func (i *index) Delete() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.f.Name())
}
