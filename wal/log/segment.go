package log

import (
	"fmt"
	"os"
	"path"

	"github.com/x-sushant-x/miniKafka/models"
)

var maxStoreBytes = 16 * 1024 * 1024 // 16MB

type segment struct {
	store            *logStore
	index            *index
	baseOff, nextOff uint64
}

func newSegment(baseOff uint64, dir string) (*segment, error) {
	s := &segment{
		baseOff: baseOff,
	}

	storeFileName := path.Join(dir, fmt.Sprintf("%d.store", baseOff))
	storeFile, err := os.OpenFile(storeFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	store, err := newLogStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFileName := path.Join(dir, fmt.Sprintf("%d%s", baseOff, ".index"))
	indexFile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	index, err := newIndex(indexFile)
	if err != nil {
		return nil, err
	}

	s.index = index
	s.store = store

	indexEntries := s.index.size / 12 // 12 is the size of each entry in index file
	s.nextOff = s.baseOff + indexEntries

	return s, nil
}

func (s *segment) Append(record *models.Record) (offset uint64, err error) {
	offset = s.nextOff

	_, pos, err := s.store.Append(record)
	if err != nil {
		return 0, err
	}

	/*
	 * Base Offset = 100
	 * Next Offset = 101
	 * Relative Offset = (101 - 100) = 1
	 */
	relOff := uint32(offset - s.baseOff)

	if err := s.index.Write(relOff, pos); err != nil {
		return 0, err
	}

	s.nextOff++

	return offset, nil
}

func (s *segment) Read(offset uint64) (*models.Record, error) {
	if offset < s.baseOff || offset >= s.nextOff {
		return nil, fmt.Errorf("offset out of range")
	}

	relOff := uint32(offset - s.baseOff)

	pos, err := s.index.Read(relOff)
	if err != nil {
		return nil, err
	}

	return s.store.Read(pos)
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= uint64(maxStoreBytes)
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.store.Close()
}
