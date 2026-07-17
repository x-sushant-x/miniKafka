package log

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/x-sushant-x/miniKafka/config"
	"github.com/x-sushant-x/miniKafka/models"
)

var maxStoreBytes = 16 * 1024 * 1024 // 16MB

type segment struct {
	store            *logStore
	index            *index
	baseOff, nextOff uint64
	metadata         metadata
	dir              string
}

type metadata struct {
	CreationTime time.Time `json:"creation_time"`
	ExpireAt     time.Time `json:"expire_time"`
}

// TODO - Handle partial errors and reverse file creation.
func newSegment(baseOff uint64, dir string) (*segment, error) {
	s := &segment{
		baseOff: baseOff,
		dir:     dir,
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

	// Metadata
	createdAt := time.Now()
	metadata := metadata{
		CreationTime: createdAt,
		ExpireAt:     createdAt.Add(time.Duration(config.Config.RetentionTimeDays) * time.Hour * 24),
	}

	metaFileExists := false
	metaFileName := path.Join(dir, fmt.Sprintf("%d.meta", baseOff))

	_, err = os.Stat(metaFileName)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	} else {
		metaFileExists = true
	}

	if !metaFileExists {
		metaFile, err := os.OpenFile(metaFileName, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		defer metaFile.Close()

		// OPTIMIZE - For now json metadata is fine. But we can convert it to binary as well.
		if err := json.NewEncoder(metaFile).Encode(metadata); err != nil {
			return nil, err
		}
	} else {
		data, err := os.ReadFile(metaFileName)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(data, &metadata)
		if err != nil {
			return nil, err
		}
	}

	s.metadata = metadata

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

func (s *segment) IsExpired() bool {
	return time.Now().After(s.metadata.ExpireAt)
}

func (s *segment) delete() error {
	if err := s.Close(); err != nil {
		return err
	}

	files := []string{
		path.Join(s.dir, fmt.Sprintf("%d.store", s.baseOff)),
		path.Join(s.dir, fmt.Sprintf("%d.index", s.baseOff)),
		path.Join(s.dir, fmt.Sprintf("%d.meta", s.baseOff)),
	}

	for _, file := range files {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}
