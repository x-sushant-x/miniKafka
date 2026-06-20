package log

import (
	"os"
	"path/filepath"

	"github.com/x-sushant-x/miniKafka/models"
)

type Topic struct {
	name string
	wal  *wal
}

func NewTopic(name string) (*Topic, error) {
	if name == "" {
		return nil, ErrEmptyTopicName
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		return nil, ErrStorageDirVariableNoProvided
	}

	topicFolder := filepath.Join(storageDir, name)

	wal, err := newWAL(topicFolder)
	if err != nil {
		return nil, err
	}

	return &Topic{
		name,
		wal,
	}, nil
}

func (t *Topic) Append(record *models.Record) (*models.Record, error) {
	offset, err := t.wal.append(record)
	if err != nil {
		return nil, err
	}

	record.Offset = offset
	return record, nil
}

func (t *Topic) Read(offset uint64) (*models.Record, error) {
	return t.wal.read(offset)
}
