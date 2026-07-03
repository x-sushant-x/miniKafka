package log

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/x-sushant-x/miniKafka/models"
)

type partition struct {
	number int
	wal    *wal
}

func newPartition(ctx context.Context, topicName string, number int) (*partition, error) {
	storageDir := os.Getenv("TOPICS_STORAGE_DIR")
	if storageDir == "" {
		return nil, ErrStorageDirVariableNoProvided
	}

	topicFolder := filepath.Join(storageDir, topicName)
	partitionFolder := filepath.Join(topicFolder, fmt.Sprintf("%d", number))

	wal, err := newWAL(ctx, partitionFolder)
	if err != nil {
		return nil, err
	}

	return &partition{
		number,
		wal,
	}, nil
}

func (t *partition) Append(record *models.Record) (*models.Record, error) {
	offset, err := t.wal.append(record)
	if err != nil {
		return nil, err
	}

	record.Offset = offset
	return record, nil
}

func (t *partition) Read(offset uint64) (*models.Record, error) {
	return t.wal.read(offset)
}
