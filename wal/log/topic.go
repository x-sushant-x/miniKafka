package log

import (
	"context"
	"time"

	"github.com/x-sushant-x/miniKafka/config"
	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/wal/utils"
)

type Topic struct {
	Name            string
	partitions      map[int]*partition
	totalPartitions uint32
}

func NewTopic(ctx context.Context, name string, partitions int) (*Topic, error) {
	if name == "" {
		return nil, ErrEmptyTopicName
	}

	if partitions == 0 {
		partitions = 1
	}

	topic := Topic{
		Name:       name,
		partitions: make(map[int]*partition),
	}

	for partition := range partitions {
		newPar, err := newPartition(ctx, topic.Name, partition)
		if err != nil {
			return nil, err
		}

		topic.partitions[newPar.number] = newPar
	}

	topic.totalPartitions = uint32(len(topic.partitions))

	return &topic, nil
}

func (t *Topic) Append(record *models.Record) (*models.Record, error) {
	partition, ok := t.selectPartition(record)
	if !ok {
		return nil, ErrPartitionNotFound
	}

	return partition.Append(record)
}

func (t *Topic) Read(offset uint64, partitionNum int) (*models.Record, error) {
	partition, ok := t.partitions[partitionNum]
	if !ok {
		return nil, ErrPartitionNotFound
	}

	return partition.Read(offset)
}

func (t *Topic) Close() error {
	for _, partition := range t.partitions {
		if err := partition.wal.close(); err != nil {
			return err
		}
	}

	return nil
}

func (t *Topic) selectPartition(record *models.Record) (*partition, bool) {
	hash := utils.MurmurHash(record.Key)
	selectedParNum := hash % (t.totalPartitions)
	par, ok := t.partitions[int(selectedParNum)]
	return par, ok
}

func (t *Topic) DeleteExpiredSegments() error {
	now := time.Now()
	retentionDays := config.Config.RetentionTimeDays
	retentionTime := now.Add(time.Duration(-retentionDays) * time.Second)

	for _, par := range t.partitions {
		segments := par.wal.segments

		for _, seg := range segments {
			if seg.metadata.CreationTime.Before(retentionTime) {
				// TODO - Safely handle segment deletion
			}
		}
	}

	return nil
}
