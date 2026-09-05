/*
	Fix record offset issue.
	Find error handling ways.
*/

package log

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/x-sushant-x/miniKafka/config"
	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/raft"
)

type partition struct {
	number    int
	wal       *wal
	raft      *raft.Raft
	applyChan chan raft.ApplyMessage
}

func newPartition(ctx context.Context, topicName string, number int, raftServer *raft.Server) (*partition, error) {
	storageDir := config.Config.TopicsStorageDir
	if storageDir == "" {
		return nil, ErrStorageDirVariableNoProvided
	}

	topicFolder := filepath.Join(storageDir, topicName)
	partitionFolder := filepath.Join(topicFolder, fmt.Sprintf("%d", number))

	wal, err := newWAL(ctx, partitionFolder)
	if err != nil {
		return nil, err
	}

	groupID := fmt.Sprintf("%s-%d", topicName, number)
	applyChan := make(chan raft.ApplyMessage)
	newRaft := raft.NewRaft(raftServer, applyChan, groupID)
	raftServer.AddRaft(groupID, newRaft)

	partition := &partition{
		number:    number,
		wal:       wal,
		applyChan: applyChan,
		raft:      newRaft,
	}

	go partition.raft.StartElectionLoop()
	go partition.raft.ApplyLoop()
	go partition.applyLoop()

	return partition, nil
}

func (p *partition) Append(record *models.Record) (*models.Record, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(record); err != nil {
		return nil, err
	}

	recordData := buf.Bytes()
	if !p.raft.Submit(recordData) {
		return nil, errors.New("unable to send command to raft")
	}

	return record, nil
}

func (p *partition) Read(offset uint64) (*models.Record, error) {
	return p.wal.read(offset)
}

func (p *partition) applyLoop() {
	for msg := range p.applyChan {
		var record models.Record
		err := gob.NewDecoder(bytes.NewReader(msg.Command)).Decode(&record)
		if err != nil {
			log.Err(err).Msg("unable to apply command")
		}

		_, err = p.wal.append(&record)
		if err != nil {
			log.Err(err).Msg("unable to append record to wal")
			continue
		}
	}
}
