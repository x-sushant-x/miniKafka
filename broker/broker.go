package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/wal/log"

	logger "log"
)

type Broker struct {
	topics sync.Map
	port   string
	ctx    context.Context
}

func New(ctx context.Context, port string) (*Broker, error) {
	topicsStoragePath := os.Getenv("TOPICS_STORAGE_DIR")
	if topicsStoragePath == "" {
		return nil, ErrEmptyTopicsStorageDir
	}

	broker := Broker{
		port:   port,
		topics: sync.Map{},
		ctx:    ctx,
	}

	logger.Println("Loading existing topics")

	err := filepath.WalkDir(topicsStoragePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() || path == topicsStoragePath {
			return nil
		}

		topicName := filepath.Base(path)

		existingTopic, err := log.NewTopic(broker.ctx, topicName)
		if err != nil {
			return err
		}

		broker.topics.Store(topicName, existingTopic)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &broker, nil
}

func (b *Broker) Start() error {
	logger.Println("Starting TCP Server on port:", b.port)

	server := TCPServer{
		Port: b.port,
	}

	return server.StartServer(b.handleRequest)
}

func (b *Broker) Produce(topicName string, record *models.Record) (*models.Record, error) {
	if topic, ok := b.topics.Load(topicName); ok {
		return topic.(*log.Topic).Append(record)
	}

	topic, err := log.NewTopic(b.ctx, topicName)
	if err != nil {
		return nil, log.ErrUnableToCreateTopic
	}

	actual, loaded := b.topics.LoadOrStore(topicName, topic)
	if loaded {
		topic = actual.(*log.Topic)
	}

	return topic.Append(record)
}

func (b *Broker) Consume(topicName string, offset uint64) (*models.Record, error) {
	if topic, ok := b.topics.Load(topicName); ok {
		return topic.(*log.Topic).Read(offset)
	}

	topic, err := log.NewTopic(b.ctx, topicName)
	if err != nil {
		return nil, log.ErrUnableToCreateTopic
	}

	actual, loaded := b.topics.LoadOrStore(topicName, topic)
	if loaded {
		topic = actual.(*log.Topic)
	}

	return topic.Read(offset)
}

func (b *Broker) handleRequest(data []byte) ([]byte, error) {
	var req models.Request

	if err := json.Unmarshal(data, &req); err != nil {
		return json.Marshal(models.Response{
			Success: false,
			Error:   err.Error(),
		})
	}

	switch req.Type {

	case "produce":
		record := models.Record{
			Value: []byte(req.Data),
		}

		stored, err := b.Produce(req.Topic, &record)
		if err != nil {
			return json.Marshal(models.Response{
				Success: false,
				Error:   err.Error(),
			})
		}

		return json.Marshal(models.Response{
			Success: true,
			Offset:  stored.Offset,
		})

	case "consume":
		for {
			record, err := b.Consume(req.Topic, req.Offset)
			if err != nil {
				if errors.Is(err, log.ErrOffsetNotFound) {
					time.Sleep(time.Millisecond * 100)
					continue
				}
			}

			return json.Marshal(models.Response{
				Success: true,
				Data:    string(record.Value),
			})
		}

	default:
		return json.Marshal(models.Response{
			Success: false,
			Error:   "unknown request type",
		})
	}
}
