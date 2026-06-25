package broker

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/wal/log"

	logger "log"
)

type Broker struct {
	// TODO - Prevent Race Conditions
	topics map[string]*log.Topic
	port   string
}

func New(port string) (*Broker, error) {
	topicsStoragePath := os.Getenv("TOPICS_STORAGE_DIR")
	if topicsStoragePath == "" {
		return nil, ErrEmptyTopicsStorageDir
	}

	broker := Broker{
		port:   port,
		topics: make(map[string]*log.Topic),
	}

	logger.Println("Loading existing topics")

	filepath.WalkDir(topicsStoragePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() || path == topicsStoragePath {
			return nil
		}

		topicName := filepath.Base(path)

		existingTopic, err := log.NewTopic(topicName)
		if err != nil {
			return err
		}

		broker.topics[topicName] = existingTopic

		return nil
	})

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
	topic, ok := b.topics[topicName]
	if !ok || topic == nil {
		createdTopic, err := log.NewTopic(topicName)
		if err != nil {
			return nil, log.ErrUnableToCreateTopic
		}

		b.topics[topicName] = createdTopic
		topic = createdTopic
	}

	return topic.Append(record)
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
		record, err := b.Consume(req.Topic, req.Offset)
		if err != nil {
			return json.Marshal(models.Response{
				Success: false,
				Error:   err.Error(),
			})
		}

		return json.Marshal(models.Response{
			Success: true,
			Data:    string(record.Value),
		})

	default:
		return json.Marshal(models.Response{
			Success: false,
			Error:   "unknown request type",
		})
	}
}

func (b *Broker) Consume(topicName string, offset uint64) (*models.Record, error) {
	topic, ok := b.topics[topicName]
	if !ok || topic == nil {
		return nil, ErrNoTopicFound
	}

	return topic.Read(offset)
}
