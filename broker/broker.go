// TODO - Later replace broker communication with TCP

package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/wal/log"
)

type Broker struct {
	topics map[string]*log.Topic
	port   string
}

func New(port string) (*Broker, error) {
	topicsStorageDir := os.Getenv("TOPICS_STORAGE_DIR")
	if topicsStorageDir == "" {
		return nil, ErrEmptyTopicsStorageDir
	}

	// TODO - Add mechanism to reload topics from storage on restart
	return &Broker{
		port:   port,
		topics: make(map[string]*log.Topic),
	}, nil
}

func (b *Broker) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics/{topic}/messages", b.produceHandler)
	mux.HandleFunc("GET /topics/{topic}/messages", b.consumeHandler)

	server := &http.Server{
		Addr:    ":" + b.port,
		Handler: mux,
	}

	fmt.Println("Broker running on port:", b.port)
	return server.ListenAndServe()
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

func (b *Broker) Consume(topicName string, offset uint64) (*models.Record, error) {
	topic, ok := b.topics[topicName]
	if !ok || topic == nil {
		return nil, ErrNoTopicFound
	}

	return topic.Read(offset)
}

func (b *Broker) produceHandler(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")

	var req models.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record := models.Record{
		Value: []byte(req.Data),
	}

	_, err := b.Produce(topicName, &record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode("Stored")
}

func (b *Broker) consumeHandler(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")

	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		http.Error(w, "offset is required", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	record, err := b.Consume(topicName, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(string(record.Value))
}
