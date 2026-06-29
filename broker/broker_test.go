package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x-sushant-x/miniKafka/models"
	"github.com/x-sushant-x/miniKafka/wal/log"
)

func TestNew_EmptyTopicsStorageDir(t *testing.T) {
	t.Setenv("TOPICS_STORAGE_DIR", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := New(ctx, "9092")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrEmptyTopicsStorageDir)
}

func TestNew_LoadExistingTopics(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := os.Mkdir(filepath.Join(dir, "orders"), 0755)
	require.NoError(t, err)

	err = os.Mkdir(filepath.Join(dir, "users"), 0755)
	require.NoError(t, err)

	broker, err := New(ctx, "9092")
	require.NoError(t, err)

	createdTopic, ok := broker.topics.Load("orders")
	require.True(t, ok)
	require.NotNil(t, createdTopic)

	createdTopic, ok = broker.topics.Load("users")
	require.True(t, ok)
	require.NotNil(t, createdTopic)

}

func TestProduce_CreatesTopic(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := New(ctx, "9092")
	require.NoError(t, err)

	record := &models.Record{
		Value: []byte("hello"),
	}

	stored, err := broker.Produce("orders", record)

	require.NoError(t, err)
	require.Equal(t, uint64(0), stored.Offset)

	_, ok := broker.topics.Load("orders")
	require.True(t, ok)
}

func TestProduce_ExistingTopic(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	r1, err := broker.Produce("orders", &models.Record{
		Value: []byte("A"),
	})

	require.NoError(t, err)

	r2, err := broker.Produce("orders", &models.Record{
		Value: []byte("B"),
	})

	require.NoError(t, err)

	require.Equal(t, uint64(0), r1.Offset)
	require.Equal(t, uint64(1), r2.Offset)
}

func TestConsume(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	_, err := broker.Produce("orders", &models.Record{
		Value: []byte("hello"),
	})

	require.NoError(t, err)

	record, err := broker.Consume("orders", 0)

	require.NoError(t, err)
	require.Equal(t, "hello", string(record.Value))
}

func TestConsume_OffsetNotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	_, err := broker.Produce("orders", &models.Record{
		Value: []byte("hello"),
	})

	require.NoError(t, err)

	_, err = broker.Consume("orders", 100)

	require.Error(t, err)
	require.ErrorIs(t, err, log.ErrOffsetNotFound)
}

func TestHandleRequest_Produce(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	req := models.Request{
		Type:  "produce",
		Topic: "orders",
		Data:  "hello",
	}

	data, _ := json.Marshal(req)

	respBytes, err := broker.handleRequest(data)

	require.NoError(t, err)

	var resp models.Response
	json.Unmarshal(respBytes, &resp)

	require.True(t, resp.Success)
	require.Equal(t, uint64(0), resp.Offset)
}

func TestHandleRequest_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	respBytes, err := broker.handleRequest([]byte("{"))

	require.NoError(t, err)

	var resp models.Response
	json.Unmarshal(respBytes, &resp)

	require.False(t, resp.Success)
	require.NotEmpty(t, resp.Error)
}

func TestHandleRequest_UnknownRequest(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	req := models.Request{
		Type: "invalid",
	}

	data, _ := json.Marshal(req)

	respBytes, _ := broker.handleRequest(data)

	var resp models.Response
	json.Unmarshal(respBytes, &resp)

	require.False(t, resp.Success)
	require.Equal(t, "unknown request type", resp.Error)
}

func TestProduce_ConcurrentTopicCreation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOPICS_STORAGE_DIR", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, _ := New(ctx, "9092")

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			_, err := broker.Produce("orders", &models.Record{
				Value: fmt.Appendf(nil, "%d", i),
			})

			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	count := 0

	broker.topics.Range(func(_, _ any) bool {
		count++
		return true
	})

	require.Equal(t, 1, count)
}
