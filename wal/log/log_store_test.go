package log

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/x-sushant-x/miniKafka/models"
)

func createTestStore(t *testing.T) (*logStore, func()) {
	t.Helper()

	dir := t.TempDir()

	f, err := os.OpenFile(
		filepath.Join(dir, "store.log"),
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}

	store, err := newLogStore(f)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		_ = store.Close()
	}

	return store, cleanup
}

func TestNewLogStore_NilFile(t *testing.T) {
	_, err := newLogStore(nil)

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAppendAndRead(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	record := &models.Record{
		Value:     []byte("This is sushant"),
		Timestamp: uint64(time.Now().Unix()),
		Offset:    0,
	}

	_, pos, err := store.Append(record)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	got, err := store.Read(pos)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(got.Value) != string(record.Value) {
		t.Fatalf("expected %q got %q", record.Value, got.Value)
	}

	if got.Timestamp != record.Timestamp {
		t.Fatalf(
			"expected timestamp %d got %d",
			record.Timestamp,
			got.Timestamp,
		)
	}
}

func TestMultipleAppends(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	records := []*models.Record{
		{
			Value:     []byte("first"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    0,
		},
		{
			Value:     []byte("second"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    1,
		},
		{
			Value:     []byte("third"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    2,
		},
	}

	var positions []uint64

	for _, record := range records {
		_, pos, err := store.Append(record)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}

		positions = append(positions, pos)
	}

	for i := range records {
		got, err := store.Read(positions[i])
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}

		if string(got.Value) != string(records[i].Value) {
			t.Fatalf(
				"expected %q got %q",
				records[i].Value,
				got.Value,
			)
		}
	}
}

func TestAppend_MessageTooLarge(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	record := &models.Record{
		Value:     make([]byte, messageMaxSize+1),
		Timestamp: uint64(time.Now().Unix()),
		Offset:    0,
	}

	_, _, err := store.Append(record)

	if !errors.Is(err, errMessageMaxSizeBreached) {
		t.Fatalf(
			"expected %v got %v",
			errMessageMaxSizeBreached,
			err,
		)
	}
}

func TestChecksumCorruptionDetection(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	record := &models.Record{
		Value:     []byte("important data"),
		Timestamp: uint64(time.Now().Unix()),
		Offset:    0,
	}

	_, pos, err := store.Append(record)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.buf.Flush(); err != nil {
		t.Fatal(err)
	}

	/*
	 * Corrupt a byte in serialized record payload.
	 */
	_, err = store.f.WriteAt(
		[]byte{0xFF},
		int64(pos)+lenWidth+checksumWidth+timestampWidth+offsetWidth,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Read(pos)

	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestRead_InvalidPosition(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	_, err := store.Read(999999)

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClose(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	err := store.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err = store.Close()

	if err == nil {
		t.Fatal("expected error on second close")
	}
}

func BenchmarkAppend(b *testing.B) {
	storeFile, err := os.CreateTemp(b.TempDir(), "a.store")
	require.NoError(b, err)

	store, err := newLogStore(storeFile)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	msgSize := 1024 // 1KB

	value := make([]byte, msgSize)

	record := &models.Record{
		Value: value,
	}

	b.ResetTimer()

	start := time.Now()

	for i := 0; i < b.N; i++ {
		record.Offset = uint64(i)
		record.Timestamp = uint64(time.Now().UnixNano())

		if _, _, err := store.Append(record); err != nil {
			b.Fatal(err)
		}
	}

	elapsed := time.Since(start)

	msgPerSec := float64(b.N) / elapsed.Seconds()

	b.StopTimer()

	fmt.Printf("\n")
	fmt.Printf("Operations      : %d\n", b.N)
	fmt.Printf("Elapsed         : %v\n", elapsed)
	fmt.Printf("Throughput      : %.2f msgs/sec\n", msgPerSec)
	fmt.Println()
}

func BenchmarkReadSequential(b *testing.B) {
	tmp, err := os.CreateTemp("", "wal-read.store")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	store, err := newLogStore(tmp)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	const numRecords = 100000

	positions := make([]uint64, numRecords)

	payload := make([]byte, 1024) // 1KB message

	for i := 0; i < numRecords; i++ {
		_, pos, err := store.Append(&models.Record{
			Value:     payload,
			Timestamp: uint64(i),
			Offset:    uint64(i),
		})
		if err != nil {
			b.Fatal(err)
		}

		positions[i] = pos
	}

	if err := store.buf.Flush(); err != nil {
		b.Fatal(err)
	}

	idx := 0

	for b.Loop() {
		_, err := store.Read(positions[idx])
		if err != nil {
			b.Fatal(err)
		}

		idx++
		if idx == len(positions) {
			idx = 0
		}
	}
}

func BenchmarkReadRandom(b *testing.B) {
	tmp, err := os.CreateTemp("", "wal-read.store")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	store, err := newLogStore(tmp)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	const numRecords = 100000

	positions := make([]uint64, numRecords)

	payload := make([]byte, 1024) // 1KB message

	for i := 0; i < numRecords; i++ {
		_, pos, err := store.Append(&models.Record{
			Value:     payload,
			Timestamp: uint64(i),
			Offset:    uint64(i),
		})
		if err != nil {
			b.Fatal(err)
		}

		positions[i] = pos
	}

	if err := store.buf.Flush(); err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(42))

	for b.Loop() {
		pos := uint64(positions[r.Intn(len(positions))])
		_, err := store.Read(pos)
		if err != nil {
			b.Fatal(err)
		}
	}
}
