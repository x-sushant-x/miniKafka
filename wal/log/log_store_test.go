package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
