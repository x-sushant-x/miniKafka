package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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

	msg := []byte("hello wal")

	_, pos, err := store.Append(msg)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	got, err := store.Read(pos)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(got) != string(msg) {
		t.Fatalf("expected %q got %q", msg, got)
	}
}

func TestMultipleAppends(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	messages := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	var positions []uint64

	for _, msg := range messages {
		_, pos, err := store.Append(msg)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}

		positions = append(positions, pos)
	}

	for i := range messages {
		got, err := store.Read(positions[i])
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}

		if string(got) != string(messages[i]) {
			t.Fatalf(
				"expected %q got %q",
				messages[i],
				got,
			)
		}
	}
}

func TestAppend_MessageTooLarge(t *testing.T) {
	store, cleanup := createTestStore(t)
	defer cleanup()

	msg := make([]byte, messageMaxSize+1)

	_, _, err := store.Append(msg)

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

	msg := []byte("important data")

	_, pos, err := store.Append(msg)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.buf.Flush(); err != nil {
		t.Fatal(err)
	}

	/*
	 * Corrupt a byte in payload.
	 */
	_, err = store.f.WriteAt(
		[]byte{0xFF},
		int64(pos)+lenWidth+checksumWidth,
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
