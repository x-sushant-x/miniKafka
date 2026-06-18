package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWAL_AppendAndRead(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	msgs := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("wal"),
		[]byte("sushant"),
	}

	var offsets []uint64

	for _, m := range msgs {
		off, err := wal.Append(m)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		data, err := wal.Read(off)
		require.NoError(t, err)
		require.Equal(t, msgs[i], data)
	}
}

func TestWAL_OffsetsAreSequential(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	for i := 0; i < 100; i++ {
		off, err := wal.Append([]byte("x"))
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// shrink segment size for testing
	old := maxStoreBytes
	maxStoreBytes = 100
	defer func() { maxStoreBytes = old }()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	// write enough to force multiple segments
	for i := 0; i < 50; i++ {
		_, err := wal.Append([]byte("this is a test message"))
		require.NoError(t, err)
	}

	require.Greater(t, len(wal.segments), 1)
}

func TestWAL_ReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()

	old := maxStoreBytes
	maxStoreBytes = 100
	defer func() { maxStoreBytes = old }()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	var offsets []uint64

	for i := 0; i < 50; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		off, err := wal.Append(msg)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		data, err := wal.Read(off)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("msg-%d", i)), data)
	}
}

func TestWAL_RestartRecovery(t *testing.T) {
	dir := t.TempDir()

	// create + write
	{
		wal, err := NewWAL(dir)
		require.NoError(t, err)

		for i := 0; i < 20; i++ {
			_, err := wal.Append([]byte(fmt.Sprintf("msg-%d", i)))
			require.NoError(t, err)
		}

		require.NoError(t, wal.Close())
	}

	// reopen
	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	for i := 0; i < 20; i++ {
		data, err := wal.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("msg-%d", i)), data)
	}
}

func TestWAL_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	_, err = wal.Read(100)
	require.Error(t, err)
}

func TestWAL_EmptyRecovery(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	require.Len(t, wal.segments, 1)
	require.Equal(t, uint64(0), wal.active.baseOff)
}
