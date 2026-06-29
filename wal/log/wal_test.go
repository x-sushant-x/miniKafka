package log

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/x-sushant-x/miniKafka/models"
)

func TestWAL_AppendAndRead(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	records := []*models.Record{
		{
			Value:     []byte("hello"),
			Timestamp: uint64(time.Now().Unix()),
		},
		{
			Value:     []byte("world"),
			Timestamp: uint64(time.Now().Unix()),
		},
		{
			Value:     []byte("wal"),
			Timestamp: uint64(time.Now().Unix()),
		},
		{
			Value:     []byte("sushant"),
			Timestamp: uint64(time.Now().Unix()),
		},
	}

	var offsets []uint64

	for _, record := range records {
		off, err := wal.append(record)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		record, err := wal.read(off)
		require.NoError(t, err)
		require.Equal(t, records[i].Value, record.Value)
	}
}

func TestWAL_OffsetsAreSequential(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	for i := 0; i < 100; i++ {
		off, err := wal.append(&models.Record{
			Value:     []byte("x"),
			Timestamp: uint64(time.Now().Unix()),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()

	old := maxStoreBytes
	maxStoreBytes = 100
	defer func() { maxStoreBytes = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	for i := 0; i < 50; i++ {
		_, err := wal.append(&models.Record{
			Value:     []byte("this is a test message"),
			Timestamp: uint64(time.Now().Unix()),
		})
		require.NoError(t, err)
	}

	require.Greater(t, len(wal.segments), 1)
}

func TestWAL_ReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()

	old := maxStoreBytes
	maxStoreBytes = 100
	defer func() { maxStoreBytes = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	var offsets []uint64

	for i := 0; i < 50; i++ {
		record := &models.Record{
			Value:     []byte(fmt.Sprintf("msg-%d", i)),
			Timestamp: uint64(time.Now().Unix()),
		}

		off, err := wal.append(record)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		record, err := wal.read(off)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(fmt.Sprintf("msg-%d", i)),
			record.Value,
		)
	}
}

func TestWAL_RestartRecovery(t *testing.T) {
	dir := t.TempDir()

	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wal, err := newWAL(ctx, dir)
		require.NoError(t, err)

		for i := 0; i < 20; i++ {
			_, err := wal.append(&models.Record{
				Value:     []byte(fmt.Sprintf("msg-%d", i)),
				Timestamp: uint64(time.Now().Unix()),
			})
			require.NoError(t, err)
		}

		require.NoError(t, wal.close())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	for i := 0; i < 20; i++ {
		record, err := wal.read(uint64(i))
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(fmt.Sprintf("msg-%d", i)),
			record.Value,
		)
	}
}

func TestWAL_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	_, err = wal.read(100)
	require.Error(t, err)
}

func TestWAL_EmptyRecovery(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal, err := newWAL(ctx, dir)
	require.NoError(t, err)
	defer wal.close()

	require.Len(t, wal.segments, 1)
	require.Equal(t, uint64(0), wal.active.baseOff)
}
