package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/x-sushant-x/miniKafka/models"
)

func TestSegment_NewSegment_Empty(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(10, dir)
	require.NoError(t, err)
	defer seg.Close()

	require.Equal(t, uint64(10), seg.baseOff)
	require.Equal(t, uint64(10), seg.nextOff)

	// files should exist
	_, err = os.Stat(filepath.Join(dir, "10.store"))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "10.index"))
	require.NoError(t, err)
}

func TestSegment_AppendAndRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(0, dir)
	require.NoError(t, err)
	defer seg.Close()

	records := []*models.Record{
		{
			Value:     []byte("a"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    0,
		},
		{
			Value:     []byte("b"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    1,
		},
		{
			Value:     []byte("c"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    2,
		},
	}

	var offsets []uint64

	for _, record := range records {
		off, err := seg.Append(record)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		record, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, records[i].Value, record.Value)
	}
}

func TestSegment_OffsetsSequential(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(5, dir)
	require.NoError(t, err)
	defer seg.Close()

	for i := range 10 {
		off, err := seg.Append(&models.Record{
			Value:     []byte("x"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    uint64(i),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(5+i), off)
	}
}

func TestSegment_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(0, dir)
	require.NoError(t, err)
	defer seg.Close()

	_, err = seg.Read(0)
	require.Error(t, err)

	_, err = seg.Read(100)
	require.Error(t, err)
}

func TestSegment_Recovery(t *testing.T) {
	dir := t.TempDir()

	// create + write
	{
		seg, err := newSegment(0, dir)
		require.NoError(t, err)

		for i := range 5 {
			_, err := seg.Append(&models.Record{
				Value:     []byte("msg"),
				Timestamp: uint64(time.Now().Unix()),
				Offset:    uint64(i),
			})
			require.NoError(t, err)
		}

		require.NoError(t, seg.Close())
	}

	// reopen
	seg, err := newSegment(0, dir)
	require.NoError(t, err)
	defer seg.Close()

	require.Equal(t, uint64(5), seg.nextOff)

	// verify data still readable
	for i := range 5 {
		record, err := seg.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, []byte("msg"), record.Value)
	}
}

func TestSegment_IsMaxed(t *testing.T) {
	dir := t.TempDir()

	old := maxStoreBytes
	maxStoreBytes = 50
	defer func() { maxStoreBytes = old }()

	seg, err := newSegment(0, dir)
	require.NoError(t, err)
	defer seg.Close()

	require.False(t, seg.IsMaxed())

	for i := range 10 {
		_, err := seg.Append(&models.Record{
			Value:     []byte("this is a test message"),
			Timestamp: uint64(time.Now().Unix()),
			Offset:    uint64(i),
		})
		require.NoError(t, err)
	}

	require.True(t, seg.IsMaxed())
}
