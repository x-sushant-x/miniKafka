package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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

	msgs := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	}

	var offsets []uint64

	for _, m := range msgs {
		off, err := seg.Append(m)
		require.NoError(t, err)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		data, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, msgs[i], data)
	}
}

func TestSegment_OffsetsSequential(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(5, dir)
	require.NoError(t, err)
	defer seg.Close()

	for i := 0; i < 10; i++ {
		off, err := seg.Append([]byte("x"))
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

		for i := 0; i < 5; i++ {
			_, err := seg.Append([]byte("msg"))
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
	for i := 0; i < 5; i++ {
		data, err := seg.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, []byte("msg"), data)
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

	for i := 0; i < 10; i++ {
		_, err := seg.Append([]byte("this is a test message"))
		require.NoError(t, err)
	}

	require.True(t, seg.IsMaxed())
}
