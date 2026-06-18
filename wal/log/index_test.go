package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(".", "index.bin")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	i, err := newIndex(f)
	require.NoError(t, err)

	cases := []struct {
		off uint32
		pos uint64
	}{
		{0, 0},
		{1, 7},
		{2, 10},
	}

	for _, c := range cases {
		err := i.Write(c.off, c.pos)
		require.NoError(t, err)
	}

	for _, c := range cases {
		pos, err := i.Read(c.off)
		require.NoError(t, err)
		require.Equal(t, pos, c.pos)
	}
}
