package log

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/x-sushant-x/miniKafka/models"
)

type WAL struct {
	dir      string
	active   *segment
	segments []*segment
	mu       sync.RWMutex
}

func NewWAL(dir string) (*WAL, error) {
	w := &WAL{
		dir: dir,
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var baseOffsets []uint64

	for _, f := range files {
		if before, ok := strings.CutSuffix(f.Name(), ".store"); ok {
			base := before
			off, err := strconv.ParseUint(base, 10, 64)
			if err != nil {
				return nil, err
			}
			baseOffsets = append(baseOffsets, off)
		}
	}

	slices.Sort(baseOffsets)

	for _, base := range baseOffsets {
		seg, err := newSegment(base, dir)
		if err != nil {
			return nil, err
		}
		w.segments = append(w.segments, seg)
	}

	if len(w.segments) == 0 {
		seg, err := newSegment(0, dir)
		if err != nil {
			return nil, err
		}
		w.segments = []*segment{seg}
	}

	w.active = w.segments[len(w.segments)-1]

	return w, nil
}

func (w *WAL) Append(record *models.Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active.IsMaxed() {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	return w.active.Append(record)
}

func (w *WAL) rotate() error {
	baseOff := w.active.nextOff

	seg, err := newSegment(baseOff, w.dir)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, seg)
	w.active = seg

	return nil
}

func (w *WAL) Read(offset uint64) (*models.Record, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	seg := w.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset not found")
	}

	return seg.Read(offset)
}

func (w *WAL) findSegment(offset uint64) *segment {
	low := 0
	high := len(w.segments) - 1

	for low <= high {
		mid := (low + high) / 2
		s := w.segments[mid]

		if offset < s.baseOff {
			high = mid - 1
		} else if offset >= s.nextOff {
			low = mid + 1
		} else {
			return s
		}
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return nil
}
