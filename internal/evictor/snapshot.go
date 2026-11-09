package evictor

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/Aphoh/s3-2q/internal/twoq"
)

// SaveSnapshot atomically writes the index as gzipped JSON lines.
func (e *Evictor) SaveSnapshot(path string) error {
	e.mu.Lock()
	entries := e.idx.Snapshot()
	e.mu.Unlock()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s3-2q-snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	zw := gzip.NewWriter(tmp)
	bw := bufio.NewWriter(zw)
	enc := json.NewEncoder(bw)
	for _, en := range entries {
		if err := enc.Encode(en); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := errors.Join(bw.Flush(), zw.Close(), tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// LoadSnapshot restores a snapshot written by SaveSnapshot. A missing file
// is not an error.
func (e *Evictor) LoadSnapshot(path string) (int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	var entries []twoq.SnapshotEntry
	dec := json.NewDecoder(bufio.NewReader(zr))
	for dec.More() {
		var en twoq.SnapshotEntry
		if err := dec.Decode(&en); err != nil {
			return 0, err
		}
		if e.tracked(en.Bucket) {
			entries = append(entries, en)
		}
	}
	e.mu.Lock()
	e.idx.Restore(entries)
	e.mu.Unlock()
	return len(entries), nil
}
