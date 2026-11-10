package evictor

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Aphoh/s3-2q/internal/twoq"
)

// Snapshot format (inside gzip):
//
//	magic
//	repeated: queue byte, uvarint bucket id [, uvarint len, name if id is new],
//	          uvarint len, key, varint size, varint last access (unix ns)
//
// Bucket ids are assigned in order of first appearance. Files written before
// this format (gzipped JSON lines) are still read.
const snapshotMagic = "s3-2q-index/2\n"

const maxSnapshotString = 64 << 10

// SaveSnapshot atomically writes the index.
func (e *Evictor) SaveSnapshot(path string) error {
	e.mu.Lock()
	entries := e.idx.Snapshot()
	e.mu.Unlock()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s3-2q-snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	zw, _ := gzip.NewWriterLevel(tmp, gzip.BestSpeed)
	bw := bufio.NewWriterSize(zw, 256<<10)
	bw.WriteString(snapshotMagic)
	var num [binary.MaxVarintLen64]byte
	uvarint := func(v uint64) { bw.Write(num[:binary.PutUvarint(num[:], v)]) }
	varint := func(v int64) { bw.Write(num[:binary.PutVarint(num[:], v)]) }
	ids := make(map[string]uint64)
	for _, en := range entries {
		bw.WriteByte(byte(en.Queue))
		id, ok := ids[en.Bucket]
		if !ok {
			id = uint64(len(ids))
			ids[en.Bucket] = id
		}
		uvarint(id)
		if !ok {
			uvarint(uint64(len(en.Bucket)))
			bw.WriteString(en.Bucket)
		}
		uvarint(uint64(len(en.Object)))
		bw.WriteString(en.Object)
		varint(en.Size)
		last := int64(math.MinInt64)
		if !en.LastAccess.IsZero() {
			last = en.LastAccess.UnixNano()
		}
		varint(last)
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
	br := bufio.NewReaderSize(zr, 256<<10)
	var entries []twoq.SnapshotEntry
	if head, _ := br.Peek(len(snapshotMagic)); bytes.Equal(head, []byte(snapshotMagic)) {
		br.Discard(len(snapshotMagic))
		entries, err = e.readBinarySnapshot(br)
	} else {
		entries, err = e.readJSONSnapshot(br)
	}
	if err != nil {
		return 0, err
	}
	e.mu.Lock()
	e.idx.Restore(entries)
	e.mu.Unlock()
	return len(entries), nil
}

func (e *Evictor) readBinarySnapshot(br *bufio.Reader) ([]twoq.SnapshotEntry, error) {
	var (
		entries []twoq.SnapshotEntry
		buckets []string
		tracked []bool
		buf     []byte
	)
	readString := func() (string, error) {
		n, err := binary.ReadUvarint(br)
		if err != nil {
			return "", err
		}
		if n > maxSnapshotString {
			return "", fmt.Errorf("snapshot string of %d bytes", n)
		}
		buf = slices.Grow(buf[:0], int(n))[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	}
	for {
		q, err := br.ReadByte()
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		id, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, unexpected(err)
		}
		switch {
		case id == uint64(len(buckets)):
			name, err := readString()
			if err != nil {
				return nil, unexpected(err)
			}
			buckets = append(buckets, name)
			tracked = append(tracked, e.tracked(name))
		case id > uint64(len(buckets)):
			return nil, fmt.Errorf("snapshot bucket id %d out of range", id)
		}
		key, err := readString()
		if err != nil {
			return nil, unexpected(err)
		}
		size, err := binary.ReadVarint(br)
		if err != nil {
			return nil, unexpected(err)
		}
		last, err := binary.ReadVarint(br)
		if err != nil {
			return nil, unexpected(err)
		}
		if !tracked[id] {
			continue
		}
		en := twoq.SnapshotEntry{Bucket: buckets[id], Object: key, Size: size, Queue: twoq.Queue(q)}
		if last != math.MinInt64 {
			en.LastAccess = time.Unix(0, last)
		}
		entries = append(entries, en)
	}
}

func (e *Evictor) readJSONSnapshot(r io.Reader) ([]twoq.SnapshotEntry, error) {
	var entries []twoq.SnapshotEntry
	dec := json.NewDecoder(r)
	for dec.More() {
		var en twoq.SnapshotEntry
		if err := dec.Decode(&en); err != nil {
			return nil, err
		}
		if e.tracked(en.Bucket) {
			entries = append(entries, en)
		}
	}
	return entries, nil
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
