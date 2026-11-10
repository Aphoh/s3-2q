package evictor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Aphoh/s3-2q/internal/accesslog"
	"github.com/Aphoh/s3-2q/internal/twoq"
)

type obj struct {
	size int64
	mod  time.Time
}

// fakeStore doubles as a fake filesystem: usage = stored bytes / capacity.
type fakeStore struct {
	mu       sync.Mutex
	objs     map[twoq.Key]obj
	capacity int64
	deleted  []string
}

func newFake(capacity int64) *fakeStore {
	return &fakeStore{objs: map[twoq.Key]obj{}, capacity: capacity}
}

func (f *fakeStore) put(b, k string, size int64, mod time.Time) {
	f.mu.Lock()
	f.objs[twoq.Key{Bucket: b, Object: k}] = obj{size, mod}
	f.mu.Unlock()
}

func (f *fakeStore) ListBuckets(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := map[string]bool{}
	for k := range f.objs {
		set[k.Bucket] = true
	}
	var out []string
	for b := range set {
		out = append(out, b)
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeStore) ListObjects(_ context.Context, b string, fn func(string, int64, time.Time)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, o := range f.objs {
		if k.Bucket == b {
			fn(k.Object, o.size, o.mod)
		}
	}
	return nil
}

func (f *fakeStore) Head(_ context.Context, b, k string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[twoq.Key{Bucket: b, Object: k}]
	return o.size, ok, nil
}

func (f *fakeStore) Delete(_ context.Context, b, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, twoq.Key{Bucket: b, Object: k})
	f.deleted = append(f.deleted, k)
	return nil
}

func (f *fakeStore) usage(string) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, o := range f.objs {
		n += o.size
	}
	return n, f.capacity, nil
}

func newTest(f *fakeStore) *Evictor {
	return New(Config{HighWatermark: 90, LowWatermark: 50, BatchSize: 64,
		TwoQ: twoq.Config{PromoteOnA1inHit: true}}, f, f.usage, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func rec(op, key string, status int, size int64) accesslog.Record {
	return accesslog.Record{Bucket: "bkt", Key: key, Operation: op, HTTPStatus: status, ObjectSize: size}
}

func TestEvictsColdFirstUntilLowWatermark(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	old := time.Now().Add(-time.Hour)
	// Existing objects discovered by reconcile: oldest is coldest.
	f.put("bkt", "seed-old", 10, old)
	f.put("bkt", "seed-new", 10, old.Add(time.Minute))
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"hot", "scan1", "scan2", "scan3", "scan4", "scan5", "scan6", "scan7"} {
		f.put("bkt", k, 10, time.Now())
		e.Handle(rec("s3_PutObject", k, 200, 10))
	}
	e.Handle(rec("s3_GetObject", "hot", 200, 0))
	// 100% used >= 90: evict until < 50%.
	if err := e.EvictOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"seed-old", "seed-new", "scan1", "scan2", "scan3", "scan4"}
	if len(f.deleted) != len(want) {
		t.Fatalf("deleted %v, want %v", f.deleted, want)
	}
	for i := range want {
		if f.deleted[i] != want[i] {
			t.Fatalf("deleted %v, want %v", f.deleted, want)
		}
	}
	if _, ok := f.objs[twoq.Key{Bucket: "bkt", Object: "hot"}]; !ok {
		t.Fatal("hot object evicted")
	}
	if st := e.Status(); st.Evictions != 6 || st.EvictedBytes != 60 || st.UsagePct != 40 {
		t.Fatalf("status %+v", st)
	}
}

func TestBatchDoesNotOvershoot(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		f.put("bkt", k, 10, time.Now())
		e.Handle(rec("s3_PutObject", k, 200, 10))
	}
	// 100% used, low 50%: exactly 6 deletes get us under (40%).
	if err := e.EvictOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 6 {
		t.Fatalf("deleted %v", f.deleted)
	}
}

type failingStore struct{ *fakeStore }

func (failingStore) Delete(context.Context, string, string) error { return context.DeadlineExceeded }

func TestFailedDeleteReconciles(t *testing.T) {
	f := newFake(100)
	e := New(Config{HighWatermark: 90, LowWatermark: 50, BatchSize: 64}, failingStore{f}, f.usage,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.put("bkt", "a", 95, time.Now())
	e.Handle(rec("s3_PutObject", "a", 200, 95))
	if err := e.EvictOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := e.Status(); st.DeleteErrors != 1 {
		t.Fatalf("status %+v", st)
	}
	select {
	case <-e.reconcile:
	default:
		t.Fatal("no reconcile requested")
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := e.Status().Index; st.A1inKeys+st.AmKeys != 1 {
		t.Fatalf("index %+v", st)
	}
}

func TestBelowHighWatermarkNoop(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	f.put("bkt", "a", 89, time.Now())
	e.Handle(rec("s3_PutObject", "a", 200, 89))
	if err := e.EvictOnce(context.Background()); err != nil || len(f.deleted) != 0 {
		t.Fatalf("deleted %v err %v", f.deleted, err)
	}
}

func TestMinAgeLeavesStuck(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	e.cfg.MinAge = time.Hour
	f.put("bkt", "a", 95, time.Now())
	e.Handle(rec("s3_PutObject", "a", 200, 95))
	if err := e.EvictOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 0 || !e.Status().Stuck {
		t.Fatalf("deleted %v status %+v", f.deleted, e.Status())
	}
}

func TestHandleDeletesAndReconcile(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	for _, k := range []string{"a", "b", "c"} {
		f.put("bkt", k, 1, time.Now())
		e.Handle(rec("s3_PutObject", k, 200, 1))
	}
	e.Handle(rec("s3_DeleteObject", "a", 204, 0))
	e.Handle(rec("s3_GetObject", "b", 404, 0))
	e.Handle(rec("s3_PutObject", "failed", 500, 1))
	if st := e.Status().Index; st.A1inKeys != 1 {
		t.Fatalf("index %+v", st)
	}
	// Objects removed out of band (e.g. DeleteObjects) are dropped by
	// reconcile; ones written outside the log are added.
	f.mu.Lock()
	delete(f.objs, twoq.Key{Bucket: "bkt", Object: "c"})
	f.mu.Unlock()
	f.put("bkt", "outside", 5, time.Now())
	time.Sleep(time.Millisecond)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	_, hasC := e.idx.Get(twoq.Key{Bucket: "bkt", Object: "c"})
	out, hasOutside := e.idx.Get(twoq.Key{Bucket: "bkt", Object: "outside"})
	e.mu.Unlock()
	if hasC || !hasOutside || out.Size != 5 {
		t.Fatalf("c=%v outside=%v %+v", hasC, hasOutside, out)
	}
}

func TestUnknownGetLearnsSize(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	f.put("bkt", "x", 42, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.resolveSizes(ctx)
	e.Handle(rec("s3_CopyObject", "x", 200, 0))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.Status().Index.A1inBytes == 42 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("index %+v", e.Status().Index)
}

func TestSnapshotRoundTrip(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	e.Handle(rec("s3_PutObject", "hot", 200, 3))
	e.Handle(rec("s3_GetObject", "hot", 200, 0))
	e.Handle(rec("s3_PutObject", "cold", 200, 4))
	path := filepath.Join(t.TempDir(), "snap.gz")
	if err := e.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	e2 := newTest(f)
	if n, err := e2.LoadSnapshot(path); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if a, b := e.Status().Index, e2.Status().Index; a != b {
		t.Fatalf("%+v != %+v", a, b)
	}
}

func TestGhostSurvivesOwnDeleteAndMiss(t *testing.T) {
	f := newFake(100)
	e := newTest(f)
	f.put("bkt", "a", 60, time.Now())
	e.Handle(rec("s3_PutObject", "a", 200, 60))
	f.put("bkt", "b", 40, time.Now())
	e.Handle(rec("s3_PutObject", "b", 200, 40))
	if err := e.EvictOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 1 || f.deleted[0] != "a" {
		t.Fatalf("deleted %v", f.deleted)
	}
	// What versitygw logs for our delete, then a client missing on it.
	e.Handle(rec("s3_DeleteObject", "a", 204, 0))
	e.Handle(rec("s3_GetObject", "a", 404, 0))
	if st := e.Status().Index; st.GhostKeys != 1 {
		t.Fatalf("index %+v", st)
	}
	// The client re-uploads it: a ghost hit goes straight to Am.
	e.Handle(rec("s3_PutObject", "a", 200, 50))
	if st := e.Status().Index; st.AmKeys != 1 || st.A1inKeys != 1 || st.GhostKeys != 0 {
		t.Fatalf("index %+v", st)
	}
}

func TestSnapshotBinaryPreservesEntries(t *testing.T) {
	e := newTest(newFake(100))
	e.mu.Lock()
	for i, k := range []twoq.Key{{Bucket: "bkt", Object: "a b/ü.txt"}, {Bucket: "other", Object: "x"}, {Bucket: "bkt", Object: "ghost"}, {Bucket: "bkt", Object: ""}} {
		e.idx.Put(k, int64(i+1))
	}
	e.idx.Access(twoq.Key{Bucket: "other", Object: "x"})
	e.idx.Evicted(twoq.Key{Bucket: "bkt", Object: "ghost"})
	e.idx.SeedCold(twoq.Key{Bucket: "bkt", Object: "seeded"}, 9, time.Time{})
	want := e.idx.Snapshot()
	e.mu.Unlock()
	path := filepath.Join(t.TempDir(), "snap.gz")
	if err := e.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	e2 := newTest(newFake(100))
	if n, err := e2.LoadSnapshot(path); err != nil || n != len(want) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got := e2.idx.Snapshot()
	for i := range want {
		if w, g := want[i], got[i]; w.Bucket != g.Bucket || w.Object != g.Object || w.Size != g.Size ||
			w.Queue != g.Queue || !w.LastAccess.Equal(g.LastAccess) {
			t.Fatalf("entry %d: got %+v want %+v", i, g, w)
		}
	}
}

func TestSnapshotLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	enc := json.NewEncoder(zw)
	ts := time.Unix(1000, 0)
	for _, en := range []twoq.SnapshotEntry{
		{Bucket: "bkt", Object: "a", Size: 3, Queue: twoq.A1in, LastAccess: ts},
		{Bucket: "bkt", Object: "b", Size: 4, Queue: twoq.Am, LastAccess: ts},
	} {
		enc.Encode(en)
	}
	zw.Close()
	f.Close()
	e := newTest(newFake(100))
	if n, err := e.LoadSnapshot(path); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if st := e.Status().Index; st.A1inKeys != 1 || st.AmKeys != 1 || st.A1inBytes != 3 {
		t.Fatalf("index %+v", st)
	}
}

func TestSnapshotTruncated(t *testing.T) {
	e := newTest(newFake(100))
	e.Handle(rec("s3_PutObject", "a", 200, 3))
	path := filepath.Join(t.TempDir(), "snap.gz")
	if err := e.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	zr, _ := gzip.NewReader(bytes.NewReader(raw))
	plain, _ := io.ReadAll(zr)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(plain[:len(plain)-2])
	zw.Close()
	os.WriteFile(path, buf.Bytes(), 0o644)
	if _, err := newTest(newFake(100)).LoadSnapshot(path); err == nil {
		t.Fatal("expected error for truncated snapshot")
	}
}
