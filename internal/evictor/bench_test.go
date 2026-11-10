package evictor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Aphoh/s3-2q/internal/twoq"
)

const benchUA = "aws-cli/2.17.0 md/awscrt#0.20.11 ua/2.0 os/linux#6.8.0 md/arch#x86_64 lang/python#3.11.9 md/pyimpl#CPython cfg/retry-mode#standard"

func logLine(op, key string, size int64) string {
	uri := "/bkt1/" + strings.ReplaceAll(key, " ", "%20")
	return fmt.Sprintf("admin bkt1 [09/November/2026:23:41:09 +0000] 127.0.0.1 admin S9SQVPPW1JWMLRBQ %s %s %s 200 - 0 %d 1 1 - %s - 8XvtpZO4hpGAUXTi7FOEdvQb6PHwhRQFbArc49PfPha5GUB1JkAFMly142TDlR/dnJxyrQIXRAZPgc91glMOPXU= SigV4 - AuthHeader s3.us-east-1.amazonaws.com - arn:aws:s3:::%s Yes\n",
		op, key, uri, size, benchUA, uri)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func benchEvictor(store Store) *Evictor {
	return New(Config{HighWatermark: 90, LowWatermark: 80, BatchSize: 64,
		TwoQ: twoq.Config{PromoteOnA1inHit: true}}, store, func(string) (int64, int64, error) { return 0, 1, nil }, quiet())
}

func benchObjectKey(i int) string { return fmt.Sprintf("dir/sub/object-%08d.bin", i) }

// BenchmarkHandleLine is the per-request cost of the sidecar: parse a log
// line and update the index.
func BenchmarkHandleLine(b *testing.B) {
	const n = 1 << 18
	puts := make([]string, n)
	gets := make([]string, n)
	for i := range n {
		puts[i] = logLine("s3_PutObject", benchObjectKey(i), 4096)
		gets[i] = logLine("s3_GetObject", benchObjectKey(i), 0)
	}
	e := benchEvictor(newFake(1))
	for _, l := range puts {
		e.HandleLine(l)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i%2 == 0 {
			e.HandleLine(gets[(i*7919)%n])
		} else {
			e.HandleLine(puts[(i*7919)%n])
		}
		i++
	}
}

// BenchmarkIndexMemory reports heap retained per object tracked from the
// access log (what a long-running sidecar holds).
func BenchmarkIndexMemory(b *testing.B) {
	const n = 1 << 18
	for b.Loop() {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		e := benchEvictor(newFake(1))
		for i := range n {
			e.HandleLine(logLine("s3_PutObject", benchObjectKey(i), 4096))
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "B/object")
		runtime.KeepAlive(e)
	}
}

func filledEvictor(n int) *Evictor {
	e := benchEvictor(newFake(1))
	e.mu.Lock()
	for i := range n {
		k := twoq.Key{Bucket: "bkt1", Object: benchObjectKey(i)}
		e.idx.Put(k, 4096)
		if i%4 == 0 {
			e.idx.Access(k)
		}
	}
	e.mu.Unlock()
	return e
}

func BenchmarkSnapshot(b *testing.B) {
	const n = 1 << 20
	e := filledEvictor(n)
	path := filepath.Join(b.TempDir(), "index.gz")
	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := e.SaveSnapshot(path); err != nil {
				b.Fatal(err)
			}
		}
		reportSize(b, path)
	})
	b.Run("load", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := benchEvictor(newFake(1)).LoadSnapshot(path); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkReconcile(b *testing.B) {
	const n = 1 << 20
	f := newFake(1)
	mod := time.Now().Add(-time.Hour)
	for i := range n {
		f.put("bkt1", benchObjectKey(i), 4096, mod.Add(time.Duration(i)*time.Millisecond))
	}
	e := filledEvictor(n)
	e.store = f
	b.ReportAllocs()
	for b.Loop() {
		if err := e.Reconcile(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func reportSize(b *testing.B, path string) {
	if st, err := osStat(path); err == nil {
		b.ReportMetric(float64(st), "file-bytes")
	}
}

func osStat(path string) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
