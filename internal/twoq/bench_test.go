package twoq

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func benchKeys(n int) []Key {
	ks := make([]Key, n)
	for i := range ks {
		ks[i] = Key{"bkt1", fmt.Sprintf("dir/sub/object-%08d.bin", i)}
	}
	return ks
}

func filled(ks []Key) *Index {
	x := New(Config{PromoteOnA1inHit: true})
	for i, k := range ks {
		x.Put(k, 4096)
		if i%4 == 0 {
			x.Access(k)
		}
	}
	return x
}

// BenchmarkPut inserts new keys while evicting the coldest, i.e. a steady
// stream of writes into a full cache.
func BenchmarkPut(b *testing.B) {
	const n = 1 << 20
	ks := benchKeys(2 * n)
	x := filled(ks[:n])
	b.ReportAllocs()
	i := n
	for b.Loop() {
		x.Put(ks[i%len(ks)], 4096)
		if v, ok := x.Victim(time.Now()); ok {
			x.Evicted(v.Key)
		}
		i++
	}
}

func BenchmarkAccess(b *testing.B) {
	const n = 1 << 20
	ks := benchKeys(n)
	x := filled(ks)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		x.Access(ks[(i*7919)%n])
		i++
	}
}

// BenchmarkMemory reports retained heap per tracked key.
func BenchmarkMemory(b *testing.B) {
	const n = 1 << 20
	for b.Loop() {
		ks := benchKeys(n)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		x := filled(ks)
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "B/key")
		runtime.KeepAlive(x)
	}
}

func BenchmarkSnapshotRestore(b *testing.B) {
	const n = 1 << 20
	x := filled(benchKeys(n))
	b.Run("snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = x.Snapshot()
		}
	})
	snap := x.Snapshot()
	b.Run("restore", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			New(Config{}).Restore(snap)
		}
	})
}
