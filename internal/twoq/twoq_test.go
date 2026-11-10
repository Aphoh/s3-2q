package twoq

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { c.t = c.t.Add(time.Second); return c.t }

func newIdx(promote bool) (*Index, *clock) {
	c := &clock{t: time.Unix(1000, 0)}
	x := New(Config{PromoteOnA1inHit: promote, MinGhost: 2})
	x.SetClock(c.now)
	return x, c
}

func k(s string) Key { return Key{"b", s} }

func victim(t *testing.T, x *Index) string {
	t.Helper()
	e, ok := x.Victim(time.Unix(1<<40, 0))
	if !ok {
		t.Fatal("no victim")
	}
	x.Evicted(e.Key)
	return e.Object
}

func TestNewKeysEvictedFIFO(t *testing.T) {
	x, _ := newIdx(true)
	for _, s := range []string{"a", "b", "c"} {
		x.Put(k(s), 10)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := victim(t, x); got != want {
			t.Fatalf("victim %q, want %q", got, want)
		}
	}
	if st := x.Stats(); st.GhostKeys != 2 || st.A1inKeys != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestHitPromotesAndScanResistance(t *testing.T) {
	x, _ := newIdx(true)
	x.Put(k("hot"), 10)
	x.Access(k("hot"))
	for _, s := range []string{"s1", "s2", "s3"} {
		x.Put(k(s), 10)
	}
	// A1in holds 30/40 bytes > 25%, so the scan keys go first.
	for _, want := range []string{"s1", "s2", "s3", "hot"} {
		if got := victim(t, x); got != want {
			t.Fatalf("victim %q, want %q", got, want)
		}
	}
}

func TestClassicDoesNotPromoteOnA1inHit(t *testing.T) {
	x, _ := newIdx(false)
	x.Put(k("a"), 10)
	x.Access(k("a"))
	if e, _ := x.Get(k("a")); e.Queue() != A1in {
		t.Fatalf("queue %v", e.Queue())
	}
}

func TestGhostHitGoesToAm(t *testing.T) {
	x, _ := newIdx(true)
	x.Put(k("a"), 10)
	victim(t, x)
	if e, ok := x.Get(k("a")); !ok || e.Queue() != A1out {
		t.Fatalf("want ghost, got %+v %v", e, ok)
	}
	if x.Access(k("a")) {
		t.Fatal("ghost reported resident")
	}
	x.Put(k("a"), 10)
	if e, _ := x.Get(k("a")); e.Queue() != Am {
		t.Fatalf("queue %v", e.Queue())
	}
}

func TestAmIsLRU(t *testing.T) {
	x, _ := newIdx(true)
	for _, s := range []string{"a", "b", "c"} {
		x.Put(k(s), 10)
		x.Access(k(s))
	}
	x.Access(k("a"))
	for _, want := range []string{"b", "c", "a"} {
		if got := victim(t, x); got != want {
			t.Fatalf("victim %q, want %q", got, want)
		}
	}
	if st := x.Stats(); st.GhostKeys != 0 {
		t.Fatalf("Am evictions must not become ghosts: %+v", st)
	}
}

func TestPrefersAmWhenA1inSmall(t *testing.T) {
	x, _ := newIdx(true)
	for _, s := range []string{"a", "b", "c", "d"} {
		x.Put(k(s), 100)
		x.Access(k(s))
	}
	x.Put(k("new"), 10) // 10/410 bytes in A1in
	if got := victim(t, x); got != "a" {
		t.Fatalf("victim %q, want a", got)
	}
}

func TestMinAgeProtects(t *testing.T) {
	x, c := newIdx(true)
	x.Put(k("a"), 10)
	if _, ok := x.Victim(c.t); ok {
		t.Fatal("recent key should be protected")
	}
	if _, ok := x.Victim(c.t.Add(time.Nanosecond)); !ok {
		t.Fatal("expected victim")
	}
}

func TestByteAccountingAndRemove(t *testing.T) {
	x, _ := newIdx(true)
	x.Put(k("a"), -1)
	x.SetSize(k("a"), 50)
	x.Put(k("b"), 20)
	x.Access(k("b"))
	x.Put(k("b"), 30)
	if st := x.Stats(); st.A1inBytes != 50 || st.AmBytes != 30 {
		t.Fatalf("stats %+v", st)
	}
	x.Remove(k("a"))
	x.Remove(k("b"))
	if st := x.Stats(); st != (Stats{}) {
		t.Fatalf("stats %+v", st)
	}
}

func TestGhostBounded(t *testing.T) {
	x, _ := newIdx(true)
	for _, s := range []string{"a", "b", "c", "d"} {
		x.Put(k(s), 1)
	}
	for range 4 {
		victim(t, x)
	}
	if st := x.Stats(); st.GhostKeys != 2 {
		t.Fatalf("ghosts %d, want 2", st.GhostKeys)
	}
}

func TestSnapshotRoundTripAndSeed(t *testing.T) {
	x, _ := newIdx(true)
	x.Put(k("hot"), 5)
	x.Access(k("hot"))
	x.Put(k("ghost"), 5)
	x.Put(k("new"), 5)
	x.Evicted(k("ghost"))
	y, _ := newIdx(true)
	y.Restore(x.Snapshot())
	if a, b := x.Stats(), y.Stats(); a != b {
		t.Fatalf("%+v != %+v", a, b)
	}
	y.SeedCold(k("old"), 7, time.Unix(1, 0))
	y.SeedCold(k("hot"), 7, time.Unix(1, 0)) // already resident: no-op
	if got := victim(t, y); got != "old" {
		t.Fatalf("victim %q, want old", got)
	}
}
