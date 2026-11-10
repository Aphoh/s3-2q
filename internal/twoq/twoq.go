// Package twoq implements a byte-aware 2Q replacement index.
//
// New keys enter A1in (a FIFO "probation" queue). A key that is read again
// while in A1in is promoted to Am (an LRU "protected" queue), similar to
// Dragonfly's probationary/protected split. Keys evicted from A1in are
// remembered in A1out, a bounded ghost FIFO without data; if such a key is
// written again it goes straight to Am. Victims come from the A1in tail while
// A1in holds more than KinRatio of the tracked bytes, otherwise from the Am
// tail.
//
// Index is not safe for concurrent use.
package twoq

import (
	"container/list"
	"time"
)

type Queue uint8

const (
	None Queue = iota
	A1in
	Am
	A1out
)

func (q Queue) String() string {
	switch q {
	case A1in:
		return "a1in"
	case Am:
		return "am"
	case A1out:
		return "a1out"
	}
	return "none"
}

type Key struct {
	Bucket string
	Object string
}

// Entry is a tracked object. Size is -1 when unknown.
type Entry struct {
	Key
	Size       int64
	LastAccess time.Time
	queue      Queue
}

func (e *Entry) Queue() Queue { return e.queue }

type Config struct {
	// KinRatio is the share of tracked bytes A1in may hold before it is
	// preferred for eviction. Default 0.25.
	KinRatio float64
	// GhostRatio bounds A1out to this many keys per resident key. Default 0.5.
	GhostRatio float64
	// MinGhost is the minimum A1out capacity in keys. Default 1024.
	MinGhost int
	// PromoteOnA1inHit moves a key read while in A1in to Am. When false the
	// classic 2Q behaviour applies and only ghost hits reach Am.
	PromoteOnA1inHit bool
}

func (c Config) withDefaults() Config {
	if c.KinRatio <= 0 || c.KinRatio >= 1 {
		c.KinRatio = 0.25
	}
	if c.GhostRatio <= 0 {
		c.GhostRatio = 0.5
	}
	if c.MinGhost <= 0 {
		c.MinGhost = 1024
	}
	return c
}

type Index struct {
	cfg                Config
	a1in, am, a1out    *list.List // front = newest / most recently used
	items              map[Key]*list.Element
	a1inBytes, amBytes int64
	now                func() time.Time
}

func New(cfg Config) *Index {
	return &Index{
		cfg:   cfg.withDefaults(),
		a1in:  list.New(),
		am:    list.New(),
		a1out: list.New(),
		items: make(map[Key]*list.Element),
		now:   time.Now,
	}
}

// SetClock overrides the time source (for tests).
func (x *Index) SetClock(now func() time.Time) { x.now = now }

func (x *Index) list(q Queue) *list.List {
	switch q {
	case A1in:
		return x.a1in
	case Am:
		return x.am
	case A1out:
		return x.a1out
	}
	return nil
}

func knownSize(s int64) int64 {
	if s < 0 {
		return 0
	}
	return s
}

func (x *Index) addBytes(q Queue, n int64) {
	switch q {
	case A1in:
		x.a1inBytes += n
	case Am:
		x.amBytes += n
	}
}

func (x *Index) unlink(el *list.Element) *Entry {
	e := el.Value.(*Entry)
	x.list(e.queue).Remove(el)
	x.addBytes(e.queue, -knownSize(e.Size))
	delete(x.items, e.Key)
	e.queue = None
	return e
}

func (x *Index) pushFront(q Queue, e *Entry) {
	e.queue = q
	x.items[e.Key] = x.list(q).PushFront(e)
	x.addBytes(q, knownSize(e.Size))
}

func (x *Index) pushBack(q Queue, e *Entry) {
	e.queue = q
	x.items[e.Key] = x.list(q).PushBack(e)
	x.addBytes(q, knownSize(e.Size))
}

// Get returns a copy of the entry for k.
func (x *Index) Get(k Key) (Entry, bool) {
	el, ok := x.items[k]
	if !ok {
		return Entry{}, false
	}
	return *el.Value.(*Entry), true
}

// Put records a write of k. size < 0 means unknown.
func (x *Index) Put(k Key, size int64) {
	now := x.now()
	if el, ok := x.items[k]; ok {
		e := el.Value.(*Entry)
		switch e.queue {
		case A1in, Am:
			x.addBytes(e.queue, knownSize(size)-knownSize(e.Size))
			e.Size = size
			e.LastAccess = now
			if e.queue == Am {
				x.am.MoveToFront(el)
			}
			return
		case A1out:
			x.unlink(el)
			x.pushFront(Am, &Entry{Key: k, Size: size, LastAccess: now})
			return
		}
	}
	x.pushFront(A1in, &Entry{Key: k, Size: size, LastAccess: now})
	x.trimGhost()
}

// Access records a read of k. It reports whether k is resident; unknown and
// ghost keys are left untouched so the caller can decide how to learn about
// them (e.g. via Put once the size is known).
func (x *Index) Access(k Key) bool {
	el, ok := x.items[k]
	if !ok {
		return false
	}
	e := el.Value.(*Entry)
	switch e.queue {
	case Am:
		e.LastAccess = x.now()
		x.am.MoveToFront(el)
		return true
	case A1in:
		e.LastAccess = x.now()
		if x.cfg.PromoteOnA1inHit {
			x.unlink(el)
			x.pushFront(Am, e)
		}
		return true
	}
	return false
}

// SetSize fills in the size of a resident key.
func (x *Index) SetSize(k Key, size int64) {
	el, ok := x.items[k]
	if !ok {
		return
	}
	e := el.Value.(*Entry)
	if e.queue != A1in && e.queue != Am {
		return
	}
	x.addBytes(e.queue, knownSize(size)-knownSize(e.Size))
	e.Size = size
}

// Remove forgets k entirely (explicit delete); it does not enter A1out.
func (x *Index) Remove(k Key) {
	if el, ok := x.items[k]; ok {
		x.unlink(el)
	}
}

// Victim returns the next key to evict without changing the index. Keys
// accessed at or after notBefore are protected; ok is false when no
// eligible victim exists.
func (x *Index) Victim(notBefore time.Time) (Entry, bool) {
	order := []*list.List{x.am, x.a1in}
	total := x.a1inBytes + x.amBytes
	if x.am.Len() == 0 || float64(x.a1inBytes) > x.cfg.KinRatio*float64(total) ||
		(total == 0 && x.a1in.Len() > 0) {
		order = []*list.List{x.a1in, x.am}
	}
	for _, l := range order {
		if el := l.Back(); el != nil {
			e := el.Value.(*Entry)
			if e.LastAccess.Before(notBefore) {
				return *e, true
			}
		}
	}
	return Entry{}, false
}

// Evicted records that k was removed to free space. A1in keys are kept in
// A1out as ghosts.
func (x *Index) Evicted(k Key) {
	el, ok := x.items[k]
	if !ok {
		return
	}
	fromA1in := el.Value.(*Entry).queue == A1in
	e := x.unlink(el)
	if fromA1in {
		e.Size = -1
		x.pushFront(A1out, e)
		x.trimGhost()
	}
}

func (x *Index) ghostCap() int {
	n := int(x.cfg.GhostRatio * float64(x.a1in.Len()+x.am.Len()))
	if n < x.cfg.MinGhost {
		n = x.cfg.MinGhost
	}
	return n
}

func (x *Index) trimGhost() {
	for limit := x.ghostCap(); x.a1out.Len() > limit; {
		x.unlink(x.a1out.Back())
	}
}

type Stats struct {
	A1inKeys  int   `json:"a1in_keys"`
	A1inBytes int64 `json:"a1in_bytes"`
	AmKeys    int   `json:"am_keys"`
	AmBytes   int64 `json:"am_bytes"`
	GhostKeys int   `json:"a1out_keys"`
}

func (x *Index) Stats() Stats {
	return Stats{x.a1in.Len(), x.a1inBytes, x.am.Len(), x.amBytes, x.a1out.Len()}
}

// Resident calls fn for every A1in and Am entry.
func (x *Index) Resident(fn func(Entry)) {
	for _, l := range []*list.List{x.a1in, x.am} {
		for el := l.Front(); el != nil; el = el.Next() {
			fn(*el.Value.(*Entry))
		}
	}
}

// SeedCold adds k to the cold end of A1in if it is not resident. Ghost
// entries for k are dropped. Used when discovering existing objects.
func (x *Index) SeedCold(k Key, size int64, lastAccess time.Time) {
	if el, ok := x.items[k]; ok {
		if el.Value.(*Entry).queue != A1out {
			return
		}
		x.unlink(el)
	}
	x.pushBack(A1in, &Entry{Key: k, Size: size, LastAccess: lastAccess})
}

// SnapshotEntry is one line of a persisted index, newest first per queue.
type SnapshotEntry struct {
	Bucket     string    `json:"b"`
	Object     string    `json:"k"`
	Size       int64     `json:"s"`
	Queue      Queue     `json:"q"`
	LastAccess time.Time `json:"t"`
}

// Snapshot returns all entries, front to back for each queue.
func (x *Index) Snapshot() []SnapshotEntry {
	out := make([]SnapshotEntry, 0, len(x.items))
	for _, l := range []*list.List{x.a1in, x.am, x.a1out} {
		for el := l.Front(); el != nil; el = el.Next() {
			e := el.Value.(*Entry)
			out = append(out, SnapshotEntry{e.Bucket, e.Object, e.Size, e.queue, e.LastAccess})
		}
	}
	return out
}

// Restore appends snapshot entries in order. Call on an empty index.
func (x *Index) Restore(entries []SnapshotEntry) {
	for _, s := range entries {
		k := Key{s.Bucket, s.Object}
		if _, dup := x.items[k]; dup || x.list(s.Queue) == nil {
			continue
		}
		x.pushBack(s.Queue, &Entry{Key: k, Size: s.Size, LastAccess: s.LastAccess})
	}
	x.trimGhost()
}
