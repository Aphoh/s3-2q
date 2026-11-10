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
	"math"
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

// Entry is a copy of a tracked object. Size is -1 when unknown.
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

// node is an intrusive list element, so each key costs one allocation.
type node struct {
	key        Key
	size       int64
	last       int64 // unix nanos; noTime if unknown
	prev, next *node
	queue      Queue
	epoch      uint32 // last sweep that saw this key
}

const noTime = math.MinInt64

// nanos converts t to unix nanos, saturating outside the int64 range
// (roughly years 1678-2262).
func nanos(t time.Time) int64 {
	switch sec := t.Unix(); {
	case t.IsZero():
		return noTime
	case sec >= math.MaxInt64/int64(time.Second):
		return math.MaxInt64
	case sec <= math.MinInt64/int64(time.Second):
		return noTime + 1
	}
	return t.UnixNano()
}

func (n *node) entry() Entry {
	e := Entry{Key: n.key, Size: n.size, queue: n.queue}
	if n.last != noTime {
		e.LastAccess = time.Unix(0, n.last)
	}
	return e
}

// queue is a doubly linked list; front = newest / most recently used.
type queue struct {
	front, back *node
	len         int
	bytes       int64
}

func (q *queue) pushFront(n *node) {
	n.prev, n.next = nil, q.front
	if q.front != nil {
		q.front.prev = n
	} else {
		q.back = n
	}
	q.front = n
	q.len++
	q.bytes += knownSize(n.size)
}

func (q *queue) pushBack(n *node) {
	n.prev, n.next = q.back, nil
	if q.back != nil {
		q.back.next = n
	} else {
		q.front = n
	}
	q.back = n
	q.len++
	q.bytes += knownSize(n.size)
}

func (q *queue) remove(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		q.front = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		q.back = n.prev
	}
	n.prev, n.next = nil, nil
	q.len--
	q.bytes -= knownSize(n.size)
}

func (q *queue) moveToFront(n *node) {
	if q.front != n {
		q.remove(n)
		q.pushFront(n)
	}
}

type Index struct {
	cfg             Config
	a1in, am, a1out queue
	items           map[Key]*node
	epoch           uint32
	now             func() time.Time
}

func New(cfg Config) *Index {
	return &Index{cfg: cfg.withDefaults(), items: make(map[Key]*node), now: time.Now}
}

// SetClock overrides the time source (for tests).
func (x *Index) SetClock(now func() time.Time) { x.now = now }

func (x *Index) queue(q Queue) *queue {
	switch q {
	case A1in:
		return &x.a1in
	case Am:
		return &x.am
	case A1out:
		return &x.a1out
	}
	return nil
}

func knownSize(s int64) int64 {
	if s < 0 {
		return 0
	}
	return s
}

func (x *Index) unlink(n *node) {
	x.queue(n.queue).remove(n)
	delete(x.items, n.key)
	n.queue = None
}

// move takes n out of its queue and puts it at the front of q.
func (x *Index) move(n *node, q Queue) {
	x.queue(n.queue).remove(n)
	n.queue = q
	x.queue(q).pushFront(n)
}

func (x *Index) insert(q Queue, n *node, front bool) {
	n.queue = q
	x.items[n.key] = n
	if front {
		x.queue(q).pushFront(n)
	} else {
		x.queue(q).pushBack(n)
	}
}

// setSize updates n's size and its queue's byte count.
func (x *Index) setSize(n *node, size int64) {
	if q := x.queue(n.queue); q != nil {
		q.bytes += knownSize(size) - knownSize(n.size)
	}
	n.size = size
}

// Get returns a copy of the entry for k.
func (x *Index) Get(k Key) (Entry, bool) {
	n, ok := x.items[k]
	if !ok {
		return Entry{}, false
	}
	return n.entry(), true
}

// Put records a write of k. size < 0 means unknown.
func (x *Index) Put(k Key, size int64) {
	now := nanos(x.now())
	if n, ok := x.items[k]; ok {
		switch n.queue {
		case A1in, Am:
			x.setSize(n, size)
			n.last = now
			if n.queue == Am {
				x.am.moveToFront(n)
			}
			return
		case A1out:
			x.a1out.remove(n)
			n.size, n.last, n.queue = size, now, Am
			x.am.pushFront(n)
			return
		}
	}
	x.insert(A1in, &node{key: k, size: size, last: now}, true)
	x.trimGhost()
}

// Access records a read of k. It reports whether k is resident; unknown and
// ghost keys are left untouched so the caller can decide how to learn about
// them (e.g. via Put once the size is known).
func (x *Index) Access(k Key) bool {
	n, ok := x.items[k]
	if !ok {
		return false
	}
	switch n.queue {
	case Am:
		n.last = nanos(x.now())
		x.am.moveToFront(n)
		return true
	case A1in:
		n.last = nanos(x.now())
		if x.cfg.PromoteOnA1inHit {
			x.move(n, Am)
		}
		return true
	}
	return false
}

// SetSize fills in the size of a resident key.
func (x *Index) SetSize(k Key, size int64) {
	if n, ok := x.items[k]; ok && (n.queue == A1in || n.queue == Am) {
		x.setSize(n, size)
	}
}

// Remove forgets k entirely (explicit delete); it does not enter A1out.
func (x *Index) Remove(k Key) {
	if n, ok := x.items[k]; ok {
		x.unlink(n)
	}
}

// Victim returns the next key to evict without changing the index. Keys
// accessed at or after notBefore are protected; ok is false when no
// eligible victim exists.
func (x *Index) Victim(notBefore time.Time) (Entry, bool) {
	first, second := &x.am, &x.a1in
	total := x.a1in.bytes + x.am.bytes
	if x.am.len == 0 || float64(x.a1in.bytes) > x.cfg.KinRatio*float64(total) ||
		(total == 0 && x.a1in.len > 0) {
		first, second = second, first
	}
	cutoff := nanos(notBefore)
	for _, q := range [2]*queue{first, second} {
		if n := q.back; n != nil && n.last < cutoff {
			return n.entry(), true
		}
	}
	return Entry{}, false
}

// Evicted records that k was removed to free space. A1in keys are kept in
// A1out as ghosts.
func (x *Index) Evicted(k Key) {
	n, ok := x.items[k]
	if !ok {
		return
	}
	if n.queue != A1in {
		x.unlink(n)
		return
	}
	x.a1in.remove(n)
	n.size, n.queue = -1, A1out
	x.a1out.pushFront(n)
	x.trimGhost()
}

func (x *Index) ghostCap() int {
	n := int(x.cfg.GhostRatio * float64(x.a1in.len+x.am.len))
	if n < x.cfg.MinGhost {
		n = x.cfg.MinGhost
	}
	return n
}

func (x *Index) trimGhost() {
	for limit := x.ghostCap(); x.a1out.len > limit; {
		x.unlink(x.a1out.back)
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
	return Stats{x.a1in.len, x.a1in.bytes, x.am.len, x.am.bytes, x.a1out.len}
}

// Resident calls fn for every A1in and Am entry.
func (x *Index) Resident(fn func(Entry)) {
	for _, q := range [2]*queue{&x.a1in, &x.am} {
		for n := q.front; n != nil; n = n.next {
			fn(n.entry())
		}
	}
}

// SeedCold adds k to the cold end of A1in if it is not resident. Ghost
// entries for k are dropped. Used when discovering existing objects.
func (x *Index) SeedCold(k Key, size int64, lastAccess time.Time) {
	if n, ok := x.items[k]; ok {
		if n.queue != A1out {
			return
		}
		x.a1out.remove(n)
		n.size, n.last, n.queue = size, nanos(lastAccess), A1in
		x.a1in.pushBack(n)
		return
	}
	x.insert(A1in, &node{key: k, size: size, last: nanos(lastAccess)}, false)
}

// BeginSweep starts a mark-and-sweep pass used to drop keys that no longer
// exist: call Seen for every existing key, then Sweep.
func (x *Index) BeginSweep() { x.epoch++ }

// Seen marks k as existing in the current sweep and returns its entry.
func (x *Index) Seen(k Key) (Entry, bool) {
	n, ok := x.items[k]
	if !ok {
		return Entry{}, false
	}
	n.epoch = x.epoch
	return n.entry(), true
}

// Sweep removes resident keys not marked by Seen since BeginSweep and last
// accessed before notBefore. It returns how many were removed.
func (x *Index) Sweep(notBefore time.Time) int {
	cutoff, removed := nanos(notBefore), 0
	for _, q := range [2]*queue{&x.a1in, &x.am} {
		for n := q.front; n != nil; {
			next := n.next
			if n.epoch != x.epoch && n.last < cutoff {
				x.unlink(n)
				removed++
			}
			n = next
		}
	}
	return removed
}

// SnapshotEntry is one entry of a persisted index, newest first per queue.
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
	for _, q := range [3]*queue{&x.a1in, &x.am, &x.a1out} {
		for n := q.front; n != nil; n = n.next {
			e := n.entry()
			out = append(out, SnapshotEntry{e.Bucket, e.Object, e.Size, e.queue, e.LastAccess})
		}
	}
	return out
}

// Restore appends snapshot entries in order. Call on an empty index.
func (x *Index) Restore(entries []SnapshotEntry) {
	if len(x.items) == 0 {
		x.items = make(map[Key]*node, len(entries))
	}
	for _, s := range entries {
		k := Key{s.Bucket, s.Object}
		if _, dup := x.items[k]; dup || x.queue(s.Queue) == nil {
			continue
		}
		x.insert(s.Queue, &node{key: k, size: s.Size, last: nanos(s.LastAccess)}, false)
	}
	x.trimGhost()
}
