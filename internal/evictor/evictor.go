// Package evictor keeps a versitygw data directory under a disk-usage
// watermark by deleting objects in 2Q order.
package evictor

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Aphoh/s3-2q/internal/accesslog"
	"github.com/Aphoh/s3-2q/internal/twoq"
)

// Store is the S3 surface the evictor needs.
type Store interface {
	ListBuckets(ctx context.Context) ([]string, error)
	ListObjects(ctx context.Context, bucket string, fn func(key string, size int64, modified time.Time)) error
	// Head returns exists=false (and no error) when the object is gone.
	Head(ctx context.Context, bucket, key string) (size int64, exists bool, err error)
	Delete(ctx context.Context, bucket, key string) error
}

// DiskUsage returns the used and total bytes of the filesystem at dir.
type DiskUsage func(dir string) (used, total int64, err error)

type Config struct {
	DataDir       string
	HighWatermark float64 // start evicting at this used %
	LowWatermark  float64 // stop evicting below this used %
	Interval      time.Duration
	MinAge        time.Duration // never evict keys touched more recently
	BatchSize     int
	CountHead     bool // HEAD requests count as accesses
	DryRun        bool
	Buckets       []string // restrict tracking to these buckets; empty = all
	TwoQ          twoq.Config
}

type Evictor struct {
	cfg   Config
	store Store
	usage DiskUsage
	log   *slog.Logger

	mu      sync.Mutex
	idx     *twoq.Index
	buckets map[string]string // interned bucket names

	resolve   chan twoq.Key
	reconcile chan struct{}

	statsMu sync.Mutex
	stats   Counters
}

type Counters struct {
	Evictions     int64     `json:"evictions"`
	EvictedBytes  int64     `json:"evicted_bytes"`
	DeleteErrors  int64     `json:"delete_errors"`
	Records       int64     `json:"records"`
	UsagePct      float64   `json:"usage_pct"`
	LastEviction  time.Time `json:"last_eviction"`
	LastReconcile time.Time `json:"last_reconcile"`
	Stuck         bool      `json:"stuck"` // above high watermark with nothing evictable
}

func New(cfg Config, store Store, usage DiskUsage, log *slog.Logger) *Evictor {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &Evictor{
		cfg:       cfg,
		store:     store,
		usage:     usage,
		log:       log,
		idx:       twoq.New(cfg.TwoQ),
		resolve:   make(chan twoq.Key, 4096),
		buckets:   make(map[string]string),
		reconcile: make(chan struct{}, 1),
	}
}

func (e *Evictor) tracked(bucket string) bool {
	if len(e.cfg.Buckets) == 0 {
		return true
	}
	for _, b := range e.cfg.Buckets {
		if b == bucket {
			return true
		}
	}
	return false
}

func (e *Evictor) needSize(k twoq.Key) {
	select {
	case e.resolve <- k:
	default: // reconcile will fill it in
	}
}

// RequestReconcile asks the reconcile loop to run soon.
func (e *Evictor) RequestReconcile() {
	select {
	case e.reconcile <- struct{}{}:
	default:
	}
}

// HandleLine parses and applies one access-log line.
func (e *Evictor) HandleLine(line string) {
	r, err := accesslog.Parse(line)
	if err != nil {
		if !accesslog.IsSkip(err) {
			e.log.Warn("bad access log line", "err", err)
		}
		return
	}
	e.Handle(r)
}

// Handle applies one access-log record to the index.
func (e *Evictor) Handle(r accesslog.Record) {
	if !e.tracked(r.Bucket) {
		return
	}
	e.statsMu.Lock()
	e.stats.Records++
	e.statsMu.Unlock()
	ok := r.HTTPStatus >= 200 && r.HTTPStatus < 300

	switch r.Operation {
	case "s3_DeleteObjects":
		if ok {
			e.RequestReconcile() // the log does not list the deleted keys
		}
		return
	case "s3_DeleteBucket":
		if ok {
			e.mu.Lock()
			var gone []twoq.Key
			e.idx.Resident(func(en twoq.Entry) {
				if en.Bucket == r.Bucket {
					gone = append(gone, en.Key)
				}
			})
			for _, g := range gone {
				e.idx.Remove(g)
			}
			e.mu.Unlock()
		}
		return
	}
	if r.Key == "" {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	k := twoq.Key{Bucket: e.intern(r.Bucket), Object: r.Key}
	switch r.Operation {
	case "s3_PutObject":
		if ok {
			e.idx.Put(owned(k), r.ObjectSize)
		}
	case "s3_CopyObject", "s3_CompleteMultipartUpload", "s3_PostObject":
		if ok {
			k = owned(k)
			e.idx.Put(k, -1)
			e.needSize(k)
		}
	case "s3_GetObject", "s3_HeadObject":
		if r.Operation == "s3_HeadObject" && !e.cfg.CountHead {
			return
		}
		switch {
		case r.HTTPStatus == 404:
			e.removeResident(k)
		case ok && !e.idx.Access(k):
			// An object we did not know about (or a ghost written behind
			// our back): learn it now.
			k = owned(k)
			e.idx.Put(k, -1)
			e.needSize(k)
		}
	case "s3_DeleteObject":
		if ok {
			e.removeResident(k)
		}
	}
}

// intern returns a shared copy of bucket so index keys do not each hold
// their own (or pin the log line it was parsed from). Requires e.mu.
func (e *Evictor) intern(bucket string) string {
	b, ok := e.buckets[bucket]
	if !ok {
		b = strings.Clone(bucket)
		e.buckets[b] = b
	}
	return b
}

// owned copies k's object name, which may alias an access-log line, before
// it is stored in the index.
func owned(k twoq.Key) twoq.Key {
	return twoq.Key{Bucket: k.Bucket, Object: strings.Clone(k.Object)}
}

// removeResident forgets a deleted object but keeps A1out ghosts: our own
// evictions show up in the log as deletes, and misses on evicted keys are
// exactly what the ghost list is for.
func (e *Evictor) removeResident(k twoq.Key) {
	if en, ok := e.idx.Get(k); ok && en.Queue() != twoq.A1out {
		e.idx.Remove(k)
	}
}

// Reconcile lists the store, seeds unknown objects at the cold end of A1in
// (oldest coldest) and drops entries for objects that no longer exist.
func (e *Evictor) Reconcile(ctx context.Context) error {
	start := time.Now()
	buckets := e.cfg.Buckets
	if len(buckets) == 0 {
		var err error
		if buckets, err = e.store.ListBuckets(ctx); err != nil {
			return err
		}
	}
	type obj struct {
		key  twoq.Key
		size int64
		mod  time.Time
	}
	var objs []obj
	for _, b := range buckets {
		err := e.store.ListObjects(ctx, b, func(key string, size int64, mod time.Time) {
			objs = append(objs, obj{twoq.Key{Bucket: b, Object: key}, size, mod})
		})
		if err != nil {
			return err
		}
	}
	// SeedCold appends to the back, so seed newest first.
	slices.SortFunc(objs, func(a, b obj) int { return b.mod.Compare(a.mod) })

	e.mu.Lock()
	added := 0
	e.idx.BeginSweep()
	for _, o := range objs {
		cur, ok := e.idx.Seen(o.key)
		switch {
		case !ok || cur.Queue() == twoq.A1out:
			o.key.Bucket = e.intern(o.key.Bucket)
			e.idx.SeedCold(o.key, o.size, o.mod)
			e.idx.Seen(o.key)
			added++
		case cur.Size != o.size:
			e.idx.SetSize(o.key, o.size)
		}
	}
	removed := e.idx.Sweep(start)
	st := e.idx.Stats()
	e.mu.Unlock()

	e.statsMu.Lock()
	e.stats.LastReconcile = time.Now()
	e.statsMu.Unlock()
	e.log.Info("reconciled", "objects", len(objs), "added", added, "removed", removed,
		"a1in", st.A1inKeys, "am", st.AmKeys, "ghost", st.GhostKeys, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// EvictOnce frees space if usage is at or above the high watermark.
func (e *Evictor) EvictOnce(ctx context.Context) error {
	usedB, total, err := e.usage(e.cfg.DataDir)
	if err != nil {
		return err
	}
	used := pct(usedB, total)
	e.setUsage(used, false)
	if used < e.cfg.HighWatermark {
		return nil
	}
	e.log.Info("disk above high watermark, evicting", "used_pct", round(used), "high", e.cfg.HighWatermark, "low", e.cfg.LowWatermark)
	var n, freed int64
	for used >= e.cfg.LowWatermark && ctx.Err() == nil {
		// Only pick what is needed to get under the low watermark, so a
		// large batch can't overshoot into hot data.
		batch := e.pickBatch(usedB - int64(e.cfg.LowWatermark/100*float64(total)))
		if len(batch) == 0 {
			e.log.Warn("above low watermark but nothing is evictable", "used_pct", round(used), "min_age", e.cfg.MinAge)
			e.setUsage(used, true)
			break
		}
		failed := 0
		for _, v := range batch {
			if e.cfg.DryRun {
				e.log.Info("dry-run evict", "bucket", v.Bucket, "key", v.Object, "size", v.Size, "queue", v.Queue())
				continue
			}
			if err := e.store.Delete(ctx, v.Bucket, v.Object); err != nil {
				failed++
				e.RequestReconcile() // re-adds it to the index if it still exists
				e.log.Warn("delete failed", "bucket", v.Bucket, "key", v.Object, "err", err)
				continue
			}
			n++
			if v.Size > 0 {
				freed += v.Size
			}
			e.log.Debug("evicted", "bucket", v.Bucket, "key", v.Object, "size", v.Size, "queue", v.Queue())
		}
		e.statsMu.Lock()
		e.stats.DeleteErrors += int64(failed)
		e.statsMu.Unlock()
		if e.cfg.DryRun || failed == len(batch) {
			break
		}
		if usedB, total, err = e.usage(e.cfg.DataDir); err != nil {
			return err
		}
		used = pct(usedB, total)
		e.setUsage(used, false)
	}
	e.statsMu.Lock()
	e.stats.Evictions += n
	e.stats.EvictedBytes += freed
	if n > 0 {
		e.stats.LastEviction = time.Now()
	}
	e.statsMu.Unlock()
	if n > 0 {
		e.log.Info("evicted", "objects", n, "bytes", freed, "used_pct", round(used))
	}
	return nil
}

// pickBatch removes up to BatchSize victims from the index, stopping once
// their known sizes add up to need bytes.
func (e *Evictor) pickBatch(need int64) []twoq.Entry {
	e.mu.Lock()
	defer e.mu.Unlock()
	notBefore := time.Now().Add(-e.cfg.MinAge)
	var out []twoq.Entry
	var sum int64
	for len(out) < e.cfg.BatchSize && (len(out) == 0 || sum <= need) {
		v, ok := e.idx.Victim(notBefore)
		if !ok {
			break
		}
		if !e.cfg.DryRun {
			e.idx.Evicted(v.Key)
		} else if len(out) > 0 && out[0].Key == v.Key {
			break
		}
		out = append(out, v)
		if v.Size > 0 {
			sum += v.Size
		}
		if e.cfg.DryRun {
			break
		}
	}
	return out
}

func (e *Evictor) setUsage(used float64, stuck bool) {
	e.statsMu.Lock()
	e.stats.UsagePct = round(used)
	e.stats.Stuck = stuck
	e.statsMu.Unlock()
}

func pct(used, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100 * float64(used) / float64(total)
}

func round(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }

type Status struct {
	Counters
	HighWatermark float64    `json:"high_watermark"`
	LowWatermark  float64    `json:"low_watermark"`
	Index         twoq.Stats `json:"index"`
}

func (e *Evictor) Status() Status {
	e.mu.Lock()
	st := e.idx.Stats()
	e.mu.Unlock()
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	return Status{e.stats, e.cfg.HighWatermark, e.cfg.LowWatermark, st}
}

// Run drives eviction, size resolution and periodic reconciliation until
// ctx is done. The initial reconcile is retried until the store is up.
func (e *Evictor) Run(ctx context.Context, reconcileEvery time.Duration) {
	go e.resolveSizes(ctx)
	for backoff := time.Second; ; backoff = min(2*backoff, 30*time.Second) {
		err := e.Reconcile(ctx)
		if err == nil {
			break
		}
		e.log.Warn("initial reconcile failed, retrying", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
	tick := time.NewTicker(e.cfg.Interval)
	defer tick.Stop()
	var recon <-chan time.Time
	if reconcileEvery > 0 {
		t := time.NewTicker(reconcileEvery)
		defer t.Stop()
		recon = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := e.EvictOnce(ctx); err != nil {
				e.log.Error("evict", "err", err)
			}
		case <-recon:
			e.runReconcile(ctx)
		case <-e.reconcile:
			e.runReconcile(ctx)
		}
	}
}

func (e *Evictor) runReconcile(ctx context.Context) {
	if err := e.Reconcile(ctx); err != nil {
		e.log.Error("reconcile", "err", err)
	}
}

func (e *Evictor) resolveSizes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case k := <-e.resolve:
			size, exists, err := e.store.Head(ctx, k.Bucket, k.Object)
			if err != nil {
				e.log.Debug("head failed", "bucket", k.Bucket, "key", k.Object, "err", err)
				continue
			}
			e.mu.Lock()
			if exists {
				e.idx.SetSize(k, size)
			} else {
				e.idx.Remove(k)
			}
			e.mu.Unlock()
		}
	}
}
