// Command s3-2q follows a versitygw access log and deletes objects in 2Q
// order whenever the data directory's filesystem crosses a usage watermark.
//
// Optionally it supervises versitygw itself: everything after "--" is run
// as a child process, signals are forwarded, and the access log is rotated
// (rename + SIGHUP) once it exceeds -rotate-bytes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Aphoh/s3-2q/internal/accesslog"
	"github.com/Aphoh/s3-2q/internal/evictor"
	"github.com/Aphoh/s3-2q/internal/s3store"
	"github.com/Aphoh/s3-2q/internal/twoq"
)

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		dataDir        = flag.String("data-dir", "", "versitygw posix root; its filesystem usage drives eviction (required)")
		accessLog      = flag.String("access-log", "", "versitygw --access-log file to follow (required)")
		endpoint       = flag.String("endpoint", "http://127.0.0.1:7070", "versitygw S3 endpoint")
		region         = flag.String("region", "us-east-1", "S3 region")
		accessKey      = flag.String("access-key", envOr("ROOT_ACCESS_KEY", ""), "S3 access key (default $ROOT_ACCESS_KEY)")
		secretKey      = flag.String("secret-key", "", "S3 secret key (default $ROOT_SECRET_KEY)")
		high           = flag.Float64("high", 90, "start evicting when the filesystem is at least this % used")
		low            = flag.Float64("low", 80, "evict until usage drops below this %")
		interval       = flag.Duration("interval", 5*time.Second, "disk usage check interval")
		minAge         = flag.Duration("min-age", 30*time.Second, "never evict objects written or read more recently than this")
		batch          = flag.Int("batch", 64, "objects deleted between disk usage checks")
		kin            = flag.Float64("kin", 0.25, "share of tracked bytes A1in (probation) may hold before it is evicted first")
		ghost          = flag.Float64("ghost-ratio", 0.5, "A1out ghost keys kept per resident key")
		classic        = flag.Bool("classic", false, "classic 2Q: a hit in A1in does not promote to Am (only ghost hits do)")
		countHead      = flag.Bool("count-head", false, "count HEAD requests as accesses")
		buckets        = flag.String("buckets", "", "comma-separated buckets to manage (default: all)")
		reconcileEvery = flag.Duration("reconcile", 10*time.Minute, "how often to re-list the store to catch missed changes (0 disables)")
		snapshot       = flag.String("snapshot", "", "persist the 2Q index here so hotness survives restarts")
		snapshotEvery  = flag.Duration("snapshot-every", time.Minute, "snapshot interval")
		fromStart      = flag.Bool("from-start", false, "replay the existing access log instead of starting at its end")
		listen         = flag.String("listen", "127.0.0.1:7071", "address for /status and /healthz (empty disables)")
		rotateBytes    = flag.Int64("rotate-bytes", 64<<20, "when supervising versitygw, rotate the access log past this size (0 disables)")
		dryRun         = flag.Bool("dry-run", false, "log what would be evicted without deleting")
		verbose        = flag.Bool("v", false, "debug logging")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] [-- versitygw args...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	child := flag.Args()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("component", "s3-2q")

	if *secretKey == "" {
		*secretKey = os.Getenv("ROOT_SECRET_KEY")
	}
	switch {
	case *dataDir == "" || *accessLog == "":
		fatal(log, "-data-dir and -access-log are required")
	case *accessKey == "" || *secretKey == "":
		fatal(log, "S3 credentials required (-access-key/-secret-key or ROOT_ACCESS_KEY/ROOT_SECRET_KEY)")
	case *low <= 0 || *high > 100 || *low >= *high:
		fatal(log, "need 0 < -low < -high <= 100")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var bl []string
	for _, b := range strings.Split(*buckets, ",") {
		if b = strings.TrimSpace(b); b != "" {
			bl = append(bl, b)
		}
	}
	ev := evictor.New(evictor.Config{
		DataDir:       *dataDir,
		HighWatermark: *high,
		LowWatermark:  *low,
		Interval:      *interval,
		MinAge:        *minAge,
		BatchSize:     *batch,
		CountHead:     *countHead,
		DryRun:        *dryRun,
		Buckets:       bl,
		TwoQ:          twoq.Config{KinRatio: *kin, GhostRatio: *ghost, PromoteOnA1inHit: !*classic},
	}, s3store.New(*endpoint, *region, *accessKey, *secretKey), evictor.StatfsUsage, log)

	if *snapshot != "" {
		n, err := ev.LoadSnapshot(*snapshot)
		if err != nil {
			log.Warn("ignoring unreadable snapshot", "path", *snapshot, "err", err)
		} else if n > 0 {
			log.Info("restored snapshot", "entries", n)
		}
	}

	var proc *exec.Cmd
	childDone := make(chan error, 1)
	if len(child) > 0 {
		proc = exec.Command(child[0], child[1:]...)
		proc.Stdin, proc.Stdout, proc.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := proc.Start(); err != nil {
			fatal(log, "start child", "err", err)
		}
		log.Info("started child", "cmd", child[0], "pid", proc.Process.Pid)
		go func() { childDone <- proc.Wait() }()
		if *rotateBytes > 0 {
			go rotateLoop(ctx, log, *accessLog, *rotateBytes, proc)
		}
	}

	go func() {
		t := &accesslog.Tailer{Path: *accessLog, FromStart: *fromStart}
		if err := t.Run(ctx, ev.HandleLine); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("access log tailer stopped", "err", err)
			cancel()
		}
	}()
	go ev.Run(ctx, *reconcileEvery)
	if *snapshot != "" {
		go func() {
			t := time.NewTicker(*snapshotEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := ev.SaveSnapshot(*snapshot); err != nil {
						log.Warn("snapshot failed", "err", err)
					}
				}
			}
		}()
	}
	if *listen != "" {
		go serveStatus(log, *listen, ev)
	}
	log.Info("running", "data_dir", *dataDir, "access_log", *accessLog, "high", *high, "low", *low)

	exitCode := 0
	select {
	case <-ctx.Done():
		if proc != nil {
			_ = proc.Process.Signal(syscall.SIGTERM)
			select {
			case <-childDone:
			case <-time.After(20 * time.Second):
				_ = proc.Process.Kill()
			}
		}
	case err := <-childDone:
		log.Error("child exited", "err", err)
		exitCode = 1
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() > 0 {
			exitCode = ee.ExitCode()
		}
		cancel()
	}
	if *snapshot != "" {
		if err := ev.SaveSnapshot(*snapshot); err != nil {
			log.Warn("final snapshot failed", "err", err)
		}
	}
	os.Exit(exitCode)
}

func fatal(log *slog.Logger, msg string, args ...any) {
	log.Error(msg, args...)
	os.Exit(2)
}

// rotateLoop renames the access log to <path>.1 and asks versitygw to reopen
// it (SIGHUP) whenever it grows past limit. The tailer drains the old file.
func rotateLoop(ctx context.Context, log *slog.Logger, path string, limit int64, proc *exec.Cmd) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st, err := os.Stat(path)
		if err != nil || st.Size() < limit {
			continue
		}
		if err := os.Rename(path, path+".1"); err != nil {
			log.Warn("rotate access log", "err", err)
			continue
		}
		if err := proc.Process.Signal(syscall.SIGHUP); err != nil {
			log.Warn("signal child", "err", err)
		}
		log.Info("rotated access log", "size", st.Size())
	}
}

func serveStatus(log *slog.Logger, addr string, ev *evictor.Evictor) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(ev.Status())
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("status server", "err", err)
	}
}
