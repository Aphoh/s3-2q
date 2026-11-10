package accesslog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const benchUA = "aws-cli/2.17.0 md/awscrt#0.20.11 ua/2.0 os/linux#6.8.0 md/arch#x86_64 lang/python#3.11.9 md/pyimpl#CPython cfg/retry-mode#standard md/installer#exe md/distrib#ubuntu.22 md/prompt#off md/command#s3.cp"

func benchLine(op, key string, status int, size int64) string {
	uri := "/bkt1/" + strings.ReplaceAll(key, " ", "%20")
	return fmt.Sprintf("admin bkt1 [09/November/2026:23:41:09 +0000] 127.0.0.1 admin S9SQVPPW1JWMLRBQ %s %s %s %d - 0 %d 1 1 - %s - 8XvtpZO4hpGAUXTi7FOEdvQb6PHwhRQFbArc49PfPha5GUB1JkAFMly142TDlR/dnJxyrQIXRAZPgc91glMOPXU= SigV4 - AuthHeader s3.us-east-1.amazonaws.com - arn:aws:s3:::%s Yes\n",
		op, key, uri, status, size, benchUA, uri)
}

func BenchmarkParse(b *testing.B) {
	for _, c := range []struct{ name, key string }{
		{"simple", "photos/2026/11/09/IMG_0001.jpg"},
		{"spaces", "my docs/quarterly report final v2.pdf"},
	} {
		line := benchLine("s3_GetObject", c.key, 200, 4<<20)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(line)))
			for b.Loop() {
				if _, err := Parse(line); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTail measures following a log from the start, i.e. the cost of
// draining a backlog.
func BenchmarkTail(b *testing.B) {
	const n = 100_000
	path := filepath.Join(b.TempDir(), "access.log")
	var sb strings.Builder
	for i := range n {
		sb.WriteString(benchLine("s3_GetObject", fmt.Sprintf("dir/obj-%07d", i), 200, 1024))
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(sb.Len()))
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(context.Background())
		seen := 0
		t := &Tailer{Path: path, FromStart: true, Poll: time.Millisecond}
		_ = t.Run(ctx, func(string) {
			if seen++; seen == n {
				cancel()
			}
		})
		cancel()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/line")
}
