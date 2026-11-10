package accesslog

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const ua = "aws-cli/1.22.34 Python/3.10.12 Linux/6.8.0-1060-aws botocore/1.23.34 - tUtq0MEj SigV4 - AuthHeader s3.us-east-1.amazonaws.com - arn:aws:s3:::/bkt1 Yes"

func line(op, key, uri, status, size string) string {
	return "admin bkt1 [09/November/2026:23:41:09 +0000] 127.0.0.1 admin S9SQVPPW1JWMLRBQ " +
		op + " " + key + " " + uri + " " + status + " - 0 " + size + " 0 0 - " + ua + "\n"
}

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want Record
	}{
		{"spaces", line("s3_PutObject", "dir/a b  c.txt", "/bkt1/dir/a%20b%20%20c.txt", "200", "3"),
			Record{"bkt1", "dir/a b  c.txt", "s3_PutObject", 200, 3}},
		{"escaped", line("s3_HeadObject", "ünï%+?&=.txt", "/bkt1/%C3%BCn%C3%AF%25%2B%3F%26%3D.txt", "200", "0"),
			Record{"bkt1", "ünï%+?&=.txt", "s3_HeadObject", 200, 0}},
		{"trailing space", line("s3_PutObject", "trail/ ", "/bkt1/trail/%20", "200", "3"),
			Record{"bkt1", "trail/ ", "s3_PutObject", 200, 3}},
		{"query", line("s3_CompleteMultipartUpload", "big", "/bkt1/big?uploadId=a11e", "200", "0"),
			Record{"bkt1", "big", "s3_CompleteMultipartUpload", 200, 0}},
		{"bucket op", line("s3_DeleteObjects", "-", "/bkt1?delete", "200", "0"),
			Record{"bkt1", "", "s3_DeleteObjects", 200, 0}},
		{"dash key", line("s3_GetObject", "-", "/bkt1/-", "200", "0"),
			Record{"bkt1", "-", "s3_GetObject", 200, 0}},
		{"key looks like fields", line("s3_GetObject", "x /bkt1/x 200 - 0 0 y", "/bkt1/x%20/bkt1/x%20200%20-%200%200%20y", "404", "0"),
			Record{"bkt1", "x /bkt1/x 200 - 0 0 y", "s3_GetObject", 404, 0}},
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if err != nil || got != c.want {
			t.Errorf("%s: got %+v, %v; want %+v", c.name, got, err, c.want)
		}
	}
	for _, bad := range []string{"log starts 2026-11-09 23:40:49 +0000 UTC\n", "", "garbage line\n"} {
		if _, err := Parse(bad); !IsSkip(err) {
			t.Errorf("%q: want skip, got %v", bad, err)
		}
	}
}

type collector struct {
	mu    sync.Mutex
	lines []string
}

func (c *collector) add(s string) { c.mu.Lock(); c.lines = append(c.lines, s); c.mu.Unlock() }
func (c *collector) waitFor(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.lines) >= n {
			out := append([]string(nil), c.lines...)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("timed out: got %q", c.lines)
	return nil
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(s)
	f.Close()
}

func TestTailerRotationAndTruncation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	appendTo(t, path, "old\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &collector{}
	go (&Tailer{Path: path, Poll: 5 * time.Millisecond}).Run(ctx, c.add)
	time.Sleep(30 * time.Millisecond)

	appendTo(t, path, "a\nb")
	c.waitFor(t, 1)
	appendTo(t, path, "c\n")
	c.waitFor(t, 2)

	// Rename-style rotation: the writer keeps appending to the old inode
	// until it reopens.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	appendTo(t, path+".1", "d\n")
	time.Sleep(30 * time.Millisecond)
	appendTo(t, path, "e\n")
	c.waitFor(t, 4)

	// copytruncate-style rotation.
	time.Sleep(30 * time.Millisecond)
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	appendTo(t, path, "f\n")
	got := c.waitFor(t, 5)
	want := []string{"a\n", "bc\n", "d\n", "e\n", "f\n"}
	if len(got) != len(want) {
		t.Fatalf("got %q want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

func TestTailerWaitsForFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &collector{}
	go (&Tailer{Path: path, Poll: 5 * time.Millisecond}).Run(ctx, c.add)
	time.Sleep(20 * time.Millisecond)
	appendTo(t, path, "first\n")
	if got := c.waitFor(t, 1); got[0] != "first\n" {
		t.Fatalf("got %q", got)
	}
}

func FuzzPathIs(f *testing.F) {
	for _, s := range [][2]string{{"/b/a%20b", "/b/a b"}, {"/b/%zz", "/b/%zz"}, {"/b/%2", "/b/%2"}, {"/b/x+y", "/b/x+y"}, {"/%C3%BC", "/ü"}} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, escaped, want string) {
		dec, err := url.PathUnescape(escaped)
		exp := err == nil && dec == want
		mid := len(want) / 2
		if got := pathIs(escaped, want[:mid], "", want[mid:]); got != exp {
			t.Fatalf("pathIs(%q, %q) = %v, want %v", escaped, want, got, exp)
		}
	})
}
