package accesslog

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"time"
)

// Tailer follows a log file that may be rotated by rename or truncated.
type Tailer struct {
	Path string
	// FromStart reads the existing contents instead of starting at the end.
	FromStart bool
	Poll      time.Duration
}

// Run calls fn for every complete line until ctx is done.
func (t *Tailer) Run(ctx context.Context, fn func(line string)) error {
	poll := t.Poll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	var (
		f       *os.File
		r       *bufio.Reader
		partial []byte
		offset  int64
	)
	open := func(seekEnd bool) error {
		nf, err := os.Open(t.Path)
		if err != nil {
			return err
		}
		offset = 0
		if seekEnd {
			if offset, err = nf.Seek(0, io.SeekEnd); err != nil {
				nf.Close()
				return err
			}
		}
		if f != nil {
			f.Close()
		}
		f, r, partial = nf, bufio.NewReaderSize(nf, 64<<10), nil
		return nil
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	first := true
	for {
		if f == nil {
			err := open(first && !t.FromStart)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			first = false
		}
		if f != nil {
			for {
				b, err := r.ReadBytes('\n')
				offset += int64(len(b))
				if err == nil {
					if len(partial) > 0 {
						b = append(partial, b...)
						partial = nil
					}
					fn(string(b))
					continue
				}
				partial = append(partial, b...)
				if !errors.Is(err, io.EOF) {
					return err
				}
				break
			}
			if rotated(f, t.Path, offset) {
				// Drain anything written to the old file before switching.
				if rest, _ := io.ReadAll(r); len(rest) > 0 {
					partial = append(partial, rest...)
				}
				if len(partial) > 0 {
					fn(string(partial))
				}
				partial = nil
				f.Close()
				f = nil
				continue
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func rotated(f *os.File, path string, offset int64) bool {
	cur, err := f.Stat()
	if err != nil {
		return true
	}
	st, err := os.Stat(path)
	if err != nil {
		// Renamed away and not yet recreated: keep reading the old file.
		return false
	}
	return !os.SameFile(cur, st) || st.Size() < offset
}
