// Package accesslog parses and follows versitygw's --access-log file.
package accesslog

import (
	"errors"
	"strconv"
	"strings"
)

// Record is the subset of a versitygw access-log line the evictor needs.
// Its strings alias the parsed line; clone them before retaining.
type Record struct {
	Bucket     string
	Key        string
	Operation  string // e.g. "s3_GetObject"
	HTTPStatus int
	ObjectSize int64
}

var errSkip = errors.New("not an access record")

// Parse parses one line written by versitygw's FileLogger:
//
//	owner bucket [dd/Month/yyyy:hh:mm:ss -zzzz] ip requester reqid operation key uri status errcode bytes size total turnaround ...
//
// Fields are space separated and unquoted, so the key may itself contain
// spaces. The request URI is URL-encoded, so the key is located by finding
// the URI whose decoded path ends in the candidate key.
func Parse(line string) (Record, error) {
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "log starts ") || line == "" {
		return Record{}, errSkip
	}
	var buf [64]int
	starts := append(buf[:0], 0) // byte offset of each field
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			starts = append(starts, i+1)
		}
	}
	field := func(i int) string {
		end := len(line)
		if i+1 < len(starts) {
			end = starts[i+1] - 1
		}
		return line[starts[i]:end]
	}
	// owner bucket [date tz] ip requester reqid op = 8 tokens before the key.
	const keyStart = 8
	if len(starts) < keyStart+8 || !strings.HasPrefix(field(2), "[") || !strings.HasSuffix(field(3), "]") {
		return Record{}, errSkip
	}
	r := Record{Bucket: field(1), Operation: field(7)}
	// Both the key and the user agent are client controlled and unquoted.
	// Take the rightmost URI whose decoded path is exactly the bucket plus
	// all tokens before it, since the real URI always follows the key.
	for i := len(starts) - 6; i > keyStart; i-- {
		// strconv allocates an error per failed attempt, and most candidate
		// fields are not numbers, so check digits first.
		if !isUint(field(i+1)) || !isUint(field(i+3)) || !isUint(field(i+4)) {
			continue
		}
		status, err := strconv.Atoi(field(i + 1))
		if err != nil || status < 100 || status > 599 {
			continue
		}
		size, err := strconv.ParseInt(field(i+4), 10, 64)
		if err != nil {
			continue
		}
		key, ok := keyFromURI(field(i), r.Bucket, line[starts[keyStart]:starts[i]-1])
		if !ok {
			continue
		}
		r.Key, r.HTTPStatus, r.ObjectSize = key, status, size
		return r, nil
	}
	return Record{}, errSkip
}

// keyFromURI checks logged against the request URI and returns the object
// key ("" for bucket-level requests). Path-style and virtual-host style
// URIs are accepted.
func keyFromURI(uri, bucket, logged string) (string, bool) {
	p, _, _ := strings.Cut(uri, "?")
	if !strings.HasPrefix(p, "/") {
		return "", false
	}
	if logged == "-" {
		switch {
		case pathIs(p, "/", bucket, "/-"), pathIs(p, "/-"):
			return "-", true
		case pathIs(p, "/", bucket), pathIs(p, "/", bucket, "/"), p == "/":
			return "", true
		}
		return "", false
	}
	if pathIs(p, "/", bucket, "/", logged) || pathIs(p, "/", logged) {
		return logged, true
	}
	return "", false
}

// pathIs reports whether the percent-decoded escaped equals the
// concatenation of want, without allocating.
func pathIs(escaped string, want ...string) bool {
	seg, j := 0, 0
	next := func() (byte, bool) {
		for seg < len(want) && j == len(want[seg]) {
			seg, j = seg+1, 0
		}
		if seg == len(want) {
			return 0, false
		}
		j++
		return want[seg][j-1], true
	}
	for i := 0; i < len(escaped); i++ {
		c := escaped[i]
		if c == '%' {
			if i+2 >= len(escaped) || !isHex(escaped[i+1]) || !isHex(escaped[i+2]) {
				return false
			}
			c = unhex(escaped[i+1])<<4 | unhex(escaped[i+2])
			i += 2
		}
		if w, ok := next(); !ok || w != c {
			return false
		}
	}
	_, more := next()
	return !more
}

func isUint(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case c <= '9':
		return c - '0'
	case c <= 'F':
		return c - 'A' + 10
	}
	return c - 'a' + 10
}

// IsSkip reports whether err means the line is not an access record.
func IsSkip(err error) bool { return errors.Is(err, errSkip) }
