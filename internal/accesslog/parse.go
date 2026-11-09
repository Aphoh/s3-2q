// Package accesslog parses and follows versitygw's --access-log file.
package accesslog

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

// Record is the subset of a versitygw access-log line the evictor needs.
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
	f := strings.Split(line, " ")
	// owner bucket [date tz] ip requester reqid op = 8 tokens before the key.
	const keyStart = 8
	if len(f) < keyStart+8 || !strings.HasPrefix(f[2], "[") || !strings.HasSuffix(f[3], "]") {
		return Record{}, errSkip
	}
	r := Record{Bucket: f[1], Operation: f[7]}
	// Both the key and the user agent are client controlled and unquoted.
	// Take the rightmost URI whose decoded path is exactly the bucket plus
	// all tokens before it, since the real URI always follows the key.
	for i := len(f) - 6; i > keyStart; i-- {
		status, err := strconv.Atoi(f[i+1])
		if err != nil || status < 100 || status > 599 {
			continue
		}
		size, err := strconv.ParseInt(f[i+4], 10, 64)
		if err != nil {
			continue
		}
		if _, err := strconv.Atoi(f[i+3]); err != nil {
			continue
		}
		key, ok := keyFromURI(f[i], r.Bucket, strings.Join(f[keyStart:i], " "))
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
	p, err := url.PathUnescape(p)
	if err != nil || !strings.HasPrefix(p, "/") {
		return "", false
	}
	if logged == "-" {
		switch p {
		case "/" + bucket + "/-", "/-":
			return "-", true
		case "/" + bucket, "/" + bucket + "/", "/":
			return "", true
		}
		return "", false
	}
	if p == "/"+bucket+"/"+logged || p == "/"+logged {
		return logged, true
	}
	return "", false
}

// IsSkip reports whether err means the line is not an access record.
func IsSkip(err error) bool { return errors.Is(err, errSkip) }
