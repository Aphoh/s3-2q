# s3-2q

A small, single-node S3 server that evicts cold objects when its disk fills
up. It is [versitygw](https://github.com/versity/versitygw) (Apache-2.0, posix
backend) plus `s3-2q`, a Go sidecar that:

1. follows versitygw's `--access-log` and feeds every PUT/GET/DELETE into a
   byte-aware **2Q** index (A1in probation FIFO, Am protected LRU, A1out ghost
   keys), and
2. checks the data directory's filesystem every `-interval`; at `-high`% used
   it deletes victims through the S3 API (so versitygw's metadata stays
   consistent) until usage is below `-low`%.

Objects written once and never read (scans, bulk loads) stay in A1in and go
first; objects read again move to Am and survive. A key evicted from A1in is
remembered as a ghost, so if it is re-uploaded it goes straight to Am.

## Run it

Prebuilt images (linux/amd64, linux/arm64) are on Docker Hub as
[`aphoh/s3-2q`](https://hub.docker.com/r/aphoh/s3-2q).

Demo: a 64 MiB tmpfs as the disk, evicting from 70% down to 50%:

```sh
docker run --rm -p 7070:7070 -p 7071:7071 --tmpfs /data:size=64m \
  -e ROOT_ACCESS_KEY=admin -e ROOT_SECRET_KEY=adminsecret \
  aphoh/s3-2q -data-dir /data -access-log /state/access.log -listen :7071 \
  -high 70 -low 50 -min-age 2s -interval 1s \
  -- versitygw --port :7070 --access-log /state/access.log posix /data

# in another shell
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=adminsecret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:7070 s3 mb s3://demo
head -c 4M /dev/urandom > obj
for i in $(seq 1 20); do aws --endpoint-url http://localhost:7070 s3 cp obj s3://demo/obj-$i; done
aws --endpoint-url http://localhost:7070 s3 ls s3://demo/   # oldest objects evicted
curl localhost:7071/status                                  # usage, evictions, queue sizes
```

For real use, keep data and state on volumes:

```sh
docker run -d -p 7070:7070 -p 7071:7071 -v s3data:/data -v s3state:/state \
  -e ROOT_ACCESS_KEY=admin -e ROOT_SECRET_KEY=change-me aphoh/s3-2q
```

Or build it yourself with `docker build -t s3-2q .`

`s3-2q` is the entrypoint and runs everything after `--` as its child
(versitygw), forwarding signals and rotating the access log (rename + SIGHUP)
past `-rotate-bytes`. To pass other flags, override the whole command, e.g.

```sh
docker run ... s3-2q -data-dir /data -access-log /state/access.log -snapshot /state/index.gz \
  -listen :7071 -high 85 -low 75 -- versitygw --port :7070 --access-log /state/access.log posix /data
```

It can also run without `--` next to a versitygw you start yourself; then
rotate the log however you like (rename or copytruncate both work).

Keep `/state` (access log, index snapshot) off the data filesystem if you
can, and don't write to `/data` behind versitygw's back: anything not seen in
the log is only picked up by the periodic re-list (`-reconcile`).

## Flags

| flag | default | |
|---|---|---|
| `-high` / `-low` | 90 / 80 | usage % at which to start / stop evicting |
| `-min-age` | 30s | never evict objects written or read more recently |
| `-kin` | 0.25 | share of tracked bytes A1in may hold before it is evicted first |
| `-classic` | false | classic 2Q: only ghost hits promote to Am. By default a re-read while in A1in promotes too, which suits object stores where objects are rarely rewritten |
| `-count-head` | false | count HEAD as an access |
| `-buckets` | all | comma-separated buckets to manage; others are never evicted |
| `-snapshot` | | persist the index so hotness survives restarts (written every `-snapshot-every` and on exit) |
| `-reconcile` | 10m | re-list all objects to catch changes missed in the log |
| `-dry-run` | false | log victims without deleting |

Run `s3-2q -h` for the rest.

## Notes

- On first start (or without a snapshot) existing objects are seeded cold in
  A1in, oldest `LastModified` first.
- `DeleteObjects` (multi-delete) doesn't log its keys, so it triggers a
  re-list. Copies and multipart completions log no size, so s3-2q HEADs them.
- The access log is asynchronous: an object read in the last moments before
  an eviction pass can still be picked. `-min-age` makes this unlikely.
- If everything is younger than `-min-age` and the disk is still above
  `-low`, `/status` reports `"stuck": true` and versitygw will start
  returning errors once the disk is full.

## Develop

```sh
go test -race ./...
# microbenchmarks (parsing, tailing, index ops, memory per key, snapshot, reconcile at 1M keys):
go test -run '^$' -bench . ./...
# end to end on a small tmpfs (as in CI):
docker run -d -p 7070:7070 -p 7071:7071 --tmpfs /data:size=64m -e ROOT_ACCESS_KEY=admin -e ROOT_SECRET_KEY=adminsecret \
  s3-2q -data-dir /data -access-log /state/access.log -listen :7071 -high 70 -low 50 -min-age 2s -interval 1s \
  -- versitygw --port :7070 --access-log /state/access.log posix /data
scripts/e2e.sh
```
