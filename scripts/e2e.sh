#!/usr/bin/env bash
# End-to-end check against a running versitygw + s3-2q whose data dir is a
# small filesystem (e.g. a 64 MiB tmpfs) with -high 70 -low 50 -min-age 2s.
#   ENDPOINT=http://127.0.0.1:7070 STATUS=http://127.0.0.1:7071 scripts/e2e.sh
set -euo pipefail
: "${ENDPOINT:=http://127.0.0.1:7070}" "${STATUS:=http://127.0.0.1:7071}"
export AWS_ACCESS_KEY_ID="${ROOT_ACCESS_KEY:-admin}" AWS_SECRET_ACCESS_KEY="${ROOT_SECRET_KEY:-adminsecret}" AWS_DEFAULT_REGION=us-east-1
s3() { aws --endpoint-url "$ENDPOINT" "$@"; }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
head -c $((4 << 20)) /dev/urandom >"$tmp/obj"

for _ in $(seq 30); do s3 s3api list-buckets >/dev/null 2>&1 && break; sleep 1; done
s3 s3 mb s3://e2e
for i in 00 01 02 03 04 05 06 07; do s3 s3 cp --quiet "$tmp/obj" "s3://e2e/first $i"; done
for _ in 1 2; do for i in 0 1 2; do s3 s3 cp --quiet "s3://e2e/first 0$i" "$tmp/out"; done; done
sleep 3 # let everything age past -min-age
for i in $(seq 0 9); do s3 s3 cp --quiet "$tmp/obj" "s3://e2e/scan $i"; sleep 0.3; done
sleep 4

left=$(s3 s3 ls s3://e2e/ | awk '{ $1=$2=$3=""; sub(/^ +/, ""); print }')
echo "remaining objects:"; echo "$left" | sed 's/^/  /'
curl -fsS "$STATUS/status"
fail=0
for i in 0 1 2; do grep -qx "first 0$i" <<<"$left" || { echo "FAIL: hot object 'first 0$i' was evicted"; fail=1; }; done
grep -qx "first 03" <<<"$left" && { echo "FAIL: cold object 'first 03' survived"; fail=1; }
used=$(curl -fsS "$STATUS/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["usage_pct"])')
python3 -c "import sys; sys.exit(0 if $used < 70 else 1)" || { echo "FAIL: usage $used% still above high watermark"; fail=1; }
[ $fail = 0 ] && echo PASS
exit $fail
