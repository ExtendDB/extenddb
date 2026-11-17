#!/usr/bin/env bash
# The backup job's kill points, at the binary level (design
# `docs/design/15-backup-restore.md`: the PR 8 acceptance and the testing
# summary). One deployment per backend, a server built with
# `cargo build --release --features extenddb-app/backup-test-hooks` (SQLite
# and S3 points: `--no-default-features --features sqlite,extenddb-app/backup-test-hooks`),
# `reconcile_interval_secs = 2`, `stale_job_after_secs = 45`, a 100k-item
# composite-key table with two GSIs. Run from a checkout; the API half is
# `devtools/backup-kill-points.py` beside this script.
#
#   devtools/backup-kill-points.sh <sqlite|postgres|s3> [after_data_file|before_manifest|after_manifest|sigterm ...]
#
# Per point: `after_data_file`, `before_manifest` and `after_manifest` set
# EXTENDDB_BACKUP_PAUSE on the server, run CreateBackup, wait for the "reached
# kill point" line, `kill -9` the server by its PID, and restart it without
# the pause; then the row and the prefix are watched through `extenddb backup
# list` and the store.
#   after_data_file: no manifest under the prefix after the kill; the row
#     CREATING, then FAILED (SQLite: the first cycle, the slot rule; PostgreSQL:
#     after the 45 s window), then the prefix deleted (once the row's heartbeat
#     is stale, within the window plus a cycle); DescribeBackup BackupNotFound.
#   before_manifest: the three documents (manifest-files, manifest-summary,
#     the sha256 sidecar) and the data file under the prefix after the kill
#     and no manifest; then as after_data_file (FAILED `owner dead`, the
#     prefix deleted once the heartbeat is stale): a prefix without
#     `extenddb-manifest.json` is never resolved AVAILABLE, whatever else is
#     under it.
#   after_manifest: the row AVAILABLE after the cycle with the manifest's
#     size and count, the prefix intact, DescribeBackup AVAILABLE.
#   sigterm: no pause; a larger table (SIGTERM_ITEMS, default 1,000,000);
#     SIGTERM while the row is CREATING; the row FAILED `shutdown`, the prefix
#     empty, `drained` in the log; on the S3 store no multipart upload left;
#     the exit is timed by polling /proc/<pid> at 50 ms.
# Every run is recorded under $LOGS_DIR/<backend>/<point>/ (LOGS_DIR defaults
# to discussions/kill-points under the checkout, which git ignores).
#
# Environment: EXTENDDB_BIN (the server binary built with the hooks; default
# target/release/extenddb under the checkout), ITEMS (default 100000),
# SIGTERM_ITEMS (default 1000000), LOGS_DIR, PG_PORT (default 55432) and
# S3_ENDPOINT (default http://127.0.0.1:9200, a moto server), the services
# the points use.
# `A && say ... || fail ...`: `say` never fails, so the `fail` branch runs only
# when the check before it fails.
# shellcheck disable=SC2015
set -u
BACKEND=${1:?backend}; shift
POINTS=("$@"); [ ${#POINTS[@]} -eq 0 ] && POINTS=(after_data_file before_manifest after_manifest sigterm)
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
LOGS=${LOGS_DIR:-$REPO/discussions/kill-points}/$BACKEND
ITEMS=${ITEMS:-100000}
SIGTERM_ITEMS=${SIGTERM_ITEMS:-1000000}
PG_PORT=${PG_PORT:-55432}
S3_ENDPOINT=${S3_ENDPOINT:-http://127.0.0.1:9200}
case $BACKEND in
  sqlite|s3|postgres) ;;
  *) echo "backend must be sqlite, postgres or s3"; exit 2 ;;
esac
BIN=${EXTENDDB_BIN:-$REPO/target/release/extenddb}
[ -x "$BIN" ] || { echo "build $BIN with --features extenddb-app/backup-test-hooks first (or set EXTENDDB_BIN)"; exit 2; }
"$BIN" version 2>/dev/null | grep -q "backup-test-hooks enabled" || { echo "$BIN was not built with --features extenddb-app/backup-test-hooks"; exit 2; }
KP="python3 $HERE/backup-kill-points.py"
export EXTENDDB_REPO=$REPO
export S3_ENDPOINT
export AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export EXTENDDB_ADMIN_PASSWORD='TestPass1!'

say() { echo "[$(date +%H:%M:%S)] $*"; }
# The points run in pipelines (subshells), so a failure is a FAIL line in the
# point's log, which the end of the script greps for.
fail() { say "FAIL: $*"; }

# --- one deployment per point --------------------------------------------------
# $1 = point; sets WORK, PORT, ROOT (filesystem) or BUCKET (s3), TAG
deploy() {
  local point=$1
  WORK=$LOGS/$point/work; rm -rf "$WORK"; mkdir -p "$WORK"; cd "$WORK" || exit 1
  PORT=$((19000 + RANDOM % 900))
  TAG="kp$(date +%s | tail -c 6)"
  case $BACKEND in
    sqlite|s3)
      "$BIN" init --backend sqlite --sqlite-path "$WORK/db.sqlite" --config extenddb.toml > init.log 2>&1 || { cat init.log; exit 1; } ;;
    postgres)
      "$BIN" init --pg-host 127.0.0.1 --pg-port "$PG_PORT" --pg-user postgres --pg-pass devpass \
        --data-db "${TAG}_data" --catalog-db "${TAG}_catalog" --extenddb-user "${TAG}_app" --config extenddb.toml > init.log 2>&1 || { cat init.log; exit 1; } ;;
  esac
  python3 -c "import sys; sys.path.insert(0, '$REPO/tests'); import lifecycle_helpers as lh; lh._patch_config_port('extenddb.toml', $PORT)"
  printf '\n[backup]\nreconcile_interval_secs = 2\nstale_job_after_secs = 45\n' >> extenddb.toml
  case $BACKEND in
    sqlite) ROOT=$WORK/backups ;;
    postgres) ROOT=$WORK/backups; mkdir -m 0700 "$ROOT"
      printf '[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "%s"\n' "$ROOT" >> extenddb.toml ;;
    s3) BUCKET="kp-$point-$(date +%s | tail -c 6)"
      python3 -c "import boto3; boto3.client('s3', endpoint_url='$S3_ENDPOINT', region_name='us-east-1').create_bucket(Bucket='$BUCKET')"
      printf '[backup.store]\nkind = "s3"\n[backup.store.s3]\nbucket = "%s"\nendpoint = "%s"\nforce_path_style = true\nregion = "us-east-1"\n' "$BUCKET" "$S3_ENDPOINT" >> extenddb.toml ;;
  esac
  export EXTENDDB_TEST_ENDPOINT="https://127.0.0.1:$PORT"
}

# start <logname> [env assignments...]
start() {
  local log=$1; shift
  env "$@" "$BIN" serve --foreground --config extenddb.toml > "$WORK/$log" 2>&1 &
  SPID=$!
  for _ in $(seq 1 100); do curl -sk "$EXTENDDB_TEST_ENDPOINT/health" > /dev/null 2>&1 && break; sleep 0.2; done
  curl -sk "$EXTENDDB_TEST_ENDPOINT/health" > /dev/null || { fail "server did not start"; tail -5 "$WORK/$log"; kill -9 "$SPID" 2>/dev/null; exit 1; }
  say "server pid $SPID on $PORT ($log)"
}

provision() {
  eval "$(cd "$REPO" && EXTENDDB_ADMIN_USER=admin EXTENDDB_CA_CERT=$HOME/.extenddb/tls/cert.pem python3 devtools/provision-test-credentials 2> "$WORK/creds.err")" || { cat "$WORK/creds.err"; exit 1; }
  "$BIN" settings --config extenddb.toml set control_plane_delay_seconds 0.05 > /dev/null 2>&1
}

# The row as `backup list` prints it (status, size, count, reason).
row() { "$BIN" backup list --config extenddb.toml 2>/dev/null | grep -F "$1" | head -1; }
row_failed() { "$BIN" backup list --failed --config extenddb.toml 2>/dev/null | grep -F "$1" | head -1; }
row_status() { local r; r=$(row "$1"); [ -n "$r" ] && echo "${r%% *}" || { r=$(row_failed "$1"); [ -n "$r" ] && echo "${r%% *}" || echo GONE; }; }

# The objects under the backup's prefix, one key per line.
prefix_keys() {
  local arn=$1 table=$2 id=${1##*/}
  case $BACKEND in
    sqlite|postgres) [ -d "$ROOT/123456789012/$table/$id" ] && (cd "$ROOT/123456789012/$table/$id" && find . -type f | sed 's|^\./||' | sort) ;;
    s3) python3 - "$BUCKET" "123456789012/$table/$id/" <<'EOF'
import boto3, os, sys
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1")
for o in s3.list_objects_v2(Bucket=sys.argv[1], Prefix=sys.argv[2]).get("Contents", []):
    print(o["Key"][len(sys.argv[2]):])
EOF
    ;;
  esac
}
manifest_json() {
  local arn=$1 table=$2 id=${1##*/}
  case $BACKEND in
    sqlite|postgres) cat "$ROOT/123456789012/$table/$id/extenddb-manifest.json" ;;
    s3) python3 -c "import boto3,sys; print(boto3.client('s3', endpoint_url='$S3_ENDPOINT', region_name='us-east-1').get_object(Bucket='$BUCKET', Key='123456789012/$table/$id/extenddb-manifest.json')['Body'].read().decode())" ;;
  esac
}
multipart_uploads() {
  python3 -c "import boto3; print(len(boto3.client('s3', endpoint_url='$S3_ENDPOINT', region_name='us-east-1').list_multipart_uploads(Bucket='$BUCKET').get('Uploads', [])))"
}

wait_status() { # arn expected timeout
  local arn=$1 want=$2 t=$3 s
  for _ in $(seq 1 "$t"); do s=$(row_status "$arn"); [ "$s" = "$want" ] && { echo "$s"; return 0; }; sleep 1; done
  echo "$s"; return 1
}

stop_server() { kill -TERM "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; }
teardown() {
  case $BACKEND in
    postgres) PGPASSWORD=devpass psql -h 127.0.0.1 -p "$PG_PORT" -U postgres -q -c "DROP DATABASE IF EXISTS ${TAG}_data" -c "DROP DATABASE IF EXISTS ${TAG}_catalog" -c "DROP ROLE IF EXISTS ${TAG}_app" > /dev/null 2>&1 ;;
    s3) python3 - "$BUCKET" <<'EOF'
import boto3, os, sys
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1")
b = sys.argv[1]
for o in s3.list_objects_v2(Bucket=b).get("Contents", []): s3.delete_object(Bucket=b, Key=o["Key"])
for u in s3.list_multipart_uploads(Bucket=b).get("Uploads", []): s3.abort_multipart_upload(Bucket=b, Key=u["Key"], UploadId=u["UploadId"])
s3.delete_bucket(Bucket=b)
EOF
    ;;
  esac
  rm -rf "$WORK/db.sqlite" "$WORK/db.sqlite-wal" "$WORK/db.sqlite-shm" "$WORK/backups" "$WORK/backup-spool" "$WORK/run"
}

# --- the points ---------------------------------------------------------------
run_pause_point() { # after_data_file | before_manifest | after_manifest
  local point=$1 table=kp_$1
  deploy "$point"
  start serve-paused.log EXTENDDB_BACKUP_PAUSE="$point"
  provision
  $KP create-table "$table" --items "$ITEMS" 2>> "$WORK/load.log" || { fail "load"; stop_server; teardown; return; }
  local arn; arn=$($KP create-backup "$table" "$point") || { fail "CreateBackup"; stop_server; teardown; return; }
  say "backup $arn"
  for _ in $(seq 1 600); do grep -q "reached kill point $point" "$WORK/serve-paused.log" && break; sleep 0.1; done
  grep -q "reached kill point $point" "$WORK/serve-paused.log" || { fail "the kill point was not reached"; stop_server; teardown; return; }
  say "reached kill point $point; row before the kill: $(row_status "$arn")"
  kill -9 "$SPID"; wait "$SPID" 2>/dev/null
  say "SIGKILLed $SPID"
  prefix_keys "$arn" "$table" > "$WORK/prefix-after-kill.txt"
  say "prefix after the kill: $(tr '\n' ' ' < "$WORK/prefix-after-kill.txt")"
  case $point in
    after_data_file)
      grep -q "extenddb-manifest.json" "$WORK/prefix-after-kill.txt" && fail "a manifest exists after the kill at after_data_file"
      grep -q "^data/000001.json.gz$" "$WORK/prefix-after-kill.txt" || fail "the data file is not under the prefix" ;;
    before_manifest)
      grep -q "^extenddb-manifest.json$" "$WORK/prefix-after-kill.txt" && fail "a manifest exists after the kill at before_manifest"
      for k in data/000001.json.gz manifest-files.json manifest-summary.json extenddb-manifest.sha256; do
        grep -q "^$k$" "$WORK/prefix-after-kill.txt" || fail "$k is not under the prefix after the kill"
      done
      [ "$(wc -l < "$WORK/prefix-after-kill.txt")" -eq 4 ] || fail "expected the four objects before the manifest put" ;;
    after_manifest)
      [ "$(wc -l < "$WORK/prefix-after-kill.txt")" -eq 5 ] || fail "expected the five objects after the manifest put" ;;
  esac
  [ "$(row_status "$arn")" = CREATING ] || fail "the row is $(row_status "$arn") after the kill, not CREATING"
  local killed_at; killed_at=$(date +%s)
  start serve-restarted.log
  case $point in
    after_data_file|before_manifest)
      local bound=8; [ "$BACKEND" = postgres ] && bound=70
      local s; s=$(wait_status "$arn" FAILED $bound) && say "FAILED after $(( $(date +%s) - killed_at )) s (bound $bound)" || fail "the row is $s after $bound s, not FAILED"
      row_failed "$arn" | grep -o 'failure_reason=.*' | tee "$WORK/reason.txt"
      grep -q "owner dead" "$WORK/reason.txt" || fail "the reason does not say the owner was dead"
      [ "$($KP status "$arn")" = NOTFOUND ] || fail "DescribeBackup is not BackupNotFound"
      # The prefix goes once the dead owner's heartbeat is stale (45 s from
      # the last beat before the kill) at the following cycle.
      for _ in $(seq 1 70); do [ -z "$(prefix_keys "$arn" "$table")" ] && break; sleep 1; done
      [ -z "$(prefix_keys "$arn" "$table")" ] && say "prefix deleted after $(( $(date +%s) - killed_at )) s" || fail "the prefix still holds: $(prefix_keys "$arn" "$table" | tr '\n' ' ')"
      grep -E "owner (was|is) dead|deleted .* object\(s\) left under the prefix" "$WORK/serve-restarted.log" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-260 ;;
    after_manifest)
      local bound=8; [ "$BACKEND" = postgres ] && bound=70
      local s; s=$(wait_status "$arn" AVAILABLE $bound) && say "AVAILABLE after $(( $(date +%s) - killed_at )) s (bound $bound)" || fail "the row is $s after $bound s, not AVAILABLE"
      manifest_json "$arn" "$table" > "$WORK/manifest.json"
      local msize mcount rsize rcount
      msize=$(python3 -c "import json; m=json.load(open('$WORK/manifest.json')); print(m['backup_size_bytes'])")
      mcount=$(python3 -c "import json; m=json.load(open('$WORK/manifest.json')); print(sum(f['item_count'] for f in m['data_files']))")
      read -r _ _ _ rsize rcount _ <<< "$(row "$arn")"
      say "manifest size=$msize count=$mcount; row size=$rsize count=$rcount"
      [ "$msize" = "$rsize" ] && [ "$mcount" = "$rcount" ] && [ "$mcount" = "$ITEMS" ] || fail "the row's size and count are not the manifest's"
      [ "$(prefix_keys "$arn" "$table" | wc -l)" -eq 5 ] || fail "the prefix did not survive whole"
      [ "$($KP status "$arn")" = AVAILABLE ] || fail "DescribeBackup is not AVAILABLE"
      $KP describe "$arn" > "$WORK/describe.json"
      grep -E "owner was dead" "$WORK/serve-restarted.log" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200 ;;
  esac
  stop_server
  teardown
}

run_sigterm() {
  local table=kp_sigterm
  deploy sigterm
  start serve.log
  provision
  # No GSIs on this table: the point is the upload's interruption, and a
  # SQLite load with two GSIs runs at a third of the speed.
  $KP create-table "$table" --items "$SIGTERM_ITEMS" --gsis 0 2>> "$WORK/load.log" || { fail "load"; stop_server; teardown; return; }
  local arn; arn=$($KP create-backup "$table" sigterm) || { fail "CreateBackup"; stop_server; teardown; return; }
  say "backup $arn"
  for _ in $(seq 1 300); do grep -q "backup $arn: started" "$WORK/serve.log" && break; sleep 0.1; done
  sleep 1
  local before; before=$($KP status "$arn")
  say "row before SIGTERM: $before"
  kill -TERM "$SPID"; local sent; sent=$(date +%s%3N)
  # The exit is timed by polling /proc/<pid> at 50 ms (a fixed sleep before
  # `wait` would report its own length as the exit time). A job that has not
  # ended 5 s after SIGTERM is dumped (every thread's stack) for the record
  # before the 30 s drain bound ends it.
  local waited=0
  while [ -d "/proc/$SPID" ] && [ -f "/proc/$SPID/status" ] && ! grep -q "^State:.*Z" "/proc/$SPID/status" 2>/dev/null; do
    sleep 0.05; waited=$(( waited + 50 ))
    if [ "$waited" -eq 5000 ]; then
      say "the server is still running 5 s after SIGTERM; dumping its threads to $WORK/stacks.txt"
      eu-stack -p "$SPID" > "$WORK/stacks.txt" 2>&1 || true
    fi
    [ "$waited" -ge 40000 ] && break
  done
  local gone; gone=$(date +%s%3N)
  wait "$SPID" 2>/dev/null
  say "server exited $(( gone - sent )) ms after SIGTERM (polled at 50 ms)"
  [ "$before" = CREATING ] || fail "the row was $before before SIGTERM: the backup had finished; raise SIGTERM_ITEMS"
  sed 's/\x1b\[[0-9;]*m//g' "$WORK/serve.log" | grep -E "backup job\(s\)|drained|FAILED \(shutdown\)" | cut -c1-220
  grep -q "drained" "$WORK/serve.log" || fail "no 'drained' line"
  grep -q "FAILED (shutdown); its objects were deleted" "$WORK/serve.log" || fail "no FAILED (shutdown) line with the objects deleted"
  local s; s=$(row_status "$arn"); [ "$s" = FAILED ] && say "row FAILED" || fail "the row is $s, not FAILED"
  row_failed "$arn" | grep -o 'failure_reason=.*' | tee "$WORK/reason.txt"
  grep -q 'failure_reason="shutdown"' "$WORK/reason.txt" || fail "the reason is not shutdown"
  local left; left=$(prefix_keys "$arn" "$table"); [ -z "$left" ] && say "prefix empty" || fail "the prefix still holds: $left"
  [ "$BACKEND" = s3 ] && { local n; n=$(multipart_uploads); [ "$n" = 0 ] && say "no multipart upload left" || fail "$n multipart upload(s) left"; }
  teardown
}

mkdir -p "$LOGS"
for point in "${POINTS[@]}"; do
  say "=== $BACKEND $point ==="
  case $point in
    after_data_file|before_manifest|after_manifest) run_pause_point "$point" 2>&1 | tee "$LOGS/$point.log" ;;
    sigterm) run_sigterm 2>&1 | tee "$LOGS/sigterm.log" ;;
    *) echo "unknown point $point"; exit 2 ;;
  esac
done
grep -l "FAIL:" "$LOGS"/*.log 2>/dev/null && exit 1
say "all points passed for $BACKEND"
