#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Regenerate one released PostgreSQL fixture: <state>/catalog.sql and
# <state>/data.sql in this directory.
#
# Usage: regenerate.sh <state> <n>
#
#   <state>  0.1.5 ... 0.1.14 (the published extenddb/extenddb-postgres image
#            of that version), pv-0.1.11 or pv-0.1.14 (the same image on
#            pgvector), or v0.1.0 and v0.1.2 (built from that tag's SQL files,
#            catalog-only, because no image of those versions was published).
#   <n>      A number that tells concurrent runs apart. The containers are
#            $FIXTURE_PREFIX-<n> (PostgreSQL) and $FIXTURE_PREFIX-<n>-init /
#            $FIXTURE_PREFIX-<n>-srv (the release), the network is
#            $FIXTURE_PREFIX-net-<n>, and the release server listens on
#            127.0.0.1:$((FIXTURE_BASE_PORT + n)).
#
# FIXTURE_PREFIX defaults to extenddb-fixture-pg and FIXTURE_BASE_PORT to
# 21550; set them when those names or ports are taken.
#
# An image state runs the release's `init` as docker/README.md documents, its
# `serve`, `extenddb manage` for an account, a user, a policy and a key, then
# seed.py: a table with a sort key, a GSI, an LSI, TTL and a stream, items, and
# a backup (and on pgvector a table with a vector index). The server is
# stopped and both databases are dumped with `pg_dump --inserts --no-owner
# --no-privileges`; scrub.py replaces the encryption key, every password hash
# and the connection string's password with fixed test values. Everything the
# script creates is removed at the end, and the release image too when this
# run pulled it (KEEP_IMAGE=1 keeps it).
set -euo pipefail

STATE="$1"; N="$2"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
OUT="$HERE/$STATE"
PREFIX="${FIXTURE_PREFIX:-extenddb-fixture-pg}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/$PREFIX-$STATE.XXXXXX")"
NET="$PREFIX-net-$N"
PGC="$PREFIX-$N"
PORT=$((${FIXTURE_BASE_PORT:-21550} + N))
PGPW=PgAdminPw123
APPPW=ExtendDBApp123
ADMINPW=ExtendDBLocalAdmin123
CFG=/var/lib/extenddb/extenddb.toml
UIDGID="$(id -u):$(id -g)"
case "$STATE" in
  pv-*) VERSION="${STATE#pv-}"; PGIMG=pgvector/pgvector:pg16; VECTOR=1 ;;
  v*) VERSION=""; PGIMG=public.ecr.aws/docker/library/postgres:16.10-bookworm; VECTOR=0 ;;
  *) VERSION="$STATE"; PGIMG=public.ecr.aws/docker/library/postgres:16.10-bookworm; VECTOR=0 ;;
esac
IMG="extenddb/extenddb-postgres:$VERSION"
# An image that was here before this run stays.
HAD_IMAGE=0
if [ -n "$VERSION" ] && docker image inspect "$IMG" >/dev/null 2>&1; then
  HAD_IMAGE=1
fi

note() { echo "[$(date -u +%H:%M:%S)] $STATE: $*"; }
cleanup() {
  docker rm -f -v "$PGC-srv" "$PGC-init" "$PGC" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  if [ -n "$VERSION" ] && [ "$HAD_IMAGE" != 1 ] && [ "${KEEP_IMAGE:-0}" != 1 ]; then
    docker rmi "$IMG" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

psql_in() { docker exec -i "$PGC" psql -X -q -v ON_ERROR_STOP=1 -U "$@"; }

docker network create "$NET" >/dev/null
note "start $PGIMG as $PGC"
docker run -d --name "$PGC" --network "$NET" -e POSTGRES_PASSWORD="$PGPW" "$PGIMG" >/dev/null
for _ in $(seq 1 120); do
  docker exec "$PGC" pg_isready -h 127.0.0.1 -U postgres -q 2>/dev/null && break
  sleep 0.5
done

if [ -z "$VERSION" ]; then
  # A tag fixture: what that tag's `init` ran, from its own files.
  TAG="$STATE"
  note "build from the $TAG files"
  psql_in postgres -d postgres <<<"CREATE USER extenddb_app WITH PASSWORD '$APPPW'; GRANT extenddb_app TO postgres;"
  psql_in postgres -d postgres <<<"CREATE DATABASE extenddb_catalog OWNER extenddb_app; CREATE DATABASE extenddb OWNER extenddb_app;"
  git -C "$REPO" show "$TAG:crates/storage-postgres/migrations/001_schema.sql" | psql_in extenddb_app -d extenddb_catalog
  psql_in extenddb_app -d extenddb_catalog <<<"INSERT INTO schema_history (filename) VALUES ('001_schema.sql');"
  python3 "$HERE/scrub.py" --bootstrap "$TAG" | psql_in extenddb_app -d extenddb_catalog
  if [ "$TAG" = v0.1.0 ]; then
    # v0.1.0 ran data 001 and kept no data ledger.
    git -C "$REPO" show "$TAG:crates/storage-postgres/data_migrations/001_data_schema.sql" | psql_in extenddb_app -d extenddb
  else
    psql_in extenddb_app -d extenddb <<<"CREATE TABLE schema_history (filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());"
    for f in $(git -C "$REPO" ls-tree --name-only "$TAG" crates/storage-postgres/data_migrations/ | sort); do
      git -C "$REPO" show "$TAG:$f" | psql_in extenddb_app -d extenddb
      psql_in extenddb_app -d extenddb <<<"INSERT INTO schema_history (filename) VALUES ('$(basename "$f")');"
    done
  fi
else
  note "init with $IMG"
  docker pull -q "$IMG" >/dev/null
  mkdir -p "$WORK/state"
  docker run --rm --name "$PGC-init" --network "$NET" --user "$UIDGID" \
    -v "$WORK/state":/var/lib/extenddb -e HOME=/var/lib/extenddb \
    -e EXTENDDB_PG_PASSWORD="$PGPW" -e EXTENDDB_APP_PASSWORD="$APPPW" \
    -e EXTENDDB_ADMIN_USER=admin -e EXTENDDB_ADMIN_PASSWORD="$ADMINPW" \
    "$IMG" init --config "$CFG" --backend postgres --pg-host "$PGC" --pg-port 5432 \
    --pg-user postgres --extenddb-user extenddb_app --bind-addr 0.0.0.0 --tls-san localhost \
    > "$WORK/init.log" 2>&1 || { cat "$WORK/init.log"; exit 1; }
  note "serve on 127.0.0.1:$PORT"
  docker run -d --name "$PGC-srv" --network "$NET" --user "$UIDGID" \
    -v "$WORK/state":/var/lib/extenddb -e HOME=/var/lib/extenddb \
    -p "127.0.0.1:$PORT:18443" "$IMG" >/dev/null
  up=0
  for _ in $(seq 1 120); do
    if docker exec "$PGC-srv" extenddb healthcheck --config "$CFG" >/dev/null 2>&1; then up=1; break; fi
    sleep 0.5
  done
  [ "$up" = 1 ] || { docker logs "$PGC-srv"; exit 1; }
  M=(docker exec -e EXTENDDB_PASSWORD="$ADMINPW" "$PGC-srv" extenddb manage --user admin --config "$CFG")
  "${M[@]}" create-account --account-id 123456789012 --account-name seed >/dev/null
  "${M[@]}" create-user --account-id 123456789012 --user-name tester >/dev/null
  "${M[@]}" put-user-policy --account-id 123456789012 --user-name tester --policy-name ddb \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}' >/dev/null
  "${M[@]}" create-access-key --account-id 123456789012 --user-name tester > "$WORK/key.json"
  python3 "$HERE/seed.py" "https://127.0.0.1:$PORT" "$WORK/key.json" "$VECTOR" "$WORK/seed.json"
  docker stop -t 20 "$PGC-srv" >/dev/null
fi

note "dump"
mkdir -p "$OUT"
for pair in catalog:extenddb_catalog data:extenddb; do
  docker exec "$PGC" pg_dump -U postgres --inserts --no-owner --no-privileges "${pair#*:}" \
    | python3 "$HERE/scrub.py" --dump > "$OUT/${pair%%:*}.sql"
done
docker exec "$PGC" psql -X -At -U postgres -c "SELECT version()" > "$OUT/postgres-version.txt"
if [ -n "$VERSION" ]; then
  docker run --rm --entrypoint extenddb "$IMG" --version > "$OUT/release-version.txt" 2>&1 || true
  python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print("\n".join(("ok " if x["ok"] else "FAILED ")+x["step"] for x in r))' \
    "$WORK/seed.json" > "$OUT/seed-steps.txt"
else
  echo "built from the $STATE files, catalog-only" > "$OUT/release-version.txt"
fi
note "wrote $OUT"
