#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Regenerate one dev-image fixture from a published release.
#
#   regenerate.sh <version> <port> <empty work directory> [--backup]
#
# Runs extenddb/extenddb-dev:<version> on 127.0.0.1:<port> with the work
# directory as its data volume, seeds it with seed.py, stops it, and dumps
# the database with dump.py to dev-image-<version>.sql beside this script
# (dev-image-<version>-backup.sql with --backup). The container is named
# $CONTAINER, by default extenddb-fixture-<version>, and is removed at the
# end. The work directory is left for the caller to delete.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
version=$1
port=$2
work=$3
backup=${4:-}
name=${CONTAINER:-extenddb-fixture-$version}
suffix=""
if [ "$backup" = "--backup" ]; then suffix="-backup"; fi

mkdir -p "$work"
if [ -n "$(ls -A "$work")" ]; then
    echo "$work is not empty" >&2
    exit 1
fi
docker run -d --name "$name" --user "$(id -u):$(id -g)" \
    -p "127.0.0.1:$port:18080" -v "$work:/var/lib/extenddb" \
    "extenddb/extenddb-dev:$version" > /dev/null
cleanup() { docker rm -f "$name" > /dev/null 2>&1 || true; }
trap cleanup EXIT

healthy=""
for _ in $(seq 1 120); do
    if curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/health" | grep -q 200; then
        healthy=yes
        break
    fi
    sleep 0.5
done
if [ -z "$healthy" ]; then
    docker logs "$name" >&2
    echo "the server did not become healthy" >&2
    exit 1
fi
python3 "$here/seed.py" "http://127.0.0.1:$port" $backup
docker stop -t 20 "$name" > /dev/null
python3 "$here/dump.py" "$work/extenddb.sqlite" "$here/dev-image-$version$suffix.sql"
