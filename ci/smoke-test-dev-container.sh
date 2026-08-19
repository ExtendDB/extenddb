#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Smoke test for the `extenddb-dev` container image.
#
# What it proves, and why each check exists:
#
#   1. The image runs and its own HEALTHCHECK goes healthy (the healthcheck
#      subcommand probing plain HTTP is itself part of what shipped).
#   2. The data plane answers real SigV4-signed DynamoDB requests: CreateTable,
#      PutItem, GetItem round-trip through the AWS CLI.
#   3. FILE MODE PERSISTS: an item written before a full container restart is
#      readable after it. This is the discriminating assertion — a server that
#      recreates its schema on boot fails here, a health ping would not catch it.
#   4. MEMORY MODE DOES NOT PERSIST: the same restart loses the table
#      (ResourceNotFoundException). This is the negative control for check 3;
#      if both modes "persist", the mode switch is broken.
#   5. Labels carry the release identity (version, revision) and --version
#      agrees with them.
#   6. Hardening: runs as 65532 (nonroot), no shell in the runtime image.
#
# Requires: docker, aws, python3. Set EXTENDDB_IMAGE to test a prebuilt image
# (CI does this); otherwise the image is built from Dockerfile.dev.

set -euo pipefail

cd "$(cd "$(dirname "$0")/.." && pwd)"

for c in docker aws python3; do
    command -v "$c" >/dev/null || { echo "error: missing $c" >&2; exit 1; }
done

BUILT_IMAGE=0
if [[ -n ${EXTENDDB_IMAGE:-} ]]; then
    EXTENDDB_VERSION=$(docker image inspect "$EXTENDDB_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.version"}}')
    VCS_REF=$(docker image inspect "$EXTENDDB_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
    for value in "$EXTENDDB_VERSION" "$VCS_REF"; do
        [[ -n "$value" && "$value" != "<no value>" ]] \
            || { echo "error: prebuilt image is missing an OCI identity label" >&2; exit 1; }
    done
else
    BUILT_IMAGE=1
    EXTENDDB_IMAGE="extenddb-dev:smoke-$$-${RANDOM}"
    EXTENDDB_VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
    VCS_REF=$(git rev-parse HEAD)
    BUILD_DATE=$(TZ=UTC date '+%Y-%m-%dT%H:%M:%SZ')
    docker build -f Dockerfile.dev \
        --build-arg VERSION="$EXTENDDB_VERSION" \
        --build-arg VCS_REF="$VCS_REF" \
        --build-arg BUILD_DATE="$BUILD_DATE" \
        -t "$EXTENDDB_IMAGE" .
fi

RUN_ID="devsmoke-$$-${RANDOM}"
FILE_CTR="${RUN_ID}-file"
MEM_CTR="${RUN_ID}-mem"
MOVED_CTR="${RUN_ID}-moved"
VOLUME="${RUN_ID}-data"

cleanup() {
    docker rm -f "$FILE_CTR" "$MEM_CTR" "$MOVED_CTR" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
    if [[ "$BUILT_IMAGE" == 1 ]]; then
        docker image rm "$EXTENDDB_IMAGE" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

wait_for_health() {
    local ctr="$1" tries=0
    until [[ "$(docker inspect --format '{{.State.Health.Status}}' "$ctr")" == healthy ]]; do
        tries=$((tries + 1))
        if [[ $tries -gt 60 ]]; then
            echo "error: $ctr did not become healthy" >&2
            docker logs "$ctr" | tail -40 >&2
            exit 1
        fi
        sleep 2
    done
}

host_port() {
    local ctr="$1" cport="${2:-18080}"
    docker inspect "$ctr" \
        --format "{{(index (index .NetworkSettings.Ports \"${cport}/tcp\") 0).HostPort}}"
}

# Dev-mode seeds AWS's documented example credential (see seed_dev_credential
# in crates/server/src/serve.rs); requests must be signed with it. AWS_PAGER=""
# disables paging on CLI v2 and is ignored by v1, unlike --no-cli-pager which
# v1 rejects.
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
       AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
       AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

ddb() {
    local port="$1"; shift
    aws dynamodb "$@" --endpoint-url "http://127.0.0.1:${port}"
}

echo "== extenddb-dev smoke test =="
echo "  image:   $EXTENDDB_IMAGE"
echo "  version: $EXTENDDB_VERSION"
echo "  commit:  $VCS_REF"

echo "== identity: --version agrees with the labels =="
VERSION_OUT=$(docker run --rm --read-only "$EXTENDDB_IMAGE" --version)
echo "$VERSION_OUT"
grep -F "extenddb $EXTENDDB_VERSION" <<<"$VERSION_OUT" >/dev/null

echo "== hardening: nonroot uid, no shell =="
IMG_USER=$(docker inspect "$EXTENDDB_IMAGE" --format '{{.Config.User}}')
[[ "$IMG_USER" == "65532:65532" ]] || { echo "error: image user is '$IMG_USER', expected 65532:65532" >&2; exit 1; }
if docker run --rm --entrypoint /bin/sh "$EXTENDDB_IMAGE" -c true >/dev/null 2>&1; then
    echo "error: runtime image contains a shell" >&2; exit 1
fi

echo "== file mode: zero-config start, healthcheck, data plane =="
docker run -d --name "$FILE_CTR" -v "$VOLUME":/var/lib/extenddb \
    -p 127.0.0.1:0:18080 "$EXTENDDB_IMAGE" >/dev/null
wait_for_health "$FILE_CTR"
PORT=$(host_port "$FILE_CTR")
ddb "$PORT" create-table --table-name smoke \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
aws dynamodb wait table-exists --table-name smoke \
    --endpoint-url "http://127.0.0.1:${PORT}"
ddb "$PORT" put-item --table-name smoke \
    --item '{"pk":{"S":"k1"},"v":{"S":"persisted"}}' >/dev/null
GOT=$(ddb "$PORT" get-item --table-name smoke --key '{"pk":{"S":"k1"}}' \
      --query 'Item.v.S' --output text)
[[ "$GOT" == "persisted" ]] || { echo "error: read-your-write failed: '$GOT'" >&2; exit 1; }

echo "== file mode: the item must survive a full container restart =="
docker rm -f "$FILE_CTR" >/dev/null
docker run -d --name "$FILE_CTR" -v "$VOLUME":/var/lib/extenddb \
    -p 127.0.0.1:0:18080 "$EXTENDDB_IMAGE" >/dev/null
wait_for_health "$FILE_CTR"
PORT=$(host_port "$FILE_CTR")
GOT=$(ddb "$PORT" get-item --table-name smoke --key '{"pk":{"S":"k1"}}' \
      --query 'Item.v.S' --output text)
[[ "$GOT" == "persisted" ]] \
    || { echo "error: file mode did NOT persist across restart: '$GOT'" >&2; exit 1; }

echo "== memory mode: works, and must NOT survive a restart (negative control) =="
docker run -d --name "$MEM_CTR" \
    -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: \
    -p 127.0.0.1:0:18080 "$EXTENDDB_IMAGE" >/dev/null
wait_for_health "$MEM_CTR"
MPORT=$(host_port "$MEM_CTR")
ddb "$MPORT" create-table --table-name ephemeral \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
aws dynamodb wait table-exists --table-name ephemeral \
    --endpoint-url "http://127.0.0.1:${MPORT}"
ddb "$MPORT" put-item --table-name ephemeral \
    --item '{"pk":{"S":"k1"},"v":{"S":"gone-after-restart"}}' >/dev/null
docker restart "$MEM_CTR" >/dev/null
wait_for_health "$MEM_CTR"
MPORT=$(host_port "$MEM_CTR")
if ddb "$MPORT" get-item --table-name ephemeral --key '{"pk":{"S":"k1"}}' >/dev/null 2>&1; then
    echo "error: memory mode persisted across restart; mode switch is broken" >&2
    exit 1
fi

echo "== moved port: EXTENDDB__SERVER__PORT must be probed by the healthcheck =="
# Discriminates the env-aware healthcheck from a hardcoded one: with the probe
# pinned to the default port this container never reports healthy, so
# wait_for_health times out and the run fails. Guards against regressing to a
# hardcoded --endpoint whenever the default happens to match the test's port,
# and covers the upcoming default-port change for free.
MOVED_PORT=28123
docker run -d --name "$MOVED_CTR" \
    -e EXTENDDB__SERVER__PORT="$MOVED_PORT" \
    -p 127.0.0.1:0:"$MOVED_PORT" "$EXTENDDB_IMAGE" >/dev/null
wait_for_health "$MOVED_CTR"
VPORT=$(host_port "$MOVED_CTR" "$MOVED_PORT")
ddb "$VPORT" list-tables >/dev/null \
    || { echo "error: data plane unreachable on the moved port" >&2; exit 1; }
docker rm -f "$MOVED_CTR" >/dev/null

echo "== notices: the dev licence file is present in the image =="
docker cp "$FILE_CTR":/usr/share/doc/extenddb/SOFTWARE-LICENSE-NOTICES.html /tmp/"$RUN_ID"-notices.html
grep -q 'libsqlite3-sys' /tmp/"$RUN_ID"-notices.html \
    || { echo "error: shipped notices do not cover the sqlite dependency set" >&2; exit 1; }
rm -f /tmp/"$RUN_ID"-notices.html

echo "== PASS: extenddb-dev smoke test complete =="
