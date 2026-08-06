#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Build and exercise the PostgreSQL-backend container locally. All credentials,
# databases, containers, networks, volumes, and temporary files are disposable.

set -euo pipefail
exec < /dev/null

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    echo "error: Docker Compose v2 (or docker-compose) is required" >&2
    exit 1
fi

for command in docker aws python3; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command not found: $command" >&2
        exit 1
    fi
done

PROJECT="extenddb-container-smoke-$$-${RANDOM}"

if [[ -n ${EXTENDDB_IMAGE:-} ]]; then
    PREBUILT_IMAGE=true
    EXPECTED_IMAGE=$(docker image inspect "$EXTENDDB_IMAGE" --format '{{.Id}}')
    EXTENDDB_VERSION=$(docker image inspect "$EXTENDDB_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.version"}}')
    VCS_REF=$(docker image inspect "$EXTENDDB_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
    BUILD_DATE=$(docker image inspect "$EXTENDDB_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.created"}}')
    export EXTENDDB_VERSION VCS_REF BUILD_DATE
    for value in "$EXTENDDB_VERSION" "$VCS_REF" "$BUILD_DATE"; do
        if [[ -z "$value" || "$value" == "unknown" ]]; then
            echo "error: prebuilt image has incomplete OCI identity metadata" >&2
            exit 1
        fi
    done
else
    PREBUILT_IMAGE=false
    export EXTENDDB_IMAGE="extenddb-postgres:smoke-$$-${RANDOM}"
    export EXTENDDB_VERSION="${EXTENDDB_VERSION:-$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')}"
    export VCS_REF="${VCS_REF:-$(git rev-parse --short=12 HEAD)}"
    export BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
fi

export PG_ADMIN_USER="${PG_ADMIN_USER:-postgres}"
export PG_ADMIN_PASSWORD="${PG_ADMIN_PASSWORD:-ExtendDBLocalPostgres123}"
export APP_DB_USER="${APP_DB_USER:-extenddb_app}"
export APP_DB_PASSWORD="${APP_DB_PASSWORD:-ExtendDBLocalApp123}"
export EXTENDDB_ADMIN_USER="${EXTENDDB_ADMIN_USER:-admin}"
export EXTENDDB_ADMIN_PASSWORD="${EXTENDDB_ADMIN_PASSWORD:-ExtendDBLocalAdmin123}"
# Port 0 asks Docker to allocate an unused loopback port atomically.
export EXTENDDB_PORT="${EXTENDDB_PORT:-0}"

TEMP_DIR="$(mktemp -d /tmp/extenddb-container-smoke.XXXXXX)"
KEY_JSON="$TEMP_DIR/access-key.json"
KEY_ENV="$TEMP_DIR/access-key.env"
VERSION_OUTPUT="$TEMP_DIR/version.txt"
GET_JSON="$TEMP_DIR/get-item.json"
touch "$KEY_JSON" "$KEY_ENV"
chmod 600 "$KEY_JSON" "$KEY_ENV"

cleanup() {
    "${COMPOSE[@]}" -p "$PROJECT" down -v --remove-orphans >/dev/null 2>&1 || true
    if [[ "$PREBUILT_IMAGE" == "false" ]]; then
        docker image rm "$EXTENDDB_IMAGE" >/dev/null 2>&1 || true
    fi
    if command -v shred >/dev/null 2>&1; then
        shred -u "$KEY_JSON" "$KEY_ENV" 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

compose() {
    "${COMPOSE[@]}" -p "$PROJECT" "$@"
}

wait_for_health() {
    local container_id="$1"
    local status=""
    for _ in $(seq 1 90); do
        status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id")
        if [[ "$status" == "healthy" ]]; then
            return 0
        fi
        if [[ "$(docker inspect --format '{{.State.Running}}' "$container_id")" != "true" ]]; then
            docker logs "$container_id" >&2 || true
            return 1
        fi
        sleep 1
    done
    echo "error: container did not become healthy (last status: $status)" >&2
    docker logs "$container_id" >&2 || true
    return 1
}

host_port() {
    docker inspect --format '{{(index (index .NetworkSettings.Ports "18443/tcp") 0).HostPort}}' "$1"
}

echo "=== Building and starting local container stack ==="
echo "  project: $PROJECT"
echo "  image:   $EXTENDDB_IMAGE"
echo "  version: $EXTENDDB_VERSION"
echo "  commit:  $VCS_REF"
if [[ "$PREBUILT_IMAGE" == "true" ]]; then
    echo "  mode:    prebuilt (build disabled)"
    compose up -d --no-build
else
    echo "  mode:    build"
    compose up -d --build
    EXPECTED_IMAGE=$(docker image inspect "$EXTENDDB_IMAGE" --format '{{.Id}}')
fi

APP_CONTAINER=$(compose ps -q extenddb)
INIT_CONTAINER=$(compose ps -a -q extenddb-volume-init)
BOOTSTRAP_CONTAINER=$(compose ps -a -q extenddb-bootstrap)
if [[ -z "$APP_CONTAINER" || -z "$INIT_CONTAINER" || -z "$BOOTSTRAP_CONTAINER" ]]; then
    echo "error: Compose did not create the expected containers" >&2
    compose ps >&2
    exit 1
fi

INIT_EXIT=$(docker inspect --format '{{.State.ExitCode}}' "$INIT_CONTAINER")
if [[ "$INIT_EXIT" != "0" ]]; then
    docker logs "$INIT_CONTAINER" >&2 || true
    echo "error: volume init exited with $INIT_EXIT" >&2
    exit 1
fi

BOOTSTRAP_EXIT=$(docker inspect --format '{{.State.ExitCode}}' "$BOOTSTRAP_CONTAINER")
if [[ "$BOOTSTRAP_EXIT" != "0" ]]; then
    docker logs "$BOOTSTRAP_CONTAINER" >&2 || true
    echo "error: bootstrap exited with $BOOTSTRAP_EXIT" >&2
    exit 1
fi
docker logs "$BOOTSTRAP_CONTAINER" 2>&1 \
    | grep -F "bootstrap: no config detected; initializing" >/dev/null

wait_for_health "$APP_CONTAINER"
HOST_PORT=$(host_port "$APP_CONTAINER")

echo "=== Verifying image identity and hardening ==="
for container_id in "$INIT_CONTAINER" "$BOOTSTRAP_CONTAINER" "$APP_CONTAINER"; do
    [[ "$(docker inspect "$container_id" --format '{{.Image}}')" == "$EXPECTED_IMAGE" ]]
done
for container_id in "$BOOTSTRAP_CONTAINER" "$APP_CONTAINER"; do
    [[ "$(docker inspect "$container_id" --format '{{.Config.User}}')" == "10001:10001" ]]
    [[ "$(docker inspect "$container_id" --format '{{.HostConfig.ReadonlyRootfs}}')" == "true" ]]
    [[ "$(docker inspect "$container_id" --format '{{json .HostConfig.CapDrop}}')" == '["ALL"]' ]]
    [[ "$(docker inspect "$container_id" --format '{{json .HostConfig.SecurityOpt}}')" == '["no-new-privileges:true"]' ]]
done
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{.Config.User}}')" == "0:0" ]]
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{.HostConfig.ReadonlyRootfs}}')" == "true" ]]
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{json .HostConfig.CapDrop}}')" == '["ALL"]' ]]
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{json .HostConfig.CapAdd}}')" == '["CAP_CHOWN"]' ]]
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{json .HostConfig.SecurityOpt}}')" == '["no-new-privileges:true"]' ]]
[[ "$(docker inspect "$APP_CONTAINER" --format '{{(index (index .NetworkSettings.Ports "18443/tcp") 0).HostIp}}')" == "127.0.0.1" ]]

docker exec "$APP_CONTAINER" /bin/sh -ec '
    ! command -v postgres >/dev/null 2>&1
    test "$(stat -c %u:%g /var/lib/extenddb)" = 10001:10001
    test "$(stat -c %a /var/lib/extenddb/extenddb.toml)" = 600
    test "$(stat -c %a /var/lib/extenddb/.extenddb/tls/key.pem)" = 600
'

docker run --rm --user 0:0 --entrypoint /bin/sh "$EXTENDDB_IMAGE" \
    -ec 'test -z "$(find / -xdev -type f -perm /6000 -print -quit)"'

docker run --rm --read-only "$EXTENDDB_IMAGE" --version \
    | tee "$VERSION_OUTPUT"
grep -Fx "extenddb $EXTENDDB_VERSION" "$VERSION_OUTPUT" >/dev/null
grep -F "commit $VCS_REF" "$VERSION_OUTPUT" >/dev/null
grep -F "built $BUILD_DATE" "$VERSION_OUTPUT" >/dev/null

echo "=== Verifying deployment and provisioning API credentials ==="
docker exec "$APP_CONTAINER" extenddb verify \
    --config /var/lib/extenddb/extenddb.toml >/dev/null

export EXTENDDB_PASSWORD="$EXTENDDB_ADMIN_PASSWORD"
docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    create-account --account-id 123456789012 --account-name container-smoke >/dev/null

docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    create-user --account-id 123456789012 --user-name tester >/dev/null

docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    put-user-policy --account-id 123456789012 --user-name tester \
    --policy-name dynamodb-full \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    create-access-key --account-id 123456789012 --user-name tester > "$KEY_JSON"
unset EXTENDDB_PASSWORD

python3 - "$KEY_JSON" "$KEY_ENV" <<'PY'
import json
import shlex
import sys

data = json.load(open(sys.argv[1]))
access_key = data.get("access_key_id")
secret_key = data.get("secret_access_key")
if not access_key or not secret_key:
    raise SystemExit(f"unexpected create-access-key fields: {sorted(data)}")
with open(sys.argv[2], "w") as output:
    output.write("export AWS_ACCESS_KEY_ID=" + shlex.quote(access_key) + "\n")
    output.write("export AWS_SECRET_ACCESS_KEY=" + shlex.quote(secret_key) + "\n")
print("  temporary access key created: " + access_key[:8] + "...")
PY
# shellcheck disable=SC1090
source "$KEY_ENV"
unset AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=""

ENDPOINT="https://127.0.0.1:${HOST_PORT}"
AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
TABLE_NAME="container-smoke-$(date +%s)"

aws dynamodb create-table "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null

TABLE_STATUS=""
for _ in $(seq 1 60); do
    TABLE_STATUS=$(aws dynamodb describe-table "${AWS_ARGS[@]}" \
        --table-name "$TABLE_NAME" --query 'Table.TableStatus' \
        --output text 2>/dev/null || true)
    [[ "$TABLE_STATUS" == "ACTIVE" ]] && break
    sleep 0.25
done
[[ "$TABLE_STATUS" == "ACTIVE" ]]

aws dynamodb put-item "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --item '{"pk":{"S":"hello"},"msg":{"S":"world"}}' >/dev/null
aws dynamodb get-item "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --key '{"pk":{"S":"hello"}}' --consistent-read > "$GET_JSON"
python3 - "$GET_JSON" <<'PY'
import json
import sys

item = json.load(open(sys.argv[1])).get("Item", {})
assert item.get("pk", {}).get("S") == "hello", item
assert item.get("msg", {}).get("S") == "world", item
PY

echo "=== Restarting serving container and checking persistence ==="
compose restart extenddb >/dev/null
APP_CONTAINER=$(compose ps -q extenddb)
wait_for_health "$APP_CONTAINER"
HOST_PORT=$(host_port "$APP_CONTAINER")
ENDPOINT="https://127.0.0.1:${HOST_PORT}"
AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
aws dynamodb get-item "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --key '{"pk":{"S":"hello"}}' --consistent-read > "$GET_JSON"
python3 - "$GET_JSON" <<'PY'
import json
import sys

item = json.load(open(sys.argv[1])).get("Item", {})
assert item.get("msg", {}).get("S") == "world", item
PY

echo "=== Verifying graceful SIGTERM shutdown ==="
compose stop -t 20 extenddb >/dev/null
APP_CONTAINER=$(compose ps -a -q extenddb)
[[ "$(docker inspect "$APP_CONTAINER" --format '{{.State.Running}}')" == "false" ]]
[[ "$(docker inspect "$APP_CONTAINER" --format '{{.State.ExitCode}}')" == "0" ]]
compose start extenddb >/dev/null
APP_CONTAINER=$(compose ps -q extenddb)
wait_for_health "$APP_CONTAINER"

echo "=== Recreating stack and exercising migration bootstrap ==="
compose down --remove-orphans >/dev/null
compose up -d --no-build
INIT_CONTAINER=$(compose ps -a -q extenddb-volume-init)
BOOTSTRAP_CONTAINER=$(compose ps -a -q extenddb-bootstrap)
APP_CONTAINER=$(compose ps -q extenddb)
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{.State.ExitCode}}')" == "0" ]]
[[ "$(docker inspect "$BOOTSTRAP_CONTAINER" --format '{{.State.ExitCode}}')" == "0" ]]
docker logs "$BOOTSTRAP_CONTAINER" 2>&1 \
    | grep -F "bootstrap: existing config detected; running migrations" >/dev/null
for container_id in "$INIT_CONTAINER" "$BOOTSTRAP_CONTAINER" "$APP_CONTAINER"; do
    [[ "$(docker inspect "$container_id" --format '{{.Image}}')" == "$EXPECTED_IMAGE" ]]
done
wait_for_health "$APP_CONTAINER"
HOST_PORT=$(host_port "$APP_CONTAINER")
ENDPOINT="https://127.0.0.1:${HOST_PORT}"
AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
aws dynamodb get-item "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --key '{"pk":{"S":"hello"}}' --consistent-read > "$GET_JSON"
python3 - "$GET_JSON" <<'PY'
import json
import sys

item = json.load(open(sys.argv[1])).get("Item", {})
assert item.get("msg", {}).get("S") == "world", item
PY

echo "=== Container smoke test passed ==="
echo "  health: healthy"
echo "  API: CreateTable / PutItem / GetItem"
echo "  restart persistence: passed"
echo "  graceful SIGTERM: passed"
echo "  migration bootstrap: passed"
