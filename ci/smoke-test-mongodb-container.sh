#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Build and exercise the production MongoDB-backed ExtendDB image locally.
# MongoDB remains a separate replica-set service, exactly as it is for a
# customer deployment. All containers, credentials, volumes, and data are
# disposable.

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

for command in docker aws; do
    command -v "$command" >/dev/null || {
        echo "error: required command not found: $command" >&2
        exit 1
    }
done

PROJECT="extenddb-mongodb-smoke-$$-${RANDOM}"
COMPOSE_FILE=docker-compose.mongodb.yml
PREBUILT_IMAGE=false
TEMP_DIR="$(mktemp -d /tmp/extenddb-mongodb-smoke.XXXXXX)"
KEY_JSON="$TEMP_DIR/access-key.json"

if [[ -n ${EXTENDDB_MONGODB_IMAGE:-} ]]; then
    PREBUILT_IMAGE=true
    docker image inspect "$EXTENDDB_MONGODB_IMAGE" >/dev/null
else
    export EXTENDDB_MONGODB_IMAGE="extenddb-mongodb:smoke-$$-${RANDOM}"
    export EXTENDDB_VERSION="${EXTENDDB_VERSION:-$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')}"
    export VCS_REF="${VCS_REF:-$(git rev-parse --short=12 HEAD)}"
    export BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
fi

export EXTENDDB_ADMIN_USER="${EXTENDDB_ADMIN_USER:-admin}"
export EXTENDDB_ADMIN_PASSWORD="${EXTENDDB_ADMIN_PASSWORD:-ExtendDBLocalAdmin123}"
export EXTENDDB_PORT="${EXTENDDB_PORT:-0}"

cleanup() {
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" down -v --remove-orphans >/dev/null 2>&1 || true
    if [[ "$PREBUILT_IMAGE" == false ]]; then
        docker image rm "$EXTENDDB_MONGODB_IMAGE" >/dev/null 2>&1 || true
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

compose() {
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" "$@"
}

wait_for_health() {
    local container_id="$1"
    local status=""
    for _ in $(seq 1 120); do
        status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id")
        if [[ "$status" == healthy ]]; then
            return 0
        fi
        if [[ "$(docker inspect --format '{{.State.Running}}' "$container_id")" != true ]]; then
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

echo "=== Building and starting MongoDB container stack ==="
echo "  project: $PROJECT"
echo "  image:   $EXTENDDB_MONGODB_IMAGE"
if [[ "$PREBUILT_IMAGE" == true ]]; then
    compose up -d --no-build
else
    compose build
    compose up -d --no-build
fi

APP_CONTAINER=$(compose ps -q extenddb)
BOOTSTRAP_CONTAINER=$(compose ps -a -q extenddb-bootstrap)
MONGO_CONTAINER=$(compose ps -q mongodb)
if [[ -z "$APP_CONTAINER" || -z "$BOOTSTRAP_CONTAINER" || -z "$MONGO_CONTAINER" ]]; then
    echo "error: Compose did not create the expected containers" >&2
    compose ps >&2
    exit 1
fi

BOOTSTRAP_EXIT=$(docker inspect --format '{{.State.ExitCode}}' "$BOOTSTRAP_CONTAINER")
if [[ "$BOOTSTRAP_EXIT" != 0 ]]; then
    docker logs "$BOOTSTRAP_CONTAINER" >&2 || true
    echo "error: MongoDB bootstrap exited with $BOOTSTRAP_EXIT" >&2
    exit 1
fi

docker exec "$MONGO_CONTAINER" mongosh --quiet --eval 'rs.status().myState' | grep -qx 1
wait_for_health "$APP_CONTAINER"
HOST_PORT=$(host_port "$APP_CONTAINER")

echo "=== Verifying MongoDB image hardening ==="
for container_id in "$BOOTSTRAP_CONTAINER" "$APP_CONTAINER"; do
    [[ "$(docker inspect "$container_id" --format '{{.Config.User}}')" == 10001:10001 ]]
    [[ "$(docker inspect "$container_id" --format '{{.HostConfig.ReadonlyRootfs}}')" == true ]]
    [[ "$(docker inspect "$container_id" --format '{{json .HostConfig.CapDrop}}')" == '["ALL"]' ]]
    [[ "$(docker inspect "$container_id" --format '{{json .HostConfig.SecurityOpt}}')" == '["no-new-privileges:true"]' ]]
done

docker exec "$APP_CONTAINER" /bin/sh -ec '
    ! command -v mongod >/dev/null 2>&1
    ! command -v mongosh >/dev/null 2>&1
    test "$(stat -c %u:%g /var/lib/extenddb)" = 10001:10001
    test "$(stat -c %a /var/lib/extenddb/extenddb.toml)" = 600
    test "$(stat -c %a /var/lib/extenddb/.extenddb/tls/key.pem)" = 600
'

echo "=== Provisioning API credentials and exercising DynamoDB operations ==="
export EXTENDDB_PASSWORD="$EXTENDDB_ADMIN_PASSWORD"
docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    create-account --account-id 123456789012 --account-name mongodb-container-smoke >/dev/null
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

export KEY_JSON
eval "$(python3 - <<'PY'
import json
data = json.load(open(__import__('os').environ['KEY_JSON']))
print('export AWS_ACCESS_KEY_ID=' + repr(data['access_key_id']))
print('export AWS_SECRET_ACCESS_KEY=' + repr(data['secret_access_key']))
PY
)"
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true AWS_PAGER=""
ENDPOINT="https://127.0.0.1:${HOST_PORT}"
AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
TABLE_NAME="mongodb-container-smoke-$(date +%s)"

aws dynamodb create-table "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
aws dynamodb wait table-exists "${AWS_ARGS[@]}" --table-name "$TABLE_NAME"
aws dynamodb put-item "${AWS_ARGS[@]}" --table-name "$TABLE_NAME" \
    --item '{"pk":{"S":"hello"},"msg":{"S":"world"}}' >/dev/null
[[ "$(aws dynamodb get-item "${AWS_ARGS[@]}" --table-name "$TABLE_NAME" \
    --key '{"pk":{"S":"hello"}}' --consistent-read \
    --query 'Item.msg.S' --output text)" == world ]]

echo "=== Restarting ExtendDB and checking MongoDB-backed persistence ==="
compose restart extenddb >/dev/null
APP_CONTAINER=$(compose ps -q extenddb)
wait_for_health "$APP_CONTAINER"
HOST_PORT=$(host_port "$APP_CONTAINER")
ENDPOINT="https://127.0.0.1:${HOST_PORT}"
AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
[[ "$(aws dynamodb get-item "${AWS_ARGS[@]}" --table-name "$TABLE_NAME" \
    --key '{"pk":{"S":"hello"}}' --consistent-read \
    --query 'Item.msg.S' --output text)" == world ]]

echo "=== PASS: MongoDB-backed ExtendDB container smoke test complete ==="
