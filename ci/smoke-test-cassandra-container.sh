#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Build and exercise the production Cassandra-backed ExtendDB image locally.
# Cassandra remains a separate service, as it is for a real deployment. All
# containers, credentials, volumes, and data are disposable.
#
# Set EXTENDDB_CASSANDRA_IMAGE to test a prebuilt image without rebuilding it;
# its version, revision and build date are then read from its OCI labels.

# -E so the ERR trap below also fires inside functions.
set -Eeuo pipefail
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

for command in docker aws python3 timeout; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command not found: $command" >&2
        exit 1
    fi
done

PROJECT="extenddb-cassandra-smoke-$$-${RANDOM}"
COMPOSE_FILE=docker-compose.cassandra.yml

if [[ -n ${EXTENDDB_CASSANDRA_IMAGE:-} ]]; then
    PREBUILT_IMAGE=true
    EXPECTED_IMAGE=$(docker image inspect "$EXTENDDB_CASSANDRA_IMAGE" --format '{{.Id}}')
    EXTENDDB_VERSION=$(docker image inspect "$EXTENDDB_CASSANDRA_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.version"}}')
    VCS_REF=$(docker image inspect "$EXTENDDB_CASSANDRA_IMAGE" \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
    BUILD_DATE=$(docker image inspect "$EXTENDDB_CASSANDRA_IMAGE" \
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
    export EXTENDDB_CASSANDRA_IMAGE="extenddb-cassandra:smoke-$$-${RANDOM}"
    export EXTENDDB_VERSION="${EXTENDDB_VERSION:-$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')}"
    export VCS_REF="${VCS_REF:-$(git rev-parse --short=12 HEAD)}"
    export BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
fi

# Fresh random credentials for every run; the compose file refuses to start
# without them. Hex, which also fits init's character set for the
# application role's password.
random_secret() {
    python3 -c 'import secrets; print(secrets.token_hex(16))'
}
export CASSANDRA_SUPERUSER_PASSWORD="${CASSANDRA_SUPERUSER_PASSWORD:-$(random_secret)}"
export EXTENDDB_CASSANDRA_APP_PASSWORD="${EXTENDDB_CASSANDRA_APP_PASSWORD:-$(random_secret)}"
export EXTENDDB_ADMIN_USER="${EXTENDDB_ADMIN_USER:-admin}"
export EXTENDDB_ADMIN_PASSWORD="${EXTENDDB_ADMIN_PASSWORD:-$(random_secret)}"
# Port 0 asks Docker to allocate an unused loopback port atomically.
export EXTENDDB_PORT="${EXTENDDB_PORT:-0}"

TEMP_DIR="$(mktemp -d /tmp/extenddb-cassandra-smoke.XXXXXX)"
KEY_JSON="$TEMP_DIR/access-key.json"
KEY_ENV="$TEMP_DIR/access-key.env"
VERSION_OUTPUT="$TEMP_DIR/version.txt"
GET_JSON="$TEMP_DIR/get-item.json"
touch "$KEY_JSON" "$KEY_ENV"
chmod 600 "$KEY_JSON" "$KEY_ENV"

cleanup() {
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" down -v --remove-orphans >/dev/null 2>&1 || true
    if [[ "$PREBUILT_IMAGE" == "false" ]]; then
        docker image rm "$EXTENDDB_CASSANDRA_IMAGE" >/dev/null 2>&1 || true
    fi
    if command -v shred >/dev/null 2>&1; then
        shred -u "$KEY_JSON" "$KEY_ENV" 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT
# A failed bare assertion would otherwise exit 1 with nothing said and the
# stack already torn down. Name the line and keep the serving container's log.
on_error() {
    echo "error: line $1: $2" >&2
    if [[ -n ${APP_CONTAINER:-} ]]; then
        docker logs --tail 50 "$APP_CONTAINER" >&2 || true
    fi
}
trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR
APP_CONTAINER=

compose() {
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" "$@"
}

# The stack must refuse to start without each password, so a default added to
# the compose file later fails here. Checked by rendering the file with one
# variable empty at a time; nothing is started.
echo "=== Verifying the stack requires every password ==="
for variable in CASSANDRA_SUPERUSER_PASSWORD EXTENDDB_CASSANDRA_APP_PASSWORD EXTENDDB_ADMIN_PASSWORD; do
    if output=$(env "$variable=" "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" config 2>&1); then
        echo "error: the compose file renders with $variable empty" >&2
        exit 1
    fi
    if ! grep -qF "$variable" <<< "$output"; then
        echo "error: compose config failed with $variable empty, but not because of it:" >&2
        echo "$output" >&2
        exit 1
    fi
done

compose_pull_with_retry() {
    local max_attempts=4
    local attempt

    for attempt in $(seq 1 "$max_attempts"); do
        if compose pull "$@"; then
            return 0
        fi
        if (( attempt == max_attempts )); then
            echo "error: Compose pull failed after $max_attempts attempts" >&2
            return 1
        fi
        local delay=$((5 << (attempt - 1)))
        echo "warning: Compose pull failed; retrying in ${delay}s (attempt $((attempt + 1))/$max_attempts)" >&2
        sleep "$delay"
    done
}

wait_for_health() {
    local container_id="$1"
    local status=""
    for _ in $(seq 1 120); do
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

# cql USER PASSWORD STATEMENT: run one statement in the Cassandra container.
# The password reaches cqlsh through a 0600 credentials file written from
# stdin, never through a command line, which any user on the host can read.
cql() {
    printf '[plain_text_auth]\nusername = %s\npassword = %s\n' "$1" "$2" \
        | docker exec -i "$CASSANDRA_CONTAINER" /bin/sh -ec '
            umask 077
            creds=$(mktemp)
            trap '"'"'rm -f "$creds"'"'"' EXIT
            cat > "$creds"
            cqlsh --credentials "$creds" -e "$1"
        ' sh "$3"
}

# expect_bad_credentials USER PASSWORD: the login must be refused as such, not
# fail for some other reason.
expect_bad_credentials() {
    local output
    if output=$(cql "$1" "$2" 'SELECT release_version FROM system.local' 2>&1); then
        echo "error: Cassandra accepted the login for $1 that it must refuse" >&2
        exit 1
    fi
    if ! grep -qF "Bad credentials" <<< "$output"; then
        echo "error: login for $1 failed for a reason other than bad credentials:" >&2
        echo "$output" >&2
        exit 1
    fi
}

check_item() {
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
}

# Compose waits on every dependency condition and has no overall deadline, so
# a bootstrap that never exits would hold the run until the job timeout.
compose_up() {
    if ! timeout "${COMPOSE_UP_TIMEOUT:-600}" "${COMPOSE[@]}" -f "$COMPOSE_FILE" -p "$PROJECT" up -d "$@"; then
        echo "error: compose up did not complete" >&2
        compose ps -a >&2 || true
        for service in cassandra-auth-init extenddb-bootstrap; do
            container_id=$(compose ps -a -q "$service" || true)
            if [[ -n "$container_id" ]]; then
                docker logs "$container_id" >&2 || true
            fi
        done
        return 1
    fi
}

set_endpoint() {
    HOST_PORT=$(host_port "$APP_CONTAINER")
    ENDPOINT="https://127.0.0.1:${HOST_PORT}"
    AWS_ARGS=(--endpoint-url "$ENDPOINT" --region us-east-1 --no-verify-ssl)
}

echo "=== Building and starting Cassandra container stack ==="
echo "  project: $PROJECT"
echo "  image:   $EXTENDDB_CASSANDRA_IMAGE"
echo "  version: $EXTENDDB_VERSION"
echo "  commit:  $VCS_REF"
compose_pull_with_retry cassandra
if [[ "$PREBUILT_IMAGE" == "true" ]]; then
    echo "  mode:    prebuilt (build disabled)"
    compose_up --no-build
else
    echo "  mode:    build"
    # The build is not under COMPOSE_UP_TIMEOUT: a cold release build of the
    # image takes longer than any stack start should be allowed to. Built with
    # `docker build` rather than `compose build`, which needs buildx 0.17 or
    # later; the arguments are the compose file's.
    docker build -f Dockerfile.cassandra -t "$EXTENDDB_CASSANDRA_IMAGE" \
        --build-arg "VERSION=$EXTENDDB_VERSION" \
        --build-arg "VCS_REF=$VCS_REF" \
        --build-arg "BUILD_DATE=$BUILD_DATE" .
    compose_up --no-build
    EXPECTED_IMAGE=$(docker image inspect "$EXTENDDB_CASSANDRA_IMAGE" --format '{{.Id}}')
fi

APP_CONTAINER=$(compose ps -q extenddb)
INIT_CONTAINER=$(compose ps -a -q extenddb-volume-init)
BOOTSTRAP_CONTAINER=$(compose ps -a -q extenddb-bootstrap)
AUTH_INIT_CONTAINER=$(compose ps -a -q cassandra-auth-init)
CASSANDRA_CONTAINER=$(compose ps -q cassandra)
if [[ -z "$APP_CONTAINER" || -z "$INIT_CONTAINER" || -z "$BOOTSTRAP_CONTAINER" \
        || -z "$AUTH_INIT_CONTAINER" || -z "$CASSANDRA_CONTAINER" ]]; then
    echo "error: Compose did not create the expected containers" >&2
    compose ps -a >&2
    exit 1
fi

for container_id in "$AUTH_INIT_CONTAINER" "$INIT_CONTAINER" "$BOOTSTRAP_CONTAINER"; do
    exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
    if [[ "$exit_code" != "0" ]]; then
        docker logs "$container_id" >&2 || true
        echo "error: $(docker inspect --format '{{.Name}}' "$container_id") exited with $exit_code" >&2
        exit 1
    fi
done
docker logs "$BOOTSTRAP_CONTAINER" 2>&1 \
    | grep -F "bootstrap: no config detected; initializing" >/dev/null

wait_for_health "$APP_CONTAINER"
set_endpoint

echo "=== Verifying Cassandra authentication and exposure ==="
# The stock superuser password must have been replaced, password
# authentication must be on, the application role init created must log in
# with the password from the environment, and Cassandra must not be published
# on the host.
cql cassandra "$CASSANDRA_SUPERUSER_PASSWORD" 'SELECT release_version FROM system.local' >/dev/null
expect_bad_credentials cassandra cassandra
expect_bad_credentials cassandra "wrong-$(random_secret)"
cql extenddb "$EXTENDDB_CASSANDRA_APP_PASSWORD" 'SELECT release_version FROM system.local' >/dev/null
# CassandraAuthorizer is on, so the application role holds only the grants init
# gave it: it may read the catalog but may not change another role.
cql extenddb "$EXTENDDB_CASSANDRA_APP_PASSWORD" 'SELECT key FROM extenddb_catalog.settings LIMIT 1' >/dev/null
if output=$(cql extenddb "$EXTENDDB_CASSANDRA_APP_PASSWORD" \
        "ALTER ROLE cassandra WITH PASSWORD = 'wrong-$(random_secret)'" 2>&1); then
    echo "error: the application role changed the superuser's password" >&2
    exit 1
fi
if ! grep -qF "Unauthorized" <<< "$output"; then
    echo "error: ALTER ROLE as the application role failed for a reason other than authorization:" >&2
    echo "$output" >&2
    exit 1
fi
[[ "$(docker inspect "$CASSANDRA_CONTAINER" --format '{{len .HostConfig.PortBindings}}')" == "0" ]]
KEYSPACES=$(cql cassandra "$CASSANDRA_SUPERUSER_PASSWORD" 'DESCRIBE KEYSPACES')
tr -s ' ' '\n' <<< "$KEYSPACES" | grep -qx extenddb_catalog

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
[[ "$(docker inspect "$INIT_CONTAINER" --format '{{json .HostConfig.CapAdd}}')" == '["CAP_CHOWN"]' ]]
[[ "$(docker inspect "$APP_CONTAINER" --format '{{(index (index .NetworkSettings.Ports "18443/tcp") 0).HostIp}}')" == "127.0.0.1" ]]

# The image carries no Cassandra server, client or JVM. (A line starting
# with `!` never stops `sh -e`, so each absence is an explicit exit.)
docker exec "$APP_CONTAINER" /bin/sh -ec '
    for tool in cassandra cqlsh java; do
        if command -v "$tool" >/dev/null 2>&1; then
            echo "error: the image contains $tool" >&2
            exit 1
        fi
    done
    test "$(stat -c %u:%g /var/lib/extenddb)" = 10001:10001
    test "$(stat -c %a /var/lib/extenddb/extenddb.toml)" = 600
    test "$(stat -c %a /var/lib/extenddb/.extenddb/tls/key.pem)" = 600
    test -s /usr/share/doc/extenddb/SOFTWARE-LICENSE-NOTICES-CASSANDRA.html
'

# The serving config holds the application role's password from the
# environment and never the superuser's. Compared in Python with the values
# taken from the environment, so neither reaches a command line.
docker exec "$APP_CONTAINER" cat /var/lib/extenddb/extenddb.toml | python3 -c '
import os
import sys

config = sys.stdin.read()
if os.environ["EXTENDDB_CASSANDRA_APP_PASSWORD"] not in config:
    sys.exit("error: extenddb.toml does not hold EXTENDDB_CASSANDRA_APP_PASSWORD")
if os.environ["CASSANDRA_SUPERUSER_PASSWORD"] in config:
    sys.exit("error: extenddb.toml holds the Cassandra superuser password")
'

docker run --rm --user 0:0 --entrypoint /bin/sh "$EXTENDDB_CASSANDRA_IMAGE" \
    -ec 'test -z "$(find / -xdev -type f -perm /6000 -print -quit)"'

docker run --rm --read-only "$EXTENDDB_CASSANDRA_IMAGE" --version \
    | tee "$VERSION_OUTPUT"
grep -Fx "extenddb $EXTENDDB_VERSION" "$VERSION_OUTPUT" >/dev/null
grep -F "commit $VCS_REF" "$VERSION_OUTPUT" >/dev/null
grep -F "built $BUILD_DATE" "$VERSION_OUTPUT" >/dev/null

echo "=== Provisioning API credentials ==="
export EXTENDDB_PASSWORD="$EXTENDDB_ADMIN_PASSWORD"
docker exec -e EXTENDDB_PASSWORD "$APP_CONTAINER" \
    extenddb manage --user "$EXTENDDB_ADMIN_USER" \
    --config /var/lib/extenddb/extenddb.toml \
    create-account --account-id 123456789012 --account-name cassandra-container-smoke >/dev/null
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

TABLE_NAME="cassandra-container-smoke-$(date +%s)"

aws dynamodb create-table "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null

TABLE_STATUS=""
for _ in $(seq 1 120); do
    TABLE_STATUS=$(aws dynamodb describe-table "${AWS_ARGS[@]}" \
        --table-name "$TABLE_NAME" --query 'Table.TableStatus' \
        --output text 2>/dev/null || true)
    [[ "$TABLE_STATUS" == "ACTIVE" ]] && break
    sleep 0.5
done
[[ "$TABLE_STATUS" == "ACTIVE" ]]

aws dynamodb put-item "${AWS_ARGS[@]}" \
    --table-name "$TABLE_NAME" \
    --item '{"pk":{"S":"hello"},"msg":{"S":"world"}}' >/dev/null
check_item

echo "=== Restarting serving container and checking persistence ==="
compose restart extenddb >/dev/null
APP_CONTAINER=$(compose ps -q extenddb)
wait_for_health "$APP_CONTAINER"
set_endpoint
check_item

echo "=== Verifying graceful SIGTERM shutdown ==="
compose stop -t 20 extenddb >/dev/null
APP_CONTAINER=$(compose ps -a -q extenddb)
[[ "$(docker inspect "$APP_CONTAINER" --format '{{.State.Running}}')" == "false" ]]
[[ "$(docker inspect "$APP_CONTAINER" --format '{{.State.ExitCode}}')" == "0" ]]

echo "=== Recreating stack and exercising the migrate preview ==="
# Volumes survive `down` without -v, so this restarts Cassandra on its data,
# re-runs cassandra-auth-init against the replaced password, and takes the
# bootstrap's existing-config branch: the migrate preview, as the application
# role, finds the catalog current and exits 0. (Nothing is pending at this
# commit, so `migrate --yes` is not reached.)
compose down --remove-orphans >/dev/null
compose_up --no-build
INIT_CONTAINER=$(compose ps -a -q extenddb-volume-init)
BOOTSTRAP_CONTAINER=$(compose ps -a -q extenddb-bootstrap)
AUTH_INIT_CONTAINER=$(compose ps -a -q cassandra-auth-init)
APP_CONTAINER=$(compose ps -q extenddb)
CASSANDRA_CONTAINER=$(compose ps -q cassandra)
for container_id in "$AUTH_INIT_CONTAINER" "$INIT_CONTAINER" "$BOOTSTRAP_CONTAINER"; do
    exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
    if [[ "$exit_code" != "0" ]]; then
        docker logs "$container_id" >&2 || true
        echo "error: $(docker inspect --format '{{.Name}}' "$container_id") exited with $exit_code on restart" >&2
        exit 1
    fi
done
docker logs "$AUTH_INIT_CONTAINER" 2>&1 \
    | grep -F "cassandra-auth-init: superuser login works" >/dev/null
docker logs "$BOOTSTRAP_CONTAINER" 2>&1 \
    | grep -F "bootstrap: existing config detected; running migrations" >/dev/null
# The preview reached the catalog and read it, rather than exiting 0 for
# another reason.
docker logs "$BOOTSTRAP_CONTAINER" 2>&1 \
    | grep -F "Up to date: the catalog is at migration" >/dev/null
# The replaced superuser password survived the recreate.
expect_bad_credentials cassandra cassandra
cql cassandra "$CASSANDRA_SUPERUSER_PASSWORD" 'SELECT release_version FROM system.local' >/dev/null
for container_id in "$INIT_CONTAINER" "$BOOTSTRAP_CONTAINER" "$APP_CONTAINER"; do
    [[ "$(docker inspect "$container_id" --format '{{.Image}}')" == "$EXPECTED_IMAGE" ]]
done
wait_for_health "$APP_CONTAINER"
set_endpoint
check_item

echo "=== Cassandra container smoke test passed ==="
echo "  health: healthy"
echo "  Cassandra: password authentication and CassandraAuthorizer, stock superuser password replaced, not published"
echo "  API: CreateTable / PutItem / GetItem"
echo "  restart persistence: passed"
echo "  graceful SIGTERM: passed"
echo "  recreate and migrate preview: passed"
