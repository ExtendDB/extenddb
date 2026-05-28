#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors. Proprietary and confidential.
# All rights reserved. Unauthorized copying, distribution, or use is prohibited.
# THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND.
#
# Comprehensive CLI test suite for extenddb.
# Exercises every extenddb command and credential-passing combination.
#
# Prerequisites:
#   - extenddb binary built at ./target/release/extenddb
#   - PostgreSQL running on localhost (current user has superuser or createdb rights)
#   - AWS CLI installed
#   - jq installed
#   - No extenddb server currently running on the test port
#
# Usage: ./tests/cli/test-cli-comprehensive.sh 2>&1 | tee test-cli-output.txt

set -uo pipefail

# --- Constants ---
EXTENDDB=./target/release/extenddb
CONFIG=extenddb-test.toml
ADMIN_USER=testadmin
ADMIN_PASS='TestPass123!'
TEST_PORT=18321
PSQL=psql

# pg_query: run a psql query using credentials from the config file.
# Usage: pg_query <database> <sql>
pg_query() {
    local db="$1" sql="$2"
    local conn_str
    conn_str=$(grep -oP 'connection_string\s*=\s*"\K[^"]+' "$CONFIG" 2>/dev/null || true)
    if [[ -n "$conn_str" ]]; then
        # Replace database name in connection string
        local base="${conn_str%/*}"
        $PSQL -t -A -c "$sql" "${base}/${db}" 2>/dev/null
    else
        # Fallback: unix socket (trust auth)
        $PSQL -h /tmp -t -A -c "$sql" "$db" 2>/dev/null
    fi
}

# --- Test Framework ---
PASS=0
FAIL=0
TOTAL=0

assert_ok() {
    local rc=$1; shift
    TOTAL=$((TOTAL + 1))
    if [ "$rc" -eq 0 ]; then
        PASS=$((PASS + 1))
        echo "PASS: $*"
    else
        FAIL=$((FAIL + 1))
        echo "FAIL: $*"
    fi
}

assert_fail() {
    local rc=$1; shift
    TOTAL=$((TOTAL + 1))
    if [ "$rc" -ne 0 ]; then
        PASS=$((PASS + 1))
        echo "PASS (expected failure): $*"
    else
        FAIL=$((FAIL + 1))
        echo "FAIL (expected failure but succeeded): $*"
    fi
}

assert_contains() {
    local haystack="$1"; local needle="$2"; shift 2
    TOTAL=$((TOTAL + 1))
    if echo "$haystack" | grep -q "$needle"; then
        PASS=$((PASS + 1))
        echo "PASS: $*"
    else
        FAIL=$((FAIL + 1))
        echo "FAIL: expected '$needle' in output — $*"
    fi
}

assert_not_contains() {
    local haystack="$1"; local needle="$2"; shift 2
    TOTAL=$((TOTAL + 1))
    if echo "$haystack" | grep -q "$needle"; then
        FAIL=$((FAIL + 1))
        echo "FAIL: did not expect '$needle' in output — $*"
    else
        PASS=$((PASS + 1))
        echo "PASS: $*"
    fi
}

# --- Helpers ---
full_cleanup() {
    $EXTENDDB stop --config "$CONFIG" 2>/dev/null || true
    sleep 1
    $EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null || true
    # Best-effort cleanup of stress test databases.
    for i in $(seq 1 25); do
        $EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null || true
    done
    rm -f "$CONFIG"
    unset EXTENDDB_ADMIN_USER 2>/dev/null || true
    unset EXTENDDB_ADMIN_PASSWORD 2>/dev/null || true
    unset EXTENDDB_PASSWORD 2>/dev/null || true
    unset AWS_ACCESS_KEY_ID 2>/dev/null || true
    unset AWS_SECRET_ACCESS_KEY 2>/dev/null || true
    unset AWS_DEFAULT_REGION 2>/dev/null || true
}

aws_ddb() {
    aws dynamodb "$@" \
        --endpoint-url "https://localhost:$TEST_PORT" \
        --no-verify-ssl \
        --region us-east-1 2>/dev/null
}

manage() {
    $EXTENDDB manage --config "$CONFIG" --user "$ADMIN_USER" --password "$ADMIN_PASS" "$@"
}

# Init with standard test database, start server, reduce control_plane_delay
standard_init_and_serve() {
    export EXTENDDB_ADMIN_USER="$ADMIN_USER"
    export EXTENDDB_ADMIN_PASSWORD="$ADMIN_PASS"
    $EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test >/dev/null 2>&1
    $EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
    sleep 3
    $EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" 2>/dev/null || true
}

# Cleanup trap
trap full_cleanup EXIT

echo "========================================"
echo "extenddb Comprehensive CLI Test Suite"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================"
echo ""

# --- Prerequisite checks ---
test -x "$EXTENDDB"; RC=$?
assert_ok $RC "extenddb binary exists and is executable"
if [ $RC -ne 0 ]; then echo "FATAL: extenddb binary not found at $EXTENDDB"; exit 1; fi

which aws >/dev/null 2>&1; RC=$?
assert_ok $RC "aws CLI available"

which jq >/dev/null 2>&1; RC=$?
assert_ok $RC "jq available"

which $PSQL >/dev/null 2>&1; RC=$?
assert_ok $RC "psql available"

echo ""
echo "========================================"
echo "Section 1: Init Credential Combinations"
echo "========================================"
echo ""

# --- Combination 1: All defaults (admin name=admin, password=auto-generated) ---
full_cleanup
unset EXTENDDB_ADMIN_USER 2>/dev/null || true
unset EXTENDDB_ADMIN_PASSWORD 2>/dev/null || true
OUTPUT=$($EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test 2>&1); RC=$?
assert_ok $RC "init combo 1: all defaults"
test -f "$CONFIG"; RC=$?
assert_ok $RC "init combo 1: config file exists"
grep -q "connection_string" "$CONFIG" 2>/dev/null; RC=$?
assert_ok $RC "init combo 1: config has connection_string"
# Auto-generated password should be printed
echo "$OUTPUT" | grep -qi "password"; RC=$?
assert_ok $RC "init combo 1: password printed in output"
# Verify server can start and admin exists
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
# Extract the generated password from output
GEN_PASS=$(echo "$OUTPUT" | grep -i "password" | grep -oP ':\s*\K\S+' | tail -1)
ADMINS=$($EXTENDDB manage --config "$CONFIG" --user admin --password "$GEN_PASS" list-admins 2>&1); RC=$?
assert_ok $RC "init combo 1: list-admins works with generated password"
assert_contains "$ADMINS" "admin" "init combo 1: default admin name is 'admin'"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"

# --- Combination 2: Password from env, default name ---
unset EXTENDDB_ADMIN_USER 2>/dev/null || true
export EXTENDDB_ADMIN_PASSWORD='EnvPass1!'
OUTPUT=$($EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test 2>&1); RC=$?
assert_ok $RC "init combo 2: password from env"
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
ADMINS=$($EXTENDDB manage --config "$CONFIG" --user admin --password 'EnvPass1!' list-admins 2>&1); RC=$?
assert_ok $RC "init combo 2: env password works"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"
unset EXTENDDB_ADMIN_PASSWORD

# --- Combination 3: User from env, password auto-generated ---
export EXTENDDB_ADMIN_USER=envadmin
unset EXTENDDB_ADMIN_PASSWORD 2>/dev/null || true
OUTPUT=$($EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test 2>&1); RC=$?
assert_ok $RC "init combo 3: user from env, auto password"
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
GEN_PASS=$(echo "$OUTPUT" | grep -i "password" | grep -oP ':\s*\K\S+' | tail -1)
ADMINS=$($EXTENDDB manage --config "$CONFIG" --user envadmin --password "$GEN_PASS" list-admins 2>&1); RC=$?
assert_ok $RC "init combo 3: envadmin works"
assert_contains "$ADMINS" "envadmin" "init combo 3: admin name is envadmin"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"
unset EXTENDDB_ADMIN_USER

# --- Combination 4: Both user and password from env ---
export EXTENDDB_ADMIN_USER=envadmin
export EXTENDDB_ADMIN_PASSWORD='EnvPass2!'
OUTPUT=$($EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test 2>&1); RC=$?
assert_ok $RC "init combo 4: both from env"
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
ADMINS=$($EXTENDDB manage --config "$CONFIG" --user envadmin --password 'EnvPass2!' list-admins 2>&1); RC=$?
assert_ok $RC "init combo 4: env user+pass works"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"
unset EXTENDDB_ADMIN_USER EXTENDDB_ADMIN_PASSWORD

# --- Re-init idempotency test ---
export EXTENDDB_ADMIN_USER="$ADMIN_USER"
export EXTENDDB_ADMIN_PASSWORD="$ADMIN_PASS"
$EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test >/dev/null 2>&1
OUTPUT=$($EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_test 2>&1); RC=$?
assert_ok $RC "re-init idempotency: second init succeeds"
echo "$OUTPUT" | grep -qi "already exists"; RC=$?
assert_ok $RC "re-init idempotency: reports already exists"
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"

# --- Custom database names ---
$EXTENDDB init --config "$CONFIG" --overwrite --data-db mydata 2>/dev/null; RC=$?
assert_ok $RC "init custom data-db name"
grep -q "mydata" "$CONFIG" 2>/dev/null; RC=$?
assert_ok $RC "config references custom data-db"
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"

$EXTENDDB init --config "$CONFIG" --overwrite --data-db mydata --catalog-db mycat 2>/dev/null; RC=$?
assert_ok $RC "init custom data-db and catalog-db"
grep -q "mycat" "$CONFIG" 2>/dev/null; RC=$?
assert_ok $RC "config references custom catalog-db"
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"
unset EXTENDDB_ADMIN_USER EXTENDDB_ADMIN_PASSWORD

echo ""
echo "========================================"
echo "Section 2: Server Lifecycle"
echo "========================================"
echo ""

full_cleanup
standard_init_and_serve

# version
OUTPUT=$($EXTENDDB version 2>&1); RC=$?
assert_ok $RC "version command"
assert_contains "$OUTPUT" "extenddb" "version output contains extenddb"
EXPECTED_VERSION=$(grep -A2 '\[workspace.package\]' Cargo.toml | grep 'version' | grep -oP '\d+\.\d+\.\d+' | head -1)
assert_contains "$OUTPUT" "$EXPECTED_VERSION" "version output contains $EXPECTED_VERSION"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

# verify (before serve)
$EXTENDDB verify --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "verify succeeds after init"

# migrate (no-op when current)
$EXTENDDB migrate --config "$CONFIG" --yes >/dev/null 2>&1; RC=$?
assert_ok $RC "migrate succeeds when already current"

# serve + status + stop
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB status --config "$CONFIG" --port "$TEST_PORT" >/dev/null 2>&1; RC=$?
assert_ok $RC "status reports running"

# Status with wrong port should fail
$EXTENDDB status --config "$CONFIG" --port 19999 >/dev/null 2>&1; RC=$?
assert_fail $RC "status on wrong port fails"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 2
$EXTENDDB status --config "$CONFIG" --port "$TEST_PORT" >/dev/null 2>&1; RC=$?
assert_fail $RC "status reports not running after stop"

# Double stop (idempotent)
$EXTENDDB stop --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "stop when already stopped is not an error"

# Serve with port override
$EXTENDDB serve --config "$CONFIG" --port $((TEST_PORT + 1)) 2>/dev/null
sleep 3
$EXTENDDB status --config "$CONFIG" --port $((TEST_PORT + 1)) >/dev/null 2>&1; RC=$?
assert_ok $RC "status on overridden port"
$EXTENDDB stop --config "$CONFIG" --port $((TEST_PORT + 1)) 2>/dev/null
sleep 1

# catalog-check
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB catalog-check --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "catalog-check clean"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 3: Settings Commands"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3

# List all settings
OUTPUT=$($EXTENDDB settings list --config "$CONFIG" 2>&1); RC=$?
assert_ok $RC "settings list"
assert_contains "$OUTPUT" "control_plane_delay_seconds" "settings list shows control_plane_delay"

# Get a specific setting
OUTPUT=$($EXTENDDB settings get control_plane_delay_seconds --config "$CONFIG" 2>&1); RC=$?
assert_ok $RC "settings get"

# Set a setting
$EXTENDDB settings set control_plane_delay_seconds 2 --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "settings set"

# Verify the set took effect
OUTPUT=$($EXTENDDB settings get control_plane_delay_seconds --config "$CONFIG" 2>&1); RC=$?
assert_contains "$OUTPUT" "2" "settings get reflects new value"

# Set it back
$EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "settings set back to 0.05"

# Set log_level
$EXTENDDB settings set log_level debug --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "settings set log_level"

# Set sqlx_log_level
$EXTENDDB settings set sqlx_log_level info --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_ok $RC "settings set sqlx_log_level"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 4: Admin User Management"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3

# list-admins
ADMINS=$(manage list-admins 2>&1); RC=$?
assert_ok $RC "list-admins"
assert_contains "$ADMINS" "$ADMIN_USER" "list-admins shows initial admin"

# create-admin
manage create-admin --admin-name admin2 --admin-password 'Admin2Pass!' >/dev/null 2>&1; RC=$?
assert_ok $RC "create-admin"

# list-admins shows both
ADMINS=$(manage list-admins 2>&1)
assert_contains "$ADMINS" "admin2" "list-admins shows admin2"

# Duplicate create-admin
manage create-admin --admin-name admin2 --admin-password 'Admin2Pass!' >/dev/null 2>&1; RC=$?
assert_fail $RC "create-admin duplicate fails"

# change-admin-password
manage change-admin-password --admin-name admin2 --new-password 'NewAdmin2Pass!' >/dev/null 2>&1; RC=$?
assert_ok $RC "change-admin-password"

# Verify new password works
$EXTENDDB manage --config "$CONFIG" --user admin2 --password 'NewAdmin2Pass!' list-admins >/dev/null 2>&1; RC=$?
assert_ok $RC "admin2 authenticates with new password"

# Old password should fail
$EXTENDDB manage --config "$CONFIG" --user admin2 --password 'Admin2Pass!' list-admins >/dev/null 2>&1; RC=$?
assert_fail $RC "admin2 old password rejected"

# delete-admin
manage delete-admin --admin-name admin2 >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-admin"

# Verify deleted
ADMINS=$(manage list-admins 2>&1)
assert_not_contains "$ADMINS" "admin2" "admin2 no longer listed"

# delete-admin nonexistent
manage delete-admin --admin-name nonexistent >/dev/null 2>&1; RC=$?
assert_fail $RC "delete-admin nonexistent fails"

# Password via env var
export EXTENDDB_PASSWORD="$ADMIN_PASS"
$EXTENDDB manage --config "$CONFIG" --user "$ADMIN_USER" list-admins >/dev/null 2>&1; RC=$?
assert_ok $RC "manage with EXTENDDB_PASSWORD env var"
unset EXTENDDB_PASSWORD

# Password via stdin
echo "$ADMIN_PASS" | $EXTENDDB manage --config "$CONFIG" --user "$ADMIN_USER" list-admins >/dev/null 2>&1; RC=$?
assert_ok $RC "manage with password from stdin"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 5: Account and IAM User Management"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3

# create-account with auto-generated ID
OUTPUT=$(manage create-account --account-name testaccount1 2>&1); RC=$?
assert_ok $RC "create-account auto-id"
ACCOUNT_ID1=$(echo "$OUTPUT" | grep -oP '\d{12}' | head -1)
test "${#ACCOUNT_ID1}" -eq 12 2>/dev/null; RC=$?
assert_ok $RC "account ID is 12 digits ($ACCOUNT_ID1)"

# create-account with explicit ID
manage create-account --account-id 123456789012 --account-name testaccount2 >/dev/null 2>&1; RC=$?
assert_ok $RC "create-account explicit-id"

# list-accounts
ACCOUNTS=$(manage list-accounts 2>&1)
assert_contains "$ACCOUNTS" "testaccount1" "list-accounts shows testaccount1"
assert_contains "$ACCOUNTS" "testaccount2" "list-accounts shows testaccount2"

# Duplicate account name
manage create-account --account-name testaccount1 >/dev/null 2>&1; RC=$?
assert_fail $RC "create-account duplicate name fails"

# Duplicate account ID
manage create-account --account-id 123456789012 --account-name testaccount3 >/dev/null 2>&1; RC=$?
assert_fail $RC "create-account duplicate id fails"

# --- IAM Users ---
manage create-user --account-id "$ACCOUNT_ID1" --user-name user1 >/dev/null 2>&1; RC=$?
assert_ok $RC "create-user no password"

manage create-user --account-id "$ACCOUNT_ID1" --user-name user2 --user-password 'UserPass123!' >/dev/null 2>&1; RC=$?
assert_ok $RC "create-user with password"

manage create-user --account-id 123456789012 --user-name user1 >/dev/null 2>&1; RC=$?
assert_ok $RC "create-user in different account (same name ok)"

# list-users
USERS=$(manage list-users --account-id "$ACCOUNT_ID1" 2>&1)
assert_contains "$USERS" "user1" "list-users shows user1"
assert_contains "$USERS" "user2" "list-users shows user2"

# Duplicate user in same account
manage create-user --account-id "$ACCOUNT_ID1" --user-name user1 >/dev/null 2>&1; RC=$?
assert_fail $RC "create-user duplicate fails"

# change-user-password
manage change-user-password --account-id "$ACCOUNT_ID1" --user-name user2 --new-password 'NewUserPass!' >/dev/null 2>&1; RC=$?
assert_ok $RC "change-user-password"

# delete-user
manage create-user --account-id "$ACCOUNT_ID1" --user-name tempuser >/dev/null 2>&1
manage delete-user --account-id "$ACCOUNT_ID1" --user-name tempuser >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-user"

# delete-user nonexistent
manage delete-user --account-id "$ACCOUNT_ID1" --user-name nonexistent >/dev/null 2>&1; RC=$?
assert_fail $RC "delete-user nonexistent fails"

# delete-account cascades
manage create-account --account-id 999999999999 --account-name cascade-test >/dev/null 2>&1
manage create-user --account-id 999999999999 --user-name cascadeuser >/dev/null 2>&1
manage delete-account --account-id 999999999999 >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-account cascades"

echo ""
echo "========================================"
echo "Section 6: Access Keys and AWS CLI Setup"
echo "========================================"
echo ""

# create-access-key
AK_OUTPUT=$(manage create-access-key --account-id "$ACCOUNT_ID1" --user-name user1 2>&1); RC=$?
assert_ok $RC "create-access-key"
ACCESS_KEY=$(echo "$AK_OUTPUT" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
SECRET_KEY=$(echo "$AK_OUTPUT" | grep -i secret | grep -oP '[A-Za-z0-9/+=]{40}' | head -1)
test -n "$ACCESS_KEY"; RC=$?
assert_ok $RC "access key ID extracted ($ACCESS_KEY)"
test -n "$SECRET_KEY"; RC=$?
assert_ok $RC "secret key extracted"

# list-access-keys
AK_LIST=$(manage list-access-keys --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_contains "$AK_LIST" "$ACCESS_KEY" "list-access-keys shows key"

# Create a second access key
AK_OUTPUT2=$(manage create-access-key --account-id "$ACCOUNT_ID1" --user-name user1 2>&1); RC=$?
assert_ok $RC "create second access-key"
ACCESS_KEY2=$(echo "$AK_OUTPUT2" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)

# list should show both
AK_LIST=$(manage list-access-keys --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_contains "$AK_LIST" "$ACCESS_KEY" "list shows first key"
assert_contains "$AK_LIST" "$ACCESS_KEY2" "list shows second key"

# delete-access-key
manage delete-access-key --account-id "$ACCOUNT_ID1" --user-name user1 --access-key-id "$ACCESS_KEY2" >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-access-key"

# Verify deleted
AK_LIST=$(manage list-access-keys --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_not_contains "$AK_LIST" "$ACCESS_KEY2" "deleted key no longer listed"

# Self-service access key creation
manage change-user-password --account-id "$ACCOUNT_ID1" --user-name user1 --new-password 'User1Pass!' >/dev/null 2>&1

SELF_AK=$($EXTENDDB manage --config "$CONFIG" --user "${ACCOUNT_ID1}/user1" --password 'User1Pass!' \
    create-access-key 2>&1); RC=$?
assert_ok $RC "self-service create-access-key"

# Self-service list
$EXTENDDB manage --config "$CONFIG" --user "${ACCOUNT_ID1}/user1" --password 'User1Pass!' \
    list-access-keys --account-id "$ACCOUNT_ID1" --user-name user1 >/dev/null 2>&1; RC=$?
assert_ok $RC "self-service list-access-keys"

# import-access-key
manage import-access-key --account-id "$ACCOUNT_ID1" --user-name user1 \
    --access-key-id AKIAIOSFODNN7EXAMPLE --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --yes >/dev/null 2>&1; RC=$?
assert_ok $RC "import-access-key"

# Give user1 full DynamoDB access for later sections
ALLOW_ALL_DDB='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'
manage put-user-policy --account-id "$ACCOUNT_ID1" --user-name user1 \
    --policy-name ddb-full --policy-document "$ALLOW_ALL_DDB" >/dev/null 2>&1; RC=$?
assert_ok $RC "put-user-policy ddb-full for user1"

# AWS CLI without policy should fail (test with user2 who has no policy)
AK_U2=$(manage create-access-key --account-id "$ACCOUNT_ID1" --user-name user2 2>&1)
KEY_U2=$(echo "$AK_U2" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
SEC_U2=$(echo "$AK_U2" | grep -i secret | grep -oP '[A-Za-z0-9/+=]{40}' | head -1)
export AWS_ACCESS_KEY_ID="$KEY_U2"
export AWS_SECRET_ACCESS_KEY="$SEC_U2"
export AWS_DEFAULT_REGION=us-east-1
aws_ddb list-tables >/dev/null 2>&1; RC=$?
assert_fail $RC "AWS CLI without policy gets 403"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 7: Groups, Policies, Roles, Tags"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3

# --- Groups ---
manage create-group --account-id "$ACCOUNT_ID1" --group-name developers >/dev/null 2>&1; RC=$?
assert_ok $RC "create-group developers"

manage create-group --account-id "$ACCOUNT_ID1" --group-name readonly >/dev/null 2>&1; RC=$?
assert_ok $RC "create-group readonly"

manage create-group --account-id "$ACCOUNT_ID1" --group-name developers >/dev/null 2>&1; RC=$?
assert_fail $RC "create-group duplicate fails"

GROUPS=$(manage list-groups --account-id "$ACCOUNT_ID1" 2>&1)
assert_contains "$GROUPS" "developers" "list-groups shows developers"
assert_contains "$GROUPS" "readonly" "list-groups shows readonly"

# add-group-member
manage add-group-member --account-id "$ACCOUNT_ID1" --group-name developers --user-name user1 >/dev/null 2>&1; RC=$?
assert_ok $RC "add-group-member user1"

manage add-group-member --account-id "$ACCOUNT_ID1" --group-name developers --user-name user2 >/dev/null 2>&1; RC=$?
assert_ok $RC "add-group-member user2"

# Duplicate membership
manage add-group-member --account-id "$ACCOUNT_ID1" --group-name developers --user-name user1 >/dev/null 2>&1; RC=$?
assert_fail $RC "add-group-member duplicate fails"

# remove-group-member
manage remove-group-member --account-id "$ACCOUNT_ID1" --group-name developers --user-name user2 >/dev/null 2>&1; RC=$?
assert_ok $RC "remove-group-member"

# --- User Policies ---
POLICIES=$(manage list-user-policies --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_contains "$POLICIES" "ddb-full" "list-user-policies shows ddb-full"

READONLY_DDB='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:Query","dynamodb:Scan","dynamodb:DescribeTable","dynamodb:ListTables"],"Resource":"*"}]}'
manage put-user-policy --account-id "$ACCOUNT_ID1" --user-name user1 \
    --policy-name ddb-readonly --policy-document "$READONLY_DDB" >/dev/null 2>&1; RC=$?
assert_ok $RC "put-user-policy readonly"

manage delete-user-policy --account-id "$ACCOUNT_ID1" --user-name user1 --policy-name ddb-readonly >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-user-policy"

# --- Group Policies ---
manage put-group-policy --account-id "$ACCOUNT_ID1" --group-name developers \
    --policy-name group-ddb --policy-document "$ALLOW_ALL_DDB" >/dev/null 2>&1; RC=$?
assert_ok $RC "put-group-policy"

GPOLICIES=$(manage list-group-policies --account-id "$ACCOUNT_ID1" --group-name developers 2>&1)
assert_contains "$GPOLICIES" "group-ddb" "list-group-policies shows group-ddb"

manage delete-group-policy --account-id "$ACCOUNT_ID1" --group-name developers --policy-name group-ddb >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-group-policy"

# --- Roles ---
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::'"$ACCOUNT_ID1"':user/user1"},"Action":"sts:AssumeRole"}]}'

manage create-role --account-id "$ACCOUNT_ID1" --role-name testrole --trust-policy "$TRUST_POLICY" >/dev/null 2>&1; RC=$?
assert_ok $RC "create-role"

ROLES=$(manage list-roles --account-id "$ACCOUNT_ID1" 2>&1)
assert_contains "$ROLES" "testrole" "list-roles shows testrole"

manage create-role --account-id "$ACCOUNT_ID1" --role-name testrole --trust-policy "$TRUST_POLICY" >/dev/null 2>&1; RC=$?
assert_fail $RC "create-role duplicate fails"

# put-role-policy
manage put-role-policy --account-id "$ACCOUNT_ID1" --role-name testrole \
    --policy-name role-ddb --policy-document "$ALLOW_ALL_DDB" >/dev/null 2>&1; RC=$?
assert_ok $RC "put-role-policy"

RPOLICIES=$(manage list-role-policies --account-id "$ACCOUNT_ID1" --role-name testrole 2>&1)
assert_contains "$RPOLICIES" "role-ddb" "list-role-policies shows role-ddb"

manage delete-role-policy --account-id "$ACCOUNT_ID1" --role-name testrole --policy-name role-ddb >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-role-policy"

# assume-role
ASSUME_OUTPUT=$(manage assume-role --account-id "$ACCOUNT_ID1" --role-name testrole \
    --caller-arn "arn:aws:iam::${ACCOUNT_ID1}:user/user1" \
    --session-name testsession --duration-seconds 900 2>&1); RC=$?
assert_ok $RC "assume-role"
assert_contains "$ASSUME_OUTPUT" "AccessKeyId" "assume-role returns AccessKeyId"
assert_contains "$ASSUME_OUTPUT" "SecretAccessKey" "assume-role returns SecretAccessKey"
assert_contains "$ASSUME_OUTPUT" "SessionToken" "assume-role returns SessionToken"

# delete-role
manage delete-role --account-id "$ACCOUNT_ID1" --role-name testrole >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-role"

manage delete-role --account-id "$ACCOUNT_ID1" --role-name nonexistent >/dev/null 2>&1; RC=$?
assert_fail $RC "delete-role nonexistent fails"

# --- Tags (user) ---
manage tag-user --account-id "$ACCOUNT_ID1" --user-name user1 \
    --tags '[{"key":"env","value":"test"},{"key":"team","value":"backend"}]' >/dev/null 2>&1; RC=$?
assert_ok $RC "tag-user"

TAGS=$(manage list-user-tags --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_contains "$TAGS" "env" "list-user-tags shows env"
assert_contains "$TAGS" "backend" "list-user-tags shows backend"

manage untag-user --account-id "$ACCOUNT_ID1" --user-name user1 --tag-keys "team" >/dev/null 2>&1; RC=$?
assert_ok $RC "untag-user"

TAGS=$(manage list-user-tags --account-id "$ACCOUNT_ID1" --user-name user1 2>&1)
assert_not_contains "$TAGS" "team" "untagged key removed"

# --- Tags (role) ---
manage create-role --account-id "$ACCOUNT_ID1" --role-name tagrole --trust-policy "$TRUST_POLICY" >/dev/null 2>&1
manage tag-role --account-id "$ACCOUNT_ID1" --role-name tagrole \
    --tags '[{"key":"purpose","value":"testing"}]' >/dev/null 2>&1; RC=$?
assert_ok $RC "tag-role"

RTAGS=$(manage list-role-tags --account-id "$ACCOUNT_ID1" --role-name tagrole 2>&1)
assert_contains "$RTAGS" "purpose" "list-role-tags shows purpose"

manage untag-role --account-id "$ACCOUNT_ID1" --role-name tagrole --tag-keys "purpose" >/dev/null 2>&1; RC=$?
assert_ok $RC "untag-role"

manage delete-role --account-id "$ACCOUNT_ID1" --role-name tagrole >/dev/null 2>&1

# --- Permissions Boundaries (user) ---
BOUNDARY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"}]}'

manage set-user-boundary --account-id "$ACCOUNT_ID1" --user-name user1 --policy-document "$BOUNDARY" >/dev/null 2>&1; RC=$?
assert_ok $RC "set-user-boundary"

UBOUNDARY=$(manage get-user-boundary --account-id "$ACCOUNT_ID1" --user-name user1 2>&1); RC=$?
assert_ok $RC "get-user-boundary"
assert_contains "$UBOUNDARY" "GetItem" "user boundary contains GetItem"

manage delete-user-boundary --account-id "$ACCOUNT_ID1" --user-name user1 >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-user-boundary"

# --- Permissions Boundaries (role) ---
manage create-role --account-id "$ACCOUNT_ID1" --role-name boundrole --trust-policy "$TRUST_POLICY" >/dev/null 2>&1

manage set-role-boundary --account-id "$ACCOUNT_ID1" --role-name boundrole --policy-document "$BOUNDARY" >/dev/null 2>&1; RC=$?
assert_ok $RC "set-role-boundary"

RBOUNDARY=$(manage get-role-boundary --account-id "$ACCOUNT_ID1" --role-name boundrole 2>&1); RC=$?
assert_ok $RC "get-role-boundary"

manage delete-role-boundary --account-id "$ACCOUNT_ID1" --role-name boundrole >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-role-boundary"

manage delete-role --account-id "$ACCOUNT_ID1" --role-name boundrole >/dev/null 2>&1

# --- Cleanup groups ---
manage remove-group-member --account-id "$ACCOUNT_ID1" --group-name developers --user-name user1 >/dev/null 2>&1
manage delete-group --account-id "$ACCOUNT_ID1" --group-name developers >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-group developers"
manage delete-group --account-id "$ACCOUNT_ID1" --group-name readonly >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-group readonly"

manage delete-group --account-id "$ACCOUNT_ID1" --group-name nonexistent >/dev/null 2>&1; RC=$?
assert_fail $RC "delete-group nonexistent fails"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 8: AWS CLI DynamoDB Operations"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" 2>/dev/null || true

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1

# Create table (hash-only)
aws_ddb create-table \
    --table-name cli-test-hash \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1; RC=$?
assert_ok $RC "create-table hash-only"
sleep 2

# Describe table
OUTPUT=$(aws_ddb describe-table --table-name cli-test-hash 2>&1); RC=$?
assert_ok $RC "describe-table"
assert_contains "$OUTPUT" "ACTIVE" "table is ACTIVE"
assert_contains "$OUTPUT" "cli-test-hash" "describe shows table name"

# Create table (hash+range)
aws_ddb create-table \
    --table-name cli-test-range \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=N \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1; RC=$?
assert_ok $RC "create-table hash+range"
sleep 2

# Create table with GSI
aws_ddb create-table \
    --table-name cli-test-gsi \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
        AttributeName=gsi_pk,AttributeType=S \
        AttributeName=gsi_sk,AttributeType=N \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --global-secondary-indexes \
        'IndexName=gsi1,KeySchema=[{AttributeName=gsi_pk,KeyType=HASH},{AttributeName=gsi_sk,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1; RC=$?
assert_ok $RC "create-table with GSI"
sleep 2

OUTPUT=$(aws_ddb describe-table --table-name cli-test-gsi 2>&1)
assert_contains "$OUTPUT" "gsi1" "describe shows GSI"

# List tables
OUTPUT=$(aws_ddb list-tables 2>&1); RC=$?
assert_ok $RC "list-tables"
assert_contains "$OUTPUT" "cli-test-hash" "list-tables shows hash table"
assert_contains "$OUTPUT" "cli-test-range" "list-tables shows range table"
assert_contains "$OUTPUT" "cli-test-gsi" "list-tables shows gsi table"

# PutItem
aws_ddb put-item --table-name cli-test-hash \
    --item '{"pk":{"S":"key1"},"data":{"S":"hello"},"num":{"N":"42"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "put-item key1"

aws_ddb put-item --table-name cli-test-hash \
    --item '{"pk":{"S":"key2"},"data":{"S":"world"},"num":{"N":"99"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "put-item key2"

# GetItem
OUTPUT=$(aws_ddb get-item --table-name cli-test-hash \
    --key '{"pk":{"S":"key1"}}' 2>&1); RC=$?
assert_ok $RC "get-item key1"
assert_contains "$OUTPUT" "hello" "get-item returns data"
assert_contains "$OUTPUT" "42" "get-item returns num"

# GetItem nonexistent
OUTPUT=$(aws_ddb get-item --table-name cli-test-hash \
    --key '{"pk":{"S":"nonexistent"}}' 2>&1); RC=$?
assert_ok $RC "get-item nonexistent returns empty (not error)"

# UpdateItem
aws_ddb update-item --table-name cli-test-hash \
    --key '{"pk":{"S":"key1"}}' \
    --update-expression "SET #d = :v" \
    --expression-attribute-names '{"#d":"data"}' \
    --expression-attribute-values '{":v":{"S":"updated"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "update-item"

OUTPUT=$(aws_ddb get-item --table-name cli-test-hash --key '{"pk":{"S":"key1"}}' 2>&1)
assert_contains "$OUTPUT" "updated" "update-item took effect"

# DeleteItem
aws_ddb delete-item --table-name cli-test-hash --key '{"pk":{"S":"key2"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-item"

OUTPUT=$(aws_ddb get-item --table-name cli-test-hash --key '{"pk":{"S":"key2"}}' 2>&1)
assert_not_contains "$OUTPUT" "world" "deleted item not returned"

# PutItem + Query on hash+range table
LOOP_RC=0
for i in 1 2 3 4 5; do
    aws_ddb put-item --table-name cli-test-range \
        --item "{\"pk\":{\"S\":\"partition1\"},\"sk\":{\"N\":\"$i\"},\"val\":{\"S\":\"item$i\"}}" >/dev/null 2>&1 || LOOP_RC=1
done
RC=$LOOP_RC
assert_ok $RC "put 5 items in range table"

# Query forward with range
OUTPUT=$(aws_ddb query --table-name cli-test-range \
    --key-condition-expression "pk = :pk AND sk BETWEEN :lo AND :hi" \
    --expression-attribute-values '{":pk":{"S":"partition1"},":lo":{"N":"2"},":hi":{"N":"4"}}' 2>&1); RC=$?
assert_ok $RC "query range"
assert_contains "$OUTPUT" "item2" "query returns item2"
assert_contains "$OUTPUT" "item4" "query returns item4"

# Query reverse
OUTPUT=$(aws_ddb query --table-name cli-test-range \
    --key-condition-expression "pk = :pk" \
    --expression-attribute-values '{":pk":{"S":"partition1"}}' \
    --scan-index-forward false --max-items 2 2>&1); RC=$?
assert_ok $RC "query reverse"
assert_contains "$OUTPUT" "item5" "query reverse returns item5"

# Scan
OUTPUT=$(aws_ddb scan --table-name cli-test-range 2>&1); RC=$?
assert_ok $RC "scan"
assert_contains "$OUTPUT" "Count" "scan returns Count"

# GSI operations
aws_ddb put-item --table-name cli-test-gsi \
    --item '{"pk":{"S":"a"},"sk":{"S":"1"},"gsi_pk":{"S":"gp1"},"gsi_sk":{"N":"100"},"info":{"S":"first"}}' >/dev/null 2>&1
aws_ddb put-item --table-name cli-test-gsi \
    --item '{"pk":{"S":"b"},"sk":{"S":"2"},"gsi_pk":{"S":"gp1"},"gsi_sk":{"N":"200"},"info":{"S":"second"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "put items with GSI attrs"

sleep 2  # GSI propagation

OUTPUT=$(aws_ddb query --table-name cli-test-gsi \
    --index-name gsi1 \
    --key-condition-expression "gsi_pk = :gpk" \
    --expression-attribute-values '{":gpk":{"S":"gp1"}}' 2>&1); RC=$?
assert_ok $RC "query GSI"
assert_contains "$OUTPUT" "first" "GSI query returns first"
assert_contains "$OUTPUT" "second" "GSI query returns second"

# BatchWriteItem
aws_ddb batch-write-item --request-items '{
    "cli-test-hash": [
        {"PutRequest":{"Item":{"pk":{"S":"batch1"},"data":{"S":"b1"}}}},
        {"PutRequest":{"Item":{"pk":{"S":"batch2"},"data":{"S":"b2"}}}},
        {"PutRequest":{"Item":{"pk":{"S":"batch3"},"data":{"S":"b3"}}}}
    ]
}' >/dev/null 2>&1; RC=$?
assert_ok $RC "batch-write-item"

# BatchGetItem
OUTPUT=$(aws_ddb batch-get-item --request-items '{
    "cli-test-hash": {
        "Keys": [{"pk":{"S":"batch1"}},{"pk":{"S":"batch2"}}]
    }
}' 2>&1); RC=$?
assert_ok $RC "batch-get-item"
assert_contains "$OUTPUT" "b1" "batch-get returns b1"
assert_contains "$OUTPUT" "b2" "batch-get returns b2"

# TransactWriteItems
aws_ddb transact-write-items --transact-items '[
    {"Put":{"TableName":"cli-test-hash","Item":{"pk":{"S":"tx1"},"data":{"S":"transacted"}}}},
    {"Put":{"TableName":"cli-test-hash","Item":{"pk":{"S":"tx2"},"data":{"S":"also-transacted"}}}}
]' >/dev/null 2>&1; RC=$?
assert_ok $RC "transact-write-items"

# TransactGetItems
OUTPUT=$(aws_ddb transact-get-items --transact-items '[
    {"Get":{"TableName":"cli-test-hash","Key":{"pk":{"S":"tx1"}}}},
    {"Get":{"TableName":"cli-test-hash","Key":{"pk":{"S":"tx2"}}}}
]' 2>&1); RC=$?
assert_ok $RC "transact-get-items"
assert_contains "$OUTPUT" "transacted" "transact-get returns transacted"

# Tag table
TABLE_ARN=$(aws_ddb describe-table --table-name cli-test-hash --query 'Table.TableArn' --output text 2>/dev/null)
aws_ddb tag-resource --resource-arn "$TABLE_ARN" --tags Key=env,Value=test Key=project,Value=cli-suite >/dev/null 2>&1; RC=$?
assert_ok $RC "tag-resource"

OUTPUT=$(aws_ddb list-tags-of-resource --resource-arn "$TABLE_ARN" 2>&1)
assert_contains "$OUTPUT" "env" "list-tags shows env"
assert_contains "$OUTPUT" "cli-suite" "list-tags shows cli-suite"

aws_ddb untag-resource --resource-arn "$TABLE_ARN" --tag-keys env >/dev/null 2>&1; RC=$?
assert_ok $RC "untag-resource"

# TTL
aws_ddb update-time-to-live --table-name cli-test-hash \
    --time-to-live-specification Enabled=true,AttributeName=ttl >/dev/null 2>&1; RC=$?
assert_ok $RC "update-time-to-live enable"

OUTPUT=$(aws_ddb describe-time-to-live --table-name cli-test-hash 2>&1)
assert_contains "$OUTPUT" "ENABLED" "TTL is ENABLED"

aws_ddb update-time-to-live --table-name cli-test-hash \
    --time-to-live-specification Enabled=false,AttributeName=ttl >/dev/null 2>&1; RC=$?
assert_ok $RC "update-time-to-live disable"

# Delete tables
aws_ddb delete-table --table-name cli-test-hash >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-table hash"
aws_ddb delete-table --table-name cli-test-range >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-table range"
aws_ddb delete-table --table-name cli-test-gsi >/dev/null 2>&1; RC=$?
assert_ok $RC "delete-table gsi"
sleep 2

OUTPUT=$(aws_ddb list-tables 2>&1)
assert_not_contains "$OUTPUT" "cli-test-hash" "deleted table not listed"

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 9: Verify Data Goes to extenddb"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" 2>/dev/null || true

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1

UNIQUE_TABLE="cli-verify-local-$(date +%s)"
aws_ddb create-table \
    --table-name "$UNIQUE_TABLE" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1
sleep 2

aws_ddb put-item --table-name "$UNIQUE_TABLE" \
    --item '{"pk":{"S":"canary"},"proof":{"S":"this-is-local-extenddb"}}' >/dev/null 2>&1; RC=$?
assert_ok $RC "put canary item"

OUTPUT=$(aws_ddb get-item --table-name "$UNIQUE_TABLE" --key '{"pk":{"S":"canary"}}' 2>&1)
assert_contains "$OUTPUT" "this-is-local-extenddb" "canary item readable via extenddb"

# Verify via direct PostgreSQL query
PG_COUNT=$(pg_query extenddb_test \
    "SELECT count(*) FROM items WHERE table_id IN (SELECT id FROM tables WHERE table_name='$UNIQUE_TABLE')" \
    || echo "0")
test "$PG_COUNT" -ge 1 2>/dev/null; RC=$?
assert_ok $RC "canary item exists in local PostgreSQL (count=$PG_COUNT)"

# Try real AWS — should fail (these are extenddb-generated credentials)
REAL_AWS_OUTPUT=$(aws dynamodb describe-table --table-name "$UNIQUE_TABLE" \
    --region us-east-1 --no-cli-pager 2>&1 || true)
echo "$REAL_AWS_OUTPUT" | grep -qE "ResourceNotFoundException|InvalidSignature|UnrecognizedClient|could not be found|error"; RC=$?
assert_ok $RC "table does not exist on real AWS"

aws_ddb delete-table --table-name "$UNIQUE_TABLE" >/dev/null 2>&1

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 10: Policy Enforcement"
echo "========================================"
echo ""

$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" 2>/dev/null || true

# Create a restricted user with only GetItem+DescribeTable
manage create-user --account-id "$ACCOUNT_ID1" --user-name restricted-user >/dev/null 2>&1
RESTRICTED_AK=$(manage create-access-key --account-id "$ACCOUNT_ID1" --user-name restricted-user 2>&1)
R_ACCESS_KEY=$(echo "$RESTRICTED_AK" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
R_SECRET_KEY=$(echo "$RESTRICTED_AK" | grep -i secret | grep -oP '[A-Za-z0-9/+=]{40}' | head -1)

READONLY_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:DescribeTable"],"Resource":"*"}]}'
manage put-user-policy --account-id "$ACCOUNT_ID1" --user-name restricted-user \
    --policy-name readonly --policy-document "$READONLY_POLICY" >/dev/null 2>&1

# Create a table as full-access user
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
aws_ddb create-table \
    --table-name authz-test \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1
sleep 2
aws_ddb put-item --table-name authz-test --item '{"pk":{"S":"k1"},"val":{"S":"v1"}}' >/dev/null 2>&1

# Switch to restricted user
export AWS_ACCESS_KEY_ID="$R_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$R_SECRET_KEY"

# GetItem should succeed
OUTPUT=$(aws_ddb get-item --table-name authz-test --key '{"pk":{"S":"k1"}}' 2>&1); RC=$?
assert_ok $RC "restricted user can GetItem"
assert_contains "$OUTPUT" "v1" "restricted user sees data"

# PutItem should fail
aws_ddb put-item --table-name authz-test --item '{"pk":{"S":"k2"},"val":{"S":"v2"}}' >/dev/null 2>&1; RC=$?
assert_fail $RC "restricted user cannot PutItem"

# DeleteItem should fail
aws_ddb delete-item --table-name authz-test --key '{"pk":{"S":"k1"}}' >/dev/null 2>&1; RC=$?
assert_fail $RC "restricted user cannot DeleteItem"

# CreateTable should fail
aws_ddb create-table \
    --table-name authz-blocked \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1; RC=$?
assert_fail $RC "restricted user cannot CreateTable"

# ListTables should fail (not in policy)
aws_ddb list-tables >/dev/null 2>&1; RC=$?
assert_fail $RC "restricted user cannot ListTables"

# DescribeTable should succeed
aws_ddb describe-table --table-name authz-test >/dev/null 2>&1; RC=$?
assert_ok $RC "restricted user can DescribeTable"

# --- Explicit Deny overrides Allow ---
DENY_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*"}]}'
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
manage put-user-policy --account-id "$ACCOUNT_ID1" --user-name restricted-user \
    --policy-name deny-get --policy-document "$DENY_POLICY" >/dev/null 2>&1

export AWS_ACCESS_KEY_ID="$R_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$R_SECRET_KEY"
aws_ddb get-item --table-name authz-test --key '{"pk":{"S":"k1"}}' >/dev/null 2>&1; RC=$?
assert_fail $RC "explicit Deny overrides Allow"

# Cleanup
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
aws_ddb delete-table --table-name authz-test >/dev/null 2>&1
manage delete-user-policy --account-id "$ACCOUNT_ID1" --user-name restricted-user --policy-name readonly >/dev/null 2>&1
manage delete-user-policy --account-id "$ACCOUNT_ID1" --user-name restricted-user --policy-name deny-get >/dev/null 2>&1
manage delete-access-key --account-id "$ACCOUNT_ID1" --user-name restricted-user --access-key-id "$R_ACCESS_KEY" >/dev/null 2>&1
manage delete-user --account-id "$ACCOUNT_ID1" --user-name restricted-user >/dev/null 2>&1

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1

echo ""
echo "========================================"
echo "Section 11: Destroy and Error Cases"
echo "========================================"
echo ""

# Destroy with confirmation
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null; RC=$?
assert_ok $RC "destroy with confirmation"

# Verify databases are gone
pg_query postgres "SELECT 1 FROM pg_database WHERE datname='extenddb_test'" | grep -q 1; RC=$?
assert_fail $RC "data database dropped"

# Re-init for force test
export EXTENDDB_ADMIN_USER="$ADMIN_USER"
export EXTENDDB_ADMIN_PASSWORD="$ADMIN_PASS"
$EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_force_test >/dev/null 2>&1
$EXTENDDB destroy --config "$CONFIG" --yes >/dev/null 2>&1; RC=$?
assert_ok $RC "destroy --yes"
rm -f "$CONFIG"

# Commands against missing config
$EXTENDDB serve --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_fail $RC "serve with missing config fails"
$EXTENDDB verify --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_fail $RC "verify with missing config fails"
$EXTENDDB status --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_fail $RC "status with missing config fails"
$EXTENDDB settings list --config "$CONFIG" >/dev/null 2>&1; RC=$?
assert_fail $RC "settings with missing config fails"

# Init with invalid PG credentials
$EXTENDDB init --config "$CONFIG" --overwrite --storage-admin-user nonexistent_user --storage-host localhost >/dev/null 2>&1; RC=$?
assert_fail $RC "init with bad PG user fails"
rm -f "$CONFIG"

# Manage with wrong admin password
$EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_errtest >/dev/null 2>&1
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB manage --config "$CONFIG" --user admin --password wrongpassword list-admins >/dev/null 2>&1; RC=$?
assert_fail $RC "manage with wrong password fails"
$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes 2>/dev/null
rm -f "$CONFIG"

echo ""
echo "========================================"
echo "Section 12: Init/Destroy Stress Test"
echo "========================================"
echo ""

for i in $(seq 1 20); do
    echo "=== Stress iteration $i ==="
    rm -f "$CONFIG"
    export EXTENDDB_ADMIN_USER="$ADMIN_USER"
    export EXTENDDB_ADMIN_PASSWORD="$ADMIN_PASS"
    $EXTENDDB init --config "$CONFIG" --overwrite --data-db "extenddb_stress_$i" >/dev/null 2>&1; RC=$?
    assert_ok $RC "stress init $i"

    $EXTENDDB verify --config "$CONFIG" >/dev/null 2>&1; RC=$?
    assert_ok $RC "stress verify $i"

    $EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
    sleep 2

    $EXTENDDB status --config "$CONFIG" --port "$TEST_PORT" >/dev/null 2>&1; RC=$?
    assert_ok $RC "stress status $i"

    $EXTENDDB stop --config "$CONFIG" 2>/dev/null
    sleep 1

    $EXTENDDB destroy --config "$CONFIG" --yes >/dev/null 2>&1; RC=$?
    assert_ok $RC "stress destroy $i"
done

echo ""
echo "========================================"
echo "Section 13: Cross-Account Isolation"
echo "========================================"
echo ""

full_cleanup
rm -f "$CONFIG"
export EXTENDDB_ADMIN_USER="$ADMIN_USER"
export EXTENDDB_ADMIN_PASSWORD="$ADMIN_PASS"
$EXTENDDB init --config "$CONFIG" --overwrite --data-db extenddb_isolation >/dev/null 2>&1
$EXTENDDB serve --config "$CONFIG" --port "$TEST_PORT" 2>/dev/null
sleep 3
$EXTENDDB settings set control_plane_delay_seconds 0.05 --config "$CONFIG" 2>/dev/null || true

ALLOW_ALL_DDB='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

# Create two accounts
manage create-account --account-id 111111111111 --account-name acct-a >/dev/null 2>&1
manage create-account --account-id 222222222222 --account-name acct-b >/dev/null 2>&1

# Create users and keys
manage create-user --account-id 111111111111 --user-name userA >/dev/null 2>&1
manage create-user --account-id 222222222222 --user-name userB >/dev/null 2>&1

AK_A=$(manage create-access-key --account-id 111111111111 --user-name userA 2>&1)
KEY_A=$(echo "$AK_A" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
SEC_A=$(echo "$AK_A" | grep -i secret | grep -oP '[A-Za-z0-9/+=]{40}' | head -1)

AK_B=$(manage create-access-key --account-id 222222222222 --user-name userB 2>&1)
KEY_B=$(echo "$AK_B" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
SEC_B=$(echo "$AK_B" | grep -i secret | grep -oP '[A-Za-z0-9/+=]{40}' | head -1)

manage put-user-policy --account-id 111111111111 --user-name userA \
    --policy-name full --policy-document "$ALLOW_ALL_DDB" >/dev/null 2>&1
manage put-user-policy --account-id 222222222222 --user-name userB \
    --policy-name full --policy-document "$ALLOW_ALL_DDB" >/dev/null 2>&1

# UserA creates a table
export AWS_ACCESS_KEY_ID="$KEY_A"
export AWS_SECRET_ACCESS_KEY="$SEC_A"
export AWS_DEFAULT_REGION=us-east-1
aws_ddb create-table \
    --table-name acct-a-table \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1
sleep 2
aws_ddb put-item --table-name acct-a-table --item '{"pk":{"S":"secret"},"data":{"S":"acct-a-data"}}' >/dev/null 2>&1

# UserA can see their table
OUTPUT=$(aws_ddb list-tables 2>&1)
assert_contains "$OUTPUT" "acct-a-table" "account A sees own table"

# UserB should NOT see acct-a-table
export AWS_ACCESS_KEY_ID="$KEY_B"
export AWS_SECRET_ACCESS_KEY="$SEC_B"
OUTPUT=$(aws_ddb list-tables 2>&1)
assert_not_contains "$OUTPUT" "acct-a-table" "account B cannot see account A tables"

# UserB should NOT be able to read acct-a-table
aws_ddb get-item --table-name acct-a-table --key '{"pk":{"S":"secret"}}' >/dev/null 2>&1; RC=$?
assert_fail $RC "account B cannot read account A data"

# Cleanup
export AWS_ACCESS_KEY_ID="$KEY_A"
export AWS_SECRET_ACCESS_KEY="$SEC_A"
aws_ddb delete-table --table-name acct-a-table >/dev/null 2>&1

$EXTENDDB stop --config "$CONFIG" 2>/dev/null
sleep 1
$EXTENDDB destroy --config "$CONFIG" --yes >/dev/null 2>&1

echo ""
echo "========================================"
echo "Section 14: Final Cleanup and Summary"
echo "========================================"
echo ""

full_cleanup
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION 2>/dev/null || true
unset EXTENDDB_ADMIN_USER EXTENDDB_ADMIN_PASSWORD EXTENDDB_PASSWORD 2>/dev/null || true

echo "========================================"
echo "CLI Test Suite Complete"
echo "Finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "RESULTS: $PASS passed / $FAIL failed / $TOTAL total"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
