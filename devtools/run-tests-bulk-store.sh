# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# The bulk store of devtools/run-tests (--bulk-store). run-tests sources
# this file; it is not run on its own. The functions read BULK_STORE,
# TARGET, BINARY and the EXTENDDB_TEST_S3_* variables, and set
# NEED_RESTART and SERVE_ENV.

# Check the --bulk-store name, then export EXTENDDB_TEST_BULK_STORE: the
# bulk tests write their inputs to and read their reports from the store it
# names (tests/bulk_put/bulk_store.py).
bulk_store_check_name() {
    case "$BULK_STORE" in
        filesystem|s3) ;;
        *)
            echo "error: --bulk-store must be 'filesystem' or 's3' (got: $BULK_STORE)"
            exit 1
            ;;
    esac
    export EXTENDDB_TEST_BULK_STORE="$BULK_STORE"
}

# For the S3 store, check the target, the endpoint and its keys.
bulk_store_check_s3() {
    [[ "$BULK_STORE" == "s3" ]] || return 0
    # The bulk tests create and delete buckets on the endpoint, so it must
    # be a local service, as the extenddb endpoint must.
    if [[ "$TARGET" != "extenddb" ]]; then
        echo "error: --bulk-store s3 applies to --extenddb only."
        exit 1
    fi
    if [[ ! "${EXTENDDB_TEST_S3_ENDPOINT:-}" =~ ^https?://(localhost|127\.0\.0\.1)(:[0-9]+)?(/.*)?$ ]]; then
        echo "error: --bulk-store s3 requires EXTENDDB_TEST_S3_ENDPOINT to point to an S3-compatible service on localhost."
        echo "  Got: ${EXTENDDB_TEST_S3_ENDPOINT:-(not set)}"
        exit 1
    fi
    if [[ -z "${EXTENDDB_TEST_S3_ACCESS_KEY_ID:-}" || -z "${EXTENDDB_TEST_S3_SECRET_ACCESS_KEY:-}" ]]; then
        echo "error: --bulk-store s3 requires EXTENDDB_TEST_S3_ACCESS_KEY_ID and EXTENDDB_TEST_S3_SECRET_ACCESS_KEY."
        exit 1
    fi
}

# Configure the bulk store in the config file $1, and set NEED_RESTART when
# the server must restart to use it. The filesystem store is one directory,
# $2, holding bulk inputs and reports as {root}/{account}/{S3Bucket}/...;
# the bulk tests write fixtures through tempfile, so it is the same TMPDIR
# the import and export paths allow. The S3 store reads and writes buckets
# on EXTENDDB_TEST_S3_ENDPOINT with path-style addressing.
bulk_store_configure() {
    local config=$1 root=$2
    if [[ "$BULK_STORE" == "s3" ]]; then
        if ! grep -A2 '^\[bulk\.s3\]' "$config" 2>/dev/null | grep -qF "\"$EXTENDDB_TEST_S3_ENDPOINT\""; then
            if grep -q '^\[bulk' "$config"; then
                echo "  ✗ $config already configures a bulk store; remove its [bulk] tables to switch to the S3 store."
                exit 1
            fi
            printf '\n[bulk]\nstore = "s3"\n\n[bulk.s3]\nendpoint_url = "%s"\nforce_path_style = true\n' "$EXTENDDB_TEST_S3_ENDPOINT" >> "$config"
            echo "  ✓ [bulk] S3 store configured at $EXTENDDB_TEST_S3_ENDPOINT"
        fi
        # The server takes its S3 keys from its environment, which a
        # running server may have been started without, so an S3 run
        # always restarts it with them.
        if [[ ! -x "$BINARY" ]]; then
            echo "  ✗ An S3 store run restarts the server with its S3 keys, which needs $BINARY."
            exit 1
        fi
        NEED_RESTART=true
    elif ! grep -A1 '^\[bulk\.filesystem\]' "$config" 2>/dev/null | grep -qF "$root"; then
        if grep -q '^\[bulk' "$config"; then
            echo "  ✗ $config already configures a bulk store; remove its [bulk] tables to switch to the filesystem store at $root."
            exit 1
        fi
        printf '\n[bulk]\n\n[bulk.filesystem]\npath = "%s"\n' "$root" >> "$config"
        NEED_RESTART=true
        echo "  ✓ [bulk] store configured at $root"
    fi
}

# Set SERVE_ENV, the command prefix the server is restarted under. The S3
# keys go to the server's environment only: the test process keeps the
# extenddb keys provisioned for it as its AWS credentials.
bulk_store_serve_env() {
    SERVE_ENV=(env)
    if [[ "$BULK_STORE" == "s3" ]]; then
        SERVE_ENV=(env -u AWS_SESSION_TOKEN -u AWS_PROFILE
            "AWS_ACCESS_KEY_ID=$EXTENDDB_TEST_S3_ACCESS_KEY_ID"
            "AWS_SECRET_ACCESS_KEY=$EXTENDDB_TEST_S3_SECRET_ACCESS_KEY"
            AWS_EC2_METADATA_DISABLED=true)
    fi
}
