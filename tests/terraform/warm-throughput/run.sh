#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Apply tests/terraform/warm-throughput/main.tf against an ExtendDB and assert
# the apply completes within five minutes and a second plan has no changes.
# The tables are destroyed afterwards.
#
# Environment:
#   EXTENDDB_TEST_ENDPOINT   the server, for example https://127.0.0.1:18443
#   AWS_ACCESS_KEY_ID        a key the server accepts
#   AWS_SECRET_ACCESS_KEY
#   TERRAFORM                the terraform binary (default: install-terraform.sh)
#
#   tests/terraform/warm-throughput/run.sh
#
# Exit 0 when the apply and the plan both pass; non-zero otherwise, with the
# failing step named.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
endpoint="${EXTENDDB_TEST_ENDPOINT:?set EXTENDDB_TEST_ENDPOINT}"
: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY}"
terraform="${TERRAFORM:-$("$here/../install-terraform.sh")}"
[[ -x "$terraform" ]] || { echo "FAIL: terraform binary $terraform not found or not executable"; exit 1; }

# A scratch working copy, so the repository never holds provider binaries,
# lock files or state.
work="$(mktemp -d "${TMPDIR:-/tmp}/extenddb-tf-warm-throughput.XXXXXX")"
# The tables use fixed names, so a run that leaves them behind makes the next
# apply fail with ResourceInUseException: remove them on every exit once the
# apply has started (a failed assertion included).
applied=0
cleanup() {
    if [[ $applied -eq 1 ]]; then
        timeout 300 "$terraform" destroy -auto-approve -no-color >/dev/null 2>&1 || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT
cp "$here/main.tf" "$work/"
cd "$work"

export TF_IN_AUTOMATION=1
export TF_INPUT=0
export TF_VAR_endpoint="$endpoint"
export TF_VAR_access_key="$AWS_ACCESS_KEY_ID"
export TF_VAR_secret_key="$AWS_SECRET_ACCESS_KEY"
# Keep the provider's own credential chain out of it.
unset AWS_PROFILE AWS_SESSION_TOKEN

echo "=== terraform init ($("$terraform" version | head -1))"
"$terraform" init -no-color >init.log 2>&1 || { cat init.log; echo "FAIL: terraform init"; exit 1; }

echo "=== terraform apply (5 minute limit)"
applied=1
set +e
timeout 300 "$terraform" apply -auto-approve -no-color >apply.log 2>&1
apply_status=$?
set -e
if [[ $apply_status -ne 0 ]]; then
    cat apply.log
    if [[ $apply_status -eq 124 ]]; then
        echo "FAIL: terraform apply did not finish within 300 s (the provider waits for WarmThroughput.Status ACTIVE)"
    else
        echo "FAIL: terraform apply exited $apply_status"
    fi
    exit 1
fi
grep -E "Apply complete! Resources: 2 added" apply.log || { cat apply.log; echo "FAIL: expected 2 resources added"; exit 1; }

echo "=== terraform plan (must report no changes)"
set +e
timeout 300 "$terraform" plan -detailed-exitcode -no-color >plan.log 2>&1
plan_status=$?
set -e
if [[ $plan_status -ne 0 ]]; then
    cat plan.log
    echo "FAIL: terraform plan exited $plan_status (2 means the applied tables drifted from the configuration)"
    exit 1
fi
grep -E "No changes" plan.log || { cat plan.log; echo "FAIL: terraform plan did not report No changes"; exit 1; }

echo "=== terraform destroy"
timeout 300 "$terraform" destroy -auto-approve -no-color >destroy.log 2>&1 || { cat destroy.log; echo "FAIL: terraform destroy"; exit 1; }
applied=0
echo "PASS: apply completed, second plan had no changes, tables destroyed"
