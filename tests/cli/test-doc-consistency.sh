#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors. Proprietary and confidential.
# All rights reserved. Unauthorized copying, distribution, or use is prohibited.
# THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND.
#
# Static documentation consistency checker for extenddb.
# Verifies that documentation matches the actual CLI, config, settings, and codebase.
# No server needed — all checks are offline.
#
# Prerequisites:
#   - extenddb binary built at ./target/release/extenddb
#   - jq, grep, diff available
#
# Usage: ./tests/cli/test-doc-consistency.sh

set -uo pipefail

EXTENDDB=./target/release/extenddb
PASS=0
FAIL=0
TOTAL=0
INFO_COUNT=0

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

info() {
    INFO_COUNT=$((INFO_COUNT + 1))
    echo "INFO: $*"
}

# Prerequisite
test -x "$EXTENDDB"; RC=$?
assert_ok $RC "extenddb binary exists"
if [ $RC -ne 0 ]; then echo "FATAL: extenddb binary not found"; exit 1; fi

echo "========================================"
echo "extenddb Documentation Consistency Checker"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================"
echo ""

echo "--- Check 1: CLI Help vs. Documentation ---"
echo ""

# Top-level commands
COMMANDS=$($EXTENDDB --help 2>&1 | awk '/Commands:/{found=1;next} /Options:/{found=0} found && NF{print $1}' | grep -v '^$')

for cmd in $COMMANDS; do
    if [ "$cmd" = "help" ]; then continue; fi
    HELP=$($EXTENDDB "$cmd" --help 2>&1 || true)
    HELP_FLAGS=$(echo "$HELP" | grep -oP '\-\-[a-z][a-z0-9-]*' | sort -u)
    for flag in $HELP_FLAGS; do
        if [ "$flag" = "--help" ] || [ "$flag" = "--version" ]; then continue; fi
        grep -rq "$flag" docs/manuals/ docs/install-from-source.md docs/first-requests.md 2>/dev/null; RC=$?
        assert_ok $RC "flag $flag for '$cmd' documented"
    done
done

# Manage subcommands
echo ""
echo "--- Check 1b: Manage Subcommands in Docs ---"
echo ""

MANAGE_CMDS=$($EXTENDDB manage --help 2>&1 | awk '/Commands:/{found=1;next} /Options:/{found=0} found && NF{print $1}' | grep -v '^$')

for subcmd in $MANAGE_CMDS; do
    if [ "$subcmd" = "help" ]; then continue; fi
    grep -rq "$subcmd" docs/manuals/ docs/install-from-source.md 2>/dev/null; RC=$?
    assert_ok $RC "manage subcommand '$subcmd' documented"
done

echo ""
echo "--- Check 2: Sample Config vs. Documentation ---"
echo ""

if [ -f extenddb.sample.toml ]; then
    SAMPLE_KEYS=$(grep -oP '^#?\s*([a-z_]+)\s*=' extenddb.sample.toml | sed 's/^#\s*//' | sed 's/\s*=.*//' | sort -u)
    for key in $SAMPLE_KEYS; do
        # Skip very generic keys
        if [ ${#key} -lt 3 ]; then continue; fi
        grep -rq "$key" docs/manuals/ docs/install-from-source.md 2>/dev/null; RC=$?
        assert_ok $RC "config key '$key' documented"
    done
else
    info "extenddb.sample.toml not found — skipping Check 2"
fi

echo ""
echo "--- Check 3: Runtime Settings vs. Documentation ---"
echo ""

# Extract setting names from source code
SETTINGS=""
for f in crates/bin/src/cmd_settings.rs crates/server/src/management/ops_settings.rs; do
    if [ -f "$f" ]; then
        FOUND=$(grep -oP '"[a-z_]+"' "$f" | tr -d '"' | sort -u)
        SETTINGS="$SETTINGS $FOUND"
    fi
done
SETTINGS=$(echo "$SETTINGS" | tr ' ' '\n' | sort -u | grep -v '^$')

for setting in $SETTINGS; do
    if [ ${#setting} -lt 3 ]; then continue; fi
    grep -q "$setting" docs/manuals/05-admin-guide.md 2>/dev/null; RC=$?
    assert_ok $RC "runtime setting '$setting' in the admin guide"
done

echo ""
echo "--- Check 4: Error Messages in Troubleshooting.md (informational) ---"
echo ""

if [ -f docs/troubleshooting.md ]; then
    MISSING=0
    while IFS= read -r pattern; do
        KEY=$(echo "$pattern" | head -c 30)
        if ! grep -q "$KEY" docs/troubleshooting.md 2>/dev/null; then
            info "error message not in troubleshooting.md: $pattern"
            MISSING=$((MISSING + 1))
        fi
    done < <(grep -rhoP '(?:error|critical)!\s*\(\s*"([^"]+)"' crates/*/src/*.rs crates/*/src/**/*.rs 2>/dev/null | grep -oP '"[^"]+"' | tr -d '"' | sort -u)
    echo "Error messages not in troubleshooting.md: $MISSING (review manually)"
else
    info "docs/troubleshooting.md not found — skipping Check 4"
fi

echo ""
echo "--- Check 5: Manage Subcommand Completeness ---"
echo ""

ACTUAL=$($EXTENDDB manage --help 2>&1 | awk '/Commands:/{found=1;next} /Options:/{found=0} found && NF{print $1}' | grep -v '^$' | sort)
DOCUMENTED=$(grep -oP 'manage\s+[a-z][-a-z]+' docs/manuals/05-admin-guide.md 2>/dev/null | awk '{print $2}' | sort -u)

# Subcommands in binary but not in docs
while IFS= read -r cmd; do
    if [ -n "$cmd" ] && [ "$cmd" != "help" ]; then
        TOTAL=$((TOTAL + 1))
        FAIL=$((FAIL + 1))
        echo "FAIL: manage subcommand '$cmd' exists in binary but not documented"
    fi
done < <(comm -23 <(echo "$ACTUAL") <(echo "$DOCUMENTED") 2>/dev/null)

# Subcommands in docs but not in binary (stale docs)
while IFS= read -r cmd; do
    if [ -n "$cmd" ]; then
        TOTAL=$((TOTAL + 1))
        FAIL=$((FAIL + 1))
        echo "FAIL: manage subcommand '$cmd' documented but not in binary (stale)"
    fi
done < <(comm -13 <(echo "$ACTUAL") <(echo "$DOCUMENTED") 2>/dev/null)

echo ""
echo "--- Check 6: Build-Docs Pipeline ---"
echo ""

if [ -d docs/manuals ] && [ -f docs/build-docs.py ]; then
    for md in docs/manuals/*.md; do
        BASENAME=$(basename "$md" .md)
        # Strip leading number prefix (e.g., "01-architecture-guide" -> "architecture-guide")
        SLUG=$(echo "$BASENAME" | sed 's/^[0-9]*-//')
        grep -q "$SLUG" docs/build-docs.py 2>/dev/null; RC=$?
        assert_ok $RC "manual '$SLUG' in build-docs.py DOCUMENTS list"
    done
else
    info "docs/manuals/ or docs/build-docs.py not found — skipping Check 6"
fi

echo ""
echo "--- Check 7: Public API Doc Coverage (informational) ---"
echo ""

UNDOCUMENTED=0
for rs in $(find crates -name '*.rs' -not -path '*/target/*' 2>/dev/null); do
    COUNT=$(awk '
        /^[[:space:]]*\/\/\// { doc=1; next }
        /^[[:space:]]*pub (fn|struct|enum|trait) / { if (!doc) undoc++; doc=0; next }
        { doc=0 }
        END { print undoc+0 }
    ' "$rs")
    UNDOCUMENTED=$((UNDOCUMENTED + COUNT))
done
echo "Undocumented public items: $UNDOCUMENTED"

echo ""
echo "--- Check 8: lib.rs Module Docs ---"
echo ""

for lib in crates/*/src/lib.rs; do
    if [ -f "$lib" ]; then
        head -5 "$lib" | grep -q '^//!'; RC=$?
        CRATE_DIR=$(dirname "$(dirname "$lib")")
        assert_ok $RC "$(basename "$CRATE_DIR") has //! module docs"
    fi
done

echo ""
echo "--- Check 9: ADR Currency (informational) ---"
echo ""

if [ -d docs/adr ]; then
    for adr in docs/adr/*.md; do
        if [ ! -f "$adr" ]; then continue; fi
        BASENAME=$(basename "$adr" .md)
        REFS=$(grep -oP '`([A-Za-z_][A-Za-z0-9_:]+)`' "$adr" | tr -d '`' | sort -u | head -20)
        STALE=0
        for ref in $REFS; do
            if [ ${#ref} -lt 4 ]; then continue; fi
            if ! grep -rq "$ref" crates/ 2>/dev/null; then
                STALE=$((STALE + 1))
            fi
        done
        if [ "$STALE" -gt 3 ]; then
            info "ADR '$BASENAME' may be stale ($STALE unresolved references)"
        fi
    done
else
    info "docs/adr/ not found — skipping Check 9"
fi

echo ""
echo "--- Check 10: Version Consistency ---"
echo ""

CARGO_VERSION=$(grep -A2 '\[workspace.package\]' Cargo.toml | grep 'version' | grep -oP '\d+\.\d+\.\d+' | head -1)
BINARY_VERSION=$($EXTENDDB version 2>&1 | grep -oP '\d+\.\d+\.\d+' | head -1)

test "$CARGO_VERSION" = "$BINARY_VERSION" 2>/dev/null; RC=$?
assert_ok $RC "binary version matches Cargo.toml ($CARGO_VERSION vs $BINARY_VERSION)"

echo ""
echo "========================================"
echo "Documentation Consistency Check Complete"
echo "Finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "RESULTS: $PASS passed / $FAIL failed / $TOTAL total"
echo "INFO items: $INFO_COUNT"
echo "Undocumented public items: $UNDOCUMENTED"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
