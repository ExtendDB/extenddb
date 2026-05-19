#!/usr/bin/env bash
#
# detect-state.sh
#
# Purpose: Environment-state detection for the extenddb skill. Resolves
# the user's current position in the extenddb onboarding journey to a single
# resume-point string on stdout, so the skill can branch the user directly to
# the correct stage instead of walking through every check in prose.
#
# Design reference: .agents/specs/extenddb-skill/design.md Section 3
# ("Environment State Detection").
#
# Read-only guarantee per Requirement 14.3: This script performs ONLY read-only
# checks (test -x, test -f, and invoking `extenddb status`, which is read-only by
# design). It does not create, modify, move, delete, or change permissions on
# any file, and it does not kill any process.
#
# Output contract:
#   stdout: exactly one of: dependencies | postgres | init | iam |
#           running-server-stopped
#   stderr: a human-readable one-line summary of what was detected
#   exit 0: a state was detected successfully
#   exit non-zero: the script itself failed (for example, REPO_ROOT could not
#           be determined)

# Strict mode. We deliberately do NOT use `set -e` because step 4
# (invoking `extenddb status`) may exit non-zero, and that non-zero exit is a
# meaningful signal (server present but not running), not a script failure.
# `set -u` catches uninitialized variables without interfering with the
# fall-through logic.
set -u

# Resolve REPO_ROOT from the script's own location. The script lives at
# .agents/skills/extenddb/scripts/detect-state.sh, so the repo root is
# four levels up from the script's directory.
script_path="${BASH_SOURCE[0]}"
script_dir="$(cd "$(dirname "${script_path}")" && pwd)"
if [ -z "${script_dir}" ]; then
    echo "detect-state.sh: unable to determine script directory" 1>&2
    exit 2
fi
REPO_ROOT="$(cd "${script_dir}/../../../.." && pwd)"
if [ -z "${REPO_ROOT}" ] || [ ! -d "${REPO_ROOT}" ]; then
    echo "detect-state.sh: unable to determine REPO_ROOT from ${script_dir}" 1>&2
    exit 2
fi

EXTENDDB_BIN="${REPO_ROOT}/target/release/extenddb"
EXTENDDB_TOML="${REPO_ROOT}/extenddb.toml"
TLS_CERT="${HOME}/.extenddb/tls/cert.pem"

# Step 1: Is the extenddb binary built?
if ! test -x "${EXTENDDB_BIN}"; then
    echo "detected state: extenddb binary absent at ${EXTENDDB_BIN}; resume at dependencies stage" 1>&2
    echo "dependencies"
    exit 0
fi

# Step 2: Has `extenddb init` been run (produces extenddb.toml)?
if ! test -f "${EXTENDDB_TOML}"; then
    echo "detected state: extenddb binary present, ${EXTENDDB_TOML} absent; resume at postgres stage" 1>&2
    echo "postgres"
    exit 0
fi

# Step 3: Did init complete far enough to emit the self-signed TLS cert?
if ! test -f "${TLS_CERT}"; then
    echo "detected state: extenddb.toml present but ${TLS_CERT} absent; resume at init stage (partial init state; the extenddb_catalog and extenddb data databases likely exist from the prior init run, so extenddb destroy --config extenddb.toml --yes is required before extenddb init will succeed again)" 1>&2
    echo "init"
    exit 0
fi

# Step 4: Is the server currently running?
# `extenddb status` is read-only by design. We capture its combined output into a
# variable (then discard it) so we can observe the exit code without letting
# output leak to the terminal and without using a file redirect.
status_exit=0
_ignored_output="$( "${EXTENDDB_BIN}" status --config "${EXTENDDB_TOML}" 2>&1 )" || status_exit=$?
unset _ignored_output
if [ "${status_exit}" -eq 0 ]; then
    echo "detected state: binary, extenddb.toml, and TLS cert all present; extenddb status reports running; resume at iam stage" 1>&2
    echo "iam"
    exit 0
else
    echo "detected state: binary, extenddb.toml, and TLS cert all present; extenddb status exit=${status_exit} (not running); offer choice between starting the server or destroying and re-initializing" 1>&2
    echo "running-server-stopped"
    exit 0
fi
