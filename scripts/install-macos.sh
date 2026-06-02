#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
# Install script for extenddb on macOS.
#
# What this script does:
#   1. Checks dependencies (Rust toolchain, TiDB/MySQL client, Python 3).
#   2. Creates a Python venv and installs doc-build requirements.
#   3. Builds extenddb in release mode.
#   4. Builds PDF documentation.
#   5. Prints post-install instructions.
#
# Usage: scripts/install-macos.sh
#
# This script does NOT install missing dependencies — it reports them
# and exits so you can install them with your package manager.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
REQUIREMENTS="$PROJECT_ROOT/requirements.txt"

# Colors (disabled if not a terminal)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    RESET='\033[0m'
else
    RED='' GREEN='' YELLOW='' BOLD='' RESET=''
fi

info()  { echo -e "${GREEN}✓${RESET} $*"; }
warn()  { echo -e "${YELLOW}⚠${RESET} $*"; }
fail()  { echo -e "${RED}✗${RESET} $*"; }

echo -e "${BOLD}=== extenddb macOS Installer ===${RESET}"
echo

# ── Step 1: Check dependencies ──────────────────────────────────────

MISSING=0

echo -e "${BOLD}Checking dependencies...${RESET}"

# Rust toolchain
if command -v cargo &>/dev/null; then
    RUST_VER="$(rustc --version | awk '{print $2}')"
    info "Rust toolchain: rustc $RUST_VER"
else
    fail "Rust toolchain not found. Install via: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    MISSING=1
fi

# TiDB/MySQL client (Homebrew)
if command -v mysql &>/dev/null; then
    MYSQL_VER="$(mysql --version)"
    info "TiDB/MySQL client: $MYSQL_VER"
else
    fail "TiDB/MySQL client not found. Install via: brew install mysql-client"
    MISSING=1
fi

# TiDB SQL endpoint readiness. This is a warning, not a build blocker, because
# users may initialize against a remote TiDB endpoint after the install step.
if command -v mysql &>/dev/null; then
    if mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();" >/dev/null 2>&1; then
        info "TiDB SQL endpoint: accepting connections on 127.0.0.1:4000"
    else
        warn "TiDB SQL endpoint not reachable on 127.0.0.1:4000. Start TiDB before running extenddb init, for example: tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor"
    fi
fi

# Python 3
if command -v python3 &>/dev/null; then
    PY_VER="$(python3 --version | awk '{print $2}')"
    info "Python: $PY_VER"
else
    fail "Python 3 not found. Install via: brew install python3"
    MISSING=1
fi

echo
if [ "$MISSING" -ne 0 ]; then
    echo -e "${RED}Missing dependencies. Install them and re-run this script.${RESET}"
    exit 1
fi

# ── Step 2: Create Python venv ───────────────────────────────────────

echo -e "${BOLD}Setting up Python virtual environment...${RESET}"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    info "Created venv at $VENV_DIR"
else
    info "Venv already exists at $VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

"$VENV_DIR/bin/pip" install -q -r "$REQUIREMENTS"
info "Installed Python dependencies"
echo

# ── Step 3: Build extenddb ───────────────────────────────────────────────

echo -e "${BOLD}Building extenddb (release mode)...${RESET}"

cargo build -j12 --release --manifest-path "$PROJECT_ROOT/Cargo.toml"
info "Built target/release/extenddb"
echo

# ── Step 4: Build PDF documentation ─────────────────────────────────

echo -e "${BOLD}Building PDF documentation...${RESET}"

python3 "$PROJECT_ROOT/docs/build-docs.py"
echo

# ── Step 5: Post-install instructions ────────────────────────────────

BINARY="$PROJECT_ROOT/target/release/extenddb"
PDF_DIR="$PROJECT_ROOT/pdfs"

echo -e "${BOLD}=== Installation complete ===${RESET}"
echo
echo "Binary:"
echo "  $BINARY"
echo
echo "To add extenddb to your PATH:"
echo "  export PATH=\"$PROJECT_ROOT/target/release:\$PATH\""
echo
echo "PDF documentation:"
echo "  $PDF_DIR/"
echo
echo "Next steps:"
echo "  1. Ensure TiDB is reachable: mysql -h 127.0.0.1 -P 4000 -uroot -e 'SELECT VERSION();'"
echo "  2. Initialize: extenddb init"
echo "  3. Verify:     extenddb verify --config extenddb.toml"
echo "  4. Start:      extenddb serve --config extenddb.toml"
echo
echo "See docs/manuals/09-install-macos.md for the full guide."
