#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# wasm-reachability-lint.py — a cheap backstop for the WebAssembly build of the
# engine. Some APIs COMPILE to wasm32-unknown-unknown but TRAP at runtime when
# called, and a trap is permanent for a wasm instance (a panic cannot unwind on
# wasm32):
#
#   * std::time::{SystemTime, Instant}::now() — "time not implemented on this
#     platform"; the build reads the clock through web-time instead.
#   * tokio::time::Instant::now() — the same wall-clock trap by another name.
#   * spawning OS threads (std::thread) or tokio tasks (tokio::spawn) — the
#     browser has one thread and no tokio reactor driving the adapter.
#
# A compile check cannot see a runtime-only trap, and clippy's own lints do not
# cover this class either, so this scan is the standing guard against one
# reappearing in code that the browser build actually runs.
#
# Scope. Only the shared engine crates the adapter pulls into the wasm module
# are scanned (the adapter crate itself is exercised end to end by the headless
# corpus, which would surface a trap directly). Within each crate only the code
# that reaches the wasm target is scanned:
#   * modules declared `#[cfg(not(target_arch = "wasm32"))] mod X;` are native
#     only — skipped whole, with their subtrees (backups, import/export, the
#     file-backed migration and snapshot paths).
#   * `#[cfg(test)]` modules and test files never enter the module — skipped.
#   * items carrying `#[cfg(not(target_arch = "wasm32"))]` or `#[cfg(test)]`
#     inside an otherwise-reachable file are blanked before scanning (so a
#     surgically native-only block, e.g. a vector-index backfill, does not trip
#     the lint while its wasm floor stands beside it).
# A short, documented allowlist covers files that compile for wasm but are not
# on the in-scope data plane — background workers the in-memory engine never
# starts, and the PartiQL parser the browser build compiles out. The wasm build
# and `cargo clippy --target wasm32-unknown-unknown -- -D warnings` remain the
# authoritative guards; this lint is the backstop for the compile-but-trap class
# in genuinely reachable code.
#
# Exit 0 = clean, 1 = a violation (printed with file:line).

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CRATES = ["engine", "storage-sqlite", "storage", "core", "cache", "auth"]

# std clock paths that trap on wasm — never allowed (read the clock through
# web-time, which re-exports std::time on native).
TIME_PATHS = ("std::time::SystemTime", "std::time::Instant", "std::time::UNIX_EPOCH")
# a `use std::time::{...}` group that names a clock type.
TIME_USE = re.compile(r"use\s+(?:::)?std::time::\{([^}]*)\}")
TIME_NAMES = ("SystemTime", "Instant", "UNIX_EPOCH")

# Thread/async spawning and the tokio wall clock, which trap on the browser
# build (one thread, no tokio reactor driving the adapter).
SPAWN_PATTERNS = (
    "std::thread::spawn",
    "thread::Builder",
    "std::thread::scope",
    "thread::scope",
    "tokio::spawn",
    "tokio::task::spawn",
    "tokio::time::Instant::now",
)

# Files that compile for wasm but are never reached by the in-scope data plane,
# so a spawn/clock in them is not a browser-build trap. Each needs a reason.
#   * The background workers a server starts: the in-memory engine runs a
#     synchronous control plane and starts none of them, so their module is
#     compiled but never entered.
#   * The PartiQL parser's big-stack thread: the browser build compiles the
#     engine without the PartiQL feature, so the parser is not in the wasm
#     module; the adapter refuses the PartiQL operations before dispatch.
#   * Background backup/snapshot helpers and the bcrypt verifier: off the
#     in-memory data plane (backups are refused; the build runs no authz).
ALLOWLIST = (
    "crates/storage-sqlite/src/hooks.rs",          # server background-worker spawner
    "crates/storage-sqlite/src/workers.rs",        # the background workers themselves
    "crates/storage-sqlite/src/store.rs",          # a non-test async-drain helper
    "crates/storage-sqlite/src/data/vector_index.rs",  # async vector backfill helper
    "crates/storage-sqlite/src/backup.rs",         # backup engine (backups are refused)
    "crates/storage-sqlite/src/backup_catalog.rs", # backup catalog sweeper
    "crates/cache/src/swr.rs",                     # stale-while-revalidate refresh task
    "crates/storage/src/hooks.rs",                 # storage background-hook interval
    "crates/storage/src/password.rs",              # bcrypt verifier (no authz on wasm)
    "crates/storage/src/bootstrapper.rs",          # admin-credential bcrypt bootstrap
    "crates/core/src/partiql/parser/limits.rs",    # PartiQL parser; compiled out on wasm
    "crates/core/src/backup_format/data_file.rs",  # backup file format (off data plane)
)

# A `[pub] mod NAME;` declaration.
MOD_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z0-9_]+)\s*;")
# The attributes that make an item native-only, test-only, or reachable only
# under a test-harness feature the browser build never enables (`test-util`,
# `test-hooks`) — none of these compile into the wasm module.
SKIP_ATTR = re.compile(
    r'#\[cfg\((?:'
    r"test"
    r'|not\(target_arch\s*=\s*"wasm32"\)'
    r'|feature\s*=\s*"test-(?:util|hooks)"'
    r')\)\]'
)


def is_test_file(path: Path) -> bool:
    name = path.name
    rel = str(path)
    return (
        name == "tests.rs"
        or name.endswith("_tests.rs")
        or "/tests/" in rel
        or "bench" in name
    )


def module_file_candidates(decl_file: Path, name: str) -> list:
    """The file(s) a `mod NAME;` in `decl_file` resolves to: NAME.rs or
    NAME/mod.rs, in the declaring module's directory (the crate/module dir for
    lib.rs/mod.rs, else a subdirectory named after the declaring file)."""
    if decl_file.name in ("lib.rs", "mod.rs"):
        base = decl_file.parent
    else:
        base = decl_file.parent / decl_file.stem
    return [base / f"{name}.rs", base / name / "mod.rs", base / name]


def collect_skipped_files(src: Path) -> set:
    """Files (and subtree directories) reached only through a native-only or
    test-only `mod` declaration, which therefore never compile to wasm."""
    skipped = set()
    for path in src.rglob("*.rs"):
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines):
            if not SKIP_ATTR.search(line):
                continue
            # The mod declaration is on a following line (after any stacked
            # attributes); an inline `mod X { ... }` block is handled by the
            # item blanking instead.
            j = i + 1
            while j < len(lines) and lines[j].strip().startswith("#["):
                j += 1
            if j >= len(lines):
                continue
            m = MOD_DECL.match(lines[j])
            if not m:
                continue
            for cand in module_file_candidates(path, m.group(1)):
                skipped.add(cand.resolve())
    return skipped


def is_skipped(path: Path, skipped: set) -> bool:
    rp = path.resolve()
    if rp in skipped:
        return True
    # under a skipped module directory
    return any(d in rp.parents for d in skipped)


def strip_skipped_items(text: str) -> str:
    """Blank `#[cfg(test)]` / `#[cfg(not(target_arch = "wasm32"))]` items —
    either a brace block or a single statement — so reachable scanning ignores
    code that never compiles to wasm. Blanked lines are kept (empty) so reported
    line numbers stay accurate."""
    lines = text.splitlines()
    out = list(lines)
    n = len(lines)
    i = 0
    while i < n:
        if SKIP_ATTR.search(lines[i].strip()):
            out[i] = ""
            j = i + 1
            while j < n and lines[j].strip().startswith("#["):
                out[j] = ""
                j += 1
            depth = 0
            seen_code = False
            while j < n:
                s = lines[j]
                out[j] = ""
                depth += s.count("{") + s.count("(") + s.count("[")
                depth -= s.count("}") + s.count(")") + s.count("]")
                st = s.strip()
                if st:
                    seen_code = True
                if seen_code and depth <= 0 and (
                    st.endswith(";") or st.endswith("}") or st.endswith(",")
                ):
                    break
                j += 1
            i = j + 1
            continue
        i += 1
    return "\n".join(out)


def main() -> int:
    time_hits = []
    spawn_hits = []
    allow = {str(ROOT / p) for p in ALLOWLIST}
    for crate in CRATES:
        src = ROOT / "crates" / crate / "src"
        if not src.is_dir():
            continue
        skipped = collect_skipped_files(src)
        for path in sorted(src.rglob("*.rs")):
            rel = str(path.relative_to(ROOT))
            # Test files, native-only modules, and the documented off-data-plane
            # allowlist never run on the in-scope wasm path, so their clock and
            # spawn calls are not browser-build traps.
            if (
                is_test_file(path)
                or is_skipped(path, skipped)
                or str(path) in allow
            ):
                continue
            body = strip_skipped_items(path.read_text())

            for lineno, line in enumerate(body.splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("//"):
                    continue
                if any(p in line for p in TIME_PATHS):
                    time_hits.append((rel, lineno, line.strip()))
                m = TIME_USE.search(line)
                if m and any(
                    t in {x.strip() for x in m.group(1).split(",")} for t in TIME_NAMES
                ):
                    time_hits.append((rel, lineno, line.strip()))
                if any(p in line for p in SPAWN_PATTERNS):
                    spawn_hits.append((rel, lineno, line.strip()))

    ok = True
    if time_hits:
        ok = False
        print("wasm-reachability-lint: std::time clock paths found (use web_time::):")
        for rel, lineno, text in time_hits:
            print(f"  {rel}:{lineno}: {text}")
    if spawn_hits:
        ok = False
        print(
            "wasm-reachability-lint: thread/task spawn or tokio clock found "
            "(the browser build has one thread, no reactor, and no host clock):"
        )
        for rel, lineno, text in spawn_hits:
            print(f"  {rel}:{lineno}: {text}")

    if ok:
        print(
            "wasm-reachability-lint: OK "
            f"({len(CRATES)} crates clean of wasm-trapping clock and spawn calls "
            "in reachable code)"
        )
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
