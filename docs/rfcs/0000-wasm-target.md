# RFC-0000: WebAssembly Target

- Status: Draft
- Author: @yesyayen
- Created: 2026-08-20

## Summary

Make the ExtendDB engine compile to `wasm32-unknown-unknown`, and ship one consumer of it: a
static page where a visitor runs Amazon DynamoDB API calls against a real ExtendDB with nothing
installed. Storage is SQLite compiled to wasm. An operation allowlist bounds the v1 surface.
Most of the work below is the target itself; the playground is one consumer of it.

## Motivation

ExtendDB runs where a native binary or a container runtime runs. A wasm build adds hosts where
neither is: a browser tab, and a Node process with no native install. v1 delivers the browser
host. The other becomes reachable and needs an embedding this RFC does not specify.

The browser host is also the cheapest proof that the target works, and it pays for itself.
Trying ExtendDB today means a Rust toolchain and a build before the visitor learns whether the
wire protocol behaves the way they expect, which is the thing they came to check.
Documentation can link to a live console, the getting-started guide becomes runnable, and a bug
report can arrive as a shareable request instead of prose.

It also forces a seam worth having. After this lands, nothing in the engine may assume a
socket, a thread, or a filesystem, and one `cargo check` invocation in CI keeps it that way.
Neither holds today: the engine reaches an HTTP framework through the auth crate, and the
import and export path opens files.

## Detailed design

### Architecture: the dispatch seam

`extenddb-engine` exposes one operation-name entry point, `dispatch`, taking an operation name,
a JSON body, a request context, and the server address that `DescribeEndpoints` echoes. The
HTTP server is one caller. A wasm shim is the second, in a new `extenddb-wasm` crate at
`crates/wasm`, exported through `wasm-bindgen`.

The shim admits only the v1 surface. A `V1Op` newtype constructible through a single
`parse_v1_op` function is the gate, over one `const V1_OPS: &[&str]`. `is_known_operation`
(`crates/engine/src/lib.rs:81`) already enumerates every operation `dispatch` accepts; it
becomes a `const OPERATIONS: &[&str]` that the predicate reads, and a test asserts that
`V1_OPS` and a named excluded list partition it, so a new operation fails the test until
someone classifies it.

One exclusion is not an operation name and needs two field checks in the shim alongside the
allowlist: CreateTable rejects `GlobalSecondaryIndexes` and `LocalSecondaryIndexes`, and Query
and Scan reject `IndexName`. The management and IAM APIs are not `dispatch` operations at all,
and the shim exports no management entry point. Everything else out of scope is an operation
and stops at the allowlist.

No auth runs on this path: no SigV4 verification, one anonymous account, no credential in the
bundle. Everything executes in the visitor's own tab.

### Storage in wasm

SQLite compiled to wasm through `sqlite-wasm-rs`, one connection, in linear memory. v1 does
not use OPFS, so in the browser host a database lives for the life of the tab and a reload
starts clean.

No background worker runs on this target. The boot path sets `control_plane_delay_seconds` and
`index_propagation_delay_ms` to zero, against seeded
defaults of `0.25` and `10`. At any non-zero value the backend writes the table as CREATING and
schedules the flip to ACTIVE for a poller that never starts. Nothing hangs: DescribeTable
reports CREATING forever and every data-plane call returns ResourceNotFoundException, so the
first PutItem a visitor tries fails against a table they watched get created. Zero completes
the transition inside the write's own transaction. The index delay cannot matter while no index
can exist; it is set anyway, so writes against a database predating the index check do not
queue for a worker that never runs.

### Executor seam and the crate split

`sqlx` does not compile to wasm, and it is reachable from every statement in the SQLite
backend. A new `extenddb-sqlite-exec` crate owns the executor surface: parameter binding, a
borrowed row cursor, column decoding, one `query_fold` primitive that the other helpers wrap,
and explicit `begin_write` and `begin_read_snapshot` constructors.

`crates/storage-sqlite` then splits along a line the code already has. The engine half, about
two thirds of its statement sites, converts to the executor and moves into an internal
`extenddb-sqlite-engine` crate. The catalog half keeps its direct `sqlx` use.

The line is drawn through the code, not the schema. `schema.rs` is on the engine side, so the
wasm build still creates every catalog table and reads `settings` directly. What stays behind
is the four stores, the bootstrapper and the backend factories.

Converted and compiled on wasm are also two different things. `update_table.rs`, `workers.rs`
and `backup.rs` convert to the executor because call sites cross from them into
transaction-typed data-layer helpers, and none of the three is wanted in the browser build.
`WorkerStore` is unaffected: it lives in `worker.rs`, which carries no tokio and compiles, so
the absent piece is the poller loop, and with the delay at zero nothing is ever scheduled for
it.

Two invariants the design rests on. One connection means no nested or overlapping
transactions, which the executor enforces with a flag on the connection handle rather than a
comment, so a violation fails on native CI instead of deadlocking a browser tab. Futures
resolve without a reactor here, so the executor is poll-once: any code that awaits a second
poll, including a `tokio::time::timeout` or a `Notify`, hangs the tab with no error.

### Shared-crate changes

Six crates change before the engine compiles for wasm at all. Today
`cargo check --target wasm32-unknown-unknown -p extenddb-engine` fails in `getrandom`, `mio`
and `uuid` without reaching ExtendDB code.

| Crate | Change |
|---|---|
| `extenddb-core` | `time` gains the `wasm-bindgen` feature, for a clock source |
| `extenddb-auth` | take `HeaderMap` from `http` instead of `axum::http`, on every target, which drops axum from the graph; the workspace resolves one `http` version, so no signature changes |
| `extenddb-cache` | a pass-through implementation with no `moka` and no refresh task |
| `extenddb-engine` | `tokio` becomes native-only, which touches one file: `import_export.rs` is its only user in the crate, and that file, plus the `ImportTable` and `ExportTableToPointInTime` arms that reach it, is gated off. The three `SystemTime::now` sites in `streams.rs` move to the `time` crate. `is_known_operation`'s 37-name `matches!` becomes a `const OPERATIONS: &[&str]` the predicate reads, so the allowlist test can assert against it |
| `extenddb-storage` | `extenddb-auth`, `tokio`, `tokio-util`, `bcrypt`, `rand` and `aes-gcm` become native-only, and the runtime-hooks and backend-registry surface goes with them. The hardest row: `spawn_workers` returns `Vec<tokio::task::JoinHandle<()>>` in a public trait |
| workspace `uuid` | gains the `js` feature, since CreateTable calls `Uuid::new_v4()` |

These land before anything else, and the check starts passing over `extenddb-engine` there. It
extends to `extenddb-sqlite-engine` once the crate split creates that crate. Afterwards a
native-only change reaching for an `sqlx` type or a tokio type inside the seam fails on the
change that introduces it.

The crate split needs a companion amendment to RFC-0002, permitting a backend to factor internal
crates beneath its own directory.

### v1 surface and order of work

v1 covers CreateTable, DeleteTable, DescribeTable, ListTables, PutItem, GetItem, UpdateItem,
DeleteItem, Query, Scan, BatchGetItem and BatchWriteItem.

Excluded: secondary indexes, transactions, stream reads, TTL, UpdateTable, backup and restore,
import and export, tags, the management and IAM APIs, and vector search.

DescribeEndpoints and DescribeLimits are answered without touching storage. The allowlist
admits them, and v1 promises nothing about them.

Rejecting before `dispatch` is what keeps this cheap. An excluded operation never reaches
storage, so no unsupported-error plumbing is needed anywhere below the shim.

The browser consumer carries a raw JSON console, an AWS CLI-style shell, an in-page AWS SDK v3
console, and a read-only data browser, all over the one entry point and one database. Hosting is
GitHub Pages, with a notices page covering the wasm dependency graph.

Order of work:

1. The shared-crate changes above, with the wasm target check turned on in CI.
2. The executor crate and the conversion of the engine half, which also binds the two
   interpolated `LIMIT` values so no statement is assembled by interpolation.
3. `extenddb-wasm`, the allowlist, and the playground.
4. Static hosting and the notices page.

## Unresolved questions

Excluding `update_table.rs` and `backup.rs` from the wasm build leaves
`TableEngine::update_table` and `BackupEngine` without bodies, and `TableEngine` cannot be
dropped because v1 needs four of its methods. Either both get wasm-only arms returning
`StorageError::Unsupported`, or the wasm build compiles the modules and relies on the
allowlist alone. The second needs no new code and no first producer of a variant nothing
produces today, and it is the one to take unless a module genuinely fails to build, which is
unmeasurable until the crate split lands.

OPFS persistence, a link that carries state, and a bundle size target are out of v1 and
unspecified.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
