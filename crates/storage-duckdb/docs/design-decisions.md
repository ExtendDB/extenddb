<!--
Copyright 2026 ExtendDB contributors
SPDX-License-Identifier: Apache-2.0
-->
# extenddb-storage-duckdb — Design Decisions

This memo records the DuckDB-specific decisions taken to reach behavioural
parity with the SQLite backend (`crates/storage-sqlite`), from which this crate
was ported, and through it with the reference PostgreSQL backend. Decisions the
SQLite backend already made and that carry over unchanged (the order-preserving
`N` sort-key TEXT encoding, the single logical database, the persistent
`gsi_pending` queue, synchronous LSI maintenance, `rowid`-based parallel scan
segments) are not repeated here; see that crate's `docs/design-decisions.md`.

---

## D1 — Async execution model

### Problem
`duckdb::Connection` is synchronous, `Send` but not `Sync`, and there is no
`sqlx` driver. The rest of the crate is async and written against `sqlx`'s
builder API (`query(...).bind(...).fetch_all(&pool)`), with ~600 call sites.

### Options
- **(A) Call DuckDB inline on the async thread.** Simplest; blocks a tokio
  worker for the duration of every statement. Point lookups are sub-millisecond
  but a `Scan` or a backfill batch is not, and blocking the executor stalls
  unrelated requests.
- **(B) `block_in_place`.** Panics on a current-thread runtime, which every
  `#[tokio::test]` in the crate uses.
- **(C) Move the connection onto a `spawn_blocking` thread per statement.**
  Each pooled connection lives in a `tokio::sync::Mutex<Option<Connection>>`
  slot; a statement takes the connection out, runs on the blocking pool, and
  puts it back. A transaction holds its slot for its whole lifetime so every
  statement in it runs on the same connection. If the blocking task panics the
  slot is left empty and re-cloned from the root connection on next use.

### Decision
**(C)**, wrapped in `src/db.rs` as a facade that reproduces the subset of the
`sqlx` API the crate uses (`query`, `query_as`, `query_scalar`, `raw_sql`,
`Pool::begin`, `Transaction::commit`, rollback on drop, `rows_affected`). Rows
are materialised into `Vec<duckdb::types::Value>` on the blocking side and
decoded into tuples or structs on the async side, so nothing borrowed from
DuckDB crosses a thread boundary. The port of the storage code is then mostly
`sqlx::` → `db::`.

---

## D2 — One database instance per process, per file

### Problem
DuckDB permits a single database instance per file per process and takes a
file lock. The SQLite backend opens *two* independent pools over the same file
at serve time (engine + catalog) and a third in the bootstrapper; a naive port
fails on the second `Connection::open`.

### Decision
`db::Pool` keeps a process-wide registry of root connections keyed by canonical
path. Every pool over a path is a set of `try_clone()`s of that path's root, so
they share one instance; the catalog pool is opened as a `sibling` of the engine
pool. `drop_databases` forgets the registry entry before deleting the file so
the lock is released. In-memory databases are **not** registered: each
`Pool::open(":memory:")` is a private database (what the unit tests expect), and
connections cloned from it all see the same data — which is why the ephemeral
mode can run a real read pool where SQLite had to pin one connection.

---

## D3 — Referential integrity without foreign keys

### Problem
DuckDB accepts `FOREIGN KEY` clauses but does not implement `ON DELETE CASCADE`,
and it rejects updates to rows that a foreign key references. The SQLite catalog
relies on cascades in eleven parent-delete paths (tables → indexes /
vector_indexes / stream_shards / stream_records; accounts → every IAM table;
users → tags / access keys / memberships; groups → memberships; roles → tags /
sessions; backups → items) and on FK violations to report `NotFound` in seven
child-insert paths.

### Options
- **(A) Keep the constraints, drop `ON DELETE CASCADE`.** Parent deletes then
  fail while children exist, and every update to `tables` (status flips, item
  counts) risks the referenced-row restriction. Rejected.
- **(B) Declare no foreign keys; enforce in code.** `src/referential.rs` holds
  one function per parent that deletes its children, called inside the parent's
  delete transaction, plus `ensure_*_exists` checks that produce the same
  `NotFound` errors the constraints did.

### Decision
**(B)**. The pool-based deletes (`delete_user`, `delete_group`, `delete_role`,
`delete_account`) become single transactions via `delete_with_children`. The
existence checks in autocommit paths are not atomic with the insert that follows;
a lost race surfaces as `NotFound` from a later read instead of from a
constraint, which is the same outcome the callers already handle.

---

## D4 — Type widths and dialect

- `INTEGER` → `BIGINT` throughout (DuckDB `INTEGER` is 32-bit; SQLite's is
  64-bit, and `table_size_bytes` alone would overflow it). `REAL` → `DOUBLE` for
  the same reason (DuckDB `REAL` is `FLOAT`).
- Booleans stay 0/1 integers in the catalog; a Rust `bool` parameter binds as
  `BIGINT` so `col = ?` compares integer to integer.
- `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGINT PRIMARY KEY DEFAULT
  nextval('gsi_pending_seq')`.
- Timestamps: `strftime('%Y-%m-%dT%H:%M:%fZ','now')` →
  `strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')`. Without the ICU
  extension a `TIMESTAMPTZ` casts to `TIMESTAMP` as UTC, which is the stored
  format. `strftime('%s','now')` → `epoch(now())`.
- `sqlite_master` → `duckdb_tables()`; `GLOB` is supported natively.
- Partial indexes (`CREATE INDEX ... WHERE`) are not supported; the two the
  SQLite schema used become full indexes.
- `BEGIN IMMEDIATE` → `BEGIN TRANSACTION`. There is no reserved-lock concept;
  the engine `write_lock` already serializes writers, and DuckDB's optimistic
  `Conflict on update` error is classified `Transient` by `map_db_err` as a
  backstop.
- The TTL sweep's `json_extract` → `json_extract_string` + `TRY_CAST` (DuckDB's
  `json_extract` returns a quoted JSON scalar, and the sweep must not error on a
  non-numeric TTL attribute).
- `rowid` starts at 0 in DuckDB (1 in SQLite); the vector-backfill cursor
  starts at `-1`.
- Numbered parameters are `$1`, not `?1`.

---

## D5 — Error classification

`is_unique_violation` matches DuckDB's `Duplicate key ... violates primary key
constraint` / `violates unique constraint` messages. A failed statement aborts
a DuckDB transaction (unlike SQLite, where the transaction stays usable); every
path in the crate that catches a constraint error inside a transaction already
returns the error and lets the transaction drop, so the rollback-on-drop in
`db::Transaction` is what keeps pooled connections clean.

---

## D6 — Out-of-process tooling and the file lock

DuckDB takes a process-exclusive lock on a read-write database file. The
SQLite backend lets `extenddb settings`, `catalog-check`, and `verify` open the
file alongside a running server; here they must run while the server is stopped.
No workaround was attempted: a read-only open conflicts with a writer too, and
routing the CLI through the server's management API is a cross-backend change
outside this crate. Documented in the README; the integration module that
depends on it (`test_gsi_async`) is excluded from the DuckDB CI job.

## D7 — TTL sweep without an expression index

DuckDB rejects `CREATE INDEX ... (json_extract_string(...))` ("Cannot use
json_extract_string in this context"). Left as an error, the engine retries the
index forever and never marks `ttl_index_ready`, so nothing ever expires. The
backend therefore skips the index and flips the flag; the sweep is a filtered
scan bounded by `LIMIT`, which at the batch sizes the worker uses is acceptable.
