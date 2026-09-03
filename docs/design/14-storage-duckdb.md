# Design: DuckDB Storage Backend

## 1. Overview

The DuckDB backend (`extenddb-storage-duckdb`) implements the same trait surface as
`extenddb-storage-postgres` and `extenddb-storage-sqlite`: the six engine traits
(`TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `BackupEngine`,
`WorkerStore`) and the catalog traits (`ManagementStore`, `AdminStore`,
`SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`).

**Driver:** `duckdb` (the official Rust binding, synchronous, statically linked via
`libduckdb-sys` with the `bundled` and `json` features). No `sqlx` driver exists for
DuckDB; the crate supplies its own thin async facade (§5.1).

**Minimum DuckDB version:** whatever `libduckdb-sys` bundles (1.5.x at the time of
writing). There is no external server and no runtime dependency.

**Lineage.** The crate is a port of the SQLite backend: same catalog schema, same
per-table data layout, same order-preserving `N` sort-key encoding, same persistent
`gsi_pending` queue, same synchronous LSI / delayed GSI model, same in-memory and
`dev-mode` contracts. This document covers only what changed. For everything else
see `crates/storage-sqlite/docs/design-decisions.md` and the crate-local
`crates/storage-duckdb/docs/design-decisions.md`.

## 2. Database Layout

One DuckDB database — a single file, or `:memory:` — holds the catalog and every
per-table data table. Data tables are `_ddb_<table_id>` (items), `_ddb_<index_id>`
(GSI / LSI projections), and `_vidx_<table_id>_<index_id>` (one row per vector).

| Object                | Purpose                                              |
|-----------------------|------------------------------------------------------|
| `extenddb.duckdb`     | Catalog tables, IAM, settings, metrics, backups, stream shards/records, `gsi_pending`, and all data tables |
| `extenddb.duckdb.wal` | DuckDB's write-ahead log; created and checkpointed by the engine |

Both files are restricted to owner read/write on Unix, since the database stores the
AES key that protects access-key secrets.

## 3. Schema

Identical to the SQLite catalog (`crates/storage-sqlite/src/schema.rs`) with the
following type and syntax substitutions:

| SQLite                                        | DuckDB                                                  | Why |
|-----------------------------------------------|---------------------------------------------------------|-----|
| `INTEGER`                                     | `BIGINT`                                                | DuckDB `INTEGER` is 32-bit; SQLite's is 64-bit |
| `REAL`                                        | `DOUBLE`                                                | DuckDB `REAL` is a 32-bit float |
| `INTEGER PRIMARY KEY AUTOINCREMENT`           | `BIGINT PRIMARY KEY DEFAULT nextval('gsi_pending_seq')` | No `AUTOINCREMENT`; sequences instead |
| `strftime('%Y-%m-%dT%H:%M:%fZ','now')`        | `strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')`   | Different `strftime` signature; `%g` is milliseconds |
| `CAST(strftime('%s','now') AS INTEGER)`       | `CAST(epoch(now()) AS BIGINT)`                          | |
| `9e999` / `-9e999`                            | `'Infinity'::DOUBLE` / `'-Infinity'::DOUBLE`            | |
| `CREATE INDEX ... WHERE <pred>`               | `CREATE INDEX ...`                                      | Partial indexes are not implemented |
| `REFERENCES ... ON DELETE CASCADE`            | *(removed)*                                             | See §5.3 |
| `sqlite_master`                               | `duckdb_tables()`                                       | |
| `PRAGMA journal_mode / foreign_keys / ...`    | *(none)*                                                | No connection pragmas needed |
| `BEGIN IMMEDIATE`                             | `BEGIN TRANSACTION`                                     | No reserved-lock concept |
| `?1`, `?2` numbered parameters                | `$1`, `$2`                                              | |
| `json_extract(item_data, p)`                  | `json_extract_string(item_data, p)` + `TRY_CAST`        | DuckDB's `json_extract` returns a quoted JSON scalar |

Booleans remain 0/1 integer columns, and a Rust `bool` binds as `BIGINT` so
`col = ?` compares like-for-like; DuckDB does not coerce a `BOOLEAN` parameter
against an integer column, nor an integer parameter against a `TEXT` column.

## 4. Data Path

Unchanged from SQLite except:

- **Parallel scan** still partitions on `rowid % total_segments`. DuckDB exposes
  `rowid` on every base table.
- **Vector backfill cursor** is `rowid > ?` starting from `-1`, because DuckDB
  rowids start at 0 where SQLite's start at 1.
- **Vector search** fetches a partition's rows in one call rather than streaming
  them (the facade materialises result sets on the blocking side).

## 5. Key Design Decisions

### 5.1 Async facade over a synchronous driver

`src/db.rs` reproduces the subset of the `sqlx` builder API the storage code uses:
`query` / `query_as` / `query_scalar` / `raw_sql` with positional `.bind()`,
`fetch_all` / `fetch_one` / `fetch_optional` / `execute`, `Pool::begin`, and a
`Transaction` that commits explicitly and rolls back on drop.

`duckdb::Connection` is `Send` but not `Sync`, and every call blocks. Each pooled
connection lives in a `tokio::sync::Mutex<Option<Connection>>` slot. To run a
statement the connection is taken out of its slot, moved onto a `spawn_blocking`
thread for the call, and put back. A transaction holds its slot for its lifetime so
every statement in it runs on the same connection. Rows come back as
`Vec<duckdb::types::Value>` and are decoded into tuples or structs on the async side,
so nothing borrowed from DuckDB crosses a thread boundary.

Decoding is permissive where DuckDB's result types differ from SQLite's: `SUM` over
`BIGINT` is `HUGEINT`, `EXISTS(...)` is `BOOLEAN`, `PRAGMA table_info` reports a
boolean `pk` flag rather than a position. Every integer variant decodes into `i64`;
`Boolean` decodes into `i64` and `bool`.

### 5.2 One database instance per file per process

DuckDB permits a single database instance per file per process. The SQLite backend
opens independent pools over the same file for the engine, the catalog store, and
the bootstrapper; here every pool over a path is a set of `try_clone()`s of one
registered root connection, and the catalog pool is opened as a `sibling` of the
engine pool. `destroy` forgets the registry entry before deleting the file so the
lock is released.

In-memory databases are not registered: each `Pool::open(":memory:")` is private
(what the unit tests expect), but connections cloned from it share one database.
The ephemeral mode therefore runs a real read pool where SQLite had to pin a single
connection.

### 5.3 Referential integrity in code

DuckDB parses `FOREIGN KEY` but rejects `ON DELETE CASCADE` and refuses updates to
rows that a foreign key references. The catalog declares no foreign keys.
`src/referential.rs` supplies:

- one child-delete function per parent (tables → indexes / vector_indexes /
  stream_shards / stream_records; accounts → every IAM table; users → tags / access
  keys / memberships; groups → memberships; roles → tags / sessions), called inside
  the parent's delete transaction;
- `ensure_{account,user,group,role}_exists` checks in the seven child-insert paths
  that previously relied on a foreign-key violation to report `NotFound`.

The pool-based deletes (`delete_user`, `delete_group`, `delete_role`,
`delete_account`) become single transactions. Existence checks in autocommit paths
are not atomic with the insert that follows; a lost race surfaces as `NotFound` from
a later read rather than from a constraint, which is the outcome callers already
handle.

### 5.4 Concurrency and isolation

Writers are serialized in-process by the engine `write_lock`, exactly as in SQLite;
that is what makes condition-check-then-write atomic. DuckDB adds MVCC on top, so
readers never block on the writer, and two connections updating the same row
produce `TransactionContext Error: Conflict on update!` on the second — classified
`Transient` by `map_db_err` as a backstop, not relied upon as the mechanism.

A failed statement aborts a DuckDB transaction; every subsequent statement on it
fails until `ROLLBACK`. Every path in the crate that catches a constraint error
inside a transaction already returns the error and drops the transaction, and
`db::Transaction` rolls back on drop, which is what keeps pooled connections clean.

### 5.5 GSI queue without savepoints

DuckDB has no `SAVEPOINT`, which the SQLite worker uses to isolate a poison row
inside a batch transaction. The DuckDB worker claims and applies each `gsi_pending`
row in its own transaction: `DELETE ... WHERE id = ?` (zero rows → already taken,
skip), apply, `COMMIT`. A transient error rolls that row back and stops the pass;
a poison row rolls back, is deleted in a statement of its own, and the pass
continues. At-least-once delivery, per-key FIFO (`ORDER BY id`), and poison
isolation are preserved.

### 5.6 What was not changed

- The `N` sort-key encoding (order-preserving TEXT). DuckDB's default collation is
  binary, so the encoding orders correctly with no custom collation.
- The persistent `gsi_pending` queue and the per-index propagation delay.
- The catalog version gate (`catalog_version` setting).
- The `dev-mode` contract and the `memory` feature.

## 6. Crate Structure

```
crates/storage-duckdb/
├── src/
│   ├── db.rs               # async facade over duckdb (pool, queries, transactions)
│   ├── referential.rs      # explicit cascades and existence checks
│   ├── duckdb_util.rs      # path normalisation, timestamps, error classification
│   ├── schema.rs           # catalog DDL (DuckDB dialect)
│   └── ...                 # everything else mirrors storage-sqlite
└── docs/design-decisions.md
```

## 7. Configuration

```toml
[storage]
backend = "duckdb"

[storage.duckdb]
path = "extenddb.duckdb"   # or ":memory:"
pool_size = 10
```

`extenddb init --backend duckdb --duckdb-path <path>` writes the path into the
generated config. `EXTENDDB__STORAGE__DUCKDB__PATH` overrides it at serve time.

Binary features: `duckdb` (file-backed default) and `duckdb-memory` (`:memory:` as
the compiled-in default). Both are accepted by the `dev-mode` gate.

## 8. Testing

- Crate unit tests (`cargo test -p extenddb-storage-duckdb`): the SQLite suite,
  retargeted. Plan-shape assertions (`EXPLAIN QUERY PLAN`) became index-definition
  assertions (`duckdb_indexes()`), because DuckDB's optimizer picks a sequential
  scan below a row-count threshold regardless of indexes.
- `run-integration-duckdb` in `.github/workflows/integration.yml` runs the shared
  Python conformance suite against a DuckDB-served instance, mirroring the SQLite
  job.

## 9. Known Limits

- **Process-exclusive file lock.** DuckDB permits one process per database file
  (read-write). While the server runs, `extenddb settings`, `catalog-check`, and
  `verify` cannot open the file from a second process; run them with the server
  stopped. The `test_gsi_async` integration module, which drives the server
  out-of-band through `settings set`, is excluded from the DuckDB CI job for this
  reason; the propagation logic it covers has crate-level unit tests.
- **No TTL expression index.** DuckDB rejects indexes over extension functions
  (`json_extract_string`), so `enable_ttl` records the table as ready without
  building one and the sweep is a filtered scan.
- Compiling DuckDB from source adds several minutes to a cold build.
- DuckDB is optimised for analytical workloads; single-row point writes go through
  the same columnar storage and are slower than SQLite's B-tree at high write
  rates. The backend is correct before it is fast.
- `EXPLAIN`-driven index assertions are not meaningful at test-table sizes.
