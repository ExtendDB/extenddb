# extenddb-storage-duckdb

DuckDB storage backend for [ExtendDB](https://github.com/ExtendDB/extenddb), an
in-tree workspace crate selected by Cargo feature.

## Design

Single-file (or in-memory) storage using the embedded [DuckDB](https://duckdb.org)
engine via the `duckdb` crate (statically linked; no server, no external
dependency at runtime). Targets:

- Local development without PostgreSQL
- CI / integration tests (especially the ephemeral in-memory mode)
- Single-node, embedded, and edge deployments
- Anyone who has ever wanted to run `SELECT ... GROUP BY` directly against the
  file their DynamoDB-compatible database lives in

The crate is a port of `extenddb-storage-sqlite` — same catalog schema, same
per-table data layout, same order-preserving `N` sort-key encoding, same
persistent `gsi_pending` queue — with the storage engine swapped. The differences
that matter:

- **No `sqlx` driver exists for DuckDB**, so `src/db.rs` provides a small
  `sqlx`-shaped async facade over the synchronous `duckdb` crate: a connection
  pool, positional `query` / `query_as` / `query_scalar` builders, and an
  explicit-commit / rollback-on-drop `Transaction`. Every statement runs on a
  `spawn_blocking` thread; the connection is moved there and back.
- **In-memory databases are shared across the pool.** All connections are
  `try_clone()`s of one root connection onto the same database instance, so
  `:memory:` gets a real read pool instead of SQLite's single pinned connection.
- **No foreign keys.** DuckDB cannot cascade deletes, so the catalog declares
  none; `src/referential.rs` removes children explicitly inside the parent's
  delete transaction and checks parents exist before child inserts.
- **MVCC inside the process.** Readers never block on the writer. Writers are
  still serialized in-process by the engine's `write_lock`, which is what makes
  condition-check-then-write atomic; DuckDB's optimistic conflict detection is a
  backstop, not the mechanism. Across processes there is no sharing at all (see
  Known limits).
- **64-bit integers everywhere.** DuckDB's `INTEGER` is 32-bit, so every catalog
  integer column is `BIGINT`, and `REAL` (32-bit in DuckDB) is `DOUBLE`.

## Building

The backend is compiled into the `extenddb` binary via feature flags (Postgres
remains the default):

```bash
# DuckDB only, no Postgres compiled in
cargo build -p extenddb --no-default-features --features duckdb

# DuckDB in zero-config, ephemeral in-memory mode
cargo build -p extenddb --no-default-features --features duckdb-memory
```

The first build compiles DuckDB from source (`libduckdb-sys` with the `bundled`
feature). Budget several minutes and a cup of something; it is cached afterwards.

## Configuration

```toml
[storage]
backend = "duckdb"

[storage.duckdb]
# Database file path, or ":memory:" for an ephemeral in-memory database.
path = "extenddb.duckdb"
# Connection pool size (writes are serialized regardless).
pool_size = 10
```

`extenddb init --backend duckdb --duckdb-path <path>` writes the path into the
generated config file.

### In-memory mode

`path = ":memory:"` selects an ephemeral database that bootstraps on `serve`
(no `init`, no file on disk). The `memory` crate feature (exposed by the binary
as `duckdb-memory`) makes `:memory:` the compiled-in default path.

## Developer mode

`dev-mode` (plain HTTP on loopback, open authorization, seeded credential, SigV4
still verified) builds with this backend exactly as it does with SQLite:

```bash
cargo build -p extenddb --no-default-features --features duckdb-memory,dev-mode
extenddb serve --config extenddb.toml   # bootstraps on serve; no init
```

## Conformance

Per [RFC-0002](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs), this
backend implements the full storage trait surface.

Mandatory traits:

- `TableEngine`, `DataEngine`
- `ManagementStore`, `AdminStore`, `Bootstrapper`

Optional traits (all implemented):

- `MetadataEngine`, `StreamEngine`, `WorkerStore`
- `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`,
  `BackupEngine`

Conformance is validated by the shared ExtendDB integration suite run against a
DuckDB-served instance (`run-integration-duckdb` in CI).

## Known limits

- **One process at a time.** DuckDB takes a process-exclusive lock on the database
  file. While `extenddb serve` is running, no second process can open the same
  file, so out-of-band tooling that connects directly (`extenddb settings get|set`,
  `catalog-check`, `verify`) must run while the server is stopped or against an
  `:memory:` server not at all. Everything that goes through the server's own
  API is unaffected.
- **No TTL expression index.** DuckDB does not allow indexes over extension
  functions, so the TTL sweep is a filtered scan of `item_data` rather than an
  indexed lookup.
- **Cold builds are slow.** DuckDB is compiled from source the first time.

## License

Apache License 2.0 — see the workspace [LICENSE](../../LICENSE).
