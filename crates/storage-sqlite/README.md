# extenddb-storage-sqlite

SQLite storage backend for [ExtendDB](https://github.com/ExtendDB/extenddb), an
in-tree workspace crate selected by Cargo feature.

## Design

Single-file (or in-memory) storage using SQLite via `sqlx`. Targets:

- Local development without PostgreSQL
- CI / integration tests (especially the ephemeral in-memory mode)
- Single-node, embedded, and edge deployments

Key differences from the PostgreSQL backend:

- One connection pool for all operations (catalog and data share a single SQLite
  database rather than separate catalog/data databases)
- WAL mode for concurrent reads; writes are serialized by the engine
- GSI propagation via a persistent `gsi_pending` queue with a configurable delay
  (a delay of 0 applies updates synchronously on the write path); LSI
  propagation is synchronous
- OR-expansion for pagination (SQLite lacks row-value tuple comparison)

It shares the PostgreSQL backend's catalog version gate: the server refuses to
start unless its compiled `CATALOG_VERSION` matches the stored `catalog_version`.

## Building

The backend is compiled into the `extenddb` binary via feature flags (Postgres
remains the default):

```bash
# Postgres (default) plus the file-backed SQLite backend
cargo build -p extenddb --features sqlite

# SQLite only, no Postgres compiled in
cargo build -p extenddb --no-default-features --features sqlite

# SQLite in zero-config, ephemeral in-memory mode
cargo build -p extenddb --no-default-features --features sqlite-memory
```

## Configuration

```toml
[storage]
backend = "sqlite"

[storage.sqlite]
# Database file path, or ":memory:" for an ephemeral in-memory database.
path = "extenddb.sqlite"
# Read connection pool size (writes are serialized regardless).
pool_size = 10
```

### In-memory mode

`path = ":memory:"` selects an ephemeral database that bootstraps on `serve`
(no `init`, no file on disk). The `memory` crate feature (exposed by the binary
as `sqlite-memory`) makes `:memory:` the compiled-in default path, so a binary
built with that feature needs no path configured.

## Developer mode

A build-time profile that makes ExtendDB a drop-in replacement for DynamoDB
Local: plain HTTP on loopback, open authorization, and a seeded credential — with
real SigV4 verification still in force. It is enabled by the `dev-mode` feature
and, by a compile-time guard, **can never be built with the Postgres backend**.

```bash
# Zero-config ephemeral server (in-memory, plain HTTP, dev credential)
cargo build -p extenddb --no-default-features --features sqlite-memory,dev-mode
extenddb serve --config extenddb.toml   # bootstraps on serve; no init
```

Point any SDK at it with the well-known default credential:

```python
boto3.client(
    "dynamodb",
    endpoint_url="http://127.0.0.1:18443",
    region_name="us-east-1",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)
```

The server adopts the credential from its own `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` if set (so a CI job that already exports those works with
no change), otherwise it seeds the AWS example credential shown above. Either way
SigV4 is verified — a request signed with a different secret is rejected with
`InvalidSignatureException`. Dev mode binds to loopback only.

## Conformance

Per [RFC-0002](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs), this
backend implements the full storage trait surface.

Mandatory traits (required for acceptance):

- `TableEngine`, `DataEngine`
- `ManagementStore`, `AdminStore`, `Bootstrapper`

Optional traits (all implemented):

- `MetadataEngine`, `StreamEngine`, `WorkerStore`
- `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`,
  `BackupEngine`

Conformance is validated by the shared ExtendDB integration suite run against a
SQLite-served instance; per-trait results are tracked in CI.

## License

Apache License 2.0 — see the workspace [LICENSE](../../LICENSE).
