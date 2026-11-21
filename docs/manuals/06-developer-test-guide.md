---
description: "Building ExtendDB, running the unit and integration test suites, and the conventions the code follows."
---

# Developer and test guide

> See [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/docs/NOTICE.md) for important disclaimers.

## Project Structure

```
extenddb/
├── Cargo.toml                 Workspace root
├── extenddb.sample.toml           Configuration template
├── crates/
│   ├── core/                  Pure sync Rust: types, expressions, validation, errors
│   ├── engine/                Async operation handlers
│   ├── storage/               Storage trait definitions
│   ├── storage-postgres/      PostgreSQL backend
│   ├── auth/                  Authentication and authorization
│   ├── server/                HTTP server, management API, web console
│   └── bin/                   CLI entry point
├── tests/                     Python integration tests
├── devtools/                  Development scripts
├── discussions/               Decision records and agent communication
├── docs/
│   ├── design/                Design documents and requirements
│   ├── adr/                   Architecture decision records
│   ├── manuals/               PDF manual sources (Markdown)
│   └── *.md                   Operational documentation
└── external-suites.toml       External test suite registry
```

## Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Check without building
cargo check

# Run clippy lints
cargo clippy -- -W clippy::pedantic

# Format code
cargo fmt
```

Both debug and release builds must pass before any phase exit.

## Rust Conventions

### Crate Boundaries

- `core` is pure sync Rust: no async runtime, no database drivers, no HTTP framework
- `storage` defines traits and backend-agnostic utilities (ARN, key parsing)
- `engine` contains async operation handlers that call storage traits
- `storage-postgres` implements storage traits for PostgreSQL
- `auth` handles authentication and authorization
- `server` handles HTTP concerns
- `bin` wires everything together

### Error Handling

- Use `Result<T, E>` for recoverable errors
- `core` defines `DynamoDbError` for all DynamoDB-fidelity errors
- `storage` defines `StorageError` for storage-layer errors
- Use `thiserror` for library error types, `anyhow` for the binary
- No `.unwrap()` in production code: use `?`, `.expect("reason")`, or handle explicitly

### Ownership

- Prefer borrowing (`&T`, `&mut T`) over cloning
- Use `Arc<str>` for shared strings (e.g., region, account_id)
- Use `Cow<'_, str>` when a function might or might not allocate

### Style

- `#[allow(dead_code)]` requires a `TODO(phase-N)` comment
- All public APIs require `///` doc comments
- All crate roots require `//!` module-level documentation
- TODOs use category tags: `TODO(fidelity)`, `TODO(phase-N)`, `TODO(cleanup)`, `TODO(security)`

### Copyright Header

Every source file must carry:

```rust
// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
```

## Adding a New DynamoDB Operation

1. Add the operation handler in `crates/engine/src/<operation>.rs`
2. Register it in the `dispatch` function in `crates/engine/src/lib.rs`
3. Add any new types to `crates/core/src/types/`
4. Add storage trait methods to `crates/storage/src/lib.rs` if needed
5. Implement the storage methods in `crates/storage-postgres/src/`
6. Add Python integration tests in `tests/`
7. Update the [Usage Guide](03-usage-guide.md)

## Testing

### Python Environment Setup

Python integration tests require a virtual environment with dependencies installed:

```bash
python3 -m venv ~/venvs/extenddb-venv
source ~/venvs/extenddb-venv/bin/activate
pip install -r requirements.txt
```

Activate the virtual environment before running any test suites:

```bash
source ~/venvs/extenddb-venv/bin/activate
```

### Test Runner Script

All tests should be run via `devtools/run-tests`, which handles credential provisioning, runtime configuration, and artifact collection.

**Usage:**

```bash
# Run all test suites against local ExtendDB
devtools/run-tests --extenddb --all

# Run specific suites
devtools/run-tests --extenddb --rust
devtools/run-tests --extenddb --pytest
devtools/run-tests --extenddb --external
devtools/run-tests --extenddb --comprehensive
devtools/run-tests --extenddb --rust-integration

# Run against Amazon DynamoDB
devtools/run-tests --real-dynamodb --pytest

# Use release build
devtools/run-tests --extenddb --rust --release

# Filter tests
devtools/run-tests --extenddb --pytest --filter test_put_item
```

**Prerequisites for integration tests (pytest, external, comprehensive, rust-integration):**

1. A running ExtendDB server: `./target/debug/extenddb serve --config extenddb.toml`
2. `EXTENDDB_TEST_ENDPOINT=https://localhost:18443`
3. `EXTENDDB_ADMIN_PASSWORD=<password from extenddb init>`
4. `EXTENDDB_TEST_BACKUP_STORE=<filesystem|s3|none>`, required for `--pytest`
   (and so for `--all`) and read by the Rust integration suite's backup
   tests: where the server under test writes its backups. `filesystem` for a
   server with a `[backup.store]` filesystem root, which a file-backed SQLite
   server without the section has by default (`backups/` beside its database
   file); `s3` for an S3 store; `none` for a server without a store
   (`kind = "disabled"`, or PostgreSQL, MongoDB and Cassandra without the
   section). With a configured store CreateBackup is asynchronous and
   RestoreTableFromBackup of its backups is the store-backed restore job
   (the response is `CREATING` and the table is loaded in the background),
   so the backup tests derive their expected outcome from the variable and
   fail rather than guess when it is unset; `run-tests --pytest` refuses before any test runs with
   `error: --extenddb --pytest requires EXTENDDB_TEST_BACKUP_STORE to be set.`
   The suites check the value against the server's `/health` `backup_store`
   component, so a wrong value fails too.

**The backup store and job suites** (`tests/test_backup_store_config.py`
and `tests/test_backup_job.py`) start servers of their own, so `run-tests`
excludes them and they run by name:
`python3 -m pytest tests/test_backup_store_config.py tests/test_backup_job.py -v`.
`EXTENDDB_BINARY` names the release build under test and decides which
deployment the cells use: a SQLite build (the default root beside the
database file) or a PostgreSQL build, which also needs
`EXTENDDB_TEST_PG_CONNECTION_STRING` and
`EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING`. The S3 cells (the store
configuration suite's S3 rows and the job suite's store-outage cell) need a
moto server at `EXTENDDB_TEST_S3_ENDPOINT` (`http://127.0.0.1:<port>`);
without it they skip. They set the credentials moto accepts
(`testing`/`testing`, `us-east-1`) themselves, in the server's environment
and on their own client, so nothing else needs to be exported. The job suite's restore cells size their
backup from `EXTENDDB_TEST_RESTORE_ITEMS` (default 10,000; at least two data
files are needed), and the multi-part-key cell enables
`[limits] allow_multipart_table_keys` on its own deployment. CI runs both
suites on the PostgreSQL job and the two SQLite jobs; the PostgreSQL job
and the SQLite S3 job start moto, so the outage cell runs there and skips on
the plain SQLite job (`.github/workflows/integration.yml`).

**The MongoDB backup cells** of `tests/test_mongodb_backup_restore.py` that
read the store's root and the server's log skip unless
`EXTENDDB_TEST_BACKUP_ROOT` (the server's filesystem store root) and
`EXTENDDB_TEST_SERVER_LOG` (the file the server logs to) are set;
`devtools/run-mongodb-tests` serves a filesystem store and exports both
(and `EXTENDDB_TEST_BACKUP_STORE=filesystem`), so run them through it:
`devtools/run-mongodb-tests -- --pytest --filter test_mongodb_backup_restore`.
The suite's legacy LSI fallback cell edits an in-catalog backup and runs
only on a server without a store:
`devtools/run-mongodb-tests --backup-store none -- --pytest --filter test_mongodb_backup_restore`
(the runner then writes no `[backup.store]` and exports
`EXTENDDB_TEST_BACKUP_STORE=none`; the store-backed cells skip under it).
The CI jobs run the suite with `filesystem`.

**The cross-backend restore matrix** is `devtools/backup-restore-matrix`:
one cell per invocation (`--source <sqlite|postgres|mongodb> --target
<sqlite|postgres|mongodb|cassandra>`, plus `--duplicate-key`, `--vector` or
`--adopt-store`), each starting a source and a target server from the
per-backend release binaries (`--bin-<backend>`, defaulting to
`target/rel-<backend>/release/extenddb`) against PostgreSQL at
`127.0.0.1:55432`, MongoDB at `127.0.0.1:27117` and Cassandra at
`127.0.0.1:9042`; its MongoDB and Cassandra cells need `pymongo` and
`cassandra-driver` (`requirements-matrix.txt`). A cell copies the source's
backup prefix under the target's root and adopts it with `extenddb backup
sync` on the target (with the target's server stopped when it is SQLite);
`--adopt-store` points the target's store at the source's root instead,
adopts the root with `backup sync --adopt-store` and checks that the
source's server then reports the store owned by another catalog. The
nightly workflow `.github/workflows/matrix-nightly.yml` builds the four
binaries, starts the three services and runs the twelve plain cells, the
four duplicate-key cells, the eight vector cells and two `--adopt-store`
cells (SQLite to PostgreSQL and back); `workflow_dispatch` runs it on
demand. The same workflow runs the restore kill points of every backend in
four jobs (`restore-kill-points-<backend>`), each building its backend's
binary with `extenddb-app/backup-test-hooks` and starting its backend as
the matrix job does, through `devtools/backup-kill-points.py restore
<backend> <point> --bin <binary> --work <dir>` (the Cassandra job builds a
plain SQLite binary too, the `--source-bin` whose backup the Cassandra
target restores); every point ends ACTIVE with the exact base and GSI
counts, except the tampered-file point, whose target is removed.

The `run-tests` script automatically:
- Performs health check on the target endpoint
- Provisions test credentials via `devtools/provision-test-credentials`
- Creates a Java truststore for external tests (self-signed TLS cert)
- Sets `control_plane_delay_seconds` to 0.05 for fast test cycles
- Sets `index_propagation_delay_ms` to 0 for immediate GSI updates
- Enables throttling for production-like behavior
- Sets `allow_credential_import` to `true` for the access key import tests
- Configures import/export paths for file operation tests
- Extracts and exports `EXTENDDB_TEST_PG_CONNECTION_STRING` for CLI lifecycle tests

**Test artifacts** are written to `discussions/` with the code repo's HEAD commit hash:
- `test-rust-<hash>.txt`: Rust unit test output
- `test-rust-integration-<hash>.txt`: Rust integration test output
- `test-pytest-<hash>.txt`: Pytest output
- `test-external-<hash>.json`: External test results (structured)
- `test-external-<hash>.txt`: External test output (verbose)
- `test-comprehensive-<hash>.txt`: Comprehensive test output
- `test-cli-<hash>.txt`: CLI lifecycle test output

### Test Suites

| Suite | Count           | Description |
|-------|-----------------|-------------|
| Rust unit tests | 317             | Expression engine, type system, validation, error codes |
| Pytest (standard) | 573 + 2 skipped | DynamoDB API tests via boto3 |
| Comprehensive (Python) | 296             | Clean-room gap analysis tests |
| External (Java) | 346             | Third-party functional test suite |
| CLI lifecycle | 9               | Binary lifecycle tests (separate, requires `EXTENDDB_TEST_PG_CONNECTION_STRING`) |

### Python Integration Tests

The primary test suite is in Python, testing ExtendDB through the same AWS SDK interface that customers use.

```bash
# Via run-tests script (recommended; EXTENDDB_TEST_BACKUP_STORE as above)
EXTENDDB_TEST_BACKUP_STORE=filesystem devtools/run-tests --extenddb --pytest

# Run in parallel (uses 1/3 of CPU cores by default)
devtools/run-tests --extenddb --pytest --parallel

# Run in parallel with a specific worker count
devtools/run-tests --extenddb --pytest --parallel=4

# Direct execution (for quick iteration during development)
# The access key import tests need import enabled. devtools/run-tests sets it.
./target/debug/extenddb settings --config extenddb.toml set allow_credential_import true
python3 -m pytest tests/ -v
python3 -m pytest tests/test_put_item.py -v
python3 -m pytest tests/test_put_item.py::test_put_item_basic -v
```

The `--parallel` flag enables pytest-xdist with `--dist loadfile`, which
distributes entire test files across workers. This keeps module/class-scoped
fixtures on a single worker while running independent files concurrently. The
default worker count (1/3 of CPU cores, minimum 2) leaves headroom for the
ExtendDB server and its Postgres backend.

### Comprehensive Tests

A separate Python test suite in `tests/python/` provides broader coverage from a clean-room gap analysis:

```bash
devtools/run-tests --extenddb --comprehensive
```

### External Test Suites

External test suites (Java) are registered in `external-suites.toml` and run via:

```bash
# Via run-tests script (recommended)
devtools/run-tests --extenddb --external

# Direct execution (for quick iteration during development)
python3 devtools/run-external-tests --verbose
python3 devtools/run-external-tests --suite "Suite Name"
python3 devtools/run-external-tests --dry-run
```

The external test runner parses Maven surefire XML reports as a fallback when `mvn -q` suppresses stdout summary lines.

### Rust Unit Tests

```bash
# Via run-tests script (recommended)
devtools/run-tests --extenddb --rust

# Direct execution (for quick iteration during development)
cargo test
cargo test -p extenddb-core
cargo test -p extenddb-engine
```

Unit tests in `core` require no database and no async runtime.

### Test Guidelines

- Every failure path should have a test
- Avoid redundant tests covering the same failure path
- Test error messages exactly: they are part of the API contract (tenet 4)
- Use `pytest.mark.parametrize` for multiple test cases
- Mock external dependencies, not your own code

## Building Documentation

PDF documentation is built from Markdown sources in `docs/manuals/`. Activate the Python virtual environment first (all dependencies are in `requirements.txt`):

```bash
source ~/venvs/extenddb-venv/bin/activate

# Build all PDFs
python3 docs/build-docs.py

# List available documents
python3 docs/build-docs.py --list

# Build a specific document
python3 docs/build-docs.py --doc 4
```

If you haven't set up the virtual environment yet, see the [Python Environment Setup](https://github.com/ExtendDB/extenddb/blob/main/README.md#python-environment-setup) section in the README.

Output goes to `pdfs/` (gitignored).

## Project Tenets

1. **Fidelity over features**: match the DynamoDB service behavior exactly
2. **Fidelity is the default**: standard operations behave identically to the DynamoDB service
3. **Fast feedback loops**: startup time, query latency, and test cycle speed matter
4. **Errors are contracts**: error responses are part of the API surface
5. **Rust-safe by default**: rely on the type system and ownership model
6. **Tests live outside the core**: test suites are clients
7. **Readable over clever**: straightforward implementations over compact ones
8. **Console is an API client**: every piece of data and every action visible in the web console is available through the management API; the console never queries the database directly

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
