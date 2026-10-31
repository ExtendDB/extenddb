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

### Local databases for the test suite

The integration suites run against a server backed by a local PostgreSQL or MongoDB. Installing either database is covered in [Installing from source](../install-from-source.md); this section records the connection details and configuration the test setup assumes.

#### PostgreSQL

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `extenddb` |
| User | `extenddb` |
| Password | `extenddb-local-dev` |
| Connection string | `postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb` |
| Admin user | the OS user that ran `initdb` (trusted via local socket) |

The paths below are the Amazon Linux 2 setup from the install page (PostgreSQL 15 from the PGDG repository, data directory `~/pgdata`, running as the current user). If `initdb` fails there, the current user may need to be in the `postgres` group; check its log.

```bash
export PATH=/usr/pgsql-15/bin:$PATH

# Start
pg_ctl -D ~/pgdata -l ~/pgdata/server.log start

# Stop
pg_ctl -D ~/pgdata stop

# Status
pg_ctl -D ~/pgdata status

# Logs
tail -f ~/pgdata/server.log
```

`pgdata/pg_hba.conf` is configured so:
- The `extenddb` user requires md5 password auth over TCP (`127.0.0.1`, `::1`)
- The OS user that ran `initdb` has trust auth for admin tasks
- Local socket connections use trust auth

`extenddb init` writes `extenddb.toml` with the catalog database it creates (`extenddb_catalog` by default). To point the server at the role above, edit the generated section:

```toml
[storage.postgres]
connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"
pool_size = 20
```

Or via environment variable:
```bash
export EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING=\
"postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"
```

#### MongoDB

```toml
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
# max_connections = 50
# max_catalog_connections = 20
```

`max_connections` limits data-operation connections. The catalog and
authentication clients share a separate pool controlled by
`max_catalog_connections`, so size that value for their combined management
and authorization concurrency.

Or via environment variable:
```bash
export EXTENDDB__STORAGE__MONGODB__CONNECTION_STRING="mongodb://localhost:27017/?replicaSet=rs0"
```

Verify the connection once the server is up:

```bash
# Health check
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health

# List tables (should return empty)
aws dynamodb list-tables \
    --endpoint-url https://127.0.0.1:18443 \
    --region us-east-1
```

Differences from the PostgreSQL backend:

- **Replica set required:** Even single-node MongoDB must be configured as a replica set.
- **DynamoDB Streams:** Inline record writes with atomic sequence numbers.
- **GSI propagation:** Synchronous inline updates during data operations.
- **Concurrency model:** Optimistic versioning with retry on conflict (vs row-level locking in PostgreSQL).

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

The `run-tests` script automatically:
- Performs health check on the target endpoint
- Provisions test credentials via `devtools/provision-test-credentials`
- Creates a Java truststore for external tests (self-signed TLS cert)
- Sets `control_plane_delay_seconds` to 0.05 for fast test cycles
- Sets `index_propagation_delay_ms` to 0 for immediate GSI updates
- Enables throttling for production-like behavior
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
# Via run-tests script (recommended)
devtools/run-tests --extenddb --pytest

# Run in parallel (uses 1/3 of CPU cores by default)
devtools/run-tests --extenddb --pytest --parallel

# Run in parallel with a specific worker count
devtools/run-tests --extenddb --pytest --parallel=4

# Direct execution (for quick iteration during development)
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

ExtendDB supports running external test suites (Java/JUnit, Python/pytest, and others) against a running instance. Suites are registered in `external-suites.toml` at the project root (copy `external-suites.sample.toml` to create it; `--config PATH` selects another registry) and referenced by path, never copied into the repo. `--suite` can be repeated to run several named suites.

Before running external suites directly, set the GSI propagation delay to 0. External suites expect synchronous GSI behavior (the service's propagation is usually under a millisecond); the async GSI path is tested separately by the ExtendDB-specific `test_gsi_async.py` suite. `run-tests` does this for you.

```bash
./target/release/extenddb settings --config extenddb.toml set index_propagation_delay_ms 0
```

Further options of `run-external-tests`:

```bash
# Override the endpoint (default: $EXTENDDB_ENDPOINT or http://localhost:18443)
python3 devtools/run-external-tests --endpoint http://localhost:9000

# Generate a JSON report
python3 devtools/run-external-tests --report results.json
```

`--dry-run` shows what would execute without running anything; `--verbose` shows full test output.

#### Registering a new suite

Add a `[[suite]]` entry to `external-suites.toml`:

```toml
[[suite]]
name = "My Test Suite"
path = "~/source/my-test-suite"
runner = "maven"       # maven, gradle, pytest, or cargo
enabled = true

[suite.env]
DDB_ENDPOINT = "${EXTENDDB_ENDPOINT}"
```

The `${EXTENDDB_ENDPOINT}` placeholder is replaced with the `--endpoint` value at runtime.

Supported runners: `maven` (`mvn test`), `gradle` (`gradle test`), `pytest` (`python3 -m pytest`), `cargo` (`cargo test`).

#### Prerequisites

Each runner requires its tools to be installed. The runner checks prerequisites before executing and skips suites with missing tools:

- **maven:** `java`, `mvn`
- **gradle:** `java`, `gradle`
- **pytest:** `python3`, `pytest`
- **cargo:** `cargo`

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

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
