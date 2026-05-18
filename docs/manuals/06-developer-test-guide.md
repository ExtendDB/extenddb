# Developer & Test Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

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

- `core` is pure sync Rust — no async runtime, no database drivers, no HTTP framework
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
- No `.unwrap()` in production code — use `?`, `.expect("reason")`, or handle explicitly

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
7. Update the Usage Guide (`docs/manuals/03-usage-guide.md`)

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

1. A running extenddb server: `./target/debug/extenddb serve --config extenddb.toml`
2. `EXTENDDB_TEST_ENDPOINT=https://localhost:8000`
3. `EXTENDDB_ADMIN_PASSWORD=<password from extenddb init>`

The `run-tests` script automatically:
- Performs health check on the target endpoint
- Provisions test credentials via `devtools/provision-test-credentials`
- Creates a Java truststore for external tests (self-signed TLS cert)
- Sets `control_plane_delay_seconds` to 0.05 for fast test cycles
- Sets `gsi_propagation_delay_ms` to 0 for immediate GSI updates
- Enables throttling for production-like behavior
- Configures import/export paths for file operation tests
- Extracts and exports `EXTENDDB_TEST_PG_CONNECTION_STRING` for CLI lifecycle tests

**Test artifacts** are written to `discussions/` with the code repo's HEAD commit hash:
- `test-rust-<hash>.txt` — Rust unit test output
- `test-rust-integration-<hash>.txt` — Rust integration test output
- `test-pytest-<hash>.txt` — Pytest output
- `test-external-<hash>.json` — External test results (structured)
- `test-external-<hash>.txt` — External test output (verbose)
- `test-comprehensive-<hash>.txt` — Comprehensive test output
- `test-cli-<hash>.txt` — CLI lifecycle test output

### Test Suites

| Suite | Count | Description |
|-------|-------|-------------|
| Rust unit tests | 317 | Expression engine, type system, validation, error codes |
| Pytest (standard) | 180 + 118 skipped | DynamoDB API tests via boto3 |
| Comprehensive (Python) | 296 | Clean-room gap analysis tests |
| External (Java) | 346 | Third-party functional test suite |
| CLI lifecycle | 9 | Binary lifecycle tests (separate, requires `EXTENDDB_TEST_PG_CONNECTION_STRING`) |

### Python Integration Tests

The primary test suite is in Python, testing extenddb through the same AWS SDK interface that customers use.

```bash
# Via run-tests script (recommended)
devtools/run-tests --extenddb --pytest

# Direct execution (for quick iteration during development)
python3 -m pytest tests/ -v
python3 -m pytest tests/test_put_item.py -v
python3 -m pytest tests/test_put_item.py::test_put_item_basic -v
```

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
- Test error messages exactly — they are part of the API contract (tenet 4)
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

If you haven't set up the virtual environment yet, see the [Python Environment Setup](../../README.md#python-environment-setup) section in the README.

Output goes to `pdfs/` (gitignored).

## Project Tenets

1. **Fidelity over features** — match real DynamoDB behavior exactly
2. **Fidelity is the default** — standard operations behave identically to real DynamoDB
3. **Fast feedback loops** — startup time, query latency, and test cycle speed matter
4. **Errors are contracts** — error responses are part of the API surface
5. **Rust-safe by default** — rely on the type system and ownership model
6. **Tests live outside the core** — test suites are clients
7. **Readable over clever** — straightforward implementations over compact ones
8. **Console is an API client** — every piece of data and every action visible in the web console is available through the management API; the console never queries the database directly

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
