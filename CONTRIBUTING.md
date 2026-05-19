# Contributing to ExtendDB

Thank you for your interest in contributing to ExtendDB.

## Getting Started

### Prerequisites

- Rust 1.85+ (stable)
- PostgreSQL 14+
- Python 3.10+ (for integration tests)

### Build

```bash
cargo build --workspace
```

### Run Tests

```bash
# Rust unit tests
cargo test --workspace

# Integration tests (requires a running extenddb server)
pytest tests/
```

### Code Style

All contributions must pass:

```bash
cargo fmt --check
cargo clippy -- -W clippy::pedantic -W clippy::unwrap_used -W clippy::expect_used
```

- Use `cargo fmt` before committing.
- Address all clippy warnings or add `#[allow(...)]` with a justification comment.
- Avoid `.unwrap()` and `.expect()` in library crates — use `?` or explicit error handling.

### Submitting Changes

1. Fork the repository and create a branch from `main`.
2. Make your changes with clear, focused commits.
3. Ensure all tests pass and code is formatted.
4. Open a pull request with a description of what changed and why.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
