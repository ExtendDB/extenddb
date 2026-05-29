# Contributing to ExtendDB

Thank you for your interest in contributing to ExtendDB!

## Getting Started

### Prerequisites

- Rust 1.85+ (stable)
- PostgreSQL 14+
- Python 3.10+ (for integration tests)

### Prepare

Familiarize yourself with ExtendDB's design and developer expectations by reviewing:
- [Architecture Guide](/docs/manuals/01-architecture-guide.md)
- [Design Guide](/docs/manuals/02-design-guide.md)
- [Developer Guide](/docs/manuals/06-developer-test-guide.md)

### Recommended Workflow

We suggest following the workflow below for proposing and contributing improvements to ExtendDB:
1. Find or create an issue in [ExtendDB's Open Issues](https://github.com/ExtendDB/extenddb/issues) that
 describes the fix, improvement or feature that you plan to work on.
1. For large changes, refactorings, new features, or changes to API specifications, wire protocol, backend
 storage traits, authentication or authorization, schema or CLI commands, please submit an
 [RFC](docs/rfcs/README.md) as a pull request and link it to the GitHub issue. Allow time for the RFC to
 be reviewed, discussed and voted on.
1. Create a fork of the [ExtendDB 'main' branch](https://github.com/ExtendDB/extenddb/tree/main).
1. Clone your fork into your development environment.
1. Make, build, test and self-review your changes on a feature branch on your fork.
1. Make your changes with clear, focused commits.
1. Ensure all tests pass and code is properly formatted.
1. Submit your changes as a pull request, linked to the issue and (if applicable) RFC that your
 work addresses.

### Build

```bash
cargo build --workspace
```

### Run Tests

```bash
# Rust unit tests
cargo test --workspace

# Integration tests (requires a running extenddb server)
devtools/run-tests --extenddb --pytest
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

## When you need an ADR or an RFC

ExtendDB uses two lightweight processes to keep core decisions reviewable:

- **ADR** ([`docs/adr/`](docs/adr/)) — records a decision *after* it has been
  made, in four sections: Context, Options Considered, Decision, Consequences.
  Use for narrower, internal calls (for example, "we chose `ring` over
  `openssl`" or "ADRs live in `docs/adr/`").
- **RFC** ([`docs/rfcs/`](docs/rfcs/)) — proposes a change *before* it is made and
  invites a comment period. Use for changes to the wire protocol, the
  `Storage` trait, the auth model, on-disk format, or the public CLI surface.
  See [`docs/rfcs/README.md`](docs/rfcs/README.md) for the lifecycle.

If you are not sure which to write, open an issue describing the change. A
maintainer will tell you which path fits.

Some areas are protected by [`.github/CODEOWNERS`](.github/CODEOWNERS) — PRs
touching them require approval from the listed owners.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
