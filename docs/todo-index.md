# TODO Index

Regenerated: v0.1.6 (BR-7085)

## TODO(fidelity)

- `crates/core/src/types/item.rs:176` — Route ConsistentRead to read replica when replica support is added.
- `crates/engine/src/batch_write_item.rs:181` — DynamoDB charges WCU based on old item size for deletes.
- `crates/engine/src/transact_write_helpers.rs:121` — DynamoDB charges WCU based on old item size for deletes.
- `crates/engine/src/backup.rs:233` — Implement real PITR using PostgreSQL temporal/history tables.
- `crates/storage-postgres/src/table_helpers.rs:141` — Two queries not in a transaction under concurrent access.

## TODO(cleanup)

- `crates/storage/src/lib.rs:592` — Unreachable method; engine handler returns before reaching it.
- `crates/storage-postgres/src/backup_engine.rs:640` — Unreachable method; engine handler returns before reaching it.
- `crates/core/src/metrics/collector.rs:124` — `#[allow(dead_code)]` on field used when console adds table-scoped latency breakdown.

## TODO (issue-tracked)

- `crates/storage-postgres/src/migrations.rs:28` — TODO(#221): applying SQL and recording it are separate commits.
- `crates/storage-postgres/src/migrations.rs:102` — TODO(#221): applying SQL and recording it are separate commits.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
