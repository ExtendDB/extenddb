# TODO Index

Regenerated: v0.0.113 (P112)

## TODO(fidelity)

- `crates/storage-postgres/src/config.rs` — Add an optional PostgreSQL default-read topology if PostgreSQL deployments need a separate physical read path.
- `crates/engine/src/batch_write_item.rs:168` — DynamoDB charges WCU based on old item size for deletes.
- `crates/engine/src/transact_write_helpers.rs:95` — DynamoDB charges WCU based on old item size for deletes.
- `crates/storage-postgres/src/table_helpers.rs:142` — Two queries not in a transaction under concurrent access.

## TODO(architecture)

- `crates/storage-postgres/src/stream_engine.rs:367` — Shard list per table requires an extra SQL round-trip.

## TODO(cleanup)

- `crates/core/src/metrics/collector.rs:124` — `#[allow(dead_code)]` on field used when console adds table-scoped latency breakdown.
- `crates/storage-postgres/migrations/003_auth.sql:71` — Dead column `permissions_boundary_arn`; boundaries use `iam_permissions_boundaries` table. Drop in a future migration.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
