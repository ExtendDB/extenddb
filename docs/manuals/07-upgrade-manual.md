# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

ExtendDB 0.1.0 defaults to the TiDB backend and currently expects TiDB catalog
version 0.0.26. Existing TiDB catalogs are upgraded in place by
`extenddb migrate`.

## How Catalog Upgrades Work

### The Migration System

TiDB catalog migrations are SQL files in `crates/storage-tidb/migrations/`,
applied in filename order:

```
001_schema.sql                         ← complete initial schema
...
026_simplify_control_plane_queue_index.sql
```

TiDB data-plane migrations live separately in
`crates/storage-tidb/data_migrations/` because the catalog database and data
database are separate TiDB databases on the same cluster timeline:

```
001_data_schema.sql
002_presplit_shared_data_tables.sql
004_stream_record_bucket_splits.sql
005_idempotency_token_native_layout.sql
```

The `schema_history` table tracks which catalog files have been applied. When `extenddb migrate` runs, it:

1. Reads all migration files embedded in the binary (via `include_str!`)
2. Checks `schema_history` for each filename
3. Applies any unapplied migrations in order
4. Records each applied filename in `schema_history`

Running `extenddb migrate` on an up-to-date catalog/data database is a no-op.

### The Catalog Version

A single row in the `settings` table stores the catalog version:

```sql
SELECT value FROM settings WHERE key = 'catalog_version';
-- '0.0.26'
```

The binary embeds an expected catalog version (`CATALOG_VERSION` constant in `crates/storage-tidb/src/lib.rs`). At startup, the server compares the database value against the binary's expectation. If they don't match, the server refuses to start and directs the operator to run `extenddb migrate`.

### Version Semantics

The catalog version follows semantic versioning:

- **MAJOR**: Breaking schema changes that may require data migration or downtime
- **MINOR**: New tables or columns (backward-compatible, additive)
- **PATCH**: Index changes, constraint fixes, seed data updates

## Writing a New Migration

When you need to change the catalog schema, here's the process:

### 1. Create the migration file

Add a new SQL file with the next sequence number:

```
crates/storage-tidb/migrations/027_your_feature.sql
```

TiDB DDL auto-commits and cannot be rolled back as part of an explicit SQL
transaction, so migration files must be written as idempotent online DDL plus
small, repeatable catalog DML. Do not wrap TiDB migration files in `BEGIN` /
`COMMIT`:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 027: Brief description of what this adds/changes.

-- Your online DDL here.
ALTER TABLE tables ADD COLUMN IF NOT EXISTS new_column TEXT;

-- Bump the catalog version after the DDL statements are in place.
UPDATE settings SET value = '0.0.27' WHERE key = 'catalog_version';
```

### 2. Register it in the migration runner

Add the file to `CATALOG_MIGRATIONS` in `crates/storage-tidb/src/migrations.rs`:

```rust
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-tidb/migrations/001_schema.sql"),
    ),
    (
        "026_simplify_control_plane_queue_index.sql",
        include_str!("../../storage-tidb/migrations/026_simplify_control_plane_queue_index.sql"),
    ),
    (
        "027_your_feature.sql",
        include_str!("../../storage-tidb/migrations/027_your_feature.sql"),
    ),
];
```

### 3. Bump the catalog version constant

In `crates/storage-tidb/src/lib.rs`:

```rust
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 27);
```

This must match the version written by your migration's `UPDATE settings` statement.

### 4. Update 001_schema.sql

The consolidated schema file is what fresh installs get. Add your new column/table/index to `001_schema.sql` as well, and update its `INSERT INTO settings` to seed the new version. This way fresh installs get the final schema in one pass, while existing deployments get there via the incremental migration.

### Design Considerations

**Idempotency.** Use `IF NOT EXISTS`, `IF EXISTS`, and `ADD COLUMN IF NOT EXISTS` so migrations can be safely re-run.

**Backward compatibility.** Prefer additive changes (new columns with defaults, new tables) over destructive ones (dropping columns, renaming tables). A running server on the old binary should survive the schema change until it's restarted with the new binary.

**TiDB online DDL.** Prefer TiDB online DDL and idempotent `IF EXISTS` / `IF NOT EXISTS` statements. DDL statements auto-commit in TiDB, so migration correctness comes from idempotency and post-migration verification, not from a frontend transaction wrapper. Do not add frontend locks or per-node migration ownership; TiDB owns distributed DDL scheduling.

**No data migrations in DDL files.** If a schema change requires backfilling data, do it in Rust code triggered by `extenddb migrate`, not in raw SQL. This gives you error handling, progress reporting, and the ability to batch large updates.

**Test both paths.** Every migration must be tested two ways:
1. Fresh install (`extenddb init`) — verifies `001_schema.sql` is correct
2. Upgrade (`extenddb migrate` on a catalog at the previous version) — verifies the incremental migration works

## General Upgrade Procedure

For future releases that include catalog changes:

1. **Stop the server**

```bash
extenddb stop --config extenddb.toml
```

2. **Back up TiDB**

Use the configured `[storage.tidb.backup]` BR path, or run TiDB BR directly for
cluster-level backup. Do not use frontend row-copy backup as a substitute for
TiDB's physical backup path.

3. **Build the new version**

```bash
git pull
cargo build -j12 --release
```

4. **Run migrations**

```bash
extenddb migrate --config extenddb.toml
```

5. **Verify**

```bash
extenddb verify --config extenddb.toml
```

6. **Start the server**

```bash
extenddb serve --config extenddb.toml
```

## Rollback Procedure

If an upgrade fails:

1. Stop the server
2. Restore with TiDB BR to a safe recovery cluster or restore the affected
   databases through the configured native BR path.

3. Rebuild the previous version and start it

## Version History

### TiDB Catalog 0.0.26 (Current)

Simplifies the TiDB control-plane transition queue index while preserving
idempotent replay through TiDB-native online DDL scheduling.

### Earlier TiDB Catalog Versions

Earlier TiDB catalog versions introduced native BR metadata, native TTL state,
binary-collated catalog defaults, append-table pre-splitting, authentication
lookup indexes, raw data hash-key columns, and removal of legacy frontend
control-plane leases and metrics tables.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
