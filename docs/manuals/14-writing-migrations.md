---
description: "How a catalog migration is written, registered and versioned, for contributors changing the catalog schema."
---

# Writing a catalog migration

> See [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/docs/NOTICE.md) for important disclaimers.

This page is for contributors changing the catalog schema. The migration runner and the catalog version check it relies on are described in the upgrade manual under [The Migration System](07-upgrade-manual.md#the-migration-system) and [The Catalog Version](07-upgrade-manual.md#the-catalog-version); the operator procedure for applying a release's migrations is [General Upgrade Procedure](07-upgrade-manual.md#general-upgrade-procedure).

When you need to change the catalog schema, this is the process. The examples below assume the tree is at catalog 0.0.4 with three catalog migration files; read the current values before you start. <!-- version-literal-ok: the page describes the next version relative to the current one -->

## 1. Create the migration file

Add a new SQL file with the next sequence number after the files already in `crates/storage-postgres/migrations/`:

```
crates/storage-postgres/migrations/004_your_feature.sql
```

The file should be a single transaction:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 004: Brief description of what this adds/changes.

BEGIN;

-- Your DDL here.
ALTER TABLE tables ADD COLUMN IF NOT EXISTS new_column TEXT;

-- Bump the catalog version.
UPDATE settings SET value = '0.0.5' WHERE key = 'catalog_version';

COMMIT;
```

## 2. Register it in the migration runner

Add the file to the end of `CATALOG_MIGRATIONS` in `crates/storage-postgres/src/migrations.rs`, after the entries already there:

```rust
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_vector_indexes.sql",
        include_str!("../../storage-postgres/migrations/002_vector_indexes.sql"),
    ),
    (
        "003_backup_definitions.sql",
        include_str!("../../storage-postgres/migrations/003_backup_definitions.sql"),
    ),
    (
        "004_your_feature.sql",
        include_str!("../../storage-postgres/migrations/004_your_feature.sql"),
    ),
];
```

## 3. Bump the catalog version constant

In `crates/storage-postgres/src/lib.rs`:

```rust
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 5);
```

This must match the version written by your migration's `UPDATE settings` statement; a unit test in `migrations.rs` (`the_last_migration_writes_the_expected_catalog_version`) fails when they differ. The SQLite and Cassandra backends carry their own constants that move with it: `CATALOG_VERSION` in `crates/storage-sqlite/src/schema.rs` and in `crates/storage-cassandra/src/lib.rs`. A PostgreSQL, SQLite or Cassandra deployment refuses to start when its stored version differs from the binary's, so all three constants change in the same commit.

## 4. Update 001_schema.sql

The consolidated schema file is what fresh installs get. Add your new column, table or index to `001_schema.sql` as well, so that a fresh install gets the final schema in one pass while an existing deployment gets there through the incremental migration. Leave the `INSERT INTO settings` seed in `001_schema.sql` alone: after the walk over `CATALOG_MIGRATIONS`, the runner writes the compiled `CATALOG_VERSION` into `settings` itself (`converge_catalog_version` in `migrations.rs`), so the stored version ends at the constant whichever files ran. The SQLite schema in `crates/storage-sqlite/src/schema.rs` is a single file with no migration list; add the change there too, and keep its seeded version literal equal to its constant (a test checks the two against each other).

## Design Considerations

**Idempotency.** Use `IF NOT EXISTS`, `IF EXISTS`, and `ADD COLUMN IF NOT EXISTS` so migrations can be safely re-run.

**Backward compatibility.** Prefer additive changes (new columns with defaults, new tables) over destructive ones (dropping columns, renaming tables). A running server on the old binary should survive the schema change until it's restarted with the new binary.

**Transaction boundaries.** Wrap each migration in `BEGIN`/`COMMIT`. If any statement fails, the entire migration rolls back and the catalog stays at the previous version.

**No data migrations in DDL files.** If a schema change requires backfilling data, do it in Rust code triggered by `extenddb migrate`, not in raw SQL. This gives you error handling, progress reporting, and the ability to batch large updates.

**Test both paths.** Every migration must be tested two ways:
1. Fresh install (`extenddb init`): verifies `001_schema.sql` is correct
2. Upgrade (`extenddb migrate` on a catalog at the previous version): verifies the incremental migration works

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
