# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

Catalog 0.0.3 is current. The 0.0.2 to 0.0.3 upgrade is the first in-place catalog upgrade ExtendDB has, and **every existing PostgreSQL deployment must run it**, including deployments that never use vector indexes: the server refuses to start against a catalog version it was not built for.

See [Catalog 0.0.3](#catalog-003-current) below for what changes and the exact sequence.

## How Catalog Upgrades Work

### The Migration System

Migrations are SQL files in `crates/storage-postgres/migrations/`, applied in filename order:

```
001_schema.sql            ← the complete initial schema
002_vector_indexes.sql    ← vector index metadata, catalog 0.0.3
```

The `schema_history` table tracks which files have been applied. When `extenddb migrate` runs, it:

1. Reads all migration files embedded in the binary (via `include_str!`)
2. Checks `schema_history` for each filename
3. Applies any unapplied migrations in order
4. Records each applied filename in `schema_history`

Running `extenddb migrate` on an up-to-date catalog is a no-op.

### The Catalog Version

A single row in the `settings` table stores the catalog version:

```sql
SELECT value FROM settings WHERE key = 'catalog_version';
-- '0.0.2'
```

The binary embeds an expected catalog version (`CATALOG_VERSION` constant in `crates/storage-postgres/src/lib.rs`). At startup, the server compares the database value against the binary's expectation. If they don't match, the server refuses to start and directs the operator to run `extenddb migrate`.

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
crates/storage-postgres/migrations/002_your_feature.sql
```

The file should be a single transaction:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 002: Brief description of what this adds/changes.

BEGIN;

-- Your DDL here.
ALTER TABLE tables ADD COLUMN IF NOT EXISTS new_column TEXT;

-- Bump the catalog version.
UPDATE settings SET value = '0.1.0' WHERE key = 'catalog_version';

COMMIT;
```

### 2. Register it in the migration runner

Add the file to `CATALOG_MIGRATIONS` in `crates/storage-postgres/src/migrations.rs`:

```rust
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_your_feature.sql",
        include_str!("../../storage-postgres/migrations/002_your_feature.sql"),
    ),
];
```

### 3. Bump the catalog version constant

In `crates/storage-postgres/src/lib.rs`:

```rust
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 1, 0);
```

This must match the version written by your migration's `UPDATE settings` statement.

### 4. Update 001_schema.sql

The consolidated schema file is what fresh installs get. Add your new column/table/index to `001_schema.sql` as well, and update its `INSERT INTO settings` to seed the new version. This way fresh installs get the final schema in one pass, while existing deployments get there via the incremental migration.

### Design Considerations

**Idempotency.** Use `IF NOT EXISTS`, `IF EXISTS`, and `ADD COLUMN IF NOT EXISTS` so migrations can be safely re-run.

**Backward compatibility.** Prefer additive changes (new columns with defaults, new tables) over destructive ones (dropping columns, renaming tables). A running server on the old binary should survive the schema change until it's restarted with the new binary.

**Transaction boundaries.** Wrap each migration in `BEGIN`/`COMMIT`. If any statement fails, the entire migration rolls back and the catalog stays at the previous version.

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

2. **Back up databases**

```bash
pg_dump extenddb_catalog > catalog_backup_$(date +%Y%m%d).sql
pg_dump extenddb > data_backup_$(date +%Y%m%d).sql
```

3. **Build the new version**

```bash
git pull
cargo build --release
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
2. Restore from backup:

```bash
psql -c "DROP DATABASE extenddb_catalog;"
psql -c "CREATE DATABASE extenddb_catalog OWNER extenddb;"
psql -d extenddb_catalog -f catalog_backup_YYYYMMDD.sql
```

3. Rebuild the previous version and start it

## Version History

### Catalog 0.0.3 (Current)

Adds vector index metadata:

- New `vector_indexes` table: one row per vector index, holding its dimensions, distance function, vector attribute, search schema, projection, and build state.
- New `vector_indexes` column on `backups`: a snapshot of the source table's vector index configuration, taken when the backup is created.

**Every PostgreSQL deployment must apply this**, whether or not it uses vector indexes, because the server refuses to start against a catalog version it was not built for.

Upgrade sequence:

```bash
extenddb stop --config extenddb.toml
extenddb migrate --yes --config extenddb.toml
extenddb serve --config extenddb.toml
```

Run `extenddb migrate` without `--yes` first to see what is pending; it reports `catalog 0.0.2 -> 0.0.3` and changes nothing.

The upgrade is not reversible in place: a 0.0.2 binary refuses to start against a 0.0.3 catalog, by the same check in the other direction. Roll back by restoring the catalog backup taken before the upgrade, as described above. Downgrading is safe for data written before the upgrade; vector indexes created afterwards are not representable in 0.0.2 and are lost with the restore.

During the upgrade the migration also attempts to install the pgvector extension on the data database. Failure is reported as a notice and does not stop the upgrade: vector indexes are then refused at request time, and every other operation is unaffected. See the Admin Guide for what the notice means and what to do about it.

### Catalog 0.0.2 (Initial Release)

Complete schema: accounts, tables, indexes, tags, streams, IAM (users, groups, roles, policies, access keys, sessions, permissions boundaries), idempotency tokens, metrics, login attempts, backups, continuous backups, TTL support, settings.

The first release, so all 0.0.2 deployments were fresh installs.

---

## Data Migrations

Some changes rewrite rows in the data database rather than the catalog schema. These are
implemented in Rust (`DATA_CODE_MIGRATIONS` in `crates/storage-postgres/src/migrations.rs`),
tracked in the data database's own `schema_history` table, and applied once by
`extenddb migrate --yes`. Running `extenddb migrate` without `--yes` lists them as pending
and changes nothing.

### 004_escape_control_chars

Strings are stored through an order-preserving escape so that U+0000, which DynamoDB
accepts anywhere a string appears, can be held in PostgreSQL `TEXT` and `jsonb` columns:
U+0000 is stored as U+0001 U+0001, and U+0001 as U+0001 U+0002. Rows written before the
escape existed hold their strings raw, and a raw U+0001 in such a row could be misread by
the decoder. This migration re-encodes those rows.

What it rewrites: every data table and index table (the text key columns and the
`item_data` document), vector index tables, `gsi_pending`, `stream_records`, and
`backup_items`. Only rows that contain U+0001 are touched; a database with none is left as
it is, apart from the `schema_history` row.

The migration scans every data and index table once (each row's `jsonb` document is
rendered to text to find candidates), so on a large deployment it takes time proportional
to the size of the data. It processes one table per transaction and keeps a per-table
progress marker in `data_code_migration_progress`, so an interrupted run resumes with the
tables that are not yet done and never rewrites a table twice.

Order of operations. A server built with the escape refuses to start on a data database
that does not record this migration, with a message naming it, because rows the new build
writes are indistinguishable from legacy rows to the migration (both can contain U+0001) and
would be rewritten by it. Upgrade a PostgreSQL deployment in this order:

1. Stop every ExtendDB server that uses the data database. A server from an earlier release
   still writing during the migration would leave unescaped rows behind.
2. Run `extenddb migrate --yes` with the new binary.
3. Start the new servers.

A fresh `extenddb init` records the migration, so new deployments start without this step.
A rolling deployment that mixes the two releases against one data database is not
supported for this release: the new server will not start until the migration has run,
and the old server must be stopped before it runs.

`extenddb migrate --yes` refuses to run this migration while any other session is connected
to the data database, and lists them, because a server still writing during the rewrite
would leave rows in the wrong encoding. If the only other connections are idle ones held
by a connection pooler, set `EXTENDDB_MIGRATE_IGNORE_CONNECTIONS=1` to skip that check.
Nothing stops a server from an earlier release from being started against a migrated
data database afterwards; it would read escaped rows as literal data. Do not downgrade
past this release without restoring the data database from a backup taken before the
migration.

MongoDB. The MongoDB backend applies the same escape to BSON field names (attribute
names and map keys) and has no migration and no startup check. Field names written by
an earlier release are stored raw. A name containing a lone U+0001 reads back unchanged,
as do all string values and all key strings; a name containing U+0001 immediately
followed by U+0001 or U+0002 reads back with that pair decoded as U+0000 or U+0001.
Deployments whose attribute names never contain U+0001 are unaffected.

---

## Behavior Changes by Release

Catalog upgrades change the schema; behavior changes alter how the running server
interprets existing data or configuration. Review these before upgrading a live
deployment, even when no catalog migration is required.

### v0.1.6 — IAM: bare operators on multivalued condition keys are no-ops (BR-7085)

**What changed.** A **bare** condition operator (no `ForAnyValue:` / `ForAllValues:`
qualifier) applied to a *multivalued* condition key — `dynamodb:Attributes` or
`dynamodb:LeadingKeys` — now **never matches**, matching real AWS IAM exactly. Before
v0.1.6, extenddb evaluated a bare operator as an implicit AND across the request's values,
so such a condition could match and appear to enforce a restriction.

**Who is affected.** Any deployment with an IAM policy that uses a bare operator (e.g.
`StringEquals`) on `dynamodb:Attributes` or `dynamodb:LeadingKeys`. A common pattern is a
denylist that *appeared* to protect an attribute:

```json
{ "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
  "Condition": { "StringEquals": { "dynamodb:Attributes": ["ssn"] } } }
```

Under the old behavior this could deny a single-attribute request; after the upgrade it is a
no-op (as it always was on real AWS). **This is a security-relevant change: policies relying
on the old semantics no longer restrict access.**

**Migration.** Rewrite affected policies to use a set qualifier before upgrading:

- Denylist (block if the request touches *any* listed attribute):
  `"ForAnyValue:StringEquals": { "dynamodb:Attributes": ["ssn"] }`
- Allowlist (allow only when *every* requested attribute is listed):
  `"ForAllValues:StringEquals": { "dynamodb:Attributes": ["pk", "sk"] }` on an `Allow`.

The same applies to `dynamodb:LeadingKeys`. Audit existing policies with
`extenddb manage list-user-policies` / `list-role-policies` and update any that use bare
operators on these keys. No catalog migration is required for this change.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
