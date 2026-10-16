# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

Catalog 0.0.4 is current. **Every existing PostgreSQL, SQLite, and Cassandra deployment must run `extenddb migrate`** to reach it. PostgreSQL and SQLite servers refuse to start against a catalog version they were not built for; a Cassandra server starts anyway and serves tables and items, but CreateBackup, RestoreTableFromBackup, and DeleteBackup fail until the catalog is migrated (see [Cassandra](#cassandra)). MongoDB's catalog version does not change in this release.

See [Catalog 0.0.4](#catalog-004-current) below for what changes and the exact sequence. <!-- version-literal-ok: names the current version and the history below -->

## How Catalog Upgrades Work

### The Migration System

Migrations are SQL files in `crates/storage-postgres/migrations/`, applied in filename order:

```
001_schema.sql            ← the complete initial schema
002_vector_indexes.sql    ← vector index metadata, catalog 0.0.3 <!-- version-literal-ok: history -->
003_backup_definitions.sql ← backup table definitions, catalog 0.0.4
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

For releases that include catalog changes. The names below in angle brackets are the ones chosen at `extenddb init`: `<catalog-db>` and `<data-db>` (`--catalog-db`, `--data-db`; defaults `extenddb_catalog` and `extenddb`) and `<owner>`, the application role (`--extenddb-user`, default `extenddb`). `<admin>` is the PostgreSQL role `init` connected as (`--pg-user`).

1. **Stop the server**

```bash
extenddb stop --config extenddb.toml
```

2. **Back up the databases** (PostgreSQL; for SQLite see [SQLite](#sqlite) below)

```bash
pg_dump -h <host> -U <admin> <catalog-db> > catalog_backup_$(date +%Y%m%d).sql
pg_dump -h <host> -U <admin> <data-db> > data_backup_$(date +%Y%m%d).sql
```

`pg_dump` refuses to dump a server of a newer major version than itself ("aborting because of server version mismatch"); use the client of the server's version.

3. **Build the new version**

```bash
git pull
cargo build --release
```

4. **See what is pending, then apply it**

```bash
export EXTENDDB_PG_PASSWORD='<admin password>'
extenddb migrate --config extenddb.toml --pg-user <admin>
extenddb migrate --config extenddb.toml --pg-user <admin> --yes
```

`migrate` connects as a PostgreSQL admin role, not as the application role in the config file. Without `--pg-user` it tries the operating-system user name, and without `EXTENDDB_PG_PASSWORD` it sends no password; the error then names the role it tried, for example `Error: Cannot connect to PostgreSQL at 127.0.0.1:5432 as admin user "alice": error returned from database: password authentication failed for user "alice". Pass the admin role with --pg-user <role> and its password in the EXTENDDB_PG_PASSWORD environment variable.`

Without `--yes`, `migrate` prints what is pending and changes nothing. Its exit status says what it found:

| Exit status | Meaning |
|-------------|---------|
| 0 | Nothing is pending (or, with `--yes`, everything pending was applied) |
| 3 | Work is pending; run again with `--yes` to apply it |
| 1 | An error: the connection failed, the catalog is newer than this binary or unreadable, or a migration failed |

A script under `set -e` can run the preview as `extenddb migrate ... || [ $? -eq 3 ]`.

5. **Verify**

```bash
extenddb verify --config extenddb.toml
```

6. **Start the server**

```bash
extenddb serve --config extenddb.toml
```

## Rollback Procedure

If an upgrade fails (PostgreSQL; for SQLite see [SQLite](#sqlite) below):

1. Stop the server
2. Restore the catalog from the backup, connected as `<admin>` to the `postgres` database:

```bash
psql -h <host> -U <admin> -d postgres -c "DROP DATABASE <catalog-db>;"
psql -h <host> -U <admin> -d postgres -c "CREATE DATABASE <catalog-db> OWNER <owner>;"
psql -h <host> -U <admin> -d <catalog-db> -f catalog_backup_YYYYMMDD.sql
```

To discard data written after the upgrade as well, restore `<data-db>` from its dump the same way.

3. Rebuild the previous version and start it

## Version History

### Catalog 0.0.4 (Current)

Adds the table definition a backup records and the registration of a running restore:

- PostgreSQL: new `backup_definitions` table, one row per backup, holding the source table's global and local secondary indexes, billing mode and provisioned throughput, table class, encryption settings, on-demand throughput, TTL, and stream specification. RestoreTableFromBackup recreates the indexes and settings; DescribeBackup reports them as `SourceTableFeatureDetails`. New `table_restores` table: one row per table created by RestoreTableFromBackup, naming the source backup, the source table ARN, and the backup's creation time. DescribeTable reports it as `RestoreSummary` while the table is `CREATING`, and DeleteBackup is refused with `BackupInUseException` while a restore from the backup is running.
- SQLite: the same two tables, and `backup_items` is rebuilt (see [SQLite](#sqlite)).
- Cassandra: `V005__backup_definitions_and_restore_registration.cql` adds columns only: `definition` and `active_restores` on `backups_by_arn`, and the restore source, time, owner, and heartbeat columns on `tables`.

A backup taken before this upgrade has no recorded definition. It restores with its keys, items, and billing mode but no secondary indexes; a provisioned one restores with 5/5 throughput on PostgreSQL and SQLite and with its recorded throughput on Cassandra. On PostgreSQL a pre-upgrade backup of a table with vector indexes is refused at restore; on SQLite it restores without the vector indexes. Tables restored before this upgrade have no registration and report no `RestoreSummary`.

The upgrade is not reversible in place: a 0.0.3 PostgreSQL or SQLite binary refuses to start against a 0.0.4 catalog (`Catalog version mismatch: expected 0.0.3, found 0.0.4`). Roll back by restoring what you saved before the upgrade. Backups live in the catalog, so that also removes every backup created after the upgrade. <!-- version-literal-ok: history -->

#### PostgreSQL

```bash
extenddb stop --config extenddb.toml
pg_dump -h <host> -U <admin> <catalog-db> > catalog_backup_$(date +%Y%m%d).sql
export EXTENDDB_PG_PASSWORD='<admin password>'
extenddb migrate --config extenddb.toml --pg-user <admin>          # exit 3, prints the plan
extenddb migrate --config extenddb.toml --pg-user <admin> --yes    # applies 003_backup_definitions.sql
extenddb serve --config extenddb.toml
```

The preview reports the version change and the unapplied migration file, `Pending: catalog 0.0.3 -> 0.0.4; data migrations [catalog 003_backup_definitions.sql].`, and exits 3; after `--yes`, a second preview reports everything up to date and exits 0. `migrate` treats any catalog migration file that the catalog's ledger does not record as pending, even when the stored catalog version already matches the binary, and applies it with `--yes`; such files are listed under `data migrations` as `catalog <file>`. <!-- version-literal-ok: names both ends of the upgrade -->

`extenddb serve` refuses to start on a catalog whose ledger does not record a catalog migration file this binary carries, naming the file, for example `` the catalog does not record these catalog migration files as applied: 003_backup_definitions.sql; stop the servers and run `extenddb migrate --yes` with the admin credentials ``. The stored version alone does not show this case: another migration of the same number can have stamped the same version.

A PostgreSQL catalog created by a pre-release build of 0.0.4 (whose `table_restores` has no `source_table_arn` column) is partly converged automatically: `extenddb serve` adds the column at startup and logs a warning. It waits at most 5 seconds for the table lock; if another session holds it (an earlier server still serving a request), the server refuses to start with `could not lock the table within 5 s` and the advice to stop the servers and run `extenddb migrate --yes`. `serve` does not remove the pre-release default on `restore_date_time`, because an earlier-build server still running relies on it; `extenddb migrate` reports `catalog: drop the default of table_restores.restore_date_time` (and `catalog: add table_restores.source_table_arn` if serve has not added it) and applies them with `--yes`. <!-- version-literal-ok: names the pre-release shape -->

On its first control-plane pass after the upgrade (at startup), the server removes any table that the old restore bug left in `CREATING` with no scheduled transition, and logs each one at warn by name, for example `removed abandoned restore target <table> (<table id>) in account <account>, restoring from <no table_restores row>: its restore did not finish and no process owns it`. Operators will see those names disappear from `ListTables`; that cleanup is the intended outcome.

#### SQLite

The database is one file plus its `-wal` and `-shm` files beside it. Stop the server before copying them: `migrate` and `serve` take the same exclusive lock on `<database>.lock`, so `migrate` refuses to run while a server is up (`this command needs exclusive use of the SQLite database and cannot have it: another extenddb process is already using ...`). A stopped server can leave a non-empty `-wal` file, so copy all three.

```bash
extenddb stop --config extenddb.toml
mkdir pre-upgrade && cp -p /path/to/extenddb.sqlite* pre-upgrade/
extenddb migrate --config extenddb.toml          # exit 3, prints the plan
extenddb migrate --config extenddb.toml --yes
extenddb serve --config extenddb.toml
```

The upgrade rebuilds `backup_items` with a stable restore cursor, copying every row into a new table inside one write transaction; the database is locked for writes until it finishes. The preview names it with its row count, for example `Pending: catalog 0.0.3 -> 0.0.4; data migrations [rebuild backup_items (80 rows)].`, and `--yes` logs its start and end at info:

```
catalog upgrade: rebuilding backup_items with a stable restore cursor (80 rows); the database is locked for writes until this finishes
catalog upgrade: rebuilt backup_items (80 rows) in 0.005s
```

To roll back, stop the server, put the copied files back in place of the current ones (remove the current `-wal` and `-shm` first), and start the previous binary.

On SQLite, `extenddb serve` refuses to start a file database whose tables are in a pre-release 0.0.4 shape and names the pending step; stop the server and run `extenddb migrate --yes`. <!-- version-literal-ok: names the pre-release shape -->

On startup the SQLite server removes any restore target left in `CREATING` by a server that stopped mid-copy, which is only safe because one server at a time can hold the file. A backup keeps a WAL read snapshot open while it copies; checkpoints cannot reclaim pages needed by that snapshot, so allow disk headroom for WAL growth proportional to the backup duration and concurrent write volume. Set `pool_size` to 3 or more (see the Admin Guide).

#### Cassandra

```bash
extenddb stop --config extenddb.toml
export EXTENDDB_CASSANDRA_PASSWORD='<password>'
extenddb migrate --config extenddb.toml --cassandra-user <admin>          # exit 3
extenddb migrate --config extenddb.toml --cassandra-user <admin> --yes    # applies V005
extenddb serve --config extenddb.toml
```

`<admin>` is the role `init` used (`--cassandra-user`). `EXTENDDB_CASSANDRA_PASSWORD` keeps the password out of process arguments; `--cassandra-pass` still works and wins if both are given. Without a user and password `migrate` connects as the operating-system user with no password, which a cluster using `PasswordAuthenticator` refuses; the error reports a connection timeout.

There is no startup version gate on Cassandra. A 0.0.4 server started before `extenddb migrate --yes` serves every table and item operation normally; CreateBackup, RestoreTableFromBackup, and DeleteBackup return an internal error until the catalog is migrated, and the server logs an abandoned-restore sweep error (`Undefined column name restore_source_backup_arn`) at startup and every 15 seconds. ListBackups and DescribeBackup work and show backups taken before the upgrade. V005 only adds columns. This manual has no tested rollback for Cassandra; take a snapshot of the catalog and account keyspaces with your usual Cassandra tooling before migrating.

Upgrade every server to 0.0.4 before restoring a backup. A 0.0.3 server still running against the migrated catalog does not know about restores in progress: its DeleteTable deletes a table that is being restored and its DeleteBackup deletes a backup a restore is copying from, and that restore then fails and its target table is removed. <!-- version-literal-ok: names both ends of the upgrade -->

GSI writes made with a non-zero propagation delay (the default) by earlier Cassandra servers were queued and never applied, because the propagation workers stopped right after startup. The first 0.0.4 server applies that queue when it starts. Reading the code, the queue is applied in the order the writes were made; neither the load this adds at startup nor how long a large queue takes to drain was measured. Until it is drained, those GSIs are missing the queued writes.

#### MongoDB

No catalog change; `extenddb version` reports `catalog 0.0.2 (mongodb)` for this release and no migration is needed.

ExtendDB now reads the MongoDB catalog from the primary with read concern `local` regardless of the connection string; the connection string must use `readPreference=primary` (any other read preference is refused at startup). The write concern follows the connection string; leave it at the server default (`majority`): with `w=1`, a backup claim acknowledged just before a primary failover can be rolled back.

### Catalog 0.0.3

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
