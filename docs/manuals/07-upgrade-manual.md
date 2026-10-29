# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

Catalog 0.0.5 is current on PostgreSQL; 0.0.4 is current on SQLite and Cassandra. **Every existing PostgreSQL deployment must run `extenddb migrate` with every server stopped** to reach 0.0.5 (see [Catalog 0.0.5](#catalog-005-postgresql-current)). **Every existing PostgreSQL, SQLite, and Cassandra deployment must run `extenddb migrate`** to reach 0.0.4. <!-- version-literal-ok: names the current version per backend -->
<!-- The sentence below predates 0.0.5 and describes the 0.0.4 step. --> PostgreSQL and SQLite servers refuse to start against a catalog version they were not built for; a Cassandra server starts anyway and serves tables and items, but CreateBackup, RestoreTableFromBackup, and DeleteBackup are refused with a `ValidationException` naming `extenddb migrate --yes` until the catalog is migrated (see [Cassandra](#cassandra)). MongoDB's catalog version does not change in this release.

See [Catalog 0.0.4](#catalog-004) below for what changes and the exact sequence. <!-- version-literal-ok: names the current version and the history below -->

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

`migrate` finds the data database through the name `init` recorded in the catalog's `settings` (`data_database_name`), so a deployment initialised with `init --data-db <other-name>` migrates without further flags; when that recorded name differs from the one derived from the catalog database name, `migrate` prints it (`--- Data database '<name>' (recorded in the catalog's settings; ...)`), and a deployment with the usual name sees no such line. A catalog that has no recorded name (one initialised by a build that did not record it) falls back to the catalog database name without its `_catalog` suffix; if the data database is named otherwise, pass `--data-db <name>` to `migrate`, which also overrides a recorded name. A wrong name fails before anything is written: `Error: Cannot connect to <name>: error returned from database: database "<name>" does not exist`.

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

### Catalog 0.0.5 (PostgreSQL, Current)

Changes no schema. It marks a change in the stored form of the seven catalog columns that carry attribute names: `tables.key_schema`, `tables.attribute_definitions`, `indexes.key_schema`, `indexes.projection`, `backups.key_schema`, `backups.attribute_definitions` and `backup_definitions.definition`. From this release those documents are written through an order-preserving escape (U+0000 stored as U+0001 U+0001, U+0001 stored as U+0001 U+0002), so a key attribute name, an index key attribute name or a projected attribute name may contain U+0000, as it may on DynamoDB. PostgreSQL `jsonb` cannot hold U+0000, which is why the escape exists. The migration `004_escape_catalog_control_chars.sql` re-encodes every stored document that holds a raw U+0001 once, before it moves the version, so a name an earlier release stored raw reads back unchanged. A document without U+0001 is left as it is; on most deployments the migration rewrites no rows.

The re-encode is not repeatable: an escaped U+0001 and a raw U+0001 pair are the same bytes, so `extenddb migrate --yes` refuses to run it while any other session is connected to the catalog database, naming the sessions (`N other connection(s) hold the catalog database: ...`). Stop every ExtendDB server first. If the only other sessions are idle ones held by a connection pooler, set `EXTENDDB_MIGRATE_IGNORE_CONNECTIONS=1` to skip the check. The check leaves out sessions whose `application_name` is `extenddb-migrate`, the name the migrate command sets on its own connections, so a server's configured connection string must not carry that name. Each table is rewritten in its own transaction with a progress marker, so a run interrupted by a crash resumes with the tables not yet done. `extenddb init` records the migration on a fresh catalog.

```bash
extenddb stop --config extenddb.toml                               # every server, every host
pg_dump -h <host> -U <admin> <catalog-db> > catalog_backup_$(date +%Y%m%d).sql
export EXTENDDB_PG_PASSWORD='<admin password>'
extenddb migrate --config extenddb.toml --pg-user <admin>          # exit 3, prints the plan
extenddb migrate --config extenddb.toml --pg-user <admin> --yes    # re-encodes, applies 004
extenddb serve --config extenddb.toml
```

The preview reports `Pending: catalog 0.0.4 -> 0.0.5; data migrations [catalog 004_escape_catalog_control_chars.sql].` and exits 3. <!-- version-literal-ok: names both ends of the upgrade -->

Downgrade: after this release, a table whose key attribute, index key or projected attribute names contain U+0000 or U+0001 on PostgreSQL is not readable by an earlier release. An earlier binary refuses to start against a 0.0.5 catalog (`Catalog version mismatch: expected 0.0.4, found 0.0.5`); if it were started against the data anyway it would read the escaped form as literal data (`p<U+0001><U+0001>k` for a name `p<U+0000>k`) and `GetItem` by the real name would answer `Missing the key ...`. Roll back by restoring the catalog saved before the upgrade. Names without those two characters are stored as before and are unaffected. <!-- version-literal-ok: history -->

SQLite, MongoDB and Cassandra store attribute names without an escape and keep their catalog versions in this release.

### Catalog 0.0.4

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

On startup the SQLite server removes any restore target left in `CREATING` by a server that stopped mid-copy, which is only safe because one server at a time can hold the file. A backup keeps a WAL read snapshot open while it copies; checkpoints cannot reclaim pages needed by that snapshot, so allow disk headroom for WAL growth proportional to the backup duration and concurrent write volume. Set `pool_size` to 3 or more (see the Admin Guide); the server runs at most `pool_size / 2 - 1` backups at once (4 with the default of 10) and answers `LimitExceededException` to a `CreateBackup` that waited 30 seconds for a slot. Running backups hold one connection each for their read snapshot plus one shared writer (`n` backups hold `n + 1` connections), so from `pool_size` 4 up a flood of backups leaves at least half the pool to other requests; with a `pool_size` of 2 or 3 the one admitted backup holds two of them. A GSI added by `UpdateTable` with `ProvisionedThroughput` on a `PROVISIONED` table made every `DescribeTable` of that table fail on earlier builds (`Internal("index pt: missing field NumberOfDecreasesToday")`); this build writes the index's throughput in the shape `DescribeTable` reads and reads the shape earlier builds wrote (as the same units with `NumberOfDecreasesToday` 0), so such a table is describable again after the upgrade without any change to the database file.

#### Cassandra

```bash
extenddb stop --config extenddb.toml
export EXTENDDB_CASSANDRA_PASSWORD='<password>'
extenddb migrate --config extenddb.toml --cassandra-user <admin>          # exit 3
extenddb migrate --config extenddb.toml --cassandra-user <admin> --yes    # applies V005
extenddb serve --config extenddb.toml
```

`<admin>` is the role `init` used (`--cassandra-user`). `EXTENDDB_CASSANDRA_PASSWORD` keeps the password out of process arguments; `--cassandra-pass` still works and wins if both are given. Without a user and password `migrate` connects as the operating-system user with no password, which a cluster using `PasswordAuthenticator` refuses; the error reports a connection timeout.

There is no startup version gate on Cassandra. A 0.0.4 server started before `extenddb migrate --yes` serves every table and item operation normally; CreateBackup, RestoreTableFromBackup, and DeleteBackup are refused with a `ValidationException` ("Backups are unavailable until the ExtendDB catalog is migrated: ... Run `extenddb migrate --yes` with the admin credentials.") until the catalog is migrated; a running server notices the migration without a restart, and the server logs an abandoned-restore sweep error (`Undefined column name restore_source_backup_arn`) at startup and every 15 seconds. ListBackups and DescribeBackup work and show backups taken before the upgrade. V005 only adds columns. This manual has no tested rollback for Cassandra; take a snapshot of the catalog and account keyspaces with your usual Cassandra tooling before migrating.

Upgrade every server to 0.0.4 before restoring a backup. A 0.0.3 server still running against the migrated catalog does not know about restores in progress: its DeleteTable deletes a table that is being restored and its DeleteBackup deletes a backup a restore is copying from, and that restore then fails and its target table is removed. <!-- version-literal-ok: names both ends of the upgrade -->

On Cassandra, `extenddb serve` does not apply asynchronously propagated GSI writes, in 0.0.4 as in earlier versions: GSI writes made with a non-zero `index_propagation_delay_ms` (the default is 10 ms) are queued and never applied, so GSI queries miss them. Set `extenddb settings set index_propagation_delay_ms 0` to write GSIs synchronously; already-queued writes stay unapplied. Tracked in issue #407.

#### MongoDB

No catalog change; `extenddb version` reports `catalog 0.0.2 (mongodb)` for this release and no migration is needed.

ExtendDB now reads the MongoDB catalog from the primary with read concern `local` regardless of the connection string; the connection string must use `readPreference=primary` (any other read preference is refused at startup). The connection string must leave the write concern at the server default or set `w=majority`: `w=0`, a numeric or tagged `w`, and `journal=false` are refused at startup, because a backup claim acknowledged under a weaker write concern can be rolled back by a primary failover. The catalog and the `$out` copies of CreateBackup and RestoreTableFromBackup are written with `w: majority` whatever the server default is.

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

### v0.1.14 LeadingKeys now enforced on Query, batch and transaction operations

**What changed.** `dynamodb:LeadingKeys` is now evaluated on `Query`,
`BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`.
Before this release those five operations carried no LeadingKeys value, so a
`ForAllValues:StringEquals dynamodb:LeadingKeys` condition on them was satisfied
vacuously and a principal fenced to its own partition could read or write any
partition through them. The item operations (`GetItem`, `PutItem`, `UpdateItem`,
`DeleteItem`) already enforced the key; they change only for a numeric (`N`)
partition key, which is now normalized (see below). Before this release the
item operations compared the number exactly as the request spelled it.

Details of how the key is bound, matching DynamoDB:

- A `Query` on the base table or a local secondary index binds LeadingKeys to
  the partition key value in the key condition (an LSI shares the base table's
  partition key).
- A `Query` on a global secondary index binds LeadingKeys to the queried
  index's partition key value (its first HASH attribute), not the base table's.
- `BatchGetItem` and `BatchWriteItem` are authorized per table over the set of
  keys that table receives; `TransactGetItems` and `TransactWriteItems` are
  authorized per item. A `ForAnyValue` condition therefore passes a mixed batch
  but not a mixed transaction.
- `Scan` and `SearchVectors` carry no LeadingKeys, because they target no single
  partition. A `ForAllValues` condition does not fence them (it is satisfied
  vacuously); fence a full-table read with `ForAnyValue:StringEquals` or a
  `Null` guard (`"Null": {"dynamodb:LeadingKeys": "false"}`) on the statement.
- A numeric (`N`) partition key is written into LeadingKeys the way the
  service writes it, which depends on the operation. `GetItem`, `PutItem`,
  `UpdateItem`, `DeleteItem`, `Query` and `BatchGetItem` use the normalized
  number, so `10`, `010`, `10.0` and `1E1` all compare as `"10"`.
  `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems` keep the scale
  the request was written with, as Java's `BigDecimal` prints it: `010` and
  `1.0E1` compare as `"10"`, `10.0` as `"10.0"` and `1E1` as `"1E+1"`. The
  policy value is matched as written. Under a fence of `["10"]`, a client that
  sends `10.0` or `1E1` is allowed on the first group and denied on the second,
  as on DynamoDB.
- A request outside the count limits of the batch and transaction operations
  returns `ValidationException` before any policy is evaluated, as on DynamoDB:
  a `TransactWriteItems` or `TransactGetItems` with no items or more than 100,
  a `BatchGetItem` or `BatchWriteItem` with an empty `RequestItems` or a table
  with no keys or requests, a `BatchGetItem` with more than 100 tables or keys
  and a `BatchWriteItem` with more than 25 tables or requests. Before this
  release a principal without permission on the table received
  `AccessDeniedException` for each of these requests except an empty
  `TransactItems` or `RequestItems`.

**Who is affected.** Any deployment with a principal whose policy fences
`dynamodb:LeadingKeys` with `ForAllValues:StringEquals` and whose Query, batch or
transaction requests previously crossed its own partition. After upgrade those
cross-partition requests return `AccessDeniedException`. Own-partition access is
unchanged, except for numeric partition keys.

A principal with a numeric LeadingKeys fence sees a change on the item
operations even if it never uses Query, batch or transactions. Under a fence of
`["10"]`, a `GetItem` on `010`, `10.0` or `1E1` was denied and is now allowed.
Under a fence written in a non-canonical form, such as `["010"]` or `["1E1"]`,
a request spelled the same way was allowed and is now denied: the policy value
is matched as written and no request is normalized to it.

**Migration.** No catalog migration is required. Before upgrading, audit
LeadingKeys policies with `extenddb manage list-user-policies` /
`list-role-policies` and confirm:

- A fence intended to cover reads and writes names the partition values the
  principal legitimately uses on Query, batch and transaction, not only item
  operations.
- Any policy relying on `Scan` or `SearchVectors` being fenced by a
  `ForAllValues` LeadingKeys condition is rewritten to `ForAnyValue` or adds a
  `Null` guard, since neither operation is fenced by `ForAllValues`.
- A numeric LeadingKeys value is written in the form each operation compares
  (see the numeric partition key item above). A fence of `["10"]` covers every
  spelling of 10 on the item operations, Query and BatchGetItem, and the
  spellings `10`, `010`, `+10` and `1.0E1` on batch writes and transactions. A
  fence value written as `010`, `10.0` or `1E1` matches nothing on the item
  operations, Query and BatchGetItem; rewrite it as `10`.

---

### IAM: operations authorize against their specific resource ARN

**What changed.** Every operation is now authorized against the specific resource
it acts on, resolved through one parser shared by authorization and the engine,
rather than a `table/*` wildcard or the raw request string. A `Deny` or a narrow
`Allow` written against a specific table, index, stream, or backup now takes
effect where it was previously bypassed.

**Who is affected and what to change** (no catalog migration is required):

- **Index reads need an index grant.** Query and Scan with an `IndexName` are
  authorized against `table/<name>/index/<index>`. A policy granting only
  `table/<name>` no longer allows that table's index reads; grant
  `table/<name>/index/<index>` or `table/<name>/index/*`.
- **Fencing a whole table with a Deny.** Index reads, `ListStreams` with a
  TableName, and `ListBackups` with a TableName are authorized against a
  sub-resource ARN (`table/<name>/index/*`, `table/<name>/stream/*`,
  `table/<name>/backup/*`), not the bare `table/<name>`. A `Deny` on
  `table/<name>` does not cover them: add `table/<name>/*` to the `Deny`
  alongside `table/<name>` to fence the whole table.
- **Index reads and resource tags.** An index Query or Scan evaluates the base
  table's resource tags, because an index inherits its table's tags. An ABAC
  `Deny` or `Allow` conditioned on the table's tags now applies to its indexes.
- **ListStreams, ListBackups, and resource tags.** ListStreams and ListBackups
  with a TableName are authorized against the table's `stream/*` or `backup/*`
  sub-resource, which carries no resource tags, as on DynamoDB. Before this
  release they evaluated the table's tags. A condition on `aws:ResourceTag/*`
  for these calls now evaluates as for an absent key: `StringEquals` no longer
  matches, and a negated operator or `Null` true now does.
- **DescribeLimits and DescribeEndpoints.** DescribeLimits is authorized against
  `*`, not `table/*`, so a grant or `Deny` scoped to `table/*` no longer applies
  to it; scope its statement to `*`. DescribeEndpoints is not evaluated against
  IAM at all, so a `Deny` on it does not block it.
- **GetRecords across the upgrade.** A shard iterator issued before this release
  does not identify its stream. GetRecords on such an iterator now returns
  `ExpiredIteratorException` with the expiry message, so a consumer reading when
  the server restarts renews its iterator by calling GetShardIterator instead of
  failing on a non-retryable `ValidationException`. A token that does not decode
  at all still returns `ValidationException` `Invalid ShardIterator`.
- **Restores.** RestoreTableFromBackup and RestoreTableToPointInTime require the
  item read and write actions on the target table in addition to the restore
  action, because a restore writes into the target.
- **Export.** `ExportTableToPointInTime` requires `TableArn` to be exactly a
  table ARN. An ARN carrying a sub-resource or an extra `table/` segment is
  rejected with `ValidationException` before authorization.

---

### `server.region` is part of every backup ARN

`server.region` (default `us-east-1`) is baked into the ARN of every backup the
server creates (`arn:aws:dynamodb:<region>:<account>:table/<name>/backup/<id>`),
and the backends store and look up that exact string. Changing `server.region`
on a deployment that already has backups does not move them: `ListBackups` keeps
reporting them with the region they were created under. `DescribeBackup`,
`DeleteBackup`, and `RestoreTableFromBackup` resolve a `BackupArn` of the
caller's account in two steps: first the ARN rebuilt in the server's current
region, then, if that names no backup and the caller wrote a different ARN, the
ARN as written. So the ARN `ListBackups` reports for a pre-change backup keeps
working; the same backup's ARN rewritten with the new region does not (it is
`BackupNotFoundException`). `ListBackups` pages past such a backup: an
`ExclusiveStartBackupArn` in another region is accepted when it is the stored
ARN of a listed backup, which is what `LastEvaluatedBackupArn` hands out, and
refused with `ValidationException` "does not match the region" otherwise (including
when that backup is deleted between two pages; start the listing again). IAM
policies are evaluated on the backup ARN in the server's current region, whatever
region the caller wrote, so after a change a statement must name the new region
(or use `*` in the region field) to cover the backups created before it (the
admin guide covers this).

Keep `server.region` within `[a-z0-9-]`. The `BackupArn` parser accepts only
that class, as the service does, so a region such as `LOCAL`, `us_east_1`, or
`dev.local` makes every backup ARN the server issues `ValidationException`
"Invalid Request: BackupArn is not valid" on `DescribeBackup`, `DeleteBackup`,
and `RestoreTableFromBackup`, and `ListBackups` refuses the cursor it hands out,
so every listing stops at its first page. The server starts with such a region
and logs a warning naming the consequence at startup.

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

### IAM: `aws:TokenIssueTime` on MongoDB role sessions created before the upgrade

**What changed.** ExtendDB now resolves `aws:TokenIssueTime` for a role session from
the session's recorded creation time. On the MongoDB backend the session store did not
record a creation time before this release.

**Who is affected.** MongoDB deployments only. A role session established before the
upgrade has no stored creation time, so `aws:TokenIssueTime` is absent for that session
until it expires and is re-established. The three SQL and the Cassandra backends already
recorded the session creation time and are not affected.

**Impact.** The direction is over-deny only and self-heals: a policy that gates on
`aws:TokenIssueTime` (a presence check or a `Date` comparison) denies a pre-upgrade
MongoDB session that it would grant, and resolves correctly once the session is
re-established. No catalog migration is required; new sessions record the creation time
automatically.

### IAM: a malformed CIDR in an identity policy is rejected at write

**What changed.** A condition value for `IpAddress` or `NotIpAddress` that is not a
valid CIDR or address (for example `10.0.0.0/33`, `999.0.0.0/8`, `not-a-cidr`,
`10.0.0.0/`) is now rejected when an identity policy or a permissions boundary is
written, with a `MalformedPolicyDocument`-class error, matching the service. Before
this release the value was accepted and the statement silently did nothing (an
`IpAddress` or `NotIpAddress` `Deny` never blocked).

**Who is affected.** Any deployment that stored an identity policy or permissions
boundary with a malformed CIDR. New writes with a malformed value now fail. A policy
already stored with a malformed value evaluates fail closed: it no longer parses, so the
request is denied rather than the statement being ignored. Audit stored policies with
`extenddb manage list-user-policies` / `list-role-policies` and correct any malformed CIDR
before upgrading so a legitimate request is not denied.

**What a stored malformed-CIDR policy does after upgrade.** The failure is
whole-document and scoped to the principal that owns the policy. Every request by
that principal is denied until the policy is corrected, including a request that a
different, valid policy of the same principal would allow (the malformed document
fails the request closed before the valid policy can grant). The caller receives
`AccessDeniedException` with the message "Not authorized to perform this action
(policy evaluation error)", not a server error: a parse failure on the authorization
path is mapped to a deny, not to `InternalServerError`. A different principal whose
policies are all well formed is unaffected.

**Trust policies are unchanged.** The live service accepts a malformed CIDR in a trust
policy at write and drops the statement at evaluation; ExtendDB matches this, so a trust
policy is not rejected for a malformed CIDR. No catalog migration is required.

### IAM: condition key names and tag keys, and more CIDR forms

**Condition key names and tag keys fold under Unicode on the request path.** A
stored identity policy whose condition key name or tag key contains a non-ASCII
letter that folds to a resolvable key (for example the long s folding to `s`, the
st ligature to `st`, or the sharp s to `ss`) now resolves that key on a DynamoDB
request, matching the service. For such a stored policy a `Deny` gated on the
Unicode-spelled key now applies where it previously did not, and a negated `Allow`
gated on it no longer grants. A policy using only ASCII key names is unaffected.
The trust (`AssumeRole`) path matches key names and tag keys ASCII-case only and
does not fold, as the service does; a trust policy whose condition key name or tag
key contains a non-ASCII letter has that key read as absent on the trust path.

**Tag keys that fold to the same string resolve deterministically.** If a
principal carries two tag keys that fold to the same string (for example `mass`
and `ma\u{00df}`), `aws:PrincipalTag/<key>` now resolves to the byte-exact stored
key, then to the smallest fold-matching key by code point, every time, rather
than at random. A policy naming one of the keys exactly is unaffected.

**Tag write rules match the service.** `TagUser`, `TagRole` and `AssumeRole`
session tags now reject a tag key or value outside IAM's grammar
`[\p{L}\p{Z}\p{N}_.:/=+\-@]`: any Unicode Letter (Lu, Ll, Lt, Lm, Lo), Number
(Nd, Nl, No) or Separator (Zs, Zl, Zp) by general category, plus the listed
symbols. A combining mark (such as U+0345, category Mn), `#`, `!` and other
punctuation, a key over 128 or value over 256 characters, the reserved `aws:`
key prefix, and two keys that differ only in ASCII case are all rejected. Only
the tags named in a given request are validated, so a tag already stored that
the new rule would reject stays in place and keeps evaluating, and does not
block setting other tags on the same principal; it is rejected only when a later
request includes it again. Live IAM behaves the same way: a tag request
validates only its own tags.

**Policy Sid charset.** A policy `Sid` is now accepted only when it is empty or
entirely ASCII letters and digits, on both the trust write (`CreateRole`) and
the identity-policy write (`PutUserPolicy`), matching the service, which rejects
any other `Sid` (a hyphen, underscore, space, dot or non-ASCII character) with
`MalformedPolicyDocument`. The `Sid` is a statement label and does not affect
evaluation, so a policy already stored with a non-conforming `Sid` keeps
evaluating unchanged; only a new write is checked.

**More CIDR forms.** `IpAddress`/`NotIpAddress` now accept a leading-zero octet
and read it as decimal (`010.0.0.0/8` is `10.0.0.0/8`, not octal), matching the
service, and reject an IPv4-mapped IPv6 literal (`::ffff:a.b.c.d`) at the identity
and boundary write (the service rejects it; accepting it would store an
`IpAddress` `Deny` that silently never matched). No catalog migration is required
for any of these changes.

---

### v0.1.14: IAM resource tags evaluate only as `aws:ResourceTag/`

**What changed.** ExtendDB now resolves resource tags only as `aws:ResourceTag/<key>`, matching the live service. `dynamodb:ResourceTag/<key>` is left absent (the live service never populates it). Before v0.1.14, ExtendDB filled `dynamodb:ResourceTag/<key>` from the target table's tags, so a condition on that key evaluated against the tag value instead of being treated as absent. `aws:ResourceTag/<key>` is unaffected and keeps resolving from the table's tags, case-insensitively on the key.

**Who is affected.** Any deployment with an IAM policy that uses a `dynamodb:ResourceTag/` condition key or a `${dynamodb:ResourceTag/...}` policy variable: inline user, group and role policies, user and role permissions boundaries, and session policies. A policy that uses `aws:ResourceTag/` is not affected. Role trust policies are not affected: the AssumeRole path never resolved `dynamodb:ResourceTag/`, so a trust policy that names it behaved then as it does now.

**Migration is required.** `extenddb serve` refuses to start on a catalog that has not been migrated, with an error that names `extenddb migrate --yes`. Run `extenddb migrate --yes` once per deployment before starting the new binary. On PostgreSQL, `migrate` connects as the admin user, so pass `--pg-user <admin>` and set `EXTENDDB_PG_PASSWORD`, as for `init`. The migration rewrites every stored `dynamodb:ResourceTag/<key>` condition key and `${dynamodb:ResourceTag/...}` policy variable to the `aws:ResourceTag/` form, keeping the operator, the tag key and the value unchanged. The rewrite preserves each policy's decision, because `aws:ResourceTag/` resolves from the same table tags the old `dynamodb:ResourceTag/` key read. The one difference is a `${dynamodb:ResourceTag/<key>}` policy variable on a table without that tag: neither form expands, and the literal text in the comparison changes from `${dynamodb:ResourceTag/<key>}` to `${aws:ResourceTag/<key>}`. The decision changes only for a request value equal to one of those literals. A freshly initialised deployment needs no migrate step; its catalog is already recorded as migrated.

**What `migrate` prints.** Without `--yes`, `migrate` lists each policy it would rewrite, one line per policy with the account, the principal, the policy name and the number of condition keys, and writes nothing. With `--yes`, it rewrites and records that the migration is complete. It runs on every `migrate`, so an interrupted run is finished by the next one. If a policy lookup fails, or a policy needs attention (below), `migrate` reports it, exits non-zero, and does not record completion, so a later run retries. Policy bodies are never printed.

**Both forms on one key.** If one operator block in a policy holds both `aws:ResourceTag/<key>` and `dynamodb:ResourceTag/<key>` for the same tag key with the same value, the two were one condition before the upgrade, and `migrate` drops the `dynamodb:` key. With different values they cannot share one JSON key. `migrate` then leaves the policy unchanged, lists it as `NEEDS ATTENTION` with the account, the principal and the policy name, exits non-zero, and does not record the migration as complete, so `serve` keeps refusing the catalog. Fix the policy with the previous version still serving the catalog (keep one of the two forms), then run `migrate --yes` with the new binary. A session policy that does not parse is reported in the log and skipped; it cannot be evaluated under either form.

**What each statement shape does for a policy that was not migrated.** An operator who writes a policy by hand after the upgrade, for example from an old template, needs to know that the `dynamodb:ResourceTag/` form has no effect. With the key absent, standard IAM absent-key semantics apply:

| Statement shape on `dynamodb:ResourceTag/env` | Outcome for an unmigrated policy |
|---|---|
| `StringEquals` (bare positive) | Never matches; an `Allow` stops granting, a `Deny` stops blocking |
| `StringNotEquals` (bare negated) | Always matches on the absent single-valued key; an `Allow` grants, a `Deny` blocks |
| `StringEqualsIfExists` | Passes (the key is absent), so the statement applies on the key alone |
| `ForAllValues:StringEquals` | Vacuously true on the absent key, so the statement applies on the key alone |
| `ForAnyValue:StringEquals` | False on the absent key, so an `Allow` does not grant |
| `Null` with value `true` | Matches (the key is absent) |
| `Null` with value `false` | Does not match (the key is absent) |
| `${dynamodb:ResourceTag/env}` policy variable in a value | Does not expand; the literal text `${dynamodb:ResourceTag/env}` is compared, which no request value equals |

To get the intended behaviour, write the condition on `aws:ResourceTag/<key>`.

**Sessions issued before the migration.** Session policies are stored in the catalog, so `migrate` rewrites them along with the identity policies. A session issued before the upgrade is evaluated with its rewritten policy once `migrate --yes` has run, and it does not need to be issued again. The new binary does not serve an unmigrated catalog, so no request is evaluated against the old policy text under the new rules.

### IAM: `ArnEquals` and `ArnNotEquals` honour wildcards

**What changed.** `ArnEquals` and `ArnNotEquals` now match `*` and `?` in each ARN field, as `ArnLike` and `ArnNotLike` already did and as IAM evaluates all four. Before this release the two Equals operators compared strings exactly. An Arn condition over a context value that is not an ARN (fewer than six `:`-separated fields) is now false under every operator, including `ArnNotEquals` and `ArnNotLike`, and in `ForAnyValue:` and `ForAllValues:` forms it is false when any value in the set is not an ARN and the policy lists at least one value (`ForAllValues:ArnNotEquals` or `ForAllValues:ArnNotLike` with an empty value list stays true, as in IAM). No catalog migration is required.

**Who is affected.** A policy that uses an Arn operator in an identity policy, a permissions boundary, a session policy or a role trust policy, in these cases:

- A `Deny` with a wildcarded `ArnEquals` now blocks the callers it names, and an `Allow` gated on one now grants to them.
- A `Deny` with a wildcarded `ArnNotEquals` now exempts the callers the wildcard matches; before, it applied to every caller.
- A bare `*` value now matches every ARN. An `Allow` gated on `ArnEquals: "*"` grants, and a `Deny` gated on it applies, whenever the key holds an ARN. A `Deny` gated on `ArnNotEquals: "*"` now applies only when the key is absent, and an `Allow` gated on it grants only then. AWS IAM Access Analyzer reports a bare `*` Arn value as an error, and the IAM policy simulator refuses to evaluate one against an ARN, but ExtendDB accepts it at write. Replace any of these shapes before upgrading if you wrote it by hand.
- An Arn operator over a key whose value is not an ARN, such as `aws:ResourceTag/<key>`, is now false, so a `Deny` written that way no longer applies and an `Allow` gated on one no longer grants.
- `dynamodb:LeadingKeys`, `dynamodb:Attributes` and `aws:TagKeys` hold values the caller or the admin chooses: the partition key values in a request (every key in a batch), the attribute names in a projection, and the session tag keys in an AssumeRole call. Each value can be ARN-shaped or not. A `ForAnyValue:` or `ForAllValues:` Arn condition over one of these keys is now false while any value in the set is not an ARN and the policy lists at least one value. A `Deny` gated on `ForAnyValue:ArnNotLike` with `arn:aws:dynamodb:us-east-1:123456789012:table/Z*` on `dynamodb:LeadingKeys` no longer applies to a request whose partition key values include one that is not an ARN, and `ForAnyValue:ArnLike` with `j*` on the same key no longer holds for the partition key value `junk`.

A policy whose Arn values are literal ARNs, applied to a context value that is an ARN, evaluates as before.

---

### v0.1.15: IAM tag condition keys apply to TagResource, UntagResource and CreateTable

**What changed.** `aws:TagKeys` and `aws:RequestTag/<key>` now resolve on the three operations that carry a tag set, as the AWS service authorization reference lists them. `TagResource` and `CreateTable` populate both keys from `Tags`: `aws:RequestTag/<key>` holds the value of the tag named `<key>`, and `aws:TagKeys` lists the tag keys in request order. `UntagResource` populates `aws:TagKeys` from `TagKeys` and leaves `aws:RequestTag/<key>` absent. Every other operation leaves both keys absent, `ListTagsOfResource` included, and so does a request whose tag list is empty. Before this release both keys were absent on every data-plane request, so a statement that named either key did not depend on the tags of a tag operation.

`aws:TagKeys` is a multivalued key. A bare operator on it, one with no `ForAnyValue:` or `ForAllValues:` prefix, never matches while the key is present, as for `dynamodb:Attributes` and `dynamodb:LeadingKeys`. Tag names in `aws:RequestTag/<key>` match without regard to case, under Unicode case folding. That folding also equates the sharp s with `ss`, which the service does not (measured 2026-10-27), so a `Deny` on `aws:RequestTag/STRASSE` applies to a tag named `Straße` on ExtendDB and not on the service, and an `Allow` gated on `aws:RequestTag/STRASSE` grants a request carrying `Straße` on ExtendDB where the service would not (inferred from the same measurement; the Allow direction was not run). A request that names one tag under two spellings that fold to one name, with differing values, fails the statement closed: a `Deny` that reads the key blocks and an `Allow` that reads it does not grant. A condition value `${aws:TagKeys}` stays literal text, because a multivalued key has no single value to put there.

**Who is affected.** Any deployment with a policy statement that names `aws:TagKeys` or `aws:RequestTag/<key>` in a condition: inline user, group and role policies, user and role permissions boundaries, and session policies. Role trust policies already resolved both keys from the session tags of the `AssumeRole` call. One trust-policy case changes: a condition value `${aws:TagKeys}` stays literal text there too, where it used to expand to one session tag key in hash order. A statement that was inert on tag operations now applies to `TagResource`, `UntagResource` and `CreateTable`:

- A `Deny` written to block a tag key or tag value now blocks the requests that carry it. `ForAnyValue:StringEquals` on `aws:TagKeys` and `StringEquals` on `aws:RequestTag/<key>` did not match before.
- An `Allow` gated on `ForAllValues:StringEquals` on `aws:TagKeys` used to grant for any tag keys, because the absent key satisfied it. It now grants only when every tag key in the request is in the list.
- `Null` with value `true` on `aws:TagKeys` matched every request before. It no longer matches a request that carries tag keys, and `Null` with value `false` now matches it.
- A bare negated operator on `aws:TagKeys` (`StringNotEquals`, `StringNotLike`) matched on the absent key before. It no longer matches a request that carries tag keys. It still matches every operation that leaves the key absent, as IAM does (measured with `iam simulate-custom-policy` on 2026-10-24), so an `Allow` gated on it stops granting the tag requests and keeps granting the other operations.
- A `Deny` gated on a bare negated operator on `aws:RequestTag/<key>` (`StringNotEquals`) matched on the absent key before, so it blocked every request its `Action` covered. It now stops blocking a `TagResource` or `CreateTable` request whose tag carries an allowed value, and still blocks a request that omits the tag.

No catalog migration is required. Before upgrading a deployment that restricts tagging, check the policies of each account for the statements above.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
