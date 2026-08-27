# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

ExtendDB is at catalog version 0.1.0. Schema migrations for both PostgreSQL databases (the catalog and the data database) are handled by sqlx's built-in migrator (`sqlx::migrate!`). Each database records applied migrations, with a per-file checksum, in a table sqlx manages called `_sqlx_migrations`.

Adopting sqlx is a one-time breaking change: an existing catalog created by an earlier build has no `_sqlx_migrations` table and cannot be upgraded in place. Upgrading to 0.1.0 requires `destroy` + `init`, which drops both databases. This is acceptable at v0.1 with no dependent catalogs. See [ADR-0003](../adr/0003-catalog-migration-mechanism.md) for the decision. Later migrations are additive and do not lose data.

## How Catalog Upgrades Work

### The Migration System

Migrations are SQL files under two directories, applied in filename order:

```
crates/storage-postgres/migrations/       ← catalog database
  001_schema.sql
crates/storage-postgres/data_migrations/   ← data database
  001_data_schema.sql
  002_gsi_pending.sql
  003_idempotency_account_scope.sql
```

sqlx embeds these files into the binary at compile time. When `extenddb init` or `extenddb migrate` runs a migrator, sqlx:

1. Creates the `_sqlx_migrations` table if it does not exist.
2. Reads the version and a checksum of each embedded file.
3. Applies any file not yet recorded, in version order, each in its own transaction.
4. Verifies that every already-applied file still matches its recorded checksum.

Step 4 is the safety net. If a file that already shipped is edited after it was applied, sqlx refuses to run and reports `migration <n> was previously applied but has been modified`. Editing a shipped migration is a hard error, not a silent no-op. A new schema change is always a new numbered file.

Running `extenddb migrate` on an up-to-date deployment applies nothing, but still runs both migrators so the checksum check above executes. That is deliberate: it catches an edited migration file even when no new migration is pending.

### The Catalog Version

A single row in the `settings` table stores the catalog version:

```sql
SELECT value FROM settings WHERE key = 'catalog_version';
-- '0.1.0'
```

sqlx has no knowledge of our semver, so the version is not written by a migration file. `init` and `migrate` write it in a separate step, right after the catalog migrator runs, from the compiled-in `CATALOG_VERSION` constant (`crates/storage-postgres/src/lib.rs`). This write is intentionally not atomic with the migration. If a crash lands between the migration and the version write, re-running `extenddb migrate` repairs the version (sqlx skips the already-applied migration). On a first-time `init`, the same crash leaves no version and no config file, so recovery there is `destroy` + `init`.

At startup the server compares the stored `catalog_version` against the binary's `CATALOG_VERSION`. If they do not match exactly, the server refuses to start and directs the operator to run `extenddb migrate`. The gate is symmetric: an older binary against a newer catalog also refuses. It exists to stop a server serving a schema it was not built for. The data database has no separate version; it relies on its own `_sqlx_migrations` table.

### Version Semantics

The catalog version follows semantic versioning:

- **MAJOR**: Breaking schema changes that may require data migration or downtime.
- **MINOR**: New tables or columns (backward-compatible, additive).
- **PATCH**: Index changes, constraint fixes, seed data updates.

Pre-1.0, the project is unstable and a breaking change rides a MINOR bump (MAJOR stays 0), per standard semver. The 0.1.0 sqlx adoption is such a case: a MINOR bump that is breaking (requires re-init).

## Writing a New Migration

### Catalog migration

1. Add a new SQL file with the next sequence number:

```
crates/storage-postgres/migrations/002_your_feature.sql
```

Do not wrap it in `BEGIN`/`COMMIT`. sqlx runs each migration in its own transaction. Write plain DDL:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 002: Brief description of what this adds.

ALTER TABLE tables ADD COLUMN IF NOT EXISTS new_column TEXT;
```

Do not write the catalog version here. The migration runner writes it after the migrator completes.

2. Bump the catalog version constant in `crates/storage-postgres/src/lib.rs`:

```rust
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 2, 0);
```

3. Update the tripwire test in `crates/storage-postgres/src/migrations.rs` so `EXPECTED_CATALOG_MIGRATIONS` and `EXPECTED_CATALOG_VERSION` match the new count and version. The test fails if a migration is added without a matching version decision.

**Do not edit `001_schema.sql` or any file that already shipped.** sqlx checksums file bytes; changing an applied file is a hard error. A new column or table is always a new numbered file. sqlx applies all numbered files in order on a fresh `init`, so a new file reaches fresh installs and existing deployments through the same path.

### Data migration

A data-schema change is a new file under `crates/storage-postgres/data_migrations/` (for example `004_your_change.sql`), plus a bump of `EXPECTED_DATA_MIGRATIONS` in the tripwire test. The data database has no version, so `CATALOG_VERSION` does not change. `extenddb migrate`, not just `init`, runs the data migrator, so existing deployments pick up the change.

### Code migration

A change that cannot be static SQL, because it enumerates dynamically-named tables (the `_ddb_*` index tables) from the catalog or must run outside a transaction (`CREATE INDEX CONCURRENTLY`), is a programmatic "code" migration: a Rust step in `DATA_CODE_MIGRATIONS` (`crates/storage-postgres/src/migrations.rs`), tracked in the data database's `code_migrations` ledger and applied by `init` and `migrate` after the SQL migrations. Add the step name to `DATA_CODE_MIGRATIONS`, implement its match arm, and bump `EXPECTED_DATA_CODE_MIGRATIONS` in the tripwire test. Code migrations have no file checksum; the code itself is reviewed and version-controlled, and each step must be idempotent (safe to re-run).

### Design Considerations

**Additive only.** Prefer new columns with defaults and new tables over dropping or renaming. There are no down migrations; a mistake is corrected by a new forward migration.

**Idempotent DDL.** Use `IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS`. sqlx will not re-run an applied file, but idempotent DDL is a cheap safeguard.

**Transactions.** sqlx wraps each migration in a transaction. Do not add `BEGIN`/`COMMIT`. A statement that cannot run inside a transaction (for example `CREATE INDEX CONCURRENTLY`) needs a `-- no-transaction` directive on the first line of that migration file; use it only when a specific statement requires it.

**Line endings.** `.gitattributes` pins `*.sql text eol=lf` so a contributor's line-ending rewrite does not change file bytes and trip a false checksum mismatch. Keep migration files LF.

**Rebuild after editing migration files.** sqlx embeds the files at compile time. `cargo` re-embeds on a content change, but a restored file with an old modification time (for example, from `mv`-ing a backup over it) is skipped by the build cache: `touch` the file or run a clean build so the binary matches the files on disk.

**No data backfill in DDL files.** If a schema change requires backfilling rows, do it in Rust triggered by `extenddb migrate`, not in raw SQL, so you get error handling and batching.

## General Upgrade Procedure

### Upgrading to 0.1.0 (adopting sqlx)

This upgrade is breaking. There is no in-place path from a pre-sqlx catalog.

1. **Stop the server**

```bash
extenddb stop --config extenddb.toml
```

2. **Destroy and re-initialize** (drops both databases; all tables and items are lost and must be recreated)

```bash
extenddb destroy --config extenddb.toml --yes
extenddb init --config extenddb.toml
```

3. **Start the server**

```bash
extenddb serve --config extenddb.toml
```

### Later releases (additive migrations)

For future releases that add migrations without a compatibility break:

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
extenddb migrate --config extenddb.toml --yes
```

5. **Verify and start**

```bash
extenddb verify --config extenddb.toml
extenddb serve --config extenddb.toml
```

The server binary and the catalog are version-locked, so a schema-bumping upgrade needs every server moved to the matching version together: a brief coordinated outage (stop old servers, `migrate`, start new). Online / rolling upgrades are out of scope.

## Rollback Procedure

If a migration dies mid-apply, sqlx rolls back that migration's transaction completely, so it leaves no partial state and no ledger row: re-running `extenddb migrate` simply retries it. (A migration is only marked dirty, blocking further runs until resolved, if it opts out of the transaction with a `-- no-transaction` directive, which none of ours do.) If an upgrade otherwise fails:

1. Stop the server.
2. Restore from backup:

```bash
psql -c "DROP DATABASE extenddb_catalog;"
psql -c "CREATE DATABASE extenddb_catalog OWNER extenddb;"
psql -d extenddb_catalog -f catalog_backup_YYYYMMDD.sql
```

3. Rebuild the previous version and start it.

## Version History

> This section is the project changelog: each catalog version and what changed.

### Catalog 0.1.0 (Current)

Adopted sqlx's migrator for both databases, replacing the homegrown filename-tracked runner. Each database now tracks applied SQL migrations, with checksums, in `_sqlx_migrations`; the old `schema_history` table is gone. Programmatic code migrations are tracked in the data database's `code_migrations` ledger. `extenddb migrate` runs both the catalog and data migrators. The catalog version is written by `init` and `migrate` after the migrator runs, not seeded inside a migration file.

Breaking: upgrading from a pre-sqlx catalog requires `destroy` + `init`, which wipes both databases. Acceptable at v0.1 with no dependent catalogs.

### Catalog 0.0.2 (Initial release, superseded)

Complete initial schema: accounts, tables, indexes, tags, streams, IAM (users, groups, roles, policies, access keys, sessions, permissions boundaries), idempotency tokens, metrics, login attempts, backups, continuous backups, TTL support, settings. Managed by the homegrown runner and its `schema_history` table.

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
