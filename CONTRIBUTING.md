# Contributing to ExtendDB

Thank you for your interest in contributing to ExtendDB!

## Getting Started

### Prerequisites

- Rust 1.88+ (stable)
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
 be reviewed, discussed and voted on. If your change is a new storage backend, it must also satisfy the
 [Backend Acceptance Criteria](docs/rfcs/0003-backend-acceptance-criteria.md).
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

## Contributing a storage backend

If you are adding a new storage backend, read these two documents before you
write code:

- [RFC-0002: Backend Plugin Policy](docs/rfcs/0002-backend-plugin-policy.md) is
  the *process*: mono-repo structure, trait-based conformance, and the
  acceptance and maintenance workflow.
- [Backend Acceptance Criteria](docs/rfcs/0003-backend-acceptance-criteria.md)
  is the *substance*: the concrete DynamoDB-behavioral invariants and
  implementation standards your backend must satisfy (item identity, secondary
  indexes, transactions, isolation, streams, condition expressions, query and
  scan, multi-tenancy, error fidelity, and operational correctness), plus the
  conformance stress tests reviewers will run.

## Adding a catalog migration

Every change to a catalog's schema, or to data the catalog already holds, is a
numbered migration. The numbers are the same on every backend. Each store
records the migrations it applied in a ledger table, `extenddb_migrations`,
and `extenddb migrate --yes` applies the missing ones in order.
[ADR-0023](docs/adr/0023-catalog-migrations.md) records the design.

SQLite, PostgreSQL, Cassandra and MongoDB migrate through this framework.

### Procedure

1. Add the id. Append an entry to the `migration_ids!` list in
   `crates/storage/src/migrations/registry.rs` with the next number, a
   snake_case name and a one-line description, which `extenddb migrate --list`
   prints. The list generates `MigrationId`, `MigrationId::ALL` and each id's
   name and description, and `MigrationId::LATEST` is its last entry. A number
   is never reused, reordered or removed.

2. Add an arm for the id to each backend's table, `migration()` in
   `crates/storage-sqlite/src/migrations/mod.rs`,
   `crates/storage-postgres/src/migrations/mod.rs`,
   `crates/storage-cassandra/src/migrations/mod.rs` and
   `crates/storage-mongodb/src/migrations/mod.rs`. All four are exhaustive
   `match`es with no wildcard arm, so the workspace does not compile until
   every table has the arm. The arm is one of:

   - `Migration::Applies { kind, convergent, bodies }`, with one body for each
     kind of store the migration changes: `StoreKind::Catalog`, on
     PostgreSQL and MongoDB also `StoreKind::Data`, and on Cassandra also
     `StoreKind::Account` (one body for every account keyspace). The runner applies the catalog body
     first, and it writes a catalog ledger row for every id, including an id
     whose only body runs in the data database.
   - `Migration::NotApplicable("reason")`, when the change has no effect on
     that backend. The reason says why. The catalog ledger records it as
     `not_applicable`, and it counts as additive and convergent.

   On PostgreSQL, also add the id to `runs_in()` in the same file. Every id
   runs in the runner's transaction (`RunsIn::Transaction`) except one whose
   code body builds indexes with `CREATE INDEX CONCURRENTLY`, which runs
   statement by statement (`RunsIn::Autocommit`).

3. Write each body, as a file or as code.

   A file body is SQL that the runner executes in one transaction with the
   store's ledger row. Name it `NNNN_<name>.sql`, where `NNNN` is the id in
   four digits and `<name>` is the id's name. PostgreSQL catalog bodies go in
   `crates/storage-postgres/migrations/` and data bodies in
   `crates/storage-postgres/data_migrations/`, and a SQLite file goes in
   `crates/storage-sqlite/migrations/`. The backend's arm in `mod.rs` embeds
   the file with the `file_body!` macro of `extenddb-storage`,
   `file_body!(store, "<directory>", "NNNN_<name>.sql")`, which names the
   file once; the directory starts at the backend crate's root, and both SQL
   backends use the same `file_body!`. The runner owns the transaction and writes the
   version, so a file must not contain a `BEGIN`, `COMMIT`, `END`,
   `ROLLBACK`, `SAVEPOINT` or `RELEASE` statement, and must not name
   `catalog_version` outside a comment. Write every statement with
   `IF NOT EXISTS` or `IF EXISTS`, so that a second run changes nothing.

   A code body is Rust, for what a file cannot express: a data rewrite, a
   step that depends on what the store holds, or an index build outside a
   transaction. Put it in its own module, `src/migrations/mNNNN_<name>.rs` in
   the backend's crate, and declare the module with `#[rustfmt::skip]` and
   `#[allow(clippy::all, deprecated)]`, so that neither tool can require an
   edit to a released body. The module defines:

   - `SOURCE`, the module's own text through `include_str!`, followed by the
     text of any SQL file the body executes. The checksum test pins its
     SHA-256.
   - `FINGERPRINT`, which the ledger records. It starts with `NNNN_<name>/`,
     which the checksum test checks: a backend's own body uses
     `"NNNN_<name>/<backend>/1"`, and a body shared by every backend, such as
     migration 2's, `"NNNN_<name>/1"`. A released body keeps its fingerprint.
   - An implementation of `CodeBody` (`crates/storage/src/migrations/code.rs`).
     `detect` is read-only and returns the items that would stop the body.
     The runner calls it for every pending code body before the first write
     of a run, and stops with exit 5 when any item comes back. `run` is
     idempotent and safe to stop at any point, and returns `Done { notes }`,
     which the ledger records and `migrate` and `verify` print.

   `detect` sees the stores as they are before the run's first pending
   migration, so it may read only objects that no earlier pending migration
   changes. A code body may call application code such as `ManagementStore`.
   A function that defines what the migration does belongs in the module,
   so that the module's checksum covers it, as `rewrite_policy_document` is in
   `crates/storage/src/migrations/m0002_resource_tag_condition_keys.rs`.

   Every code body has a golden test on each backend it runs on, apart from
   migration 1, which the runner checks against
   `0001_baseline.expected.json`. The test
   runs the body on a committed released fixture and compares what the body
   changed, and its ledger notes, with `tests/fixtures/golden/NNNN_<name>.txt`
   in the backend's crate. SQLite's golden tests are in
   `crates/storage-sqlite/src/migrations/golden_tests.rs`, and PostgreSQL's
   in `crates/storage-postgres/tests/migrations_golden.rs`. With
   `EXTENDDB_BLESS_EXPECTED=1` the test writes the golden file instead of
   comparing; read the file before you commit it.

   On MongoDB every body is code; the backend has no file bodies. A body
   sends every request through `MongoStores::request`
   (`crates/storage-mongodb/src/migrations/backend.rs`), which checks first
   that this process still holds the migration lock and waits at most 10 s
   for the answer, and every request must be safe to send again: the body
   is not in a transaction, and a migrator stopped part way runs it again.
   The runner writes the ledger row, and in the catalog the
   `catalog_version` row, in one transaction after the body. Accept an
   object that already exists only by the server's error code, such as
   NamespaceExists (48) from `create`, never by discarding a result. A body
   that reaches the catalog through `ManagementStore` gets one that checks
   the lock before each call. MongoDB's golden tests are in
   `crates/storage-mongodb/tests/migrations_released.rs`; they need a
   replica set, named by `EXTENDDB_TEST_MONGODB_URI`.

   On Cassandra a file body is CQL, `NNNN_<name>.cql` in
   `crates/storage-cassandra/migrations/catalog/` for the catalog keyspace or
   `crates/storage-cassandra/migrations/account/` for the account keyspaces,
   embedded with `file_body!` like the others. Cassandra has no transactions:
   the runner sends the statements one by one, checks before each that this
   process still holds the migration lock, and writes the ledger row at
   `QUORUM` after the body, so every statement must be safe to run again
   (`CREATE ... IF NOT EXISTS`, `ALTER TABLE ... ADD IF NOT EXISTS ...`). A
   statement is `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE`, `INSERT INTO`
   or `DELETE FROM`, naming its table without a keyspace: the runner writes
   the keyspace in front of the table (`crates/storage-cassandra/src/migrations/cql.rs`)
   and refuses a file with any other statement before it runs anything. After
   each `CREATE` and `ALTER` the runner waits until every live node reports the
   same schema version. The file is split on the semicolon character, so no
   comment may hold one. The golden tests are in
   `crates/storage-cassandra/src/migrations/tests.rs`; the
   `0001_baseline.expected.json` beside each body holds the definitions the
   runner compares the keyspace with the first time migration 1 is applied.

   On PostgreSQL, a body that runs `CREATE INDEX CONCURRENTLY` builds every
   index through `IndexHelper` in
   `crates/storage-postgres/src/migrations/index_helper.rs`. `ensure` drops
   an index that a cancelled build left invalid and builds it again, and
   `finish` checks that every index the body asked for is valid before the
   ledger row is written. `CREATE INDEX CONCURRENTLY IF NOT EXISTS` on its own
   skips an invalid index and leaves it invalid.

4. Declare the kind. A migration is `Kind::Additive` when the previous
   release reads and writes correctly against the result, and everything the
   previous release writes reads back through the new release the same as
   what the new release writes. Otherwise it is `Kind::Breaking`. New tables,
   nullable or defaulted columns and non-unique catalog indexes can be
   additive. A column that the new release needs to read back what it writes
   is breaking, unless the new release derives the value when it is missing.
   Unique indexes and constraints, and changes to per-table or per-account
   objects, are never additive. A 1.x binary serves a catalog whose unknown
   migrations are all additive, and refuses one with a breaking migration it
   does not know (exit 4), so the kind decides whether a rollback can run the
   previous binary or has to restore a backup.

   Declaring `Kind::Additive` on a backend requires the old-binary job to
   pass on that backend. In that job the previous release's binary and its
   wire tests run against a catalog the new binary migrated, with writes from
   both binaries interleaved, and every object the old binary writes is read
   back through the new binary and compared with the same object written by
   the new binary. The job does not exist yet; the first PR that declares an
   additive migration adds it, and deletes the tests that refuse
   `Kind::Additive` until then (`no_migration_is_additive_before_the_old_binary_job`
   in the SQLite crate and in `crates/storage-cassandra/src/migrations/tests.rs`,
   its PostgreSQL counterpart in `crates/bin`, and
   `no_migration_is_declared_additive_before_the_old_binary_job_exists` in
   the MongoDB crate).

   Set `convergent: true` when every body is safe to run again on a store at
   any later migration. `extenddb migrate --yes --reconverge`, which repairs a
   catalog that a release before 1.0 wrote to, runs only convergent
   migrations, and refuses a catalog that has one that is not.

5. Handle per-table objects. CreateTable and CreateAccount create objects at
   request time with the binary's own DDL: the `_ddb_<id>` tables and their
   indexes on PostgreSQL, the index tables on SQLite, the `_ddb_<id>`
   collections and their indexes on MongoDB, and on Cassandra the
   `items_<table>` tables and each account's keyspace. Their shape depends on
   the release that created them. A migration that changes them is breaking.
   It lists the objects from the catalog rows, never by name in the data
   store, where PostgreSQL's vector tables and MongoDB's index collections
   share the `_ddb_` prefix. It skips
   a catalog row whose table is `DELETING` or whose physical table is
   missing, and adds a note for it. The same PR changes the DDL that creates
   new objects, in `crates/storage-sqlite/src/data/ddl.rs`,
   `crates/storage-postgres/src/data/ddl.rs`,
   `crates/storage-cassandra/src/data/ddl.rs` and
   `crates/storage-mongodb/src/table_engine.rs`. Migration 4,
   `m0004_index_table_base_key.rs` in the SQLite and PostgreSQL crates'
   `src/migrations/`, does this.

6. Commit the current schema. Each backend's integration job regenerates
   `docs/schema/<backend>.txt` from a fresh install. When the result differs
   from the committed file, the job fails, prints the diff and uploads the
   regenerated file as an artifact. Commit that file with the migration; it
   is where a reader finds a backend's current schema. To regenerate it
   locally, dump a fresh install with `devtools/schema-dump`: `devtools/schema-dump
   sqlite <database file>`, or `devtools/schema-dump postgres <base URL>
   <catalog db> <data db>` for a deployment `init` created on that server.
   The dump lists the kinds of object `devtools/schema-dump`'s docstring
   names; a migration that creates another kind adds a query for it there.
   On MongoDB, `devtools/check-mongodb-schema <extenddb binary> <output file>`
   runs `init` on a replica set of its own, writes the schema to the output
   file and compares it with `docs/schema/mongodb.txt`.

7. Leave the checksum literals to the release. Each backend's `RELEASED`
   table pins the checksum, kind and convergent flag of every released body,
   which is every migration at or below `RELEASED_THROUGH` in
   `crates/storage/src/migrations/registry.rs`. The test
   `the_released_bodies_match_their_checksums` refuses a released body that
   changed. A migration above `RELEASED_THROUGH` carries no literal, so its
   body can change during review with nothing to paste. The release PR that
   raises `RELEASED_THROUGH` to the last id pastes the lines the test
   prints; the release workflows' gate jobs run
   `devtools/check-released-through` on the tagged commit and refuse a tag
   whose bound is below the last id:

   ```bash
   cargo test -p extenddb-storage-sqlite --lib the_released_bodies -- --nocapture
   cargo test -p extenddb-storage-postgres --lib the_released_bodies -- --nocapture
   cargo test -p extenddb-storage-mongodb --lib the_released_bodies -- --nocapture
   ```

   A released body does not change. If one has to, for example because a
   dependency upgrade changes a code body's source, add a second `RELEASED`
   entry for it with the new checksum and `rebless: Some("<reason>")`. A
   code body keeps its fingerprint, and its golden test shows that its output
   did not change. A later migration leaves migration 1's file and
   `0001_baseline.expected.json` as they are: they define the v1.0 schema,
   which the runner checks a store against the first time migration 1 runs on
   it.

8. Clear a changed migration from your development database. When a
   migration's body changes after it ran on your database, `extenddb migrate`
   reports `Changed` and exits 5. `extenddb migrate --forget <id>` deletes
   that migration's ledger rows, and the next `extenddb migrate --yes` runs
   it again. `--forget` refuses a migration at or below `RELEASED_THROUGH`.

### Example: a nullable column on `tables`

Migration 5, `table_note`, adds a nullable `note TEXT` column to the catalog's
`tables` table on SQLite and PostgreSQL.

In `crates/storage/src/migrations/registry.rs`, at the end of the
`migration_ids!` list:

```rust
    /// Adds a nullable `note` column to the `tables` catalog table.
    TableNote = 5, "table_note",
        "add a nullable note column to the tables catalog";
```

PostgreSQL has `ADD COLUMN IF NOT EXISTS`, so its body is a file,
`crates/storage-postgres/migrations/0005_table_note.sql`:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 5, table_note, PostgreSQL catalog database: a nullable note
-- column on tables. IF NOT EXISTS makes a second run change nothing.

ALTER TABLE tables ADD COLUMN IF NOT EXISTS note TEXT;
```

In `crates/storage-postgres/src/migrations/mod.rs`, the arm in `migration()`:

```rust
        MigrationId::TableNote => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations",
                "0005_table_note.sql"
            )],
        },
```

and the id in the transactional arm of `runs_in()`:

```rust
        MigrationId::Baseline
        | MigrationId::ResourceTagConditionKeys
        | MigrationId::SortKeyDefinitions
        | MigrationId::TableNote => RunsIn::Transaction,
```

SQLite has no `ADD COLUMN IF NOT EXISTS`, and a file with a plain
`ADD COLUMN` fails when it runs a second time, so the SQLite body is code
that checks for the column first.
`crates/storage-sqlite/src/migrations/m0005_table_note.rs`:

```rust
// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 5, `table_note`, SQLite: a nullable `note` column on `tables`.
//!
//! SQLite has no `ADD COLUMN IF NOT EXISTS`, so the body reads the table's
//! columns first and adds the column only when it is missing. A second run
//! changes nothing, which makes the migration convergent.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};

use super::SqliteStores;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0005_table_note.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0005_table_note/sqlite/1";

pub(crate) struct TableNote;

#[async_trait]
impl CodeBody<SqliteStores> for TableNote {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        let has_note: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('tables') WHERE name = 'note')",
        )
        .fetch_one(&stores.pool)
        .await
        .map_err(|e| MigrationError::store("read the columns of tables", e))?;
        if !has_note {
            sqlx::query("ALTER TABLE tables ADD COLUMN note TEXT")
                .execute(&stores.pool)
                .await
                .map_err(|e| MigrationError::store("add tables.note", e))?;
        }
        Ok(Done::default())
    }
}
```

In `crates/storage-sqlite/src/migrations/mod.rs`, the module declaration
beside the others:

```rust
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0005_table_note;
```

and the arm in `migration()`:

```rust
        MigrationId::TableNote => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0005_table_note::TableNote,
                m0005_table_note::SOURCE,
            )],
        },
```

The golden test, at the end of
`crates/storage-sqlite/src/migrations/golden_tests.rs`, runs the migrations
on the fixture of the v0.1.14 dev image and records the new column and the
rows of `tables`:

```rust
#[tokio::test]
async fn migration_5_adds_the_note_column_to_tables() {
    let db = temp_db("m5");
    load_dump(&db, DEV_0_1_14).await;
    run(&db).await.expect("migrate");
    let mut text = dump(
        &db,
        "SELECT name, type, \"notnull\", dflt_value FROM pragma_table_info('tables') \
         WHERE name = 'note'",
    )
    .await;
    text.push_str(&dump(&db, "SELECT table_name, note FROM tables ORDER BY table_name").await);
    golden("0005_table_note", &text);
}
```

`EXTENDDB_BLESS_EXPECTED=1 cargo test -p extenddb-storage-sqlite --lib migration_5`
writes `crates/storage-sqlite/tests/fixtures/golden/0005_table_note.txt`, one
tab-separated line per row under each query:

```
-- SELECT name, type, "notnull", dflt_value FROM pragma_table_info('tables') WHERE name = 'note'
note	TEXT	0	NULL
-- SELECT table_name, note FROM tables ORDER BY table_name
cu_orders	NULL
```

The PostgreSQL body is a file, so it has no golden test.

The example declares `Kind::Breaking`. A nullable column can be additive by
the rule in step 4, but declaring it additive needs the old-binary job, which
this example does not add. As a breaking migration it makes a 1.x binary
without migration 5 refuse a catalog that has it, and a rollback restores the
backup.

The same PR changes the code that reads and writes `tables.note` in each
backend's catalog store, and commits the `docs/schema/sqlite.txt` and
`docs/schema/postgres.txt` that the integration jobs regenerate. It changes
no per-table DDL, because `tables` is a catalog table, and adds no `RELEASED`
entry. `extenddb migrate --list` then shows migration 5, and on a catalog at
migration 4 `extenddb migrate` lists migration 5 as pending and exits 3.

### Running the migration tests

The framework's and SQLite's tests need no database:

```bash
cargo test -p extenddb-storage -p extenddb-storage-sqlite
```

They cover the checksum rules, the golden tests, an upgrade of every
committed released SQLite catalog compared with a fresh install, and a
migrator killed inside a body and then run again.

PostgreSQL's unit tests need no database either:
`cargo test -p extenddb-storage-postgres --lib`. Its migration integration
tests need a PostgreSQL server and a role that can create databases; each
test creates its own databases and drops them. The vector fixtures need the
pgvector package, which the `pgvector/pgvector:pg16` image that CI uses has:

```bash
docker run -d --name extenddb-test-pg -p 127.0.0.1:5432:5432 \
    -e POSTGRES_PASSWORD=devpass pgvector/pgvector:pg16
export EXTENDDB_TEST_PG_CONNECTION_STRING=postgresql://postgres:devpass@127.0.0.1:5432
cargo test -p extenddb-storage-postgres --tests
docker rm -f -v extenddb-test-pg
```

`--tests` runs every target of the crate, as CI does. The connection
string names the server, with no database. Without
`EXTENDDB_TEST_PG_CONNECTION_STRING` each test prints a skip line and
passes, so check the output for skips; CI sets `EXTENDDB_TEST_PG_REQUIRED=1`,
which turns a missing connection string into a failure. `EXTENDDB_TEST_PGVECTOR_CONNECTION_STRING`
points the vector fixtures at a second server when the first has no
pgvector. `migrations_tag_blobs` needs the release tags in your clone
(`git fetch --tags`).

Two scripts check SQLite end to end with a release binary, as CI's
`run-integration-sqlite` job runs them:

```bash
cargo build --release -p extenddb --no-default-features --features sqlite
devtools/check-sqlite-catalog-lock ./target/release/extenddb 18450
docker pull extenddb/extenddb-dev:0.1.14
devtools/check-sqlite-older-binary ./target/release/extenddb \
    crates/storage-sqlite/tests/fixtures/released/dev-image-0.1.14.sql \
    extenddb/extenddb-dev:0.1.14 18451
```

`check-sqlite-catalog-lock` checks that `migrate --yes` and `serve` exclude
each other, that the `migrate` preview and `verify` run beside a server,
that `verify` exits 3 on a catalog behind the binary, and that
`migrate --yes` refuses a missing database without creating it.
`check-sqlite-older-binary` runs the last 0.1.x dev image against catalogs
this binary migrated, and checks that the binary then reports
`WrittenByOlderRelease` and that `extenddb migrate --yes --reconverge`
restores `Current`; with `DEV_BIN` set to a `sqlite,dev-mode` build it also
checks the repair a dev server without a config file advises.

Two scripts check PostgreSQL the same way, as CI's `run-integration` job
runs them, against a server whose superuser URL (with no database) they are
given:

```bash
cargo build --release -p extenddb
devtools/check-postgres-catalog-lock ./target/release/extenddb \
    postgresql://postgres:devpass@127.0.0.1:5432 18452
docker pull extenddb/extenddb-postgres:0.1.14
devtools/check-postgres-older-binary ./target/release/extenddb \
    postgresql://postgres:devpass@127.0.0.1:5432 \
    crates/storage-postgres/tests/fixtures/released/0.1.14 \
    extenddb/extenddb-postgres:0.1.14 18453
```

`check-postgres-catalog-lock` checks the migration lock and the serving
registration between `serve` and `migrate`; `check-postgres-older-binary`
runs the 0.1.14 image against catalogs this binary created and migrated.
With `POOLER_URL` set to a connection pooler in front of the same server
(`postgresql://127.0.0.1:6432` for the PgBouncer that CI starts in
transaction mode, logging roles in through `auth_query` as the superuser),
`check-postgres-catalog-lock` also runs `verify`, the preview,
`migrate --yes`, `--reconverge`, `--forget` and `serve` through it; without
it the script prints a skip line. `check-postgres-older-binary` names the
image's containers `$CONTAINER` (default `extenddb-older-binary`).

The four scripts are Python and import `devtools/catalog_checks.py`, which
runs a command with a time limit, checks its exit code and prints one `ok`
or `FAIL` line per check; a new check script imports it too.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
