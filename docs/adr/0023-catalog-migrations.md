# ADR-0023: One list of catalog migrations for every backend, with a ledger in every store

- Status: Accepted
- Date: 2026-10-29
- Deciders: @LeeroyHannigan

## Context

A deployment's catalog holds its table and index definitions, accounts, IAM
principals and policies, backups and settings. Each backend keeps it in its
own stores: PostgreSQL in a catalog database and a data database, SQLite in
one database file, Cassandra in a catalog keyspace and one keyspace per
account, and MongoDB in a catalog database and a data database. Deployments
created by 0.1.x releases hold data, and each new release upgrades their
catalogs in place. The code references in this section are to main at
`cf321f05`, before this decision was implemented.

Each backend migrated its stores its own way:

- PostgreSQL applied numbered SQL files and recorded each one by file name,
  with no checksum (`crates/storage-postgres/src/migrations.rs:480-506`).
  `migrate` and `serve` also ran shape steps that no file held, and a code
  migration added an index to every index table.
- SQLite applied one schema file, `SCHEMA_SQL`, again on every upgrade, then
  ran converge steps for the changes `CREATE TABLE IF NOT EXISTS` cannot make.
- Cassandra applied numbered CQL files after a `USE` on the shared session, so
  a statement could run in another keyspace (#632).
- MongoDB created its collections and indexes in Rust code during `init`. It
  wrote a ledger document that nothing read, and no step changed the data
  database after `init`.

These mechanisms failed in the following ways:

- The same change had a different number on each backend. PostgreSQL's
  catalog file 003 and Cassandra's V005 make the same change. The version
  strings moved at different releases: SQLite reached schema version 0.0.3 at
  v0.1.7, PostgreSQL reached it at v0.1.11, and MongoDB stayed at 0.0.2.
- Released files were edited in place, and no check compared a file with the
  one that had been applied. PostgreSQL's `001_schema.sql` changed for v0.1.2,
  v0.1.7 and v0.1.14, and Cassandra's V001 changed once. The runner skips a
  file it has recorded, so the v0.1.2 edit, which added `table_class`,
  `sse_specification` and `on_demand_throughput` to `tables`, never reached
  catalogs that v0.1.0 or v0.1.1 created. Measured on catalogs built from the
  SQL files of those two tags: `migrate --yes` built from `cf321f05` exits 0,
  `verify` reports the catalog healthy, and every CreateTable then fails with
  `column "table_class" of relation "tables" does not exist`.
- ADR-0003 recorded that failure and chose `sqlx::migrate` for PostgreSQL,
  with `destroy` and `init` as the upgrade path. It was not implemented. Main
  still ran the runner above, the workspace `sqlx` dependency has no `migrate`
  feature (`Cargo.toml:69`), `.gitattributes` has no rule for `*.sql`, and
  upgrades ran in place through `extenddb migrate`.
- The ledgers and the `catalog_version` settings row could disagree, and
  `migrate` decided from the row. The files wrote the version themselves, so
  running Cassandra's V001 again reset the row and overwrote settings an
  operator had changed.
- SQLite's re-applied file could not change a table that existed. Every
  column change needed a hand-written converge step, and SQLite index tables
  created by v0.1.3 to v0.1.7 never got the base key index that CreateTable
  adds to new ones. Re-applying the file also inserted the resource-tag marker
  row before the policy rewrite ran (`crates/storage-sqlite/src/schema.rs:460`,
  `crates/app/src/cmd_migrate.rs:305-307` and `331`), so a rewrite that
  stopped at a collision left a catalog that `serve` accepted.
- MongoDB and Cassandra took no migration lock
  (`crates/storage/src/bootstrapper.rs:107-127`), and nothing stopped
  `migrate` while a PostgreSQL, MongoDB or Cassandra server was running.
- On a Cassandra cluster of more than one node, a statement after a
  `CREATE TABLE` can reach a node that does not have the table yet. With two
  `cassandra:4.1` nodes and statements sent round robin, ten runs of the
  migrations at `cf321f05` failed nine times with one connection per node and
  ten times with ten connections per node (#637).
- The startup check differed by backend. PostgreSQL and SQLite refused any
  mismatch and told the operator to run `migrate`, which is the wrong advice
  for a catalog newer than the binary. MongoDB and Cassandra never read the
  version.
- No test started from a catalog that a released binary created, and no test
  compared an upgraded catalog with a fresh one.

v1.0 supports one serving instance per catalog (`README.md:141`). Upgrades
have to run in place: `destroy` drops the databases with their items, and
ADR-0003 accepted that only while no deployment depended on a catalog.

## Options Considered

1. One numbered list of migrations for every backend, a ledger in every
   store, and one runner that each backend implements (the Decision below).
2. Numbering each backend's migrations separately, as Temporal and
   Cassandra Reaper do. Each backend would keep its own directory and
   sequence, and a change would be written once per backend under that
   backend's next number.
3. A desired-state schema file per backend, applied by a tool that diffs it
   against the live store.
4. sqlx's migrator, which ADR-0003 chose. sqlx records a checksum for each
   migration and commits the ledger row in the migration's transaction, and
   this design keeps both properties; sqlx ran migrations again while the two
   committed separately
   ([sqlx#1966](https://github.com/launchbadge/sqlx/issues/1966), fixed by
   #1991).
5. Destroy and init, the upgrade path ADR-0003 chose. `destroy` drops the
   databases, items included, and `init` creates new ones.

## Decision

Number every catalog migration once for all backends, record each one a
store applies in that store's ledger, and apply the list with one runner
that each backend implements, as follows.

1. Every migration has one number, the same on every backend. The list is
   `MigrationId` in `crates/storage/src/migrations/registry.rs`, written as
   one `migration_ids!` invocation that generates the enum, `MigrationId::ALL`
   and each id's name and description; `MigrationId::LATEST` is the last
   entry of `ALL`. Each backend has a table in its `src/migrations/mod.rs`,
   an exhaustive `match` from `MigrationId` to that backend's entry with no
   wildcard arm, so a backend without an entry for an id does not compile.
   An entry gives a body for each kind of store the migration changes, its
   kind (`Additive` or `Breaking`) and whether it is convergent, meaning safe
   to run again on a store at any later id. An entry can instead be
   `NotApplicable` with a reason that says why the change has no effect on
   that backend. A body is a SQL or CQL file embedded with `include_str!`, or
   a Rust code body with a read-only `detect` step and an idempotent `run`
   step. Kong defines its migrations the same way, one module per migration
   with a required body for each of its stores.

2. Every store has a ledger, `extenddb_migrations`, with a checksum for each
   migration it applied. Its columns are `id`, `name`, `kind`, `checksum`,
   `outcome` (`applied` or `not_applicable`), `notes`, `applied_at` and
   `applied_by`. The runner applies ids in order and, for each id, the body of
   every store the id changes, catalog store first. On PostgreSQL and SQLite a
   body and its ledger row commit in one transaction, except a PostgreSQL
   body that must run outside one (migration 4's `CREATE INDEX
   CONCURRENTLY`), whose row is written after the body completes. On Cassandra and MongoDB
   every statement of a body is idempotent and the row is written after the
   body completes, so after a crash the next run executes the body again. The
   catalog ledger has a row for every id, `NotApplicable` ones included, and
   is the deployment's record.

   A file body's checksum is the SHA-256 of its text with CRLF normalised to
   LF. A code body records its fingerprint, a constant of its module that
   changes only with a new migration. A recorded checksum that differs from
   the binary's stops `migrate` and `serve` with exit 5. A released body does
   not change. Each backend's `RELEASED` table pins the checksum, kind and
   convergent flag of every body at or below `RELEASED_THROUGH`, and a unit
   test refuses a changed one unless its entry carries a `rebless` reason,
   which a reviewer sees in the diff. The release PR that raises
   `RELEASED_THROUGH` pastes the lines that test prints, so a migration under
   review carries no checksum literal.

3. The catalog version is read from the ledgers. It is the highest id such
   that it and every lower id have a row in the catalog ledger and in every
   store their bodies touch, so it is one integer, the same on every backend.
   No file or constant carries a version string. The runner also writes the
   integer to the `catalog_version` settings row with each catalog ledger row,
   because releases before 1.0 read that row: the v0.1.14 PostgreSQL binary's
   `serve` refuses the integer with `invalid catalog version '4': expected 3
   dot-separated components`, its `verify` reports a failed check, and its
   `migrate --yes` accepts it and prints `4 -> 4`. Two decisions in 1.0 read
   that row: the check for a catalog that an older release wrote to
   (decision 6), and whether a store with no ledger rows is a catalog at all
   (one with neither is `Uninitialized`).

4. Migration 1 is one convergent body per store. It creates the v1.0 schema
   in an empty store and brings every released catalog to the same schema.
   Fresh installs and upgrades run the same body, and it is recorded as
   `applied` in both cases. Nothing identifies the legacy state first, so a
   store in a state nobody listed converges the same way. Changes after
   migration 1 are later numbered migrations, because a file that is edited
   for every change cannot upgrade a store that already exists.

   - PostgreSQL: `crates/storage-postgres/migrations/0001_baseline.sql` and
     `crates/storage-postgres/data_migrations/0001_baseline.sql` hold the
     legacy files' statements in order. Every `CREATE INDEX` is guarded by
     `IF NOT EXISTS` and keeps its legacy name, the three `tables` columns that
     catalogs from v0.1.0 and v0.1.1 lack are added with
     `ADD COLUMN IF NOT EXISTS`, the shape steps that `migrate` and `serve`
     ran are in the file, and the sequence reset never lowers a live sequence.
   - SQLite: a code body runs `crates/storage-sqlite/migrations/0001_baseline.sql`
     and then the two steps that SQLite cannot make conditional in SQL. It
     rebuilds `backup_items` only when the table has no `id` column, and adds
     `table_restores.source_table_arn` only when the column is missing.
   - Cassandra: one CQL file for the catalog keyspace and one for the account
     keyspaces, with every statement `IF NOT EXISTS`.
   - MongoDB: code bodies for the catalog and data databases, which accept an
     existing collection or index only by its error code.

   The first time migration 1 is applied to a store, the backend's apply step
   compares the store's definitions with `0001_baseline.expected.json`, which is generated
   by running migration 1 on an empty store and committed beside the body.
   The comparison runs inside migration 1's transaction, so on a difference
   the runner commits nothing and names the object. It ignores objects the
   file does not name, because the same stores hold per-table objects that
   requests create.

   Migration 1 seeds no settings rows, because every seeded setting has an
   equal default in code; on PostgreSQL `init`, its catalog transaction
   writes the data database's connection rows. It deletes the `resource_tag_migration` marker row
   that every released baseline seeded. It records every legacy file in the
   legacy ledgers (the `schema_history` tables on PostgreSQL and Cassandra,
   the `001_initial` document on MongoDB), which stay and are never read
   again, so a released 0.1.x `migrate --yes` finds every legacy file recorded
   and runs none of them. Without those rows it replays them. Measured with
   the v0.1.14 image against a PostgreSQL catalog whose data database had no
   legacy ledger: its `migrate --yes` replayed data file 003, which drops and
   recreates `idempotency_tokens`, and a token row planted beforehand was
   gone.

5. Every backend has a migration lock, and a serving registration keeps
   `migrate --yes` and `serve` apart. Each side takes its own before it looks
   for the other, so two that start together cannot both miss each other.

   | Backend | Migration lock | Serving registration |
   |---|---|---|
   | PostgreSQL | A transaction-level advisory lock on the catalog database, on the same key that every release since v0.1.3 takes, held on a dedicated connection that runs `SELECT 1` every 15 s | `serve` holds a second advisory key in shared mode, on a connection whose TCP keepalive settings end the session within 30 s of the serving host dying; `migrate --yes` takes that key in exclusive mode |
   | SQLite | `flock` on `<db>.lock`, retried every 250 ms up to the lock timeout | The same `flock`, which `serve` holds while it runs |
   | Cassandra | A row in the catalog's `settings` table, written only by lightweight transactions at `SERIAL`, with a 60 s TTL renewed every 15 s | A row per server in `extenddb_servers`, renewed every 15 s with a 45 s TTL |
   | MongoDB | A document in the `settings` collection, claimed by a conditional update with majority write concern and renewed by its owner | A document per server in `extenddb_servers` that expires 45 s after its last renewal |

   The Cassandra row's values are in
   `crates/storage-cassandra/src/migrations/locks.rs` (the lock) and
   `crates/storage-cassandra/src/migrations/registrations.rs` (the
   registration); the MongoDB row's are in
   `crates/storage-mongodb/src/migrations/locks.rs`.

   On PostgreSQL there are two layouts in which the serving key does not
   match the servers that run, and the upgrade manual's step that stops
   every server before an upgrade covers both. Behind PgBouncer, the serving
   session's keepalives (`tcp_keepalives_idle` 10, `tcp_keepalives_interval`
   5, `tcp_keepalives_count` 4, so 30 s) run on the database's connection to
   PgBouncer, which stays up when a serving host dies: the dead host's key
   stays until PgBouncer closes its connection, which by PgBouncer's
   defaults (the operating system's, two hours idle on Linux) takes hours,
   and PgBouncer's `tcp_keepalive = 1`, `tcp_keepidle = 10`,
   `tcp_keepintvl = 5` and `tcp_keepcnt = 4` give the same 30 s. After a
   failover of the catalog's server, the new primary holds no serving key,
   while a server that served through the old primary goes on serving until
   its registration counts as lost, at most 30 s after its last renewal, so
   a migration can run beside it for up to 30 s.

   `migrate --yes` waits up to `--lock-timeout` (60 s by default) for the lock
   and for running servers to stop, then exits 6. A server that finds a
   migration holding the lock after it registers exits 6, so a restart loop
   retries. The `migrate` preview and `verify` take no lock and write
   nothing, so they run beside a server.

   Cassandra uses `SERIAL` rather than the `LOCAL_SERIAL` the crate uses
   elsewhere. With two datacenters of one node each and the link between them
   cut, `INSERT ... IF NOT EXISTS USING TTL 60` at `LOCAL_SERIAL` succeeded in
   both datacenters for two different owners in a measured run, and at
   `SERIAL` it was refused. The Cassandra and MongoDB locks live in the
   existing `settings` table and collection because a lock table that the
   migrator creates would be created by two migrators at once, and AtlasDB
   found that two Cassandra nodes creating the same table concurrently can
   lose data after a restart
   ([AtlasDB ADR-0002](https://palantir.github.io/atlasdb/html/miscellaneous/doc/adr/0002-prevent-tables-from-being-creating-simultaneously-in-cassandra-via-a-locks-table.html)).

6. One catalog status decides what each command does. `Runner::status` reads
   the ledgers and writes nothing, and `serve`, `migrate` and `verify` act on
   its result:

   | Status | Meaning | Exit |
   |---|---|---|
   | `Current` | Every migration the binary carries is recorded in every store it touches | 0 |
   | `AheadCompatible` | The catalog ledger has ids above the binary's, all `additive` | 0, with a warning |
   | `Behind` | Migrations the binary carries are missing; a catalog with no ledger is behind at migration 1 | 3 |
   | `AheadIncompatible` | The catalog ledger has an id above the binary's that is `breaking` | 4 |
   | `NeedsAttention` | A pending code body's `detect` returned items that a person must fix | 5 |
   | `Changed` | A recorded checksum differs from the binary's body | 5 |
   | `WrittenByOlderRelease` | The catalog ledger has a row for migration 1, and a release before 1.0 wrote to the catalog afterwards | 5 |
   | `Uninitialized` | The store holds no catalog | 1 in a production build |

   Exit 1 also means an error, 2 a usage error, and 6 a lock or a running
   server that outlasted the timeout. `serve` reads the status before the
   backend creates its engine, because creating the engine writes to the
   catalog on several backends. It serves `Current` and `AheadCompatible`.
   It refuses every other status with that status's exit code and a message
   that lists the pending migrations with their kinds, says what to back up
   on that backend, and gives the `migrate` command. A dev build applies an
   `Uninitialized` or `Behind` catalog at startup through the same runner and
   refuses the other statuses as a production build does.

   A binary serves a catalog ahead of it only when every id it does not know
   is additive, so a binary can be rolled back across additive migrations
   without a restore. The reader of `kind` ships in 1.0, because the check
   runs in the older binary.

7. No switch serves a catalog whose status forbids it. A binary on a catalog
   behind it sends statements to tables and columns that do not exist, which
   is the `table_class` failure above. A binary on a catalog with a breaking
   migration it does not know writes rows in the shape that migration
   replaced. Each case such a switch would cover has another path. After
   additive migrations, run the older binary. After a breaking migration,
   restore the backup taken before it. For development, use a dev build,
   which migrates at startup. For a catalog an older release wrote to, run
   `extenddb migrate --yes --reconverge`, which runs every convergent
   migration again and rewrites the version row.

## Rationale

- Option 2 lost because one change then has a different number on each
  backend, and no number says what a deployment has. ExtendDB's backends
  already differed this way: PostgreSQL's catalog file 003 is Cassandra's
  V005. In Temporal v1.32.1 the manifest "Adds tasks_v2 table for fairness
  tasks" is v1.13 on Cassandra, v1.18 on MySQL and PostgreSQL and v0.10 on
  SQLite. In Cassandra Reaper 2.3.1 the "percent repaired schedule" change is
  028 on Cassandra, 004 on Astra and V21_0_0 on PostgreSQL and H2. With
  separate sequences the compiler cannot reject a backend that lacks a change,
  and `migrate --list`, the status and the upgrade manual cannot name a
  migration the same way on every backend. Reaper removed its PostgreSQL and
  H2 backends in 3.0.0, and the issue that proposed it gave the reason: "It
  often resulted in features that worked fine with a specific backend and were
  buggy with others"
  ([cassandra-reaper#1120](https://github.com/thelastpickle/cassandra-reaper/issues/1120)).
- Option 3 lost because the diff tools surveyed (Atlas, Skeema, sqldef)
  support SQL databases only, so Cassandra and MongoDB would still need
  ordered migrations; every tool surveyed that migrates MongoDB or Cassandra
  uses ordered migrations with a ledger in the store. A computed diff also
  cannot tell a rename from a drop followed by an add: Skeema refuses renames,
  and Atlas and sqldef need an annotation in the schema to plan one. A diff
  has no place for a data change, and migrations 2 and 3 are data changes.
  Atlas's documentation recommends versioned files "if you're shipping
  software that customers install and manage themselves" ([Atlas, declarative
  vs versioned](https://atlasgo.io/concepts/declarative-vs-versioned)).
  SQLite's re-applied file showed the same limit inside ExtendDB: a file that
  creates only what is missing never changes a table that exists.
- Option 4 lost because it migrates SQL databases only, so of ExtendDB's
  backends it covers PostgreSQL and SQLite, and ExtendDB would still have
  three migration mechanisms: sqlx's, and one runner each for Cassandra and
  MongoDB. It also numbers migrations per migrator and checksums SQL text
  only, and it has no code bodies with a read-only `detect` step, no
  per-migration kind for an older binary to read, and no way to see a running
  server.
- Option 5 lost because deployments created by every 0.1.x release hold data
  that an upgrade has to keep. ADR-0003 accepted the loss only while no
  deployment depended on a catalog, and that condition no longer holds.

## Consequences

- A contributor who changes a catalog adds an id to `migration_ids!` and an
  arm for it in the table of every backend on the runner: a file, a code body
  with a golden test, or `NotApplicable` with a reason. The contributor
  declares the kind by the rule in CONTRIBUTING.md and commits the
  current-schema file that the backend's CI job regenerates. A change to
  per-table objects also changes the DDL that creates new ones, in the same
  PR, and is breaking. Released bodies are not edited, and the release PR
  pastes the checksum literals. The procedure is in the
  [catalog migration section of CONTRIBUTING.md](../../CONTRIBUTING.md#adding-a-catalog-migration).
- An operator upgrades by stopping the server, backing up the stores the
  backend uses, running `extenddb migrate` to see what is pending (exit 3
  when something is), running `extenddb migrate --yes`, and starting `serve`.
  On SQLite `migrate --yes` copies the database beside it before its first
  change, and nothing removes the copy. `extenddb migrate --list` prints every
  migration the binary carries with its kind on that backend, `verify` prints
  the status, and `serve` refuses with the status's exit code and the command
  to run.
- Rollback after additive migrations runs the previous 1.x binary. Rollback
  after a breaking migration restores the backup taken before the upgrade.
  All four v1.0 migrations are breaking, so rolling back from 1.0 is a
  restore.
- Releases before 1.0 refuse a 1.0 catalog or leave a trace in it. PostgreSQL
  `serve` refuses the integer version row, and v0.1.14 `serve` on every
  backend refuses a catalog without the `resource_tag_migration` row, which
  migration 1 deletes. A released `migrate --yes` runs no legacy file and
  inserts the marker row again, and SQLite dev builds from v0.1.7 to v0.1.14
  write a dotted version back at startup. 1.0 reports either trace as
  `WrittenByOlderRelease` (exit 5), and `extenddb migrate --yes --reconverge`
  repairs it. Measured on SQLite with the v0.1.14 dev image by
  `devtools/check-sqlite-older-binary`: after the image's `migrate --yes` or
  its `serve` ran on a migrated catalog, 1.0 reported
  `WrittenByOlderRelease`, and `--reconverge` restored `Current` with the
  schema the catalog had before.
- A 0.1.x server that is still running while migration 1 adopts its catalog
  is not detected, because released servers do not register. On SQLite,
  servers from v0.1.14 on hold the `flock` that `migrate` takes, so only older
  servers are not detected. The `migrate` preview, the refusal message and
  the upgrade manual tell the operator to stop every server before the 1.0
  upgrade.
- `migrate --yes` refuses while a server serves, so every 1.x upgrade stops
  the server first and no server runs while its catalog changes. The kind of
  each migration still matters for rolling a binary back.
- The tests of each backend on the runner load committed fixtures of every
  released state, upgrade them, and compare each with a fresh install seeded
  the same way, per-table objects included, against a list of accepted
  differences per state. GitLab's CI does the same with a seeded PostgreSQL
  dump of its latest required upgrade stop.
- Every backend carries an entry for every id, including a reason for a
  change that does not apply to it. Code bodies may call application code,
  such as `ManagementStore` in migration 2. Each keeps its golden test (SQLite's
  migration 1 is checked against its expected definitions file instead) for
  as long as the body ships, so a change in that code that alters a body's output
  fails the test.
- On Cassandra a migration needs every replica of the catalog keyspace
  reachable, because the lock is taken at `SERIAL` and running servers are
  read at `ALL`. The runner waits for schema agreement after every DDL
  statement, before every ledger row and before it releases the lock, reading
  `schema_version` from `system.local` on each live node
  (`crates/storage-cassandra/src/migrations/agreement.rs`).
- Every backend runs on the runner. Each backend's previous mechanism was
  deleted when it moved: the PostgreSQL and SQLite migration helpers first,
  then the Cassandra and MongoDB bootstrapper migrations, with the marker
  check in `serve.rs`.
- The shared numbering and the ledger in every store are read by every later
  release, so reversing this decision means migrating every deployment's
  ledgers. A released body changes only through a `rebless` entry.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
