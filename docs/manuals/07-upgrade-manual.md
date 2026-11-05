---
description: "Upgrading an ExtendDB deployment from one version to the next, with the catalog migrations each release requires."
---

# Upgrading

> See [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/docs/NOTICE.md) for important disclaimers.

## Current Status

This release versions the PostgreSQL, SQLite, Cassandra and MongoDB catalogs by a migration ledger and carries catalog migrations 1 to 5, which `extenddb migrate --list` prints. **Every deployment created by a 0.1.x release runs `extenddb migrate --yes` once** before a server of this release starts on it: the server refuses a catalog with pending migrations. Migration 1 brings the catalog of every 0.1.x release to the v1.0 schema, and migration 5 drops `continuous_backups`, a table and on MongoDB a collection (see [Catalog 0.0.5](#catalog-005)).

A backup that a 0.1.x release took can restore without its global and local secondary indexes: a SQLite backup taken by 0.1.14 records only the table's key schema and attribute definitions, and its restore creates the table with its items and no index. Take a new backup of each table after upgrading to v1.0, and keep the 0.1.x backups for their items only.

The [general upgrade procedure](#general-upgrade-procedure) gives the sequence. The [version history](#version-history) records the 0.1.x catalog versions and what the 0.1.x binaries print, and [Behavior Changes by Release](#behavior-changes-by-release) what each release changes in behavior.

This page covers a server built from source ([General Upgrade Procedure](#general-upgrade-procedure)) and a deployment running the published container images ([Upgrading a container deployment](#upgrading-a-container-deployment)). For installing either, see [Install from source](../install-from-source.md), [Docker Compose with PostgreSQL](../local-dev-docker-compose.md) and [Development image](../dev-image.md).

## How Catalog Upgrades Work

### The Migration System

Every backend shares one numbered list of catalog migrations, in `crates/storage/src/migrations/registry.rs`. For each id a backend declares the body it runs in each of its stores, a SQL file under `crates/storage-<backend>/migrations/` or code, or why the id does not apply to it. `extenddb migrate --yes` applies the pending bodies in id order, catalog store first, and commits each body with a row in that store's ledger, the `extenddb_migrations` table: the id, name, kind, checksum, notes, time, and the release that applied it. On PostgreSQL the catalog database and the data database each have a ledger; on SQLite the database file has one. On MongoDB the bodies are code, and the catalog database and the data database each have a ledger collection: there a body's requests are each safe to send again, and its ledger row and, in the catalog, the `catalog_version` row commit in one transaction after the body, so a migrator stopped part way runs the body again. On Cassandra the bodies are CQL files and code, and the catalog keyspace and each account keyspace have a ledger table. Cassandra has no transactions: every statement of a body is safe to send again and a body's ledger row is written after it, so a run that stopped or failed part way, for example with `no answer within 20 s` from a loaded node, finishes when `migrate --yes` runs again.

The catalog's version is the highest id such that it and every lower id are recorded in every store its bodies touch. The `catalog_version` row in `settings` holds the same integer, written with each catalog row, for tools that read it; on Cassandra a stopped run can leave it one ahead of the ledger, or one behind after migration 1's last account keyspace, until `migrate --yes` runs again. The `schema_history` tables of 0.1.x PostgreSQL catalogs stay in place and are not read.

Running `extenddb migrate --yes` on an up-to-date catalog changes nothing.

### The Catalog Status

`serve`, `migrate` and `verify` read the catalog's status from the ledgers, take no lock to do it, and act on it with the same exit codes:

| Status | Meaning | `serve` | `migrate` | `migrate --yes` | `verify` |
|--------|---------|---------|-----------|-----------------|----------|
| Current | Every migration this binary carries is recorded | serves | 0 | 0 | 0 |
| Behind | Migrations are pending | 3 | 3 | applies them, 0 | 3 |
| AheadCompatible | The catalog has migrations this binary does not know, all additive | serves, with a warning | 0 | 0 | 0 |
| AheadIncompatible | The catalog has a migration this binary does not know that is breaking | 4 | 4 | 4 | 4 |
| NeedsAttention | A pending migration found data that a person must fix; each item is named | 5 | 5 | 5 | 5 |
| Changed | A recorded checksum differs from this binary's body | 5 | 5 | 5 | 5 |
| WrittenByOlderRelease | A 0.1.x release wrote to the catalog after it was migrated | 5 | 5 | 5; `--yes --reconverge` repairs it | 5 |
| Uninitialized | Not a catalog; `extenddb init` creates one | 1 | 1 | 1 | 1 |

Exit status 6 means the migration lock, or a running server, was still held when `--lock-timeout` ran out; run the command again once it is free. Every refusal prints the command that resolves it. A dev build's `serve` applies a Behind or Uninitialized catalog at startup instead of refusing it. The `serve` codes are those of `serve --foreground`; in daemon mode `serve` exits 1 and writes the refusal to syslog.

`verify` also checks the connection to the data database. When that check fails, `verify` exits 1 on a Behind catalog instead of 3, because `migrate --yes` cannot apply the pending migrations while the data database cannot be reached. It still exits 4 or 5 for the statuses that need a person.

A migration is additive when the previous release serves the catalog correctly with it applied, and breaking otherwise. Migrations 1 to 5 are breaking: do not run a 0.1.x server against a migrated catalog. A 0.1.x binary that writes to a migrated catalog (its `migrate --yes`, or a SQLite dev server) leaves it WrittenByOlderRelease. `migrate --yes --reconverge` repairs it by running the convergent migrations again. Migration 2, which rewrites stored policies, is not convergent: the repair skips it and prints a line that says so (see the v0.1.14 notes below).

## Writing a New Migration

Writing a catalog migration is contributor work. The procedure, with a worked example and the tests to run on each backend, is in the [catalog migration section](https://github.com/ExtendDB/extenddb/blob/main/CONTRIBUTING.md#adding-a-catalog-migration) of `CONTRIBUTING.md` in the repository root. `extenddb migrate --list` prints the migrations the binary carries, with each one's kind on its backend.

## General Upgrade Procedure

For releases that include catalog changes. The names below in angle brackets are the ones chosen at `extenddb init`: `<catalog-db>` and `<data-db>` (`--catalog-db`, `--data-db`; defaults `extenddb_catalog` and `extenddb`) and `<owner>`, the application role (`--extenddb-user`, default `extenddb`). `<admin>` is the PostgreSQL role `init` connected as (`--pg-user`).

1. **Stop the server**

```bash
extenddb stop --config extenddb.toml
```

2. **Back up the databases** (PostgreSQL; for SQLite see [SQLite](#sqlite) below, for Cassandra see the [rollback procedure](#rollback-procedure))

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
extenddb migrate --config extenddb.toml
extenddb migrate --config extenddb.toml --yes
```

`migrate` connects as the application role in the config file, which does all the schema work. On PostgreSQL that role must own every table, view and sequence in the public schema of both databases, and may create tables there, as it does after `init`. A catalog restored by another role (for example from a `pg_dump --no-owner` dump loaded by an admin) is not owned by it, and such a dump carries no grants, so the role cannot read the restored tables either: the preview, `verify` and `migrate --yes` exit 1 with the database's `permission denied for table settings` followed by the catalog database's note. The data database's name is a row of the catalog's `settings`, which the role cannot read, so the commands do not guess it and print a line saying so after the note. Run the catalog database's statements first, and the next run prints the data database's note, or pass `migrate --data-db <name>` to see both notes at once. When the role can read the tables but does not own them, the preview and `verify` print a note per database, and `migrate --yes` refuses with exit status 1 before it takes the lock, printing one note per line. Each note names the objects the role does not own, with their owners, and the statements to run in that database, numbered in the order they run, each with the role that runs it. For each owner a `DO` block, run by that owner or by a superuser, changes the owner of that owner's objects with `ALTER ... OWNER TO` and of nothing else. Where no superuser exists, as on a managed service, PostgreSQL lets an owner give an object only to a role it is a member of, and only when that role may create objects in the object's schema. Call the role that owns the restored objects `<restoring role>`. When it is not a superuser, the note first prints `GRANT "<owner>" TO "<restoring role>"`, run by an administrator that may grant the application role (on PostgreSQL 16, a role with `ADMIN OPTION` on it, which the role that created it holds; the application role cannot grant itself to `<restoring role>`). Then it prints `GRANT CREATE ON SCHEMA public TO "<owner>"` when the application role may not create tables there, run by the public schema's owner, then the `DO` blocks, and last `REVOKE "<owner>" FROM "<restoring role>"`, which takes back the membership the `DO` blocks needed. Skip that last statement if `<restoring role>` was already a member of `<owner>`. A superuser can run every statement as printed. The note does not use `REASSIGN OWNED BY`, which would also give the role the old owner's other databases and its objects in other schemas. A migration that still fails on a privilege (`must be owner of table ...`) stops, commits nothing in that database, and prints the same note. `--pg-user <admin>`, with the password in `EXTENDDB_PG_PASSWORD`, names an admin role for one step only, creating the pgvector extension in the data database, on that database's server; without it `migrate --yes` reports whether pgvector is available and goes on.

`migrate` finds the data database through the name `init` recorded in the catalog's `settings` (`data_database_name`), and connects to it with the connection string `init` recorded beside it (`data_database_connection_string`), which the server connects with. A catalog that has no recorded name falls back to the catalog database name without its `_catalog` suffix. `--data-db <name>` overrides both and uses that database on the recorded connection string's server. When the two rows name different databases and `--data-db` is not given, `migrate`, `verify` and `serve` refuse with exit status 1 and name both; correct the row that is wrong.

The PostgreSQL commands and `serve` work through PgBouncer in transaction mode and in session mode: no session sets a parameter at login other than `application_name`, which PgBouncer passes on to the server, and every other setting is sent inside the transaction that needs it.

`migrate --yes` finds running servers through the key each one holds on the catalog database while it serves. In two layouts the key does not match the servers that run, and stopping every server before the upgrade (step 1) covers both:

- Behind PgBouncer, the TCP keepalives the serving session sets (30 s) run on the database's connection to PgBouncer, which stays up when a serving host dies. The database keeps that host's key until PgBouncer closes the dead host's connection, which PgBouncer's own keepalive settings decide: by default the operating system's, two hours idle on Linux. Until then `migrate --yes` exits 6 naming the dead server. Set `tcp_keepalive = 1`, `tcp_keepidle = 10`, `tcp_keepintvl = 5` and `tcp_keepcnt = 4` in PgBouncer's `[pgbouncer]` section for the same 30 s, or end the session with `SELECT pg_terminate_backend(<pid>)` on the pid `migrate` names.
- After a failover of the catalog's server, the new primary holds no server's key, while a server that served through the old primary goes on serving through connections that reconnect to the new primary, until it counts its key as lost, at most 30 s after its last renewal, and registers again on the new primary. A `migrate --yes` in those 30 s does not see that server. Wait 30 s after a failover before you run `migrate --yes`, or stop the servers first.

A server notices at its next renewal, within 15 s, that the database ended the session holding its key: a restart or failover of the catalog's server, `SELECT pg_terminate_backend(<pid>)`, or a pooler that closed the connection. From then on it answers each new request, `/health` included, with `ServiceUnavailable` (HTTP 503), which the AWS SDKs retry, lets the requests already running finish, and stops its background workers, aborting the ones still running after 5 s. Work that requests started in the background goes on until it ends or the server exits: a RestoreTableFromBackup's item copy on every backend, an UpdateTimeToLive backfill on Cassandra and PostgreSQL, and a GSI or vector index build on PostgreSQL. It tries to register again on a new session at once and then every second, and once it holds the key and the catalog's status is the one it started with, it starts its background workers again and serves. When a migration holds the migration lock or waits for the servers to stop, when the status changed, when 30 s pass without a registration, or when a background worker could not be stopped, the server stops the way SIGTERM stops it, finishing the requests in flight and removing its PID file, and exits 1. Until the renewal that finds the session gone, the server serves as before, so a `migrate --yes` that was waiting for the servers can start beside it in those seconds: stop every server before you migrate, as the steps above say.

The two transactions that hold the keys, the server's registration and the migration lock, turn off `idle_in_transaction_session_timeout` for themselves, and `transaction_timeout` on PostgreSQL 17 and later, so a role or database setting of either does not end them. PgBouncer's `idle_transaction_timeout` is a setting of PgBouncer, which a transaction cannot change. Each holder runs `SELECT 1` every 15 s, so set `idle_transaction_timeout` to 0, its default, or to 30 s or more. A value under 15 s closes the server's connection between two renewals, and the server then registers again every time. Each time it also stops its background workers and starts a new set, and each worker waits its interval before its first run, so a worker whose interval is longer than the 15 s between two renewals never runs: among them the TTL scan (60 s), the table size refresh (300 s), and the idempotency token and stream record cleanups (600 s and 3600 s).

On MongoDB, `migrate` connects with the connection string in the config file, which the server uses too. A user with the `readWrite` role on `extenddb_catalog` and on `extenddb_data` and no other role can run `init`, `migrate --yes`, `verify` and `serve`. `migrate --yes` takes the migration lock, a document in the catalog's `settings` collection, and then waits for the registration every running server keeps in the catalog's `extenddb_servers` collection, and after `--lock-timeout` exits 6 naming each server's host and pid. A server renews its registration every 5 s, and every second after a renewal failed, and deletes it when it stops, once every background worker has ended (it leaves it to expire when one could not be stopped); a server that was killed leaves it until it expires, at most 45 s after its last renewal. A server whose registration was deleted, or that has not renewed it for 30 s, for example because the primary stopped answering, answers new requests with `ServiceUnavailable` and registers again, as a PostgreSQL server does after its session ends (above): it serves again once it holds a new registration and the catalog's status is the one it started with, and it stops with exit 1 when a migration holds the lock, when the status changed, or when 30 s pass without a registration. The lock and the registrations expire by the clock of the replica set's primary, so keep the clocks of its members in sync with NTP. Migration 1 deletes the `resource_tag_migration` settings document in the transaction that records its catalog step, so a 0.1.14 server refuses the migrated catalog, and a run that fails before that transaction leaves the document in place. 0.1.12 and 0.1.13 servers read neither that document nor the catalog version, so they start on a migrated catalog and serve it: do not run them against it. To go back to a 0.1.x release, restore the backup taken before `migrate --yes`. A 0.1.x `migrate` on a migrated catalog reports a pending change, and its `--yes` writes the settings document back; this release's status then reports the catalog as written by an older release, and `migrate --yes --reconverge` repairs it.

On Cassandra, `migrate` connects as the role the config file names, which the server connects as too. `--cassandra-user <role>` connects as another role, with its password in `EXTENDDB_CASSANDRA_PASSWORD` or `--cassandra-pass`; a wrong password is reported as a connection timeout. Migration 1 adopts a 0.1.x catalog: it keeps every setting except the `resource_tag_migration` row, which it deletes, and records itself in a ledger in the catalog keyspace and in every account keyspace. On a cluster that runs `CassandraAuthorizer`, `init` grants the application role SELECT, MODIFY, CREATE and ALTER on the catalog keyspace, those four and DROP on the default account's keyspace, and CREATE ON ALL KEYSPACES, which CreateAccount needs to create a keyspace per account and which also lets the role create tables in every keyspace on the cluster; a run of `migrate --yes --cassandra-user <role>` that creates a missing account keyspace grants the application role the five permissions on it. A 0.1.x `init` granted the application role nothing, so a deployment created before 1.0 on such a cluster needs the statements below before `migrate`. `migrate --yes` takes the migration lock, a row in the catalog's `settings` table written by lightweight transactions, and then waits for the registration every running server of this release keeps in the catalog's `extenddb_servers` table, and after `--lock-timeout` exits 6 naming each server. A server renews its registration every 5 s, and every second after a renewal failed, and deletes it when it stops, once every background worker has ended (it leaves it to expire when one could not be stopped); a server that was killed leaves it until it expires, at most 45 s after its last renewal. A server tries its renewals for 30 s after the last one that succeeded, so it keeps serving through an outage of 25 to 30 s, depending on when the outage starts, for example while the only replica of the catalog restarts. After that, 30 to 35 s after its last successful renewal, it answers new requests with `ServiceUnavailable` and registers again, as a PostgreSQL server does after its session ends (above), and stops with exit 1 when a migration holds the lock, when the status changed, or when 30 s pass without a registration. A migration needs every replica of the catalog keyspace reachable: the lock is written at `SERIAL`, the registrations are read at `ALL`, and after each schema statement the runner waits until every live node reports the same schema version. `serve` refuses a catalog with pending migrations, exiting 3 under `--foreground` with the pending migrations and the `extenddb migrate --config <config> --yes` command to run. A 0.1.x server refuses a migrated catalog.

On a deployment created before 1.0 on a cluster that runs `CassandraAuthorizer`, this release's `migrate`, `verify` and `serve`, connected as the application role, fail with `Unauthorized ... has no SELECT permission on <table <prefix>_catalog.accounts>` until the role holds its permissions. With every server stopped, run the statements below in `cqlsh` as a superuser; the application role cannot grant them to itself. `<role>` and `<prefix>` are `username` and `keyspace_prefix` under `[storage.cassandra]` in the config file. Each account has its own keyspace, so list the accounts first:

```
SELECT account_id FROM <prefix>_catalog.accounts;
```

Grant on the catalog keyspace and on all keyspaces:

```
GRANT SELECT ON KEYSPACE <prefix>_catalog TO '<role>';
GRANT MODIFY ON KEYSPACE <prefix>_catalog TO '<role>';
GRANT CREATE ON KEYSPACE <prefix>_catalog TO '<role>';
GRANT ALTER ON KEYSPACE <prefix>_catalog TO '<role>';
GRANT CREATE ON ALL KEYSPACES TO '<role>';
```

Then, for each account id whose keyspace `<prefix>_account_<account id>` exists:

```
GRANT SELECT ON KEYSPACE <prefix>_account_<account id> TO '<role>';
GRANT MODIFY ON KEYSPACE <prefix>_account_<account id> TO '<role>';
GRANT CREATE ON KEYSPACE <prefix>_account_<account id> TO '<role>';
GRANT ALTER ON KEYSPACE <prefix>_account_<account id> TO '<role>';
GRANT DROP ON KEYSPACE <prefix>_account_<account id> TO '<role>';
```

A GRANT on an account keyspace that does not exist fails with `Resource ... doesn't exist`; skip that account, and `migrate --yes` creates its keyspace. A statement for a permission the role already holds prints `Role '<role>' was already granted ...` and changes nothing, so a role granted some of these by hand runs the whole list.

Without `--yes`, `migrate` reads the [catalog status](#the-catalog-status), prints each pending migration, and changes nothing; it takes no lock, so it runs beside a server. While migration 2 is pending, the preview also prints the number of stored policies migration 2 rewrites and names the first 20 of them, each with its account, principal, policy name and number of keys, followed by the number of the others. To list them, it reads the catalog's policies a second time. With `--yes` it takes the migration lock, waiting up to `--lock-timeout` seconds (60 by default) for running servers and other migrations, reads the status again under the lock, and applies what is pending. One `--lock-timeout`, counted from the start, bounds the wait for the lock and the wait for the servers together. On PostgreSQL, Cassandra and MongoDB it holds the migration lock while it waits for the servers, so a server that starts in that time exits 6. It refuses a store that is not a catalog with exit status 1 and the advice to run `extenddb init`; on Cassandra it refuses a keyspace that holds another program's tables with a message that names them. The exit statuses are those in the status table; a script under `set -e` can run the preview as `extenddb migrate ... || [ $? -eq 3 ]`.

On PostgreSQL `migrate --yes` can also print:

- `Waiting up to <n> s for the statements of an earlier migrator that stopped`: a migrator that was killed left a statement running in a database. The run waits until it ends, and after `--lock-timeout` exits 6 naming each session with `SELECT pg_cancel_backend(<pid>)` to end it; a statement that waits for a lock does not end by itself. The run looks for them in each database through its own connection to it, so a data database on another server is covered.
- A line naming an index statement and the sessions it waits for: migration 4 builds its indexes with `CREATE INDEX CONCURRENTLY`, which waits for every transaction on the table. After 5 s it cancels the build and tries again, three times, and then stops naming the sessions; end them and run again.
- `could not lock a table ... within 5 s, 4 times`: other sessions held locks on the tables a migration changes, which it names. Nothing of that migration was committed; stop those sessions and run again.
- `after migration 1 the database <name> differs from the v1.0 schema in <n> place(s)`: the first run of migration 1 on that database found objects that differ from the v1.0 schema, such as a column added by hand or a changed default, and committed nothing there. Each following line names an object and its table, what the database has, what v1.0 has, and the change to make. Make each change, then run `migrate --yes` again.

On PostgreSQL one `--lock-timeout` bounds every wait of a run together: the wait for another migrator's lock, the wait for running servers, and the wait for an earlier migrator's statements. Each wait gets the time the earlier ones left, and its line prints the seconds left. With `--lock-timeout 0` a run makes one attempt at each: it exits 6 at once when another migrator or a running server holds its key, or when an earlier migrator's statement is still running.

Exit 3 for a pending preview is the behaviour of a binary built from this tree. The published tags up to `0.1.14` print `Error: --yes is required to apply migrations. Pending: ...` and exit 1 instead; see [Upgrading a container deployment](#upgrading-a-container-deployment).

5. **Verify**

```bash
extenddb verify --config extenddb.toml
```

`verify` prints the catalog status, and on a Behind catalog the policies migration 2 rewrites, as the preview does. For each migration that recorded notes in the ledger, such as the policies migration 2 rewrote, it prints the number of notes and the first 5; `verify --all-notes` prints every note.

6. **Start the server**

```bash
extenddb serve --config extenddb.toml
```

SQLite deployments follow the same sequence, backing up the database files as [SQLite](#sqlite) describes. MongoDB deployments track their own catalog version, and a release states whether it changes; the Behavior Changes section below still applies to them.

## Upgrading a container deployment

The published images carry the same binary as a source build, so the catalog rules above apply unchanged: `migrate` must run once per deployment before the new server starts, and an older binary refuses a newer catalog. What differs is where `migrate` runs and how the version is chosen: the version is the image tag, and the repository's Compose files run `migrate` for you in the `extenddb-bootstrap` service.

Published tags are listed on Docker Hub for [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres/tags), [`extenddb/extenddb-dev`](https://hub.docker.com/r/extenddb/extenddb-dev/tags) and [`extenddb/extenddb-mongodb`](https://hub.docker.com/r/extenddb/extenddb-mongodb/tags). `docker run --rm <image> version` prints the catalog version an image expects. At the time of writing the newest published tag, `0.1.14`, reports `catalog 0.0.3 (postgres)`, `catalog 0.0.3 (sqlite)` and `catalog 0.0.2 (mongodb)`; the PostgreSQL tags `0.1.5` to `0.1.10` report `catalog 0.0.2 (postgres)`, and every published `extenddb-dev` tag reports `catalog 0.0.3 (sqlite)`. The migration ledger this page describes, with migrations 1 to 5, is not yet in a published tag. The commands and output below were run on the PostgreSQL stack upgraded from `0.1.10` (catalog 0.0.2) to `0.1.14` (catalog 0.0.3), so they show a volume initialised by an older catalog version being migrated. <!-- version-literal-ok: names the measured tags -->

### Docker Compose (PostgreSQL)

This is the stack started as described in [Docker Compose with PostgreSQL](../local-dev-docker-compose.md): the repository's `docker-compose.yml`, run from the directory that holds it, with the image chosen by `EXTENDDB_IMAGE`. The catalog lives in the `postgres` container's `postgres-data` volume; the config file and TLS material live in the `extenddb-state` volume. Neither volume is touched by changing the image tag, so an upgrade keeps the tables, items, users and access keys.

Every `docker compose` command below needs the same `EXTENDDB_IMAGE` the stack was started with, until the step that changes it. Setting it on each command works; so does putting `EXTENDDB_IMAGE=extenddb/extenddb-postgres:0.1.14` in a `.env` file beside the Compose file, which Compose reads on every command, in which case the upgrade is one edit to that file.

1. **Back up the catalog and data databases.** `pg_dump` runs inside the `postgres` container; the PostgreSQL superuser is `PG_ADMIN_USER` (default `postgres`) and the database names are the Compose defaults.

```bash
docker compose exec -T postgres pg_dump -U postgres extenddb_catalog > catalog_backup_$(date +%Y%m%d).sql
docker compose exec -T postgres pg_dump -U postgres extenddb > data_backup_$(date +%Y%m%d).sql
```

A fresh catalog at 0.0.2 dumped to 747 non-empty lines; the dump's `settings` rows include `catalog_version\t0.0.2`, which is how to check what a dump holds.

2. **Change the tag, pull, and start.** Compose compares the image of every service against the running containers, stops the server, recreates `extenddb-volume-init`, `extenddb-bootstrap` and `extenddb` from the new image, runs the two bootstrap services to completion and starts the server. The server is stopped before `extenddb-bootstrap` starts, so `migrate` runs with no server on the catalog.

```bash
export EXTENDDB_IMAGE=extenddb/extenddb-postgres:0.1.14
docker compose pull extenddb
docker compose up -d
```

```
 Container extenddb-postgres-dev-postgres-1  Running
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Stopping
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Stopped
 Container extenddb-postgres-dev-extenddb-volume-init-1  Recreate
 Container extenddb-postgres-dev-extenddb-volume-init-1  Recreated
 Container extenddb-postgres-dev-extenddb-1  Stopping
 Container extenddb-postgres-dev-extenddb-1  Stopped
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Recreate
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Recreated
 Container extenddb-postgres-dev-extenddb-1  Recreate
 Container extenddb-postgres-dev-extenddb-1  Recreated
 Container extenddb-postgres-dev-extenddb-volume-init-1  Started
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Started
 Container extenddb-postgres-dev-extenddb-bootstrap-1  Exited
 Container extenddb-postgres-dev-extenddb-1  Started
```

3. **Read the bootstrap log.** `extenddb-bootstrap` finds the config file written by the first start in `/var/lib/extenddb/extenddb.toml` and runs `extenddb migrate --config /var/lib/extenddb/extenddb.toml --yes --pg-user $PG_ADMIN_USER`, with `EXTENDDB_PG_PASSWORD` taken from `PG_ADMIN_PASSWORD`. On a first start, when the file does not exist, the same service runs `extenddb init` instead.

```bash
docker compose logs extenddb-bootstrap
```

```
bootstrap: existing config detected; running migrations
=== extenddb migrate ===
Config:           /var/lib/extenddb/extenddb.toml

--- Loading defaults from /var/lib/extenddb/extenddb.toml
--- Checking current catalog version...
  Current version: 0.0.2
--- Checking data migrations...
  Pending: 004_vector_index_state.sql
--- Running catalog migrations...
    Applying 002_vector_indexes.sql...
    Migrations applied.
--- Running data migrations...
    Applying 004_vector_index_state.sql...
    Data migrations applied.

=== extenddb migrate complete ===
Catalog version: 0.0.2 -> 0.0.3
```

The log is shortened to the lines that change between runs. On a volume whose catalog already matches the new image, the same service prints `Everything is up to date (catalog version 0.0.3). No migrations needed.` and exits 0. The `pgvector` notice described under [Catalog 0.0.3](#catalog-003) appears in this log on the Compose stack, because the `postgres` image carries no `vector` extension.

4. **Check the server.** `docker compose ps -a` shows `extenddb-bootstrap` as `Exited (0)` and `extenddb` as `(healthy)` after 10 to 20 seconds, both on the new image. The first line of the server's log after the banner names the version and catalog it started with:

```bash
docker compose logs extenddb
```

```
extenddb 0.1.14 (catalog 0.0.3) starting on 0.0.0.0:18443
```

A table created and an item written under `0.1.10` were listed and read back unchanged through the `0.1.14` server with the same access key; `select value from settings where key = 'catalog_version'` on the catalog database returned `0.0.3` and `schema_history` listed `001_schema.sql` and `002_vector_indexes.sql`.

#### Running migrate as a one-off step

To see or apply the migration separately from starting the server, stop the server and run the bootstrap service on its own. It runs the same `migrate --yes` and exits with the exit status of `migrate`; `--rm` removes the one-off container.

```bash
docker compose stop extenddb
EXTENDDB_IMAGE=extenddb/extenddb-postgres:0.1.14 docker compose run --rm extenddb-bootstrap
echo $?
```

On a catalog at 0.0.2 this applied `002_vector_indexes.sql` and `004_vector_index_state.sql`, printed `Catalog version: 0.0.2 -> 0.0.3` and exited 0. To preview instead of apply, override the entrypoint and leave `--yes` off; with the published `0.1.14` image a pending migration prints the plan and exits 1 (a source build exits 3, see [The Catalog Status](#the-catalog-status)):

```bash
EXTENDDB_IMAGE=extenddb/extenddb-postgres:0.1.14 docker compose run --rm --entrypoint extenddb \
  extenddb-bootstrap migrate --config /var/lib/extenddb/extenddb.toml --pg-user postgres
```

```
Error: --yes is required to apply migrations. Pending: catalog 0.0.2 -> 0.0.3; data migrations [004_vector_index_state.sql]; record the resource-tag migration marker.
```

`docker compose run --rm extenddb-bootstrap` does not stop a running server first: run while the `0.1.14` server was up, it reported nothing pending and exited 0. Stop the server before a run that applies a migration, as step 1 of the general procedure does and as `docker compose up -d` does. After the one-off run, `docker compose up -d` recreates the server from the new image; its own `extenddb-bootstrap` run then reports nothing pending.

#### What a downgrade shows

Pointing `EXTENDDB_IMAGE` at the older tag and running `docker compose up -d` does not fail at the Compose level. The older image's `extenddb-bootstrap` runs its `migrate --yes` against the newer catalog, reports `Current version: 0.0.3` and `Catalog version: 0.0.3 -> 0.0.3`, and exits 0; the catalog is not changed. The older server then refuses to start, and because the `extenddb` service has `restart: unless-stopped`, Docker restarts it until it is stopped. `docker compose ps -a` shows `extenddb` as `Up Less than a second (health: starting)` on every look, `docker inspect` on the container shows the restart count climbing, and the port answers nothing. The refusal is in the server's log, not the bootstrap's:

```bash
docker compose logs extenddb
```

```
extenddb 0.1.10 (catalog 0.0.2) starting on 0.0.0.0:18443
extenddb fatal: Catalog version mismatch: expected 0.0.2, found 0.0.3. Run 'extenddb migrate'
```

Setting `EXTENDDB_IMAGE` back to the newer tag and running `docker compose up -d` brought the server back with its data. To run the older version on the data written before the upgrade, follow [Rollback Procedure](#rollback-procedure) against the `postgres` container (`docker compose exec -T postgres psql -U postgres -d postgres -c ...` and `docker compose exec -T postgres psql -U postgres -d extenddb_catalog < catalog_backup_YYYYMMDD.sql`), then start the older image.

### Docker Compose (MongoDB)

`docker-compose.mongodb.yml` has the same `extenddb-bootstrap` service; its image variable is `EXTENDDB_MONGODB_IMAGE` and its `migrate` takes no admin flags. The upgrade is the same three commands with `-f docker-compose.mongodb.yml` and the MongoDB variable. Upgrading from `extenddb/extenddb-mongodb:0.1.13` to `0.1.14` printed `Catalog version: 0.0.2 -> 0.0.2` in the bootstrap log and the server started with `extenddb 0.1.14 (catalog 0.0.2) starting on 0.0.0.0:18443`; the MongoDB catalog version has not changed between published tags, so the migrate step had nothing to apply. The MongoDB container's notes are in [docker/README-mongodb.md](https://github.com/ExtendDB/extenddb/blob/main/docker/README-mongodb.md).

### The development image

`extenddb/extenddb-dev` (see [Development image](../dev-image.md)) has no bootstrap service and no config file: `serve` creates the SQLite database in `/var/lib/extenddb` on first start and opens it on later starts. Upgrading is replacing the container with one from the new tag on the same volume. With `docker run`:

```bash
docker pull extenddb/extenddb-dev:0.1.14
docker rm -f <container>
docker run -d -p 127.0.0.1:18080:18080 -v extenddb:/var/lib/extenddb extenddb/extenddb-dev:0.1.14
```

With the repository's `docker-compose.dev.yml`, where the image is `EXTENDDB_DEV_IMAGE`:

```bash
EXTENDDB_DEV_IMAGE=extenddb/extenddb-dev:0.1.14 docker compose -f docker-compose.dev.yml pull extenddb-dev
EXTENDDB_DEV_IMAGE=extenddb/extenddb-dev:0.1.14 docker compose -f docker-compose.dev.yml up -d extenddb-dev
```

Both were run from `0.1.13` to `0.1.14`, with a table created on the first version. The second start logs the new version and the existing file, and skips the `Bootstrapped file backend` and admin credential lines the first start printed; the table was listed afterwards:

```
extenddb 0.1.14 (catalog 0.0.3) starting on 0.0.0.0:18080
  storage: sqlite (/var/lib/extenddb/extenddb.sqlite)
```

The dev image does not run `migrate` at start, and `migrate` cannot be run against its volume: `docker compose -f docker-compose.dev.yml run --rm extenddb-dev migrate` exits 1 with `Error: Config file 'extenddb.toml' not found`, because the image serves from built-in defaults and writes no config file. Every published `extenddb-dev` tag expects catalog 0.0.3, so a start against a volume at another catalog version has not been observed. When a future tag changes the SQLite catalog, the server answers a volume behind it as the [status table](#the-catalog-status) gives for a Behind catalog; the dev image holds development data, and the recovery is a fresh volume (`docker compose -f docker-compose.dev.yml down -v`, or `docker volume rm` on the volume named in `docker run`).

## Rollback Procedure

The upgrade is not reversible in place when it applies a breaking migration, and every migration in this release is breaking (see [The Catalog Status](#the-catalog-status)). If an upgrade fails or must be undone (PostgreSQL; for SQLite see [SQLite](#sqlite) below):

1. Stop the server
2. Restore the catalog from the backup, connected as `<admin>` to the `postgres` database:

```bash
psql -h <host> -U <admin> -d postgres -c "DROP DATABASE <catalog-db>;"
psql -h <host> -U <admin> -d postgres -c "CREATE DATABASE <catalog-db> OWNER <owner>;"
psql -h <host> -U <admin> -d <catalog-db> -f catalog_backup_YYYYMMDD.sql
```

To discard data written after the upgrade as well, restore `<data-db>` from its dump the same way.

3. Rebuild the previous version and start it

This manual has no tested rollback for Cassandra. Before migrating, with every server stopped, take a snapshot of the catalog keyspace and every account keyspace on every node with your usual Cassandra tooling; a rollback restores those snapshots, again with every server stopped, before the previous binary starts.

Data written after the upgrade in schema shapes the previous version cannot represent is lost with the restore.

## Version History

The catalog versions below are those of the 0.1.x releases, and the commands, output and procedures in them are those of the 0.1.x binaries. This release upgrades the catalog of any 0.1.x release with the [general upgrade procedure](#general-upgrade-procedure): its migration 1 applies the changes catalogs 0.0.2 to 0.0.4 made, and its migration 5 the change catalog 0.0.5 made. <!-- version-literal-ok: history -->

### Catalog 0.0.5

Removes the storage of the per-table point-in-time recovery flag, which only `DescribeContinuousBackups` read; the engine now answers the three point-in-time recovery operations without storage (see [Point-in-time recovery is reported as unsupported](#v0115-point-in-time-recovery-is-reported-as-unsupported-catalog-migration-required) under Behavior Changes for the wire change and the per-backend `extenddb migrate` steps):

- PostgreSQL: migration `004_drop_continuous_backups.sql` drops the `continuous_backups` table.
- SQLite: the re-applied catalog schema drops the `continuous_backups` table.
- Cassandra: catalog migration `V006__drop_continuous_backups.cql` drops the `continuous_backups` table.
- MongoDB: the catalog version stays at its own value; `extenddb migrate --yes` drops the `continuous_backups` collection from a catalog an earlier build created.

From v1.0 migration 5, `drop_continuous_backups`, drops the table on PostgreSQL, SQLite and Cassandra and the collection on MongoDB.

The upgrade is not reversible in place: a 0.0.4 PostgreSQL or SQLite binary refuses to start against a 0.0.5 catalog (`Catalog version mismatch: expected 0.0.4, found 0.0.5`). Roll back by restoring what you saved before the upgrade. <!-- version-literal-ok: history -->

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

The database is one file plus its `-wal` and `-shm` files beside it. Stop the server before copying them: `migrate --yes` and `serve` take the same exclusive lock on `<database>.lock`, so while a server is up `migrate --yes` waits for up to `--lock-timeout` seconds (60 by default) and then exits 6 (`another extenddb process is already using ...`). `migrate` without `--yes` takes no lock, writes nothing, and runs beside a server. Before its first change `migrate --yes` copies the database to `<database>.pre-migration-<from>-to-<to>-<UTC time>`, readable by its owner only (mode 0600) whatever the database's mode, and prints the copy's path and the database's size before it copies and the time the copy took after. It takes the copy with SQLite's online backup API, which reads the database file and the committed frames of its WAL in one read transaction, so the copy holds every committed write, and its time grows with the database's size. The copy is in rollback-journal mode, so it can be read without a `-shm` file beside it, also from a directory the reader may not write to. A run that stops before it commits anything removes its copy, since the database is unchanged; a run that stops after a step committed keeps its copy and names it in its error. A run that is killed after its copy is complete and before it commits anything leaves the copy; the next run whose own copy has the same bytes removes it once that copy is complete, and names it, also in its error when that run then stops before its first change, unless a `-wal` or `-journal` file beside it shows that a process opened it or the file is a link to the database. Every other copy stays until you remove it. `<database>.lock` stays beside the database after every run and server, also after a refused `serve` or `migrate --yes`. Do not remove it while an ExtendDB process may have it open: a process that then creates and locks a new file of that name does not exclude the process that holds the old one. A stopped server can leave a non-empty `-wal` file, so copy all three.

```bash
extenddb stop --config extenddb.toml
mkdir pre-upgrade && cp -p /path/to/extenddb.sqlite* pre-upgrade/
extenddb migrate --config extenddb.toml          # exit 3, prints the plan
extenddb migrate --config extenddb.toml --yes
extenddb serve --config extenddb.toml
```

The upgrade rebuilds `backup_items` with a stable restore cursor, copying every row into a new table inside one write transaction; the database is locked for writes until it finishes. From v1.0 migration 1 makes this rebuild on a database whose `backup_items` has no `id` column. The preview lists migration 1 and does not name the rebuild. `--yes` prints the rebuild as a note under migration 1, and `verify` prints the same note from the ledger:

```
  Applied 1 baseline
    rebuilt backup_items with an id column, copying 80 row(s); the old table's pages stay free inside the database file until a VACUUM
```

The 0.1.x binaries named the rebuild in the preview, as `Pending: catalog 0.0.3 -> 0.0.4; data migrations [rebuild backup_items (80 rows)].`, and logged its start and end at info.

To roll back, stop the server, put the copied files back in place of the current ones (remove the current `-wal` and `-shm` first), and start the previous binary.

On SQLite, `extenddb serve` reads the catalog's migration ledger before it opens the database and refuses a catalog with pending migrations, exiting 3 under `--foreground` with each pending migration and the `extenddb migrate --config <config> --yes` command to run (in daemon mode it exits 1 and writes the refusal to syslog); a dev build applies them at startup instead, taking the same copy.

The `extenddb migrate` command that `serve`, `migrate` and `verify` advise names the config file by its absolute path, quoted for a POSIX shell, so it can be pasted as printed. A relative `path` in `[storage.sqlite]`, such as the `extenddb.sqlite` that `init` writes, resolves against the directory a command runs in. So when the config file names the database by a relative `path`, or names none, the advised command starts with `EXTENDDB__STORAGE__SQLITE__PATH=<absolute path>`, and it reaches the same database from any directory.

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

### v0.1.15: Point-in-time recovery is reported as unsupported (catalog migration required)

**What changed.** `UpdateContinuousBackups` no longer accepts an enable request, and `DescribeContinuousBackups` no longer reports point-in-time recovery `ENABLED` with a 35-day window. Point-in-time recovery is reported as unsupported: describe answers `PointInTimeRecoveryStatus: DISABLED`, an enable request returns `ContinuousBackupsUnavailableException`, and `RestoreTableToPointInTime` checks the source table and then returns `PointInTimeRecoveryUnavailableException`. A table that does not exist is `TableNotFoundException` on all three operations, where `DescribeContinuousBackups` and `UpdateContinuousBackups` answered `ResourceNotFoundException`, and `UpdateContinuousBackups` without `PointInTimeRecoverySpecification` is a `ValidationException`, where it was taken as disabling. See [Differences from DynamoDB](../differences-from-dynamodb.md) for the full surface. The catalog migration removes the storage of a per-table enable flag that only the description read.

**Who is affected.** Any client or infrastructure template that enables point-in-time recovery (for example a Terraform resource with `point_in_time_recovery = true`) now receives `ContinuousBackupsUnavailableException` and must stop requesting it. A restore attempt now receives `PointInTimeRecoveryUnavailableException` instead of a generic `ValidationException`.

**Migration.** Use on-demand backups (`CreateBackup`, `RestoreTableFromBackup`) instead; they are supported on every backend. `extenddb migrate --yes` removes the flag's storage on each backend:

- PostgreSQL: migration `004_drop_continuous_backups.sql` drops the `continuous_backups` table.
- SQLite: the re-applied catalog schema drops the `continuous_backups` table.
- Cassandra: catalog migration `V006__drop_continuous_backups.cql` drops the `continuous_backups` table. Until it runs, CreateBackup, RestoreTableFromBackup, and DeleteBackup are refused, as on any catalog with a pending migration (see [Cassandra](#cassandra)). Stop every server of an earlier build first: an earlier build's DeleteTable, its account deletion, and its removal of `DELETING` tables write to `continuous_backups` and fail once it is gone, and its `DescribeContinuousBackups` and `UpdateContinuousBackups` answer HTTP 500.
- MongoDB: the catalog version does not change. `extenddb migrate` reports `catalog: drop the continuous_backups collection`, and `--yes` drops it. `extenddb serve` drops nothing, and a new catalog does not create the collection. Stop every server of an earlier build first: an earlier build starts against the migrated catalog (MongoDB has no startup version check), accepts an enable request with `ENABLED` and a 35-day window, and recreates the collection, which the next `extenddb migrate` reports as pending again.

From v1.0 migration 5, `drop_continuous_backups`, drops the table on PostgreSQL, SQLite and Cassandra and the collection on MongoDB.

An earlier PostgreSQL or SQLite binary refuses to start against the migrated catalog; roll back by restoring what you saved before the upgrade, as described under [Rollback Procedure](#rollback-procedure).

---

### v0.1.15: IAM policy variables expand in `Resource` and `NotResource`

**What changed.** In a `2012-10-17` policy document, a `${key}` variable in a
`Resource` or `NotResource` pattern is now expanded from the request before the
pattern is matched, as IAM does. Before this release the pattern was compared as
literal text and matched no table. Text from a variable value or a default is
matched literally, a variable whose key the request does not carry, with no
default, makes the pattern match nothing, and a `2008-10-17` document, or one
with no `Version`, still matches the pattern as written. With a default, the
default is used: `table/${aws:PrincipalTag/team, 'none'}` matches `table/none`
when the request has no `team` tag (measured with the IAM policy simulator,
issue #533). Four decisions change for stored policies:

1. A pattern with a variable now matches the table it names. `Allow
   table/${aws:username}` now grants the caller's own table, where it granted
   nothing. `Deny table/${aws:PrincipalTag/team}` now denies the team's table,
   where it denied nothing. `Deny NotResource table/${aws:PrincipalTag/team}` now
   spares the team's table, where it denied every table, and an `Allow
   NotResource` written that way no longer grants the team's table.
2. A condition value naming a multivalued key, such as `${dynamodb:LeadingKeys}`
   or `${aws:TagKeys}` in a trust policy, is no longer expanded to the key's first
   value; the text is compared as written. Under a positive operator such as
   `StringEquals` the condition no longer matches, and under a negated operator
   such as `StringNotEquals` it now matches. For example, an `Allow` with the
   condition `StringNotEquals` on `aws:PrincipalTag/team` and the value
   `${dynamodb:LeadingKeys}` now grants a caller tagged `team=blue` whose request
   has the leading key `blue`, where it did not, and a `Deny` with that condition
   now applies to the same request.
3. In a `2012-10-17` document, a condition value written as
   `${key, 'default'}` under a string or ARN operator now expands to the
   request's value for `key`, or to `default` when the request does not carry
   `key`. Before, the whole text was compared as written. For example, an
   `Allow` with the condition `StringEquals` on `aws:ResourceTag/owner` and the
   value `${aws:username, 'anon'}` now grants `alice` a table tagged
   `owner=alice`, and a caller with no user name a table tagged `owner=anon`;
   before, it granted neither. In a `2008-10-17` document, in one with no
   `Version`, and under the numeric, date, `Bool` and `Null` operators, the
   text is still compared as written; IAM does not read the default form
   there either (measured with the IAM policy simulator). A user, group or
   role policy or boundary write refuses an `IpAddress` value written as a
   variable as a malformed CIDR, as before. A role trust policy follows its
   own `Version`.
4. Spaces, and the other code points up to U+0020, around the key name of a
   variable are now ignored, in `Resource` and `NotResource` patterns and in the
   condition values item 3 covers. An `Allow` with the condition `StringEquals`
   on `aws:ResourceTag/owner` and the value `${ aws:username }` now grants
   `alice` a table tagged `owner=alice`. Before, the spaces were part of the
   key name, so the variable named a key no request carries, the value was
   compared as written, and the `Allow` granted nothing.

**Policy writes check `Version`.** Writing a user, group or role policy or a
permissions boundary, through the management API or the console, now refuses a
document whose `Version` is not the string `2012-10-17` or `2008-10-17`, for
example `"2012-10-18"` or the number `20121017`, with `Invalid policy document:
policy parse error: invalid Version ...`, or for a permissions boundary `Invalid
permissions boundary document: policy parse error: invalid Version ...`. A
document already stored with such a value keeps evaluating, with its `Resource`
patterns matched as written, but sending it again unchanged is refused with the
same message. A stored `Deny` whose `Resource` or `NotResource` holds a variable
in such a document keeps denying what the pattern as written matches, which for
`table/${aws:PrincipalTag/team}` is nothing, until its `Version` is changed to
`2012-10-17`. To find such documents before upgrading, list the stored policies
and boundaries with the commands under **Who is affected** below and check each
document's `Version`. A session policy passed to `AssumeRole` is checked only
for its `Version` and `Statement` members.

**Who is affected.** Any deployment with a stored policy whose `Resource`,
`NotResource` or condition values contain `${`: inline user, group and role
policies, permissions boundaries and session policies, and, for the condition
value changes in items 2 to 4, role trust policies. Search the stored policies for `${`
(`extenddb manage list-user-policies`, `list-group-policies` and
`list-role-policies` print inline policies, `get-user-boundary` and
`get-role-boundary` permissions boundaries, and `list-roles` trust policies) and
check each match against the cases above before upgrading, in particular an
`Allow` that now grants (a `Resource` pattern with a variable, or a negated
condition whose value names a multivalued key), a `Deny` that now applies (the
same two forms), and a `Deny NotResource` that now spares a table. No catalog
migration is required.

---

### v0.1.14: LeadingKeys enforced on Query, batch and transaction operations

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

### v0.1.6: IAM bare operators on multivalued condition keys are no-ops

**What changed.** A **bare** condition operator (no `ForAnyValue:` / `ForAllValues:`
qualifier) applied to a *multivalued* condition key, `dynamodb:Attributes` or
`dynamodb:LeadingKeys`, now **never matches**, matching AWS IAM exactly. Before
v0.1.6, ExtendDB evaluated a bare operator as an implicit AND across the request's values,
so such a condition could match and appear to enforce a restriction.

**Who is affected.** Any deployment with an IAM policy that uses a bare operator (e.g.
`StringEquals`) on `dynamodb:Attributes` or `dynamodb:LeadingKeys`. A common pattern is a
denylist that *appeared* to protect an attribute:

```json
{ "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
  "Condition": { "StringEquals": { "dynamodb:Attributes": ["ssn"] } } }
```

Under the old behavior this could deny a single-attribute request; after the upgrade it is a
no-op (as it always was on the DynamoDB service). **This is a security-relevant change: policies relying
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

**Migration is required.** `extenddb serve` refuses to start on a catalog that has not been migrated, with an error that names `extenddb migrate --yes`. Run `extenddb migrate --yes` once per deployment before starting the new binary. On PostgreSQL, the 0.1.14 `migrate` connects as the admin user, so pass `--pg-user <admin>` and set `EXTENDDB_PG_PASSWORD`, as for `init`; from v1.0 the rewrite is catalog migration 2, which `migrate --yes` applies as the application role. The migration rewrites every stored `dynamodb:ResourceTag/<key>` condition key and `${dynamodb:ResourceTag/...}` policy variable to the `aws:ResourceTag/` form, keeping the operator, the tag key and the value unchanged. The rewrite preserves the decision of each policy that a release before 0.1.14 stored, because `aws:ResourceTag/` resolves from the same table tags the old `dynamodb:ResourceTag/` key read. A policy that names `dynamodb:ResourceTag/` and that 0.1.14 stored after its last `migrate --yes` had no effect on 0.1.14, which leaves that key absent; migration 2 rewrites it with the others, so it takes effect after the upgrade to v1.0. The one difference is a `${dynamodb:ResourceTag/<key>}` policy variable on a table without that tag: neither form expands, and the literal text in the comparison changes from `${dynamodb:ResourceTag/<key>}` to `${aws:ResourceTag/<key>}`. The decision changes only for a request value equal to one of those literals. A freshly initialised deployment needs no migrate step: 0.1.14's `init` records the catalog as migrated, and from v1.0 `init` applies migration 2 with the others.

**What `migrate` prints.** Without `--yes`, `migrate` lists each policy it would rewrite, one line per policy with the account, the principal, the policy name and the number of condition keys, and writes nothing. With `--yes`, it rewrites and records that the migration is complete. It runs on every `migrate`, so an interrupted run is finished by the next one. If a policy lookup fails, or a policy needs attention (below), `migrate` reports it, exits non-zero, and does not record completion, so a later run retries. Policy bodies are never printed. From v1.0 the preview of a catalog with migration 2 pending prints the number of policies migration 2 rewrites and names the first 20 of them in that form, followed by the number of the others, and `migrate --yes` prints a line for each policy it rewrote, which the ledger keeps as notes of migration 2. A failed lookup is named with the account and principal it was reading and the error's text, and the run exits 1 without recording migration 2; a policy that needs attention makes the run exit 5.

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

**`--reconverge` and policies written after the upgrade.** From v1.0 the rewrite is catalog migration 2, which runs once. `migrate --yes --reconverge` skips it, so a policy written after the upgrade keeps its `dynamodb:ResourceTag/` keys and its decision. This includes a policy that a SQLite dev build from v0.1.7 to v0.1.13 wrote to the catalog after the upgrade: that build evaluated the key against the table's tags, and v1.0 leaves the key absent, as the table above describes. Rewrite such a policy to the `aws:ResourceTag/` form by hand. A v0.1.14 `migrate --yes` runs its rewrite whenever it finds the `dynamodb:` form, including in policies written through the API after the upgrade, so do not run it against a migrated catalog.

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

### v0.1.15: console account pages serve an IAM user its own account only

**What changed.** Every console page and form under `/console/accounts/{account_id}` now redirects an IAM-user session to `/console` when `{account_id}` is not the session's own account, and the answer is the same whether or not an account has that ID. Before this release the account, user, group and role pages checked only that a console session existed. An IAM user who requested another account's ID read that account's name, the names of its users, groups and roles, each user's access key IDs, inline policy names, groups and tags, and each role's trust policy. The forms under that path already refused IAM users, except the access key forms, which accept an IAM user's own keys. A refused form for another account now redirects to `/console`. Before this release 8 of the 17 forms redirected to a page of that account (the forms that create and delete users, groups and roles, and that add and remove group members), the delete account form redirected to `/console/accounts`, and the 2 access key forms and 6 policy forms redirected to `/console`. The console home page now shows the deployment's account count and admin count to admin sessions only, and shows an IAM-user session a link to its own account.

**Who is affected.** IAM users who opened another account's pages in the console. They are redirected to `/console`. The pages of an IAM user's own account and every page an admin session sees are unchanged.

No catalog migration is required.

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

This change needs no catalog migration of its own. Before upgrading a deployment that restricts tagging, check the policies of each account for the statements above.

### IAM: `?` and `*` count UTF-16 code units

**What changed.** `?` in a policy pattern matches one UTF-16 code unit and `*` matches any number of them, as in IAM. Before this release both counted bytes of the value's UTF-8 encoding. The rule applies to `Action`, `NotAction`, `Resource` and `NotResource` patterns and to the `StringLike`, `StringNotLike`, `ArnLike` and `ArnNotLike` condition operators. ASCII values evaluate as before.

**Who is affected.** A policy whose pattern uses `?` against a request value with a character outside ASCII. A character up to U+FFFF, such as `é` or `日`, is one unit and now takes one `?`; before this release it took one `?` per byte (two for `é`, three for `日`). A character above U+FFFF, such as an emoji, is two units and takes `??`, where it took four. A pattern written with one `?` per byte stops matching such a value and a pattern with one `?` per unit starts matching it, so a `Deny` or an `Allow` that relies on such a pattern can change its decision. Review policies that use `?` on condition keys that carry caller data, such as `dynamodb:LeadingKeys` and `dynamodb:Attributes`. No catalog migration is required.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
