---
description: "Build ExtendDB from source on Linux or macOS, start a database, initialize a deployment and run the server, with the steps for every backend."
---

# Install from source

A build from source gives you one binary for one storage backend. This page covers the whole path: installing the database, building, `extenddb init`, `extenddb serve`, and the smoke test. For a server in one command without a build, see [Getting started](start.md) (the development image) or [Docker Compose with PostgreSQL](local-dev-docker-compose.md).

## Prerequisites

- Rust 1.88 or newer (`rustup update`)
- A storage backend, one of: PostgreSQL 14 or newer, MongoDB 7.0 or newer running as a replica set, Apache Cassandra 4.1, or SQLite, which needs nothing installed
- AWS CLI version 2, for the smoke test and the first requests
- Python 3.10 or newer, if you use the installer scripts (they create a virtual environment for the documentation build)

## Installer scripts

The repository ships one installer script per platform. Each checks dependencies, builds the release binary, sets up a Python virtual environment for the documentation build, and renders the PDF documentation. The scripts report missing software and exit; they never install anything.

```bash
# Linux
scripts/install-linux.sh

# macOS
scripts/install-macos.sh
```

After a script completes, continue at [3. Initialize the deployment](#3-initialize-the-deployment).

## 1. Install and start the database

Skip this step for SQLite, or if your database is already running; continue at [2. Build ExtendDB](#2-build-extenddb).

### Install PostgreSQL

**Ubuntu or Debian**

```bash
sudo apt-get update
sudo apt-get install -y postgresql postgresql-client
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Fedora or RHEL 9 and newer**

```bash
sudo dnf install -y postgresql-server postgresql
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Amazon Linux 2023**

```bash
sudo dnf install -y postgresql15-server postgresql15
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Amazon Linux 2** has no PostgreSQL 14 package of its own; install PostgreSQL 15 from the PGDG repository and run it as your own user with a data directory under your home:

```bash
sudo rpm -ivh --nodeps \
    https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo yum install -y --releasever=7 \
    --disablerepo="*" --enablerepo="pgdg15" \
    postgresql15-server postgresql15
/usr/pgsql-15/bin/initdb -D ~/pgdata --auth=trust --no-locale --encoding=UTF8
/usr/pgsql-15/bin/pg_ctl -D ~/pgdata -l ~/pgdata/server.log start
```

**macOS (Homebrew)**

```bash
brew install postgresql@17
brew services start postgresql@17
export PATH="$(brew --prefix postgresql@17)/bin:$PATH"
```

The `postgresql@17` formula is keg-only, so its binaries (`pg_isready`, `psql`) are not on `PATH` until the export above or `brew link --overwrite postgresql@17`. On Homebrew the PostgreSQL superuser is your macOS user name, not `postgres`, with trust authentication over the local socket. The same is true of the Amazon Linux 2 setup above.

Confirm the server accepts connections:

```bash
pg_isready
```

```
/var/run/postgresql:5432 - accepting connections
```

On macOS the socket directory shown is `/tmp`.

Vector indexes need the pgvector extension on the data database. `init` tries to create it and prints a notice when it is not installed; the server runs without it and refuses vector indexes until it is present.

### Install MongoDB

ExtendDB uses multi-document transactions, so MongoDB must run as a replica set, including a single node.

**Container**

```bash
docker run -d --name extenddb-mongo -p 27017:27017 mongo:7 --replSet rs0
docker exec extenddb-mongo mongosh --quiet --eval "rs.initiate()"
```

**Ubuntu or Debian**

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
    https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org
```

Add the replica set name to `/etc/mongod.conf`, restart `mongod`, then run `mongosh --eval "rs.initiate()"`:

```yaml
replication:
  replSetName: rs0
```

**macOS (Homebrew)**

```bash
brew tap mongodb/brew
brew install mongodb-community@7.0
```

Add the same `replication` block to the file that `brew --prefix mongodb-community@7.0` points at (`/opt/homebrew/etc/mongod.conf` on Apple silicon), then:

```bash
brew services restart mongodb-community@7.0
mongosh --eval "rs.initiate()"
```

Wait a few seconds for the primary election and confirm:

```bash
mongosh --eval "rs.status().ok"
```

```
1
```

### Install Cassandra

The test suite runs against the `cassandra:4.1` image with password authentication. A single node for development:

```bash
docker run -d --name extenddb-cassandra -p 9042:9042 \
    -e CASSANDRA_CLUSTER_NAME=extenddb -e HEAP_NEWSIZE=128M -e MAX_HEAP_SIZE=1024M \
    cassandra:4.1
```

Cassandra takes a minute or more to start. It is ready when this returns the version:

```bash
docker exec extenddb-cassandra cqlsh -e 'SELECT release_version FROM system.local'
```

`extenddb init` creates an application role with a password, which the image's default `AllowAllAuthenticator` rejects (`CassandraRoleManager doesn't support PASSWORD`). Switch the node to `PasswordAuthenticator` and restart it; the `cassandra` superuser with password `cassandra` then exists:

```bash
docker exec extenddb-cassandra sed -i \
    's/^authenticator: .*/authenticator: PasswordAuthenticator/' /etc/cassandra/cassandra.yaml
docker restart extenddb-cassandra
```

It is ready again when this returns the version (about 15 seconds):

```bash
docker exec extenddb-cassandra cqlsh -u cassandra -p cassandra \
    -e 'SELECT release_version FROM system.local'
```

## 2. Build ExtendDB

```bash
git clone https://github.com/ExtendDB/extenddb.git
cd extenddb
```

```bash
# PostgreSQL (the default feature)
cargo build --release

# MongoDB
cargo build --release --no-default-features --features mongodb

# Cassandra
cargo build --release --no-default-features --features cassandra

# SQLite
cargo build --release --no-default-features --features sqlite
```

The backend features are mutually exclusive: a build that enables more than one is rejected at compile time, and each build produces a single-backend binary at `target/release/extenddb`. To run two backends side by side, build one binary per backend.

Check the build:

```bash
./target/release/extenddb version
```

```
extenddb 0.1.14
catalog 0.0.4 (postgres)
commit ceb6a63a
built 2026-10-24T08:37:05Z
```

`extenddb -V` prints the same. A source build carries the same release version as the published `0.1.14` images and a newer catalog: 0.0.4 here, 0.0.3 in the images (see the [upgrade manual](manuals/07-upgrade-manual.md#upgrading-a-container-deployment)). <!-- version-literal-ok: names the published tag's catalog, which is behind the tree -->

## 3. Initialize the deployment

`extenddb init` creates the application database user, the catalog and data databases, applies the schema migrations, generates an AES-256-GCM key for access key storage, creates a default account and an `admin` user, generates a self-signed TLS certificate at `~/.extenddb/tls/`, and writes `extenddb.toml`. For PostgreSQL, Cassandra and SQLite, `init` writes the whole file: do not write one by hand first. MongoDB is the exception, below.

`init` connects as a database administrator to create the databases. Which flags you pass depends on the backend and on who that administrator is.

### Initialize on PostgreSQL

On a system PostgreSQL (Ubuntu, Debian, Fedora, RHEL, Amazon Linux 2023) the administrator is `postgres`:

```bash
./target/release/extenddb init --pg-user postgres
```

Where PostgreSQL runs as your own user (Homebrew on macOS, the Amazon Linux 2 setup above), omit `--pg-user`; it defaults to your user name:

```bash
./target/release/extenddb init
```

For a remote server or Amazon Aurora, give the host and the administrator password. Put the password in `EXTENDDB_PG_PASSWORD` rather than on the command line:

```bash
export EXTENDDB_PG_PASSWORD='<admin password>'
./target/release/extenddb init \
    --pg-host my-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com \
    --pg-user postgres
```

Without a password `init` connects with peer or ident authentication, which works only over a local Unix socket. To use a socket, pass the socket directory to `--pg-host` as an absolute path (`--pg-host /var/run/postgresql`).

### Initialize on MongoDB

The MongoDB build reads the connection string from the config file, so write a two-section `extenddb.toml` first and let `init` complete it:

```bash
cat > extenddb.toml <<'EOF'
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
EOF

./target/release/extenddb init --backend mongodb --config extenddb.toml --overwrite
```

`--overwrite` lets `init` replace the seed file with the full config; without it `init` exits because the file exists. The databases are `extenddb_catalog` and `extenddb_data`; the MongoDB build does not take `--data-db` or `--catalog-db`. For a container on another port use `mongodb://localhost:27018/?replicaSet=rs0&directConnection=true`.

### Initialize on Cassandra

```bash
export EXTENDDB_CASSANDRA_PASSWORD=cassandra
./target/release/extenddb init --backend cassandra \
    --cassandra-contact-points 127.0.0.1:9042 \
    --cassandra-user cassandra
```

`init` connects as the superuser named by `--cassandra-user`, creates the `extenddb` application role, the catalog keyspace and one keyspace for the default account (further accounts get their own on demand), and writes `[storage.cassandra]` with the contact points, the application role, `keyspace_prefix = "extenddb"` and `replication_factor = 1`. Raise the replication factor for a cluster. `--keyspace-prefix` changes the prefix the keyspaces are named with.

### Initialize on SQLite

```bash
./target/release/extenddb init --backend sqlite --sqlite-path extenddb.sqlite
```

The path is written into the generated config under `[storage.sqlite].path`, so `serve` finds it without further flags. `:memory:` gives an ephemeral database that is discarded on shutdown. At serve time the environment variable `EXTENDDB__STORAGE__SQLITE__PATH` overrides the config.

### Options

What `init` prints, abridged (the full output also names the certificate files it generated and, when the rendered documentation is absent, says that `/console/docs` is disabled):

```
=== extenddb init (backend: postgres) ===
--- Ensuring application user 'extenddb' exists...
    Created user 'extenddb'.
--- Creating database 'extenddb_catalog'...
    Created.
--- Creating database 'extenddb'...
    Created.
--- Running catalog migrations...
    Migrations applied.
--- Running data migrations...
    Data migrations applied.
--- Generating AES-256-GCM encryption key...
    Encryption key stored.
--- Creating default account '976532242161'...
    Account ID: 976532242161
--- Creating admin user 'admin'...

  Admin credentials (shown once, save them now)
  Username: admin
  Password: <generated>

--- Generating extenddb.toml...
    Created extenddb.toml

=== extenddb init complete ===
```

Save the admin password and the account ID. The password is shown once and cannot be retrieved; the account ID is the one you create IAM users in.

`init` refuses to run when the catalog or data database already exists. To start over, run `extenddb destroy --config extenddb.toml --yes` first (see [Tear down](#tear-down)). It also refuses to overwrite an existing `extenddb.toml` unless `--overwrite` is given.

| Flag | What it does |
|---|---|
| `--data-db <name>` | Data database name (default `extenddb`); PostgreSQL |
| `--catalog-db <name>` | Catalog database name (default `<data-db>_catalog`); PostgreSQL |
| `--extenddb-user <name>` | The application database user `init` creates (default `extenddb`); PostgreSQL and Cassandra |
| `--pg-host <host>`, `--pg-port <port>` | PostgreSQL host (or absolute socket directory) and port |
| `--bind-addr <addr>` | Address the server binds to; also added to the certificate's Subject Alternative Names |
| `--tls-san <name>` | Another name or address for the certificate, repeatable (for example an in-cluster service name) |
| `--config <path>` | Where to write the config file (default `extenddb.toml`) |

If the certificate files already exist at `~/.extenddb/tls/`, `init` keeps them. To regenerate with different names, delete `cert.pem` and `key.pem` there and run `init` again with the `--bind-addr` and `--tls-san` values the clients will use; every address in a client's endpoint URL must be a SAN or TLS verification fails. To make a certificate yourself instead:

```bash
mkdir -p ~/.extenddb/tls
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ~/.extenddb/tls/key.pem \
  -out ~/.extenddb/tls/cert.pem \
  -days 3650 \
  -subj "/CN=extenddb self-signed/O=extenddb" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:myhost.example.com,IP:10.0.1.5"
chmod 600 ~/.extenddb/tls/key.pem
```

## 4. Verify the deployment

```bash
./target/release/extenddb verify --config extenddb.toml
```

```
=== extenddb verify ===
Config:           extenddb.toml
Catalog database: extenddb_catalog

--- Checking catalog connection...
  OK: Connected to catalog.
--- Checking catalog version...
  OK: Catalog version 0.0.4
--- Checking data database...
  OK: Connected to data database 'extenddb'.
--- Enumerating tables...
  Tables: 0
  Indexes: 0

=== HEALTHY: All checks passed ===
```

## 5. Start the server

```bash
./target/release/extenddb serve --config extenddb.toml
```

```
extenddb 0.1.14 (catalog 0.0.4) starting on 127.0.0.1:18443
  storage: postgres (postgresql://extenddb:***@localhost:5432/extenddb_catalog)
extenddb server started (pid 8644, 127.0.0.1:18443)
```

The server runs as a daemon and logs to syslog. The banner says "starting": the socket accepts connections a moment later.

```bash
./target/release/extenddb status --config extenddb.toml
```

```
extenddb is running on port 18443 (pid 8644)
```

Read the logs:

```bash
# Linux
journalctl -t extenddb -f

# macOS
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info
```

Stop it:

```bash
./target/release/extenddb stop --config extenddb.toml
```

```
extenddb stopped (pid 8644)
```

[Administration](manuals/05-admin-guide.md) covers the server lifecycle, the configuration file, runtime settings and monitoring.

## 6. Smoke test

The health endpoint needs no credential:

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
```

```json
{"status":"healthy"}
```

Every DynamoDB request must be signed with an access key, so create an IAM user in the default account and give it a policy and a key. `extenddb manage` reads the server endpoint from `extenddb.toml` in the current directory; `--endpoint host:port` overrides it. Use the admin password and the account ID that `init` printed.

```bash
./target/release/extenddb manage --user admin --password '<admin password>' \
    create-user --account-id <account-id> \
    --user-name alice --user-password '<console password>'

./target/release/extenddb manage --user admin --password '<admin password>' \
    put-user-policy --account-id <account-id> --user-name alice \
    --policy-name FullAccess \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

./target/release/extenddb manage --user <account-id>/alice --password '<console password>' \
    create-access-key
```

```json
{
  "access_key_id": "AKIAEXTENDDB...",
  "secret_access_key": "..."
}
```

The secret is shown once. Point the AWS CLI at the server and list tables:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:18443
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<access_key_id>
export AWS_SECRET_ACCESS_KEY=<secret_access_key>
aws dynamodb list-tables
```

```json
{
    "TableNames": []
}
```

The management console is at `https://127.0.0.1:18443/console/`; log in as `admin` with the password `init` printed, or as `<account-id>/alice` with the console password.

The server is ready. [First requests](first-requests.md) walks through tables, items, batches, transactions and streams from here, and the [usage guide](manuals/03-usage-guide.md) has the SDK configuration for Java, Rust, JavaScript and Python.

## Upgrading after a `git pull`

When the new binary expects a newer catalog version than the deployed one, `serve` refuses to start and `verify` reports a version mismatch. Rebuild and migrate:

```bash
cargo build --release
./target/release/extenddb migrate --config extenddb.toml --pg-user <admin>
./target/release/extenddb migrate --config extenddb.toml --pg-user <admin> --yes
```

The first command prints what is pending and changes nothing (exit status 3 when work is pending); the second applies it. Table data is not touched. The [upgrade manual](manuals/07-upgrade-manual.md) has the backup step, the rollback procedure and the per-release behavior changes.

## Tear down

```bash
./target/release/extenddb stop --config extenddb.toml
./target/release/extenddb destroy --config extenddb.toml --yes
```

On PostgreSQL, `destroy` drops both databases, connecting as the same administrator `init` used (`--pg-user`, `EXTENDDB_PG_PASSWORD`); the `extenddb` application role, `extenddb.toml` and the certificate under `~/.extenddb/tls/` are left in place and a later `init` reuses them. On MongoDB it drops the two databases through the configured connection string. On SQLite it removes the database file. `--yes` is required; there is no interactive prompt.

On Cassandra, `destroy` has no way to take the superuser's credentials (it accepts only the PostgreSQL flags), so it cannot connect to a node with password authentication. Drop the keyspaces with `cqlsh` instead; `DESCRIBE KEYSPACES` lists them under the prefix, `extenddb_catalog` and `extenddb_account_<id>` by default:

```bash
docker exec extenddb-cassandra cqlsh -u cassandra -p cassandra \
    -e "DROP KEYSPACE extenddb_account_<id>; DROP KEYSPACE extenddb_catalog; DROP ROLE extenddb;"
```

## Linux and macOS differences

| Item | Linux | macOS (Homebrew) |
|---|---|---|
| PostgreSQL administrator | `postgres` (pass `--pg-user postgres`) | your user name (omit `--pg-user`) |
| Service manager | `systemctl` or `pg_ctl` | `brew services` or `pg_ctl` |
| Syslog reader | `journalctl -t extenddb` | `log stream --predicate 'processImagePath ENDSWITH "extenddb"'` |
| Socket directory in `pg_isready` | `/var/run/postgresql` | `/tmp` |

## Troubleshooting

| Symptom | Fix |
|---|---|
| `connection refused` on port 18443 | The server is not running: `./target/release/extenddb serve --config extenddb.toml` |
| `Catalog version mismatch: expected X, found Y` | `./target/release/extenddb migrate --config extenddb.toml --pg-user <admin> --yes` |
| `role "postgres" does not exist` | PostgreSQL runs as your user: omit `--pg-user` |
| `FATAL: Peer authentication failed` | Allow `trust` or `md5` for local connections in `pg_hba.conf` |
| `Database '<name>' already exists` from `init` | `./target/release/extenddb destroy --config extenddb.toml --yes`, then `init` again |
| `UnrecognizedClientException` from the AWS CLI | The access key ID or secret does not match what `create-access-key` returned |
| `AccessDeniedException` | The user's policy does not allow the operation |
| `ResourceNotFoundException` right after `create-table` | The table is still `CREATING`; `aws dynamodb wait table-exists --table-name <name>` |

[Troubleshooting](troubleshooting.md) lists every startup, stop and runtime error message with its cause.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
