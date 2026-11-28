# ExtendDB

> **ExtendDB is an open source project maintained by AWS. It is not Amazon DynamoDB and does not contain any DynamoDB
> source code. It is not an AWS service.** "DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates. ExtendDB is
> a clean-room implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are
> documented in [Differences from DynamoDB](docs/differences-from-dynamodb.md).

ExtendDB is a server that speaks the Amazon DynamoDB API and stores your tables in PostgreSQL, SQLite, MongoDB or Cassandra. Point an AWS SDK or the AWS CLI at its endpoint, sign requests with an access key stored in ExtendDB, and your DynamoDB code runs against a database you operate: on a laptop, in CI, on premises or on a network with no internet access.

[Install](#install) · [First request](#make-your-first-request) · [Storage backends](#storage-backends) · [Differences from DynamoDB](docs/differences-from-dynamodb.md)

The AWS CLI reading back an item it wrote to a local ExtendDB server (the commands are in [Make your first request](#make-your-first-request)):

```text
$ aws dynamodb get-item --endpoint-url https://127.0.0.1:18443 \
    --table-name Music \
    --key '{"Artist": {"S": "No One You Know"},
            "SongTitle": {"S": "Call Me Today"}}'
{
    "Item": {
        "Artist": {
            "S": "No One You Know"
        },
        "SongTitle": {
            "S": "Call Me Today"
        },
        "Year": {
            "N": "2025"
        }
    }
}
```

## Features

- **DynamoDB clients work with an endpoint change.** Tables, items, Query and Scan, secondary indexes, batches, transactions and streams use the DynamoDB wire protocol. An application sets the ExtendDB endpoint and region, trusts its certificate and signs with an ExtendDB key, and needs no other code change for the [operations ExtendDB serves](#supported-operations).
- **Your choice of database.** Build for PostgreSQL, SQLite, MongoDB or Cassandra. Each binary serves one backend, and each backend has a [support tier](#storage-backends). The SQLite build needs no database server.
- **IAM built in.** Every DynamoDB API request must be SigV4-signed, and the server checks it against IAM policies (the dev-mode build for local development skips that check). Manage users, groups, roles, policies and permissions boundaries with `extenddb manage`, and all but permissions boundaries in the web console at `https://127.0.0.1:18443/console/`.
- **Backup and restore.** `CreateBackup` and `RestoreTableFromBackup` back up a table and restore it, to and from a filesystem or S3 backup store that you configure, or the catalog ([Your data](#your-data) says which).
- **Streams, TTL and PartiQL.** Read changes with `GetRecords`, expire items with Time to Live, and run PartiQL statements with `ExecuteStatement`.
- **Vector search.** Find the nearest items to a vector with `SearchVectors`, on PostgreSQL and SQLite.
- **Runs under a supervisor.** `serve --foreground` logs to stderr, `extenddb healthcheck` is a liveness probe that needs no shell, and `/metrics` returns metrics as JSON to a request with admin credentials.
- **Runs in a web page (experimental).** The [WebAssembly build](docs/webassembly.md) answers DynamoDB requests from JavaScript inside the browser, over an in-memory SQLite database, with no ExtendDB server, no IAM and no persistence.

## Install

Building from source needs git, Rust 1.94.0 or later ([rustup](https://rustup.rs)) and a C toolchain (compiler, linker and C library headers). The first request below also needs the AWS CLI version 2. The SQLite backend runs inside the server process, so this route needs no database server:

```sh
git clone https://github.com/ExtendDB/extenddb.git
cd extenddb
cargo build --release -p extenddb --no-default-features --features sqlite
```

The binary is `target/release/extenddb`. Continue with [Make your first request](#make-your-first-request).

Other routes:

- **PostgreSQL** (the default build): `cargo build --release`, and a PostgreSQL 14 or later server. `init` connects to the server you name with `--pg-host` and `--pg-port`, as the admin role you name with `--pg-user`, and reads that role's password from `EXTENDDB_PG_PASSWORD` ([remote PostgreSQL](docs/getting-started.md#remote-postgresql--aurora), [local setup](docs/local-postgres-setup.md)).
- **MongoDB** or **Cassandra**: `cargo build --release --no-default-features --features mongodb` (or `cassandra`). Initialize with the [MongoDB](docs/local-mongodb-setup.md#initializing-extenddb-with-mongodb) or [Cassandra](docs/cassandra-setup.md) setup guide. On MongoDB, `init` needs an `extenddb.toml` that already holds the connection string, and the first request needs an account that you create; the MongoDB guide gives both steps.
- **Container images** that hold the server only and connect to a database you run: [PostgreSQL](docker/README.md), [MongoDB](docker/README-mongodb.md), [Cassandra](docker/README-cassandra.md). The SQLite backend has no production image.
- **For local development and CI**, the [`@extenddb/dev`](packaging/npm/README.md) npm package and the [`extenddb/extenddb-dev`](docs/dev-image.md) image run a dev-mode SQLite build. Dev mode uses plain HTTP and seeds an example key that can do everything, so keep a dev-mode server on loopback. At startup it migrates a catalog that is behind it, after it copies the database file.
- **Installer scripts** that check for Rust, PostgreSQL and Python 3, then install the Python requirements into `.venv` (from the network), build the PostgreSQL binary and build the PDF manuals: `scripts/install-linux.sh` and `scripts/install-macos.sh` ([Linux](docs/manuals/08-install-linux.md), [macOS](docs/manuals/09-install-macos.md)).
- **In a web browser**: the [experimental WebAssembly build](#experimental-webassembly-build) answers DynamoDB requests from JavaScript, with no ExtendDB server. You build it with `wasm-pack`, as [Building it](docs/webassembly.md#building-it) describes.

To update, stop the server, run `git pull` and the same `cargo build`, run `./target/release/extenddb migrate` to list pending migrations and `./target/release/extenddb migrate --yes` to apply them, then start the server again.

## Make your first request

Run these from the `extenddb` folder after the SQLite build. On PostgreSQL, add the connection flags above to `init`. On Cassandra, run `init` as its setup guide gives it. The steps after `init` are the same on these three builds. On MongoDB, run `init` as its setup guide gives it, and in step 3 set `ACCOUNT_ID` with the guide's `create-account` line in place of the `list-accounts` one. The commands use bash or zsh (Linux, macOS, or WSL on Windows).

**1. Initialize.** `init` creates the catalog (the database that holds table definitions, accounts, users, policies, access keys and settings; see [Catalog Model](docs/manuals/01-architecture-guide.md#catalog-model)), an `admin` user, a TLS certificate in `~/.extenddb/tls/` and `extenddb.toml`. On SQLite the catalog and the tables share one file, `extenddb.sqlite`. `init` prints the admin password once; save it.

```sh
./target/release/extenddb init
```

**2. Start the server.** It listens on `https://127.0.0.1:18443` and runs in the background.

```sh
./target/release/extenddb serve
```

**3. Create an access key.** These commands create a user named `dev`, allow it every DynamoDB action, and print its key. First run this line on its own, type or paste the admin password (it is not echoed) and press Enter; `manage` reads it from `EXTENDDB_PASSWORD`:

```sh
read -rs EXTENDDB_PASSWORD && export EXTENDDB_PASSWORD
```

Then run the rest:

```sh
ACCOUNT_ID=$(./target/release/extenddb manage --user admin \
    list-accounts | grep -o '[0-9]\{12\}' | head -n 1)
./target/release/extenddb manage --user admin create-user \
    --account-id "$ACCOUNT_ID" --user-name dev
./target/release/extenddb manage --user admin put-user-policy \
    --account-id "$ACCOUNT_ID" --user-name dev \
    --policy-name DynamoDBAll \
    --policy-document '{"Version":"2012-10-17","Statement":[
      {"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'
./target/release/extenddb manage --user admin create-access-key \
    --account-id "$ACCOUNT_ID" --user-name dev
unset EXTENDDB_PASSWORD
```

**4. Point the AWS CLI at it.** Run this line on its own. `aws configure` asks for the `access_key_id` and `secret_access_key` that `create-access-key` printed; answer `us-east-1` for the region, and press Enter at the output format.

```sh
aws configure --profile extenddb
```

Then run the rest. AWS keys set in the shell override the profile, so the first line clears them. The server accepts only requests signed for its region, `us-east-1` unless `extenddb.toml` sets another, so the second line pins it. The last line trusts the self-signed certificate and turns off the CLI pager.

```sh
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_PROFILE=extenddb AWS_REGION=us-east-1
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem AWS_PAGER=""
```

**5. Create a table and use it.** A new table starts in `CREATING`, and a write before it is `ACTIVE` gets `ResourceNotFoundException`, so wait for it first.

```sh
aws dynamodb create-table --endpoint-url https://127.0.0.1:18443 \
    --table-name Music \
    --attribute-definitions AttributeName=Artist,AttributeType=S \
        AttributeName=SongTitle,AttributeType=S \
    --key-schema AttributeName=Artist,KeyType=HASH \
        AttributeName=SongTitle,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
aws dynamodb wait table-exists --endpoint-url https://127.0.0.1:18443 \
    --table-name Music
aws dynamodb put-item --endpoint-url https://127.0.0.1:18443 \
    --table-name Music \
    --item '{"Artist": {"S": "No One You Know"},
             "SongTitle": {"S": "Call Me Today"},
             "Year": {"N": "2025"}}'
aws dynamodb get-item --endpoint-url https://127.0.0.1:18443 \
    --table-name Music \
    --key '{"Artist": {"S": "No One You Know"},
            "SongTitle": {"S": "Call Me Today"}}'
```

`get-item` prints the item shown at the top of this page. An AWS SDK takes the same endpoint, region, key and certificate. [Getting Started](docs/getting-started.md) covers the other backends and the web console, and [`samples/`](samples/) has Python examples.

Stop the server with `./target/release/extenddb stop`. To undo step 4, run `unset AWS_PROFILE AWS_REGION AWS_CA_BUNDLE AWS_PAGER`; the `extenddb` profile stays in `~/.aws/` until you delete it.

On SQLite, PostgreSQL and MongoDB, `./target/release/extenddb destroy --yes` deletes the databases. It leaves `extenddb.toml`, the backup store and `~/.extenddb/`.

- **SQLite:** `destroy` deletes `extenddb.sqlite` and its `-wal` and `-shm` files. It leaves the `.lock` file and the copies that `migrate --yes` makes, one for each run that applies a migration, named `extenddb.sqlite.pre-migration-<from>-to-<to>-<time>`. Each copy holds the whole database, the catalog included, and by default the key that the access key secrets are encrypted under. Delete the copies too.
- **PostgreSQL:** `destroy` takes the same `--pg-user` and `EXTENDDB_PG_PASSWORD` as `init`, and it leaves the application role.
- **Cassandra:** on a node with `PasswordAuthenticator`, `destroy` cannot sign in at this release. Sign in with `cqlsh` as in the [Cassandra Setup](docs/cassandra-setup.md#local-single-node-setup-docker) guide, list the keyspaces as in its [Verifying the Connection](docs/cassandra-setup.md#verifying-the-connection) section, and drop the catalog keyspace and each account keyspace; for the default prefix, `DROP KEYSPACE extenddb_catalog;` and `DROP KEYSPACE extenddb_account_<account id>;`. Leave the `extenddb` role unless no other deployment on the cluster uses it, because `init` reuses a role that exists.
- **The next `init`:** because `extenddb.toml` stays, a new `init` in the same folder needs `--overwrite`. On SQLite, move `backups/` aside before that `init`: the kept store names the old catalog, so the new catalog's server leaves it unconfigured until `extenddb backup sync --adopt-store` takes it over.

## Storage backends

An ExtendDB binary contains one storage backend, which the build selects. The tier of a backend says what the 1.x releases promise for it:

- Stable: for production. The [versioning policy](docs/versioning.md) applies in full. The ExtendDB CI is reliably green for the backend.
- Beta: for production within the known limits in the table. The ExtendDB CI is reliably green for the backend. The versioning policy applies with one exception. To remove a known limit, a minor release can change the behavior that the table lists, and its release notes name the change.
- Experimental: for evaluation. The backend is not for production use. The versioning policy does not apply. A release can change the backend's behavior or configuration with no deprecation period.

A backend moves up one tier in a minor release. Before that release, the maintainers agree in an issue that the ExtendDB CI has been reliably green for the backend. They also agree that its known limits fit the higher tier. The release notes name the move. A backend moves down a tier only through the [deprecation steps](docs/versioning.md#deprecation) of the versioning policy.

| Backend | Tier | Topology | Data across upgrades | API coverage | CI | Known limits |
|---|---|---|---|---|---|---|
| PostgreSQL | Stable | PostgreSQL 14 or later, on the same host, on another host or as a managed service. | `extenddb migrate --yes` migrates the catalog in place. CI upgrades catalogs that 0.1.14 and earlier releases created. | The operations in [Supported operations](#supported-operations). Vector search needs the pgvector extension. | `integration`, required, on PostgreSQL 16 | Time to Live (TTL) removes about 100 expired items per table per minute. |
| SQLite | Stable (single node) | SQLite runs in the server process. A lock file allows one server for each database file. There is no production image, so build from source. | As PostgreSQL. `migrate --yes` copies the database file before its first change. | The operations in Supported operations. | `integration`, required | One write transaction at a time. TTL as PostgreSQL. |
| MongoDB | Beta | MongoDB 7.0 or later, as a replica set, also for one node. | As PostgreSQL. | No vector search. On a global secondary index (GSI) with more than one RANGE attribute, `Query` takes a key condition on the first one only. | `mongodb-integration`, required, on MongoDB 7 | A number in a key attribute has at most 34 significant digits. The database names are fixed, so one replica set holds one catalog. TTL as PostgreSQL. |
| Cassandra | Experimental | Cassandra 4.1 or later, with `PasswordAuthenticator`. Each table takes its writes in one datacenter. | `migrate --yes` migrates the catalog. No rollback procedure is tested. | No vector search. CreateBackup keeps each backup in the catalog, with or without a backup store. The `Query` limit of MongoDB on a GSI with more than one RANGE attribute. | `cassandra-integration`, required, on Cassandra 4.1 | Some management operations are not atomic. A few known race conditions are open. TTL removes about 1,000 expired items per table per minute. See [Known Limitations](docs/cassandra-setup.md#known-limitations). |

On every backend, run one ExtendDB server for each catalog. Only SQLite enforces this. [Differences from DynamoDB](docs/differences-from-dynamodb.md) lists the differences of each backend from the service. The [versioning policy](docs/versioning.md) says what a release can change.

## Supported operations

- **Tables:** CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, DescribeEndpoints, DescribeLimits
- **Items:** PutItem, GetItem, DeleteItem, UpdateItem (SET, REMOVE, ADD and DELETE actions; condition expressions; all ReturnValues modes)
- **Query and Scan:** key conditions, filters, projections, pagination, index selection
- **Batches and transactions:** BatchGetItem (100 keys), BatchWriteItem (25 requests), TransactGetItems (100 items), TransactWriteItems (100 actions)
- **PartiQL:** ExecuteStatement, BatchExecuteStatement, ExecuteTransaction
- **Vector search:** SearchVectors, and vector indexes on CreateTable and UpdateTable (cosine, Euclidean and dot product; on-demand tables only; five per table). PostgreSQL needs the pgvector extension on the data database, checked when the server starts; without it every vector operation is refused and nothing else is affected. MongoDB and Cassandra refuse every vector operation.
- **Streams:** ListStreams, DescribeStream, GetShardIterator, GetRecords
- **Backups:** CreateBackup, DescribeBackup, ListBackups, DeleteBackup, RestoreTableFromBackup, DescribeContinuousBackups. Point-in-time recovery is not supported: UpdateContinuousBackups refuses to enable it, and RestoreTableToPointInTime is refused.
- **Other:** UpdateTimeToLive, DescribeTimeToLive, TagResource, UntagResource, ListTagsOfResource, ImportTable and ExportTableToPointInTime (to and from local folders, off until you configure them)

ExtendDB does not implement global tables, contributor insights or Kinesis streaming destinations. Every operation not listed above answers `UnknownOperationException`. [Differences from DynamoDB](docs/differences-from-dynamodb.md#operations-not-implemented) lists some of them.

## Configuration

`extenddb init` writes `extenddb.toml`, and the other commands read it from the current folder unless you pass `--config`. [`extenddb.sample.toml`](extenddb.sample.toml) lists the main keys with their defaults. An environment variable with the `EXTENDDB__` prefix overrides a key for every command that reads the configuration, for example `EXTENDDB__SERVER__PORT=9000`. `init` and `destroy` take the database locations from the file, and `init` also from its flags, not from these variables. A running server picks up a runtime setting within 30 seconds, with no restart:

```sh
./target/release/extenddb settings set log_level debug
./target/release/extenddb settings list   # secrets are masked
```

TLS is always on outside the dev-mode build, and the server refuses to start with it disabled. To use a certificate from your own CA, set its paths in `extenddb.toml`:

```toml
[server.tls]
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"
```

To add a name to the generated certificate, such as a service DNS name, pass `--tls-san` (repeatable) to `init`. `init` keeps a certificate and key it finds in `~/.extenddb/tls/`, and fails if that certificate does not cover a requested name.

`extenddb --help` lists the commands: `serve`, `init`, `destroy`, `backup`, `verify`, `migrate`, `status`, `stop`, `settings`, `manage`, `catalog-check`, `encryption-key`, `healthcheck` and `version`. The [Admin Guide](docs/manuals/05-admin-guide.md) covers the CLI, the management API and the runtime settings.

## Run it under a supervisor

`extenddb serve` puts itself in the background and logs to syslog. Under Docker, Kubernetes, `systemd` `Type=simple` or another supervisor, run it in the foreground; it then logs to stderr and writes no PID file, so stop it through the supervisor (`SIGTERM` shuts it down gracefully):

```sh
./target/release/extenddb serve --foreground
```

For a container `HEALTHCHECK`, `extenddb healthcheck` probes `/health` and exits 0 when `/health` answers 2xx (also when it reports `degraded`) and 1 when not. It needs no shell or `curl`, and it accepts the self-signed certificate. `/health` is a liveness check: it does not query the storage backend after startup. When the backend is unreachable at startup, the server stops. `serve --foreground` exits 1 with the error. A background `serve` reports the failure only when it comes within about 3 seconds of the start; after that it has already printed that it started, and the error goes to syslog.

The first line below checks health, the second reads the metrics (curl prompts for the admin password), and the third follows the background server's log on Linux:

```sh
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
curl --cacert ~/.extenddb/tls/cert.pem -u admin \
    https://127.0.0.1:18443/metrics
journalctl -t extenddb -f
```

On macOS, the first line below shows the last 5 minutes of the log, and the second follows it:

```sh
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 5m
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info
```

The [container image READMEs](docker/README.md) and the [Deployment Guide](docs/manuals/11-deployment-guide.md) cover the full `init`, `migrate` and `serve` lifecycle in production.

## Your data

- On SQLite, the tables and the catalog are in `extenddb.sqlite` (or `init --sqlite-path`), with `-wal`, `-shm` and `.lock` files beside it, and the copies that `migrate --yes` makes before it changes the file.
- `extenddb.toml` holds the storage connection settings, including the database password on PostgreSQL and Cassandra and the connection string, as you gave it, on MongoDB; `init` writes it readable by your user only.
- Admin passwords are stored as bcrypt hashes. Access key secrets are stored encrypted, but by default under a key that the catalog itself holds, so anyone with a copy of the catalog can read them. `extenddb encryption-key switch` replaces it with a key file that you keep outside the catalog ([Admin Guide](docs/manuals/05-admin-guide.md#encryption-key)).
- Backups go to `backups/` beside a SQLite database file, or to the folder or S3 bucket in `[backup.store]`. With no store, and always on Cassandra, a backup stays in the catalog. See [Backup and Restore](docs/manuals/14-backup-and-restore.md).
- The server listens on `127.0.0.1:18443` unless you change `bind_addr` and `port`. It connects to its storage backend, and to S3 and the AWS credential sources when the backup store is S3.

## What ExtendDB is not

- Amazon DynamoDB: ExtendDB is a separate implementation of the API. It answers every operation that [Supported operations](#supported-operations) does not list with `UnknownOperationException`. [Differences from DynamoDB](docs/differences-from-dynamodb.md) lists the known differences.
- A managed service: you provide the database, and the database's own tools handle replication, failover and database backups.
- A multi-server deployment: run one ExtendDB server for each catalog. In 1.0, more than one server on one catalog is not supported: each server runs every background worker, and credential caches are not shared, so a revoked key stays valid on another server until its cache expires. See the [Deployment Guide](docs/manuals/11-deployment-guide.md#multi-instance-considerations).

## Experimental: WebAssembly build

The WebAssembly build runs ExtendDB inside a web page. JavaScript in the page sends it DynamoDB requests, and it answers them from an in-memory SQLite database inside the page, with no ExtendDB server. Do not use it for data you need to keep or to protect with IAM, or for tests that check the errors a server returns. The build is experimental and outside the [versioning and support policy](docs/versioning.md): its surface may change or be removed in any release. Its crate is not published, so you build the module from source.

The [WebAssembly build](docs/webassembly.md) page says [what it does not do](docs/webassembly.md#what-it-does-not-do), [what it refuses](docs/webassembly.md#what-it-refuses) and [which checks of the server it does not make](docs/webassembly.md#checks-the-server-makes-that-this-build-does-not), how to [build it](docs/webassembly.md#building-it), and how to [use it from JavaScript](docs/webassembly.md#using-it-from-javascript).

## Documentation

[Getting Started](docs/getting-started.md) walks through setup on each backend. The [Admin Guide](docs/manuals/05-admin-guide.md) covers the CLI, configuration and IAM. The [Deployment Guide](docs/manuals/11-deployment-guide.md), [Security Model](docs/manuals/10-security-model.md) and [Backup and Restore](docs/manuals/14-backup-and-restore.md) cover running ExtendDB in production. Also see the [Architecture Guide](docs/manuals/01-architecture-guide.md), [Differences from DynamoDB](docs/differences-from-dynamodb.md), the [Versioning and Support Policy](docs/versioning.md), [Troubleshooting](docs/troubleshooting.md), [Verifying Container Images](docs/verifying-images.md) and the [WebAssembly build](docs/webassembly.md). `python3 docs/build-docs.py` builds the manuals as PDFs, with the packages in [`requirements.txt`](requirements.txt).

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers the build, the tests and the code style, and the [Developer & Test Guide](docs/manuals/06-developer-test-guide.md) sets up the Python environment for the integration tests. Decisions are recorded as [ADRs](docs/adr/README.md). Changes that affect the wire protocol or its operations, the storage trait or a backend contract, the auth model, the on-disk format, the schema or migration semantics, or the public CLI or the configuration file format go through an [RFC](docs/rfcs/README.md) before implementation, as does any significant new feature or subsystem. A new storage backend starts with [RFC-0002: Backend Plugin Policy](docs/rfcs/0002-backend-plugin-policy.md) and the [Backend Acceptance Criteria](docs/rfcs/0003-backend-acceptance-criteria.md). [`.github/CODEOWNERS`](.github/CODEOWNERS) names the reviewers of protected paths.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full text.

This software is provided "as is" without warranty of any kind.
