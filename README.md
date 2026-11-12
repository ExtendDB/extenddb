# ExtendDB

> **ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does
> not contain any DynamoDB source code.** "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room
> implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are documented
> in [Differences from DynamoDB](docs/differences-from-dynamodb.md).

A DynamoDB-compatible API adapter, ExtendDB speaks the DynamoDB wire protocol — any AWS SDK, CLI, or tool that works
with DynamoDB works with ExtendDB, unchanged.

## Use Cases

- **Local development** — run DynamoDB workloads on your laptop with zero cloud dependency
- **CI/CD pipelines** — deterministic integration tests against a DynamoDB-compatible backend
- **Self-hosted deployments** — run DynamoDB workloads on your own infrastructure (on-premises, private cloud, edge)
- **Multi-cloud** — use DynamoDB semantics on any cloud that runs an ExtendDB-supported database backend
- **Air-gapped environments** — DynamoDB functionality with no internet connectivity

## Scope

ExtendDB is a server that implements the Amazon DynamoDB API on a database that you provide: PostgreSQL, SQLite, MongoDB or Cassandra. An application connects to it through the endpoint setting of an AWS SDK or the AWS CLI. The application signs each request with an access key stored in ExtendDB.

ExtendDB 1.0 is for:

- Local development and continuous integration (CI) test runs.
- Self-hosted deployments: on premises, in a private or public cloud, at the edge, or on a network with no internet access.

ExtendDB 1.0 is not:

- Amazon DynamoDB: ExtendDB is a separate implementation of the API. [Differences from DynamoDB](docs/differences-from-dynamodb.md) lists the known differences and the operations that ExtendDB does not implement, for example global tables.
- A managed service: you provide the database, and the database's own tools handle replication, failover and database backups.
- A multi-server deployment: run one ExtendDB server for each catalog. In 1.0, more than one server on one catalog is not supported.

[Storage backends](#storage-backends) says what 1.0 promises for each backend. The [versioning policy](docs/versioning.md) says what a release can change.

## Features

- Full DynamoDB wire protocol: CRUD, Query, Scan, Batch, Transactions, Streams, TTL, Import/Export
- Pluggable storage: PostgreSQL (default), SQLite, MongoDB, and Cassandra
- SigV4 authentication with IAM compatibility: users, groups, roles, policies, permissions boundaries
- Web management console for account and credential administration
- TLS with automatic self-signed certificate generation (replaceable with CA-signed certs)
- CSRF protection, security headers, session management
- JSON metrics endpoint with DynamoDB CloudWatch-style metric names and dimensions
- Daemon mode with syslog logging, plus `--foreground` for container and supervisor environments

## Storage backends

An ExtendDB binary contains one storage backend, which the build selects. The tier of a backend says what the 1.x releases promise for it:

- Stable: for production. The [versioning policy](docs/versioning.md) applies in full. The ExtendDB CI is reliably green for the backend.
- Beta: for production within the known limits in the table. The ExtendDB CI is reliably green for the backend. The versioning policy applies with one exception. To remove a known limit, a minor release can change the behavior that the table lists, and its release notes name the change.
- Experimental: for evaluation. The backend is not for production use. The versioning policy does not apply. A release can change the backend's behavior or configuration with no deprecation period.

A backend moves up one tier in a minor release. Before that release, the maintainers agree in an issue that the ExtendDB CI has been reliably green for the backend. They also agree that its known limits fit the higher tier. The release notes name the move. A backend moves down a tier only through the [deprecation steps](docs/versioning.md#deprecation) of the versioning policy.

| Backend | Tier | Topology | Data across upgrades | API coverage | CI | Known limits |
|---|---|---|---|---|---|---|
| PostgreSQL | Stable | PostgreSQL 14 or later, on the same host, on another host or as a managed service. | `extenddb migrate --yes` migrates the catalog in place. CI upgrades catalogs that 0.1.x releases created. | The operations in [Supported Operations](#supported-operations). Vector search needs the pgvector extension. | `integration`, required, on PostgreSQL 16 | Time to Live (TTL) removes about 100 expired items per table per minute. |
| SQLite | Stable (single node) | SQLite runs in the server process. A lock file allows one server for each database file. There is no production image, so build from source. | As PostgreSQL. `migrate --yes` copies the database file before its first change. | The operations in Supported Operations. | `integration`, required | One write transaction at a time. TTL as PostgreSQL. |
| MongoDB | Beta | MongoDB 7.0 or later, as a replica set, also for one node. | As PostgreSQL. | No vector search. On a global secondary index (GSI) with more than one RANGE attribute, `Query` takes a key condition on the first one only. | `mongodb-integration`, required, on MongoDB 7 | A number in a key attribute has at most 34 significant digits. The database names are fixed, so one replica set holds one catalog. TTL as PostgreSQL. |
| Cassandra | Experimental | Cassandra 4.1 or later, with `PasswordAuthenticator`. Each table takes its writes in one datacenter. | `migrate --yes` migrates the catalog. No rollback procedure is tested. | No vector search. CreateBackup keeps each backup in the catalog, with or without a backup store. The `Query` limit of MongoDB on a GSI with more than one RANGE attribute. | `cassandra-integration`, required, on Cassandra 4.1 | Some management operations are not atomic. A few known race conditions are open. TTL removes about 1,000 expired items per table per minute. See [Known Limitations](docs/cassandra-setup.md#known-limitations). |

On every backend, run one ExtendDB server for each catalog. Only SQLite enforces this. [Differences from DynamoDB](docs/differences-from-dynamodb.md) lists the differences of each backend from the service.

The `@extenddb/dev` npm package and the `extenddb/extenddb-dev` image run a dev-mode SQLite build, for local development and CI. Dev mode uses plain HTTP and seeds an example key that can do everything, so keep a dev-mode server on loopback. At startup it migrates a catalog that is behind it, after it copies the database file.

## Quick Start

```bash
# Build
cargo build --release

# Initialize (creates databases, admin credentials, TLS cert, config file)
./target/release/extenddb init

# Start
./target/release/extenddb serve --config extenddb.toml

# Use with any AWS SDK (TLS with self-signed cert — trust via AWS_CA_BUNDLE)
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem

# Requests must be signed with an access key this server issued. Create an IAM
# user, attach a policy that allows DynamoDB, and create an access key for the
# user, with the admin password that init printed. manage reads the password
# from EXTENDDB_PASSWORD, which keeps it out of the shell history and the
# process list. list-accounts prints the account_id of the default account
# that init created.
read -rs EXTENDDB_PASSWORD && export EXTENDDB_PASSWORD   # the admin password, not echoed
./target/release/extenddb manage --user admin list-accounts
./target/release/extenddb manage --user admin \
    create-user --account-id <account-id> --user-name quickstart
./target/release/extenddb manage --user admin \
    put-user-policy --account-id <account-id> --user-name quickstart \
    --policy-name FullAccess \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'
./target/release/extenddb manage --user admin \
    create-access-key --account-id <account-id> --user-name quickstart
unset EXTENDDB_PASSWORD

# Set the access_key_id and secret_access_key that create-access-key printed
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
aws dynamodb list-tables --endpoint-url https://127.0.0.1:18443 --region us-east-1
```

See [Getting Started](docs/getting-started.md) for the full walkthrough, or use the platform installer scripts:

```bash
scripts/install-linux.sh   # Linux
scripts/install-macos.sh   # macOS
```

## Prerequisites

- Rust 1.91.1+ (`rustup update`)
- Python 3.10+ (for test suites and documentation)
- A storage backend:
  - PostgreSQL 14+ (see `docs/local-postgres-setup.md`)
  - SQLite (embedded)
  - MongoDB 7+ (see `docs/local-mongodb-setup.md`)
  - Cassandra 4.1+ (see `docs/cassandra-setup.md`)

### Python Environment

```bash
python3 -m venv ~/venvs/extenddb-venv
source ~/venvs/extenddb-venv/bin/activate
pip install -r requirements.txt
```

## Authentication Modes

ExtendDB ships with builtin IAM-like authentication enabled by default. All requests must be signed with valid SigV4 credentials created via the management API.

| Mode | Config | Description |
|------|--------|-------------|
| Builtin IAM-like | `auth.provider = "builtin"` | Full SigV4 signature verification with local credential store and IAM policy evaluation. This is the default and only supported mode. |

## Configuration

`extenddb init` generates `extenddb.toml` automatically. See `extenddb.sample.toml` for all keys, defaults, and descriptions.

Environment variable overrides use the `EXTENDDB__` prefix:

```bash
export EXTENDDB__SERVER__PORT=9000
export EXTENDDB__AUTH__PROVIDER=builtin
```

Runtime settings (no restart required):

```bash
extenddb settings --config extenddb.toml set log_level debug
```

## TLS

TLS is mandatory. `extenddb init` generates a self-signed certificate at `~/.extenddb/tls/cert.pem`. The server refuses to start with TLS disabled.

To use the self-signed cert with AWS CLI and SDKs, set `AWS_CA_BUNDLE`:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

Replace with a CA-signed certificate for production:

```toml
[server.tls]
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"
```

## Running in Containers

The production images contain ExtendDB only. The storage backend runs as a separate service. See
[`docker/README.md`](docker/README.md) for the PostgreSQL image, [`docker/README-mongodb.md`](docker/README-mongodb.md) for the MongoDB image,
and [`docker/README-cassandra.md`](docker/README-cassandra.md) for the Cassandra image (published from 1.0): external database requirements,
hardened local Compose stacks, smoke tests, and explicit init/migrate/serve production lifecycle. The SQLite
backend has no production container image; [`docs/dev-image.md`](docs/dev-image.md) covers the SQLite development image.

By default `extenddb serve` daemonizes itself, which doesn't play well with container runtimes and process supervisors that expect PID 1 to stay attached. Pass `--foreground` (alias `--no-daemon`) to keep the process in the foreground and stream logs to stderr instead of syslog:

```bash
extenddb serve --config extenddb.toml --foreground
```

Use this with Docker, Kubernetes, `systemd Type=simple`, runit, s6, or any other supervisor that captures stdout/stderr.

In `--foreground` mode extenddb writes no PID file and never creates `run_dir`, so the container can run with a read-only root filesystem. Two consequences:

- `extenddb status` still works (it probes the port) but reports the PID as unknown.
- `extenddb stop` cannot signal the process — there is no PID file to read. Stop it through your supervisor, which delivers `SIGTERM` and triggers the same graceful shutdown.

Pass `--write-pid-file` to opt back in when you do want `stop` and `status` to work against a foreground server, for example when running it from a shell rather than under a supervisor. The file goes to the usual `run_dir` path, so neither command needs extra arguments, and `run_dir` has to be writable.

For a container `HEALTHCHECK`, use the `healthcheck` subcommand rather than `curl`; it needs no shell or extra binaries, so it works on a `distroless`/`scratch` image and accepts the self-signed certificate:

```bash
extenddb healthcheck --config extenddb.toml            # exit 0 healthy, 1 not
extenddb healthcheck --endpoint https://127.0.0.1:18443 # explicit target
```

This is a liveness check, which is what a container `HEALTHCHECK` wants. The `/health` path does not query the storage
backend, so it will report healthy even if the storage backend becomes unreachable after startup. That is deliberate,
since a liveness probe that failed on a database outage would restart every replica at once. A backend that is
unreachable at startup does stop the server from listening. There is no separate readiness endpoint yet.

### One instance per database

Run one `extenddb serve` per catalog. On SQLite this is enforced: the server takes an exclusive lock on `<database>.lock`
next to the database file and a second server on the same file will refuse to start (`init`, `migrate --yes`,
`migrate --forget` and `destroy` take the same lock while they run; the `migrate` preview and `verify` take none). On
PostgreSQL, MongoDB and Cassandra nothing prevents a second ExtendDB instance from running, but running more than one
against the same catalog is not supported in this release: every instance runs every background worker, credential and
policy caches are per instance with no cross-instance invalidation (a revoked key stays valid on the other instance for
up to the cache TTL), and `/health` does not check the database.
See the [deployment guide](docs/manuals/11-deployment-guide.md#multi-instance-considerations).

To make the generated self-signed certificate valid for the name clients use — an in-cluster service DNS name, for example — pass `--tls-san` to `init` (repeatable):

```bash
extenddb init --catalog-db extenddb_catalog \
  --tls-san extenddb.default.svc.cluster.local --tls-san extenddb.example.com
```

`init` never regenerates an existing certificate, so if one is already present it verifies that it already covers every requested `--tls-san` and fails with an explicit error if it does not, rather than silently dropping the name.

## Monitoring

```bash
# Health check
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health

# JSON metrics (DynamoDB CloudWatch-style), admin credentials required
curl --cacert ~/.extenddb/tls/cert.pem -u admin:<admin-password> https://127.0.0.1:18443/metrics

# Syslog (Linux)
journalctl -t extenddb -f

# Syslog (macOS)
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info
```

## Management Console

Web-based administration at `https://127.0.0.1:18443/console/`. Manage accounts, users, groups, roles, policies, and access keys through a browser. Accept the self-signed certificate warning on first visit.

## CLI Reference

```
extenddb serve --config extenddb.toml          # Start server (daemon)
extenddb serve --config extenddb.toml --foreground  # Start in foreground
extenddb init --catalog-db NAME                # Initialize deployment
extenddb stop --config extenddb.toml           # Graceful shutdown
extenddb status --config extenddb.toml         # Check if running
extenddb healthcheck --config extenddb.toml    # Probe /health (exit 0 healthy, 1 not)
extenddb verify --config extenddb.toml         # Validate deployment
extenddb migrate --config extenddb.toml        # Apply schema migrations
extenddb destroy --config extenddb.toml        # Tear down deployment
extenddb settings --config extenddb.toml list  # List runtime settings (secrets masked)
extenddb manage --user admin --password <pw> <subcommand>  # IAM management
```

See [Admin Guide](docs/manuals/05-admin-guide.md) for the full CLI and management API reference.

## Supported Operations

### Table Operations
CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, DescribeEndpoints, DescribeLimits

### Item Operations
PutItem, GetItem, DeleteItem, UpdateItem (SET, REMOVE, ADD, DELETE actions; condition expressions; all ReturnValues modes)

### Query & Scan
Query, Scan (key conditions, filters, projections, pagination, index selection)

### Batch & Transactions
BatchGetItem (100 keys), BatchWriteItem (25 ops), TransactGetItems (100 items), TransactWriteItems (100 ops)

### Vector Search
SearchVectors, and vector indexes on CreateTable and UpdateTable (cosine, Euclidean and dot product; on-demand tables only). On PostgreSQL this requires the pgvector extension on the data database; without it every vector operation is refused and nothing else is affected.

### Streams
ListStreams, DescribeStream, GetShardIterator, GetRecords

### Other
UpdateTimeToLive, DescribeTimeToLive, TagResource, UntagResource, ListTagsOfResource, ImportTable, ExportTableToPointInTime

## Project Structure

```
crates/
  auth/               — SigV4 verification, IAM policy engine
  bin/                — CLI, config, daemon lifecycle
  core/               — types, validation, expressions (pure sync Rust, no async)
  engine/             — operation handlers
  server/             — HTTP server, management API, web console
  storage/            — storage trait definitions
  storage-cassandra/  — Cassandra backend (experimental)
  storage-mongodb/    — MongoDB backend
  storage-postgres/   — PostgreSQL backend (default/reference)
  storage-sqlite/     — SQLite backend (embedded)


docs/
  adr/              — architecture decision records
  design/           — architecture and design documents
  manuals/          — user-facing guides (PDF pipeline)
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Full setup walkthrough |
| [Architecture Guide](docs/manuals/01-architecture-guide.md) | System design and crate structure |
| [Admin Guide](docs/manuals/05-admin-guide.md) | Server lifecycle, configuration, IAM management |
| [Security Model](docs/manuals/10-security-model.md) | Threat model, authentication, authorization |
| [Deployment Guide](docs/manuals/11-deployment-guide.md) | Self-hosted, multi-cloud, air-gapped deployments |
| [Backup and Restore](docs/manuals/14-backup-and-restore.md) | Backup strategy, recovery figures, restore drill, catalog-loss recovery |
| [Differences from DynamoDB](docs/differences-from-dynamodb.md) | Behavioral differences and unsupported operations |
| [Versioning and Support Policy](docs/versioning.md) | What each release can change, and how long a release gets fixes |
| [Troubleshooting](docs/troubleshooting.md) | Common errors and solutions |
| [Verifying Container Images](docs/verifying-images.md) | Build provenance and SBOM attestations for the published images |

Build PDFs:

```bash
python3 docs/build-docs.py
```

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for build,
test, and code-style requirements.

ExtendDB uses two lightweight processes for tracking decisions:

- **[ADRs](docs/adr/README.md)** record decisions that have been made.
- **[RFCs](docs/rfcs/README.md)** propose changes that affect the wire protocol,
  storage trait, auth model, on-disk format, public CLI, or any significant
  new feature or subsystem. Substantial changes go through an RFC before
  implementation.

Adding a storage backend? Start with [RFC-0002: Backend Plugin Policy](docs/rfcs/0002-backend-plugin-policy.md)
and the [Backend Acceptance Criteria](docs/rfcs/0003-backend-acceptance-criteria.md).

Code in protected paths is reviewed via [`.github/CODEOWNERS`](.github/CODEOWNERS).

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full text.

This software is provided "as is" without warranty of any kind.
