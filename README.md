# ExtendDB

> **ExtendDB is an open source project maintained by AWS. It is not Amazon DynamoDB and does not contain any DynamoDB
> source code. It is not an AWS service.** "DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates. ExtendDB is
> a clean-room implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are
> documented in [Differences from DynamoDB](docs/differences-from-dynamodb.md).

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
| PostgreSQL | Stable | PostgreSQL 14 or later, on the same host, on another host or as a managed service. | `extenddb migrate --yes` migrates the catalog in place. CI upgrades catalogs that 0.1.14 and earlier releases created. | The operations in [Supported Operations](#supported-operations). Vector search needs the pgvector extension. | `integration`, required, on PostgreSQL 16 | Time to Live (TTL) removes about 100 expired items per table per minute. |
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
and [`docker/README-cassandra.md`](docker/README-cassandra.md) for the Cassandra image (published from 0.1.15): external database requirements,
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

## Experimental: WebAssembly build

The `extenddb-wasm-adapter` crate compiles the engine to `wasm32-unknown-unknown`
so it runs in a web browser over an in-memory SQLite database, with no server
and no network. JavaScript constructs an engine and submits a DynamoDB request
through a direct in-process call (`ExtendDb.create()` / `submit(operation, body)`),
and gets the DynamoDB-shaped response back.

This build is **experimental** and **outside the [versioning and support
policy](docs/versioning.md)**: its surface may change or be removed in any
release, and it is not covered by the compatibility guarantees the server
backends carry. It is an internal crate (`publish = false`) and is not released
to crates.io.

### What it does not do

- **No authentication and no authorization.** It verifies no signature and runs
  no IAM policy evaluation; every request runs under the placeholder account
  `000000000000`. Do not put data in it you would not put in client-side
  browser memory.
- **Memory-only storage.** The database lives in wasm linear memory and is gone
  when the page unloads. There is no persistence, no file, and no import/export.
  The module declares no memory maximum, so linear memory can grow to the
  wasm32 limit of 4 GiB (65,536 pages of 64 KiB), or less if the JavaScript
  engine allows less. Near that limit, a write that SQLite cannot allocate for
  answers `InternalServerError`, and a request whose Rust code cannot allocate
  aborts, which traps the module. After the first write fails this way, a read
  can trap too: in a test that filled a table to the limit, a `Scan` with
  `Select` `COUNT` over the table did. Deleting data before any request has
  trapped frees space, and the instance then keeps serving writes. What a trap
  leaves behind is in the next item.
- **No recovery from a trap.** On `wasm32` a panic cannot unwind, so it
  traps. A trap leaves the instance in an undefined state (a later call may
  answer, fail or hang), so reload the page to get a fresh module instance.
  The calling contract of `submit` (which objects it may be called on, and how
  it delivers errors and traps to the caller) is in its documentation,
  `ExtendDb::submit` in `crates/wasm-adapter/src/wasm.rs`.
- **No background tasks.** The build runs no background workers, so: an item
  past its time-to-live is still returned (TTL never deletes it); `ItemCount`
  and `TableSizeBytes` stay `0`; and stream records are never trimmed.
- **A synchronous control plane.** The in-memory engine sets the control-plane
  delay and the index propagation delay (the `control_plane_delay_seconds` and
  `index_propagation_delay_ms` settings) to 0. So a table it creates is
  `ACTIVE` when `CreateTable` answers, a change that `CreateTable` or
  `UpdateTable` makes is complete when it answers, and a write has updated
  every index of the table when it answers. A server with the SQLite backend
  uses 0.25 s and 10 ms while the settings are not set, and then answers
  differently in the cases below. A request that it refuses in one of them
  leaves its tables as they were, where this build makes the change. The
  corpus cases whose names start with "control plane:" record this build's
  answers.
  - **A new table.** After `CreateTable`, the server's table is `CREATING`
    until the server's control-plane worker, which runs about once a second,
    next runs at least 0.25 s later. Until then the server answers
    `CreateTable` and `DescribeTable` with `TableStatus` `CREATING` and with no
    `WarmThroughput` on the table or on its global secondary indexes (for a
    table or index whose `CreateTable` set `WarmThroughput` units, with those
    units and `Status` `UPDATING`), and `DescribeTable` with `IndexStatus`
    `CREATING` for each vector index. Unless its handler refuses the request
    first for another reason (a body that does not parse, or a request to
    disable a time to live that is not enabled, for example), it answers
    `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`,
    `SearchVectors`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and
    `TransactWriteItems` on the table with `ResourceNotFoundException`
    "Requested resource not found"; `UpdateTable` and `UpdateTimeToLive` with
    `ResourceInUseException` "Table <name> is not in ACTIVE state"; and
    `TagResource`, `UntagResource` and `ListTagsOfResource` on the table's
    ARN or its stream's with `ResourceNotFoundException` "Requested resource
    not found: ResourceArn: <arn> not found". Every other operation gets the
    answer it gets for an `ACTIVE` table, apart from the next case and the
    operations this build refuses (see "What it refuses").
  - **A changed warm throughput.** A `CreateTable` or `UpdateTable` that
    carries a `WarmThroughput` for the table or for a global secondary index
    (an empty one, and one for an index that the `UpdateTable` creates,
    included) sets it, and an `UpdateTable` that switches a provisioned table
    to on-demand can set the table's. For 0.25 s after that, the server's
    answers that describe the table while it is not `CREATING` (those of
    `UpdateTable`, `DescribeTable` and `DeleteTable`) give that
    `WarmThroughput` `Status` `UPDATING` and the values that
    `WarmThroughputRecord::reported` (in
    `crates/core/src/types/warm_throughput.rs`) gives while it is `UPDATING`.
    This build gives `Status` `ACTIVE` and the values after the request.
  - **A write.** The server's background worker applies a write to a global
    secondary index or a vector index no sooner than 6 ms after the write (it
    waits a random 6 to 10 ms, from just over half of the 10 ms delay to all of
    it), so a `Query` or `Scan` of that global secondary index, or a
    `SearchVectors` on that vector index, that arrives before the worker has
    applied the write answers as before the write. A local secondary index is
    written with the table on both.
  - **A vector index that `UpdateTable` adds.** This build refuses the
    request (see "What it refuses"). The server adds the index and builds it
    after it has answered, so from then on its table has an index that this
    build's does not. Its `DescribeTable` and `UpdateTable` answers list the
    index with `IndexStatus` `CREATING` for at least 1 s (the default of the
    `vector_index_min_creating_ms` setting) and while it is built; until then
    it answers `DeleteTable` with `ResourceInUseException` "Cannot delete
    table while indexes are being created, updated, or deleted."; and, as for
    any table with a vector index, it answers an `UpdateTable` that switches
    the table to provisioned billing with `ValidationException` "One or more
    parameter values were invalid: Vector indexes are only supported for
    PAY_PER_REQUEST tables".
  - **No other state.** With a control-plane delay under 1 s the server drops
    a table before `DeleteTable` answers, so it has no `DELETING` table;
    `UpdateTable` leaves a table `ACTIVE`; and a global secondary index that
    `UpdateTable` creates or deletes is built or dropped before `UpdateTable`
    answers.

`hmac_sha256` (HMAC-SHA256, in `crates/core/src/crypto.rs`) is compiled for
this build, and so are its two callers: the SigV4 signing-key and signature
code in `crates/auth/src/sigv4/signing_key.rs`, and `key_id` in
`crates/storage/src/key_ring.rs`, which gives the key ring's key ids. No
request reaches them: the adapter builds no credential store, verifies no
signature and uses no key ring. On `wasm32`, `hmac_sha256` uses the RustCrypto
`hmac` 0.12 and `sha2` 0.10 crates, which have no zeroize support. A call
there can leave copies on the stack, until later calls overwrite them, of the
zero-padded key block, of the two SHA-256 states derived from it (together
they compute the HMAC of any message under the key) and of the bytes of the
message after its last whole 64-byte block (the message of `catalog_key_id` is
the catalog key). Which of them a call leaves depends on the build profile,
the caller and the length of the message; in a release build as in a dev
build, a call can leave the states and the message bytes. A native build's
AWS-LC backend wipes its copy of the key block and its HMAC context, but a
call there can leave copies of the states and the message bytes on the stack
too.

The AES-256-GCM cipher that seals stored secrets (`seal` and `open` in
`crates/core/src/crypto.rs`) compiles in too, but no request reaches it: the
in-memory engine holds no encryption key. If it were called on `wasm32`, the
`zeroize` features of its RustCrypto crates would wipe the cipher's AES round
keys and POLYVAL key where the cipher is dropped, but not the copies that
`seal` and `open` leave on the stack, which stay in linear memory after the
calls return until later calls overwrite them. They include copies of the AES
round keys, of the GHASH key and the POLYVAL key derived from it, and of the
call's working values: the CTR keystream blocks (the keystream XOR the
ciphertext is the plaintext), the tag mask (the encryption of the first
counter block) and plaintext blocks. Which of them a call leaves depends on
the call, the length of the value and the build profile; in a release build
as in a dev build, `open` can leave keystream or plaintext blocks of the value
it returns.

### What it refuses

Operations that need a facility the in-browser build lacks return a
`ValidationException` (HTTP 400, which an SDK does not retry), with a message
that names the build:

- The store-backed backup operations — `CreateBackup`, `DescribeBackup`,
  `DeleteBackup`, `RestoreTableFromBackup` — "… the code that serves it is not
  compiled for wasm32." The engine's backup subsystem does not build for
  `wasm32`, so the engine refuses these four there, after the body parse, as
  it refuses any other known operation whose handler is compiled only off
  `wasm32`. A body that is not JSON gets `SerializationException`, as on a
  server; a JSON body gets the refusal, even one the handler would refuse,
  such as a `DescribeBackup` with no `BackupArn` (natively and on a server,
  "1 validation error detected: Value null at 'backupArn' …"). Built
  natively, the adapter runs the engine's backup handlers instead, and with no
  backup store `CreateBackup` backs a table up in the catalog, as a server
  without one does.
- The PartiQL operations — `ExecuteStatement`, `BatchExecuteStatement`,
  `ExecuteTransaction` — "… the PartiQL surface is not part of the browser build."
  This build refuses them by name, before it parses the body, so every body
  gets the refusal: a body that is not JSON, which a server answers with
  `SerializationException` "Start of structure or map found where not
  expected: …", and a JSON body the handler would refuse, such as an
  `ExecuteStatement` with no `Statement` (on a server, "1 validation error
  detected: Value null at 'statement' failed to satisfy constraint: Member
  must not be null").
- `DescribeEndpoints` — "… it is called in-process and has no network endpoint
  to report." It is refused by name too, so a body that is not JSON gets the
  refusal, where a server answers `SerializationException`.
- An `UpdateTable` that creates a vector index — "… it cannot build a vector
  index; create the table with its vector indexes instead." The SQLite backend
  refuses it after the engine has validated the request, so a request for a
  missing table or with a malformed `Create` gets the engine's own answer
  (`ResourceNotFoundException`, `SerializationException`). The refusal comes
  before the catalog commit, so the table is left as it was and the instance
  keeps serving. On a server, the same request starts the index build.

The parity corpus (`crates/wasm-adapter/parity/corpus.json`) runs on the
native build and on this one with the same expectations, except in the cases
that carry an `onWasm` expectation for this build. Eight do. Two are for the
vector-index refusal: "UpdateTable creating a vector index on VecT adds the
index to the table" expects the refusal here, and "DescribeTable VecT after the
vector-index create" expects the table to keep only the index it was created
with. The other six are the backup cases with a JSON body (one
`CreateBackup`, three `DescribeBackup`, one `DeleteBackup` and one
`RestoreTableFromBackup`), which expect the backup refusal here and the backup
handlers' answers natively.

`ListBackups`, `DescribeContinuousBackups`, `UpdateContinuousBackups`,
`ImportTable` and `ExportTableToPointInTime` run the engine's own handlers, as
on a server, so a malformed request gets the handler's validation error on both
builds. This build creates no backup (see above) and has no import or export
paths, so `ListBackups` lists no backups, `DescribeContinuousBackups` reports
point-in-time recovery `DISABLED` for a table that exists and
`TableNotFoundException` for one that does not, and `ImportTable` /
`ExportTableToPointInTime` return the "disabled" `ValidationException` of a
server with no import or export paths. As the server does, the build refuses
every `RestoreTableToPointInTime` (`PointInTimeRecoveryUnavailableException`)
and an `UpdateContinuousBackups` that enables point-in-time recovery
(`ContinuousBackupsUnavailableException`) before any table is read, so a
request naming a missing table gets the refusal too.

### Checks the server makes that this build does not

Before the HTTP server passes a request to the engine, its request handler
(`handle_request` in `crates/server/src/handler.rs`) and its authorization
(`authorize_request` in `crates/server/src/request_helpers.rs`) check it. Both
builds run the checks the engine owns, `parse_request` and
`ParsedRequest::admit` in `crates/engine/src/pre_dispatch.rs`, except on the
operations this build refuses by name before them, whatever the body (see
"What it refuses"). This build makes none of the
server's other checks: it passes the request to the engine's handler for the
operation, so where a check below refuses a request, this build answers what
the handler answers, and the two answers can differ.

The server makes the checks in the table for every caller, in developer mode
too, except the IAM decision of **signature**, which it makes only outside
developer mode. Outside developer mode its authorization also refuses, before
the IAM decision, a batch or transaction outside its item counts, with the
error the handler gives for it. Before dispatch it reads storage too. It looks
up the key schema of the table that a `GetItem`, `PutItem`, `DeleteItem`,
`UpdateItem`, `Query` or `Scan` names, and outside developer mode of every
table a batch or transaction names, and answers a storage fault in that lookup
with the error the engine's `storage_err_to_dynamo` gives for it. To answer a
resource of another 12-digit account (see **accounts**), it looks the account
up in its catalog, and answers a failure of that lookup with
`InternalServerError` "Internal error during authorization". This build makes
no lookup before the handler's own.

Its HTTP framing checks have no counterpart in `submit(operation, body)`: the
connection deadlines (a request's first byte and its HTTP/1 headers must
arrive within the request timeout, its body must not stall for longer, and an
HTTP/2 client must answer the keep-alive pings), the redirect of plain HTTP on
a TLS port, the router's 404 and 405 answers to a path it has no route for or
a method other than `POST` on `/`, the refusal of a repeated `host` or
`authorization` header, and the checks of the `X-Amz-Target` header. Nor has
this build the server's refusal of every request while a Cassandra, MongoDB or
PostgreSQL backend registers again with its catalog, or the
`AccessDeniedException` "User: is not authorized to perform this operation" it
gives a request that passes its signature check and `admit` when its state has
no catalog store, which `extenddb serve` never builds. The server's name checks of a `RestoreTableToPointInTime` never
run: `admit` refuses that operation first, on both builds.

The checks of a request's names and ARNs (**table names**, **index names**,
**table ARNs** and **names inside other ARNs**) and **StreamArn kind** come
before the **accounts** answer, except that on the ten control-plane
operations of **table ARNs** the account of a `TableName` ARN comes first. For
an ARN in another account that the server's catalog holds, the **tag
ResourceArn kind**, **StreamArn scope**, **BackupArn kind** and **export
TableArn** checks come before the **accounts** answer; for one it does not
hold, after it, except the **BackupArn kind** check of a backup ARN's table
segment, which comes first in every account. Before the **accounts** answer, a
batch or transaction outside its item counts gets the count error, which its
handler gives as well.

The table gives, for each check, the requests it refuses and both answers. In
an answer, `<name>`, `<arn>` and `<value>` stand for the value the request sent
in the member the check reads, and `<member>` for that member's location as the
**table names** row writes it. For these checks the server reads the keys of
`RequestItems` in the order its JSON objects keep: byte order (`B` before `a`),
whatever order the request sends them in, unless the build includes the
MongoDB backend, whose `bson` dependency turns on `serde_json`'s
`preserve_order` and so keeps the order the request sends (a build of the
`extenddb` binary with its `mongodb` feature, or of the whole workspace); and it
reads `TransactItems` members in order, and within a member its `Put`,
`Delete`, `Update` and `ConditionCheck` in that order. Where a batch or
transaction sends several values that the **table names** or **table ARNs**
check refuses, the answer is for the first of them in that order.
Three rows read a placeholder from elsewhere:
the `ListBackups` row of **table ARNs** takes `<name>` from the ARN's
`table/<name>` resource, **GetRecords iterator scope** takes `<value>` from the
iterator's ARN field, and **accounts** says what its `<principal>`, `<action>`
and `<arn>` stand for. "The pattern" is `[a-zA-Z0-9_.-]+`. This build's answer
is the one to a request that is otherwise well formed. The server makes these
checks before the handler reads the rest of the body, so where the rest of the
body is something the handler refuses, this build gives the handler's error for
it instead (for example `SerializationException` "Start of structure or map
found where not expected: missing field `Key`"), and an operation that ignores
the member a check reads answers as it always does (`DescribeLimits` with any
`TableName` answers its limits). A request for an operation this build refuses
(see "What it refuses") gets that refusal. The parity corpus records each of
this build's answers below in a case whose name starts with the check and a
colon, except these: the refusals, which the cases listed under "What it
refuses" record; the answer to an oversized body, which the unit test
`a_list_tables_body_over_16_mib_is_answered` in
`crates/wasm-adapter/src/runtime.rs` records, in native builds only (the
`tests` module that holds it is not compiled for `wasm32`); and the signature
and throttling answers, which no test records.

This table is kept by hand against the server source. No test checks it
against the server, because the corpus records only this build's answers, so
a server check that is added or changed leaves the table out of date until
someone edits it.

| Check | The requests it refuses | The server answers | This build answers |
|---|---|---|---|
| **body size** | any request whose body is over 16 MiB | `RequestEntityTooLargeException` (HTTP 413) "Request size has exceeded the maximum allowed size" | the request: it has no size limit, so `ListTables` with a 17 MiB body answers `{"TableNames":[]}` |
| **signature** | a request without a SigV4 signature the server can verify with a key it issued and, outside developer mode, one its IAM policies do not allow | its authentication errors, such as `MissingAuthenticationToken`, `IncompleteSignatureException`, `UnrecognizedClientException` and `InvalidSignatureException`, and its authorization errors | the request, under account `000000000000` |
| **table names** | a `TableName` outside the pattern (`bad name!`) on `GetItem`, `Query`, `Scan`, `SearchVectors` and `UpdateTable` | `ValidationException` "1 validation error detected: Value '<name>' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"; for a name shorter than 3 or longer than 255 characters, "2 validation errors detected: ", the length clause ("Member must have length greater than or equal to 3" or "… less than or equal to 255") and then the pattern clause, joined by "; " | `ResourceNotFoundException` "Requested resource not found" |
| | a `RequestItems` key outside the pattern on `BatchGetItem` and `BatchWriteItem` | the same, at 'requestItems', with the constraint "Map keys must satisfy constraint: [Member must have length less than or equal to 255, Member must have length greater than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]" | `ResourceNotFoundException` "Requested resource not found" |
| | a `TableName` outside the pattern in a `TransactItems` member on `TransactGetItems` and `TransactWriteItems` | the same, at 'transactItems.<n>.member.<op>.tableName', with `<n>` counted from 1 and `<op>` one of `get`, `put`, `delete`, `update` and `conditionCheck` | `ResourceNotFoundException` "Requested resource not found" |
| | a `TableName` outside the pattern on `PutItem`, `UpdateItem`, `DeleteItem`, `CreateTable`, `DescribeTable`, `DeleteTable`, `DescribeTimeToLive` and `UpdateTimeToLive` | the answer at 'tableName' above | the engine's name check, which measures the name in bytes: for a name shorter than 3 or longer than 255 bytes, its length clause alone ("1 validation error detected: Value '<name>' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3", or "… less than or equal to 255"), and otherwise the pattern clause alone, which is the server's answer only when the name also has 3 to 255 characters (`éa`, 2 characters in 3 bytes, gets the pattern clause alone) |
| | a `TableName` outside the pattern on `DescribeContinuousBackups`, `UpdateContinuousBackups` and `ListBackups` | the answer at 'tableName' above | the same answer: their handlers count characters too |
| | a `TableName` outside the pattern on `DescribeLimits` and `GetRecords`, which take none (nor do the operations this build refuses by name, under "What it refuses") | the answer at 'tableName' above | the operation's answer to the rest of the request: `DescribeLimits` answers its limits, and `GetRecords` answers for its `ShardIterator` |
| | an `ExclusiveStartTableName` outside the pattern on `ListTables` (a value starting with `arn:` is refused only when it carries U+0000) | the same, at 'exclusiveStartTableName' | `ValidationException` "1 validation error detected: Value '<name>' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", with no length clause |
| | a `TableCreationParameters.TableName` outside the pattern on `ImportTable` (as above for `arn:`) | the same, at 'tableCreationParameters.tableName' | `ValidationException` "Import is disabled. Configure [import] paths in extenddb.toml to enable." |
| | a `TargetTableName` outside the pattern on `RestoreTableFromBackup` (as above for `arn:`); the server checks the `BackupArn` first (see **BackupArn kind**) | the same, at 'targetTableName' | the refusal of the operation (see "What it refuses") |
| | a `TableName` on `ListStreams` that is outside the pattern, has 1, 2 or more than 255 characters, or is an ARN | `ValidationException` "TableName must be at least 3 characters long and at most 255 characters long" | `{"Streams":[]}` |
| **index names** | an `IndexName` outside the pattern (`bad index!`) on `Query`, `Scan` and `SearchVectors` | `ValidationException` "1 validation error detected: Value '<name>' at 'indexName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+" | `ValidationException` "The table does not have the specified index: <name>", and `ResourceNotFoundException` "Requested resource not found" when the table does not exist |
| **table ARNs** | a `TableName`, a `RequestItems` key or a `TransactItems` member's `TableName` that starts with `arn:` and is not a plain table ARN (`arn:bogus`, an index, stream or backup ARN), names another vendor (`arn:aws:s3:…:table/T`), has a region with a character outside `[a-z0-9-]`, a table segment outside the pattern, or an account segment that is not 12 digits; on `GetItem`, `Query`, `Scan`, `SearchVectors`, `UpdateTable`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems` | `ValidationException` "1 validation error detected: Value '<arn>' at '<member>' failed to satisfy constraint: <reason>", with the first of these reasons that applies: "Valid ARN format is 'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', …", "Invalid vendor code", "Invalid AWS region", "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", "Invalid subscriber ID"; at 'requestItems' the constraint is "Map keys must satisfy constraint: [Valid resource name or resource arn must be given. …]". On `BatchWriteItem` an account fault gets the **accounts** answer instead, unless the account carries U+0000. On `CreateTable`, `DescribeTable`, `UpdateTable`, `DeleteTable`, `DescribeTimeToLive`, `UpdateTimeToLive`, `DescribeContinuousBackups`, `UpdateContinuousBackups`, `CreateBackup` and `ListBackups` the account comes first: a `TableName` ARN of five fields whose account field is not the caller's gets the **accounts** answer whatever else is wrong with it, and in the caller's account a vendor or region fault is "1 validation error detected: Invalid vendor code in '<arn>'" (or "Invalid AWS region in '<arn>'") | `ResourceNotFoundException` "Requested resource not found" |
| | the same on `PutItem`, `UpdateItem`, `DeleteItem`, `CreateTable`, `DescribeTable`, `DeleteTable`, `DescribeTimeToLive` and `UpdateTimeToLive` | as above | the engine's name check, as for **table names**: for an ARN of up to 255 bytes, `ValidationException` "1 validation error detected: Value '<arn>' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", and for a longer one the length clause "… less than or equal to 255" |
| | the same on `DescribeContinuousBackups` and `UpdateContinuousBackups` | as above | `TableNotFoundException` "Table not found: <arn>" |
| | the same on `DescribeLimits` and `GetRecords` | as above | the operation's answer to the rest of the request, as for **table names** |
| | the same on `ListBackups` | as above | its handler's own reading of the ARN: `ValidationException` "Invalid Request: TableName is not a valid table name or table ARN", unless the ARN has exactly five colon-separated fields, partition `aws`, service `dynamodb`, a non-empty region, an account of digits and a `table/<name>` resource with no further `/`; for such an ARN, "1 validation error detected: Invalid AWS region in '<arn>'" when its region is not the server's, and otherwise the answer for `<name>` as a `TableName` at 'tableName' (its length clause, its pattern clause, or both) |
| **names inside other ARNs** | a stream ARN whose table segment is outside the pattern or whose label carries U+0000, as the `StreamArn` of `DescribeStream` | `ValidationException` "Invalid StreamArn" | `ResourceNotFoundException` "Requested resource not found: Stream: <arn> not found" |
| | the same, as the `StreamArn` of `GetShardIterator` | `ValidationException` "Invalid StreamArn" | `ResourceNotFoundException` "Requested resource not found: Stream does not exist" |
| | the same, as the `ExclusiveStartStreamArn` of `ListStreams` | `ValidationException` "Invalid StreamArn" | the handler's list of streams from that start ARN; with a `TableName` and a table segment outside the pattern, `ValidationException` "One or more parameter values were invalid: ExclusiveStartStreamArn provided is invalid: <arn>" |
| | the same, as the `ResourceArn` of `TagResource`, `UntagResource` and `ListTagsOfResource` | `ValidationException` "Invalid StreamArn" | for U+0000 in the label, the tag handler's answer for that ARN: `TagResource` and `UntagResource` answer `{}` and change its tags, and `ListTagsOfResource` lists them; for a table segment outside the pattern, `ResourceNotFoundException` "Requested resource not found: ResourceArn: <arn> not found" |
| | a table ARN whose table segment is outside the pattern, as the `ResourceArn` of the same three operations | `ValidationException` "Invalid TableArn: Invalid ResourceArn provided as input <arn>" | `ResourceNotFoundException` "Requested resource not found: ResourceArn: <arn> not found" |
| | a table ARN whose table segment is outside the pattern, as the `TableArn` of `ExportTableToPointInTime` | `ValidationException` "Invalid TableArn: Invalid Table ARN" | `ValidationException` "Export is disabled. Configure [export] paths in extenddb.toml to enable." |
| **StreamArn kind** | a `StreamArn` of `DescribeStream` and `GetShardIterator` that is not a stream ARN (a table, index or backup ARN, `not-an-arn`) | `ValidationException` "Invalid StreamArn" | `ValidationException` "Invalid stream ARN format: <value>" |
| | a stream ARN with an empty partition, service or account field | `ValidationException` "Invalid StreamArn" | the stream's description; an iterator for the stream |
| **accounts** | a table, index, stream or backup ARN in another account (an account segment that is not 12 digits, where the **table ARNs** check does not refuse it first, counts as an account the server does not hold), as a `TableName`, a `RequestItems` key, a `TransactItems` member's `TableName`, a `StreamArn`, a tag operation's `ResourceArn`, a `BackupArn` or an export `TableArn`, and on the ten control-plane operations of **table ARNs** any `TableName` ARN of five fields whose account field is not the caller's; on `GetItem`, `Query`, `Scan`, `SearchVectors`, `UpdateTable`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems` | for an account the server does not hold, `AccessDeniedException` "The caller is not authorized to access the specified resource." on `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan` and `BatchGetItem`, and "Access is denied" on the others; for an account it holds, `ValidationException` "This action is only supported by accounts that match the resource owner’s account." on `CreateTable`, `DescribeTimeToLive`, `UpdateTimeToLive`, `DescribeContinuousBackups`, `UpdateContinuousBackups`, `CreateBackup`, `ListBackups`, `DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup` and `ExportTableToPointInTime`, and `AccessDeniedException` "User: <principal> is not authorized to perform: dynamodb:<action> on resource: <arn> because no resource-based policy allows the dynamodb:<action> action" on the others, where `<principal>` is the ARN of the calling IAM user or assumed-role session, `<action>` the operation and `<arn>` the ARN as sent, except: for a `Query`, `Scan` or `SearchVectors` with an `IndexName` that is a non-empty string, `<arn>` is its `TableName` followed by `/index/` and the `IndexName`; for a `BatchGetItem` or `BatchWriteItem`, `<arn>` is the first `RequestItems` key in the order given above the table; for a `TransactGetItems`, `<action>` is `GetItem` and `<arn>` the `TableName` of the first `TransactItems` member whose `Get` has a `TableName` that is a string; and for a `TransactWriteItems`, `<action>` and `<arn>` come from the first `Put`, `Delete`, `Update` or `ConditionCheck` with a `TableName` that is a string, in the order given above the table: `PutItem`, `DeleteItem`, `UpdateItem` or `ConditionCheckItem`, and that `TableName` (so a member is passed over when it has none of the four, as `{}` has none, or when none of its four has a `TableName` that is a string); on a control-plane operation, "Access is denied" in either case for an ARN that the server's ARN parser cannot read as a table resource (`…:foo/bar`, `…:table/`, `…:table/T/x`); for a batch or transaction whose tables are in more than one account (a name that is not an ARN, the empty one included, is in the caller's), `ValidationException` "Invalid request! All tables in the request must be from a single account." | `ResourceNotFoundException` "Requested resource not found" |
| | the same on `PutItem`, `UpdateItem`, `DeleteItem`, `CreateTable`, `DescribeTable`, `DeleteTable`, `DescribeTimeToLive` and `UpdateTimeToLive` | as above | the engine's name check, as on these operations for **table ARNs** |
| | the same on `DescribeContinuousBackups` and `UpdateContinuousBackups` | as above | `TableNotFoundException` "Table not found: <arn>" |
| | the same on `ListBackups` | as above | its handler's own reading of the ARN, as for **table ARNs**: "Invalid Request: TableName is not a valid table name or table ARN" for an ARN it cannot read, and `AccessDeniedException` "Access is denied", the server's answer for an account it does not hold, for one it can |
| | the same on `TagResource`, `UntagResource` and `ListTagsOfResource` | as above | the tag handler's answer, which checks an ARN's kind before its account: "One or more parameter values were invalid: Provided Arn is not a DynamoDB resource arn: <arn>" for a backup ARN, "… Invalid resource arn provided, only table or stream is accepted. Provided resource arn: <arn>" for an index ARN, and `AccessDeniedException` "Access is denied", the server's answer for an account it does not hold, for a table or stream ARN |
| | the same on `DescribeStream` and `GetShardIterator` | as above | the stream's description; an iterator for the stream |
| | the same on `ExportTableToPointInTime` | as above | `ValidationException` "Export is disabled. Configure [export] paths in extenddb.toml to enable." |
| **tag ResourceArn kind** | a non-empty `ResourceArn` of `TagResource`, `UntagResource` and `ListTagsOfResource` that does not start with `arn:`, is an index or backup ARN (for one in another account that the server does not hold, the **accounts** answer comes first), or is an ARN of service `dynamodb`, in any account, that the server's ARN parser cannot read as a table resource (`…:foo/bar`, `…:table/`, `…:table/T/`, `…:table/T/x/y`) | `ValidationException` "One or more parameter values were invalid: ARNs must start with 'arn:': <value>", "… Invalid resource arn provided, only table or stream is accepted. Provided resource arn: <arn>" for an index ARN, or "… Provided Arn is not a DynamoDB resource arn: <arn>" | the tag handler's answer, which splits the resource at `/`: the same answer for a value that does not start with `arn:`, an index or backup ARN, a resource that does not start with `table/`, and a resource whose third segment is `backup`, `export` or `import`; "… Invalid resource arn provided, only table or stream is accepted. Provided resource arn: <arn>" when the third segment is anything else but empty or `stream` (`table/T/x/y`, `table/T/index/`); otherwise (`table/`, `table/T/`, `table/T/stream/`, `table/T/stream/a/b`) its answer for a table or stream ARN: `AccessDeniedException` "Access is denied" in another account, "Invalid StreamArn" (third segment `stream`) or "Invalid TableArn: Invalid ResourceArn provided as input <arn>" in another partition, service or region, `ResourceNotFoundException` "Requested resource not found: ResourceArn: <arn> not found" when no table has the second segment's name, and the operation's answer when one does, with the tags kept under the ARN as sent |
| **StreamArn scope** | a stream ARN as the `StreamArn` of `DescribeStream` and `GetShardIterator` whose partition is not `aws`, service not `dynamodb` or region not the server's (`aws-cn`, `DYNAMODB`, `us-west-2`, an empty region); for one in another account that the server does not hold, the **accounts** answer comes first | `ValidationException` "Invalid StreamArn" | the stream's description; an iterator for the stream |
| **BackupArn kind** | a backup ARN whose table segment is outside the pattern, as the `BackupArn` of `DescribeBackup`, `DeleteBackup` and `RestoreTableFromBackup`, and a `BackupArn` of `DescribeBackup` and `DeleteBackup` that is not a backup ARN or has an empty partition, service, region or account field (for an ARN of a table resource in another account that the server does not hold, the **accounts** answer comes first) | `ValidationException` "Invalid Backup ARN" | the refusal of the operation (see "What it refuses") |
| **export TableArn** | a `TableArn` of `ExportTableToPointInTime` that does not parse as a table resource ARN (`table/Products`, a repeated `table/` segment) | `ValidationException` "One or more parameter values were invalid: tableArn is not a valid ARN" | `ValidationException` "Export is disabled. Configure [export] paths in extenddb.toml to enable." |
| **GetRecords iterator** | `GetRecords` with no `ShardIterator`, a `ShardIterator` that is not a string, or a body that is not an object | `ValidationException` "Invalid ShardIterator" | `SerializationException` from the body parse: "missing field `ShardIterator`", "invalid type: integer `5`, expected a string", "invalid length 0, expected struct GetRecordsInput with 2 elements" and the like |
| | an iterator that does not decode, beside a member the handler cannot read (`"Limit":"5"`) | `ValidationException` "Invalid ShardIterator" | `SerializationException` `invalid type: string "5", expected i64` |
| | any other iterator that does not decode, the empty string included | `ValidationException` "Invalid ShardIterator" | the same answer |
| | an iterator from before iterators carried their stream ARN | `ExpiredIteratorException` "Shard iterator has expired" | the same answer |
| **GetRecords iterator scope** | an iterator whose ARN field is not a stream ARN in the server's partition, service and region and the caller's account (a table, index or backup ARN, `not-an-arn`, a stream ARN in another region) | `ValidationException` "Invalid ShardIterator" | the handler's answer: `ExpiredIteratorException` "Shard iterator has expired" when the iterator's timestamp is more than 15 minutes old; otherwise, for a stream ARN, the records of the stream with that table name and label in the caller's account (this build issues such an iterator for a respelled `StreamArn`; see **StreamArn scope**), or `ResourceNotFoundException` "Requested resource not found: Stream does not exist" when that stream or its shard does not exist, and for any other value `ValidationException` "Invalid stream ARN format: <value>" |
| **throttling** | reads and writes past a table's provisioned throughput, when `throttling_enabled` is on (it is off by default) | `ProvisionedThroughputExceededException` | the request: it never throttles |

### Building it

The module is built with [`wasm-pack`](https://wasm-bindgen.github.io/wasm-pack/):

```bash
wasm-pack build --target web --release crates/wasm-adapter
```

Building requires a C compiler that targets `wasm32`: the embedded SQLite is
compiled from C by the `sqlite-wasm-rs` dependency, so a `clang` with a wasm32
target must be on `PATH`, or the build stops in that crate's build script.

The emitted module is about 6.6 MiB after `wasm-bindgen`; `wasm-pack --release`
runs `wasm-opt`, which brings it to about 5.0 MiB.

The command above can download two tools. wasm-pack 0.15.1 uses the
`wasm-bindgen` on `PATH` when it is the version `Cargo.lock` names, and any
`wasm-opt` on `PATH`. For each one it does not find there, it uses the copy
that an earlier run downloaded into its cache (the directory that
`WASM_PACK_CACHE` names, by default `.wasm-pack` in the user's cache
directory), and otherwise downloads a prebuilt archive from GitHub releases
(wasm-bindgen at the version in `Cargo.lock`, binaryen at `version_130`) and
checks no checksum of it. When there is no wasm-bindgen archive for the
platform or its download fails, it uses a `wasm-bindgen-cli` that an earlier
`cargo install` of it left in that cache, or else builds one there with
`cargo install`. To build without those downloads, build the
`wasm-bindgen-cli` of the `wasm-bindgen` version in `Cargo.lock` from
crates.io (`devtools/run-wasm-step wasm-bindgen-version` prints that version),
and skip `wasm-opt`:

```bash
cargo install --locked --version "$(devtools/run-wasm-step wasm-bindgen-version)" wasm-bindgen-cli
wasm-pack build --target web --release --no-opt --mode no-install crates/wasm-adapter
```

With `--mode no-install`, wasm-pack stops with an error instead of
downloading when it finds no `wasm-bindgen` of that version; with `--no-opt`,
it does not run `wasm-opt`, so the module stays at about 6.6 MiB. In either
form wasm-pack also runs `cargo metadata` on the crate's manifest before it
builds, without `--locked`, so it updates a `Cargo.lock` that is out of date;
runs `rustup target add wasm32-unknown-unknown` when the toolchain is a rustup
one without that target; and asks crates.io for the latest wasm-pack version
when no `wasm-pack.stamp` file is beside its binary or the one there records a
check made 25 or more hours before (the answer only feeds a warning).

The `wasm32` build is the adapter's dependency graph, built through
`extenddb-wasm-adapter` as above. Two crates of that graph do not build for
`wasm32-unknown-unknown` on their own: `extenddb-engine` with its default
`partiql` feature, whose PartiQL code (`crates/engine/src/partiql/snapshot.rs`)
sets the backup runtime field that `OperationContext` has only on other
targets (the adapter takes the engine without default features), and
`extenddb-cache`, because the `uuid` crate needs a source of randomness on
that target and in this workspace only `extenddb-core` selects one.

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
  wasm-adapter/       — experimental WebAssembly build (see its section above; publish = false)


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
