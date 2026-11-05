---
description: "Run ExtendDB on Apache Cassandra: a local single-node setup, initialization, the configuration keys, cluster deployment and the backend's known limitations."
---

# Cassandra setup

> See [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/docs/NOTICE.md) for important disclaimers.

> **Experimental.** As of October 2026, the Cassandra backend is experimental.
> Validate it against your workload before you rely on it in production. This
> guide covers both local development and production deployment.

## Prerequisites

- Apache Cassandra 4.1 or later
- Docker (for the local single-node setup below), or an existing Cassandra
  cluster
- The `cqlsh` client, for the verification steps (bundled in the Cassandra
  Docker image)

## Local Single-Node Setup (Docker)

These steps start a single-node Cassandra 4.1 container for local development.

1. Start the container:

   ```bash
   docker run -d --name cassandra -p 127.0.0.1:9042:9042 \
       -e CASSANDRA_CLUSTER_NAME=extenddb-dev \
       cassandra:4.1
   ```

   The port is published on the loopback interface only. Until step 3 the
   node accepts any login, and after it the superuser is on Apache
   Cassandra's documented default password; neither should be reachable from
   other hosts.

2. Wait for the node to accept CQL. The port opens before the database is
   ready, so poll with `cqlsh` until it succeeds:

   ```bash
   until docker exec cassandra cqlsh -e "SELECT release_version FROM system.local" >/dev/null 2>&1; do
       sleep 2
   done
   ```

3. Enable password authentication. The `cassandra:4.1` image ignores the
   `CASSANDRA_AUTHENTICATOR` environment variable, so edit `cassandra.yaml`
   directly and restart:

   ```bash
   docker exec cassandra sed -i \
       's/^authenticator: .*/authenticator: PasswordAuthenticator/' \
       /etc/cassandra/cassandra.yaml
   docker restart cassandra
   ```

4. Wait for authentication to come up, then confirm the default superuser login
   works. Apache Cassandra's publicly documented default credentials are
   `cassandra` / `cassandra`:

   ```bash
   until docker exec cassandra cqlsh -u cassandra -p cassandra \
       -e "SELECT release_version FROM system.local" >/dev/null 2>&1; do
       sleep 2
   done
   ```

The node now listens on `127.0.0.1:9042` with password authentication enabled.

> **Password authentication is required.** `extenddb init` always creates its
> application role with `CREATE ROLE ... WITH PASSWORD`, which Cassandra accepts
> only when the node runs `PasswordAuthenticator`. Against a stock node using the
> default `AllowAllAuthenticator`, `init` fails at the role step with
> `CassandraRoleManager doesn't support PASSWORD`. Do not skip step 3.

## Connection Details

| Setting | Value |
|---------|-------|
| Contact point | `127.0.0.1:9042` |
| Superuser (default) | `cassandra` / `cassandra` |
| Keyspace prefix | `extenddb` (default) |
| Catalog keyspace | `<keyspace_prefix>_catalog` (e.g. `extenddb_catalog`) |
| Per-account keyspaces | `<keyspace_prefix>_account_<account_id>` |

ExtendDB creates its keyspaces under the configured `keyspace_prefix`, so one
cluster can host more than one deployment by giving each a different prefix.

## Building with Cassandra Support

The Cassandra backend is behind a feature flag. Backends are mutually exclusive
(a build enabling more than one is rejected at compile time), so disable the
default `postgres` feature:

```bash
cargo build --release --no-default-features --features cassandra
```

The binary is single-backend. To run Cassandra and another backend side by
side, build one binary per backend.

## Container Image

Starting with ExtendDB 1.0, the `extenddb/extenddb-cassandra` image is
published to Docker Hub, GHCR and ECR Public. It contains this build of
ExtendDB and no Cassandra server, client or Java runtime; Cassandra stays a
separate service. [`docker/README-cassandra.md`](https://github.com/ExtendDB/extenddb/blob/main/docker/README-cassandra.md) covers
running `init`, `serve` and `migrate` with the image, and a local reference
stack, `docker-compose.cassandra.yml`, that starts a password-authenticated
Cassandra node next to ExtendDB. The cluster requirements below apply to the
image unchanged.

## Initializing ExtendDB with Cassandra

Initialize against the authenticated local node from above. Pass the contact
point and the superuser credentials used to create the keyspaces and the
application role:

```bash
EXTENDDB_CASSANDRA_PASSWORD=cassandra ./target/release/extenddb init \
    --backend cassandra \
    --cassandra-contact-points 127.0.0.1:9042 \
    --cassandra-user cassandra \
    --config extenddb.toml
```

`init` reads the superuser password from `EXTENDDB_CASSANDRA_PASSWORD` when
`--cassandra-pass` is not given, which keeps it out of the process list.

The node must run `PasswordAuthenticator` (step 3). `init` always creates its
application role with a password, so it fails against an unauthenticated
(`AllowAllAuthenticator`) node; there is no credential-free path.

> **Authorizer.** The node may run `AllowAllAuthorizer` (the default) or
> `CassandraAuthorizer`. `init` reads the node's authorizer from
> `system_views.settings`; under any authorizer other than
> `AllowAllAuthorizer` it grants the application role SELECT, MODIFY, CREATE
> and ALTER on the catalog keyspace, CREATE ON ALL KEYSPACES (for
> CreateAccount), and SELECT, MODIFY, CREATE, ALTER and DROP on the default
> account's keyspace (DeleteTable drops the account's tables and DeleteAccount
> drops the keyspace). A keyspace that CreateAccount creates as the application
> role needs no grant: a role holds every permission on a keyspace it created.
> Under `AllowAllAuthorizer`, which grants everything and supports no GRANT,
> `init` grants nothing.

init creates:
- The catalog keyspace (`extenddb_catalog` by default)
- The default account's keyspace (`extenddb_account_<account id>` by default)
- The application role ExtendDB uses at serve time (named `extenddb` by
  default; override with `--extenddb-user` / `--extenddb-pass`), with the
  grants above when the node's authorizer is not `AllowAllAuthorizer`
- Admin user credentials (printed to stdout once; save them)
- A self-signed TLS certificate at `~/.extenddb/tls/cert.pem`
- The config file `extenddb.toml`

Further account keyspaces are created when accounts are added.

An `init` that stopped after its migrations and before the admin user is
finished by the next `init`, which writes the rows the catalog lacks. One that
stopped while it created the default account's keyspace, before the account's
row in `<prefix>_catalog.accounts`, leaves that keyspace with no account: the
next `init` creates another default account and keyspace, and the first
keyspace, whose id is in no `accounts` row, stays until you drop it with
`DROP KEYSPACE <prefix>_account_<id>`.

> **Save the admin credentials.** They are printed once and cannot be retrieved
> later.

## Config Mapping

init writes a `[storage.cassandra]` section to `extenddb.toml`. For example:

```toml
[storage]
backend = "cassandra"

[storage.cassandra]
contact_points = ["127.0.0.1:9042"]
username = <extenddb-user>      # From extenddb init args
password = <extenddb-pass>      # From extenddb init args
keyspace_prefix = "extenddb"
replication_factor = 1           # Single node (use 3 or more for production)
datacenter = "datacenter1"
max_connections = 10
```

Fields:

- `contact_points`: one or more `host:port` entries. One reachable node is
  enough; the driver discovers the rest of the cluster.
- `username` / `password`: the application credentials ExtendDB uses at serve
  time. Required against any authenticated node (which `init` requires anyway).
- `keyspace_prefix`: prefix for this deployment's keyspaces (default
  `extenddb`).
- `replication_factor` — keyspace replication factor. `init` always writes `1`
  and creates its own keyspaces with it; see [Keyspace replication](#keyspace-replication)
  for a multi-node cluster.
- `datacenter`: the local datacenter name for `NetworkTopologyStrategy`. Currently,
  `init` always writes `datacenter1` and has no flag to change it, and the keyspaces
  it creates reference `datacenter1`, so the cluster's local datacenter must be
  named `datacenter1`. A node started with a different datacenter name (for
  example `CASSANDRA_DC=dc1`) fails keyspace creation with
  `Unrecognized strategy option {datacenter1} ... for keyspace extenddb_catalog`.
- `max_connections`: maximum connections per host.

Edit these values in `extenddb.toml` after `init`. Unlike the PostgreSQL and
MongoDB backends, the Cassandra backend has no single connection-string field;
set `contact_points` and the other fields directly in the config section above.

> **Connections are not encrypted.** The Cassandra backend connects over plain
> TCP (the driver is built without TLS), so credentials and item data cross the
> network in clear text. Run ExtendDB and Cassandra on a trusted network (for
> example a private subnet or an encrypted overlay); do not send Cassandra
> traffic across an untrusted network.

## Starting the Server

```bash
./target/release/extenddb serve --config extenddb.toml
```

## Verifying the Connection

```bash
# Health check
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health

# List tables (should return empty on a fresh deployment)
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables \
    --endpoint-url https://127.0.0.1:18443 \
    --region us-east-1
```

You can also confirm the keyspaces exist directly in Cassandra:

```bash
docker exec cassandra cqlsh -u cassandra -p cassandra \
    -e "DESCRIBE KEYSPACES" | tr ' ' '\n' | grep '^extenddb'
```

## Stopping

```bash
# ExtendDB
./target/release/extenddb stop --config extenddb.toml

# Docker Cassandra
docker stop cassandra
```

## Cluster Deployment

For deployment concerns common to every backend (single-node vs separated
topologies, containers, the production checklist, systemd, multi-instance
limits), see the [deployment guide](manuals/11-deployment-guide.md). This
section covers only what is specific to Cassandra.

> **Not recommended for production yet.** As of October 2026 the Cassandra
> backend is experimental. It has known limitations: some multi-entity
> management operations are not atomic, and a few concurrency issues are still
> open (see Known Limitations below). Treat a Cassandra deployment as
> evaluation or pre-production, and test it against your own workload before you
> depend on it.

### Keyspace replication

Set `replication_factor` to 3 or more for a multi-node cluster. `init` has no
flag for it: it creates the catalog keyspace and the default account's
keyspace with replication factor 1 and writes `replication_factor = 1` to the
config. Keyspaces created later (one per new account) take the value from the
config when `serve` starts, except an account keyspace that `migrate --yes`
creates for an account whose CreateAccount stopped before its keyspace, which
takes the catalog keyspace's replication. On a multi-node cluster, after
`init`:

1. Set `replication_factor = 3` in `extenddb.toml` (example below), before you
   start `serve` or, if it is already running, restart it before you create
   accounts.
2. Raise the replication of the keyspaces that already exist: Cassandra's own
   `system_auth` keyspace, which holds the roles ExtendDB logs in with and
   starts at `SimpleStrategy` with replication factor 1, and every keyspace
   whose name starts with the keyspace prefix (`DESCRIBE KEYSPACES` lists
   them):

   ```sql
   ALTER KEYSPACE system_auth
     WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
   ALTER KEYSPACE extenddb_catalog
     WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
   -- and the same for each extenddb_account_<id> keyspace
   ```

3. Run `nodetool repair -full` on each node.

Use a replication factor no larger than the number of nodes in `datacenter1`.
Logins read `system_auth` at `QUORUM` for the default superuser, so a
`system_auth` replication factor above the node count makes those logins fail
with `Cannot achieve consistency level QUORUM`.

The `datacenter` must be `datacenter1`:
`init` hardcodes that name into the keyspace definitions and provides no flag to
change it, so your cluster's local datacenter has to be named `datacenter1`
until that is configurable. ExtendDB creates its keyspaces with
`NetworkTopologyStrategy` using these values.

With a replication factor of 1, the catalog has one replica, so a restart of
that node stops every request to it. A running server renews its serving
registration every 5 s and keeps serving while renewals fail. It tries its
renewals for 30 s after the last one that succeeded, so it keeps serving
through an outage of 25 to 30 s, depending on when the outage starts. After
that, 30 to 35 s after its last successful renewal, it answers new requests
with `ServiceUnavailable`, stops its background workers, and tries to register
again every second for up to 30 s.
It registers again, starts its workers again and serves on only when the
Cassandra driver has reconnected to the node within that time, which after a
restart of 30 s or more it often has not; otherwise it stops with exit 1. Run
the server under a supervisor that starts it again, such as systemd or
Kubernetes.

```toml
[storage.cassandra]
contact_points = ["cass-1:9042", "cass-2:9042", "cass-3:9042"]
username = "extenddb"
password = "<application-password>"
keyspace_prefix = "extenddb"
replication_factor = 3
datacenter = "datacenter1"   # must be datacenter1 (hardcoded by init)
max_connections = 10
```

### Authentication

Run the cluster with authentication enabled: `init` requires
`PasswordAuthenticator` or stronger, since it creates its application role with
a password. Give ExtendDB a dedicated application role rather than the
`cassandra` superuser, and supply its credentials through `username` and
`password`; both are required.

`CassandraAuthorizer` works: when the node's authorizer is not
`AllowAllAuthorizer`, `init` grants the application role SELECT, MODIFY, CREATE
and ALTER on the catalog keyspace, CREATE ON ALL KEYSPACES, and those four and
DROP on the default account's keyspace; a keyspace the role creates itself
(CreateAccount) needs no grant. Under `AllowAllAuthorizer` `init` grants
nothing, since that authorizer grants everything.

### Connection encryption

The Cassandra backend connects over plain TCP; the driver is compiled without
TLS, so credentials and item data travel in clear text. There is no config
option to enable encryption today. Deploy ExtendDB and the Cassandra cluster on
a trusted, isolated network, and do not route Cassandra traffic over an
untrusted network.

### Known Limitations

Understand these before you evaluate a Cassandra deployment. They are why the
backend is experimental rather than production-ready.

- **Non-atomic management operations.** Some multi-entity management operations
  (for example across the IAM entities) are not all-or-nothing on Cassandra.
  Cassandra has no multi-row ACID transaction, and ExtendDB uses lightweight
  transactions (LWT) for referential integrity, preventing the use of logged
  batches for atomicity. A failure part-way through such an operation can leave
  partial state.
- **Open concurrency issues.** A small number of concurrency/race conditions
  are known (observed as intermittent integration-test failures) and are being
  worked. Under concurrent load, outcomes may occasionally diverge from
  DynamoDB semantics.
- **Single-datacenter writes per table.** Transaction and TTL lifecycle
  operations use `LOCAL_QUORUM` / `LOCAL_SERIAL`, which are linearizable only
  within one datacenter. Serve writes for a given table from one Cassandra
  datacenter.
- **Multi-node ExtendDB needs node-id configuration.** Running more than one
  ExtendDB instance against the cluster requires a distinct node ID per
  instance (used to derive stream sequence numbers).
- **TTL throughput.** The TTL sweeper is sized for roughly 1,000 expirations
  per table per minute; a table expiring faster accumulates backlog. Enable TTL
  before opening a table to writes, or quiesce it briefly while enable and
  backfill complete.
- **Clock synchronization.** Keep Cassandra coordinators and ExtendDB hosts
  time-synchronized; request batches use explicit timestamps.
- **No vector search.** The Cassandra backend does not implement vector indexes
  or `SearchVectors`; every vector operation is refused.

If you do evaluate a deployment, monitor the TTL expiration backlog, Cassandra
LWT latency, and background-worker health. The design decisions and their
rationale are documented in the Cassandra ADRs (`docs/adr/0010` to `0021`).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
