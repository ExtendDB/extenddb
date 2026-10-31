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
   docker run -d --name cassandra -p 9042:9042 \
       -e CASSANDRA_CLUSTER_NAME=extenddb-dev \
       cassandra:4.1
   ```

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

## Initializing ExtendDB with Cassandra

Initialize against the authenticated local node from above. Pass the contact
point and the superuser credentials used to create the keyspaces and the
application role:

```bash
./target/release/extenddb init \
    --backend cassandra \
    --cassandra-contact-points 127.0.0.1:9042 \
    --cassandra-user cassandra \
    --cassandra-pass cassandra \
    --config extenddb.toml
```

The node must run `PasswordAuthenticator` (step 3). `init` always creates its
application role with a password, so it fails against an unauthenticated
(`AllowAllAuthenticator`) node; there is no credential-free path.

> **Authorizer.** Run the node with `AllowAllAuthorizer` (the default). `init`
> creates the application role but grants it no permissions, so under
> `CassandraAuthorizer` the server later fails to start with
> `User extenddb has no SELECT permission ...`. Until `init` issues the grants,
> only `AllowAllAuthorizer` works.

init creates:
- The catalog keyspace (`extenddb_catalog` by default)
- The application role ExtendDB uses at serve time (named `extenddb` by
  default; override with `--extenddb-user` / `--extenddb-pass`)
- Admin user credentials (printed to stdout once; save them)
- A self-signed TLS certificate at `~/.extenddb/tls/cert.pem`
- The config file `extenddb.toml`

Per-account keyspaces are created later, when accounts are added.

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
- `replication_factor`: keyspace replication factor. `init` always writes `1`,
  regardless of the cluster. For a multi-node cluster, edit this to 3 or more
  **after** `init`; keyspaces created later (per account) use the config value.
  Existing keyspaces are not altered retroactively.
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

Set `replication_factor` to 3 or more for a multi-node cluster. `init` always
writes `1`, so edit the config after `init`; keyspaces created afterward (per
account) use the value in the config. The `datacenter` must be `datacenter1`:
`init` hardcodes that name into the keyspace definitions and provides no flag to
change it, so your cluster's local datacenter has to be named `datacenter1`
until that is configurable. ExtendDB creates its keyspaces with
`NetworkTopologyStrategy` using these values.

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

Use `AllowAllAuthorizer` for now. `init` creates the application role but grants
it no permissions, so `CassandraAuthorizer` makes `serve` fail with
`User <role> has no SELECT permission ...`. Until `init` issues the grants a
role needs, authorization must be left open at the Cassandra layer.

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

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
