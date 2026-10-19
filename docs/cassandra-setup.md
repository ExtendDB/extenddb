# Cassandra Setup

> See [NOTICE](NOTICE.md) for important disclaimers.

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
They mirror `devtools/cassandra-node`, which the project uses for its own tests.

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

> **Note:** Password authentication is optional for a purely local node. If you
> skip step 3, the node runs with the default `AllowAllAuthenticator` (no
> authentication), and you can run `extenddb init --backend cassandra` with no
> credential flags. The steps above enable authentication because production
> clusters require it, and testing against an authenticated node locally avoids
> surprises later.

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

If your node runs without authentication (you skipped step 3 above), omit the
credential flags:

```bash
./target/release/extenddb init --backend cassandra --config extenddb.toml
```

init creates:
- The catalog keyspace (`extenddb_catalog` by default)
- The application role ExtendDB uses at serve time
- Admin user credentials (printed to stdout once — save them)
- A self-signed TLS certificate at `~/.extenddb/tls/cert.pem`
- The config file `extenddb.toml`

Per-account keyspaces are created later, when accounts are added.

> **Save the admin credentials.** They are printed once and cannot be retrieved
> later.

## Config Mapping

init writes a `[storage.cassandra]` section to `extenddb.toml`:

```toml
[storage]
backend = "cassandra"

[storage.cassandra]
contact_points = ["127.0.0.1:9042"]
# username = "cassandra"          # Application user
# password = "cassandra-password" # Application password
keyspace_prefix = "extenddb"
replication_factor = 1           # Single node (use 3 or more for production)
datacenter = "datacenter1"
max_connections = 10
```

Fields:

- `contact_points` — one or more `host:port` entries. One reachable node is
  enough; the driver discovers the rest of the cluster.
- `username` / `password` — the application credentials ExtendDB uses at serve
  time. Omit both for an unauthenticated node.
- `keyspace_prefix` — prefix for this deployment's keyspaces (default
  `extenddb`).
- `replication_factor` — keyspace replication factor. `1` suits a single local
  node; use 3 or more in production.
- `datacenter` — the local datacenter name for `NetworkTopologyStrategy`
  (default `datacenter1`, which matches a stock single-node cluster).
- `max_connections` — maximum connections per host.

Edit these values in `extenddb.toml`. Unlike the PostgreSQL and MongoDB
backends, the Cassandra backend has no single connection-string field; set
`contact_points` and the other fields directly in the config section above.

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

For deployment concerns common to every backend — single-node vs separated
topologies, containers, the production checklist, systemd, multi-instance
limits — see the [Deployment Guide](manuals/11-deployment-guide.md). This
section covers only what is specific to Cassandra.

> **Not recommended for production yet.** As of October 2026 the Cassandra
> backend is experimental. It has known limitations — some multi-entity
> management operations are not atomic, and a few concurrency issues are still
> open (see Known Limitations below). Treat a Cassandra deployment as
> evaluation or pre-production, and test it against your own workload before you
> depend on it.

### Keyspace replication

Set `replication_factor` to 3 or more for a multi-node cluster, and set
`datacenter` to your cluster's local datacenter name. ExtendDB creates its
keyspaces with `NetworkTopologyStrategy` using these values.

```toml
[storage.cassandra]
contact_points = ["cass-1:9042", "cass-2:9042", "cass-3:9042"]
username = "extenddb"
password = "<application-password>"
keyspace_prefix = "extenddb"
replication_factor = 3
datacenter = "dc1"
max_connections = 10
```

### Authentication

Run the cluster with authentication enabled (`PasswordAuthenticator` or
stronger), and give ExtendDB a dedicated application role rather than the
`cassandra` superuser. Supply its credentials through `username` and `password`.
ExtendDB connects without authentication when both are absent, so confirm they
are set for any cluster that is not fully trusted.

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
rationale are documented in the Cassandra ADRs (`docs/adr/0010`–`0021`).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
