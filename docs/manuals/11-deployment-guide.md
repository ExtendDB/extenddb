---
description: "Running ExtendDB on your own servers: single host, split host, managed PostgreSQL, containers, air-gapped, and the production checklist."
---

# Deployment

> See [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/docs/NOTICE.md) for important disclaimers.

This guide covers deploying ExtendDB in various environments beyond local development.

> **Backend-specific sections.** This guide covers deployment concerns common to
> all storage backends, and uses PostgreSQL, the default and reference backend,
> for concrete examples. Sections headed *PostgreSQL* apply only to that backend.
> For other backends, see their setup docs:
> [Cassandra setup](../cassandra-setup.md) (experimental; local and production)
> and [MongoDB setup](../local-mongodb-setup.md). SQLite does not need a separate
> backend service; see its section in the getting-started guide.

## Architecture Overview

ExtendDB is a single Rust binary that connects to the storage backend it was built with (PostgreSQL by default; see [Storage backends](../storage-backends.md)). All durable state lives in the backend; ExtendDB keeps only short-lived credential, policy, and table-metadata caches in process. This means:

- Running more than one ExtendDB instance against one catalog is not supported in this release (see [Multi-Instance Considerations](#multi-instance-considerations))
- The backend's own HA, backup, and replication tools provide durability
- ExtendDB can run anywhere the storage backend is reachable

## Deployment Models

### Single-Node

ExtendDB and its storage backend on the same host. Simplest setup, suitable for development, CI, and small workloads.

```
┌──────────────────────────────┐
│  Host                        │
│  ┌──────────┐  ┌───────────┐ │
│  │ ExtendDB │──│  Storage  │ │
│  │          │  │  backend  │ │
│  └──────────┘  └───────────┘ │
└──────────────────────────────┘
```

```bash
extenddb init
extenddb serve --config extenddb.toml
```

### Separated Database

ExtendDB on an application server, the storage backend on a dedicated database
server or managed service.

```
┌──────────┐       ┌──────────────┐
│ App Host │──────▶│   DB Host    │
│ ExtendDB │       │   Backend    │
└──────────┘       └──────────────┘
```

Point ExtendDB at the remote backend at init time, and use an encrypted
connection. The flags and connection-string format are backend-specific: for
PostgreSQL see [Configuring PostgreSQL](#configuring-postgresql) below; for
Cassandra see [Cassandra setup](../cassandra-setup.md).

### Managed Database

ExtendDB works with managed database services as well as self-managed servers.
The managed-service topology is the same as Separated Database: ExtendDB on an
application host, the backend provided as a service. For the PostgreSQL managed
services ExtendDB supports (Amazon RDS and Aurora, Google Cloud SQL, Azure
Database), see [Configuring PostgreSQL](#configuring-postgresql).

### Containerized

ExtendDB runs in Docker or Kubernetes. The binary has no runtime dependencies beyond libc and network access to the storage backend.

Example Dockerfile:

```dockerfile
# Match rust-version in Cargo.toml
FROM rust:1.95 AS builder
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates tini && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/extenddb /usr/local/bin/extenddb
COPY extenddb.toml /etc/extenddb/extenddb.toml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
EXPOSE 18443
# `extenddb healthcheck` needs no shell or curl, so it also works on a
# distroless/scratch base. It probes GET /health over loopback.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
  CMD ["/usr/local/bin/extenddb", "healthcheck", "--config", "/etc/extenddb/extenddb.toml"]
ENTRYPOINT ["tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
```

Run the server with `serve --foreground` (alias `--no-daemon`) so it stays PID 1's
child and receives `SIGTERM` directly. In foreground mode ExtendDB writes no PID
file and never creates `run_dir`, so the root filesystem can be read-only. Use
`tini` as PID 1 to reap any stray children and forward signals:

```bash
#!/bin/sh
# entrypoint.sh
set -e
# Safe to run on every start: already-applied migrations are skipped.
extenddb migrate --yes --config /etc/extenddb/extenddb.toml
exec extenddb serve --foreground --config /etc/extenddb/extenddb.toml
```

`extenddb stop` cannot stop a foreground server, because there is no PID file to
read; stop the container instead, which delivers `SIGTERM` and triggers the same
graceful shutdown. Outside a container, where you may want `stop` to work, add
`--write-pid-file`; it writes the PID file to the usual `run_dir` path, which
then has to be writable.

`extenddb healthcheck` exits 0 when the server is
listening, completing TLS, and serving HTTP. Use it as a liveness probe, which is
what a Docker `HEALTHCHECK` and a Kubernetes `livenessProbe` are for. `/health`
does not query the storage backend, and for a liveness probe that is the behaviour you
want: one that failed on a database outage would restart every replica at once.
A backend that is unreachable at startup does stop the server from listening, so
container start-up failures are still caught.

There is no readiness endpoint yet, so do not expect a Kubernetes
`readinessProbe` on `/health` to remove a replica from service when its database
becomes unreachable. Until a readiness endpoint that probes the storage layer
exists, treat backend failures as something clients retry through.

For Kubernetes, run `extenddb init` as an init container or a one-time Job, then deploy ExtendDB as a Deployment with the generated `extenddb.toml` mounted as a ConfigMap or Secret.

### Air-Gapped

ExtendDB requires no internet connectivity. All functionality is self-contained in the binary. Build on a connected host, transfer the binary and `extenddb.toml` to the air-gapped environment, and run.

Requirements in the air-gapped environment:
- A supported storage backend, reachable from the ExtendDB host
- The `extenddb` binary (statically linked or with matching libc)

## Configuring PostgreSQL

This section applies to the PostgreSQL backend. For Cassandra, see
[Cassandra setup](../cassandra-setup.md); for MongoDB, see
[MongoDB setup](../local-mongodb-setup.md).

### Remote and separated databases

Point ExtendDB at a remote PostgreSQL server at init time:

```bash
extenddb init \
  --pg-host db.example.com \
  --pg-pass
```

Configure the connection string in `extenddb.toml`:

```toml
[storage.postgres]
connection_string = "postgresql://extenddb:<password>@db.example.com:5432/extenddb_catalog?sslmode=require"
pool_size = 20
```

Use `sslmode=require` (or `verify-full` with a CA certificate) for encrypted
database connections.

The Cassandra backend cannot encrypt its connection today: the driver is
compiled without TLS, so credentials and item data cross the network in clear
text and there is no config option to turn encryption on. Run ExtendDB and the
Cassandra cluster on a trusted, isolated network; do not route Cassandra traffic
over an untrusted network.

### Managed PostgreSQL services

ExtendDB works with any PostgreSQL 14+ service:

- **Amazon RDS for PostgreSQL** / **Amazon Aurora PostgreSQL**
- **Google Cloud SQL for PostgreSQL**
- **Azure Database for PostgreSQL**
- **Self-managed PostgreSQL** with streaming replication

```bash
# Amazon RDS / Aurora example
extenddb init \
  --pg-host mydb.cluster-abc123.us-east-1.rds.amazonaws.com \
  --pg-user extenddb_admin \
  --pg-pass
```

## Production Checklist

### Authentication

- [ ] Set `auth.provider = "builtin"` in `extenddb.toml`
- [ ] Change the admin password from the auto-generated one
- [ ] Create named accounts and IAM users with least-privilege policies
- [ ] Disable credential import if not needed: `extenddb settings set allow_credential_import false`

### Transport

- [ ] TLS is always on; the server refuses to start with `tls.enabled = false`. Confirm the certificate and key paths in `extenddb.toml`
- [ ] Replace the self-signed certificate with a CA-signed certificate
- [ ] Encrypt the connection to the storage backend. For PostgreSQL, use `sslmode=require` or `sslmode=verify-full` in the connection string. The Cassandra backend cannot encrypt its connection; place ExtendDB and the Cassandra cluster on a trusted, isolated network instead

### Network

- [ ] Set `bind_addr` to the interface your clients reach, not `0.0.0.0`
- [ ] Firewall: allow only necessary ports (the ExtendDB port, the storage backend port)
- [ ] Consider a reverse proxy (nginx, HAProxy) for rate limiting and access logging. The proxy re-encrypts to the server, which always serves TLS; see [Multi-Instance Considerations](#multi-instance-considerations) before putting more than one instance behind it

### Database (PostgreSQL)

- [ ] Use a dedicated PostgreSQL user for ExtendDB with minimal privileges
- [ ] Configure PostgreSQL `max_connections` ≥ ExtendDB `pool_size` + 3
- [ ] Enable PostgreSQL TLS
- [ ] Set up automated backups (pg_dump, WAL archiving, or managed service snapshots)
- [ ] Monitor PostgreSQL disk usage, connection count, and query performance

### Monitoring

- [ ] Poll `/metrics` for JSON snapshots and forward to your monitoring system (the response is custom JSON, not Prometheus exposition format; convert as needed)
- [ ] Forward syslog to a log aggregation service
- [ ] Set up alerts on health check failures (`/health`)
- [ ] Monitor ExtendDB process with systemd, supervisord, or equivalent

### Upgrades

- [ ] Test upgrades in a staging environment first
- [ ] Run `extenddb migrate --config extenddb.toml` after binary upgrade
- [ ] Verify catalog version matches: `extenddb verify --config extenddb.toml`

## systemd Service

Example unit file for Linux:

```ini
[Unit]
Description=ExtendDB
After=postgresql.service
Requires=postgresql.service

[Service]
Type=forking
ExecStart=/usr/local/bin/extenddb serve --config /etc/extenddb/extenddb.toml
ExecStop=/usr/local/bin/extenddb stop --config /etc/extenddb/extenddb.toml
# PID file path: {run_dir}/extenddb-{port}.pid
# Default run_dir is ~/.extenddb/run (~ expands to $HOME of the User= below)
# Adjust if run_dir or port are overridden in extenddb.toml
PIDFile=/home/extenddb/.extenddb/run/extenddb-18443.pid
User=extenddb
Group=extenddb
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable extenddb
sudo systemctl start extenddb
```

## Multi-Instance Considerations

Run one ExtendDB instance per catalog. Multiple instances behind a load balancer are not supported in this release:

- Credentials, policies, and table metadata are cached in each process (see the cache TTLs in the admin guide). Nothing invalidates a cache on another instance, so a key revoked or a policy changed through one instance stays in force on the others until the TTL expires.
- Every instance runs every background worker (TTL expiry, GSI backfill, stream retention, control-plane transitions). The workers are safe to run twice, but the work is duplicated.
- `/health` is a liveness check that does not query the database, so a load balancer cannot use it to take an instance with a broken database connection out of rotation.

On SQLite this limit is enforced: `extenddb serve` holds an exclusive lock on `<database>.lock` and a second server on the same file refuses to start. On PostgreSQL, MongoDB, and Cassandra it is not enforced. On PostgreSQL, if you start a second instance anyway, size `pool_size × instance_count + 3 × instance_count ≤ PostgreSQL max_connections`.

## Performance Tuning (PostgreSQL)

This section covers the PostgreSQL backend.

### Connection Pool

The default `pool_size = 20` is suitable for moderate workloads. For high-concurrency deployments:

```toml
[storage.postgres]
pool_size = 50  # Increase for higher concurrency
```

Ensure PostgreSQL `max_connections` accommodates the total pool size plus overhead.

Backup and restore work is admitted against the pool so it cannot take every connection from ordinary requests. On PostgreSQL and MongoDB one server runs at most 4 `RestoreTableFromBackup` copies per account and `min(16, pool/2)` in total (PostgreSQL: `pool_size`; MongoDB: the engine's driver pool, `maxPoolSize` in the connection string or `max_connections`, default 10, so 5 restores); a restore that waits more than 30 seconds for a slot is refused with `LimitExceededException` and creates nothing. On SQLite one server runs at most `pool_size / 2 - 1` `CreateBackup` copies at once (between 1 and 16; 4 for the default `pool_size` of 10), each holding its snapshot connection for its duration, and refuses a backup that waited 30 seconds with `LimitExceededException`. A MongoDB restore request that is not registered within 60 seconds answers an internal error while the restore goes on in the background; the MongoDB driver does not enforce `waitQueueTimeoutMS`, so the admission bound is what keeps a flood of restores from holding every pooled connection.

### PostgreSQL Tuning

Key PostgreSQL settings for ExtendDB workloads:

- `shared_buffers`: 25% of available RAM
- `effective_cache_size`: 75% of available RAM
- `work_mem`: 64MB (for sort operations in Query/Scan)
- `max_connections`: ≥ `2 * pool_size + catalog_pool_size`, plus one per vector index build you expect to overlap and one during a schema migration. `pool_size` sizes both the catalog and the data pool, and `max_connections` is per cluster, so the default pool sizes need 60 of PostgreSQL's default 100

### Monitoring Queries

```sql
-- Active connections from extenddb
SELECT count(*) FROM pg_stat_activity WHERE usename = 'extenddb';

-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
FROM pg_class WHERE relname LIKE 'ddb_%' ORDER BY pg_total_relation_size(oid) DESC;

-- Slow queries
SELECT query, mean_exec_time, calls
FROM pg_stat_statements WHERE usename = 'extenddb'
ORDER BY mean_exec_time DESC LIMIT 10;
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
