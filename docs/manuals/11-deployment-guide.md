# Deployment Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

This guide covers deploying extenddb in various environments beyond local development.

> **Backend-specific sections.** This guide covers deployment concerns common to
> all storage backends, and uses PostgreSQL — the default and reference backend —
> for concrete examples. Sections headed *PostgreSQL* apply only to that backend.
> For other backends, see their setup docs:
> [Cassandra setup](../cassandra-setup.md) (experimental; local and production)
> and [MongoDB setup](../local-mongodb-setup.md). SQLite does not need a separate
> backend service; see its section in the getting-started guide.

## Architecture Overview

extenddb is a single Rust binary that connects to a storage backend. All durable state lives in the backend; extenddb keeps only short-lived credential, policy, and table-metadata caches in process. This means:

- Multiple extenddb instances can share one backend catalog (with caveats — see Multi-Instance below)
- The backend's own HA, backup, and replication tools provide durability; with `[backup.store]` configured (file-backed SQLite has one by default), table backups go to the store on PostgreSQL, SQLite and MongoDB; without one, and always on Cassandra, they are in-catalog backups inside the database; the [Backup and Restore manual](14-backup-and-restore.md) covers both
- extenddb can run anywhere the storage backend is reachable

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
child and receives `SIGTERM` directly. In foreground mode extenddb writes no PID
file and never creates `run_dir`, so the root filesystem can be read-only. Use
`tini` as PID 1 to reap any stray children and forward signals:

```bash
#!/bin/sh
# entrypoint.sh
set -e
CONFIG=/etc/extenddb/extenddb.toml
# The preview takes no lock and exits 3 when migrations are pending. Only then
# does migrate --yes run, which waits for every running server of the catalog
# to stop, so a start beside another server with nothing to apply does not
# wait for it.
rc=0
extenddb migrate --config "$CONFIG" || rc=$?
if [ "$rc" -eq 3 ]; then
  extenddb migrate --yes --config "$CONFIG"
elif [ "$rc" -ne 0 ]; then
  exit "$rc"
fi
exec extenddb serve --foreground --config "$CONFIG"
```

`extenddb stop` cannot stop a foreground server, because there is no PID file to
read; stop the container instead, which delivers `SIGTERM` and triggers the same
graceful shutdown. With a backup job running the shutdown takes up to 35
seconds before the workers stop, and up to 76 seconds in all when the database
is slow to answer (the admin guide's Stopping section); a runtime whose grace
period is shorter (Docker's default is 10 s) kills the process during the job
drain, and the reconciler resolves the job's row as it does after a crash. The
repository's compose files set `stop_grace_period: 80s`, above that sum; a
compose file of your own needs the same. Outside a container, where you may want `stop` to work, add
`--write-pid-file` — it writes the PID file to the usual `run_dir` path, which
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

For Kubernetes, run `extenddb init` as an init container or a one-time Job, then deploy extenddb as a Deployment with the generated `extenddb.toml` mounted as a ConfigMap or Secret.

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
- [ ] Leave credential import off unless you need it. The server refuses imports unless `allow_credential_import` is `true`. If you enabled it to import keys, turn it off again: `extenddb settings set allow_credential_import false`

### Transport

- [ ] TLS is enabled by default. Verify it is not disabled in `extenddb.toml`
- [ ] Replace the self-signed certificate with a CA-signed certificate
- [ ] Encrypt the connection to the storage backend. For PostgreSQL, use `sslmode=require` or `sslmode=verify-full` in the connection string. The Cassandra backend cannot encrypt its connection — place ExtendDB and the Cassandra cluster on a trusted, isolated network instead

### Network

- [ ] Set `bind_addr` to the appropriate interface (not `0.0.0.0` unless behind a load balancer)
- [ ] Firewall: allow only necessary ports (extenddb port, storage backend port)
- [ ] Consider a reverse proxy (nginx, HAProxy) for TLS termination, rate limiting, and access logging. Unless the proxy preserves the client address, login limits count the proxy's address as the source for every caller behind it (see the security model manual).

### Database (PostgreSQL)

- [ ] Use a dedicated PostgreSQL user for extenddb with minimal privileges
- [ ] Configure PostgreSQL `max_connections` ≥ `2 * pool_size + catalog_pool_size + max_concurrent_jobs`, plus one per overlapping vector index build and one during a schema migration
- [ ] Enable PostgreSQL TLS
- [ ] Set up automated backups that copy the catalog and data databases as one copy: a base backup with WAL archiving or managed service snapshots of the instance (either covers one instance, so only when both databases are on one PostgreSQL instance), or `pg_dump` of both with the server stopped, under `umask 077` (the admin guide's [Data Recovery](05-admin-guide.md#data-recovery) entry)
- [ ] Configure `[backup.store]` (S3 for production) per the [Backup and Restore manual](14-backup-and-restore.md)
- [ ] Monitor PostgreSQL disk usage, connection count, and query performance

### Monitoring

- [ ] Poll `/metrics` with an admin user's credentials (HTTP Basic auth) for JSON snapshots and forward to your monitoring system (the response is custom JSON, not Prometheus exposition format; convert as needed)
- [ ] Forward syslog to a log aggregation service
- [ ] Set up alerts on health check failures (`/health`)
- [ ] Monitor extenddb process with systemd, supervisord, or equivalent

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
# On a host that backs up tables of large items, one malloc arena keeps a
# backup job's high-water mark at the pipeline's terms whatever the number of
# large items it reads (the admin guide's [backup] max_concurrent_jobs row
# gives the figures). It costs the request path a few percent (3.5 to 9.2%
# on GetItem, Query and Scan on SQLite, about 2% on PostgreSQL (the scans,
# 2.1%)), so the line is left to the operator. On SQLite it also lowers
# the peak of a restore of tables whose items are larger than a few
# hundred bytes, of any attribute types (the Backup and Restore manual
# gives the figures):
#Environment=MALLOC_ARENA_MAX=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable extenddb
sudo systemctl start extenddb
```

With this unit only the `extenddb` user and root can read export files: the server creates them with mode 0600, and the import and export roots and account directories it creates with mode 0700, which the daemon's umask (027) can only narrow; before v0.1.15 that umask gave the `extenddb` group read access (see [the v0.1.15 entry](07-upgrade-manual.md#v0115-export-files-are-created-0600-and-the-import-and-export-directories-the-server-creates-0700) of the upgrade manual). To give a group access to a root, for example to put import files into it, create the root before the first start with that group and the mode you want, since an existing directory keeps its mode; export files in it are still created 0600. Import files go under `<root>/<account id>`. When one directory is both an import and an export root (the deprecated `import_export_root`, or the same path under `[import]` and `[export]`), the first export of an account creates that directory 0700, so also create each `<root>/<account id>` beforehand with the group and mode you want, or use separate import and export roots. Keep each parent directory of a root writable by its owner only (root or `extenddb`): the server logs a warning at startup for a parent directory that group or others can write to without the sticky bit, since a user who can write to it could replace the root.

## Multi-Instance Considerations

Run one extenddb instance per catalog. Multiple instances behind a load balancer are not supported in this release:

- Credentials, policies, and table metadata are cached in each process (see the cache TTLs in the admin guide). Nothing invalidates a cache on another instance, so a key revoked or a policy changed through one instance stays in force on the others until the TTL expires.
- Every instance runs every background worker (TTL expiry, GSI backfill, stream retention, control-plane transitions). The workers are safe to run twice, but the work is duplicated.
- `/health` is a liveness check that does not query the database, so a load balancer cannot use it to take an instance with a broken database connection out of rotation. It does report the backup store: `components.backup_store` is `unhealthy` and `status` is `degraded` while the configured store cannot be reached or fails its identity check, with HTTP 200 kept, so a store outage never drains a replica.
- Instances that share a catalog must share one backup store root (`[backup.store]`: one S3 bucket and prefix, or one shared mount that supports hard links, which the store's conditional create needs; some CIFS and FUSE mounts refuse `link(2)` with `EPERM`, and the server then reports the store as having failed validation, with the `link` error in the log line). A host-local directory configured at the same path on each host is not one root: once the catalog has rows for the root, the second instance finds the root's identity file missing or the newest backup's manifest absent and starts with the store unhealthy. While the catalog has no rows for the root (a fresh deployment, or one whose backups were purged), nothing tells the two directories apart: each instance creates its own identity file and store id and both start healthy, and the first backups then split the rows between two ids, which the next check reports as an identity mismatch. `backup purge`, plain `backup sync` and `--prune-missing` all refuse while the store is unhealthy, and `--forget-store` refuses the one root label both directories share. To clear it, keep one directory: remove each row that carries the other directory's store id with `extenddb backup sync --forget-backup <arn>` (`backup list` prints each row's `store_id`; the command runs on the unhealthy store and deletes no object), copy that directory's account directories, without its `extenddb-store.json`, into the kept one as the server's user, keeping directories `0700` and files `0600` (`cp -rp` or `rsync -a`; `backup sync` exits 1 on a directory left `0755`), point every instance at the kept directory over a shared mount, and run plain `backup sync`, which adopts them; or restore the catalog from a copy taken before the split, keep the directory whose store id the restored rows carry, and copy the other directory's account directories into it the same way before `backup sync` (the backups that the other directory's instance took after the catalog's copy are otherwise unknown to it). The [Backup and Restore manual](14-backup-and-restore.md#configuration) gives both, with the case of a `DELETING` row among them. Point every instance at one root before the first backup. To change the root later without losing the backups, reconfigure every instance to the new root, copy the old root's tree there without its `extenddb-store.json` (the new root has its own once the instances restart) and run `extenddb backup sync --move-store <old root>` ([Moving a store to a new root](14-backup-and-restore.md#moving-a-store-to-a-new-root)). Each instance on a host needs its own `spool_path`; the spool's `.lock` refuses a second instance. An existing spool directory must be owned by the server's user with mode `0700`, as the root must; a spool created `0755` stops startup naming it.

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

Backup and restore work is bounded per server by `[backup] max_concurrent_jobs` (default 4): `CreateBackup` and `RestoreTableFromBackup` jobs together, on every backend, and a request that finds every slot held waits up to 30 s for one, holding nothing meanwhile, and is then refused with `LimitExceededException` and creates nothing. On PostgreSQL each running job holds one pooled connection for its copy plus one dedicated connection outside the pool for its claim, and startup raises `pool_size` and `catalog_pool_size` to at least `2 * max_concurrent_jobs + 10` with a warning naming both numbers (the admin guide's `[backup]` table). On SQLite a `CreateBackup` also holds its snapshot connection for its duration, and a server admits at most `pool_size / 2 - 1` of them at once (between 1 and 16; 4 for the default `pool_size` of 10), so that half the pool stays free for ordinary requests, and raises a smaller `pool_size` to `2 * (max_concurrent_jobs + 1)`, at most 34, so that bound refuses no job `max_concurrent_jobs` admits (the admin guide's `max_concurrent_jobs` row; above 16 jobs the slot refuses the seventeenth); a `CreateBackup` over that bound is refused at once with the same `LimitExceededException`. A MongoDB restore request that is not registered within 60 seconds answers an internal error while the restore goes on in the background; the MongoDB driver does not enforce `waitQueueTimeoutMS`, so the admission bound is what keeps a flood of restores from holding every pooled connection.

### Cassandra Connection Pool

The Cassandra backend opens `max_connections` connections per node (default 10)
and scatters requests across them:

```toml
[storage.cassandra]
max_connections = 10  # connections per node
```

Keep this above 1. A single connection serializes every request's protocol
stream id onto one socket and leaves no headroom if that socket drops. Spreading
requests over several connections also divides the stream-id space, which eases a
driver-level stream-id reuse hazard seen under sustained load with a slow
statement in flight.

### PostgreSQL Tuning

Key PostgreSQL settings for extenddb workloads:

- `shared_buffers`: 25% of available RAM
- `effective_cache_size`: 75% of available RAM
- `work_mem`: 64MB (for sort operations in Query/Scan)
- `max_connections`: ≥ `2 * pool_size + catalog_pool_size + max_concurrent_jobs`, plus one per vector index build you expect to overlap and one during a schema migration. `pool_size` sizes both the catalog and the data pool, each backup or restore job holds one claim connection outside every pool, and `max_connections` is per cluster, so the default pool sizes and the default `max_concurrent_jobs` of 4 need 64 of PostgreSQL's default 100

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
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
