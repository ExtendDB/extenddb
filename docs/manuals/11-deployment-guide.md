# Deployment Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

This guide covers deploying extenddb in environments beyond local development.
The default and recommended storage backend is TiDB. PostgreSQL remains an
explicit alternate backend for users who build and configure it deliberately,
but production guidance in this manual assumes TiDB.

## Architecture Overview

extenddb is a single Rust binary that connects to a configured storage backend. All durable state lives in that backend — extenddb itself is stateless (no in-process caching). This means:

- Multiple extenddb instances can share one TiDB catalog/data topology
- Backend-native HA, backup, and replication tools provide durability
- extenddb can run anywhere the configured storage backend is reachable

## Deployment Models

### Single-Node

extenddb and the storage backend on the same host. Simplest setup, suitable for development, CI, and small workloads.

```
┌─────────────────────────┐
│  Host                   │
│  ┌─────┐  ┌──────────┐ │
│  │ extenddb│──│Storage DB │ │
│  └─────┘  └──────────┘ │
└─────────────────────────┘
```

```bash
extenddb init
extenddb serve --config extenddb.toml
```

### Separated TiDB Cluster

extenddb on application servers, TiDB on a dedicated TiDB cluster or managed
TiDB service.

```
┌──────────┐       ┌──────────────┐
│  App Host│──────▶│  TiDB SQL    │
│ extenddb │       │  + PD/TiKV   │
└──────────┘       └──────────────┘
```

```bash
extenddb init \
  --storage-host tidb.example.com \
  --storage-admin-password
```

Configure the connection string in `extenddb.toml`:

```toml
[storage]
backend = "tidb"

[storage.tidb]
connection_string = "mysql://extenddb:<password>@tidb.example.com:4000/extenddb_catalog"
pool_size = 20
catalog_pool_size = 20
resource_group = "extenddb_api"
```

Enable TLS on the TiDB endpoint in production and use a dedicated TiDB user for
the ExtendDB catalog and data databases.

### Managed TiDB

The standard build uses TiDB's MySQL-compatible endpoint:

```toml
[storage]
backend = "tidb"

[storage.tidb]
connection_string = "mysql://extenddb:<password>@tidb.example.com:4000/extenddb_catalog"
pool_size = 20
catalog_pool_size = 20

[storage.tidb.backup]
pd_endpoint = "pd.example.com:2379"
storage_uri = "s3://extenddb-backups/prod"
send_credentials_to_tikv = false
```

Use TiDB-native HA for the cluster and BR for physical backup/restore.
Use TiDB Resource Control/resource groups for distributed capacity governance
across multiple extenddb frontends.

### PostgreSQL Alternate Backend

PostgreSQL is not the product-default deployment path. To use it, build the
binary with `--no-default-features --features postgres`, set
`[storage].backend = "postgres"`, and follow PostgreSQL-native HA and backup
guidance. Do not mix TiDB and PostgreSQL frontends against one deployment.

### Containerized

extenddb runs in Docker or Kubernetes. The binary has no runtime dependencies beyond libc and network access to the configured storage backend.

Example Dockerfile:

```dockerfile
# Match rust-version in Cargo.toml
FROM rust:1.95 AS builder
WORKDIR /src
COPY . .
RUN cargo build -j12 --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates tini && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/extenddb /usr/local/bin/extenddb
COPY extenddb.toml /etc/extenddb/extenddb.toml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
EXPOSE 8000
ENTRYPOINT ["tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
```

extenddb always daemonizes (there is no foreground mode). In a container, the parent process exits after forking, which causes the container runtime to stop the container. Use `tini` as PID 1 and a wrapper script that starts extenddb and waits on the daemon process:

```bash
#!/bin/sh
# entrypoint.sh
extenddb serve --config /etc/extenddb/extenddb.toml
# Wait on the daemon PID — the PID file location depends on run_dir in extenddb.toml
# Default: ~/.extenddb/run/extenddb-<port>.pid
PID_FILE="${HOME}/.extenddb/run/extenddb-8000.pid"
if [ -f "$PID_FILE" ]; then
  tail --pid="$(cat "$PID_FILE")" -f /dev/null
else
  echo "extenddb failed to start — PID file not found at $PID_FILE" >&2
  exit 1
fi
```

For Kubernetes, run `extenddb init` as an init container or a one-time Job, then deploy extenddb as a Deployment with the generated `extenddb.toml` mounted as a ConfigMap or Secret.

### Air-Gapped

extenddb requires no internet connectivity. All functionality is self-contained in the binary. Build on a connected host, transfer the binary and `extenddb.toml` to the air-gapped environment, and run.

Requirements in the air-gapped environment:
- A supported storage backend reachable from the extenddb host
- The `extenddb` binary (statically linked or with matching libc)

## Production Checklist

### Authentication

- [ ] Set `auth.provider = "builtin"` in `extenddb.toml`
- [ ] Change the admin password from the auto-generated one
- [ ] Create named accounts and IAM users with least-privilege policies
- [ ] Disable credential import if not needed: `extenddb settings set allow_credential_import false`

### Transport

- [ ] TLS is enabled by default. Verify it is not disabled in `extenddb.toml`
- [ ] Replace the self-signed certificate with a CA-signed certificate
- [ ] Enable TLS on the TiDB SQL endpoint when traffic leaves localhost or a trusted private network

### Network

- [ ] Set `bind_addr` to the appropriate interface (not `0.0.0.0` unless behind a load balancer)
- [ ] Firewall: allow only necessary ports (extenddb port and TiDB SQL/PD ports needed by the deployment)
- [ ] Consider a reverse proxy (nginx, HAProxy) for TLS termination, rate limiting, and access logging

### Storage Backend

- [ ] Use a dedicated backend user for extenddb with minimal privileges
- [ ] Size TiDB SQL nodes for the active frontend count. Each frontend creates strong data, default-read data, engine catalog, and catalog-store/auth pools.
- [ ] Enable backend transport encryption
- [ ] Set up automated backups with TiDB BR or TiDB cluster-level PITR
- [ ] Use TiDB Resource Control/resource groups for distributed capacity governance instead of `throttling_enabled`
- [ ] Monitor backend disk usage, connection count, replication health, and query performance

### Monitoring

- [ ] Poll `/metrics` for JSON snapshots and forward to your monitoring system (the response is custom JSON, not Prometheus exposition format; convert as needed)
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
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
ExecStart=/usr/local/bin/extenddb serve --config /etc/extenddb/extenddb.toml
ExecStop=/usr/local/bin/extenddb stop --config /etc/extenddb/extenddb.toml
# PID file path: {run_dir}/extenddb-{port}.pid
# Default run_dir is ~/.extenddb/run (~ expands to $HOME of the User= below)
# Adjust if run_dir or port are overridden in extenddb.toml
PIDFile=/home/extenddb/.extenddb/run/extenddb-8000.pid
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

Multiple extenddb instances can connect to the same catalog. However:

- extenddb does not cache database state in-process — every request reads directly from the storage backend
- This means multiple instances see consistent data without cache invalidation
- The storage backend's connection pool and transaction model handle concurrent access
- Ensure each instance's backend-specific pool footprint fits the backend's connection limits
- TiDB capacity control is cluster-native. Process-local token buckets are disabled for TiDB because each frontend would otherwise admit its own burst.

## Performance Tuning

### Connection Pool

The default `pool_size = 20` is suitable for moderate workloads. For high-concurrency deployments:

```toml
[storage.tidb]
pool_size = 50          # strong data pool + default-read data pool
catalog_pool_size = 50  # engine catalog pool + auth/catalog-store pool
resource_group = "extenddb_api"
```

Ensure TiDB SQL nodes can accept the total session count:
`frontends * (2 * pool_size + 2 * catalog_pool_size)`, plus operational
sessions and any other applications using the cluster.

### TiDB Tuning

Key TiDB settings and practices for ExtendDB workloads:

- Use TiDB Resource Control/resource groups for cluster-wide API capacity.
- Keep TiDB native TTL enabled for catalog, stream, idempotency-token, and user-table TTL cleanup.
- Use BR or TiDB cluster-level PITR for backup and recovery.
- Keep catalog and data databases in the same TiDB cluster so TSO snapshots, online DDL, TTL, and BR share one timeline.
- Monitor TiDB SQL session count, PD scheduling, Region distribution, DDL jobs, TTL jobs, BR jobs, and slow queries.

### Monitoring Queries

```sql
-- Active TiDB sessions from extenddb
SELECT USER, HOST, DB, COMMAND, TIME, STATE
FROM information_schema.cluster_processlist
WHERE USER = 'extenddb';

-- Online DDL jobs for ExtendDB data tables
SELECT TABLE_SCHEMA, TABLE_NAME, JOB_TYPE, STATE
FROM information_schema.ddl_jobs
WHERE TABLE_NAME LIKE '\\_ddb\\_%';

-- TTL-enabled tables
SELECT TABLE_SCHEMA, TABLE_NAME, TTL_ENABLE, TTL_JOB_INTERVAL
FROM information_schema.tables
WHERE TABLE_SCHEMA IN ('extenddb_catalog', 'extenddb_catalog_data')
  AND TTL IS NOT NULL;
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
