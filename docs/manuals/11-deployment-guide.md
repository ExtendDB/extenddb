# Deployment Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

This guide covers deploying extenddb in various environments beyond local development.

## Architecture Overview

extenddb is a single Rust binary that connects to PostgreSQL. All state lives in PostgreSQL — extenddb itself is stateless (no in-process caching). This means:

- Multiple extenddb instances can share a PostgreSQL catalog (with caveats — see Multi-Instance below)
- Standard PostgreSQL HA, backup, and replication tools provide durability
- extenddb can run anywhere PostgreSQL is reachable

## Deployment Models

### Single-Node

extenddb and PostgreSQL on the same host. Simplest setup, suitable for development, CI, and small workloads.

```
┌─────────────────────────┐
│  Host                   │
│  ┌─────┐  ┌──────────┐ │
│  │ extenddb│──│PostgreSQL │ │
│  └─────┘  └──────────┘ │
└─────────────────────────┘
```

```bash
extenddb init
extenddb serve --config extenddb.toml
```

### Separated Database

extenddb on an application server, PostgreSQL on a dedicated database server or managed service.

```
┌──────────┐       ┌──────────────┐
│  App Host│──────▶│  DB Host     │
│  extenddb    │       │  PostgreSQL  │
└──────────┘       └──────────────┘
```

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

Use `sslmode=require` (or `verify-full` with a CA certificate) for encrypted database connections.

### Managed PostgreSQL

extenddb works with any PostgreSQL 14+ service:

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

### Containerized

extenddb runs in Docker or Kubernetes. The binary has no runtime dependencies beyond libc and network access to PostgreSQL.

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
- PostgreSQL 14+ (reachable from the extenddb host)
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
- [ ] Use `sslmode=require` or `sslmode=verify-full` in the PostgreSQL connection string

### Network

- [ ] Set `bind_addr` to the appropriate interface (not `0.0.0.0` unless behind a load balancer)
- [ ] Firewall: allow only necessary ports (extenddb port, PostgreSQL port)
- [ ] Consider a reverse proxy (nginx, HAProxy) for TLS termination, rate limiting, and access logging

### Database

- [ ] Use a dedicated PostgreSQL user for extenddb with minimal privileges
- [ ] Configure PostgreSQL `max_connections` ≥ extenddb `pool_size` + 3
- [ ] Enable PostgreSQL TLS
- [ ] Set up automated backups (pg_dump, WAL archiving, or managed service snapshots)
- [ ] Monitor PostgreSQL disk usage, connection count, and query performance

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
After=postgresql.service
Requires=postgresql.service

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

Multiple extenddb instances can connect to the same PostgreSQL catalog. However:

- extenddb does not cache database state in-process — every request reads directly from PostgreSQL
- This means multiple instances see consistent data without cache invalidation
- PostgreSQL's connection pool and row-level locking handle concurrent access
- Ensure `pool_size × instance_count + 3 × instance_count ≤ PostgreSQL max_connections`

## Performance Tuning

### Connection Pool

The default `pool_size = 20` is suitable for moderate workloads. For high-concurrency deployments:

```toml
[storage.postgres]
pool_size = 50  # Increase for higher concurrency
```

Ensure PostgreSQL `max_connections` accommodates the total pool size plus overhead.

### PostgreSQL Tuning

Key PostgreSQL settings for extenddb workloads:

- `shared_buffers`: 25% of available RAM
- `effective_cache_size`: 75% of available RAM
- `work_mem`: 64MB (for sort operations in Query/Scan)
- `max_connections`: ≥ extenddb pool_size + 10

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
