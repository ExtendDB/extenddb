# Local PostgreSQL Setup

## Installation

PostgreSQL 15.17 installed from PGDG repository (EL-7 compatible) on Amazon Linux 2:

```bash
sudo rpm -ivh --nodeps \
    https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo yum install -y --releasever=7 \
    --disablerepo="*" --enablerepo="pgdg15" \
    postgresql15-server postgresql15
```

Binaries: `/usr/pgsql-15/bin/`

## Data Directory

```
$HOME/pgdata
```

Initialized with:
```bash
/usr/pgsql-15/bin/initdb -D ~/pgdata --auth=trust --no-locale --encoding=UTF8
```

Runs as the current user (not as `postgres` system user).

You may need to add the current user to the `postgres` group; if initdb fails, check your logs.

## Connection Details

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `extenddb` |
| User | `extenddb` |
| Password | `extenddb-local-dev` |
| Connection string | `postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb` |
| Admin user | the OS user that ran `initdb` (trusted via local socket) |

## Starting / Stopping

```bash
export PATH=/usr/pgsql-15/bin:$PATH

# Start
pg_ctl -D ~/pgdata -l ~/pgdata/server.log start

# Stop
pg_ctl -D ~/pgdata stop

# Status
pg_ctl -D ~/pgdata status

# Logs
tail -f ~/pgdata/server.log
```

## Authentication

`pgdata/pg_hba.conf` is configured so:
- The `extenddb` user requires md5 password auth over TCP (`127.0.0.1`, `::1`)
- The OS user that ran `initdb` has trust auth for admin tasks
- Local socket connections use trust auth

## Config Mapping

The extenddb `config.toml` should use:
```toml
[storage.postgres]
connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb"
pool_size = 20
```

Or via environment variable:
```bash
export EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING=\
"postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb"
```

---

## Docker Setup (Alternative)

A `docker-compose.yml` is provided at the repository root for a quick PostgreSQL setup with no manual installation.

### Prerequisites

- Docker Engine 20.10+ with Compose plugin (`docker compose`)

### Start PostgreSQL

```bash
docker compose up -d
```

Wait for the container to be healthy:

```bash
docker compose ps
```

### Connection Details

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Admin user | `postgres` |
| Admin password | `extenddb-local-dev` |

### Initialize ExtendDB

Since the Docker container uses `postgres` as the superuser, pass it explicitly to `init`:

```bash
./target/release/extenddb init --config extenddb.toml \
  --pg-user postgres \
  --pg-pass extenddb-local-dev
```

### Stop and Clean Up

```bash
# Stop (preserves data)
docker compose down

# Stop and delete all data
docker compose down -v
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
