---
description: "Set up a PostgreSQL instance on a development machine for building and testing ExtendDB."
---

# Local PostgreSQL setup (development)

These notes set up a PostgreSQL instance on a development machine for building and testing ExtendDB. For a running server with PostgreSQL in containers see [Docker Compose with PostgreSQL](local-dev-docker-compose.md); for production deployments see the [deployment guide](manuals/11-deployment-guide.md).

## Installation

One worked example, PostgreSQL 15 from the PGDG repository on Amazon Linux 2 (any PostgreSQL 14 or newer works):

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

`extenddb init` writes `extenddb.toml` with the catalog database it creates (`extenddb_catalog` by default). To point the server at the role above, edit the generated section:

```toml
[storage.postgres]
connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"
pool_size = 20
```

Or via environment variable:
```bash
export EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING=\
"postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb"
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
