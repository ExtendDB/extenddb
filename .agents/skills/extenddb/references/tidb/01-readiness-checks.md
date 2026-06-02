# TiDB Readiness Checks

## Purpose

Use this reference when the default TiDB backend is not ready for `extenddb init`
or `extenddb serve`. TiDB is the default backend for standard builds, so setup
must verify the TiDB SQL endpoint rather than PostgreSQL readiness.

## Client Check

Confirm a MySQL-compatible client is available:

```bash
which mysql && mysql --version
```

If missing:

- Linux (Ubuntu/Debian): install `default-mysql-client`
- Linux (Fedora/RHEL): install `mysql`
- macOS: install `mysql-client`

## Local TiDB Check

For local development, confirm TiDB is accepting SQL connections on the default
TiDB port:

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

If the command cannot connect, start a local TiDB playground or point init at a
remote TiDB SQL endpoint.

## Local TiUP Playground

One local development option:

```bash
tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor
```

Leave the playground process running in its terminal while running `extenddb
init`, `verify`, and `serve` from another terminal.

## Remote TiDB

For a remote TiDB cluster, pass the SQL endpoint and admin credentials during
init:

```bash
./target/release/extenddb init \
  --storage-host tidb.example.com \
  --storage-port 4000 \
  --storage-admin-user root \
  --storage-admin-password <admin-password>
```

When local TiUP playground uses the default root user with no password, omit
`--storage-admin-password`.
