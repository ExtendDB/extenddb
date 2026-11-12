# ExtendDB

> **ExtendDB is an open source project maintained by AWS. It is not Amazon DynamoDB and does
> not contain any DynamoDB source code.** "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room
> implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are documented
> in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

ExtendDB is a DynamoDB-compatible API server. Any AWS SDK, CLI or tool that works with DynamoDB works with ExtendDB.
This crate builds the `extenddb` binary.

## Install

You need Rust 1.88 or later and a C compiler. Each build contains one storage backend, which a Cargo feature selects:

| Backend | Command |
|---|---|
| PostgreSQL (the default) | `cargo install extenddb --locked` |
| MongoDB | `cargo install extenddb --locked --no-default-features --features mongodb` |
| Apache Cassandra (experimental) | `cargo install extenddb --locked --no-default-features --features cassandra` |
| SQLite dev server | `cargo install extenddb --locked --no-default-features --features sqlite,dev-mode` |

Every build installs a binary named `extenddb`, so you can have one of them installed at a time.

With `--locked`, Cargo builds from the `Cargo.lock` inside the crate, which holds the dependency versions that the
project tests. Without it, Cargo resolves every dependency again and can choose versions that the project never
tested.

## Run the dev server

The SQLite dev build needs no database and no configuration file:

```bash
extenddb serve --foreground
```

It serves plain HTTP on `127.0.0.1:18443`. It refuses a bind address other than loopback unless the environment sets
`EXTENDDB_DEV_ALLOW_ANY_BIND=1`, as the dev container image does for its own network.

When it starts with no database, it creates `extenddb.sqlite` and a `backups/` directory in the working directory. It
also adds an `admin` user to the database and prints the admin password. At startup it prints the AWS example key pair,
which it always accepts:

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
aws dynamodb list-tables --region us-east-1 --endpoint-url http://127.0.0.1:18443
```

The dev build has no TLS, and its authorization is open: a request signed with any key that the server holds can run
every operation. Use it for local development and tests only, and never store data in it that you need to keep
private.

Without `--foreground`, `extenddb serve` runs as a daemon and writes its log to syslog. Daemon mode works on Unix
systems only, so on Windows always pass `--foreground`.

## Run a server build

The PostgreSQL, MongoDB and Cassandra builds each need a running database:

- PostgreSQL 14 or later
- MongoDB 7.0 or later, configured as a replica set
- Apache Cassandra 4.1 or later

Run `extenddb init` once to create the catalog, the admin user, a TLS certificate and `extenddb.toml`. Then run
`extenddb serve`, which listens with TLS on port 18443.
[Getting started](https://github.com/ExtendDB/extenddb/blob/main/docs/getting-started.md) describes each step and
each backend's options.

## Other ways to run ExtendDB

- Docker images, with no Rust toolchain needed:
  [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres),
  [`extenddb/extenddb-mongodb`](https://hub.docker.com/r/extenddb/extenddb-mongodb) and
  [`extenddb/extenddb-dev`](https://hub.docker.com/r/extenddb/extenddb-dev).
- [`@extenddb/dev`](https://www.npmjs.com/package/@extenddb/dev) on npm starts a prebuilt dev server from a Node.js
  test suite.

The [documentation](https://github.com/ExtendDB/extenddb/tree/main/docs) covers configuration, operations and the API
surface.

## The library crates

The `extenddb-*` library crates on crates.io are the parts of this binary. Their Rust APIs carry no stability promise,
and any release can change them. Each release publishes every crate at the same version, and each crate requires its
ExtendDB dependencies at exactly that version.

## License

ExtendDB is licensed under the Apache License 2.0. See
[LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) and
[NOTICE](https://github.com/ExtendDB/extenddb/blob/main/NOTICE).
