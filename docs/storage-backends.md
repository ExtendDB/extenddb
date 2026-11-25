---
title: Storage backends
description: The four storage backends ExtendDB runs on, which is the default, how a backend is selected, and where each one is documented.
---

# Storage backends

ExtendDB stores tables, items, indexes and its own catalog in a database you run. Four backends exist. One binary holds one backend, chosen when the binary is built, and the configuration file names the same backend for `init` and `serve`.

| Backend | Build feature | Published image | Database you provide | Documented in |
|---|---|---|---|---|
| PostgreSQL (default) | `postgres` | `extenddb/extenddb-postgres` | PostgreSQL 14 or newer; the pgvector extension on the data database for vector indexes | [Docker Compose](local-dev-docker-compose.md), [from source](install-from-source.md#install-postgresql), [deployment](manuals/11-deployment-guide.md) |
| MongoDB | `mongodb` | `extenddb/extenddb-mongodb` | MongoDB 7.0 or newer as a replica set, including for a single node | [from source](install-from-source.md#install-mongodb), [MongoDB container](https://github.com/ExtendDB/extenddb/blob/main/docker/README-mongodb.md) |
| SQLite | `sqlite` | `extenddb/extenddb-dev` (built in developer mode, see below) | None; a file, or `:memory:` | [development image](dev-image.md), [from source](install-from-source.md#initialize-on-sqlite) |
| Cassandra (experimental) | `cassandra` | `extenddb/extenddb-cassandra` | An Apache Cassandra 4.1 or newer cluster | [Cassandra setup](cassandra-setup.md), [Cassandra container](https://github.com/ExtendDB/extenddb/blob/main/docker/README-cassandra.md), [from source](install-from-source.md#install-cassandra), [upgrade manual](manuals/07-upgrade-manual.md), [backend RFC (draft)](rfcs/draft-cassandra-backend.md) |

## Selecting a backend

A build enables exactly one backend feature; enabling two fails at compile time. The default feature is `postgres`:

```bash
cargo build --release                                            # PostgreSQL
cargo build --release --no-default-features --features mongodb   # MongoDB
cargo build --release --no-default-features --features sqlite    # SQLite
cargo build --release --no-default-features --features cassandra # Cassandra
```

`extenddb init` writes a configuration for the backend the binary was built with; `--backend` selects the matching `[storage.<name>]` section. The MongoDB binary reads its connection string from an existing `extenddb.toml`, so write that section first and pass `--overwrite`:

```bash
extenddb init                      # PostgreSQL
extenddb init --backend sqlite --sqlite-path /data/extenddb.sqlite

printf '[storage]\nbackend = "mongodb"\n\n[storage.mongodb]\nconnection_string = "mongodb://localhost:27017/?replicaSet=rs0"\n' > extenddb.toml
extenddb init --backend mongodb --overwrite
```

The published images are built one per backend, so with a container the choice is the image name.

## What differs between backends

Most of the DynamoDB API behaves the same on every backend. Where a backend differs from the service, or from another backend, the row on the [Differences from DynamoDB](differences-from-dynamodb.md) page names the backend. Backend-specific items include:

- PostgreSQL: vector indexes and `SearchVectors` need the pgvector extension on the data database; without it every vector operation is refused and nothing else is affected.
- MongoDB: no vector indexes or `SearchVectors`. `Query` on a global secondary index with more than one RANGE attribute takes a key condition on the first one only. Partition and sort key numbers carry 34 significant digits (BSON Decimal128) against the service's 38; larger precision is refused with a `ValidationException`.
- SQLite: one server per database file, enforced with a lock file next to the database. The `extenddb/extenddb-dev` image compiles this backend in developer mode: plain HTTP, open authorization, a seeded credential, for local development and CI only ([development image](dev-image.md)).
- Cassandra: the backend is experimental. It does not implement vector indexes or `SearchVectors`, so every vector operation is refused. It has the same `Query` limit as MongoDB on a GSI with more than one RANGE attribute. PutItem, UpdateItem and a conditional DeleteItem on a table without TTL, and the writes of a TransactWriteItems, commit the item before its stream record is written, so a failure between the two leaves the item changed with no stream record. `CreateBackup` keeps each backup in the catalog, with or without a backup store, and backups copy the live table and are not point-in-time. The vector, stream and backup rows on the differences page give the detail.

Running more than one server against one PostgreSQL, MongoDB or Cassandra catalog is not supported; the [deployment guide](manuals/11-deployment-guide.md#multi-instance-considerations) explains why.

## Developer notes

Setting up a local PostgreSQL or MongoDB for building and testing ExtendDB itself is covered in the [developer and test guide](manuals/06-developer-test-guide.md#local-databases-for-the-test-suite).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
