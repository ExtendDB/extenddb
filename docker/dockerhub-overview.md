# ExtendDB

ExtendDB is a DynamoDB-compatible API adapter that enables teams to run DynamoDB workloads on their own infrastructure, backed by a PostgreSQL database they operate.

## Benefits of using ExtendDB

ExtendDB works with your existing DynamoDB API calls. Any AWS SDK, CLI, or tool that talks to DynamoDB talks to ExtendDB unchanged, so you point your client at a different endpoint and nothing else about your code changes. The full surface is there: CRUD, Query, Scan, Batch, Transactions, Streams, TTL, Import and Export, plus SigV4 with IAM-compatible users, roles and policies.

It is an adapter, not a database. This image contains the ExtendDB server compiled with the PostgreSQL backend; you supply PostgreSQL 14 or newer, whether that is RDS, Aurora PostgreSQL, or self-managed. Your data therefore lives somewhere you already know how to back up, replicate, monitor and restore, using tools you already run.

Because it runs on your infrastructure, ExtendDB needs no internet connection, and there are no provisioned throughput, data storage, or data transfer costs. That makes it a fit for self-hosted and air-gapped deployments, for DynamoDB semantics on any cloud that runs PostgreSQL, and for continuous integration against a durable backend rather than an ephemeral one.

## Getting started with ExtendDB on Docker

For local development, the Compose stack in the repository starts PostgreSQL alongside ExtendDB and runs the one-time bootstrap for you. See [local development with Docker Compose](https://github.com/ExtendDB/extenddb/blob/main/docs/local-dev-docker-compose.md).

For a real deployment the image serves three explicit operations: `extenddb init` once to bootstrap the database, `extenddb migrate` when upgrading, and `extenddb serve` in steady state, which is the default command. It listens on port 18443 over TLS, keeps configuration and certificates under `/var/lib/extenddb`, runs as a non-root user, and supports a read-only root filesystem.

To learn how to configure and operate it, see [getting started](https://github.com/ExtendDB/extenddb/blob/main/docs/getting-started.md) and the [container notes](https://github.com/ExtendDB/extenddb/blob/main/docker/README.md).

## Tags and verification

Pin `X.Y.Z` or a digest for production; a version tag is never overwritten. `latest` tracks the highest release and only moves forward. Tags of the form `sha-<commit>` are unpromoted build candidates, not releases. Both `linux/amd64` and `linux/arm64` ship as one multi-architecture index.

Every published image is signed with ExtendDB's release key, an ECDSA P-256 key held in AWS KMS. Each [release](https://github.com/ExtendDB/extenddb/releases) attaches the public key and gives the exact `cosign verify` command for that release. The same images are mirrored by digest to `ghcr.io/extenddb/extenddb-postgres`.

## Note

ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol; behavioral differences from the service are documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

More at [extenddb.org](https://extenddb.org) and [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb). Licensed under Apache-2.0.
