# ExtendDB (PostgreSQL backend)

**The DynamoDB API, everywhere you run code.** — [extenddb.org](https://extenddb.org)

> ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

ExtendDB speaks the DynamoDB wire protocol, so any AWS SDK, CLI, or tool that works with DynamoDB works with ExtendDB unchanged. Point your client at the ExtendDB endpoint and nothing else about your code changes.

This image is an **adapter, not a database**. It contains the ExtendDB server compiled with the PostgreSQL backend. It does **not** contain a PostgreSQL server or data directory: bring your own PostgreSQL 14 or newer, whether that is RDS, Aurora PostgreSQL, or self-managed.

## Tags

| Tag | What it is |
| --- | --- |
| `X.Y.Z` | A released version. Immutable: a version tag is never overwritten. |
| `latest` | The highest released version. Only ever moves forward. |
| `sha-<40-char-commit>` | A build candidate, published before promotion. Not a release; pin a version instead. |

Both `linux/amd64` and `linux/arm64` are published as a single multi-architecture index, so `docker pull` selects the right platform automatically.

For production, pin `X.Y.Z` or a digest rather than `latest`.

## Quick start

For local development the fastest path is the Compose stack in the repository, which brings up PostgreSQL alongside ExtendDB and runs the one-time bootstrap for you. See [Local development with Docker Compose](https://github.com/ExtendDB/extenddb/blob/main/docs/local-dev-docker-compose.md).

Once running, use any DynamoDB client against the endpoint:

```bash
aws dynamodb list-tables \
  --endpoint-url https://127.0.0.1:18443 \
  --region us-east-1
```

## Running it yourself

The image serves three explicit operations. Do not use the Compose bootstrap behaviour as a production control plane.

**1. Bootstrap once**, as a single Job holding the PostgreSQL bootstrap credential. Inject `EXTENDDB_PG_PASSWORD` and `EXTENDDB_APP_PASSWORD` from your orchestrator's secret store rather than passing them as arguments:

```bash
extenddb init \
  --config /var/lib/extenddb/extenddb.toml \
  --backend postgres \
  --pg-host <postgres-host> --pg-port 5432 \
  --pg-user <bootstrap-user> --extenddb-user <application-user> \
  --bind-addr 0.0.0.0 --tls-san <service-dns-name>
```

**2. On upgrade**, back up PostgreSQL, scale down incompatible serving versions, then run one migration Job:

```bash
extenddb migrate --config /var/lib/extenddb/extenddb.toml --yes --pg-user <bootstrap-user>
```

The generated config stores the application connection but not the bootstrap password, so the migration Job receives that elevated credential separately. The serving container never gets it.

**3. Steady state** is the image's default command:

```bash
extenddb serve --config /var/lib/extenddb/extenddb.toml --foreground
```

## Runtime contract

| | |
| --- | --- |
| Port | `18443` (HTTPS) |
| State volume | `/var/lib/extenddb` — must be writable; holds config and TLS material |
| User | `10001:10001`, non-root |
| Entrypoint | `tini`, so `SIGTERM` reaches the server and shutdown is graceful |
| Healthcheck | `extenddb healthcheck` — liveness only, it does not probe PostgreSQL readiness |

Supports a read-only root filesystem. Drop all capabilities, keep the runtime's default seccomp profile, and set CPU and memory limits in your orchestrator.

TLS is on by default with a generated self-signed certificate, replaceable with a CA-signed one. The Compose stack's certificate and passwords are development defaults and must not be used anywhere shared.

## Verifying the image

Every published image is signed with ExtendDB's release key, an ECDSA P-256 key held in AWS KMS. The public key is attached to each [GitHub release](https://github.com/ExtendDB/extenddb/releases) as `extenddb-signing.pub.pem`, and each release's notes carry the exact `cosign verify` invocation for that release. Verify against the digest you are deploying.

## Also available on GHCR

The same images, by digest, are mirrored to `ghcr.io/extenddb/extenddb-postgres`.

## Links

- Project site: [extenddb.org](https://extenddb.org)
- Source, issues and discussions: [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb)
- [Getting started](https://github.com/ExtendDB/extenddb/blob/main/docs/getting-started.md)
- [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md)
- [Troubleshooting](https://github.com/ExtendDB/extenddb/blob/main/docs/troubleshooting.md)
- Licensed under Apache-2.0
