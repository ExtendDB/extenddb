# ExtendDB dev (SQLite)

**The DynamoDB API, everywhere you run code.** — [extenddb.org](https://extenddb.org)

> ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are documented in [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

A single container that speaks the DynamoDB wire protocol over SQLite, for **local development and CI**. No external database, no init step, no certificate to trust.

> **Not for production, and not for shared networks.** This image serves plain HTTP with open authorization: whoever holds the credential can do everything. Always publish it to loopback only. For a durable, TLS-terminated deployment use [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres).

## Quick start

```bash
docker run -d -p 127.0.0.1:18443:18443 -v extenddb:/var/lib/extenddb \
  extenddb/extenddb-dev
```

Then point any AWS SDK, CLI, or tool at it, unchanged:

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
aws dynamodb list-tables \
  --region us-east-1 --endpoint-url http://127.0.0.1:18443
```

That is AWS's documented example key pair. The server seeds it at startup and prints it in the logs. Secret scanners recognise it as an example credential, so it is safe in test fixtures. Pass `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to the container to use a different one.

## Storage modes

One image, mode chosen at run time.

**File-backed (default).** The database is `/var/lib/extenddb/extenddb.sqlite`. Mount a volume there and data survives restarts and upgrades:

```bash
docker run -d -p 127.0.0.1:18443:18443 -v extenddb:/var/lib/extenddb \
  extenddb/extenddb-dev
```

**In-memory.** Everything lives in process memory and vanishes when the container stops, which is what you want for a suite that needs a pristine database per run:

```bash
docker run -d -p 127.0.0.1:18443:18443 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: \
  extenddb/extenddb-dev
```

## Why loopback matters

The container binds `0.0.0.0` internally, which is required for port publishing to work at all. Containment is therefore the publish flag, not the bind address: keep the host side on `127.0.0.1`, as every example above does. Do not publish this port on a routable or shared interface, and do not put real data in it.

## Tags

| Tag | What it is |
| --- | --- |
| `X.Y.Z` | A released version. Immutable: a version tag is never overwritten. |
| `latest` | The highest released version. Only ever moves forward. |

Both `linux/amd64` and `linux/arm64` are published as a single multi-architecture index, so `docker pull` selects the right platform automatically.

## Runtime contract

| | |
| --- | --- |
| Endpoint | `http://127.0.0.1:18443` — plain HTTP, no TLS |
| State volume | `/var/lib/extenddb` (file mode); omit it for a throwaway container |
| User | `65532:65532`, non-root |
| Healthcheck | built in, so `docker ps` and Compose `depends_on: condition: service_healthy` work with no extra wiring |

The runtime base is distroless (`gcr.io/distroless/cc-debian12:nonroot`): no shell and no package manager, so `docker exec` into it is not possible by design. Licence notices for this image's exact dependency set ship at `/usr/share/doc/extenddb/SOFTWARE-LICENSE-NOTICES.html`.

## Limitations

- Single node, single process. Throughput settings are accepted and tracked but not enforced as capacity.
- No TLS and no access control, as above.

## Verifying the image

Signed with the same ExtendDB release key as the production image, an ECDSA P-256 key held in AWS KMS. The public key is attached to each [GitHub release](https://github.com/ExtendDB/extenddb/releases) as `extenddb-signing.pub.pem`, and each release's notes carry the exact `cosign verify` invocation for that release.

## Links

- Project site: [extenddb.org](https://extenddb.org)
- Source, issues and discussions: [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb)
- [The `extenddb-dev` image](https://github.com/ExtendDB/extenddb/blob/main/docs/dev-image.md)
- [Getting started](https://github.com/ExtendDB/extenddb/blob/main/docs/getting-started.md)
- [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md)
- Production image: [`extenddb/extenddb-postgres`](https://hub.docker.com/r/extenddb/extenddb-postgres)
- Licensed under Apache-2.0
