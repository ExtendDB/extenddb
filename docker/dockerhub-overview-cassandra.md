# ExtendDB with Cassandra

ExtendDB is a DynamoDB-compatible API server backed by your Apache Cassandra
cluster. This image contains ExtendDB only; Cassandra is a separate service
that you operate.

**The Cassandra backend is experimental.** Validate it against your workload
before you rely on it; the setup guide below lists its known limitations.

## Quick start

The Cassandra backend requires Apache Cassandra 4.1 or newer with
`PasswordAuthenticator` enabled and a local datacenter named `datacenter1`.
Read the
[Cassandra setup guide](https://github.com/ExtendDB/extenddb/blob/main/docs/cassandra-setup.md)
first: it explains these requirements and the backend's known limitations.
Run `CassandraAuthorizer`, under which `init` grants the application role only
the permissions ExtendDB needs. Under `AllowAllAuthorizer`, the default, that
role can read, drop and alter everything in the cluster, so use such a cluster
for ExtendDB alone.

Pull the image:

```console
docker pull docker.io/extenddb/extenddb-cassandra:latest
```

Initialize the state volume against your cluster. `init` connects as a
Cassandra superuser, creates its keyspaces and an application role, and
writes the configuration. The contact point must be resolvable and reachable
from the container. Pass the passwords through the environment. Always set
`EXTENDDB_APP_PASSWORD` (ASCII letters, digits, space and
``-_.,!@#$%^&*()+=~` `` only): without it the application role gets a fixed,
publicly known password. Docker creates the named volume `extenddb-state`
owned by the image's unprivileged user; a host directory would have to be
owned by UID 10001 first.

```console
export EXTENDDB_CASSANDRA_PASSWORD='<superuser password>'
export EXTENDDB_APP_PASSWORD='<password for the application role>'
export EXTENDDB_ADMIN_PASSWORD='<first ExtendDB administrator password>'

docker run --rm \
  -v extenddb-state:/var/lib/extenddb \
  -e EXTENDDB_CASSANDRA_PASSWORD -e EXTENDDB_APP_PASSWORD \
  -e EXTENDDB_ADMIN_USER=admin -e EXTENDDB_ADMIN_PASSWORD \
  docker.io/extenddb/extenddb-cassandra:latest \
  init --backend cassandra \
  --config /var/lib/extenddb/extenddb.toml \
  --cassandra-contact-points cassandra.example.com:9042 \
  --cassandra-user <superuser> \
  --bind-addr 0.0.0.0 \
  --tls-san localhost \
  --tls-san 127.0.0.1
```

`init` creates its keyspaces with replication factor 1. On a multi-node
cluster, raise it afterwards as "Keyspace replication" in the setup guide
describes.

Start the server using the generated configuration:

```console
docker run --rm --name extenddb-cassandra \
  -p 127.0.0.1:18443:18443 \
  -v extenddb-state:/var/lib/extenddb \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  docker.io/extenddb/extenddb-cassandra:latest
```

The server listens on `https://127.0.0.1:18443`. Initialization creates a
self-signed certificate; clients need
`/var/lib/extenddb/.extenddb/tls/cert.pem` from the volume. Run one serving
container per catalog.

## Configuration and security

The ExtendDB container runs as an unprivileged user and is designed to run
with a read-only root filesystem. The serving container needs only the
generated configuration and TLS files, not the Cassandra superuser password.

ExtendDB connects to Cassandra over plain TCP; the driver is built without
TLS. Run ExtendDB and Cassandra on a private network.

For a complete deployment guide, upgrades, and an optional local reference
stack, see
[`docker/README-cassandra.md`](https://github.com/ExtendDB/extenddb/blob/main/docker/README-cassandra.md).

## Tags and verification

Pin `X.Y.Z` or a digest for production; a version tag is never overwritten.
`latest` tracks the highest release and only moves forward. Tags of the form
`sha-<commit>` are unpromoted build candidates, not releases. Both
`linux/amd64` and `linux/arm64` ship as one multi-architecture index.

Every published image is signed with ExtendDB's release key, an elliptic-curve
P-256 key held in the AWS Key Management Service. The public key is committed
in the repository as
[`extenddb-signing.pub.pem`](https://github.com/ExtendDB/extenddb/blob/main/extenddb-signing.pub.pem).
Verify an image before deploying it:

```console
cosign verify --key extenddb-signing.pub.pem \
  docker.io/extenddb/extenddb-cassandra:X.Y.Z
```

The same images and their signatures are mirrored by digest to
`ghcr.io/extenddb/extenddb-cassandra` and
`public.ecr.aws/extenddb/extenddb-cassandra`.

## Note

ExtendDB is an open source project maintained by AWS. It is not Amazon
DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a
trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that
speaks the DynamoDB wire protocol; behavioral differences from the service are
documented in
[Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

More at [extenddb.org](https://extenddb.org) and
[github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb). Licensed
under Apache-2.0. Apache Cassandra, Cassandra and Apache are trademarks of
The Apache Software Foundation.
