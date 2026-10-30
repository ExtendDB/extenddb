# ExtendDB with MongoDB

ExtendDB is a DynamoDB-compatible API server backed by your MongoDB deployment.
This image contains ExtendDB only; MongoDB is a separate service that you
choose and operate, including MongoDB Atlas or a self-managed replica set.

## Quick start

The MongoDB backend requires MongoDB 7.0 or newer configured as a replica set.
Transactions and change streams are used by the backend, so a standalone
MongoDB server is not sufficient.

Pull the image:

```console
docker pull docker.io/extenddb/extenddb-mongodb:latest
```

Create a writable configuration directory and initialize it against your
MongoDB deployment:

```console
mkdir -p ./extenddb-data
cat > ./extenddb-data/extenddb.toml <<'EOF'
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://mongodb.example.com:27017/?replicaSet=rs0"
EOF

docker run --rm \
  -v "$PWD/extenddb-data:/var/lib/extenddb" \
  docker.io/extenddb/extenddb-mongodb:latest \
  init --backend mongodb \
  --config /var/lib/extenddb/extenddb.toml \
  --overwrite \
  --bind-addr 0.0.0.0 \
  --tls-san localhost \
  --tls-san 127.0.0.1
```

Start the server using the generated configuration:

```console
docker run --rm --name extenddb-mongodb \
  -p 18443:18443 \
  -v "$PWD/extenddb-data:/var/lib/extenddb" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  docker.io/extenddb/extenddb-mongodb:latest \
  serve --config /var/lib/extenddb/extenddb.toml --foreground
```

The server listens on `https://127.0.0.1:18443` by default. Initialization
creates a self-signed certificate and the first administrator credentials;
save the password printed by `init`.

## Configuration and security

Keep the MongoDB connection string in the generated configuration or provide
it through the configuration mechanisms documented in the repository. Use
MongoDB authentication and TLS for production deployments. The ExtendDB
container runs as an unprivileged user and is designed to run with a
read-only root filesystem.

For a complete deployment guide and an optional local reference stack, see
[`docker/README-mongodb.md`](https://github.com/ExtendDB/extenddb/blob/main/docker/README-mongodb.md).

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
  docker.io/extenddb/extenddb-mongodb:X.Y.Z
```

The same images and their signatures are mirrored by digest to
`ghcr.io/extenddb/extenddb-mongodb` and
`public.ecr.aws/extenddb/extenddb-mongodb`.

## Note

ExtendDB is an independent open source project managed by Amazon Web
Services. It is not Amazon DynamoDB and does not contain any DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room
implementation that speaks the DynamoDB wire protocol; behavioral differences
from the service are documented in
[Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

More at [extenddb.org](https://extenddb.org) and
[github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb). Licensed
under Apache-2.0.
