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
cat > ./extenddb-data/bootstrap.toml <<'EOF'
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
  --tls-san localhost
```

Start the server using the generated configuration:

```console
docker run --rm --name extenddb-mongodb \
  -p 18443:18443 \
  -v "$PWD/extenddb-data:/var/lib/extenddb:ro" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  docker.io/extenddb/extenddb-mongodb:latest \
  serve --config /var/lib/extenddb/extenddb.toml
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

## Versioning

The image tag is the ExtendDB release version. The MongoDB server version is
independent and is selected in your MongoDB deployment. Pin an ExtendDB
version in production instead of relying on `latest`.

ExtendDB is licensed under the Apache License 2.0. The image includes the
third-party license notices required by its dependencies.
