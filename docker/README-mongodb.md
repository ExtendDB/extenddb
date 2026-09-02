# ExtendDB MongoDB Container

This directory documents the registry-neutral ExtendDB MongoDB container
image. The image contains the ExtendDB executable compiled with the `mongodb`
backend. It does **not** contain MongoDB itself.

Provide MongoDB 7.0 or newer separately through MongoDB Atlas, a managed
MongoDB service, or a self-managed replica set. Transactions and change
streams require a replica set, including for a single-node deployment.

## Image contents and security defaults

The image:

* runs as UID/GID `10001:10001`;
* supports a read-only root filesystem;
* uses `tini` to forward SIGTERM;
* publishes only port 18443;
* stores configuration and TLS state under `/var/lib/extenddb`;
* uses `extenddb healthcheck` for liveness;
* contains no MongoDB server or MongoDB data directory.

The healthcheck verifies that ExtendDB is alive. It does not continuously
verify MongoDB availability after startup; use the orchestrator's readiness
and database monitoring for that.

## Build locally

Generate the notices for the MongoDB feature set, then build the image:

```bash
devtools/generate-software-license-notices --backend mongodb
docker build \
  --file Dockerfile.mongodb \
  --build-arg VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')" \
  --build-arg VCS_REF="$(git rev-parse --short=12 HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t extenddb-mongodb:dev .
```

## Connect to an existing MongoDB deployment

The customer supplies a connection string pointing at a replica set. The
MongoDB hostname must be resolvable from the ExtendDB container:

```text
mongodb://user:password@mongo.example.internal:27017/?replicaSet=rs0
```

Run `extenddb init` once using a writable state volume. The initial config
must contain the MongoDB connection string so the initializer can connect:

```toml
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://user:password@mongo.example.internal:27017/?replicaSet=rs0"
```

Then initialize and serve with the same image:

```bash
docker run --rm \
  -v "$PWD/extenddb-state:/var/lib/extenddb" \
  -e EXTENDDB_ADMIN_USER=admin \
  -e EXTENDDB_ADMIN_PASSWORD='replace-this-password' \
  extenddb/extenddb-mongodb:0.1.8 \
  init --backend mongodb \
  --config /var/lib/extenddb/extenddb.toml \
  --overwrite --bind-addr 0.0.0.0 --tls-san localhost

docker run -d --name extenddb-mongodb \
  -p 127.0.0.1:18443:18443 \
  -v "$PWD/extenddb-state:/var/lib/extenddb" \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --cap-drop ALL --security-opt no-new-privileges:true \
  extenddb/extenddb-mongodb:0.1.8
```

For production, inject credentials through the orchestrator's secret store,
pin a version or digest, and run initialization as a one-time Job rather than
as part of every serving replica.

## Local reference stack

For users who want MongoDB and ExtendDB launched together locally, the
repository provides an optional reference stack:

```bash
export EXTENDDB_VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')"
export VCS_REF="$(git rev-parse --short=12 HEAD)"
export BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose -f docker-compose.mongodb.yml up -d --build
```

It starts MongoDB as a single-node `rs0` replica set, initializes ExtendDB,
and persists both services in named volumes. This is a development convenience
and does not restrict production users to the bundled MongoDB version.

## Automated smoke test

The smoke test builds the image, starts the optional reference stack, verifies
replica-set readiness and image hardening, performs a DynamoDB API round trip,
and verifies persistence after restarting ExtendDB:

```bash
ci/smoke-test-mongodb-container.sh
```

To test an already-built image without rebuilding it:

```bash
EXTENDDB_MONGODB_IMAGE=extenddb-mongodb:dev \
  ci/smoke-test-mongodb-container.sh
```
