# Local MongoDB Setup

## Prerequisites

- MongoDB 6.0+ (for multi-document transactions)
- A replica set configuration (required even for single-node deployments)

## Installation

### macOS (Homebrew)

```bash
brew tap mongodb/brew
brew install mongodb-community@7.0
```

### Linux (Ubuntu/Debian)

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
    https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org
```

### Docker (recommended for development)

```bash
docker run -d --name extenddb-mongo \
    -p 27017:27017 \
    mongo:7 --replSet rs0
```

## Replica Set Initialization

MongoDB must run as a replica set for transactions and Change Streams.

### Single-node replica set (development)

```bash
# If using Docker:
docker exec extenddb-mongo mongosh --quiet --eval "rs.initiate()"

# If using a local install:
mongosh --eval "rs.initiate()"
```

Wait a few seconds for the replica set to elect a primary, then verify:

```bash
mongosh --eval "rs.status().ok"
# Should output: 1
```

### Homebrew (macOS) with replica set

Edit the MongoDB config to add replica set:

```bash
# Find the config file
brew --prefix mongodb-community@7.0
# Usually: /opt/homebrew/etc/mongod.conf
```

Add to `mongod.conf`:
```yaml
replication:
  replSetName: rs0
```

Restart and initiate:
```bash
brew services restart mongodb-community@7.0
mongosh --eval "rs.initiate()"
```

## Connection Details

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `27017` |
| Replica set | `rs0` |
| Connection string | `mongodb://localhost:27017/?replicaSet=rs0` |

For Docker on a non-default port:
```
mongodb://localhost:27018/?replicaSet=rs0&directConnection=true
```

## Building with MongoDB Support

The MongoDB backend is behind a feature flag:

```bash
cargo build --release --features mongodb
```

To build with both backends:
```bash
cargo build --release --features postgres,mongodb
```

## Initializing ExtendDB with MongoDB

```bash
./target/release/extenddb init --backend mongodb --config extenddb.toml
```

This creates:
- `extenddb_catalog` database (table metadata, IAM, settings)
- `extenddb_data` database (per-table item collections)
- Admin user credentials (printed to stdout)
- Self-signed TLS certificate at `~/.extenddb/tls/cert.pem`
- Config file `extenddb.toml`

The generated config will contain:
```toml
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
```

## Starting the Server

```bash
./target/release/extenddb serve --config extenddb.toml
```

## Config Mapping

```toml
[storage]
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
# max_pool_size = 20
```

Or via environment variable:
```bash
export EXTENDDB__STORAGE__MONGODB__CONNECTION_STRING="mongodb://localhost:27017/?replicaSet=rs0"
```

## Verifying the Connection

```bash
# Health check
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health

# List tables (should return empty)
aws dynamodb list-tables \
    --endpoint-url https://127.0.0.1:8000 \
    --region us-east-1
```

## Differences from PostgreSQL Backend

- **Replica set required:** Even single-node MongoDB must be configured as a replica set.
- **DynamoDB Streams:** Inline record writes with atomic sequence numbers.
- **GSI propagation:** Synchronous inline updates during data operations.
- **Concurrency model:** Optimistic versioning with retry on conflict (vs row-level locking in PostgreSQL).

## Stopping

```bash
# ExtendDB
./target/release/extenddb stop --config extenddb.toml

# Docker MongoDB
docker stop extenddb-mongo

# Homebrew MongoDB
brew services stop mongodb-community@7.0
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
