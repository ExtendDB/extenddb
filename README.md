# ExtendDB

> **ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code.** "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol. Behavioral differences from the real service are documented in [Differences from DynamoDB](docs/differences-from-dynamodb.md).

A DynamoDB-compatible API adapter, ExtendDB speaks the DynamoDB wire protocol — any AWS SDK, CLI, or tool that works with DynamoDB works with ExtendDB, unchanged.

## Use Cases

- **Local development** — run DynamoDB workloads on your laptop with zero cloud dependency
- **CI/CD pipelines** — deterministic integration tests against a DynamoDB-compatible backend
- **Self-hosted deployments** — run DynamoDB workloads on your own infrastructure (on-premises, private cloud, edge)
- **Multi-cloud** — use DynamoDB semantics on any cloud that runs PostgreSQL
- **Air-gapped environments** — DynamoDB functionality with no internet connectivity

## Features

- Full DynamoDB wire protocol: CRUD, Query, Scan, Batch, Transactions, Streams, TTL, Import/Export
- SigV4 authentication with local IAM: users, groups, roles, policies, permissions boundaries
- Web management console for account and credential administration
- TLS with automatic self-signed certificate generation (replaceable with CA-signed certs)
- CSRF protection, security headers, session management
- Prometheus-compatible metrics endpoint
- Daemon mode with syslog logging
- PostgreSQL storage — use standard backup, replication, and HA tools

## Quick Start

```bash
# Build
cargo build --release

# Initialize (creates databases, admin credentials, TLS cert, config file)
./target/release/extenddb init

# Start
./target/release/extenddb serve --config extenddb.toml

# Use with any AWS SDK (TLS with self-signed cert — trust via AWS_CA_BUNDLE)
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables --endpoint-url https://127.0.0.1:8000 --region us-east-1
```

See [Getting Started](docs/getting-started.md) for the full walkthrough, or use the platform installer scripts:

```bash
scripts/install-linux.sh   # Linux
scripts/install-macos.sh   # macOS
```

## Prerequisites

- Rust 1.85+ (`rustup update`)
- PostgreSQL 14+ (see `docs/local-postgres-setup.md`)
- Python 3.10+ (for test suites and documentation)

### Python Environment

```bash
python3 -m venv ~/venvs/extenddb-venv
source ~/venvs/extenddb-venv/bin/activate
pip install -r requirements.txt
```

## Authentication Modes

ExtendDB ships with builtin IAM-like authentication enabled by default. All requests must be signed with valid SigV4 credentials created via the management API.

| Mode | Config | Description |
|------|--------|-------------|
| Builtin IAM-like | `auth.provider = "builtin"` | Full SigV4 signature verification with local credential store and IAM policy evaluation. This is the default and only supported mode. |

## Configuration

`extenddb init` generates `extenddb.toml` automatically. See `extenddb.sample.toml` for all keys, defaults, and descriptions.

Environment variable overrides use the `EXTENDDB__` prefix:

```bash
export EXTENDDB__SERVER__PORT=9000
export EXTENDDB__AUTH__PROVIDER=builtin
```

Runtime settings (no restart required):

```bash
extenddb settings --config extenddb.toml set log_level debug
```

## TLS

TLS is mandatory. `extenddb init` generates a self-signed certificate at `~/.extenddb/tls/cert.pem`. The server refuses to start with TLS disabled.

To use the self-signed cert with AWS CLI and SDKs, set `AWS_CA_BUNDLE`:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

Replace with a CA-signed certificate for production:

```toml
[server.tls]
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"
```

## Monitoring

```bash
# Health check
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health

# Prometheus metrics
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/metrics

# Syslog (Linux)
journalctl -t extenddb -f

# Syslog (macOS)
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info
```

## Management Console

Web-based administration at `https://127.0.0.1:8000/console/`. Manage accounts, users, groups, roles, policies, and access keys through a browser. Accept the self-signed certificate warning on first visit.

## CLI Reference

```
extenddb serve --config extenddb.toml          # Start server (daemon)
extenddb init --catalog-db NAME            # Initialize deployment
extenddb stop --config extenddb.toml           # Graceful shutdown
extenddb status --config extenddb.toml         # Check if running
extenddb verify --config extenddb.toml         # Validate deployment
extenddb migrate --config extenddb.toml        # Apply schema migrations
extenddb destroy --config extenddb.toml        # Tear down deployment
extenddb settings --config extenddb.toml list  # List runtime settings
extenddb manage --user admin --password <pw> <subcommand>  # IAM management
```

See [Admin Guide](docs/manuals/05-admin-guide.md) for the full CLI and management API reference.

## Supported Operations

### Table Operations
CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, DescribeEndpoints, DescribeLimits

### Item Operations
PutItem, GetItem, DeleteItem, UpdateItem (SET, REMOVE, ADD, DELETE actions; condition expressions; all ReturnValues modes)

### Query & Scan
Query, Scan (key conditions, filters, projections, pagination, index selection)

### Batch & Transactions
BatchGetItem (100 keys), BatchWriteItem (25 ops), TransactGetItems (100 items), TransactWriteItems (100 ops)

### Streams
ListStreams, DescribeStream, GetShardIterator, GetRecords

### Other
UpdateTimeToLive, DescribeTimeToLive, TagResource, UntagResource, ListTagsOfResource, ImportTable, ExportTableToPointInTime

## Project Structure

```
crates/
  core/             — types, validation, expressions (pure sync Rust, no async)
  engine/           — operation handlers
  storage/          — storage trait definitions
  storage-postgres/ — PostgreSQL backend
  auth/             — SigV4 verification, IAM policy engine
  server/           — HTTP server, management API, web console
  bin/              — CLI, config, daemon lifecycle
docs/
  design/           — architecture and design documents
  manuals/          — user-facing guides (PDF pipeline)
  adr/              — architecture decision records
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Full setup walkthrough |
| [Architecture Guide](docs/manuals/01-architecture-guide.md) | System design and crate structure |
| [Admin Guide](docs/manuals/05-admin-guide.md) | Server lifecycle, configuration, IAM management |
| [Security Model](docs/manuals/10-security-model.md) | Threat model, authentication, authorization |
| [Deployment Guide](docs/manuals/11-deployment-guide.md) | Self-hosted, multi-cloud, air-gapped deployments |
| [Differences from DynamoDB](docs/differences-from-dynamodb.md) | Behavioral differences and unsupported operations |
| [Troubleshooting](docs/troubleshooting.md) | Common errors and solutions |

Build PDFs:

```bash
python3 docs/build-docs.py
```

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. 
