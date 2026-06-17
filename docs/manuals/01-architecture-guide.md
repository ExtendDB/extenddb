# Architecture Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Overview

extenddb (ExtendDB) is a standalone DynamoDB-compatible API server written in Rust. It receives DynamoDB wire protocol requests over HTTP/HTTPS, authenticates and authorizes them via SigV4 and a local IAM policy engine, executes operation logic in a backend-agnostic engine, and delegates persistence to a pluggable storage backend (currently PostgreSQL).

extenddb runs as a daemon process, logging to syslog. It is designed for any environment where DynamoDB semantics are needed — local development, CI pipelines, self-hosted production, multi-cloud, or air-gapped deployments. Developers and applications point their AWS SDKs at extenddb and get identical DynamoDB behavior.

## Cargo Workspace

The project is structured as a Cargo workspace with 7 crates. Crate boundaries enforce dependency rules at compile time.

```
extenddb/
├── crates/
│   ├── core/              Pure sync Rust: types, expressions, validation, errors
│   ├── engine/            Async operation handlers (PutItem, Query, etc.)
│   ├── storage/           Storage trait definitions and backend-agnostic utilities
│   ├── storage-postgres/  PostgreSQL backend implementation
│   ├── auth/              AuthProvider trait, SigV4 verification, IAM policy engine
│   ├── server/            HTTP server (axum), management API, web console
│   └── bin/               CLI entry point, config loading, daemon lifecycle
```

## Crate Dependency Graph

```
bin ──→ server ──→ engine ──→ core
 │        │          │
 │        │          └──→ storage ──→ core
 │        │
 │        ├──→ auth ──→ core
 │        └──→ storage
 │
 ├──→ storage-postgres ──→ storage ──→ core
 └──→ core
```

Key principle: `core` depends on nothing in the workspace and has no async runtime. It contains pure sync Rust — types, expression parsing and evaluation, input validation, capacity calculation, and error types. Every other crate depends on `core`.

## Crate Roles

### core

Pure sync Rust library. No async runtime, no database drivers, no HTTP framework.

- **types**: `AttributeValue`, `KeySchema`, `TableMetadata`, `StreamRecord`, and all DynamoDB data types
- **expression**: Parser and evaluator for `ConditionExpression`, `FilterExpression`, `UpdateExpression`, `ProjectionExpression`, and `KeyConditionExpression`
- **validation**: Input validation (table names, item sizes, attribute types, key schema rules)
- **error**: `DynamoDbError` enum mapping every DynamoDB error code to its HTTP status and message
- **limits**: Configurable limits matching real DynamoDB defaults (item size, batch sizes, expression depth/tokens)
- **capacity**: RCU/WCU calculation logic

### engine

Async operation handlers. Each DynamoDB operation (PutItem, GetItem, Query, Scan, etc.) has a dedicated handler module. The engine:

- Validates input using `core`
- Parses expressions using `core` (with configurable depth and token limits)
- Calls storage traits to read/write data
- Applies filter and projection expressions after reads
- Calculates consumed capacity
- Formats wire-protocol JSON responses

The `dispatch` function routes `X-Amz-Target` operation names to handlers.

### storage

Trait definitions for the storage layer. Thirteen storage traits partition backend responsibilities:

- **TableEngine**: Table lifecycle (create, delete, describe, list, update)
- **DataEngine**: Item CRUD (put, get, update, delete, query, scan, batch, transact)
- **MetadataEngine**: Settings, TTL configuration, tagging
- **StreamEngine**: Stream record persistence and retrieval
- **WorkerStore**: Background worker coordination
- **BackupEngine**: Backup and restore operations
- **ManagementStore**: IAM and account management
- **AdminStore**: Admin user and credential management
- **SettingsStore**: Runtime settings persistence
- **MetricsStore**: Metrics collection and retrieval
- **RateLimitStore**: Rate limiting state
- **AuthorizationStore**: Policy evaluation cache
- **Bootstrapper**: Initial database setup

Traits use `BoxFuture` for object safety. Backends register at compile time via the `inventory` crate and are selected at startup by name. The `RuntimeHooks` trait allows backends to spawn backend-specific workers (PostgreSQL spawns 7).

### storage-postgres

PostgreSQL implementation of all storage traits using `sqlx`. Features:

- Dual-database architecture: catalog DB (metadata) + data DB (user items)
- Schema migrations managed by version-stamped SQL files
- Items stored as JSONB with indexed key columns
- GSI/LSI implemented as separate PostgreSQL tables
- Transactions use `SELECT FOR UPDATE` + single-transaction commits
- Stream records stored in a dedicated table with background cleanup
- All queries parameterized (no dynamic SQL construction)

### auth

Authentication and authorization:

- **BuiltinAuthProvider**: Full SigV4 signature verification with local IAM (`auth.provider = "builtin"`, the only supported mode)

The IAM policy engine evaluates identity-based policies, group policies, role policies, session policies, and permissions boundaries. Policy evaluation follows the same logic as real AWS IAM. Unparseable stored policies fail closed (access denied).

### server

HTTP/HTTPS server built on axum + tower. Responsibilities:

- DynamoDB wire protocol endpoint (`POST /`)
- Management REST API (`/management/*`)
- Web console (`/console/*`) with CSRF protection and security headers
- Health check (`/health`) and JSON metrics (`/metrics`)
- TLS via rustls (self-signed or CA-signed certificates)
- Request ID generation, CRC32 checksums, content-type headers
- Graceful shutdown on SIGTERM/SIGINT

### bin

Thin binary that wires everything together:

- CLI parsing (clap): `serve`, `init`, `destroy`, `verify`, `migrate`, `status`, `settings`, `manage`, `version`
- Configuration loading (TOML + env vars)
- Daemon lifecycle (bind socket → fork → syslog → serve)
- Background tasks (log level polling, throttling polling, GSI delay polling, stream record cleanup, TTL expiry, metrics persistence)

## Request Lifecycle

1. AWS SDK sends `POST /` with `X-Amz-Target` header and JSON body
2. axum extracts headers and body
3. `X-Amz-Target` is parsed to determine the operation (e.g., `DynamoDB_20120810.PutItem`)
4. Authentication: SigV4 signature verified against the local credential store
5. Authorization: IAM policies evaluated against the operation and resource ARN
6. Operation handler in `engine` validates input, parses expressions
7. Storage trait methods called to read/write data
8. Response formatted with correct DynamoDB JSON structure
9. HTTP response includes `x-amzn-RequestId`, `x-amz-crc32`, and `Content-Type` headers

## Daemon Lifecycle

extenddb always runs as a daemon. There is no foreground mode.

1. Parse CLI arguments and load configuration
2. Bind TCP socket (port conflicts reported before forking)
3. Fork to background via `daemonize`
4. Initialize syslog logging
5. Connect to PostgreSQL (catalog + data databases)
6. Verify catalog version matches binary expectation
7. Start axum server on the pre-bound socket
8. Spawn background tasks (log level polling, throttling polling, GSI delay polling, stream cleanup, TTL expiry, metrics persistence)
9. On SIGTERM/SIGINT: drain connections (5s timeout), exit

## Catalog Model

extenddb uses a dual-database architecture:

- **Catalog database** (e.g., `extenddb_catalog`): Stores table metadata, account/user/group/role/policy definitions, access keys, settings, stream metadata, and metrics. Shared across all accounts.
- **Data database** (e.g., `extenddb_catalog_data`): Stores user items, GSI/LSI data, and stream records. Each table gets its own PostgreSQL table.

The catalog version (currently 0.0.2) is stored in the `catalog_metadata` table and checked at startup. Version mismatches prevent the server from starting — run `extenddb migrate` to upgrade.

## Pluggable Architecture

### Storage

Storage backends implement thirteen traits (see **storage** section above). The traits use `BoxFuture` for object safety. Backends register at compile time via the `inventory` crate, and the `bin` crate selects the backend by name at startup. Currently only PostgreSQL is implemented.

### Authentication

Auth providers implement the `AuthProvider` trait using `#[async_trait]` for object safety. The provider is selected at startup and stored as `Arc<dyn AuthProvider>`. The `BuiltinAuthProvider` uses a `CredentialStore` trait (also `#[async_trait]`) for credential lookup, with a database-backed implementation.

## Security Architecture

### Authentication

SigV4 signature verification follows the AWS specification. Credentials are stored encrypted (AES-256-GCM) in PostgreSQL. Access key prefixes distinguish long-term (`VDAK`) from temporary (`VDSK`) credentials.

### Authorization

The IAM policy engine evaluates:
- Identity-based policies (user, group)
- Role policies and session policies
- Permissions boundaries
- Explicit Deny takes precedence over Allow

Unparseable policies fail closed — a corrupted Deny policy results in access denied, not silent skip.

### Transport

TLS is supported via rustls. `extenddb init` generates a self-signed certificate; production deployments should use CA-signed certificates. When TLS is enabled, HSTS headers are sent automatically.

### Web Console Security

- CSRF tokens on all state-changing POST handlers
- HttpOnly session cookies
- Security headers: X-Content-Type-Options, X-Frame-Options, Referrer-Policy
- 8-hour session expiry

### Input Validation

All user-supplied strings are validated at the engine layer before reaching storage. Expression parsing enforces configurable depth and token limits. Policy documents are size-capped before JSON parsing. The storage layer uses parameterized queries exclusively.

## Web Console

The management web console is a server-rendered HTML interface mounted at `/console/*`. It runs inside the `server` crate — no separate process, no external dependencies. All HTML is generated with Rust string formatting.

### Features

- **Login**: Admin users and IAM users (`account_id/user_name`)
- **Dashboard**: Account and admin user counts, version info, quick links
- **Account management**: Create, view, delete accounts
- **User management**: Create, delete users; view access keys, policies, tags, group memberships
- **Access key management**: Create and delete access keys (secret shown once at creation)
- **Group management**: Create, delete groups; add/remove members
- **Role management**: Create, delete roles; view trust policies
- **Policy management**: Add, delete inline policies for users, groups, and roles (JSON editor with template)
- **Metrics dashboard**: Real-time and historical operation metrics with interactive charts
- **Settings viewer**: Read-only display of runtime settings and static configuration (admin only)
- **Documentation browser**: All project manuals rendered as HTML, with PDF download (`/console/docs`)

### Architecture

The console uses the `ManagementStore` trait for all data access — it never queries the database directly. Write operations route through shared functions in `management::ops`, ensuring validation and business logic are defined once. This means every action available in the console is also available through the management REST API.

### Authentication Model

- Session-based authentication with 8-hour expiry
- Sessions stored in-memory with random tokens in HttpOnly cookies (`SameSite=Strict`, `Path=/console`)
- CSRF tokens on all state-changing POST handlers
- Security headers: X-Content-Type-Options (nosniff), X-Frame-Options (DENY), Referrer-Policy (strict-origin-when-cross-origin)
- Login rate limiting with account lockout on excessive failures

### Unauthenticated Routes

The documentation browser at `/console/docs` is accessible without login. All other console routes require authentication.

## Configuration

Two configuration surfaces:

- **`extenddb.toml`**: Static configuration requiring a restart (bind address, port, database connection, auth provider, TLS, log format)
- **Settings table**: Runtime configuration via `extenddb settings set` (log level, control plane delay, credential import toggle). A background poller picks up changes every 30 seconds.

Configuration precedence: CLI flags > environment variables > config file > defaults.

## DynamoDB Streams

extenddb implements DynamoDB Streams for change data capture. Stream records are captured atomically with data writes inside the same PostgreSQL transaction. Both the DynamoDB API and Streams API are served on the same port.

Supported operations: `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords`.

Stream records are retained for 24 hours. A background worker cleans up expired records hourly.

## Deployment Models

extenddb is a single-binary server that connects to PostgreSQL. Deployment options include:

- **Single-node**: extenddb + PostgreSQL on the same host (development, small workloads)
- **Separated**: extenddb on an application server, PostgreSQL on a dedicated database server or managed service (RDS, Aurora, Cloud SQL)
- **Containerized**: Docker/Kubernetes with PostgreSQL as a sidecar or external service
- **Air-gapped**: No internet connectivity required; all functionality is self-contained

PostgreSQL provides the durability, replication, and backup capabilities. Use standard PostgreSQL HA tools (streaming replication, Patroni, managed services) for production availability.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
