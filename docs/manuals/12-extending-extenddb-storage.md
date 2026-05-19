# Extending extenddb Storage

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Introduction

extenddb uses a fully trait-based storage abstraction. The default backend is PostgreSQL, implemented in the `storage-postgres` crate. This document explains the storage architecture, lists every trait a new backend must implement, and provides guidance for adding a new storage backend (e.g., Cassandra, SQLite, FoundationDB).

As of v0.0.81, the server crate has **no direct PostgreSQL dependencies**. All database access goes through traits defined in the `storage` and `auth` crates. PostgreSQL-specific code lives exclusively in `storage-postgres` and the `bin` crate's wiring layer.

## Architecture Overview

extenddb is organized as a Cargo workspace with seven crates:

```
bin              → CLI entry point (init, serve, stop, migrate, manage, etc.)
server           → HTTP layer (axum), console, management API (no database dependency)
engine           → DynamoDB operation logic (pure business rules, no DB access)
core             → Types, expressions, validation (sync, no async runtime)
auth             → SigV4 verification, policy evaluation (trait-based credential store)
storage          → Trait definitions and backend-agnostic utilities (ARN construction, key parsing)
storage-postgres → PostgreSQL implementation of all storage traits
```

The key architectural principle: neither the `engine` nor the `server` crate touches any database directly. They receive trait objects and call their methods. The `storage` crate defines these traits with no database dependencies, and provides backend-agnostic utilities in `storage::util` (ARN construction, partition/sort key parsing, netstring encoding) that any backend can reuse. The `storage-postgres` crate implements the traits. The `bin` crate is the wiring layer that creates concrete PostgreSQL stores and passes them to the server.

## Trait Overview

A new backend must implement **13 storage traits** plus the `CredentialStore` trait from the `auth` crate:

**DynamoDB data path** (defined in `crates/storage/src/lib.rs`):
1. `TableEngine` — table lifecycle
2. `DataEngine` — item CRUD, query, scan, transactions
3. `MetadataEngine` — TTL, tags, table statistics
4. `StreamEngine` — DynamoDB Streams
5. `WorkerStore` — background worker operations (GSI propagation, TTL cleanup)

**Management and operational** (defined in `crates/storage/src/`):
6. `ManagementStore` — IAM CRUD (users, groups, roles, policies, access keys, accounts)
7. `AdminStore` — admin user management
8. `SettingsStore` — runtime settings
9. `MetricsStore` — historical metrics persistence and query
10. `RateLimitStore` — login rate limiting and account lockout
11. `AuthorizationStore` — policy lookups for authorization decisions
12. `BackupEngine` — backup and restore operations
13. `Bootstrapper` — database initialization, destruction, migration, verification

**Additionally**, the `auth` crate defines:
14. `CredentialStore` — access key and session credential lookup for SigV4 verification

Backends register at compile time using the `inventory` crate and are selected by name at startup. The `RuntimeHooks` trait allows backends to spawn backend-specific workers (PostgreSQL spawns 7).

## DynamoDB Data Path Traits

These are defined in `crates/storage/src/lib.rs`.

### TableEngine

Table lifecycle operations:

| Method | Purpose |
|--------|---------|
| `create_table` | Create a table with key schema, attribute definitions, optional GSIs/LSIs |
| `delete_table` | Delete a table and all its data |
| `describe_table` | Return full table metadata (status, key schema, indexes, size, item count) |
| `list_tables` | Paginated list of table names for an account |
| `update_table` | Modify billing mode, throughput, deletion protection |
| `table_key_info` | Lightweight metadata fetch (key schema, attribute definitions) for data ops |
| `index_info` | Fetch metadata for a specific secondary index |

Key design decisions:
- Tables have a lifecycle: CREATING → ACTIVE → DELETING → (gone). The `control_plane_delay_seconds` setting controls how long tables stay in CREATING before becoming ACTIVE.
- Tables are scoped by `account_id`. Multi-tenancy is a first-class concern.
- GSI creation can be asynchronous (CREATING → ACTIVE) with a configurable propagation delay.

### DataEngine

Item CRUD, query, scan, and transaction operations:

| Method | Purpose |
|--------|---------|
| `put_item` | Write/replace an item, with optional condition expression and stream capture |
| `get_item` | Read a single item by primary key |
| `delete_item` | Delete an item by primary key, with optional condition and stream capture |
| `update_item` | Upsert with update expressions (SET, REMOVE, ADD, DELETE) |
| `query` | Query by partition key with optional sort key condition, pagination, index routing |
| `scan` | Full table/index scan with pagination and parallel scan segments |
| `transact_get_items` | Multi-item consistent read (serializable isolation) |
| `transact_write_items` | Multi-item atomic write with conditions and idempotency tokens |
| `cleanup_expired_idempotency_tokens` | Garbage-collect old idempotency tokens |

Key design decisions:
- **Condition expressions** are evaluated inside the storage transaction. The engine parses and compiles expressions; the storage layer receives an AST (`Expr`) and evaluates it against the existing item within the same transaction that performs the write. This is critical for correctness — condition checks and writes must be atomic.
- **Stream capture** is passed as `Option<&StreamCapture>`. When present, the stream record must be written in the same transaction as the data write.
- **Idempotency tokens** for `TransactWriteItems` must be checked and stored atomically with the writes.
- **Items** are `BTreeMap<String, AttributeValue>`. A new backend must handle the full `AttributeValue` type (S, N, B, SS, NS, BS, L, M, BOOL, NULL).
- **Query** must support forward/reverse sort order, exclusive start key pagination, and routing to secondary index storage.
- **Parallel scan** uses `segment` and `total_segments` to partition the keyspace.

### MetadataEngine

TTL, tags, and table statistics:

| Method | Purpose |
|--------|---------|
| `describe_ttl` | Get TTL configuration for a table |
| `update_ttl` | Enable/disable TTL on a table attribute |
| `find_expired_items` | Find items with expired TTL attribute (for background deletion) |
| `tag_resource` | Add/overwrite tags on a resource ARN |
| `untag_resource` | Remove tags by key |
| `list_tags` | List all tags for a resource ARN |
| `tables_with_ttl` | List tables with TTL enabled (single account) |
| `all_tables_with_ttl` | List tables with TTL enabled (all accounts) |
| `refresh_table_size` | Recompute and store table size and item count |
| `list_active_table_names` | List active table names (single account) |
| `all_active_tables` | List active tables (all accounts) |

Key design decisions:
- TTL deletion is a background process. The engine calls `find_expired_items` periodically, then deletes each item via `DataEngine::delete_item` (which handles index sync and stream capture).
- Tags are stored by ARN string.
- Table size refresh is a background operation that counts rows and sums sizes.

### StreamEngine

DynamoDB Streams support:

| Method | Purpose |
|--------|---------|
| `write_stream_record` | Write a stream record (called within data write transaction) |
| `get_stream_records` | Read records from a shard after a sequence number |
| `describe_stream` | Describe a stream (shards, status, view type) |
| `list_streams` | List streams, optionally filtered by table |
| `cleanup_expired_stream_records` | Delete records older than retention period |
| `assign_shard` | Hash-assign a partition key to a shard |
| `next_sequence_number` | Generate the next sequence number for a shard |
| `validate_shard` | Verify a shard exists for a given stream ARN |
| `latest_sequence_number` | Get the latest sequence number in a shard |

Key design decisions:
- Stream records are written atomically with data writes (same transaction).
- Shards are hash-assigned based on partition key.
- Sequence numbers must be monotonically increasing within a shard.
- The retention period is configurable (default 24 hours).

### WorkerStore

Background worker operations:

| Method | Purpose |
|--------|---------|
| `activate_pending_tables` | Transition tables from CREATING to ACTIVE after delay |
| `activate_pending_gsis` | Transition GSIs from CREATING to ACTIVE after delay |
| `process_gsi_queue` | Process queued GSI index writes |

## Management and Operational Traits

### ManagementStore

Defined in `crates/storage/src/management_store/mod.rs`. Covers all IAM CRUD operations:

| Method | Purpose |
|--------|---------|
| `create_account` / `delete_account` / `list_accounts` | Account lifecycle |
| `create_user` / `delete_user` / `list_users` / `get_user` | IAM user CRUD |
| `create_group` / `delete_group` / `list_groups` / `get_group` | IAM group CRUD |
| `create_role` / `delete_role` / `list_roles` / `get_role` | IAM role CRUD |
| `create_policy` / `delete_policy` / `list_policies` / `get_policy` | IAM policy CRUD |
| `create_access_key` / `delete_access_key` / `list_access_keys` | Access key management |
| `add_user_to_group` / `remove_user_from_group` / `list_group_members` | Group membership |
| `attach_user_policy` / `detach_user_policy` / `list_user_attached_policies` | User policy attachment |
| `attach_group_policy` / `detach_group_policy` / `list_group_attached_policies` | Group policy attachment |
| `attach_role_policy` / `detach_role_policy` / `list_role_attached_policies` | Role policy attachment |
| `set_permissions_boundary` / `delete_permissions_boundary` | Permissions boundaries |
| `create_session` / `get_session` | STS AssumeRole session management |
| `get_account_summary` | Account summary (user/group/role/policy counts) |

Key design decisions:
- Account deletion must cascade to all users, groups, roles, policies, and access keys atomically.
- Access key secrets are encrypted (AES-256-GCM) before storage.
- Sessions have expiration enforcement.

### AdminStore

Defined in `crates/storage/src/management_store/mod.rs`. Admin user management (separate from IAM users):

| Method | Purpose |
|--------|---------|
| `create_admin` | Create an admin user with password hash |
| `delete_admin` | Delete an admin user |
| `list_admins` | List all admin users |
| `verify_admin` | Verify admin credentials |
| `change_admin_password` | Update admin password hash |

### SettingsStore

Defined in `crates/storage/src/management_store/mod.rs`. Runtime settings that can change without restart:

| Method | Purpose |
|--------|---------|
| `get_setting` | Read a single setting value |
| `set_setting` | Write a setting value |
| `list_settings` | List all settings |

### MetricsStore

Defined in `crates/storage/src/management_store/mod.rs`. Historical metrics persistence:

| Method | Purpose |
|--------|---------|
| `record_metrics` | Insert a metrics snapshot |
| `query_metrics` | Query metrics by time range, operation, and table filters |

### RateLimitStore

Defined in `crates/storage/src/management_store/mod.rs`. Login rate limiting:

| Method | Purpose |
|--------|---------|
| `record_login_attempt` | Record a login attempt (success or failure) |
| `recent_failed_attempts` | Count recent failed attempts for lockout decisions |
| `cleanup_old_attempts` | Garbage-collect old login attempt records |

### AuthorizationStore

Defined in `crates/storage/src/authorization_store.rs`. Policy lookups for authorization:

| Method | Purpose |
|--------|---------|
| `get_user_policies` | Get all policies for a user (direct + group-inherited + role) |
| `get_permissions_boundary` | Get the permissions boundary for a user or role |

### BackupEngine

Defined in `crates/storage/src/lib.rs`. Backup and restore operations:

| Method | Purpose |
|--------|---------|
| `export_table` | Export table data to external storage |
| `import_table` | Import table data from external storage |

### Bootstrapper

Defined in `crates/storage/src/bootstrapper.rs`. Database lifecycle:

| Method | Purpose |
|--------|---------|
| `init` | Create databases, run migrations, create initial admin user |
| `destroy` | Drop databases |
| `migrate` | Apply pending migrations |
| `verify` | Check catalog version and migration status |
| `catalog_version` | Return the current catalog version |

### CredentialStore

Defined in `crates/auth/src/lib.rs`. Used by SigV4 verification:

```rust
#[async_trait]
pub trait CredentialStore: Send + Sync {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError>;
}
```

The PostgreSQL implementation (`DbCredentialStore` in `storage-postgres/credential_store.rs`) handles:
- Access key lookup with secret key decryption (AES-256-GCM)
- Session credential lookup with expiration enforcement
- Inactive key detection

## PostgreSQL Implementation

The `storage-postgres` crate provides the default implementation:

| Struct | Traits Implemented |
|--------|-------------------|
| `PostgresEngine` | `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore` |
| `PostgresCatalogStore` | `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore` |
| `PostgresBootstrapper` | `Bootstrapper` |
| `DbCredentialStore` | `CredentialStore` |

### Database Architecture

extenddb uses a dual-database architecture:
- **Catalog database** (`extenddb`) — table metadata, IAM data, settings, metrics, stream records, tags, idempotency tokens, login attempts
- **Data database** (`extenddb_data`) — DynamoDB items and index rows

### Migrations

Schema is managed via SQL migration files in `crates/storage-postgres/migrations/`:

| Migration | Content |
|-----------|---------|
| `001_initial_schema.sql` | Tables, items, indexes core schema |
| `002_streams.sql` | stream_shards, stream_records |
| `003_auth.sql` | Full IAM schema (accounts, users, groups, roles, policies, access keys, sessions) |
| `004_account_cascade.sql` | CASCADE constraints for account deletion |
| `005_idempotency_tokens.sql` | idempotency_tokens table |
| `006_gsi_propagation_delay.sql` | GSI async propagation support |
| `007_stream_sequence.sql` | Stream sequence number generation |
| `008_metrics.sql` | metrics_history table |
| `009_login_attempts.sql` | login_attempts table for rate limiting |

## Adding a New Backend

### Step 1: Create a New Crate

Create a new crate (e.g., `storage-cassandra`) in the workspace. It should depend on `extenddb-storage`, `extenddb-auth`, and `extenddb-core`.

### Step 2: Implement the DynamoDB Data Traits

Implement `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, and `WorkerStore`. Use the PostgreSQL implementation as a reference for expected behavior and edge cases.

### Step 3: Implement the Management Traits

Implement `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore`, and `AuthorizationStore`. These cover IAM, settings, metrics, and rate limiting.

### Step 4: Implement Bootstrapper

Implement `Bootstrapper` for database initialization, destruction, migration, and verification.

### Step 5: Implement CredentialStore

Implement the `CredentialStore` trait from the `auth` crate for SigV4 credential lookup.

### Step 6: Register Your Backend

Backends register at compile time using the `inventory` crate. Implement the `ServerComponentsFactory` trait and use the `inventory::submit!` macro to register your backend:

```rust
use extenddb_storage::{ServerComponentsFactory, ServerComponentsRegistration};

pub struct MyBackendFactory;

impl ServerComponentsFactory for MyBackendFactory {
    fn name(&self) -> &'static str {
        "mybackend"
    }
    
    fn create(&self, config: &Config) -> Result<ServerComponents, BackendError> {
        // Construct your backend's stores
        // Return ServerComponents with all trait implementations
    }
}

inventory::submit! {
    ServerComponentsRegistration::new(MyBackendFactory)
}
```

The `bin` crate will discover your backend at startup and select it by name from the configuration.

### Step 7: Test

Use the test suite as your specification:
- **Python integration tests** (`tests/`) exercise the DynamoDB wire protocol end-to-end. They are backend-agnostic.
- **External Java SDK test suite** (`run-external-tests`) runs real AWS SDK integration tests.
- If your backend passes the same tests, it is correct.

## Design Constraints for New Backends

### Transaction Isolation

DynamoDB's transactional guarantees are strict:
- `TransactWriteItems` requires ACID across multiple items and tables.
- `TransactGetItems` requires serializable isolation.
- Condition expressions must be evaluated atomically with writes.

If your backend cannot provide serializable isolation, document the limitations clearly.

### Atomic Stream Capture

Stream records must be written in the same transaction as data writes. A backend that cannot provide this atomicity will produce incorrect stream behavior.

### No Caching

extenddb prohibits in-process caching of database state. Multiple extenddb instances may share the same backend. Any caching requires a cross-instance invalidation design and explicit human approval.

### Sequence Monotonicity

Stream sequence numbers must be monotonically increasing within a shard. Your backend must provide a coordination mechanism for this.

### CASCADE Semantics

Account deletion must cascade to all child resources (users, groups, roles, policies, access keys, sessions) atomically. Your backend must provide equivalent cascade logic.

## Hypothetical: Implementing a Cassandra Backend

### What Maps Naturally

- **Item storage** — Cassandra's wide-column model maps well to DynamoDB's key-value items. Partition key → Cassandra partition key, sort key → clustering column, item attributes → a blob/map column.
- **Query by partition key** — native Cassandra operation.
- **TTL** — Cassandra has native TTL support per row, though the semantics differ (Cassandra TTL is per-cell, DynamoDB TTL is per-item with a specific attribute).
- **Horizontal scaling** — Cassandra's distributed nature would provide natural scaling.

### What Requires Design Decisions

- **Condition expressions** — Cassandra has lightweight transactions (LWT) with `IF` clauses, but they are limited compared to DynamoDB's full expression language. You would likely need to read-then-write with application-level locking, or implement condition evaluation in the application layer with Cassandra's compare-and-set.
- **Transactions** — DynamoDB's `TransactWriteItems` requires ACID across multiple items/tables. Cassandra has no multi-partition transactions. Options: (a) use a transaction coordinator library, (b) implement saga patterns, (c) limit transaction support. This is the hardest problem.
- **Sort key ordering** — Cassandra clustering columns provide ordering within a partition, which maps well. However, `scan` across all partitions with consistent ordering is not native to Cassandra.
- **Parallel scan segments** — would need a custom partitioning scheme (e.g., token range splitting).
- **Stream records** — would need a separate table or Cassandra CDC (Change Data Capture).
- **Sequence numbers** — monotonically increasing per-shard sequence numbers require coordination. Cassandra counters or a lightweight transaction could work but add latency.

### What Does Not Map

- **Serializable isolation for TransactGetItems** — Cassandra does not provide serializable reads across partitions. You would need to accept weaker consistency or implement a coordination layer.
- **Atomic multi-table writes** — `TransactWriteItems` can span multiple DynamoDB tables. Cassandra has no cross-table atomicity.
- **Secondary indexes with consistent propagation** — Cassandra's secondary indexes are local. GSI-like behavior would require materialized views or application-managed denormalization.

## PostgreSQL-isms in the Default Backend

An honest assessment of where the PostgreSQL implementation makes backend-specific choices. These are implementation details inside `storage-postgres`, not leaks in the trait abstraction:

1. **JSONB item storage** — items are stored as JSONB, enabling PostgreSQL-specific query optimizations. The traits pass `Item` = `BTreeMap<String, AttributeValue>` — your backend can use any serialization format.

2. **Transaction isolation** — the PostgreSQL backend uses `BEGIN ISOLATION LEVEL SERIALIZABLE` for transactions. Your backend needs equivalent isolation guarantees.

3. **Sequence generation** — stream sequence numbers use PostgreSQL sequences (`nextval`). Your backend needs a monotonic counter mechanism.

4. **CASCADE deletes** — account deletion cascades through foreign keys. Your backend needs equivalent cascade logic (can be application-level).

5. **Dual-database architecture** — catalog and data are separate PostgreSQL databases. Your backend might use a single database with keyspace separation, or a different topology entirely.

6. **SQL migrations** — migration files are raw SQL. Your backend needs its own schema initialization mechanism, exposed through `Bootstrapper`.

## Summary of Traits and Implementations

| Trait | Defined In                            | PostgreSQL Implementation | Purpose |
|-------|---------------------------------------|--------------------------|---------|
| `TableEngine` | `storage/src/lib.rs`                  | `PostgresEngine` | Table lifecycle |
| `DataEngine` | `storage/src/lib.rs`                  | `PostgresEngine` | Item CRUD, query, scan, transactions |
| `MetadataEngine` | `storage/src/lib.rs`                  | `PostgresEngine` | TTL, tags, table statistics |
| `StreamEngine` | `storage/src/lib.rs`                  | `PostgresEngine` | DynamoDB Streams |
| `WorkerStore` | `storage/src/lib.rs`                  | `PostgresEngine` | Background workers |
| `BackupEngine` | `storage/src/lib.rs`                  | `PostgresEngine` | Backup and restore |
| `ManagementStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | IAM CRUD |
| `AdminStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | Admin users |
| `SettingsStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | Runtime settings |
| `MetricsStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | Historical metrics |
| `RateLimitStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | Login rate limiting |
| `AuthorizationStore` | `storage/src/authorization_store.rs`  | `PostgresCatalogStore` | Policy lookups |
| `Bootstrapper` | `storage/src/bootstrapper.rs`         | `PostgresBootstrapper` | Init, destroy, migrate |
| `CredentialStore` | `auth/src/lib.rs`                     | `DbCredentialStore` | SigV4 credential lookup |

All PostgreSQL-specific code lives in `crates/storage-postgres/`. The `server` crate has no direct database dependencies. Backends register at compile time via the `inventory` crate and are selected by name at startup.
