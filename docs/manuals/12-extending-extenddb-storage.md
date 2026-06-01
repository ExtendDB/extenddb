# Extending extenddb Storage

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Introduction

extenddb uses a fully trait-based storage abstraction. The default backend is PostgreSQL, implemented in the `storage-postgres` crate. TiDB is available as an optional in-tree backend in `storage-tidb`. This document explains the storage architecture, lists every trait a new backend must implement, and provides guidance for adding a new storage backend.

As of v0.0.81, the server crate has **no direct database driver dependencies**. All database access goes through traits defined in the `storage` and `auth` crates. Backend-specific code lives in backend crates such as `storage-postgres` and `storage-tidb`, with the `bin` crate acting as the wiring layer.

## Architecture Overview

extenddb is organized as a Cargo workspace with eight crates:

```
bin              → CLI entry point (init, serve, stop, migrate, manage, etc.)
server           → HTTP layer (axum), console, management API (no database dependency)
engine           → DynamoDB operation logic (pure business rules, no DB access)
core             → Types, expressions, validation (sync, no async runtime)
auth             → SigV4 verification, policy evaluation (trait-based credential store)
storage          → Trait definitions and backend-agnostic utilities (ARN construction, key parsing)
storage-postgres → PostgreSQL implementation of all storage traits
storage-tidb     → TiDB implementation of all storage traits
```

The key architectural principle: neither the `engine` nor the `server` crate touches any database directly. They receive trait objects and call their methods. The `storage` crate defines these traits with no database dependencies, and provides backend-agnostic utilities in `storage::util` (ARN construction, partition/sort key parsing, netstring encoding) that any backend can reuse. Backend crates implement the traits. The `bin` crate is the wiring layer that creates concrete stores and passes them to the server.

## Trait Overview

A new backend must implement **13 storage traits** plus the `CredentialStore` trait from the `auth` crate:

**DynamoDB data path** (defined in `crates/storage/src/lib.rs`):
1. `TableEngine` — table lifecycle
2. `DataEngine` — item CRUD, query, scan, transactions
3. `MetadataEngine` — TTL, tags, table statistics
4. `StreamEngine` — DynamoDB Streams
5. `WorkerStore` — background worker operations (control-plane transitions, TTL cleanup)

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

Backends register at compile time using the `inventory` crate and are selected by name at startup. The `RuntimeHooks` trait allows backends to spawn backend-specific workers.

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
| `update_table` | Modify billing mode, throughput, stream specification, deletion protection, and GSI create/delete |
| `table_key_info` | Lightweight metadata fetch (key schema, attribute definitions) for data ops |
| `table_read_info` | Base table metadata plus optional resolved secondary-index metadata for Query and Scan |
| `index_info` | Fetch metadata for a specific secondary index |

Key design decisions:
- Tables have a lifecycle: CREATING → ACTIVE → UPDATING → ACTIVE → DELETING → (gone). Backends that emulate a fixed control-plane delay may use `control_plane_delay_seconds`; backends with native online DDL scheduling, such as TiDB, should make transitions immediately eligible and let the database coordinate physical schema work. `UPDATING` should not automatically become a table-wide application mutex on those backends; TiDB continues serving writes and can accept additional compatible schema intent while native online DDL is pending.
- Tables are scoped by `account_id`. Multi-tenancy is a first-class concern.
- GSI creation is a control-plane transition. Backends persist the catalog intent first, then reconcile data artifacts from that durable state. TiDB should include indexes known at `CreateTable` time in the physical `CREATE TABLE` DDL. If replay finds the physical table already exists, treat that as the native distributed race signal and repair missing generated columns/indexes with TiDB online `IF NOT EXISTS` DDL before publishing `ACTIVE`. For later GSI additions, use the transition path for crash-safe native backfill while keeping ordinary GSI writes transactional: batch generated-column additions per table, submit pending native index changes as TiDB multi-schema online DDL, and let TiDB own distributed backfill. Keep column-add and index-add as two online DDL statements, because TiDB validates a multi-change `ALTER TABLE` against the starting schema and the index cannot depend on a generated column that does not exist yet; GSI delete can drop indexes and generated columns together in one multi-schema online DDL. After physical DDL completes, publish or remove the still-pending index batch with one conditional catalog statement instead of per-index status probes. TiDB native secondary-index definitions should contain only the DynamoDB index key columns. Do not append the base table key: TiDB already stores the clustered row handle with secondary-index entries, and duplicating the base key can exceed TiDB's 3072-byte index key limit for otherwise legal DynamoDB key sizes. TiDB index Query and Scan should page in TiDB's native secondary-index order: DynamoDB index key columns followed by the clustered primary-key handle. Attribute definitions are merged into the table catalog when adding GSIs; a GSI update must not replace definitions needed by the base table or another native index. Query and Scan must still enforce DynamoDB projection semantics above the backend's physical layout: default index reads return projected attributes, GSI reads cannot request non-projected attributes, and LSI reads may fetch from the base table. Prefer generated columns over generic expression indexes for DynamoDB JSON key materialization, because TiDB documents generated columns as the production JSON indexing path while unrestricted expression-index functions are experimental.

### DataEngine

Item CRUD, query, scan, and transaction operations:

| Method | Purpose |
|--------|---------|
| `put_item` | Write/replace an item, with optional condition expression and stream capture |
| `get_item` | Read a single item by primary key; receives the DynamoDB `ConsistentRead` flag |
| `delete_item` | Delete an item by primary key, with optional condition and stream capture |
| `update_item` | Upsert with update expressions (SET, REMOVE, ADD, DELETE) |
| `query` | Query by partition key with optional sort key condition, pagination, index routing, and the `ConsistentRead` flag |
| `scan` | Full table/index scan with pagination, parallel scan segments, and the `ConsistentRead` flag |
| `transact_get_items` | Multi-item consistent read (serializable isolation) |
| `transact_write_items` | Multi-item atomic write with conditions and idempotency tokens |
| `cleanup_expired_idempotency_tokens` | Garbage-collect old idempotency tokens |

Key design decisions:
- **Condition expressions** are evaluated inside the storage transaction. The engine parses and compiles expressions; the storage layer receives an AST (`Expr`) and evaluates it against the existing item within the same transaction that performs the write. This is critical for correctness — condition checks and writes must be atomic.
- **DataEngine futures borrow request metadata.** Backend implementations
  should return the implementation future directly and should not clone keys,
  expression maps, resolved index info, or transaction batches just to enter
  async code. Owned item payloads can still move into the future normally.
- **Read consistency belongs in storage routing.** The engine passes
  `ConsistentRead` into `get_item`, `query`, and `scan`. Backends that have a
  native default-read path can use it for default DynamoDB reads,
  while strong reads and all writes stay on the authoritative path.
- **String key collation** belongs in schema, not in scattered query clauses.
  TiDB creates catalog and data metadata tables with
  `DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, while metrics labels use
  `ascii_bin`, and migrations convert existing metadata columns to those
  binary collations. DynamoDB names, ARNs, stream shard ids, and idempotency
  tokens stay case-sensitive under any cluster default.
- **TiDB-style distributed SQL backends** should set their required transaction
  mode when each pool connection is created. ExtendDB's TiDB backend sets
  `tidb_txn_mode = 'pessimistic'` so write-path row locks are explicit and do
  not depend on cluster upgrade history.
- **Default reads should use native read routing.** TiDB uses a separate
  `closest-adaptive` follower-read pool for default DynamoDB reads.
  `TransactGetItems` runs plain reads inside one TiDB transaction, getting a
  native snapshot without application-level locks.
- **Conditional create after an absent-key read** should rely on backend-native
  key locking and unique-key errors, not no-op upsert affected-row counts. TiDB
  pessimistic point reads lock absent primary keys, so the create path is a
  plain `INSERT`; duplicate-key is the remaining race signal.
- **Unconditional transactional Put/Delete operations** should not pre-read the
  item when there is no condition expression and no stream record needs the old
  image. TiDB can execute the write directly inside the transaction and let
  native primary-key and index maintenance coordinate the mutation.
- **Physical key encoding** must be centralized. TiDB stores the full DynamoDB
  HASH-key tuple in one physical `pk` column, so get/update/delete,
  transactions, and stream shard assignment must all use the same physical key
  encoder as put/query. TiDB stores HASH keys as raw bytes in 2048-byte
  `VARBINARY` columns; paired with 1024-byte typed sort-key columns, legal
  DynamoDB keys fit exactly within TiDB's 3072-byte native index limit. TiDB
  backend startup rejects configured key-size limits wider than this native
  shape, and TiDB table/index creation rejects multi-RANGE key schemas instead
  of letting an impossible online DDL job fail later. Multi-HASH extension
  values are accepted only when their encoded tuple fits the raw 2048-byte
  hash slot.
- **Stream shard assignment** should not add a metadata read to every write
  when the backend has a fixed shard layout. The TiDB backend derives the shard
  id directly from the encoded partition-key tuple and uses TiDB's native TSO
  plus an in-transaction ordinal as the sequence source instead of a per-shard
  counter row.
- **Stream capture** is passed as `Option<&StreamCapture>`. When present, the stream record must be written in the same transaction as the data write.
- **Idempotency tokens** for `TransactWriteItems` must be claimed atomically with the writes by inserting into a unique-key token table. Do not pre-select then insert; use the database's native duplicate-key result to distinguish a new token from a replay. Backends may use the token as a safe retry boundary for retryable transaction conflicts; without a token, avoid automatic retries that could duplicate non-idempotent updates after an ambiguous commit.
- **Items** are `BTreeMap<String, AttributeValue>`. A new backend must handle the full `AttributeValue` type (S, N, B, SS, NS, BS, L, M, BOOL, NULL).
- **Query** must support forward/reverse sort order, exclusive start key pagination, and routing to secondary index storage.
- **Parallel scan** uses `segment` and `total_segments` to partition the keyspace.

### MetadataEngine

TTL, tags, and table statistics:

| Method | Purpose |
|--------|---------|
| `describe_ttl` | Get TTL configuration for a table |
| `update_ttl` | Enable/disable TTL on a table attribute |
| `apply_ttl_update` | Apply the full TTL state transition, including backend-specific physical artifacts |
| `find_expired_items_indexed` | Find items with expired TTL attribute for indexed-worker backends |
| `tag_resource` | Add/overwrite tags on a resource ARN |
| `untag_resource` | Remove tags by key |
| `list_tags` | List all tags for a resource ARN |
| `tables_with_ttl` | List tables with TTL enabled (single account) |
| `all_tables_with_ttl` | List tables with TTL enabled (all accounts) |
| `refresh_table_size` | Recompute and store table size and item count for cache-based backends |
| `list_active_table_names` | List active table names (single account) |
| `all_active_tables` | List active tables (all accounts) |

Key design decisions:
- TTL mutation is storage-owned through `apply_ttl_update`. Backends may use the default indexed-worker path, or override it for native database TTL. When a worker deletes expired items, it must call `DataEngine::delete_item` so index sync and stream capture remain correct. A backend that delegates deletion to native TTL, such as TiDB, should return no tables/items from indexed-worker TTL enumeration methods so application-level cleanup cannot duplicate native deletion. Native TTL backends should persist explicit transition state (`DISABLED`, `ENABLING`, `ENABLED`, `DISABLING`) and let the backend control-plane reconciler submit native online TTL DDL from that durable intent, so live nodes and startup repair can finish enable and disable paths without guessing from artifact booleans. Legacy readiness booleans should be migrated into explicit status and removed. If native tooling can disable TTL jobs during restore or flashback, the backend should verify physical TTL state on startup and re-enable the native table option.
- Tags are stored by ARN string.
- Native-stat backends should prefer reading table size and item count from
  backend metadata when building `DescribeTable` and backup metadata. A
  periodic frontend refresh worker is only needed when the backend has no
  cheap native table-stat view. TiDB follows this native path: its table
  catalog does not store cached size/count columns, and descriptions read
  `information_schema.tables` on demand.

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
| `next_sequence_number` | Generate the next sortable sequence number for a shard |
| `validate_shard` | Verify a shard exists for a given stream ARN |
| `latest_sequence_number` | Get the latest sequence number in a shard |

Key design decisions:
- Stream records are written atomically with data writes (same transaction).
- Shards are hash-assigned based on partition key.
- Sequence numbers must be monotonically increasing within a shard.
- The retention period is configurable (default 24 hours).
- TiDB backends should derive user-visible stream sequence numbers from native
  MVCC commit timestamps, not transaction start timestamps. Insert the stream
  record atomically with the item write, then finalize the visible sequence
  from TiDB `commit_ts` plus an in-transaction ordinal so `LATEST` and
  `GetRecords` follow commit order without a per-shard counter row.
- TiDB should not foreground-delete stream history during `DeleteTable`; native
  TTL owns shared `stream_records` retention and immutable table IDs prevent
  reuse conflicts.

### WorkerStore

Background worker operations:

| Method | Purpose |
|--------|---------|
| `process_control_plane_transitions` | Recover and advance pending table lifecycle transitions |

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
| `insert_metrics` | Insert a metrics snapshot |
| `query_metrics` | Query metrics by time range, operation, and table filters |

TiDB stores flushed metrics as append-only `metrics_samples` rows with native
`AUTO_RANDOM` IDs and native TTL, then aggregates them during `query_metrics`,
avoiding multi-frontend hot-row upserts. TiDB migrations drop the legacy
aggregate `metrics` table after the samples table is available; runtime code
does not query both paths. Flushes should batch the drained metrics rows into
bounded multi-row inserts so the append-only design does not pay one frontend
catalog round trip per metric row while still avoiding oversized TiDB write
transactions.

For fixed-retention data tables, such as TiDB stream records and transaction
idempotency tokens, prefer native TTL over frontend cleanup indexes. Keep only
indexes needed by read/write paths; TTL does not require an application-facing
`created_at` lookup index. For same-token idempotency-window expiry, use one
native primary-key upsert claim that atomically recycles an expired row and a
per-attempt `claim_id` to distinguish a newly claimed token from an in-window
replay; do not put a cleanup delete on the transaction write path.

For append-only TiDB catalog tables without a clustered primary key, such as
failed-login attempts, use TiDB `SHARD_ROW_ID_BITS` to scatter implicit row IDs
instead of adding a frontend-generated identifier. This keeps the write path a
single insert while avoiding one-Region hotspots under multiple frontend nodes.

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
- **Catalog database** (`extenddb`) — table metadata, IAM data, settings, metrics, tags, login attempts, and backup metadata
- **Data database** (`extenddb_data`) — DynamoDB items, index rows or native index artifacts, stream records, and transaction idempotency tokens that must commit atomically with item writes. TiDB derives fixed stream shard IDs instead of persisting shard metadata.

### Migrations

The PostgreSQL reference backend currently keeps its catalog schema in
`crates/storage-postgres/migrations/001_schema.sql`. TiDB keeps its own
backend-specific migration stream in `crates/storage-tidb/migrations/` and data
schema setup in `crates/storage-tidb/data_migrations/`.

New backend migrations should describe the backend's physical layout, not copy
PostgreSQL's file names. For TiDB, that means native secondary-index artifacts,
native TTL clauses, BR metadata, and binary collation defaults live in the TiDB
migration stream. TiDB data-table validation that needs to inspect `_ddb_*`
tables runs in the data migration pass; for example, incompatible older base
`pk` columns and native generated hash-key columns are rejected before startup,
while the catalog migration records the backend version change.

## Adding a New Backend

NOTE: ExtendDB ships with a built-in functional reference backend implementation for PostgreSQL. Future backend
implementations should be developed and released separately from the ExtendDB project itself, to simplify dependencies
and maintenance.

### Step 1: Create a New Crate

Create a new crate (e.g., `storage-mydbengine`) in your workspace. It should depend on `extenddb-storage`, `extenddb-auth`, and `extenddb-core`.

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

Stream sequence numbers must be monotonically increasing within a shard. Your backend must provide a coordination mechanism for this. Native timestamp-oracle backends can use the database's monotonic timestamp service; sequence numbers do not need to be contiguous.

### CASCADE Semantics

Account deletion must cascade to all child resources (users, groups, roles, policies, access keys, sessions) atomically. Your backend must provide equivalent cascade logic.

## PostgreSQL-isms in the Default Backend

The PostgreSQL implementation makes backend-specific choices. These are implementation details inside `storage-postgres`, not leaks in the trait abstraction:

1. **JSONB item storage** — items are stored as JSONB, enabling PostgreSQL-specific query optimizations. The traits pass `Item` = `BTreeMap<String, AttributeValue>` — your backend can use any serialization format.

2. **Transaction isolation** — the PostgreSQL backend uses `BEGIN ISOLATION LEVEL SERIALIZABLE` for transactions. Your backend needs equivalent isolation guarantees.

3. **Sequence generation** — stream sequence numbers use a backend-native monotonic source. PostgreSQL can use sequences (`nextval`); TiDB uses MVCC commit timestamps plus an in-transaction ordinal so stream-enabled writes do not contend on a shared counter row, multiple records from one transaction remain uniquely ordered, and stream iteration follows commit order across nodes. Treat sequence numbers as decimal strings in iterator code rather than parsing them into host integers; TiDB sequence numbers can be wider than `u64`.

4. **CASCADE deletes** — account deletion cascades through foreign keys. Your backend needs equivalent cascade logic (can be application-level).

5. **Dual-database architecture** — catalog and data are separate PostgreSQL databases. Your backend might use a single database with keyspace separation, or a different topology entirely.

6. **SQL migrations** — migration files are raw SQL. Your backend needs its own schema initialization mechanism, exposed through `Bootstrapper`.

## Summary of Traits and Implementations

| Trait | Defined In | PostgreSQL Implementation | TiDB Implementation | Purpose |
|-------|------------|---------------------------|---------------------|---------|
| `TableEngine` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | Table lifecycle |
| `DataEngine` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | Item CRUD, query, scan, transactions |
| `MetadataEngine` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | TTL, tags, table statistics |
| `StreamEngine` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | DynamoDB Streams |
| `WorkerStore` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | Background workers |
| `BackupEngine` | `storage/src/lib.rs` | `PostgresEngine` | `TidbEngine` | Backup and restore |
| `ManagementStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | IAM CRUD |
| `AdminStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | Admin users |
| `SettingsStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | Runtime settings |
| `MetricsStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | Historical metrics |
| `RateLimitStore` | `storage/src/management_store/mod.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | Login rate limiting |
| `AuthorizationStore` | `storage/src/authorization_store.rs` | `PostgresCatalogStore` | `TidbCatalogStore` | Policy lookups |
| `Bootstrapper` | `storage/src/bootstrapper.rs` | `PostgresBootstrapper` | `TidbBootstrapper` | Init, destroy, migrate |
| `CredentialStore` | `auth/src/lib.rs` | `DbCredentialStore` | `DbCredentialStore` | SigV4 credential lookup |

Backend-specific code lives in backend crates such as `crates/storage-postgres/` and `crates/storage-tidb/`. The `server` crate has no direct database dependencies. Backends register at compile time via the `inventory` crate and are selected by name at startup.
