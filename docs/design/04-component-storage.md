# extenddb — Component Design: Storage

**Version:** 3.0
**Date:** 2026-05-19
**Status:** Active
**Crates:** `storage` (traits), `storage-postgres` (PostgreSQL backend), `storage-tidb` (TiDB backend)

## 1. Purpose

The storage layer provides a trait-based abstraction for all persistent data operations. Traits are defined in the
`storage` crate with no database-specific dependencies. Backend implementations live in separate crates (e.g.,
`storage-postgres`, `storage-tidb`) and register themselves via a factory pattern using the `inventory` crate.

The trait-based design allows new storage backends to be added by implementing the storage traits and registering a
factory function, with no changes needed to the `engine` or `server` crates. The factory pattern enables runtime
backend selection based on configuration.

**Current status**: PostgreSQL is the default backend. TiDB is available as an optional in-tree backend selected with
`storage.backend = "tidb"` when compiled with the `tidb` feature.

## 2. Storage Trait Hierarchy

The storage abstraction is split into focused traits following the Interface Segregation Principle. This allows
backends to implement traits incrementally, and allows consumers to depend only on the traits they need.

### 2.1 Trait Categories

ExtendDB defines **13 traits** across three categories:

**DynamoDB Data Path** (defined in `storage/src/lib.rs`):
- `TableEngine` — table lifecycle operations
- `DataEngine` — item CRUD, query, scan, batch, transactions
- `MetadataEngine` — TTL, tags, table statistics
- `StreamEngine` — DynamoDB Streams record storage and retrieval
- `WorkerStore` — background worker operations (control plane transitions)
- `BackupEngine` — backup and restore operations

**Management and Operational** (defined in `storage/src/management_store.rs` and related modules):
- `ManagementStore` — auth-related CRUD (users, groups, roles, policies, access keys, accounts)
- `AdminStore` — admin user management
- `SettingsStore` — runtime settings (key-value store)
- `MetricsStore` — historical metrics persistence and query
- `RateLimitStore` — login rate limiting and account lockout
- `AuthorizationStore` — policy lookups for authorization decisions

**Initialization and Lifecycle** (defined in `storage/src/bootstrapper.rs`):
- `Bootstrapper` — database initialization, migration, verification, destruction

**Authentication** (defined in `auth/src/lib.rs`):
- `CredentialStore` — access key and session credential lookup for SigV4 verification

### 2.2 Composite Traits

Two composite traits aggregate related functionality:

```rust
/// All DynamoDB data path operations
pub trait StorageEngine:
    TableEngine + DataEngine + MetadataEngine + StreamEngine + WorkerStore + BackupEngine
{
}

/// All catalog and management operations
pub trait CatalogStore:
    ManagementStore
    + AdminStore
    + SettingsStore
    + MetricsStore
    + RateLimitStore
    + AuthorizationStore
{
}
```

Backends implement the individual traits, then implement the composite traits with empty bodies:

```rust
impl StorageEngine for PostgresEngine {}
impl CatalogStore for PostgresEngine {}

impl StorageEngine for TidbEngine {}
impl CatalogStore for TidbEngine {}
```

The `engine` crate receives `Arc<dyn StorageEngine>` for data operations. The `server` crate receives
`Arc<dyn CatalogStore>` for management API operations. This separation ensures components depend only
on the traits they need.

### 2.3 BoxFuture Pattern

Storage traits use explicit `BoxFuture` return types:

```rust
use futures::future::BoxFuture;

pub trait TableEngine: Send + Sync {
    fn create_table(&self, account_id: &str, input: CreateTableInput)
        -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    fn table_read_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<TableReadInfo, StorageError>>;
}

pub trait DataEngine: Send + Sync {
    fn get_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>>;
}
```

This pattern provides:
- **Object safety**: Traits can be used as `Arc<dyn Trait>`
- **Explicit lifetimes**: control-plane futures can borrow from `&self`; data-plane futures can borrow request metadata until awaited
- **No macro overhead**: No `#[async_trait]` macro expansion

The `CredentialStore` trait in the `auth` crate uses `#[async_trait]` instead
(older pattern). The `bin` crate bridges the two patterns with a thin adapter.

### 2.4 Key Trait Methods

**TableEngine** (table lifecycle):
- `create_table`, `delete_table`, `describe_table`, `list_tables`, `update_table`
- `table_key_info` — returns lightweight metadata (key schema, attribute definitions)
  for data operations, avoiding the overhead of a full `describe_table` call
- `table_read_info` — returns base table metadata plus optional resolved GSI/LSI
  metadata for Query and Scan so storage backends do not re-fetch catalog rows
  after the engine has selected an index
- `index_info` — returns GSI/LSI metadata for query operations

All table operations are scoped by `account_id` for multi-account isolation.

**DataEngine** (item CRUD, query, scan, transactions):
- `put_item`, `get_item`, `delete_item`, `update_item`
- `query`, `scan`
- `transact_get_items`, `transact_write_items`
- `cleanup_expired_idempotency_tokens`

Data operations receive `TableKeyInfo` from the engine layer, which has already
validated the table exists and can serve the data plane. Query and Scan also
receive resolved `IndexInfo` when they target a secondary index; the storage
backend must not re-fetch index metadata from an index name string. Condition
expressions are evaluated inside the storage transaction to prevent TOCTOU
races. Read methods receive the DynamoDB `ConsistentRead` flag so a backend can
route strong reads and eventually consistent reads through different native
paths. Stream records are written atomically with data writes when `stream` is
`Some`.

For TiDB, every storage connection uses pessimistic transaction mode. Conditional
writes first perform a primary-key `SELECT ... FOR UPDATE`; TiDB locks the
point key even when the row is absent, so a subsequent create path can use a
plain `INSERT` and rely on the native unique-key result as the final race
signal. The implementation must not infer correctness from MySQL affected-row
counts on no-op upserts.
Unconditional transactional Put/Delete operations should not pay for a pre-read
when no stream record needs the old image; TiDB can execute the write directly
inside the transaction and let native primary-key/index maintenance do the
coordination. `TransactGetItems` starts a normal TiDB transaction and performs
plain reads inside that transaction, getting one native snapshot without
application-level locks.

**MetadataEngine** (TTL, tags, table statistics):
- `describe_ttl`, `update_ttl`, `apply_ttl_update`
- `tag_resource`, `untag_resource`, `list_tags`
- `refresh_table_size` — updates cached table size and item count for backends
  that maintain a catalog cache. Native-stat backends can answer from their
  database metadata at describe/backup time instead.
- `create_ttl_index`, `find_expired_items_indexed` — backend-specific TTL
  artifact support for indexed-worker backends. `apply_ttl_update` lets the
  storage backend own the whole TTL transition; PostgreSQL uses the default
  indexed-worker workflow, while TiDB records native TTL intent and lets the
  control-plane reconciler submit TiDB online TTL DDL. TiDB batches legacy
  artifact cleanup with multi-schema `ALTER TABLE` and does not run or expose
  an item sweeper. TiDB persists explicit TTL intent (`DISABLED`, `ENABLING`,
  `ENABLED`, `DISABLING`) so the live reconciler and startup repair can finish
  the correct native DDL path after a frontend crash instead of inferring
  intent from artifact booleans. Legacy readiness booleans are migrated into
  that explicit status and then dropped. Repair also reads physical table TTL state and
  re-enables `TTL_ENABLE` when TiDB recovery tools such as BR or Flashback have
  disabled TTL jobs.

**StreamEngine** (DynamoDB Streams):
- `write_stream_record` — writes stream record atomically with data write
- `get_stream_records` — retrieves stream records for a shard
- `describe_stream`, `list_streams`
- `cleanup_expired_stream_records` — removes records older than 24 hours

TiDB uses a fixed deterministic stream shard layout, so streamed writes compute
the shard id from the encoded partition-key tuple. Stream rows are inserted
atomically with item writes under a transaction-local storage sequence, then
finalized to the user-visible sequence number using TiDB's native MVCC
`commit_ts` (`TIDB_MVCC_INFO` over `TIDB_ENCODE_RECORD_KEY`) plus the
in-transaction ordinal. This avoids a shard counter row while preserving
commit-order stream iteration across multiple TiDB nodes. Stream iterator code
treats sequence numbers as opaque decimal strings and computes
`AT_SEQUENCE_NUMBER` predecessors with string arithmetic, because TiDB
TSO-plus-ordinal values are wider than native host integers.
TiDB does not foreground-delete stream history during `DeleteTable`; table ids
are immutable, the catalog deletion makes the stream unreachable, and native
TTL owns retention for the shared `stream_records` table. TiDB data migrations
pre-split both the clustered stream key and the commit-sequence secondary index
by shard-id range, so a fresh shared stream table does not start as one Region
for all frontend writers.

**WorkerStore** (background worker operations):
- `process_control_plane_transitions` — handles table state transitions
  (CREATING → ACTIVE, UPDATING → ACTIVE, DELETING → deleted)

**BackupEngine** (backup and restore):
- `create_backup`, `describe_backup`, `list_backups`, `delete_backup`
- `restore_table_from_backup`

Backend implementations own the physical backup data plane. PostgreSQL keeps
its existing implementation. TiDB uses native BR for snapshot data and keeps
only ExtendDB metadata in the catalog; unsupported BR restore shapes are
reported explicitly rather than emulated by item replay. For TiDB restore,
physical BR restore and online DDL normalization complete before the target
catalog row is published, so failed restores do not create durable transitional
table metadata. TiDB `DeleteBackup` removes only ExtendDB catalog metadata;
the BR snapshot directory is lifecycle-managed by the configured backup storage
or TiDB Operator rather than by an ExtendDB frontend.

`RestoreTableToPointInTime` is a storage-owned operation, not an engine stub.
Backends that cannot provide it faithfully return an explicit validation error.
TiDB intentionally does not implement DynamoDB table-level PITR restore with a
frontend item replay path: TiDB BR PITR is a cluster recovery primitive, TiDB
`FLASHBACK TABLE` is for dropped/truncated tables, and TiDB historical reads are
read-only for the live target-table shape. A future TiDB implementation should
be added only if TiDB exposes a native set-based online restore into a new table.

Backends also declare whether capacity control is frontend-local or storage
native. PostgreSQL can use ExtendDB's process-local token buckets when operators
want DynamoDB-like throttling in a single-frontend test environment. TiDB marks
capacity control as backend-native so the server does not enforce local token
buckets; TiDB Resource Control/resource groups own distributed flow control and
scheduling across all frontends. The TiDB adapter exposes the optional
`storage.tidb.resource_group` value through storage config and binds catalog,
strong data, default-read data, and catalog-store runtime sessions with
`SET RESOURCE GROUP`. Catalog stores also declare when retention is owned by
native backend TTL so generic workers do not issue periodic no-op cleanup calls.

```rust
pub trait WorkerStore: Send + Sync {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>>;
}
```

The `WorkerStore` trait provides operations needed by background workers. The `process_control_plane_transitions`
method handles table state transitions (CREATING → ACTIVE, UPDATING → ACTIVE, DELETING → deleted) and is called by the
control plane poller worker.

## 3. Management and Operational Traits

The management traits handle authentication, authorization, settings, metrics, and rate limiting. These traits are
defined in `storage/src/management_store/mod.rs` and related modules.

### ManagementStore

Auth entity CRUD operations for accounts, users, groups, roles, policies, and access keys. Key methods:

- **Accounts**: `create_account`, `delete_account`, `list_all_accounts`, `get_account_detail`
- **Users**: `create_user`, `delete_user`, `list_users`, `get_user_detail`, `verify_iam_user_password`
- **Groups**: `create_group`, `delete_group`, `list_groups`, `add_group_member`, `remove_group_member`
- **Roles**: `create_role`, `delete_role`, `list_roles`, `get_role_detail`
- **Policies**: `attach_user_policy`, `detach_user_policy`, `attach_group_policy`, `attach_role_policy`
- **Access Keys**: `create_access_key`, `delete_access_key`, `list_access_keys`, `deactivate_access_key`
- **Tags**: `tag_user`, `untag_user`, `tag_role`, `untag_role`

All Auth operations are scoped by `account_id` for multi-account isolation.

### AdminStore

Admin user management (separate from IAM-type users):
- `create_admin`, `list_admins`, `delete_admin`, `change_admin_password`,
  `verify_admin_password`

Admin users have access to the management console and can create accounts and
IAM-type entities.

### SettingsStore

Runtime settings storage (key-value store):
- `get_setting`, `set_setting`, `list_settings`, `cached_encryption_key`

Settings include backend-specific control-plane timing, `log_level`, and the
encryption key for access key secrets.

### MetricsStore

Historical metrics persistence and query:
- `insert_metrics`, `query_metrics`, `prune_metrics`

Metrics are flushed periodically from the in-memory collector to persistent
storage. TiDB persists immutable rows in `metrics_samples` with a native
`AUTO_RANDOM` clustered primary key, pre-split Regions, and native TTL, then
aggregates at query time. The old aggregate `metrics` table is migrated away
instead of kept in the runtime read path. This avoids cross-frontend
contention on a shared per-minute `ON DUPLICATE` metrics row and avoids a
single initial write Region for a new TiDB catalog. TiDB metrics flushes use
one multi-row append-only insert per bounded batch rather than one insert per
metric row, keeping frontend latency low while following TiDB's guidance to
keep write transactions split into modest batches.

### RateLimitStore

Login rate limiting and account lockout:
- `count_principal_failures`, `count_ip_failures`, `record_failed_login`,
  `cleanup_old_attempts`

Tracks failed login attempts by principal and source IP to mitigate clients
sending excessive traffic. TiDB stores these as append-only rows with native
TTL retention, `SHARD_ROW_ID_BITS` on the implicit row id, and pre-split
Regions, so concurrent frontends do not concentrate failed-login inserts on
one TiKV Region.

### AuthorizationStore

Policy lookups for authorization decisions:
- `get_user_policies`, `get_group_policies`, `get_role_policies`,
  `get_permissions_boundary`

Used by the authorization policy engine to retrieve policies for authorization
evaluation.

### Bootstrapper

Database initialization, migration, verification, and destruction:
- `initialize`, `migrate`, `verify`, `destroy`

Used by CLI commands (`extenddb init`, `extenddb migrate`, `extenddb verify`,
`extenddb destroy`).

The TiDB bootstrapper versions both catalog migrations and data-database
migrations. Shared data tables such as `stream_records` and
`idempotency_tokens` keep their native TTL repair paths, while one-time physical
layout work such as TiDB Region splitting is recorded in `data_schema_history`
instead of being replayed on every frontend startup.

### OperationsEngine

Backend CLI diagnostics and formatting helpers:
- connection-string parsing and redaction
- DDL identifier validation
- expected catalog version reporting
- backend-owned `catalog-check` integrity checks

`extenddb catalog-check` is intentionally backend-owned. PostgreSQL checks
physical `_ddb_<table_id>` data tables and companion index tables. TiDB checks
physical `_ddb_<table_id>` data tables plus native generated-column secondary
index artifacts, native TTL state, and stuck online-DDL catalog transitions.
The binary only loads config, refuses to run while the server PID is alive, and
prints the backend report.

## 4. Core Types Used by Storage Traits

Storage trait methods use types defined in `extenddb_core::types`. These types represent data concepts in a
backend-agnostic way. Storage implementers must understand these types to implement the traits correctly.

### Item and AttributeValue

```rust
/// An item — a map of attribute names to values.
pub type Item = BTreeMap<String, AttributeValue>;
```

`AttributeValue` is an enum representing all supported data types:
- `S(String)` — string
- `N(String)` — number (stored as string to preserve precision)
- `B(Vec<u8>)` — binary
- `SS(Vec<String>)` — string set
- `NS(Vec<String>)` — number set
- `BS(Vec<Vec<u8>>)` — binary set
- `L(Vec<AttributeValue>)` — list
- `M(BTreeMap<String, AttributeValue>)` — map
- `BOOL(bool)` — boolean
- `NULL(bool)` — null (always true)

Storage backends must preserve the exact type and value of each attribute.

### Table Metadata Types

**CreateTableInput**: Specifies table schema for `TableEngine::create_table`:
- `table_name: String`
- `key_schema: Vec<KeySchemaElement>` — partition key and optional sort key
- `attribute_definitions: Vec<AttributeDefinition>` — types for key attributes
- `billing_mode: BillingMode` — PAY_PER_REQUEST or PROVISIONED
- `global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>>`
- `local_secondary_indexes: Option<Vec<LocalSecondaryIndex>>`
- `stream_specification: Option<StreamSpecification>`

**TableDescription**: Returned by table operations, includes:
- `table_name: String`
- `table_status: TableStatus` — CREATING, ACTIVE, DELETING, UPDATING
- `table_arn: String`
- `table_id: String` — backend-specific unique identifier
- `key_schema: Vec<KeySchemaElement>`
- `attribute_definitions: Vec<AttributeDefinition>`
- `table_size_bytes: i64`
- `item_count: i64`
- `creation_date_time: f64` — Unix timestamp

**TableKeyInfo**: Lightweight metadata for data operations:
- `account_id: String`
- `table_name: String`
- `table_id: String`
- `key_schema: Vec<KeySchemaElement>`
- `attribute_definitions: Vec<AttributeDefinition>`
- `has_lsi: bool`
- `stream_specification: Option<StreamSpecification>`

**TableReadInfo**: Resolved read-path metadata:
- `table: TableKeyInfo` — the base table identity and key metadata
- `index: Option<IndexInfo>` — the selected secondary index metadata for
  Query/Scan, if the request uses an index

### Expression Types

**Expr**: Parsed condition expression AST (from `extenddb_core::expression`):
- Evaluated by storage backends inside transactions
- Supports comparisons, logical operators, functions (`attribute_exists`, `begins_with`, etc.)
- Storage backends call `extenddb_core::expression::evaluate()` to evaluate conditions

**ExpressionMaps**: Name and value substitutions for expressions:
- `names: HashMap<String, String>` — maps `#name` placeholders to attribute names
- `values: HashMap<String, AttributeValue>` — maps `:value` placeholders to values

**KeyCondition**: Parsed key condition for Query operations:
- `partition_key: (String, AttributeValue)` — partition key name and value
- `sort_key_condition: Option<SortKeyCondition>` — optional sort key condition

**UpdateAction**: Parsed update expression action:
- `SET`, `REMOVE`, `ADD`, `DELETE` operations
- Applied by storage backends inside transactions using `extenddb_core::expression::apply_update()`

### Stream Types

**StreamRecord**: Complete stream record for persistence:
- `event_id: String`
- `event_name: StreamEventName` — INSERT, MODIFY, REMOVE
- `event_version: String`
- `event_source: String`
- `aws_region: String`
- `dynamodb: StreamRecordData` — keys, old_image, new_image, size_bytes

**StreamCapture**: Metadata for constructing stream records inside transactions:
- `view_type: StreamViewType` — KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES
- `user_identity: Option<UserIdentity>` — set for TTL-originated deletions
- `region: Arc<str>`

Storage backends write stream records atomically with data writes when `stream` is `Some`.
For TiDB, stream shard assignment uses the same physical HASH-key tuple encoder
as the base table and derives the fixed shard id directly; visible stream
sequence numbers are finalized from TiDB MVCC commit timestamps, so writers do
not serialize through a per-shard counter row.

### Error Types

**StorageError**: Errors returned by storage trait methods:
- `TableNotFound(String)` — table does not exist
- `TableAlreadyExists(String)` — table already exists
- `TableNotActive(String)` — table cannot serve the requested operation (for example CREATING or DELETING; TiDB data-plane operations continue during UPDATING)
- `ConditionFailed { old_item: Option<Item> }` — condition expression evaluated to false
- `TransactionCanceled(Vec<CancellationReason>)` — transaction failed with per-item reasons
- `IdempotentReplay` — idempotency token matched previous request
- `IdempotentMismatch` — idempotency token exists with different operations
- `IndexNotFound` — secondary index does not exist
- `Internal(String)` — backend-specific error

The `engine` crate maps `StorageError` to wire protocol error responses.

## 5. PostgreSQL Backend Implementation

### 5.1 Schema Design

The PostgreSQL backend uses two categories of tables:

**Catalog tables** (metadata, created by migrations):
- `tables` — DynamoDB table metadata (key schema, status, ARN, table_id)
- `indexes` — GSI/LSI metadata (key schema, projection, status, index_id)
- `tags` — resource tags
- `_dynamodb_credentials` — access keys and session tokens
- `_dynamodb_users`, `_dynamodb_roles`, `_dynamodb_groups` — IAM-type entities
- `_dynamodb_group_members` — group membership
- `_dynamodb_principal_tags` — user/role tags
- `_dynamodb_policies` — IAM-type policy documents
- `_dynamodb_sessions` — temporary role session credentials
- `_dynamodb_stream_records` — DynamoDB Streams records
- `_dynamodb_import_jobs`, `_dynamodb_export_jobs` — import/export tracking
- `_dynamodb_idempotency_tokens` — TransactWriteItems idempotency

**Data tables** (created dynamically per DynamoDB table):
- `_ddb_<table_id>` — base table (table_id is a UUID)
- PostgreSQL companion index tables — backend-owned GSI/LSI projection tables
- TiDB generated columns and native secondary indexes on `_ddb_<table_id>` —
  no separate physical table per secondary index

**Schema files:**
- Catalog schema: `crates/storage-postgres/migrations/001_schema.sql`
- Data table DDL generation:
  `crates/storage-postgres/src/data/ddl.rs` and
  `crates/storage-tidb/src/data/ddl.rs`
- Table name helpers:
  `crates/storage-postgres/src/data/mod.rs` and
  `crates/storage-tidb/src/data/mod.rs`

**Design notes:**

- **Table naming**: Physical PostgreSQL tables use UUIDs instead of
  user-provided names. `table_id` and `index_id` are generated with
  `uuid::Uuid::new_v4()` and stored in the catalog. This avoids SQL injection
  risks and allows DynamoDB table names to use any characters (including
  Unicode, spaces, SQL keywords). The `_ddb_` prefix prevents collisions with
  catalog tables.

- **Partition key storage**: PostgreSQL stores partition key values as text:
  string keys directly, number keys as their string representation, and binary
  keys as canonical base64. TiDB stores the physical partition-key slot as raw
  `VARBINARY(2048)`: strings and numbers use their UTF-8 bytes, and binary keys
  use their decoded bytes. All TiDB point reads, writes, locks, stream shard
  assignment, and transaction helpers must use that same physical key helper so
  multipart keys cannot split across different SQL predicates. The raw
  2048-byte hash-key slot plus the 1024-byte sort-key slot fits TiDB's default
  3072-byte native index limit. TiDB rejects configured key-size limits wider
  than that native shape, rejects multi-RANGE key schemas before catalog commit,
  and rejects multi-HASH values whose encoded tuple cannot fit in the raw
  2048-byte hash-key slot.

- **Sort key storage**: Sort key values use typed columns (`sk_s TEXT`,
  `sk_n NUMERIC`, `sk_b BYTEA`) to ensure correct ordering. Only one `sk_*`
  column is populated per table, determined by the sort key's
  `AttributeDefinition` type. The `CREATE TABLE` DDL and `PRIMARY KEY`
  constraint are generated dynamically based on the key schema.
  - `NUMERIC` ensures `2 < 10 < 100` (not lexicographic `"10" < "2"`)
  - `BYTEA` ensures correct binary comparison order
  - `TEXT` ensures correct UTF-8 string ordering

- **Item storage**: `item_data` JSONB contains the complete item including key
  attributes, matching the DynamoDB model where key attributes are part of the
  item.

- **Secondary indexes**: Backend implementations use the database-native shape
  that preserves DynamoDB key ordering and pagination. PostgreSQL stores GSI
  companion tables with base table primary key columns (`base_pk`, `base_sk_*`)
  as actual SQL columns. TiDB stores each item once in the base table, exposes
  index keys as generated columns over `item_data`, and creates native TiDB
  secondary indexes over those generated columns. TiDB secondary indexes
  already carry the clustered row handle, so ExtendDB does not duplicate the
  full base table key into the index definition; that keeps legal DynamoDB key
  sizes within TiDB's 3072-byte index key limit and avoids unnecessary write
  amplification. Native secondary-index hash columns use the same raw
  2048-byte physical width as the base `pk` column; paired with 1024-byte
  sort-key columns, the native index tuple fits TiDB's default key limit. TiDB
  validates secondary-index key values before writes reach generated columns,
  so empty, oversized, or type-mismatched index keys return DynamoDB-shaped
  validation errors instead of leaking database constraint errors.
  Startup data migrations reject older incompatible `_ddb_*` key layouts
  instead of attempting unsupported TiDB primary-key/generated-column rewrites.
  The generated columns are deliberate: TiDB documents generated columns as the
  production path for indexing JSON-derived values, while generic expression
  indexes would make DynamoDB's casts, binary decoding, and composite-key
  expressions depend on the expression-index experimental function surface.
  On `CreateTable`, TiDB includes initial generated columns and native
  secondary indexes directly in the physical `CREATE TABLE` DDL. If replay
  finds the physical table already exists, the reconciler treats that as TiDB's
  native distributed race signal and converges any missing generated columns
  and indexes with `IF NOT EXISTS` online DDL before publishing `ACTIVE`. For
  later `UpdateTable` index changes, TiDB batches all generated-column
  additions currently pending for a table into one online `ALTER TABLE`, then
  submits the pending native index creations as one TiDB multi-schema
  `ALTER TABLE` DDL job.
  TiDB has no separate local-index physical path; GSI versus LSI remains
  DynamoDB API metadata.

- **Table statistics**: TiDB does not cache table size or item count in the
  ExtendDB table catalog. `DescribeTable` and backup metadata read TiDB's
  native `information_schema.tables` statistics when needed, keeping catalog
  rows as control-plane metadata rather than a stale data-plane counter cache.

- **String-key collation**: TiDB catalog and data metadata tables are created
  with `DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, and metrics labels use
  `ascii_bin`. Migrations convert existing TiDB metadata columns to those
  binary collations instead of only changing future table defaults.
  DynamoDB-visible names, ARNs, stream shard ids, and idempotency tokens
  therefore remain case-sensitive and byte-ordered without per-query collation
  overrides or dependence on the cluster/database default.

- **GSI consistency**: GSI write consistency is backend-specific. TiDB relies
  on native secondary indexes, which are maintained by TiDB from the base row.
  When a DynamoDB request supplies `IndexName`, TiDB read SQL forces the
  matching native secondary index because the client has already selected the
  access path.
  PostgreSQL can simulate asynchronous GSI propagation for compatibility
  testing. LSI updates are always synchronous. See §6 for details.

### 5.2 Connection Pooling

Backends own their native pool layout behind the storage traits. PostgreSQL
keeps catalog metadata, catalog-store/auth, and data pools. TiDB keeps separate
engine-catalog, catalog-store/auth, strong-data, and default-read data pools,
but the catalog and data databases must be in the same TiDB cluster. TiDB backup
metadata, BR `--backupts`, read-only snapshot reads, online DDL, and native TTL all rely
on one PD-owned global TSO timeline. TiDB startup validates the invariant by
comparing the catalog and data pools' native
`information_schema.cluster_info` topology fingerprints. If a TiDB edition does
not expose that topology table, the backend accepts only the same SQL endpoint
and user for both databases; different SQL endpoints or users must expose
native topology metadata so ExtendDB can prove they share one cluster. The
strong-data pool uses leader reads for writes and `ConsistentRead=true`; the
default-read pool sets
`tidb_replica_read = 'closest-adaptive'` for DynamoDB reads that did not request
`ConsistentRead=true`.

### 5.3 Read Consistency Model

DynamoDB supports two read consistency modes: strongly consistent and eventually
consistent (the default). The engine passes the request flag to `get_item`,
`query`, and `scan`; `BatchGetItem` passes each table's per-request flag.

**PostgreSQL:** all reads currently use the configured primary data pool. This
is stronger than the DynamoDB default and preserves existing behavior while the
trait carries the consistency signal for backends that can use it.

**TiDB:** `ConsistentRead=true` uses the strong data pool. Default reads use a
dedicated default-read pool with TiDB's `closest-adaptive` follower-read mode.
TiDB follower read is still strongly consistent, but it lets TiDB offload larger
read-only statements to local replicas and reduce leader/AZ pressure while
remaining valid for DynamoDB's weaker default read contract.

**Which operations are affected:**
- `GetItem`: uses `consistent_read` field (default `false` in DynamoDB)
- `Query`: uses `consistent_read` field (default `false` in DynamoDB)
- `Scan`: uses `consistent_read` field (default `false` in DynamoDB)
- `BatchGetItem`: uses per-table `consistent_read` field
- `TransactGetItems`: always strongly consistent (DynamoDB spec — serializable isolation)
- All write operations: always use the primary pool

### 5.4 Query Translation

The storage backend translates `KeyCondition` to SQL:

```rust
// KeyCondition { pk_name: "user_id", pk_value: "alice", sort: Some(BeginsWith("2024")) }
// →
// SELECT item_data FROM _ddb_Users WHERE pk = $1 AND sk_s >= $2 AND sk_s < $3
// params: ["alice", "2024", "2025"]  -- $3 is prefix with last char incremented
```

For sort key conditions:
| SortKeyCondition | SQL |
|-----------------|-----|
| `Eq(v)` | `sk_x = $n` |
| `Lt(v)` | `sk_x < $n` |
| `Le(v)` | `sk_x <= $n` |
| `Gt(v)` | `sk_x > $n` |
| `Ge(v)` | `sk_x >= $n` |
| `Between(a, b)` | `sk_x BETWEEN $n AND $m` |
| `BeginsWith(s)` | `sk_x >= $n AND sk_x < $m` (where `$m` = prefix upper bound, see algorithm below) |

> **Note on `sk_x`:** The actual column name (`sk_s`, `sk_n`, `sk_b`) is determined by the sort key's 
> `AttributeDefinition` type, looked up from table metadata at query time. `BeginsWith` only applies to `S` and `B`
> type sort keys.

> **Note on `BeginsWith`:** Using a range scan (`>= prefix AND < prefix_next`) instead of SQL `LIKE` avoids two
> problems: (1) `%` and `_` characters in the prefix would be interpreted as LIKE wildcards, causing incorrect matches;
> (2) range scans are more B-tree index friendly than LIKE patterns.

> **`BeginsWith` upper bound algorithm:** The upper bound is computed by stripping trailing `0xFF` bytes, then
> incrementing the last non-`0xFF` byte. If the prefix is entirely `0xFF` bytes, there is no upper bound (scan to end
> of partition). For string sort keys, operate on raw UTF-8 bytes, not characters.

```rust
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while upper.last() == Some(&0xFF) {
        upper.pop();
    }
    if upper.is_empty() {
        return None; // all 0xFF — no upper bound, scan to end
    }
    *upper.last_mut().unwrap() += 1;
    Some(upper)
}
// When None is returned, the SQL omits the upper bound:
//   sk_x >= $n  (no AND sk_x < $m)
```

### 5.5 Transaction Support

TransactWriteItems maps to a PostgreSQL transaction:

```rust
async fn transact_write_items(&self, input: TransactWriteInput) -> Result<...> {
    let mut tx = self.pool.begin().await?;
    for item in &input.items {
        match item {
            TransactWriteItem::Put { .. } => { /* INSERT/UPSERT within tx */ }
            TransactWriteItem::Delete { .. } => { /* DELETE within tx */ }
            TransactWriteItem::Update { .. } => { /* UPDATE within tx */ }
            TransactWriteItem::ConditionCheck { .. } => { /* SELECT + evaluate */ }
        }
    }
    tx.commit().await?;
    Ok(...)
}
```

### 5.6 Migrations

Migrations are embedded in the binary at compile time via `include_str!` and applied in order by the
`catalog::run_migrations` helper. Each migration is tracked in the `schema_history` table.

Migration files are numbered sequentially:
```
migrations/
└── 001_schema.sql
```

## 6. GSI Consistency Model

**Decision:** GSI consistency is backend-specific. TiDB uses native secondary
indexes maintained from the base table write. PostgreSQL can simulate
DynamoDB-style asynchronous GSI propagation with a configurable delay. LSI
updates are always synchronous.

**Implementation:**
- TiDB stores each item once and uses generated columns plus native secondary
  indexes, leveraging TiDB's globally consistent transaction model. Initial
  secondary indexes are created with the base table; create replay repairs an
  already-existing physical table through the same TiDB online `IF NOT EXISTS`
  DDL used for later changes. For each later reconciliation pass, generated key
  columns for all pending indexes on a table are added in one online
  `ALTER TABLE` DDL job before the native index DDL.
- Explicit TiDB index reads use `FORCE INDEX` for the generated native index.
  DynamoDB `IndexName` is not an optimizer suggestion; it is the requested read
  path, so stale TiDB statistics must not turn an index query into a table scan.
- The engine layer enforces DynamoDB projection semantics above the physical
  path: default index reads return projected attributes, GSI reads cannot ask
  for non-projected attributes, and LSI reads may fetch non-projected
  attributes from the base table. This keeps TiDB's single-row native-index
  layout from leaking full `item_data` through a KEYS_ONLY or INCLUDE GSI.
- TiDB write operations use the table's already-fetched `AttributeDefinitions`
  as a cheap guard before secondary-index key validation. Writes that cannot
  contain a candidate secondary-index key do not re-read the catalog; TiDB
  generated columns and native indexes maintain physical index state from the
  base row.
- TiDB storage pools set `tidb_txn_mode = 'pessimistic'` at connection time so
  row-level `SELECT ... FOR UPDATE` semantics do not depend on whether the
  cluster was freshly created or upgraded from an older optimistic default.
- PostgreSQL supports `gsi_propagation_delay_ms` for asynchronous compatibility
  testing.
- LSIs are always synchronous (delay is ignored) to match DynamoDB behavior

**Rationale:**
- **Leverages backend strengths**: TiDB can keep secondary indexes transactionally
  consistent without a worker queue.
- **Matches DynamoDB semantics where useful**: PostgreSQL can still simulate
  eventually consistent GSI propagation for compatibility testing.
- **Surfaces real bugs**: Applications that incorrectly assume immediate GSI
  consistency can be tested against the PostgreSQL async path.

**Trade-off:** Asynchronous GSI updates add complexity (queue, workers, delay
tracking) but provide higher fidelity to DynamoDB behavior. TiDB chooses the
simpler transactional path as the default and only path.

### 6.1 Table Status Enforcement

All data plane operations (PutItem, GetItem, Query, etc.) must check
`table_status` before proceeding. `CREATING` and `DELETING` tables cannot serve
data-plane traffic. Backends with online schema changes may continue to serve
data during `UPDATING`; TiDB does this because native online DDL maintains
writes while TiDB's DDL owner schedules generated-column, index, and TTL jobs.

Control-plane operations that modify table artifacts must:

1. Persist the durable intent in catalog metadata (`tables.table_status`,
   `indexes.index_status`, stream specification, TTL metadata, or table deletion
   state)
2. Let the backend reconciler or native database feature create, backfill,
   drop, or repair data artifacts from that durable intent
3. Publish completion by marking the artifact `ACTIVE`, returning the table to
   `ACTIVE`, or removing the catalog row

TiDB does not use `UPDATING` as an ExtendDB-level DDL mutex. Multiple frontend
nodes may append compatible GSI/TTL/delete intent while a table is already
`UPDATING`; the catalog row lock is held only for the short metadata mutation,
and TiDB's distributed online DDL queue owns the physical ordering.

### 6.1.1 Async Control Plane Transitions (Phase 1c)

Real DynamoDB control plane operations are not instantaneous — `CreateTable` returns `CREATING` status and the table
transitions to `ACTIVE` asynchronously. extenddb emulates this behavior while letting each backend use its native
coordination model.

**Implementation:**

- A `status_transition_at TIMESTAMPTZ` column on the `tables` table records when a pending transition should fire.
When `NULL`, no transition is pending.
- `CreateTable` inserts with `table_status = 'CREATING'` and sets `status_transition_at` according to backend policy.
PostgreSQL can set `NOW() + control_plane_delay_seconds` to emulate a fixed delay. TiDB sets immediate eligibility and
delegates physical schema scheduling to TiDB native online DDL.
- `DeleteTable` sets `table_status = 'DELETING'` with a transition time. The row, its indexes, and tags are removed
when the transition fires. TiDB uses immediate eligibility and idempotent `DROP TABLE IF EXISTS`.
- A background poller processes pending transitions. `CREATING → ACTIVE`
  creates any missing data artifacts before activation. `UPDATING → ACTIVE`
  reconciles pending GSI work and TiDB native TTL enable/disable intent. TiDB
  stream enablement only publishes stream metadata because shard IDs are derived
  from the fixed layout and sequence numbers come from TiDB MVCC commit
  timestamps plus an in-transaction ordinal; it does not need data-side shard
  rows or an async table-status transition.
  `DELETING → removed` drops data artifacts before
  deleting catalog metadata.
- On startup, `process_control_plane_transitions()` recovers any in-flight
  operations from a previous server instance.
- A backend-appropriate work index over pending table status and transition
  time keeps the poller query efficient regardless of table count.

**Design decisions and future direction (from Phase 1c human review):**

- The single-column approach works for base table lifecycle. Index-level
  transitions are represented by `indexes.index_status` while the parent table
  is `UPDATING`; TiDB may accept more catalog intent during that state and
  converges through native online DDL plus set-based conditional catalog
  publication for the pending index batch.
- Control-plane operations wake the poller immediately. TiDB keeps the idle
  sweep only as crash-recovery insurance; it is not a DDL ownership mechanism.
- Backends that simulate delay may randomize it to `[5, 20]` seconds for more realistic DynamoDB emulation. TiDB must not add an ExtendDB delay because TiDB already owns distributed online DDL scheduling.
- Startup recovery replays durable TiDB catalog intent directly. Backends that
  intentionally simulate DynamoDB delay may choose to reschedule future
  transition timestamps instead.
- `control_plane_delay_seconds` is a backend-specific runtime setting (0–300 range), managed via
  `extenddb settings set`. It is not a `.toml` config key. TiDB ignores it because TiDB's own DDL owner already
  coordinates distributed online schema changes.
- TiDB does not elect an ExtendDB DDL owner. Multiple frontend nodes may replay
  the same catalog intent concurrently; idempotent `IF EXISTS` / `IF NOT EXISTS`
  DDL and conditional catalog publication converge on the TiDB-owned schema
  state.

**Crash recovery and in-flight operation tracking:**

The `status_transition_at` column on the `tables` table serves as the
in-flight operation tracker. When the extenddb server shuts down (cleanly or
via crash) while tables have pending transitions, the state is durable in
the backend catalog. On the next startup, `process_control_plane_transitions()` scans
for due transitions and completes them from durable catalog intent. TiDB writes
immediate transition timestamps, so startup recovery submits any remaining
online DDL without waiting for an ExtendDB timer. Rows where
`status_transition_at` is in the future are left for the background poller on
backends that intentionally emulate a delay.

This column-on-tables approach is sufficient while control plane operations are scoped to one table
(`CREATING → ACTIVE`, `UPDATING → ACTIVE`, `DELETING → removed`). A separate `control_plane_operations` table becomes
necessary when:
- Operations span multiple tables or accounts
- Operations have intermediate states beyond a single status flip (e.g., multi-step UpdateTable)
- Audit or observability requires a history of completed operations, not just pending ones

Until those requirements arise, the single-column approach avoids the complexity of a separate job queue while providing
full crash recovery.

### 6.2 GSI Backfill on CreateIndex

When `UpdateTable` adds a new GSI to a table with existing data:

1. Merge the new GSI key `AttributeDefinitions` into the table catalog, set the parent table to `UPDATING`, and insert the new index with `index_status = 'CREATING'`
2. Commit the catalog transaction so the pending operation is durable; another frontend may append another compatible
   update while the table remains `UPDATING`
3. The control-plane reconciler performs the backend-native physical work
4. TiDB adds generated key columns for all currently pending indexes on the
   table in one online `ALTER TABLE`, then submits all pending native secondary
   indexes in one TiDB multi-schema `ALTER TABLE`; the split is intentional
   because TiDB validates a multi-change `ALTER TABLE` against the starting
   schema, so an `ADD INDEX` must not depend on a generated column introduced
   earlier in the same statement. TiDB's online DDL backfills and maintains the
   indexes from the base table, so ExtendDB does not run a separate item-replay
   backfill. Initial `CreateTable` indexes avoid this follow-up path because
   they are part of the physical `CREATE TABLE`; if replay observes that the
   table already exists, it repairs missing index artifacts with the same
   online `IF NOT EXISTS` DDL before activation. GSI deletes drop the native
   indexes and their generated key columns in one multi-schema online
   `ALTER TABLE`, because those objects already exist when the drop starts.
   Concurrent reconcilers replay the same `IF [NOT] EXISTS` DDL and let TiDB
   converge the schema. The entire per-table replay plan is retried on TiDB
   transient write conflicts, schema-version races, lock waits, and deadlocks,
   so the retry boundary is the durable catalog intent rather than a single
   partially completed SQL statement. After physical DDL returns, TiDB
   publishes all still pending index rows for that batch with one conditional
   catalog statement, rather than probing or publishing each index separately.
5. PostgreSQL creates and backfills its companion index table
6. On completion, the reconciler marks the index `ACTIVE` and returns the table to `ACTIVE`
7. Queries against a `CREATING` index return `ResourceNotFoundException` (matching DynamoDB behavior)

## 7. Pagination Token Encoding

`ExclusiveStartKey` and `LastEvaluatedKey` use the same format: a map of key attribute names to `AttributeValue`s,
serialized as standard DynamoDB JSON.

```rust
/// LastEvaluatedKey is the primary key of the last item evaluated.
/// For a base table: { "pk_name": {"S": "val"}, "sk_name": {"N": "42"} }
/// For a GSI: { "gsi_pk": {"S": "val"}, "gsi_sk": {"S": "val"}, "table_pk": {"S": "val"}, "table_sk": {"N": "42"} }
pub type PaginationKey = BTreeMap<String, AttributeValue>;
```

The storage backend translates this to a SQL `WHERE` clause:

**Base table pagination (forward scan):**
```sql
WHERE (pk = $last_pk AND sk_n > $last_sk)
   OR pk > $last_pk
```

**Base table pagination (reverse scan):**
```sql
WHERE (pk = $last_pk AND sk_n < $last_sk)
   OR pk < $last_pk
```

**GSI pagination (forward scan):**
GSI keys are not unique, so the base table primary key is used as a tiebreaker:
```sql
WHERE (pk = $gsi_pk AND sk_s > $gsi_sk)
   OR (pk = $gsi_pk AND sk_s = $gsi_sk AND base_pk > $base_pk)
   OR (pk = $gsi_pk AND sk_s = $gsi_sk AND base_pk = $base_pk AND base_sk_n > $base_sk)
   OR pk > $gsi_pk
```

For GSI queries, the pagination key includes both the GSI key attributes and the base table primary key (needed to
uniquely identify the position, since GSI keys are not unique). Backends must preserve this tie-breaker. PostgreSQL
stores `base_pk` and `base_sk_*` as actual columns in companion GSI tables. TiDB keeps the seek predicate's base-key
tie-breaker but orders only by the native secondary-index key columns; TiDB's secondary-index entry already carries the
clustered row handle, so this preserves duplicate-key pagination while allowing TiDB to serve `ORDER BY` and `LIMIT` from
the ordered index scan instead of a root sort.

## 8. Parallel Scan Segment Assignment

`Segment` and `TotalSegments` map to PostgreSQL via hash-based partitioning of the primary key:

```sql
-- Scan segment 2 of 4 total segments:
SELECT item_data FROM _ddb_Users
WHERE (hashtext(pk)::bigint & x'7FFFFFFF'::bigint) % 4 = 2
ORDER BY pk, sk_s
LIMIT $limit;
```

`hashtext()` is a built-in PostgreSQL function that produces a deterministic
int32 hash. We cast to `bigint` and mask with `0x7FFFFFFF` to ensure a
non-negative result (avoiding the `abs(INT_MIN)` overflow edge case where
`abs(-2147483648)` returns a negative value in PostgreSQL). Using modulo
arithmetic assigns each partition key to exactly one segment, ensuring:
- Every item appears in exactly one segment (no duplicates, no gaps)
- Segments can be scanned in parallel by independent workers
- The assignment is deterministic (same item always in same segment)

> **Portability note:** `hashtext()` is PostgreSQL-specific. Segment
> assignment is not guaranteed to be consistent across different storage
> backends. If cross-backend consistency is needed in the future, define a
> hash function in the `core` crate (e.g., CRC32 of the partition key bytes)
> that all backends use, and pass the pre-computed segment filter to the
> storage backend.

## 9. Idempotency Token Storage

`TransactWriteItems` supports `ClientRequestToken` for idempotency. Tokens are stored in a dedicated table:

```sql
CREATE TABLE _dynamodb_idempotency_tokens (
    client_request_token <string> PRIMARY KEY,
    response <json> NOT NULL,
    created_at <timestamp> NOT NULL DEFAULT <current_timestamp>
);
```

**Flow:**
1. At the start of the backend transaction, claim the token with an atomic
   insert into a unique-key token table.
2. If the insert succeeds, execute the transaction and commit the token claim
   atomically with the item writes.
3. If the database reports a native unique-key conflict, lock/read the existing
   token row. Matching fingerprints are idempotent replays; different
   fingerprints are `IdempotentParameterMismatchException`.
4. Backend-native retention removes tokens older than 10 minutes (matching
   DynamoDB's idempotency window). TiDB uses native table TTL, so it does not
   add a frontend cleanup index on `created_at`.

Storage backends must not implement this as a preflight `SELECT` followed by
`INSERT`: under multiple frontend writers, both transactions can observe a
missing row. The unique key is the distributed race detector.

TiDB uses the token as the retry boundary for retryable write-conflict,
deadlock, lock-timeout, and schema-change errors. When a token is present,
the TiDB backend can retry the whole `TransactWriteItems` operation: if the
previous attempt committed, the retry observes the token as an idempotent
replay; if it rolled back, TiDB re-executes the transaction. Without a client
request token, TiDB does not broaden automatic write retries because a commit
outcome could be ambiguous for non-idempotent updates.
TiDB stores this token table in the data database, not the catalog database,
so the token claim, item writes, and stream records share one TiDB transaction.
TiDB does not add a `created_at` lookup index or run a foreground cleanup
delete for token cleanup: the write path uses one primary-key
`INSERT ... ON DUPLICATE KEY UPDATE` claim with a per-attempt `claim_id`, and
native TTL owns retention inside TiDB. If a same-token row has passed the
10-minute DynamoDB idempotency window but TiDB's TTL job has not deleted it
yet, the upsert atomically recycles the row; otherwise the unchanged `claim_id`
distinguishes replay from mismatch.

## 10. Backend Plugin Architecture

ExtendDB uses a factory pattern with compile-time registration to enable
pluggable storage backends. The `bin` crate selects a backend by name, invokes
its factory function, and receives trait objects that are passed to the server
and engine layers.

### 10.1 ServerComponents

Backends return a `ServerComponents` struct containing all trait objects needed by the server:

```rust
pub struct ServerComponents {
    /// Storage engine for all data/metadata operations
    pub engine: Arc<dyn StorageEngine>,
    
    /// Catalog store for management API operations
    pub catalog_store: Arc<dyn CatalogStore>,
    
    /// Auth provider (wraps credential store internally)
    pub auth_provider: Arc<dyn AuthProvider>,
    
    /// Optional backend-specific runtime hooks for worker spawning
    pub runtime_hooks: Option<Box<dyn ServerRuntimeHooks>>,
}
```

### 10.2 Factory Function Type

```rust
pub type ServerComponentsFactory =
    fn(
        &dyn StorageConfig,
        &str,
    ) -> Pin<Box<dyn Future<Output = Result<ServerComponents, BackendError>> + Send>>;
```

The factory receives:
- `&dyn StorageConfig`: Connection string, pool size, and other backend-agnostic config
- `&str`: AWS region for ARN construction

It returns a `Future` that resolves to `ServerComponents` or `BackendError`.

### 10.3 Backend Registration with inventory

Backends register themselves using the `inventory` crate for compile-time registration:

```rust
// In crates/storage-postgres/src/lib.rs
inventory::submit! {
    ServerComponentsRegistration {
        backend: "postgres",
        factory: |config, region| {
            Box::pin(async move {
                // Extract config
                let connection_string = config.connection_config().to_string();
                let max_connections = config.max_connections();
                
                // Create PostgreSQL engine
                let pg_config = PostgresConfig {
                    connection_string: connection_string.clone(),
                    pool_size: max_connections,
                    max_item_size_bytes: 400_000,
                };
                
                let engine = PostgresEngine::new(&pg_config, region)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "postgres".to_string(),
                        details: e.to_string(),
                    })?;
                
                // Verify catalog version
                engine.check_catalog_version().await?;
                
                // Recover control plane state
                engine.process_control_plane_transitions().await?;
                
                let engine = Arc::new(engine);
                
                // Create catalog store (same engine, different Arc)
                let catalog_store = engine.clone() as Arc<dyn CatalogStore>;
                
                // Create auth provider
                let credential_adapter = Arc::new(StorageCredentialAdapter::new(
                    engine.clone()
                ));
                let auth_provider = Arc::new(BuiltinAuthProvider::new(
                    credential_adapter,
                    catalog_store.clone(),
                ));
                
                // Create runtime hooks
                let runtime_hooks = Some(Box::new(PostgresRuntimeHooks::new(
                    engine.clone(),
                    /* ... backend-specific state ... */
                )) as Box<dyn ServerRuntimeHooks>);
                
                Ok(ServerComponents {
                    engine,
                    catalog_store,
                    auth_provider,
                    runtime_hooks,
                })
            })
        },
    }
}
```

The `inventory` crate collects all registrations at compile time. The `storage`
crate provides `create_server_components(backend_name, config, region)` which
looks up the matching factory and invokes it.

### 10.4 Backend Selection in cmd_serve

```rust
// In bin/src/cmd_serve.rs
let components = create_server_components(
    &config.storage.backend,  // "postgres"
    &config.storage,
    &config.server.region,
)
.await?;

// Pass trait objects to server
let app_state = AppState {
    storage: components.engine.clone(),
    auth: components.auth_provider,
    catalog_store: Some(components.catalog_store.clone()),
    metrics: Arc::new(MetricsCollector::new()),
    // ...
};

// Spawn backend-agnostic workers (6 workers)
spawn_backend_agnostic_workers(&app_state);

// Spawn backend-specific workers (if any)
if let Some(hooks) = components.runtime_hooks {
    let ctx = WorkerContext {
        metrics: app_state.metrics.clone(),
        catalog_store: components.catalog_store.clone(),
        reload_handle: reload_handle.clone(),
        config_log_level: config.logging.level.clone(),
    };
    hooks.spawn_workers(&ctx).await;
}

// Start HTTP server
server.run().await?;
```

The `cmd_serve` module has no PostgreSQL imports or dependencies. It receives trait objects and calls their methods.

### 10.5 RuntimeHooks: Backend-Specific Workers

Backends implement `ServerRuntimeHooks` to spawn workers that need access to backend-specific state:

```rust
#[async_trait]
pub trait ServerRuntimeHooks: Send + Sync {
    /// Spawn backend-specific workers.
    ///
    /// Called after server components are created but before the HTTP server
    /// starts. Backends spawn workers that need access to backend-specific
    /// state (connection pools, notify handles, etc.).
    async fn spawn_workers(&self, ctx: &WorkerContext);
    
    /// Get backend-specific info for logging (optional).
    fn backend_info(&self) -> Option<String> {
        None
    }
}

pub struct WorkerContext {
    pub metrics: Arc<MetricsCollector>,
    pub catalog_store: Arc<dyn CatalogStore>,
    pub reload_handle: reload::Handle<EnvFilter, Registry>,
    pub config_log_level: String,
}
```

**Worker classification:**

**Backend-agnostic workers** (spawned in `cmd_serve`, use trait methods only):
1. `poll_log_level` — uses `SettingsStore::get_setting`
2. `poll_throttling_enabled` — uses `SettingsStore::get_setting`
3. `metrics_prune_worker` — uses in-memory `MetricsCollector`
4. `metrics_flush_worker` — uses `MetricsStore::insert_metrics`
5. `capacity_warning_worker` — uses in-memory metrics
6. `login_attempt_cleanup_worker` — uses `RateLimitStore::cleanup_old_attempts`

**Backend-specific workers** (spawned via `RuntimeHooks`, access backend
internals):
- PostgreSQL spawns its control-plane poller, pool metrics, GSI delay poller,
  TTL cleanup, stream cleanup, idempotency token cleanup, and table size refresh
  workers
- TiDB spawns only its control-plane poller and pool metrics workers; TiDB
  online DDL owns schema jobs, TiDB native TTL handles all item TTL plus
  stream-record, idempotency-token, metrics, login-attempt, and assume-role
  session retention, startup repair re-enables native TTL jobs if TiDB tooling
  left `TTL_ENABLE = 'OFF'`, one-time data migrations pre-split shared write
  tables with TiDB Region split/scatter, and TiDB `information_schema` table
  statistics are read on demand instead of refreshed by a frontend worker.
- Runtime hooks also expose backend readiness to `/health`, so TiDB checks every
  pool opened by the frontend instead of reporting web-process liveness only.
- Other backends may spawn different workers or none at all

Example PostgreSQL implementation:

```rust
impl ServerRuntimeHooks for PostgresRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
        // Control plane poller (uses PostgreSQL LISTEN/NOTIFY)
        let engine = self.engine.clone();
        let notify = self.control_plane_notify.clone();
        tokio::spawn(async move {
            workers::poll_control_plane(engine, notify).await
        });
        
        // Pool metrics (accesses PostgreSQL connection pool internals)
        let engine = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            workers::report_pool_metrics(engine, metrics).await
        });
        
        // ... 5 more workers ...
    }
    
    fn backend_info(&self) -> Option<String> {
        Some(format!("data_db={}", self.data_db_name))
    }
}
```

### 10.6 BoxFuture Pattern for Object Safety

All storage traits use explicit `BoxFuture` return types for object safety:

```rust
use futures::future::BoxFuture;

pub trait TableEngine: Send + Sync {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;
}
```

**Why BoxFuture:**
- **Object safety**: Enables `Arc<dyn StorageEngine>` usage
- **Explicit lifetimes**: control-plane futures can borrow from `&self`; data-plane futures can borrow request metadata until awaited
- **No macro overhead**: No `#[async_trait]` macro expansion

**Implementation pattern:**

```rust
impl TableEngine for PostgresEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        // Owned request payloads can be moved into the future.
        let account_id = account_id.to_string();

        Box::pin(async move {
            self.create_table_impl(&account_id, input).await
        })
    }
}
```

Hot data-plane methods should return their implementation future directly so
borrowed metadata stays borrowed instead of cloned:

```rust
impl DataEngine for MyBackend {
    fn get_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        Box::pin(self.get_item_impl(key_info, key))
    }
}
```

The `CredentialStore` trait in the `auth` crate uses `#[async_trait]` instead
(older pattern). The `bin` crate bridges the two patterns with a thin adapter.

## 11. Adding a New Backend

To add a new storage backend (e.g., SQLite):

### 11.1 Create Backend Crate

```
crates/storage-sqlite/
├── Cargo.toml
└── src/
    ├── lib.rs              # SqliteEngine struct, trait impls, factory registration
    ├── table_engine.rs     # TableEngine implementation
    ├── data_engine.rs      # DataEngine implementation
    ├── metadata_engine.rs  # MetadataEngine implementation
    ├── stream_engine.rs    # StreamEngine implementation
    ├── worker_store.rs     # WorkerStore implementation
    ├── backup_engine.rs    # BackupEngine implementation
    ├── management_store.rs # ManagementStore implementation
    ├── admin_store.rs      # AdminStore implementation
    ├── settings_store.rs   # SettingsStore implementation
    ├── metrics_store.rs    # MetricsStore implementation
    ├── rate_limit_store.rs # RateLimitStore implementation
    ├── authorization_store.rs # AuthorizationStore implementation
    ├── bootstrapper.rs     # Bootstrapper implementation
    ├── hooks.rs            # ServerRuntimeHooks implementation (optional)
    └── workers.rs          # Background worker functions (if needed)
```

### 11.2 Implement All Traits

Implement all 13 storage traits listed in Section 2. Use the PostgreSQL
implementation (`crates/storage-postgres/`) as a reference.

**Required traits:**
- `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore`, `BackupEngine`
- `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`
- `Bootstrapper`

**Composite traits** (implement with empty bodies):
```rust
impl StorageEngine for SqliteEngine {}
impl CatalogStore for SqliteEngine {}
```

### 11.3 Register Factory

In `lib.rs`:

```rust
use extenddb_storage::{
    ServerComponents, ServerComponentsRegistration, BackendError,
    StorageConfig, StorageEngine, CatalogStore,
};
use extenddb_auth::{BuiltinAuthProvider, CredentialStore};

inventory::submit! {
    ServerComponentsRegistration {
        backend: "sqlite",
        factory: |config, region| {
            Box::pin(async move {
                // Extract config
                let connection_string = config.connection_config().to_string();
                
                // Initialize SQLite engine
                let engine = SqliteEngine::new(&connection_string, region)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "sqlite".to_string(),
                        details: e.to_string(),
                    })?;
                
                // Verify catalog version
                engine.check_catalog_version().await?;
                
                let engine = Arc::new(engine);
                
                // Create catalog store
                let catalog_store = engine.clone() as Arc<dyn CatalogStore>;
                
                // Create auth provider
                let credential_adapter = Arc::new(StorageCredentialAdapter::new(
                    engine.clone()
                ));
                let auth_provider = Arc::new(BuiltinAuthProvider::new(
                    credential_adapter,
                    catalog_store.clone(),
                ));
                
                // Create runtime hooks (optional)
                let runtime_hooks = if needs_backend_workers() {
                    Some(Box::new(SqliteRuntimeHooks::new(engine.clone()))
                        as Box<dyn ServerRuntimeHooks>)
                } else {
                    None
                };
                
                Ok(ServerComponents {
                    engine,
                    catalog_store,
                    auth_provider,
                    runtime_hooks,
                })
            })
        },
    }
}
```

### 11.4 Update bin Crate Cargo.toml

Add the new backend as a dependency:

```toml
[dependencies]
extenddb-storage-sqlite = { path = "../storage-sqlite" }
```

This ensures the backend's `inventory::submit!` registration is linked into the binary.

### 11.5 Test

Run the full test suite:

```bash
# Build with new backend
cargo build --release

# Initialize with SQLite backend
./target/release/extenddb init --config extenddb.toml
# (Edit extenddb.toml to set storage.backend = "sqlite")

# Start server
./target/release/extenddb serve --config extenddb.toml

# Run tests
cargo test --workspace
./devtools/run-tests --extenddb --all
```

### 11.6 RuntimeHooks Decision Tree

**Does your backend need `ServerRuntimeHooks`?**

**YES** if your backend needs workers that:
- Access backend-specific state (connection pools, notify handles, internal queues)
- Use backend-specific APIs (PostgreSQL LISTEN/NOTIFY, Cassandra token ranges)
- Perform backend-specific maintenance (connection pool metrics, backend-specific cleanup)

**NO** if your backend:
- Only needs operations available through storage traits (use backend-agnostic workers)
- Has no background maintenance requirements
- Delegates all background work to the database itself

**Example: PostgreSQL needs RuntimeHooks** because it spawns 7 workers that
access PostgreSQL-specific state (connection pools, LISTEN/NOTIFY, expression
indexes for TTL).

**Example: A hypothetical DynamoDB-backed backend would NOT need RuntimeHooks**
because DynamoDB handles all background work internally (TTL, streams,
backups).

### 11.7 Design Rationale

**Why factory pattern instead of direct construction?**
- `cmd_serve` remains backend-agnostic (no PostgreSQL imports)
- Adding a new backend requires zero changes to `cmd_serve`
- Backend-specific initialization logic stays in the backend crate

**Why trait objects instead of generics?**
- Single server implementation (no monomorphization bloat)
- Fast incremental builds (changing a backend doesn't recompile the server)
- Small binary size (multiple backends don't multiply binary size)
- Runtime backend selection (choose backend from config file)

**Why separate StorageEngine and CatalogStore?**
- Interface Segregation Principle: components depend only on traits they need
- Management API only needs catalog operations, not data operations
- Auth provider only needs credential lookup, not table operations
- Easier testing (can mock just the catalog store)

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
