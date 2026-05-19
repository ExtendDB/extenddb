# extenddb — Component Design: Storage

**Version:** 3.0
**Date:** 2026-05-19
**Status:** Active
**Crates:** `storage` (traits), `storage-postgres` (PostgreSQL backend)

## 1. Purpose

The storage layer provides a trait-based abstraction for all persistent data operations. Traits are defined in the
`storage` crate with no database-specific dependencies. Backend implementations live in separate crates (e.g.,
`storage-postgres`) and register themselves via a factory pattern using the `inventory` crate.

The trait-based design allows new storage backends to be added by implementing the storage traits and registering a
factory function, with no changes needed to the `engine` or `server` crates. The factory pattern enables runtime
backend selection based on configuration.

**Current status**: PostgreSQL is the only supported backend. The trait architecture and plugin infrastructure provide
the foundation for future backend implementations.

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
```

The `engine` crate receives `Arc<dyn StorageEngine>` for data operations. The `server` crate receives
`Arc<dyn CatalogStore>` for management API operations. This separation ensures components depend only
on the traits they need.

### 2.3 BoxFuture Pattern

All storage traits use `BoxFuture<'_, Result<T, StorageError>>` return types:

```rust
use futures::future::BoxFuture;

pub trait TableEngine: Send + Sync {
    fn create_table(&self, account_id: &str, input: CreateTableInput)
        -> BoxFuture<'_, Result<TableDescription, StorageError>>;
}
```

This pattern provides:
- **Object safety**: Traits can be used as `Arc<dyn Trait>`
- **Explicit lifetimes**: `BoxFuture<'_>` borrows from `&self`
- **No macro overhead**: No `#[async_trait]` macro expansion

The `CredentialStore` trait in the `auth` crate uses `#[async_trait]` instead
(older pattern). The `bin` crate bridges the two patterns with a thin adapter.

### 2.4 Key Trait Methods

**TableEngine** (table lifecycle):
- `create_table`, `delete_table`, `describe_table`, `list_tables`, `update_table`
- `table_key_info` — returns lightweight metadata (key schema, attribute definitions)
  for data operations, avoiding the overhead of a full `describe_table` call
- `index_info` — returns GSI/LSI metadata for query operations

All table operations are scoped by `account_id` for multi-account isolation.

**DataEngine** (item CRUD, query, scan, transactions):
- `put_item`, `get_item`, `delete_item`, `update_item`
- `query`, `scan`
- `transact_get_items`, `transact_write_items`
- `cleanup_expired_idempotency_tokens`

Data operations receive `TableKeyInfo` from the engine layer, which has already
validated the table exists and is ACTIVE. Condition expressions are evaluated
inside the storage transaction to prevent TOCTOU races. Stream records are
written atomically with data writes when `stream` is `Some`.

**MetadataEngine** (TTL, tags, table statistics):
- `describe_ttl`, `update_ttl`
- `tag_resource`, `untag_resource`, `list_tags`
- `refresh_table_size` — updates cached table size and item count
- `create_ttl_index`, `find_expired_items_indexed` — TTL cleanup support

**StreamEngine** (DynamoDB Streams):
- `write_stream_record` — writes stream record atomically with data write
- `get_stream_records` — retrieves stream records for a shard
- `describe_stream`, `list_streams`
- `cleanup_expired_stream_records` — removes records older than 24 hours

**WorkerStore** (background worker operations):
- `process_control_plane_transitions` — handles table state transitions
  (CREATING → ACTIVE, DELETING → deleted)

**BackupEngine** (backup and restore):
- `create_backup`, `describe_backup`, `list_backups`, `delete_backup`
- `restore_table_from_backup`

```rust
pub trait WorkerStore: Send + Sync {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>>;
}
```

The `WorkerStore` trait provides operations needed by background workers. The `process_control_plane_transitions`
method handles table state transitions (CREATING → ACTIVE, DELETING → deleted) and is called by the control plane
poller worker.

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

Settings include `control_plane_delay_seconds`, `log_level`, and the
encryption key for access key secrets.

### MetricsStore

Historical metrics persistence and query:
- `insert_metrics`, `query_metrics`, `prune_metrics`

Metrics are flushed periodically from the in-memory collector to persistent
storage.

### RateLimitStore

Login rate limiting and account lockout:
- `count_principal_failures`, `count_ip_failures`, `record_failed_login`,
  `cleanup_old_attempts`

Tracks failed login attempts by principal and source IP to mitigate clients
sending excessive traffic.

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

### Error Types

**StorageError**: Errors returned by storage trait methods:
- `TableNotFound(String)` — table does not exist
- `TableAlreadyExists(String)` — table already exists
- `TableNotActive(String)` — table is CREATING, DELETING, or UPDATING
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
- `_ddb_<index_id>` — GSI/LSI projection tables (index_id is a UUID)

**Schema files:**
- Catalog schema: `crates/storage-postgres/migrations/001_schema.sql`
- Data table DDL generation: `crates/storage-postgres/src/data/ddl.rs`
- Table name helpers: `crates/storage-postgres/src/data/mod.rs`

**Design notes:**

- **Table naming**: Physical PostgreSQL tables use UUIDs instead of
  user-provided names. `table_id` and `index_id` are generated with
  `uuid::Uuid::new_v4()` and stored in the catalog. This avoids SQL injection
  risks and allows DynamoDB table names to use any characters (including
  Unicode, spaces, SQL keywords). The `_ddb_` prefix prevents collisions with
  catalog tables.

- **Partition key storage**: Partition key values are always stored as TEXT.
  String keys store directly, number keys store their string representation,
  binary keys store base64. Partition keys only need equality comparison, so
  text storage is correct. **Important:** Binary partition keys must use
  canonical base64 encoding (standard alphabet with padding, via
  `base64::engine::general_purpose::STANDARD`) to ensure equality comparison
  is reliable. A validation step on ingest must normalize the encoding.

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

- **GSI tables**: GSI tables include base table primary key columns (`base_pk`,
  `base_sk_*`) as actual SQL columns (not just inside `item_data` JSONB). This
  is required because: (1) GSI keys are not unique — two base table items can
  project to the same GSI key, so the base table PK is needed for uniqueness;
  (2) pagination requires a tiebreaker when GSI keys collide; (3) the base
  table PK is needed to look up the full item for projections.

- **GSI consistency**: GSI updates are asynchronous by default (10ms delay) to
  match DynamoDB behavior. LSI updates are always synchronous. See §6 for
  details on the propagation delay model.

### 5.2 Connection Pooling

```rust
use sqlx::postgres::PgPoolOptions;

pub struct PostgresEngine {
    /// Primary connection pool — used for all writes and consistent reads.
    pool: PgPool,
    /// Optional read replica pool — used for eventually consistent reads
    /// (ConsistentRead=false). When None, all reads use the primary pool.
    read_pool: Option<PgPool>,
}

impl PostgresEngine {
    pub async fn new(config: &PostgresConfig) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.pool_size)
            .connect(&config.connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let read_pool = match &config.read_replica_url {
            Some(url) => {
                let rp = PgPoolOptions::new()
                    .max_connections(config.read_replica_pool_size.unwrap_or(config.pool_size))
                    .connect(url)
                    .await
                    .map_err(|e| StorageError::Connection(format!("read replica: {e}")))?;
                Some(rp)
            }
            None => None,
        };

        Ok(Self { pool, read_pool })
    }

    /// Returns the appropriate connection pool for a read operation.
    /// Uses the read replica for eventually consistent reads (when available),
    /// falls back to the primary pool otherwise.
    fn read_pool(&self, consistent_read: bool) -> &PgPool {
        if consistent_read {
            &self.pool
        } else {
            self.read_pool.as_ref().unwrap_or(&self.pool)
        }
    }
}
```

### 5.3 Read Consistency Model

DynamoDB supports two read consistency modes: strongly consistent and eventually consistent (the default). The
PostgreSQL backend models this via an optional read replica.

**Single-node mode (no read replica configured):** All reads are strongly consistent regardless of the `consistent_read`
flag. This is strictly stronger than the DynamoDB spec and compatible with all applications. The `consistent_read`
parameter is accepted and correctly reflected in capacity calculations (eventually consistent reads consume 0.5 RCU vs
1.0 RCU for strongly consistent).

**Read replica mode (`read_replica_url` configured):** Eventually consistent reads (`consistent_read=false`) are routed
to a PostgreSQL streaming replica that is naturally a few milliseconds behind the primary. Strongly consistent reads
(`consistent_read=true`) always read from the primary. This mirrors how DynamoDB achieves eventual consistency
— via storage node replicas — and surfaces the exact class of bugs that applications may encounter in production
DynamoDB (e.g., read-after-write without `ConsistentRead=true` returning stale data).

**Which operations are affected:**
- `GetItem`: uses `consistent_read` field (default `false` in DynamoDB)
- `Query`: uses `consistent_read` field (default `false` in DynamoDB)
- `Scan`: uses `consistent_read` field (default `false` in DynamoDB)
- `BatchGetItem`: uses per-table `consistent_read` field
- `TransactGetItems`: always strongly consistent (DynamoDB spec — serializable isolation)
- All write operations: always use the primary pool

The `read_pool()` helper method on `PostgresEngine` encapsulates this routing. All read implementations call
`self.read_pool(input.consistent_read)` instead of `&self.pool` directly.

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
└── 001_initial_schema.sql
```

## 6. GSI Consistency Model

**Decision:** GSI updates are **asynchronous by default** with a configurable
propagation delay. LSI updates are always synchronous.

**Implementation:**
- Each GSI has an optional `propagation_delay_ms` column in the `indexes` table
- If `propagation_delay_ms` is `NULL` or negative, the system default is used
  (default: 10ms, configurable via `gsi_propagation_delay_ms` setting)
- If `propagation_delay_ms` is `0`, the GSI is updated synchronously in the
  same transaction as the base table write
- If `propagation_delay_ms` is positive, the GSI update is enqueued and applied
  after a random delay within `[0, propagation_delay_ms]`
- LSIs are always synchronous (delay is ignored) to match DynamoDB behavior

**Rationale:**
- **Matches DynamoDB semantics**: Real DynamoDB GSIs are eventually consistent
  with propagation delays typically in the range of milliseconds to seconds
- **Surfaces real bugs**: Applications that incorrectly assume immediate GSI
  consistency will fail in ExtendDB just as they would in production DynamoDB
- **Configurable**: Can be set to 0ms for synchronous behavior when needed for
  testing or specific use cases

**Trade-off:** Asynchronous GSI updates add complexity (queue, workers, delay
tracking) but provide higher fidelity to DynamoDB behavior. The synchronous
mode (delay=0) is available for applications that need it.

### 6.1 Table Status Enforcement

All data plane operations (PutItem, GetItem, Query, etc.) must check `table_status` before proceeding. If the table is
not `ACTIVE`, return `StorageError::TableNotActive` (mapped to `ResourceInUseException`). Control plane operations that
modify the table (`UpdateTable`, `DeleteTable`) must:

1. Atomically set `table_status` to `UPDATING` or `DELETING` (using `UPDATE ... WHERE table_status = 'ACTIVE'` — if
 zero rows affected, the table is already being modified, return `ResourceInUseException`)
2. Perform the operation
3. Set `table_status` back to `ACTIVE` (or remove the row for `DeleteTable`)

This prevents concurrent DDL operations on the same table.

### 6.1.1 Async Control Plane Transitions (Phase 1c)

Real DynamoDB control plane operations are not instantaneous — `CreateTable` returns `CREATING` status and the table
transitions to `ACTIVE` asynchronously. extenddb emulates this behavior.

**Implementation:**

- A `status_transition_at TIMESTAMPTZ` column on the `tables` table records when a pending transition should fire.
When `NULL`, no transition is pending.
- `CreateTable` inserts with `table_status = 'CREATING'` and sets `status_transition_at` to
`NOW() + control_plane_delay_seconds`. The delay is read from the settings table via a subquery in the same INSERT
(no extra round-trip).
- `DeleteTable` sets `table_status = 'DELETING'` with a scheduled transition time. The row, its indexes, and tags are
removed when the transition fires.
- A background poller processes pending transitions. `CREATING → ACTIVE` is a single UPDATE; `DELETING → removed`
uses `DELETE ... FOR UPDATE SKIP LOCKED ... RETURNING` for concurrent safety.
- On startup, `process_control_plane_transitions()` recovers any in-flight
  operations from a previous server instance.
- A partial index (`idx_tables_pending_transition ON tables
  (status_transition_at) WHERE status_transition_at IS NOT NULL`) keeps the
  poller query efficient regardless of table count.

**Design decisions and future direction (from Phase 1c human review):**

- The single-column approach works because each table has exactly one pending status transition at a time. Index-level
transitions (e.g., GSI backfill) will need a separate `status_transition_at`
on the `indexes` table when GSI operations are implemented.
- The poller interval will be increased to 10 seconds at idle, with control
  plane operations poking the poller to wake up immediately and backoff
  appropriately (Phase 2).
- The default delay will be randomized to `[5, 20]` seconds for more realistic
  DynamoDB emulation (Phase 2).
- Startup recovery will reset stuck `CREATING` tables to
  `NOW() + random[5, 20]` instead of instant activation (Phase 2).
- `control_plane_delay_seconds` is a runtime setting (0–300 range), managed
  via `extenddb settings set`. It is not a `.toml` config key.

**Crash recovery and in-flight operation tracking:**

The `status_transition_at` column on the `tables` table serves as the
in-flight operation tracker. When the extenddb server shuts down (cleanly or
via crash) while tables have pending transitions, the state is durable in
PostgreSQL. On the next startup, `process_control_plane_transitions()` scans
for rows where `status_transition_at IS NOT NULL AND
status_transition_at <= NOW()` and completes them immediately. Rows where
`status_transition_at` is in the future are left for the background poller.

This column-on-tables approach is sufficient while control plane operations map 1:1 to table status changes
(`CREATING → ACTIVE`, `DELETING → removed`). A separate `control_plane_operations` table becomes necessary when:
- Operations span multiple catalog entities (e.g., GSI backfill touches both `indexes` and data tables)
- Operations have intermediate states beyond a single status flip (e.g., multi-step UpdateTable)
- Audit or observability requires a history of completed operations, not just pending ones

Until those requirements arise, the single-column approach avoids the complexity of a separate job queue while providing
full crash recovery.

### 6.2 GSI Backfill on CreateIndex

When `UpdateTable` adds a new GSI to a table with existing data:

1. Set the new index status to `CREATING` in `indexes`
2. Spawn a background task that scans the base table in batches (configurable batch size, default 1000)
3. For each batch, INSERT the projected attributes into the new GSI table
4. On completion, set index status to `ACTIVE`
5. During backfill, writes to the base table also write to the new GSI table (the write path checks index status and
includes `CREATING` indexes)
6. Queries against a `CREATING` index return `ResourceNotFoundException` (matching DynamoDB behavior)

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
uniquely identify the position, since GSI keys are not unique). This is why the GSI PostgreSQL table includes `base_pk`
and `base_sk_*` as actual columns.

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
> crate (e.g., CRC32 of the partition key bytes) that all backends use, and pass the pre-computed segment filter to the
> storage backend.

## 9. Idempotency Token Storage

`TransactWriteItems` supports `ClientRequestToken` for idempotency. Tokens are stored in a dedicated table:

```sql
CREATE TABLE _dynamodb_idempotency_tokens (
    client_request_token TEXT PRIMARY KEY,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON _dynamodb_idempotency_tokens(created_at);
```

**Flow:**
1. Before executing a transaction, check if the token exists
2. If found: return the stored response (idempotent replay)
3. If not found: execute the transaction, store the token + response
   atomically in the same PostgreSQL transaction
4. Background cleanup: delete tokens older than 10 minutes (matching
   DynamoDB's idempotency window)

If a request arrives with the same token but different parameters, return
`IdempotentParameterMismatchException`.

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
    engine: components.engine,
    catalog_store: components.catalog_store,
    auth_provider: components.auth_provider,
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
        reload_handle: app_state.reload_handle.clone(),
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
- PostgreSQL spawns 7 workers (control plane poller, pool metrics, GSI delay
  poller, TTL cleanup, stream cleanup, idempotency token cleanup, table size
  refresh)
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

All storage traits use `BoxFuture<'_, Result<T, StorageError>>` return types instead of RPITIT (`impl Future`):

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
- **Explicit lifetimes**: `BoxFuture<'_>` borrows from `&self`
- **No macro overhead**: No `#[async_trait]` macro expansion

**Implementation pattern:**

```rust
impl TableEngine for PostgresEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        // Clone borrowed params before async move
        let account_id = account_id.to_string();
        let input = input.clone();
        
        Box::pin(async move {
            self.create_table_impl(&account_id, &input).await
        })
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
