# Design: MongoDB Storage Backend

## 1. Overview

The MongoDB backend (`extenddb-storage-mongodb`) implements the same trait surface as
`extenddb-storage-postgres`: the 6 engine traits (`TableEngine`, `DataEngine`,
`MetadataEngine`, `StreamEngine`, `BackupEngine`, `WorkerStore`) and the catalog
traits (`ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`,
`RateLimitStore`, `AuthorizationStore`).

**Driver:** `mongodb` (official Rust driver, async, supports multi-document ACID
transactions on replica sets).

**Minimum MongoDB version:** 6.0 (for multi-document transactions and snapshot
reads).

## 2. Database Layout

Two databases, mirroring the PostgreSQL backend's catalog/data separation:

| Database             | Purpose                                              |
|----------------------|------------------------------------------------------|
| `extenddb_catalog`   | Table metadata, IAM, settings, metrics               |
| `extenddb_data`      | Per-table item collections, idempotency tokens       |

DynamoDB Streams are implemented via inline stream record writes during data
operations. GSI updates are propagated synchronously inline during writes.

## 3. Catalog Database Collections

### 3.1 `accounts`
```json
{ "_id": "<account_id>", "account_name": "...", "created_at": ISODate }
```
Unique index on `account_name`.

### 3.2 `tables`
```json
{
  "_id": { "account_id": "...", "table_name": "..." },
  "key_schema": [...],
  "attribute_definitions": [...],
  "billing_mode": "PAY_PER_REQUEST",
  "provisioned_throughput": { ... },
  "stream_specification": { ... },
  "table_status": "ACTIVE",
  "creation_date_time": ISODate,
  "table_size_bytes": NumberLong,
  "item_count": NumberLong,
  "table_arn": "...",
  "table_id": "...",
  "ttl_attribute": null,
  "deletion_protection_enabled": false,
  "status_transition_at": null,
  "stream_label": null,
  "ttl_index_ready": false
}
```
Unique index on `table_id`. Partial index on `status_transition_at` where not null.

### 3.3 `indexes`
```json
{
  "_id": { "table_id": "...", "index_name": "..." },
  "index_id": "...",
  "index_type": "GSI|LSI",
  "key_schema": [...],
  "projection": { ... },
  "index_status": "ACTIVE",
  "provisioned_throughput": { ... }
}
```

### 3.4 `tags`
```json
{ "_id": { "resource_arn": "...", "tag_key": "..." }, "tag_value": "..." }
```

### 3.5 `settings`
```json
{ "_id": "<key>", "value": "..." }
```

### 3.6 `admin_users`
```json
{ "_id": "<admin_name>", "password_hash": "...", "created_at": ISODate }
```

### 3.7 `iam_users`
```json
{
  "_id": { "account_id": "...", "user_name": "..." },
  "user_arn": "...",
  "password_hash": null,
  "tags": { "<key>": "<value>", ... },
  "created_at": ISODate
}
```
Unique index on `user_arn`.

### 3.8 `access_keys`
```json
{
  "_id": "<access_key_id>",
  "secret_key_encrypted": BinData,
  "account_id": "...",
  "user_name": "...",
  "is_active": true,
  "created_at": ISODate
}
```
Index on `(account_id, user_name)`.

### 3.9 `iam_groups`
```json
{
  "_id": { "account_id": "...", "group_name": "..." },
  "group_arn": "...",
  "members": ["user1", "user2"],
  "created_at": ISODate
}
```
Unique index on `group_arn`.

### 3.10 `iam_roles`
```json
{
  "_id": { "account_id": "...", "role_name": "..." },
  "role_arn": "...",
  "trust_policy": { ... },
  "permissions_boundary_arn": null,
  "tags": { "<key>": "<value>", ... },
  "created_at": ISODate
}
```
Unique index on `role_arn`.

### 3.11 `iam_sessions`
```json
{
  "_id": "<session_token>",
  "access_key_id": "...",
  "secret_key_encrypted": BinData,
  "account_id": "...",
  "role_name": "...",
  "session_name": "...",
  "session_tags": { ... },
  "session_policy": { ... },
  "expires_at": ISODate,
  "created_at": ISODate
}
```
Unique index on `access_key_id`. TTL index on `expires_at`.

### 3.12 `iam_policies`
```json
{
  "_id": { "account_id": "...", "principal_type": "...", "principal_name": "...", "policy_name": "..." },
  "policy_document": { ... },
  "created_at": ISODate
}
```

### 3.13 `iam_permissions_boundaries`
```json
{
  "_id": { "account_id": "...", "principal_type": "...", "principal_name": "..." },
  "policy_document": { ... }
}
```

### 3.14 `metrics`
```json
{
  "_id": { "bucket": ISODate, "metric": "...", "table_name": "...", "index_name": "...", "operation": "..." },
  "sum": 0.0,
  "count": NumberLong(0),
  "min": Infinity,
  "max": -Infinity
}
```
Index on `bucket` for pruning.

### 3.15 `login_attempts`
```json
{
  "principal": "...",
  "attempted_at": ISODate,
  "success": false,
  "source_ip": "..."
}
```
Compound index on `(principal, attempted_at)`.
Partial index on `(source_ip, attempted_at)` where source_ip exists.

### 3.16 `backups` (metadata only)
```json
{
  "_id": "<backup_arn>",
  "backup_name": "...",
  "table_id": "...",
  "table_name": "...",
  "account_id": "...",
  "backup_status": "AVAILABLE",
  "backup_type": "USER",
  "backup_size_bytes": NumberLong,
  "item_count": NumberLong,
  "key_schema": [...],
  "attribute_definitions": [...],
  "billing_mode": "PAY_PER_REQUEST",
  "provisioned_throughput": null,
  "stream_specification": null,
  "backup_collection": "_backup_{backup_id}",
  "created_at": ISODate
}
```
Index on `(account_id, table_name)`.

Backup item data is stored in a cloned collection (see Section 5.7).

### 3.17 `continuous_backups`
```json
{
  "_id": { "account_id": "...", "table_name": "..." },
  "pitr_enabled": false,
  "earliest_restorable": null,
  "latest_restorable": null
}
```

### 3.18 `schema_history`
```json
{ "_id": "<filename>", "applied_at": ISODate }
```

## 4. Data Database Collections

### 4.1 Per-Table Item Collections: `_ddb_{table_id}`

Each DynamoDB virtual table maps to a MongoDB collection.

**Document structure:**
```json
{
  "_id": "<pk>#<sk>",
  "pk": "...",
  "sk_s": "...",
  "sk_n": Decimal128,
  "sk_b": BinData,
  "item_data": { ... }
}
```

Fields:
- `_id` — deterministic compound key for upserts
- `pk` — partition key value (string-encoded)
- `sk_s` — sort key (string type), null if not applicable
- `sk_n` — sort key (numeric type, native BSON Decimal128), null if not applicable
- `sk_b` — sort key (binary type), null if not applicable
- `item_data` — full DynamoDB item serialized as BSON

**Indexes:**
- `{ pk: 1, sk_s: 1 }` or `{ pk: 1, sk_n: 1 }` or `{ pk: 1, sk_b: 1 }` depending
  on sort key type, or just `{ pk: 1 }` for PK-only tables

**Sort key ordering:**
- **String (`sk_s`):** Collection uses `collation: { locale: "simple" }` for
  byte-order sorting (matches DynamoDB's UTF-8 byte-order comparison).
- **Numeric (`sk_n`):** Native BSON Decimal128. MongoDB sorts numbers by value —
  no encoding tricks needed. DynamoDB supports 38 significant digits; Decimal128
  provides 34, which covers all practical use cases.

### 4.2 Per-Index Collections: `_ddb_{index_id}`

Same structure as item collections. GSI/LSI data is projected and stored here.
Written synchronously inline during data operations (PutItem, UpdateItem, DeleteItem).

### 4.3 `idempotency_tokens`
```json
{
  "_id": "<token>",
  "fingerprint": "...",
  "created_at": ISODate
}
```
TTL index on `created_at` (10 minutes) — MongoDB automatically cleans up expired
tokens.

## 5. Key Design Decisions

### 5.1 Transactions

MongoDB multi-document ACID transactions are used **only** for:

1. **TransactWriteItems** — all operations in a single transaction.
2. **TransactGetItems** — snapshot read using a session with `snapshot` read concern.

Everything else is transaction-free:
- Single-item conditional writes use filter pushdown (Section 5.2)
- GSI updates are done synchronously inline during the write operation (Section 5.4)
- Stream records are written inline during data operations (Section 5.5)

### 5.2 Condition Evaluation — Filter Pushdown

DynamoDB condition expressions are compiled to MongoDB query filters and pushed into
the write operation itself. This exploits MongoDB's single-document atomicity: a
`findOneAndReplace`/`findOneAndUpdate`/`findOneAndDelete` with a filter is atomic
without an explicit transaction.

**Flow:**
1. Compile `ConditionExpression` AST → MongoDB filter document
2. Combine with primary key filter: `{ pk: X, sk_s: Y, ...condition_filter... }`
3. Execute as `findOneAndReplace` (PutItem), `findOneAndUpdate` (UpdateItem), or
   `findOneAndDelete` (DeleteItem)
4. If result is `None` and the item exists → condition failed →
   `StorageError::ConditionFailed`

**Condition-to-filter translation:**

| DynamoDB condition | MongoDB filter |
|---|---|
| `attribute_exists(foo)` | `{ "item_data.foo": { $exists: true } }` |
| `attribute_not_exists(foo)` | `{ "item_data.foo": { $exists: false } }` |
| `foo = :val` | `{ "item_data.foo.S": val }` (typed) |
| `foo <> :val` | `{ "item_data.foo.S": { $ne: val } }` |
| `foo < :val` | `{ "item_data.foo.N": { $lt: val } }` |
| `foo > :val` | `{ "item_data.foo.N": { $gt: val } }` |
| `begins_with(foo, :p)` | `{ "item_data.foo.S": { $regex: "^<p>" } }` |
| `contains(foo, :v)` | `{ "item_data.foo.S": { $regex: "<v>" } }` |
| `size(foo) = :n` | `{ $expr: { $eq: [{ $size: "$item_data.foo.L" }, n] } }` |
| `cond1 AND cond2` | `{ $and: [filter1, filter2] }` |
| `cond1 OR cond2` | `{ $or: [filter1, filter2] }` |
| `NOT cond` | `{ $nor: [filter] }` |

**Implementation:** `condition_to_filter(expr: &Expr, maps: &ExpressionMaps) -> bson::Document`
walks the expression AST and emits a MongoDB filter.

**Common patterns (all transaction-free):**

| DynamoDB pattern | MongoDB operation |
|---|---|
| PutItem + `attribute_not_exists(pk)` | `updateOne({ pk, sk, "item_data.pk": {$exists: false} }, $setOnInsert, upsert)` |
| UpdateItem + `version = :v` | `findOneAndUpdate({ pk, sk, "item_data.version.N": v }, $set)` |
| DeleteItem + `status = :val` | `findOneAndDelete({ pk, sk, "item_data.status.S": val })` |
| PutItem (unconditional) | `replaceOne({ pk, sk }, doc, upsert: true)` |

**Returning the old item:**

`findOneAndReplace`/`findOneAndDelete` atomically returns the pre-modification
document when `return_old = true`. No transaction needed.

For `ConditionFailed` with `ReturnValuesOnConditionCheckFailure`, a follow-up
`find_one` fetches the existing item. This is acceptable — DynamoDB has the same
best-effort semantics for the returned item.

### 5.3 Query and Scan

**Query:** Translates `KeyCondition` to a MongoDB `find()` filter:
- Partition key equality: `{ pk: "<value>" }`
- Sort key conditions:
  - `=` → `{ sk_s: value }`
  - `<` → `{ sk_s: { $lt: value } }`
  - `begins_with` → `{ sk_s: { $gte: prefix, $lt: prefix_upper } }`
  - `BETWEEN` → `{ sk_s: { $gte: low, $lte: high } }`

Sort direction: `.sort({ sk_s: 1 })` for forward, `.sort({ sk_s: -1 })` for reverse.

Pagination: `exclusive_start_key` translates to an additional `$gt`/`$lt` filter on
the sort key (or partition key for scans).

**Scan:** Full collection scan with `.find({})`, paginated via sort-key-based cursor.

**Parallel scan:** Segments are handled by filtering in application
(`crc32(pk) % total_segments == segment`). Each segment scans the full collection.
This is a known tradeoff — the only way to avoid redundant scans is a pre-bucketed
field on every document, which adds write-path overhead for a feature that is rarely
used in practice.

### 5.4 GSI Propagation (Synchronous Inline)

GSI updates are performed synchronously inline during each write operation. There is
no background worker, no Change Stream consumer, and no resume token tracking for GSI
propagation.

**How it works:**

On each write (PutItem, UpdateItem, DeleteItem), after writing to the base table
collection, the `sync_indexes` method:

1. Checks the in-memory `gsi_cache` (`DashMap<String, bool>`) keyed by `table_id`.
   If the cache entry is `false`, skip the catalog query entirely (fast path for
   tables with no GSIs).
2. If the cache misses or is `true`, query the `indexes` collection in the catalog
   database for all indexes belonging to this `table_id`.
3. For each GSI found:
   - If an old item exists and has the index keys: delete the old entry from
     `_ddb_{index_id}`
   - If a new item exists and has the index keys: project the relevant attributes
     (respecting the GSI's `Projection` setting) and upsert into `_ddb_{index_id}`
4. Update the cache: `gsi_cache.insert(table_id, found_any)`.

**Cache invalidation:**
- On table delete (`delete_table`): `gsi_cache.remove(table_id)`
- On GSI create (`update_table`): `gsi_cache.insert(table_id, true)`
- On GSI delete (`update_table`): `gsi_cache.remove(table_id)` (will be re-populated
  on next write)

**Consistency model:**
- GSI reads are strongly consistent (index is updated before write returns to client)
- This is stricter than DynamoDB's eventual consistency model for GSIs, which is
  acceptable (stronger guarantees never break application code)

**Rationale:** Synchronous inline propagation avoids the complexity of Change Stream
recovery, resume token management, and eventual consistency bugs. The overhead is one
catalog query per write for tables with GSIs (cached to zero for tables without GSIs).

### 5.5 DynamoDB Streams (Inline Record Storage)

DynamoDB Streams are implemented by writing stream records inline during data
operations, using the same storage model as the PostgreSQL backend. Stream records
are stored in MongoDB collections (`stream_records` and `stream_shards` in the data
database) with explicit sequence numbers and shard assignment. This approach provides
behavioral parity with the PostgreSQL backend rather than relying on MongoDB Change
Streams.

**Data model:**

- `stream_shards` — one document per shard (4 shards per stream-enabled table),
  keyed by `shard_id` + `table_id`
- `stream_records` — one document per event, containing `sequence_number`, `shard_id`,
  `table_id`, `event_name`, `record_data` (full `StreamRecord` serialized as BSON),
  and `created_at`

**Write path:**

When `StreamCapture` is provided to a data operation (PutItem, UpdateItem, DeleteItem),
the `write_stream_inline` helper:

1. Determines the event type (INSERT/MODIFY/REMOVE) from old/new item presence
2. Builds key images and old/new images based on `StreamViewType`
3. Assigns a shard using `crc32(partition_key) % shard_count`
4. Obtains a sequence number via atomic `findOneAndUpdate` on a counter document
5. Writes the stream record to the `stream_records` collection

**Shard assignment:** `crc32(pk) % SHARDS_PER_STREAM` (currently 4 shards per table).

**Sequence numbers:** Global monotonic counter stored in `counters` collection, using
`findOneAndUpdate` with `$inc` for atomic increment. Format: zero-padded 21 digits.

**`StreamEngine` trait mapping:**

| Trait method                     | Implementation                                                   |
|----------------------------------|------------------------------------------------------------------|
| `write_stream_record`            | Insert record document into `stream_records` collection          |
| `get_stream_records`             | Query `stream_records` by `shard_id`, ordered by `sequence_number` |
| `describe_stream`                | Query `tables` + `stream_shards`, return shard list              |
| `list_streams`                   | Query `tables` where `stream_label` is not null                  |
| `cleanup_expired_stream_records` | Delete records older than retention cutoff                        |
| `assign_shard`                   | `crc32(pk) % shard_count` over shards for the table              |
| `next_sequence_number`           | Atomic `$inc` on counter document in `counters` collection       |
| `validate_shard`                 | Check table+stream exist and shard_id belongs to the stream      |
| `latest_sequence_number`         | Query last record in shard by descending `sequence_number`       |

**Retention:** `cleanup_expired_stream_records` deletes records with `created_at`
older than the configured retention period.

### 5.6 TTL Handling

MongoDB's built-in TTL indexes handle automatic cleanup for:
- `idempotency_tokens` — expire after 10 minutes
- `iam_sessions` — expire at `expires_at`

For DynamoDB-level TTL (user-configured `TimeToLive`), the application-level TTL
worker is still needed because TTL deletion must emit stream records with a specific
`UserIdentity`. When TTL is enabled on a table, a sparse index is created on the TTL
attribute path for efficient expired-item lookup:

```rust
db.collection("_ddb_{table_id}")
    .create_index(IndexModel::builder()
        .keys(doc! { format!("item_data.{ttl_attribute}.N"): 1 })
        .options(IndexOptions::builder().sparse(true).build())
        .build())
```

### 5.7 Backups

`CreateBackup` clones the source collection server-side using `$out`:

```rust
// CreateBackup — server-side collection clone
data_db.collection("_ddb_{table_id}")
    .aggregate([doc! { "$out": "_backup_{backup_id}" }])
    .await?;

// RestoreTableFromBackup — clone back to new table
data_db.collection("_backup_{backup_id}")
    .aggregate([doc! { "$out": "_ddb_{new_table_id}" }])
    .await?;

// DeleteBackup — drop the backup collection
data_db.collection("_backup_{backup_id}").drop().await?;
```

No document size limits, handles tables of any size, no client-side data transfer.

### 5.8 Write Conflict Handling

**UpdateItem (optimistic concurrency):**

`UpdateItem` uses a read-modify-write pattern with a `_v` version field for conflict
detection:

1. Read the existing document and note its `_v` (version) value (defaults to 0 if
   absent)
2. Apply update expressions in memory to produce the new item
3. Set `_v = current_version + 1` on the new document
4. Execute `replaceOne` with a filter matching both the primary key AND the expected
   `_v` value
5. If `matched_count == 0`, a concurrent writer incremented the version first —
   retry with jittered exponential backoff (base 100us, up to 50 attempts)

This avoids multi-document transactions for single-item updates while preventing
lost updates.

**Conditional writes (PutItem, DeleteItem):**

These use a find-then-write pattern. For PutItem, the condition is evaluated
client-side against the fetched document, then `findOneAndReplace` (or `insert_one`
for new items) is used. Duplicate key errors on insert are caught and mapped to
`ConditionFailed`.

**Explicit transactions (`TransactWriteItems`):**

All operations in a `TransactWriteItems` call execute within a single MongoDB
multi-document transaction with snapshot read concern and majority write concern.
If the transaction fails, it is not retried — the error propagates as
`StorageError::TransactionCanceled`.

### 5.9 Account ID Validation

Defense against MongoDB operator injection:
- Reject `$` (operator injection)
- Reject `.` (field path traversal)
- Reject null bytes
- Reject non-ASCII

### 5.10 Catalog Version Check

Read `catalog_version` from the `settings` collection and compare against the
compiled-in constant. Same pattern as PostgreSQL.

## 6. Crate Structure

```
crates/storage-mongodb/
├── Cargo.toml
└── src/
    ├── lib.rs                  # MongoEngine struct, inventory registrations
    ├── config.rs               # Configuration parsing
    ├── bootstrapper.rs         # Database initialization (init/destroy)
    ├── table_engine.rs         # CreateTable, DeleteTable, DescribeTable, UpdateTable
    ├── data_engine.rs          # PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, Transactions
    ├── data/mod.rs             # Document <-> Item conversion helpers
    ├── condition.rs            # DynamoDB condition expressions -> MongoDB filters
    ├── stream_engine.rs        # DynamoDB Streams (shard management, sequence numbers)
    ├── metadata_engine.rs      # TTL, tags, table size tracking
    ├── ttl_worker.rs           # Background TTL cleanup
    ├── backup_engine.rs        # Backup/restore via collection cloning
    ├── management_store.rs     # IAM management, settings, metrics, rate limiting
    ├── authorization_store.rs  # Policy evaluation, boundaries, sessions
    ├── credential_store.rs     # Access key lookup with AES-GCM decryption
    ├── catalog_store.rs        # Catalog and diagnostics
    ├── admin_store.rs          # Admin operations
    └── worker_store.rs         # Control plane state transitions
```

## 7. MongoEngine Struct

```rust
pub struct MongoEngine {
    client: mongodb::Client,
    catalog_db: mongodb::Database,
    data_db: mongodb::Database,
    region: String,
    max_connections: u32,
    /// Cache of `table_id` -> `has_gsi`. Avoids catalog queries on every write
    /// for tables with no GSIs.
    gsi_cache: dashmap::DashMap<String, bool>,
}
```

MongoDB's driver manages connection pooling internally (configurable via
`ClientOptions`). A single `Client` is shared; `Database` handles are lightweight
references. The `gsi_cache` provides a fast path to skip GSI catalog lookups for
tables known to have no indexes.

## 8. Configuration

```toml
[storage.mongodb]
connection_string = "mongodb://localhost:27017"
pool_size = 20
```

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct MongoStorageConfig {
    #[serde(default = "default_connection_string")]
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}
```

## 9. Bootstrapper Flow

**`extenddb init`:**
1. Connect to MongoDB (databases created implicitly on first write)
2. Create catalog collections with indexes
3. Seed `settings` with `catalog_version`
4. Generate and store encryption key
5. Create default account
6. Create admin user
7. Create data database `idempotency_tokens` collection with TTL index
8. Record data database name in catalog settings

**`extenddb destroy`:**
1. Drop data database
2. Drop catalog database

## 10. Inventory Registrations

```rust
inventory::submit! { BackendRegistration { name: "mongodb", .. } }
inventory::submit! { OperationsEngineRegistration { name: "mongodb", .. } }
inventory::submit! { StorageConfigRegistration { backend: "mongodb", .. } }
inventory::submit! { ServerComponentsRegistration { backend: "mongodb", .. } }
inventory::submit! { SettingsStoreRegistration { backend: "mongodb", .. } }
```

## 11. Dependencies

```toml
[dependencies]
mongodb = { version = "3", features = ["tokio-runtime"] }
bson = "2"
dashmap = "6"
tokio = { workspace = true, features = ["sync"] }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
toml = { workspace = true }
tracing = { workspace = true }
time = { workspace = true }
uuid = { workspace = true }
base64 = { workspace = true }
rand = { workspace = true }
bcrypt = { workspace = true }
aes-gcm = { workspace = true }
async-trait = { workspace = true }
zeroize = { workspace = true }
inventory = { workspace = true }
extenddb-core = { workspace = true }
extenddb-storage = { workspace = true }
extenddb-auth = { workspace = true }
crc32fast = { workspace = true }
```

## 12. Implementation Phases

### Phase 1: Core (MVP) -- Complete
- `MongoEngine` struct and connection setup
- `TableEngine` (create/delete/describe/list/update)
- `DataEngine` (put/get/delete/update/query/scan) with condition filter compiler
- `Bootstrapper` (init, destroy)
- `StorageConfig` and inventory registrations
- Unit tests against a local MongoDB replica set

### Phase 2: Management & Auth -- Complete
- `CatalogStore` (ManagementStore, AdminStore, SettingsStore, MetricsStore,
  RateLimitStore)
- `AuthorizationStore`
- `MongoCredentialStore`
- Web console and management API working

### Phase 3: Streams & Transactions -- Complete
- `StreamEngine` (inline stream record writes, shard management, sequence numbers)
- `TransactGetItems` / `TransactWriteItems`
- Idempotency tokens

### Phase 4: Advanced Features -- Complete
- `BackupEngine` (collection cloning via `$out`)
- `WorkerStore` (control plane state transitions)
- Synchronous inline GSI propagation with `DashMap` cache
- TTL worker (application-level DynamoDB TTL)
- `MetadataEngine` (full TTL lifecycle)

### Phase 5: Testing & Production Readiness -- Complete
- Full pytest integration suite passes against MongoDB backend
- Performance benchmarking vs PostgreSQL backend
- Documentation

## 13. Testing Strategy

- **Unit tests:** Mock the MongoDB client for pure logic tests
- **Integration tests:** Single-node replica set in Docker (`mongod --replSet rs0`)
- **Existing pytest suite:** Passes unchanged (speaks DynamoDB wire protocol)
- **CI:** GitHub Actions job with MongoDB replica set, runs
  `cargo test -p extenddb-storage-mongodb`

## 14. Deployment Requirements

- MongoDB **6.0+** in **replica set** mode (required for multi-document transactions)
- Single-node replica set is fine for development/testing
- For production: 3-node replica set
- Target scale: < 500 DynamoDB tables. At 500 tables with 2 GSIs each (~1500
  collections), WiredTiger handles this comfortably with default settings.
  Ensure `ulimit -n` ≥ 65536.

## 15. Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Conditional writes | Filter pushdown (no transaction) | Single-document atomicity, no tx overhead on hot path |
| GSI updates | Synchronous inline | Simplicity, no Change Stream recovery complexity, strongly consistent |
| DynamoDB Streams | Inline record writes to MongoDB collections | Behavioral parity with PostgreSQL backend, explicit sequence numbers |
| Stream shards | 4 per table, CRC32 hash assignment | Predictable parallelism for consumers |
| Sort key numbers | Native BSON Decimal128 | Correct ordering by value, zero encoding overhead |
| Backups | `$out` collection clone | Server-side, no size limits |
| Parallel scan | Filter in application | Rarely used, not worth write-path overhead of `_seg` field |
| Write conflict (UpdateItem) | Optimistic concurrency with `_v` field + jittered backoff | Avoids transactions for single-item updates |

## 16. Performance Characteristics

**Hot path (single-item writes):** Transaction-free. A PutItem with condition is a
single `findOneAndReplace` with a filter — one network roundtrip, one WiredTiger
document write. No locking, no multi-phase commit.

**GSI overhead on write path:** One catalog query per write for tables with GSIs
(to fetch index definitions), plus one upsert/delete per GSI. For tables with no
GSIs, the `gsi_cache` short-circuits to zero overhead (no catalog query, no I/O).

**Stream overhead on write path:** When streaming is enabled, one counter increment
(atomic `findOneAndUpdate`) plus one document insert to `stream_records` per write
operation.

**Query/Scan:** Direct index lookups on `{ pk, sk_* }`. Same performance
characteristics as any indexed MongoDB query.

**TransactWriteItems:** Multi-collection transaction. Rare in practice (most
workloads are single-item operations). Limited to 100 operations per DynamoDB
API spec.
