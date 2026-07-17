# RFC-206: MongoDB Storage Backend

- Status: Draft
- Author: @diegotoledano95
- Created: 2026-07-08
- Tracking issue: #206

## Summary

This RFC proposes adding MongoDB as a backend for ExtendDB. The goal is to let developers run DynamoDB-compatible workloads on MongoDB while preserving ExtendDB’s core value: a DynamoDB-compatible API over multiple storage backends. The implementation covers all mandatory traits defined in RFC-0002 and all optional traits, uses ExtendDB's existing `inventory`-based plugin registration system without modifying the server or engine layers, and is maintained by the MongoDB team who commit to ongoing ownership of the backend crate.

## Motivation

ExtendDB’s core premise is DynamoDB API compatibility over multiple storage backends. The initial reference PostgreSQL backend demonstrates the feasibility of this approach while opening the opportunity for other databases to participate. 

MongoDB is a natural fit as an additional database target: data model alignment; high read/write throughput through horizontal scalability; infrastructure fit. 

DynamoDB and MongoDB share the same data model approach - documents stored as schema-less JSON-like data. MongoDBs document model maps directly to the approach taken by DynamoDB with each item stored as a MongoDB BSON document with no impedance mismatch at the data model level. Unlike relational databases, the translation from JSON to BSON is direct without complicated relational mapping techniques required.

Customers evaluating DynamoDB and MongoDB often consider scalability as a key requirement. ExtendDB’s deployment approach requiring high write throughput is matched by MongoDB’s replica set model via horizontal scaling. High read and write throughput across multiple nodes is a core tenant of MongoDB and aligns naturally with the scalability requirement for an ExtendDB customer. 

Organizations running ExtendDB, DynamoDB, and MongoDB have already evaluated the usefulness of a non-relational database approach. These shared customers do not want to run PostgreSQL or other relational databases solely for DynamoDB compatibility.  Rather, taking advantage of the infrastructure they already run that aligns with the document model design and scalability requirements they require makes MongoDB a natural fit. 


## Detailed design

### Scope

This RFC proposes:

- A new optional `extenddb-storage-mongodb` crate at `crates/storage-mongodb/`
- A `mongodb` Cargo feature flag on the `extenddb` binary crate
- Backend registration through the existing `inventory`-based plugin system without changes to `crates/engine/`, `crates/server/`, `crates/auth/`, or `crates/core/`
- MongoDB-specific implementations of all mandatory and optional storage traits defined in RFC-0002
- Setup documentation and sample configuration for MongoDB deployments

This RFC does not propose:

- Changes to the DynamoDB wire protocol or API response shapes
- MongoDB Atlas, Atlas Data API, or any hosted MongoDB service as a target
- Sharded cluster support (replica sets only)
- Changing the default backend from PostgreSQL
- Dual-write or online migration from PostgreSQL to MongoDB
- A generic document-store abstraction shared with future document database backends

### Repository structure

The backend lives at `crates/storage-mongodb/` in the main ExtendDB repository, following the mono-repo structure prescribed by RFC-0002. It is selected at build time via a `mongodb` Cargo feature flag on the `extenddb` binary crate.


Feature flag definition: `crates/bin/Cargo.toml` — `[features]` section defines `mongodb = ["extenddb-storage-mongodb"]` with the crate as an optional dependency. The `postgres` feature remains the default. Both features can be enabled together to compile a binary supporting both backends.

### Plugin registration

The backend registers itself with ExtendDB's `inventory`-based plugin system without modifying the server, engine, or auth layers. Five `inventory::submit!` calls in `lib.rs` register the backend for: bootstrapping (`extenddb init`), config parsing, settings store access, diagnostics store access, and server component construction (`extenddb serve`).


All five registration blocks: `crates/storage-mongodb/src/lib.rs`. The `ServerComponentsRegistration` block is the critical one — it is the factory function called when `extenddb serve --backend mongodb` is run. No changes were required in `crates/server/`, `crates/engine/`, or `crates/auth/`.

### Database layout

The backend uses two MongoDB databases:

**`extenddb_catalog`** — metadata and management. Created on `extenddb init`. Contains 18 collections covering: table definitions (`tables`), index metadata, accounts, tags, admin users, IAM users, groups, group memberships, roles, access keys, IAM sessions, policies, permissions boundaries, session data, settings, metrics, login attempts, continuous backup metadata, and schema migration history. `iam_group_members` and `backup_items` are used by the runtime but auto-created on first insert rather than during migrations.

**`extenddb_data`** — item data. One MongoDB collection per DynamoDB table, named `_ddb_{table_id}`. One additional collection per GSI/LSI, named the same way with the index's ID. Shared collections: `stream_records` and `stream_shards` for DynamoDB Streams, `idempotency_tokens` for transaction deduplication, and `counters` for sequence number generation.


Catalog collection creation and index setup: `crates/storage-mongodb/src/bootstrapper.rs` — `run_catalog_migrations()` creates the migration-managed catalog collections and their MongoDB indexes; `schema_history` is created separately by `create_catalog_db()` in the same file. Data database setup: `create_data_db()` creates `idempotency_tokens` with a 10-minute TTL index. Collection naming convention: `crates/storage-mongodb/src/data/mod.rs` — `data_collection_name()` and `index_collection_name()`.

### Document structure for DynamoDB items

Each DynamoDB item is stored as a MongoDB document with the following structure:

```
{
  _id:       "partitionKeyValue#sortKeyValue",
  pk:        "partitionKeyValue",
  sk_s:      "sortKeyValue",       // string sort keys
  sk_n:      Decimal128(...),      // number sort keys, native MongoDB numeric type
  sk_b:      Binary(...),          // binary sort keys
  item_data: { ... full DynamoDB item in DynamoDB JSON format ... }
}
```

The `_id` field enables O(1) point lookups. The `pk` field is indexed separately to support partition scans (Query operations). Sort keys are stored in typed fields (`sk_s`, `sk_n`, `sk_b`) so MongoDB can apply native range comparisons with correct ordering — notably, numeric sort keys use MongoDB's `Decimal128` type rather than strings to ensure correct numeric ordering. Values that exceed Decimal128's 34-significant-digit precision are rejected at write and query time with a ValidationException, rather than downcast; DynamoDB itself supports up to 38 significant digits. This is documented as a backend-specific behavioral difference (see `docs/differences-from-dynamodb.md`). The full item is stored in `item_data` using DynamoDB's own type-tagged format (`{"S": "hello"}`, `{"N": "42"}`, etc.), preserving type information for non-key attributes.

String sort key collections are created with `{ locale: "simple" }` collation, ensuring byte-for-byte ordering that matches DynamoDB's behavior rather than locale-aware Unicode ordering.


Document conversion functions: `crates/storage-mongodb/src/data/mod.rs` — `item_to_document()` (DynamoDB Item → BSON document) and `document_to_item()` (BSON document → DynamoDB Item). Sort key type handling including `Decimal128`: same file, `item_to_document()` `ScalarAttributeType::N` branch. Collation: `crates/storage-mongodb/src/table_engine.rs` — `CreateTable` implementation, index creation with `Collation` options.

### Condition expression evaluation

Condition expressions (`ConditionExpression` on PutItem, DeleteItem, UpdateItem) are evaluated inside a MongoDB client session that also wraps the write. Within the session, the backend reads the current item, evaluates the condition in Rust against the loaded item, and issues the write — the read and write share the session's transactional atomicity, so a concurrent writer's changes are either visible to the condition or cause the write to observe its version guard (see Write conflict handling). This delivers DynamoDB's atomicity contract for conditional writes and matches the `ReturnValuesOnConditionCheckFailure = ALL_OLD` semantics naturally (the read result is available for the response).

The condition compiler exists as scaffolding for a follow-up optimization: it translates DynamoDB expressions into MongoDB filter documents that could be pushed into `findOneAndReplace` / `findOneAndDelete` for a single-round-trip conditional write. The compiler handles: `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `BETWEEN`, `IN`, `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`. Because items are stored with DynamoDB type tags, compiled paths include the type suffix: `item_data.fieldName.S` for strings, `.N` for numbers. The compiler is not on the load-bearing correctness path today; the session-scoped read-then-write is.

**Alternative considered — filter pushdown as the primary path.** Compiling filters into `findOneAndReplace` / `findOneAndDelete` would reduce a conditional write to a single round-trip. It is planned as a follow-up optimization once the compiler is exercised through the integration test suite. The current design was chosen because it gives us the loaded item for `ReturnValuesOnConditionCheckFailure` responses without a follow-up read, and because the compiler's behavior on edge-case expressions (multi-valued `size()`, `NOT` on missing paths, mixed-type set membership) is easier to validate incrementally under the session-scoped path.


Session-scoped condition path: `crates/storage-mongodb/src/data_engine.rs` — `put_item_impl()`, `delete_item_impl()`, `update_item_impl()`, and the four `OwnedTransactWriteOp` arms in `execute_write_op_in_session()` each do read → `expression::evaluate_condition` → write, all with the session bound to each driver call. Condition compiler (scaffolding for the future pushdown path): `crates/storage-mongodb/src/condition.rs` — `condition_to_filter()` is the entry point. Unit tests in the same file demonstrate each compiled output.

### Query and Scan

**Query** translates `KeyConditionExpression` to a MongoDB `find()` filter. Partition key equality maps to `{ pk: "<value>" }`. Sort key conditions map to typed range filters on `sk_s`, `sk_n`, or `sk_b`. `ScanIndexForward: false` applies a descending sort. Pagination uses `ExclusiveStartKey` to add a `$gt` or `$lt` bound on the sort key, making each page fetch a single indexed range query.

**Scan** performs a full collection scan with `.find({})`, paginated via sort-key-based cursor. Filter expressions are evaluated after retrieval.

**Parallel scan** (`Segment` / `TotalSegments`) is handled in the application: each segment filters documents using `crc32(pk) % TotalSegments == Segment`. This means each segment scans the full collection. Pre-bucketing documents at write time would avoid this but adds overhead to every write for a feature that is rarely used in practice. The current tradeoff favors write-path simplicity.

### Global Secondary Indexes (GSI)  and Local Secondary Indexes (LSI)

Each secondary index has its own MongoDB collection. On writes that modify indexed attributes, the backend updates the index collection in the same operation, maintaining synchronous GSI propagation. A `DashMap` in-memory cache on `MongoEngine` tracks which tables have GSIs, avoiding catalog lookups on every write to tables with no indexes.


GSI collection creation: `crates/storage-mongodb/src/table_engine.rs` — `CreateTable` implementation. GSI cache: `crates/storage-mongodb/src/lib.rs` — `MongoEngine` struct `gsi_cache` field. Index write propagation: `crates/storage-mongodb/src/data_engine.rs` — `put_item_impl()` and related write paths check the cache before updating index collections.

### Transactions

`TransactWriteItems` uses MongoDB multi-document ACID transactions — a client session is opened, all operations execute within it, and the session is committed or aborted atomically. This requires MongoDB to be running as a replica set (standalone MongoDB does not support multi-document transactions). `TransactGetItems` performs a consistent snapshot read.

Idempotency tokens for `TransactWriteItems` are stored in the `idempotency_tokens` collection in `extenddb_data` with a 10-minute MongoDB TTL index, matching DynamoDB's 10-minute idempotency window.


Transaction implementation: `crates/storage-mongodb/src/data_engine.rs` — `transact_write_items_impl()`. Idempotency token storage: same file, idempotency check at the start of `transact_write_items_impl()`. Replica set requirement documentation: `docs/local-mongodb-setup.md` — replica set initialization section.

### Write conflict handling

**UpdateItem** uses optimistic concurrency. A `_v` version counter is stored on each document. The write path reads the current `_v`, applies the update expression in memory, sets `_v = current_version + 1`, then executes `replaceOne` filtered on both the primary key and the expected `_v`. If `matched_count == 0`, a concurrent writer incremented the version first. The operation retries with jittered exponential backoff (50 µs base, up to 50 attempts). Exhausted retries propagate the error to the caller.

**PutItem and DeleteItem** run their conditional read, condition evaluation, and write within a single MongoDB client session (see Condition expression evaluation). The item loaded to evaluate the condition is reused directly for the `ReturnValuesOnConditionCheckFailure = ALL_OLD` response — no follow-up read is issued. When no condition is present, the write is a straight `findOneAndReplace` / `findOneAndDelete`.

**TransactWriteItems** runs all operations inside a single MongoDB ACID transaction with snapshot read concern and majority write concern. Transaction failures are not retried — the error propagates as `TransactionCanceled`.

Write conflict handling: `crates/storage-mongodb/src/data_engine.rs` — `update_item_impl()`, version field handling and retry loop.

### DynamoDB Streams

DynamoDB Streams are implemented using explicit stream record storage in MongoDB collections, not MongoDB's native Change Streams feature. The explicit approach was adopted to maintain behavioral parity with the PostgreSQL backend and to retain full application control over sequence number generation, shard assignment, and record retention lifecycle — all of which the DynamoDB Streams API contract tightly specifies.

Each table is assigned 4 shards at creation time. On each data write with streams enabled, the backend assigns the write to a shard by hashing the partition key with CRC32, generates a monotonically increasing 21-digit sequence number using MongoDB's atomic `findOneAndUpdate` with `$inc`, and writes a record to `stream_records`. `GetRecords` paginates using `{ "sequence_number": { "$gt": after } }` range queries with ascending sort.


Stream implementation: `crates/storage-mongodb/src/stream_engine.rs`. Shard initialization: `init_stream_shards()`. Sequence number generation: `next_sequence_number()` using `$inc` on a counters document. Shard assignment by CRC32 hash: `assign_shard()`. Inline stream write from data operations: `crates/storage-mongodb/src/data_engine.rs` — `write_stream_inline()`.

### Time to Live (TTL)

When TTL is enabled on a table, the backend creates a sparse MongoDB index on `item_data.{ttl_attribute}.N`. A background worker spawned at server startup sweeps expired items every 60 seconds in batches of 100. Each deletion uses `DataEngine::delete_item` with a condition expression re-checking expiry, preventing races. TTL deletions carry `UserIdentity { type: "Service", principalId: "dynamodb.amazonaws.com" }` on their stream records, matching DynamoDB's TTL stream record format.


TTL index creation: `crates/storage-mongodb/src/metadata_engine.rs` — `create_ttl_index()`. Background worker: `crates/storage-mongodb/src/ttl_worker.rs` — `ttl_cleanup_worker()`, `sweep_expired_items()`. Worker spawn: `crates/storage-mongodb/src/lib.rs` — `MongoRuntimeHooks::spawn_workers()`. UserIdentity on TTL stream records: `crates/storage-mongodb/src/ttl_worker.rs` — `sweep_expired_items()`, `ttl_identity` construction.

### Control plane state transitions

Table creation and deletion are asynchronous at the DynamoDB API level — `CreateTable` returns `CREATING` status, `DeleteTable` returns `DELETING`. A background `WorkerStore` implementation polls the `tables` catalog collection for entries whose `status_transition_at` timestamp has passed and completes the transition: flipping CREATING → ACTIVE, or for DELETING → dropped (drops the data collection, index collections, removes catalog entries and tags).


Worker implementation: `crates/storage-mongodb/src/worker_store.rs` — `process_control_plane_transitions()`.

### Authentication and authorization

ExtendDB's mandatory SigV4 authentication is fully supported. Access key secrets are stored AES-GCM encrypted in the `extenddb_catalog.access_keys` collection. The encryption key is a 256-bit random key generated during `extenddb init`, base64-encoded, and stored in `extenddb_catalog.settings` under `_id: "encryption_key"`. Admin passwords are bcrypt-hashed before storage in `extenddb_catalog.admin_users`.

IAM policy evaluation fetches user-attached policies, group-attached policies (via `iam_group_members` → `iam_policies` join), role policies, permissions boundaries, and session policies from the catalog.


Encryption key generation and storage: `crates/storage-mongodb/src/bootstrapper.rs` — `bootstrap_encryption_key()`. Admin password hashing: same file, `bootstrap_admin_user()`. Access key decryption at request time: `crates/storage-mongodb/src/credential_store.rs`. IAM policy fetching: `crates/storage-mongodb/src/authorization_store.rs` — `fetch_user_policies()`, `fetch_user_group_policies()`, `fetch_role_policies()`, `fetch_session_data()`.

### Backup

`CreateBackup` iterates the source table's collection and inserts each item into a shared `backup_items` collection in the `extenddb_catalog` database, tagged with the `backup_arn`. Backup metadata (arn, table, timestamps, status) is stored in `extenddb_catalog.backups`. `RestoreTableFromBackup` reads `backup_items` filtered by `backup_arn` and reconstructs the table. `DeleteBackup` removes the entries for that arn from the shared collection and marks the metadata row deleted.

**Alternative considered — server-side `$out` aggregation.** MongoDB's `$out` stage would copy a collection server-side without transferring data through the application. It was not adopted here because backup metadata (retention policies, tags, cross-collection references, account-scoped ARN lookups) does not compose with `$out`'s single-collection-target model, and because a shared `backup_items` collection avoids proliferating per-backup collection names in the WiredTiger file namespace at typical scale. A future revision could switch to `$out` for the copy phase while keeping the shared metadata schema.


Backup implementation: `crates/storage-mongodb/src/backup_engine.rs`. `backup_items` collection layout and `backup_arn` indexing are documented in the module header.

### Operational requirements

**Minimum MongoDB version: 8.0.** This is the minimum supported version for this backend. The MongoDB Rust driver 3.x is technically compatible with MongoDB 4.2+, but this backend targets 8.0 as the minimum supported server version.

**Replica set required.** MongoDB must be configured as a replica set before running `extenddb init`. A standalone node does not support multi-document transactions (`TransactWriteItems`). A single-node replica set is sufficient for development and CI. Production deployments should use a 3-node replica set for high availability.

**File descriptor limit.** Each MongoDB collection maps to one WiredTiger file. At 500 DynamoDB tables with 2 GSIs each (~1,500 collections), ensure `ulimit -n ≥ 65536` on the MongoDB host. See `docs/local-mongodb-setup.md` for platform-specific instructions.

**Target scale.** This backend is designed for deployments of up to ~500 DynamoDB tables. At that scale, WiredTiger handles the collection count comfortably with default settings. Deployments significantly beyond this range have not been validated.

Configuration is added under `[storage.mongodb]` in `extenddb.toml`:

```toml
backend = "mongodb"

[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
max_connections = 50
max_catalog_connections = 20
```

Initialization uses backend-selection flags on `extenddb init`:

```text
extenddb init \
  --storage-backend mongodb \
  --storage-host 127.0.0.1 \
  --storage-port 27017 \
  --config extenddb.toml
```

Configuration struct: `crates/storage-mongodb/src/config.rs`. Sample configuration: `extenddb.sample.toml` — `[storage.mongodb]` section. Setup guide: `docs/local-mongodb-setup.md`.

### Implementation summary

| Crate modified | Change |
|---|---|
| `crates/storage-mongodb/` | New crate — full backend implementation |
| `crates/bin/Cargo.toml` | Added `mongodb` optional feature flag |
| `crates/bin/src/main.rs` | Added `#[cfg(feature = "mongodb")] extern crate` |
| `crates/bin/src/cmd_serve.rs` | Generalized the supported-backend gate from a hard-coded `"postgres"` check to a compile-time-conditional list built from the enabled feature flags (accepts `"mongodb"` when the feature is on) |
| `Cargo.toml` (workspace) | Added crate to members, added `mongodb`, `bson`, `dashmap` workspace dependencies |

No changes to `crates/engine/`, `crates/server/`, `crates/storage/` (trait definitions), `crates/auth/`, or `crates/core/`.


Full diff: `mongodb-forks/extenddb` branch `extenddb-on-mongo` compared to `main`. The absence of changes in engine/server/auth/core crates can be verified directly in that diff.

### Design decisions summary

| Decision | Choice | Rationale |
|---|---|---|
| Conditional writes (PutItem, DeleteItem) | Read + evaluate + write within one MongoDB client session | Atomicity for the DynamoDB contract; loaded item is reused for `ReturnValuesOnConditionCheckFailure = ALL_OLD` without a follow-up read. Filter pushdown is planned as an optimization (see Condition expression evaluation). |
| UpdateItem write conflict | Optimistic concurrency with `_v` version field + jittered backoff | Avoids multi-document transactions for single-item updates while preventing lost updates |
| GSI updates | Synchronous inline within the base write's session, with `DashMap` cache short-circuit for tables with no GSIs | No Change Stream recovery complexity; GSI reads are strongly consistent |
| DynamoDB Streams | Inline writes to `stream_records` collection within the base write's session | Behavioral parity with PostgreSQL backend; explicit control over sequence numbers, shard assignment, and retention |
| Stream shards | 4 per table, CRC32 hash assignment | Predictable consumer parallelism; no catalog lookup at shard assignment time |
| Sort key numbers | Native BSON `Decimal128` | Correct ordering by value; no string-encoding tricks. Values exceeding Decimal128 precision (34 digits) are rejected — see `docs/differences-from-dynamodb.md`. |
| Backups | Per-item inserts to shared `backup_items` collection keyed by `backup_arn` | Composes cleanly with backup metadata (tags, retention, ARN-scoped restore/delete); avoids collection-name proliferation. Server-side `$out` documented as a future optimization. |
| Parallel scan | Application-side `crc32(pk) % segments` filter | Avoids per-document write overhead of a pre-bucketed segment field |

### Performance characteristics

**Single-item writes.** A conditional PutItem, DeleteItem, or UpdateItem is executed within a MongoDB client session that covers the condition read, the write, and any dependent writes (stream record insert, GSI collection updates). This adds one session start/commit pair over a raw driver call — ~sub-millisecond on a local replica set. The session wrap is what gives DynamoDB's atomicity contract on conditional writes; it is not overhead in the DynamoDB-compatibility sense, it is the compatibility. A sessionless fast path for the narrow case of tables with no streams and no GSIs is planned as a follow-up optimization.

**GSI write overhead.** For tables with no GSIs, the `gsi_cache` short-circuits to zero overhead — no catalog query, no additional I/O. For tables with GSIs, one catalog query fetches index definitions (cached for subsequent writes on the same table), plus one upsert or delete per index collection per write, all within the same session as the base write.

**Stream write overhead.** When streams are enabled, each write adds one atomic `findOneAndUpdate` counter increment and one document insert into `stream_records`, both within the base write's session.

**Query and Scan.** Direct index lookups on `{ pk, sk_* }`. Performance characteristics match any indexed MongoDB query. Parallel scans scan the full collection once per segment (see Query and Scan).

**TransactWriteItems.** Multi-collection ACID transaction with snapshot read concern. Uncommon in practice — most DynamoDB workloads are single-item operations.

### Testing

Testing is organized in three layers.

**Unit tests** cover pure logic without a live MongoDB instance: condition expression compilation (`condition.rs`), document encoding and decoding (`data/mod.rs`), sort key ordering, and sequence number generation. The MongoDB client is mocked at this layer.

**Integration tests** run against a single-node replica set in Docker (`mongod --replSet rs0`). They cover the full table lifecycle, all item operations (including condition expressions), query and scan pagination, transactions, TTL worker behavior, stream record writes, GSI propagation, backup and restore, and all catalog and IAM operations. These execute as `cargo test -p extenddb-storage-mongodb`.

**End-to-end tests** run the existing ExtendDB pytest suite (`tests/`) unchanged against a MongoDB-backed ExtendDB server. The pytest suite speaks the DynamoDB wire protocol and has no backend awareness — a passing run against MongoDB is equivalent to a passing run against PostgreSQL. This is the conformance test baseline required by RFC-0002.

The CI job spins up a single-node MongoDB 8.0 replica set, builds ExtendDB with `--features mongodb`, runs `cargo test -p extenddb-storage-mongodb`, then runs `devtools/run-tests --extenddb --pytest` and `devtools/run-tests --extenddb --external` against the MongoDB-backed server.

## Drawbacks

**Replica set requirement.** MongoDB must be run as a replica set for `TransactWriteItems` support. This adds operational complexity for users who currently run standalone MongoDB. Users who run standalone MongoDB will receive a runtime error on transactional operations. This is documented in setup guides and is a MongoDB architectural constraint, not an ExtendDB limitation.

**Time to Live expiration throughput at scale.** DynamoDB's Time to Live deletion must emit stream records with a specific service identity. MongoDB's native Time to Live index operates at the storage engine level with no awareness of ExtendDB's stream system, so this implementation uses an application-level background worker that owns the full deletion lifecycle — finding expired items, deleting them, and emitting correctly attributed stream records. The worker runs every 60 seconds and processes expired items in batches of 100 per table. This is sufficient for ExtendDB's target deployment contexts. At very high expiration rates — tables where a large number of items expire per minute continuously — the worker will fall behind and the backlog will grow. This is a known limitation at scale, not a correctness issue, as DynamoDB's own contract only guarantees expiration within 48 hours rather than immediately (see `docs/differences-from-dynamodb.md`, TTL deletion row).

## Alternatives

### Use MongoDB Change Streams for DynamoDB Streams

MongoDB has a native change data capture feature (Change Streams) that could back DynamoDB Streams. This approach was not fully evaluated. The implementation instead adopted the explicit `stream_records` collection approach used by the PostgreSQL backend, which gives ExtendDB full control over sequence number generation, shard assignment, record retention, and iterator behavior — all of which the DynamoDB Streams API contract tightly specifies. Reviewers are invited to weigh in on whether a comparative evaluation of the Change Streams approach should be documented before acceptance.


## Prior art

**MongoDB document model and DynamoDB.** MongoDB's flexible document model has been noted as a natural fit for DynamoDB-style workloads in multiple independent analyses. Amazon DocumentDB (MongoDB-compatible) demonstrates AWS's own recognition of this overlap. The key difference in this implementation is that ExtendDB provides the full DynamoDB API layer — clients using the AWS SDK do not need to know they are talking to MongoDB.

**Condition pushdown pattern.** Compiling application-level filter expressions into storage-native query operators is a well-established pattern in query engines (Apache Arrow DataFusion, Spark, Presto all implement predicate pushdown). The `condition.rs` compiler in this implementation applies the same principle at the storage backend level.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
