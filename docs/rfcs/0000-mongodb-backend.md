# RFC-206: MongoDB Storage Backend

- Status: Draft
- Author: @diegotoledano95
- Created: 2026-07-08
- Tracking issue: #206

## Summary

This RFC proposes adding MongoDB as a backend for ExtendDB. The goal is to let developers run DynamoDB-compatible workloads on MongoDB while preserving ExtendDB's core value: a DynamoDB-compatible API over multiple storage backends. The implementation covers all mandatory traits defined in RFC-0002 and all optional traits, uses ExtendDB's existing `inventory`-based plugin registration system without modifying the server or engine layers, and is maintained by the MongoDB team who commit to ongoing ownership of the backend crate.

## Motivation

ExtendDB's core premise is DynamoDB API compatibility over multiple storage backends. The initial reference PostgreSQL backend demonstrates the feasibility of this approach while opening the opportunity for other databases to participate.

MongoDB is a natural fit as an additional database target: data model alignment; high read/write throughput through horizontal scalability; infrastructure fit.

DynamoDB and MongoDB share the same data model approach — documents stored as schema-less JSON-like data. MongoDB's document model maps directly to the approach taken by DynamoDB with each item stored as a MongoDB BSON document with no impedance mismatch at the data model level. Unlike relational databases, the translation from JSON to BSON is direct without complicated relational mapping techniques required.

Customers evaluating DynamoDB and MongoDB often consider scalability as a key requirement. ExtendDB's deployment approach requiring high write throughput is matched by MongoDB's replica set model via horizontal scaling. High read and write throughput across multiple nodes is a core tenet of MongoDB and aligns naturally with the scalability requirement for an ExtendDB customer.

Organizations running ExtendDB, DynamoDB, and MongoDB have already evaluated the usefulness of a non-relational database approach. These shared customers do not want to run PostgreSQL or other relational databases solely for DynamoDB compatibility. Rather, taking advantage of the infrastructure they already run that aligns with the document model design and scalability requirements they require makes MongoDB a natural fit.

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

The backend registers itself with ExtendDB's `inventory`-based plugin system without modifying the server, engine, or auth layers. Six `inventory::submit!` calls in `lib.rs` register the backend for: operations engine (CLI commands), bootstrapping (`extenddb init`), config parsing, settings store access, diagnostics store access, and server component construction (`extenddb serve`).

All registration blocks live in `crates/storage-mongodb/src/lib.rs`. The `ServerComponentsRegistration` block is the critical one — it is the factory function called when `extenddb serve --backend mongodb` is run. No changes are required in `crates/server/`, `crates/engine/`, or `crates/auth/`.

### Database layout

The backend uses two MongoDB databases:

**`extenddb_catalog`** — metadata and management. Created on `extenddb init`. Contains collections for table definitions (`tables`), index metadata (`indexes`), accounts, tags, admin users, IAM users, groups, roles, access keys, IAM sessions, policies, permissions boundaries, settings, metrics, login attempts, backup metadata, continuous backup state, and schema migration history.

**`extenddb_data`** — item data. One MongoDB collection per DynamoDB table, named `_ddb_{table_id}`. One additional collection per GSI/LSI, named `_ddb_{index_id}`. Shared collections: `stream_records` and `stream_shards` for DynamoDB Streams, `counters` for per-shard sequence-number counters, `idempotency_tokens` for transaction deduplication, and one `_backup_{backup_id}` collection per user-created backup.

Catalog collection creation and index setup: `crates/storage-mongodb/src/bootstrapper.rs` — `run_catalog_migrations()`. Data-database setup (`idempotency_tokens`, `stream_shards`, `stream_records` and their indexes): `create_data_db()` in the same file. Collection naming: `data/mod.rs` — `data_collection_name()`, shared between base-table and index collections.

### Document structure for DynamoDB items

Each DynamoDB item is stored as a MongoDB document:

```
{
  _id:       "<netstring(pk,sk)>",
  pk:        "partitionKeyValue",
  sk_s:      "sortKeyValue",       // string sort keys
  sk_n:      Decimal128(...),      // number sort keys, native BSON Decimal128
  sk_b:      "aabb...",            // binary sort keys, lowercase hex string
  _v:        NumberLong,           // OCC version counter (present on updated docs)
  item_data: { ... full DynamoDB item in DynamoDB JSON format ... }
}
```

The `_id` is a netstring-encoded composite key of the form `<len>:<pk>,<len>:<sk>,`. Netstring framing prevents the collision an ad-hoc `"{pk}#{sk}"` scheme suffers from when either component contains the delimiter (e.g. `pk="a#b", sk="c"` and `pk="a", sk="b#c"`). PK-only tables use the raw pk text as `_id`.

Typed sort-key fields (`sk_s`, `sk_n`, `sk_b`) let MongoDB apply native range comparisons with correct ordering:

- **String** sort keys use collection-level `{ locale: "simple" }` collation so range comparisons are byte-order, matching DynamoDB.
- **Numeric** sort keys use BSON `Decimal128`. Values whose precision exceeds Decimal128 (34 significant digits) are rejected at write and query time with a ValidationException rather than silently downcast. DynamoDB itself supports 38 significant digits; this is documented in `docs/differences-from-dynamodb.md`.
- **Binary** sort keys are stored as lowercase hex strings. BSON's native Binary comparison is length-first-then-content, which diverges from DynamoDB's unsigned-lex byte order (DynamoDB says `[0x01,0xFF] < [0x02]`; BSON Binary reverses that). Hex-encoded strings preserve DynamoDB byte order under MongoDB's default lexicographic string comparison and let `begins_with` use a plain `$gte` / `$lt` range filter.

The full item is stored in `item_data` using DynamoDB's own type-tagged format (`{"S": "hello"}`, `{"N": "42"}`), preserving type information for non-key attributes. Item conversion helpers live in `data/mod.rs` (`item_to_document`, `document_to_item`, `composite_id`, `binary_sk_to_hex`).

**Secondary-index documents** carry a superset of these fields. In addition to the index-key components (`pk`, `sk_?`), each index document also stores the base-table key attributes as first-class fields: `base_pk` (text) and `base_sk_s|n|b` (typed). The `_id` is a 4-tuple netstring `[idx_pk, idx_sk, base_pk, base_sk]`. GSI keys are non-unique across base items, so encoding the base key into `_id` gives each index entry a unique identity keyed to the base item it describes; without this, two base items sharing an index-key value would upsert to the same document and one would silently overwrite the other. The base-key fields also let index pagination form a compound cursor `(index_sk, base_pk, base_sk)` without traversing the JSON `item_data` payload. See `index_document()` and `index_entry_filter()` in `data/mod.rs`.

### Condition expression evaluation

Conditional writes (`ConditionExpression` on PutItem, DeleteItem, UpdateItem) run the condition read, evaluation, and write inside a MongoDB client session bound to a multi-document transaction. Within the session, the backend reads the current item, evaluates the DynamoDB condition in Rust against the loaded item (`extenddb_core::expression::evaluate_condition`), and issues the write on the same session. Read and write share the transaction's snapshot isolation, so a concurrent writer's changes either become visible to the condition (in which case the caller sees the same outcome as if the writes were serial) or trigger a WriteConflict at commit (which the write path retries — see Write conflict handling).

This delivers DynamoDB's atomicity contract for conditional writes and matches `ReturnValuesOnConditionCheckFailure = ALL_OLD` semantics naturally: the item loaded to evaluate the condition is reused directly in the failure response.

An optional filter-pushdown fast path (`pushdown.rs` + `condition.rs`) skips the session for a restricted subset of conditions on tables that have no GSIs and no stream capture. A compile-time analyzer (`is_pushable`) certifies that a condition's compiled MongoDB filter agrees with `evaluate_condition` on every item; when it says yes, the backend collapses read + check + write into a single `find_one_and_replace` / `find_one_and_delete` or replace with a merged key+condition filter. The analyzer is the correctness boundary: the compiler in `condition.rs` covers a broader syntax (numeric compare, sets, `IN`, `BETWEEN`, `size`, arbitrary `NOT`) than the analyzer certifies, and only the analyzer-approved subset ever reaches production. Anything else falls through to the session-scoped path, which is always authoritative.

Session-scoped condition path: `data_engine.rs` — `put_item_impl`, `delete_item_impl`, `update_item_impl`, and each `OwnedTransactWriteOp` arm in `execute_transact_write_op_in_session`. Pushdown fast path: `delete_item_pushdown` and `update_item_pushdown` in the same file, gated on `is_pushable(cond, maps) == Yes && stream.is_none() && gsi_cache_get_fresh(table_id) == Some(false)`.

### Query and Scan

**Query** translates `KeyConditionExpression` to a MongoDB `find()` filter. Partition key equality maps to `{ pk: "<value>" }`. Sort key conditions map to typed range filters on `sk_s`, `sk_n`, or `sk_b`. `BETWEEN` with `low > high` is rejected upfront with a ValidationException. `begins_with` on strings emits `{ $gte: prefix, $lt: next_string_prefix(prefix) }`, where the upper bound is the least string strictly greater than any prefix-starting string (built by incrementing the rightmost non-`char::MAX` code point). `begins_with` on binary emits the same range shape on the hex-encoded sort key. `ScanIndexForward: false` applies a descending sort.

Pagination via `ExclusiveStartKey` **merges** the resume bound into the existing sort-key predicate rather than replacing it. Naively inserting `{sk: {$gt: cursor}}` drops the caller's original `BETWEEN` / `begins_with` bound and returns items outside it on page 2 and beyond. The merge covers three cases: no existing sk predicate (insert), existing operator map (merge into it), and existing equality (fall back to `$and`). See `query_impl` in `data_engine.rs`.

**Index queries** paginate over a compound tuple `(index_sk?, base_pk, base_sk?)`. Index-key values are non-unique — duplicates fall through to the base-key tie-breaker. The cursor is expressed as a lexicographic `$or` of the form `(a > A) OR (a == A AND b > B) OR (a == A AND b == B AND c > C)` (reversed for descending). Sort direction is applied to the same compound tuple so ordering is deterministic across groups of items sharing index keys. `LastEvaluatedKey` carries both index-key and base-key components so the next page's `ExclusiveStartKey` resolves the compound cursor.

**Scan** performs a full collection scan with lazy cursor iteration. Base-table scans paginate on `_id` (unique after netstring encoding). Index scans paginate on the same compound cursor as index Query. Filter expressions are evaluated after retrieval.

**Parallel scan** (`Segment` / `TotalSegments`) filters items in the application via `crc32(pk) % TotalSegments == Segment`. Each segment scans the full collection. The scan loop streams the cursor and terminates when either `limit + 1` in-segment items are accumulated or the cursor exhausts, without imposing a server-side hard limit — a hard `limit * total_segments` cap combined with post-fetch segment filtering silently drops items under any hot-key skew. Pre-bucketing documents at write time would avoid the per-segment full scan but adds overhead to every write for a feature that is rarely used in practice.

### Global and Local Secondary Indexes

Each secondary index has its own MongoDB collection with a compound index on `(pk, sk_?, base_pk, base_sk_?)` (created by `create_index_data_collection` in `table_engine.rs`). String-sorted index columns use `simple` collation matching the query path. On writes that modify indexed attributes, the backend synchronizes the index collection in the same session as the base write: `sync_indexes_in_session` deletes the old projected entry (filtered on the full index-key + base-key tuple so duplicate index keys don't cross-delete) and upserts the new one.

A `DashMap` in-memory cache on `MongoEngine` short-circuits the catalog lookup for tables known to have no indexes. Cache entries carry an insertion timestamp and expire after 60 seconds (`GSI_CACHE_TTL`), so out-of-band GSI changes on another ExtendDB instance converge within the TTL window.

**Async GSI backfill.** `UpdateTable`'s GSI-create path writes the catalog document with `index_status: "CREATING"` and pre-creates the mongo collection and its query index; a background `gsi_backfill_worker` (in `ttl_worker.rs`) discovers `CREATING` rows, iterates the base collection in batches keyed by a persistent `backfill_cursor` field on the index document, upserts projected items into the index collection via `backfill_gsi_batch`, and flips the status to `ACTIVE` when the base is fully scanned. The cursor is persisted between batches so a mid-backfill server restart resumes where it left off. Live writes during the backfill window continue to route through `sync_indexes_in_session`, which writes to CREATING indexes too — all writes are upserts on the same `_id` shape, so a base item touched by both paths converges regardless of interleaving.

Before every put and update, `validate_index_keys_for_item` rejects wrong-type or empty index-key attributes as a top-level `ValidationException` (or a per-item `CancellationReason` inside `TransactWriteItems`). Without this check, a mismatched-type index-key attribute would be silently dropped from the index doc, leaving the row un-locatable for subsequent deletes.

### Transactions

`TransactWriteItems` runs all operations inside a single MongoDB multi-document ACID transaction with snapshot read concern and majority write concern. Each operation's condition evaluation, base-row write, GSI synchronization, and stream record insert happens on the same `ClientSession` (`sync_indexes_in_session` + `write_stream_inline_in_session` are called inline from each `OwnedTransactWriteOp` arm). Without this, a transactional write to a GSI-bearing or streams-enabled table would commit the base row while silently dropping its dependent side effects.

Idempotency tokens live in the `idempotency_tokens` collection in `extenddb_data`. A unique compound index on `(account_id, token)` catches races between concurrent transacts under snapshot isolation — an inserter that races through the pre-check gets an `E11000` on insert, resolved by re-reading the winner and returning `IdempotentReplay` (fingerprints match) or `IdempotentMismatch` (fingerprints differ). Retention is enforced by a 540-second MongoDB TTL index plus a data-plane age filter (`created_at` within 600 000 ms) so worst-case retention stays ≤10 minutes regardless of the TTL monitor's ~60s cadence.

`TransactGetItems` performs a consistent snapshot read using a `ClientSession` with snapshot read concern.

Transaction implementation: `data_engine.rs` — `transact_write_items_impl`, `transact_get_items_impl`, `execute_transact_write_op_in_session`.

### Write conflict handling

**Session-scoped writes** (PutItem, DeleteItem, UpdateItem, TransactWriteItems) detect transient MongoDB conflicts via `is_transient_write_conflict`, which returns true for any of: the `TransientTransactionError` label, the `UnknownTransactionCommitResult` label, or a raw `WriteConflict` (code 112). Conflicts trigger a retry loop with jittered exponential backoff (`backoff_sleep`, base 50 µs) up to `TRANSIENT_RETRY_ATTEMPTS` (50). Exhausted retries on single-item operations return a `StorageError::Internal`; on `TransactWriteItems`, they surface as a `TransactionCanceled` with a synthetic per-op `TransactionConflict` cancellation reason so wire consumers see the DDB-canonical error string instead of a bare HTTP 500.

**UpdateItem** additionally uses an optimistic-concurrency version guard on top of the transaction. A `_v` counter is stored on each document. The write path reads the current `_v` under the transaction snapshot, applies the update expression in memory, sets `_v = current_version + 1`, then executes `replace_one` filtered on both the primary key AND the expected `_v`. If `matched_count == 0`, a concurrent writer committed a higher version between the snapshot read and the replace; the attempt aborts and the outer retry loop re-reads. The version guard doubles up when a native-fast-path update (unconditional, no stream, no GSI) is possible — that path uses a single `find_one_and_update` with `$inc: {_v: 1}` outside a transaction, and always bumps the counter so a concurrent session-scoped update against a stale snapshot fails its versioned filter and retries.

**PutItem** with an existence guard on a new document maps duplicate-key errors (`E11000`) to `ConditionFailed` after re-reading the winner. This is the runtime signature of a conditional-put race the transaction snapshot didn't see.

### DynamoDB Streams

DynamoDB Streams are implemented using explicit stream record storage in MongoDB collections, not MongoDB's native Change Streams feature. The explicit approach maintains behavioral parity with the PostgreSQL backend and retains full application control over sequence-number generation, shard assignment, and record retention — all of which the DynamoDB Streams API contract tightly specifies.

Each stream-enabled table is assigned 4 shards at creation time. Shard identifiers embed the table's globally-unique `table_id` UUID rather than the caller-visible `table_name` (`build_shard_id` in `stream_engine.rs`): `shardId-{table_id}-{i:012}`. Table names are only unique per-account, so a name-derived shard_id would let one account's `GetRecords` read another's records on same-named tables. `table_id` is per-instance, so a `DeleteTable + CreateTable` sequence produces fresh shard_ids; leftover stream records from the deleted table are cleaned up in `delete_table_impl` (`cleanup_stream_state_for_table`). A unique index on `stream_shards.shard_id` rules out duplicate insertions structurally.

On each data write with streams enabled, `write_stream_inline_in_session` runs inside the same session as the base write:

1. Resolve the shard for the item's partition key by reading the table's shard set under the session (`assign_shard_in_session`) and hashing the pk with CRC32.
2. Draw the next sequence number by `$inc`-ing the per-shard counter at `_id: "stream_seq:<shard_id>"` in the `counters` collection — also under the session.
3. Insert the stream record into `stream_records`.

Per-shard counters preserve DynamoDB Streams' contract that sequence numbers are strictly monotonic within a shard and independent across shards. A single global counter would couple unrelated shards' sequence spaces. Session-scoped assignment closes an ordering hole: without it, a fast writer B can draw seq=6 and commit before a slow writer A (which drew seq=5) commits, and a consumer polling between B's commit and A's commit would advance past seq=6 and never see seq=5. With the counter increment inside the write transaction, two writers racing on the same shard conflict at commit time and the loser retries.

Stream event names use DynamoDB wire casing (`INSERT`, `MODIFY`, `REMOVE`) via `event_name_ddb_str`. When `UpdateItem` creates an item that didn't exist (upsert case), the stream layer emits `INSERT`, not `MODIFY` with a fabricated key-only `OldImage`.

`GetRecords` paginates using `{ sequence_number: { $gt: after } }` range queries with ascending sort, backed by a compound index on `(shard_id, sequence_number)`. Retention is 24 hours: a TTL index on `stream_records.created_at` (24 h) drives primary enforcement; a background `stream_record_cleanup_worker` runs hourly as defense in depth. `UpdateTable` stream-enable is idempotent: if shards already exist for the table, it reuses them and preserves the existing `stream_label` rather than rotating it (which would invalidate ARNs previously handed out to consumers). `stream_label` uses `YYYY-MM-DDThh:mm:ss` (second precision, no timezone), byte-for-byte compatible with the PostgreSQL backend.

The `StreamEngine::write_stream_record` trait method is not used on this backend; it returns an explicit error so a caller who invokes it doesn't get a subtly-wrong write outside any transaction session.

### Time to Live (TTL)

When TTL is enabled on a table, the backend creates a sparse MongoDB index on `item_data.{ttl_attribute}.N` for ordinary attribute names and marks `ttl_index_ready: true` on the table doc. This flag means that the table's TTL cleanup path is ready. Dotted attribute names use the literal-field expression path instead of a physical index, but are also marked ready. A background TTL worker (spawned at server startup by `MongoRuntimeHooks::spawn_workers`) sweeps expired items every 60 seconds in batches of 100 per table. Each deletion goes through `DataEngine::delete_item` with a condition expression re-checking expiry, preventing races with concurrent writes. TTL deletions carry `UserIdentity { type: "Service", principalId: "dynamodb.amazonaws.com" }` on their stream records, matching DynamoDB's TTL stream record format.

TTL index creation: `metadata_engine.rs` — `create_ttl_index`. Background worker and stream/GSI companions: `ttl_worker.rs` — `ttl_cleanup_worker`, `stream_record_cleanup_worker`, `gsi_backfill_worker`. Worker spawn: `lib.rs` — `MongoRuntimeHooks::spawn_workers`.

### Control plane state transitions

`CreateTable` (and `RestoreTableFromBackup`) write the catalog row and create the data collection with its indexes before returning. When `control_plane_delay_seconds` > 0 (the default is 0.25) the row is written with `TableStatus: CREATING` and a `status_transition_at` timestamp, and the returned `TableDescription` carries `TableStatus: CREATING`; a background `control_plane_worker` (`ttl_worker.rs`) flips the row to `ACTIVE` once the transition time passes. During the window, data-plane operations against the table return `ResourceNotFoundException`, matching DynamoDB and the PostgreSQL backend. When `control_plane_delay_seconds` is 0, the row is written `ACTIVE` directly and the worker has nothing to do. `DeleteTable` remains inline: it removes the catalog row, drops the data + index collections, deletes tags, and cleans up stream shards / records / counters via `cleanup_stream_state_for_table` (`table_engine.rs`), all before returning — there is no transient `DELETING` state.

GSI creation on `UpdateTable` is the one control-plane operation that does need asynchronous work — a background worker drains index rows in `CREATING` state, backfills the base collection, and flips the row to `ACTIVE`. See the Global and Local Secondary Indexes section for the state machine.

### Authentication and authorization

ExtendDB's mandatory SigV4 authentication is fully supported. Access-key secrets are stored AES-GCM encrypted in `extenddb_catalog.access_keys`. The encryption key is a 256-bit random key generated during `extenddb init`, base64-encoded, and stored in `extenddb_catalog.settings` under `_id: "encryption_key"`. Admin passwords are bcrypt-hashed before storage in `extenddb_catalog.admin_users`.

IAM policy evaluation fetches user-attached policies, group-attached policies (via `iam_groups.members` → `iam_policies` join), role policies, permissions boundaries, and session policies from the catalog.

`MongoEngine::new` rejects connection strings that specify a non-primary read preference (`secondary`, `secondaryPreferred`, `nearest`, `primaryPreferred`). DynamoDB's `ConsistentRead=true` requires linearizable reads; only MongoDB's Primary read preference provides that. A connection string that routes reads to a replica would silently return stale data — a fidelity violation the caller has no way to detect. The check fails at engine construction so misconfiguration surfaces at `extenddb serve` startup, not at request time.

Encryption key bootstrap: `bootstrapper.rs::bootstrap_encryption_key`. Admin password hashing: same file, `bootstrap_admin_user`. Access-key decryption: `credential_store.rs`. IAM policy fetching: `authorization_store.rs`.

### Backup

`CreateBackup` snapshots the source table by running a server-side aggregation pipeline `[{ $out: "_backup_<backup_id>" }]` on the data collection. MongoDB copies items server-side without transferring them through the driver, and the destination is a per-backup collection in `extenddb_data` whose name derives from a UUID (never the caller-visible ARN, which contains characters MongoDB doesn't allow in collection names). Backup metadata (arn, backup_id, table, timestamps, key schema, table class, SSE, on-demand throughput, status) is stored in `extenddb_catalog.backups`.

`RestoreTableFromBackup` recreates the table via the normal CreateTable path (preserving TableClass, SSESpecification, OnDemandThroughput from the backup metadata) and clones the backup collection into the new data collection with the same `$out` stage. `DeleteBackup` drops the backup collection and marks the metadata row `DELETED`.

Backup implementation: `backup_engine.rs`.

### Operational requirements

**Minimum MongoDB version: 7.0.** Required for multi-document ACID transactions and snapshot reads. The MongoDB Rust driver 3.x is technically compatible with earlier server versions; this backend targets 7.0 as the minimum supported.

**Replica set required.** MongoDB must be configured as a replica set before running `extenddb init`. A standalone node does not support multi-document transactions. A single-node replica set is sufficient for development and CI; production deployments should use a 3-node replica set for high availability.

**Primary read preference.** Connection strings must use `readPreference=primary` (the driver default). Non-primary preferences are rejected at engine startup.

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

Configuration struct: `crates/storage-mongodb/src/config.rs`. Sample configuration: `extenddb.sample.toml` — `[storage.mongodb]` section. Setup guide: `docs/local-mongodb-setup.md`.

### Implementation summary

| Crate modified | Change |
|---|---|
| `crates/storage-mongodb/` | New crate — full backend implementation |
| `crates/bin/Cargo.toml` | Added `mongodb` optional feature flag |
| `crates/bin/src/main.rs` | Added `#[cfg(feature = "mongodb")] extern crate` |
| `crates/bin/src/cmd_serve.rs` | Generalized the supported-backend gate from a hard-coded `"postgres"` check to a compile-time list built from enabled features |
| `Cargo.toml` (workspace) | Added crate to members; added `mongodb`, `bson`, `dashmap` workspace dependencies |

No changes to `crates/engine/`, `crates/server/`, `crates/storage/` (trait definitions), `crates/auth/`, or `crates/core/`.

### Design decisions summary

| Decision | Choice | Rationale |
|---|---|---|
| Conditional writes | Read + evaluate + write inside a MongoDB transaction session | Snapshot atomicity gives DynamoDB's contract; loaded item reused for `ReturnValuesOnConditionCheckFailure = ALL_OLD` without a follow-up read. Analyzer-gated pushdown fast path skips the session for a certified subset on tables with no GSIs / streams. |
| UpdateItem concurrency | `_v` version guard inside snapshot txn + WriteConflict retry with jittered exponential backoff | Prevents lost updates; retry ceiling (50) bounds tail latency under sustained contention. |
| WriteConflict handling | Detect via `TransientTransactionError` label, `UnknownTransactionCommitResult` label, or raw code 112; retry with backoff | Converts a raw HTTP 500 into a retryable operation; TWI exhaustion surfaces as `TransactionCanceled` with per-op `TransactionConflict` reasons. |
| GSI updates | Synchronous inline within the base write's session, with 60-second TTL cache short-circuit for tables with no GSIs; async worker-driven backfill on UpdateTable | No Change Stream recovery; GSI reads are strongly consistent; UpdateTable matches DDB's async CREATING → ACTIVE contract. |
| GSI/LSI index docs | Composite `_id` includes both index and base keys; `base_pk` / `base_sk_?` stored as first-class fields | GSI keys are non-unique; base-key disambiguation prevents cross-item overwrite. Base keys as fields let index pagination form a compound cursor without traversing item_data. |
| Composite `_id` | Netstring-encoded (`<len>:<part>,...`) | Unambiguous boundary between pk and sk regardless of content. |
| Binary sort keys | Stored as lowercase hex strings | MongoDB's BSON Binary sort order diverges from DDB's unsigned-lex byte order across mismatched lengths. Hex-encoded strings preserve DDB order under default string comparison and make `begins_with` a plain range filter. |
| Sort key numbers | Native BSON `Decimal128` | Correct ordering by value. Values exceeding Decimal128's 34-digit precision are rejected. |
| DynamoDB Streams | Inline writes to `stream_records` inside the base write's session; per-shard sequence counters | Behavioral parity with PostgreSQL backend; sequence-number monotonicity within a shard is a contract. Session-scoped assignment prevents ordering holes under concurrent writes. |
| Stream shard ID | `shardId-{table_id}-{i:012}` | Table names are only account-unique; `table_id` UUID prevents cross-tenant shard address collisions. |
| Stream retention | TTL index on `stream_records.created_at` (24h) + hourly worker as defense in depth | Primary enforcement is at the storage layer; worker covers TTL-monitor lag or missing index. |
| Idempotency tokens | Unique compound index on `(account_id, token)` + 540s TTL + 600 ms data-plane age filter | Race safety under snapshot isolation; worst-case retention stays ≤10 min regardless of TTL-monitor cadence. |
| Backups | Per-backup collection via server-side `$out` aggregation | No per-item traffic between driver and server; backup metadata schema decouples from collection naming; ARN characters are unsafe as collection names. |
| Parallel scan | Application-side `crc32(pk) % segments` filter with lazy cursor iteration | Avoids per-document write overhead; lazy iteration prevents item-drops on hot-key skew that a hard server-side limit would cause. |
| Non-primary read preference | Rejected at engine startup | `ConsistentRead=true` requires linearizable reads; only Primary provides that. |

### Performance characteristics

**Single-item conditional writes.** One transaction session covers the pre-image read, condition evaluation, base write, GSI synchronization, and stream record insert. On a local replica set this adds ~sub-millisecond of session-start/commit overhead over a raw driver call. The session wrap is what gives DynamoDB's atomicity contract on conditional writes — it is the compatibility, not overhead. The analyzer-gated pushdown fast path collapses this to a single `find_one_and_*` call for the narrow case of certified pushable conditions on tables with no GSIs and no streams.

**Unconditional single-item updates on GSI-free / stream-free tables.** A native-fast-path `find_one_and_update` runs outside any transaction. It always includes `$inc: {_v: 1}` so a concurrent slow-path update cannot pass its versioned filter against a stale snapshot.

**GSI write overhead.** For tables with no GSIs, the `gsi_cache` short-circuits to zero overhead (no catalog query, no I/O) — refreshed at most once per `GSI_CACHE_TTL` window per table. For tables with GSIs, one catalog query fetches the index definitions (cached for subsequent writes) and one upsert or delete runs per index collection per write, all within the base write's session.

**Stream write overhead.** When streams are enabled, each write adds one atomic per-shard counter `$inc` and one document insert into `stream_records`, both within the base write's session.

**Query and Scan.** Direct index lookups on `(pk, sk_?)` for base tables; compound `(pk, sk_?, base_pk, base_sk_?)` lookups for index queries. `GetRecords` uses the compound `(shard_id, sequence_number)` index.

**TransactWriteItems.** Multi-collection ACID transaction; up to 100 operations per the DDB spec. Uncommon in practice — most workloads are single-item operations.

### Testing

Testing is organized in three layers.

**Unit tests** cover pure logic without a live MongoDB instance: netstring composite `_id` encoding, hex sort-key ordering, condition filter compilation, pushdown-analyzer decisions, sequence-number formatting, stream shard-id derivation. Property tests (`crates/storage-mongodb/tests/pushdown_parity.rs`) exercise the parity between the pushdown compiler and the in-Rust `evaluate_condition` reference over randomly generated items and expressions.

**Integration tests** run the dual-target `tests/rust/` suite — the same AWS-SDK wire-conformance tests the PostgreSQL backend runs — against a MongoDB-backed ExtendDB server on a single-node replica set (`mongod --replSet rs0`), covering the full table lifecycle, all item operations (conditional and unconditional), query and scan pagination (base and index), transactions, TTL worker behavior, stream record writes and consumer pagination, GSI propagation and async backfill, backup and restore, and catalog operations. They execute via `devtools/run-mongodb-tests -- --rust --rust-integration`, which stands up the replica set, serves ExtendDB against it, and delegates to `devtools/run-tests --backend mongodb`.

**End-to-end tests** run the existing ExtendDB pytest suite (`tests/`) unchanged against a MongoDB-backed ExtendDB server. The pytest suite speaks the DynamoDB wire protocol and has no backend awareness — a passing run against MongoDB is equivalent to a passing run against PostgreSQL. This is the conformance test baseline required by RFC-0002.

CI (`.github/workflows/integration-mongodb.yml`) runs the pytest and rust-integration suites as two parallel jobs. Each builds ExtendDB with `--no-default-features --features mongodb` (the backend features are mutually exclusive, so the default `postgres` feature must be disabled) and delegates to `devtools/run-mongodb-tests`, which bootstraps a single-node MongoDB 7.0 replica set (`rs.initiate` + wait-for-PRIMARY — a step GitHub `services:` cannot express), serves ExtendDB against it, provisions credentials, and runs the suite via `devtools/run-tests --backend mongodb`. Backend-crate unit and property tests run in the standard `cargo test` workflow.

## Drawbacks

**Replica set requirement.** MongoDB must be run as a replica set for multi-document transactions. Users who run standalone MongoDB will receive a runtime error on transactional operations. This is a MongoDB architectural constraint, not an ExtendDB limitation, and is documented in setup guides.

**TTL throughput at scale.** DynamoDB TTL deletions must emit stream records with a specific service identity. MongoDB's native TTL indexes operate at the storage-engine level with no awareness of ExtendDB's stream system, so this implementation uses an application-level background worker that owns the full deletion lifecycle. The worker runs every 60 seconds and processes 100 expired items per table per pass. This is sufficient for ExtendDB's target deployment contexts. At very high sustained expiration rates the worker will fall behind, and the backlog will grow. This is a known scale limitation, not a correctness issue — DynamoDB's own contract only guarantees expiration within 48 hours, not immediately (see `docs/differences-from-dynamodb.md`, TTL row).

## Alternatives

### Use MongoDB Change Streams for DynamoDB Streams

MongoDB has a native change-data-capture feature (Change Streams) that could back DynamoDB Streams. The implementation instead adopted the explicit `stream_records` collection approach used by the PostgreSQL backend, which gives ExtendDB full control over sequence-number generation, shard assignment, record retention, and iterator behavior — all of which the DynamoDB Streams API contract tightly specifies. Reviewers are invited to weigh in on whether a comparative evaluation of the Change Streams approach should be documented before acceptance.

## Prior art

**MongoDB document model and DynamoDB.** MongoDB's flexible document model has been noted as a natural fit for DynamoDB-style workloads in multiple independent analyses. Amazon DocumentDB (MongoDB-compatible) demonstrates AWS's own recognition of this overlap. The key difference in this implementation is that ExtendDB provides the full DynamoDB API layer — clients using the AWS SDK do not need to know they are talking to MongoDB.

**Condition pushdown pattern.** Compiling application-level filter expressions into storage-native query operators is a well-established pattern in query engines (Apache Arrow DataFusion, Spark, Presto all implement predicate pushdown). The pushdown-analyzer / compiler split in this implementation applies the same principle at the storage backend level, with the analyzer serving as the correctness boundary between the two.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
