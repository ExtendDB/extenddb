# Design: MongoDB Storage Backend

## 1. Overview

The MongoDB backend (`extenddb-storage-mongodb`) implements the same trait surface as
`extenddb-storage-postgres`: the six engine traits (`TableEngine`, `DataEngine`,
`MetadataEngine`, `StreamEngine`, `BackupEngine`, `WorkerStore`) and the catalog
traits (`ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`,
`RateLimitStore`, `AuthorizationStore`).

**Driver:** `mongodb` (official Rust driver, async, multi-document ACID
transactions on replica sets).

**Minimum MongoDB version:** 7.0 (multi-document transactions, snapshot reads).

**Read preference:** `primary` only. `MongoEngine::new` rejects connection strings
that request `secondary`, `secondaryPreferred`, `primaryPreferred`, or `nearest` —
DynamoDB's `ConsistentRead=true` contract requires linearizable reads, which only
Primary provides. Silently routing reads to replicas would return stale data with
no signal to the caller.

## 2. Database Layout

Two databases, mirroring the PostgreSQL backend's catalog / data separation:

| Database             | Purpose                                              |
|----------------------|------------------------------------------------------|
| `extenddb_catalog`   | Table metadata, IAM, settings, metrics, backup metadata |
| `extenddb_data`      | Per-table item collections, per-index collections, streams, idempotency tokens, backup snapshots |

DynamoDB Streams are implemented via inline stream-record writes during data
operations. GSI updates are propagated synchronously inline for existing indexes
and asynchronously via a background worker during UpdateTable-driven GSI
creation.

## 3. Catalog Database Collections

Collections created by `run_catalog_migrations` in `bootstrapper.rs`. Two
additional collections (`iam_group_members`, `backup_items` — the latter is
no longer used) are auto-created on first insert and not in the migration list.

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
  "table_id": "<uuid>",
  "ttl_attribute": null,
  "ttl_index_ready": false,
  "deletion_protection_enabled": false,
  "status_transition_at": null,
  "stream_label": null,
  "table_class": null,
  "sse_specification": null,
  "on_demand_throughput": null
}
```
Unique index on `table_id`. Partial index on `status_transition_at` where not null.

### 3.3 `indexes`
```json
{
  "_id": { "table_id": "...", "index_name": "..." },
  "index_id": "<uuid>",
  "index_type": "GSI|LSI",
  "key_schema": [...],
  "projection": { ... },
  "index_status": "ACTIVE|CREATING",
  "provisioned_throughput": { ... },
  "backfill_cursor": <bson>       // present while index_status = CREATING
}
```
`backfill_cursor` is written by `gsi_backfill_worker` after every batch. The
field is unset when the index flips to `ACTIVE`.

### 3.4 `tags`
```json
{ "_id": { "resource_arn": "...", "tag_key": "..." }, "tag_value": "..." }
```

### 3.5 `settings`
```json
{ "_id": "<key>", "value": "..." }
```
Bootstrapped keys include `catalog_version`, `encryption_key` (base64-encoded
256-bit AES-GCM key), `data_database_name`, and `data_connection_string`.

### 3.6 `admin_users`
```json
{ "_id": "<admin_name>", "password_hash": "<bcrypt>", "created_at": ISODate }
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
  "access_key_id": "<AKIA... | ASIA...>",
  "secret_key_encrypted": BinData,
  "account_id": "...",
  "user_name": "...",
  "is_active": true,
  "created_at": ISODate
}
```
Index on `(account_id, user_name)`. `secret_key_encrypted` is AES-256-GCM
ciphertext of the secret access key using the `settings.encryption_key`;
`access_key_id` is bound as additional authenticated data.

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
  "access_key_id": "ASIA...",
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
Unique index on `access_key_id`. TTL index on `expires_at` (`expireAfterSeconds: 0`).

### 3.12 `iam_policies`
```json
{
  "account_id": "...",
  "principal_type": "user|role|group",
  "principal_name": "...",
  "policy_name": "...",
  "policy_document": { ... },
  "created_at": ISODate
}
```

### 3.13 `iam_permissions_boundaries`
```json
{
  "account_id": "...",
  "principal_type": "user|role",
  "principal_name": "...",
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
Index on `_id.bucket` for pruning.

### 3.15 `login_attempts`
```json
{ "principal": "...", "attempted_at": ISODate, "success": false, "source_ip": "..." }
```
Compound index on `(principal, attempted_at)`.

### 3.16 `backups`
```json
{
  "_id": "<backup_arn>",
  "backup_id": "<uuid>",
  "backup_name": "...",
  "backup_status": "AVAILABLE|DELETED",
  "backup_type": "USER",
  "table_id": "...",
  "table_name": "...",
  "table_arn": "...",
  "account_id": "...",
  "backup_size_bytes": NumberLong,
  "item_count": NumberLong,
  "key_schema": [...],
  "attribute_definitions": [...],
  "billing_mode": "PAY_PER_REQUEST",
  "table_class": null,
  "sse_specification": null,
  "on_demand_throughput": null,
  "created_at": ISODate,
  "table_creation_date_time": <epoch_secs>
}
```
Index on `(account_id, table_name)`. The physical backup collection lives in
`extenddb_data` as `_backup_{backup_id}` — see §4.5.

### 3.17 `continuous_backups`
```json
{
  "account_id": "...",
  "table_name": "...",
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

Each DynamoDB virtual table maps to a MongoDB collection named
`_ddb_{table_id}` (table_id is a UUID assigned at CreateTable). The
collection-name derivation shields the physical layer from caller-visible
name changes and from characters that are unsafe as MongoDB collection
names.

**Document structure:**
```json
{
  "_id": "<netstring(pk, sk)>",
  "pk": "...",
  "sk_s": "...",
  "sk_n": Decimal128,
  "sk_b": "<lowercase hex>",
  "_v": NumberLong,
  "item_data": { ... }
}
```

Fields:

- `_id` — netstring-encoded composite key `<len>:<pk>,<len>:<sk>,`.
  Netstring framing gives an unambiguous boundary between `pk` and `sk`
  regardless of their contents. A naive `{pk}#{sk}` delimiter collides
  on `pk="a#b",sk="c"` vs `pk="a",sk="b#c"`. PK-only tables use raw `pk`
  text as `_id`.
- `pk` — partition key text (matches `composite_pk_to_text` from
  `extenddb_storage::util`).
- `sk_s` / `sk_n` / `sk_b` — typed sort key, absent when the schema has
  no sort key. See §5.2 for the type-specific encoding.
- `_v` — OCC version counter used by `UpdateItem`'s versioned filter
  guard. Absent on freshly-inserted rows (treated as 0); bumped by every
  update, including the native fast path.
- `item_data` — full DynamoDB item serialized as BSON via the
  `AttributeValue` JSON representation. Non-key attribute values retain
  their DynamoDB type tags (`{"S": "hello"}`, `{"N": "42"}`, ...).

**Indexes:**

- `{ pk: 1 }` (PK-only tables) or `{ pk: 1, sk_?: 1 }`, unique.
  String sort-key indexes use `{ locale: "simple" }` collation for
  byte-order comparisons.

### 4.2 Per-Index Collections: `_ddb_{index_id}`

Each GSI/LSI has its own collection. The document schema extends §4.1
with the base table's key attributes as first-class fields:

```json
{
  "_id": "<netstring(idx_pk, idx_sk, base_pk, base_sk)>",
  "pk": "...",              // index partition key
  "sk_s|sk_n|sk_b": ...,    // index sort key
  "base_pk": "...",         // base-table partition key text
  "base_sk_s|_n|_b": ...,   // base-table sort key, typed
  "item_data": { ... }      // projected item per the index's Projection
}
```

Two reasons for the extra base-key material:

1. **Unique identity per base item.** GSI keys are non-unique — multiple
   base items can share `(index_pk, index_sk)`. If `_id` were only
   derived from index keys, two base items with the same GSI-key values
   would upsert to the same document; one would silently overwrite the
   other. Including the base keys in `_id` and the entry-delete filter
   (`index_entry_filter`) makes each entry addressable independently.
2. **Compound pagination cursor.** Index queries and scans sort and
   paginate on `(index_sk?, base_pk, base_sk?)` — see §5.3.
   Having the base-key components as fields (not buried under
   `item_data.<name>.S`) lets the sort and cursor filters use per-field
   indexes.

Every index collection carries a compound index on
`(pk, sk_?, base_pk, base_sk_?)` created by `create_index_data_collection`.
Simple collation is used whenever the tuple contains a string component.

### 4.3 `stream_records`
```json
{
  "sequence_number": "<21-digit zero-padded>",
  "shard_id": "shardId-{table_id}-{index:012}",
  "table_id": "...",
  "event_name": "INSERT|MODIFY|REMOVE",
  "record_data": { ... full StreamRecord as BSON ... },
  "created_at": ISODate
}
```

- TTL index on `created_at` with `expireAfterSeconds = 24 * 3600` —
  primary retention enforcement.
- Compound index on `(shard_id, sequence_number)` — powers `GetRecords`
  (`shard_id` equality + `sequence_number > cursor` range with ascending
  sort). Without it, every consumer poll runs a full-collection scan.

### 4.4 `stream_shards`
```json
{
  "shard_id": "shardId-{table_id}-{index:012}",
  "table_id": "...",
  "starting_sequence_number": "<21-digit>",
  "ending_sequence_number": null,
  "created_at": ISODate
}
```

Unique index on `shard_id`. Four shards per stream-enabled table.

`shard_id` embeds `table_id` (a UUID) rather than `table_name`. Table
names are only unique per-account; a name-derived scheme would let one
account's `GetRecords(shard_id)` observe another account's records on
same-named tables. `table_id` resets on `DeleteTable + CreateTable`, so
recreated tables get fresh shard_ids and leftover records from the
deleted table cannot resurface.

### 4.5 `counters`
```json
{ "_id": "stream_seq:<shard_id>", "value": NumberLong }
```

One document per shard. `$inc` on `value` inside a session yields the
next sequence number. Per-shard counters (not a single global counter)
preserve DynamoDB Streams' contract that sequence numbers are strictly
monotonic within a shard and independent across shards.

### 4.6 `idempotency_tokens`
```json
{
  "account_id": "...",
  "token": "...",
  "fingerprint": "...",
  "created_at": ISODate
}
```

- TTL index on `created_at` with `expireAfterSeconds = 540`.
- **Unique compound index on `(account_id, token)`.**

The TTL is 540s (9 min), tighter than DDB's 10-min window. MongoDB's TTL
monitor runs on a ~60s cadence, so worst-case retention with TTL = 540s
is ≤10 min. The data-plane read path (`transact_write_items_impl`) also
filters existing rows by `created_at` age < 600 000 ms so retention is
correct regardless of TTL-monitor timing.

The unique index closes a race window: two concurrent `TransactWriteItems`
calls with the same token both take snapshot reads that miss the other's
uncommitted insert; without the constraint, both would commit and the
operation would execute twice. With it, the second inserter fails
`E11000` and the write path resolves the winner by re-reading (still
subject to the age filter — if the winner has just expired, the retry
does a fresh insert).

### 4.7 `_backup_{backup_id}`

One collection per user-created backup. Populated by a server-side
`[{ $out: "_backup_{backup_id}" }]` aggregation pipeline on the source
data collection, so items are copied server-side without transferring
through the driver. Restored the same way, in reverse. `DeleteBackup`
drops the collection.

## 5. Key Design Decisions

### 5.1 Session-scoped conditional writes

`PutItem`, `DeleteItem`, and `UpdateItem` — when they carry a
`ConditionExpression`, a `StreamCapture`, or write to a table with GSIs —
run inside a MongoDB client session bound to a multi-document transaction
with snapshot read concern and majority write concern. Within the session:

1. `find_one` the current document.
2. Evaluate the DynamoDB condition in Rust
   (`extenddb_core::expression::evaluate_condition`) against the loaded
   item.
3. Write (`find_one_and_replace` / `delete_one` / versioned `replace_one`).
4. Synchronize GSIs (`sync_indexes_in_session`).
5. Emit any stream record (`write_stream_inline_in_session`), including
   per-shard sequence-number `$inc` — also in the same session.
6. Commit.

All five happen on the same session, so a concurrent conflicting writer
manifests as a WriteConflict at commit — which the caller retries — not
as a stale-read anomaly. The pre-image loaded in step 1 is reused for
`ReturnValuesOnConditionCheckFailure = ALL_OLD` and for `OldImage` on any
attached stream capture; no follow-up read is needed.

Update-as-insert (the pre-image was `None`) emits an `INSERT` stream
event with no `OldImage`, not a `MODIFY` with a fabricated key-only stub.

`UpdateItem` also always fetches the pre-image regardless of the caller's
`ReturnValues` setting — the pre-image is required to compute correct
GSI deltas when the update changes or removes an indexed attribute, and
skipping it leaves stale entries in index collections forever.

**Native fast path.** For unconditional updates on tables with no streams
and no GSIs (fresh cache says `Some(false)`), the backend collapses the
transaction to a single `find_one_and_update` outside any session using
compiled MongoDB atomic operators (`$set` / `$unset` / plus
`$inc: {_v: 1}`). The `_v` bump is unconditional on this path: without
it, a concurrent session-scoped update running against a stale snapshot
could pass its versioned filter and lost-update over the fast-path
write.

Implementation: `data_engine.rs::put_item_impl`, `delete_item_impl`,
`update_item_impl`, and `execute_transact_write_op_in_session` for the
TWI arms.

### 5.2 Filter-pushdown fast path (analyzer-gated)

An optional pushdown fast path skips the session for conditional writes
that a static analyzer certifies as safe. The path is gated on:

- `condition` is present.
- `stream` is `None`.
- `gsi_cache_get_fresh(table_id) == Some(false)` (i.e. the cache is
  fresh AND says the table has no GSIs).
- `pushdown::is_pushable(cond, maps) == Pushable::Yes`.

Under those guards, single-document `find_one_and_replace` /
`find_one_and_delete` provides atomicity — no session, no GSI sync, no
stream write. The compiled filter is merged with the primary-key filter
under `$and`.

The **compiler** (`condition.rs`) is intentionally broader than
production usage: it translates
`attribute_exists`, `attribute_not_exists`, `attribute_type`,
`begins_with`, `contains`, `BETWEEN`, `IN`, `=`, `<>`, `<`, `<=`, `>`,
`>=`, `AND`, `OR`, `NOT`, and `size` into BSON filters. Some of those
translations are correct only for certain operand types.

The **analyzer** (`pushdown.rs::is_pushable`) is the load-bearing
correctness boundary. It certifies a whole-condition subset that is
provably in agreement with `evaluate_condition`:

- Existence functions (`attribute_exists`, `attribute_not_exists`) — always
  pushable.
- `attribute_type(path, :t)` — pushable when `:t` resolves to a placeholder
  whose value is one of the 10 valid DDB type tags
  (`S`, `N`, `B`, `BOOL`, `NULL`, `L`, `M`, `SS`, `NS`, `BS`). Without
  the whitelist a malicious `:t` could produce a `$`-prefixed pseudo-field.
- `begins_with(path, :S)` — string-only.
- `contains(path, :S)` — string-only.
- `path <op> :S` for any comparator — string operands are stored
  verbatim, lex order matches wire order.
- `path = :B` / `path <> :B` — binary equality only. Ordering
  comparators on binary are refused because the compiler stores B as
  base64 strings inside `item_data`, and base64 lex order diverges from
  bytewise order across mismatched lengths.
- `path = / <> :BOOL` and `path = / <> :NULL` — value-only.
- `AND` / `OR` — pushable iff both children are pushable (all-or-nothing;
  cherry-picking would confuse composition semantics).
- `NOT attribute_exists(path)` / `NOT attribute_not_exists(path)` — the
  only pushable `NOT` forms. Anywhere else, MongoDB's `$nor` semantics
  on missing paths diverge from DDB's three-valued logic.

Not pushable: any operand of type `N` (numbers stored as strings; `"10"
> "9"` is false lex-wise), `size` (MongoDB has no UTF-16 code-unit
count), `BETWEEN` / `IN` (pending proptest coverage; the compiler emits
them, the analyzer refuses them), and `NOT` around anything else.

Property tests (`tests/pushdown_parity.rs`) generate random items and
expressions, compile the filter, and check that a pure-Rust BSON
interpreter and `evaluate_condition` agree on match/no-match — the
regression harness that lets the analyzer's certification be extended
safely.

### 5.3 Query and Scan

**Query key mapping.**
- Partition-key equality: `{ pk: <value> }`.
- Sort-key conditions map to typed filters on `sk_s` / `sk_n` / `sk_b`.
- `BETWEEN` with `low > high` is rejected at the storage boundary with a
  `ValidationException`.
- `begins_with(:S)` emits `{ sk_s: { $gte: prefix, $lt: next_string_prefix(prefix) } }`.
  `next_string_prefix` computes the exclusive upper bound by incrementing
  the rightmost non-`char::MAX` code point (skipping the surrogate gap
  via `char::from_u32` retry); if the entire prefix is `char::MAX` it
  returns `None` and the caller emits only the `$gte` bound. The
  earlier `prefix + char::MAX` scheme excluded stored strings equal to
  `s + char::MAX` (or extending past it) that DDB matches.
- `begins_with(:B)` emits the same range shape on the hex-encoded sort
  key: `{ sk_b: { $gte: hex(prefix), $lt: hex(increment_bytes(prefix)) } }`.

**Pagination.** `ExclusiveStartKey` **merges** into the existing sort-key
predicate rather than replacing it. Base-table Query paginates on a
single `$gt` / `$lt` sort-key comparison. Naively inserting
`filter.insert(sk, {$gt: cursor})` drops the caller's original
`BETWEEN` / `begins_with` bound and returns items outside it on page
2+. The merge covers three shapes:

- No existing sk predicate → insert cursor bound.
- Existing operator map (`{ $gte: X, $lt: Y }`) → merge the cursor bound
  into the map.
- Existing equality (`sk = X`) → wrap both under `$and`.

**Index Query and Scan cursors.** Index-key values are non-unique, so
pagination cannot rely on `(pk, sk)` alone — items with duplicate index
keys would form an unstable page boundary. Instead, index queries
paginate on the compound tuple `(index_sk?, base_pk, base_sk?)`
expressed as a lexicographic `$or`:

```
(a > A) OR (a == A AND b > B) OR (a == A AND b == B AND c > C)
```

reversed to `$lt` for descending scans. Index Scan paginates on
`(pk, sk?, base_pk, base_sk?)` (index Scan lacks the partition-key
equality that Query has). Sort direction is applied to the whole
tuple so pagination is deterministic across items sharing an index-key
value. `LastEvaluatedKey` carries both the index-key and base-key
components so the next page's `ExclusiveStartKey` can rehydrate the
cursor.

**Scan** uses lazy cursor iteration and stops when either `limit + 1`
in-segment items are accumulated or the cursor exhausts. It does not
impose a server-side hard limit. `Parallel Scan` filters items in the
application via `crc32(pk) % TotalSegments == Segment`. A hard
`(limit + 1) * TotalSegments` limit combined with post-fetch filtering
silently drops items under hot-key skew — an entire limit window can
land in one segment, terminating the scan with the others empty.
MongoDB batches under the hood (~101 docs), so lazy iteration is
efficient even without a hard limit — at most one extra network batch
beyond what is returned.

Implementation: `data_engine.rs::query_impl`, `scan_impl`,
`build_sk_filter`, `next_string_prefix`, `increment_bytes`.

### 5.4 GSI propagation (synchronous inline + async backfill)

**Live writes** synchronize GSIs in the same session as the base write.
`sync_indexes_in_session` walks the `indexes` catalog for the table_id,
and for each index:

1. If the old item had the index-key attributes, project it into the
   index shape (`project_item`, respecting the `Projection` setting) and
   run `delete_one` filtered on both index-key AND base-key components
   (`index_entry_filter`). Filtering on index keys alone would delete
   every base item's entry sharing those keys.
2. If the new item has the index-key attributes, project and upsert into
   the index collection (`index_document` + `replace_one` with
   `upsert: true`).

The `gsi_cache` on `MongoEngine` (`DashMap<table_id, (has_gsi, inserted)>`)
short-circuits the catalog walk when we know the table has no indexes.
Cache entries expire after `GSI_CACHE_TTL` (60s) so out-of-band GSI
changes on other ExtendDB instances converge within the window.

**Async backfill.** `UpdateTable` GSI-create inserts the catalog
document with `index_status: "CREATING"` and pre-creates the mongo
index-collection + its compound query index (so live reads on the
CREATING index don't run collection scans). A background
`gsi_backfill_worker` (spawned in `MongoRuntimeHooks::spawn_workers`)
runs every 5 seconds:

1. `find { index_status: "CREATING", index_type: "GSI" }` on the
   `indexes` catalog collection.
2. For each job, read the base collection in batches of 500 items
   (`backfill_gsi_batch`) starting from the row's persistent
   `backfill_cursor` field.
3. Upsert projected items into the index collection.
4. After every batch, persist `backfill_cursor` back to the catalog
   document so a mid-backfill server restart resumes where it left off.
5. When a batch returns fewer docs than the batch size (base fully
   scanned), flip the catalog row to
   `index_status: "ACTIVE"` and unset `backfill_cursor`.

Live writes during the backfill window continue to hit `sync_indexes_in_session`,
which writes to CREATING indexes too — index-catalog membership, not
status, is what gates the write path. All writes are upserts on the
same `_id` shape, so a base item touched by both paths converges
regardless of interleaving.

**Index-key input validation.** `validate_index_keys_for_item` rejects
wrong-type or empty index-key attributes on the item **before** any
write work (post-apply for `UpdateItem`). Without this,
`index_document` would silently skip the typed `sk_?` field when it
sees a type mismatch, leaving the resulting index row un-locatable for
subsequent deletes. Inside `TransactWriteItems`, the failure surfaces
as a per-item `CancellationReason::ValidationError` rather than a
top-level `ValidationException`.

### 5.5 DynamoDB Streams

Stream records are written inline during data operations, using the same
storage model as the PostgreSQL backend. This design gives ExtendDB full
control over sequence numbers, shard assignment, and retention — all of
which the DynamoDB Streams API contract tightly specifies. Native
MongoDB Change Streams are not used.

**Shard model.** Four shards per stream-enabled table, created at
`CreateTable` (or on the first `UpdateTable` stream-enable). Shard IDs
embed the table's UUID: `shardId-{table_id}-{index:012}`. Table names
are only unique per account, so a name-derived scheme would allow
cross-tenant shard-id collisions on same-named tables. `table_id` is
per-instance; a `DeleteTable + CreateTable` sequence produces fresh
shard_ids, and `cleanup_stream_state_for_table` in `delete_table_impl`
removes the deleted table's shards, records, and counters so nothing
resurfaces on recreation.

**Write path** (`write_stream_inline_in_session`): resolve the event
type from `(old_item, new_item)` presence, build key + old-image +
new-image per `StreamViewType`, hash the pk with CRC32 to select a
shard, draw the next sequence number, insert the record. Both shard
resolution and sequence-number assignment run inside the same session
as the data write.

**Session-scoped sequence numbers.** `next_sequence_number_in_session`
does `find_one_and_update` with `$inc` on the per-shard counter
document — inside the write session. Without this, a fast writer B
could draw seq=6 and commit before a slow writer A (which drew seq=5)
commits; a consumer polling between B's commit and A's commit sees
seq=6 and advances past it, so when A finally commits, seq=5 lands
behind the cursor and is never returned. Session-scoped assignment
also serializes concurrent writers on the same shard: two `$inc`s
racing under snapshot isolation conflict at commit, and the loser
retries.

**Per-shard counters.** Counter documents are keyed by
`_id: "stream_seq:<shard_id>"`. A single global counter would couple
the sequence spaces of unrelated shards, so a writer pushing records
into shard B would advance the counter shard A reads back — producing
non-contiguous sequence numbers on shard A's `GetRecords` pages.

**Event names.** `event_name_ddb_str` emits DynamoDB wire casing
(`INSERT`, `MODIFY`, `REMOVE`). When `UpdateItem` creates an item
(upsert with no pre-image), the stream layer emits an `INSERT`, not a
`MODIFY` with a fabricated key-only `OldImage`.

**Retention.** 24 hours, enforced by a TTL index on
`stream_records.created_at`. A `stream_record_cleanup_worker` (hourly)
provides defense in depth.

**`GetRecords` path.** `{ shard_id: <s>, sequence_number: { $gt: after } }`
with ascending sort, backed by the compound index
`(shard_id, sequence_number)`.

**Non-session `write_stream_record`.** The `StreamEngine::write_stream_record`
trait method is a stub that returns an explicit error — the mongo backend
has no callers for it, and enrolling in the wrong or no session would let
a stream record commit while its base-table write rolls back.

**`UpdateTable` stream-enable is idempotent.** If shards already exist
for the table, it reuses them and preserves the existing `stream_label`;
only a first-time enable rotates it. A repeat `UpdateTable`
`{ StreamEnabled: true }` would otherwise duplicate the shard set and
invalidate stream ARNs previously handed out to consumers.

**`stream_label` format.** `YYYY-MM-DDThh:mm:ss` (second precision, no
timezone). Byte-for-byte compatible with the PostgreSQL backend so an
ARN issued by one backend is parseable by tooling that only ever saw
the other. See `format_stream_label` in `table_engine.rs`.

### 5.6 Write conflict handling

**Transient-conflict detection.** `is_transient_write_conflict` returns
true for any of: the `TransientTransactionError` label, the
`UnknownTransactionCommitResult` label, or a raw `WriteConflict` (code
112). Under snapshot isolation these all mean "your write lost to a
concurrent writer; retry the whole transaction."

**Retry loop.** Session-scoped writes (Put / Delete / Update / TWI) wrap
the transaction body in a `for attempt in 0..TRANSIENT_RETRY_ATTEMPTS`
loop (50 attempts). Each attempt starts a fresh transaction, runs the
body, and either commits, aborts and retries (transient), or aborts and
returns (fatal). Retries sleep with jittered exponential backoff
(`backoff_sleep`, base 50 µs).

**UpdateItem's OCC guard on top.** Even inside the transaction snapshot,
`UpdateItem` uses a `_v` version filter. The transaction guarantees the
snapshot the update was computed from; the versioned replace_one
guarantees the write only commits if the row's `_v` still matches what
we read. If `matched_count == 0` the attempt returns `Stale` (a distinct
signal from `Transient`) and the loop restarts. The native fast path
always emits `$inc: {_v: 1}` so a concurrent slow-path update racing
against a stale snapshot fails its filter and retries.

**Exhaustion behavior.** Single-item retry exhaustion returns
`StorageError::Internal` (rare in practice; the retry ceiling is high).
`TransactWriteItems` exhaustion surfaces as
`StorageError::TransactionCanceled` with a synthetic per-op
`TransactionConflict` reason so wire consumers see the DDB-canonical
error string instead of a bare HTTP 500.

**Conditional insert races.** A conditional PutItem on a nonexistent key
that raced against a concurrent inserter can manifest as either an
E11000 duplicate-key (unique-index race) or a WriteConflict (snapshot
race). The write path maps E11000 to `ConditionFailed` after
re-reading the winner outside the session; WriteConflict falls through
the normal retry loop, and the retry re-reads and sees the winner via
the existing-doc branch.

### 5.7 TTL

Two TTL surfaces:

**Storage-native TTL indexes** — configured at bootstrap:
- `idempotency_tokens.created_at` — 540s (§4.6 for the rationale).
- `stream_records.created_at` — 24 h.
- `iam_sessions.expires_at` — `expireAfterSeconds: 0`.

**Application-level DynamoDB TTL** — user-configured `TimeToLive`
attribute per table. MongoDB's native TTL runs at the storage engine
and cannot emit ExtendDB stream records with the required `Service`
user identity, so the backend maintains its own worker.

`update_ttl` sets `ttl_attribute` on the table doc. `create_ttl_index`
creates a sparse index on `item_data.{ttl_attribute}.N` for ordinary
attribute names and marks `ttl_index_ready: true`. The flag means that
the table's TTL cleanup path is ready; dotted attribute names use the
literal-field expression path instead of a physical index, but are also
marked ready. The `ttl_cleanup_worker` (60s cadence) walks tables with
`ttl_index_ready`, finds expired items in batches of 100
per table, and issues `DataEngine::delete_item` with a re-check
condition (`attribute_exists(ttl) AND ttl <= now`) to prevent races
with concurrent writes. The delete carries a `StreamCapture` with
`UserIdentity { identity_type: "Service", principal_id: "dynamodb.amazonaws.com" }`
so the stream record matches DynamoDB's format.

### 5.8 Backups

`CreateBackup` snapshots the source table by running a server-side
aggregation pipeline `[{ $out: "_backup_<backup_id>" }]` on the data
collection. `$out` writes the target collection server-side without
per-item traffic between the driver and the server. The destination
name is derived from a UUID because the caller-visible `backup_arn`
contains characters (`:`, `/`) that MongoDB does not allow in
collection names.

`RestoreTableFromBackup` recreates the target table via
`CreateTable` (preserving `TableClass` / `SSESpecification` /
`OnDemandThroughput` from the backup metadata), then clones the backup
collection into the new data collection with the same `$out` pipeline
in reverse.

`DeleteBackup` drops the physical collection using `backup_id` from
the metadata document and marks the metadata row `DELETED`.

Implementation: `backup_engine.rs`.

### 5.9 Account ID validation

Injection defense on all account-scoped operations (`validate_account_id`
in `lib.rs`). Reject `$` (operator injection), `.` (field-path
traversal), null bytes, and non-ASCII. Runs before any query
construction.

### 5.10 Catalog version check

`read_catalog_version` returns the `settings.catalog_version` value;
`expected_catalog_version` returns the compiled-in `0.0.2`. The bin
layer compares them on `extenddb serve` startup — same pattern as the
PostgreSQL backend.

## 6. Crate Structure

```
crates/storage-mongodb/
├── Cargo.toml
└── src/
    ├── lib.rs                  # MongoEngine, GSI cache, inventory registrations
    ├── config.rs               # MongoStorageConfig
    ├── operations.rs           # OperationsEngine (CLI): connection parsing, redaction
    ├── bootstrapper.rs         # init / destroy / migrations
    ├── table_engine.rs         # CreateTable, UpdateTable, DeleteTable, DescribeTable
    ├── data_engine.rs          # PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, Transactions, pushdown fast path
    ├── data/mod.rs             # composite_id, item_to_document, index_document, binary_sk_to_hex
    ├── condition.rs            # DDB condition Expr → MongoDB filter compiler
    ├── pushdown.rs             # is_pushable analyzer — pushdown correctness boundary
    ├── stream_engine.rs        # Shard management, sequence numbers, GetRecords
    ├── metadata_engine.rs      # TTL configuration, tags, table size bookkeeping
    ├── ttl_worker.rs           # TTL sweep, stream record cleanup, GSI backfill workers
    ├── backup_engine.rs        # $out-based backup and restore
    ├── management_store.rs     # IAM CRUD, settings, metrics, rate limiting
    ├── authorization_store.rs  # Policy fetching for auth decisions
    ├── credential_store.rs     # Access-key lookup + AES-GCM decryption
    ├── catalog_store.rs        # SettingsStore / DiagnosticsStore glue
    ├── admin_store.rs          # Admin operations (currently thin)
    └── worker_store.rs         # WorkerStore: CREATING -> ACTIVE table transitions
```

## 7. `MongoEngine` Struct

```rust
pub struct MongoEngine {
    client: mongodb::Client,
    catalog_db: mongodb::Database,
    data_db: mongodb::Database,
    region: String,
    max_connections: u32,
    gsi_cache: dashmap::DashMap<String, (bool, std::time::Instant)>,
}

const GSI_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);
```

`MongoEngine::new` parses the connection string, rejects non-primary
read preferences, then constructs the client with `max_pool_size =
max_connections`. `catalog_db` and `data_db` are lightweight handles
against the single shared client.

`gsi_cache` entries carry the observation time so a stale entry
(`elapsed() > GSI_CACHE_TTL`) is treated as a miss and re-read from the
catalog. This keeps writes correct when a GSI is added or dropped on
another ExtendDB instance sharing the catalog.

## 8. Configuration

```toml
[storage.mongodb]
connection_string = "mongodb://localhost:27017/?replicaSet=rs0"
max_connections = 50
max_catalog_connections = 20
```

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoStorageConfig {
    pub connection_string: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_catalog_connections")]
    pub max_catalog_connections: u32,
}
```

The connection string must use `readPreference=primary` (the driver
default). Persisting the connection string in
`settings.data_connection_string` uses the raw string as-is; the bin
layer redacts the password from it for display via the
`OperationsEngine::redact_connection_string` hook.

## 9. Bootstrapper Flow

**`extenddb init`:**

1. Materialize both databases with sentinel collections (MongoDB creates
   databases lazily on first write, so we explicitly create
   `schema_history` in the catalog db and `idempotency_tokens` in the
   data db).
2. `run_catalog_migrations` — create the 17 catalog collections and
   their indexes; seed `settings.catalog_version = 0.0.2`; record
   the migration in `schema_history`.
3. `create_data_db` — create `idempotency_tokens` (TTL + unique index),
   `stream_shards` (unique on shard_id), `stream_records` (TTL + compound
   query index).
4. `bootstrap_encryption_key` — generate a random 256-bit key,
   base64-encode, insert into `settings` (idempotent — E11000 races are
   silently ignored).
5. `bootstrap_default_account` — insert `default` account if none
   exists.
6. `bootstrap_admin_user` — bcrypt-hash the password, insert into
   `admin_users`.
7. `record_data_connection` — record `data_database_name` and
   `data_connection_string` in `settings`.

**`extenddb destroy`:** drop both databases.

## 10. Inventory Registrations

Six `inventory::submit!` blocks in `lib.rs`:

- `BackendRegistration` — factory for `MongoBootstrapper`, called by
  `extenddb init`.
- `OperationsEngineRegistration` — CLI operations (connection parsing,
  redaction, identifier validation, sensitive-key detection).
- `StorageConfigRegistration` — TOML deserializer for
  `[storage.mongodb]`.
- `SettingsStoreRegistration` — factory for `MongoCatalogStore` acting
  as `SettingsStore`.
- `DiagnosticsStoreRegistration` — factory for `MongoCatalogStore`
  acting as `DiagnosticsStore`.
- `ServerComponentsRegistration` — factory called by `extenddb serve`
  that constructs `MongoEngine`, `MongoCatalogStore`,
  `MongoCredentialStore`, and `MongoRuntimeHooks` (which spawns the
  TTL, stream-cleanup, and GSI-backfill workers).

No changes to `crates/server/`, `crates/engine/`, or `crates/auth/`
are required — everything flows through the plugin registration
system.

## 11. Dependencies

```toml
[dependencies]
mongodb.workspace = true       # 3.x, async, tokio-runtime
bson.workspace = true
dashmap.workspace = true       # GSI existence cache
tokio.workspace = true
async-trait.workspace = true
futures.workspace = true
serde.workspace = true
serde_json.workspace = true
toml.workspace = true
tracing.workspace = true
time.workspace = true
uuid.workspace = true
base64.workspace = true
rand.workspace = true
bcrypt.workspace = true
aes-gcm.workspace = true
zeroize.workspace = true
thiserror.workspace = true
anyhow.workspace = true
inventory.workspace = true
extenddb-core.workspace = true
extenddb-storage.workspace = true
extenddb-auth.workspace = true
crc32fast.workspace = true

[dev-dependencies]
proptest = "1"   # pushdown parity harness
```

## 12. Feature coverage

The backend implements every trait in `extenddb-storage`:

**Data plane**
- `TableEngine` — Create/Delete/Describe/List/UpdateTable, including
  GSI create with async backfill, LSI create, and idempotent
  stream-enable on UpdateTable.
- `DataEngine` — PutItem, GetItem, DeleteItem, UpdateItem, Query,
  Scan (including parallel scan), TransactGetItems, TransactWriteItems,
  BatchGetItem, BatchWriteItem. Condition expressions run session-
  scoped with an analyzer-gated pushdown fast path for a certified
  subset.
- `StreamEngine` — session-scoped per-shard sequence numbers, four
  shards per stream, CRC32 pk routing, TRIM_HORIZON / LATEST /
  AT_SEQUENCE_NUMBER / AFTER_SEQUENCE_NUMBER iterators, 24h retention.
- `MetadataEngine` — TTL lifecycle, tags, table-size tracking.
- `BackupEngine` — CreateBackup, RestoreTableFromBackup, DeleteBackup
  via server-side `$out` aggregation.

**Control plane and catalog**
- `Bootstrapper` — init, destroy, migrate, verify. Creates the
  catalog and data databases, seeds encryption key and admin user,
  applies index schema.
- `WorkerStore` — `process_control_plane_transitions` flips tables
  from `CREATING` to `ACTIVE` once their `status_transition_at`
  passes. `create_table_impl` (and restore) write `CREATING` with a
  scheduled transition when `control_plane_delay_seconds` > 0, or
  `ACTIVE` directly when it is 0. `delete_table_impl` remains inline
  (no `DELETING` transient state).
- `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`,
  `RateLimitStore` — the catalog trait surface.
- `AuthorizationStore` — user/group/role/permissions-boundary/session
  policy lookup for IAM evaluation.
- `MongoCredentialStore` — SigV4 credential resolution with
  AES-GCM-decrypted secret keys.

**Background workers** — spawned from `MongoRuntimeHooks::spawn_workers`:
- `ttl_cleanup_worker` — sweep expired items every 60 s, emit
  service-attributed stream records for the deletes.
- `stream_record_cleanup_worker` — hourly defense-in-depth for the
  24 h retention TTL index.
- `gsi_backfill_worker` — drain `indexes` rows in `CREATING` state,
  scan the base collection with a persistent cursor, flip to ACTIVE.
- `control_plane_worker` — flip `tables` rows from `CREATING` to
  `ACTIVE` once their scheduled `status_transition_at` passes.

## 13. Testing Strategy

- **Unit tests:** netstring `_id` encoding, hex sort-key ordering,
  condition compiler, shard-id derivation, next_string_prefix,
  Decimal128 rejection, index-doc key disambiguation.
- **Property tests:** `tests/pushdown_parity.rs` — random items and
  expressions checked for agreement between the compiled BSON filter
  and `evaluate_condition`.
- **Integration tests:** The dual-target `tests/rust/` SDK suite (the
  same AWS-SDK wire tests the PostgreSQL backend runs) executed against a
  MongoDB-backed server on a single-node replica set in Docker
  (`mongod --replSet rs0`), via `devtools/run-mongodb-tests -- --rust
  --rust-integration`.
- **Existing pytest suite:** Passes unchanged (backend-agnostic wire
  protocol tests), via `devtools/run-mongodb-tests -- --pytest
  --comprehensive --parallel`.
- **CI:** `.github/workflows/integration-mongodb.yml` runs the pytest and
  rust-integration suites as two parallel jobs, each building with
  `--no-default-features --features mongodb` and delegating to
  `devtools/run-mongodb-tests`,
  which bootstraps a single-node MongoDB 7.0 replica set.

## 14. Deployment Requirements

- MongoDB **7.0+** in **replica set** mode. Standalone nodes reject
  multi-document transactions.
- **`readPreference=primary`** on the connection string. Non-primary
  is rejected at engine startup.
- Single-node replica set is fine for development / CI. Production:
  3-node replica set.
- Target scale: < 500 DynamoDB tables. At 500 tables with 2 GSIs each
  (~1,500 collections), WiredTiger handles the count comfortably with
  default settings. Ensure `ulimit -n ≥ 65536`.

## 15. Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Conditional writes | Read + evaluate + write inside a MongoDB transaction session | Snapshot atomicity delivers DDB's contract; pre-image reused for `ReturnValuesOnConditionCheckFailure = ALL_OLD` and `OldImage`. |
| Filter pushdown | Analyzer-gated fast path; `is_pushable` certifies a subset | Compiler in `condition.rs` handles broader syntax than production uses; the analyzer is the correctness boundary. |
| GSI live sync | Synchronous inline within the base write's session, gated by a 60s-TTL cache | Strongly-consistent GSI reads; no Change Stream recovery. |
| GSI async backfill | `CREATING` → `ACTIVE` via `gsi_backfill_worker` with persistent `backfill_cursor` | Matches DDB's async UpdateTable contract; restart-safe. |
| Index-doc identity | Composite `_id = netstring(idx_pk, idx_sk, base_pk, base_sk)` + `base_pk` / `base_sk_?` as first-class fields | Base-key disambiguation for non-unique index keys; compound cursor pagination without touching `item_data`. |
| Composite `_id` | Netstring `<len>:<part>,...` | Delimiter-free framing between pk and sk. |
| Binary sort keys | Stored as lowercase hex strings | BSON Binary comparison is length-first-then-content, diverging from DDB unsigned-lex byte order across mismatched lengths. |
| Sort key numbers | Native BSON `Decimal128`; values exceeding 34 sig-digits rejected | Correct numeric ordering; no silent precision loss. |
| DynamoDB Streams | Inline record writes to `stream_records` in the base write's session; per-shard sequence counters | Behavioral parity with PostgreSQL backend; per-shard monotonicity is a contract. |
| Sequence assignment | Inside the write session | Prevents ordering holes where a fast writer commits a higher seq before a slower earlier one does. |
| Stream shard ID | `shardId-{table_id}-{i:012}` | Cross-tenant isolation on same-named tables. |
| Stream retention | 24h TTL index on `stream_records.created_at` + hourly cleanup worker | Primary enforcement at storage; worker is defense in depth. |
| WriteConflict handling | Retry with jittered exponential backoff (50 attempts); TWI exhaustion → `TransactionCanceled` with synthetic `TransactionConflict` reasons | Bounded tail latency; DDB-canonical error surface. |
| UpdateItem concurrency | Snapshot txn + `_v` version filter + retry; native fast-path always `$inc: {_v: 1}` | Prevents lost updates; fast path stays safe against a concurrent slow path. |
| Idempotency retention | Unique `(account_id, token)` index + 540s TTL + 600 ms data-plane age filter | Race safety under snapshot isolation; ≤10-min worst-case retention regardless of TTL-monitor cadence. |
| Backups | Per-backup collection via server-side `$out` aggregation | No per-item driver traffic; metadata schema decoupled from collection name. |
| Parallel scan | Application-side `crc32(pk) % segments` + lazy cursor | Rare feature; server-side bucketing would tax every write. Lazy iteration prevents item-drops under hot-key skew. |
| Read preference | `primary` enforced at engine startup | `ConsistentRead=true` requires linearizable reads. |

## 16. Performance Characteristics

**Hot path — single-item conditional write.** One MongoDB transaction
session covers pre-image read, condition eval, base write, GSI sync,
stream insert (per-shard counter `$inc` + document insert). On a local
replica set, session overhead is ~sub-ms over a raw driver call. The
session is what buys the DDB atomicity contract — it is the
compatibility, not overhead. The pushdown fast path collapses this to
a single `find_one_and_*` for the certified subset on tables with no
GSIs / streams.

**Unconditional single-item update, no GSIs, no streams.** Native
fast path: one `find_one_and_update` with compiled `$set` / `$unset` /
`$inc` outside any session. The `$inc: {_v: 1}` keeps the fast path
safe against a concurrent slow-path update.

**GSI write overhead.** No GSIs: zero (cached). Has GSIs: one catalog
`find` (cached for subsequent writes on the same table until
`GSI_CACHE_TTL` elapses) + one upsert or delete per index per write,
all inside the base write's session.

**Stream write overhead.** One counter `$inc` + one `stream_records`
insert per write, inside the base write's session.

**Query / Scan.** Index lookups on `(pk, sk_?)` for base tables and
`(pk, sk_?, base_pk, base_sk_?)` for index queries. `GetRecords`
uses the compound `(shard_id, sequence_number)` index.

**TransactWriteItems.** Multi-collection ACID transaction with
snapshot read concern and majority write concern; up to 100 operations
per the DDB spec. Retried on transient conflicts with jittered backoff.
