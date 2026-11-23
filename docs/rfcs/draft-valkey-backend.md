# RFC-000X: Valkey Storage Backend (with and without Valkey Search)

- Status: Draft
- Author: @rlunar
- Created: 2026-11-23
- FCP ends: (set when entering Final Comment Period)
- Tracking issue: TBD

## Summary

This RFC proposes a Valkey storage backend for ExtendDB whose purpose is a DynamoDB-compatible API with
sub-millisecond point-operation latency. The backend keeps all data, including the IAM catalog, in Valkey. It
uses Valkey Functions (server-side Lua) so that every DynamoDB write, including its secondary-index, stream, TTL
and idempotency side effects, is one atomic, one-round-trip call.

The RFC defines two operating modes of one backend, selected at runtime by probing the server:

- **Core mode (no Valkey Search).** Only built-in Valkey data types (hashes, sorted sets, streams) and Functions.
  This mode serves every operation in the "Supported Operations" list except `SearchVectors` and vector indexes.
  It is the mode that carries the latency claim.
- **Search mode (Valkey Search module loaded).** Everything in core mode, plus vector indexes and `SearchVectors`
  backed by `FT.*` indexes. Search-backed secondary indexes are an experiment gated on benchmark results, not a
  commitment.

The RFC also specifies how to benchmark the backend against the PostgreSQL, SQLite, MongoDB and Cassandra
backends, and sets a go/no-go gate: if the measured latency does not beat the existing backends by a clear
margin, the backend is not worth carrying.

## Motivation

Every existing ExtendDB backend is a disk-oriented database reached through a driver and, for PostgreSQL and
MongoDB, a transaction. A single-item write is one or more round trips plus a commit, which puts the typical
point-write latency in the low milliseconds. SQLite in memory mode is faster but is a single-process, ephemeral
store (`sqlite-memory`), and SQLite files serialize writers.

There is a class of DynamoDB workloads where latency, not scale-out or durability, is the primary requirement:
session and feature stores, leaderboards, rate limiters, real-time bidding and personalization lookups,
and development or test environments that want production-like latency. These users often already run Valkey
(or Redis-compatible stores) and would like to keep the DynamoDB API and SDK surface.

Valkey is a fit for this role:

- Single-threaded command execution on in-memory data gives microsecond-scale command latency. A loopback
  `GET` takes tens of microseconds.
- Valkey Functions run a Lua script atomically on the server. That removes the transaction machinery
  other backends need (Cassandra's two-phase commit in ADR-0017, MongoDB's session and write-conflict retry).
- Sorted sets with lexicographic range commands (`ZRANGEBYLEX`) map directly onto DynamoDB's partition key and
  sort key model, including `begins_with`, `BETWEEN` and reverse order.
- Valkey is BSD-3-Clause licensed. The server is a runtime dependency the operator runs, not code ExtendDB links,
  so it does not enter the Apache-2.0 distribution. The Rust client crate does, and must pass
  `devtools/check-licenses`.
- Amazon ElastiCache for Valkey and Amazon MemoryDB run Valkey as managed services, which gives self-hosted and
  cloud operators a path without running the database themselves. MemoryDB adds multi-AZ durability through a
  transaction log, at a documented write-latency cost (see Durability).

### What "sub-millisecond" means here

End-to-end latency from an SDK is the sum of client, network, TLS, HTTP, SigV4, authorization, JSON
encoding and storage time. Only the last part is the backend's. This RFC therefore defines the claim in
layers (see Benchmarking):

1. **Storage layer (L0):** the `DataEngine` call itself, measured in process.
2. **Server layer (L1):** request received to response written, measured by the server's own latency metrics.
3. **Wire layer (L2):** a native load generator over loopback TLS with keep-alive connections.

The target is p99 under 1 ms at L1 and p50 under 1 ms at L2, for single-item `GetItem`, `PutItem` and simple
`UpdateItem` on items up to 1 KB, on a warm connection, with the backend at or below about 50% of saturation. These
are proposed targets. Phase 0 exists to measure whether they are reachable before the rest is built.

The auth/authz cache (`docs/design/12-auth-authz-cache.md`) already removes catalog round trips from the
steady-state data path, so the hot path is one storage call. That is what makes the target plausible.

## Detailed design

### Scope

This RFC proposes:

- A new crate `extenddb-storage-valkey` at `crates/storage-valkey/`.
- A `valkey` Cargo feature on the `extenddb` binary crate, mutually exclusive with the other backend features
  per the existing build-time check.
- Implementations of all traits required for a `StorageEngine` and `CatalogStore`, following RFC-0002 and
  meeting the invariants in RFC-0003.
- Runtime detection of the Valkey Search module, with vector search enabled only when it is present.
- A benchmark harness and methodology (see Benchmarking).

This RFC does not propose:

- Valkey Cluster (sharded) support in the first release. See "Future: cluster mode" for why this is a real
  constraint and not a deferral of convenience.
- Using Valkey as a cache in front of another backend. See Alternatives.
- Support for non-Valkey servers (Redis OSS, Dragonfly, KeyDB). The backend relies on Valkey Functions
  semantics. Compatibility with other servers may happen but is neither tested nor promised.
- Changes to the wire protocol, `crates/engine/`, `crates/server/`, `crates/auth/` or `crates/core/`.
- Changing the default backend.

### Repository structure and plugin registration

The backend lives at `crates/storage-valkey/`, selected by a `valkey` feature on the `extenddb` bin crate
(`valkey = ["extenddb-storage-valkey"]`). It follows the descriptor pattern in
`crates/storage/src/backend.rs`: the crate exposes `backend()` returning an `extenddb_storage::Backend`, and the
thin `main` calls `set_backend(extenddb_storage_valkey::backend())`. The descriptor carries the bootstrapper,
config deserializer, operations engine, settings store, management store, diagnostics store, server components
factory and catalog migrations, as for the other backends.

The Search module is **not** a build-time feature. The module is a property of the server the operator runs, so
the backend probes it once at startup (`MODULE LIST`, then `FT._LIST`), the way the PostgreSQL backend probes
pgvector. There is nothing to link, and one binary serves both modes.

### Client library

The backend needs async, pipelined Valkey access with `FCALL` support, TLS and authentication (ACL user and
password). Two candidates:

| Candidate | For | Against |
|---|---|---|
| `redis` crate (redis-rs), multiplexed connections | Mature, tokio-native, small dependency set, generic `cmd()` for `FCALL`, `FUNCTION LOAD` | Not Valkey-specific; need our own reconnect and failover policy |
| `valkey-glide` (Rust core) | Official Valkey client, built-in reconnect, AZ-aware routing, Apache-2.0 | Larger dependency tree (protobuf, native core), more license review |

Default: `redis` crate with a small fixed pool (default 8) of multiplexed connections, round-robin. Pool size
and multiplexed-versus-dedicated connections are benchmark parameters (Phase 0), because head-of-line effects
on a single multiplexed connection can cost more tail latency than the extra connections cost in CPU.
The licenses of the chosen crate and its transitive dependencies must pass `devtools/check-licenses`.

### Conceptual model

Valkey executes one command (or one Function call) at a time per instance. A Function call is therefore an
atomic, isolated unit with respect to every other client. The backend relies on this for what other backends
build from transactions:

- Every write is one `FCALL` that performs the base-item write, the old-index-entry deletes, the new-index-entry
  inserts, the stream record, the TTL registration and the idempotency token together.
- Optimistic concurrency covers the part that cannot run in Lua: condition and update-expression evaluation
  stays in Rust (`extenddb_core::expression`, as RFC-0003 6.1 requires). The Rust side reads the item and its
  version, evaluates, then calls a Function that applies the write only if the version is unchanged.

Because the Functions are generic (they receive precomputed keys, blobs and side-effect lists from Rust and never
interpret DynamoDB expressions), the DynamoDB semantics have exactly one implementation, in `extenddb_core`.

### Data layout

All keys carry a configurable prefix `{p}` (default `xdb`). The backend never issues `FLUSHALL`, `FLUSHDB` or
unprefixed `SCAN`; `extenddb destroy` deletes only keys under its prefix. Several ExtendDB deployments can share
one Valkey instance with distinct prefixes, though this is discouraged because of the single-threaded
execution model (one tenant's slow Function delays every other).

| Key pattern | Type | Contents |
|---|---|---|
| `{p}:i:{tid}:{pk}[:{sk}]` | HASH | One item. Fields: `d` (item blob), `v` (version, integer), `ix` (index entries the item currently has, for deletion), `ttl` (current TTL score, if any) |
| `{p}:sk:{tid}:{pk}` | ZSET | Sort keys of one partition, all score 0, ordered lexicographically. Absent on tables with no sort key |
| `{p}:pr:{tid}:{b}` | ZSET | Partition registry, bucket `b` of 4096. Members are partition keys. On tables with no sort key this is the item registry |
| `{p}:gi:{tid}:{iid}:{ipk}` | ZSET | One secondary-index partition. Member: index sort key, then base pk, then base sk |
| `{p}:gr:{tid}:{iid}:{b}` | ZSET | Index partition registry (for index `Scan`) |
| `{p}:ttl:{tid}` | ZSET | Score: expiry epoch seconds. Member: item key |
| `{p}:ss:{shard_id}` | STREAM | Stream records of one shard |
| `{p}:idem:{acct}:{token}` | STRING | Idempotency token and request fingerprint, with a 540 s expiry |
| `{p}:cat:...` | HASH, SET, ZSET | Catalog: accounts, tables, indexes, IAM entities, access keys, policies, settings, backups, schema version |
| `{p}:vi:{tid}:{iid}` | FT index over HASH | Vector indexes (Search mode only) |

`tid` and `iid` are the table's and index's unique ids, not their names, so a deleted and re-created table of
the same name inherits nothing (RFC-0003 8.2).

#### Key encoding

- **Identity (RFC-0003 1.1, 1.2).** Every key component is length-prefixed (`<len>:<bytes>`), never joined by
  an unescaped delimiter. Valkey keys are binary-safe, so `pk` values of any byte sequence, including the empty
  string where permitted, cannot collide.
- **Sort keys (RFC-0003 1.3, 1.4, 7.4).** The `ZSET` member for a sort key is a memcomparable encoding whose
  byte order equals DynamoDB order:
  - String: UTF-8 bytes, with `0x00` escaped and a terminator, so byte order is the required UTF-8 byte order.
  - Binary: raw bytes, same escaping, giving unsigned lexicographic order regardless of length.
  - Number: sign, exponent and decimal digits encoded so byte order equals numeric order, with a complemented
    form for negative values. It holds the full 38 significant digits and the exponent range -130 to +125, so
    this backend has no precision limit to reject (unlike MongoDB's 34 digits).
  `ZRANGEBYLEX` and `ZREVRANGEBYLEX` then implement `=`, `<`, `<=`, `>`, `>=`, `BETWEEN` and `begins_with`
  (prefix range) directly, in either direction, with `LIMIT`.
- **Secondary-index members (RFC-0003 2.1).** The member is `enc(index sk) || enc(base pk) || enc(base sk)`, so
  index entries are unique per base item even when index keys repeat, and ordering matches the compound
  `(index sk, base pk, base sk)` cursor that `LastEvaluatedKey` carries (RFC-0003 2.6).

#### Item blob

The `d` field holds the item in DynamoDB's type-tagged JSON, the same representation other backends store.
Rust encodes and decodes it. Lua never decodes the blob: everything the Function needs (index entries, TTL score,
version) arrives as separate arguments and is stored in sibling fields. A denser binary format is a Phase 2
benchmark candidate (Open question 3).

### Operations and round trips

The latency claim rests on round trips, so this table is part of the design.

| DynamoDB operation | Round trips | Mechanism |
|---|---|---|
| `GetItem` | 1 | `HGET d` (eventually consistent may use a replica) |
| `PutItem`, unconditional | 1 | `FCALL put`; returns the old blob when `ReturnValues` asks |
| `PutItem`/`DeleteItem`, `attribute_not_exists(pk)` or `attribute_exists(pk)` | 1 | Same Function with an existence flag. A certified, narrow pushdown (see Condition evaluation) |
| `PutItem`/`DeleteItem`, other conditions | 2 | `HMGET d v`, evaluate in Rust, `FCALL` with expected version |
| `UpdateItem` | 2 | `HMGET d v`, apply `UpdateAction`s in Rust, `FCALL` with expected version |
| `Query` (base table or index) | 1 | `FCALL query`: `ZRANGEBYLEX` then `HGET` of up to `limit` items, bounded by 1 MB |
| `Scan` | 1 | `FCALL scan` over the partition registry |
| `BatchGetItem` (100) | 1 | One `FCALL` or one pipeline |
| `BatchWriteItem` (25) | 1 | One pipeline of write Functions |
| `TransactGetItems` | 1 | `FCALL snapshot_read`, atomic, so all reads see one state (RFC-0003 3.5) |
| `TransactWriteItems` | 2 | Atomic snapshot read, evaluate in Rust, one `FCALL` that checks every version then applies every write |

#### Writes

`put` receives: item key, new blob, new version, the new set of index entries, the stream capture parameters,
the TTL score, the expected-version precondition, and the optional idempotency token. In one atomic step it:

1. Reads the old `v`, `ix` and `ttl`, and checks the precondition. On a mismatch it returns a
   `VERSION_MISMATCH` marker and changes nothing.
2. Removes the old index entries listed in the stored `ix`, and writes the new ones. Index maintenance does
   not depend on `ReturnValues` or streams, because the old entries come from the stored `ix` (RFC-0003 2.2).
3. Writes the item hash, adds the sort key to `sk`, adds the partition to the registry, and updates the TTL
   zset.
4. When capture is requested, appends the stream record (event type derived from old-item existence:
   `INSERT` if absent, else `MODIFY`; `REMOVE` for deletes).

Because the Function runs atomically, a crash between "data" and "stream record" is not possible, and
the stream record becomes visible if and only if the data write did (RFC-0003 5.1, 3.2).

#### Condition evaluation

Conditions and update expressions are evaluated by the existing `extenddb_core::expression` code against the
item read in step 1 of the optimistic loop. This satisfies RFC-0003 6.1 by delegation and keeps one
implementation of DynamoDB semantics.

A narrow pushdown exists for two conditions only, `attribute_not_exists(<key attr>)` and
`attribute_exists(<key attr>)`, because key-attribute existence is exactly "does the item hash exist" and cannot
diverge from the evaluator. These are the common idempotent-create and update-only-if-present patterns, and
they take one round trip instead of two. The Mongo backend's experience (a compiler broader than its
certifying analyzer) argues against growing this set. Any addition must come with a property test against
`evaluate_condition`, as `crates/storage-mongodb/tests/pushdown_parity.rs` does.

`ReturnValuesOnConditionCheckFailure=ALL_OLD` is free: the item read for evaluation is the item returned.

#### Concurrency (RFC-0003 section 4)

- **4.1 Unconditional writes never fail on contention.** The unconditional `put` carries no expected version, so
  concurrent writers serialize in the server and both succeed, last-writer-wins.
- **4.2 Conditional writes.** If the version check fails because another writer got in, the loop re-reads and
  re-evaluates. A condition that is no longer true yields `ConditionalCheckFailedException`.
- **4.4 `UpdateItem` serialization.** The version precondition prevents lost updates. Raw optimistic retry would
  degrade under a hot key, where *n* clients each pay two round trips and one wins per cycle. ExtendDB
  supports one server per catalog (README, "One instance per database"), so the backend adds a striped
  in-process async lock keyed by item key (4096 stripes). Contending requests on one server queue on the lock
  and almost never see a version mismatch. The version check remains the safety net for background workers and
  for a second instance, which is unsupported but must not corrupt data. The retry ceiling and backoff follow the
  MongoDB design, and exhaustion returns `TransientConflict` mapped to `ServiceUnavailable` for single-item
  writes and `TransactionConflict` for transactions (RFC-0003 4.3).
- **4.5 Read-your-writes.** `ConsistentRead=true` always reads the primary. Reads from replicas are used only for
  eventually consistent requests, and only when `read_from_replicas = true` (default `false`).

#### Transactions

`TransactWriteItems` reads every target item and version in one atomic `snapshot_read`, evaluates every condition
and update in Rust, then makes one `FCALL txn_write` that (a) checks the idempotency token, (b) checks every
expected version, and only if all match, (c) applies every write with all index, stream and TTL effects. If a
version differs, the call changes nothing, and Rust re-runs from the read, up to a bounded number of times. A
condition failure is detected in Rust from the snapshot and returns `TransactionCanceled` with per-operation
`CancellationReasons` in input order (RFC-0003 3.4). Exhausting the retry budget returns
`TransactionCanceled` with `TransactionConflict`.

`txn_write` with 100 operations holds the server for the duration of the call. The cost is bounded by
DynamoDB's limits (100 operations, 4 MB), and Phase 0 measures its effect on the latency of other clients
(Open question 5).

Idempotency tokens (`ClientRequestToken`) are checked and set inside the same Function with `SET NX` semantics
and a 540 s expiry, so concurrent duplicates execute exactly once (RFC-0003 3.3).

### Secondary indexes

Both modes maintain LSIs and GSIs the same way, as sorted sets updated in the base write's Function:

- Sparse: the Rust side computes index entries only for items that have the index key attributes. An
  attribute of the wrong type is rejected before the write (RFC-0003 2.3).
- Entries are updated synchronously, so GSI reads are strongly consistent. This is stronger than DynamoDB, which
  is allowed, and is the same choice the MongoDB and SQLite backends made. `ConsistentRead=true` on a GSI is
  still rejected by the engine (RFC-0003 2.7).
- Index `Query` is one `FCALL`: `ZRANGEBYLEX` on the index partition, then `HGET d` of each base item, with
  the projection applied in Rust. This avoids storing projected copies, and a base-item read inside the Function
  costs microseconds.
- Index `Scan` walks the index partition registry.
- Adding a GSI through `UpdateTable` writes the index as `CREATING` and starts a background backfill that pages
  the base table registry with a persisted cursor, as `gsi_backfill_worker` does in MongoDB. Live writes
  during the backfill already maintain `CREATING` indexes. The index flips to `ACTIVE` only when the backfill
  completes (RFC-0003 2.4).
- `TransactWriteItems` goes through the same Function, so transactional writes update indexes (RFC-0003 2.5).

### Query and Scan

- `Query`: `ZRANGEBYLEX` bounds come from the key condition. `ExclusiveStartKey` is applied as an *additional* `(`
  exclusive bound on the same side (RFC-0003 2.6), so a paginated `BETWEEN` keeps its upper bound on page 2.
  `Limit` caps items read before filtering (7.1). `LastEvaluatedKey` is returned only when more items remain (7.2).
- `Scan`: iterates the partition registry buckets in order, then each partition's sort keys. The cursor is the
  last item key. A page ends at `Limit`, 1 MB or the end of data.
- **Parallel scan.** `segment = (crc32(pk) % 4096) % TotalSegments`. Each segment walks only its own buckets, so no
  segment reads other segments' partitions. This avoids the full-collection scan per segment that MongoDB's
  design accepted. `scan_key_in_segment` answers by hashing the key, with no storage call.
- Order across partitions is registry order, which is stable and deterministic, though unspecified in DynamoDB.

### DynamoDB Streams

Each stream-enabled table has 4 shards, with ids `shardId-{table_id}-{i:012}` (RFC-0003 5.3). One Valkey
`STREAM` per shard stores the records.

- **Sequence numbers.** The Function appends with `XADD` and an auto-generated `<ms>-<seq>` id. The stream id is
  monotonic and ordered by commit, because the append happens inside the atomic write. The DynamoDB sequence
  number is the id rendered as a zero-padded 21-digit decimal (`ms` and a 6-digit counter), matching
  `sequence_number_width()` of 21. A record cannot become visible before an earlier one on the same shard
  (RFC-0003 5.2).
- **Atomicity.** Same Function as the data write (RFC-0003 5.1).
- **`GetRecords`.** `XRANGE` from the exclusive start id with `COUNT`. `LATEST` uses the last id.
- **Retention.** `XTRIM MINID` with a 30-hour horizon, run by a worker, as the Cassandra backend keeps a buffer
  over DynamoDB's 24 hours (RFC-0003 5.5). Time-based ids make this a cheap trim.
- **Enable idempotency.** `UpdateTable` enabling an already-enabled stream reuses the shards and label (5.6).
- **Event types.** `INSERT`, `MODIFY` and `REMOVE` with `OldImage` and `NewImage` per `StreamViewType` (5.4).

The Function stores `old` and `new` blobs as stream entry fields without decoding them, so Lua stays
format-agnostic.

### Time to Live

The `put` Function maintains `{p}:ttl:{tid}` (score = expiry epoch seconds) from the TTL score passed in by
Rust, and removes the previous entry using the item's stored `ttl` field. The TTL worker, spawned through
`ServerRuntimeHooks::spawn_workers` (RFC-0003 10.1), reads `ZRANGEBYSCORE -inf now LIMIT 0 N` and deletes each
due item through the normal delete path with an expiry re-check, so the delete emits the `REMOVE` stream record
with the `dynamodb.amazonaws.com` service identity and cleans index entries. Sorted-set TTL is cheaper than
Cassandra's generation-fenced queue (ADR-0010) and has no scan at expiry time.

Enabling TTL on an existing table backfills the zset by paging the registry with a persisted cursor, and
`create_ttl_index` marks the table ready when finished. Valkey 9's per-field hash expiration (`HEXPIRE`) is
deliberately **not** used: it removes data without any application hook, so it cannot emit `REMOVE` records.

### Catalog and IAM

The `ManagementStore`, `AdminStore`, `PasswordHashStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore` and
`AuthorizationStore` traits are implemented over `{p}:cat:*` hashes and sets. Valkey Functions give atomic
multi-key updates, so unlike Cassandra's catalog (ADR-0011, ADR-0012) referential integrity is enforced
exactly, in one `FCALL`, with no race window.

The catalog must live in the same backend, because a binary serves exactly one backend and the `Backend`
descriptor has one bootstrapper. It follows that **Valkey must be configured to persist** (see Durability): the
catalog holds credentials, and losing it locks every client out. Access-key secrets are encrypted with the
credential key as in other backends (RFC `0000-credential-encryption-key`). Catalog migrations use a
`{p}:cat:schema_version` key plus a versioned Function library (`FUNCTION LOAD REPLACE`); an upgrade loads the
new library before the binary starts serving.

### Durability and persistence

Valkey acknowledges writes from memory. What survives a crash depends on configuration the operator owns, so the
backend states its durability class and checks what it can.

| Profile (`durability`) | Valkey configuration | Loss window on crash | Latency impact |
|---|---|---|---|
| `latency` (default) | AOF `appendfsync everysec`, or RDB | Up to about 1 s of acknowledged writes | None |
| `durable` | AOF `appendfsync always`, plus `WAIT 1` on a replica for each write | None for a single failure, once the replica acked | Adds fsync and replication wait, typically milliseconds |
| Managed (MemoryDB) | Multi-AZ transaction log | None for acknowledged writes | Documented single-digit-ms writes: outside the sub-ms target |

Startup checks (fail fast, like MongoDB's read-preference check):

- `maxmemory-policy` must be `noeviction`. Any evicting policy can silently delete catalog or item keys and is
  refused. A full instance then returns `OOM`, which the backend maps to `InternalServerError` (503-class retry),
  never partial writes.
- Persistence must be enabled (`appendonly yes` or an RDB schedule), unless `durability = "ephemeral"` is set
  explicitly. The ephemeral profile is for development and CI, and is the analogue of `sqlite-memory`.
- The server version and Functions support must meet the minimum (see Operational requirements).

This is a real difference from DynamoDB and the disk-based backends, and goes into
`docs/differences-from-dynamodb.md` together with the profile table.

### Backup, restore and snapshots

`BackupCatalog`, `LegacyBackupReader`, `RestoreEngine` and `SnapshotEngine` are required by the `StorageEngine`
supertrait. Valkey has no multi-version snapshot read across keys, so:

- **Phase 1:** `SnapshotEngine` returns `StorageError::Unsupported`, as Cassandra does. Backups use the in-catalog
  logical path (ADR-0019 pattern), a documented non-point-in-time scan. Operators additionally use Valkey's own
  RDB/AOF for disaster recovery.
- **Phase 3 (optional):** a true snapshot through a copy-on-write undo log. The snapshot records the current
  write counter `E` for the table and sets `snap:{tid}`. While it is set, the write Function saves the pre-image
  of any item with `v <= E` into an undo hash before overwriting it. The snapshot reader pages the registry and
  prefers the undo image. The cost is one extra existence check per write, only on tables being snapshotted.
  This makes `SnapshotEngine` conformant and lets the engine-owned backup path stream to a backup store
  instead of holding backups in memory.

### Valkey Search mode

#### What Search adds

Valkey Search indexes `HASH` or JSON keys by prefix and supports vector (`HNSW` and `FLAT`), tag, numeric and, from
1.2, full-text fields, queried with `FT.SEARCH` (`PARAMS`, `LIMIT`, `SORTBY`, `INKEYS`, `RETURN`).

Items are already stored as hashes, so no data model change is needed: in Search mode the write Function also
writes the vector attribute to a sibling field (`vec:{iid}`, FLOAT32 little-endian) and the optional partition
scope to a TAG field.

#### Vector search (the committed use)

- `CreateTable` and `UpdateTable` with vector indexes create one `FT.CREATE` per vector index over the table's
  item prefix, with `DIM`, `DISTANCE_METRIC` mapped from `COSINE`, `EUCLIDEAN` (`L2`) and `DOT_PRODUCT` (`IP`),
  `TYPE FLOAT32`.
- `SearchVectors` is `FT.SEARCH` with a `KNN` clause. A search supplies an equality on the index's `HASH`
  element (ADR-0004), mapped to a tag pre-filter, `@pk:{value}=>[KNN k @vec $q]`.
- **Exactness.** ADR-0004 chose an exact scan so results are deterministic. The default is `FLAT`, which is
  exact. `HNSW` is approximate, so it is an opt-in per-index extension, documented as such, which RFC-0002 allows
  for backend-specific extensions that do not change standard semantics. Whether `HNSW` is selectable through
  existing DynamoDB request shapes is Open question 6.
- **Visibility.** Whether an `FT.SEARCH` immediately after a write sees that write must be verified
  (Open question 4). If not, `SearchVectors` is documented as eventually consistent, or the write Function
  waits on the module's ingestion.
- Without the module, the backend returns `None` from `as_vector_search`. The engine then refuses
  `CreateTable` and `UpdateTable` with vector indexes and `SearchVectors` using the existing strings
  (`Vector indexes are not supported by this storage backend`, `SearchVectors is not supported by this storage
  backend`), which keeps the grep contract in `AGENTS.md`.

Search indexes live in memory and are rebuilt after a restart, so vector index availability lags startup.
`DescribeTable` reports such an index as not yet `ACTIVE` until the rebuild completes.

#### Search-backed secondary indexes (experiment, not a commitment)

An alternative to sorted-set GSIs: declare `TAG` and `NUMERIC` fields on the hash and query with
`FT.SEARCH ... SORTBY ... LIMIT`. This removes index-entry maintenance from the write Function. The costs are
likely to outweigh that:

- Valkey Search `NUMERIC` fields are believed to be double precision, which cannot hold DynamoDB's 38-digit numbers
  and would violate RFC-0003 1.3. This needs verification.
- A search query costs more than `ZRANGEBYLEX` and returns through a different execution path, which works
  against the latency goal.
- Byte-order sorting of string and binary sort keys through `SORTBY` must match DynamoDB's order (RFC-0003 7.4).
- It adds index memory and a module dependency for a feature core mode already serves.

The experiment (E1 below) measures it. It is adopted only if it beats sorted-set GSIs on write latency without
losing correctness. Otherwise Search mode stays vector-only.

#### What Search does not change

`FilterExpression`, key-condition evaluation, `Scan` ordering and parallel-scan semantics stay in the engine and
the sorted-set paths. Pushing filter expressions into `FT.SEARCH` would create a second implementation of the
evaluator that must match `extenddb_core` for every input (RFC-0003 6.1), and the engine applies filters after
`Limit` by contract (7.1). It is out of scope.

### Mode comparison

| Capability | Core mode | Search mode |
|---|---|---|
| Item CRUD, conditions, transactions, batch | Yes | Yes |
| Query and Scan, base table and indexes (LSI, GSI) | Yes (sorted sets) | Yes (sorted sets); search-backed GSIs only if E1 passes |
| Streams, TTL, tags, IAM, backups | Yes | Yes |
| Vector indexes and `SearchVectors` | Refused | Yes (`FLAT` exact; `HNSW` opt-in) |
| Extra server requirement | Valkey with Functions | Valkey with the valkey-search module |
| Extra memory | None | Index memory per vector index |
| Sub-ms point-operation target | Primary target | Same, plus write overhead on tables with vector indexes |

### Configuration

```toml
backend = "valkey"

[storage.valkey]
url = "rediss://127.0.0.1:6379"   # TLS strongly recommended off-loopback
# Credentials come from EXTENDDB__STORAGE__VALKEY__USERNAME / __PASSWORD, not the file.
key_prefix = "xdb"
pool_size = 8
durability = "latency"            # "latency" | "durable" | "ephemeral"
search = "auto"                   # "off" | "auto" | "required"
read_from_replicas = false        # eventually consistent reads only
```

`search = "required"` fails startup if the module is missing, `auto` probes and degrades to core mode, `off` ignores
the module. Connection strings and passwords are redacted in logs through the operations engine, as for the
other backends.

### Operational requirements

- **Valkey version.** Minimum 8.0; 9.x recommended. Functions require 7.0 or later. The tested matrix is fixed in
  Phase 0 (Valkey 8.x and 9.x, with and without the module). Valkey Search from the version that supports
  the features used.
- **Memory.** The whole dataset, the indexes and replication buffers must fit in RAM. Per-item overhead is the
  hash, one `sk` zset entry, one registry entry, and one entry per secondary index, plus the optional TTL entry.
  Phase 0 measures bytes per item against the other backends' on-disk size.
- **Network.** Run ExtendDB and Valkey on the same host or the same AZ. Latency targets assume
  loopback or sub-100 µs links. Valkey is a plaintext-by-default server: enable TLS, `requirepass` or ACL
  users, and bind to a private interface. The backend refuses `redis://` URLs to non-loopback hosts unless
  `allow_insecure_remote = true`.
- **Single instance per catalog,** as for all backends.
- **Script duration.** Page size, batch size and transaction size are bounded so no Function exceeds a
  few milliseconds. `busy-reply-threshold` (default 5 s) is not a tuning target. Backfill and TTL workers use small batches.
- **Monitoring.** `INFO` stats (`used_memory`, `instantaneous_ops_per_sec`, `latency` doctor), `SLOWLOG` and
  `LATENCY` are the operator's tools. ExtendDB's own metrics record `StorageQueryLatency` and
  `PoolAcquireLatency` (existing metric names) per operation.

### Conformance against RFC-0003

| Criterion | How the Valkey backend meets it |
|---|---|
| 1 Identity (collision-free keys, 38-digit numbers, binary order) | Length-prefixed key parts; memcomparable sort-key encoding holds 38 digits and unsigned byte order |
| 2 Secondary indexes (duplicates, stale entries, sparse, backfill, transactional, pagination) | Index members include base keys; old entries from stored `ix`; Rust-side sparsity; background backfill; same Function for transactions; additive cursor bounds |
| 3 Transactions (atomic, streams, tokens, reasons, snapshot) | One `FCALL`; token inside; ordered reasons computed in Rust; `snapshot_read` is atomic |
| 4 Concurrency (no spurious failures, update serialization) | Unconditional put has no precondition; striped lock plus version check; mapped errors |
| 5 Streams (atomic, ordered, isolated, retention, idempotent enable) | Append in the write Function; commit-ordered ids; shard ids embed `table_id`; `XTRIM MINID`; reuse shards |
| 6 Conditions (evaluator parity; no dead code; docs match code) | Delegates to `extenddb_core`; narrow certified pushdown only; features not built are marked as proposed |
| 7 Query and Scan (limit, `LastEvaluatedKey`, parallel completeness, sort order) | `ZRANGEBYLEX` order; bucketed segments; no early termination |
| 8 Multi-tenancy (account isolation, table-name reuse) | Account in every key; `tid`-based keys |
| 9 Error fidelity | Valkey errors (`OOM`, `LOADING`, `READONLY`, `BUSY`, connection loss) are mapped to DynamoDB-compatible codes; none leak |
| 10 Operations (workers, cache coherence, `UpdateTable` idempotency) | Workers spawned via `spawn_workers`; GSI-presence cache with short TTL; idempotent `UpdateTable` |

Optional traits are all implemented, except `SnapshotEngine` in Phase 1 (see Backup). Per RFC-0002, a trait is
implemented fully or returns `Unsupported`, never half-implemented.

### Behavioral differences from DynamoDB (to record in `docs/differences-from-dynamodb.md`)

- Durability depends on Valkey configuration (profile table above). `latency` can lose about one second of
  acknowledged writes on a crash.
- The dataset must fit in memory, and a full instance refuses writes.
- Phase 1 backups are not point-in-time. `RestoreTableToPointInTime` is not supported until a change log exists.
- GSIs are strongly consistent, as on MongoDB and SQLite.
- `SearchVectors` on `HNSW` is approximate, and on `FLAT` is exact. Core mode has no vector search.
- Long `Scan` or `Query` pages and 100-operation transactions briefly occupy the server thread. A latency-sensitive
  workload should not share an instance with long batch jobs.

### Future: cluster mode

Valkey Cluster shards by key hash slot. Functions can only touch keys in one slot, and `TransactWriteItems` can
span any items in an account. To make a partition-local write atomic, the item key, `sk` zset and TTL
entry need a hash tag derived from the partition key (`{tid:pk}`). Index entries and stream records live under
different partition keys, so a write that touches an index partition on another slot cannot be atomic with the
base write. Cluster mode would therefore need asynchronous GSIs (a queue entry in the base write's slot, as in
the Cassandra and PostgreSQL designs) and could not offer cross-partition `TransactWriteItems` without a protocol
like ADR-0017. That is a different backend in practice. The decision here is to keep v1 on a single primary
(with replicas for failover and optional eventually consistent reads), and to scale vertically or by running
one ExtendDB catalog per Valkey shard. A separate RFC covers cluster mode if demand appears.

## Benchmarking

The backend is justified only if it is measurably faster, so the benchmark is part of the design and is
built first.

### Questions the benchmark must answer

1. Does Valkey (core mode) reach the sub-ms targets at L1 and L2, and with what headroom?
2. How much faster is it than each existing backend for each operation, at equal durability class?
3. What does Search mode cost: write overhead with vector indexes, vector query latency and recall for `FLAT`
   and `HNSW`?
4. Where does it fall over: memory per item, hot-key contention, large transactions, scan pages, restart time?
5. Does latency stay flat as data grows (1M, 10M, 100M items)?

### Layers

| Layer | What is measured | How |
|---|---|---|
| Floor | The server and network without ExtendDB | `valkey-benchmark -t get,set -P 1`; `PING` round trip |
| L0 storage | `DataEngine` and `TableEngine` calls in process, no HTTP | A harness binary links the backend crate and drives the traits directly, one table per run. Isolates driver and database from the server stack |
| L1 server | Request received to response written | The server's own latency metrics (`SuccessfulRequestLatency`, `StorageQueryLatency`, `PoolAcquireLatency`), read from `/metrics` before and after a run. Attributes time to storage vs the rest |
| L2 wire | What an SDK sees on the wire | A native load generator (Rust, `hyper` plus SigV4 signing, TLS, keep-alive) with HdrHistogram. A Python or Java SDK adds its own overhead (about a millisecond for boto3) that would hide the signal, so SDK runs are reported separately as L3 and not used for the gate |

The L0 and L2 numbers together give the budget, and the difference between L2 and L0 is the cost of the server stack,
which is the same for every backend. That is how the comparison stays fair.

Indicative budget for the 1 ms p99 target, to be replaced by measured values in Phase 0: network and TLS record
handling about 100 µs, HTTP and JSON about 100 µs, SigV4 plus cached authorization about 50 µs, storage call
about 300 µs, leaving about 450 µs for scheduling jitter and tail.

### Load generator

- **Closed loop** (fixed connections, each sends the next request after the previous returns) for throughput and
  the saturation curve, at 1, 8, 32 and 128 connections.
- **Open loop** (fixed arrival rate, latency measured from the *scheduled* send time) for latency under a given
  load. This avoids coordinated omission, which closed-loop tools hide. Rates are set at 25%, 50% and 75% of the
  measured closed-loop saturation of the backend under test, and of the slowest backend, so there is a common
  rate every backend sustains.
- Warm-up of at least 60 s discarded; at least 5 measurement runs of at least 120 s; report median of runs with the
  min and max, plus p50, p90, p99, p99.9 and max per operation, error rate and throughput.
- Client and server pinned to separate cores, CPU governor fixed, host otherwise idle (the existing PR8
  record documents what happens when that is not controlled).

### Workloads

| Id | Mix | Notes |
|---|---|---|
| W1 | 100% `GetItem` | Item sizes 100 B, 1 KB, 10 KB, 100 KB; uniform and Zipfian (0.99) keys |
| W2 | 100% `PutItem`, unconditional | Same sizes |
| W3 | 95% `GetItem`, 5% `PutItem` | YCSB-B shape |
| W4 | 50% `GetItem`, 50% `UpdateItem` (SET) | YCSB-A shape |
| W5 | `UpdateItem ADD counter` on one hot key | 1, 10 and 50+ concurrent clients; checks the final count (RFC-0003 4.4) |
| W6 | Conditional `PutItem` with `attribute_not_exists` | And a general condition, to show the 1-vs-2 round-trip difference |
| W7 | `Query`, sort-key range | Pages of 10 and 100 items; ascending and descending; `begins_with` |
| W8 | `Query` on a GSI, plus writes that change the GSI key | Checks index maintenance cost and correctness |
| W9 | `TransactWriteItems` | 2, 4 and 25 items; conflicting and non-conflicting |
| W10 | `BatchGetItem` (100) and `BatchWriteItem` (25) | |
| W11 | `Scan` pages and parallel scan | 1, 4 and 16 segments |
| W12 | Streams: write with capture, then `GetRecords` | Consumer lag and write overhead |
| W13 | TTL: sustained expiry rate | Worker throughput and stream `REMOVE` records |
| W14 | `SearchVectors` | 128 and 768 dimensions; 100K and 1M vectors; `FLAT` and `HNSW`; recall@10 against exact |
| W15 | Mixed production-like | 80/15/5 get/update/query, Zipfian, with a GSI and streams on |

Dataset sizes 1M, 10M and 100M items where memory allows (the 100M run applies to the disk-based backends and Valkey
on a host sized for it), with the table pre-loaded through the wire, then a restart so caches are cold, then a
warm-up.

### Comparison matrix and fairness

Backends under test: Valkey core, Valkey search, PostgreSQL 16, SQLite (file, WAL), SQLite in memory
(`sqlite-memory`, the true lower bound for an in-process store), MongoDB 7 (replica set), Cassandra 4.1
(single node). Optionally Amazon DynamoDB and DynamoDB Local as an external reference, clearly labelled as
different systems.

Throughput and latency are only comparable at equal durability, so each backend runs in two classes:

| Class | Meaning | Valkey | PostgreSQL | SQLite | MongoDB | Cassandra |
|---|---|---|---|---|---|---|
| D0 | Ack after memory or OS cache | `appendfsync everysec` | `synchronous_commit=off` | WAL, `synchronous=NORMAL` | `w:1, j:false` | `commitlog_sync=periodic` |
| D1 | Ack after durable commit | `appendfsync always` plus `WAIT 1` | `synchronous_commit=on` | WAL, `synchronous=FULL` | `w:majority, j:true` | `commitlog_sync=batch` |

Results are reported for D0 and D1 side by side, never mixed. Every backend runs behind the same ExtendDB binary
build profile (release, same feature set apart from the backend), the same TLS and SigV4 settings, the same
auth cache, the same table schema (one GSI, streams off unless the workload says otherwise), the same item
generator and the same connection pool bounds. Database memory limits are set to comparable values, and each
backend's own configuration is tuned in good faith to its documentation, with every non-default setting
recorded in the report.

### Additional measurements

- **Memory per item** (resident set per million items at 1 KB, with one GSI) for Valkey versus the disk-based
  backends' store size, as a cost ratio.
- **Crash recovery.** `kill -9` during W2 at D0 and D1: count acknowledged-but-lost writes per backend. This
  is the measured version of the durability table.
- **Restart time** to serve (AOF replay or RDB load, plus Search index rebuild) at 1M, 10M, 100M items.
- **Replication lag** and failover time with one replica.
- **Server-thread interference.** p99 of W1 while a 100-operation `TransactWriteItems` loop and a 1 MB scan
  loop run on the same instance.
- **Experiment E1.** Sorted-set GSIs versus search-backed GSIs on W8: write latency, query latency, memory,
  correctness against RFC-0003 section 2 (including 38-digit numeric keys).
- **Experiment E2.** Pool shape: one multiplexed connection versus N, versus dedicated connections per in-flight
  request; and `redis` crate versus `valkey-glide`.
- **Experiment E3.** Item blob format: DynamoDB JSON versus a compact binary form, effect on L0 latency and bytes.

### Reporting and gates

Results are committed as design records in the style of `docs/design/15-backup-restore-bench-pr*.md`: host
specification (cores, memory, kernel, database versions), every non-default setting, raw histograms kept beside
the PR, and a statement of what was not held constant. The harness lives under `devtools/` (proposed names
`devtools/run-bench` and a `bench` crate that is not part of the default workspace build) and is runnable against any
backend, so the comparison can be repeated by anyone and by CI on a schedule, with regressions tracked against the
last record.

Gates (proposed, ratified after Phase 0 from measured data):

| Gate | Condition |
|---|---|
| G1 Latency target | L1 p99 under 1 ms for W1, W2 and W4 at 1 KB and at most 50% of saturation, on the reference host |
| G2 Justification | Valkey core beats the best of PostgreSQL, SQLite-file and MongoDB on p50 and p99 for W1, W2 and W4 by a margin set in Phase 0 (a 3x p99 improvement is the starting proposal), at equal durability class. If it does not, the backend is not carried |
| G3 Correctness under load | The RFC-0003 stress scenarios pass during the runs (hot-key counter exact, no 500s, no cross-tenant stream leakage) |
| G4 Recall | W14 `FLAT` recall@10 is 1.0; `HNSW` recall is reported with its parameters |
| G5 No regression of others | Adding the crate does not change other backends' build or test results (RFC-0002 architectural discipline) |

## Testing

- **Unit tests** in the crate: key and sort-key encodings (round trip, order against a reference comparator,
  38-digit and exponent boundaries, binary length-ordering), index-entry construction, sequence-number
  formatting, shard-id derivation, error mapping. Property tests for the encoding order and for the pushdown
  parity with `evaluate_condition`.
- **Function tests**, directly against Valkey: each Function under version mismatch, absent items, repeated
  index keys and token replays.
- **Conformance.** The existing dual-target Rust suite (`tests/rust/`) and the Python pytest suite run unchanged
  against a Valkey-backed server, through a new `devtools/run-valkey-tests` modelled on `devtools/run-mongodb-tests`
  and `devtools/run-tests --backend valkey`. The RFC-0003 stress scenarios are included.
- **CI.** A `.github/workflows/integration-valkey.yml` workflow with a matrix: Valkey 8.x and 9.x, core mode
  and Search mode (a Valkey image bundling the module, or a built module). Jobs are `valkey-integration` and
  `valkey-search-integration`, the latter running the vector tests, and both required before the backend can
  leave Experimental.
- **Crash and recovery tests.** Kill and restart Valkey with AOF and with RDB, and confirm the catalog and
  tables survive, and that startup refuses unsafe settings (`maxmemory-policy allkeys-lru`, no persistence).
- Helper scripts follow the existing `devtools/check-*-catalog-lock` and `check-*-older-binary` conventions.

## Implementation plan

| Phase | Content | Exit criteria |
|---|---|---|
| 0. Spike and gate | Minimal `get_item`, `put_item` (with one GSI, stream append and TTL entry) in a throwaway crate; the L0 harness; floor and comparison against SQLite-memory, SQLite-file and PostgreSQL at W1, W2, W4 | G1 and G2 evidence on the reference host. A decision to proceed, to proceed with revised targets, or to stop |
| 1. Core backend | Crate skeleton and registration; catalog and IAM stores; `TableEngine`, `DataEngine`, `MetadataEngine`; Functions and loader; tests and CI in core mode; experimental tier | Full conformance in core mode except snapshots; `devtools/run-valkey-tests` and the CI job green |
| 2. Streams, TTL, batch | Streams and shards, TTL worker, backfill workers, transactions, idempotency; E2 and E3 | All RFC-0003 stress scenarios pass |
| 3. Search mode | Probe, vector indexes, `SearchVectors`, vector tests; E1; optional snapshot undo log | G4; E1 decision recorded |
| 4. Benchmark record and hardening | Full benchmark suite, crash tests, restart tests; documentation (setup guide, `differences-from-dynamodb.md`, troubleshooting); tier proposal | Maintainers agree that CI has been reliably green and the limits fit the proposed tier |

### Tier

The backend enters as **Experimental** under the README tier definitions. Moving to Beta requires a reliably green
required CI job, the benchmark record, and the known limits of this RFC being acceptable for production within
their stated bounds. Stable would additionally need point-in-time snapshots and a tested upgrade path across
catalog versions.

### Implementation summary

| Area | Change |
|---|---|
| `crates/storage-valkey/` | New crate: backend descriptor, catalog and IAM stores, table and data engines, Functions library, workers |
| `crates/bin/Cargo.toml`, `crates/bin/src/main.rs` | `valkey` feature, `set_backend` call, mutual-exclusion check |
| `Cargo.toml` (workspace) | New crate and client dependency |
| `devtools/` | `run-valkey-tests`, `check-valkey-*` helpers, `run-bench` |
| `.github/workflows/` | `integration-valkey.yml`, scheduled benchmark workflow |
| `docs/` | `local-valkey-setup.md`, `differences-from-dynamodb.md`, `design/NN-storage-valkey.md`, benchmark records, ADRs for decisions below |
| `extenddb.sample.toml` | `[storage.valkey]` section |

No changes to `crates/engine/`, `crates/server/`, `crates/storage/` (trait definitions), `crates/auth/`
or `crates/core/`. If Phase 0 finds that a trait needs to change (for example a hook for striped locking), that
change goes through its own RFC.

### Candidate ADRs

Functions-based atomicity and the generic-Function rule (no DynamoDB semantics in Lua); memcomparable sort-key
encoding; registry buckets and parallel-scan segment function; stream storage on Valkey Streams; durability
profiles and startup refusals; Search mode scope.

## Drawbacks

- **Durability is the operator's job.** The default profile can lose about a second of acknowledged writes, and the
  catalog shares that exposure. A new user can build an unsafe setup, so the startup checks are essential and still do
  not make Valkey a database of record.
- **Memory-bound and costly per GB.** The whole dataset, including every index entry, is in RAM. At large sizes
  the cost per item is higher than every other backend.
- **Single-threaded execution.** One slow Function stalls all clients. Bounded pages and transactions limit
  this, but a noisy workload degrades latency for everyone, which hurts the very goal the backend exists for.
- **No horizontal scale in v1.** One primary per catalog. Cluster mode is a separate and difficult problem.
- **Lua Functions are a second code base** (small, generic, but separately tested and versioned), plus a loading
  and upgrade protocol.
- **Optimistic reads add a round trip** for conditional writes and `UpdateItem`. Without a server-side
  expression evaluator, which this RFC rejects as a duplicate of `extenddb_core`, two round trips is the floor.
- **Phase 1 backups are weak** (non-PIT) until the undo-log snapshot lands.
- **Another backend to maintain**, with its own CI matrix (versions times Search on/off), in a project that already carries four.

## Alternatives

- **Valkey as a cache in front of an existing backend (write-through or write-behind).** Reads would be fast, but
  writes pay both systems, consistency and invalidation become the hard problem, write-behind loses
  acknowledged writes on failure, and it needs two databases to operate. A cache layer in the engine is a
  separate proposal that could use any backend, and this RFC does not preclude it.
- **`WATCH`/`MULTI`/`EXEC` instead of Functions.** No Lua, but conditional logic still needs a read round trip
  and a retry, and multi-key conditional atomicity is limited. It costs more round trips on the hot path.
- **Valkey JSON module for items.** Gives path updates, but `UpdateItem` semantics (including list and set
  behavior) are implemented by `extenddb_core`, and a second implementation in the module would need to
  match it. Hashes with a blob are simpler and independent of any module.
- **Store items as plain `STRING`.** Slightly less memory per item than a hash for small items, but there is no
  place for `v`, `ix` and `ttl` without a binary header that Lua then has to parse. Phase 0 compares the two.
- **Redis OSS.** Its license has changed across releases (source-available terms, then AGPL for later versions),
  which needs a legal review against the project's Apache-2.0 distribution before it could be named a supported
  server. Valkey is BSD-3-Clause and needs no such review. Redis is not a target of this RFC.
- **Other protocol-compatible servers (Dragonfly, KeyDB).** Different execution models and Functions support.
  Not tested; not promised.
- **A native in-process memory backend** (not `sqlite-memory`) with no network hop. Faster than Valkey, but
  single-process, not shared, and nothing new to learn from. It is the L0 lower bound that
  `sqlite-memory` already represents in the benchmark.
- **Streams on sorted sets** with a time-index zset for retention. Works, but Valkey Streams already provide
  ordered ids and `MINID` trimming.
- **Search-backed secondary indexes as the default.** Rejected pending E1, for the precision and latency reasons given above.

## Unresolved questions

1. **Reference hardware and version matrix.** Which host defines "sub-millisecond"? Which Valkey and
   valkey-search versions are in the CI matrix?
2. **Client library.** `redis` crate or `valkey-glide`, decided by E2 and license review.
3. **Item blob format.** DynamoDB JSON or a compact binary encoding (E3), and the migration implications if
   it changes after release.
4. **Search visibility.** Is an `FT.SEARCH` after a write guaranteed to see it, on a single node? If not, how does
   `SearchVectors` meet read-your-writes expectations?
5. **Server-thread interference.** What size caps on transactions, scan pages and batches keep other clients'
   p99 under target, and should they be configurable?
6. **`HNSW` exposure.** Can `HNSW` be selected through existing request shapes, or only by a backend setting? A
   setting that changes results is an extension under RFC-0002 and must not alter standard behavior.
7. **Snapshot design.** Is the copy-on-write undo log worth its write-path check, or is Unsupported plus RDB
   backup acceptable for Beta?
8. **Durability default.** Is `latency` the right default, given the catalog shares it? An alternative is
   `durable` for the catalog keys only (a `WAIT` on catalog writes) and `latency` for data.
9. **Managed Valkey.** ElastiCache and MemoryDB restrict some commands and module sets. Does the Functions
   library load and run unchanged, and which profile does each support?
10. **Gate values.** Is a 3x p99 improvement over the best existing backend the right G2 margin, and what
    happens if the result is only sub-millisecond at D0?

The specific feedback requested from reviewers: the Functions-with-optimistic-reads design (one versus two round trips),
the durability profiles and startup refusals, the scope of Search mode (vector-only versus search-backed
indexes), and whether the benchmark gates are the right bar for carrying the backend.

## Prior art

- **Valkey Functions and Lua scripting.** The documented way to run atomic multi-key logic on the server,
  and the reason this design needs no transaction protocol.
- **Valkey Search** (valkey-io/valkey-search). Vector, tag, numeric and, from 1.2, full-text indexing over
  hashes and JSON, with `FT.SEARCH` and aggregations. Similar to RediSearch in query surface.
- **Amazon MemoryDB.** Valkey-compatible with a durable multi-AZ log: the managed form of the `durable`
  profile, with higher write latency.
- **Amazon DynamoDB Accelerator (DAX) and ElastiCache in front of DynamoDB.** Evidence that DynamoDB users
  want in-memory latency. DAX is a cache, which is the alternative this RFC rejects for ExtendDB.
- **Cassandra backend design (ADR-0010, 0012, 0017, 0020).** The same DynamoDB semantics built without
  transactions: a useful reference for what is hard (TTL with stream records, transactions, GSIs), and for
  what Functions make easier here.
- **MongoDB backend RFC.** Source of the netstring identity, the additive pagination bound, the pushdown
  analyzer discipline, the startup refusal of unsafe settings and the per-shard sequence design that this
  RFC adapts.
- **YCSB** (workload shapes) and **wrk2** (open-loop, coordinated-omission-corrected latency measurement) for
  the benchmark method.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
