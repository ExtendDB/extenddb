# RFC-0000: Cloud Bigtable Storage Backend for ExtendDB

- Status: Draft
- Author: @annguy3n
- Created: 2026-06-29
- Tracking issue: #185

## Summary

This RFC defines the design and implementation of a production-grade Google Cloud Bigtable storage backend (`crates/storage-bigtable`) for ExtendDB `v1.0.0`. It addresses transaction isolation and serializable read-through-log snapshots, deterministic Optimistic Concurrency Control (OCC) cell timestamps compatible with `MaxNumVersions(1)` garbage collection and Cloud Bigtable's millisecond granularity, incarnation-fenced two-phase commit (2PC) apply, unified write-ahead fenced DynamoDB Streams emission with contiguous per-shard sequence slots and disable/re-enable shard lineage (`stream_lineage`), FoundationDB tuple escape-encoded Global Secondary Index (GSI) keys and version-guarded shadow projections, sharded Time-to-Live (TTL) indexing, `SecretKeys` KEK/DEK envelope encryption and `encryption-key` lifecycle commands, CAS-guarded IAM/metrics/DDL catalog conformance, gRPC transient-error classification, application-profile single-cluster routing, and Google Cloud Platform (GCP) authentication.

## Motivation

Mapping DynamoDB's API contract onto Cloud Bigtable's wide-column, single-row-atomic data model introduces several distributed-systems challenges that a production backend must resolve:

1. **Transaction Isolation & TOCTOU Races:** Naive read-modify-write conditional operations evaluate `ConditionExpression`s without atomic concurrency control, allowing concurrent writers to overwrite uncommitted state or bypass 2PC locks.
2. **Cell Timestamps vs. `MaxNumVersions(1)` Compaction:** Relying on server-assigned cell timestamps (`SetCell.timestamp_micros = -1`) allows tablet-server wall-clock differences or narrow concurrency windows to interleave cells from distinct writes across qualifiers (`d:*` vs. `m:v`), producing torn rows once `MaxNumVersions(1)` compaction runs.
3. **2PC Race Conditions (Read-then-Lock, Unfenced Commit/Abort & Unfenced Apply):** Reading participant rows before placing intent locks exposes `TransactWriteItems` to TOCTOU races; unconditional coordinator `COMMITTED` writes or unfenced sweeper rollbacks allow partial 2PC commits; and applying Phase 2 mutations unconditionally allows stalled coordinators whose intents were swept to overwrite newer committed data.
4. **Unfenced Stream Emission & Out-of-Order Shard Visibility:** Emitting DynamoDB Stream records as an unfenced trailing write loses stream records if the server crashes in the gap ([RFC-0003](0003-backend-acceptance-criteria.md) §3.2, §5.1), while pre-allocating global sequence numbers in Phase 1 allows a later sequence number (`seq_2 > seq_1`) to persist before `seq_1` on the same shard and cause `AFTER_SEQUENCE_NUMBER` consumers to permanently skip `seq_1` (§5.2).
5. **Inefficient & ABA-Prone TTL Eviction:** Full-table scans degrade read throughput at scale, while unlocked trailing TTL index maintenance or unbounded index row deletes suffer `1000 -> 2000 -> 1000` ABA races and risk wiping concurrent same-second TTL inserts.
6. **GSI Sort-Order, Online Lifecycle & Shadow Consistency:** Non-terminal sort keys in composite GSI row keys suffer sort inversion unless escape-terminated, online `UpdateTable` GSI builds must backfill existing rows before serving queries, and delayed writers or reconcilers (`v1 < v2`) must never clobber newer `v2` shadow state via unbounded `DeleteFromFamily` or `DeleteFromRow`.
7. **Multi-Cluster Routing & Auth Constraints:** Cloud Bigtable rejects `CheckAndMutateRow` under multi-cluster routing with eventual consistency unless routed via a single-cluster application profile, and enterprise deployments require explicit service-account credential configuration.

## Detailed Design

### 1. Table Mapping & Key Encoding

#### A. Table Structure & Naming
* **Catalog & Stream Ledger (`__extenddb_catalog__`):** Stores system settings (`setting:catalog_id`, `setting:encryption_key`, `setting:encryption_key_ring`), accounts, admins, IAM users/groups/roles/policies/boundaries/tags, `AKIA*` access keys and `ASIA*` STS sessions (`iam_session:<token>` + secondary index `iam_session_ak:<access_key_id>`), login rate-limit windows (`failed_login:<principal>\x00<nanos:020>:<uuid>`), minute-bucketed operational metrics (`metric:<bucket_secs:012>:<dims_sha256>`), idempotency tokens (`idem:<account_id>:<token>`), catalog migration ledger rows (`migration_ledger:catalog:<id:04>`), distributed worker leases (`lock:<name>`), stream shard lineage/cursor rows (`stream_shard:<shard_id>` carrying `next_seq_num` and `last_sequence_number`), per-event stream deduplication markers (`stream_dedup:<shard_id>:<dedup_key>`), and stream records (`stream_record:<shard_id>:<seq_26>`) in column family `c` (qualifier `j`).
* **Coordinator Transaction Log (`__extenddb_txn_log__`):** Stores coordinator state (`t:state`), incarnation start timestamp (`t:started_at`), canonical sorted participant list (`t:participants`), pre-computed mutations (`t:mutations`), pre-generated stream records (`t:stream_records`), logged TTL index transitions (`t:ttl_mutations`), and `ClientRequestToken` metadata (`t:token_meta`) in column family `t`.
* **Sharded TTL Index (`__extenddb_ttl_index__`):** Stores OCC-timestamped, millisecond-aligned expiration index entries across 16 hash shards in column family `d`.
* **Data Tables (`t<table_id_16hex>`):** Each DynamoDB table maps to a dedicated Bigtable table named `t` followed by the first 16 lowercase hex characters of the table's unique `table_id` UUID (17 characters total). Deriving physical table names from `table_id` rather than `table_name` guarantees clean isolation across accounts and across `DeleteTable` + `CreateTable` name reuse ([RFC-0003](0003-backend-acceptance-criteria.md) §8.2). Data attributes live in column family `d` (one qualifier per top-level attribute); OCC row versions (`m:v`) and 2PC intent locks (`m:intent:<txn_id>`) live in column family `m`; column family `t` is reserved.
* **GSI Shadow Tables (`t<table_id_16hex>_g<idx_hash_8hex>`):** Each GSI maps to a shadow Bigtable table where `idx_hash_8hex` is the 8-character hex FNV-1a hash of `index_name` (27 characters total, well within Cloud Bigtable's 50-character table-ID limit), storing projected item attributes in column family `d` and base-row OCC version guards (`m:v`) in column family `m`.

#### B. Exact Decimal & Order-Preserving Key Encoding
Cloud Bigtable sorts row keys by unsigned raw byte lexicographical order and enforces a **4 KiB (4096-byte)** row-key limit, validated upfront in `row_key::encode_key` and `gsi::shadow_row_key_for_item`.

Base-table row keys follow:
```text
Composite Key: [pk_tag:1] [pk_len:u32BE:4] [pk_bytes] [sk_tag:1] [sk_bytes]
PK-Only Key:   [pk_tag:1] [pk_len:u32BE:4] [pk_bytes]
```
Where `pk_tag` and `sk_tag` identify the DynamoDB scalar type (`0x53` for `S`, `0x4E` for `N`, `0x42` for `B`).

* **Partition (`HASH`) Keys:** Length-prefixed (`[pk_len:u32BE:4]`) before `[pk_bytes]` so variable-length partition keys cannot collide across boundaries and all items sharing a partition key sort contiguously. Parallel scan segment assignment hashes the `[pk_tag:1][pk_len:4][pk_bytes]` prefix with CRC32 (`crc32(pk_prefix) % total_segments`), shared identically between `QueryScan::scan` and `DataEngine::scan_key_in_segment`.
* **Sort (`RANGE`) Keys:** Stored as raw order-preserving bytes **without** a length prefix on terminal segments so string and binary sort keys compare in unsigned byte-lexicographic order regardless of length.
* **Exact 38-Digit Decimal `N` Encoding (`encoding::number`):** DynamoDB numbers support up to 38 significant digits over scientific exponents `[-130, +125]`. Numbers are normalized (stripping leading/trailing zeros and collapsing `-0` / `0.0` to canonical zero) and encoded without floating-point conversion:
  * **Negative (`value < 0`):** Sign byte `0x00`, inverted biased exponent `0xFF - (sci_exp + 130)`, and 38 digit bytes inverted as `9 - digit` (padded with `0x09` so shorter magnitudes compare greater / closer to zero).
  * **Zero (`value == 0`):** Single byte `[0x80]`.
  * **Positive (`value > 0`):** Sign byte `0xFF`, biased exponent `(sci_exp + 130) as u8`, and 38 digit bytes `0..=9` (padded with `0x00`).
  * Inputs exceeding 38 significant digits or exponent bounds `[-130, 125]` are rejected at write/query time with `ValidationException`.
* **GSI Shadow Row Keys (`gsi::shadow_row_key_for_item`):**
  ```text
  GSI Composite Key: [gsi_pk_tag:1] [gsi_pk_len:u32BE:4] [gsi_pk_bytes]
                     [gsi_sk_tag:1] [escape_encoded(gsi_sk_bytes)] [0x00 0x00]
                     [base_pk_tag:1] [base_pk_len:u32BE:4] [base_pk_bytes]
                     [base_sk_tag:1] [base_sk_bytes]
  ```
  * **Non-Terminal Sort Key Escape Encoding (`gsi_sk`):** Every `0x00` byte in `gsi_sk_bytes` is escaped as `0x00 0xFF` and terminated by `0x00 0x00` (FoundationDB tuple encoding). Without termination, the following `base_pk_tag` byte (e.g., `0x53`) would participate in sort-key comparison and invert ordering (e.g., `"A"` sorting after `"AB"` because `0x53 > 0x42`).
  * **Terminal Sort Key (`base_sk`):** Remains raw order-preserving bytes to the end of the row key.

### 2. Transaction Rules, Deterministic OCC Timestamps & Isolation Guarantees

#### A. Transaction Validation & Idempotency Rules
* **`ClientRequestToken` Idempotency (`RFC-0003` §3.3):** `TransactWriteItems` scopes idempotency keys by `(account_id, token)` (`idem:<account_id>:<token>`) over a strict **10-minute (`600s`) `applied_at` window** (`check_idempotency_record` evicts expired entries via `Catalog::mutate_json_cas`) and derives a deterministic coordinator ID `SHA-256(account_id || ":" || token)`:
  * `TxnCoordinator::open_with_token` atomically persists `TxnTokenMeta` (`account_id`, `token`, `fingerprint`) in `t:token_meta` alongside `t:state = PENDING` and `t:started_at`, returning `(created, started_at_micros)`.
  * When `created == true`, the caller immediately re-verifies `idem:<account_id>:<token>` (rolling back and dropping its nascent `PENDING` incarnation if a prior winner persisted `idem:` and dropped `txn:<id>` between pre-check and `open_with_token`).
  * When `created == false`, the caller never strips intents belonging to an active concurrent owner; instead, the follower loop validates `t:token_meta.fingerprint` across `PENDING`, `COMMITTED`, and `CLEANED` states (returning `IdempotentMismatch` immediately on payload mismatch without poisoning `idem:`, and cooperatively rolling forward `COMMITTED` transactions before returning `IdempotentReplay`).
  * Both inline Phase 2 (`roll_forward`) and `sweeper::sweep_with_cutoff_override` (on both `COMMITTED` and `CLEANED` coordinator rows) persist `idem:<account_id>:<token>` via `persist_idempotency_if_unexpired` **before** marking `CLEANED` and calling `drop_matching_start`, while Phase 1 `ConditionFailed` rollbacks drop `txn:<id>` without writing `idem:` so retries with the same token can execute cleanly.
* **Duplicate Participant Rejection & Canonical Lock Ordering (`RFC-0003` §3.4):** Any `TransactWriteItems` request targeting the same `(data_table, row_key)` more than once is rejected upfront with `ValidationException`. Participant locks are acquired in canonical `(data_table, row_key)` lexicographical order (`ParticipantRow: Ord`) to prevent `[k1, k2]` vs. `[k2, k1]` deadlocks while preserving caller-order indices on `StorageError::TransactionCanceled(reasons)` (never leaking `StorageError::TransactionConflict` from 2PC, and honoring per-item `ReturnValuesOnConditionCheckFailure::None` vs. `AllOld`).

#### B. Lock-then-Read Coordinator 2PC Flow
`TransactWriteItems` acquires per-row intents in sorted order *before* reading participant rows and evaluating condition expressions, transitions `(t:state == PENDING && t:started_at == started_at_micros) -> COMMITTED` via incarnation-fenced CAS `CheckAndMutateRow` in `__extenddb_txn_log__`, applies mutations idempotently via `roll_forward`, and acknowledges HTTP 200 OK only after Phase 2 mutations, GSI shadows, TTL transitions, stream records, and `idem:` entries are durable:

```mermaid
sequenceDiagram
    participant C as Coordinator (ExtendDB)
    participant L as __extenddb_txn_log__
    participant B as Bigtable Data / GSI / Catalog

    C->>L: Phase 1: open_with_token (CheckAndMutateRow: t:state = PENDING, t:started_at, t:token_meta)
    loop Each participant row in sorted (data_table, row_key) order
        C->>B: place_intent_once (CheckAndMutateRow: fail if any m:intent:* exists; cooperatively resolve stale/COMMITTED)
    end
    alt Intent Contention Exhausts Retries
        C->>L: try_abort_pending (CAS PENDING + started_at -> ABORTED)
        C->>B: Clear placed intents & drop_matching_start
        Note over C: Return TransactionCanceled (TransactionConflict)
    end
    C->>B: Read locked participant rows & capture OCC versions (m:v)
    Note over C: Evaluate ConditionExpressions & compute next_occ_version
    alt Any Condition Fails
        C->>L: try_abort_pending (CAS PENDING + started_at -> ABORTED)
        C->>B: Clear intents & drop_matching_start
        Note over C: Return TransactionCanceled (ConditionalCheckFailed honoring ReturnValuesOnCCF)
    end
    C->>L: Phase 2 Commit: try_commit_full (CAS PENDING + started_at -> COMMITTED, mutations, stream_records, ttl_mutations)
    loop Each mutated participant (roll_forward)
        C->>B: apply_fenced_mutation (if m:intent:<txn_id> && OCC state == expected: write d:* & m:v at occ_ts, retain intent)
        C->>B: Version-guarded GSI shadow updates (put/delete_shadow_versioned) & OCC-bounded TTL index updates
    end
    C->>B: Append contiguous per-shard stream slot (write_record_ordered + stream_dedup)
    C->>B: Persist idem:<account_id>:<token> (if t:token_meta present)
    loop Each locked participant
        C->>B: Clear intent (DeleteFromColumn m:intent:<txn_id>)
    end
    C->>L: Mark CLEANED (@ clean_ts > commit_ts) & drop_matching_start
    Note over C: Return HTTP 200 OK (Read-Your-Writes Boundary)
```

#### C. Read Isolation, Incarnation-Fenced CAS Commit/Abort & `TransactGetItems` Double-Read Snapshot
* **Read-Your-Writes Acknowledgment Boundary:** Success is returned only after Phase 2 mutations are applied to column family `d` and GSI shadow tables, guaranteeing `ConsistentRead = true` (`GetItem`, `Query`, `Scan`) and `TransactGetItems` immediately observe the committed state.
* **Read Committed (No Dirty Reads):** Uncommitted transactions write only `m:intent:<txn_id>` in family `m`. Standard reads filter strictly on family `d` (`Chain([FamilyNameRegexFilter("^d$"), CellsPerColumnLimitFilter(1)])`) and never observe uncommitted writes.
* **Incarnation-Fenced Mutual Exclusion Between `try_commit_full` and `try_abort_pending` (`RFC-0003` §3.1):** Both `TxnCoordinator::try_commit_full` (`PENDING -> COMMITTED`) and `TxnCoordinator::try_abort_pending` (`PENDING -> ABORTED`) execute `CheckAndMutateRow` predicated on `build_pending_incarnation_predicate(started_at_micros)` (applying `CellsPerColumnLimitFilter(1)` **before** `ValueRegexFilter` on `t:state == "PENDING"` and `t:started_at == started_at_micros`), and `sweeper` / inline cooperative recovery always CAS-transitions `PENDING -> ABORTED` **before** clearing any participant intent (if CAS fails because the coordinator concurrently reached `COMMITTED`, recovery immediately invokes `roll_forward`). Row drops use `drop_matching_start(txn_id, started_at_micros)` and stale intent cleanup uses timestamp-bounded `clear_intent_up_to` so a stale cleaner can never delete a newer reused token incarnation.
* **Zero Wall-Clock Intent Bypass & Cooperative Resolution (`RFC-0003` §3.1, §3.5, §4.2):** `build_active_intent_filter` and `build_intent_presence_filter` carry **no** wall-clock `TimestampRangeFilter`, because an intent older than `intent_timeout_secs` may belong to a `COMMITTED` coordinator awaiting roll-forward. Whenever a writer or worker encounters an intent on a row, `resolve_blocking_intents` / `read_row_resolving_committed_intents` inspects `__extenddb_txn_log__`: `COMMITTED` transactions are rolled forward immediately, timed-out `PENDING` transactions are CAS-aborted and cleaned up, and `ABORTED`/`CLEANED`/dropped intents are cleared safely.
* **ABA-Safe Fenced Phase 2 Apply & Side-Effect Gating:** `TxnCoordinator::apply_fenced_mutation` predicates its `CheckAndMutateRow` via an outer `Filter::Condition` asserting **both** `m:intent:<txn_id>` presence (`exact_re2_bytes`) **and** the 3-case `(expected_version, row_existed)` OCC state guard (`(Some(v), _) -> m:v == v`; `(None, false) -> neither m:v nor d:* exists`; `(None, true) -> d:* exists && m:v absent`). Unified `roll_forward` tracks `materialized_participants` (`applied || (cur_ver == Some(new_version) && payload_matches)`), gating GSI shadow writes, `TxnTtlMutation` maintenance, **and** `TxnStreamRecord` emission so superseded coordinators emit zero phantom side effects.
* **Serializable `TransactGetItems` via Optimistic Double-Read Snapshot Validation (`RFC-0003` §3.5, §9.1, §9.2):** `TransactGetItems` validates keys upfront via `validate_key_only` (returning `TransactionCanceled` with `ValidationError` reasons on malformed keys), deduplicates physical row keys while preserving positional output mapping, and executes a non-mutating double-read loop:
  * **Pass 1:** Batch-reads `RowSnapshot` (`d:*`, `m:v`, and all `(m:intent:<txn_id>, ts_micros)` regardless of age) across all unique keys and queries `TxnCoordinator::get_state` for every referenced `txn_id`.
  * **Pass 2 Validation:** Re-reads `RowSnapshot`s across all unique keys and re-queries referenced coordinator states, accepting the snapshot if and only if `(m:v, intents, d:*)` and coordinator `(state, started_at)` match identically across Pass 1 and Pass 2—proving a single linearization point across concurrent `PENDING -> COMMITTED`, `COMMITTED -> CLEANED -> drop`, and OCC updates.
  * **Monotonic Coordinator `SetCell` Transitions:** State transitions (`PENDING` at `t0`, `ABORTED`/`COMMITTED` at `t1 > t0`, `CLEANED` at `t2 > t1`) write monotonic `SetCell(t:state)` values without `DeleteFromColumn(t:state, ...)`, and `get_state` treats any row lacking `t:state` as `Ok(None)`, eliminating transient `StorageError::Internal` HTTP 500s on `cbtemulator`.

#### D. Deterministic OCC-Derived Cell Timestamps & Single-Row Concurrency Control
Using server-assigned cell timestamps (`timestamp_micros: -1`) is unsafe under `MaxNumVersions(1)`: if two writers pass filters within a narrow window, or tablet servers experience slight clock skew across tablet moves or recovery roll-forward, `MaxNumVersions(1)` compaction retains whichever cell had the higher wall-clock arrival timestamp per qualifier rather than the higher logical OCC version, risking torn rows (`d:*` from one write paired with `m:v` or attributes from another).

To eliminate torn rows while satisfying Cloud Bigtable's **`TimestampGranularity::MILLIS`** rule (which rejects explicit `SetCell.timestamp_micros` and `TimestampRange` bounds not divisible by `1_000` with `INVALID_ARGUMENT`), all row writes and range filters derive deterministic millisecond-aligned timestamps directly from the monotonic OCC version:

* **Hybrid Monotonic OCC Version (`item_ops::next_occ_version` & `next_millis_exclusive`):**
  * `now_ms = unix_timestamp_millis().max(1)`
  * `next_occ_version(None) = now_ms`
  * `next_occ_version(Some(prev)) = prev.max(now_ms) + 1`
  * `version_to_timestamp_micros(v) = (v * 1_000) as i64`
  * `next_millis_exclusive(ts_micros) = ((ts_micros / 1_000) + 1) * 1_000`
* **Uniform Row-Wide & Version-Guarded GSI Shadow Stamping:** Every `SetCell` in a write—every attribute cell in `d:*`, the `m:v` version cell (followed by bounded `DeleteFromColumn(m:v, [0, occ_ts))`), and every projected cell + `m:v` guard in GSI shadow tables—is stamped with `version_to_timestamp_micros(next_version)`. GSI shadow updates (`ItemOps::put_shadow_versioned`, `ItemOps::delete_shadow_versioned`, and `ItemOps::delete_shadow_orphan_if_unchanged`) never issue unbounded `DeleteFromFamily("d")` or `DeleteFromRow`: instead, they inspect `(existing_quals, max_d_ts, cur_ver)` and execute `CheckAndMutateRow` predicated on exact `m:v == cur_ver` (or missing `m:v`), placing `SetCell` before bounded per-qualifier `DeleteFromColumn` (`[0, occ_ts)` / `[0, next_millis_exclusive(occ_ts))`) and rejecting (`Ok(false)`) any delayed writer or reconciler (`new_version < cur_ver`) so stale mutations never overwrite or delete newer `v2` shadow state.
* **Monotonic Tombstone Versioning on Delete:** Deleting an existing base or GSI shadow row advances `SetCell("m", "v", next_version)` at `version_to_timestamp_micros(next_version)` (with bounded `DeleteFromColumn("m", "v", [0, occ_ts))`). Retaining the monotonic `m:v` marker across deletes prevents ABA version reuse and timestamp regression on rapid `Put -> Delete -> Put` cycles, while `Query`, `Scan`, and `BatchGetItem` filter on `FamilyNameRegexFilter("^d$")` so version-only tombstones never appear as items or consume `rows_limit`.
* **De Morgan OCC CAS & Retry Loop (`select_occ_predicate`):** Because Bigtable row filters have no boolean `NOT` operator, `ItemOps::put_cas` and `ItemOps::delete_cas` express OCC guards in De Morgan conflict form with mutations in `false_mutations`:
  * **Versioned Existing Row (`read_version = Some(v)`):** Predicate matches if `(any m:intent:* exists) OR (m:v != v)`.
  * **Unversioned Pre-Existing Row (`read_version = None, row_existed = true`):** Predicate matches if `(any m:intent:* exists) OR (m:v exists) OR (d:* missing)`.
  * **New Row (`read_version = None, row_existed = false`):** Predicate matches if `(any m:intent:* exists) OR (m:v exists) OR (d:* exists)`.
  * **Conflict Classification & Contention Retry:** When `predicate_matched == true`, `BigtableEngine` classifies `CasOutcome::IntentPresent` vs. `CasOutcome::OccMismatch`, cooperatively resolves `COMMITTED` or expired `PENDING` intents via `resolve_blocking_intents` (waiting/retrying across transient `sgl-*` single-item fenced writes), returns `StorageError::TransactionConflict` if a live multi-item 2PC `txn-*` holds the row ([RFC-0003](0003-backend-acceptance-criteria.md) §4.3), or retries OCC mismatches with jittered backoff.

#### E. Cloud Bigtable App Profile & Single-Cluster Routing Requirements
Cloud Bigtable requires single-cluster routing (or multi-cluster routing with single-row transactions enabled) to execute `CheckAndMutateRow`. Multi-cluster profiles with eventual consistency reject `CheckAndMutateRow` with `FailedPrecondition`. `BigtableStorageConfig` exposes `app_profile_id` and forwards it on every `ReadRows`, `MutateRow`, `MutateRows`, and `CheckAndMutateRow` RPC so multi-cluster deployments can bind ExtendDB to a single-cluster profile.

#### F. Garbage Collection (GC) Rules & Recovery Sweeper
* **Column Family GC:** `d` and `m` on data/GSI tables, `c` on `__extenddb_catalog__`, and `d` on `__extenddb_ttl_index__` are provisioned with `MaxNumVersions(1)`.
* **2PC Recovery Sweeper (`sweeper.rs`):** Spawned via `BigtableRuntimeHooks::spawn_workers` under a distributed catalog lease (`lock:sweeper`) with graceful `CancellationToken` shutdown. Timed-out `PENDING` coordinators are first CAS-transitioned to `ABORTED` (`try_abort_pending`) before clearing intents and calling `drop_matching_start` (rolling forward if concurrently `COMMITTED`); `COMMITTED` coordinators are rolled forward idempotently (`roll_forward` applying base rows, versioned GSI shadows, `t:ttl_mutations`, contiguous deduplicated stream records, and `idem:<account_id>:<token>` before marking `CLEANED` and dropping); and stranded `CLEANED` rows persist `idem:` if `t:token_meta` is present before dropping.

### 3. Unified Fenced DynamoDB Streams Architecture & Stream Lineage

#### A. Write-Ahead Fenced Stream Emission & Labelled Shard Lineage (`stream_lineage`)
To satisfy [RFC-0003](0003-backend-acceptance-criteria.md) §3.2 and §5.1 ("a crash between a committed data write and its stream record insert must not be possible"), stream emission is **never** performed as an unfenced trailing write. Whenever a caller supplies `stream: Some(&StreamCapture)` on a table whose `StreamSpecification.stream_enabled` is active (`active_stream_target_for_capture`), single-item writes (`PutItem`, `UpdateItem`, `DeleteItem`—including TTL sweeper deletions) execute through `BigtableEngine::execute_fenced_single_item_write`, sharing the exact same `__extenddb_txn_log__` commit fence as `TransactWriteItems` (while `DataEngine` writes with `stream: None`—such as `ImportTable` bulk imports—preserve the upstream stream-suppression contract without emitting stream events):

1. **Labelled 4-Shard Stream Lineage (`extenddb_storage::stream_lineage`):** Each stream enablement (`CreateTable` or `UpdateTable`) allocates a monotonic stream label via `stream_lineage::next_stream_label` and provisions `SHARDS_PER_STREAM` (`4`) open `ShardMeta::new_open` rows (`00..03`) in `__extenddb_catalog__` at `stream_shard:<shard_id>`, where `shard_id = shardId-{table_id}-{label_millis:013}{view_digit:1}{index:02}` (61 characters, within the 65-character AWS SDK `ShardId` limit). `UpdateTable` commits `table_meta` via `Catalog::mutate_json_cas` **before** closing disabled shards (`close_shards_by_id` via CAS) or provisioning newly enabled shards (`put_shard_if_absent` followed by `stream_lineage::enable_still_current` re-verification so concurrent disable calls immediately close superseded shards). Writes and `StreamEngine::assign_shard` route partition keys deterministically across the 4 shards via `crc32fast::hash(pk_text.as_bytes()) % SHARDS_PER_STREAM`.
2. **Contention-Resilient Single-Item Serialization (`sgl-` Prefix):** Single-item fenced writes serialize same-process contention per `(data_table, row_key)` via an async mutex (`acquire_single_write_lock`) and tag their coordinator IDs with `sgl-<uuid>` (`TxnCoordinator::new_single_txn_id`) so cross-process single-item contenders wait and retry cooperatively rather than aborting unconditional `PutItem`/`UpdateItem` calls with spurious `TransactionConflictException`.
3. **Wait-Free Contiguous Per-Shard Sequence Slot Append (`streams.rs`, `RFC-0003` §5.2):** Phase 1 constructs each `TxnStreamRecord` with `UNASSIGNED_SEQUENCE_NUMBER` (`"00000000000000000000000000"`), its associated `ParticipantRow`, and `<txn_id>:<op_idx>` `dedup_key`. During Phase 2 (`streams::write_record_ordered`), under an in-process per-shard mutex (`shard_local_mutex`), the writer reads `ShardMeta.next_seq_num` (`>= 1`) **first**, checks `stream_dedup:<shard_id>:<dedup_key>` **second**, formats `slot_seq = slot_sequence_number(shard_id, slot)` (`{label_ms:013}{slot:011}{shard_idx:02}`), atomically claims `stream_record:<shard_id>:<slot_seq>` via `Catalog::put_if_absent` (on slot collision, persisting the slot winner's `stream_dedup` marker **before** advancing `ShardMeta.next_seq_num`), persists its own `stream_dedup` markers, and monotonically advances `ShardMeta.next_seq_num` / `last_sequence_number` via CAS (`advance_shard_cursor_at_least`, plus `ensure_shard_past_record` before 24h retention pruning). Because slots `1, 2, 3, ...` are filled strictly in publication order without gaps or duplicates, `get_stream_records` (`after_sequence`) never skips a delayed writer on the same shard.
4. **Atomic Commit Point & Phantom-Free Crash Recovery (`RFC-0003` §5.1, §5.4):** `try_commit_full` writes `COMMITTED` to `__extenddb_txn_log__` together with `t:mutations`, `t:stream_records`, and `t:ttl_mutations` *before* mutating column family `d`. During `roll_forward`, stream records are emitted **only** for `materialized_participants` whose base-row mutation actually materialized, and `stream_dedup` guarantees exact-once emission across arbitrary crash/recovery replays.

#### B. Why the `__extenddb_catalog__` Table Ledger Beats Cloud Bigtable Native Change Streams
Cloud Bigtable offers a native Change Streams CDC feature (`ReadChangeStream`), which we evaluated and deliberately rejected in favor of the coordinator-fenced `__extenddb_catalog__` stream ledger for five architectural reasons:

1. **Transactional Commit Gating:** Bigtable Change Streams emit low-level per-row storage mutations immediately upon `MutateRow` / `CheckAndMutateRow` without awareness of `__extenddb_txn_log__`. Intent placements (`m:intent:<txn_id>`), rolled-back intents, or multi-table 2PC writes in progress would require a complex stateful stream-join consumer to buffer and filter against coordinator state.
2. **Item-Level `StreamViewType` Fidelity:** DynamoDB Streams require complete item-level `OldImage` and `NewImage` projections (`KEYS_ONLY`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`) and exact `INSERT` vs. `MODIFY` vs. `REMOVE` classification. Bigtable Change Streams emit cell-level deltas (`SetCell`, `DeleteFromColumn`, `DeleteFromFamily`) and do not materialize prior full-row item images on partial updates or deletes.
3. **Shard Stability & `u128` Sequence-Number Contract:** Bigtable Change Streams dynamically split and merge tablet partitions and address positions via opaque `ChangeStreamContinuationToken`s, which cannot satisfy DynamoDB's stable shard topology, `u128`-parseable decimal `SequenceNumber` ordering (`AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER`), or 15-minute `ShardIterator` semantics.
4. **TTL Service `UserIdentity` Attribution:** DynamoDB requires TTL deletions to carry `userIdentity = { type: "Service", principalId: "dynamodb.amazonaws.com" }`. Native storage-level CDC carries no application caller or TTL service identity metadata.
5. **Hermetic Emulator & Zero-Admin Portability:** The official `cbtemulator` does not implement `ReadChangeStream`, whereas the `__extenddb_catalog__` ledger runs identically in hermetic CI (`cbtemulator`) and production GCP without requiring separate change-stream retention DDL on every user table.

### 4. Scalable TTL Indexing & Fenced Expiration

#### A. Sharded TTL Index Schema (`__extenddb_ttl_index__`)
* **Row Key Binary Layout:** `[shard_id:1] [expiry_epoch_secs_be:i64:8] [account_id_len:1] [account_id] [table_name_len:1] [table_name] [encoded_base_row_key]`
  * `shard_id = fnv1a(base_row_key) % 16` distributes writes and sweeps evenly across 16 prefixes instead of creating a monotonic timestamp hotspot.
  * `expiry_epoch_secs_be` orders entries chronologically within each shard so each sweep reads only `[shard_id || 0, shard_id || (now + 1))`.
* **OCC-Ordered Write & Recovery Maintenance (`apply_ttl_mutation_versioned`):** `PutItem`, `UpdateItem`, `DeleteItem`, `TransactWriteItems`, and `roll_forward` (via `t:ttl_mutations`) parse TTL attributes via `parse_ttl_epoch_secs` (supporting integer, fractional, and scientific `AttributeValue::N` strings via `bigdecimal` while treating `<= 0` as non-expiring per DynamoDB semantics). Whenever `old_expiry != new_expiry`, old TTL index cells are cleared via bounded `DeleteFromColumn("d", b"", [0, occ_ts))`; whenever `new_expiry` is positive, `insert_ttl_index_entry_at` stamps `SetCell("d", b"", occ_ts)` first followed by bounded `DeleteFromColumn("d", b"", [0, occ_ts))` at `version_to_timestamp_micros(new_version)` (even when `old_expiry == new_expiry`, e.g., `"1000" -> "1000.5"`), making out-of-order `1000 -> 2000 -> 1000` ABA updates commute deterministically with millisecond-aligned timestamps.

#### B. Live Verification, Metrics & Fenced TTL Stream Emission (`ttl_worker.rs`)
1. Under the distributed `lock:ttl_worker` catalog lease, scan `[shard || 0, shard || (now + 1))` across all 16 shards, capturing each scanned index cell's `idx_ts_micros`.
2. Confirm the table still exists and has `TimeToLiveStatus::Enabled`. If the table was dropped or TTL was disabled, retire the stale index cell via bounded `DeleteFromColumn("d", b"", [0, next_millis_exclusive(idx_ts_micros)))` without touching base data.
3. Read the live base item + OCC version (`get_item_with_version`, cooperatively resolving any `COMMITTED` intent) and inspect its current TTL attribute (`parse_ttl_epoch_secs`):
   * If the item was already deleted or updated to a non-positive value, retire only the scanned index cell up to `next_millis_exclusive(bound_ts)` so concurrent same-second TTL inserts at higher OCC timestamps are never wiped.
   * If `live_secs > now` (extended into the future), re-assert `(live_secs, live_ver)` via `ensure_ttl_index_entry` before retiring the stale expired entry.
   * Whenever `0 < live_secs <= now` (even if `live_secs != scanned_expiry` due to ABA index skew), proceed to eviction.
4. Issue `DataEngine::delete_item` with `#ttl = :live_exp_str` `ConditionExpression` and, when streams are enabled on the table, a `StreamCapture` carrying `UserIdentity { principal_id: "dynamodb.amazonaws.com", identity_type: "Service" }`, recording `TimeToLiveDeletedItemCount` and `TtlStalenessSeconds` in `MetricsCollector`, then retire the scanned TTL index cell via bounded `DeleteFromColumn("d", b"", [0, next_millis_exclusive(bound_ts)))`.

### 5. Query, Scan, LSI Ordering & Online GSI Lifecycle

* **Query Direction & `cbtemulator` Compatibility (`query_scan.rs`):** Forward and reverse `Query` calls (`ScanIndexForward`) construct tight byte-range bounds over `[pk_prefix, pk_upper]` (merging `ExclusiveStartKey` into the active sort-key range bound so `BETWEEN` and `begins_with` never lose their upper/lower bounds across pages, [RFC-0003](0003-backend-acceptance-criteria.md) §2.6, and short-circuiting inverted/empty bounds such as `BETWEEN :hi AND :lo` where `:hi > :lo` before issuing an RPC). Production Cloud Bigtable executes descending queries server-side via `ReadRowsRequest { reversed: true }`; when running against `cbtemulator` (which returns `UNIMPLEMENTED` or `FAILED_PRECONDITION` for `reversed: true`), `QueryScan::query` automatically falls back to a bounded partition scan plus in-memory reversal with identical `LastEvaluatedKey` semantics.
* **Exact `LastEvaluatedKey`, LSI `Scan` & Parallel Scan Completeness:** Base and GSI `Query`/`Scan` always resolve `base_key_schema` from catalog `TableDescription` (even when `extenddb-engine` rewrites `TableKeyInfo.key_schema` to the target index schema) and fetch `limit + 1` storage rows so `LastEvaluatedKey` is emitted if and only if unread items remain ([RFC-0003](0003-backend-acceptance-criteria.md) §7.2). `QueryScan::scan_lsi` streams base rows partition-by-partition, filters sparse items lacking `lsi_sk_name` **before** applying `limit`, sorts each partition by `(enc_lsi_sk, enc_base_sk)`, and emits/resumes `LastEvaluatedKey` tuples carrying `(pk, base_sk, lsi_sk)` (§7.1, §7.4). Parallel scans (`total_segments > 1`) page through storage in chunks until `limit + 1` in-segment items (`crc32(pk_prefix) % total_segments == segment`) are collected or the table exhausts (§7.3).
* **Online `UpdateTable` GSI Lifecycle, Version-Guarded Shadows & Background Reconciler (`engine.rs`, `gsi.rs`, `gsi_worker.rs`):** `UpdateTable` executes via `Catalog::mutate_json_cas` (`CatalogJsonAction`) on `table_meta` (matching `update_ttl`, `tag_resource`, `untag_resource`, IAM/policy/secret mutations, and `MetricsStore::insert_metrics`), supporting `GlobalSecondaryIndexUpdates` (`Create` with `CREATING -> backfill -> ACTIVE` transition under `BuildingGsiGuard` and `IndexNotFound` read refusal on `index_info`/`query`/`scan` while `index_status != "ACTIVE"`, `Update` of `WarmThroughput`, and `Delete` with shadow table teardown and orphaned `AttributeDefinitions` pruning), `TableClass`, `WarmThroughput`, and `NoOpUpdate` checks. Both live writes and `gsi_worker` repairs mutate GSI shadow rows exclusively through version-guarded `put_shadow_versioned` / `delete_shadow_versioned` / `delete_shadow_orphan_if_unchanged` (rolling forward committed transactions for the table prior to backfill/reconciliation), and `gsi_worker` promotes interrupted `CREATING` GSIs to `ACTIVE` via CAS only after reconciliation succeeds cleanly.

### 6. GCP Connectivity, Secret Key Ring, Catalog Stores & Error Classification

```toml
[storage.bigtable]
project_id = "my-gcp-project"
instance_id = "my-bt-instance"
data_instance_id = "my-bt-data-instance"  # Optional: separate catalog and data instances
app_profile_id = "single-cluster-profile" # Recommended: single-cluster routing for CheckAndMutateRow
credentials_path = "/etc/extenddb/sa.json" # Optional: explicit GCP service-account JSON key
# emulator_host = "127.0.0.1:8086"        # Optional: local cbtemulator endpoint
pool_size = 20
catalog_pool_size = 8
sweeper_cadence_secs = 30
intent_timeout_secs = 60
ttl_scan_cadence_secs = 60
gsi_reconcile_cadence_secs = 300
```

* **Safe Client Construction & Split-Instance Isolation (`data/client.rs`):** `BigtableClient::connect` builds emulator connections via `BigTableConnection::new_with_emulator` and explicit service-account connections via `gcp_auth::CustomServiceAccount::from_file` + `BigTableConnection::new_with_token_provider` without any `unsafe` process-environment mutation (`std::env::set_var`) or `Box::leak` allocations. When `data_instance_id` differs from `instance_id`, catalog tables, distributed worker leases (`lock:*`), and stream ledger records stay strictly on `instance_id` (`catalog_client` sized by `catalog_pool_size`) while user data tables, GSI shadow tables, `__extenddb_txn_log__`, and `__extenddb_ttl_index__` live strictly on `data_instance_id` (`data_client` sized by `pool_size`).
* **Thin-Binary Compile-Time Registration (`default = ["postgres"]` Preserved):** `crates/bin` preserves `default = ["postgres"]` so standard `cargo build` / `cargo check` invocations work unchanged, while exposing a mutually exclusive `bigtable` Cargo feature (`cargo build --no-default-features --features bigtable`) that installs `extenddb_storage_bigtable::backend()` via `extenddb_storage::set_backend` with zero edits to `crates/core`, `crates/engine`, `crates/auth`, or `crates/server`.
* **At-Rest Secret Encryption & KEK/DEK Key Ring (`extenddb_storage::secret_keys`):** `m0001_baseline` seeds a unique `setting:catalog_id` UUID, and `BigtableCatalogStore` / `BigtableCredentialStore` encrypt and decrypt `AKIA*` access-key secrets and `ASIA*` STS session secrets via `SecretKeys` (`SecretRow::access_key` and `SecretRow::session` AAD binding `(table, access_key_id, account_id, principal, session_name, expires_at)`). `SettingsStore::replace_setting` uses single-row `CheckAndMutateRow` compare-and-swap (`Catalog::mutate_json_cas` / `put_if_absent`), and `ManagementStore` implements cursor-paged `read_secret_page`, CAS `write_secrets`, `delete_secrets`, and `delete_setting` to support `extenddb encryption-key switch`, `rotate`, `rewrap`, and `reset`.
* **IAM Leftover Hygiene, Operational Metrics & Login Rate Limiting (`catalog_store.rs`):** Deleting or creating a user, group, role, or account atomically purges associated policies, permission boundaries, tags, memberships, access keys, and `failed_login:<account_id>/` rate-limit entries (`iam_leftovers_conformance`). `MetricsStore::insert_metrics` accumulates minute-bucketed metrics keyed by `metric:<bucket_secs:012>:<dims_sha256>` via `Catalog::mutate_json_cas` and supports retention pruning (`metrics_conformance`).
* **Frozen Baseline & Additive Migration Ledger (`m0001_baseline.rs`, `migrations.rs`):** `m0001_baseline.rs` (`#[rustfmt::skip]`) provisions `__extenddb_catalog__`, `__extenddb_txn_log__`, and `__extenddb_ttl_index__` plus `setting:catalog_id`; `MigrationId`s `2..=12` record cleanly as schema-less no-ops in `migration_ledger:catalog:<id:04>` under `lock:catalog_migrations`.
* **gRPC Transient Error Classification (`data/client.rs`):** Data-plane and catalog errors are classified via `map_bigtable_err`, `map_catalog_err`, and `classify_grpc_status`:
  * `Unavailable`, `DeadlineExceeded`, `Aborted`, `ResourceExhausted`, RPC timeouts, and transient I/O errors map to `StorageError::Transient` (surfaced by the server as retryable HTTP 503 `ServiceUnavailable` rather than opaque HTTP 500s).
  * Transport connection failures map to `StorageError::Connection`.
  * `InvalidArgument` and `OutOfRange` map to `StorageError::Validation` (HTTP 400).
  * `Unimplemented` maps to `StorageError::Unsupported` (HTTP 400).

### 7. Backup, Snapshot, Restore & Point-in-Time Recovery

* **Explicit `StorageError::Unsupported` Refusal (`backup_stubs.rs`):** Cloud Bigtable does not provide cross-table point-in-time snapshot reads on the data plane, and GCP-native Cloud Bigtable Backups operate asynchronously through the GCP Admin API rather than ExtendDB's logical/store-backed backup format. `BigtableEngine` implements `SnapshotEngine`, `LegacyBackupReader`, `BackupCatalog`, and `RestoreEngine` by returning `StorageError::Unsupported` on all user-facing backup, snapshot, and restore operations (`CreateBackup`, `DescribeBackup`, `ListBackups`, `DeleteBackup`, `RestoreTableFromBackup`), which `extenddb-engine` maps cleanly to `ValidationException` (HTTP 400) rather than `InternalServerError` (HTTP 500). Passive background `BackupReconciler` queries (`check_maintenance_claim`, `list_dead_restore_jobs`, `abandoned_creating_backups`, `list_backup_rows`) return empty `Ok` results so `extenddb serve` runs cleanly without log noise.
* **Point-in-Time Recovery (`#335`):** `DescribeContinuousBackups`, `UpdateContinuousBackups`, and `RestoreTableToPointInTime` are owned directly by `extenddb-engine`, consulting `BigtableEngine::describe_table` solely for table existence.

### 8. Deliverables & Trait Conformance Matrix

Per [RFC-0002](0002-backend-plugin-policy.md) and [RFC-0003](0003-backend-acceptance-criteria.md), `crates/storage-bigtable` implements the full ExtendDB `v1.0.0` backend surface as follows:

| Trait / Subsystem | Classification | Status | Implementation Module | Notes |
|---|---|---|---|---|
| `TableEngine` | Mandatory (Data/Control) | Implemented | `src/engine.rs` | CAS `CreateTable`/`UpdateTable` (online GSI `Create`/`Update`/`Delete` + backfill, `TableClass`, `WarmThroughput`, post-CAS `stream_lineage` transitions), `DeleteTable`, `DescribeTable`, `ListTables`, `table_key_info`, `index_info` |
| `DataEngine` | Mandatory (Data Plane) | Implemented | `src/engine.rs`, `src/data/` | OCC CAS `PutItem`/`UpdateItem`/`DeleteItem` with cooperative intent resolution & versioned GSI shadows, `GetItem`, `Query`, `Scan` (including sparse sorted `scan_lsi`), `scan_key_in_segment`, incarnation-fenced CAS 2PC `TransactWriteItems`, double-read snapshot `TransactGetItems` |
| `VectorSearchEngine` | Optional (`as_vector_search`) | Opted out (`None`) | `src/engine.rs` | Engine rejects vector index DDL and `SearchVectors` upfront |
| `MetadataEngine` | Optional (TTL & Tags) | Implemented | `src/engine.rs`, `src/ttl_worker.rs` | CAS resource tagging (`TagResource`/`UntagResource`/`ListTagsOfResource`) + 16-shard `__extenddb_ttl_index__` with `parse_ttl_epoch_secs` & millisecond-aligned bounds |
| `StreamEngine` | Optional (Streams) | Implemented | `src/engine.rs`, `src/streams.rs` | Fenced write-ahead single-item (`sgl-*`), TTL, and 2PC streams; 4-shard CRC32 partition-key routing + contiguous `put_if_absent` sequence slots (`write_record_ordered`) + `stream_dedup` + `stream_lineage` disable/re-enable history |
| `WorkerStore` | Optional (Lifecycle) | Implemented | `src/engine.rs` | Inline `ACTIVE` table creation (`process_control_plane_transitions` no-op) |
| `SnapshotEngine`, `BackupCatalog`, `LegacyBackupReader`, `RestoreEngine` | Optional (Backup/Restore) | Explicit Refusal | `src/backup_stubs.rs` | User operations return `StorageError::Unsupported` (`ValidationException` HTTP 400); passive reconciler probes return empty `Ok` |
| `ManagementStore`, `AdminStore`, `PasswordHashStore` | Mandatory (IAM & Admin) | Implemented | `src/catalog_store.rs` | Accounts, admins, IAM users/groups/roles/policies/boundaries/tags, STS sessions, CAS password-hash persistence, `SecretKeys` KEK/DEK paging & rotation |
| `AuthorizationStore` | Mandatory (Authz) | Implemented | `src/catalog_store.rs` | User/group/role policies, permission boundaries, resource/principal tags, RFC 3339 `created_at` |
| `SettingsStore`, `MetricsStore`, `RateLimitStore`, `DiagnosticsStore` | Infrastructure / Management | Implemented | `src/catalog_store.rs` | CAS `replace_setting`, CAS SHA-256 dimension metrics accumulation/pruning, failed-login rate-limit store, table/index diagnostics |
| `CredentialStore` | Mandatory (`SigV4` Auth) | Implemented | `src/catalog_store.rs` | `AKIA*` + `ASIA*` `SecretKeys` decryption with `SecretRow` AAD, `ExpiredTokenException`, and inactive-key S-5 propagation |
| `Bootstrapper` | Mandatory (`extenddb init`) | Implemented | `src/bootstrapper.rs` | Idempotent catalog/key/account/admin bootstrap (`catalog_key_present`, `data_connection_present`) and `destroy` cleanup |
| `CatalogMigrations` & `MigrationBackend` | Mandatory (`extenddb migrate`) | Implemented | `src/migrations.rs`, `src/m0001_baseline.rs` | Exhaustive `MigrationId` 1..=12 coverage, frozen `m0001_baseline.rs` checksum, `lock:catalog_migrations` lease |
| `OperationsEngine` | Mandatory (CLI) | Implemented | `src/operations.rs` | Connection URL parsing, identifier validation, sensitive-key redaction |
| `ServerRuntimeHooks` | Mandatory (Workers) | Implemented | `src/runtime_hooks.rs` | Spawns 6 lease-guarded workers (`ttl_worker` with metrics, 2PC `sweeper`, `gsi_reconciler`, 24h `stream_record_cleanup`, 10m `idempotency_token_cleanup`, 5m `table_size_refresh`) with `CancellationToken` shutdown |

## Testing Strategy

1. **Pure Unit Tests (`cargo test -p extenddb-storage-bigtable --lib`):**
   * Exact 38-digit decimal `N` encoding/decoding, zero normalization, and full-spectrum lexicographic order (`number.rs`).
   * Row-key 4 KiB limit enforcement, partition length prefixing, raw terminal sort-key ordering, FDB escape-encoded GSI sort-key roundtrips, and LSI `LastEvaluatedKey` / `ExclusiveStartKey` validation (`row_key.rs`, `gsi.rs`, `ttl_key.rs`, `query_scan.rs`).
   * Monotonic millisecond-aligned OCC version generation (`next_occ_version`, `version_to_timestamp_micros`, `next_millis_exclusive`), `parse_ttl_epoch_secs` decimal/zero/negative parsing, wall-clock-free intent filters, 3-case De Morgan `select_occ_predicate`, and incarnation-fenced `build_pending_incarnation_predicate` / ABA-safe `build_fenced_apply_predicate` `RowFilter` construction (`item_ops.rs`, `transact.rs`), RE2 metacharacter escaping and `monotonic_timestamp_after_micros` (`client.rs`), URL-encoded query parameter / TOML / `string_coerce` env-var config roundtripping (`config.rs`, `bootstrapper.rs`), 26-digit `u128` stream sequence numbers (`slot_sequence_number`) and TTL `UserIdentity` propagation (`streams.rs`), gRPC transient error mapping (`client.rs`), `check_released` migration checksum invariants (`migrations.rs`), and `StorageError::Unsupported` backup/snapshot/restore stubs (`backup_stubs.rs`).
2. **Live Bigtable Emulator Integration & Conformance Suite (`tests/emulator_integration.rs` against `cbtemulator`):**
   * Catalog bootstrap, `BigtableCatalogMigrations` baseline run, table DDL lifecycle, and split catalog-vs-data instance (`data_instance_id != instance_id`) isolation across `BigtableBootstrapper` and background workers (`sweeper`, `ttl_worker`, `gsi_worker`).
   * Guarded single-row CRUD, `ConditionExpression` evaluation, arithmetic `UpdateItem`, `removes_only` missing-item no-ops, and `#478` invalid-operand rejection on missing items.
   * Deterministic OCC cell timestamps (`m:v * 1000` shared across all cells), post-`DeleteItem` `m:v` tombstone retention and rapid re-insert monotonicity, concurrent 10-way `UpdateItem` counter increments (zero lost updates), and 8-way conditional `PutItem` races (1 winner, 7 `ConditionFailed`).
   * Multi-table 2PC `TransactWriteItems`, incarnation-fenced `try_commit_full` vs. `sweeper` race (`AllOrNothing2PCAtomicity`), post-`COMMITTED` expired intent protection (`BUG-03`), canonical sorted `[k1, k2]` vs. `[k2, k1]` locking, `ReturnValuesOnConditionCheckFailure::None` vs. `AllOld` fidelity, 4-vector `ClientRequestToken` idempotency (`IdempotencyAtMostOnce`), and concurrent `TransactGetItems` linearizable snapshot verification (`NoTornTransactGetSnapshot`).
   * Fenced single-item stream emission (`INSERT`/`MODIFY`/`REMOVE`) across `SHARDS_PER_STREAM` (`4`) CRC32-routed shards, out-of-order Phase 2 contiguous slot ordering (`NoSkippedStreamSequenceOnShard`), 12-way concurrent identical-`event_id` deduplication (`write_record_ordered`), superseded `roll_forward` phantom suppression (`NoPhantomStreamRecord`), 36-way single-item stream contention with live tailing consumer, `ImportTable` `stream: None` bulk-import suppression, and full stream disable/re-enable lineage (`stream_lineage`).
   * Exact decimal range queries, inverted `BETWEEN` range guards, reverse `Query` (`ScanIndexForward = false`) pagination, sparse sorted LSI `Scan` (`scan_lsi`) with deleted-`ExclusiveStartKey` resumption, online `UpdateTable` GSI `Create`/`Update`/`Delete` + `CREATING` read refusal + version-guarded GSI shadow mutations (`QuiescentGsiShadowConsistency`), `roll_forward` TTL indexing + `1000 -> future -> 1000 -> 1000.5` ABA / millisecond-aligned TTL bounds (`QuiescentTtlIndexCompleteness`), runtime GC helpers (`cleanup_expired_idempotency_tokens`, `cleanup_expired_stream_records`), and concurrent CAS `MetricsStore::insert_metrics` / `tag_resource`.
   * Upstream shared backend conformance suites: `iam_leftovers_conformance::run`, `metrics_conformance::run`, `key_conformance::run` (`switch`/`rotate`/`rewrap`/`reset`), and `BigtableCredentialStore` `AKIA*` + `ASIA*` (`ExpiredTokenException`) verification.
3. **End-to-End Conformance (`devtools/run-tests`) & TLA+ Formal Verification (`tlc2.TLC`):**
   * Wire-level Rust and pytest conformance suites against an `extenddb` binary built with `--no-default-features --features bigtable`.
   * Exhaustive breadth-first `tlc2.TLC` model checking across 2PC CAS commit/rollback atomicity (`AllOrNothing2PCAtomicity`), double-read `TransactGetItems` linearizability (`NoTornTransactGetSnapshot`), post-commit lease expiry protection (`NoActiveIntentBypassAfterCommit`), multi-RPC per-shard stream sequence visibility & deduplication (`NoSkippedStreamSequenceOnShard`), superseded roll-forward stream gating (`NoPhantomStreamRecord`), TTL index completeness (`QuiescentTtlIndexCompleteness`), 4-vector `ClientRequestToken` idempotency (`IdempotencyAtMostOnce`), and versioned GSI shadow consistency (`QuiescentGsiShadowConsistency`).

## Alternatives Considered

* **Server-Assigned Cell Timestamps (`timestamp_micros: -1`):** Simpler to construct, but risks torn rows under `MaxNumVersions(1)` when two writers or recovery sweeps touch the same row in a narrow window or under clock skew. Rejected in favor of deterministic OCC-derived millisecond-aligned timestamps (`occ_version * 1_000`).
* **Unfenced Trailing Stream Writes on Single-Item Operations:** Lower write amplification (1 RPC instead of coordinator log + fenced apply), but loses stream records if the process crashes after the base-row mutation lands. Rejected because [RFC-0003](0003-backend-acceptance-criteria.md) §5.1 requires strict atomicity between data visibility and stream record durability.
* **Cloud Bigtable Native Change Streams (`ReadChangeStream`):** Rejected for the five reasons detailed in §3.B (lack of 2PC coordinator commit gating, missing pre-image `StreamViewType` reconstruction, dynamic tablet-split continuations incompatible with DynamoDB `ShardIterator` / `u128` `SequenceNumber`s, missing TTL `Service` `UserIdentity`, and lack of `cbtemulator` support).
* **Unprotected Single-Row Writes / Full-Table Scan TTL / Length-Prefixed Sort Keys:** Rejected due to dirty-write isolation violations, $O(N)$ scan cost at scale, and lexicographic sort inversion respectively.
* **Cloud Spanner Backend:** Provides native multi-row serializable transactions without application-layer 2PC, and remains a viable separate backend candidate for relational/Spanner-invested GCP environments.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
