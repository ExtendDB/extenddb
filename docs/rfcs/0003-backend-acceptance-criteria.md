# RFC-0003: Backend Acceptance Criteria

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-07-15
- FCP ends: (set when entering Final Comment Period)

## Summary

This RFC defines the concrete DynamoDB-behavioral invariants and implementation standards every storage backend must satisfy before acceptance into the ExtendDB repository. It complements [RFC-0002](0002-backend-plugin-policy.md) (the *process*: trait conformance, mono-repo structure, CI) with the substance: what "DynamoDB-compatible" means at the storage layer, the observable behavior a backend must uphold, and the standards its code and RFC must meet (e.g. the RFC describes the system as built, not dead code).

These criteria were distilled from the review of several backend RFCs, which revealed ten classes of behavioral divergence that a process-focused policy alone did not prevent.

## Motivation

[RFC-0002](0002-backend-plugin-policy.md) requires backends to "match DynamoDB behavior including error responses, pagination, isolation guarantees, atomicity guarantees, and consistency models" but does not enumerate what those guarantees are at the storage-trait level. A backend author reading only the trait signatures and RFC-0002 can produce an implementation that compiles, registers correctly, and handles the happy path, while silently diverging on concurrency, ordering, key identity, and transactional side-effects.

This document exists so that:

1. Backend authors have a checklist to design against before writing code.
2. Reviewers have a rubric to evaluate submissions against.
3. The conformance test suite can be organized around these invariants.

## Criteria

Each criterion below states an invariant, the rationale (what breaks if violated), and where applicable, a reference to the storage trait contract or the PostgreSQL reference implementation.

### 1. Item Identity

**1.1. Unique addressability.** Every DynamoDB item is uniquely identified by its full primary key (partition key + optional sort key). The storage encoding of this identity must be collision-free for all valid key values, including values containing any byte sequence, delimiter characters, and the empty string.

**1.2. No delimiter ambiguity.** If the backend encodes composite keys into a single field (e.g., a document `_id`), the encoding must be provably collision-free. Acceptable approaches: netstring encoding, length-prefixed encoding, or separate fields. Unacceptable: concatenation with an unescaped delimiter character that can appear in key values.

**1.3. Numeric precision.** DynamoDB numbers have 38 significant digits and an exponent range of -130 to +125. The backend must either:

* Store numeric sort keys with at least 38 digits of precision (e.g., `BigDecimal`, arbitrary-precision decimal), OR
* Reject keys exceeding its precision limit with a documented `ValidationException` at write time, never silently round, truncate, or collide. Mixing storage types for the same logical field (e.g., some documents as Decimal128, others as float64) is prohibited.

**1.4. Binary key ordering.** DynamoDB orders binary values by unsigned byte-level lexicographic comparison, regardless of length. If the backend's native binary comparison differs (e.g., BSON BinData compares by length first), the backend must implement application-level ordering for sort, range queries, and `begins_with` on binary sort keys, or document the divergence as an unsupported configuration.

### 2. Secondary Indexes

**2.1. Duplicate index keys.** GSI keys are non-unique. Multiple base-table items may share identical GSI partition key and sort key values. The index entry must be uniquely identified by the combination of index keys AND base-table primary keys. Implementations that key index entries solely on index key values will silently lose items.

**2.2. Stale entry prevention.** When a write changes or removes a GSI key attribute, the backend must delete the old index entry and (if applicable) insert a new one. This requires capturing the pre-mutation item image on every write path that can modify indexed attributes, not just when `ReturnValues` or streams are requested.

**2.3. Sparse index semantics.** Items missing a GSI key attribute must NOT appear in the GSI. Items where the GSI key attribute exists but has a type incompatible with the index's declared attribute type must be rejected with `ValidationException` at write time.

**2.4. Backfill on UpdateTable.** When a GSI is added to an existing table via `UpdateTable`, all existing items matching the index key schema must be backfilled into the index. The index status must remain CREATING (with backfilling in progress) until the backfill completes, then transition to ACTIVE; it must never report ACTIVE before the backfill completes.

**2.5. Transactional index maintenance.** `TransactWriteItems` operations must propagate to secondary indexes within the same transaction (or equivalent atomic unit). An index that is updated for single-item writes but not for transactional writes is non-conformant.

**2.6. Index query pagination.** `LastEvaluatedKey` for a query against a secondary index must contain both the index keys and the base-table keys. The resume logic must apply the `ExclusiveStartKey` bounds in addition to (not instead of) the original key condition. A paginated `BETWEEN` query must not lose its upper bound on page 2.

**2.7. ConsistentRead on GSI.** Queries on a GSI with `ConsistentRead=true` must return `ValidationException`. The engine layer enforces this, but the backend must not advertise or rely on strong consistency for GSI reads.

### 3. Transactions

**3.1. All-or-nothing atomicity.** All operations within a `TransactWriteItems` call must succeed or all must roll back. This includes the data writes, the secondary index updates, the stream records, and the idempotency token, all in one atomic unit.

**3.2. Stream records in transaction.** When `stream` is `Some` on a `TransactWriteOp`, the stream record must be written in the same atomic unit as the data write. This is not optional, the trait contract explicitly requires it.

**3.3. Idempotency token atomicity.** The `ClientRequestToken` check, store, and associated writes must be atomic. Concurrent duplicate requests must result in exactly one execution. This typically requires a unique constraint on the token value and insertion within the write transaction.

**3.4. CancellationReasons fidelity.** On transaction failure, the `TransactionCanceled` error must carry per-operation `CancellationReason` entries in the same order as the input operations, with `ConditionalCheckFailed` at the correct index. When `ReturnValuesOnConditionCheckFailure=ALL_OLD` is set, the reason must include the existing item.

**3.5. TransactGetItems isolation.** All reads within a `TransactGetItems` must observe the same snapshot, no read can see a write that another read in the same call does not see.

### 4. Concurrency and Isolation

**4.1. Single-item writes must not fail under contention.** Two concurrent unconditional `PutItem` calls on the same key must both succeed (last-writer-wins semantics). The backend must not surface internal concurrency-control mechanisms (e.g., WriteConflict, version mismatches) as client-visible errors for unconditional single-item writes.

**4.2. Conditional writes: correct error on race.** When a conditional write races with another write and the condition is no longer met, the backend must return `ConditionalCheckFailedException`, never `InternalServerError` or an unrecognized error code.

**4.3. Transaction conflict error mapping.** When a `TransactWriteItems` call conflicts with another transaction or a concurrent write and is cancelled, the backend must return `TransactionCanceledException` with a `TransactionConflict` cancellation reason, not an internal error. When a single-item write (`PutItem`/`UpdateItem`/`DeleteItem`) conflicts with an in-flight transaction on the same item, the backend must return `TransactionConflictException`, not `InternalServerError`. The backend may retry transient conflicts internally, but must not exhaust retries and surface an unmapped error.

**4.4. UpdateItem must serialize.** Concurrent `UpdateItem` calls on the same key (including ADD operations) must all eventually succeed (assuming no condition failures), and their effects must be applied cumulatively: two concurrent `ADD counter :one` operations must leave the counter incremented by two, with no lost update. The backend may use any mechanism (locks, OCC with retry, native atomic operators) but must not return errors to the client for contention that DynamoDB handles transparently, nor silently discard a committed write.

**4.5. Read-your-writes.** A `GetItem` with `ConsistentRead=true` immediately after a successful write to the same key must return the written item. The backend must not route consistent reads to replicas that may lag.

### 5. DynamoDB Streams

**5.1. Atomicity with data writes.** A stream record must become visible if and only if its corresponding data write committed. A crash between a committed data write and its stream record insert must not be possible, they must be in the same atomic unit (transaction, WAL entry, etc.).

**5.2. Per-shard ordering.** Within a shard, records must be strictly ordered by sequence number, and a consumer paging forward must never skip a committed record. This means sequence number assignment and record visibility must be tied to commit order. A record with sequence N must not become visible to consumers before all records with sequence < N on the same shard are also visible.

**5.3. Account and table isolation.** Stream records for one account/table must never be visible to consumers of a different account/table's stream. Shard identifiers must incorporate the table's unique identity (not just the table name) and queries must be scoped to prevent cross-tenant leakage.

**5.4. Correct event types.** INSERT when the item did not previously exist (including UpdateItem upserts); MODIFY when the item existed and was changed; REMOVE when the item was deleted. The presence/absence of OldImage and NewImage must match the event type and the stream's `StreamViewType`.

**5.5. Retention and cleanup.** Stream records must be removed after the configured retention period (default 24 hours). A `TRIM_HORIZON` iterator must not replay records older than the retention window. A background worker or native TTL mechanism must enforce this.

**5.6. Shard stability.** The number and identity of shards for a table must be stable across server restarts and across redundant `UpdateTable` calls with the same stream specification. Enabling an already-enabled stream must be idempotent (or rejected), not create duplicate shards.

### 6. Condition Expressions

**6.1. Evaluation semantics must match `extenddb_core`.** The canonical condition evaluator lives in `extenddb_core::expression`. Backends may either:

* Delegate to this evaluator (read item, evaluate in application code, as the PostgreSQL backend does), OR
* Implement native filter pushdown that produces identical results for all inputs.

If implementing pushdown, numeric comparisons must be numeric (not string-lexicographic), type mismatches must evaluate to false (not match or error), and missing attributes must follow DynamoDB's rules (comparisons are false, `attribute_not_exists` is true).

**6.2. Unused code must be marked experimental or removed.** Code that is not wired into a live path (for example, a compiler or helper that is imported but never called) must be clearly marked experimental, feature-gated, or deleted. It must not be hidden behind crate-level or blanket `#[allow(unused)]` suppressions, which can conceal unimplemented features the trait surface expects to be called and let latent bugs ship dormant. A targeted `#[allow(unused)]` on an intentionally stubbed method is acceptable when accompanied by a comment explaining why.

**6.3. Documentation must describe the system as built.** The RFC and accompanying documentation must not present a mechanism as operational unless it is implemented and integrated into the live code path. Where an RFC proposes a mechanism that is not yet built, it must be clearly marked as proposed or future work, not described as the current behavior.

### 7. Query and Scan

**7.1. Limit semantics.** `Limit` caps the number of items *read from storage* (before FilterExpression is applied by the engine layer). The storage layer must apply the limit to its fetch, not to the final result set.

**7.2. LastEvaluatedKey correctness.** `LastEvaluatedKey` must reflect the last item read from storage, using the correct key schema (base-table schema for table queries/scans; combined base+index schema for index queries). It must not be omitted when more items exist, and must not be present when no more items exist.

**7.3. Parallel scan completeness.** Each segment of a parallel scan must eventually return every item that hashes to that segment, regardless of the Limit parameter or the physical ordering of documents. A segment must not terminate early (return no `LastEvaluatedKey`) while unread items remain.

**7.4. Sort order.** Query results must be sorted by sort key value in the direction specified by `ScanIndexForward`. Sort order must match DynamoDB's rules: string keys by UTF-8 byte order, numeric keys by numeric value, binary keys by unsigned byte-level lexicographic order.

### 8. Multi-Tenancy

**8.1. Account isolation.** All data-plane operations are scoped by `account_id`. It must be impossible for operations in one account to read, write, or observe data belonging to another account, including stream records, shard metadata, sequence counters, and idempotency tokens.

**8.2. Table-name reuse.** After a table is deleted, a new table with the same name (in the same or different account) must not inherit any data, index entries, stream records, or shards from the deleted table. Identifiers derived from table names (rather than unique table IDs) are a common source of this bug.

### 9. Error Fidelity

**9.1. No silent degradation.** If an operation cannot be performed correctly, the backend must return an error, never silently drop side-effects (stream records, index updates), return partial results without a `LastEvaluatedKey`, or succeed while leaving inconsistent state.

**9.2. DynamoDB error codes.** Client-visible errors must use DynamoDB-compatible error codes and HTTP status codes. Internal storage errors (connection failures, type mismatches) must map to appropriate DynamoDB errors, typically `InternalServerError` (500) with retry semantics, or `ValidationException` (400) for input problems. Backend-specific error codes must never leak to clients.

### 10. Operational Correctness

**10.1. Background workers.** If the backend implements `StreamEngine`, `MetadataEngine` (TTL), or other features requiring periodic maintenance, the corresponding background workers must be spawned via `ServerRuntimeHooks::spawn_workers`. Implementing the trait method without scheduling the worker is non-conformant.

**10.2. Cache coherence.** Any in-memory cache (e.g., "does this table have GSIs?") must either be invalidated on relevant DDL operations across all server processes, or use a TTL short enough that stale entries self-heal within a bounded period. A negative cache entry that persists until process restart is not acceptable for mutable metadata.

**10.3. UpdateTable idempotency.** Repeated `UpdateTable` calls with the same specification must not corrupt state (duplicate shards, broken ARNs, phantom index entries). Either reject the redundant call with the appropriate error, or make it a no-op.

## Conformance Verification

### Required test suites

A backend submission must pass, at 100%, against the backend-under-test. The Python and Rust suites are both mandatory gates; neither is primary over the other.

1. **Rust tests** (unit and integration): `devtools/run-tests --extenddb --rust` and `devtools/run-tests --extenddb --rust-integration`.
2. **Python conformance suite** (integration and comprehensive): `devtools/run-tests --extenddb --pytest` and `devtools/run-tests --extenddb --comprehensive`.
3. **External test suite** (where applicable): `devtools/run-tests --extenddb --external`.

All of the above run together as `devtools/run-tests --extenddb --all`, which must pass in CI before acceptance.

### Recommended stress tests

The following scenarios expose the most common failure modes and should be tested under concurrency:

* Two concurrent `PutItem` on the same key (must both succeed).
* Two concurrent conditional `PutItem` with `attribute_not_exists(pk)` (one succeeds, one gets `ConditionalCheckFailedException`).
* Concurrent `UpdateItem ADD counter :one` at 50+ concurrent clients (all must succeed, and the counter must reflect every increment).
* `TransactWriteItems` racing a single-item `PutItem` on a shared key (the transaction either succeeds or returns `TransactionCanceledException` with `TransactionConflict`, and the single-item write either succeeds or returns `TransactionConflictException`, never 500).
* GSI query after updating a GSI key attribute (old value must not appear; new value must appear).
* Paginated query with `BETWEEN` condition spanning 3+ pages (all pages respect the range bounds).
* Stream consumer reading during concurrent writes (no gaps, correct event types, no cross-table leakage).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
