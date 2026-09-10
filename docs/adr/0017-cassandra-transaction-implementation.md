# ADR-0007: DynamoDB Transaction Implementation on Cassandra

- Status: Draft
- Date: 2026-06-24
- Deciders: ExtendDB Cassandra plugin contributors

## Context

DynamoDB provides transactional operations via `TransactWriteItems` and `TransactGetItems` APIs that guarantee ACID properties:

- **Atomicity**: All operations succeed or all fail
- **Consistency**: Strong consistency for reads, serializable isolation for writes
- **Isolation**: Serializable - transactions appear to execute in some serial order
- **Durability**: Committed transactions persist

The PostgreSQL backend implements these using native database transactions with row-level locks (`SELECT ... FOR UPDATE`). Cassandra lacks multi-statement ACID transactions, requiring a different approach.

### DynamoDB Transaction Semantics (Public API Behavior)

From the [AWS DynamoDB documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html):

**Isolation guarantees:**
- `TransactWriteItems` provides **serializable isolation**: concurrent transactions appear to execute in some serial order
- Writes to an item involved in a transaction are **rejected** until the transaction completes (with `TransactionConflictException`)
- Reads to an item involved in a transaction return the **last committed value** (ignore in-progress transaction state)

**Atomicity:**
- Either all operations in a transaction succeed, or all fail
- Partial completion is not possible

**Idempotency:**
- Optional `ClientRequestToken` prevents duplicate execution of same transaction

**Conflict behavior:**
- If two transactions attempt to modify the same item, one succeeds and the other fails with `TransactionCanceledException`
- Non-transactional writes (PutItem, UpdateItem, DeleteItem) to an item involved in a transaction fail with `TransactionConflictException`

### Cassandra Constraints

- **Logged BATCH**: Provides atomicity (all-or-nothing) but **not isolation** - concurrent operations can interleave
- **Lightweight Transactions (LWT)**: Provides compare-and-set with `IF` conditions, but **cannot be used inside BATCH statements**
- **No row-level locks**: Cannot prevent concurrent access to same item
- **No multi-statement transactions**: Each statement commits independently

## Proposal

Implement DynamoDB transactions on Cassandra using **item-level transaction markers with Lightweight Transactions (LWT) for conflict detection**, implementing a two-phase commit protocol similar to standard distributed transaction systems.

### Schema Changes

**1. Add transaction system attributes to all data tables:**

```cql
ALTER TABLE {keyspace}.items_{table_id} ADD (
    partition_max_delete_timestamp bigint STATIC,  -- partition-level: prevents old txns creating deleted items
    prepared_txn_id uuid,                          -- NULL = not in transaction, UUID = in transaction
    prepared_txn_timestamp bigint,                 -- when PREPARE started (for timeout detection)
    last_committed_txn_timestamp bigint,           -- ordering for serializability
    created_to_prepare boolean                     -- true if item created by PREPARE (for ROLLBACK)
);
```

**STATIC column explanation:** `partition_max_delete_timestamp` is shared across all rows with the same partition key (`pk`), providing partition-level metadata that persists even after all rows are deleted. This prevents transactions with old timestamps from incorrectly creating items after they've been deleted at a later timestamp.

**2. Create transaction ledger table (in catalog keyspace):**

```cql
CREATE TABLE {catalog_keyspace}.transaction_ledger (
    txn_id uuid PRIMARY KEY,
    state text,                          -- 'preparing', 'committing', 'rollback'
    started_at bigint,                   -- timestamp for age-based detection
    client_token text,                   -- optional ClientRequestToken for idempotency
    request_fingerprint text,            -- hash of request for idempotency validation
    items_blob text                      -- JSON: [{keyspace, table_id, pk, sk, operation}, ...]
);

-- Index for efficient age-based scanning during recovery
CREATE INDEX ON transaction_ledger (started_at);
```

**Rationale for single-row ledger design:**
- Single INSERT creates complete transaction record atomically (no partial-write issues)
- State updates are single-row operations (atomic state transitions)
- Ledger is read only during recovery (rare event) - parsing JSON blob has negligible performance impact
- Simpler than multi-row design: no static columns, no row counting, no sentinel records
- Cell size limit (~2MB) supports thousands of items (DynamoDB limits to 100 items per transaction)

### Transaction Protocol

#### TransactWriteItems

**Phase 0: LEDGER CREATION**

1. Generate unique `txn_id` (UUID)
2. Serialize all items to JSON blob
3. **Write transaction to ledger atomically**:
   ```cql
   INSERT INTO transaction_ledger (txn_id, state, started_at, 
                                    client_token, request_fingerprint, items_blob)
   VALUES (?, 'preparing', ?, ?, ?, ?);
   ```

**Phase 1: PREPARE**

4. **Parse items from ledger** (only needed during recovery; normal path has items in memory)
5. For each item:
   - **Read existing item** (if any) to evaluate conditions
   - **Validate all conditions** in-memory
   - **For non-existent items (PUT operations), check partition max delete timestamp**:
     ```cql
     SELECT partition_max_delete_timestamp FROM items WHERE pk = ? LIMIT 1;
     ```
     If `partition_max_delete_timestamp` exists and `txn_timestamp <= partition_max_delete_timestamp`, reject transaction (would violate timestamp ordering—item was deleted at a later timestamp)
6. If all conditions pass, **execute PREPARE for each item**:
   - If item exists: `UPDATE items SET prepared_txn_id = ?, prepared_txn_timestamp = ? WHERE pk = ? AND sk = ? IF prepared_txn_id IS NULL`
   - If item doesn't exist (for Put): `INSERT INTO items (pk, sk, prepared_txn_id, prepared_txn_timestamp, created_to_prepare) VALUES (?, ?, ?, ?, true) IF NOT EXISTS`
7. **If any PREPARE fails** (item already in another transaction): update ledger state to 'rollback' and proceed to ROLLBACK

**Phase 2: COMMIT** (if all prepares succeeded)

8. **Update ledger state**: `UPDATE transaction_ledger SET state = 'committing' WHERE txn_id = ?`
9. For each item (from memory or parsed from ledger), execute in parallel:
   - Put/Update: `UPDATE items SET item_data = ?, prepared_txn_id = NULL, last_committed_txn_timestamp = ? WHERE pk = ? AND sk = ? IF prepared_txn_id = ?`
   - Delete:
     ```cql
     -- Update partition STATIC column
     UPDATE items SET partition_max_delete_timestamp = ?
     WHERE pk = ?
     IF partition_max_delete_timestamp < ? OR partition_max_delete_timestamp IS NULL;
     
     -- Delete the item
     DELETE FROM items WHERE pk = ? AND sk = ? IF prepared_txn_id = ?;
     ```
10. Wait for all COMMIT operations to complete
11. **Delete transaction from ledger**: `DELETE FROM transaction_ledger WHERE txn_id = ?`

**Phase 2: ROLLBACK** (if any prepare failed)

8. **Update ledger state** (if not already): `UPDATE transaction_ledger SET state = 'rollback' WHERE txn_id = ?`
9. For each item (from memory or parsed from ledger), execute in parallel:
   - If `created_to_prepare = true`: `DELETE FROM items WHERE pk = ? AND sk = ? IF prepared_txn_id = ?`
   - Else: `UPDATE items SET prepared_txn_id = NULL WHERE pk = ? AND sk = ? IF prepared_txn_id = ?`
10. Wait for all ROLLBACK operations to complete
11. **Delete transaction from ledger**: `DELETE FROM transaction_ledger WHERE txn_id = ?`

#### TransactGetItems

Execute using a **two-phase protocol** to ensure serializability:

**Phase 1: Initial Reads**

For each item in the read set:
```cql
SELECT item_data, last_committed_txn_timestamp, prepared_txn_id
FROM items WHERE pk = ? AND sk = ?
USING CONSISTENCY LOCAL_QUORUM;
```

- If **any** item has `prepared_txn_id != NULL`: reject transaction with `TransactionConflictException` (concurrent write transaction is preparing this item)
- Store item values and timestamps in memory

**Phase 2: Verification Reads**

For each item:
```cql
SELECT last_committed_txn_timestamp, prepared_txn_id
FROM items WHERE pk = ? AND sk = ?
USING CONSISTENCY LOCAL_QUORUM;
```

- Compare `last_committed_txn_timestamp` from Phase 1 vs Phase 2
- If **any** timestamp changed: reject transaction (item was written between phases)
- If **any** `prepared_txn_id != NULL`: reject transaction (new write transaction started)
- If all verifications pass: return item values captured in Phase 1

**Rationale:** This two-phase protocol ensures serializability by detecting concurrent writes without updating items or acquiring locks. The `last_committed_txn_timestamp` serves as a sequence number to detect changes. Both phases return committed data, ignoring prepared state, matching DynamoDB's read-committed visibility.

### Integration with Non-Transactional Operations

**Write operations** (PutItem, UpdateItem) must check for prepared transactions:

```cql
-- Example: PutItem
UPDATE items 
SET item_data = ?, last_committed_txn_timestamp = ? 
WHERE pk = ? AND sk = ? 
IF prepared_txn_id IS NULL
```

**Delete operations** must update partition max delete timestamp and check for prepared transactions:

```cql
-- Update partition STATIC column
UPDATE items
SET partition_max_delete_timestamp = ?
WHERE pk = ?
IF partition_max_delete_timestamp < ? OR partition_max_delete_timestamp IS NULL;

-- Delete the item
DELETE FROM items 
WHERE pk = ? AND sk = ? 
IF prepared_txn_id IS NULL;
```

If `prepared_txn_id IS NULL` condition fails, return `TransactionConflictException` to client.

**Read operations** (GetItem, Query, Scan) execute normally - prepared transactions are ignored, returning committed data.

### Idempotency Token Handling

Idempotency uses the existing `idempotency_tokens` table (same as non-transactional operations):

1. **Before creating ledger entry**, check for existing token:
   ```cql
   SELECT token, fingerprint, created_at 
   FROM idempotency_tokens 
   WHERE token = ?;
   ```
2. If token exists with matching fingerprint: return success (transaction already completed)
3. If token exists with different fingerprint: return error (token reused incorrectly)  
4. If token doesn't exist: **insert token** and proceed with ledger creation:
   ```cql
   INSERT INTO idempotency_tokens (token, fingerprint, created_at) 
   VALUES (?, ?, ?) IF NOT EXISTS;
   ```
5. Token is written **before** ledger entry, ensuring idempotency is established before any transaction state

The ledger's `client_token` and `request_fingerprint` columns are retained for observability (which token was used for this transaction) but not queried for idempotency checks.

### Fault Tolerance and Failed Transaction Recovery

**Failed transactions** occur when ExtendDB process fails between PREPARE and COMMIT/ROLLBACK. Detection mechanisms:

1. **Age-based detection**: Background worker scans ledger for transactions with `started_at` older than threshold (e.g., 60 seconds):
   ```cql
   SELECT txn_id, state, started_at, items_blob
   FROM transaction_ledger 
   WHERE started_at < ?
   ALLOW FILTERING;
   ```

2. **Conflict-based detection**: Non-transactional writes encountering prepared transactions check `prepared_txn_timestamp`; if too old, trigger recovery

**Recovery process:**

For each failed transaction found:

1. **Read transaction from ledger**:
   ```cql
   SELECT state, items_blob FROM transaction_ledger WHERE txn_id = ?;
   ```

2. **Parse items from JSON blob** to get complete list

3. **Continue based on state**:
   - state = 'preparing' → execute ROLLBACK for all items
   - state = 'committing' → execute COMMIT for all items (resume)
   - state = 'rollback' → execute ROLLBACK for all items (resume)

4. **Delete from ledger** once complete:
   ```cql
   DELETE FROM transaction_ledger WHERE txn_id = ?;
   ```

The single-row ledger ensures recovery always has the complete, consistent list of items (no partial-write issues).

### Streams Integration

**Important**: Stream records should **only** be generated for successfully committed transactions. The following operations do NOT generate stream records:
- PREPARE phase operations (writing transaction markers)
- ROLLBACK phase operations (cleaning up failed transactions)
- Recovery operations (rolling back failed transactions)

Only the COMMIT phase generates stream records, ensuring that streams reflect the logical transaction as a single atomic unit.

## Background Workers

The transaction implementation requires background workers (implemented in `src/workers.rs`):

### 1. Transaction Recovery Worker

**Responsibility:** Detect and recover failed transactions

**Operation:**
- Periodically scans `transaction_ledger` for transactions older than timeout threshold (e.g., 60 seconds)
- For each failed transaction:
  - Reads transaction state and items from ledger
  - Executes ROLLBACK for transactions in 'preparing' state
  - Resumes COMMIT for transactions in 'committing' state
  - Resumes ROLLBACK for transactions in 'rollingback' state
  - Deletes transaction from ledger once complete
- Recommended scan frequency: every 30 seconds

**Error handling:** Failures during recovery are logged; transaction remains in ledger for next scan iteration

### 2. Ledger Cleanup Worker (Optional)

**Responsibility:** Clean up stale ledger entries that recovery worker couldn't process

**Operation:**
- Scans for transactions older than extended threshold (e.g., 24 hours)
- Logs warnings for investigation
- Optionally moves to dead-letter table for manual review
- Prevents unbounded ledger growth

**Recommended scan frequency:** every hour or daily, depending on operational requirements

## Implementation Details

Background workers are spawned during ExtendDB initialization and run for the lifetime of the process. Worker implementation follows existing patterns in `src/workers.rs` (control plane transition worker, etc.).

## Rationale

### Why This Approach Provides DynamoDB Semantics

**Atomicity**: 
- PREPARE phase ensures all items can be included in the transaction before any writes
- If any PREPARE fails, transaction rollbacks and all prepared items are cleaned up
- COMMIT/ROLLBACK phases execute all operations; failures trigger retry

**Consistency**:
- `TransactGetItems` uses `LOCAL_QUORUM` reads
- `TransactWriteItems` uses `LOCAL_QUORUM` writes

**Isolation (Serializable)**:
- Items with `prepared_txn_id != NULL` reject all concurrent writes (transactional or not)
- LWT ensures only one transaction can prepare an item at a time
- `last_committed_txn_timestamp` establishes transaction order
- Reads ignore prepared state, returning last committed data (matches DynamoDB)

**Durability**:
- All writes use `LOCAL_QUORUM` for durability
- Idempotency tokens prevent duplicate execution

### Why Not Cassandra Logged BATCH

Logged BATCH provides atomicity but **not isolation**. Without isolation:
- Two transactions could prepare the same item concurrently
- Non-transactional writes could interleave with transaction writes
- Cannot guarantee serializable isolation

### Why LWT Outside BATCH

Cassandra limitation: LWT (`IF` conditions) cannot be used inside BATCH statements. However:
- PREPARE operations need LWT to detect conflicts atomically
- COMMIT/ROLLBACK operations can execute in parallel (not batched) since atomicity is already guaranteed by PREPARE phase
- Performance impact is acceptable: DynamoDB itself executes operations in parallel during COMMIT

### Comparison to DynamoDB

**Similarities**:
- Two-phase commit protocol with PREPARE/COMMIT/ROLLBACK phases
- Transaction ledger tracks in-progress transactions for recovery
- Item-level markers prevent concurrent modifications to same item
- Reads ignore in-progress transactions (return last committed value)
- Idempotency token support
- Age-based detection and recovery for failed ExtendDB processs

**Differences**:
1. **Ledger structure**: Implementation uses single-row ledger with JSON blob for items list. DynamoDB's internal ledger structure is not publicly documented but likely differs.

2. **COMMIT/ROLLBACK not batched**: Operations execute in parallel but are not batched into single atomic operation (Cassandra LWT limitation). This matches DynamoDB's parallel execution approach during COMMIT/ROLLBACK phases.

3. **Recovery strategy**: Transactions found in 'preparing' state are always rolled back (conservative). This is a safe default when ExtendDB process state is unknown.

4. **Process failure handling**: Failed transactions are detected by age-based scanning of the ledger rather than heartbeat mechanisms.

## Consequences

**Positive**:
- Achieves DynamoDB transaction semantics on Cassandra
- No external dependencies (no Redis, ZooKeeper, etc.)
- Leverages Cassandra's native LWT for conflict detection
- Compatible with existing non-transactional operations
- Fault-tolerant with orphan detection

**Negative**:
- LWT operations are expensive (quorum reads + writes with Paxos round)
- Cannot batch COMMIT/ROLLBACK operations (performance impact vs. PostgreSQL)
- PREPARE phase requires one LWT per item (N items = N round trips)
- Orphan recovery requires scanning for items with same `prepared_txn_id` (can be optimized with secondary index)

**Performance Characteristics**:
- PREPARE phase: `O(N)` LWT operations (N = number of items)
- COMMIT/ROLLBACK phase: `O(N)` parallel LWT operations  
- Non-transactional writes: one additional LWT condition check (`IF prepared_txn_id IS NULL`)
- Reads: no performance impact (ignore prepared state)

**Implementation Impact**:
- Modify all write operations (PutItem, UpdateItem, DeleteItem) to check `prepared_txn_id`
- Add background worker for orphan detection
- Implement transaction ExtendDB process logic (PREPARE/COMMIT/ROLLBACK state machine)
- Add metrics for transaction latency, rollback rate, failed transaction detection

## Alternatives Considered

### Option 1: Cassandra Logged BATCH Only

Use logged BATCH for atomicity without LWT.

**Rejected**: Cannot guarantee serializable isolation. Concurrent transactions can prepare the same item, violating DynamoDB semantics.

### Option 2: External Lock Service (Redis, ZooKeeper)

Use distributed locks to serialize access to items.

**Rejected**: Adds infrastructure dependency, single point of failure, operational complexity. Defeats purpose of using Cassandra's distributed architecture.

### Option 3: Optimistic Concurrency with Version Column

Use existing `version` column for all operations, retry on conflict.

**Rejected**: 
- Cannot distinguish between "item changed by another transaction" vs. "transaction in progress" without transaction markers
- Retry logic becomes complex (how many retries? exponential backoff?)
- No clear rollback mechanism for failed transactions

### Option 4: Document Limitation (No Isolation)

Accept that Cassandra cannot provide serializable isolation.

**Rejected**: Fundamentally breaks DynamoDB transaction contract. Transactions without isolation are not useful for most use cases (e.g., bank transfers, inventory management).

### Option 5: Generic Transaction Coordination in ExtendDB Core

Implement transaction coordination logic (two-phase commit, ledger, recovery) in the `storage` crate, exposing minimal primitives that backends must implement (e.g., atomic conditional writes, strongly consistent reads).

**Rejected**: While appealing for code reuse, this approach has fundamental problems:

1. **Different databases need different mechanisms**: PostgreSQL uses native ACID transactions with `SELECT ... FOR UPDATE` (no ledger, no two-phase commit, no recovery needed). Forcing it to use a ledger would be strictly worse—fighting the database instead of leveraging its strengths.

2. **Primitives don't compose uniformly**: The Cassandra implementation uses LWT for conflict detection and STATIC columns for partition-level metadata. Many databases lack these features. Abstracting over such differences creates a lowest-common-denominator that makes all backends worse.

3. **Performance vs. correctness trade-offs vary**: What constitutes an acceptable trade-off depends on what the backend provides. PostgreSQL can use row locks (blocking). Cassandra must use conflict detection (non-blocking). A shared abstraction can't optimize for both.

4. **Backend-specific optimizations matter**: PostgreSQL's native rollback is orders of magnitude faster than implementing manual rollback. Cassandra's LWT parallelization strategy differs from PostgreSQL's lock acquisition order. These details affect correctness and performance.

**Better approach**: Share high-level validation logic (condition expression evaluation, error types, request parsing) but keep coordination mechanisms backend-specific. The trait defines *what* (DynamoDB API semantics) not *how* (implementation mechanism).

## Open Questions

1. **Transaction timeout**: What is appropriate timeout for `started_at` before considering transaction failed and triggering recovery? Suggested: 60 seconds (conservative, allows for slow operations).

2. **Cross-keyspace transactions**: Should transactions spanning multiple accounts (different keyspaces) be supported? DynamoDB supports cross-table transactions within same account. Implementation would require ledger to store keyspace per item (already included in schema).

3. **GSI/LSI updates**: How do secondary index updates integrate with transaction protocol? Should index updates be part of COMMIT phase? Recommendation: treat index entries as additional items in transaction (add to ledger, PREPARE/COMMIT them).

4. **Streams integration**: Should stream records be written during COMMIT phase (atomically with item writes) or after COMMIT completes? Recommendation: write stream records during COMMIT phase, include in transaction for atomicity.

5. **Ledger retention**: Should failed transactions remain in ledger for debugging, or be deleted immediately after recovery? Trade-off between observability and storage cost.

## Implementation Plan

1. **Phase 1**: Schema changes (add transaction columns to all item tables)
2. **Phase 2**: Implement `TransactGetItems` (simpler - just parallel reads)
3. **Phase 3**: Implement `TransactWriteItems` PREPARE phase with LWT
4. **Phase 4**: Implement COMMIT/ROLLBACK phases
5. **Phase 5**: Integrate with non-transactional operations (add `IF prepared_txn_id IS NULL`)
6. **Phase 6**: Implement failed transaction detection and recovery
7. **Phase 7**: Testing (unit tests, integration tests, correctness tests with concurrent transactions)

Estimated effort: **3-4 weeks** for complete implementation and testing.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
