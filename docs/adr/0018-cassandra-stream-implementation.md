# ADR-0008: DynamoDB Streams Implementation on Cassandra

- Status: Draft
- Date: 2026-06-25
- Deciders: ExtendDB Cassandra plugin contributors

## Context

DynamoDB Streams captures a time-ordered sequence of item-level modifications in DynamoDB tables. From the [AWS DynamoDB Streams documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html):

> "A DynamoDB stream is an ordered flow of information about changes to items in a DynamoDB table. When you enable a stream on a table, DynamoDB captures information about every modification to data items in the table."

### DynamoDB Streams Semantics (Public API Behavior)

**Ordering guarantees:**
- Each stream record is assigned a sequence number
- "For each item that is modified in a DynamoDB table, the stream records appear in the same sequence as the actual modifications to the item"
- Sequence numbers reflect the order in which records were published to the stream

**Data retention:**
- "All data in DynamoDB Streams is subject to a 24-hour lifetime"
- Records older than 24 hours are subject to removal at any time

**Shard structure:**
- Stream records are organized into shards
- Each shard contains multiple stream records
- Applications can process records from multiple shards in parallel

**Stream view types (StreamSpecification):**
- `KEYS_ONLY`: Only key attributes of modified item
- `NEW_IMAGE`: Entire item after modification
- `OLD_IMAGE`: Entire item before modification
- `NEW_AND_OLD_IMAGES`: Both old and new item images

**Atomicity with data operations:**
- Stream records must be captured atomically with the data modification
- No stream record if operation fails
- No operation success without stream record

### PostgreSQL Reference Implementation

The PostgreSQL backend implements streams using:
- PostgreSQL SEQUENCE for monotonically increasing sequence numbers
- Multi-table transaction (data + stream records in single ACID transaction)
- Fixed 4 shards per stream with CRC32-based partition key hashing
- Stream records stored in data database (not catalog)
- Background worker for periodic TTL cleanup

### Cassandra Constraints

- **No SEQUENCE support**: Cannot use PostgreSQL's `nextval()` approach
- **LOGGED BATCH**: Provides atomicity across tables (suitable for data + stream writes)
- **Native TTL**: Per-row TTL on INSERT (more efficient than periodic DELETE)
- **Multi-node deployment**: Multiple ExtendDB nodes may write to same stream concurrently

## Proposal

Implement DynamoDB Streams on Cassandra using **Hybrid Logical Clocks (HLC) for sequence numbers** and **LOGGED BATCH for atomic writes** of data and stream records.

### Schema Design

**Stream shards table (in account keyspace):**

```cql
CREATE TABLE {account_keyspace}.stream_shards (
    shard_id text PRIMARY KEY,
    table_id text,
    parent_shard_id text,
    starting_sequence_number text,
    ending_sequence_number text,
    created_at timestamp
);

-- Index for queries by table_id (used during describe_stream)
CREATE INDEX ON stream_shards (table_id);
```

**Stream records table (in account keyspace):**

```cql
CREATE TABLE {account_keyspace}.stream_records (
    shard_id text,
    sequence_number text,
    table_id text,
    event_name text,              -- INSERT, MODIFY, REMOVE
    record_data text,             -- JSON-serialized StreamRecord
    created_at timestamp,
    PRIMARY KEY (shard_id, sequence_number)
) WITH CLUSTERING ORDER BY (sequence_number ASC);
```

**Rationale for account keyspace:**
- Enables atomic writes with item data (same keyspace, can use LOGGED BATCH)
- Stream records are account-specific data
- Account deletion drops entire keyspace (cascades streams automatically)
- Matches PostgreSQL pattern (data database, not catalog)

### Hybrid Logical Clock (HLC) for Sequence Numbers

**Problem:** Cassandra has no atomic sequence generator like PostgreSQL's SEQUENCE.

**Solution:** Use Hybrid Logical Clock pattern with node identifier:

**Format (23 digits, within DynamoDB's 40-char limit):**
```
{timestamp_ms:013}{counter:06}{node_id:04}
```

**Example:** Node 5 at timestamp 1719345600000, first write:
```
"17293456000000000010005"
 └─timestamp──┘││││││└─node─┘
               └─counter─┘
```

**Components (ordered by significance for lexicographic comparison):**
1. **Physical timestamp** (13 digits): Milliseconds since Unix epoch
   - Primary ordering - wall-clock time
   - Most significant segment
2. **Logical counter** (6 digits): Per-node, per-millisecond counter
   - Secondary ordering - handles concurrent writes in same millisecond
   - Resets when timestamp advances
   - Supports 999,999 operations/millisecond/node (overflow is theoretical)
3. **Node ID** (4 digits): ExtendDB server identifier
   - Tie-breaker only (effectively impossible to reach with 6-digit counter)
   - Derived from config or hostname hash
   - Range: 0001-9999

Note: The 21-digit format used by PostgreSQL is NOT a DynamoDB API requirement. DynamoDB allows sequence numbers up to 40 characters.

**Algorithm:**
```rust
struct HybridClock {
    node_id: u16,
    last_timestamp_ms: i64,
    logical_counter: u32,  // u32 to hold up to 999_999
}

fn generate_sequence_number(clock: &mut HybridClock) -> String {
    let now_ms = current_timestamp_ms();

    if now_ms > clock.last_timestamp_ms {
        clock.last_timestamp_ms = now_ms;
        clock.logical_counter = 0;
    } else {
        clock.logical_counter += 1;
        // Counter exhausted: wait for next millisecond.
        // Requires ~1B writes/sec/node — will never occur in practice.
        if clock.logical_counter > 999_999 {
            sleep(1ms);
            clock.last_timestamp_ms = current_timestamp_ms();
            clock.logical_counter = 0;
        }
    }

    // Format: timestamp (13) + counter (6) + node_id (4) = 23 digits
    format!("{:013}{:06}{:04}",
        clock.last_timestamp_ms,
        clock.logical_counter,
        clock.node_id)
}
```

**Why this works:**
- **Ordered:** Lexicographic string comparison matches temporal order
- **Unique:** Node ID prevents collisions across ExtendDB nodes
- **DynamoDB compatible:** 23-character numeric string, well within 40-char limit
- **No coordination:** Each node maintains independent in-memory state
- **Future-ready:** HLC pattern supports PITR and replication use cases

### Shard Assignment

**Fixed 4 shards per stream** (matches PostgreSQL and DynamoDB behavior):

**Shard ID format:**
```
shardId-{table_id}-{000000000000}  // 12-digit zero-padded index
```

**Assignment algorithm:**
```rust
fn assign_shard(partition_key: &str, table_id: &str) -> String {
    let hash = crc32fast::hash(partition_key.as_bytes());
    let shard_idx = (hash as usize) % 4;
    format!("shardId-{}-{:012}", table_id, shard_idx)
}
```

**Rationale:**
- CRC32 provides stable, deterministic hashing
- Same partition key always routes to same shard (consistent)
- Good distribution across shards
- Uses `table_id` (UUID), not `table_name` — prevents shard ID collision if a table is deleted and recreated with the same name (criterion 8.2)
- Matches PostgreSQL implementation

### Atomic Stream + Data Writes

**Use existing LOGGED BATCH pattern** (already used for data + index writes):

```rust
// Build batch statements
let mut batch_statements = Vec::new();

// 1. Data operation (UPDATE/INSERT/DELETE)
batch_statements.push(data_statement);

// 2. Index updates (if any)
sync_indexes(&mut batch_statements, ...);

// 3. Stream record (if streams enabled)
if let Some(stream_spec) = table.stream_specification {
    let sequence_number = hlc.generate_sequence_number();
    let shard_id = assign_shard(&partition_key, &table_id);
    let record = build_stream_record(old_item, new_item, &stream_spec);
    
    batch_statements.push(format!(
        "INSERT INTO {}.stream_records \
         (shard_id, sequence_number, table_id, event_name, record_data, created_at) \
         VALUES ('{}', '{}', '{}', '{}', '{}', {}) \
         USING TTL {}",
        account_keyspace, shard_id, sequence_number, table_id,
        event_name, record_json, now_ms, ttl_seconds
    ));
}

// Execute atomically
let batch_query = format!("BEGIN BATCH\n{}\nAPPLY BATCH", 
                          batch_statements.join(";\n"));
session.query(&batch_query).await?;
```

**Event type determination:**
- `old=None, new=Some` → INSERT
- `old=Some, new=Some` → MODIFY  
- `old=Some, new=None` → REMOVE
- `old=None, new=None` → No record (operation had no effect)

### TTL Strategy

**Use Cassandra native row-level TTL:**

**Default retention:** 30 hours (108,000 seconds)
- DynamoDB contract: "at least 24 hours" retention
- 6-hour buffer provides operational margin
- Protects against clock skew and brief instance downtime
- Still exceeds minimum guarantee

**Implementation:**
```cql
INSERT INTO stream_records (...) VALUES (...) 
USING TTL 108000;
```

**Benefits:**
- Automatic expiration (no background worker needed)
- More efficient than periodic DELETE queries
- Simpler than PostgreSQL implementation
- Per-row granularity

### OLD_IMAGE Capture

For `StreamViewType` of `OLD_IMAGE` or `NEW_AND_OLD_IMAGES`:

**Requirement:** Must read item before modification to capture old state

**Implementation:**
- Already reading for condition expression evaluation (common case)
- If no condition but OLD_IMAGE needed: explicit SELECT before write
- Read within same connection/session for consistency
- Acceptable performance trade-off for correctness

### Stream Initialization

**During CreateTable (if StreamSpecification present):**

1. Generate `stream_label` (ISO 8601 timestamp)
2. Update catalog: `tables.stream_label`, `tables.stream_specification`
3. Create 4 shard rows in account keyspace:
   ```cql
   INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number)
   VALUES ('shardId-{table_id}-{i:012}', '{table_id}', '000000000000000000000');
   ```

### OLD_IMAGE Capture

For `StreamViewType` of `OLD_IMAGE` or `NEW_AND_OLD_IMAGES`, the pre-mutation item state is required. This is already guaranteed: index maintenance requires a pre-image read on every write path that can affect indexed attributes. Stream record capture piggybacks on this existing read — no additional round-trip is needed in the common case.

### Stream APIs

**Important:** The `StreamEngine` trait's `write_stream_record` method cannot be used for stream record writes. The atomicity requirement (criterion 5.1) mandates that stream records be written in the same LOGGED BATCH as the data operation. A standalone trait method that executes its own query cannot participate in a caller's batch.

Instead, stream record writes are handled by an internal helper that returns a CQL statement to be appended to the batch being built by the write operation (the same pattern used for index maintenance). The `StreamEngine` trait is used only for the read-side APIs (`GetShardIterator`, `GetRecords`, `DescribeStream`, `ListStreams`) and control-plane operations (`init_stream_shards`, `disable_stream`).

**StreamEngine trait methods used:**

```rust
// Read records from shard with pagination
fn get_stream_records(
    shard_id: &str,
    after_sequence: Option<&str>,
    limit: i64,
) -> Result<(Vec<StreamRecord>, Option<String>)>;

// Describe stream metadata and shards
fn describe_stream(
    account_id: &str,
    input: &DescribeStreamInput,
) -> Result<StreamDescription>;

// List streams for account
fn list_streams(
    account_id: &str,
    table_name: Option<&str>,
    limit: i64,
    exclusive_start_stream_arn: Option<&str>,
) -> Result<(Vec<StreamSummary>, Option<String>)>;

// Assign shard for partition key
fn assign_shard(
    account_id: &str,
    table_name: &str,
    partition_key: &str,
) -> Result<String>;

// Generate next sequence number (HLC)
fn next_sequence_number(&self, shard_id: &str) -> Result<String>;
```

## Alternatives Considered

### Alternative 1: Simple Timestamp Sequences

**Approach:** Use microsecond timestamps without logical counter

**Rejected because:**
- Collision risk at high write rates (>1M ops/sec/node)
- No mechanism for tie-breaking across nodes
- Would require retry logic on Cassandra duplicate key errors
- Less robust for future replication/PITR use cases

### Alternative 2: Counter Table

**Approach:** Use Cassandra counter table for sequence generation

**Rejected because:**
- Cassandra counters not idempotent (retry doubles increment)
- Cannot read counter value within LOGGED BATCH
- Would require separate round-trip for each sequence number
- Performance impact on write path

## Consequences

### Positive

- **No schema changes to items tables** (unlike transactions)
- **Atomic writes** using existing LOGGED BATCH pattern
- **Collision-free sequences** with HLC + Node ID
- **Native TTL** simpler than PostgreSQL's worker approach
- **Future-ready** for PITR and replication features
- **Customer-friendly** TTL buffer exceeds minimum guarantee

### Negative

- **Node ID configuration required** for multi-node deployments
- **Microsecond precision** subject to clock quality (standard limitation)
- **OLD_IMAGE requires read-before-write** (performance cost, already done for conditions)
- **HLC state per node** (in-memory, lost on restart - acceptable)

### Neutral

- **Different from PostgreSQL** (HLC vs SEQUENCE) but equivalent semantics
- **21-digit sequences** may have gaps (DynamoDB doesn't prohibit this)
- **Cassandra-specific advantages** (native TTL) vs disadvantages (no SEQUENCE)

## Code Reuse: Refactoring Database-Independent Logic

The PostgreSQL implementation and this Cassandra proposal share significant database-independent logic that should be extracted into the `storage` crate:

### Functions to Extract

**1. Shard Assignment (`assign_shard_id`)**
```rust
/// Assign a shard ID based on partition key hash (CRC32).
pub fn assign_shard_id(pk_value: &str, shard_ids: &[String]) -> &str {
    let hash = crc32fast::hash(pk_value.as_bytes());
    let idx = (hash as usize) % shard_ids.len();
    &shard_ids[idx]
}
```
- Currently duplicated in both backends
- Identical CRC32 hashing logic
- Ensures consistent shard assignment across all backends

**2. Stream Record Construction (`build_stream_record`)**
```rust
/// Build a StreamRecord from operation context.
pub fn build_stream_record(
    event_type: StreamEventName,
    keys: BTreeMap<String, AttributeValue>,
    old_item: Option<Item>,
    new_item: Option<Item>,
    view_type: StreamViewType,
    region: &str,
    user_identity: Option<UserIdentity>,
    sequence_number: String,
) -> StreamRecord
```
- Determines event type from old/new state
- Filters images by view type
- Calculates size_bytes
- Assigns event_id (UUID v4)
- Pure data transformation with no I/O

**3. Shard ID Formatting (`format_shard_id`)**
```rust
/// Generate standard shard ID format.
pub fn format_shard_id(table_name: &str, shard_index: u32) -> String {
    format!("shardId-{table_name}-{shard_index:012}")
}
```
- Ensures consistent shard ID format across backends
- Used during shard initialization

### Rationale

**Unlike transactions** (where different backends need different *mechanisms* like ledgers vs native ACID), streams share identical *computations* but differ only in *storage primitives*:

- **Shared:** Hash computation, record construction, formatting
- **Backend-specific:** Sequence generation (SEQUENCE vs HLC), atomic writes (transactions vs BATCH), TTL (worker vs native)

**Benefits:**
- Eliminates 100+ lines of duplicated code per backend
- Ensures consistency (identical hashing, formatting, record structure)
- Simplifies testing (test once in storage crate)
- Zero performance cost (pure functions)
- Future backends get streams logic "for free"

**Backend-Specific Remaining:**
- Sequence number generation (trait method)
- Shard metadata queries (may need caching in Cassandra)
- Atomic write coordination (transactions vs BATCH)
- TTL implementation (background worker vs native)

### Implementation Priority

This refactoring should be done **as part of the Cassandra streams implementation**, not afterward:
1. Extract shared logic to storage crate first
2. Refactor PostgreSQL to use shared functions
3. Implement Cassandra streams using shared functions
4. Benefits both backends immediately

## Resolved Questions

1. **Node ID assignment strategy:** Derived as `crc32(format!("{}:{}", hostname, port)) % 9999 + 1`. Hostname alone is insufficient — multiple instances on the same host listening on different ports must have distinct node IDs. Hostname + port is stable across restarts, requires no configuration, and works correctly in containerized environments where the hostname is a pod/container ID and the port is the service binding. Hash collision (two instances mapping to the same node ID) is not a correctness problem — it only creates a theoretical ordering ambiguity that requires matching timestamp + counter on the same shard simultaneously.

2. **HLC state persistence:** Not needed. On restart, `now_ms` will always be greater than any previously generated timestamp (restarts take at least 1ms). Clock-going-backwards is handled by clamping to `last_timestamp_ms` and incrementing the counter, with a WARN log.

3. **TTL configuration:** Global setting (`stream_retention_hours`, default 30). Per-stream configuration adds complexity with no practical benefit — DynamoDB itself uses a fixed 24-hour retention.

4. **Clock skew handling:** Log a WARN if `now_ms < last_timestamp_ms`. Clamp to `last_timestamp_ms` and increment counter. No hard error — the sequence remains valid and ordered.

## References

- [DynamoDB Streams Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
- [DynamoDB Streams API Reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB_Streams.html)
- [PostgreSQL Reference Implementation](../../extenddb/crates/storage-postgres/src/stream_engine.rs)
- [Hybrid Logical Clocks Paper](https://cse.buffalo.edu/tech-reports/2014-04.pdf) (Kulkarni et al.)
