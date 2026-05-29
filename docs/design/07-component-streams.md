# extenddb — Component Design: DynamoDB Streams

**Version:** 1.0
**Date:** 2026-04-03
**Status:** Draft — High-Level Design Choices Only (Detailed Design Deferred)

## 1. Purpose

DynamoDB Streams provides change data capture (CDC) for DynamoDB tables. When enabled on a table, every write (PutItem, UpdateItem, DeleteItem, BatchWriteItem, TransactWriteItems) generates a stream record containing the item's key and optionally the old/new images.

This document outlines the design space and recommended direction. Detailed design decisions (shard management, retention, iterator semantics) are deferred until the core data plane is stable.

## 2. API Surface

Four operations, served on the same HTTP endpoint as DynamoDB operations:

| Operation | Description |
|-----------|-------------|
| `DescribeStream` | Return stream ARN, status, shard list with parent/child relationships |
| `ListStreams` | List streams, optionally filtered by table name |
| `GetShardIterator` | Get an iterator for a shard (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER) |
| `GetRecords` | Read up to 1000 records (or 1 MB) from a shard using an iterator |

**Note:** In real DynamoDB, Streams is a separate service with its own endpoint (`streams.dynamodb.<region>.amazonaws.com`) and its own SDK client (`boto3.client('dynamodbstreams')` in Python, `DynamoDbStreamsClient` in Java). extenddb serves both services on a single port. This works because SDK clients accept `endpoint_url` overrides — the user sets both `AWS_ENDPOINT_URL_DYNAMODB` and `AWS_ENDPOINT_URL_DYNAMODB_STREAMS` to the same extenddb address. The `X-Amz-Target` prefix distinguishes the two services: DynamoDB operations use `DynamoDB_20120810.<Op>`, Streams operations use `DynamoDBStreams_20120810.<Op>`. Both use `dynamodb` as the SigV4 signing name, so authentication is identical. See `01-requirements.md` §11 and `08-component-config.md` §10 for SDK configuration details.

## 3. Stream Record Format

```rust
pub struct StreamRecord {
    pub event_id: String,
    pub event_name: StreamEventName,  // INSERT, MODIFY, REMOVE
    pub event_version: String,        // "1.1"
    pub event_source: String,         // "aws:dynamodb"
    pub aws_region: String,
    pub dynamodb: StreamRecordData,
}

pub struct StreamRecordData {
    pub approximate_creation_date_time: i64,  // epoch seconds
    pub keys: BTreeMap<String, AttributeValue>,
    pub new_image: Option<BTreeMap<String, AttributeValue>>,
    pub old_image: Option<BTreeMap<String, AttributeValue>>,
    pub sequence_number: String,
    pub size_bytes: i64,
    pub stream_view_type: StreamViewType,
}

pub enum StreamEventName { Insert, Modify, Remove }
pub enum StreamViewType { KeysOnly, NewImage, OldImage, NewAndOldImages }
```

## 4. Design Space

### 4.1 Capture Mechanism

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A: Application-layer** | Core operation handlers call `storage.write_stream_record()` after successful writes | Portable across all backends; consistent behavior | Slight overhead per write; must handle failure (stream write fails after data write) |
| **B: Storage-layer triggers** | PostgreSQL triggers / CDC (e.g., logical replication) | Zero application overhead; native to backend | Backend-specific; different behavior per backend; harder to control stream view type |
| **C: Hybrid** | Application layer captures the record, storage layer persists it in the same transaction | Atomic with the data write; portable capture logic | Requires the storage trait to support "write data + stream record in one transaction" |

**Recommended: Option C (Hybrid)**
The core handler constructs the stream record (it has access to old/new images). The storage engine persists the stream record in the same transaction as the data write. This ensures atomicity (no stream record without a data write, no data write without a stream record) while keeping the capture logic portable.

### 4.2 Shard Management

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A: Fixed shards** | One shard per table, never splits | Simple; predictable | Doesn't scale; single reader bottleneck |
| **B: Hash-based shards** | Fixed number of shards per table (e.g., 4), records assigned by partition key hash | Parallel reads; simple assignment | Fixed parallelism; no dynamic scaling |
| **C: Dynamic splitting** | Shards split when throughput exceeds threshold, creating parent/child relationships | Matches DynamoDB behavior; scales | Complex; requires shard lineage tracking |

**Recommended: Option B initially, migrate to C later**
Start with a configurable fixed number of shards per table (default: 4). Records are assigned to shards by hashing the partition key. This supports parallel consumers without the complexity of dynamic splitting. The shard iterator API is designed to support parent/child relationships from day one, so migrating to dynamic splitting later is backward-compatible.

### 4.3 Record Storage

| Option | Description |
|--------|-------------|
| **A: Same database** | Stream records stored in the storage backend (e.g., `_dynamodb_stream_records` table in PostgreSQL) |
| **B: Separate store** | Stream records in a dedicated system (e.g., Kafka, Redis Streams) |

**Recommended: Option A**
Store stream records in the same database as the data. This allows atomic writes (data + stream record in one transaction) and avoids introducing additional infrastructure dependencies. The `StorageEngine` trait already includes stream record methods.

### 4.4 Retention & Cleanup

DynamoDB retains stream records for 24 hours. Options:
- **Background worker**: Periodically delete records older than the retention period
- **TTL on the storage side**: Backend-native TTL where available, or an application-level scheduled task

**Recommended:** Application-level background task (same pattern as TTL cleanup), configurable retention period (default: 24 hours).

## 5. Integration Points

### 5.1 Write Path Integration

```
PutItem/DeleteItem handler:
  1. Construct full StreamRecord (engine knows old/new images for Put and Delete)
  2. Call storage.put_item(..., stream_record) or storage.delete_item(..., stream_record)
     - Storage executes both in a single transaction
  3. Return response

UpdateItem handler:
  1. Construct StreamCapture metadata (stream ARN, view type, shard, sequence, keys)
     — engine does NOT know the new_image yet (it depends on apply_update inside the tx)
  2. Call storage.update_item(..., stream_capture)
     - Storage: SELECT FOR UPDATE, apply_update, then construct full StreamRecord
       with old_image/new_image based on stream_view_type, all in one transaction
  3. Return response
```

The `StorageEngine` trait supports two patterns for atomic stream writes:

- **PutItem/DeleteItem:** The engine pre-constructs the full `StreamRecord` and passes it via the `stream_record` field. The storage backend simply INSERTs it in the same transaction.
- **UpdateItem:** The engine passes a `StreamCapture` struct with metadata. The storage backend constructs the full `StreamRecord` after `apply_update` produces the `new_image`, then INSERTs it in the same transaction.

```rust
// PutItem/DeleteItem: engine pre-constructs the full record
pub struct PutItemInput {
    // ...
    pub stream_record: Option<StreamRecord>,  // None if streams not enabled
}

// UpdateItem: engine passes metadata, storage constructs the record
pub struct UpdateItemInput {
    // ...
    pub stream_capture: Option<StreamCapture>,  // None if streams not enabled
}

/// Metadata for stream record construction inside the storage transaction.
pub struct StreamCapture {
    pub stream_arn: String,
    pub stream_view_type: StreamViewType,
    pub shard_id: String,
    pub sequence_number: String,
    pub event_name: StreamEventName,
    pub keys: BTreeMap<String, AttributeValue>,
}
```

### 5.2 Read Path (GetRecords)

```
GetRecords handler:
  1. Validate shard iterator
  2. Call storage.get_stream_records(shard_id, after_sequence, limit)
  3. Format response with records + next shard iterator
```

## 6. Deferred Decisions

| Decision | Status | Notes |
|----------|--------|-------|
| Dynamic shard splitting algorithm | Deferred | Start with fixed shards |
| Shard iterator expiration (DynamoDB: 15 min) | Deferred | Implement after basic flow works |
| Cross-instance stream consistency | Deferred | Relevant for multi-instance deployments |
| Stream record deduplication | Deferred | Relevant for at-least-once delivery guarantees |
| Kinesis adapter compatibility | Deferred | DynamoDB Streams has a Kinesis-compatible adapter |
| Stream enable/disable lifecycle | Deferred | What happens to in-flight records when streams are disabled |

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
