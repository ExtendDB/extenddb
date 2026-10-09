# Differences from DynamoDB

This document lists all known behavioral differences between ExtendDB and real
Amazon DynamoDB. Use it to understand what works identically and what requires
adaptation when switching between ExtendDB and the real service.

## Storage and Infrastructure

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Storage backend | Proprietary distributed storage | PostgreSQL (default), SQLite, or MongoDB, selected by mutually exclusive Cargo features at build time; one backend per binary |
| Global Tables | CreateGlobalTable, replication | Not implemented (returns UnknownOperationException) |
| DAX (Accelerator) | In-memory caching layer | Not applicable |
| PartiQL | ExecuteStatement, BatchExecuteStatement | Not implemented (returns UnknownOperationException) |
| Numeric precision on partition/sort keys (MongoDB backend only) | 38 significant digits | 34 significant digits (BSON Decimal128). Values that exceed this precision are rejected at write and query time with a ValidationException rather than silently downcast. PostgreSQL backend supports the full 38 digits. |
| Inverted numeric `BETWEEN` on a sort key (MongoDB backend only) | ValidationException ("The BETWEEN operator requires upper bound to be greater than or equal to lower bound") | Same error in all practical cases. The inversion guard compares bounds via `f64`, so a `KeyConditionExpression` `BETWEEN` whose bounds are inverted only beyond f64's ~15–17 significant digits (e.g. `BETWEEN 10000000000000002 AND 10000000000000001`) is not rejected and returns an empty result set instead. Valid ranges are never wrongly rejected. |
| `TransactGetItems` overlapping an in-flight `TransactWriteItems` on the same item | May cancel the read with `TransactionCanceledException` and reason `TransactionConflict` | Never cancels for this reason on PostgreSQL: the read runs in one `REPEATABLE READ` snapshot taken at its first read, so it sees either all of the write or none of it and returns the pre-write state while the write is in flight. That is a legal serial order (the read ordered before the write) and the all-or-nothing guarantee holds, but a client that retries on `TransactionConflict` to observe the newest state will not get that signal here. SQLite behaves the same (one WAL snapshot); MongoDB with the default `snapshot` read concern likewise, and the row above covers the other read concerns. |
| Transaction read concern (MongoDB backend only) | No user-configurable equivalent | `snapshot` is the default, fidelity-preserving mode. With `majority` or `local`, `TransactGetItems` is not guaranteed a single point-in-time snapshot and transaction condition reads have weaker isolation. With `local`, a condition can be evaluated against data that is later rolled back after failover. |
| Contended `TransactWriteItems` | A transaction that conflicts with another in-flight transaction on the same item is canceled with a `TransactionConflict` reason ("Transaction is ongoing for the item") | **PostgreSQL** queues the second transaction on the first one's row locks and **SQLite** runs one writer at a time, so both normally commit and no `TransactionConflict` is returned. PostgreSQL locks each transaction's items in table and key order, so two `TransactWriteItems` cannot deadlock on each other. A PutItem, UpdateItem, DeleteItem or BatchWriteItem of an item that a `TransactWriteItems` has locked also waits for it, where the Amazon DynamoDB developer guide lists that request as a conflict. On PostgreSQL this includes an item that the transaction checks or deletes while it is missing: the transaction holds the key until it commits or rolls back, so a request that creates the item waits. Many transactions that check one hot missing item queue less fairly than on an existing item, because every waiter wakes at each commit and only one proceeds. Each such check or delete of a missing item also inserts and deletes its key, so it writes WAL and leaves a dead row and index entry for autovacuum, even though it changes no item. When PostgreSQL still aborts a transaction (a deadlock with another lock holder, or a serialization failure under a stricter `default_transaction_isolation`), the answer is a `TransactionCanceledException` with a `TransactionConflict` reason, not a 500. **MongoDB** retries write conflicts internally and cancels with `TransactionConflict` on every item only after sustained contention. A client that relies on `TransactionConflict` to back off does not see it under ordinary contention. |

## Authentication and Authorization (AWS IAM/STS auth surface used by DynamoDB)

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Credential management | AWS IAM console/API | `extenddb manage` CLI and `/management` REST API |
| Access key prefixes | `AKIA` (long-term), `ASIA` (session) AWS-wide IAM/STS conventions | `AKIAEXTENDDB` (long-term), `ASIAEXTENDDB` (session) |
| Federated roles | AssumeRoleWithSAML, AssumeRoleWithWebIdentity | Not implemented |
| Role chaining | Supported | Not implemented |
| SourceIdentity, TransitiveTagKeys | Supported | Not implemented |
| Resource policies | Supported | Not implemented (deferred) |

## Import and Export

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Import source | S3BucketSource (S3 bucket) | FileSource (local filesystem path) |
| Export destination | S3 bucket | Local filesystem path |
| Import formats | CSV, DYNAMODB_JSON, ION | CSV, DYNAMODB_JSON, ION |
| Export formats | DYNAMODB_JSON, ION | DYNAMODB_JSON, ION |
| Import execution | Asynchronous (background job) | Synchronous (completes before returning) |
| Export execution | Point-in-time snapshot | Current snapshot, synchronous |

## Control Plane

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Table creation delay | Returns `CREATING` immediately; transitions to `ACTIVE` typically within seconds. Same behavior for on-demand and provisioned | Configurable via `control_plane_delay_seconds` runtime setting (default: 5s) |
| DeletionProtectionEnabled | Enforced | Enforced (accepted and stored, DeleteTable rejects when enabled) |

## Time to Live (TTL)

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| TTL attribute name | Any UTF-8 string (1–255 bytes) | Restricted to `[a-zA-Z0-9._-]+` (1–255 bytes). Names with spaces, quotes, or other special characters are rejected. This eliminates SQL injection risk in the TTL expression index. |
| TTL deletion | Background process, items deleted within 48 hours of expiry | Background worker with indexed sweep, configurable target via `ttl_deletion_target_seconds` (default: 300s) |
| TTL stream records | REMOVE events with `userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}` | Supported — TTL deletions generate REMOVE stream records with the same `userIdentity` |
| TTL modification cooldown | Enforces a cooldown period between enable/disable changes ("Time to live has been modified multiple times within a fixed interval") | No cooldown — TTL can be enabled and disabled immediately. Intentional divergence for faster local development. |

## Tagging

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| TagResource / UntagResource | Validates resource ARN exists, returns `ResourceNotFoundException` for missing tables | Matches DynamoDB — validates resource ARN and returns `ResourceNotFoundException` for missing tables. |

## Secondary Indexes

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| GSI update propagation | Eventually consistent (milliseconds to seconds) | Per-GSI propagation delay. System default: `index_propagation_delay_ms` setting (default 10ms). Each GSI can override with its own `propagation_delay_ms` (stored in catalog). A value of 0 means synchronous (future sync GSI feature). |
| Vector index update propagation | Eventually consistent, the same model as a GSI | Matches DynamoDB. Maintenance is queued on the same propagation queue as async GSIs, so a search immediately after a write may not see it. Governed by the same `index_propagation_delay_ms` setting; unlike a GSI there is no per-index override. A value of 0 applies maintenance inline in the write's own transaction, which is stricter than the service and exists so a test can assert steady state without waiting. Inline additionally requires the table's propagation queue to be empty: while rows queued during an index build are still draining, a new write queues behind them instead of overtaking them, so a brief search-after-write window exists even at 0 until the queue drains. That window is ordering, not data loss; the write is applied, in order, by the queue worker. That zero behaves differently while an index is still building, and the two backends differ: **PostgreSQL** applies inline only to an index that is already `ACTIVE`, and defers a write to a building index whatever the delay says, because the write must not reach the index ahead of the backfill's older snapshot of the same item; **SQLite** does not check the status on this path, so with a zero delay a write during a backfill is applied inline and bypasses the hold that keeps the two ordered. The SQLite behaviour is tracked as a defect (F-20), not intended, and it is reachable only with the zero delay, which is a test setting. |
| `SearchVectors` score at extreme magnitudes (PostgreSQL backend only) | Returns the true distance as a number for any vector of finite components | The score is bounded to a finite value instead, and the bound is not a measured service answer. pgvector accumulates distances in single precision, so magnitudes far below `f32::MAX` overflow inside the extension: Euclidean above about 9.2e18, dot product above about 1.8e19, and cosine at both ends, above about 1.8e19 and below about 3.7e-23, which is where a component's single-precision square rounds to zero. A non-finite score cannot be serialised as JSON at all, so the result is bounded in SQL: `1e308` for an overflowed distance, `-1e308` for an overflowed negated inner product, which the score contract negates so a client sees `1e308` in `Score`, and 1.0 for a cosine that comes back NaN. Ranking is unaffected at the overflow end, because each bound sits at the end its metric overflows towards, so the farthest row stays farthest and the most similar stays most similar; two rows that both overflow tie, and the tie breaks on the base key. At the underflow end cosine loses resolution rather than being bounded, and which side is tiny decides how much. For a tiny **query** vector, its norm underflows while the inner product usually does not, so the quotient is an infinity that pgvector clamps and the reported distance collapses to one of 0, 1 or 2 following the sign of the inner product, with the 1.0 substitute firing only when the vectors are exactly orthogonal and the quotient is therefore 0/0 (measured). For a tiny **stored** vector it is worse: the stored norm is computed in single precision and reaches zero, so the zero-vector guard fires and that row reports 1.0 at every angle, parallel included. A corpus of tiny embeddings therefore loses ranking altogether rather than losing resolution. For a tiny query vector, by contrast, ranking still separates nearer-than-orthogonal from farther and loses resolution only within each half. The SQLite backend owns its own arithmetic, computes in double precision, and reports the true value, which is why this row is scoped to PostgreSQL. |
| Vector index deletion window | `UpdateTable` Delete leaves the index in `DELETING` long enough to observe, then removes it | No observable `DELETING` window on either backend: the catalog row is removed inside the `UpdateTable` transaction, so a `DescribeTable` immediately afterwards already omits the index. The index's data table is dropped after that commit, in a separate transaction, and the two backends handle a failure there differently. **PostgreSQL** treats it as best effort, because the data table lives in a different database entirely: a failure is logged and skipped rather than failing the request. **SQLite** propagates it, so a failed drop returns an error from an `UpdateTable` whose catalog change has already committed, which is the more surprising outcome of the two: the index is gone from the catalog and the caller saw a failure. So an operator debugging a leftover `_ddb_vec_*` table should look for that warning rather than assume the delete was incomplete. |
| Restoring a backup of a table that had vector indexes | Restores the table with its vector indexes intact: the configuration survives, items keep their vector attributes, and `SearchVectors` works as soon as the table is `ACTIVE` (measured) | Neither backend restores the indexes, and both refuse the restore with a `ValidationException` naming the backup and the index count, because restore does not carry index data across and a table that looks restored while answering every search with nothing is worse than a refusal a caller can act on. A backup taken from a table with no vector indexes restores normally on both. |
| `SearchVectors` endpoint | Served only on `search-dynamodb.<region>.amazonaws.com`. The standard `dynamodb.<region>.amazonaws.com` endpoint answers the same request with HTTP 400 `UnknownOperationException` ("This operation is not supported by this endpoint"); every control-plane and item operation stays on the standard endpoint. Signing is unchanged either way (service name `dynamodb`, target prefix `DynamoDB_20120810`) | Served on the same endpoint as every other operation, so a client is pointed at one ExtendDB endpoint for all of them. Two consequences worth knowing: an SDK that resolves a separate search hostname from its endpoint ruleset needs its endpoint overridden to reach ExtendDB, and ExtendDB does **not** reproduce the service's refusal, so a test asserting `UnknownOperationException` for vector search on the base endpoint passes against Amazon DynamoDB and fails here. |
| `SearchVectors` result order for equal distances | Measured unstable: three identical searches returned tied rows in three different orders, and a top-k that truncates a tie group keeps an arbitrary subset of it | Deterministic on both backends, by different means. **PostgreSQL** sorts explicitly on the base table's full primary key after the score, so the order is a property of the query. **SQLite** issues no `ORDER BY` and resolves ties by scan order through a stable top-k, so its order is a property of the plan rather than something the query guarantees. Either way a client re-issuing an identical search sees the same order, which is stricter than the service rather than divergent in outcome. Do not rely on the two backends agreeing on which subset of a truncated tie group they keep. |
| Vector indexes on the MongoDB backend | Vector indexes and `SearchVectors` are available | Not supported at all. That backend provides no vector search implementation, so the engine's capability gate refuses `CreateTable` and `UpdateTable` carrying `VectorIndexes` and every `SearchVectors` request, with the same capability message a PostgreSQL deployment without pgvector returns. Every other vector row in this document describes the PostgreSQL and SQLite backends. |
| Multi-part base table keys | Not supported | Preview extension (opt-in via `enable_multipart_keys` setting). Standard single/composite keys work identically. |

## Backup and Restore

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| What a restore carries across | Key schema, attribute definitions, items, secondary indexes, billing mode and provisioned throughput, SSE settings. Stream settings, TTL, tags, auto scaling, IAM policies, and CloudWatch settings must be reapplied by hand | The same set on PostgreSQL, SQLite, and MongoDB, plus table class and on-demand throughput. Streams, TTL, tags, and deletion protection are not restored. A backup written by a version before catalog 0.0.4 carries no table definition and restores as keys and items only, with 5/5 provisioned throughput |
| `RestoreTableFromBackup` overrides | Current request members are `BillingModeOverride`, `ProvisionedThroughputOverride`, `GlobalSecondaryIndexOverride`, `LocalSecondaryIndexOverride`, `SSESpecificationOverride`, `OnDemandThroughputOverride`, and `VectorIndexOverride`; DynamoDB applies them to the restored table | PostgreSQL, SQLite, and MongoDB support `BillingModeOverride` and `ProvisionedThroughputOverride`. Switching to `PAY_PER_REQUEST` drops table and GSI throughput. `GlobalSecondaryIndexOverride`, `LocalSecondaryIndexOverride`, `SSESpecificationOverride`, `OnDemandThroughputOverride`, and `VectorIndexOverride` are refused with `ValidationException` before the target table is created. Switching an on-demand backup with GSIs to `PROVISIONED` is likewise refused because it requires `GlobalSecondaryIndexOverride` |
| Multi-part base table keys (preview) | Not supported | A backup of a table created with `enable_multipart_keys` is refused at restore with `ValidationException`; the item paths address such tables by their first HASH and RANGE attribute only |
| `DeleteBackup` during a restore from that backup | `BackupInUseException` | `BackupInUseException` (HTTP 400) until the restore completes or fails, on all three backends. PostgreSQL and SQLite register the restore in the same transaction that creates its target, with the backup row held so a concurrent `DeleteBackup` orders strictly before (the restore then sees no backup) or after (it sees the restore). MongoDB has no cross-collection transaction and uses a claim marker on the backup's metadata instead: a restore claims the backup before reading it and releases the claim once its `CREATING` target exists; `DeleteBackup` claims it the same way before dropping data. A claim left by a crashed process is cleared at the next startup, so a backup is never stuck undeletable; until then a restore or delete of that backup gets `BackupInUseException` |
| `DeleteTable` on a table being restored | `ResourceInUseException` | PostgreSQL, SQLite, and MongoDB match on a restore target. On all three backends an ordinary table in `CREATING` can still be deleted during its control-plane delay, where the service refuses |
| `RestoreSummary` lifecycle | The immediate restore response reports `RestoreInProgress=true`; later `DescribeTable` calls report `false` after completion and retain the same `RestoreDateTime` | PostgreSQL, SQLite, and MongoDB do the same. The engine deliberately forces `RestoreInProgress=true` on the immediate response even when a synchronous backend has already finished copying; the backend's recorded `RestoreDateTime` is reused by later `DescribeTable` calls |
| Restore interrupted by a server crash | Managed by the service | PostgreSQL removes an abandoned `CREATING` restore target on a control-plane pass; SQLite removes one when the next server starts. MongoDB has no abandoned-restore sweep: the target remains `CREATING`, the API continues to refuse `DeleteTable`, and an operator must remove its catalog and data collections by hand |
| Point-in-time recovery | 35-day window | Not supported; `UpdateContinuousBackups` and `RestoreTableToPointInTime` are refused |
| Where backups live | Managed storage, independent of the table | Inside the database the server already uses, so a lost database loses its backups too: on PostgreSQL in the catalog database (`backups`, `backup_items`, `backup_definitions`, `table_restores`), on SQLite in the one database file, on MongoDB as a `backups` metadata collection in the catalog database and one data collection per backup in the data database. Take database-level backups for off-host copies until the backup store program ships |
| Backup creation status | `CreateBackup` returns while the backup is `CREATING`; `ListBackups` and `DescribeBackup` expose that state until it becomes `AVAILABLE` | SQLite similarly exposes `CREATING` while it copies: size and item count remain zero, `DeleteBackup` is refused, and `RestoreTableFromBackup` reports the backup missing. PostgreSQL and MongoDB complete their copy before returning and expose only `AVAILABLE` |
| Consistency of a backup | Point-in-time snapshot of the table | PostgreSQL holds the table's catalog row shared from the first definition read until the data snapshot (`REPEATABLE READ`, pinned by a read of the table before the row is released) is taken, so the definition recorded is the one in force for the items read, and the items are from one snapshot; the catalog and the data are separate databases, so this is a barrier, not one cross-database snapshot. SQLite reads all items in one deferred read transaction opened while the engine's write lock was held for the definition read, so definition and items agree and writers continue during the copy; that long reader pins WAL checkpointing, so the WAL grows for the duration of the backup by the size of the backup rows written plus any concurrent writes. MongoDB copies with `$out` from the live collection and is not a snapshot |

## Capacity and Throttling

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Provisioned throughput | Token bucket per table/partition | Token bucket per table/partition, matching DynamoDB's burst and refill behavior |
| On-demand capacity | Automatic scaling | Fixed initial burst capacity (4000 WCU / 12000 RCU), no auto-scaling |
| Throttling | Always on; throttles requests that exceed provisioned/burst capacity. No setting to disable | Configurable via `throttling_enabled` runtime setting (default: `true`) |

## Operations Not Implemented

The following operations return `UnknownOperationException`:

- CreateGlobalTable, DescribeGlobalTable, ListGlobalTables, UpdateGlobalTable
- DescribeGlobalTableSettings, UpdateGlobalTableSettings
- ExecuteStatement, BatchExecuteStatement, ExecuteTransaction
- DescribeContributorInsights, UpdateContributorInsights
- DescribeKinesisStreamingDestination, EnableKinesisStreamingDestination, DisableKinesisStreamingDestination
- DescribeTableReplicaAutoScaling, UpdateTableReplicaAutoScaling

## Runtime Configuration

ExtendDB exposes runtime settings that have no DynamoDB equivalent:

| Setting | Default | Description |
|---------|---------|-------------|
| `control_plane_delay_seconds` | 5 | Simulated delay for table state transitions (CREATING → ACTIVE, DELETING → removed) |
| `index_propagation_delay_ms` | 10 | System-wide default propagation delay for asynchronous secondary-index maintenance (milliseconds), covering GSIs and vector indexes alike. Per-GSI overrides stored in catalog; vector indexes have no per-index override. 0 = synchronous. Accepts the pre-rename name `gsi_propagation_delay_ms` as a deprecated alias, and a catalog created before the rename keeps honouring a value stored under it. |
| `throttling_enabled` | `true` | Enable provisioned capacity throttling (token bucket per table/partition) |
| `enable_multipart_keys` | `false` | Enable multi-part base table key extension |
| `log_level` | `info` | Runtime log level (trace, debug, info, warn, error) |
| `sqlx_log_level` | `warn` | Separate log level for sqlx query traces |
| `allow_credential_import` | `true` | Allow importing credentials via the management API |

## Web Console

ExtendDB includes a built-in web management console at `/console` for credential
and account management. DynamoDB uses the AWS Management Console.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
