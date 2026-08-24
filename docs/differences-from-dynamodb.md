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
| Vector index update propagation | Eventually consistent, the same model as a GSI | Matches DynamoDB. Maintenance is queued on the same propagation queue as async GSIs, so a search immediately after a write may not see it. Governed by the same `index_propagation_delay_ms` setting; unlike a GSI there is no per-index override. A value of 0 applies maintenance synchronously in the write's own transaction, which is stricter than the service and exists so a test can assert steady state without waiting. |
| `SearchVectors` score at extreme magnitudes (PostgreSQL backend only) | Returns the true distance as a number for any vector of finite components | The score is bounded to a finite value instead, and the bound is not a measured service answer. pgvector accumulates distances in single precision, so magnitudes far below `f32::MAX` overflow inside the extension: Euclidean above about 9.2e18, dot product above about 1.8e19, and cosine at both ends, above about 1.8e19 and below about 3.7e-23. A non-finite score cannot be serialised as JSON at all, so the result is bounded in SQL: `1e308` for an overflowed distance, `-1e308` for an overflowed negated inner product, which the score contract negates so a client sees `1e308` in `Score`, and 1.0 for a cosine that comes back NaN. Ranking is unaffected at the overflow end, because each bound sits at the end its metric overflows towards, so the farthest row stays farthest and the most similar stays most similar; two rows that both overflow tie, and the tie breaks on the base key. At the underflow end cosine loses resolution rather than being bounded: pgvector's underflowed norms yield infinities that it clamps before the value is read, so the reported distance collapses to one of 0, 1 or 2, following the sign of the inner product, with the 1.0 substitute firing only when the vectors are exactly orthogonal (measured). Ranking there still separates nearer-than-orthogonal from farther, and loses all resolution within each half. The SQLite backend owns its own arithmetic, computes in double precision, and reports the true value, which is why this row is scoped to PostgreSQL. |
| Vector index deletion window | `UpdateTable` Delete leaves the index in `DELETING` long enough to observe, then removes it | No observable `DELETING` window on either backend: the catalog row is removed inside the `UpdateTable` transaction, so a `DescribeTable` immediately afterwards already omits the index. The index's data table is dropped after that commit, in a separate transaction, best effort: on PostgreSQL it is a different database entirely, and a failure there is logged and skipped rather than failing the request. So an operator debugging a leftover `_ddb_vec_*` table should look for that warning rather than assume the delete was incomplete. |
| Restoring a backup of a table that had vector indexes | Restores the table with its vector indexes intact: the configuration survives, items keep their vector attributes, and `SearchVectors` works as soon as the table is `ACTIVE` (measured) | Neither backend restores the indexes, and the two fail differently. **PostgreSQL refuses the restore** with a `ValidationException` naming the backup and the index count, because restore does not carry index data across and a table that looks restored while answering every search with nothing is worse than a refusal a caller can act on. **SQLite does not refuse**: its backup path does not capture vector indexes at all, so a restore silently produces the table without them. That silence is tracked as a defect rather than intended, and it is the reason the PostgreSQL path refuses instead of matching it. A backup taken from a table with no vector indexes restores normally on both. |
| `SearchVectors` endpoint | Served only on `search-dynamodb.<region>.amazonaws.com`. The standard `dynamodb.<region>.amazonaws.com` endpoint answers the same request with HTTP 400 `UnknownOperationException` ("This operation is not supported by this endpoint"); every control-plane and item operation stays on the standard endpoint. Signing is unchanged either way (service name `dynamodb`, target prefix `DynamoDB_20120810`) | Served on the same endpoint as every other operation, so a client is pointed at one ExtendDB endpoint for all of them. Two consequences worth knowing: an SDK that resolves a separate search hostname from its endpoint ruleset needs its endpoint overridden to reach ExtendDB, and ExtendDB does **not** reproduce the service's refusal, so a test asserting `UnknownOperationException` for vector search on the base endpoint passes against Amazon DynamoDB and fails here. |
| `SearchVectors` result order for equal distances | Measured unstable: three identical searches returned tied rows in three different orders, and a top-k that truncates a tie group keeps an arbitrary subset of it | Deterministic on both backends, by different means. **PostgreSQL** sorts explicitly on the base table's full primary key after the score, so the order is a property of the query. **SQLite** issues no `ORDER BY` and resolves ties by scan order through a stable top-k, so its order is a property of the plan rather than something the query guarantees. Either way a client re-issuing an identical search sees the same order, which is stricter than the service rather than divergent in outcome. Do not rely on the two backends agreeing on which subset of a truncated tie group they keep. |
| Multi-part base table keys | Not supported | Preview extension (opt-in via `enable_multipart_keys` setting). Standard single/composite keys work identically. |

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
