# Differences from DynamoDB

This document lists all known behavioral differences between ExtendDB and real
Amazon DynamoDB. Use it to understand what works identically and what requires
adaptation when switching between ExtendDB and the real service.

## Storage and Infrastructure

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Storage backend | Proprietary distributed storage | PostgreSQL (default), or the optional `dynamodb` backend (see below) |
| Global Tables | CreateGlobalTable, replication | Not implemented (returns UnknownOperationException) |
| DAX (Accelerator) | In-memory caching layer | Not applicable |
| PartiQL | ExecuteStatement, BatchExecuteStatement | Not implemented (returns UnknownOperationException) |

## The `dynamodb` Storage Backend ("DynamoDB at home")

ExtendDB ships an optional `dynamodb` storage backend (build with
`--features dynamodb`, select with `[storage] backend = "dynamodb"`). It forwards
the **data plane** to a real DynamoDB endpoint via the AWS SDK, while the
**catalog/IAM/auth plane** is delegated to the PostgreSQL backend. Run it yourself,
point it at DynamoDB, and you are technically self-hosted — the entire point.

| Area | Behavior with `backend = "dynamodb"` |
|------|--------------------------------------|
| Data plane (PutItem, GetItem, UpdateItem, DeleteItem, Query, Scan, transactions) | Forwarded to real DynamoDB |
| Catalog / IAM / settings / metrics / rate-limits | Stored in PostgreSQL (`catalog_connection_string`) |
| Table namespacing | ExtendDB is multi-tenant; physical DynamoDB tables are named `<table_prefix><account_id>_<table>` (default prefix `athome_`) |
| TTL | Native DynamoDB TTL (`UpdateTimeToLive`); ExtendDB's TTL worker is a no-op |
| Tags | Forwarded to DynamoDB `TagResource`/`UntagResource`/`ListTagsOfResource` (table ARNs only) |
| Idempotency tokens | Forwarded as `ClientRequestToken`; DynamoDB manages its own ~10-minute window |
| Streams | **Not implemented in v1** — each method errors naming the DynamoDB Streams call it maps to |
| Backups / PITR | **Not implemented in v1** — each method errors naming the DynamoDB Backup call it maps to |
| `UpdateItem` returning both old and new | DynamoDB returns only one per call; the backend prefers `ALL_NEW` when both are requested |
| `update_table` GSI mutations | Not forwarded in v1 (table create/describe/delete and billing changes are) |
| `index_info_by_table_id` | Not supported (DynamoDB has no TableId→name reverse lookup) |
| `endpoint_url` | May point at DynamoDB Local — or at **another ExtendDB endpoint**, which is documented as a feature, not a bug (the near-identity encoding makes the stack composable) |

Note that this backend still requires PostgreSQL for the catalog: your data is
on-prem, but the bureaucracy that proves you're on-prem still needs a real
database.

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
| GSI update propagation | Eventually consistent (milliseconds to seconds) | Per-GSI propagation delay. System default: `gsi_propagation_delay_ms` setting (default 10ms). Each GSI can override with its own `propagation_delay_ms` (stored in catalog). A value of 0 means synchronous (future sync GSI feature). |
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
| `gsi_propagation_delay_ms` | 10 | System-wide default GSI propagation delay (milliseconds). Per-GSI overrides stored in catalog. 0 = synchronous. |
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
