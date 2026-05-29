# Differences from DynamoDB

This document lists all known behavioral differences between ExtendDB and real
Amazon DynamoDB. Use it to understand what works identically and what requires
adaptation when switching between ExtendDB and the real service.

## Storage and Infrastructure

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Storage backend | Proprietary distributed storage | Pluggable SQL storage backend. PostgreSQL is the default; TiDB is available with the `tidb` feature. |
| Global Tables | CreateGlobalTable, replication | Not implemented (returns UnknownOperationException) |
| DAX (Accelerator) | In-memory caching layer | Not applicable |
| PartiQL | ExecuteStatement, BatchExecuteStatement | Not implemented (returns UnknownOperationException) |

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
| TTL deletion | Background process, items deleted within 48 hours of expiry | Backend-specific. PostgreSQL uses an indexed sweep. TiDB uses native table TTL for non-streaming tables and keeps an indexed worker only where DynamoDB Streams REMOVE records must be emitted. |
| TTL stream records | REMOVE events with `userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}` | Supported — TTL deletions generate REMOVE stream records with the same `userIdentity` |
| TTL modification cooldown | Enforces a cooldown period between enable/disable changes ("Time to live has been modified multiple times within a fixed interval") | No cooldown — TTL can be enabled and disabled immediately. Intentional divergence for faster local development. |

## Tagging

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| TagResource / UntagResource | Validates resource ARN exists, returns `ResourceNotFoundException` for missing tables | Matches DynamoDB — validates resource ARN and returns `ResourceNotFoundException` for missing tables. |

## Secondary Indexes

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| GSI update propagation | Eventually consistent (milliseconds to seconds) | Backend-specific. PostgreSQL can simulate asynchronous propagation via `gsi_propagation_delay_ms`; TiDB uses native secondary indexes maintained from the base table write. |
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
| `control_plane_delay_seconds` | 5 | Simulated delay for table create/delete state transitions (CREATING → ACTIVE, DELETING → removed). UpdateTable GSI/stream transitions report UPDATING until the backend reconciler completes, while table data-plane reads and writes remain available. |
| `gsi_propagation_delay_ms` | 10 | PostgreSQL backend default GSI propagation delay (milliseconds). TiDB ignores this setting because GSI writes are transactional. |
| `throttling_enabled` | `true` | Enable provisioned capacity throttling (token bucket per table/partition) |
| `enable_multipart_keys` | `false` | Enable multi-part base table key extension |
| `log_level` | `info` | Runtime log level (trace, debug, info, warn, error) |
| `sqlx_log_level` | `warn` | Separate log level for sqlx query traces |
| `allow_credential_import` | `true` | Allow importing credentials via the management API |

## TiDB Native Backup/Restore

The TiDB backend uses TiDB BR for backup data instead of copying items into
ExtendDB catalog tables. `CreateBackup` requires `[storage.tidb.backup]`
configuration (`pd_endpoint` and `storage_uri`).

BR restores physical TiDB tables to their recorded database/table identity.
That means TiDB `RestoreTableFromBackup` is available only when the target TiDB
cluster is empty or conflict-free for the backed physical table. ExtendDB does
not emulate unsupported BR restore shapes by replaying item rows.

Restored tables do not inherit TTL or stream settings. When BR restores a TiDB
table that previously used native TTL, ExtendDB strips the restored physical TTL
artifacts before publishing the target table as `ACTIVE`.

`DeleteBackup` removes TiDB BR backup data only for `local://` or `file://`
backup URIs. For remote object stores, TiDB BR leaves backup-data lifecycle to
the operator or object-store lifecycle policy, so ExtendDB refuses metadata-only
deletion instead of reporting a false delete.

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
