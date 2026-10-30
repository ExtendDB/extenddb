# DynamoDB Limits Enforcement Status

Last updated: 2026-04-23 (P42)

Source: [AWS DynamoDB Service Quotas](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)

## Legend

- **Enforced**: ExtendDB validates and rejects requests that exceed this limit
- **Partial**: ExtendDB enforces the limit in some but not all code paths
- **Not enforced**: ExtendDB accepts requests that would exceed this limit on real DynamoDB
- **N/A**: Limit does not apply to ExtendDB (e.g., provisioned throughput billing, global tables)

## Read/Write Throughput

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Per-table RCU (provisioned) | 40,000 | Enforced | `LimitsConfig::per_table_max_rcu`, configurable |
| Per-table WCU (provisioned) | 40,000 | Enforced | `LimitsConfig::per_table_max_wcu`, configurable |
| Per-table read request units (on-demand) | 40,000 | N/A | ExtendDB does not throttle on-demand reads |
| Per-table write request units (on-demand) | 40,000 | N/A | ExtendDB does not throttle on-demand writes |
| Per-account RCU (provisioned) | 80,000 | Enforced | `LimitsConfig::per_account_max_rcu`, configurable |
| Per-account WCU (provisioned) | 80,000 | Enforced | `LimitsConfig::per_account_max_wcu`, configurable |
| Minimum throughput per table/GSI | 1 RCU / 1 WCU | Enforced | `validate_provisioned_throughput` rejects < 1 |
| Provisioned capacity decrease limit | 27 per day (4 + 1/hour) | Not enforced | No decrease tracking implemented |
| Reserved capacity per account | 1,000,000 units | N/A | ExtendDB has no reserved capacity concept |

## Tables

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Maximum tables per account per region | 2,500 (adjustable to 10,000) | Enforced | `LimitsConfig::max_tables_per_account`, configurable |
| Table size | No practical limit | Enforced | ExtendDB has no table size limit |
| Table name length | 3–255 characters | Enforced | `validate_table_name`, `LimitsConfig` |
| Table name character set | `[a-zA-Z0-9_.-]` | Enforced | `validate_table_name_chars` |

## Items

| Limit | DynamoDB Value | Status | Notes                                                                         |
|-------|---------------|--------|-------------------------------------------------------------------------------|
| Maximum item size | 400 KB (409,600 bytes) | Enforced | `LimitsConfig::max_item_size_bytes`, validated on PutItem and post-UpdateItem |
| Partition key size | 1–2,048 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_partition_key_size_bytes`            |
| Sort key size | 1–1,024 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_sort_key_size_bytes`                 |
| Attribute name size | 1–64 KB (65,535 bytes) | Partial | `validate_attribute_name_sizes`, `LimitsConfig::max_attribute_name_bytes`; top-level names and nested map keys at every depth, on PutItem, BatchWriteItem.PutRequest, TransactWriteItems.Put, and ImportTable. Not enforced on UpdateItem or TransactWriteItems.Update at any level. |
| Attribute nesting depth | 32 levels | Enforced | `validate_item_nesting_depth`, applied on PutItem, UpdateItem, BatchWriteItem.PutRequest, TransactWriteItems.Put, ImportTable |
| Number of attributes per item | No practical limit | Enforced | ExtendDB has no per-item attribute count limit                                |

## Secondary Indexes

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| GSIs per table | 20 | Enforced | `validate_gsi_count`, `LimitsConfig::max_gsis_per_table`, configurable |
| LSIs per table | 5 | Enforced | `validate_lsi_count`, `LimitsConfig::max_lsis_per_table`, configurable |
| Projected attributes across all indexes | 100 | Enforced | `validate_projected_attribute_total`: every `NonKeyAttributes` entry of every GSI and LSI counts, on CreateTable (and so RestoreTableFromBackup) and UpdateTable's GSI Create |
| Index name length | 3–255 characters | Enforced | `validate_index_name` |
| Index name character set | `[a-zA-Z0-9_.-]` | Enforced | `validate_index_name` |
| LSI item collection size | 10 GB | Not enforced | No item collection size tracking |

## Query and Scan

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Response size per page | 1 MB (1,048,576 bytes) | Enforced | `read_helpers.rs` reads from storage in chunks and stops at the 1 MB page budget; applies whether or not items pass `FilterExpression` |
| `Limit` parameter (max items evaluated) | No maximum | Enforced | Honored in query/scan |
| Filter expression size | 4 KB | Enforced | `check_expression_size`, `LimitsConfig::max_expression_length_bytes` (raw bytes, pre-substitution) |
| Projection expression size | 4 KB | Enforced | `check_expression_size`, `LimitsConfig::max_expression_length_bytes` |
| Condition expression size | 4 KB | Enforced | `check_expression_size`, `LimitsConfig::max_expression_length_bytes` |
| Update expression size | 4 KB | Enforced | `check_expression_size`, `LimitsConfig::max_expression_length_bytes` |
| Key condition expression size | 4 KB | Enforced | `check_expression_size`, `LimitsConfig::max_expression_length_bytes` |
| Document path length in an expression | 32 elements (each attribute name and list index counts as one) | Enforced | Every expression kind: `Invalid <Kind>: The document path has too many nesting levels; nesting levels: <n>`. Checked path by path in the order the paths appear, after each path's `#name` references, so an undefined `#name` in an earlier path or inside the long path is reported first, as on the service. A syntax error anywhere in the expression is reported first. ExtendDB checks reserved words on the whole expression before any path, where the service checks them in the same walk, so a long path followed by a reserved word answers the reserved word error |
| Expression attribute names | 2 MB total | Not enforced | Per-entry key/value validated (`serde_helpers`); no aggregate size validation |
| Expression attribute values | 2 MB total | Not enforced | Per-entry key/value validated (`serde_helpers`); no aggregate size validation |

## Batch Operations

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| BatchGetItem: max keys | 100 | Enforced | `MAX_BATCH_GET_KEYS` in `batch_get_item.rs` |
| BatchGetItem: response size | 16 MB | Not enforced | No aggregate response size limit |
| BatchWriteItem: max operations | 25 | Enforced | `MAX_BATCH_WRITE_ITEMS` in `batch_write_item.rs` |
| BatchWriteItem: max item size | 400 KB | Enforced | Item size validated per item |
| BatchWriteItem: max request size | 16 MB | Not enforced | No aggregate request size limit |

## Transactions

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| TransactWriteItems: max items | 100 | Enforced | `MAX_TRANSACT_WRITE_ITEMS` in `transact_write_items.rs` |
| TransactGetItems: max items | 100 | Enforced | `MAX_TRANSACT_GET_ITEMS` in `transact_get_items.rs` |
| Transaction request size | 4 MB | Not enforced | No aggregate request size validation |
| Items per transaction across tables | No limit on table count | Enforced | ExtendDB supports cross-table transactions |

## DynamoDB Streams

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Simultaneous shard readers | 2 (1 for global tables) | Not enforced | No concurrent reader tracking |
| Max write capacity with streams (provisioned) | 40,000 WCU | Enforced | Same as table WCU limit |
| GetRecords: max records per call | 1,000 | Not enforced | No per-call record count limit |
| Shard iterator lifetime | 15 minutes | Not enforced | No shard iterator expiration |

## API-Level Limits

| Limit | DynamoDB Value | Status | Notes                                    |
|-------|---------------|--------|------------------------------------------|
| ListTables: max per page | 100 | Enforced | `LimitsConfig::list_tables_max_per_page` |
| DescribeTable: request rate | No specific limit | N/A | ExtendDB does not rate-limit             |
| TagResource: max tags per resource | 50 | Not enforced | No tag count validation                  |
| Tag key length | 1–128 characters | Not enforced | No tag key length validation             |
| Tag value length | 0–256 characters | Not enforced | No tag value length validation           |

## Import from Amazon S3

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent import jobs | 50 | N/A | ExtendDB import is synchronous, single-threaded |
| Max S3 objects per import | 50,000 | N/A | ExtendDB imports from local files |
| Total import size | 15 TB (us-east-1/us-west-2/eu-west-1), 1 TB (other) | N/A | No size limit on local import |

## Table Export to Amazon S3

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent export tasks | 300 | N/A | ExtendDB export is synchronous, single-threaded |
| Total in-flight export size | 100 TB | N/A | No size limit on local export |
| Incremental export window | 15 min – 24 hours | N/A | ExtendDB does not support incremental export |

## Backup and Restore

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent restores | 50 | Partial | PostgreSQL: one server process runs at most 4 restores per account and at most half its `pool_size` (after the minimum of 10 is applied; 10 with the default `pool_size` of 20), capped at 16, in total. A restore that cannot get both an account slot and a server slot within 30 seconds is refused with `LimitExceededException`. Measured on an earlier build of this branch, with four restores running in one account: a fifth in the same account waited for a slot and answered after 3.1 s, when one of the four finished, while another account's restore answered in 0.05 s. The 30-second refusal is from the code and storage-level tests, not from a running server. MongoDB: the same bound, 4 per account and `min(16, pool/2)` per server where the pool is the engine client's `maxPoolSize` (`max_connections`, or the driver default of 10, so 5 restores), refused with `LimitExceededException` after 30 seconds; a restore request that is not registered within 60 seconds answers an internal error while the restore continues (storage-level tests; measured with 20 concurrent restores on a served default build: all accepted, server responsive). SQLite and Cassandra have no restore limit (five started at once were all answered without waiting for each other, on an earlier build); SQLite bounds `CreateBackup` instead: at most `pool_size / 2 - 1` (4 by default) at once, `LimitExceededException` after 30 seconds |
| `BackupName` and `TableName` on `CreateBackup`, `TableName` on `ListBackups` | Length 3 to 255, `[a-zA-Z0-9_.-]+` | Enforced | The service model's constraints, with its texts ("1 validation error detected: Value '<v>' at 'backupName' failed to satisfy constraint: ...", "2 validation errors detected: ...; ..." when length and pattern both fail). Length is counted in characters. `BackupName` takes the model's 3 to 255; a `TableName` member takes the deployment's `[limits]` `min_table_name_length` and `max_table_name_length` (defaults 3 and 255), as `CreateTable` does, so a table the deployment can create can be backed up and listed. `CreateBackup`'s `TableName` may also be the table's ARN; `ListBackups`'s may not. The value is echoed in the message as sent |
| `BackupArn` | 37 to 1024 characters | Maximum enforced; minimum implied | Surrounding ASCII whitespace is trimmed first, then over 1024 bytes, not of the backup ARN shape, a control or non-ASCII character, or a region outside `[a-z0-9-]`, an account that is not digits, or a table name outside `[a-zA-Z0-9_.-]` is `ValidationException` "Invalid Request: BackupArn is not valid"; the right shape with an id that is not a backup id is "Invalid Backup ARN"; a digit-only account that is not the caller's is `AccessDeniedException`. No minimum is checked: the shortest string of the right shape is longer than 37 characters. Another region of the caller's account resolves (the server's region is tried first, then the ARN as written). `ListBackups`'s `ExclusiveStartBackupArn` must also match the account, the region, and `TableName` when given |
| `ListBackups` page size | 100 | Enforced | `Limit` 1 to 100, default 100; more rows come back through `LastEvaluatedBackupArn` and `ExclusiveStartBackupArn`. Each request reads every backup of the account (or of the table, with `TableName`) and pages in memory, so walking N backups reads about N²/100 rows. `TimeRangeLowerBound`, `TimeRangeUpperBound`, and `BackupType` filter the list before paging; a bound before the epoch, or a lower bound above the upper bound ("Time range lower bound must be less than or equal to upper bound"), is `ValidationException` |
| Backup contents | Key schema, attribute definitions, items, indexes, billing mode, provisioned throughput, on-demand throughput (table and GSI level), SSE. Table class is not carried (a restore is `STANDARD`) | Partial | ExtendDB restores the same set, except a GSI's on-demand throughput (it records none per index); table class is not restored, as on the service. Streams, TTL, tags, and deletion protection are not restored, as on the service. Backups are stored inside the database the server uses; take database-level backups for off-host copies |
| Point-in-time recovery | 35-day window | N/A | `RestoreTableToPointInTime` is refused. `UpdateContinuousBackups` accepts enabling point-in-time recovery and reports a 35-day restorable window, but nothing is recorded (see differences-from-dynamodb.md) |
| `RestoreTableFromBackup` overrides | `BillingModeOverride`, `ProvisionedThroughputOverride`, `GlobalSecondaryIndexOverride`, `LocalSecondaryIndexOverride`, `SSESpecificationOverride`, `OnDemandThroughputOverride`, `VectorIndexOverride` | Partial | All but `VectorIndexOverride` are applied as the service applies them: an index override list replaces the backup's indexes of that kind (empty restores none, a subset restores those, a GSI entry's throughput is the restored index's), `OnDemandThroughputOverride` is the restored table's on-demand throughput on an on-demand restore. Refused with `ValidationException` before the target is created: an index the backup does not have or a changed index definition, a throughput member contradicting the billing mode, `{0, 0}`, a `PROVISIONED` restore of a GSI without throughput ("Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to PROVISIONED for index <name>"), a GSI entry's `OnDemandThroughput` on a `PROVISIONED` restore (on an on-demand restore it is accepted and not recorded), and `VectorIndexOverride`. See differences-from-dynamodb.md |

## Global Tables

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| MRSC global tables | 400 | N/A | ExtendDB does not support global tables |
| Backfill data per account/region/day | 10 TB | N/A | ExtendDB does not support global tables |

## Contributor Insights

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| All Contributor Insights quotas | Various | N/A | ExtendDB does not support Contributor Insights |

## Summary

### Enforcement Coverage

| Category | Enforced | Partial | Not Enforced | N/A |
|----------|----------|---------|--------------|-----|
| Throughput | 5 | 0 | 1 | 2 |
| Tables | 4 | 0 | 0 | 0 |
| Items | 5 | 1 | 0 | 0 |
| Secondary Indexes | 5 | 0 | 1 | 0 |
| Query/Scan | 2 | 0 | 5 | 0 |
| Batch Operations | 3 | 0 | 2 | 0 |
| Transactions | 3 | 0 | 1 | 0 |
| Streams | 1 | 0 | 3 | 0 |
| API-Level | 1 | 0 | 3 | 1 |
| Import/Export/Backup | 2 | 2 | 0 | 7 |
| Global Tables | 0 | 0 | 0 | 2 |
| Contributor Insights | 0 | 0 | 0 | 1 |
| **Total** | **31** | **3** | **16** | **13** |

### Unenforced Limits Requiring Tracking

The following unenforced limits are tracked in `docs/technical-debt.md`:

1. **Provisioned capacity decrease limit** (27/day) — would require per-table decrease counter with hourly replenishment
2. **LSI item collection size** (10 GB) — requires per-partition size tracking in storage layer
3. **Expression aggregate size limits** (2 MB total expression attribute names/values) — requires aggregate byte-length tracking across all expression parameters. (Per-expression 4 KB limits for condition/filter/projection/update/key-condition are enforced via `check_expression_size`.)
4. **BatchGetItem response size** (16 MB) — requires aggregate response size tracking
5. **BatchWriteItem request size** (16 MB) — requires aggregate request size tracking
6. **Transaction request size** (4 MB) — requires aggregate request size tracking
7. **GetRecords max per call** (1,000) — requires record count limit in streams
8. **Shard iterator lifetime** (15 minutes) — requires timestamp tracking on shard iterators
9. **Tag count per resource** (50) — requires count validation in TagResource
10. **Tag key/value length** (128/256 chars) — requires length validation in TagResource

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
