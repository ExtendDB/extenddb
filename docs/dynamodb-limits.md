# DynamoDB Limits Enforcement Status

Last updated: 2026-10-19

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
| Maximum item size | 400 KB (409,600 bytes) | Enforced | `LimitsConfig::max_item_size_bytes`. The engine enforces it on PutItem, BatchWriteItem, the transaction Put path and ImportTable; the PostgreSQL and SQLite storage layers enforce it on UpdateItem and the transaction Update path. Cassandra applies a fixed 409,600 to the finished item of an update with the put wording; MongoDB applies no limit on updates. The sizing calculation (item bytes, number cost, and the statement-based UpdateItem rule) lives in [differences-from-dynamodb.md](differences-from-dynamodb.md#item-size). |
| Partition key size | 1–2,048 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_partition_key_size_bytes`            |
| Sort key size | 1–1,024 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_sort_key_size_bytes`                 |
| Attribute name size | 1 to 65,535 bytes (64 KB) | Enforced | `validate_attribute_name_sizes`, `validate_expression_attribute_name_sizes`, `validate_attribute_updates_name_sizes`, `LimitsConfig::max_attribute_name_bytes`; UTF-8 bytes, top-level names and map keys at every depth. PutItem, BatchWriteItem.PutRequest, TransactWriteItems.Put and ImportTable check the item. PutItem, UpdateItem, DeleteItem and every TransactWriteItems member (Put, Update, Delete, ConditionCheck) check every `ExpressionAttributeNames` value and every map key inside every `ExpressionAttributeValues` value; UpdateItem also checks the names and values of `AttributeUpdates`. PutItem and UpdateItem report the name ahead of the table lookup; UpdateItem also reports it ahead of the condition and the index-key checks. TransactWriteItems checks every member, and BatchWriteItem every PutRequest, before any table lookup, so neither a missing table (in any member) nor a PutRequest item missing its key is reported first; a transaction fails as a whole with a top-level ValidationException. With an over-limit name in both expression maps, UpdateItem reports the `ExpressionAttributeNames` placeholder and a transacted Update or Put reports the `ExpressionAttributeValues` placeholder; a transacted Put with a long item name and a long `ExpressionAttributeNames` value reports the placeholder, a standalone PutItem the item. The message depends on the request form (`AttributeNameRequestForm`): PutItem, UpdateItem and DeleteItem answer `1 validation error detected: Attribute name is too large, must be less than 65536 bytes`, followed by ` for key #n` when the name came through `ExpressionAttributeNames`; a TransactWriteItems member answers `ExpressionAttributeNames contains invalid value: Attribute name is too large, must be less than 65536 bytes for key #n`, or `ExpressionAttributeValues contains invalid value: ... for key :v` for a map key inside a value; TransactWriteItems.Put and BatchWriteItem.PutRequest answer `Attribute name is too large, must be less than 65536 bytes` for a name in the item. |
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
| Document path length in an expression | 32 elements (each attribute name and list index counts as one) | Enforced | Every expression kind: `Invalid <Kind>: The document path has too many nesting levels; nesting levels: <n>`. Checked path by path in the order the paths appear, after each path's `#name` references, so an undefined `#name` in an earlier path or inside the long path is reported first, as on the service; a KeyConditionExpression checks its `:value` references in the same pass. A syntax error anywhere in the expression is reported first. ExtendDB checks reserved words on the whole expression before any path, where the service checks them in the same walk, so a long path followed by a reserved word answers the reserved word error |
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
| GetRecords: max records per call | 1,000 | Enforced | A `Limit` under 1 answers `1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1`, a `Limit` over 1000 answers `One or more parameter values were invalid: Limit should be less than or equal to 1000` (both measured 2026-10-25) |
| Shard iterator lifetime | 15 minutes | Enforced | Iterators older than 15 minutes are rejected with `ExpiredIteratorException` |

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
| Concurrent restores | 50 | Partial | Every backend: one server process runs at most `[backup] max_concurrent_jobs` (default 4, at most 1024) backup and restore jobs at once, `CreateBackup` and `RestoreTableFromBackup` together and not per account; a request that finds every slot held waits up to 30 s for one (holding nothing while it waits: the permit is the last of its checks) and is then refused with `LimitExceededException` ("Too many backup and restore jobs are running on this server (at most N at once, the [backup] max_concurrent_jobs bound); retry later") and creates nothing (storage-level and engine tests; measured on PostgreSQL with `max_concurrent_jobs = 1` on an earlier build that did not wait: a running restore made a CreateBackup and a second restore refuse in under 60 ms). On PostgreSQL `pool_size` and `catalog_pool_size` are raised to at least `2 * max_concurrent_jobs + 10` so every admitted job has a connection. SQLite also bounds the snapshot slots `CreateBackup` and table snapshots (#395) share, a count derived from `[storage.sqlite] pool_size` (the formula is stated once, on that setting's documentation in `SqliteConfig`; 4 for the default pool) so half the pool stays free for ordinary requests; a server raises a smaller `pool_size` to `2 * (max_concurrent_jobs + 1)`, at most 34 (the slots are capped at 16), with a warning naming both numbers, so the slot bound refuses no job the `max_concurrent_jobs` permit admitted (no change at the defaults: a pool of 10 has 4 slots for 4 jobs); above 16 jobs a 17th concurrent job is refused by the slot. A `CreateBackup` that finds no slot free is refused at once with the same `LimitExceededException`, its parenthesis naming that bound and its count in place of `max_concurrent_jobs` ("at most N at once, the [storage.sqlite] pool_size snapshot-slot bound"); PostgreSQL bounds open table snapshots at half its (raised) pool the same way, naming `[storage.postgres] pool_size snapshot-slot`; a PostgreSQL job whose dedicated claim connection the database server refuses is refused with the same exception naming the server's connection slots ("the database server has no connection slot left for the job"), with no count; the same error while `extenddb backup purge` holds the catalog's maintenance claim names a backup maintenance command and the claim, not a bound. A restore request on any backend that is not registered within 60 seconds (`RESTORE_REGISTRATION_TIMEOUT`) answers an internal error while the restore continues in the background. A restore from an S3 store holds the compressed backup size at `spool_path` while it runs; a restore's write batch on SQLite and Cassandra is bounded at 32 MiB of item text, measured on the serialized item (not on the service's `item_size_bytes`), and on Cassandra the item is serialized once more to measure it, a CPU cost per item the bound buys; a restore ends by counting the target against the manifest's item total, which on Cassandra is a full `COUNT(*)` of the restored table (one scan of a CREATING table, run once per restore and once more on a resume without recorded progress; no run has measured its cost past 12,000 rows, the largest Cassandra restore measured; a transient failure of this count is retried a few times, and a count that keeps failing leaves the record RUNNING for the reconciler to resume rather than failing the restore and removing its loaded target) |
| `BackupName` and `TableName` on `CreateBackup`, `TableName` on `ListBackups` | Length 3 to 255, `[a-zA-Z0-9_.-]+` | Enforced | The service model's constraints, with its texts ("1 validation error detected: Value '<v>' at 'backupName' failed to satisfy constraint: ...", "2 validation errors detected: ...; ..." when length and pattern both fail). Length is counted in characters. `BackupName` takes the model's 3 to 255; a `TableName` member takes the deployment's `[limits]` `min_table_name_length` and `max_table_name_length` (defaults 3 and 255), as `CreateTable` does, so a table the deployment can create can be listed and backed up whatever its name, as on the service: a name over 255 bytes (with `max_table_name_length` raised) or made only of dots fits no store prefix, so the store-backed create refuses it before it opens a snapshot or writes a row, and the in-catalog create (no store, Cassandra) carries it (see the differences document's backup creation row). Both members may also be the table's ARN; an ARN whose region is not the server's is `ValidationException` "1 validation error detected: Invalid AWS region in '<arn>'", the service's text. The value is echoed in the message as sent |
| `BackupArn` | 37 to 1024 characters | Maximum enforced; minimum implied | Surrounding ASCII whitespace is trimmed first, then over 1024 bytes, not of the backup ARN shape, a control or non-ASCII character, or a region outside `[a-z0-9-]`, an account that is not digits, or a table name outside `[a-zA-Z0-9_.-]` is `ValidationException` "Invalid Request: BackupArn is not valid"; the right shape with an id that is not a backup id is "Invalid Backup ARN"; a digit-only account that is not the caller's is `AccessDeniedException`. No minimum is checked: the shortest string of the right shape is longer than 37 characters. Another region of the caller's account resolves (the server's region is tried first, then the ARN as written). `ListBackups`'s `ExclusiveStartBackupArn` must also match the account, the region, and `TableName` when given |
| `ListBackups` page size | 100 | Enforced | `Limit` 1 to 100, default 100; more rows come back through `LastEvaluatedBackupArn` and `ExclusiveStartBackupArn`. Each request reads every backup of the account (or of the table, with `TableName`) and pages in memory, so walking N backups reads about N²/100 rows. `TimeRangeLowerBound`, `TimeRangeUpperBound`, and `BackupType` filter the list before paging; a bound before the epoch, or a lower bound above the upper bound ("Time range lower bound must be less than or equal to upper bound"), is `ValidationException` |
| Backup store | A backup lives in the service's own storage | N/A | `[backup.store]` selects where CreateBackup writes its backups on PostgreSQL, SQLite and MongoDB (Cassandra, any backend without a store, and SQLite on a platform where the serve lock is not taken (not Unix), write the in-catalog format) and where restores and deletes of store-backed backups read: a filesystem root (`backups/` beside a SQLite database file when the section is absent), an S3 bucket and prefix, or `disabled`. The knobs are `[backup]`: `data_file_target_bytes` 64 MiB (minimum 32 MiB), `heartbeat_interval_secs` 15, `stale_job_after_secs` 300 (at least three heartbeats), `failed_row_retention_secs` 7 days, `reconcile_interval_secs` 60, `max_concurrent_jobs` 4 per instance, `spool_path` (required with an S3 store except beside a SQLite database file). A filesystem store holds every backup's compressed size under its root, beside the database on the SQLite default, and `extenddb destroy` leaves it; a restore from an S3 store holds the compressed backup size at `spool_path` while it runs. Instances that share a catalog must share one store root; an unhealthy store refuses CreateBackup where it writes to the store and the restore and delete of store-backed backups with `ValidationException` naming the store's kind, and the server keeps serving (`/health` reports `degraded`). See the admin guide's `[backup.store]` |
| Backup data file | Not specified | N/A | A data file holds one item per line as DynamoDB JSON, gzip-compressed, in key order on a table whose key schema is orderable (`item_order` in the manifest); a line is at most 32 MiB (`MAX_LINE_BYTES`, which holds the largest line a 400 KB item can expand to, 27.7 MB); a file closes at the first item that takes its uncompressed bytes to or past `[backup] data_file_target_bytes` (default 64 MiB, minimum 32 MiB, the line cap), so it exceeds the knob by less than one line. The snapshot streams on PostgreSQL and SQLite page the table by key and cut each page at 64 MiB of item text (`SNAPSHOT_FETCH_BYTES`, a constant), so a page holds at most that plus one row; MongoDB's batches are bounded by the server's 16 MiB reply |
| Backup contents | Key schema, attribute definitions, items, indexes, billing mode, provisioned throughput, on-demand throughput (table and GSI level), SSE. Table class is not carried (a restore is `STANDARD`) | Partial | ExtendDB restores the same set, except a GSI's on-demand throughput (it records none per index); table class is not restored, as on the service. Streams, TTL, tags, and deletion protection are not restored, as on the service (a store-backed backup's manifest records the stream specification and TTL for `DescribeBackup`). With a configured store on PostgreSQL, SQLite and MongoDB the backup is written under the store's root and restores into the backend that took it, and into another backend once `backup sync --adopt-store` (a later release) has indexed the backup in the target's catalog; this release restores the backups its own catalog indexes (the differences document's "Where backups live" row). Vector indexes are restored where the target supports them, and a backup of a multi-part-key table (the `limits.allow_multipart_table_keys` preview) restores with its item set intact (the engine's `a_multi_part_key_manifest_is_registered` and `tests/test_backup_job.py`'s multi-part cell, which restores such a backup on a server with the preview enabled and compares the restored item set with the source's); without one, and on Cassandra, it is stored inside the database the server uses, so take database-level backups for off-host copies there |
| Point-in-time recovery | 35-day window | N/A | Not supported: `UpdateContinuousBackups` refuses to enable it (`ContinuousBackupsUnavailableException`) and `RestoreTableToPointInTime` refuses every restore (`PointInTimeRecoveryUnavailableException`); use on-demand backups (see differences-from-dynamodb.md) |
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
| Items | 6 | 0 | 0 | 0 |
| Secondary Indexes | 5 | 0 | 1 | 0 |
| Query/Scan | 2 | 0 | 5 | 0 |
| Batch Operations | 3 | 0 | 2 | 0 |
| Transactions | 3 | 0 | 1 | 0 |
| Streams | 3 | 0 | 1 | 0 |
| API-Level | 1 | 0 | 3 | 1 |
| Import/Export/Backup | 2 | 2 | 0 | 7 |
| Global Tables | 0 | 0 | 0 | 2 |
| Contributor Insights | 0 | 0 | 0 | 1 |
| **Total** | **34** | **2** | **14** | **13** |

### Unenforced Limits Requiring Tracking

The following unenforced limits are tracked in `docs/technical-debt.md`:

1. **Provisioned capacity decrease limit** (27/day) — would require per-table decrease counter with hourly replenishment
2. **LSI item collection size** (10 GB) — requires per-partition size tracking in storage layer
3. **Expression aggregate size limits** (2 MB total expression attribute names/values) — requires aggregate byte-length tracking across all expression parameters. (Per-expression 4 KB limits for condition/filter/projection/update/key-condition are enforced via `check_expression_size`.)
4. **BatchGetItem response size** (16 MB) — requires aggregate response size tracking
5. **BatchWriteItem request size** (16 MB) — requires aggregate request size tracking
6. **Transaction request size** (4 MB) — requires aggregate request size tracking
7. **Tag count per resource** (50): requires count validation in TagResource
8. **Tag key/value length** (128/256 chars): requires length validation in TagResource

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
