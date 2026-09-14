# Design Spec: Off-Host Backup and Restore for ExtendDB

Status: Draft for maintainer review, 2026-09-10. Target: v1.0. Baseline: main at 4fd2c5f8.

## Summary

ExtendDB backups today are rows inside the same database that holds the data, in a shape private to each storage backend. This spec replaces that with a single portable backup format written to a configured backup store, either a local directory or an S3 bucket, driven by the engine rather than by each backend. The DynamoDB API contract does not change: CreateBackup, DescribeBackup, ListBackups, DeleteBackup, and RestoreTableFromBackup keep their request and response shapes. Backends gain one new obligation, a consistent snapshot iterator over a table, and lose the obligation to implement backup storage themselves. The format is the DynamoDB S3 export layout plus one ExtendDB manifest, so a backup restores into any backend, imports into Amazon DynamoDB with ImportTable, and a DynamoDB export restores into ExtendDB.

The spec also settles point-in-time recovery for v1: unsupported, reported honestly, with the format designed so a later change-log archive can add it.

## Motivation

The v1 readiness analysis (v1gaps-backup-dr.md, v1gaps-data-integrity.md) found:

1. Every backup shares the failure domain of its data. Host, disk, or database loss takes the backups with it. No off-host mechanism exists.
2. PostgreSQL backup drops the sort key of every composite-key table because it checks for a column literally named sk (backup_engine.rs:100, :548); restore then wedges the table in CREATING.
3. SQLite and PostgreSQL restore drop GSIs, LSIs, throughput, table class, and encryption settings. SQLite silently drops vector indexes (F-19). MongoDB preserves what it supports. The three backends disagree on what a restore produces.
4. Backups cannot cross backends or versions because each backend stores its own shape.
5. Point-in-time recovery reports ENABLED with a 35-day window and can never restore.
6. Export is not a consistent snapshot (issue #57, PR #80 open).
7. Loss of the catalog destroys the ability to find or restore backups.

Three separate per-backend fixes would leave items 1, 4, and 7 open. One engine-owned path with a portable format closes all seven.

## Goals

- Backups survive loss of the ExtendDB host and of the backend database when the store is S3.
- One backup format for all backends; a backup taken on any backend restores on any backend.
- Restore reproduces what Amazon DynamoDB reproduces: items, key schema, attribute definitions, GSIs, LSIs, billing mode and throughput, table class, and the vector index set. Streams settings, TTL, tags, and PITR settings are not restored, matching DynamoDB.
- A backup is a consistent snapshot of the table as of one instant.
- CreateBackup and RestoreTableFromBackup are asynchronous with DynamoDB status transitions; a crash mid-operation leaves state a startup reconciler resolves.
- The catalog can be rebuilt from the store.
- Existing API request and response shapes are unchanged.

## Non-goals

- Point-in-time recovery. Phase 3 describes how the format supports it later.
- Cross-account restore. The backup ARN account must equal the caller account, as today.
- Backup encryption inside ExtendDB. Encryption at rest is the store's job (S3 SSE-S3 or SSE-KMS, filesystem encryption). The spec states this in the deployment guide.
- Scheduled backups. Operators schedule CreateBackup externally in v1. Phase 3 adds a scheduler.
- Multi-instance coordination beyond what the single-instance lease from the readiness plan already provides.

## Detailed design

### 1. Backup format

A backup is a prefix in the store containing:

```
<root>/<account_id>/<table_name>/<backup_id>/
  extenddb-manifest.json          written last; its presence means the backup is complete
  manifest-summary.json           DynamoDB export summary shape
  manifest-files.json             DynamoDB export file list shape, one JSON object per line
  data/<part>.json.gz             DynamoDB JSON, one {"Item": {...}} per line, gzip
```

`manifest-summary.json` and `manifest-files.json` follow the shapes Amazon DynamoDB writes for ExportTableToPointInTime with exportFormat DYNAMODB_JSON, so the `data/` prefix is a valid ImportTable source for the real service and for ExtendDB's own ImportTable. `manifest-files.json` carries per-file `itemCount`, `md5Checksum`, and `dataFileS3Key`, which restore verifies before applying.

`extenddb-manifest.json` is the ExtendDB extension and the commit record:

```json
{
  "format_version": 1,
  "backup_arn": "arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>",
  "backup_name": "...",
  "backup_creation_date_time": 1757500000.123,
  "backup_size_bytes": 123456,
  "source_backend": "postgres",
  "source_extenddb_version": "0.1.11",
  "snapshot": { "kind": "postgres_repeatable_read", "marker": "..." },
  "source_table": {
    "table_name": "...", "table_id": "...", "table_arn": "...",
    "table_creation_date_time": 1757400000.0,
    "key_schema": [...], "attribute_definitions": [...],
    "billing_mode": "PROVISIONED", "provisioned_throughput": {...}, "on_demand_throughput": null,
    "table_class": "STANDARD", "sse_specification": {...},
    "global_secondary_indexes": [...], "local_secondary_indexes": [...],
    "vector_indexes": [...],
    "item_count": 100000, "table_size_bytes": 123456
  },
  "data_files": [ { "key": "data/000001.json.gz", "item_count": 50000, "sha256": "...", "bytes": 61728 } ],
  "manifest_sha256": "..."
}
```

Rules:
- The `data/` prefix contains only data files, so ImportTable against that prefix never sees a manifest.
- `extenddb-manifest.json` is written after every data file and both DynamoDB manifests succeed. A prefix without it is an incomplete backup and is invisible to ListBackups after `backup sync`.
- `format_version` is checked on restore; unknown versions refuse with a clear error.
- `vector_indexes` is present when the source backend supports vector search; a restore target without vector support refuses the restore with the existing message ("restoring a table with vector indexes is not supported by this storage backend") rather than dropping them. This closes F-19 uniformly.
- Numbers are DynamoDB JSON strings, so the 38-digit precision survives the file. A restore into MongoDB of a value beyond Decimal128's 34 digits fails item validation with ValidationException and the restore reports FAILED (see 5.3); it never rounds.

### 2. Backup store abstraction

New crate `crates/backup-store` with one trait and two implementations:

```rust
pub trait BackupStore: Send + Sync {
    fn put(&self, key: &str, body: ByteStream) -> BoxFuture<Result<(), StoreError>>;
    fn get(&self, key: &str) -> BoxFuture<Result<ByteStream, StoreError>>;
    fn head(&self, key: &str) -> BoxFuture<Result<Option<ObjectMeta>, StoreError>>;
    fn list(&self, prefix: &str) -> BoxStream<Result<ObjectMeta, StoreError>>;
    fn delete_prefix(&self, prefix: &str) -> BoxFuture<Result<u64, StoreError>>;
}
```

- `FilesystemStore`: root directory from config. Keys map to paths under the root; the existing export-path sandboxing rules apply (canonicalize, refuse traversal, refuse symlinks that resolve outside the root). Writes go to a temporary name and rename into place so a crash never leaves a truncated file under its final name.
- `S3Store`: bucket, prefix, region, optional endpoint and path-style flag for S3-compatible stores, credentials from the standard AWS SDK chain (environment, shared config, IMDS, IRSA, container credentials). Uploads over `upload_part_size_bytes` use multipart; failed multipart uploads are aborted. Dependency: the official `aws-sdk-s3` crate, chosen over `object_store` because it inherits the SDK credential chain, IRSA support, and retry behavior that operators already configure for other AWS tooling. The dependency adds to the licenses check (Apache-2.0, no notice change expected).

Both implementations are exercised by the same conformance test module in the crate.

### 3. Storage trait changes

`BackupEngine` on the storage trait is reduced to what only a backend can do. Removed: `create_backup`, `describe_backup`, `list_backups`, `delete_backup`, `restore_table_from_backup`, `restore_table_to_point_in_time`. The engine owns those now. Added:

```rust
pub trait SnapshotEngine: Send + Sync {
    /// Open a consistent read snapshot of one table and stream every item in it.
    /// The stream observes the table as of a single instant; writes committed after
    /// the snapshot opens are not visible. `SnapshotInfo` records how the snapshot
    /// was taken for the manifest.
    fn snapshot_items(
        &self, account_id: &str, table_name: &str,
    ) -> BoxFuture<Result<(SnapshotInfo, BoxStream<Result<Item, StorageError>>), StorageError>>;
}
```

Per-backend implementation:
- PostgreSQL: a read-only transaction at REPEATABLE READ with a server-side cursor fetched in batches. `SnapshotInfo.marker` records `pg_current_snapshot()`. One transaction per backup, held for the duration of the upload. Long transactions delay vacuum on the table; the deployment guide states this and the worker logs snapshot age.
- SQLite: a read transaction on a pool connection. In WAL mode a read transaction sees the database as of its first read for its whole duration. The writer lock is not taken, so writes continue.
- MongoDB: a session with snapshot read concern and a cursor over the base collection. Snapshot history is bounded server-side (`minSnapshotHistoryWindowInSeconds`, default 300), so a slow upload of a large collection can fail with SnapshotTooOld. Mitigation in v1: the worker reads the collection in batches under the snapshot session while buffering to the store; if the cursor fails with SnapshotTooOld the backup fails and the operator raises the server window (documented). Alternative considered: a server-side `$out` copy to a scratch collection, then a plain stream from the copy; rejected for v1 because it doubles storage per backup and the existing restore path already uses `$out` for a different reason. Revisit if operators hit the window.
- Cassandra: out of scope for this spec. The backend is Experimental at v1 and implements the trait when it graduates.

The continuous-backups methods (describe and update) and the point-in-time restore method leave the trait in the PITR PR; the engine serves those three operations itself (section 6).

Existing in-catalog backup tables (`backups` and per-backend item copies) stay readable for one minor release: `describe_backup`, `list_backups`, and `restore_table_from_backup` on the engine first consult the new catalog index, then fall back to a `LegacyBackupReader` per backend that wraps the current code. `create_backup` never writes the legacy shape again. The upgrade manual tells operators to restore or delete legacy backups before the release that removes the reader.

### 4. Catalog index

The engine keeps a `backups` catalog row per backup as the fast index for Describe and List, and as the state machine record:

```
backup_id, account_id, table_name, backup_arn, backup_name,
status            CREATING | AVAILABLE | DELETED | FAILED (internal; surfaced as DELETED)
store_kind        filesystem | s3
store_root        the configured root at creation time
store_key_prefix  <account>/<table>/<backup_id>/
size_bytes, item_count, created_at, completed_at, failure_reason,
source_table_details_json
```

The store is the source of truth; the row is a cache. `extenddb backup sync` lists `<root>/<account>/` prefixes, reads every `extenddb-manifest.json`, and upserts rows, marking rows whose prefix is gone as DELETED and prefixes with no row as AVAILABLE. This is the recovery path when the catalog is lost and the store survives, and it is how a fresh ExtendDB adopts a bucket of backups from another instance.

The ARN encodes account, table, and backup id, and the key prefix is derived from those three fields deterministically, so `describe_backup(arn)` works from the store alone after a sync.

### 5. Operations

#### 5.1 CreateBackup

1. Validate table exists and is ACTIVE for the account (existing checks).
2. Insert catalog row with status CREATING and a fresh backup id; return `BackupDetails` with BackupStatus CREATING. This matches DynamoDB, where CreateBackup returns CREATING and the backup becomes AVAILABLE shortly after.
3. Enqueue a backup job. The job runner is a new worker in `crates/server/src/workers`, following the pattern of the existing workers, with the single-instance lease from the readiness plan guaranteeing one runner per catalog.
4. The job: open the snapshot, stream items into gzip-compressed data files of at most `data_file_target_bytes` (default 64 MiB uncompressed), computing sha256 and md5 and item count per file, upload each as it completes, write `manifest-files.json` and `manifest-summary.json`, write `extenddb-manifest.json`, update the row to AVAILABLE with size and count.
5. On any failure: abort in-flight multipart uploads, `delete_prefix` the partial backup, set the row FAILED with the reason, log at error level with the request id. DescribeBackup returns the backup as DELETED (DynamoDB has no FAILED status); ListBackups omits it. A metric `backup_jobs_failed_total` increments.
6. Crash mid-job: on startup the reconciler finds CREATING rows older than `stale_job_after_secs` (default 3600) with no running job, checks the store for a complete manifest (present means AVAILABLE, update the row), otherwise performs step 5.

#### 5.2 DescribeBackup, ListBackups, DeleteBackup

Served from the catalog index with the legacy fallback. DeleteBackup sets the row DELETED, then `delete_prefix` on the store; if the store delete fails the row stays AVAILABLE and the API returns InternalServerError, so a backup is never reported deleted while its bytes remain. The request returns the description with BackupStatus DELETED after both succeed.

#### 5.3 RestoreTableFromBackup

1. Resolve the ARN to a row or, if absent, to a store prefix; read `extenddb-manifest.json`, verify `manifest_sha256`, verify `format_version`.
2. Validate the target: name free in the account, the target backend supports every feature in `source_table` (vector indexes, table class); refuse before creating anything otherwise.
3. Create the target table with `defer_active` (the pattern PR #311 introduced for MongoDB, generalized to all backends): base table exists, status CREATING, indexes registered CREATING, no writes accepted. Return `TableDescription` with TableStatus CREATING. DynamoDB returns CREATING from RestoreTableFromBackup as well.
4. Enqueue a restore job that, per data file, downloads, verifies checksum and item count, validates each item against the key schema and attribute definitions (the ImportTable validation path), and writes items through the backend's bulk-load path with stream capture and TTL disabled (the table has no stream and no TTL at restore). Progress (files completed) is recorded on the job row so a restart resumes at the next file; files are idempotent to re-apply because writes are keyed puts.
5. After all files: mark the restore backfill pending so the existing GSI, LSI, and vector backfill workers populate indexes (PostgreSQL gsi_pending, MongoDB restore-mode backfill, SQLite inline path), then the table transitions to ACTIVE when every index is ACTIVE. This reuses the deferred activation machinery from #311 and gives every backend the same lifecycle.
6. Failure: the job marks the restore FAILED, deletes the partially created table (the existing DeleteTable path), and logs the reason with the backup ARN. Crash mid-restore: the reconciler finds CREATING tables with a restore job row and resumes from the recorded file index; a restore job row with no store prefix reachable fails and cleans up.

Restore overrides (`BillingModeOverride`, `ProvisionedThroughputOverride`, `GlobalSecondaryIndexOverride`, `LocalSecondaryIndexOverride`, `SSESpecificationOverride`) are accepted and applied at step 3, matching the DynamoDB request shape; today they are ignored.

#### 5.4 ExportTableToPointInTime and ImportTable

Export gains the store as a target and uses `snapshot_items`, which fixes the inconsistent-snapshot defect (issue #57, PR #80) with the same code. The request's `S3Bucket` and `S3Prefix` are honored when the store is S3 and the bucket matches the configured one (a different bucket is refused with ValidationException naming the configured bucket, since the server holds credentials for one bucket); with a filesystem store the existing path behavior remains. Import gains the same store as a source. The missing sibling operations DescribeExport, ListExports, DescribeImport, ListImports are added over the same job rows so SDK poll loops work (v1gaps-api-fidelity.md gap 6). Export continues to ignore ExportTime and incremental export in v1 and now says so with ValidationException instead of silently ignoring them.

### 6. Point-in-time recovery in v1

- DescribeContinuousBackups returns ContinuousBackupsStatus ENABLED (DynamoDB always reports this) and PointInTimeRecoveryDescription with PointInTimeRecoveryStatus DISABLED and no EarliestRestorableDateTime.
- UpdateContinuousBackups with PointInTimeRecoveryEnabled true returns ContinuousBackupsUnavailableException, the typed DynamoDB exception modeled on that operation. With false it returns the DISABLED description.
- RestoreTableToPointInTime first checks that the source table exists (TableNotFoundException otherwise, as the live service does), then returns PointInTimeRecoveryUnavailableException, the typed exception the service models and returns on that operation (live service message: "Point in time recovery is not enabled for table '<name>'").
- The differences doc gains a row stating PITR is not supported and pointing at on-demand backups.
- The per-backend `describe_continuous_backups`, `update_continuous_backups`, and `restore_table_to_point_in_time` trait methods and their `continuous_backups` catalog columns are removed.

### 7. Configuration

```toml
[backup]
store = "filesystem"                  # "filesystem" | "s3"; filesystem is the default
data_file_target_bytes = 67108864     # 64 MiB uncompressed per data file
stale_job_after_secs = 3600

[backup.filesystem]
path = "/var/lib/extenddb/backups"    # required when store = "filesystem"

[backup.s3]
bucket = "my-extenddb-backups"        # required when store = "s3"
prefix = "extenddb/"                  # optional
region = "us-east-1"                  # optional; SDK chain otherwise
endpoint = "http://minio:9000"        # optional; S3-compatible stores
force_path_style = false
upload_part_size_bytes = 8388608
max_concurrent_uploads = 4
```

`deny_unknown_fields` applies, matching the other sections. `extenddb init` writes the `[backup]` section with the filesystem default and a commented S3 example. The startup banner prints the backup store kind and root. A filesystem store keeps the same-host failure domain and the deployment guide says so in the first sentence of the backup section, with S3 as the production recommendation.

Startup validates the store: filesystem root exists and is writable; S3 bucket answers HeadBucket with the configured credentials. Failure is a startup error, not a runtime surprise at the first CreateBackup.

### 8. Security

- Credentials for S3 come from the SDK chain; ExtendDB never stores them in its catalog. The deployment guide gives least-privilege bucket policy examples (PutObject, GetObject, DeleteObject, ListBucket, AbortMultipartUpload on the prefix).
- Object keys are prefixed by account id, so a tenant's backups are separable by bucket policy or lifecycle rule. The engine refuses any ARN whose account differs from the caller before touching the store.
- Filesystem store keys are validated with the export-path rules; no key component may be `..` or contain a path separator beyond the fixed layout.
- Checksums on every data file and on the manifest are verified before any item is written on restore. A checksum mismatch fails the restore and names the file.
- Backup contents are plaintext DynamoDB JSON inside gzip. Encryption at rest is the store's responsibility and the docs state it. Bucket versioning and object lock are recommended for ransomware resistance; ExtendDB does not depend on either.
- Management actions (CreateBackup, DeleteBackup, RestoreTableFromBackup, `backup sync`) are audit-logged with account, ARN, and request id.

### 9. Observability

Metrics: `backup_jobs_total{outcome}`, `backup_job_duration_seconds`, `backup_bytes_uploaded_total`, `restore_jobs_total{outcome}`, `restore_job_duration_seconds`, `backup_snapshot_age_seconds` (gauge while a job runs), `backup_store_errors_total{op}`. Logs: one info line at job start and completion with ARN, item count, bytes, duration; one error line per failure with the reason. The `/health` readiness work from the operations report should include store reachability as a component, not as a hard failure (a backup store outage should not take the data plane out of rotation).

### 10. Documentation

- New `docs/manuals/14-backup-and-restore.md`: strategy, RPO and RTO statements per store kind (RPO equals backup interval, no PITR; RTO measured in the acceptance tests and published), configuration, IAM policy example, restore drill procedure, catalog loss recovery with `backup sync`, migrating legacy backups, what a restore preserves and what it does not (the table from the readiness discussion), moving a backup to or from Amazon DynamoDB with ImportTable and export.
- Corrections: admin guide database names, usage guide "backups not supported" line, differences doc PITR row, limits doc backup rows.
- Design doc `docs/design/15-backup-restore.md` derived from this spec after review.

## Phasing and work breakdown

Each task is sized for one implementer with an independent review pass after it. Tasks within a phase are independent unless noted.

Phase 0, honesty (S, one PR):
- T0.1 PITR surface per section 6, with tests for the three operations on all backends. Removes the trait methods and columns.

Phase 1, portable format and store (the v1 blocker):
- T1.1 `crates/backup-store`: trait, `FilesystemStore`, `S3Store`, conformance tests against a temp dir and a MinIO container. Acceptance: put/get/head/list/delete_prefix round-trip, multipart above the threshold, abort on failure, traversal refused.
- T1.2 Manifest types and serde in `crates/core`: `extenddb-manifest.json`, DynamoDB `manifest-summary.json` and `manifest-files.json` writers and readers, checksum helpers, `format_version` gate. Acceptance: golden-file tests against a real DynamoDB export captured once and checked in.
- T1.3 `SnapshotEngine` on PostgreSQL, SQLite, MongoDB with a shared consistency test: a writer thread increments a per-item version while a snapshot streams; every item in the snapshot has a version at or below the snapshot marker and no item is missing or duplicated. PostgreSQL variant must include composite-key tables (the sk regression). Depends on nothing.
- T1.4 Catalog index table and migration, engine-side Describe, List, Delete with the legacy reader fallback per backend. Acceptance: existing backup tests pass unchanged against legacy backups created by the previous binary.
- T1.5 CreateBackup job and worker per 5.1, including the reconciler. Acceptance: backup of a 100k-item composite-key table with GSIs on each backend; SIGKILL mid-upload leaves no prefix without a manifest after restart; failure cleans the prefix. Depends on T1.1 to T1.4.
- T1.6 Restore job per 5.3 with deferred activation generalized to all backends and the override parameters. Acceptance: round trip on each backend preserves items, key schema, GSIs, LSIs, throughput, table class, vector indexes where supported; cross-backend matrix (each source into each target, nine cells, MongoDB targets refuse vector manifests with the documented error); SIGKILL mid-restore resumes; checksum tamper fails before any write. Depends on T1.5.
- T1.7 `extenddb backup sync` CLI. Acceptance: drop the catalog rows, run sync, Describe and List and Restore work; a prefix without a manifest is not listed.
- T1.8 Configuration, init template, startup validation, banner line. Acceptance: bad bucket fails startup with the bucket name in the message; config display shows the store.
- T1.9 CI: MinIO service in the integration workflows, backup and restore suites unfiltered on every backend, the cross-backend matrix on a nightly schedule. Depends on T1.1.
- T1.10 Documentation per section 10.

Phase 2, unify export and import (v1 if time allows, otherwise v1.1):
- T2.1 Export through the store with `snapshot_items`; closes #57 and supersedes PR #80. ImportTable from the store. Explicit ValidationException for ExportTime and incremental export.
- T2.2 DescribeExport, ListExports, DescribeImport, ListImports over job rows.
- T2.3 Live compatibility check, measured not asserted: import an ExtendDB backup's `data/` prefix into an Amazon DynamoDB table with ImportTable and compare item counts and a sampled item set; restore a real DynamoDB export into ExtendDB. Results recorded in the backup manual.

Phase 3, later (v1.x):
- Scheduled backups with retention (a cron-like setting per table or account, DeleteBackup after N days).
- Point-in-time recovery: an internal change-log archive of stream records to `<root>/<account>/<table>/changelog/`, a periodic base snapshot through the Phase 1 path, and restore-to-time as snapshot plus replay. The format reserves the `changelog/` prefix and the manifest reserves a `snapshot.marker` field for this.

## Testing summary

- Unit: manifest serde and golden files, key mapping and ARN parsing, store conformance.
- Integration, every backend, every PR: round trip with full configuration preservation, composite keys, GSIs and LSIs, vector indexes where supported, legacy fallback, PITR surface.
- Consistency: concurrent-writer snapshot test on every backend.
- Crash: SIGKILL during upload and during restore, verified by the reconciler tests. These extend the SIGKILL harness the migration tests already use.
- Cross-backend matrix: nightly.
- Live DynamoDB: Phase 2, measured.
- Performance: backup and restore of a 1M-item table per backend, and of a 1M-item table carrying a vector index on the backends that support one, duration and peak memory recorded as the published RTO baseline; memory must stay flat with table size (streaming, never materializing the table). Index contents are not stored in the backup; every index is rebuilt from the items on restore, so the vector cell measures the dominant restore cost.

## Review checklist for implementation PRs

- No data file is uploaded outside a snapshot; the snapshot is opened before the first read and closed after the last.
- `extenddb-manifest.json` is written last and only after every other object succeeded.
- Every failure path deletes the prefix or leaves it unreachable from List, and the row never says AVAILABLE for an incomplete prefix.
- Restore writes nothing before checksums verify, and refuses before creating the table when the target cannot represent the manifest.
- Account scoping is checked on the ARN before any store access.
- No credential, bucket name with credentials, or presigned URL appears in a log line.
- New workers stop on the shutdown signal within the drain window.
- Tests create composite-key tables, not only hash-only tables.

## Drawbacks

- Backups are asynchronous where the current implementation is synchronous; clients that assumed AVAILABLE on return must poll, which is what they do against DynamoDB.
- Long snapshot transactions on PostgreSQL delay vacuum for the backup duration; MongoDB snapshot windows bound backup duration by server configuration.
- A new dependency on the AWS S3 SDK and a MinIO service in CI.
- Two code paths for one release while legacy backups remain readable.

## Alternatives considered

- Per-backend fixes to the existing in-catalog backups. Leaves the failure domain, cross-backend, and catalog-loss problems open. Rejected.
- Backend-native tooling (pg_dump, mongodump, sqlite backup API) documented as the strategy. Cluster-scoped, operator-driven, not per-table, and disconnected from the DynamoDB API. Rejected as the primary path; the manual still mentions it for whole-instance disaster recovery.
- `object_store` crate instead of `aws-sdk-s3`. Smaller and multi-cloud, but a second credential chain implementation for operators to learn. Rejected for v1; the `BackupStore` trait keeps the option open.
- A custom binary format instead of the DynamoDB export layout. Faster to parse, but loses ImportTable compatibility in both directions. Rejected.

## Decisions taken 2026-09-10

1. The filesystem store is the default. Local development and the npm launcher keep working with no configuration; the backup manual states in its first sentence that a filesystem store shares the host's failure domain and recommends S3 for production.
2. The legacy PostgreSQL backup reader is fixed to recover sort keys (the column-named-sk defect) so backups taken before this work restore correctly, in PR 5.
3. The legacy in-catalog reader ships for one minor release after this work lands, then is removed.
4. MongoDB snapshots read in batches under a snapshot-read-concern session; the server's snapshot history window is documented as an operator setting. The scratch-copy approach is deferred until an operator hits the window.
5. This document lands in the repository as `docs/design/15-backup-restore.md` in the first PR; later PRs reference it instead of opening a separate RFC.

## Resolved from the open questions above

Question 1: filesystem default (decision 1). Question 2: batch under snapshot (decision 4). Question 3: a non-configured export bucket is refused with ValidationException naming the configured bucket. Question 4: one minor release (decision 3).
