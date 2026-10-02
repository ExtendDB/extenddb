# ADR-0009: Logical backup and restore for Cassandra

**Status:** Accepted
**Date:** 2026-08-03

## Context

ExtendDB's `BackupEngine` exposes DynamoDB-compatible on-demand backup, restore, listing, deletion, and continuous-backup status operations. Cassandra's native snapshots are node-local SSTable artifacts managed through `nodetool`, JMX, and filesystem/object-storage tooling. They cannot be created, enumerated, or restored through the CQL session available to this plugin, and a distributed snapshot requires cluster-level orchestration outside the storage trait.

The plugin uses a keyspace per account. Backup payloads therefore need to remain account-isolated, while ARN-addressed metadata must remain available if the source table is deleted.

## Decision

Implement on-demand backups as logical snapshots:

* Store authoritative backup metadata in catalog table `backups_by_arn`, physically partitioned by `(account_id, backup_arn)` so ARN-addressed reads and mutations cannot cross account boundaries.
* Store denormalized, query-shaped listing rows in `backups_by_account` and `backups_by_table`; no `ALLOW FILTERING` or secondary index is required. `backups_by_table` is intentionally keyed by table name because `ListBackups` accepts a name and backups outlive their source table. Each row also records the immutable source `table_id`, so delete/recreate generations remain distinguishable while old backups stay discoverable and restorable.
* Store item JSON in the owning account keyspace's `backup_items` table. Partition by `(backup_arn, bucket)` and cap each bucket at 64 items. Given DynamoDB's 400 KiB item limit, this bounds a payload partition to roughly 25 MiB before Cassandra overhead.
* Generate backup IDs as `<epoch-millis>-<8 random hex characters>`, matching the PostgreSQL backend and DynamoDB-compatible ARN shape.
* Write metadata as `CREATING`, scan and persist all item chunks, then use one bounded logged batch to create both listing rows and publish the authoritative row as `AVAILABLE`. Failed creation removes written chunks and metadata best-effort. Readers and restore accept only `AVAILABLE` rows.
* Delete transitions metadata to `DELETING`, removes listing rows and payload partitions idempotently, then transitions to `DELETED`. Describe/list treat non-`AVAILABLE` backups as absent, while the successful delete response reports `DELETED`.
* Restore creates the target through the normal table path, deserializes every snapshot item, and writes through the normal Cassandra item path so typed partition/sort-key columns are reconstructed. The target is made `ACTIVE` only after item-count verification. A failed restore removes the partial target best-effort.
* Scope every ARN lookup and mutation to the caller's account even though the ARN is globally unique. The shared engine layer separately rejects foreign-account ARNs with `AccessDeniedException`.

Catalog and account schemas are introduced as V002 migrations. The migrate workflow applies account-data migrations to every existing account keyspace; new accounts already receive all registered migrations during provisioning.

## Consistency and feature boundary

This is an item-consistent logical copy, not a cluster SSTable snapshot. Cassandra has no MVCC snapshot spanning a token-range scan, so concurrent writes can race backup creation. This matches the current PostgreSQL reference's logical scan limitation but is not advertised as strict point-in-time recovery.

`DescribeContinuousBackups` and `UpdateContinuousBackups` persist DynamoDB-compatible status. `RestoreTableToPointInTime` remains explicitly unsupported, matching the upstream engine handler. Implementing real PITR requires mutation history or CDC plus a restore watermark and retention worker; taking a fresh on-demand backup and labeling it a historical restore would violate fidelity.

The first implementation restores base table schema and items, matching the PostgreSQL reference. GSI/LSI definitions, tags, TTL configuration, and stream history are not restored.

## Consequences

### Positive

* Backup APIs work through CQL without privileged node access.
* Payloads stay in account-isolated keyspaces and are removed with the account keyspace.
* Listing and ARN lookup follow Cassandra query-first schema design.
* Bounded payload partitions avoid a single unbounded backup partition.
* Interrupted creation is never exposed as an available backup.

### Negative

* Backup creation is not a strict point-in-time image under concurrent mutation.
* Snapshot and restore cost scale linearly with item count and currently write items sequentially.
* Backup and restore execute synchronously in the request task for parity with the current backends. A production-scale iteration should persist jobs and move payload copying, retries, and reconciliation into background workers.
* Interrupted `CREATING`/`DELETING` operations need a future reconciliation worker for guaranteed orphan cleanup.
* Restoring secondary-index and other table-adjacent configuration requires a future schema extension.
