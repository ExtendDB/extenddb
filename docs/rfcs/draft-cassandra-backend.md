# RFC-000X: Apache Cassandra Storage Backend

- Status: Draft
- Author: @jcshepherd
- Created: 2026-09-10
- Tracking issue: TBD

## Summary

This RFC proposes a production-grade Apache Cassandra storage backend for ExtendDB. The design targets teams that
operate Cassandra clusters and want linear horizontal scalability, native multi-datacenter replication, and a peer-to-peer
architecture with no single write bottleneck: a deployment profile the reference PostgreSQL backend does not address.

The non-trivial design work is in conforming to DynamoDB semantics on a database that provides neither multi-row ACID
transactions nor native sequence primitives. This document focuses on those gaps, the decisions made to close them, and
the places where full parity with Amazon DynamoDB is not yet achieved.

## Motivation

ExtendDB's PostgreSQL backend is well-suited for single-node and small-cluster deployments, but PostgreSQL's single-primary
architecture limits write scalability and does not provide native multi-datacenter active-active replication. Teams that
need linear, horizontal write scaling, geographic distribution with active-active writes, or a peer-to-peer architecture
with no single point of failure often already operate Cassandra, and will want DynamoDB API compatibility without adopting
a new infrastructure dependency.

Cassandra is a natural fit for this role. Its data model — partition key for distribution, clustering key for ordering
within a partition — maps directly onto DynamoDB's data architecture. Both systems are designed for high write throughput
with tunable consistency, both treat non-key attributes as schemaless, and both support native multi-datacenter replication.
The design challenges addressed in this RFC arise not from a mismatch between the two systems but from the specific
DynamoDB features — transactions, TTL with Streams integration, eventually consistent GSIs — that require capabilities
Cassandra does not provide natively.

The Cassandra backend is not a replacement for the PostgreSQL backend. It targets a different operational profile:
higher complexity in exchange for horizontal scalability and geographic distribution.

## Detailed Design

### Repository structure

The backend lives at `../../crates/storage-cassandra` in the main ExtendDB repository, following the mono-repo structure
prescribed by RFC-0002. It is selected at build time via a `cassandra` Cargo feature flag on the `extenddb` binary crate.
Backends are mutually exclusive; a build enabling more than one is rejected at compile time.

### Plugin registration

The crate exposes a single `backend()` function returning an `extenddb_storage::Backend` descriptor. The thin `main`
binary calls `extenddb_storage::set_backend(extenddb_storage_cassandra::backend())` before dispatching any subcommand.
The descriptor carries factory functions for bootstrapping, config parsing, settings and diagnostics store access,
and server component construction.

### Data Architecture

The backend uses Cassandra keyspaces to independently manage cross-account 'catalog' data, and individual account-owned
data.

**`{prefix}_catalog`** — system-wide metadata, created during `extenddb init`. Holds accounts, IAM entities (users,
groups, roles, policies, access keys), table definitions, stream shard metadata, idempotency tokens, settings, and schema
migration history.

**`{prefix}_account_{account_id}`** — one keyspace per ExtendDB account, created when the account is created. Holds item
tables (one per DynamoDB table), secondary index tables, stream records, transaction ledger, TTL expiration queues, and
backup payloads. Account deletion drops the entire keyspace, atomically removing all the account's data.

The keyspace-per-account split is primarily an operational decision: it allows independent replication configuration per
account, per-account granularity in Cassandra's backup and monitoring tooling, and scoped schema migrations. Account
keyspaces are created eagerly at account creation time so the schema-agreement cost is paid before the user creates
any ExtendDB resources in their account.

Item tables use typed columns for the primary key (`pk`, and `sk_s`/`sk_n`/`sk_b` for string, number, and binary sort
keys respectively) and a JSON blob for all other attributes. Typed key columns enable Cassandra's native clustering-key
ordering and range scans for sort key conditions. The JSON blob preserves DynamoDB's schemaless attribute model without
requiring schema migrations when items gain or lose non-key attributes.

All data-plane writes and strongly consistent reads use `LOCAL_QUORUM`. Lightweight Transactions (LWT) use Paxos and are
linearizable within a datacenter. Background workers use `LOCAL_ONE` where eventual consistency is acceptable.

### Catalog integrity

The IAM catalog — which has parent-child relationships between accounts, users, roles, policies, and access keys — relies
on referential integrity and transactional mutations across entities to maintain consistency. Cassandra does not support
referential integrity constraints, and Cassandra's logged batches (atomic batch updates) impose significant constraints
on LWT statements (`IF NOT EXISTS`, `IF EXISTS`) within a batch. Duplicate detection and multi-row atomic writes cannot
be composed into a single operation. The Cassandra backend uses two compensating patterns:

**Referential integrity** is enforced via application-layer pre-checks: before inserting a child entity, a quorum read
verifies the parent exists, returning the same error a database FK violation would produce. This is correct on the normal
path. The narrow exception is a concurrent deletion race: if a parent entity is deleted in the window between the
pre-check read and the child insert, an orphaned child record can be created. This window is milliseconds on a local
cluster, and IAM deletions are rare admin-driven operations, so the practical risk is low. Orphaned records are cleaned
up when the parent account is deleted (via `DROP KEYSPACE`). This is a behavioral difference from DynamoDB, where IAM
operations are atomically enforced at the service layer. See [ADR-0011](../adr/0011-cassandra-foreign-key-emulation.md).

**Uniqueness and multi-row atomicity** for operations that must both detect duplicates and write multiple rows (e.g.,
creating a user, which writes both a user record and an initial policy row) use a check-then-batch pattern: a SELECT
checks for duplicates, then a logged batch writes all rows atomically. Because LWTs across partitions cannot be part of
the same logged batch execution, the duplicate check and the write are separate operations, introducing a narrow race
window between them. Again, IAM operations are infrequent enough that the collision probability is negligible. See [ADR-0012](../adr/0012-cassandra-transaction-atomicity-patterns.md).

### Transactions

DynamoDB's `TransactWriteItems` requires serializable isolation: all operations succeed or all fail, concurrent transactions
on overlapping items are serialized, and in-progress state is never visible to concurrent transactions. Cassandra logged
batches provide atomicity but not isolation: two concurrent batches can interleave freely, and there is no mechanism to
block a non-transactional write to an item that a transaction is currently preparing.

The implementation uses a two-phase commit protocol with per-item intent markers and LWT for conflict detection. The full
protocol is specified in [ADR-0017](../adr/0017-cassandra-transaction-implementation.md); the three decisions worth highlighting here:

**Intent markers via LWT.** Each item row carries a `prepared_txn_id` column. The PREPARE phase claims each item atomically
using an LWT conditioned on `prepared_txn_id IS NULL`. If any claim fails, because another transaction or a non-transactional
write holds the item, the transaction cancels and all claimed items are released. Non-transactional writes (`PutItem`,
`UpdateItem`, `DeleteItem`) check the same column and return `TransactionConflictException` if it is set. Read operations
return the last committed value and ignore the marker entirely, matching DynamoDB's read-committed visibility for
non-transactional reads.

**Durable ledger.** Before touching any item, a ledger entry is written to `transaction_ledger` in the account keyspace
containing the complete write set. The ledger lives in the account keyspace — not the catalog — because transactions are
scoped to a single account's data, and co-locating the ledger with the data it describes means it is dropped atomically
with the account. The ledger is written in two phases: key information before PREPARE begins, full computed write data
after all claims succeed and before transitioning to COMMITTING. A transaction in COMMITTING state therefore always has
complete write data available for recovery.

**Recovery worker.** A background worker scans the transaction ledger every 30 seconds for transactions older than
60 seconds. Transactions in COMMITTING state are resumed; transactions in PREPARING state are rolled back. All recovery
operations are idempotent. LWT conditions handle items already committed or rolled back by a concurrent recovery pass.

### Secondary indexes (GSI/LSI)

**LSIs** are updated synchronously in the same (atomic) logged batch as the base table write. The batch deletes the old
index row (if the item previously existed and had values for the index key attributes) and inserts the new one. Items
without values for the index key attributes are omitted, consistent with DynamoDB's sparse index behavior.

**GSIs** are eventually consistent by design in DynamoDB, and the implementation reflects this. Similarly to the reference
PostgreSQL backend, a queue entry is written atomically in the same logged batch as the base write, ensuring no update
is silently lost even if the server crashes immediately after the batch commits. A background worker processes queue
entries and applies the index update.

**Index table primary key structure** requires care. A naive translation of the base table's key schema would place the
index sort key as a regular column, but Cassandra range scans require the sort key to be a clustering key. The index
table PRIMARY KEY is therefore `((index_pk), index_sk_*, base_pk, base_sk_*)`: the index partition key as the partition
key, index sort key columns as leading clustering keys (enabling range scans for `KeyConditionExpression`), and base
table key columns as trailing clustering keys to ensure each base item appears at most once even when multiple items
share the same index key values. DELETE operations on the index must supply all primary key components (index keys and
base keys) which differs from what a direct port of the base table delete logic would expect. See [ADR-0016](../adr/0016-cassandra-index-table-primary-key-structure.md).

### DynamoDB Streams

**Sequence numbers.** DynamoDB Streams requires sequence numbers that are strictly ordered and comparable within a shard.
Cassandra has no sequence primitive; its `counter` implementation is not idempotent and cannot participate in a logged
batch. The ExtendDB Cassandra backend generates stream sequence numbers using a Hybrid Logical Clock (HLC): a numeric
string combining a millisecond wall-clock timestamp, a per-node logical counter (for multiple events within the same
millisecond), and a node identifier (for concurrent writes across server nodes). Lexicographic string comparison of HLC
values matches temporal order within a shard. The node identifier is derived from a hash of the host and port, requiring
no configuration and remaining stable across restarts. See [ADR-0018](../adr/0018-cassandra-stream-implementation.md) and the [HLC paper](https://cse.buffalo.edu/tech-reports/2014-04.pdf) for details.

**Atomic writes.** Stream records are written in the same logged batch as the data modification, guaranteeing that a
stream record is emitted if and only if the data write commits.

**Retention.** Stream records are stored with a native Cassandra row-level TTL (30 hours, providing a buffer above
DynamoDB's 24-hour minimum). Native TTL is appropriate here because stream record expiry carries no observable DynamoDB
semantics, unlike item TTL expiry, which must emit a `REMOVE` stream record and is addressed in the next section.

### Time to Live

DynamoDB TTL deletions are observable: they must emit `REMOVE` stream records with a service identity (`dynamodb.amazonaws.com`),
clean up secondary index rows, and respect the item's current TTL value at delete time: an item whose TTL attribute has
been updated or removed since expiry was scheduled must not be deleted. Cassandra's native row-level TTL evicts rows
silently at the storage layer with no application hook, making it unsuitable for DynamoDB item TTL.

The implementation uses an application-layer expiration worker backed by a durable, generation-fenced expiration queue.
When TTL is enabled on a table, the backend scans all items and registers those with a valid TTL timestamp into a queue
partitioned by `(table_id, generation, expiry_day, key_shard)`. A background worker sweeps due entries, re-reads each
item to verify it is still expired and unchanged, then deletes it through the normal delete path, including emitting stream
records and cleaning up index rows as part of the deletion. The full state machine, recovery protocol, and operational
constraints are specified in [ADR-0010](../adr/0010-cassandra-ttl-expiration-queue.md).

Three constraints are worth calling out explicitly:

**Enabling TTL on a table that is already serving writes requires a brief write quiesce.** The expiration queue is built
by scanning the table at enable time, and writes that began before TTL was enabled cannot be retroactively enrolled in
the new generation. New tables can enable TTL before accepting traffic and avoid this entirely.

**TTL cannot be enabled on a table with asynchronously propagated GSIs.** The async GSI queue has no version-conditional
replay fence, so a stale TTL delete could overtake a recreated item's GSI insert. Base tables, LSIs, and synchronous GSIs
are supported.

**Expiration throughput is bounded.** The worker processes up to 100 items per table per 60-second cycle under a per-table
sweep lease. This matches the throughput of the other backends and is sufficient for typical workloads; tables with sustained
high expiration rates will accumulate backlog.

### Backup and restore

Cassandra's native snapshot mechanism (`nodetool snapshot`) produces node-local SSTable artifacts managed outside CQL.
Creating, enumerating, or restoring a distributed snapshot requires cluster-level orchestration that is not available
through the driver session, so the backend implements on-demand backups as logical snapshots instead.

`CreateBackup` scans the source table and writes item payloads into a `backup_items` table in the account keyspace,
partitioned to bound individual partition size. Backup metadata is written in two phases: a `CREATING` row first, then a
transition to `AVAILABLE` only after all item chunks are persisted. An interrupted backup is never visible to callers
as available. `RestoreTableFromBackup` creates the target table through the normal path and writes items back through
the normal item write path, so typed key columns and index rows are reconstructed correctly. See [ADR-0019](../adr/0019-cassandra-logical-backup-restore.md).

The first implementation restores base table schema and items. GSI/LSI definitions, tags, TTL configuration, and stream
history are not restored; this is a known gap.

Backup creation and restore execute synchronously within the API request. This is consistent with the current behavior
of all ExtendDB backends, but differs from Amazon DynamoDB, where these are asynchronous operations. The same applies
to `ExportTableToPointInTime` and `ImportTable`. Moving these to background jobs is planned future work.

## 4. Behavioral Differences from DynamoDB

The following behaviors differ from Amazon DynamoDB. The complete list across ExtendDB is maintained in
`../differences-from-dynamodb.md`.

**IAM catalog consistency.** Referential integrity in the IAM catalog is enforced via application-layer pre-checks rather
than atomic database constraints. A narrow race window exists on concurrent parent-entity deletion; see the Catalog
integrity section.

**TTL: brief effects-before-delete window.** Because Cassandra cannot atomically combine a conditional write on the base
item with mutations in other partitions, secondary index cleanup and stream record emission become durable immediately
before the final base item delete. A brief window exists where index rows are removed but the base item is still present.
This is not visible to normal read operations but is observable if the server crashes in that window and the item is read
before recovery completes.

**TTL: not supported alongside asynchronously propagated GSIs.** Enabling TTL on a table with async GSIs returns an error.
See the TTL section.

**TTL: write quiesce required when enabling on a live table.** See the TTL section.

**Backup is not a strict point-in-time image.** Logical backup scans the table without a cluster-wide snapshot; concurrent
writes can race the scan. `RestoreTableToPointInTime` is not supported.

**Data movement operations are synchronous.** `CreateBackup`, `RestoreTableFromBackup`, `ExportTableToPointInTime`,
and `ImportTable` execute synchronously and block the API response until complete. Amazon DynamoDB performs these
asynchronously.

**Transaction recovery is conservative.** Transactions found in PREPARING state during recovery are always rolled back,
even if they could theoretically have been committed. This is safe but means a transaction that completed PREPARE and
then encountered a server crash will be rolled back rather than committed.

## Known Gaps and Future Work

**TTL expiration throughput does not scale horizontally.** The sweep lease is per table; the queue is already sharded 64
ways by key, and those shards are disjoint partitions. Leasing per `(table, shard)` would allow up to 64 concurrent workers
per table with no change to the claim protocol. This is the highest-value follow-up.

**TTL backfill has no durable cursor.** A failure during `UpdateTimeToLive` restarts the table scan from the beginning,
and the API call blocks for its duration. Large tables will need a durable, checkpointed background backfill before this
path is suitable at scale.

**Backup and restore are synchronous.** Moving `CreateBackup`, `RestoreTableFromBackup`, `ExportTableToPointInTime`, and
`ImportTable` to background jobs with status polling is planned but not yet implemented. This applies to all current
ExtendDB backends.

**Restore does not recover GSI/LSI definitions, tags, TTL configuration, or stream history.**

**Transaction fence uses item data equality.** The TTL deletion protocol conditions the final base item delete on the
stored item image rather than a version counter. A monotonic version column would be a more robust fence and remove
the dependency on stable JSON encoding.

**Transaction recovery does not reconcile pre-commit images.** The transaction ledger does not persist the pre-commit
item image, so a transaction that crashes between COMMIT and TTL queue reconciliation can leave the item's previous
expiration entry in the queue until it comes due. The entry is inert — the worker revalidates before deleting — but it
is queue garbage.
