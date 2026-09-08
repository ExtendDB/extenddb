# RFC-NNNN: TiDB storage backend

- Status: Draft
- Author: @ngaut
- Created: 2026-05-28
- FCP ends: TBD
- Tracking issue: #141

## Summary

Add TiDB as a first-class, optional ExtendDB storage backend. The backend uses
TiDB's MySQL-compatible SQL surface for normal DynamoDB operations and delegates
database-native responsibilities, such as secondary index maintenance,
transactional snapshots, TTL cleanup, and physical backup/restore, to TiDB
instead of reimplementing them in ExtendDB.

## Motivation

ExtendDB currently has one production storage backend. PostgreSQL is a good
default for local development and small installations, but some operators need a
horizontally scalable SQL store with MySQL protocol compatibility, distributed
transactions, online DDL, native TTL, and native backup/restore. TiDB is a good
fit for that operator profile while preserving ExtendDB's DynamoDB-compatible
wire protocol.

The target users are:

- teams that already operate TiDB and want DynamoDB-compatible APIs on top of
  their existing database platform;
- teams that need storage growth beyond a single-node PostgreSQL deployment;
- CI and self-hosted users who need a MySQL-compatible backend option without
  changing AWS SDK clients.

The proposal is intentionally backend-additive. Existing PostgreSQL deployments
continue to work without data migration or behavior changes.

## Detailed design

### Scope

This RFC proposes:

- a new optional `extenddb-storage-tidb` crate;
- feature-gated binary wiring, for example `--features tidb`;
- backend registration through the existing storage abstraction;
- TiDB-specific catalog, data, worker, stream, and backup implementations;
- backend-neutral public storage configuration and CLI initialization flags;
- documentation for TiDB installation, configuration, backup/restore, and
  operational constraints.

This RFC does not propose:

- changes to the DynamoDB wire protocol;
- dual-write or online migration from PostgreSQL to TiDB;
- changing the default backend from PostgreSQL;
- implementing a generic SQL backend that hides important PostgreSQL and TiDB
  differences behind one leaky implementation.

### Backend selection

ExtendDB should treat each backend as an explicitly registered implementation.
The binary chooses a backend from config:

```toml
[storage]
backend = "tidb"

[storage.tidb]
connection_string = "mysql://extenddb:password@127.0.0.1:4000/extenddb_catalog"
pool_size = 20

[storage.tidb.backup]
binary = "tiup"
component = "br"
pd_endpoint = "127.0.0.1:2379"
storage_uri = "s3://example-bucket/extenddb-backups/"
send_credentials_to_storage_nodes = false
```

If a build contains only one backend, that backend may remain the implicit
default. If multiple backends are compiled in, explicit `storage.backend`
selection should be preferred so deployments are not surprised by build-feature
order.

### Public CLI and configuration shape

Initialization should use backend-neutral flags:

```text
extenddb init \
  --storage-backend tidb \
  --storage-host 127.0.0.1 \
  --storage-port 4000 \
  --storage-admin-user root \
  --storage-admin-password "$TIDB_PASSWORD"
```

Backend-specific details belong in the backend's config factory and
bootstrapper. Shared CLI code should pass typed bootstrap options, not
PostgreSQL-shaped argument vectors. This keeps the shared storage contract
honest as more backends are added.

Open question: whether legacy PostgreSQL-specific aliases such as `--pg-host`
should remain as deprecated aliases for one release, or whether the first TiDB
release is allowed to normalize directly to `--storage-*`.

### Storage trait impact

The existing `TableEngine` contract remains the primary DynamoDB data-plane
interface. TiDB should implement the same operation-level semantics as the
PostgreSQL backend:

- account-scoped table namespaces;
- conditional writes;
- batch operations;
- transactions;
- query and scan pagination;
- streams where enabled;
- TTL APIs;
- import/export and backup/restore surfaces where supported.

Backend-native capabilities should be modeled as explicit storage
configuration or backend methods when the abstraction needs to expose an
operator-visible capability. They should not be hidden as special cases in the
engine layer.

### TiDB schema model

The TiDB backend owns its physical schema. It should not copy PostgreSQL DDL or
try to preserve PostgreSQL table layout. A practical layout is:

- catalog and management tables in the configured catalog database;
- one physical data table per DynamoDB table;
- base item storage as JSON plus typed key columns used for key access;
- generated columns for secondary-index key extraction;
- TiDB secondary indexes over generated index-key columns plus the base table
  key to support stable pagination.

This keeps each DynamoDB item stored once. TiDB maintains secondary index
entries transactionally as part of the base table write. ExtendDB should still
validate DynamoDB index-key type rules before writing malformed index-key
attributes, but it should not maintain separate GSI item tables for TiDB.

### Transactions and consistency

TiDB provides distributed transactions through its own transaction manager. The
backend should rely on TiDB transactions for multi-row metadata updates,
conditional writes, and DynamoDB transaction APIs rather than introducing
application-level lock tables for correctness.

For point-in-time reads, export, and backup metadata capture, the backend should
use TiDB snapshot semantics where a timestamp-level snapshot is required. This
preserves global consistency without blocking writers.

### Secondary indexes

TiDB secondary indexes are native database indexes. ExtendDB should map
DynamoDB GSI and LSI query requirements to TiDB-native indexes instead of
writing custom index rows. The proposed mapping is:

- create virtual generated columns for each index key component;
- create a TiDB index over those generated columns;
- include base table key columns as tie-breakers for deterministic query
  pagination;
- use `IS NOT NULL` predicates to preserve DynamoDB sparse-index behavior;
- let TiDB online DDL and index backfill handle index creation.

The initial schema should not introduce TiDB table partitioning. If a later RFC
adds partitioned physical tables, the index DDL must explicitly request TiDB
global indexes wherever DynamoDB query semantics require one table-wide index.

This reduces write amplification and removes custom index-consistency code from
the backend.

### TTL

TiDB has native table TTL. The TiDB backend should use native TTL for internal
tables and user data tables when DynamoDB stream REMOVE records are not required
for expired items.

When a table has streams enabled and ExtendDB must emit DynamoDB-compatible
REMOVE records for TTL expiration, the backend may need an ExtendDB worker path
for that table. That should be an explicit semantic requirement, not the default
cleanup implementation for all TiDB tables.

Operators must know that TiDB TTL deletion is asynchronous. The DynamoDB
contract already allows TTL expiration to be asynchronous, so this is compatible
as long as ExtendDB reports TTL status accurately and documents the stream
record caveat.

### Backup and restore

TiDB backup/restore should use TiDB's native BR capability rather than a
logical item-copy backup table. The backend should:

- store DynamoDB backup metadata in ExtendDB catalog tables;
- invoke TiDB BR or the SQL `BACKUP`/`RESTORE` surface for physical data;
- record the native backup location and snapshot timestamp/TSO;
- reject backup shapes that cannot preserve DynamoDB semantics instead of
  silently falling back to a lossy logical path;
- document operator requirements for PD access, storage URI permissions, and
  BR version compatibility.

The initial implementation may use the BR command line through `tiup br` because
it works across current TiDB deployments. A future implementation can switch to
SQL `BACKUP`/`RESTORE` where the TiDB version and deployment mode support it.

### Streams

Streams remain an ExtendDB responsibility because DynamoDB stream records are a
wire-protocol feature, not a TiDB feature. The TiDB backend should write stream
records in the same transaction as the item mutation when streams are enabled.

TTL is the main exception: native TiDB TTL does not call ExtendDB code for each
deleted item. Therefore, tables that require DynamoDB-compatible stream records
for TTL deletes need either an ExtendDB-managed TTL path or a separately
accepted design for consuming TiDB change data safely.

### Bootstrap and migrations

The TiDB backend should provide its own bootstrapper and migrations:

- create the catalog database if requested by `extenddb init`;
- create the runtime database objects needed by the backend;
- run schema migrations idempotently;
- support `extenddb migrate` for existing TiDB deployments;
- keep PostgreSQL bootstrap behavior isolated in the PostgreSQL backend.

Shared storage bootstrap code should carry only backend-neutral typed options,
such as host, port, admin user, admin password, database prefix, and requested
backend. Backend-specific validation and DDL generation belong to the backend.

### Testing

The TiDB backend needs tests at three layers:

- unit tests for connection parsing, key encoding, DDL generation, native backup
  command construction, and unsupported-shape validation;
- Rust backend tests for table lifecycle, item operations, query/scan,
  transactions, TTL mode selection, and backup metadata;
- integration tests against a real TiDB cluster for online DDL, generated-column
  indexes, transaction behavior, TTL cleanup, and BR backup/restore.

PostgreSQL tests should continue to run unchanged. Tests should not require real
cloud credentials; backup tests can use local or test-cluster storage URIs where
TiDB supports them.

### Rollout

The proposed rollout is:

1. Accept this RFC.
2. Land backend-neutral storage registration and bootstrap option cleanup.
3. Add the TiDB crate behind an opt-in Cargo feature.
4. Add documentation and sample config.
5. Add real TiDB integration coverage to CI when maintainers choose the
   supported TiDB deployment mode.
6. Consider enabling TiDB in release binaries after operational docs and CI are
   mature.

## Drawbacks

Adding TiDB increases maintenance cost. ExtendDB maintainers will need to review
two storage implementations, keep storage trait behavior precise, and decide how
much TiDB operational surface belongs in ExtendDB docs.

TiDB also introduces operational requirements that PostgreSQL users do not have,
including PD endpoints, TiKV nodes, BR version compatibility, backup storage
permissions, and distributed DDL behavior.

The main design risk is accidental abstraction leakage. If shared code assumes
PostgreSQL details, TiDB support will become fragile. If shared code hides all
database differences, both backends will become less idiomatic. The RFC favors a
small shared contract and backend-owned implementation details.

## Alternatives

### Keep PostgreSQL as the only backend

This is simplest for maintainers, but it leaves users who need TiDB's scale and
MySQL-compatible operations without a path.

### Build a generic SQL backend

A generic SQL backend sounds attractive, but PostgreSQL and TiDB differ in DDL,
JSON indexing, backup/restore, TTL, locking, generated columns, and operational
semantics. A generic backend would either become full of conditionals or avoid
native database strengths. Separate backend crates are clearer.

### Store custom GSI rows in TiDB

This would copy patterns that are useful for some storage engines but unnecessary
for TiDB. Native secondary indexes reduce write amplification and keep index
maintenance in the database transaction layer.

### Implement logical backup/restore in ExtendDB

Logical item-copy backup is portable, but it is slower and less faithful than
TiDB's native physical backup/restore. For TiDB, native backup should be the
default. Unsupported cases should fail explicitly.

### Use TiCDC for streams

TiCDC may be useful in the future, but DynamoDB streams require precise record
shape and ordering semantics. This RFC keeps streams in ExtendDB until a
separate design proves that TiCDC can satisfy those semantics.

## Unresolved questions

- Should `--pg-*` CLI flags remain as deprecated aliases for one release after
  introducing `--storage-*`?
- Which TiDB deployment mode should CI use: TiUP playground, Docker Compose,
  TiDB Operator, or a hosted test cluster?
- What minimum TiDB version should ExtendDB support?
- Should the initial backup implementation prefer `tiup br` or SQL
  `BACKUP`/`RESTORE` when both are available?
- Should native TTL be enabled for user tables by default when streams are
  disabled, or should operators opt in per deployment until TiDB TTL behavior is
  covered in CI?

## Prior art

- TiDB generated columns can be indexed, which supports extracting DynamoDB
  index keys from JSON item bodies while keeping native index maintenance in the
  database.
- TiDB global indexes are the relevant design point if ExtendDB later adds
  partitioned physical tables.
- TiDB stale read and snapshot behavior supports globally consistent historical
  reads at a selected timestamp, which is useful for point-in-time operations.
- TiDB TTL provides asynchronous native cleanup with documented operational
  limits and tool interactions.
- TiDB BR and SQL `BACKUP`/`RESTORE` provide native distributed backup/restore
  for TiDB clusters, including snapshot and incremental backup support.
- DynamoDB TTL expiration is asynchronous, and DynamoDB streams define the
  externally visible delete-record behavior that ExtendDB must preserve when
  streams are enabled.

References:

- <https://docs.pingcap.com/tidb/stable/generated-columns/>
- <https://docs.pingcap.com/tidb/v8.5/global-indexes>
- <https://docs.pingcap.com/tidb/stable/stale-read/>
- <https://docs.pingcap.com/tidb/stable/time-to-live/>
- <https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/>
- <https://docs.pingcap.com/tidb/stable/sql-statement-backup/>
- <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html>
- <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html>

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
