# RFC-0000: Couchbase Storage Backend

- Status: Draft
- Author: @suravarjjala
- Created: 2026-11-04
- FCP ends: —
- Tracking issue: ExtendDB/extenddb#1183
- Acceptance rubric: upstream RFC-0003 "Backend Acceptance Criteria" — every criterion is mapped in the section "RFC-0003 acceptance criteria" below
- Reference implementation: https://github.com/couchbaselabs/extenddb-couchbase (private today; read access for this RFC's reviewers is granted on request, and the repository is opened before the Final Comment Period)

## Summary

This RFC proposes Couchbase Server as a storage backend for ExtendDB, shipped as
**Couchbase Converge for DynamoDB Workloads** (crate `extenddb-storage-couchbase`,
binary `extenddb-couchbase`). It lets DynamoDB clients — the AWS CLI, every
language SDK, PynamoDB, boto3, the Node.js and Go SDKs — run unmodified against a
Couchbase cluster. The backend implements all 13 traits in ExtendDB's storage
abstraction (every mandatory and every optional trait in RFC-0002), installs
itself through `extenddb_storage::Backend` from a thin `extenddb` bin with no
changes to the engine, server, or auth crates, and passes the upstream
conformance suite: **912
passed, 1 xfail (upstream-marked)** top-level and **331 / 331** parity. Items
are stored as native
Couchbase JSON with the business attributes at the document top level and one
reserved `_xdb` object for the machinery, so the same documents are usable from
SQL++, Search, Analytics, Eventing, Mobile, and XDCR. Transactions run natively
over the Couchbase query service in Rust with no sidecar. Couchbase intends to
own and maintain the backend crate under RFC-0002's contribution process. The
backend was built against ExtendDB `v0.1.x`; this RFC also scopes its
adaptation to the `Storage` contract on `main` today, including the code-based
catalog and data migrations every in-tree backend now carries.

## Motivation

ExtendDB's premise is one DynamoDB-compatible API over several storage backends.
Couchbase is a natural additional target for three reasons.

**Data model.** Couchbase stores schema-less JSON documents addressed by key,
with a memory-first KV engine for point operations and SQL++ (N1QL) for
secondary access. A DynamoDB item maps to one document, a `GetItem` to one KV
get, a `Query` to one indexed SQL++ range. There is no relational mapping, and
unlike a DynamoDB-JSON envelope, the stored document is the item: `amount` is a
number, `active` is a boolean, `tags` is an array.

**Operational fit.** DynamoDB applications that must run outside an AWS
region, whether by customer preference, by design strategy, or by regulation,
benefit from a choice of ExtendDB storage backends. Couchbase adds a
memory-first, horizontally scalable backend with built-in replication to that
choice. Because the stored
documents are native Couchbase JSON, the same deployment can extend to the edge:
Couchbase cross-datacenter replication (XDCR) can keep a DynamoDB-API dataset
replicated to an edge cluster that stays consistent and keeps serving through
network failures (`docs/couchbase-side-operations.md` covers XDCR on this
data). The replication, rebalance, auto-failover, backup, and multi-dimensional
scaling (KV, index, query, and search on separate nodes) that such deployments
need come from Couchbase, not from the API tier.

**Evidence the problem is real.** The backend has been built and validated end to
end before this RFC. Every `docs/...`, `scripts/...`, `tests/...` and
`CONFORMANCE.md` path in this document is a file in
https://github.com/couchbaselabs/extenddb-couchbase; the figures are repeated
here so the RFC can be read without it:

- Conformance: top-level **912 passed, 1 xfail** (upstream's own expected-fail
  marking), parity **331 / 331** (one v3.1-stack run recorded 330 / 331, the
  known read-lag flapper passing in isolation), the 30-test web-console suite,
  and 47 asserted demo checks, on the frozen v3.2 storage format
  (`CONFORMANCE.md`).
- QE: a 343-case Couchbase-specific test plan (`docs/qe/testcases.md`) run against
  a real multi-node cluster; every failure traced to code and fixed or filed
  (`docs/qe/qe-report.md`). No acknowledged write was lost in any rebalance,
  failover, node kill, churn, or API-restart test.
- Performance: YCSB through the same binding measures workload A at 2,822 ops/s
  on the 5K-record baseline and 2,096 ops/s at 1M records, with bulk load at
  ~7,400 inserts/s on the 1M-record run (`docs/ycsb-benchmark.md`). The PERF
  team's multi-node YCSB characterization (Cloud-Tester-Rosetta-Dynamo runs,
  2026-10-22 to 2026-11-01) shows the read-heavy workload B scaling
  near-linearly from 3 to 18 nodes (34K to 164K ops/s) with flat read latency,
  and workload A at 5.7 ms update p99 and 3.0 ms read p99 at 5,000 ops/s (see
  "Performance characteristics").
- Interop: five self-asserting client apps across four languages, plus a
  reverse-ETL path that moves native Couchbase data into DynamoDB tables through
  ExtendDB.

## Detailed design

### Scope

This RFC proposes:

- A Couchbase storage backend crate, `extenddb-storage-couchbase`, implementing
  every ExtendDB storage trait, selected with a `couchbase` Cargo feature.
- Installation through `extenddb_storage::Backend` and `set_backend` from a
  thin `extenddb` bin behind a `couchbase` Cargo feature, with no changes to
  `crates/engine/`, `crates/server/`, `crates/auth/`, or `crates/core/`.
- Code-based catalog and data migrations through `extenddb_storage::migrations`:
  a `MigrationBackend` for the two Couchbase stores, a ledger in each, a
  baseline migration, the shared resource-tag migration, a migration lock and
  serving registrations, in the shape `crates/storage-mongodb` uses (see
  "Catalog and data migrations").
- The adaptation of the existing backend from the ExtendDB rev it was built
  against to the `Storage` contract on `main` (see "Alignment with upstream
  `main`").
- A native-JSON on-disk format (v3.2, frozen for GA) with a documented version
  marker, read-side coexistence of all prior formats, and a `migrate` command.
- Native in-process transactions over the Couchbase query service.
- A production deployment surface: a stateless API tier (Helm chart) against an
  external Couchbase cluster, TLS mandatory, least-privilege Couchbase user.

This RFC does not propose:

- Changes to the DynamoDB wire protocol or API response shapes.
- Changes to the `Storage` trait. The backend will track `main`; the fixes it
  carried on a fork at its current pin (#398's bounded metrics memory, the
  #418–#421 auth cluster) are merged or submitted upstream on their own, not
  through this RFC.
- Couchbase Capella (the hosted service) as a distinct target. Nothing in the
  design is self-managed-only, but Capella has **not** been validated and no
  claim is made for it here.
- Replacing PostgreSQL as the default backend.
- Online migration between backends.

Future work this RFC signals but does not propose: Couchbase is interested in
**bi-directional replication between DynamoDB tables and Converge tables**, so
that one application can run in an AWS region and off-region against the same
data. AWS has said it would be interested in collaborating on this, and
Couchbase wants to design it jointly with AWS and the ExtendDB maintainers
rather than in this backend alone. What exists today is one-way:
`scripts/cb_to_ddb.py` moves native Couchbase data into DynamoDB tables through
ExtendDB, and XDCR replicates between Couchbase clusters. Nothing in this RFC
depends on it; see unresolved question 7.

### Repository structure

Today the backend lives out of tree at `couchbaselabs/extenddb-couchbase` with
ExtendDB as a git dependency pinned to a single rev. The proposed end state
follows RFC-0002: the crate moves to `crates/storage-couchbase/` in the ExtendDB
mono-repo as `extenddb-storage-couchbase`, selected by a `couchbase` feature on
the `extenddb` binary crate in the same mutually-exclusive set as `postgres`,
`mongodb`, `cassandra` and `sqlite` (`crates/bin/Cargo.toml`), with a
`crates/storage-couchbase/` entry in `.github/CODEOWNERS` naming the Couchbase
maintainers. The `postgres` feature remains the default. Following the thin-bin
model, `extenddb_storage_couchbase::backend()` returns the
`extenddb_storage::Backend` the bin installs with `set_backend`, so a
`extenddb-couchbase` image can also be shipped from a copy of `crates/bin` with
one call swapped, as `main.rs` describes.

Module layout (`backend/src/` today, `crates/storage-couchbase/src/` upstream):

| Module | Responsibility |
|---|---|
| `item.rs`, `number.rs` | DynamoDB item ↔ Couchbase document mapping; order-preserving number encoding |
| `engine.rs` | `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `VectorSearchEngine`, `BackupEngine`, `WorkerStore` |
| `catalog.rs` | `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`, `CatalogStore` |
| `credentials.rs`, `authn.rs` | `CredentialStore` (AES-256-GCM access-key secrets) |
| `bootstrap.rs` | `Bootstrapper`, `OperationsEngine` (CLI), `DiagnosticsStore`; scope/collection/index DDL |
| `conn.rs` | Couchbase connection, KV helpers, durability, query-service transactions |
| `leader.rs`, `ttl_worker.rs`, `reindex.rs` | Leader lease and the leader-only background workers |
| `config.rs`, `backend.rs` | `StorageConfig`, `ServerRuntimeHooks`, the `backend()` that builds `extenddb_storage::Backend` |
| `migrations/` | `MigrationBackend`, `CatalogMigrations`, ledgers, lock, serving registrations, `m0001_baseline` (new, see "Catalog and data migrations") |

### Backend installation

The backend was written against the `inventory`-based registration of
`v0.1.x`. On `main` a backend is a value: `backend()` returns an
`extenddb_storage::Backend` whose fields carry the bootstrapper factory, the
`StorageConfig` deserializer for `[storage.couchbase]`, the operations engine
(`init`, `verify`, `migrate`, `destroy`), the settings, management and
diagnostics store factories, the server-components factory, and
`catalog_migrations`, the backend's `CatalogMigrations`. The thin bin installs it
once per process with `set_backend`. The port from `inventory::submit!` blocks
to that struct is mechanical and is part of the alignment work below. No change
to the server, engine, or auth layers is required.

### Bucket layout

One Couchbase bucket holds everything, split into two scopes.

**`catalog` scope** — control-plane state: `tables`, `accounts`, `users`,
`access_keys`, `policies`, `roles`, `groups`, `sessions`, `admins`,
`login_attempts`, `settings` (including the `catalog_version` marker), `metrics`,
`backups`, and `leases` (leader election).

**`data` scope** — user data and data-plane state:

| Collection | Holds |
|---|---|
| `items` | every table's items in the default (`shared`) isolation tier, told apart by `_xdb.tid` |
| `t_<tid>` (tier 2) / `acct_<id>.t_<tid>` (tier 3) | per-table or per-account placement on the isolation ladder (§ "Multi-tenancy") |
| `stream_records`, `stream_seq` | DynamoDB Streams records and per-table sequence counters |
| `idempotency` | `TransactWriteItems` client request tokens (10-minute retention) |
| `txn_atr` | query-service transaction records (created on first use) |
| `txn_leases` | transaction intent leases (10 s expiry) that plain writes check before touching a leased item |

`init` creates scopes, collections, and the indexes the read paths ride; `migrate`
is idempotent and catalog-only; `verify` is the deep readiness probe; `destroy`
tears the layout down. In-tree, the layout is created and evolved by the
migrations below rather than by hand-written `init` steps.

### Catalog and data migrations

Out of tree the backend records a single `catalog_version` document in
`catalog.settings` and a `resource_tag_migration` marker, and `migrate --yes`
runs an idempotent, catalog-only list of steps (index rebuilds, stream indexes,
secret re-encryption, the resource-tag condition-key rewrite). `main` replaced
that model for every backend with `extenddb_storage::migrations`: one numbered
`MigrationId` list shared by all backends, a ledger in every store, a runner,
a migration lock, and serving registrations. The Couchbase backend adopts it
as follows, mirroring `crates/storage-mongodb/src/migrations/`.

- **Stores.** `StoreKind::Catalog` is the `catalog` scope and `StoreKind::Data`
  the `data` scope of the one bucket. Each gets a ledger collection
  `extenddb_migrations` with the shared `LedgerColumns`; the catalog version is
  read from the ledgers, never from a settings document. The legacy
  `catalog_version` and `resource_tag_migration` documents are read once by the
  baseline body to adopt a released deployment, then deleted in the step that
  records it.
- **Table.** `migration()` is the exhaustive `match` from every `MigrationId`
  to a `Migration` for each store, with no wildcard arm, so the crate fails to
  compile when the registry gains an id this backend has no entry for.
  `RELEASED` pins the checksums of released bodies and the crate runs
  `check_released` as a test, exactly as the other backends do.
- **Bodies.** `m0001_baseline` (catalog): the `catalog` scope, its collections
  and primary indexes, `extenddb_servers`, `extenddb_migrations`.
  `m0001_baseline` (data): the `data` scope, `items`, `stream_records`,
  `stream_seq`, `idempotency`, `txn_atr`, `txn_leases`, the base index
  `idx_items_tid_pk_sk`, the partial `idx_items_untid`, `idx_stream_shard_seq`
  and `idx_stream_ts`, with no primary index on `items` or `txn_atr` (see
  "Bucket layout"). Every request is idempotent: `CREATE ... IF NOT EXISTS`,
  "already exists" accepted by code and left to the post-check, any other
  error stops the body so nothing is recorded. The post-check compares
  `describe_store` (scopes, collections, index definitions) with
  `migrations/catalog/0001_baseline.expected.json` and
  `migrations/data/0001_baseline.expected.json` the first time the baseline is
  applied to a store. `m0002_resource_tag_condition_keys` is the shared body
  from `extenddb_storage`, run against the `policies` collection through the
  backend's `HasManagementStore`.
- **Recording.** Couchbase KV has no multi-document transaction, so a body and
  its ledger row are not one commit. The backend uses the Cassandra pattern:
  bodies are safe to run again, the ledger row (and in the catalog the version
  row) is written after the body with majority durability through
  `LedgerWrites` and `record_rows`, and a crash between the two re-runs the
  body on the next `migrate --yes`.
- **Lock and registrations.** The migration lock is the `migration_lock`
  document in `catalog.settings`, taken with a KV insert and renewed by CAS
  under a document expiry, so a migrator that dies releases it within the
  lease. Servers hold a serving registration document in
  `catalog.extenddb_servers` with the same expiry-and-renew discipline
  (`ServingRegistrar`); `migrate --yes` waits for live registrations, and a
  server whose registration lapses refuses requests, registers again, re-reads
  the catalog status, and serves only if the status it started with still
  holds. Every store request checks the lock's lease first and is bounded by a
  request timeout, as the MongoDB backend does.
- **Commands.** `CouchbaseCatalogMigrations` implements `CatalogMigrations` and
  is installed in `Backend.catalog_migrations`, so `init`, `migrate --yes`,
  `migrate --forget` and `verify` reach the Couchbase bodies through the shared
  commands, and `serve` registers and checks status before opening its engine.
- **Format history.** In-tree, the baseline *is* v3.2: there are no released
  in-tree versions to migrate from. The out-of-tree dual reader for v3.1
  array-form sidecars and the pre-3.1 base-index rebuild remain in the engine
  for existing deployments; a later convergent data migration that rewrites
  any remaining v3.1 sidecars to map form would let the dual reader retire,
  and is proposed, not scheduled.
- **devtools.** `check-couchbase-catalog-lock`, `check-couchbase-older-binary`
  and `check-couchbase-schema` are added next to their MongoDB and Cassandra
  counterparts, and `run-couchbase-tests` stands up a Couchbase container for
  the crate's live tests.

### Document structure for DynamoDB items

Each item is one JSON document. The business attributes sit at the top level as
native JSON; one reserved object, `_xdb`, carries the machinery:

```json
{
  "pk": "a", "status": "open", "amount": 250.50, "tags": ["x", "y"],
  "_xdb": {
    "tid": "863d1c610c4054b9",
    "pk": "S#a", "sk": "N#21001",
    "_g80d88d5epk": "S#open", "_g80d88d5esk": "N#2100325050",
    "ddb": { "/tags": "ss" }
  }
}
```

- `_xdb.tid` is the table id, the partition every index and query is scoped by.
- `_xdb.pk` / `_xdb.sk` are the **order-encoded** key values the base index
  sorts on. Numbers use an order-preserving string encoding (`number.rs`) so range
  scans are exact at DynamoDB's full 38 significant digits rather than capped at a
  double's 15–17 or `Decimal128`'s 34. Strings compare byte-wise; binary compares
  as unsigned bytes, matching DynamoDB.
- `_xdb._g<slot>pk` / `_xdb._g<slot>sk` are the derived GSI and LSI keys, one slot
  per index (name-hashed), maintained on every write.
- `_xdb.ddb` is the **type sidecar**: a map from JSON-Pointer path to the DynamoDB
  type JSON cannot express on its own — string, number, and binary **sets** (stored
  as arrays), **binary** (base64 string), and numbers too wide for a double (kept
  as an exact string). A plain item has no sidecar and is 100% ordinary JSON.
- `_xdb._v<i>` holds vector attributes for vector indexes; `_xdb.body` deep-wraps
  bodies nested beyond the depth Couchbase will index.
- `_xdb` is the **only** reserved top-level key. A business attribute itself named
  `_xdb` is shadowed under `_xdb.shadow`. The format signal is the presence of
  `_xdb`; there is no separate version field.

The document key is `<table_id>#<pk_enc>|<sk_enc>`, SHA-256-hashed when it
exceeds Couchbase's key length, and built only by `full_doc_key` so a string key
value containing the separator cannot forge a boundary (covered by
`tests/test_encoding_edges.py`).

The format is **v3.2**, recorded as `catalog_version = "3"`. All earlier formats
(v1 DynamoDB-JSON envelope, v2 `item`-wrapped, v3 / v3.1) remain readable by a
dual reader, so old and new documents coexist through any migration without a
rewrite. `docs/runbooks/storage-format.md` is the source of truth, including the
two forward-only rollback edges.

### Reads: GetItem, Query, Scan

**GetItem** is a KV binary get (`Collection::get`): no query-service hop,
sub-millisecond, always strongly consistent. **BatchGetItem** fans out KV gets.

**Query** becomes a SQL++ range over the base index
`idx_items_tid_pk_sk (_xdb.tid, _xdb.pk, _xdb.sk)`; sort-key conditions
(`=`, `<`, `BETWEEN`, `begins_with`) translate to bounds on the encoded `_xdb.sk`.
Index queries use the per-index GSI on `_xdb._g<slot>pk/sk`. `begins_with` on a
**numeric** sort key is refused with an explicit error rather than returning
positional nonsense (real DynamoDB rejects it too). Pagination carries the
encoded keys in `LastEvaluatedKey`.

**Scan** is a SQL++ scan of the table's partition; a parallel-scan segment is
selected by hashing the encoded primary key (`SHA-256` of `_xdb.pk` and
`_xdb.sk`, modulo `TotalSegments`), shared between the scan and the
`ExclusiveStartKey` check. Filter expressions and projections are
applied by `extenddb-core` after the read; the backend deliberately does no
filter pushdown, so a correct-but-slower path is still correct.

**Scan consistency.** `Query`/`Scan` run at `request_plus` (read-your-writes) by
default. `EXTENDDB_SCAN_CONSISTENCY=eventual` drops them to `not_bounded`,
matching DynamoDB's eventually-consistent default; on a 1M-row table under
concurrent writes this raised YCSB workload E scan throughput up to 8.7×. Point
reads and all metadata reads stay strong regardless.

### Writes and conditional writes

Writes go through the KV layer, which carries the durability level
(`EXTENDDB_DURABILITY`, default `majority`; `none` only for single-node demos).
Conditional writes are CAS-guarded: the backend reads the current document with
its CAS, evaluates the DynamoDB condition in Rust (`extenddb_core::expression`),
and replaces with the CAS it read; a CAS mismatch re-reads and retries with
jittered backoff, bounded by attempts and a wall-clock deadline, and surfaces a
retryable `TransactionConflict` on exhaustion rather than a 500.

**UpdateItem** has a sub-document fast path, taken only for the narrow,
provably-correct case: `ReturnValues NONE`, no condition, no stream, every action
a `SET` of a bare placeholder holding a scalar (on a v3.2 table: number, string,
boolean, null, binary, or wide number; on a pre-v3.2 table only a native-range
number, boolean, or null) or a `REMOVE`, and every touched path a simply-named
top-level attribute that is not a key, index, or vector attribute. That update
becomes a single `MutateIn` (two round trips instead of three); on v3.2 the same
`MutateIn` writes the attribute's `_xdb.ddb` pointer, so the new value's type is
pinned regardless of what the attribute held before. Anything else falls back to
the CAS read-modify-write loop,
which rebuilds the sidecar. Every update path re-checks the 400 KB item limit.
Writes to stream-enabled tables and all transactional writes take the
query-service transaction path below so the stream record and the data commit
together.

### Global and Local Secondary Indexes

An index does not get its own collection. Its keys are derived into the item
document (`_xdb._g<slot>pk/sk`) on every write, and one Couchbase GSI per
DynamoDB index is created over those columns, scoped by `_xdb.tid` so that in
shared mode an index sees only its own table's items (the unscoped form was a
real defect, fixed; it is now the storage-invariant lint's job to keep it fixed).
Index queries are therefore strongly consistent with the base write at
`request_plus`, with no secondary write to keep in sync.

`CreateTable` with many indexes issues deferred `CREATE INDEX` statements and a
single `BUILD INDEX`, cutting a 20-GSI create from 97 s to 26 s. `UpdateTable`
GSI creation writes the catalog row as `CREATING` and lets the leader-only index
reconciler backfill `_xdb._g<slot>` for existing items and flip the row to
`ACTIVE`; index DDL never runs on the request path. `EXTENDDB_INDEX_NUM_REPLICAS`
sets `num_replica` on every index the backend creates, failing closed when the
cluster has too few index nodes. Multi-part RANGE keys are supported for
bare-partition and single-condition queries; two or more range conditions are
gated on a one-line upstream fix (tracked in `GA-CHECKLIST.md`).

### Transactions

The Couchbase Rust SDK ships no distributed multi-document KV transactions, but
the **query service** exposes ACID transactions to every client
(`BEGIN WORK` / statements / `COMMIT WORK`). `TransactWriteItems`, serializable
`TransactGetItems`, and every write to a stream-enabled table run as one such
transaction **in process, in Rust**; there is no sidecar and no dependency beyond
Couchbase. All statements of one transaction must execute on the node that ran
`BEGIN WORK`, so the engine pins each transaction to a query node (carried in the
opaque txid), round-robins across nodes, retries `BEGIN` on the next node, and
re-discovers the node list when all cached nodes fail. Durability is mapped from
`EXTENDDB_DURABILITY`; `txtimeout` is `EXTENDDB_TXN_TIMEOUT` (default 30 s).
Transaction records live in `data.txn_atr`.

Each read inside `TransactGetItems` is locked within the transaction, which
closed a torn-read defect QE found (14 of 150 torn pairs → 0). Transactional
writes competing with a firehose of plain KV writes on the same key take an
**intent lease** (a 10 s KV document in `data.txn_leases`) so the plain path
yields with `TransactionConflictException` instead of starving the transaction:
in QE case TX-08, a 30 s loop of transactional transfers against a plain
`UpdateItem` writer on the same item went from 2 commits to 329 commits, with
the conservation invariant holding in both runs. `ClientRequestToken` idempotency is a document in
`data.idempotency` with a 600 s retention; a replay with a matching fingerprint
returns the original result, a mismatch is rejected.

Validated: conformance 912 + the 31-test transaction suite + the 15-test
concurrency suite single-node; on a 3-query-node cluster, transactions survive a
query node being failed over mid-flight (`scripts/query-txn-multinode-test.sh`);
`tests/test_transaction_rollback.py` covers single-key contention, cross-table
all-or-nothing, canceled-stream no-orphan, and a backend crash mid-transaction.

### DynamoDB Streams

Streams are explicit records in `data.stream_records`, not Couchbase's DCP or
Eventing, so sequence numbers, shard assignment, and retention follow the
DynamoDB contract exactly. Each stream-enabled table has **one shard**,
`shardId-00000000000000000001-<table_id>`; the id embeds the per-incarnation
table id, so `DeleteTable` + `CreateTable` yields fresh shard ids and leftover
records are cleaned up. The sequence number is drawn from `data.stream_seq`
**inside the same query-service transaction as the data write**, so a consumer
can never advance past a sequence number whose write has not committed. Records
carry `INSERT` / `MODIFY` / `REMOVE` with before and after images as the stream
view type requires; a write that does not change the item emits no record.
`GetRecords` paginates on `idx_stream_shard_seq`; a 24-hour retention is
enforced by `idx_stream_ts` plus a leader-only delete on every TTL sweep cycle
(every 30 s). Enabling streams is done at `CreateTable` today; `UpdateTable`
stream changes are refused explicitly.

**Stream lineage (in-tree).** `main` now carries a table's streams as a lineage
(`extenddb_storage::stream_lineage`): disabling a stream closes it and gives
every shard an `EndingSequenceNumber`, enabling again creates a new stream with
a new label, and the shard id encodes the stream's identity as
`shardId-{table_id}-{label_millis:013}{view_type:1}{index:02}`, so no catalog
change is needed. The Couchbase backend adopts that scheme as part of the
alignment work: one shard per stream (not per table), shard ids in the lineage
format, `UpdateTable` stream enable and disable supported, and
`idx_stream_shard_seq` unchanged since it keys on the shard id. That adoption
also closes the two `test_stream_table_reuse` cases the backend fails today.

### Time to Live

Native Couchbase document expiry is deliberately **not** used: it would delete
items without a stream `REMOVE` record and without the `Service` identity
DynamoDB attributes to expirations. Instead a leader-elected sweeper deletes
expired items through the normal delete path, with the DynamoDB five-year floor
on expiry timestamps (an epoch more than five years in the past is not an
expiry). Enabling TTL creates a per-table index on the attribute; the sweeper
finds expired items with `attribute_exists(#ttl) AND #ttl <= :now` over the
top-level native number and deletes them in batches under that same condition, so
a concurrent update that pushes the expiry out is never lost.

### Vector search

`VectorSearchEngine` maps to a Couchbase composite vector GSI over
`(_xdb.tid, _xdb._v<i> VECTOR)`. A table declares vector indexes at
`CreateTable`; the IVF build is asynchronous, and the leader-only reconciler
rebuilds an index whose build is stuck `pending` once a search has found vectors,
recording it `online` when Couchbase serves it. Search returns ids and distances
only, verified ANN-vs-brute-force in `examples/`.

### Control plane, authentication, authorization

Table definitions, IAM principals, policies, sessions, tags, settings, metrics,
rate-limit windows, and backup metadata are documents in the `catalog` scope.
SigV4 authentication is fully supported. Access-key secrets are AES-256-GCM
encrypted with a key supplied from **outside** Couchbase
(`EXTENDDB_ENCRYPTION_KEY` or `_FILE`: KMS, Vault, or a Kubernetes Secret), never
stored beside the secrets it protects; key rotation is
`EXTENDDB_ENCRYPTION_KEY_PREVIOUS[_FILE]` plus `migrate`. The admin password is
bcrypt-hashed. Login rate limiting fails closed on a Couchbase error.

**Secret keys (in-tree).** `main` moved every backend to a shared model: a
server holds one `SecretKeys` built at startup through
`server_components::load_secret_keys`, either the catalog key or the active DEK
of a key ring stored as the `encryption_key_ring` settings row and sealed under
a KEK file, with rotation and switch-over through the shared `extenddb key`
commands and per-row AAD. The backend's AES-256-GCM-with-external-key principle
is the same; the adaptation replaces its own `EXTENDDB_ENCRYPTION_KEY_PREVIOUS`
rotation with the key ring and runs `key_conformance` in the crate's live
tests. Policy
evaluation (IBAC, RBAC, ABAC with principal and resource tags, `LeadingKeys`
fine-grained access, permission boundaries, AssumeRole sessions) is upstream's;
the backend persists the primitives and the `aws:ResourceTag` migration marker.

### Backup and restore

`CreateBackup` snapshots a table into backup documents (survives the table's
deletion); `RestoreTableFromBackup` recreates the table through the normal
`CreateTable` path, including its GSIs and LSIs and vector indexes, and restores
byte-identically. Backup's adopt pass picks up `_xdb`-less documents edited
from the Couchbase side through a partial index rather than a sequential scan.
PITR is snapshot-now only; a backup is consistent per item, not point-in-time
(tracked as P2).

**Engine-owned backup path (in-tree).** `main` moved backup out of the backends:
the engine owns the job and a backend provides `SnapshotEngine` (a table's
definition and every item as of one instant, read page by page while writers
run), `BackupCatalog`, and `RestoreEngine` (bulk-loading verified files into a
`CREATING` target, recording progress, flipping `ACTIVE`). The Couchbase
backend implements those three in place of its own `BackupEngine`. The open
design point is the snapshot instant: Couchbase query-service transactions are
read-committed and KV has no cross-document snapshot, so a consistent snapshot
of a table under concurrent writes needs a mechanism the backend does not have
today (unresolved question 8). `snapshot_conformance` is the check that decides
it.

### Multi-instance, HA, and background workers

The API tier is stateless: everything durable is in Couchbase, and the only
in-memory state is the auth cache and the `tid → placement` routing cache, both
rebuilt on demand. Any replica serves any request; the SDK's retries are safe
because conditional writes are CAS-checked and transactions carry a
`ClientRequestToken`.

Three singleton workers (TTL sweeper, reindex reconciler for out-of-band edits,
index reconciler) run only on the replica holding a 90 s lease document in
`catalog.leases`; a leader crash delays them by at most the lease TTL. Because
ExtendDB caches a table's key info per process for 60 s and invalidates only on
the instance that ran the DDL, every write checks a catalog read at most
`EXTENDDB_CATALOG_FRESHNESS_MS` old (default 2000 ms), so a table deleted,
re-created, or given a GSI through another instance is handled correctly.

Liveness is the static `/health`; readiness is `extenddb-couchbase verify`, a
deep probe that pings the data connection and enumerates tables and indexes,
bounded by a 30 s connect timeout so it fails loud instead of hanging.

Proven on a 3-node cluster: an acknowledged `majority` write survives a node
crash and failover, two instances behind a load balancer lose nothing when one
is killed, the conformance sweep runs through a mid-run failover with no data
loss (nightly build #51 on 2026-11-02, commit `201f03f`, run against the older
e41f370 test suite as noted in row 6.3: 913 tests, 9 non-passes under
failover of which 4 were new, re-run of those 4 passed 4 of 4; this is after
couchbaselabs/extenddb-couchbase#153 fixed the re-run step, which previously reported success while
running zero tests), and the 14-case QE cluster-ops suite (rebalance in
and out under load, stopped and swap rebalance, graceful and hard failover,
delta and full recovery, auto-failover, node SIGKILL, churn, index-node add and
remove) passes on the `extenddb-couchbase-qe-cluster-ops` Jenkins job.

### Alignment with upstream `main`

The backend was built and validated against ExtendDB `v0.1.x` (pinned rev
`7b828b0`); `main` is 307 commits ahead of that pin at the time of writing and
the `Storage` crate has grown from 13 traits to 22. The in-tree move is
therefore a port, not a copy. In order:

1. **Installation and migrations.** `backend()` returning
   `extenddb_storage::Backend`; the migrations module above; `init`, `migrate`,
   `verify` and `serve` through the shared commands; `devtools` checks.
2. **Secret keys.** `SecretKeys`, the key ring, `key_conformance`.
3. **Streams.** Stream lineage shard ids and enable/disable.
4. **Backup.** `SnapshotEngine`, `BackupCatalog`, `RestoreEngine`,
   `LegacyBackupReader` for backups the out-of-tree backend wrote;
   `snapshot_conformance`.
5. **Metrics.** The account dimension `MetricsStore` now carries
   (`metrics_conformance`), which also scopes the admin console's metrics.
6. **The rest of the surface.** `StorageEngine`, `TableKeyInfoLookup`,
   `JobClaim`, `MaintenanceClaim`, `ServingRegistrar`, and whatever the trait
   set holds when the implementation PR opens.

Each step lands with the conformance suite green on the port so far; the
numbers in this RFC are the out-of-tree backend's and will be re-measured
in-tree before FCP.

### Multi-tenancy and the isolation ladder

Tenancy is by AWS account: a distinct table id per (account, table), the catalog
keyed by `account#name`, and every read path scoped by `_xdb.tid`.
`EXTENDDB_TABLE_ISOLATION` adds physical isolation:

| Tier | Value | Layout | Gains |
|---|---|---|---|
| 1 | `shared` (default) | all items in `data.items` | effectively unlimited tables |
| 2 | `collection` | `data.t_<tid>` per table | per-table TTL, eviction, XDCR filters; `DeleteTable` drops the collection |
| 3 | `scope` | `acct_<id>.t_<tid>` | per-account Couchbase RBAC (`docs/runbooks/per-account-rbac.md`) |

Placement is recorded on the catalog document; pre-existing tables need no
migration. Couchbase documents a limit of **1000 collections and 1000 scopes
per cluster** (Couchbase Server docs, "Scopes and Collections"); the backend's
budget check counts keyspaces in its bucket and falls back to shared placement
when a tier-2 or tier-3 create would approach it.

### Operational requirements

- **Couchbase Server**: the demo and CI run Couchbase Server Enterprise 8.0.0.
  Required services on the cluster: **KV, Index, Query**. Search is not required
  (vector search uses a composite vector GSI, which needs 8.0). Analytics,
  Eventing, and Mobile are optional consumers of the native documents.
- **Bucket**: created once by an operator. The backend runs as a least-privilege
  user with `data_reader`, `data_writer`, `query_select`, `query_insert`,
  `query_update`, `query_delete`, `query_manage_index`, and `scope_admin` on that
  bucket — not Administrator. Per-account RBAC provisioning (tier 3) is opt-in
  behind a separate operator credential.
- **TLS** to clients is mandatory (bring your own certificate). TLS to Couchbase
  (`couchbases://`) trusts the public roots or `EXTENDDB_COUCHBASE_CA_FILE`;
  `EXTENDDB_QUERY_TLS_INSECURE=1` is dev-only.
- **Durability**: `majority` by default, which needs enough data nodes to hold a
  majority of `replicas + 1` copies. The bucket preflight (`bucket_problems`)
  computes this from the bucket's `replicaNumber` and node count: `init`
  **refuses** such a bucket before creating anything, `verify` **fails the
  readiness probe**, and `serve` logs an error at startup and then every write
  fails with the KV durability-impossible error (surfaced as a retryable
  `InternalServerError`, never a silent acknowledgment). The same preflight
  refuses `durabilityImpossibleFallback` set to anything but `disabled`, bucket
  or collection `maxTTL`, and eviction on ephemeral buckets. Single-node demos
  set `EXTENDDB_DURABILITY=none` explicitly.
- **Strict startup**: unknown or misspelled `EXTENDDB_*` values and unknown
  `[storage.couchbase]` keys are refused by name.

Configuration:

```toml
[server]
bind_addr = "0.0.0.0"

[storage.couchbase]
# credentials and bucket travel in the connection string
connection_string = "couchbases://extenddb_app:<password>@couchbase.example.svc/extenddb"
```

| Knob | Default | Purpose |
|---|---|---|
| `EXTENDDB_DURABILITY` | `majority` | KV and transaction durability |
| `EXTENDDB_SCAN_CONSISTENCY` | `request_plus` | `eventual` for DynamoDB's default Scan/Query consistency |
| `EXTENDDB_TABLE_ISOLATION` | `shared` | isolation ladder tier |
| `EXTENDDB_TXN_TIMEOUT` | `30s` | query-service `txtimeout` |
| `EXTENDDB_CATALOG_FRESHNESS_MS` | `2000` | cross-instance catalog staleness bound |
| `EXTENDDB_INDEX_NUM_REPLICAS` | `0` | `num_replica` on every created index |
| `EXTENDDB_CONNECT_TIMEOUT_SECS` | `30` | bound on `Cluster::connect` |
| `EXTENDDB_ENCRYPTION_KEY[_FILE]`, `..._PREVIOUS[_FILE]` | — | access-key secret encryption and rotation |

The Helm chart (`deploy/helm/extenddb-couchbase/`) exposes the same as values,
defaults to two replicas, `ClusterIP`, mandatory TLS Secret, and warns when the
Service is externally exposed. Runbooks cover backup/restore, rolling upgrades
and drain order, the reindex reconciler, per-account RBAC, encryption-key
rotation, the storage format and migration, and the dependency-bump gate.

### Implementation summary

| Component | Change |
|---|---|
| `crates/storage-couchbase/` (today `backend/`) | New crate — full backend: 26.4k lines in `backend/src`, of which ~2.4k are inline `#[cfg(test)]` modules (24k non-test) |
| `crates/bin/Cargo.toml`, `main.rs`, `cmd_serve.rs` | `couchbase` feature flag and backend gate (same shape as the MongoDB RFC) |
| workspace `Cargo.toml` | `couchbase` (Rust SDK 1.1.0), `aes-gcm`, `bcrypt` dependencies |
| `deploy/helm/`, `demo/`, `docs/`, `qe/` | Deployment chart, Docker demo, runbooks, QE harness (may stay in the Couchbase repo) |

No changes to `crates/engine/`, `crates/server/`, `crates/storage/` (trait
definitions), `crates/auth/`, or `crates/core/`.

### Design decisions summary

| Decision | Choice | Rationale |
|---|---|---|
| Document shape | Native JSON at the top level + one reserved `_xdb` object | Data is directly usable by SQL++, Search, Analytics, Eventing, Mobile; one reserved key is the smallest footprint; `_xdb` presence is the format signal |
| Numbers | Native JSON number when a double round-trips exactly; otherwise an exact string recorded in the sidecar; keys use an order-preserving string encoding | Full 38-digit fidelity and exact range order without a precision cap |
| Sets, binary | Arrays and base64 strings + the `_xdb.ddb` type sidecar (map form) | JSON cannot express them; the map form lets one sub-doc op clear one pointer (UpdateItem fast path) |
| Point reads | KV binary get | Sub-millisecond, strongly consistent, no query hop |
| Query/Scan | SQL++ over `_xdb` index columns, `request_plus` default | Read-your-writes by default; `eventual` opt-in matches DynamoDB and is up to 8.7× faster on large scans |
| Conditional writes | CAS read-evaluate-replace with bounded jittered retry | DynamoDB atomicity without a transaction; conflict surfaces as retryable |
| GSIs/LSIs | Derived keys inside the item + one `_xdb.tid`-scoped Couchbase GSI per index | No secondary write to keep in sync; strongly consistent index reads; deferred build + single `BUILD INDEX` |
| Transactions | Query-service `BEGIN WORK`/`COMMIT` pinned to one node, in process | The Rust SDK has no KV transactions; no sidecar, no extra credentials, survives node failover |
| Streams | Explicit records, one shard per table, sequence drawn inside the data transaction | DynamoDB contract (monotonic sequence, images, 24 h retention); no ordering hole |
| TTL | Leader-only sweeper through the delete path, never native expiry | Native expiry would break the `REMOVE` stream record contract |
| Isolation | Shared collection by default, collection/scope tiers opt-in | Unlimited tables by default; physical isolation and RBAC when a tenant needs it |
| Secrets | Encryption key injected from outside Couchbase | A bucket backup must not contain the key that decrypts its access keys |
| Couchbase user | Least-privilege scoped roles, never Administrator | Proven by the whole data plane running as `extenddb_app` in the demo |
| Multi-instance DDL | `EXTENDDB_CATALOG_FRESHNESS_MS` catalog check per write | Upstream's 60 s per-process key cache is invalidated only locally |
| Migrations | `extenddb_storage::migrations` with the ledger row written after an idempotent body (the Cassandra pattern) | Couchbase KV has no multi-document transaction; a crash between body and row re-runs a body that is safe to run again |
| Streams in-tree | Stream lineage shard ids, one shard per stream | No catalog change; `UpdateTable` enable and disable become idempotent by construction |

### Performance characteristics

- **Point operations** are KV hash lookups: YCSB workload A (50/50 read/update)
  2,822 ops/s at 5K records and 2,096 ops/s at 1M records (~1 GB, past cache),
  a 26% drop at 20× the data. Bulk load ~7,400 inserts/s.
- **Scans** are index-bound; `eventual` consistency recovers 1M-row scans from
  ~97 ops/s (index catching up after a bulk load) to ~850–890 ops/s.
- **Transactions** add query-service round trips; the backend exports
  per-operation request latency and storage-query latency at `/metrics` (no
  CAS-retry counter is exported today). A 100-action
  `TransactWriteItems` can exceed the 30 s `txtimeout` at WAN latency (couchbaselabs/extenddb-couchbase#177).
- **Multi-node scaling** (PERF team runs cited under Motivation): workload B
  scales near-linearly (~80% efficiency at 18 nodes; read p50 ~4 ms, p99 9–15 ms,
  flat across scale). Workload A below saturation on 18 nodes with 6 API
  instances: update p99 5.7 ms and read p99 3.0 ms at 5,000 ops/s, 8.6 ms and
  4.0 ms at 10,000, 29 ms and 12 ms at 20,000, 68 ms and 28 ms at 30,000. Peak
  throughput scales sub-linearly (15.7K to 55.7K ops/s from 3 to 18 nodes, ~59%
  efficiency) and the tails at peak (115–164 ms on 3 nodes, 175–197 ms at 18)
  are saturation latency: a single API instance stalls near 10,000 ops/s and
  read p99 rises in step with update p99, so the limiter is API-tier capacity,
  not the write path. Sizing the API tier to load, not to data nodes, is the
  open item (`docs/perf-pass-plan.md`).
- Connection-pool metrics are not emitted: the Couchbase SDK owns pooling and
  does not expose per-acquire latency (couchbaselabs/extenddb-couchbase#178).

### Testing

| Layer | What | Where |
|---|---|---|
| Unit | number encoding, item round-trip, sidecar, key forging, policy hardening | `cargo test --lib`: 262 passed, 0 failed, 0 ignored (offline run, 2026-11-09) |
| Storage-invariant lint | stale top-level `_pk`/`_sk` reads, hand-built keys, unescaped `LIKE` | `scripts/lint-storage-invariants.sh`, per PR |
| Conformance | upstream top-level 912 + parity 331 + web console 30 | `CONFORMANCE.md`; the Jenkins nightly's sweep (the standalone conformance job has not run since 2026-10-14 and is not yet a required check) |
| Backend tests | transaction rollback (7), DynamoDB head-to-head parity, encoding edges | `tests/` |
| Concurrency | property harness (conservation, no-lost-update, one-winner, CAS counters), write-race, txn-concurrency | `scripts/` |
| HA | failover race, multi-instance, sweep under failover, query-txn multinode | `scripts/ha-test.sh` and friends, nightly |
| QE | 343-case plan, 23 areas, executable `qe/tests/`; cluster-ops on a real 3-node cluster | `docs/qe/`, Jenkins `extenddb-couchbase-qe-cluster-ops` |
| Interop | five client apps in four languages, reverse ETL, Couchbase-side ops matrix | `examples/`, `scripts/cb_to_ddb.py`, `docs/couchbase-side-operations.md` |

## RFC-0003 acceptance criteria

Upstream RFC-0003 is the rubric every backend is judged against: ten sections
and about forty criteria. The table below maps each criterion to the mechanism
in this backend and to the test that proves it. "Upstream" tests are files in
the ExtendDB conformance suite (`upstream/tests/`); QE ids are cases in
`docs/qe/testcases.md`; `tests/` and `scripts/` are this repository's own.
Criteria that this backend does **not** fully satisfy today are marked
**OPEN** with the planned fix.

### 1. Item identity

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 1.1 | Unique addressability | Document key `<table_id>#<pk_enc>|<sk_enc>`, SHA-256-hashed past Couchbase's 250-byte key limit; built only by `full_doc_key` | DT-15, LM-02/03/04, `tests/test_encoding_edges.py` |
| 1.2 | No delimiter ambiguity | Key values are type-tagged and encoded before the separator is applied; a key value containing the separator cannot forge a boundary; `scripts/lint-storage-invariants.sh` blocks any hand-built key | `tests/test_encoding_edges.py` (separator-collision class), DT-14, DT-15 |
| 1.3 | Numeric precision | Key and index columns always use the order-preserving string encoding at full 38 digits; they are never stored as a native number, so one logical field has one storage type. The top-level attribute copy is a JSON number only when a double round-trips exactly, otherwise an exact string recorded in the type sidecar | DT-04 to DT-08, DT-17, parity suite number-fidelity battery incl. the ULP case |
| 1.4 | Binary key ordering | Encoded as unsigned-byte lexicographic order | DT-18, QS-04, IX-05, `tests/test_dynamodb_parity.py` |

### 2. Secondary indexes

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 2.1 | Duplicate index keys | There is no separate index entry: the derived keys live in the item document and the Couchbase GSI indexes the document itself, so identity is the base document key | IX-04, upstream `test_put_item_create_race.py` (GSI and LSI tables) |
| 2.2 | Stale entry prevention | Every write path rebuilds `_xdb` through `item_to_document`; the sub-document fast path is refused for any key, index, or vector attribute | IX-09, upstream `test_query_scan.py` GSI cases, stress "GSI query after updating a key attribute" |
| 2.3 | Sparse semantics and type validation | `validate_index_key_types` / `validate_index_key_values` reject a wrong-type or empty index key with `ValidationException`; a missing attribute yields no derived key and the GSI `WHERE` excludes it | IX-03, DT-20, upstream `test_gsi_attribute_definitions.py` |
| 2.4 | Backfill on UpdateTable | Catalog row written `CREATING`; the leader-only index reconciler backfills `_xdb._g<slot>` for existing items and only then flips to `ACTIVE` | IX-07 (the upstream `gsi_async*` files spawn their own server and are outside the endpoint sweep, so IX-07 is the gate) |
| 2.5 | Transactional index maintenance | Transactional writes rewrite the whole document, derived keys included, inside the query-service transaction | TX-01, TX-13, upstream `test_transaction_operations.py` |
| 2.6 | Index query pagination | `LastEvaluatedKey` carries index and base keys; the resume bound is merged with, not substituted for, the key condition | IX-06, QS-07, upstream `test_read_paging.py` |
| 2.7 | ConsistentRead on GSI | Rejected with `ValidationException` ("Consistent reads are not supported on global secondary indexes"). The rejection is enforced by the upstream engine layer; the backend neither advertises nor relies on strong GSI reads: index queries run at the configured scan consistency and the backend exposes no strong-read option for them | IX-10, upstream `test_vector_index_query_scan.py`, QS-20 |

### 3. Transactions

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 3.1 | All-or-nothing | One query-service transaction carries the data writes (with their derived index columns), the stream records, and the idempotency create; `COMMIT` or nothing | TX-01 to TX-03, `tests/test_transaction_rollback.py` (cross-table, rollback completeness, crash mid-transaction) |
| 3.2 | Stream records in transaction | `stream_txn_write` appends the records to the same transaction as the writes | ST-04, TX-13, upstream `test_streams.py` |
| 3.3 | Idempotency token atomicity | The token document `<account>#<token>` is read first (replay or mismatch by fingerprint, 600 s retention), then **recorded as a fail-if-exists create inside the same transaction as the writes**. Two concurrent requests with the same fresh token race on that create; exactly one commits, the loser's "exists" is mapped to replay or mismatch. Token and data commit or roll back together | TX-05, TX-06, upstream `test_idempotency_account_scope.py`, `tests/test_transaction_rollback.py` ("N concurrent transactional puts produce exactly one") |
| 3.4 | CancellationReasons fidelity | Per-operation reasons in input order; `ALL_OLD` item attached on condition failure | TX-02, CR-12, upstream `test_transaction_operations.py`, `test_transact_expression_validation.py` |
| 3.5 | TransactGetItems isolation | Every read is locked inside one read transaction | TX-07, upstream `test_transact_get_snapshot.py` |

### 4. Concurrency and isolation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 4.1 | Unconditional writes never fail under contention | A bare unconditional `PutItem` with no `ReturnValues` is a plain KV upsert: no CAS, no retry, last writer wins, **both racing calls succeed**. With `ALL_OLD` it is a CAS loop bounded by a deadline that still returns success to both callers; no CAS or conflict detail is client-visible | upstream `test_put_item_create_race.py::test_racing_puts_with_all_old_all_succeed`, `::test_racing_puts_on_a_stream_table_all_succeed`, `test_concurrency.py::test_parallel_inserts`, CR-03 |
| 4.2 | Conditional writes: correct error on race | The CAS loop re-evaluates the condition against the winner's image and returns `ConditionalCheckFailedException` | CC-01, upstream `test_put_item_create_race.py::test_racing_puts_check_their_condition_against_the_winner`, `::test_racing_conditional_creates_have_one_winner` |
| 4.3 | Transaction conflict mapping | A plain write hitting a live intent lease returns `TransactionConflictException`; a transaction losing its validation returns `TransactionCanceledException` with `TransactionConflict`; CAS exhaustion surfaces as a retryable conflict, never a 500 | TX-08, CC-09, upstream `test_transact_write_conflict.py`, `scripts/write-race-test.py` |
| 4.4 | UpdateItem serializes | CAS read-modify-write with jittered backoff; arithmetic and `ADD` never take the sub-document path | CC-02, CC-03, `scripts/concurrency-property-test.py` (`prop_add_counter`, `prop_optlock_counter`), upstream `test_concurrency.py::test_atomic_counter` |
| 4.5 | Read-your-writes | `GetItem` is a KV get served by the active vbucket; no replica reads | CC-04, CR-06 |

### 5. DynamoDB Streams

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 5.1 | Atomicity with data writes | Records are written in the data transaction | ST-04, upstream `test_streams.py` |
| 5.2 | Per-shard ordering | The sequence number is drawn from `data.stream_seq` inside the transaction, so visibility follows commit order; `GetRecords` reads `idx_stream_shard_seq` ascending | ST-03, upstream `test_streams.py`, stress "consumer reading during concurrent writes" |
| 5.3 | Account and table isolation | Shard id embeds the per-incarnation table id; records carry the table id and every read is scoped by it | upstream `test_cross_account_isolation.py` (one case in this file failed in the nightly baseline for a test-suite-skew reason, see 6.3; it passes against the pinned suite), `test_stream_table_reuse.py` |
| 5.4 | Correct event types | `INSERT` / `MODIFY` / `REMOVE` with images per view type; an unchanged item emits no record | ST-01, ST-05, upstream `test_streams.py` |
| 5.5 | Retention and cleanup | `idx_stream_ts` plus a leader-only delete at 24 h on every TTL sweep cycle | ST-06, ST-07 |
| 5.6 | Shard stability | Today: one deterministic shard per table, streams declared at `CreateTable`, `UpdateTable` stream changes refused, so there is no re-enable path to make non-idempotent. In-tree: stream lineage, one shard per stream with the lineage shard id, enable and disable through `UpdateTable` (see "DynamoDB Streams") | ST-02, upstream `test_stream_table_reuse.py` (the two cases that fail today pass with lineage) |

### 6. Condition expressions, code hygiene, documentation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 6.1 | Evaluation semantics | Delegates to `extenddb_core::expression::evaluate_condition` on the read image; no native pushdown | CR-10 to CR-13, upstream `test_conditional_writes.py`, `test_conditional_operator.py` |
| 6.2 | No hidden unused code | **OPEN.** `engine.rs` and `catalog.rs` carry a crate-level `#![allow(unused_variables, unused_imports, clippy::too_many_arguments)]`, which RFC-0003 prohibits, plus three targeted `#[allow(dead_code)]` with comments. Measured on 2026-11-05 with those lints force-enabled: the blanket allows hide 3 unused imports and no unused variables; the dead-code lint is not suppressed and reports 5 never-used items (two sidecar-format helpers, one keyspace helper, one encryption-key helper, two never-read struct fields). Fix before upstream submission: delete the blanket allows and the 5 dead items, keep only commented targeted allows | `cargo check --lib` with `--force-warn unused_imports --force-warn unused_variables --force-warn dead_code` |
| 6.3 | Documentation describes the system as built | Every mechanism described in this RFC is implemented and on the live code path; Capella is stated as unvalidated, the failover-sweep claim cites a specific nightly run, and the scaling figures cite the PERF team's measured runs. Also recorded here for honesty: nightlies #46 through #58 reported a **baseline** sweep of 908 of 913 with the same 5 non-passes every run (`test_abac.py` ×3, `test_auth_permissions.py::test_explicit_deny_all_service_operations`, `test_cross_account_isolation.py::TestStreamAccountScoping::test_shard_iterator_only_returns_owning_account_records`). Triaged 2026-11-09: **test-suite skew, not a product defect.** The nightly's heavy lane skipped the "clone upstream at the pinned rev" stage, so the failover sweep ran the stale `e41f370` test checkout (913 collected) against the auth-cluster build; the pinned `7b828b0` suite (1234 collected) passes all five against the same build, locally in isolation and in a full run. Fix prepared, pending merge: `Jenkinsfile` (clone stage runs in both lanes) and `scripts/sweep-under-failover.sh` (refuses a checkout that does not match the Cargo pin; keeps its pytest outputs for Jenkins) | Jenkins `extenddb-couchbase-nightly` #51 console |

### 7. Query and Scan

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 7.1 | Limit semantics | `Limit` becomes the storage fetch bound; the engine filters afterwards | QS-07, QS-09, QS-17, upstream `test_query_scan.py` |
| 7.2 | LastEvaluatedKey correctness | Reflects the last item read, in the base or combined schema; present iff more items exist | QS-07, QS-18, upstream `test_read_paging.py` |
| 7.3 | Parallel scan completeness | Segment membership is `SHA-256(pk_enc, sk_enc) mod TotalSegments`, shared by the scan and the `ExclusiveStartKey` check; a segment pages until exhausted | QS-16, upstream `test_query_scan.py` parallel-scan cases |
| 7.4 | Sort order | Ordering on the encoded `_xdb.sk` string equals DynamoDB order for strings (bytes), numbers (by value via the encoding), and binary (unsigned bytes) | DT-16 to DT-18, QS-02 to QS-06 |

### 8. Multi-tenancy

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 8.1 | Account isolation | Table id per (account, table); every index and query scoped by `_xdb.tid`; stream records and sequence counters carry the table id; idempotency keys are `<account>#<token>`; catalog keyed by `account#name` | AU-19, QS-21, upstream `test_cross_account_isolation.py`, `test_idempotency_account_scope.py` |
| 8.2 | Table-name reuse | Table id is random per incarnation; shard ids, documents, and counters derive from it, and `DeleteTable` cleans up | upstream `test_stream_table_reuse.py`, TX-15, QS-19 |

### 9. Error fidelity

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 9.1 | No silent degradation | Unsupported shapes are refused explicitly; an unsatisfiable durability fails the write; failed index DDL surfaces; a partially created table is rolled back (`CreateRollback`) | NE-04, NE-06, SM-04, SM-12 |
| 9.2 | DynamoDB error codes | Couchbase and SDK errors map to `StorageError` variants that the engine renders as DynamoDB codes; no backend-specific code reaches a client | NE-01 to NE-03, upstream `test_auth_error_fidelity.py`, `test_expression_error_prefix.py` |

### 10. Operational correctness

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 10.1 | Background workers | TTL sweeper, reindex reconciler, and index reconciler are spawned by `CouchbaseRuntimeHooks::spawn_workers` and gated by the leader lease | ST-08, ST-13, `docs/architecture.md` §6 |
| 10.2 | Cache coherence | Upstream's 60 s per-process key-info cache is backed by a catalog read at most `EXTENDDB_CATALOG_FRESHNESS_MS` (2000 ms) old on every write; the placement cache is rebuilt on miss | upstream `test_cache_coherence.py`, `test_console_cache_coherence.py`, Helm README multi-instance section |
| 10.3 | UpdateTable idempotency | Vector changes via `UpdateTable` are refused; GSI add is driven by catalog status so a repeat is a no-op; stream enable and disable (in-tree, via stream lineage) are idempotent by construction since a repeat targets the same newest stream | upstream `test_table_operations.py`, `test_stream_table_reuse.py`, IX-07 |

### Required suites

RFC-0003 requires `devtools/run-tests --extenddb --all` to pass in CI. That
driver assumes an in-tree crate; today the equivalent is the runbook in
`CONFORMANCE.md` (the pytest conformance and parity suites against a running
endpoint) plus `cargo test --lib`, which runs on every PR in GitHub Actions as a
required check; the pytest sweeps run in a Jenkins nightly. In-tree, the crate
gains `--backend couchbase` in `devtools/run-tests`, a `run-couchbase-tests`
driver that starts a Couchbase container for the crate's `*_live_tests.rs`, and
the in-crate checks every backend runs with its own store: `metrics_conformance`,
`key_conformance`, `snapshot_conformance`.

## Drawbacks

- **A query-service dependency for transactions and scans.** Every cluster needs
  Index and Query nodes, and transactional latency is bounded by query-service
  round trips rather than KV. If the Rust SDK gains native KV transactions, the
  transaction path should move (see Alternatives); the query path for Scan is
  inherent.
- **Shared-collection default.** Many tables in one collection make `_xdb.tid`
  scoping a correctness invariant every index and query must honor; a missed
  filter is a cross-table leak. The storage-invariant lint and the isolation
  ladder mitigate; the risk is structural.
- **Reserved key and sidecar.** `_xdb` is visible to Couchbase-side consumers,
  and sets, binary, and wide numbers are only faithful through the sidecar.
  External writes that drop `_xdb` leave indexes stale until the reconciler
  adopts them; native expiry bypasses streams. Both footguns are documented.
- **A real port, not a copy.** The backend is validated against `v0.1.x`;
  adopting the migrations framework, the key ring, stream lineage and the
  engine-owned backup path is new code whose correctness is re-proven in-tree,
  and the snapshot instant (unresolved question 8) may need a mechanism
  Couchbase does not provide natively.
- **Single stream shard per table.** Correct and simple, but a very high write
  rate to one table serializes on one sequence counter; sharding is a P2.
- **Maintenance burden.** ~24k non-test lines of backend plus QE harness, Helm,
  and runbooks owned by the Couchbase team; trait changes upstream require a
  coordinated update, as RFC-0002 anticipates.
- **Known issue: the admin console leaks across accounts (couchbaselabs/extenddb-couchbase#216, SC-21/SC-22).**
  With the pinned upstream, a non-admin IAM-user session can open the console
  (login falls through to IAM auth), `metrics_data` has no account filter, and
  `account_detail(<id>)` is an IDOR that returns any account's name and user
  count. Shipped mitigation: the Helm chart keeps the Service on `ClusterIP`,
  warns at install when it is exposed, and documents an L7 block of `/console`
  (couchbaselabs/extenddb-couchbase#204). Planned fix, upstream: account-scoped metrics once metrics carry an
  account dimension (CP-29), an ownership guard on `account_detail`, and a
  console-disable switch. Until then "`/console` not reachable by tenant
  principals" is a hard deployment requirement, not a recommendation.
- **Known constraint: query prepared statements are off by default (couchbaselabs/extenddb-couchbase#194).** The
  Couchbase Rust SDK's query core (1.0.2, still present in 1.1.0) retries a stale
  prepared-statement name forever after the query service restarts (rebalance,
  failover, node restart, online upgrade), so every Query and Scan hangs until
  the process restarts while KV stays healthy. The backend therefore runs its
  parameterized data-plane statements ad-hoc unless `EXTENDDB_PREPARED_QUERIES`
  is set, trading plan caching for topology-change safety. This is an SDK fix
  Couchbase can drive; until it lands, prepared statements stay opt-in for
  controlled runs only.
- **Known constraint: no in-place rollback past two format edges.** Once a v3.2
  build has served a bucket holding any set, binary, or wide-number item, a
  pre-v3.2 build misreads the map-form sidecar (a set comes back as a list,
  binary as a base64 string) with a green KV health check; and the auth-cluster
  build refuses to serve without the resource-tag migration marker that only
  `migrate` or `init` writes. Both are restore-from-backup rollbacks, documented
  in `docs/runbooks/storage-format.md`. Pre-1.0 there are no released versions,
  so these are forward-only steps in practice, but they are real.

## Alternatives

- **DynamoDB-JSON envelope (v1) or `item`-wrapped (v2) storage.** Simpler
  encoders, exact by construction, but the data is opaque to every Couchbase
  service and tool; rejected in favor of native JSON after both were built and
  measured. Both remain readable.
- **A Java transaction sidecar** using the Couchbase Java SDK's KV transactions.
  Built, hardened, then removed: a per-op plaintext HTTP hop with its own
  credentials and a livelock mode. The query-service path is pure Rust and
  survives node failover.
- **Native Couchbase expiry for TTL** and **Eventing or DCP for Streams.**
  Rejected because neither honors the DynamoDB stream contract (a `REMOVE`
  record with the `Service` identity, monotonic sequence within a shard).
- **One collection per table by default.** Cleaner isolation and `DeleteTable`
  as a drop, but a hard ~1000-collection budget caps table count; kept as an
  opt-in tier instead.
- **Do it upstream, in `crates/storage-couchbase/`, from day one.** That is the
  proposed end state; the out-of-tree start let the backend reach conformance
  while the `Storage` trait was still moving at v0.1.x.
- **Do nothing.** Teams that run DynamoDB in AWS regions and Couchbase
  elsewhere keep maintaining two application stacks and two data access
  layers.

## Unresolved questions

1. **Landing shape.** Should the implementation land as one PR with the crate
   at full conformance on `main`, or in the phases listed under "Alignment with
   upstream `main`" (installation and migrations first, behind the feature
   flag, then keys, streams and backup), each gated on the suites?
2. **Trait classification.** The backend implements every optional trait. Under
   RFC-0002's all-or-nothing rule, do `VectorSearchEngine` and `BackupEngine`'s
   snapshot-only PITR count as complete?
3. **`ConsistentRead` threading** (couchbaselabs/extenddb-couchbase#11). Upstream does not pass the flag into
   `query`/`scan`; strong reads are served regardless. Should the trait grow a
   parameter so the backend can serve eventual reads only when asked?
4. **Stream sharding** and **true point-in-time backup** are single-shard and
   per-item-consistent today (P2). Are these blocking for acceptance in any reviewer's view?
5. **Couchbase version floor.** Every script, the demo compose file, and the HA
   and multinode harnesses pin `couchbase:enterprise-8.0.0`; the demo and scratch
   rigs have also run 8.0.1 (reviewer-reported); the QE cluster-ops cluster's
   exact build is not recorded in CI output and should be. Should the RFC commit
   to 7.6.x without vector search, or require 8.0?
6. **API-tier scaling.** Peak workload A throughput scales sub-linearly with
   data nodes because a single API instance saturates near 10,000 ops/s;
   latency below that point is DynamoDB-class. Runs with more API instances
   per data node and a non-colocated driver have not been done yet. Reviewers
   are asked whether the backend should document a per-instance throughput
   budget as part of its operational requirements.
7. **Bi-directional replication with DynamoDB.** Couchbase and AWS have both
   expressed interest in collaborating on replication between DynamoDB tables
   and Converge tables. Should it be scoped as an upstream RFC of its own (a
   replication subsystem under RFC-0002's "significant new subsystem" rule), and
   which side owns each half: DynamoDB Streams into Converge, and Converge
   streams into DynamoDB?
8. **Snapshot instant for `SnapshotEngine`.** Couchbase query transactions are
   read-committed and KV has no cross-document snapshot, so "every item as of
   one instant" under concurrent writes needs either a per-table write fence
   for the duration of the snapshot read (writes queue or are refused briefly),
   a DCP-based cut, or an accepted per-item-consistency divergence documented
   against `snapshot_conformance`. Which do the maintainers consider
   acceptable for a backend?

## Prior art

- ExtendDB's reference PostgreSQL backend and the
  [MongoDB backend RFC](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs/0000-mongodb-backend.md):
  this design keeps the same explicit-stream-record and inline-GSI philosophy but
  derives index keys into the item instead of per-index collections, and uses
  server-side transactions over a query service rather than driver sessions.
- The upstream [Cassandra backend draft](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs/draft-cassandra-backend.md).
- Couchbase's own order-preserving key encodings in its DynamoDB-compatible
  predecessor prototypes, and **Couchbase Converge for MongoDB Workloads** (the
  MongoDB-wire proxy), which stores a per-field type envelope in every document;
  the sidecar approach here was chosen precisely to avoid an envelope so the
  stored document stays plain JSON.
- DynamoDB's documented contracts for Streams sequence numbers, the 400 KB item
  limit, the TTL five-year floor, and the 38-digit number range, each of which
  surfaced as a QE finding and is now enforced.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
