# RFC-0000: Couchbase Storage Backend

- Status: Draft (revision 6, 2026-11-26: most engineering items of revision 3
  are implemented in the reference repository `[ref-repo]`, which no reviewer
  can read yet, so rows 2.4, 3.5, 4.1, 4.3, 5.2 and 6.2 of the acceptance table
  stay OPEN. Open parts remain in engineering items 1, 2, 5 and 8, item 13 is
  blocked on the pin, and item 15 is two OPEN gaps, Gap A (visibility after a
  commit, no code) and Gap B (the idempotency-token read after an unknown
  outcome, fixed in the unknown-outcome arm only); see "Engineering items of
  revision 3")
- Author: @suravarjjala
- Created: 2026-11-04
- FCP ends: —
- Tracking issue: ExtendDB/extenddb#1183
- Acceptance rubric: RFC-0003 "Backend Acceptance Criteria"; every criterion is mapped in "RFC-0003 acceptance criteria" below
- Reference implementation: https://github.com/couchbaselabs/extenddb-couchbase.
  Read access is granted to any reviewer who asks: the invitation to the first
  reviewer was sent on 2026-11-24 at 19:15 UTC and is pending at
  https://github.com/couchbaselabs/extenddb-couchbase/invitations (GitHub
  answers 404 until it is accepted). The repository goes public before the Final
  Comment Period, once Couchbase's open-source release approval completes. Until
  a reviewer has read it, every `docs/`, `scripts/`, `tests/` path, QE id,
  commit id, pull-request number, Jenkins build number, pass count and timing in
  this document that comes from that repository, its demo stack or its Jenkins
  jobs is a claim that only that repository can show. Each such item is followed
  by `[ref-repo]` wherever it appears in a table or a list of results; upstream
  test files (`upstream/tests/`, the `test_*.py` names) are not so marked. Dates
  of commits are UTC: the three commits of 2026-11-25 carry the author time
  2026-11-24 17:25 to 17:31 in UTC-8. Figures from Couchbase's performance CI
  (multi-node YCSB runs, 2026-10-22 to 2026-11-01) come from that CI, not from
  this repository, and no reviewer can repeat them from here; each is followed
  by `[perf-ci]`, and until a reviewer is given access to those runs they are
  claims that only Couchbase can show.
- Tier at entry: Experimental, per `README.md` "Storage backends"

## Summary

This RFC proposes Couchbase Server as a storage backend for ExtendDB, shipped
as **Couchbase Converge for DynamoDB Workloads** (crate
`extenddb-storage-couchbase`, binary `extenddb-couchbase`). It lets DynamoDB
clients — the AWS CLI, every language SDK, PynamoDB, boto3, the Node.js and Go
SDKs — run unmodified against a Couchbase cluster. The backend implements the
mandatory traits of RFC-0002 and the optional traits except vector search,
installs itself through `extenddb_storage::Backend` from a thin `extenddb` bin
with no changes to the engine, server, or auth crates, and passes the upstream
conformance suite (the numbers and the suite revision that produced each are
in "Evidence" below). Items are stored as native Couchbase JSON with the
business attributes at the document top level and one reserved `_xdb` object
for the machinery, so the same documents are usable from SQL++, Search,
Analytics, Eventing, Mobile, and XDCR. Transactions run natively over the
Couchbase query service in Rust with no sidecar. Couchbase intends to own and
maintain the backend crate under RFC-0002's contribution process. The backend
was built against ExtendDB `v0.1.x`; this RFC scopes its adaptation to the
`Storage` contract on `main`, including the code-based catalog and data
migrations every in-tree backend carries, and enters at the Experimental tier
with one server per catalog, as every backend on `main`.

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
choice. The replication, rebalance, auto-failover, backup, and multi-dimensional
scaling (KV, index, query, and search on separate nodes) that such deployments
need come from Couchbase, not from the API tier. Couchbase cross-datacenter
replication (XDCR, Enterprise Edition only) can copy the native documents to
another cluster; `docs/couchbase-side-operations.md` `[ref-repo]` covers it, including its
limits with transactions: XDCR replicates documents, not transactions, so a
dropped connection can deliver part of a transaction's writes, and Couchbase
advises against active-active replication of transactional data. XDCR is an
operator tool on this data, not a feature this RFC proposes.

**Evidence.** The backend has been built and validated end to end before this
RFC. Every `docs/...`, `scripts/...`, `tests/...` and `CONFORMANCE.md` path in
this document is a file in the reference repository; the figures are repeated
here so the RFC can be read without it, each with the suite revision and the
backend build that produced it:

- Conformance, measured 2026-09-18 with the upstream suite at `e41f370`
  against the v3.2 backend: top-level `tests/test_*.py` **912 passed, 1 xfail**
  (upstream's own expected-fail marking; 913 collected after excluding the
  files that run ExtendDB-internal infrastructure: dev mode, host CLI, web
  console, SQLite, the self-spawning `gsi_async` queue) and `tests/python/`
  **331 / 331** (`CONFORMANCE.md` `[ref-repo]`). The pinned suite the backend builds against
  is `7b828b0` on the `couchbaselabs/extenddb` fork (the pin plus six fork-only
  commits; see "Alignment with upstream `main`"). Its full run on 2026-11-24
  against backend `201f03f` `[ref-repo]` (the demo stack, same file exclusions): top-level
  **1,211 passed, 6 failed, 16 skipped, 1 xfail** of 1,234 collected, and
  `tests/python/` **331 / 331** `[ref-repo]`. The six: four `aws:SourceIp` loopback cases
  (`test_abac.py` ×3, `test_auth_integration.py` ×1) that require the server to
  see the client on 127.0.0.1, which a server behind Docker Desktop's bridge
  on the machine that ran it does not (an environment limit of that run; the
  nightly's Linux agent runs the same stack, and its next build says whether
  the loopback source survives there), and the two `test_stream_table_reuse.py` cases that `UpdateTable` stream
  changes fail today and stream lineage closes (row 5.6). The nightly that
  runs this suite is `extenddb-couchbase-nightly`; the clone stage that kept
  it on the stale `e41f370` checkout was fixed in
  couchbaselabs/extenddb-couchbase#237 `[ref-repo]`, merged 2026-11-24.
- The engineering items of revision 3 (the lease, the stream reader's
  transactional read, the descending twins, the budgets, the backfill fence,
  the scoped `destroy`, and the smaller ones) are merged on the reference
  repository's `main` as commits `b0c272f`, `5e8462b` and `f4f293d`
  (2026-11-25 UTC) `[ref-repo]`, run against the demo stack where a test
  exists (each item's row says which). The three-node harness
  (`scripts/query-txn-multinode-test.sh` `[ref-repo]`, three Couchbase nodes each
  running data, index and query, durability `majority`) ran when those commits
  merged; its commit-point hold `[ref-repo]` (see "Transactions" and "DynamoDB
  Streams") was run twice on the round-4 branch and its gate failed both times
  (see "DynamoDB Streams"); the figures once quoted for it are withdrawn as
  untraceable. Round 4 changed the lease and the fence
  afterwards; that work is on a branch of the reference repository that is
  pushed to the private repository and not merged `[ref-repo]`.
- QE `[ref-repo]`: a 343-case Couchbase-specific test plan (`docs/qe/testcases.md`) run
  against a real multi-node cluster; every failure traced to code and fixed or
  filed (`docs/qe/qe-report.md`). No acknowledged write was lost in any
  rebalance, failover, node kill, churn, or API-restart test.
- Performance: YCSB through the same binding measures workload A at 2,822 ops/s
  on the 5K-record baseline and 2,096 ops/s at 1M records, with bulk load at
  ~7,400 inserts/s on the 1M-record run (`docs/ycsb-benchmark.md` `[ref-repo]`).
  Couchbase's performance CI `[perf-ci]` (multi-node YCSB runs, 2026-10-22 to
  2026-11-01) shows the
  read-heavy workload B scaling near-linearly from 3 to 18 nodes (34K to 164K
  ops/s) with flat read latency, and workload A at 5.7 ms update p99 and 3.0 ms
  read p99 at 5,000 ops/s (see "Performance characteristics"). Those runs used
  several API instances; under the one-server-per-catalog model proposed here
  the per-instance figures are the ones that apply.
- Interop: five self-asserting client apps across four languages, plus a
  reverse-ETL path that moves native Couchbase data into DynamoDB tables through
  ExtendDB.

The required gate for acceptance is `devtools/run-tests --extenddb --all
--backend couchbase` in the `couchbase-integration` CI job (see "Required
suites"); the numbers above are the out-of-tree measurements that precede it.

## Detailed design

### Scope

This RFC proposes:

- A Couchbase storage backend crate, `extenddb-storage-couchbase`, implementing
  the mandatory storage traits and the optional ones except
  `VectorSearchEngine`, selected with a `couchbase` Cargo feature.
- Installation through `extenddb_storage::Backend` and `set_backend` from a
  thin `extenddb` bin behind a `couchbase` Cargo feature, with no changes to
  `crates/engine/`, `crates/server/`, `crates/auth/`, or `crates/core/`.
- Code-based catalog and data migrations through `extenddb_storage::migrations`:
  a `MigrationBackend` for the two Couchbase stores, a ledger in each, an entry
  for every id in the registry, a migration lock and serving registrations, in
  the shape `crates/storage-mongodb` uses (see "Catalog and data migrations").
- The adaptation of the existing backend from the ExtendDB rev it was built
  against to the `Storage` contract on `main` (see "Alignment with upstream
  `main`").
- A native-JSON on-disk format (v3.2) with read-side coexistence of the prior
  out-of-tree formats.
- Native in-process transactions over the Couchbase query service.
- One ExtendDB server per catalog, as `README.md` requires of every backend, with
  a Helm chart that deploys one replica against an external Couchbase cluster,
  TLS mandatory, least-privilege Couchbase user.

This RFC does not propose:

- Changes to the DynamoDB wire protocol or API response shapes.
- Changes to the `Storage` trait. The backend will track `main`; the fixes it
  carried on a fork at its current pin (#398's bounded metrics memory, the
  #418–#421 auth cluster) are merged or submitted upstream on their own, not
  through this RFC.
- Vector search. The out-of-tree backend has a `VectorSearchEngine` over a
  Couchbase composite vector index; under RFC-0002's all-or-nothing rule for an
  optional trait it is left out of the first in-tree release, as MongoDB and
  Cassandra do, and returns in a later phase once it passes every upstream
  vector test with its approximate results recorded in
  `docs/differences-from-dynamodb.md`.
- Several ExtendDB servers on one catalog. The out-of-tree backend runs a
  stateless API tier with a leader lease and a cross-instance catalog freshness
  check; that deployment model differs from every backend on `main` and is
  proposed separately, in a follow-up RFC, once this backend is in-tree.
- Couchbase Capella (the hosted service) as a distinct target. Nothing in the
  design is self-managed-only, but Capella has **not** been validated and no
  claim is made for it here.
- Replacing PostgreSQL as the default backend.
- Online migration between backends.
- Replication or change data capture between DynamoDB tables and Couchbase.
  What exists today is one-way tooling (`scripts/cb_to_ddb.py` moves native
  Couchbase data into DynamoDB tables through ExtendDB). Per
  `docs/rfcs/README.md`, replication needs its own RFC.

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
`extenddb_storage::Backend` the bin installs with `set_backend`, so an
`extenddb-couchbase` image can also be shipped from a copy of `crates/bin` with
one call swapped, as `main.rs` describes. The first release that ships the crate
takes ADR-0029's new-crate steps (`packaging/README.md`, "Releasing to
crates.io").

Module layout (`backend/src/` today, `crates/storage-couchbase/src/` upstream):

| Module | Responsibility |
|---|---|
| `item.rs`, `number.rs` | DynamoDB item ↔ Couchbase document mapping; key encoding; order-preserving number encoding |
| `engine.rs` | `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore`, `SnapshotEngine` (returns `Unsupported`), `BackupCatalog`, `RestoreEngine` |
| `catalog.rs` | `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`, `CatalogStore` |
| `bootstrap.rs` | `Bootstrapper`, `OperationsEngine` (CLI), `DiagnosticsStore`; the bucket preflight |
| `conn.rs` | Couchbase connection, KV helpers, durability, query-service transactions |
| `ttl_worker.rs`, `reindex.rs` | The background workers (TTL sweeper, reindex reconciler, index reconciler) |
| `config.rs`, `backend.rs` | `StorageConfig` for `[storage.couchbase]`, `ServerRuntimeHooks`, the `backend()` that builds `extenddb_storage::Backend` |
| `migrations/` | `MigrationBackend`, `CatalogMigrations`, ledgers, lock, serving registrations, the bodies (see "Catalog and data migrations") |

Access-key secrets and passwords go through `extenddb_storage::secret_cipher`
and `extenddb_storage::password`; the crate has no cipher code of its own (see
"Control plane, authentication, authorization").

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

One Couchbase bucket holds one ExtendDB deployment, split into two scopes.

**`catalog` scope** — control-plane state: `tables`, `accounts`, `users`,
`access_keys`, `policies`, `roles`, `groups`, `sessions`, `admins`,
`login_attempts`, `settings` (the catalog version row among them),
`account_metrics`, `backups`, `extenddb_servers`, `extenddb_migrations`. The
migration lock is not in this scope: it is the `extenddb_migration_lock`
document in `_default._default` (see "Catalog and data migrations").

**`data` scope** — user data and data-plane state:

| Collection | Holds |
|---|---|
| `items` | every table's items in the default (`shared`) isolation tier, told apart by `_xdb.tid` |
| `t_<tid>` (tier 2) / `acct_<id>.t_<tid>` (tier 3) | per-table or per-account placement on the isolation ladder (§ "Multi-tenancy") |
| `stream_records`, `stream_seq` | DynamoDB Streams records (and skip markers) and the per-shard sequence counters |
| `idempotency` | `TransactWriteItems` client request tokens (10-minute retention) |
| `txn_atr` | query-service transaction records (created on first use) |
| `txn_leases` | the per-item leases that transactions and plain writes take before writing (see "Transactions") |
| `extenddb_migrations` | the data store's migration ledger |

`init` creates nothing by hand: the scopes, collections and indexes are created
by the migration bodies below. The status follows `main`'s runner. A bucket
whose `catalog` scope has neither a ledger row nor the out-of-tree
`catalog_version` document is `Uninitialized`; only then does `init` read the
contents, and a `catalog` or `data` scope that holds something else is someone
else's data, which `init` refuses by name. A `catalog` scope that holds
`catalog_version` but no `extenddb_migrations` ledger is an older deployment of
this backend, from before the ledger. It is not `Uninitialized` and not
`WrittenByOlderRelease`: it is `Behind` at migration 1 with no version (exit 3),
and `migrate --yes` adopts it (the baseline body reads that document).
`WrittenByOlderRelease` is the later state, a ledger row for migration 1
followed by a write of a release that predates the ledger, and needs `migrate
--yes --reconverge`. `verify` is the readiness probe. `destroy` drops exactly
the scopes, collections and indexes that the catalog and the ledgers record, by
name from those records, never by pattern, and leaves anything else in the
bucket alone.

### Catalog and data migrations

Out of tree the backend records a single `catalog_version` document in
`catalog.settings` and runs an idempotent, catalog-only `migrate` list. `main`
replaced that model for every backend with `extenddb_storage::migrations`: one
numbered `MigrationId` list shared by all backends, a ledger in every store, a
runner, a migration lock, and serving registrations (ADR-0023). The Couchbase
backend adopts it as follows, mirroring `crates/storage-mongodb/src/migrations/`.

- **Stores.** `StoreKind::Catalog` is the `catalog` scope and `StoreKind::Data`
  the `data` scope of the one bucket. Each gets a ledger collection
  `extenddb_migrations` with the shared `LedgerColumns`. The catalog version row
  is written with every catalog ledger row through `LedgerWrites::write_version`
  and read by the shared status check, as ADR-0023 requires; it is the same
  `settings` document the out-of-tree backend wrote, so an out-of-tree
  deployment's version is readable by the in-tree binary. Nothing deletes it.
  The out-of-tree `resource_tag_migration` marker is deleted by migration 1's
  body, as ADR-0023 decision 4 does; migration 1 records nothing for migration 2.
- **Table.** `migration()` is the exhaustive `match` from every `MigrationId`
  to a `Migration` for each store, with no wildcard arm, so the crate fails to
  compile when the registry gains an id this backend has no entry for.
  `RELEASED` pins the checksums of released bodies and `NotApplicable` reasons,
  and the crate runs `check_released` as a test, exactly as the other backends
  do. The entries for the twelve ids on `main` today:

| Id | Name | Kind, convergent | Couchbase |
|---|---|---|---|
| 1 | `baseline` | Breaking, convergent | Applies, both stores. Catalog: the `catalog` scope, its collections, their primary indexes, `extenddb_servers`, `extenddb_migrations`, the version row. Data: the `data` scope, `items`, `stream_records`, `stream_seq`, `idempotency`, `txn_atr`, `txn_leases`, `extenddb_migrations`, the base index `idx_items_tid_pk_sk` and its descending twin, the partial `idx_items_untid`, `idx_stream_shard_seq`, `idx_stream_ts`; no primary index on `items` or `txn_atr`. Adopts an out-of-tree deployment by reading its `catalog_version` document and deletes its `resource_tag_migration` marker. |
| 2 | `resource_tag_condition_keys` | Breaking, not convergent | Applies, catalog: the shared body from `extenddb_storage`, run against the `policies` collection through `HasManagementStore`. |
| 3 | `sort_key_definitions` | — | `NotApplicable`: no Couchbase catalog predates the #259 fix; every stored definition is complete. |
| 4 | `index_table_base_key` | — | `NotApplicable`: there are no index tables; index keys are derived columns in the item document. |
| 5 | `drop_continuous_backups` | Breaking, convergent | Applies, catalog, as a body that drops the collection if present and records the row: the out-of-tree baseline created no `continuous_backups` collection, and the body guards the later `--reconverge` rule that nothing creates one. |
| 6 | `backup_catalog_index` | Breaking, convergent | Applies, catalog: the index on `catalog.backups`, the restore job fields on the restore registrations, the maintenance claim record and the catalog id. |
| 7 | `stream_pending` | — | `NotApplicable`: the Couchbase reader stops at an unresolved sequence number and resolves it by a transactional read instead of holding records back (see "DynamoDB Streams"); no new collection. |
| 8 | `restore_progress` | — | `NotApplicable`: a store-backed restore keeps its progress on the job record, as MongoDB and Cassandra do. |
| 9 | `warm_throughput` | — | `NotApplicable`: a table document holds any field; nothing to add. |
| 10 | `account_metrics` | Breaking, convergent | Applies, catalog: replaces the out-of-tree `metrics` collection with `account_metrics` keyed by account, dropping the rows recorded without one (#728). |
| 11 | `attribute_name_escape` | — | `NotApplicable`: a Couchbase JSON document holds U+0000 in a field name as JSON does; the escape is PostgreSQL's. |
| 12 | `unique_access_key_ids` | Breaking, convergent | Applies, catalog: the body verifies that `access_keys` is keyed by the ID (the document key, unique by construction) and records the row. |

The kinds and convergent flags mirror the MongoDB backend's entries for the
same ids (ADR-0023 decision 1).

- **Bodies.** Every request in a body is idempotent: `CREATE ... IF NOT
  EXISTS`, "already exists" accepted by code and left to the post-check, any
  other error stops the body so nothing is recorded. A body's post-check
  compares `describe_store` (scopes, collections, index definitions) with
  `migrations/catalog/<n>.expected.json` and `migrations/data/<n>.expected.json`
  the first time it is applied to a store.
- **Recording.** A body and its ledger row are not one commit: the Couchbase
  Rust SDK has no multi-document KV transactions, and a SQL++ transaction takes
  no DDL. The backend therefore uses the Cassandra pattern: bodies are safe to
  run again, the ledger row (and in the catalog the version row) is written
  after the body with majority durability through `LedgerWrites` and
  `record_rows`, and a crash between the two re-runs the body on the next
  `migrate --yes`.
- **Lock and registrations.** The migration lock is the
  `extenddb_migration_lock` document in the bucket's `_default._default`
  collection, which exists in every bucket, so the lock is taken before the
  baseline creates the `catalog` scope (the Cassandra backend creates its lock
  table before taking the lock for the same reason). It is taken with a KV
  insert under a 60 s document expiry and renewed every 15 s by a CAS replace,
  so a migrator that dies releases it within 60 s. Each server holds a
  registration document in
  `catalog.extenddb_servers`, renewed every 5 s (every second after a failure),
  that expires 45 s after its last renewal and counts as lost after 30 s
  without one (`ServingRegistrar`), the values ADR-0023 gives MongoDB and
  Cassandra. `migrate --yes` waits for live registrations; a server whose
  registration lapses refuses requests, registers again, re-reads the catalog
  status, and serves only if the status it started with still holds. Every
  store request checks the lock's lease first and is bounded by a request
  timeout, as the MongoDB backend does.
- **Commands.** `CouchbaseCatalogMigrations` implements `CatalogMigrations` and
  is installed in `Backend.catalog_migrations`, so `init`, `migrate --yes`,
  `migrate --forget` and `verify` reach the Couchbase bodies through the shared
  commands, and `serve` registers and checks status before opening its engine.
- **Format history.** In-tree, the baseline *is* v3.2: there are no released
  in-tree versions to migrate from. The out-of-tree dual reader for v3.1
  array-form sidecars stays in the engine for existing deployments. The
  out-of-tree base-index rebuild moves into the baseline body (an index
  definition that differs from the expected one is dropped and re-created
  there), so no shape step runs outside the migration list. A later convergent
  data migration that rewrites any remaining v3.1 sidecars to map form would
  let the dual reader retire, and is proposed, not scheduled.
- **devtools.** `check-couchbase-catalog-lock` and
  `check-couchbase-older-binary` are added next to the MongoDB and Cassandra
  checks of the same names, `check-couchbase-schema` next to
  `check-mongodb-schema`, `run-couchbase-tests` stands up a Couchbase container
  for the crate's live tests, and `devtools/run-tests --backend` gains
  `couchbase`.

### Document structure for DynamoDB items

Each item is one JSON document. The business attributes sit at the top level as
native JSON; one reserved object, `_xdb`, carries the machinery. For a table
with partition key `pk` (S) and sort key `seq` (N), a GSI on `status` (S) and
`amount` (N), and a string-set attribute `tags`:

```json
{
  "pk": "a", "seq": 1, "status": "open", "amount": 250.50, "tags": ["x", "y"],
  "_xdb": {
    "tid": "863d1c610c4054b9",
    "pk": "S#a", "sk": "N#210011",
    "_g80d88d5epk": "S#open", "_g80d88d5esk": "N#210032505",
    "ddb": { "/tags": "ss" }
  }
}
```

- `_xdb.tid` is the table id, the partition every index and query is scoped by.
- `_xdb.pk` / `_xdb.sk` are the **encoded** key values the base index sorts on
  (see "Key encoding").
- `_xdb._g<slot>pk` / `_xdb._g<slot>sk` are the derived GSI and LSI keys, one slot
  per index (name-hashed), maintained on every write.
- `_xdb.ddb` is the **type sidecar**: a map from JSON-Pointer path to the DynamoDB
  type JSON cannot express on its own — string, number, and binary **sets** (stored
  as arrays), **binary** (base64 string), and numbers too wide for a double (kept
  as an exact string). A plain item has no sidecar and is 100% ordinary JSON.
- `_xdb.body` deep-wraps bodies nested beyond the depth Couchbase will index.
- `_xdb` is the **only** reserved top-level key. A business attribute itself named
  `_xdb` is shadowed under `_xdb.shadow`. The format signal is the presence of
  `_xdb`; there is no separate version field.

A number is stored as a native JSON number only when the text
`extenddb_core::validation::number::normalize_number` produces for it is
reproduced exactly by parsing the JSON number back and formatting it with the
backend's positional (never scientific) shortest-round-trip formatter; any
other number, including one that a double holds exactly but a shortest
formatter would print in exponent form, is stored as the exact string and
recorded in the sidecar.

The format is **v3.2**. The earlier out-of-tree formats (v1 DynamoDB-JSON
envelope, v2 `item`-wrapped, v3 / v3.1) remain readable by a dual reader, so
old and new documents coexist without a rewrite. `docs/runbooks/storage-format.md`
is the source of truth, including the two forward-only rollback edges.

### Key encoding

A key attribute value is encoded to a string whose byte order is DynamoDB's
order for that type, prefixed by a one-letter type tag and `#`:

| Type | Encoding of the value | Order |
|---|---|---|
| S | `S#` + the string's UTF-8 bytes | byte order, as DynamoDB |
| N | `N#` + the order-preserving numeric encoding below | numeric order, with no precision cap |
| B | `B#` + lowercase hexadecimal of the bytes | unsigned byte order, as DynamoDB (standard base64 does not preserve it) |

The tag is one of `S`, `N`, `B` and nothing else, which the hashed form below
relies on. `_xdb.pk` and `_xdb.sk` hold these strings, and so do the derived
index columns; the index sorts on them, so the document key's own order does
not matter.

**The N encoding** (`number.rs`). The value is normalised to a sign, a digit
string `d1d2...dn` with no leading or trailing zeros, and a decimal exponent
`e` such that the value is `0.d1d2...dn × 10^e`. It is then written as:

| Sign | Layout |
|---|---|
| positive | `2`, then `1000 + e` as four decimal digits, then the digits |
| zero | `1` |
| negative | `0`, then `1000 − e` as four decimal digits, then the nine's complement of each digit, then `~` |

The leading byte orders the sign classes. The four-digit biased exponent
(bias 1000, which covers DynamoDB's 10^-130 to 10^125 with room) precedes the
digits because magnitude dominates. For negatives the exponent and the digits
are complemented so the order inverts, and the `~` terminator, which is above
every digit, makes a prefix sort after its extensions (`-1.5 < -1`); positives
need no terminator because there a prefix sorts before its extensions
(`1 < 1.5`). Encoded values from the implementation:

| Value | Encoded |
|---|---|
| `-1.55` | `00999844~` |
| `-1.5` | `0099984~` |
| `-1` | `009998~` |
| `-0.5` | `010004~` |
| `0` | `1` |
| `0.001` | `209981` |
| `1` | `210011` |
| `1.5` | `2100115` |
| `10` | `210021` |
| `250.50` | `210032505` |
| `1e125` | `211261` |
| `-1e-130` | `011298~` |

**The document key.** Each encoded component is escaped before it is joined:
`\` becomes `\\` first and then `|` becomes `\|`, so the escape is reversible
and the separator cannot occur unescaped inside a component. The raw key is
then `<table_id>#<pk_esc>|<sk_esc>` (`<table_id>#<pk_enc>`, the partition key's type-tagged encoding unescaped, on a table without
a sort key, where nothing follows it to confuse). The reviewer's example, pk `x` with sk `y|S#z` and pk `x|S#y` with
sk `z`, gives `S#x|S#y\|S#z` and `S#x\|S#y|S#z`, which differ;
`tests/test_encoding_edges.py::test_separator_in_string_key_does_not_collide`
is that case, and a partition key containing `|S#` is added to it. Main's
`extenddb_storage::util::key::encode_netstring_composite` would do the same
job; the backend keeps the escape because every document key of every v3.x
deployment is already written with it, a document key cannot be rewritten in
place, and the escape is unambiguous (a `\` is always followed by `\` or `|`).

Couchbase allows a key of 246 bytes in a named collection, and every item
lives in a named collection, so a document key over **240 bytes**, table id included, is replaced by
`<table_id>#H#<hex SHA-256 of pk_esc|sk_esc>`, which is 3 + 64 bytes past the
table id; the escaped composite is hashed, so the pairs that differ raw differ
hashed. The two forms cannot collide: the segment after `<table_id>#` begins
with a type tag (`S#`, `N#`, `B#`) in the raw form and with `H#` in the hashed
form, and `H` is not a type tag. Both forms share the `<table_id>#` prefix,
which the point operations and the `META().id LIKE` predicate of Query and
Scan use as the table's keyspace. The key is built only by `full_doc_key`, and
`scripts/lint-storage-invariants.sh` blocks any hand-built key. A point read
of a hashed key returns the document and compares its `_xdb.pk` and `_xdb.sk`
with the requested key before answering; a mismatch, which SHA-256 makes
practically impossible, is treated as a miss. `LastEvaluatedKey` and
`ExclusiveStartKey` are the DynamoDB key attributes as attribute values (the
index keys and the base keys for an index query), which the engine builds from
the last item of the page; the backend re-encodes them into the resume
predicate.

### Reads: GetItem, Query, Scan

**GetItem** is a KV binary get (`Collection::get`): no query-service hop,
sub-millisecond, always strongly consistent. **BatchGetItem** fans out KV gets.

**Query** becomes a SQL++ range over the base index
`idx_items_tid_pk_sk (_xdb.tid, _xdb.pk, _xdb.sk)`; sort-key conditions
(`=`, `<`, `BETWEEN`, `begins_with`) translate to bounds on the encoded `_xdb.sk`.
Index queries use the per-index GSI on `_xdb._g<slot>pk/sk`. Pagination carries
the DynamoDB key attributes in `LastEvaluatedKey`, and a page resumes with a
tuple comparison on the encoded columns in the same direction as its `ORDER
BY`. `ScanIndexForward=false` runs the same statement with `ORDER BY ... DESC`
and the reversed comparison. Couchbase pushes an `ORDER BY` down to an index
only when the keys are aligned with the index order and does not scan an index
in reverse ("Index pushdowns"), so the base index and every GSI and LSI index
have a **descending twin** (`<name>_desc`, every range column `DESC`) that the
descending statement uses; the twins are created, built and dropped with
their siblings, `ensure_desc_twins` gives an existing deployment's indexes
their twins at server start, the twins count in the index budget (see
"Multi-tenancy"), and descending paging is in the tests
(`tests/test_desc_paging.py`: base table, GSI, LSI and range queries across
several pages). Measured: `EXPLAIN` of a descending Query plans on
`idx_items_tid_pk_sk_desc` with no `Order` operator, and the ascending one on
the base index, also without.

**Scan** is a SQL++ scan of the table's partition; a parallel-scan segment is
selected by hashing the encoded primary key (`SHA-256` of `_xdb.pk` and
`_xdb.sk`, modulo `TotalSegments`), shared between the scan and the
`ExclusiveStartKey` check. Filter expressions and projections are applied by
`extenddb-core` after the read; the backend deliberately does no filter
pushdown, so a correct-but-slower path is still correct.

**Consistency.** `Query` and `Scan` run at `request_plus` (read-your-writes),
always: the trait carries no `ConsistentRead` flag, and RFC-0002 allows an
eventual read served as strong and no mode that weakens one, so the out-of-tree
`eventual` setting is dropped from the in-tree backend. Point reads and all
metadata reads are strong. The engine rejects `ConsistentRead=true` on a GSI
with DynamoDB's `ValidationException`, so the backend neither advertises nor
exposes strong GSI reads; that index reads happen to see every committed write
at `request_plus` is an implementation property, not a promise.

### Writes and conditional writes

Writes go through the KV layer, which carries the durability level
(`durability`, default `majority`; `none` only for single-node demos).
Conditional writes are CAS-guarded: the backend reads the current document with
its CAS, evaluates the DynamoDB condition in Rust (`extenddb_core::expression`),
and replaces with the CAS it read; a CAS mismatch re-reads and retries with
jittered backoff.

**Bounds and answers.** The CAS loop runs at most 64 attempts and 10 s. A
conditional write that loses every attempt to other plain writers answers
503 `ServiceUnavailable`, which the SDKs retry, as the MongoDB backend does
after its 50 attempts; `TransactionConflictException` is answered only when the
item is held by a `TransactWriteItems` lease (see "Transactions"). An
unconditional `PutItem` with `ReturnValues=ALL_OLD` takes the same loop, since
the old image must be the one the write replaced; at exhaustion it has written
nothing and answers 503. A bare unconditional `PutItem` or `DeleteItem` with no
`ReturnValues` is one KV write with no CAS and no retry, so two racing calls
both succeed. Every plain write also takes the item's lease first and releases
it after, on every table (see "Transactions"); on a stream-enabled table every
write is a query-service transaction, and two single-item writes that meet on
one key both succeed, the later one retrying until the same 10 s deadline and
answering 503 at its end. The lease wait has no attempt count, only the
deadline. These rows go into `docs/differences-from-dynamodb.md` with the
implementation PR.

**UpdateItem** has a sub-document fast path, taken only for the narrow,
provably-correct case: `ReturnValues NONE`, no condition, no stream, every action
a `SET` of a bare placeholder holding a scalar (on a v3.2 table: number, string,
boolean, null, binary, or wide number; on a pre-v3.2 table only a native-range
number, boolean, or null) or a `REMOVE`, and every touched path a simply-named
top-level attribute that is not a key, index, or vector attribute. That update
becomes a single `MutateIn` (two round trips instead of three); on v3.2 the same
`MutateIn` writes the attribute's `_xdb.ddb` pointer, so the new value's type is
pinned regardless of what the attribute held before. One `MutateIn` takes at
most 16 operations; each v3.2 `SET` takes two, so an update with more than
eight `SET` actions, or whose `SET`s and `REMOVE`s exceed 16 operations, falls
back (the lease below is a separate document, so the `MutateIn` carries no
guard operations of its own). Anything else falls back to the CAS
read-modify-write loop, which
rebuilds the sidecar. Every update path re-checks the 400 KB item limit. Writes
to stream-enabled tables and all transactional writes take the query-service
transaction path below so the stream record and the data commit together.

### Global and Local Secondary Indexes

An index does not get its own collection. Its keys are derived into the item
document (`_xdb._g<slot>pk/sk`) on every write, and one Couchbase GSI per
DynamoDB index is created over those columns, scoped by `_xdb.tid` so that in
shared mode an index sees only its own table's items (the unscoped form was a
real defect, fixed; it is now the storage-invariant lint's job to keep it fixed).
There is no secondary write to keep in sync.

`CreateTable` with many indexes issues deferred `CREATE INDEX` statements and a
single `BUILD INDEX`, cutting a 20-GSI create from 97 s to 26 s `[ref-repo]`.
`UpdateTable` GSI creation runs in two parts. On the request path, under the
server's create lock, it creates the Couchbase index and its descending twin,
builds them, backfills `_xdb._g<slot>` for the existing items once, publishes
the catalog row with the index `CREATING`, and returns. A detached task on that
server then runs the fenced second pass: it sleeps the catalog freshness window
(`EXTENDDB_CATALOG_FRESHNESS_MS`, default 2 s, plus 0.5 s), waits up to 30 s for
every write on this server that was already in flight, backfills again, and
flips the index `ACTIVE`. The server's GSI worker repeats the backfill and the
flip 75 s after the publish, which covers a task lost to a restart and any pass
that left the index `CREATING`. Before its backfill the worker waits, with the
same drain helper and the same 30 s cap as the fenced pass, for the writes
counted before it to finish; on a timeout it warns, leaves the mark and the
index `CREATING`, backfills nothing, and retries at the next interval (`38eeab5`
`[ref-repo]`, round-5 branch). The 75 s is not what makes this safe: no longest
in-flight time is claimed, and the wait is what a worker pass after a drain
timeout depends on. No unit test covers the wait (the helper needs a running
engine).

The fence between the backfill and in-flight writes: `main`'s
`CachedTableKeyInfoStore` is stale-while-revalidate and `UpdateTable`
invalidates it only after the backend returns, which the backend cannot observe,
so the fence does not wait for that invalidation. Instead every single-item
write checks the key info it arrived with against the backend's own catalog
view, never older than the freshness window, and takes the index list from the
view, so a write that starts after the publish plus the window carries the
index. The in-flight counter covers the writes that started earlier. It is kept
per table and per generation: the fenced pass starts a new generation and waits
only for the writes counted before it, so a busy table does not hold it. A write
takes the counter before it reads its catalog view. `TransactWriteItems` (one
guard per table) and the reindex reconciler take it too; the reconciler reads
the catalog row for every document it rewrites, one KV get per document, rather
than reusing key info cached earlier in its drain. A drain that times out leaves
the index `CREATING` for the worker's pass instead of flipping it. Each
backfilled item, and each document the reindex reconciler rewrites, is rewritten
under the item's `plain` lease, like any plain write, and what the rewrite does
depends on the lease it finds (`e7f94b0`, `84c30aa` `[ref-repo]`, round-5
branch): on a `transact_write` lease it skips the item at once (the reconciler
leaves it queued and logs it; the backfill counts it skipped); on a
`transact_get` or `plain` lease it waits up to 10 s and then skips; on a
`committing` lease (a committed transaction not yet visible to KV reads, or one
whose outcome was unknown) it takes the lease over by CAS and rewrites in a
query-service transaction, with one `replace-expect` carrying the CAS it read,
which is the path of an escalated plain write, so a `committing` lease does not
hold an index `CREATING`. A skipped item makes the fenced pass leave the index
`CREATING`, and the worker retries every 30 s, so an item skipped on a
`transact_write` lease, or on a `plain` or `transact_get` lease still held after
10 s, keeps the index `CREATING` until a pass finds it free (the first backfill
on the request path ignores that count, so `UpdateTable` does not fail on it).
The takeover of a `committing` lease by a background rewrite was not exercised
(it needs a harness that holds a commit between its commit point and its
unstaging) and has no unit test; the unit tests cover the lease arithmetic only.
The counter is
per server process; with one server per catalog it covers every writer. The
sub-document fast path reads the same key info and refuses any attribute an
index names, so it is covered by the same fence.

What tests the fence, honestly. `tests/test_gsi_fence.py` `[ref-repo]` races four
ordinary writers against the create and finds every item in the `ACTIVE` index,
but it holds nothing, so a fence that waited for nothing could pass it. The held
write test (`tests/test_fault_hooks.py` `[ref-repo]`) has one hold point, after
the catalog-view read. A `PutItem` whose key info was resolved before the
publish is parked there, and the test holds it for the catalog freshness window
(`EXTENDDB_CATALOG_FRESHNESS_MS`, default 2000 ms) plus 0.5 s plus 3 s, which is
past the point where a pass that waited for nothing would have run its second
backfill and flipped the index. While the write is held the test asserts that
the index is still `CREATING`; it then releases the write and requires that the
write lands with the index column, that the index goes `ACTIVE` and that a query
on it returns the held item (`7cc900a`, round-5 branch `[ref-repo]`). Tess-2 to
Tess-6 are runs of `tests/test_fault_hooks.py` on a `fault-hooks` build on the
single-node demo stack (`PutItem` only), one run each; the held-write test
passed on every one, on the earlier form of the test in Tess-2 to Tess-5 and on
the rewritten form in Tess-6 `[ref-repo]`. Negative control (Tess-7, same stack and
build, `[ref-repo]`): with the early return added to the one function both drain
waits share, so that neither the fenced pass nor the worker's pass waits, the
rewritten test failed at "index left CREATING while a write was held: ACTIVE" in
two of three runs, and failed once with a client read timeout that has no
explanation yet; no patched run passed. The unpatched build passed the same test
once, in 8.5 s. That shows a fence with no drain fails this test. It does not
show that a short or partial drain, or a fault in only one of the two passes, is
caught (both were removed together), nor the multi-server case. The
guard before the view read, the transaction guard, the reconciler change, the
worker's drain wait and the background `committing` takeover are built and
unit-tested only (the takeover is not exercised), with no dedicated test; the
reconciler held-write test is a skipped stub (**OPEN**).

`index_num_replicas` sets `num_replica` on every index the backend creates,
failing closed when the cluster has too few index nodes. Multi-part RANGE keys
are implemented on every part and not yet exercised (engineering item 13): the backend derives one column per part
(`_g<slot>sk`, `_g<slot>sk1`, ...) and builds per-part predicates with tuple
pagination. The engine classifies the conditions: `resolve_multipart_with`
(upstream commit `f49c5f82`, #429, 31 October) is what hands the backend a
per-attribute condition set. At the pinned engine revision `7b828b0` that
function does not exist and the engine refuses a multi-condition Query on such
an index before the backend sees it, so the per-part predicates are implemented
but not yet exercised by the upstream multi-part tests (engineering item 13).
The pin moves to a revision containing `f49c5f82` before that item can pass.

### Transactions

The Couchbase Rust SDK ships no distributed multi-document KV transactions, but
the **query service** exposes ACID transactions to every client
(`BEGIN WORK` / statements / `COMMIT WORK`). `TransactWriteItems`,
`TransactGetItems`, and every write to a stream-enabled table run as one such
transaction **in process, in Rust**; there is no sidecar and no dependency beyond
Couchbase. All statements of one transaction must execute on the node that ran
`BEGIN WORK`, so the engine pins each transaction to a query node (carried in the
opaque txid), round-robins across nodes, retries `BEGIN` on the next node, and
re-discovers the node list when all cached nodes fail. Durability is mapped from
`durability`; `txtimeout` is `txn_timeout` (default 30 s). Transaction records
live in `data.txn_atr`. A failed statement inside a SQL++ transaction rolls back
that statement only, so the backend issues `ROLLBACK WORK` itself whenever a
statement it needs fails, the idempotency-token insert included; a cancelled
transaction's token is released, so a retry with the same token runs again once
the condition holds, as `tests/test_conditional_atomicity.py` expects.

**Plain writes and transactions on one document.** Couchbase's transactions
documentation says a non-transactional write must not touch a document while a
transaction that involves it is in progress, calls the outcome undefined when
one does, and says a non-transactional actor cannot see a transaction's staged
insert of a key that does not exist yet. The backend therefore does not let its
own writers meet at the document: on every table, **ExtendDB's plain writes and
transactions exclude each other through one lease document per item**, and nothing depends on
how Couchbase stages a mutation. There is no per-table switch: the first
transaction on a table needs no state change, and no plain write can have read a
stale "no transactions here" view.

- The **lease** is a document in `data.txn_leases`. Its key is the lower-case
  hex SHA-256 of the item's scope, collection and document key, so it is always
  64 bytes, under Couchbase's 246-byte key limit whatever the item key. It is
  taken with a KV insert (fail-if-exists, which is the whole mechanism) and
  carries `id` (a per-request id), `op` (`transact_write`, `transact_get`,
  `plain`, or `committing`) and `exp` (wall-clock milliseconds, compared on
  every server that reads it; the native expiry is the backstop). A lease is
  three KV operations around a write: the insert, a get that reads it back with
  its CAS, and a remove by that CAS, which checks the owner `id` first. Every
  lease insert, replace and remove carries the backend's `durability`
  (`majority` by default), so a lease is not lost in a failover unless the
  operator turned durability off. The native expiry is 10 s for a transaction's
  lease (renewed), 30 s for a plain KV write's, `txtimeout` plus 20 s for a
  plain write that runs as a query-service transaction, and 24 h for
  `committing`.
- A **`TransactWriteItems`** takes a lease on every target item, including
  `ConditionCheck` items and keys that do not exist yet, in sorted order, within
  2 s (past it the transaction answers `TransactionCanceledException` with
  `TransactionConflict` on the items it could not lease), stages its writes, and
  renews every lease every 3.3 s by a CAS replace from its own task. Before
  `COMMIT WORK` it verifies by CAS that it still owns every lease; a lease it
  lost (a renewal failed, or expired) makes it `ROLLBACK WORK` and answer the
  same cancellation, so nothing it staged reaches a document while anything else
  may be writing. Then it rewrites its leases to `op = "committing"` by CAS and
  sends `COMMIT WORK`. Couchbase's commit is eventually consistent for KV
  readers: `COMMIT WORK` can return before every document is unstaged. For every
  CAS-pinned action (a replace or delete that carries the CAS it read) the
  backend deletes the `committing` leases, and answers success, only after a KV
  read of the written document shows the committed body, or its absence for a
  delete, within 15 s `[ref-repo]`. Past 15 s the success is still answered and
  the leases stay `committing` until the next writer ends them (below). An
  action that does not pin a document by CAS (an unconditional `Put`, a `Put`
  that only requires the key to be absent, an unconditional `Delete`, a
  `ConditionCheck`) gets no such wait, so a transaction made only of those
  deletes its leases when `COMMIT WORK` returns: that gap is Gap A, **OPEN**
  (engineering item 15). Every lease delete is a CAS remove that checks the
  owner id first.
- A **plain write** (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`)
  takes the same lease with `op = "plain"` before it writes and deletes it
  after; a bare `PutItem` becomes lease insert, KV write, lease get, lease
  remove. When the insert fails: a `transact_write` lease answers
  `TransactionConflictException` at once, which is DynamoDB's answer while a
  transaction holds the item; a `transact_get` or `plain` lease is retried with
  jittered backoff until the 10 s deadline (there is no attempt count on the
  lease wait; the 64 attempts belong to the CAS loops), and the write answers 503
  at its end; a `committing` lease (a committed transaction not yet visible to
  KV reads, or one whose outcome was unknown) is taken over by CAS, and the
  write runs as a query-service transaction, which Couchbase orders after the
  committed one, holding the lease across it. That escalated write, and every
  single-item write on a stream-enabled table, commits through `COMMIT WORK` and
  deletes its lease when it returns, with no wait for the commit to be visible
  to KV reads: that is Gap A (engineering item 15, **OPEN**), so the next plain
  write is an ordinary one only once that commit is visible.
- A **transaction** that meets a `committing` lease does not cancel: a
  `TransactWriteItems` takes the lease over by CAS; a `TransactGetItems` waits on
  it for its 5 s, then reads that key inside a throwaway query transaction,
  which sees the committed version, and repeats the whole read until two passes
  agree (see "TransactGetItems").
- **A lease is a bound, not a fence.** A plain write finishes well inside its
  lease in normal operation, and a transaction checks its own leases by CAS
  before it commits, but nothing re-checks a plain write's lease between taking
  it and its KV write, and a bare `PutItem` is one unfenced KV write. A process
  stalled for longer than the lease (30 s for a KV write) can still land a write
  after the lease lapsed, and nothing bounds how late that write lands: the
  lease length bounds how long contending writers wait and how long a crashed
  holder blocks its key, and the transaction's pre-commit CAS check covers the
  transaction's own leases, not a stale plain write that lands later. The
  durability level makes each write durable; it does not bound when a stalled
  write lands. The design accepts that, as any lease without a fencing token
  does.
- **Every ending path deletes the leases, except the ones that must keep them.**
  A `TransactWriteItems` that cannot take its leases, whose condition fails,
  that rolls back, whose lease is lost, or whose `txtimeout` expires before the
  commit point deletes the leases it holds before it answers, so a plain write
  after it is not refused until the 10 s expiry. A commit that is not yet visible
  to KV reads, and a commit whose outcome is unknown, deliberately keep their
  `committing` leases, so the next writer orders itself after a transaction that
  may have applied. A request abandoned by its client mid-transaction is not
  covered by a release; the native expiry (10 s, or 24 h for `committing`) is the
  backstop, and whether the server detaches the request from the connection is
  not checked.

The outcomes for one key, both orders:

| First | Second | Outcome |
|---|---|---|
| `TransactWriteItems` holds the lease | `PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem` on it | the plain write answers `TransactionConflictException`; the transaction commits |
| `TransactWriteItems` is committed but not yet visible to KV readers (or its node failed before it unstaged) | a plain write on it | the plain write finds the `committing` lease, takes it over by CAS, runs as a transaction ordered after the committed one, and succeeds |
| `TransactWriteItems` is committed but not yet visible to KV readers | a second `TransactWriteItems` on the item | the second takes the `committing` lease over by CAS and stages against the committed image |
| `PutItem` holds the lease | `TransactWriteItems` on the item | the transaction retries its lease for up to 2 s (the plain write holds it for milliseconds), then stages against the new image |
| `PutItem` commits | `TransactWriteItems` on the item | the transaction reads the new image, evaluates its conditions against it, commits |
| `TransactWriteItems` holds the lease | a second `TransactWriteItems` on it | the second fails to take the lease within 2 s and answers `TransactionCanceledException` with `TransactionConflict` on that item |
| `TransactWriteItems` creates a key that does not exist | a `PutItem` of that key | the key's lease was taken before the insert was staged, so the `PutItem` answers `TransactionConflictException`; the staged insert's invisibility to plain actors never matters because no ExtendDB writer writes without the lease, the background writers included (the GSI backfill and the reindex reconciler rewrite under the item's lease, see "Global and Local Secondary Indexes"); the exceptions are a write from the Couchbase side, through SQL++, Eventing, Mobile or XDCR, which takes no lease and, made while a transaction is in progress, can interfere with the transaction's updates or be overwritten by them (Couchbase's transactions documentation, above), and a plain write stalled past its lease, which is the bound described above |

**TransactGetItems.** SQL++ transactions run at read committed with a monotonic
atomic view, which does not by itself make a multi-item read serializable. The
backend does not read inside a query transaction, except for a key still `committing` at the bound (see below). It takes a `transact_get`
lease on every key it reads, present or absent, in sorted order, then reads each
key by KV and releases the leases. An absent key is protected by its lease alone:
no plain write creates it while the lease is held, and a `TransactWriteItems`
that would create it must take the same lease first, so the read returns it
absent and every other value as of one instant, which is the snapshot RFC-0003
3.5 asks for. This closed the torn-read defect QE found (14 of 150 torn pairs
to 0) `[ref-repo]`. No document is rewritten, so the read changes no CAS and
writes no stream record. A plain writer that meets the read's lease retries
until its 10 s deadline and answers 503 at its end (DynamoDB fails the read
instead; the difference is that the plain write here waits milliseconds rather
than failing, and goes in the differences document). The read has one deadline
of 5 s, taken before its first lease, for acquiring the leases and for the
agreement passes together, plus an allowance of 3 s for one pass already running
(an estimate, not measured), so the read ends within 8 s of its first lease, 2 s
inside the 10 s lease; the leases are not renewed. Within the deadline it waits
on a `transact_write` lease, and then answers `TransactionCanceledException`
with `TransactionConflict`, as DynamoDB cancels a read that meets an ongoing
transaction. It waits on a `committing` lease for the same deadline; a key still
under one then is read inside a throwaway query transaction, which sees the
committed version, and the whole set is read again until two passes agree; the
deadline is checked before each pass, and once it has passed the read is
cancelled the same way. Every cancelled or failed read deletes its leases, and
the section that holds them runs detached, so a client that drops the request
still releases them (`3ffb3df` `[ref-repo]`; a compile-time check and two unit
tests cover the arithmetic, and the release on cancel is not unit-tested). On a
very slow cluster a pass over 100 keys could still outlast the 3 s allowance and
the lease; that is not measured. A `ConditionCheck`
item in `TransactWriteItems` is leased like every other target, so a plain write
to it between the check and the commit meets the lease and is refused.

**Limits.** A `TransactWriteItems` of 100 actions and 4 MB runs as one SQL++
transaction, and two query-service settings bound it: `txtimeout` (the
backend's `txn_timeout`) and the node's `memory-quota`, which bounds a
transaction's delta table and defaults to 0, no quota. A transaction that runs
past `txtimeout` before its commit point is rolled back by the query service
and answers 503 `ServiceUnavailable` with nothing written. One whose `COMMIT
WORK` was sent and whose outcome is unknown (no answer reached the backend, the
node went away, error 17004, or an error the backend does not recognise) is
never run again. The rule, from the code: an error 17010 at `COMMIT WORK` with
no `raise` is Expired, which is retryable, and the stream writes a skip marker
only when the answer says nothing committed; `raise` is checked before the code
(conn.rs:177-181, 220; engine.rs:2006); 17004 and unrecognised `COMMIT WORK`
errors are Ambiguous `[ref-repo]`. After an unknown outcome the backend reads
the request's idempotency token, which is written inside the same transaction,
with `txn_peek`, a read by key inside a throwaway query transaction (`3231463`
`[ref-repo]`): a token with the request's fingerprint means the transaction
committed, an absent token or another fingerprint is taken as not committed, and
a failed read is an unknown outcome that keeps the leases. This replaces a KV
read, which can miss the token of a transaction that did commit when Couchbase
has not unstaged it yet (Gap B, engineering item 15). The decision is
unit-tested; a committed token that is still staged, and a failed `txn_peek`,
are not tested end to end, because the test hook cannot make a committed token
invisible to a KV read. The other token reads, the pre-check before the writes
and the `Exists` arm of the retry path, are still KV reads (Gap B, **OPEN**). If
the token is there with the request's fingerprint, the backend waits for the
written documents to be visible to KV reads, but only for the CAS-pinned actions
(Gap A: the other actions get no wait; see "Plain writes and transactions on one
document"), and answers success; otherwise it answers 500
`InternalServerError` ("commit outcome unknown, not retried") with the query
service's error in the server log and leaves the leases `committing`, so the
next writer orders itself after the transaction if it did commit. A client
retry with the same `ClientRequestToken` then replays the committed result or
runs the request if it did not commit, so the outcome is resolved by the retry.

A `TransactWriteItems` with no `ClientRequestToken` has no token document to
read. If its `COMMIT WORK` outcome is unknown, the backend does not guess: it
answers 500 `InternalServerError` ("commit outcome unknown, not retried") and
leaves the request's leases `committing`, so no plain write runs ahead of a
commit that may have happened, and it never runs the writes again itself. A
client that retries the same request without a token runs it a second time, and
applies it again if the first commit succeeded; boto3 fills a token in by
default (checked on botocore 1.43.86), so this path is for a caller that
bypasses it. This no-token behaviour is tested on a `fault-hooks` build of the reference
repository's backend on a single-node demo stack [ref-repo]: an injected ambiguous
`COMMIT WORK` with no token answered 500 and was not retried, and the same
fault with a token resolved to success with the counter at 1.

The operational requirement is `memory-quota` at 0 (the default, no quota). On
the one-node demo stack, at that default and at `EXTENDDB_DURABILITY=none`
(which a one-node bucket needs), a 100-action request of about 4 MB on a
stream-enabled table with `NEW_AND_OLD_IMAGES` committed in 0.67 s
`[ref-repo]`. A node configured below what a request needs makes the query
service raise "Transaction memory (N) exceeded quota (M)" (code 17016): at a
statement the backend answers 503 with that error logged and nothing written,
as observed with a 1 MB quota `[ref-repo]`; at `COMMIT WORK` it is an unknown
outcome, as above (unit-tested only). No partial commit was observed at a statement.

`ClientRequestToken` idempotency is a document in `data.idempotency` with a
600 s retention; a replay with a matching fingerprint returns the original
result, a mismatch is rejected (see RFC-0003 row 3.3 for the atomicity with the
writes).

Validated `[ref-repo]`: conformance 912 + the 31-test transaction suite + the 15-test
concurrency suite single-node; on a 3-query-node cluster, transactions survive a
query node being failed over mid-flight (`scripts/query-txn-multinode-test.sh` `[ref-repo]`);
`tests/test_transaction_rollback.py` `[ref-repo]` covers single-key contention, cross-table
all-or-nothing, canceled-stream no-orphan, and a backend crash mid-transaction.

### DynamoDB Streams

Streams are explicit records in `data.stream_records`, not Couchbase's DCP or
Eventing, so sequence numbers, shard assignment, and retention follow the
DynamoDB contract exactly. In-tree a table's streams are a lineage
(`extenddb_storage::stream_lineage`): disabling a stream closes it and gives
every shard an `EndingSequenceNumber`, enabling again creates a new stream with
a new label, and a shard id is
`shardId-{table_id}-{label_millis:013}{view_type:1}{index:02}`. Each stream has
`SHARDS_PER_STREAM` (four) shards, a record's shard is chosen by the hash of its
item's encoded partition key, and each shard has its own counter document in
`data.stream_seq`. The out-of-tree backend's one shard per table is replaced by
this scheme as part of the alignment work, which also closes the two
`test_stream_table_reuse` cases it fails today, and `UpdateTable` stream enable
and disable are supported. Records carry `INSERT` / `MODIFY` / `REMOVE` with
before and after images as the stream view type requires; a write that does not
change the item emits no record, which is what DynamoDB does (measured by the
reviewer on 2026-11-20; `main` differs, #1229).

**Ordering and visibility.** A record's sequence number is taken from its
shard's counter with an atomic KV increment *before* the write's transaction
commits, so under concurrent writers N+1 can commit before N, and a number
whose transaction did not commit never holds a record. The writer sends
`BEGIN WORK` first and takes the number after it returns, because the
transaction's `txtimeout` clock starts when `BEGIN WORK` succeeds (and `BEGIN`
may retry on another node); the number is also taken after the write's
pre-read, and the step from taking it to its commit or skip marker runs as
its own task, so a client that goes away mid-write cannot leave a number
unresolved. A **skip marker** (a `skip: true` document at the record's own
key, inserted fail-if-exists) is written for a number only when the answer says
nothing committed: the statements or the pre-commit check failed and the writer
rolled back (it does not look at the answer to `ROLLBACK WORK`), or `COMMIT
WORK` answered that the transaction lost a race, expired before its commit
point, or was rolled back. A commit with an unknown outcome writes no marker,
so a marker can never hide a committed record: a `commit_ambiguous` answer, a
transaction the query node no longer holds, a request sent without a readable
answer, a resource-limit error at `COMMIT WORK`, and any answer the backend
does not recognise. Such a number is resolved by the reader. `GetRecords` is a
`request_plus` SQL++ read of `idx_stream_shard_seq`; it walks the shard in
sequence order and **stops at the first missing number** unless that number is
resolved: by a skip marker, or, once the writer's timestamp on the row after it
is older than `txn_timeout` plus a 15 s margin (the HTTP slack of a `COMMIT`
still in flight; the writers' clocks must agree to within what is left of it),
by **reading the number's record by key inside a query-service transaction**
(`SELECT ... USE KEYS`, a read by key and not an index scan), which sees the
committed version as soon as the transaction record says committed, whatever
the state of unstaging or of a node that has not been failed over. If that
read finds the record, it is delivered; if it finds nothing, the number is
skipped; if the read fails, the stop stays. A call makes at most 64 such reads;
a shard with more open gaps resolves them over several polls. Couchbase gives
no bound on cleanup after a node failure, so no age alone resolves a number.
The iterator only ever moves over the contiguous resolved prefix, so a record
that commits late is still delivered, exactly once, and `LATEST` and
`AT_TIMESTAMP` iterators start no later than the first unresolved number of
the shard (`AT_TIMESTAMP` is a DynamoDB Streams iterator type that `main`
supports from #468, `6aef346f`, and the pinned engine does not have; for it
this is the in-tree design, engineering item 2). Implemented in
`resolve_aged_gaps` and `txn_peek` (at most 64 probes per `GetRecords` call),
with `contiguous_prefix` as the fast path for a gapless prefix. The test is the
three-node harness's commit-point hold `[ref-repo]`: the harness watches the
backend log for the `COMMIT WORK` of a 20-document filler transaction, freezes
the query engine that received it (`SIGSTOP`) half a second later, once the
commit point is written and the unstaging is under way, and holds it 60 s,
longer than the gap grace, under three transaction writers, two plain writers
and a stream reader on one table. Its gate fails a run unless the backend logged
at least one stream record resolved by a transactional read after the freeze
began, which is a number that the walk's `request_plus` read did not return and
a transactional read by key did, and the reader, against the final shard
content, never passed a number whose record appeared later
(`scripts/query-txn-multinode-test.sh` step 7,
`scripts/txn-commitpoint-test.py`). The log line (engine.rs:8772) fires on any
transactional-read hit, carries no transaction id (a stream record has none) and
does not check that the KV read missed, so even a nonzero count would not show
that the record was still staged at the instant of the read.

Result. The hold was run twice on the round-4 branch (a worktree at `60192cd`,
three query nodes; that the image was built from that commit is inferred from
timestamps, not proven) `[ref-repo]`. The safety invariants passed both times:
the driver's 10 invariants held (no lost or overwritten plain write, every
acknowledged transaction applied, the live reader monotonic and never past a
later record), and the 31 transaction conformance tests, 10 concurrency rounds,
the cross-collection rollback and a failover (6 of 6 committed) passed. Run 1:
transactions ok 1421, cancelled 114, unknown 0, timeout 0; filler ok 6, unknown
1; plain writes ok 200, conflicts 2987, 503 0; stream records live 2965,
final 3164. Run 2: transactions ok 1271, cancelled 120, unknown 0; filler ok 8,
unknown 0; plain ok 315, conflicts 2542, 503 0; stream live 2877, final 3019.
**The gate failed both times**, in all six attempts: 0 stream records were
resolved by a transactional read after the freeze (the `txn_peek` count is 0,
and neither run's backend log holds that line at all), although each freeze
landed on the node named in the `ops=41` commit line about 0.5 s after the send.
The run therefore shows the safety invariants under a freeze; it does not show
that any record was committed and unstaged during the hold, or that the gap
reader's transactional path ran. This is **OPEN** and is not a pass.

The reviewer asked for a gate on a state seen after the freeze begins: a KV get
misses a filler record that a transactional read by key returns. That condition
cannot be met on the version tested as probed: on Couchbase 8.0.4, single node,
two probes (Pip, Quin) found no staged state observable from outside a
transaction `[ref-repo]` (see engineering item 15), so no KV-miss,
transactional-hit state has been seen to gate on. A harness that lands the
freeze between the commit point and unstaging is still owed. The record counts
quoted in revision 4 could not be traced to a run and are withdrawn.

**Single-item writes on a stream-enabled table** take the item's lease like a
write on any table (see "Plain writes and transactions on one document") and
then run as a query-service transaction, so the lease's expiry is `txtimeout`
plus the HTTP slack plus 5 s rather than the 30 s of a single KV write. Two that
meet on one key do not meet at the document: the second waits on the first's
lease, retrying within the 10 s bound, and writes after the first has released
it, so both succeed, which is what
`test_racing_puts_on_a_stream_table_all_succeed` expects, and at the end of the
bound the write answers 503. The paths that can still conflict at the document
are a writer that takes no lease (a write from the Couchbase side), a holder
stalled past its lease (the bound described above), and a second write that
starts after the first's `COMMIT WORK` returned and before its commit is visible
(Gap A, **OPEN**); between ExtendDB's own lease-holding writers no other is
known. `TransactionConflictException` is answered only
when the lease says `transact_write`, which is how a write tells a transaction
from another single-item write. The shard counter is a plain atomic
increment, so writers of different keys do not contend on it.

`GetRecords` paginates on `idx_stream_shard_seq`; a 24-hour retention is
enforced by `idx_stream_ts` plus a delete on every TTL sweep cycle (every
30 s).

### Time to Live

Native Couchbase document expiry is deliberately **not** used: it would delete
items without a stream `REMOVE` record and without the `Service` identity
DynamoDB attributes to expirations. Instead the TTL sweeper deletes expired items
through the normal delete path, with the DynamoDB five-year floor on expiry
timestamps (an epoch more than five years in the past is not an expiry).
Enabling TTL creates a per-table index on the attribute; the sweeper finds
expired items with `attribute_exists(#ttl) AND #ttl <= :now AND #ttl >= :floor`
over the top-level native number and deletes them in batches under that same
condition, so a concurrent update that pushes the expiry out is never lost. A
TTL attribute whose value is kept as an exact string in the sidecar (a number
too wide for a double) never matches that predicate and is never an expiry;
DynamoDB likewise ignores a TTL value that is not a valid epoch in seconds.

### Control plane, authentication, authorization

Table definitions, IAM principals, policies, sessions, tags, settings, metrics,
rate-limit windows, and backup metadata are documents in the `catalog` scope.
SigV4 authentication is fully supported. Policy evaluation (IBAC, RBAC, ABAC
with principal and resource tags, `LeadingKeys` fine-grained access, permission
boundaries, AssumeRole sessions) is upstream's; the backend persists the
primitives.

**Secrets.** The backend uses `main`'s code and nothing of its own: access-key
secrets are encrypted through `extenddb_storage::secret_cipher` on aws-lc-rs
(ADR-0026), under the server's `SecretKeys` built at startup through
`server_components::load_secret_keys`, either the catalog key or the active DEK
of a key ring stored as the `encryption_key_ring` settings row and sealed under
a KEK file, with rotation and switch-over through the shared `extenddb key`
commands and per-row AAD; passwords go through `extenddb_storage::password`.
The backend ships with `main`'s default key placement (the key in the catalog)
and supports `[auth.encryption] key_file` for a KEK outside the bucket, which
is the recommended production setting since a bucket backup must not contain
the key that decrypts its access keys. The out-of-tree `aes-gcm` dependency and
the `EXTENDDB_ENCRYPTION_KEY[_FILE]` / `_PREVIOUS` settings are dropped, and
`key_conformance` runs in the crate's live tests. Login rate limiting fails
closed on a Couchbase error.

### Backup and restore

`main` owns backup in the engine; a backend provides `SnapshotEngine`,
`BackupCatalog` and `RestoreEngine`. Couchbase has no read that fixes an instant
across a table: query-service transactions are read committed and KV has no
cross-document snapshot. The backend therefore follows Cassandra:
`open_table_snapshot` returns `StorageError::Unsupported`, `CreateBackup` takes
the in-catalog logical backup (ADR-0019), which is consistent per item and not
at an instant, `RestoreTableFromBackup` recreates the table through the normal
`CreateTable` path (GSIs and LSIs included) and restores byte-identically, and
`docs/differences-from-dynamodb.md` gets the Couchbase entry in its backup row.
A write fence for the duration of a snapshot read was considered and rejected:
DynamoDB backups do not affect writes. The point-in-time recovery operations
are answered by the engine without storage, for every backend. Export is the
engine's paged `Scan` with no point in time, and import goes through the normal
write path; neither needs anything from the backend beyond `Scan` and
`PutItem`. Backup's adopt pass picks up `_xdb`-less documents edited from the
Couchbase side through a partial index rather than a sequential scan.

### Server model and background workers

One ExtendDB server runs per catalog, as `README.md` states for every backend.
The server is stateless beyond the per-process caches the engine keeps (key
info, auth, console sessions), which the server that ran a DDL invalidates
itself, so no cross-instance freshness check is needed. The three background
workers (TTL sweeper, reindex reconciler for out-of-band edits, index
reconciler) run as tasks of that server, spawned by
`CouchbaseRuntimeHooks::spawn_workers`; the out-of-tree leader lease exists for
several servers and is not part of the in-tree backend. The Helm chart
(`deploy/helm/extenddb-couchbase/`) deploys one replica; running several
servers on one catalog is the subject of the follow-up RFC named under Scope.

Liveness is the static `/health`; readiness is `extenddb-couchbase verify`, a
deep probe that pings the data connection and enumerates tables and indexes,
bounded by a 30 s connect timeout so it fails loud instead of hanging.

Proven on a 3-node Couchbase cluster: an acknowledged `majority` write survives
a node crash and failover; the conformance sweep runs through a mid-run
failover with no data loss (nightly build #51 on 2026-11-02, commit `201f03f` `[ref-repo]`,
run against the older `e41f370` test suite as noted in row 6.3: 913 tests, 9
non-passes under failover of which 4 were new, re-run of those 4 passed 4 of 4 `[ref-repo]`);
and the 14-case QE cluster-ops suite (rebalance in and out under load, stopped
and swap rebalance, graceful and hard failover, delta and full recovery,
auto-failover, node SIGKILL, churn, index-node add and remove) passes on the
`extenddb-couchbase-qe-cluster-ops` Jenkins job `[ref-repo]`.

### Alignment with upstream `main`

The backend was built and validated against ExtendDB `v0.1.x` (pinned rev
`7b828b0` on the `couchbaselabs/extenddb` fork, which is 329 commits behind
`main` at the time of this revision and carries six commits `main` does not:
the fixes named under Scope); the `Storage` crate has grown from 13 traits to
22. The in-tree move is therefore a port, not a copy. In order:

1. **The first PR.** The mandatory traits of RFC-0002 complete with their
   tests, `backend()` returning `extenddb_storage::Backend`, the migrations
   module above with an entry for every id, `init`, `migrate`, `verify` and
   `serve` through the shared commands, the `[storage.couchbase]` settings,
   the `devtools` checks, the `couchbase-integration` job as a required check,
   and the README row at Experimental.
2. **Secret keys.** `secret_cipher`, `SecretKeys`, the key ring,
   `key_conformance`.
3. **Streams.** Stream lineage shard ids, four shards per stream, enable and
   disable.
4. **Backup.** `SnapshotEngine` returning `Unsupported`, `BackupCatalog`,
   `RestoreEngine`, `LegacyBackupReader` for backups the out-of-tree backend
   wrote; `snapshot_conformance`.
5. **Metrics.** The account dimension `MetricsStore` now carries
   (`metrics_conformance`).
6. **The rest of the surface.** `StorageEngine`, `TableKeyInfoLookup`,
   `JobClaim`, `MaintenanceClaim`, `ServingRegistrar`, and whatever the trait
   set holds when the implementation PR opens.

Each step lands with the required suites green on the port so far; the numbers
in this RFC are the out-of-tree backend's and are re-measured in-tree before
FCP.

### Multi-tenancy and the isolation ladder

Tenancy is by AWS account: a distinct table id per (account, table), the catalog
keyed by `account#name`, and every read path scoped by `_xdb.tid`.
`table_isolation` adds physical isolation:

| Tier | Value | Layout | Gains |
|---|---|---|---|
| 1 | `shared` (default) | all items in `data.items` | tables without a secondary index or TTL add no index; the count of index-bearing tables is bounded by the cluster index limit below |
| 2 | `collection` | `data.t_<tid>` per table | per-table TTL, eviction, XDCR filters; `DeleteTable` drops the collection |
| 3 | `scope` | `acct_<id>.t_<tid>` | per-account Couchbase RBAC (`docs/runbooks/per-account-rbac.md`) |

Placement is recorded on the catalog document; pre-existing tables need no
migration. Two Couchbase limits are cluster-wide: 1,000 scopes and 1,000
collections per cluster ("Scopes and Collections"), and 10,000 indexes per
cluster (the Operator documentation). In the shared tier every GSI, LSI and
TTL index of every table is an index on `data.items`, so each index-bearing
table costs its index count against the 10,000 and the index maintenance of a
write grows with the tier's index count. The budget counts what Couchbase
counts: every index definition the create would add, its descending twin, and
`1 + index_num_replicas` copies of each, so a table with `g` secondary indexes
costs `2 × g × (1 + replicas)` index instances. A TTL index is one index
without a twin and costs `1 + replicas`; `UpdateTimeToLive` takes the same lock
and passes the same check (built and unit-tested on the reference repository's
round-4 branch `[ref-repo]`; the unit test covers the cost arithmetic only).
The check reads the cluster's scope, collection and index counts from the query
service's system keyspaces (`system:scopes`, `system:keyspaces`,
`system:indexes`) over every bucket the backend's user can see, under the
backend's own create lock: an in-process mutex of one server, taken by
`CreateTable`, `UpdateTable` and the TTL index creation before the check and
held until the request has created its indexes (the rebuild of an out-of-date TTL index also runs under the create lock: it drops the index, re-checks that it is gone, runs the `1 + replicas` budget check and then creates it, so the dropped index is not counted twice; commit `d75f669` on the round-4 branch `[ref-repo]`. Residual weakness: if the budget check or the create fails after the drop, the table has no TTL index until the next attempt, which was already so when the create failed. Coverage is a unit test of the cost arithmetic only; the rebuild path was not run live, because it needs an out-of-date index). It does not coordinate two
servers; the one-server-per-catalog rule does that. An index the catalog lists
as `CREATING` is already in `system:indexes`, because `UpdateTable` creates and
builds the index on the request path before it publishes the row, so it is
counted. The count is of the rows `system:indexes` lists; whether it lists a
replica as a row of its own has not been checked on a cluster with
`index_num_replicas` above 0, so there the used count may be of definitions
where the 10,000 limit is of instances (OPEN, engineering item 8). Two
concurrent creates count one after the other and cannot both pass with room for
only one. `CreateTable` and `UpdateTable` answer
`LimitExceededException` when the create would cross a limit (a tier-2 or
tier-3 create that would cross the keyspace limit answers it rather than
falling back to shared placement, which hid the limit out of tree). The
measured bound on index-bearing tables in the shared tier is published with
the implementation PR.

### Operational requirements

- **Couchbase Server**: 8.0, the version the CI job runs and the demo uses.
  Required services on the cluster: **KV, Index, Query**. Search, Analytics,
  Eventing, and Mobile are optional consumers of the native documents.
- **Edition**: CI, the demo and every QE run use Couchbase Server
  **Enterprise Edition** 8.0.0 (`couchbase:enterprise-8.0.0`; the image's
  licence is free for development and testing, and production needs a
  subscription). Nothing the backend uses is Enterprise-only except XDCR, which
  it does not use; Community Edition 8.0 (clusters of up to five nodes, no
  XDCR) is expected to work and is **not validated**. The README row states
  Enterprise as validated and Community as untested until a Community run is
  added to CI.
- **Bucket**: created once by an operator; one bucket holds one deployment.
  The backend runs as a least-privilege user with `data_reader`, `data_writer`,
  `query_select`, `query_insert`, `query_update`, `query_delete`,
  `query_manage_index`, and `scope_admin` on that bucket — not Administrator.
  Per-account RBAC provisioning (tier 3) is opt-in behind a separate operator
  credential.
- **TLS** to clients is mandatory (bring your own certificate). TLS to Couchbase
  (`couchbases://`) trusts the public roots or `ca_file`. There is no run-time
  switch that turns certificate checks off; `main`'s dev mode is a build
  feature, and the out-of-tree `EXTENDDB_QUERY_TLS_INSECURE` switch is deleted.
- **Durability**: `majority` by default, which needs enough data nodes to hold a
  majority of `replicas + 1` copies. The bucket preflight (`bucket_problems`)
  computes this from the bucket's `replicaNumber` and node count: `init`
  **refuses** such a bucket before creating anything, `verify` **fails the
  readiness probe**, and `serve` logs an error at startup and then every write
  fails with the KV durability-impossible error (surfaced as a retryable
  `InternalServerError`, never a silent acknowledgment). The same preflight
  refuses `durabilityImpossibleFallback` set to anything but `disabled`, a
  bucket with three replicas (Couchbase supports durable writes for up to two),
  bucket or collection `maxTTL`, and eviction on ephemeral buckets. Single-node
  demos set `durability = "none"` explicitly.
- **Query service**: `memory-quota` at 0 (the default, no quota) or at a
  value large enough for the request. The 100-action measurement ran at 0; at a 1 MB quota the query service raised 17016, the backend answered 503 and nothing was written (single-node demo stack `[ref-repo]`).
- **Strict startup**: unknown or misspelled `[storage.couchbase]` keys are
  refused by name.

Configuration follows `main`'s convention: `[storage.couchbase]` keys in
`extenddb.toml`, each overridable by `EXTENDDB__STORAGE__COUCHBASE__<KEY>`, and
the Couchbase password in `EXTENDDB_COUCHBASE_PASSWORD`, never in the file or
the connection string:

```toml
[server]
bind_addr = "0.0.0.0"

[storage]
backend = "couchbase"

[storage.couchbase]
connection_string = "couchbases://couchbase.example.svc/extenddb"   # bucket in the path
username = "extenddb_app"                                           # password: EXTENDDB_COUCHBASE_PASSWORD
```

| Key | Default | Purpose |
|---|---|---|
| `connection_string` | — | cluster address and bucket |
| `username` | — | the least-privilege Couchbase user; password from `EXTENDDB_COUCHBASE_PASSWORD` |
| `ca_file` | — | CA for `couchbases://` when not in the public roots |
| `durability` | `majority` | KV and transaction durability |
| `table_isolation` | `shared` | isolation ladder tier |
| `txn_timeout` | `30s` | query-service `txtimeout` |
| `index_num_replicas` | `0` | `num_replica` on every created index |
| `connect_timeout_secs` | `30` | bound on `Cluster::connect` |

The Helm chart exposes the same as values, one replica, `ClusterIP`, mandatory
TLS Secret, and warns when the Service is externally exposed. Runbooks cover
backup/restore, upgrades, the reindex reconciler, per-account RBAC, the
storage format and migration, and the dependency-bump gate.

### Implementation summary

| Component | Change |
|---|---|
| `crates/storage-couchbase/` (today `backend/`) | New crate — full backend: 26.4k lines in `backend/src`, of which ~2.4k are inline `#[cfg(test)]` modules (24k non-test) `[ref-repo]`, before the vector-search and multi-server code is removed |
| `crates/bin/Cargo.toml`, `main.rs` | `couchbase` feature flag and the `backend()` install (same shape as the MongoDB RFC) |
| workspace `Cargo.toml` | the `couchbase` dependency (Rust SDK 1.1.1 or later); `sha2` is already a workspace dependency; no cipher or password crate |
| `devtools/run-tests`, `devtools/check-couchbase-*`, `devtools/run-couchbase-tests` | `--backend couchbase`; the catalog-lock, older-binary and schema checks; the live-test driver |
| `.github/workflows/integration-couchbase.yml` | the `couchbase-integration` job on `couchbase:enterprise-8.0.0`, required |
| `README.md`, `docs/differences-from-dynamodb.md` | the Couchbase row at Experimental; the Couchbase entries |
| `deploy/helm/`, `demo/`, `docs/`, `qe/` | Deployment chart, Docker demo, runbooks, QE harness (may stay in the Couchbase repo) |

No changes to `crates/engine/`, `crates/server/`, `crates/storage/` (trait
definitions), `crates/auth/`, or `crates/core/`.

### Design decisions summary

| Decision | Choice | Rationale |
|---|---|---|
| Document shape | Native JSON at the top level + one reserved `_xdb` object | Data is directly usable by SQL++, Search, Analytics, Eventing, Mobile; one reserved key is the smallest footprint; `_xdb` presence is the format signal |
| Numbers | Native JSON number when `normalize_number`'s text survives the round trip; otherwise an exact string recorded in the sidecar; keys use an order-preserving string encoding | Full 38-digit fidelity and exact range order without a precision cap |
| Sets, binary | Arrays and base64 strings + the `_xdb.ddb` type sidecar (map form) | JSON cannot express them; the map form lets one sub-doc op clear one pointer (UpdateItem fast path) |
| Point reads | KV binary get | Sub-millisecond, strongly consistent, no query hop |
| Query/Scan | SQL++ over `_xdb` index columns, `request_plus` only | Read-your-writes always; RFC-0002 allows no weaker mode |
| Conditional writes | CAS read-evaluate-replace, 64 attempts / 10 s, then 503 | DynamoDB atomicity without a transaction; exhaustion is retried by the SDKs, as on MongoDB |
| GSIs/LSIs | Derived keys inside the item + one `_xdb.tid`-scoped Couchbase GSI per index | No secondary write to keep in sync; deferred build + single `BUILD INDEX` |
| Transactions | Query-service `BEGIN WORK`/`COMMIT` pinned to one node, in process | The Rust SDK has no KV transactions; no sidecar, no extra credentials, survives node failover |
| Plain write vs transaction | One lease document per item, taken by every plain write and every transaction on every table | Mutual exclusion through KV insert's fail-if-exists, a documented operation; no per-table switch; nothing depends on how Couchbase stages a mutation |
| TransactGetItems | Leases on every read key, present or absent, then plain KV reads | SQL++ transactions are read committed; the lease excludes plain creates of absent keys and writers of present ones, and no document is rewritten |
| Streams | Explicit records, four shards per stream, number taken before commit, reader stops at an unresolved number and resolves it by a transactional read | DynamoDB contract (monotonic sequence, images, 24 h retention); designed so that no reader passes a number that may still commit, with no reliance on a cleanup bound; the hold that tests it ran twice and its gate failed both times (0 records resolved by a transactional read) |
| Descending queries | A descending twin of the base index and of every secondary index | Couchbase does not scan an index in reverse; the twins are counted in the budget |
| TTL | Sweeper through the delete path, never native expiry | Native expiry would break the `REMOVE` stream record contract |
| Isolation | Shared collection by default, collection/scope tiers opt-in | Physical isolation and RBAC when a tenant needs it; limits counted cluster-wide |
| Secrets | `main`'s `secret_cipher` and key ring; `key_file` recommended | One cipher for every backend (ADR-0026); a bucket backup must not contain its own key |
| Couchbase user | Least-privilege scoped roles, never Administrator | Proven by the whole data plane running as `extenddb_app` in the demo |
| Servers | One per catalog | `main`'s model for every backend; multi-server is its own RFC |
| Migrations | `extenddb_storage::migrations` with the ledger row written after an idempotent body (the Cassandra pattern) | The Rust SDK has no KV transactions and a SQL++ transaction takes no DDL; a crash between body and row re-runs a body that is safe to run again |
| Backup | `SnapshotEngine` returns `Unsupported`; in-catalog backup | Couchbase fixes no instant across a table; Cassandra's precedent; a write fence would make backups affect writes |

### Performance characteristics

- **Point operations** are KV hash lookups: YCSB workload A (50/50 read/update)
  2,822 ops/s at 5K records and 2,096 ops/s at 1M records (~1 GB, past cache),
  a 26% drop at 20× the data. Bulk load ~7,400 inserts/s `[ref-repo]`.
- **Scans** are index-bound and run at `request_plus`; a scan issued while the
  indexer is catching up after a bulk load waits for it.
- **Transactions** add query-service round trips; the backend exports
  per-operation request latency and storage-query latency at `/metrics` (no
  CAS-retry counter is exported today). A 100-action `TransactWriteItems` can
  exceed the 30 s `txtimeout` at WAN latency
  (couchbaselabs/extenddb-couchbase#177 `[ref-repo]`).
- **Multi-node scaling** (Couchbase's performance CI `[perf-ci]`, cited under Motivation,
  measured with several API instances): workload B scales near-linearly (~80%
  efficiency at 18 nodes; read p50 ~4 ms, p99 9–15 ms, flat across scale).
  Workload A below saturation on 18 nodes: update p99 5.7 ms and read p99 3.0 ms
  at 5,000 ops/s, 8.6 ms and 4.0 ms at 10,000. A single API instance stalls
  near 10,000 ops/s, and with one server per catalog that is the deployment's
  bound; the README row states it as a known limit, and the sizing guidance in
  `docs/perf-pass-plan.md` is the per-instance one.
- **The lease cost.** Every plain write takes the item's lease: an insert, then
  a read and a remove, three KV operations added to its write, each with the
  backend's durability level. The one-node demo-stack figures of revision 4
  predate the lease on every table and are withdrawn. The figures for the
  README row come from the performance cluster's YCSB workload A on a
  replicated bucket at `durability = majority`, with the lease on every table;
  that run has not been made, and it is a condition of FCP.
- Connection-pool metrics are not emitted: the Couchbase SDK owns pooling and
  does not expose per-acquire latency (couchbaselabs/extenddb-couchbase#178 `[ref-repo]`).

### Testing

| Layer | What | Where |
|---|---|---|
| Unit | number encoding, item round-trip, sidecar, key forging, policy hardening | `cargo test --lib`: 272 passed, 0 failed (276 with the `fault-hooks` feature) on the round-5 branch after its last item, 269 and 273 on the round-4 branch `[ref-repo]` |
| Storage-invariant lint | stale top-level `_pk`/`_sk` reads, hand-built keys, unescaped `LIKE` | `scripts/lint-storage-invariants.sh` `[ref-repo]`, per PR |
| Conformance | upstream `tests/test_*.py` 912 + 1 xfail, `tests/python/` 331, web console 30 | `CONFORMANCE.md` `[ref-repo]`; the Jenkins nightly's sweep `[ref-repo]` |
| Backend tests | transaction rollback (7), DynamoDB head-to-head parity, encoding edges | `tests/` `[ref-repo]` |
| Concurrency | property harness (conservation, no-lost-update, one-winner, CAS counters), write-race, txn-concurrency | `scripts/` `[ref-repo]` |
| HA | failover race, sweep under failover, query-txn multinode | `scripts/ha-test.sh` and friends, nightly `[ref-repo]` |
| QE | 343-case plan, 23 areas, executable `qe/tests/`; cluster-ops on a real 3-node cluster | `docs/qe/`, Jenkins `extenddb-couchbase-qe-cluster-ops` `[ref-repo]` |
| Interop | five client apps in four languages, reverse ETL, Couchbase-side ops matrix | `examples/`, `scripts/cb_to_ddb.py`, `docs/couchbase-side-operations.md` `[ref-repo]` |

## RFC-0003 acceptance criteria

Upstream RFC-0003 is the rubric every backend is judged against: ten sections
and about forty criteria. The table below maps each criterion to the mechanism
in this backend and to the test that proves it. "Upstream" tests are files in
the ExtendDB conformance suite (`upstream/tests/`); QE ids are cases in
`docs/qe/testcases.md` `[ref-repo]`; `tests/` and `scripts/` are the reference
repository's own `[ref-repo]`. Six criteria (2.4, 3.5, 4.1, 4.3, 5.2 and 6.2)
are marked OPEN: their evidence is in the reference repository, which no
reviewer can read yet, and they close when a reviewer has read it. Open
engineering parts: items 1, 2, 5 and 8 (their OPEN parts), item 13 (blocked on
the pin), item 15 (Gap A, visibility after a commit, no code; Gap B, the token
read after an unknown outcome, fixed in the unknown-outcome arm only). Gap A
leaves an OPEN part in rows 4.4 and 4.5 and Gap B in row 3.3; the items and
their state are under "Engineering items of
revision 3" below.

### 1. Item identity

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 1.1 | Unique addressability | Document key `<table_id>#<pk_esc>\|<sk_esc>`, each component escaped (backslash doubled, then the separator preceded by a backslash) after its type tag; on a table without a sort key the key is `<table_id>#<pk_enc>`, the encoded partition key unescaped, since there is no separator to forge; a raw key over 240 bytes is `<table_id>#H#<hex SHA-256 of the composite as stored>`, and a hashed read compares the stored `_xdb.pk` and `_xdb.sk` with the request's (Couchbase's limit in a named collection is 246); built only by `full_doc_key` | DT-15, LM-02/03/04, `tests/test_encoding_edges.py` `[ref-repo]` |
| 1.2 | No delimiter ambiguity | Each key component is escaped (backslash doubled, then the separator preceded by a backslash) after its type tag is applied and before the separator joins them, so a separator inside a value cannot forge the boundary: the review's pair, pk `x` with sk `y\|S#z` against pk `x\|S#y` with sk `z`, gives two different keys (see "Key encoding"). The hashed form's `H#` segment cannot be a type tag, and `scripts/lint-storage-invariants.sh` blocks any hand-built key | `tests/test_encoding_edges.py` `[ref-repo]` (separator-collision class, including a partition key that contains the separator and a type tag), DT-14, DT-15 |
| 1.3 | Numeric precision | Key and index columns always use the order-preserving string encoding at full 38 digits; they are never stored as a native number, so one logical field has one storage type. The top-level attribute copy is a JSON number only when `normalize_number`'s text survives the round trip, otherwise an exact string recorded in the type sidecar | DT-04 to DT-08, DT-17, parity suite number-fidelity battery incl. the ULP case |
| 1.4 | Binary key ordering | Encoded as lowercase hex, so unsigned-byte lexicographic order | DT-18, QS-04, IX-05, `tests/test_dynamodb_parity.py` |

### 2. Secondary indexes

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 2.1 | Duplicate index keys | There is no separate index entry: the derived keys live in the item document and the Couchbase GSI indexes the document itself, so identity is the base document key | IX-04, upstream `test_put_item_create_race.py` (GSI and LSI tables) |
| 2.2 | Stale entry prevention | Every write path rebuilds `_xdb` through `item_to_document`; the sub-document fast path is refused for any key or index attribute | IX-09, upstream `test_query_scan.py` GSI cases, stress "GSI query after updating a key attribute" |
| 2.3 | Sparse semantics and type validation | The shared validators in `crates/core/src/validation` reject a wrong-type or empty index key with `ValidationException`; a missing attribute yields no derived key and the GSI `WHERE` excludes it | IX-03, DT-20, upstream `test_gsi_attribute_definitions.py` |
| 2.4 | Backfill on UpdateTable | `UpdateTable` creates and builds the index and backfills once, then writes the catalog row `CREATING`; a fenced second pass on the same server waits for the catalog freshness window and for the writes then in flight on that server, backfills again and flips `ACTIVE`; the server's GSI worker repeats it 75 s after the publish and flips a still-`CREATING` index (implemented in `f4f293d` `[ref-repo]`, fence hardened on the round-4 branch; see "Global and Local Secondary Indexes") | **OPEN** until a reviewer reads the reference repository. IX-07 `[ref-repo]`, `tests/test_gsi_fence.py` `[ref-repo]` (racing writers; the upstream `gsi_async*` files spawn their own server and are outside the endpoint sweep, so IX-07 is the gate), and the held-write test in `tests/test_fault_hooks.py` `[ref-repo]` (`PutItem` only, on a `fault-hooks` build; engineering item 5 says what has no test) |
| 2.5 | Transactional index maintenance | Transactional writes rewrite the whole document, derived keys included, inside the query-service transaction | TX-01, TX-13, upstream `test_transaction_operations.py` |
| 2.6 | Index query pagination | `LastEvaluatedKey` carries index and base keys; the resume bound is merged with, not substituted for, the key condition | IX-06, QS-07, upstream `test_read_paging.py` |
| 2.7 | ConsistentRead on GSI | Rejected by the engine with `ValidationException`; the backend exposes no strong-read option for index reads (see "Reads") | IX-10, QS-20 |
| 2.8 | Multi-part RANGE key queries | Per-part columns and predicates are implemented; the engine at the pin `7b828b0` refuses a multi-condition Query before the backend sees it. Shipping behaviour in both states: until the engine pin moves to or past `f49c5f82`, `couchbase` is listed in `FIRST_RANGE_ONLY_BACKENDS`, which two files on `main` define (`tests/test_gsi_multi_range_query.py` and `tests/test_fgac_leading_keys.py`), in `docs/differences-from-dynamodb.md` and in the README row; after, per-part predicates ship with `tests/test_gsi_multi_range_query.py` at '63 passed, 1 skipped, 0 failed', out of the 64 tests it collects at `f49c5f82` and on `main` (its `TestFirstRangeOnlyBackends` case skips itself on a backend that serves every RANGE attribute) | **OPEN**, blocked on the pin (engineering item 13); `tests/test_gsi_multi_range_query.py` NOT RUN |

### 3. Transactions

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 3.1 | All-or-nothing | One query-service transaction carries the data writes (with their derived index columns), the stream records, and the idempotency create; `COMMIT` or nothing, and the backend issues `ROLLBACK WORK` on any failed statement | TX-01 to TX-03, `tests/test_transaction_rollback.py` (cross-table, rollback completeness, crash mid-transaction) |
| 3.2 | Stream records in transaction | `stream_txn_write` appends the records to the same transaction as the writes | ST-04, TX-13, upstream `test_streams.py` |
| 3.3 | Idempotency token atomicity | The token document `<account>#<token>` is read first (replay or mismatch by fingerprint, 600 s retention), then **recorded as a fail-if-exists create inside the same transaction as the writes**. Two concurrent requests with the same fresh token race on that create; exactly one commits, the loser's "exists" is mapped to replay or mismatch. Token and data commit or roll back together, and a cancelled transaction releases its token | TX-05, TX-06, upstream `test_idempotency_account_scope.py`, `test_conditional_atomicity.py`, `tests/test_transaction_rollback.py` |
| 3.4 | CancellationReasons fidelity | Per-operation reasons in input order; `ALL_OLD` item attached on condition failure | TX-02, CR-12, upstream `test_transaction_operations.py`, `test_transact_expression_validation.py` |
| 3.5 | TransactGetItems isolation | Every read key is leased, present or absent, in sorted order, then read by KV with the leases held and released afterwards (see "Transactions"); no document is rewritten, so a plain create of an absent key meets the lease. A read that cannot take its leases within its 5 s deadline is cancelled with `TransactionConflict` | **OPEN** until a reviewer reads the reference repository. TX-07 `[ref-repo]`, upstream `test_transact_get_snapshot.py`, `tests/test_txn_lease.py` `[ref-repo]` (a `TransactWriteItems` creates and updates a pair under three `TransactGetItems` readers: never torn across an absent key; and a `PutItem` creates the absent key during a `TransactGetItems`, on a plain and a stream-enabled table; 8 of 8 passed on the demo stack, 2026-11-25) |

### 4. Concurrency and isolation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 4.1 | Unconditional writes never fail under contention | A bare unconditional `PutItem` with no `ReturnValues` takes the item's lease (a KV insert), makes one KV write and removes the lease (a read and a CAS remove): there is no CAS on the item and no retry of the write itself, and the last writer wins. **This differs from RFC-0003 4.1** ("two concurrent unconditional `PutItem` calls both succeed"), and the two cases are stated separately. (1) Between plain writes on a table without a stream: the second waits on the first's lease with jittered backoff and answers 503 at the end of a 10 s bound; the lease is held for one KV write, so in practice two racing calls both succeed, and one fails only if the other holds the item for more than 10 s; a plain write that crashed leaves its lease until it expires (30 s), and writes to that key can answer 503 until then. (2) On a stream-enabled table the lease spans a query-service transaction (up to `txtimeout` plus 20 s): the same 10 s bound and 503 apply to a writer that waits on it, and a crashed holder blocks its key for that long. Both are recorded in the `docs/differences-from-dynamodb.md` rows that the implementation PR adds (`main` has no Couchbase entry yet). Sharing the lease between plain writers, so that only a transaction excludes them, is the alternative; it is a follow-up if the reviewers prefer it. What a plain write answers when it meets a transaction's lease is under "Transactions". With `ALL_OLD` it is a CAS loop of 64 attempts / 10 s that answers 503 at exhaustion, as MongoDB does; no CAS detail is client-visible (`cas_contended`) | **OPEN** until a reviewer reads the reference repository. The tests that race bare puts on one key are in upstream `test_put_item_create_race.py` (8 writers, 15 rounds, every put must succeed): `::test_racing_puts_on_a_gsi_table_leave_only_the_final_entry`, `::test_racing_puts_on_an_lsi_table_leave_only_the_final_entry` and `::test_racing_puts_on_a_stream_table_all_succeed`, with `::test_racing_puts_with_all_old_all_succeed` for the `ALL_OLD` loop. Recorded run: the file 9 passed at the pin `7b828b0`, single-node demo stack, durability none `[ref-repo]` (Tess-6 re-ran it, 9 passed). `test_concurrency.py::test_parallel_inserts` is not evidence here (its threads write distinct keys), and `TestAtomicCounter::test_atomic_counter`, which passed 1 of 1 on the same run, belongs to row 4.4. No test drives two plain writers into the 10 s bound or a crashed lease. CR-03 `[ref-repo]` |
| 4.2 | Conditional writes: correct error on race | The CAS loop re-evaluates the condition against the winner's image and returns `ConditionalCheckFailedException` | CC-01, upstream `test_put_item_create_race.py::test_racing_puts_check_their_condition_against_the_winner`, `::test_racing_conditional_creates_have_one_winner` |
| 4.3 | Transaction conflict mapping | A plain write meeting a `transact_write` lease returns `TransactionConflictException`; a transaction that cannot take its leases in 2 s returns `TransactionCanceledException` with `TransactionConflict`; a plain write meeting a `transact_get` or `plain` lease retries and answers 503 at its 10 s bound; plain-writer contention never surfaces as a transaction conflict (implemented `[ref-repo]`; see "Transactions") | **OPEN** until a reviewer reads the reference repository. TX-08 `[ref-repo]`, CC-09 `[ref-repo]`, upstream `test_transact_write_conflict.py`, `scripts/write-race-test.py` `[ref-repo]`, `tests/test_txn_lease.py` `[ref-repo]`, the three-node harness's commit-point hold `[ref-repo]` |
| 4.4 | UpdateItem serializes | CAS read-modify-write with jittered backoff; arithmetic and `ADD` never take the sub-document path; on a stream-enabled table each write is a transaction that holds the item's lease, so two on one key do not meet at the document: the second waits on the lease and writes after the first releases it, and both succeed (the paths that can still conflict at the document are listed under "DynamoDB Streams"; Gap A, engineering item 15, leaves this row **OPEN** in part) | CC-02, CC-03, `scripts/concurrency-property-test.py` (`prop_add_counter`, `prop_optlock_counter`), upstream `test_concurrency.py::TestAtomicCounter::test_atomic_counter` |
| 4.5 | Read-your-writes | `GetItem` is a KV get served by the active vbucket; no replica reads. **OPEN** in part (Gap A, engineering item 15): a commit through `COMMIT WORK` that is not waited on for visibility can release its lease before a KV read shows it; the window was not observed on Couchbase 8.0.4 | CC-04, CR-06 |

### 5. DynamoDB Streams

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 5.1 | Atomicity with data writes | Records are written in the data transaction | ST-04, upstream `test_streams.py` |
| 5.2 | Per-shard ordering | Numbers are taken before commit; `GetRecords` walks the shard in sequence order and stops at an unresolved number, which only a skip marker written after a definite failure, or a transactional read of the record after the age, resolves (see "DynamoDB Streams"), designed so that no reader passes a number whose record may still appear (implemented; the hold that tests it ran twice and its gate failed both times, with 0 records resolved by a transactional read, so that path is unexercised; see "DynamoDB Streams") | **OPEN** until a reviewer reads the reference repository. ST-03, DC-07 `[ref-repo]`, upstream `test_streams.py`; RFC-0003's stress scenario "consumer reading during concurrent writes" (RFC-0003:160, not a test in `upstream/tests/`) is covered only for gaps, by the stream reader of `scripts/txn-commitpoint-test.py` `[ref-repo]` (the three-node harness's commit-point hold: a query engine frozen 60 s, longer than the age); event types and cross-table leakage under concurrent writes are not yet tested |
| 5.3 | Account and table isolation | Shard id embeds the table id; records carry the table id and every read is scoped by it | upstream `test_cross_account_isolation.py` (one case in this file failed in the nightly baseline for a test-suite-skew reason, see 6.3; it passes against the pinned suite), `test_stream_table_reuse.py` |
| 5.4 | Correct event types | `INSERT` / `MODIFY` / `REMOVE` with images per view type; an unchanged item emits no record | ST-01, ST-05, upstream `test_streams.py` |
| 5.5 | Retention and cleanup | `idx_stream_ts` plus a delete at 24 h on every TTL sweep cycle | ST-06, ST-07 |
| 5.6 | Shard stability | In-tree: stream lineage, four shards per stream with the lineage shard id, enable and disable through `UpdateTable`, idempotent by construction since a repeat targets the same newest stream. Today: one deterministic shard per table and `UpdateTable` stream changes refused | ST-02, upstream `test_stream_table_reuse.py` (the two cases that fail today pass with lineage) |

### 6. Condition expressions, code hygiene, documentation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 6.1 | Evaluation semantics | Delegates to `extenddb_core::expression::evaluate_condition` on the read image; no native pushdown | CR-10 to CR-13, upstream `test_conditional_writes.py`, `test_conditional_operator.py` |
| 6.2 | No hidden unused code | The crate-level `#![allow(unused_variables, unused_imports, clippy::too_many_arguments)]` of `engine.rs` and `catalog.rs` is gone, with the 3 unused imports and the 5 dead items it hid (measured 2026-11-05, removed 2026-11-24, commit `b0c272f`; `[ref-repo]`). One targeted, commented `#[allow(dead_code)]` remains, on the sidecar-format detector the proposed v3.1 → map-form data migration will call | **OPEN** until a reviewer reads the reference repository. `cargo rustc --lib -- --force-warn unused_imports --force-warn unused_variables --force-warn dead_code`: that one item |
| 6.3 | Documentation describes the system as built | Every mechanism described in this RFC is implemented and on the live code path, or is marked OPEN: rows 2.4, 3.5, 4.1, 4.3, 5.2 and 6.2 are OPEN until a reviewer reads the reference repository, and, in "Engineering items of revision 3", items 1, 2, 5 and 8 have OPEN parts, item 13 is blocked on the pin and item 15 is two OPEN gaps (Gap A and Gap B, see its row); Capella is stated as unvalidated, the failover-sweep claim cites a specific nightly run, every conformance figure names its suite revision, and the scaling figures cite measured runs. Also recorded here for honesty: nightlies #46 through #58 `[ref-repo]` reported a **baseline** sweep of 908 of 913 with the same 5 non-passes every run (`test_abac.py` ×3, `test_auth_permissions.py::test_explicit_deny_all_service_operations`, `test_cross_account_isolation.py::TestStreamAccountScoping::test_shard_iterator_only_returns_owning_account_records`). Triaged 2026-11-09 as test-suite skew: the nightly's heavy lane skipped the "clone upstream at the pinned rev" stage, so the sweep ran the stale `e41f370` test checkout (913 collected) against the auth-cluster build. What the pinned `7b828b0` run of 2026-11-24 (under "Evidence") shows: the `test_auth_permissions.py` and `test_cross_account_isolation.py` cases pass, and three `test_abac.py` cases fail, all three `aws:SourceIp` loopback cases, behind Docker Desktop's bridge on the machine that ran it. Whether the nightly's three `test_abac.py` non-passes were those cases, and whether they pass on the Linux agent, is settled by the first nightly after couchbaselabs/extenddb-couchbase#237 `[ref-repo]` (merged 2026-11-24: the clone stage runs in both lanes and `scripts/sweep-under-failover.sh` refuses a checkout that does not match the Cargo pin); until then the skew triage covers two of the five, not all five | Jenkins `extenddb-couchbase-nightly` #51 console `[ref-repo]`; #237 `[ref-repo]`; the pinned run under "Evidence" |

### 7. Query and Scan

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 7.1 | Limit semantics | `Limit` becomes the storage fetch bound; the engine filters afterwards | QS-07, QS-09, QS-17, upstream `test_query_scan.py` |
| 7.2 | LastEvaluatedKey correctness | Reflects the last item read, in the base or combined schema; present iff more items exist | QS-07, QS-18, upstream `test_read_paging.py` |
| 7.3 | Parallel scan completeness | Segment membership is `SHA-256(pk_enc, sk_enc) mod TotalSegments`, shared by the scan and the `ExclusiveStartKey` check; a segment pages until exhausted | QS-16, upstream `test_query_scan.py` parallel-scan cases |
| 7.4 | Sort order | Ordering on the encoded `_xdb.sk` string equals DynamoDB order for strings (bytes), numbers (by value via the encoding), and binary (unsigned bytes); descending order is served by `ORDER BY ... DESC` on the descending twin index, and descending paging is in the tests | DT-16 to DT-18, QS-02 to QS-06 |

### 8. Multi-tenancy

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 8.1 | Account isolation | Table id per (account, table); every index and query scoped by `_xdb.tid`; stream records and sequence counters carry the table id; idempotency keys are `<account>#<token>`; catalog keyed by `account#name` | AU-19, QS-21, upstream `test_cross_account_isolation.py`, `test_idempotency_account_scope.py` |
| 8.2 | Table-name reuse | Table id is random per incarnation; shard ids, documents, and counters derive from it, and `DeleteTable` cleans up | upstream `test_stream_table_reuse.py`, TX-15, QS-19 |

### 9. Error fidelity

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 9.1 | No silent degradation | Unsupported shapes are refused explicitly; an unsatisfiable durability fails the write; failed index DDL surfaces; a partially created table is rolled back (`CreateRollback`); a cluster limit answers `LimitExceededException` | NE-04, NE-06, SM-04, SM-12 |
| 9.2 | DynamoDB error codes | Couchbase and SDK errors map to `StorageError` variants that the engine renders as DynamoDB codes; no backend-specific code reaches a client; a query-service resource limit (the memory quota, code 17016) answers 503 at a statement with the cause logged, and is an unknown outcome at `COMMIT WORK` | NE-01 to NE-03, upstream `test_auth_error_fidelity.py`, `test_expression_error_prefix.py` |

### 10. Operational correctness

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 10.1 | Background workers | TTL sweeper, reindex reconciler, and index reconciler are spawned by `CouchbaseRuntimeHooks::spawn_workers` in the one server | ST-08, ST-13, `docs/architecture.md` §6 |
| 10.2 | Cache coherence | One server per catalog: the engine's per-process key-info cache is invalidated by the server that ran the DDL, which is the only server; the placement cache is rebuilt on miss | upstream `test_cache_coherence.py`, `test_console_cache_coherence.py` |
| 10.3 | UpdateTable idempotency | GSI add is driven by catalog status so a repeat is a no-op; stream enable and disable (via stream lineage) are idempotent by construction since a repeat targets the same newest stream | upstream `test_table_operations.py`, `test_stream_table_reuse.py`, IX-07 |

### Required suites

RFC-0003 requires `devtools/run-tests --extenddb --all` to pass in CI before
acceptance: the Rust tests (`--rust`, `--rust-integration`), the Python
conformance suite (`--pytest`, `--comprehensive`) and the external suite
(`--external`). The required gate for this backend is that command with
`--backend couchbase` in the `couchbase-integration` GitHub Actions job on
`couchbase:enterprise-8.0.0`, a required check as `mongodb-integration` and
`cassandra-integration` are. The crate's own `*_live_tests.rs` run there
through `run-couchbase-tests`, together with the in-crate checks every backend
runs with its own store: `metrics_conformance`, `key_conformance`,
`snapshot_conformance`. Until the crate is in-tree, the equivalent is the
runbook in `CONFORMANCE.md` plus `cargo test --lib`, which runs on every PR of
the reference repository as a required check, and the pytest sweeps in its
Jenkins nightly.

### Engineering items of revision 3

The items revision 3 listed as open, with their state in the reference
repository (commits on its `main`, dated 2026-11-25 UTC, are marked
`[ref-repo]`; work on the round-4 branch, which is pushed to the private
repository and not merged, keeps its commit ids and the same `[ref-repo]` mark,
round-5 work is on the branch `fix/round5` `[ref-repo]`, and each row says what
ran and what did not):

| # | Item | State |
|---|---|---|
| 1 | The lease on every plain write path, the `committing` state, the transactional escalation, the pre-commit lease check with rollback | Implemented: the first form, with a per-table `transactional` flag, in `5e8462b` `[ref-repo]`; the lease on every table, without the flag and held until the commit is visible (for CAS-pinned actions only: item 15 is OPEN), in `bb94b2c` `[ref-repo]`. `tests/test_txn_lease.py` `[ref-repo]` (8 tests; passed 8 of 8 on the demo stack on 2026-11-25, round-4 branch): plain updates racing transfers (balance conserved, no acknowledged update lost, transactions keep committing), the create race on a key a transaction creates, `TransactGetItems` never torn across an absent key, `PutItem`s against a read lease all acknowledged. These check answers and invariants; they do not show that a plain write inserts its lease, because a write that only read the lease would pass whenever the race is not hit. The test that would, holding a `PutItem` after its lease insert while a transaction commits on the key, is not written: the `fault-hooks` build (`tests/test_fault_hooks.py` `[ref-repo]`, hooks compiled out of release builds) holds a write after its catalog-view read, not after its lease insert. **OPEN** for that test. Three-node harness commit-point hold `[ref-repo]`: a query engine frozen 60 s under the load; run twice on the round-4 branch, the invariants passed and the gate failed both times (0 records resolved by a transactional read; counts under "DynamoDB Streams"); revision 4's counts could not be traced to a run and stay withdrawn. Plain-lease loop deadline: fixed in code (`a44e465` on the round-4 branch `[ref-repo]`). `take_plain_lease_opts` checks the 10 s deadline at the top of every loop iteration through a pure `deadline_passed`, so every retry branch reaches it and the bound is strict. The test covers only the pure function (the loop has no clock injection); the retry branches themselves are not run. Tess-5 re-ran `tests/test_txn_lease.py` (8 passed) on the final code (HEAD `7f5ae94` of the round-4 branch, single-node demo stack `[ref-repo]`) |
| 2 | The transactional read that resolves an aged stream number; `LATEST` and `AT_TIMESTAMP` no later than the first unresolved number | The read is implemented (`5e8462b` `[ref-repo]`) and not exercised by the commit-point hold, which ran twice and resolved 0 records by a transactional read (see "DynamoDB Streams"). The narrower skip-marker rule (an answer to `COMMIT WORK` that is not recognised is an unknown outcome and writes no marker) is built and unit-tested only, with no dedicated test. **OPEN** for `AT_TIMESTAMP`: it is a DynamoDB Streams iterator type that `main` supports from #468 (`6aef346f`) and the pinned engine (`7b828b0`) does not have. In-tree, `first_sequence_number_at_or_after` returns the first unresolved number when one precedes the first record at or after the timestamp, because a record that commits late may carry a timestamp at or after it; that is in-tree design with no code at the pin, and its acceptance is `tests/test_streams_at_timestamp.py` on `main` once the pin moves |
| 3 | The `UpdateItem` fast path falls back above 16 sub-document operations | Implemented (`b0c272f` `[ref-repo]`), unit-tested at 8, 9 and 8 + `REMOVE` |
| 4 | `TransactGetItems` leases on every read key, absent or present, read by KV, with one 5 s deadline for acquisition and passes together plus a 3 s pass allowance (`3ffb3df` `[ref-repo]`) | Implemented (`5e8462b`, reworked in `bb94b2c`) `[ref-repo]`, `tests/test_txn_lease.py` `[ref-repo]` (passed, see item 1), including a `PutItem` that creates an absent key during `TransactGetItems` on a plain and on a stream-enabled table |
| 5 | The GSI backfill fence | Implemented (`f4f293d` `[ref-repo]`, then hardened on the round-4 branch `[ref-repo]`): the first backfill on the request path, `CREATING`, the fenced second pass (the freshness window, then this server's in-flight writes, counted per generation: single-item writes before their catalog-view read, `TransactWriteItems`, and the reindex reconciler's rewrites; a drain timeout leaves the index `CREATING`), the worker's pass at 75 s, which now waits for the writes counted before it to drain (`38eeab5` `[ref-repo]`), `ACTIVE`. A backfill takes a `committing` lease over by CAS and rewrites in a transaction; an item under a `transact_write` lease (or a `plain` or `transact_get` lease held past 10 s) is skipped and the index stays `CREATING` until the worker's next pass finds it free (`e7f94b0`, `84c30aa` `[ref-repo]`). Tests: `tests/test_gsi_fence.py` `[ref-repo]` (racing writers; passed 1 of 1); the held-write test, which holds a `PutItem` after its catalog-view read until the index is published and then requires the item to carry the index column (`tests/test_fault_hooks.py` `[ref-repo]`, on a `fault-hooks` build, `PutItem` only), passed on five `fault-hooks` runs (Tess-2 to Tess-5 on the earlier form, Tess-6 on the rewritten form with its `CREATING` assertion; see "Global and Local Secondary Indexes"); the reconciler held-write test is a skipped stub. The worker's drain wait, the `TransactWriteItems` guard (C2), the reconciler's per-document key-info read (C3) and the background `committing` takeover are built and unit-tested only, with no dedicated test, and the takeover was not exercised. **OPEN** for those four |
| 6 | The 100-action, 4 MB `TransactWriteItems` on `NEW_AND_OLD_IMAGES`; the answer below the query service's `memory-quota`; the unknown-outcome answers | Implemented. On the one-node demo stack at the default `memory-quota` (0) and `EXTENDDB_DURABILITY=none` (a one-node bucket cannot run a higher level), a 100-action request of about 4 MB (100 `Put`s of about 40 KB) on a stream-enabled table with `NEW_AND_OLD_IMAGES` committed in 0.67 s; at a `memory-quota` of 1 MB the query service raised "Transaction memory (N) exceeded quota (M)" (code 17016), the backend answered 503 `ServiceUnavailable` and nothing was written (observed on the round-4 branch, 2026-11-25) `[ref-repo]`. The same error at `COMMIT WORK` is classed an unknown outcome: unit-tested only. An ambiguous `COMMIT WORK` with a token resolved to success and applied the writes once, and a same-token retry did not apply them again: passed on a `fault-hooks` build (`tests/test_fault_hooks.py` `[ref-repo]`). The `txtimeout` answer (503, nothing written) has unit tests of its mapping and a script that does not assert the 503 or that nothing was written; no live test asserts both |
| 7 | The descending twin indexes, counted in the budget, descending paging in the tests | Implemented (`f4f293d` `[ref-repo]`); `tests/test_desc_paging.py` `[ref-repo]`; `EXPLAIN` confirms the twin serves a descending Query without a sort |
| 8 | Cluster-wide scope, collection and index counting under the create lock, twins and replicas included, and the TTL index; `LimitExceededException` | Implemented (`f4f293d` `[ref-repo]`). The TTL index takes the same lock and budget at a cost of `1 + index_num_replicas`, including the rebuild of an out-of-date TTL index, which drops, re-checks existence, runs the budget check and then creates, all under the create lock (`d75f669` `[ref-repo]`, round-4 branch; coverage is a unit test of the cost arithmetic only, and the rebuild path was NOT run live, because it needs an out-of-date index; if the budget check or the create fails after the drop, the table has no TTL index until the next attempt). A TTL live check on the final code (HEAD `7f5ae94`, single-node demo stack `[ref-repo]`) enabled TTL on a fresh table and the sweeper deleted the 5 expired items and left the 4 others; the `idx_ttl` index itself was not inspected. Whether `system:indexes` lists a replica as a row of its own is not checked on a cluster with replicas, so the used count may be of definitions where the limit is of instances (**OPEN**). The measured shared-tier bound is still to be published (unresolved question 1) |
| 9 | `destroy` from the catalog; the foreign-store check; adoption by `migrate --yes`; the lock in `_default._default` | `destroy` and the foreign-store refusal implemented (`f4f293d` `[ref-repo]`); adoption and the lock location are the in-tree migrations module |
| 10 | CAS exhaustion answers 503; the `docs/differences-from-dynamodb.md` rows | The out-of-tree loops already answered 503 (`cas_contended`); revision 3's text was wrong. The rows come with the implementation PR, with the hot-key observation under Drawbacks |
| 11 | The hashed-key read compares the stored encoded keys; a partition key containing `\|S#` in the separator-collision test | Implemented (`b0c272f` `[ref-repo]`) |
| 12 | The lease cost and the hot-key refusal counts | Not measured with the lease on every table: the one-node figures of revision 4 predate it and are withdrawn. The multi-node YCSB workload A figures (at `durability = majority`) and the refusal counts (plain writes acknowledged, `TransactionConflictException` and 503, with the transaction side split the same way) are a condition of FCP. `scripts/txn-commitpoint-test.py` `[ref-repo]` prints the plain writers' answers, but its transaction side merges 503 into cancellations and a run that includes the freeze is not a steady state, so a run without a freeze and with the split counters is needed. RFC-0003 4.1, 4.3 and 4.4 are reported against those counts when they exist |
| 13 | `tests/test_gsi_multi_range_query.py` on `main` (64 tests collected at `f49c5f82` and on `main`) against the per-part columns, with `couchbase` not in `FIRST_RANGE_ONLY_BACKENDS` (defined in both `tests/test_gsi_multi_range_query.py` and `tests/test_fgac_leading_keys.py`) | **OPEN**, blocked on the pin: the engine at `7b828b0` predates `f49c5f82` (which adds `resolve_multipart_with`) and refuses a multi-condition Query on a one-HASH, several-RANGE GSI before the backend sees it. Done when the pin is at or after `f49c5f82`, the suite gives 63 passed, 1 skipped and 0 failed out of 64 on the three-node harness `[ref-repo]` (its `TestFirstRangeOnlyBackends` case skips itself on a backend that serves every RANGE attribute), and the engine revision and the count are recorded here |
| 14 | The lint item 6.2 | Done (`b0c272f` `[ref-repo]`) |
| 15 | Gap A: the visibility wait covers every commit path. Gap B: the token read after an unknown outcome sees a committed token | **OPEN**. **Gap A (visibility).** After a commit the backend deletes the `committing` leases only once a KV read shows the committed body, but it waits only for the actions that pin a document by CAS (a conditional or pinned write or delete). An unconditional `Put`, a create, an unconditional `Delete` and a `ConditionCheck` are not waited on, and two commit paths do not wait at all: the escalated plain write, and every single-item write on a stream-enabled table, commit through `COMMIT WORK` and delete their `plain` lease when it returns. A `ConditionCheck` rewrites the identical image, so no body comparison can show it unstaged. Gap A leaves rows 4.4 and 4.5 **OPEN** in part. It closes with a wait for the unpinned actions and for the two commit paths, and a harness that lands a freeze between the commit point and unstaging, to show the wait is needed and sufficient. **Gap B (token read).** The unknown-outcome arm of `TransactWriteItems` is fixed: it reads the token with `txn_peek` (`3231463` `[ref-repo]`), present means committed, absent means not committed, a read failure means unknown and answers 500 with the leases kept; the decision is unit-tested, the staged-token case is not tested end to end. **OPEN** for the retry path: the pre-check and the `Exists` arm still read the token by KV, which can miss a committed token that is not yet unstaged; what follows from such a miss is not analysed here. Gap B leaves row 3.3 **OPEN** in part (the mapping to row 3.3 is this revision's reading) and closes when the retry path's token reads use a transactional read. **What was probed.** On Couchbase 8.0.4, single node (Pip, Quin `[ref-repo]`), a document mutated in an open query transaction shows no transaction xattr (`$XTOC` empty, `txn` and `_txn` absent); a KV read and an out-of-transaction `SELECT` see the old body; and the commit was visible at the first sample, 1 to 4 ms after `COMMIT WORK` returned. The window the transactions documentation describes was NOT observed here, and a held or crashed unstaging window was not tested; this does not show that the window cannot exist |

Two `init` fixes came out of the three-node harness and are in `f4f293d` `[ref-repo]`:
the base index's twin was created before the collection existed on a fresh
cluster (a regression of item 7), and the idempotent DDL of `init` now
retries while a scope or collection created through one query node
propagates to another's manifest, a pre-existing multi-node race.

## Drawbacks

- **A query-service dependency for transactions and scans.** Every cluster needs
  Index and Query nodes, and transactional latency is bounded by query-service
  round trips rather than KV. If the Rust SDK gains native KV transactions, the
  transaction path should move (see Alternatives); the query path for Scan is
  inherent.
- **Shared-collection default.** Many tables in one collection make `_xdb.tid`
  scoping a correctness invariant every index and query must honor; a missed
  filter is a cross-table leak. The storage-invariant lint and the isolation
  ladder mitigate; the risk is structural. The tier's index-bearing tables are
  bounded by the cluster index limit.
- **Reserved key and sidecar.** `_xdb` is visible to Couchbase-side consumers,
  and sets, binary, and wide numbers are only faithful through the sidecar.
  External writes that drop `_xdb` leave indexes stale until the reconciler
  adopts them; native expiry bypasses streams. Both footguns are documented.
- **A Couchbase-side write during a transaction.** ExtendDB's own writers
  exclude each other through the lease, but a write from the Couchbase side
  (SQL++, Eventing, Mobile, XDCR) takes no lease. Couchbase's transactions
  documentation says a non-transactional write to a document while a transaction
  that involves it is in progress can interfere with the transaction's updates,
  and calls the outcome undefined: the write can be overwritten by the
  transaction's commit, or the transaction's update can be disturbed. An
  operator who writes from both sides to the same items should know that such a
  write can be lost.
- **A real port, not a copy.** The backend is validated against `v0.1.x`;
  adopting the migrations framework, the key ring, stream lineage and the
  engine-owned backup path is new code whose correctness is re-proven in-tree.
- **No table snapshot.** `CreateBackup` is consistent per item, not at an
  instant, as on Cassandra.
- **One server per catalog.** The out-of-tree multi-instance API tier is not
  part of this RFC; throughput is bounded by one instance until the follow-up
  RFC.
- **The lease on every table.** A plain write takes the item's lease: an insert
  before the write, then a read and a remove after it, three KV operations
  around the write instead of one, each with the backend's durability level.
  There is no per-table flag and no way to turn the lease off (see
  "Performance characteristics" for what is and is not measured). Under a
  sustained transaction firehose on one item, plain writes to that item are
  refused with `TransactionConflictException` while a transaction holds the
  item's lease, which on a hot key can be most of the time. No count is quoted:
  the figures of revision 4 (acknowledged writes against refusals in one harness
  run) could not be traced to a run and are withdrawn. A harness run does not
  separate a refusal by a transaction that goes on to commit from one by a
  transaction that was cancelled and has not yet released its lease, and
  DynamoDB has not been measured under the same load, so this describes this
  backend's behaviour and claims no parity with DynamoDB's priority for the
  transaction. A client that mixes the two on a hot key will see it.
- **Maintenance burden.** ~24k non-test lines of backend plus QE harness, Helm,
  and runbooks owned by the Couchbase team; trait changes upstream require a
  coordinated update, as RFC-0002 anticipates.
- **Known constraint: query prepared statements are off by default
  (couchbaselabs/extenddb-couchbase#194 `[ref-repo]`).** The Couchbase Rust SDK's query core
  (1.0.2, still present in 1.1.x) retries a stale prepared-statement name
  forever after the query service restarts (rebalance, failover, node restart,
  online upgrade), so every Query and Scan hangs until the process restarts
  while KV stays healthy. The backend therefore runs its parameterized
  data-plane statements ad-hoc unless `prepared_queries` is set, trading plan
  caching for topology-change safety. This is an SDK fix Couchbase can drive;
  until it lands, prepared statements stay opt-in for controlled runs only.
- **Known constraint: no in-place rollback past two format edges.** Once a v3.2
  build has served a bucket holding any set, binary, or wide-number item, a
  pre-v3.2 build misreads the map-form sidecar (a set comes back as a list,
  binary as a base64 string) with a green KV health check; and the auth-cluster
  build refuses to serve without the resource-tag migration row that only
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
- **Holding stream records back until commit (`stream_pending`, migration 7).**
  `main`'s PostgreSQL answer. On Couchbase the record and its number would still
  be staged until unstaging, so the reader-side stop at an unresolved number is
  needed anyway and makes the pending collection redundant.
- **A write fence for table snapshots.** Rejected: DynamoDB backups do not
  affect writes.
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

The eight questions of revision 1 were answered in the review on #1184 and the
answers are adopted above: phased landing with complete traits per PR (1);
vector search out until it passes every vector test (2); no trait change and
no eventual mode (3); four shards (4); Couchbase 8.0 with the edition named
(5); one server per catalog (6); replication as its own RFC (7); `Unsupported`
snapshots (8). What remains:

1. **Shared-tier bound.** The measured number of index-bearing tables the
   shared tier supports under the 10,000-index cluster limit now that every
   index has a descending twin, and whether the README row should recommend
   tier 2 above some table count.
2. **Community Edition.** Whether a Community Edition run belongs in CI before
   the README row can say more than "untested".
3. **The lease on every table.** The `transactional` flag of revision 4 is gone:
   a plain write that read the flag as unset could land while the first
   transaction committed the same key, and that race cannot be fenced from the
   backend alone, so every plain write takes the item's lease. The cost is a
   lease insert, read and remove (three KV operations) around each write, each
   with the backend's durability level. Whether the maintainers accept that on
   every table, or want an opt-out for tables that never see a transaction
   (which would need the fence the flag lacked), is the question. No figure is
   quoted: the one-node figure of revision 4 predates the lease on every table
   and is withdrawn, and the multi-node YCSB workload A measurement with the
   lease on every table, at `durability = majority` on a replicated bucket, has
   not been run (a condition of FCP).

## Prior art

- ExtendDB's reference PostgreSQL backend and the
  [MongoDB backend RFC](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs/0000-mongodb-backend.md):
  this design keeps the same explicit-stream-record and inline-GSI philosophy but
  derives index keys into the item instead of per-index collections, and uses
  server-side transactions over a query service rather than driver sessions.
- The upstream [Cassandra backend draft](https://github.com/ExtendDB/extenddb/blob/main/docs/rfcs/draft-cassandra-backend.md).
  The `Unsupported` table snapshot this backend adopts is
  `crates/storage-cassandra/src/snapshot_engine.rs`, and the ledger-after-body
  migration pattern is ADR-0023 decision 2.
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

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
