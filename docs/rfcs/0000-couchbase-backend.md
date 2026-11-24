# RFC-0000: Couchbase Storage Backend

- Status: Draft (revision 3, 2026-11-24, responds to both reviews on #1184)
- Author: @suravarjjala
- Created: 2026-11-04
- FCP ends: —
- Tracking issue: ExtendDB/extenddb#1183
- Acceptance rubric: RFC-0003 "Backend Acceptance Criteria"; every criterion is mapped in "RFC-0003 acceptance criteria" below
- Reference implementation: https://github.com/couchbaselabs/extenddb-couchbase. Read access is granted to any reviewer who asks: the invitation to the first reviewer was sent on 2026-11-24 at 19:15 UTC and is pending at https://github.com/couchbaselabs/extenddb-couchbase/invitations (GitHub answers 404 until it is accepted). The repository goes public before the Final Comment Period, once Couchbase's open-source release approval completes. Until a reviewer has read it, every `docs/`, `scripts/`, `tests/` path and QE id in this document is a claim that only that repository can show.
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
another cluster; `docs/couchbase-side-operations.md` covers it, including its
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
  **331 / 331** (`CONFORMANCE.md`). The pinned suite the backend builds against
  is `7b828b0` on the `couchbaselabs/extenddb` fork (the pin plus six fork-only
  commits; see "Alignment with upstream `main`"). Its full run on 2026-11-24
  against backend `201f03f` (the demo stack, same file exclusions): top-level
  **1,211 passed, 6 failed, 16 skipped, 1 xfail** of 1,234 collected, and
  `tests/python/` **331 / 331**. The six: four `aws:SourceIp` loopback cases
  (`test_abac.py` ×3, `test_auth_integration.py` ×1) that require the server to
  see the client on 127.0.0.1, which a server behind Docker Desktop's bridge
  on the machine that ran it does not (an environment limit of that run; the
  nightly's Linux agent runs the same stack, and its next build says whether
  the loopback source survives there), and the two `test_stream_table_reuse.py` cases that `UpdateTable` stream
  changes fail today and stream lineage closes (row 5.6). The nightly that
  runs this suite is `extenddb-couchbase-nightly`; the clone stage that kept
  it on the stale `e41f370` checkout was fixed in
  couchbaselabs/extenddb-couchbase#237, merged 2026-11-24.
- QE: a 343-case Couchbase-specific test plan (`docs/qe/testcases.md`) run
  against a real multi-node cluster; every failure traced to code and fixed or
  filed (`docs/qe/qe-report.md`). No acknowledged write was lost in any
  rebalance, failover, node kill, churn, or API-restart test.
- Performance: YCSB through the same binding measures workload A at 2,822 ops/s
  on the 5K-record baseline and 2,096 ops/s at 1M records, with bulk load at
  ~7,400 inserts/s on the 1M-record run (`docs/ycsb-benchmark.md`). Couchbase's
  performance CI (multi-node YCSB runs, 2026-10-22 to 2026-11-01) shows the
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
`login_attempts`, `settings` (the migration lock and the catalog version row
among them), `account_metrics`, `backups`, `extenddb_servers`,
`extenddb_migrations`.

**`data` scope** — user data and data-plane state:

| Collection | Holds |
|---|---|
| `items` | every table's items in the default (`shared`) isolation tier, told apart by `_xdb.tid` |
| `t_<tid>` (tier 2) / `acct_<id>.t_<tid>` (tier 3) | per-table or per-account placement on the isolation ladder (§ "Multi-tenancy") |
| `stream_records`, `stream_seq` | DynamoDB Streams records (and skip markers) and the per-shard sequence counters |
| `idempotency` | `TransactWriteItems` client request tokens (10-minute retention) |
| `txn_atr` | query-service transaction records (created on first use) |
| `txn_leases` | the per-item leases that transactions and, on tables a transaction has touched, plain writes take before writing (see "Transactions") |
| `extenddb_migrations` | the data store's migration ledger |

`init` creates nothing by hand: the scopes, collections and indexes are created
by the migration bodies below. The foreign-store check follows `main`: it runs
only when the status is `Uninitialized` (no ledger), and then reads the
contents. A `catalog` scope that holds the out-of-tree `catalog_version`
document is an older deployment of this backend, reported as
`WrittenByOlderRelease`, and `migrate --yes` adopts it (the baseline body
reads that document); a `catalog` or `data` scope with neither a ledger nor
that document holds someone else's data and `init` refuses the bucket by
name. `verify` is the readiness probe. `destroy` drops exactly the scopes,
collections and indexes that the catalog and the ledgers record, by name from
those records, never by pattern, and leaves anything else in the bucket alone.

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
  The out-of-tree `resource_tag_migration` marker is deleted by the baseline
  body once it has recorded migration 2 from it, as ADR-0023 decision 4 does.
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
then `<table_id>#<pk_esc>|<sk_esc>` (`<table_id>#<pk_esc>` on a table without
a sort key). The reviewer's example, pk `x` with sk `y|S#z` and pk `x|S#y` with
sk `z`, gives `S#x|S#y\|S#z` and `S#x\|S#y|S#z`, which differ;
`tests/test_encoding_edges.py::test_separator_in_string_key_does_not_collide`
is that case, and a partition key containing `|S#` is added to it. Main's
`extenddb_storage::util::key::encode_netstring_composite` would do the same
job; the backend keeps the escape because every document key of every v3.x
deployment is already written with it, a document key cannot be rewritten in
place, and the escape is unambiguous (a `\` is always followed by `\` or `|`).

Couchbase allows a key of 246 bytes in a named collection, and every item
lives in a named collection, so a raw key over **240 bytes** is replaced by
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
have a **descending twin** (`... DESC` on the sort column) that the descending
statement uses; the twins count in the index budget (see "Multi-tenancy") and
descending paging is in the tests.

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
both succeed. On a table a transaction has touched, every plain write also
takes the item's lease first (see "Transactions"); on a stream-enabled table
every write is a query-service transaction, and two single-item writes that
meet on one key both succeed, the later one retrying within the same 64-attempt,
10 s budget and answering 503 at its end. These rows go into
`docs/differences-from-dynamodb.md` with the implementation PR.

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
single `BUILD INDEX`, cutting a 20-GSI create from 97 s to 26 s. `UpdateTable`
GSI creation writes the catalog row as `CREATING` and lets the index reconciler
backfill `_xdb._g<slot>` for existing items and flip the row to `ACTIVE`; index
DDL never runs on the request path. The fence between the backfill and
in-flight writes: `main`'s `CachedTableKeyInfoStore` is stale-while-revalidate
and `UpdateTable` invalidates it after the catalog mutation succeeds, so a
request that started before that invalidation may carry the old index list.
The server keeps an in-flight counter per table (incremented when a request's
key info is resolved, decremented when the request finishes), and the
reconciler starts the backfill only after the invalidation has happened and
every request in flight at that moment, the reindex reconciler's own writes
included, has finished; every write that resolves its key info after the
invalidation carries the new index. The sub-document fast path reads the same
key info and refuses any attribute an index names, so it is covered by the
same fence. `index_num_replicas` sets `num_replica` on every index the backend
creates, failing closed when the cluster has too few index nodes. Multi-part
RANGE keys are supported on every part: the backend derives one column per
part (`_g<slot>sk`, `_g<slot>sk1`, ...) and builds per-part predicates with
tuple pagination, verified against the engine's `resolve_multipart_with` (the
fix #381 that landed on 31 October), so the in-tree crate runs the upstream
multi-part tests rather than `refuse_conditions_beyond_first_range`; if any
case fails there, the shared helper and the MongoDB README wording are the
fallback.

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
insert of a key that does not exist yet. The backend therefore never lets the
two meet at the document: on any table a transaction has touched, **plain
writes and transactions exclude each other through one lease document per
item**, and nothing depends on how Couchbase stages a mutation.

- The **lease** is a document `<table_id>#<item key>` in `data.txn_leases`,
  taken with a KV insert (fail-if-exists, which is the whole mechanism) under
  a 10 s native expiry, with the fields `owner` (a per-request id), `op`
  (`transact_write`, `transact_get`, `plain`, or `committing`) and `exp`.
- A **`TransactWriteItems`** sets the table's `transactional` flag (below)
  before anything else, takes a lease on every target item, including
  `ConditionCheck` items and keys that do not exist yet, within 2 s (past it
  the transaction answers `TransactionCanceledException` with
  `TransactionConflict` on the items it could not lease), stages its writes,
  and renews every lease every 3.3 s by a CAS replace from its own task. Before
  `COMMIT WORK` it verifies by CAS that it still owns every lease; a lease it
  lost (a renewal failed, or expired) makes it `ROLLBACK WORK` and answer the
  same cancellation, so nothing it staged reaches a document while anything
  else may be writing. Then it rewrites its leases to `op = "committing"` with
  a 24 h expiry, sends `COMMIT WORK`, and deletes them when the commit has
  returned and Couchbase has unstaged every document. If the query node fails
  between the commit point and the unstaging, the `committing` leases stay
  until the cleanup has run, and the next transactional write to the item
  (below) ends them.
- A **plain write** (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`)
  on a table whose `transactional` flag is set takes the same lease with
  `op = "plain"` before it writes and deletes it after; a bare `PutItem`
  becomes lease insert, KV write, lease delete, so the lease is the only cost
  and the document-level write is unchanged. When the insert fails: a lease
  held by a `transact_write` answers `TransactionConflictException` at once,
  which is DynamoDB's answer while a transaction holds the item; a lease held
  by a `transact_get` or another `plain` write is retried with jittered
  backoff within the 64-attempt, 10 s budget, and the write answers 503 at its
  end; a `committing` lease (a committed transaction whose node died before it
  unstaged) makes the plain write run as a query-service transaction instead,
  which the protocol orders after the committed one (a transaction reads the
  committed version as soon as the transaction record says committed, and
  waits for or conflicts with a staged document until cleanup unstages it),
  retried within the same budget and answering 503 at its end; a successful
  transactional write deletes the `committing` lease. On a table whose flag is
  not set, a plain write is what it was: no lease, one KV operation.
- The **`transactional` flag** is a field of the table's catalog document, set
  once by the first `TransactWriteItems` or `TransactGetItems` that names the
  table (written with majority durability before the transaction takes any
  lease) and never cleared, and mirrored in the server's key info; with one
  server per catalog there is no other process whose view could be stale. The
  cost of the flag on a workload is measured before FCP (see "Performance
  characteristics").

The outcomes for one key, both orders:

| First | Second | Outcome |
|---|---|---|
| `TransactWriteItems` holds the lease | `PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem` on it | the plain write answers `TransactionConflictException`; the transaction commits |
| `TransactWriteItems` is committed but not unstaged (node failure) | a plain write on it | the plain write finds the `committing` lease, runs as a transaction ordered after the committed one, and succeeds |
| `PutItem` holds the lease (flagged table) | `TransactWriteItems` on the item | the transaction retries its lease for up to 2 s (the plain write holds it for milliseconds), then stages against the new image |
| `PutItem` commits | `TransactWriteItems` on the item | the transaction reads the new image, evaluates its conditions against it, commits |
| `TransactWriteItems` holds the lease | a second `TransactWriteItems` on it | the second fails to take the lease within 2 s and answers `TransactionCanceledException` with `TransactionConflict` on that item |
| `TransactWriteItems` creates a key that does not exist | a `PutItem` of that key | the key's lease was taken before the insert was staged, so the `PutItem` answers `TransactionConflictException`; the staged insert's invisibility to plain actors never matters because no plain actor writes without the lease |

**TransactGetItems.** SQL++ transactions run at read committed with a monotonic
atomic view, which does not by itself make a multi-item read serializable. The
backend sets the tables' `transactional` flags, takes a `transact_get` lease on
every key it reads, present or absent, and then reads inside one query-service
transaction, taking each present document's write lock by deleting and
re-inserting it with the identical body so that another transaction's write
conflicts. An absent key is protected by its lease alone: no plain write
creates it while the lease is held, and a `TransactWriteItems` that would
create it must take the same lease first, so the read returns it absent and
every other value as of one instant, which is the snapshot RFC-0003 3.5 asks
for. This closed the torn-read defect QE found (14 of 150 torn pairs → 0). The
side effects: a plain writer that meets the read's lease retries within its
budget and answers 503 at its end (DynamoDB fails the read instead; the
difference is that the plain write here waits milliseconds rather than
failing, and goes in the differences document); the re-insert changes the
document's CAS, so a CAS loop on the item retries once; no stream record is
written, since the body is unchanged and stream records are explicit. A
`TransactGetItems` that cannot take a lease, or whose locking read collides
with another transaction's staged mutation, loses that round and retries the
snapshot, up to 10 attempts and 5 s, then answers `TransactionCanceledException`
with `TransactionConflict`. A `ConditionCheck` item in `TransactWriteItems` is
leased like every other target and takes the same lock on a present document,
so a plain write to it between the check and the commit meets the lease and is
refused.

**Limits.** A `TransactWriteItems` of 100 actions and 4 MB runs as one SQL++
transaction, and two query-service settings bound it: `txtimeout` (the
backend's `txn_timeout`) and the node's `memory-quota`, which bounds a
transaction's delta table and defaults to 0, no quota. A transaction that runs
past `txtimeout` before its commit point is rolled back by the query service
and answers 503 `ServiceUnavailable` with nothing written. One whose `COMMIT
WORK` was sent and whose outcome is unknown (the statement timed out or the
node went away) is never run again: the backend reads the request's
idempotency token, which is written inside the same transaction, and answers
success if the token is there with the request's fingerprint, otherwise 500
`InternalServerError` ("commit outcome unknown, not retried") with the
transaction id in the server log; a client retry with the same
`ClientRequestToken` then replays the committed result or runs the request if
it did not commit, so the outcome is resolved by the retry. The operational
requirement is `memory-quota` at 0 (the default) or at least the value a
measured 100-action, 4 MB `TransactWriteItems` on a stream-enabled table with
`NEW_AND_OLD_IMAGES` needs (an open engineering item); a valid request on a
node configured below that answers 503 with the query-service error logged,
never a silent partial commit.

`ClientRequestToken` idempotency is a document in `data.idempotency` with a
600 s retention; a replay with a matching fingerprint returns the original
result, a mismatch is rejected (see RFC-0003 row 3.3 for the atomicity with the
writes).

Validated: conformance 912 + the 31-test transaction suite + the 15-test
concurrency suite single-node; on a 3-query-node cluster, transactions survive a
query node being failed over mid-flight (`scripts/query-txn-multinode-test.sh`);
`tests/test_transaction_rollback.py` covers single-key contention, cross-table
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
whose transaction did not commit never holds a record. The number is taken
after the write's pre-read and the step from taking it to its commit or skip
marker runs as its own task, so a client that goes away mid-write cannot leave
a number unresolved. A **skip marker** (a `skip: true` document) is written
for a number only when its transaction definitely did not commit: a
`ROLLBACK WORK` that succeeded, or an error before `COMMIT WORK` was sent. A
commit with an unknown outcome writes no marker (the code's `Ambiguous`
outcome), so a marker can never hide a committed record; such a number is
resolved by the reader. `GetRecords` is a `request_plus` SQL++ read of
`idx_stream_shard_seq`; it walks the shard in sequence order and **stops at the
first missing number** unless that number is resolved: by a skip marker, or,
once the writer's timestamp on the row after it is older than `txn_timeout`
plus a 15 s margin (the `BEGIN` round trip and clock skew), by **reading the
number's record inside a query-service transaction**, which sees the committed
version as soon as the transaction record says committed, whatever the state
of unstaging or of a node that has not been failed over. If that read finds
the record, it is delivered; if it finds nothing after the attempt has
expired, the number is skipped; if the read fails, the stop stays. Couchbase
gives no bound on cleanup after a node failure, so no age alone resolves a
number. The iterator only ever moves over the contiguous resolved prefix, so a
record that commits late is still delivered, exactly once, and `LATEST` and
`AT_TIMESTAMP` iterators start no later than the first unresolved number of
the shard. The out-of-tree reader resolves by age alone; the transactional
read, and the test that holds one transaction between its commit point and
its unstaging for longer than the age while a later one commits, are open
engineering items.

**Single-item writes on a stream-enabled table** run as query-service
transactions without a `transact_write` lease (they are not
`TransactWriteItems`), after checking that no such lease holds the item. Two
that meet on one key conflict at the document, the later one retries within
the 64-attempt, 10 s budget, and both succeed, which is what
`test_racing_puts_on_a_stream_table_all_succeed` expects; at the end of the
budget the write answers 503. `TransactionConflictException` is answered only
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
failover with no data loss (nightly build #51 on 2026-11-02, commit `201f03f`,
run against the older `e41f370` test suite as noted in row 6.3: 913 tests, 9
non-passes under failover of which 4 were new, re-run of those 4 passed 4 of 4);
and the 14-case QE cluster-ops suite (rebalance in and out under load, stopped
and swap rebalance, graceful and hard failover, delta and full recovery,
auto-failover, node SIGKILL, churn, index-node add and remove) passes on the
`extenddb-couchbase-qe-cluster-ops` Jenkins job.

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
costs `2 × g × (1 + replicas)` index instances, plus two for a TTL index. The
check reads the cluster's scope, collection and index counts through the REST
API under the server's create lock (one server per catalog, and every
`CreateTable` and `UpdateTable` of the server runs under one process-wide
lock), so two concurrent creates count one after the other and cannot both
pass with room for only one. `CreateTable` and `UpdateTable` answer
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
- **Query service**: `memory-quota` at 0 (the default, no quota) or at least
  the value the 100-action transaction measurement sets.
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
| `crates/storage-couchbase/` (today `backend/`) | New crate — full backend: 26.4k lines in `backend/src`, of which ~2.4k are inline `#[cfg(test)]` modules (24k non-test), before the vector-search and multi-server code is removed |
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
| Plain write vs transaction | One lease document per item, taken by both sides on tables a transaction has touched; tables without transactions keep the single KV write | Mutual exclusion through KV insert's fail-if-exists, a documented operation; DynamoDB's priority for the transaction; nothing depends on how Couchbase stages a mutation |
| TransactGetItems | Leases on every read key plus a locking read (delete and re-insert) of present documents | SQL++ transactions are read committed; the lease excludes plain creates of absent keys, the lock excludes other transactions |
| Streams | Explicit records, four shards per stream, number taken before commit, reader stops at an unresolved number and resolves it by a transactional read | DynamoDB contract (monotonic sequence, images, 24 h retention); no reader passes a number that may still commit, with no reliance on a cleanup bound |
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
  a 26% drop at 20× the data. Bulk load ~7,400 inserts/s.
- **Scans** are index-bound and run at `request_plus`; a scan issued while the
  indexer is catching up after a bulk load waits for it.
- **Transactions** add query-service round trips; the backend exports
  per-operation request latency and storage-query latency at `/metrics` (no
  CAS-retry counter is exported today). A 100-action `TransactWriteItems` can
  exceed the 30 s `txtimeout` at WAN latency
  (couchbaselabs/extenddb-couchbase#177).
- **Multi-node scaling** (Couchbase's performance CI, cited under Motivation,
  measured with several API instances): workload B scales near-linearly (~80%
  efficiency at 18 nodes; read p50 ~4 ms, p99 9–15 ms, flat across scale).
  Workload A below saturation on 18 nodes: update p99 5.7 ms and read p99 3.0 ms
  at 5,000 ops/s, 8.6 ms and 4.0 ms at 10,000. A single API instance stalls
  near 10,000 ops/s, and with one server per catalog that is the deployment's
  bound; the README row states it as a known limit, and the sizing guidance in
  `docs/perf-pass-plan.md` is the per-instance one.
- **Flagged tables.** A plain write on a table a transaction has touched adds
  a lease insert and delete, two sub-millisecond KV operations, to its write;
  the measured YCSB workload A figures for a flagged and an unflagged table
  are published before FCP (open item 12).
- Connection-pool metrics are not emitted: the Couchbase SDK owns pooling and
  does not expose per-acquire latency (couchbaselabs/extenddb-couchbase#178).

### Testing

| Layer | What | Where |
|---|---|---|
| Unit | number encoding, item round-trip, sidecar, key forging, policy hardening | `cargo test --lib`: 262 passed, 0 failed, 0 ignored (offline run, 2026-11-09) |
| Storage-invariant lint | stale top-level `_pk`/`_sk` reads, hand-built keys, unescaped `LIKE` | `scripts/lint-storage-invariants.sh`, per PR |
| Conformance | upstream `tests/test_*.py` 912 + 1 xfail, `tests/python/` 331, web console 30 | `CONFORMANCE.md`; the Jenkins nightly's sweep |
| Backend tests | transaction rollback (7), DynamoDB head-to-head parity, encoding edges | `tests/` |
| Concurrency | property harness (conservation, no-lost-update, one-winner, CAS counters), write-race, txn-concurrency | `scripts/` |
| HA | failover race, sweep under failover, query-txn multinode | `scripts/ha-test.sh` and friends, nightly |
| QE | 343-case plan, 23 areas, executable `qe/tests/`; cluster-ops on a real 3-node cluster | `docs/qe/`, Jenkins `extenddb-couchbase-qe-cluster-ops` |
| Interop | five client apps in four languages, reverse ETL, Couchbase-side ops matrix | `examples/`, `scripts/cb_to_ddb.py`, `docs/couchbase-side-operations.md` |

## RFC-0003 acceptance criteria

Upstream RFC-0003 is the rubric every backend is judged against: ten sections
and about forty criteria. The table below maps each criterion to the mechanism
in this backend and to the test that proves it. "Upstream" tests are files in
the ExtendDB conformance suite (`upstream/tests/`); QE ids are cases in
`docs/qe/testcases.md`; `tests/` and `scripts/` are the reference repository's
own. Criteria that this backend does **not** fully satisfy today are marked
**OPEN** with the planned fix.

### 1. Item identity

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 1.1 | Unique addressability | Document key `<table_id>#<pk_enc>|<sk_enc>`, hashed past 240 bytes (Couchbase's limit in a named collection is 246); built only by `full_doc_key` | DT-15, LM-02/03/04, `tests/test_encoding_edges.py` |
| 1.2 | No delimiter ambiguity | Key values are type-tagged and encoded before the separator is applied, the hashed form's `H#` segment cannot be a type tag, and `scripts/lint-storage-invariants.sh` blocks any hand-built key (see "Key encoding") | `tests/test_encoding_edges.py` (separator-collision class), DT-14, DT-15 |
| 1.3 | Numeric precision | Key and index columns always use the order-preserving string encoding at full 38 digits; they are never stored as a native number, so one logical field has one storage type. The top-level attribute copy is a JSON number only when `normalize_number`'s text survives the round trip, otherwise an exact string recorded in the type sidecar | DT-04 to DT-08, DT-17, parity suite number-fidelity battery incl. the ULP case |
| 1.4 | Binary key ordering | Encoded as lowercase hex, so unsigned-byte lexicographic order | DT-18, QS-04, IX-05, `tests/test_dynamodb_parity.py` |

### 2. Secondary indexes

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 2.1 | Duplicate index keys | There is no separate index entry: the derived keys live in the item document and the Couchbase GSI indexes the document itself, so identity is the base document key | IX-04, upstream `test_put_item_create_race.py` (GSI and LSI tables) |
| 2.2 | Stale entry prevention | Every write path rebuilds `_xdb` through `item_to_document`; the sub-document fast path is refused for any key or index attribute | IX-09, upstream `test_query_scan.py` GSI cases, stress "GSI query after updating a key attribute" |
| 2.3 | Sparse semantics and type validation | The shared validators in `crates/core/src/validation` reject a wrong-type or empty index key with `ValidationException`; a missing attribute yields no derived key and the GSI `WHERE` excludes it | IX-03, DT-20, upstream `test_gsi_attribute_definitions.py` |
| 2.4 | Backfill on UpdateTable | Catalog row written `CREATING`; the index reconciler waits for the key-info cache invalidation and for every request in flight at that moment to finish, backfills `_xdb._g<slot>` for existing items, and only then flips to `ACTIVE` (the fence is an open engineering item) | IX-07 (the upstream `gsi_async*` files spawn their own server and are outside the endpoint sweep, so IX-07 is the gate) |
| 2.5 | Transactional index maintenance | Transactional writes rewrite the whole document, derived keys included, inside the query-service transaction | TX-01, TX-13, upstream `test_transaction_operations.py` |
| 2.6 | Index query pagination | `LastEvaluatedKey` carries index and base keys; the resume bound is merged with, not substituted for, the key condition | IX-06, QS-07, upstream `test_read_paging.py` |
| 2.7 | ConsistentRead on GSI | Rejected by the engine with `ValidationException`; the backend exposes no strong-read option for index reads (see "Reads") | IX-10, QS-20 |

### 3. Transactions

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 3.1 | All-or-nothing | One query-service transaction carries the data writes (with their derived index columns), the stream records, and the idempotency create; `COMMIT` or nothing, and the backend issues `ROLLBACK WORK` on any failed statement | TX-01 to TX-03, `tests/test_transaction_rollback.py` (cross-table, rollback completeness, crash mid-transaction) |
| 3.2 | Stream records in transaction | `stream_txn_write` appends the records to the same transaction as the writes | ST-04, TX-13, upstream `test_streams.py` |
| 3.3 | Idempotency token atomicity | The token document `<account>#<token>` is read first (replay or mismatch by fingerprint, 600 s retention), then **recorded as a fail-if-exists create inside the same transaction as the writes**. Two concurrent requests with the same fresh token race on that create; exactly one commits, the loser's "exists" is mapped to replay or mismatch. Token and data commit or roll back together, and a cancelled transaction releases its token | TX-05, TX-06, upstream `test_idempotency_account_scope.py`, `test_conditional_atomicity.py`, `tests/test_transaction_rollback.py` |
| 3.4 | CancellationReasons fidelity | Per-operation reasons in input order; `ALL_OLD` item attached on condition failure | TX-02, CR-12, upstream `test_transaction_operations.py`, `test_transact_expression_validation.py` |
| 3.5 | TransactGetItems isolation | Every read key is leased, present or absent, and every present document's write lock is taken inside one read transaction (see "Transactions"); a read that cannot lease or lock within 10 attempts and 5 s is cancelled with `TransactionConflict`. **OPEN**: the absent-key lease, with a test where a `PutItem` creates an absent key during a `TransactGetItems` | TX-07, upstream `test_transact_get_snapshot.py` |

### 4. Concurrency and isolation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 4.1 | Unconditional writes never fail under contention | A bare unconditional `PutItem` with no `ReturnValues` is one KV write: no CAS, no retry, last writer wins, **both racing calls succeed**. With `ALL_OLD` it is a CAS loop of 64 attempts / 10 s that answers 503 at exhaustion, as MongoDB does; no CAS detail is client-visible. **OPEN**: the out-of-tree loop answers `TransactionConflict` at exhaustion; the 503 is part of the port, with the differences-document rows | upstream `test_put_item_create_race.py::test_racing_puts_with_all_old_all_succeed`, `::test_racing_puts_on_a_stream_table_all_succeed`, `test_concurrency.py::test_parallel_inserts`, CR-03 |
| 4.2 | Conditional writes: correct error on race | The CAS loop re-evaluates the condition against the winner's image and returns `ConditionalCheckFailedException` | CC-01, upstream `test_put_item_create_race.py::test_racing_puts_check_their_condition_against_the_winner`, `::test_racing_conditional_creates_have_one_winner` |
| 4.3 | Transaction conflict mapping | A plain write meeting a `transact_write` lease returns `TransactionConflictException`; a transaction that cannot take its leases in 2 s returns `TransactionCanceledException` with `TransactionConflict`; a plain write meeting a `transact_get` or `plain` lease retries and answers 503 at its budget; plain-writer contention never surfaces as a transaction conflict. **OPEN**: the lease on every plain path (see "Open engineering items") | TX-08, CC-09, upstream `test_transact_write_conflict.py`, `scripts/write-race-test.py` |
| 4.4 | UpdateItem serializes | CAS read-modify-write with jittered backoff; arithmetic and `ADD` never take the sub-document path; on a stream-enabled table each write is a transaction, two on one key conflict at the document and the later retries within the same budget, and both succeed | CC-02, CC-03, `scripts/concurrency-property-test.py` (`prop_add_counter`, `prop_optlock_counter`), upstream `test_concurrency.py::test_atomic_counter` |
| 4.5 | Read-your-writes | `GetItem` is a KV get served by the active vbucket; no replica reads | CC-04, CR-06 |

### 5. DynamoDB Streams

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 5.1 | Atomicity with data writes | Records are written in the data transaction | ST-04, upstream `test_streams.py` |
| 5.2 | Per-shard ordering | Numbers are taken before commit; `GetRecords` walks the shard in sequence order and stops at an unresolved number, which only a skip marker written after a definite failure, or a transactional read of the record after the age, resolves (see "DynamoDB Streams"), so no reader passes a number whose record may still appear. **OPEN**: the transactional read, and the test that holds one transaction between commit point and unstaging for longer than the age while a later one commits | ST-03, DC-07, upstream `test_streams.py`, stress "consumer reading during concurrent writes" |
| 5.3 | Account and table isolation | Shard id embeds the table id; records carry the table id and every read is scoped by it | upstream `test_cross_account_isolation.py` (one case in this file failed in the nightly baseline for a test-suite-skew reason, see 6.3; it passes against the pinned suite), `test_stream_table_reuse.py` |
| 5.4 | Correct event types | `INSERT` / `MODIFY` / `REMOVE` with images per view type; an unchanged item emits no record | ST-01, ST-05, upstream `test_streams.py` |
| 5.5 | Retention and cleanup | `idx_stream_ts` plus a delete at 24 h on every TTL sweep cycle | ST-06, ST-07 |
| 5.6 | Shard stability | In-tree: stream lineage, four shards per stream with the lineage shard id, enable and disable through `UpdateTable`, idempotent by construction since a repeat targets the same newest stream. Today: one deterministic shard per table and `UpdateTable` stream changes refused | ST-02, upstream `test_stream_table_reuse.py` (the two cases that fail today pass with lineage) |

### 6. Condition expressions, code hygiene, documentation

| # | Criterion | How the backend meets it | Evidence |
|---|---|---|---|
| 6.1 | Evaluation semantics | Delegates to `extenddb_core::expression::evaluate_condition` on the read image; no native pushdown | CR-10 to CR-13, upstream `test_conditional_writes.py`, `test_conditional_operator.py` |
| 6.2 | No hidden unused code | **OPEN.** `engine.rs` and `catalog.rs` carry a crate-level `#![allow(unused_variables, unused_imports, clippy::too_many_arguments)]`, which RFC-0003 prohibits, plus three targeted `#[allow(dead_code)]` with comments. Measured on 2026-11-05 with those lints force-enabled: the blanket allows hide 3 unused imports and no unused variables; the dead-code lint is not suppressed and reports 5 never-used items. Fix before the implementation PR: delete the blanket allows and the 5 dead items, keep only commented targeted allows | `cargo check --lib` with `--force-warn unused_imports --force-warn unused_variables --force-warn dead_code` |
| 6.3 | Documentation describes the system as built | Every mechanism described in this RFC is implemented and on the live code path or marked OPEN with its engineering item; Capella is stated as unvalidated, the failover-sweep claim cites a specific nightly run, every conformance figure names its suite revision, and the scaling figures cite measured runs. Also recorded here for honesty: nightlies #46 through #58 reported a **baseline** sweep of 908 of 913 with the same 5 non-passes every run (`test_abac.py` ×3, `test_auth_permissions.py::test_explicit_deny_all_service_operations`, `test_cross_account_isolation.py::TestStreamAccountScoping::test_shard_iterator_only_returns_owning_account_records`). Triaged 2026-11-09 as test-suite skew: the nightly's heavy lane skipped the "clone upstream at the pinned rev" stage, so the sweep ran the stale `e41f370` test checkout (913 collected) against the auth-cluster build. What the pinned `7b828b0` run of 2026-11-24 (under "Evidence") shows: the `test_auth_permissions.py` and `test_cross_account_isolation.py` cases pass, and three `test_abac.py` cases fail, all three `aws:SourceIp` loopback cases, behind Docker Desktop's bridge on the machine that ran it. Whether the nightly's three `test_abac.py` non-passes were those cases, and whether they pass on the Linux agent, is settled by the first nightly after couchbaselabs/extenddb-couchbase#237 (merged 2026-11-24: the clone stage runs in both lanes and `scripts/sweep-under-failover.sh` refuses a checkout that does not match the Cargo pin); until then the skew triage covers two of the five, not all five | Jenkins `extenddb-couchbase-nightly` #51 console; #237; the pinned run under "Evidence" |

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
| 9.2 | DynamoDB error codes | Couchbase and SDK errors map to `StorageError` variants that the engine renders as DynamoDB codes; no backend-specific code reaches a client; a query-service resource limit answers 503 with the cause logged | NE-01 to NE-03, upstream `test_auth_error_fidelity.py`, `test_expression_error_prefix.py` |

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

### Open engineering items

Work the implementation PR carries, each with the test that proves it, listed
so the acceptance table above is read with them in mind:

1. The lease on every plain write path of a flagged table, the `transactional`
   flag, the `committing` state and the transactional escalation, and the
   pre-commit lease check with rollback. Tests: `PutItem` against a
   transaction held between its commit point and its unstaging (the query node
   killed after `COMMIT WORK` is sent), both orders; a `PutItem` of a key the
   transaction creates; a live test that stages a mutation and checks that
   each plain write path (`PutItem`, `UpdateItem` on both of its paths,
   `DeleteItem`, `BatchWriteItem`) refuses while the lease is held; a
   transaction whose renewal is made to fail rolls back before `COMMIT WORK`.
2. The transactional read that resolves an aged stream number, and `LATEST`
   and `AT_TIMESTAMP` starting no later than the first unresolved number.
   Test: hold one transaction between commit point and unstaging for longer
   than the age while a later one commits; a reader must not pass the earlier
   number and must deliver it afterwards.
3. The `UpdateItem` fast path falls back above 16 sub-document operations.
   Test: nine `SET`s on a v3.2 table.
4. `TransactGetItems` leases on absent keys and its 5 s bound. Test: a
   `PutItem` creates an absent key during a `TransactGetItems`.
5. The GSI backfill fence on the cache invalidation and the in-flight
   counter. Test: a write whose key info was resolved before the invalidation
   lands after the backfill passed its item; the reindex reconciler's writes
   in the same window.
6. The 100-action, 4 MB `TransactWriteItems` measurement on a stream-enabled
   table with `NEW_AND_OLD_IMAGES`, the `memory-quota` it sets, the 503 below
   it, and the two timeout answers.
7. The descending twin indexes, counted in the budget, with descending paging
   in the tests.
8. Cluster-wide scope, collection and index counting under the create lock,
   twins and replicas included; `LimitExceededException` at the limit; the
   measured shared-tier bound.
9. `destroy` from the catalog and the ledgers; the foreign-store check on an
   `Uninitialized` store; adoption of an out-of-tree deployment by
   `migrate --yes`; the lock in `_default._default`.
10. CAS exhaustion answers 503, and the `docs/differences-from-dynamodb.md`
    rows: contended single-item writes, plain writes meeting a read lease,
    `TransactGetItems` conflicts, backup, the per-instance throughput bound.
11. The hashed-key read compares the stored encoded keys; a partition key
    containing `|S#` in the separator-collision test.
12. The lease cost: YCSB workload A on a flagged table against an unflagged
    one, published under "Performance characteristics" and in the README row.
13. The upstream multi-part RANGE tests against the per-part columns.
14. The lint item 6.2.

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
- **A real port, not a copy.** The backend is validated against `v0.1.x`;
  adopting the migrations framework, the key ring, stream lineage and the
  engine-owned backup path is new code whose correctness is re-proven in-tree.
- **No table snapshot.** `CreateBackup` is consistent per item, not at an
  instant, as on Cassandra.
- **One server per catalog.** The out-of-tree multi-instance API tier is not
  part of this RFC; throughput is bounded by one instance until the follow-up
  RFC.
- **The lease on transaction-touched tables.** A plain write on a table that
  has ever seen a transaction costs a lease insert and delete around its KV
  write, about three KV operations instead of one, and the flag never clears.
  The measured cost is published before FCP (open item 12); an operator who
  needs the single-operation path back recreates the table.
- **Maintenance burden.** ~24k non-test lines of backend plus QE harness, Helm,
  and runbooks owned by the Couchbase team; trait changes upstream require a
  coordinated update, as RFC-0002 anticipates.
- **Known constraint: query prepared statements are off by default
  (couchbaselabs/extenddb-couchbase#194).** The Couchbase Rust SDK's query core
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
3. **The `transactional` flag.** Whether a never-clearing per-table flag is
   acceptable to the maintainers, or the lease should be taken on every table
   unconditionally once the measured cost is known.

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
