<!--
Copyright 2026 ExtendDB contributors
SPDX-License-Identifier: Apache-2.0
-->
# extenddb-storage-sqlite — Design Decisions

Status: **D1 and D2 APPROVED.** Rationale stands on correctness grounds below:
`BEGIN IMMEDIATE` + single writer is the only model that makes
condition-check-then-write atomic under SQLite (D1); an order-preserving TEXT
encoding ordered under SQLite's default BINARY collation is the only portable way
to preserve DynamoDB's 38-digit numeric ordering and precision without a custom
collation or `REAL` truncation (D2).

This memo records the SQLite-specific decisions taken to reach behavioural
parity with the reference PostgreSQL backend (`crates/storage-postgres`). It is
the SQLite analogue of the project's ADRs. The guiding principle is the one
stated for the task: **correctness and as close to DynamoDB semantics as
possible**, matching the integrity of the Postgres backend.

The trait contract is the local `extenddb` working tree on `main`
(v0.1.1-17-g93de8e3 at time of writing).

---

## D1 — Transaction isolation model  *(needs sign-off)*

### Requirement
DynamoDB requires, and the Postgres backend provides:
- Condition expressions evaluated **atomically** with the write they guard.
- `TransactWriteItems`: all-or-nothing across multiple items/tables.
- `TransactGetItems`: a single consistent (serializable) snapshot.
- Atomic stream-record capture in the same transaction as the data write.
- Atomic idempotency-token check+store with the writes.

Postgres achieves this with `BEGIN ISOLATION LEVEL SERIALIZABLE`. SQLite has no
equivalent knob; a naive multi-connection pool with deferred `BEGIN` allows
write-skew and `SQLITE_BUSY` on the read-then-write path.

### Options considered
- **(A) Deferred `BEGIN` on a shared pool** — what a naive port does. Rejected:
  a `SELECT` (condition read) that later `UPDATE`s upgrades the lock late and
  can deadlock/`SQLITE_BUSY`, and two concurrent guarded writes can interleave
  (write-skew). Not correct.
- **(B) `BEGIN IMMEDIATE` on every write txn + `busy_timeout`** — acquires the
  reserved write lock up front, so the condition read and the write are inside
  the writer lock; other writers wait (bounded by `busy_timeout`) rather than
  failing. Correct. Still allows many concurrent readers (WAL).
- **(C) Single dedicated writer connection behind an async `Mutex`** (reads use
  a separate read pool). All writes globally serialized in-process — this is
  the exact model the in-memory backend uses (one global `RwLock`), which the
  project accepts as behaving "the same as Postgres". SQLite already serializes
  writes physically, so this sheds little real throughput while removing all
  `SQLITE_BUSY` races by construction.

### Recommendation
**(C) + (B): a single writer connection (async `Mutex`) AND `BEGIN IMMEDIATE`,
with `busy_timeout` as a safety net; reads served from a concurrent WAL read
pool.** This is the most defensible for correctness, mirrors the in-memory
backend's accepted serialization model, and eliminates write-skew and
`SQLITE_BUSY` deterministically. Tradeoff: one writer at a time — acceptable for
SQLite's single-node/embedded target, and SQLite serializes writes anyway.

PRAGMAs on every connection: `journal_mode=WAL`, `busy_timeout=5000`,
`foreign_keys=ON`, `synchronous=NORMAL`.

---

## D2 — Numeric (`N`) sort-key encoding  *(needs sign-off — main fidelity risk)*

### Requirement
DynamoDB numbers are signed decimals with up to 38 significant digits. Sort-key
semantics require **numeric ordering** for `Query` ordering, `BETWEEN`, `<`,
`<=`, `>`, `>=`, and **numeric equality** (so `5`, `5.0`, `+5` compare equal).
Postgres stores `N` sort keys in a `NUMERIC` column → exact precision + correct
order, binding the shared `SortKeyValue::N(BigDecimal)` directly.

SQLite's only column affinities are INTEGER / REAL / TEXT / BLOB — **no
arbitrary-precision numeric type.**

### Options considered
- **(A) `REAL` (f64)** — PR #109's approach. **Rejected (defect):** silently
  truncates beyond ~15–17 digits, corrupting ordering and equality for
  large/high-precision sort keys. Fails number-precision conformance.
- **(B) Custom SQLite collation** registered per-connection that compares the
  raw decimal string as `BigDecimal`. Exact, but: must be registered on every
  pooled connection and on the persisted index, adds a non-portable
  `libsqlite3-sys` dependency surface, and a missing-collation path silently
  reverts to wrong ordering. Higher operational fragility.
- **(C) Order-preserving TEXT encoding** — store `N` sort keys in a TEXT column
  holding a canonical, byte-lexicographically order-preserving encoding of the
  `BigDecimal` (sign + normalized exponent + normalized mantissa). SQLite's
  default BINARY collation then yields exact numeric ordering. The **exact
  original value is preserved in `item_data` JSON** (what we return to clients),
  so there is zero precision loss on read; the encoded column is used only for
  ordering/range/equality. Canonicalization makes `5` / `5.0` / `+5` collapse to
  one key (matching DynamoDB normalization).

### Recommendation
**(C) Order-preserving TEXT encoding**, implemented as a small, exhaustively
unit-tested function `encode_orderable_number(&BigDecimal) -> String` with
property tests asserting `a.cmp(b) == enc(a).cmp(enc(b))` across negatives,
zero, varying scale, and 38-digit extremes. This is portable (no extra native
deps), keeps full precision in `item_data`, and is correctness-critical so it
gets its own dedicated test module before any data-path code depends on it.

Partition keys (incl. `N`) remain TEXT via the shared `pk_to_text` (equality
only) — identical to Postgres, no change needed.

---

## D3 — Structural & schema decisions (informational, no sign-off needed)

- **Module layout** mirrors the in-memory crate (the current, same-packaging
  sibling): `store.rs`, `bootstrapper.rs`, `catalog*.rs`, `data*.rs`,
  `metadata.rs`, `stream.rs`, `backup.rs`, `credential_store.rs`, `hooks.rs`,
  `worker.rs`, `operations.rs`, `diagnostics.rs`, `registration.rs`, `config.rs`,
  `schema.rs`. SQL logic is ported from PR #109 and corrected/retargeted.
- **Single file, single logical DB.** Catalog and data tables co-locate (SQLite
  has no multi-database server). The dual-pool Postgres split collapses to one
  logical store; `data_database_name` is recorded for diagnostics parity only.
  Connection *pools* are not collapsed for file-backed databases: the serve path
  opens two pools — the engine pool (all data reads/writes, serialized by
  `write_lock`) and a separate catalog pool (catalog + credential stores) so
  catalog reads don't contend with the data write lock. Only `:memory:` uses a
  single pinned connection (see D4).
- **Schema** is one authoritative migration mirroring Postgres `001_schema.sql`
  semantics: accounts, tables, indexes, tags, settings (seeded `catalog_version`
  = the compiled `CATALOG_VERSION`), stream_shards/records, seq_counters,
  admin_users, full IAM (users/groups/roles/policies/boundaries/sessions),
  access_keys, idempotency_tokens, metrics, login_attempts, backups/backup_items,
  continuous_backups. Per-DynamoDB-table data tables (`_ddb_<id>`) are created
  dynamically by `create_table`, same as Postgres.
- **Contract drift already identified to honour:** `ServerComponents` uses
  `credential_store` (not PR #109's `auth_provider`); `StorageConfig` requires
  `as_any`; `TableKeyInfo` now carries `base_key_schema`.
- **LSI propagation is synchronous; GSI propagation is a persistent queue.**
  LSIs (always) and GSIs with an effective delay of 0 are reconciled inside the
  base write transaction, so they are strongly consistent. GSIs with a non-zero
  effective propagation delay are deferred: a single row capturing the base-item
  transition (`table_id`, `old_item`, `new_item`, `ready_at = now + delay`) is
  inserted into a `gsi_pending` table **inside the same write transaction** as
  the item mutation, giving a zero crash window (the queue entry commits atomically
  with the data). A background worker claims rows whose `ready_at` has passed via
  `DELETE … WHERE id IN (SELECT … WHERE ready_at <= ? ORDER BY id LIMIT ?)
  RETURNING …` and applies the index updates; claim and apply share one
  transaction under the engine write lock, so a crash rolls back and the rows are
  retried (at-least-once; the index writes are idempotent). This mirrors the
  Postgres backend's persistent-queue design rather than its `FOR UPDATE SKIP
  LOCKED` mechanics, which are unnecessary under the single-writer model.
  The effective delay is the per-GSI `propagation_delay_ms` override when set,
  otherwise the `index_propagation_delay_ms` runtime setting (cached on the engine
  and refreshed by a poller). Delay 0 ⇒ fully synchronous GSI maintenance.
  This **supersedes the earlier "synchronous because local I/O is fast"
  rationale**: that approach ignored the configured propagation delay and so did
  not match the Postgres backend's eventual-consistency behaviour for GSIs.
- **Stream sequence numbers** come from a `seq_counters` row updated inside the
  write txn → monotonic per the contract.
- **Parallel scan** uses `rowid % total_segments`.
- **Encryption** (access-key secrets) reuses AES-256-GCM with AAD, identical to
  Postgres/memory.

---

## D4 — In-memory (`:memory:`) support and the dedicated-memory-backend consolidation question

**Capability.** The backend supports `path = ":memory:"` as a first-class,
ephemeral mode for local dev / CI/CD. Two pieces make it work:

- **Single shared connection.** An in-memory database lives only inside its own
  connection — a second connection opens a *separate* empty database. So for
  `:memory:` the pool is pinned to one connection that is never recycled (idle
  and lifetime timeouts disabled), giving one shared database for the process
  lifetime. Writes are already serialized by `write_lock`, so a single
  connection costs only read concurrency. File-backed databases keep the full
  WAL pool (concurrent readers + one writer).
- **Serve-time bootstrap.** An in-memory database does not survive the `init`
  process, so the `ServerComponents` factory bootstraps the catalog at serve
  time on the engine's shared connection (`SqliteEngine::bootstrap_ephemeral`:
  schema, encryption key, default account, env-driven admin) and reuses that
  connection for the catalog/credential stores. This mirrors the dedicated
  in-process memory backend, which also bootstraps account + admin from
  `EXTENDDB_ADMIN_PASSWORD` on every start.

**Benchmark vs the dedicated in-memory backend.** Measured end-to-end over HTTP
with `extenddb-bench`'s open-loop, HDR-histogram load generator (same laptop,
sweep 2k→20k rps, 1 KiB items, putitem / getitem / mixed-80:20), then fused with
`report-compare` (bootstrap 95% CI):

- **At low/moderate load (~2k rps) the two are within noise** — both sub-ms p50
  (~300–430 µs). This is the regime a CI/CD test suite actually generates.
- **Above that, sqlite `:memory:` saturates ~3–4× earlier** (clean ceiling:
  put ~2k / get ~5k / mixed ~5k rps, vs the dedicated backend's ~10k / >20k /
  ~10–20k). `report-compare` headline verdict was `Regression` on all three
  workloads, driven entirely by the high-rps steps.
- **Root cause is the connection model, not SQL overhead.** `:memory:` uses one
  connection, so every read and write serializes through it; the dedicated
  backend uses an `RwLock` over in-process maps (concurrent reads, cheap
  writes). It is SQLite's single-writer-per-database nature surfacing, not
  query cost.

**Account/region sharding — considered and rejected for this use case.** SQLite
lacks Postgres's MVCC, so the global `write_lock` serializes writes across all
accounts and tables. Sharding the *data* into one SQLite database per
`(account, region)` — a single global catalog plus per-account data
connections — would recover cross-account write concurrency while keeping
multi-table `TransactWriteItems` atomic (an account's tables stay in one
database; per-*table* sharding would not). It is the right granularity *if*
cross-account concurrency matters. It does **not** help single-account
throughput, and the in-memory backend's mandate is single-account, ephemeral
CI/CD, which never sees cross-account load. So account sharding is **not
pursued for `:memory:`**; it remains a possible future direction for
high-tenant-count *file* deployments only.

**Conclusion (consolidation).** For the in-memory backend's real mandate —
local dev, CI/CD, unit tests: single-account, low/moderate throughput,
discarded between runs — sqlite `:memory:` is performance-equivalent to the
dedicated in-process backend and identical in correctness (same engine, same
conformance/parity results). A single SQLite package can therefore cover both
the persistent (file) and ephemeral (`:memory:`) cases, retiring the separate
in-memory crate to cut maintenance, with no practical loss for how in-memory is
used. The dedicated backend retains only marginal high-concurrency headroom
that CI/CD does not exercise.

This refines **D3's "single file, single logical DB"**: still one database per
server, but that database may be in memory, and an in-memory server
bootstraps its catalog at serve time rather than during `init`.

---

## Open items requiring your decision
1. **D1** — approve "single writer + `BEGIN IMMEDIATE`" isolation model.
2. **D2** — approve "order-preserving TEXT encoding" for `N` sort keys.
