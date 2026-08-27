# ADR-0005: Index-build lifecycle stays in the backend until a second backend needs it

- Status: Accepted
- Date: 2026-08-14
- Deciders: @LeeroyHannigan

## Context

The vector index build is asynchronous where the GSI build is not: the table
stays open for writes during the backfill, so the SQLite backend carries a
lifecycle machine the GSI path never needed. Measured against the branch, that
machinery is roughly 490 lines: the batched backfill (~140), the CREATING to
ACTIVE state machine with its detached task and flip (~152), the startup
reconciler, stuck-build watchdog and rebuild (~145), and the queue hold and
replay that keeps mid-backfill writes ordered (~50).

Review of the vector PRs (yesyayen, 2026-08-13) asked where this code should
live, pointing at what happened when each backend kept its own copy of the
simpler GSI build: the two `backfill_gsi` copies have already diverged. All
three claimed divergences are confirmed in the tree:

- The PostgreSQL copy keeps the all-0xFF upper-bound defect in
  `increment_bytes` (`storage-postgres/src/data/query.rs:267-277`,
  `vec![0xFF; 1025]`); the SQLite copy fixed it to return `None`
  (`storage-sqlite/src/data/query.rs:139-148`).
- PostgreSQL has no crash reconciler: `reconcile_incomplete_gsis` /
  `reconcile_incomplete_vector_indexes` have zero occurrences in
  `storage-postgres`; SQLite has both in its `update_table.rs`.
- PostgreSQL's `backfill_gsi` (`table_helpers.rs:65-139`) is a synchronous
  single transaction with no CREATING reporting.

The proposal in the review: move the state machine into shared engine code and
have each backend supply only storage primitives (scan a batch, insert a row,
drop a table). The question is whether to do that now, on a branch that has one
live implementation, or when a second one appears.

## Options Considered

1. **Refactor now.** Extract the ~490-line lifecycle into the engine behind a
   trait of storage primitives, and rewrite the SQLite backend as the first
   implementor, before the vector PRs merge.
2. **Defer until a second backend implements `VectorSearchEngine`,** recording
   the commitment here. The SQLite implementation stays where it is; the
   refactor is the first task of the next backend's vector work, using the
   SQLite behaviour (and its tests) as the specification.
3. **Never share; document per-backend copies as the convention.** Accept GSI's
   status quo as the model.

## Decision

Option 2: the lifecycle moves to shared engine code when, and as the first step
of, a second backend implementing `VectorSearchEngine`; until then it stays in
`storage-sqlite`.

## Rationale

- A shared abstraction extracted from one implementation encodes that
  implementation's accidents as the interface. SQLite's lifecycle leans on
  properties the next backend will not share: a process-wide write lock,
  `rowid` as the backfill cursor, `BEGIN IMMEDIATE` batch semantics, and a
  single-process queue hold. Which of those are essential and which are
  SQLite-shaped only becomes visible against a second backend (PostgreSQL has
  no rowid; its equivalent cursor, its lock story, and its LISTEN/NOTIFY-style
  wakeups are all different). Extracting now means designing the trait against
  a hypothetical.
- The GSI divergence is real evidence, but of the opposite discipline failing:
  the copies diverged because nothing forced the second copy to match the
  first. The forcing function this ADR creates is explicit: a second backend
  MUST NOT re-implement the lifecycle; its review gate is the extraction.
- The refactor is ~490 lines of the most correctness-sensitive code on the
  branch (crash reconciliation, write ordering during backfill, poison-row
  handling just landed in review). Doing it under review pressure, with no
  consumer to validate the interface, maximises the chance of both a worse
  interface and new defects.
- The behaviour is already pinned where it matters for a future extraction:
  the backfill, hold-and-replay, poison-skip, transient-retry, and
  reconciliation paths all carry discriminating tests that a shared
  implementation must keep passing.

## Crash durability of async propagation

The async model raises the question a synchronous single-transaction design
never faces: what happens to an enqueued-but-unapplied index write when the
process dies?

Nothing is lost, because there is no in-memory queue. The pending row is
INSERTed into `gsi_pending` on the same transaction as the base write
(`data/index.rs`, `maintain_vector_indexes`), so the enqueue is exactly as
durable as the write it describes: either both committed before the crash or
neither did. On restart the worker claims due rows from `gsi_pending` as on
any other pass; no component needs to re-derive pending work from the base
table, because the queue itself is the durable record. This holds identically
for GSI and vector kinds, which share the queue and the worker.

Proven by `enqueued_propagation_survives_a_process_crash` (workers.rs): two
engine lifetimes on one database file, three writes committed with the queue
full and the index empty, cold restart, one drain converges the index to all
three. Its negative control (enqueue rolled back instead of committed) fails
the survival assertion, so the test discriminates. The one fidelity gap is
deliberate: dropping the engine closes SQLite cleanly where SIGKILL leaves an
uncheckpointed WAL, and SQLite's recovery of committed WAL transactions on
the next open is the layer below the property this pins.

## Consequences

- Easier now: the vector PRs merge without a 490-line rewrite appended, and
  the lifecycle keeps its SQLite-tuned performance characteristics.
- Harder later: the extraction lands as the first task of the next backend's
  vector work, where it belongs, but that makes the next backend's first PR
  larger than it would otherwise be.
- Commitment that is expensive to reverse: none; this ADR is itself the cheap
  reversible artifact. What it commits us to is procedural: reviewers of any
  future `VectorSearchEngine` implementation reject a second copy of the
  lifecycle, citing this document.
- Follow-up created: the PostgreSQL GSI divergences confirmed above (the
  all-0xFF bound defect in particular) are pre-existing defects independent of
  this decision and need their own issues.

## Outcome (2026-08-19)

The condition this decision waited on arrived: the PostgreSQL backend is the
second implementor of `VectorSearchEngine`, and the extraction landed as the
first step of that port, exactly as the Decision section prescribes. The
lifecycle now lives in `crates/storage/src/vector_lifecycle/` (the
`VectorIndexBuild` primitives trait plus the shared backfill, publish, and
rebuild drivers), with the SQLite backend rewired as the first implementor and
its pre-existing vector tests, unchanged, as the acceptance gate. The
procedural commitment above stands: reviewers reject any second copy of the
lifecycle, which now means any backend logic that bypasses the shared module.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
