// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared vector index-build lifecycle, owned here so every backend runs the
//! same state machine.
//!
//! `docs/adr/0005-index-build-lifecycle-ownership.md` committed to this
//! extraction: the lifecycle was allowed to live in `storage-sqlite` only until
//! a second backend implemented `VectorSearchEngine`, and that backend MUST NOT
//! re-implement it. This module is the extraction. A backend supplies storage
//! primitives through [`VectorIndexBuild`]; the ordering rules, the poison-row
//! semantics, and the crash-recovery contract live here and cannot drift
//! per backend.
//!
//! # The lifecycle contract
//!
//! The observable behaviour was measured against Amazon DynamoDB (2026-08-06)
//! and is pinned by the SQLite backend's wire tests:
//!
//! 1. **Status sequence.** An index created by `UpdateTable` appears as
//!    `CREATING` with `Backfilling: false`, flips to `Backfilling: true` when
//!    the scan starts ([`VectorIndexBuild::set_backfilling`]), and becomes
//!    `ACTIVE` with the `Backfilling` member absent in a single transition
//!    ([`VectorIndexBuild::mark_active`]). An index created by `CreateTable`
//!    skips the sequence: the table is empty, so it is `ACTIVE` from birth.
//! 2. **The table stays writable throughout.** The backfill commits in
//!    independent batches ([`run_backfill`]) rather than holding one
//!    transaction, and the build task is detached from the `UpdateTable`
//!    call ([`complete_build`] is the task body).
//! 3. **Write ordering during the backfill.** A write that lands while the
//!    index is `CREATING` must not reach the index's data table before the
//!    backfill's (older) snapshot of the same item does, or the index converges
//!    on the stale generation. Each backend enforces this with a claim-time
//!    hold on its propagation queue: rows for a table whose vector index is
//!    `CREATING` are not claimed, and [`VectorIndexBuild::notify_active`] wakes
//!    the worker to replay them once the index is published. The hold is per
//!    TABLE, not per index, so a GSI row and a vector row for one item keep
//!    their relative order.
//! 4. **Poison rows are skipped and counted, never fatal.** Rows written
//!    before the index existed never passed vector validation, so a malformed
//!    or wrong-dimension stored vector is expected backfill input, not a bug.
//!    [`classify_backfill_row`] owns that classification; the count is
//!    recorded on the catalog row at the `ACTIVE` flip. Transient errors still
//!    propagate and abort the batch pre-commit.
//! 5. **Failure leaves the index `CREATING`.** There is no failure state on
//!    the wire, and flipping to `ACTIVE` would publish a partially populated
//!    index. A build that dies is repaired by rebuilding: drop the data table,
//!    recreate it, backfill, flip ([`rebuild_index`]). Rebuilding rather than
//!    resuming, because rows already written would collide with the backfill's
//!    deliberately plain INSERT.
//! 6. **Build ownership is backend-defined.** Recovery must not rebuild an
//!    index whose build is still making progress. A single-process backend can
//!    prove liveness with an in-process registry; a multi-process backend
//!    needs cross-process ownership (an advisory lock and a heartbeat column
//!    renewed via [`VectorIndexBuild::heartbeat`]). The candidate-selection
//!    policy therefore stays in the backend; the repair it triggers is the
//!    shared [`rebuild_index`].
//!
//! # What stays in the backend
//!
//! SQL and transactions (the primitives behind [`VectorIndexBuild`]), the
//! backfill cursor type (SQLite scans by `rowid`; a backend without rowids
//! uses keyset pagination over the full primary key), the queue hold's claim
//! predicate, and the stuck-build detection policy. The write-path maintenance
//! entry point also stays per backend, built on the shared helpers here
//! ([`item_is_indexable`], [`item_partition`], [`projected_payload`],
//! [`VectorApplyContext`]) so the row shape cannot drift between a live write
//! and a backfill.

mod backfill;
mod build;
mod meta;
mod partition;
mod payload;

pub use backfill::{
    BACKFILL_BATCH, BackfillOutcome, BackfillRow, BatchOutcome, classify_backfill_row,
};
pub use build::{VectorIndexBuild, complete_build, rebuild_index, run_backfill};
pub use meta::{VectorApplyContext, VectorIndexMeta, item_is_indexable, item_partition};
pub use partition::{UNSCOPED_PARTITION, partition_value};
pub use payload::projected_payload;
