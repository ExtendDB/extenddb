// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane side of a store-backed restore: how a backend takes the
//! items of one verified data file into a CREATING target, records how far
//! it got, flips the target ACTIVE, and counts what it holds.
//!
//! The engine (`crates/engine/src/backup/restore.rs`) owns the job: it
//! downloads and verifies every file, decodes and validates the items, and
//! hands them to [`RestoreEngine::bulk_load`] one file at a time. The
//! backend owns the writes: the base rows, the index rows and the vector
//! rows in its own batches and transactions, the progress record its
//! resume reads, and the ACTIVE flip conditioned on the claim. Progress is
//! recorded inside `bulk_load`, never by a separate call, because the
//! backends record it at different points (PostgreSQL per file in the data
//! database, SQLite, MongoDB and Cassandra per batch) and only the
//! write that applied the items knows when they are durable.

use extenddb_core::types::Item;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::backup_catalog::{JobClaim, RestoreTargetKey};
use crate::error::StorageError;

/// Items per write batch of the SQLite and Cassandra `bulk_load` (SQLite's
/// write transaction, Cassandra's run of upserts before a progress write):
/// the count cap of the batch term of a restore's memory (the design's
/// section 7), and the unit other writers wait for on SQLite.
pub const RESTORE_BATCH_ITEMS: usize = 10_000;

/// Bytes of serialized item text per write batch of the SQLite and Cassandra
/// `bulk_load`: a batch closes at the first item that takes its `item_data`
/// text past this, so the batch term of a restore's memory is bounded in
/// bytes as well as in items (10,000 items of the 400 KB limit would be
/// 3.9 GiB). Measured on the JSON text each backend writes as the row's
/// `item_data`, not on the service's `item_size_bytes`.
pub const RESTORE_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Which data file a [`RestoreEngine::bulk_load`] call carries, and where in
/// it the items start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkLoadFile {
    /// The file's index in the manifest's `data_files`.
    pub file_index: u32,
    /// Items of this file already applied by an earlier run, which the
    /// engine has skipped: the stream holds the rest. The backend records
    /// `start_at` plus the items it applied as the file's running total.
    pub start_at: u64,
    /// The file is the one a resumed job may have applied in part without a
    /// record of it (a backend that records progress per file and died
    /// after some of the file's writes). A backend whose writes are plain
    /// inserts may suppress duplicate-key failures for this file only;
    /// elsewhere a duplicate key is the backup's and fails the file.
    pub reapplying: bool,
}

/// What [`RestoreEngine::finish_restore`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finished {
    /// The record is DONE and the table and every index are ACTIVE.
    Active,
    /// The record is DONE and the table waits for its index backfill; the
    /// backend's worker flips it ACTIVE once every index has left CREATING
    /// (MongoDB).
    Pending,
    /// The record was not RUNNING under the claim's generation, or the
    /// table was not a CREATING restore target: nothing was written. Carries
    /// the backend's account of what it found.
    NotMatched(String),
}

/// The data-plane operations of a store-backed restore, implemented by each
/// backend beside [`crate::backup_catalog::BackupCatalog`].
pub trait RestoreEngine: Send + Sync {
    /// Write one verified data file's items, from `file.start_at` on, into
    /// the CREATING target: the base rows and the rows of every secondary
    /// and vector index the target has, in the backend's own batches, and
    /// the file's progress as the backend records it (the design's 5.3 step
    /// 5), under `claim`'s generation where the backend's record carries it.
    /// Returns the items it applied from the stream.
    ///
    /// A duplicate primary key, within the file or against rows an earlier
    /// file wrote, fails the file (the backup holds a key twice), except as
    /// [`BulkLoadFile::reapplying`] says. An `Err` item of the stream (a
    /// decode or validation failure the engine found) fails the file with
    /// that error, and nothing of a failed file is recorded as applied where
    /// the backend writes the file in one transaction.
    fn bulk_load<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        file: BulkLoadFile,
        items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>>;

    /// The end of a restore (the design's 5.3 step 6): the record RUNNING to
    /// DONE and the table and every index CREATING to ACTIVE with
    /// `item_count` and `size_bytes`, in one write where the backend can,
    /// conditional on the claim's generation and on the record RUNNING and
    /// the table CREATING ([`Finished::Active`]); or DONE with the ACTIVE
    /// flip left to the backend's index backfill ([`Finished::Pending`]).
    /// [`Finished::NotMatched`] when the record or the table was not the
    /// claim's. A backend that keeps progress outside the record removes it
    /// after the flip.
    fn finish_restore<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>>;

    /// The items the target's base table holds: the count check every
    /// restore ends with (equal to the manifest's total, else the backup
    /// holds a key twice), and the emptiness check a resume with no recorded
    /// progress makes.
    fn count_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>>;

    /// Whether the target's data tables exist as the registration leaves
    /// them: the reconciler's check before it resumes a dead owner's record.
    /// Every backend commits the record before the target's data DDL
    /// (PostgreSQL and SQLite create the data table after the catalog
    /// transaction, MongoDB the collections and the index metadata after the
    /// insert's commit, Cassandra the data table after its registration
    /// LWT), so an owner killed in between leaves a RUNNING record over a
    /// target with no table, or on MongoDB with its index metadata missing;
    /// `false` names that record, which the reconciler fails and removes
    /// instead of resuming. `Err` when `key` is not a CREATING restore
    /// target with this table id, or the read failed.
    fn restore_target_ready<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// The progress rows a backend keeps outside its job record, as
    /// `(file_index, items_applied)` pairs: PostgreSQL's `restore_progress`
    /// rows in the data database. Empty by default: the other backends
    /// carry the progress on the record ([`crate::backup_catalog::RestoreJobRow`]).
    fn read_restore_progress<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Vec<(u32, u64)>, StorageError>> {
        let _ = key;
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Remove progress rows of targets not among `running` (the table ids
    /// of RUNNING restore targets, read from the catalog): the reconciler's
    /// sweep of rows a crash left behind where the progress lives apart
    /// from the record. The candidates are a positive list: the backend
    /// reads the distinct table ids the progress rows name first, then the
    /// catalog's RUNNING records, and deletes only the rows of ids in the
    /// first set that are in neither the second nor `running`; a target
    /// whose first row lands after the first read is never a candidate, and
    /// one registered before it is RUNNING at the second read, so a row of
    /// a live job is never deleted. Returns the rows removed; `0` by
    /// default.
    fn sweep_restore_progress<'a>(
        &'a self,
        running: &'a [String],
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        let _ = running;
        Box::pin(async { Ok(0) })
    }
}
