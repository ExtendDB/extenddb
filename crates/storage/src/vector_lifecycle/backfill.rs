// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row classification and batch accounting for the vector index backfill.

use std::fmt;

use extenddb_core::types::Item;
use extenddb_core::validation::vector_components;

use super::meta::{VectorIndexMeta, item_is_indexable};

/// Rows scanned per backfill batch, shared by the create path and recovery.
pub const BACKFILL_BATCH: i64 = 500;

/// One batch's outcome: rows indexed, rows skipped as poison, rows fetched
/// (for termination), and the cursor for the next batch.
///
/// The cursor type is backend-owned: SQLite scans by `rowid`; a backend whose
/// base tables have no rowid pages by keyset over the full primary key.
/// `cursor` is `None` when the batch fetched no rows.
#[derive(Debug)]
pub struct BatchOutcome<C> {
    pub written: usize,
    pub skipped: usize,
    pub fetched: i64,
    pub cursor: Option<C>,
}

/// A completed backfill: rows indexed and rows skipped as poison. `skipped`
/// is recorded on the catalog row so an ACTIVE index that deliberately omits
/// rows says so, rather than the omission being indistinguishable from a bug.
#[derive(Debug)]
pub struct BackfillOutcome {
    pub written: usize,
    pub skipped: usize,
}

/// What a backfill does with one scanned base-table row.
#[derive(Debug)]
pub enum BackfillRow {
    /// Parsed, indexable, and its stored vector is well formed: write the row.
    Index(Item),
    /// Not indexable (no vector attribute, or no HASH attribute when the index
    /// declares one): omitted without counting, exactly as a GSI omits an item
    /// missing its index key.
    Omit,
    /// Poison: the stored bytes cannot enter the index. Skipped and counted.
    Poison,
}

/// Classify one scanned base-table row for the backfill.
///
/// Poison classification. The live write path treats a malformed vector
/// as an invariant violation and errors loudly, because core validation
/// ran before storage was reached. That reasoning is FALSE here: rows
/// written before the index existed never passed vector validation, so
/// a malformed or wrong-dimension vector in the base table is expected
/// input for a backfill, not a bug. Propagating it wedged the build in
/// an infinite recovery loop: the error left the index CREATING, the
/// watchdog re-ran the rebuild, and the same row failed again, forever,
/// while the CREATING hold also froze every queued index write for the
/// table. A row whose stored bytes cannot enter the index is skipped
/// and counted instead, exactly as a GSI omits an item whose key
/// attribute has the wrong type. Transient failures (the row INSERT itself
/// erroring) still propagate from the backend's batch and must not drop rows;
/// classification never sees them.
///
/// `cursor` names the row in the skip warnings, in whatever form the backend's
/// scan uses.
pub fn classify_backfill_row(
    item_json: &str,
    meta: &VectorIndexMeta,
    cursor: &dyn fmt::Display,
) -> BackfillRow {
    let Ok(item) = serde_json::from_str::<Item>(item_json) else {
        tracing::warn!(
            cursor = %cursor,
            index = %meta.index_id,
            "backfill: stored item is unparseable; skipping row"
        );
        return BackfillRow::Poison;
    };
    if !item_is_indexable(&item, meta) {
        return BackfillRow::Omit;
    }
    let vector_ok = item
        .get(&meta.vector_attribute_name)
        .and_then(vector_components)
        .is_some_and(|c| c.len() == meta.dimensions);
    if !vector_ok {
        tracing::warn!(
            cursor = %cursor,
            index = %meta.index_id,
            "backfill: vector attribute malformed or wrong dimension; skipping row"
        );
        return BackfillRow::Poison;
    }
    BackfillRow::Index(item)
}
