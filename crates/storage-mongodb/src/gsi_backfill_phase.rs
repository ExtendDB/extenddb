// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `backfilling` field of a GSI catalog document, which DescribeTable
//! reports as `Backfilling` and which decides a Query or Scan refusal for a
//! `CREATING` GSI (see `extenddb_storage::gsi_backfill`).
//!
//! An index `UpdateTable` adds is inserted without the field, which reads as
//! `false`. The backfill worker sets it to `true` before it scans the first
//! batch, and the `ACTIVE` flip removes it. A job resumed after a restart
//! finds it already set. A restore builds its indexes as part of restoring
//! the table and never sets it.

use std::time::Duration;

use bson::{Document, doc};
use extenddb_storage::error::StorageError;

use crate::MongoEngine;

/// Record that the backfill of a `CREATING` index has started, after the
/// table's `gsi_creating_phase_delay_ms` test hold, and then wait the hold
/// again. The worker runs one job at a time, so a hold also delays the jobs
/// queued behind this one.
///
/// Returns `false` when the index is no longer `CREATING` (deleted, or
/// published by another process), in which case the job has nothing to do.
pub(crate) async fn mark_started(
    storage: &MongoEngine,
    index_id: &str,
    table_name: &str,
) -> Result<bool, StorageError> {
    let hold = creating_phase_delay(storage, table_name).await;
    tokio::time::sleep(hold).await;
    let result = storage
        .catalog_db
        .collection::<Document>("indexes")
        .update_one(
            doc! { "index_id": index_id, "index_status": "CREATING" },
            doc! { "$set": { "backfilling": true } },
        )
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    if result.matched_count == 0 {
        return Ok(false);
    }
    tokio::time::sleep(hold).await;
    Ok(true)
}

/// The `Backfilling` member for a GSI catalog document.
pub(crate) fn member(index_doc: &Document, index_status: &str) -> Option<bool> {
    if index_doc.get_str("backfill_mode") == Ok("restore") {
        return None;
    }
    extenddb_storage::gsi_backfill::backfilling_member(index_status, started(index_doc))
}

/// Whether the worker has recorded the start of this index's backfill.
pub(crate) fn started(index_doc: &Document) -> bool {
    index_doc.get_bool("backfilling").unwrap_or(false)
}

/// The `gsi_creating_phase_delay_ms` test hold for one table; zero when unset.
async fn creating_phase_delay(storage: &MongoEngine, table_name: &str) -> Duration {
    let key = extenddb_core::settings_keys::gsi_creating_phase_delay_key(table_name);
    let setting = storage
        .catalog_db
        .collection::<Document>("settings")
        .find_one(doc! { "_id": key })
        .await
        .ok()
        .flatten();
    extenddb_storage::gsi_backfill::phase_delay(
        setting.as_ref().and_then(|d| d.get_str("value").ok()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_added_index_reports_false_then_true_then_nothing() {
        let inserted = doc! { "index_status": "CREATING" };
        assert_eq!(member(&inserted, "CREATING"), Some(false));
        let started = doc! { "index_status": "CREATING", "backfilling": true };
        assert_eq!(member(&started, "CREATING"), Some(true));
        let active = doc! { "index_status": "ACTIVE" };
        assert_eq!(member(&active, "ACTIVE"), None);
    }

    #[test]
    fn a_restored_index_reports_no_member() {
        let restoring = doc! { "index_status": "CREATING", "backfill_mode": "restore" };
        assert_eq!(member(&restoring, "CREATING"), None);
    }
}
