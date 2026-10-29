// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The two `CREATING` phases of a GSI that `UpdateTable` adds.
//!
//! Measured against the service on 2026-10-26 (us-east-1) with a GSI added to
//! a 3000-item table, which became `ACTIVE` after 512 s and 519 s on two runs:
//!
//! | `IndexStatus` | `Backfilling` | Query or Scan naming the index |
//! |---|---|---|
//! | `CREATING` | `false` | `The table does not have the specified index: <name>` |
//! | `CREATING` | `true` | `Cannot read from backfilling global secondary index: <name>` |
//! | `ACTIVE` | absent | served |
//!
//! Both refusals are `ValidationException`. The first phase lasted about 30 s.
//!
//! Each backend decides from its own build state whether the backfill has
//! started and passes that one answer to [`backfilling_member`] for
//! DescribeTable and to [`creating_read_refusal`] for Query and Scan, so the
//! description and the refusal cannot disagree.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use extenddb_core::types::VECTOR_INDEX_BACKFILLING_SCAN_PREFIX;

use crate::error::StorageError;

/// The `Backfilling` member of a GSI whose status is `index_status`: present
/// only while the index is `CREATING`, and then `true` once its backfill has
/// started.
#[must_use]
pub fn backfilling_member(index_status: &str, backfill_started: bool) -> Option<bool> {
    (index_status == "CREATING").then_some(backfill_started)
}

/// The refusal for a Query or Scan naming a `CREATING` GSI.
///
/// Before the backfill starts it is `IndexNotFound`, which the engine reports
/// as "The table does not have the specified index", the same answer as for a
/// name that matches no index. After, it is the backfilling sentence, which the
/// engine passes through verbatim as a `ValidationException`.
#[must_use]
pub fn creating_read_refusal(index_name: &str, backfill_started: bool) -> StorageError {
    if backfill_started {
        StorageError::Validation(format!(
            "{VECTOR_INDEX_BACKFILLING_SCAN_PREFIX}{index_name}"
        ))
    } else {
        StorageError::IndexNotFound(index_name.to_owned())
    }
}

/// The hold a `gsi_creating_phase_delay_ms:<table>` setting asks for, from its
/// stored value. Absent or unreadable means no hold.
#[must_use]
pub fn phase_delay(value: Option<&str>) -> Duration {
    Duration::from_millis(
        value
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0),
    )
}

/// GSI builds in this process whose backfill has started, by index id.
///
/// For a backend whose build runs inside the server process and records no
/// progress in its catalog (PostgreSQL and SQLite). One process serves a
/// catalog, which is the supported deployment, so the process running a build
/// is also the one answering DescribeTable for it.
///
/// An id stays in the set until its index leaves `CREATING`: a build removes it
/// once it has published the index, found it deleted, or removed it after a
/// failure that would recur. A build that fails for a passing reason leaves it,
/// so the index keeps reporting `Backfilling: true` and the backfilling refusal
/// while it waits for the retry, which records it again; the service is not
/// known to go back to the first phase. Index ids are not reused, so an id left
/// behind by an index that another path deleted is never read again. Clones
/// share the set.
#[derive(Clone, Debug, Default)]
pub struct GsiBackfills(Arc<Mutex<HashSet<String>>>);

impl GsiBackfills {
    /// Record that a backfill of `index_id` has started. The record stays
    /// until [`Self::finish`] removes it.
    pub fn start(&self, index_id: &str) {
        self.lock().insert(index_id.to_owned());
    }

    /// Remove the record of `index_id`, once its index has left `CREATING`.
    pub fn finish(&self, index_id: &str) {
        self.lock().remove(index_id);
    }

    /// [`Self::start`], and [`Self::finish`] when the returned guard drops, for
    /// a build that deletes its index in the same call whenever it fails.
    #[must_use]
    pub fn start_scoped(&self, index_id: &str) -> BackfillGuard {
        self.start(index_id);
        BackfillGuard {
            set: self.clone(),
            index_id: index_id.to_owned(),
        }
    }

    /// Whether a backfill of `index_id` has started in this process and its
    /// index is still `CREATING`.
    #[must_use]
    pub fn started(&self, index_id: &str) -> bool {
        self.lock().contains(index_id)
    }

    /// The ids recorded now, for a reader to take before it reads the catalog
    /// rows it pairs them with.
    ///
    /// A build removes an id only after the index's row has left `CREATING`,
    /// so a set read first can be paired only with the same row or a newer
    /// one. A `CREATING` row is then reported with a set that still holds its
    /// id, and the answer never goes back to the first phase. Read the other
    /// way round, a call could read the row as `CREATING` just before the flip
    /// and the set just after the id was removed, and report the first phase
    /// for an index whose backfill had finished.
    #[must_use]
    pub fn snapshot(&self) -> HashSet<String> {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, HashSet<String>> {
        // A panic while the lock was held leaves a set that is still usable, so
        // a poisoned lock is taken as it is.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Removes one backfill record from [`GsiBackfills`] when dropped.
#[derive(Debug)]
pub struct BackfillGuard {
    set: GsiBackfills,
    index_id: String,
}

impl Drop for BackfillGuard {
    fn drop(&mut self) {
        self.set.finish(&self.index_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_member_is_present_only_while_creating() {
        assert_eq!(backfilling_member("CREATING", false), Some(false));
        assert_eq!(backfilling_member("CREATING", true), Some(true));
        assert_eq!(backfilling_member("ACTIVE", true), None);
        assert_eq!(backfilling_member("ACTIVE", false), None);
        assert_eq!(backfilling_member("DELETING", true), None);
    }

    #[test]
    fn the_refusal_follows_the_phase_byte_for_byte() {
        match creating_read_refusal("gsi_slow", false) {
            StorageError::IndexNotFound(name) => assert_eq!(name, "gsi_slow"),
            other => panic!("first phase: {other:?}"),
        }
        match creating_read_refusal("gsi_slow", true) {
            StorageError::Validation(msg) => assert_eq!(
                msg,
                "Cannot read from backfilling global secondary index: gsi_slow"
            ),
            other => panic!("second phase: {other:?}"),
        }
    }

    #[test]
    fn a_record_lasts_until_finish() {
        let running = GsiBackfills::default();
        assert!(!running.started("i1"));
        running.start("i1");
        assert!(running.clone().started("i1"));
        assert!(!running.started("i2"));
        // A second build of the same index (the retry after a passing failure)
        // records it again; one finish ends it, because finish is called only
        // once the index has left CREATING.
        running.start("i1");
        running.finish("i1");
        assert!(!running.started("i1"));
    }

    #[test]
    fn a_scoped_record_lasts_as_long_as_its_guard() {
        let running = GsiBackfills::default();
        let guard = running.start_scoped("i1");
        assert!(running.started("i1"));
        drop(guard);
        assert!(!running.started("i1"));
    }

    #[test]
    fn a_snapshot_keeps_the_ids_recorded_when_it_was_taken() {
        let running = GsiBackfills::default();
        running.start("i1");
        let before = running.snapshot();
        running.finish("i1");
        running.start("i2");
        assert!(before.contains("i1"));
        assert!(!before.contains("i2"));
        assert!(!running.started("i1"));
    }

    #[test]
    fn an_absent_or_unreadable_delay_holds_nothing() {
        assert_eq!(phase_delay(None), Duration::ZERO);
        assert_eq!(phase_delay(Some("not a number")), Duration::ZERO);
        assert_eq!(phase_delay(Some("0")), Duration::ZERO);
        assert_eq!(phase_delay(Some(" 2500 ")), Duration::from_millis(2500));
    }
}
