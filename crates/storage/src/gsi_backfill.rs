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

use std::collections::HashMap;
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
/// is also the one answering DescribeTable for it. A count rather than a flag,
/// because two builds of one index can overlap (a recovery pass beside the
/// build that `UpdateTable` started) and the first to finish must not clear the
/// other's entry. Clones share the set.
#[derive(Clone, Debug, Default)]
pub struct GsiBackfills(Arc<Mutex<HashMap<String, usize>>>);

impl GsiBackfills {
    /// Record that a backfill of `index_id` has started. The record is removed
    /// when the returned guard drops, which a build does once it has published
    /// the index or given up on it.
    #[must_use]
    pub fn start(&self, index_id: &str) -> BackfillGuard {
        *self.lock().entry(index_id.to_owned()).or_insert(0) += 1;
        BackfillGuard {
            set: self.clone(),
            index_id: index_id.to_owned(),
        }
    }

    /// Whether a backfill of `index_id` is running in this process.
    #[must_use]
    pub fn started(&self, index_id: &str) -> bool {
        self.lock().contains_key(index_id)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, usize>> {
        // A panic while the lock was held leaves a map of counts that is still
        // usable, so a poisoned lock is taken as it is.
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
        let mut running = self.set.lock();
        if let Some(n) = running.get_mut(&self.index_id) {
            *n -= 1;
            if *n == 0 {
                running.remove(&self.index_id);
            }
        }
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
    fn a_record_lasts_as_long_as_its_guard() {
        let running = GsiBackfills::default();
        assert!(!running.started("i1"));
        let guard = running.start("i1");
        assert!(running.clone().started("i1"));
        assert!(!running.started("i2"));
        drop(guard);
        assert!(!running.started("i1"));
    }

    #[test]
    fn overlapping_builds_of_one_index_keep_it_started_until_both_end() {
        let running = GsiBackfills::default();
        let first = running.start("i1");
        let second = running.start("i1");
        drop(first);
        assert!(running.started("i1"));
        drop(second);
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
