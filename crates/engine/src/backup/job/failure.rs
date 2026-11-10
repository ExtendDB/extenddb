// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The conditional status writes of a backup row and the job's failure
//! path.
//!
//! Every status write goes through [`write_status`]: the backend writes
//! conditioned on the claim's generation and on the record being in the
//! status the job holds it in (`from`: CREATING for a backup row, RUNNING
//! for a restore record), and this module gives the three outcomes one
//! meaning. `Matched` is the write. `NotMatched` means the record is no
//! longer this claim's (gone, another generation, or not in `from`) and the
//! caller makes no further store or table access of any kind. `Unknown`
//! (the connection failed after the statement was sent) is followed by a
//! re-read of the record, not treated as a failure: a record that shows the
//! write landed is `Matched`; one still in `from` under this generation did
//! not take the write, which is sent once more, at most; anything else is
//! `NotMatched`. Each unknown
//! outcome has its re-read, the second write's included, so a second
//! unknown outcome whose write landed is `Matched` (the row is in the
//! target status under this generation) and one whose write was lost is
//! `NotMatched`. An error on the claim connection, on the write or the
//! re-read, ends the caller's work the way `NotMatched` does.
//!
//! The second write is for a backend whose claim connection survives the
//! unknown outcome (MongoDB and Cassandra, whose claim writes go through
//! the shared client). On PostgreSQL an unknown outcome is an error after
//! which the claim session's one connection is dead, so the second write
//! fails with the session and its re-read settles it as `NotMatched`: the
//! job stops, the session's end frees the advisory lock, and the row is the
//! reconciler's (AVAILABLE from a complete manifest, FAILED otherwise).
//!
//! The backup job's failure path ([`fail_row`]) is the FAILED write
//! followed, only when it matched, by the deletion fence and then
//! `delete_prefix` of the backup's prefix; the holder of the claim owns the
//! prefix, so a job whose FAILED write did not match never deletes, and a
//! root another catalog adopted while the job ran is left as it is (the
//! fence marks the store unhealthy). A deletion that fails leaves the
//! prefix to the reconciler, which deletes a FAILED row's prefix once its
//! heartbeat is stale. The restore job's failure path, FAILED then the
//! target's removal under the same gate, is `super::super::restore::fail_restore`.

use extenddb_backup_store::BackupStore;

use super::super::StoreFence;
use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::JobClaim;
use extenddb_storage::backup_catalog::{JobStatus, StatusFields, WriteOutcome};

/// What a conditional status write settled to (module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Written {
    /// The row was moved.
    Matched,
    /// The row is not this claim's to write; the text says what the row
    /// showed.
    NotMatched(String),
    /// The claim connection failed on the write or the re-read.
    Error(String),
}

/// Move the record from `from` to `to` through `claim` (module doc).
pub(crate) async fn write_status(
    claim: &dyn JobClaim,
    from: JobStatus,
    to: JobStatus,
    fields: &StatusFields,
) -> Written {
    for attempt in 0..2 {
        let outcome = match claim.set_status(from, to, fields).await {
            Ok(outcome) => outcome,
            Err(e) => return Written::Error(format!("the {to} write failed: {e}")),
        };
        match outcome {
            WriteOutcome::Matched => return Written::Matched,
            WriteOutcome::NotMatched => {
                return Written::NotMatched(format!(
                    "the record is no longer {from} under this claim's generation"
                ));
            }
            WriteOutcome::Unknown => {}
        }
        let row = match claim.read().await {
            Ok(row) => row,
            Err(e) => {
                return Written::Error(format!(
                    "the {to} write's outcome is unknown and the row could not be re-read: {e}"
                ));
            }
        };
        if row.owner_generation != claim.generation() {
            return Written::NotMatched(format!(
                "the row carries generation {} (this claim's is {})",
                row.owner_generation,
                claim.generation()
            ));
        }
        if row.status == to {
            return Written::Matched;
        }
        if row.status != from || attempt == 1 {
            return Written::NotMatched(format!(
                "the record is {} after a {to} write whose outcome was unknown",
                row.status
            ));
        }
        tracing::warn!(
            "the {to} write's outcome was unknown and the record is still {from} under this \
             claim; writing once more"
        );
    }
    unreachable!("the loop returns on its second attempt")
}

/// The end of the failure path.
#[derive(Debug)]
pub(crate) enum FailurePath {
    /// FAILED written and the prefix deleted.
    Deleted,
    /// FAILED written; the deletion failed and is the reconciler's.
    DeleteFailed(String),
    /// FAILED written; the deletion fence refused, and nothing was
    /// deleted.
    Fenced(String),
    /// FAILED not written by this claim (the row is another's, or the claim
    /// connection failed): no store access was made.
    NoStoreAccess(String),
}

/// The failure path (module doc): FAILED with `reason`, then, when the
/// write matched, the fence for `catalog_id` and the deletion of `prefix`.
pub(crate) async fn fail_row(
    claim: &dyn JobClaim,
    store: &dyn BackupStore,
    fence: &StoreFence,
    catalog_id: &str,
    prefix: &str,
    reason: &str,
    metrics: Option<&MetricsCollector>,
) -> FailurePath {
    let fields = StatusFields {
        failure_reason: Some(reason.to_owned()),
        completed: true,
        ..StatusFields::default()
    };
    match write_status(claim, JobStatus::Creating, JobStatus::Failed, &fields).await {
        Written::Matched => {}
        Written::NotMatched(why) | Written::Error(why) => return FailurePath::NoStoreAccess(why),
    }
    if let Err(problem) = fence.check(catalog_id).await {
        return FailurePath::Fenced(problem.to_string());
    }
    match store.delete_prefix(prefix).await {
        Ok(_) => FailurePath::Deleted,
        Err(e) => {
            if let Some(metrics) = metrics {
                metrics.record_backup_store_error("delete_prefix");
            }
            FailurePath::DeleteFailed(e.to_string())
        }
    }
}
