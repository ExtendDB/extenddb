// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The conditional status writes of a backup row and the job's failure
//! path.
//!
//! Every status write goes through [`write_status`]: the backend writes
//! conditioned on the claim's generation and on the row being CREATING, and
//! this module gives the three outcomes one meaning. `Matched` is the
//! write. `NotMatched` means the row is no longer this claim's (gone,
//! another generation, or not CREATING) and the caller makes no further
//! store or table access of any kind. `Unknown` (the connection failed
//! after the statement was sent) is followed by a re-read of the row, not
//! treated as a failure: a row that shows the write landed is `Matched`; a
//! row still CREATING under this generation did not take the write, which
//! is sent once more, at most; anything else is `NotMatched`. Each unknown
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
//! The failure path ([`fail_row`]) is the FAILED write followed, only when
//! it matched, by `delete_prefix` of the backup's prefix; the holder of the
//! claim owns the prefix, so a job whose FAILED write did not match never
//! deletes. A deletion that fails leaves the prefix to the reconciler,
//! which deletes a FAILED row's prefix once its heartbeat is stale.

use extenddb_backup_store::BackupStore;
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

/// Move the row from CREATING to `to` through `claim` (module doc).
pub(crate) async fn write_status(
    claim: &dyn JobClaim,
    to: JobStatus,
    fields: &StatusFields,
) -> Written {
    for attempt in 0..2 {
        let outcome = match claim.set_status(JobStatus::Creating, to, fields).await {
            Ok(outcome) => outcome,
            Err(e) => return Written::Error(format!("the {to} write failed: {e}")),
        };
        match outcome {
            WriteOutcome::Matched => return Written::Matched,
            WriteOutcome::NotMatched => {
                return Written::NotMatched(
                    "the row is no longer CREATING under this claim's generation".to_owned(),
                );
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
        if row.status != JobStatus::Creating || attempt == 1 {
            return Written::NotMatched(format!(
                "the row is {} after a {to} write whose outcome was unknown",
                row.status
            ));
        }
        tracing::warn!(
            "the {to} write's outcome was unknown and the row is still CREATING under this \
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
    /// FAILED not written by this claim (the row is another's, or the claim
    /// connection failed): no store access was made.
    NoStoreAccess(String),
}

/// The failure path (module doc): FAILED with `reason`, then the deletion
/// of `prefix` when the write matched.
pub(crate) async fn fail_row(
    claim: &dyn JobClaim,
    store: &dyn BackupStore,
    prefix: &str,
    reason: &str,
    metrics: Option<&MetricsCollector>,
) -> FailurePath {
    let fields = StatusFields {
        failure_reason: Some(reason.to_owned()),
        completed: true,
        ..StatusFields::default()
    };
    match write_status(claim, JobStatus::Failed, &fields).await {
        Written::Matched => {}
        Written::NotMatched(why) | Written::Error(why) => return FailurePath::NoStoreAccess(why),
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
