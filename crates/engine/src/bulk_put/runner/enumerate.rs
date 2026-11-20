// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The input steps of an operation's first claim, before any row is read:
//! the caller's `s3:ListBucket`, the input bucket and its owner, the output
//! encryption key, and the listing. The claim records the listed files and
//! their total (`BulkOperationStore::record_input_files`); a later claim
//! reads the recorded files and lists nothing.
//!
//! The service answers the Start of such an operation with HTTP 200
//! `IN_PROGRESS` and fails it afterwards: a missing input bucket ends it
//! `FAILED` `S3NoSuchBucket` with no byte member and no output object
//! (measured on the service). Every failure here ends the operation the
//! same way: nothing is recorded, so the description has no byte member,
//! and `finalize` writes no output object for a record with no files.

use super::*;

/// The documented total input size of one operation for each
/// `PerformanceTier`: 100 GB for `STANDARD` and 1 TB for `ACCELERATED`, in
/// the binary units the service documents (1 GB is 2^30 bytes).
const STANDARD_INPUT_BYTES: u64 = 100 << 30;
const ACCELERATED_INPUT_BYTES: u64 = 1 << 40;

/// The total input quota of an operation in `tier` (`None` is `STANDARD`):
/// the documented quota, held at or below the server's `ceiling`
/// (`max_bulk_input_bytes`).
pub(super) fn input_bytes_quota(tier: Option<&str>, ceiling: u64) -> u64 {
    let documented = if tier == Some("ACCELERATED") {
        ACCELERATED_INPUT_BYTES
    } else {
        STANDARD_INPUT_BYTES
    };
    documented.min(ceiling)
}

/// What the input steps ended with.
pub(super) enum Listed {
    /// The files are recorded; the record carries them and their total.
    Recorded(BulkOperationRecord),
    /// The run ends with the failure or the cancel set on the state.
    Finish,
    /// The lease is lost; the runner stops without another write.
    Stop,
}

/// Run the input steps for a record whose files are not recorded, keeping
/// the lease while the store answers. A cancel or a deleted table seen by a
/// renewal ends the steps as at a boundary.
pub(super) async fn list_input(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    st: &mut RunState,
) -> Result<Listed, RunnerError> {
    let mut deadline = st.lease_deadline;
    let checked = keeping_lease_in_place(
        deps,
        record,
        &mut deadline,
        Watch::CancelAndTable,
        checked_listing(deps, record, config),
    )
    .await;
    st.lease_deadline = deadline;
    let (files, total) = match checked {
        InPlace::Answer(Ok(listed)) => listed,
        InPlace::Answer(Err(failure)) => {
            tracing::info!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                failure_code = %failure.code,
                "bulk operation input check failed; no file is recorded"
            );
            st.failure = Some(failure);
            return Ok(Listed::Finish);
        }
        InPlace::Lost => return Ok(Listed::Stop),
        InPlace::Cancelled => {
            st.cancelled = true;
            return Ok(Listed::Finish);
        }
        InPlace::TableGone => {
            st.failure = Some(Failure::table_not_found());
            return Ok(Listed::Finish);
        }
        InPlace::Interrupted(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "lease renewal failed during the input listing; the runner stops"
            );
            return Ok(Listed::Stop);
        }
    };
    let renewed_at = Instant::now();
    let state = deps
        .storage
        .record_input_files(
            &record.account_id,
            &record.operation_id,
            &deps.owner,
            deps.config.lease,
            files.clone(),
            total,
        )
        .await
        .map_err(RunnerError::Storage)?;
    match state {
        LeaseState::Lost => Ok(Listed::Stop),
        LeaseState::Held => {
            st.lease_deadline = Some(renewed_at + deps.config.lease);
            Ok(Listed::Recorded(BulkOperationRecord {
                input_files: files,
                total_input_bytes: Some(total),
                ..record.clone()
            }))
        }
    }
}

/// The four checks in order, then the two quotas on the listing: the
/// object count (the listing stops one object past it) and the total. An
/// `Err` is the failure the operation ends with.
async fn checked_listing(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
) -> Result<(Vec<InputFile>, i64), Failure> {
    let account = &record.account_id;
    if let Some(failure) = decision_failure(list_bucket_decision(deps, record, config).await) {
        return Err(failure);
    }
    let input_at = config.input_at(account);
    // The store decides the owner, then the existence, and renders the
    // fault (`BulkSource::check_bucket`).
    if let Err(error) = deps
        .source
        .check_bucket(&input_at, &config.input_prefix)
        .await
    {
        return Err(listing_failure(record, &error));
    }
    // An SSE-KMS output key the store cannot use fails the operation the
    // same way, before the listing and with both byte members absent
    // (measured on the service). The key checked is report object 0 of the
    // error report; a bucket or owner fault of the output is left to the
    // first report record.
    if let Some(bucket) = config.output_bucket.as_deref() {
        let key = report::report_key(
            &config.output_prefix,
            account,
            &record.operation_id,
            ReportKind::Error,
            0,
        );
        match deps
            .sink
            .check_encryption(&config.output_at(account, bucket), &key, &config.output_sse)
            .await
        {
            Ok(()) => {}
            Err(SinkError::Store(fault)) => return Err(Failure::from_store(&fault)),
            Err(SinkError::Io(io)) if StoreUnavailable::of_io(&io) => {
                return Err(Failure::store_unavailable());
            }
            Err(error) => {
                tracing::error!(
                    account_id = %account,
                    operation_id = %record.operation_id,
                    error = ?error,
                    "bulk operation output encryption check failed"
                );
                return Err(Failure::internal());
            }
        }
    }
    let max_files = deps.limits.max_bulk_input_files;
    let count_failure = || Failure::objects_count_limit_exceeded(&config.input_prefix, max_files);
    let listed = match deps
        .source
        .list(&input_at, &config.input_prefix, max_files)
        .await
    {
        Ok(listed) => listed,
        // The filesystem walk's visit ceiling: more entries under the
        // prefix than four times the object quota.
        Err(SourceError::TooManyFiles) => return Err(count_failure()),
        Err(error) => return Err(listing_failure(record, &error)),
    };
    if u64::try_from(listed.len()).unwrap_or(u64::MAX) > max_files {
        return Err(count_failure());
    }
    let total = listed
        .iter()
        .fold(0_u64, |sum, file| sum.saturating_add(file.size));
    let quota = input_bytes_quota(
        record.performance_tier.as_deref(),
        deps.limits.max_bulk_input_bytes,
    );
    if total > quota {
        return Err(Failure::size_limit_exceeded(quota));
    }
    let files = listed
        .into_iter()
        .map(|file| InputFile {
            path: file.path,
            size: i64::try_from(file.size).unwrap_or(i64::MAX),
            rows: None,
            etag: file.etag,
            abandon: None,
        })
        .collect();
    Ok((files, i64::try_from(total).unwrap_or(i64::MAX)))
}

/// The failure for a bucket check or a listing that did not answer: a fault
/// the store rendered keeps its code and message (a bucket removed or
/// access lost between the check and a later page included); anything else
/// is logged and mapped as a read fault is (`source_failure`).
fn listing_failure(record: &BulkOperationRecord, error: &SourceError) -> Failure {
    if !matches!(error, SourceError::Store(_)) {
        tracing::error!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            error = ?error,
            "bulk operation input enumeration failed"
        );
    }
    source_failure(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_quota_is_the_documented_one_for_the_tier_held_under_the_ceiling() {
        const GIB: u64 = 1 << 30;
        for (tier, ceiling, quota) in [
            (None, u64::MAX, 100 * GIB),
            (Some("STANDARD"), u64::MAX, 100 * GIB),
            (Some("ACCELERATED"), u64::MAX, 1024 * GIB),
            (Some("ACCELERATED"), 1024 * GIB, 1024 * GIB),
            (Some("STANDARD"), 10 * GIB, 10 * GIB),
            (Some("ACCELERATED"), 10, 10),
        ] {
            assert_eq!(
                input_bytes_quota(tier, ceiling),
                quota,
                "{tier:?} under {ceiling}"
            );
        }
    }
}
