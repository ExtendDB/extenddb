// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Writing the reports and the manifest at the end of a run, with the lease
//! kept while the output store is waited on (`in_place`). The run decided
//! its outcome at the final checkpoint, so only a lost lease ends a write
//! ([`Watch::LeaseOnly`]): a cancelled or failed run writes every report
//! and its manifest, and a cancel that arrives now is recorded by the
//! terminal transition.

use super::*;

/// How writing the outputs ended.
pub(super) enum Outputs {
    /// Go on to the terminal transition.
    Finish,
    /// The lease passed to another process; the runner stops with no
    /// further write.
    Stop,
    /// The lease could not be read or renewed; the runner stops without a
    /// terminal transition.
    Interrupted(StorageError),
    /// The output store was unavailable past its retries; the operation
    /// resumes and writes its outputs again (`finalize`).
    Unavailable,
}

/// The answer of a held call, or how the outputs end when its lease
/// renewal saw the lease lost or could not run.
fn answer_of<T>(held: InPlace<T>, record: &BulkOperationRecord) -> Result<T, Outputs> {
    match held {
        InPlace::Answer(answer) => Ok(answer),
        InPlace::Lost => {
            tracing::info!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                "lease lost while a report or the manifest was written; the runner stops"
            );
            Err(Outputs::Stop)
        }
        InPlace::Interrupted(error) => Err(Outputs::Interrupted(error)),
        // A `Watch::LeaseOnly` renewal reads neither the cancel flag nor
        // the table, so neither answer is produced for a write. Were one
        // produced, the write it ended is not done; leaving the operation
        // for the next claim, which writes the outputs again, loses none.
        InPlace::Cancelled | InPlace::TableGone => Err(Outputs::Interrupted(
            StorageError::Internal("an output write was ended by a cancel or a table check".into()),
        )),
    }
}

/// Render the reports, then check the manifest's location and write the
/// manifest, each call keeping the lease while it waits.
pub(super) async fn render_outputs(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    output_bucket: &str,
    st: &mut RunState,
    deadline: &mut Option<Instant>,
) -> Outputs {
    match write_outputs(deps, record, config, output_bucket, st, deadline).await {
        Ok(()) => Outputs::Finish,
        Err(outputs) => outputs,
    }
}

async fn write_outputs(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    output_bucket: &str,
    st: &mut RunState,
    deadline: &mut Option<Instant>,
) -> Result<(), Outputs> {
    let account = &record.account_id;
    let operation = &record.operation_id;
    let rendered = keeping_lease_in_place(
        deps,
        record,
        deadline,
        Watch::LeaseOnly,
        render_reports(deps, record, config, output_bucket),
    )
    .await;
    match answer_of(rendered, record)? {
        Ok(Render::Rendered) => {}
        Ok(Render::LeaseLost) => {
            tracing::info!(
                account_id = %account,
                operation_id = %operation,
                "lease lost during report rendering; the runner stops"
            );
            return Err(Outputs::Stop);
        }
        Ok(Render::Interrupted(error)) => return Err(Outputs::Interrupted(error)),
        Err(failure) if failure.is_store_unavailable() => return Err(Outputs::Unavailable),
        // An earlier failure (an input object gone mid-run, a cancel's
        // outcome) keeps precedence over a render failure: the first
        // fault the run met is the one reported. Measured only for the
        // bucket-removed case on its own; the combination is
        // ExtendDB's choice.
        Err(failure) => {
            if st.failure.is_none() {
                st.failure = Some(failure);
            }
        }
    }
    // The manifest follows the reports (measured on the service: it lands
    // after the report objects). It is written whenever the operation
    // read input, whatever the terminal status; a born-terminal
    // operation never reaches this function with a configuration. An
    // output location that the first report record never checked
    // (no failing rows) is checked here: a configured owner that is
    // not the caller, or a missing bucket, fails the operation with the
    // location fault's code and the measured "Manifest is not
    // available." prefix.
    // A bucket removed after report records were written reaches the
    // same shape: the hosted service finished every row and reported
    // the missing bucket from the manifest write,
    // so a render that found the bucket gone yields to this
    // check's form of the same fault.
    let render_lost_bucket = st
        .failure
        .as_ref()
        .is_some_and(|failure| failure.code == Failure::output_bucket_missing().code);
    let manifest_key = report::manifest_key(&config.output_prefix, account, operation);
    // The caller's s3:PutObject on the manifest comes first; a denial
    // takes the location fault's place in the measured form.
    let denied = put_object_failure(deps, record, config, output_bucket, &manifest_key).await;
    let location = match denied {
        Some(failure) => Err(failure),
        None => {
            let checked = keeping_lease_in_place(
                deps,
                record,
                deadline,
                Watch::LeaseOnly,
                deps.sink.check_location(
                    &config.output_at(account, output_bucket),
                    &manifest_key,
                    &config.output_sse,
                ),
            )
            .await;
            match answer_of(checked, record)? {
                Ok(()) => Ok(()),
                Err(SinkError::Store(fault)) => Err(Failure::from_store(&fault)),
                Err(error) => Err(render_sink_failure(account, operation, &error)),
            }
        }
    };
    match location {
        Err(fault) if fault.is_store_unavailable() => return Err(Outputs::Unavailable),
        Ok(()) => {
            let written = keeping_lease_in_place(
                deps,
                record,
                deadline,
                Watch::LeaseOnly,
                render_manifest(deps, record, config, output_bucket, &*st),
            )
            .await;
            match answer_of(written, record)? {
                Err(failure) if failure.is_store_unavailable() => {
                    return Err(Outputs::Unavailable);
                }
                Err(failure) if st.failure.is_none() => st.failure = Some(failure),
                _ => {}
            }
        }
        Err(fault) => {
            if st.failure.is_none() || render_lost_bucket {
                st.failure = Some(Failure::manifest_unavailable(fault));
            }
        }
    }
    Ok(())
}
