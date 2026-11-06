// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The output location fault found at the operation's first report record,
//! held while the run reads on to the end of the input, then made the
//! operation's failure.
//!
//! Measured on the service for a missing output bucket, an output owner that
//! is not the caller and a report write the output bucket refuses: the
//! service reads past the row whose record found the fault, writes every
//! good row to the end of the input, then ends `FAILED` with the fault, with
//! `ProcessedItemCount` the rows written, `FailedItemCount` 0,
//! `ProcessedInputBytes` 0 and zero usage, and writes no report and no
//! manifest. A cancel, or another fault met before the end, ends the
//! operation instead, as it does with no fault held.
//!
//! The held fault is stored with every checkpoint, the final one included,
//! in the usage value under [`HELD_FAULT_MEMBER`], and a claim that resumes
//! restores it, so a crash anywhere after the hold ends the operation as the
//! run without the crash does.

use serde_json::json;

use super::*;

/// The member of the stored usage value that carries a held fault while the
/// operation runs: `{"FailureCode": ..., "FailureMessage": ...}`. Only the
/// checkpoints write it and only [`restore_held_fault`] reads it. Describe
/// renders usage through the named members of `bulk_put::usage` and only
/// once the operation is terminal, and the terminal transition stores the
/// usage without it, so no response carries it. A record with no such
/// member holds no fault.
pub(super) const HELD_FAULT_MEMBER: &str = "HeldOutputLocationFault";

/// Hold `fault`, found by the location check at the first report record.
/// The records waiting for a checkpoint are dropped and no later record is
/// kept (`RunState::push_record`), so the persisted record count stays
/// where it was. Nothing is rendered on any path out of the run.
pub(super) fn hold_location_fault(st: &mut RunState, fault: Failure) {
    let pending = u64::try_from(st.pending.len()).unwrap_or(u64::MAX);
    st.kept_records = st.kept_records.saturating_sub(pending);
    st.kept_bytes = st.kept_bytes.saturating_sub(st.pending_bytes);
    st.pending.clear();
    st.pending_bytes = 0;
    st.skip_render = true;
    st.held_location = Some(fault);
}

/// At the end of the input, make the held fault the operation's failure
/// with the counters the service reports for it. A run that ended on a
/// cancel or on another failure keeps that ending and its counters. The
/// fault stays held, so the final checkpoint stores it with those counters
/// and a claim that resumes after that checkpoint ends the same way.
pub(super) fn end_with_held_location_fault(st: &mut RunState) {
    let Some(fault) = st.held_location.clone() else {
        return;
    };
    if st.cancelled || st.failure.is_some() {
        return;
    }
    // Processed minus failed is the rows written plus the rows whose
    // condition failed, which ExtendDB counts as processed; the service's
    // count for a condition failure under this fault is not measured.
    st.processed = st.processed.saturating_sub(st.failed);
    st.failed = 0;
    st.completed_bytes = 0;
    st.current_bytes = 0;
    st.bytes_floor = 0;
    st.usage = UsageAccum::default();
    st.failure = Some(fault);
}

/// The usage value a checkpoint stores: the accumulated usage, and the held
/// fault under [`HELD_FAULT_MEMBER`] while one is held.
pub(super) fn persisted_usage(st: &RunState, table_arn: &str) -> Value {
    let mut usage = st.usage.to_value(table_arn);
    if let (Some(fault), Some(members)) = (&st.held_location, usage.as_object_mut()) {
        members.insert(
            HELD_FAULT_MEMBER.to_owned(),
            json!({ "FailureCode": fault.code, "FailureMessage": fault.message }),
        );
    }
    usage
}

/// `st`, restored from `record`, with the fault an earlier claim held: held
/// again, the output location taken as checked, and nothing rendered, as in
/// the claim that held it.
pub(super) fn restore_held_fault(record: &BulkOperationRecord, mut st: RunState) -> RunState {
    let held = record.usage.get(HELD_FAULT_MEMBER);
    let code = held.and_then(|fault| fault.get("FailureCode")?.as_str());
    let message = held.and_then(|fault| fault.get("FailureMessage")?.as_str());
    if let (Some(code), Some(message)) = (code, message) {
        st.held_location = Some(Failure {
            code: Cow::Owned(code.to_owned()),
            message: message.to_owned(),
        });
        st.output_checked = true;
        st.skip_render = true;
    }
    st
}
