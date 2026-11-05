// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the Start handler recorded on the operation for its runner, in the
//! record's run context, checked once when a claim starts.

use super::*;

use crate::bulk_put::run_context::RecordedRunContext;

/// The recorded run context, when it names the store this runner reads and
/// writes through. Otherwise the detail for the log line: a record started
/// on another store, or a run context that does not parse.
pub(super) fn check_recorded_store(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
) -> Result<RecordedRunContext, String> {
    let recorded = RecordedRunContext::of(record)?;
    if recorded.store != deps.store_kind {
        return Err(format!(
            "the operation was started on the {} store",
            recorded.store
        ));
    }
    Ok(recorded)
}
