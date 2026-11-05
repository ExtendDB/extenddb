// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which target the bulk handlers file their ERROR lines under. An
//! operator filters the log by target, so a bulk handler fault filed under
//! another handler's target is hard to find.

use super::logs::Capture;
use super::*;

/// The target every bulk handler module logs under.
const HANDLERS_TARGET: &str = "extenddb_engine::bulk_put::handlers";

/// A Describe whose storage read fails internally answers
/// `InternalServerError`, and the ERROR line is filed under the bulk
/// handlers' target, not the table handlers' one.
#[tokio::test]
async fn a_describe_storage_fault_is_logged_under_the_bulk_handlers_target() {
    let h = Harness::new().await;
    let operation_id = raw_op(&h, ACCOUNT, 1, now_ms()).await;
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_get_on_call = Some(1);
    let ctx = ctx_with_storage(&h, Arc::new(faulting));

    let (capture, guard) = Capture::install();
    let result = describe(&ctx, &arn_of(ACCOUNT, &operation_id)).await;
    drop(guard);

    assert!(
        matches!(&result, Err(DynamoDbError::InternalServerError(m)) if m == "Internal server error"),
        "{result:?}"
    );
    let lines = capture.errors_containing("storage internal error");
    assert_eq!(
        lines.len(),
        1,
        "one ERROR line for the fault, got {lines:?}"
    );
    assert_eq!(lines[0].target, HANDLERS_TARGET, "{lines:?}");
}

/// An input enumeration that fails for a reason other than a store fault
/// is logged under the Start handler's target.
#[tokio::test]
async fn a_start_enumeration_failure_is_logged_under_the_start_target() {
    let h = Harness::new().await;
    let ctx = super::start_store_faults::ctx_with_listing(&h, None);

    let (capture, guard) = Capture::install();
    let result = start(&ctx, start_body()).await;
    drop(guard);

    assert!(
        matches!(&result, Err(DynamoDbError::InternalServerError(m)) if m == "Internal server error"),
        "{result:?}"
    );
    let lines = capture.errors_containing("bulk operation input enumeration failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(
        lines[0].target, "extenddb_engine::bulk_put::handlers::start",
        "{lines:?}"
    );
}

/// Each storage fault the table handlers log at ERROR, mapped by the bulk
/// handlers: the same answer and the same message as the table handlers
/// give, in one ERROR line filed under the bulk handlers' target.
#[test]
fn every_logged_storage_fault_is_logged_under_the_bulk_handlers_target() {
    use extenddb_storage::error::StorageError;
    type Fault = fn() -> StorageError;
    let faults: Vec<(&str, Fault)> = vec![("Connection", || {
        StorageError::Connection("connection refused".to_owned())
    })];
    for (name, fault) in faults {
        let (capture, guard) = Capture::install();
        let table_answer = crate::create_table::storage_err_to_dynamo(fault());
        let table_lines = capture.errors_containing("");
        drop(guard);

        let (capture, guard) = Capture::install();
        let bulk_answer = crate::bulk_put::handlers::bulk_storage_err(fault());
        let lines = capture.errors_containing("");
        drop(guard);

        assert_eq!(
            format!("{bulk_answer:?}"),
            format!("{table_answer:?}"),
            "{name}: the bulk handlers answer as the table handlers do"
        );
        assert_eq!(lines.len(), 1, "{name}: one ERROR line, got {lines:?}");
        assert_eq!(lines[0].target, HANDLERS_TARGET, "{name}: {lines:?}");
        assert_eq!(
            table_lines.len(),
            1,
            "{name}: the table handlers log one line"
        );
        assert_eq!(
            lines[0].message, table_lines[0].message,
            "{name}: the same message as the table handlers"
        );
    }
}
