// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A stored operation that does not decode: the claim ends it and returns
//! the other rows, and the listing leaves it out and pages past it.
//!
//! No trait method can write such a row, so each backend's test file hands
//! these cases a [`RowEditor`] that edits the stored row with SQL.

use futures::future::BoxFuture;

use crate::bulk_operation_store::{UNREADABLE_FAILURE_CODE, UNREADABLE_FAILURE_MESSAGE};

use super::*;

/// Edits stored rows directly, for the cases that need a row the trait
/// cannot write.
pub trait RowEditor: Send + Sync {
    /// Sets the `abandon` member of the operation's first `input_files`
    /// element to one of a kind no build knows, so the row no longer
    /// decodes.
    fn corrupt<'a>(&'a self, account_id: &'a str, operation_id: &'a str) -> BoxFuture<'a, ()>;

    /// Removes that `abandon` member again, so the row decodes and its
    /// stored status can be read through the trait.
    fn repair<'a>(&'a self, account_id: &'a str, operation_id: &'a str) -> BoxFuture<'a, ()>;

    /// Sets the stored status of the table with this id to `DELETING`,
    /// the status `DeleteTable` leaves while the deletion is scheduled.
    fn mark_table_deleting<'a>(
        &'a self,
        account_id: &'a str,
        table_id: &'a str,
    ) -> BoxFuture<'a, ()>;
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Starts one operation on a new table of its own, so several can be
/// runnable at once.
async fn started<E: StorageEngine>(fx: &Fixture<E>, table: &str, start_time_ms: i64) -> String {
    let table = create_table(fx, &fx.account_a, table).await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", start_time_ms),
            50,
        )
        .await,
    )
    .operation_id
}

/// A runnable row that does not decode, older than a good one: the claim
/// returns the good one with its lease and ends the bad one `FAILED` with
/// the internal failure pair and no lease, and a second claim returns
/// neither.
pub async fn a_claim_ends_an_unreadable_row_and_returns_the_rest<E: StorageEngine>(
    fx: &Fixture<E>,
    editor: &dyn RowEditor,
) {
    let bad = started(fx, "unreadable_claim_bad", 1000).await;
    let good = started(fx, "unreadable_claim_good", 2000).await;
    editor.corrupt(&fx.account_a, &bad).await;

    let before = now_ms();
    let claimed = fx
        .engine
        .claim_runnable("claimer", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("a claim over an unreadable row still succeeds");
    let ids: Vec<&str> = claimed.iter().map(|r| r.operation_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![good.as_str()],
        "the claim returns the good row and only it"
    );
    let good_row = get(fx, &fx.account_a, &good)
        .await
        .expect("the good row exists");
    assert_eq!(good_row.lease_owner.as_deref(), Some("claimer"));
    assert!(
        good_row.lease_until_ms.is_some(),
        "the good row keeps its lease"
    );

    match fx.engine.get_bulk_operation(&fx.account_a, &bad).await {
        Err(StorageError::Internal(_)) => {}
        other => panic!("the corrupt row still refuses the read, got {other:?}"),
    }
    editor.repair(&fx.account_a, &bad).await;
    let bad_row = get(fx, &fx.account_a, &bad)
        .await
        .expect("the bad row exists");
    assert_eq!(bad_row.status, "FAILED");
    assert_eq!(
        bad_row.failure_code.as_deref(),
        Some(UNREADABLE_FAILURE_CODE)
    );
    assert_eq!(
        bad_row.failure_message.as_deref(),
        Some(UNREADABLE_FAILURE_MESSAGE)
    );
    assert!(
        bad_row.end_time_ms.is_some_and(|t| t >= before),
        "the claim stamps end_time_ms with its own time, got {:?}",
        bad_row.end_time_ms
    );
    assert_eq!(bad_row.lease_owner, None, "the ended row holds no lease");
    assert_eq!(bad_row.lease_until_ms, None, "the ended row holds no lease");

    let again = fx
        .engine
        .claim_runnable("other", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("second claim");
    assert!(
        again.is_empty(),
        "the ended row is not runnable and the good row is leased, got {:?}",
        again.iter().map(|r| &r.operation_id).collect::<Vec<_>>()
    );
}

/// A row that does not decode between two good ones: a listing returns the
/// good ones and leaves the bad one out, and a page that ends on the bad row
/// carries a cursor past it.
pub async fn a_listing_skips_an_unreadable_row_and_pages_past_it<E: StorageEngine>(
    fx: &Fixture<E>,
    editor: &dyn RowEditor,
) {
    let old = started(fx, "unreadable_list_old", 1000).await;
    let bad = started(fx, "unreadable_list_bad", 2000).await;
    let new = started(fx, "unreadable_list_new", 3000).await;
    editor.corrupt(&fx.account_a, &bad).await;

    let ids = |records: &[crate::BulkOperationRecord]| -> Vec<String> {
        records.iter().map(|r| r.operation_id.clone()).collect()
    };

    let (records, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 10, None)
        .await
        .expect("a listing over an unreadable row still succeeds");
    assert_eq!(ids(&records), vec![new.clone(), old.clone()]);
    assert!(cursor.is_none(), "one page holds every row");

    // A page of two covers the new row and the bad one.
    let (first, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 2, None)
        .await
        .expect("first page");
    assert_eq!(ids(&first), vec![new.clone()]);
    let cursor = cursor.expect("a row is left after the page");
    assert_eq!(
        cursor,
        ListCursor {
            start_time_ms: 2000,
            operation_id: bad.clone(),
        },
        "the cursor names the last row the page covered, the bad one"
    );
    let (second, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 2, Some(cursor))
        .await
        .expect("second page");
    assert_eq!(ids(&second), vec![old]);
    assert!(cursor.is_none(), "nothing is left after the old row");
}

/// A cancelled runnable row that does not decode ends as `finish` ends a
/// `CANCELLING` row: `CANCELLED` with the cancelled failure pair, no lease.
/// A good row beside it is still returned.
pub async fn a_claim_ends_an_unreadable_cancelled_row_cancelled<E: StorageEngine>(
    fx: &Fixture<E>,
    editor: &dyn RowEditor,
) {
    let bad = started(fx, "unreadable_cancel_bad", 1000).await;
    let good = started(fx, "unreadable_cancel_good", 2000).await;
    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, &bad)
        .await
        .expect("cancel");
    assert!(
        matches!(cancel, CancelOutcome::Cancelling),
        "the running row accepts the cancel"
    );
    editor.corrupt(&fx.account_a, &bad).await;

    let before = now_ms();
    let claimed = fx
        .engine
        .claim_runnable("claimer", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("a claim over an unreadable row still succeeds");
    let ids: Vec<&str> = claimed.iter().map(|r| r.operation_id.as_str()).collect();
    assert_eq!(ids, vec![good.as_str()], "the claim returns the good row");

    editor.repair(&fx.account_a, &bad).await;
    let bad_row = get(fx, &fx.account_a, &bad)
        .await
        .expect("the bad row exists");
    assert_eq!(bad_row.status, "CANCELLED");
    assert_eq!(
        bad_row.failure_code.as_deref(),
        Some(CANCELLED_FAILURE_CODE)
    );
    assert_eq!(
        bad_row.failure_message.as_deref(),
        Some(CANCELLED_FAILURE_MESSAGE)
    );
    assert!(
        bad_row.end_time_ms.is_some_and(|t| t >= before),
        "the claim stamps end_time_ms with its own time, got {:?}",
        bad_row.end_time_ms
    );
    assert!(bad_row.cancel_requested, "the cancel request is kept");
    assert_eq!(bad_row.lease_owner, None, "the ended row holds no lease");
    assert_eq!(bad_row.lease_until_ms, None, "the ended row holds no lease");
}

/// A row with checkpointed report records whose lease ran out, then made
/// unreadable: the claim that ends it deletes its report records in the
/// same transaction, as `finish` does.
pub async fn a_claim_ending_an_unreadable_row_deletes_its_report_records<E: StorageEngine>(
    fx: &Fixture<E>,
    editor: &dyn RowEditor,
) {
    let id = started(fx, "unreadable_records", 1000).await;
    let lease = Duration::from_millis(50);
    let claimed = fx
        .engine
        .claim_runnable("owner-1", lease, 10, &[], &[])
        .await
        .expect("first claim");
    assert!(claimed.iter().any(|r| r.operation_id == id));
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "owner-1",
            lease,
            progress(vec![error_record(1), error_record(2)]),
        )
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Held), "{state:?}");
    let stored = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert_eq!(stored.len(), 2, "the checkpoint stored both records");

    // Long past the 50 ms lease, so the next claim selects the row again.
    tokio::time::sleep(Duration::from_millis(200)).await;
    editor.corrupt(&fx.account_a, &id).await;
    let claimed = fx
        .engine
        .claim_runnable("owner-2", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("a claim over an unreadable row still succeeds");
    assert!(claimed.is_empty(), "the unreadable row is not returned");

    let left = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert!(
        left.is_empty(),
        "the ending deletes the row's report records, {} left",
        left.len()
    );
    editor.repair(&fx.account_a, &id).await;
    let row = get(fx, &fx.account_a, &id).await.expect("the row exists");
    assert_eq!(row.status, "FAILED");
}
