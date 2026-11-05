// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Reads, account isolation, the operation list and its paging, and limit validation.

use super::*;

/// Another account cannot see or move an operation: get, list, cancel,
/// checkpoint, finish, and report reads all come back empty-handed, and the
/// row is untouched afterwards.
pub async fn an_operation_is_invisible_and_inert_to_another_account<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "isolation").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    assert!(
        get(fx, &fx.account_b, &id).await.is_none(),
        "get must not cross accounts"
    );

    let (rows, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_b, 100, None)
        .await
        .expect("list");
    assert!(
        rows.is_empty() && cursor.is_none(),
        "list must not cross accounts"
    );

    let cancel = fx
        .engine
        .request_cancel(&fx.account_b, &id)
        .await
        .expect("request_cancel");
    assert!(
        matches!(cancel, CancelOutcome::NotFound),
        "cancel must not cross accounts"
    );

    let checkpoint = fx
        .engine
        .checkpoint(
            &fx.account_b,
            &id,
            owner,
            Duration::from_secs(60),
            progress(vec![error_record(3)]),
        )
        .await
        .expect("checkpoint");
    assert!(
        matches!(checkpoint, LeaseState::Lost),
        "checkpoint must not cross accounts"
    );

    let finish = fx
        .engine
        .finish(&fx.account_b, &id, owner, terminal("COMPLETED", 9000))
        .await
        .expect("finish");
    assert!(
        matches!(finish, FinishState::Lost),
        "finish must not cross accounts"
    );

    let reports = all_report_records(fx, &fx.account_b, &id, ReportKind::Error).await;
    assert!(reports.is_empty(), "report reads must not cross accounts");

    let page = fx
        .engine
        .report_records_page(&fx.account_b, &id, ReportKind::Error, None, 10)
        .await
        .expect("report_records_page");
    assert!(page.is_empty(), "paged reads must not cross accounts");

    let row = get(fx, &fx.account_a, &id).await.expect("the row survives");
    assert_eq!(row.status, "IN_PROGRESS");
    assert!(!row.cancel_requested);
    assert_eq!(row.cursor_row_index, 0);
    assert_eq!(row.processed_item_count, 0);
}

/// A created record reads back with the fields it was stored with.
pub async fn get_returns_the_record_as_stored<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "fidelity").await;
    let req = new_op(fx, &fx.account_a, &table, Some("tok"), "fp", 1234);
    let req_id = req.operation_id.clone();
    let record = created(start(fx, &fx.account_a, req, 50).await);
    assert_eq!(record.operation_id, req_id);

    let row = get(fx, &fx.account_a, &req_id).await.expect("stored row");
    assert_eq!(row.account_id, fx.account_a);
    assert_eq!(row.table_id, table.table_id);
    assert_eq!(row.table_name, table.table_name);
    assert_eq!(
        row.table_arn,
        arn(&fx.region, &fx.account_a, &table.table_name)
    );
    assert_eq!(row.status, "IN_PROGRESS");
    assert_eq!(row.client_token.as_deref(), Some("tok"));
    assert_eq!(row.request_fingerprint, "fp");
    assert_eq!(
        row.input_config,
        json!({"S3Bucket": "in-bucket", "S3KeyPrefix": "data/"})
    );
    assert_eq!(
        row.output_config,
        json!({"S3Bucket": "out-bucket", "OutputType": ["ERROR"]})
    );
    assert_eq!(row.input_files.len(), 1);
    assert_eq!(row.input_files[0].path, "data/one.json");
    assert_eq!(row.input_files[0].size, 100);
    assert_eq!(row.max_write_request_units, Some(1000));
    assert_eq!(row.performance_tier.as_deref(), Some("STANDARD"));
    assert_eq!(row.start_time_ms, 1234);
    assert_eq!(row.end_time_ms, None);
    assert_eq!(row.processed_item_count, 0);
    assert_eq!(row.failed_item_count, 0);
    assert_eq!(row.processed_input_bytes, 0);
    assert_eq!(row.total_input_bytes, Some(100));
    assert_eq!(row.failure_code, None);
    assert_eq!(row.failure_message, None);
    assert!(!row.cancel_requested);
    assert_eq!(row.cursor_file_index, 0);
    assert_eq!(row.cursor_row_index, 0);
    assert_eq!(row.lease_owner, None);
    assert_eq!(row.lease_until_ms, None);

    assert!(
        get(fx, &fx.account_a, "00000000000000deadbeef")
            .await
            .is_none(),
        "an unknown id reads as None, not an error"
    );
}

/// Listing is newest first by (`start_time_ms`, `operation_id`), the cursor is
/// exclusive, and paging across a boundary neither skips nor repeats a row.
/// Two rows share a start time so the id tiebreak is exercised.
pub async fn list_pages_newest_first_with_an_exclusive_cursor<E: StorageEngine>(fx: &Fixture<E>) {
    let times: [i64; 5] = [1000, 2000, 3000, 3000, 4000];
    let mut ids = Vec::new();
    for (i, t) in times.iter().enumerate() {
        let table = create_table(fx, &fx.account_a, &format!("list_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", *t),
                50,
            )
            .await,
        );
        ids.push((record.start_time_ms, record.operation_id));
    }
    // The expected order: start_time_ms descending, operation_id descending
    // within a tie.
    let mut expected = ids.clone();
    expected.sort_by(|a, b| b.cmp(a));

    let mut seen = Vec::new();
    let mut cursor: Option<ListCursor> = None;
    let mut pages = 0;
    loop {
        let (page, next) = fx
            .engine
            .list_bulk_operations(&fx.account_a, 2, cursor.clone())
            .await
            .expect("list");
        assert!(page.len() <= 2, "the page must respect the limit");
        for r in &page {
            seen.push((r.start_time_ms, r.operation_id.clone()));
        }
        pages += 1;
        assert!(pages <= 5, "paging must terminate");
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(
        seen, expected,
        "full scan must be newest first with no gap or repeat"
    );

    // The cursor is exclusive: paging from the newest row's own position
    // returns everything after it and never the row itself.
    let after_first = ListCursor {
        start_time_ms: expected[0].0,
        operation_id: expected[0].1.clone(),
    };
    let (rest, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 100, Some(after_first))
        .await
        .expect("list after cursor");
    let rest_ids: Vec<_> = rest
        .iter()
        .map(|r| (r.start_time_ms, r.operation_id.clone()))
        .collect();
    assert_eq!(rest_ids, expected[1..].to_vec());

    // A cursor naming a position no row occupies still pages by order, it
    // does not error: rows strictly after the position come back.
    let synthetic = ListCursor {
        start_time_ms: 3000,
        operation_id: "z".repeat(22),
    };
    let (after_synthetic, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 100, Some(synthetic))
        .await
        .expect("list after a synthetic cursor");
    assert!(
        after_synthetic.iter().all(|r| r.start_time_ms < 3000
            || (r.start_time_ms == 3000 && r.operation_id.as_str() < "z")),
        "a synthetic cursor filters by order"
    );
}

/// An account whose row count is an exact multiple of the page size ends on
/// a full second page with no cursor; a live cursor never yields an empty
/// page.
pub async fn list_on_an_exact_page_multiple_ends_without_a_cursor<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    for i in 0..4 {
        let table = create_table(fx, &fx.account_a, &format!("page_multiple_{i}")).await;
        created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", 1000 + i),
                50,
            )
            .await,
        );
    }

    let (first, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 2, None)
        .await
        .expect("first page");
    assert_eq!(first.len(), 2);
    let cursor = cursor.expect("two rows remain, so the first page carries a cursor");

    let (second, end) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 2, Some(cursor))
        .await
        .expect("second page");
    assert_eq!(
        second.len(),
        2,
        "a live cursor must never resolve to an empty page"
    );
    assert!(
        end.is_none(),
        "the exact multiple ends on the second page with no cursor"
    );
}

/// A list limit of zero is refused as a validation error; an empty page
/// would read as an account with no operations.
pub async fn list_with_limit_zero_is_refused_as_validation<E: StorageEngine>(fx: &Fixture<E>) {
    let result = fx.engine.list_bulk_operations(&fx.account_a, 0, None).await;
    assert!(
        matches!(result, Err(StorageError::Validation(_))),
        "limit 0 must be StorageError::Validation"
    );
}
