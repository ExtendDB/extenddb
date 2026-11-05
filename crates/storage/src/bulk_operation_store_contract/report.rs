// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Paged report reads and the report totals that round-trip through checkpoint.

use super::*;

/// Report records read back in (`file_index`, `row_index`) ascending order,
/// whatever order their checkpoints inserted them in; the error report file
/// is rendered in this order.
pub async fn report_records_read_back_in_file_and_row_order<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "report_order").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    let out_of_order = [(1_i64, 2_i64), (0, 9), (0, 1)];
    let records = out_of_order
        .iter()
        .map(|(file, row)| ReportRecord {
            kind: ReportKind::Error,
            file_index: *file,
            row_index: *row,
            record: json!({"file": file, "itemIndex": row}),
        })
        .collect();
    fx.engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            Progress {
                cursor_file_index: 1,
                cursor_row_index: 3,
                processed_item_count: 12,
                failed_item_count: 3,
                processed_input_bytes: 1200,
                report_record_count: 3,
                report_bytes: 300,
                usage: json!({}),
                records,
                finished_files: Vec::new(),
                abandoned_files: Vec::new(),
            },
        )
        .await
        .expect("checkpoint");

    let read = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    let positions: Vec<(i64, i64)> = read
        .iter()
        .map(|r| {
            (
                r["file"].as_i64().expect("file"),
                r["itemIndex"].as_i64().expect("itemIndex"),
            )
        })
        .collect();
    assert_eq!(
        positions,
        vec![(0, 1), (0, 9), (1, 2)],
        "read-back is (file_index, row_index) ascending"
    );
}

/// Insert seven error records across two files through two checkpoints, out
/// of order, plus one condition record that must stay out of the error pages.
async fn seed_paged_records<E: StorageEngine>(fx: &Fixture<E>, table_stem: &str) -> String {
    let owner = "owner-1";
    let table = create_table(fx, &fx.account_a, table_stem).await;
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    let error_at = |file: i64, row: i64| ReportRecord {
        kind: ReportKind::Error,
        file_index: file,
        row_index: row,
        record: json!({"file": file, "itemIndex": row}),
    };
    let first = vec![error_at(1, 4), error_at(0, 9), error_at(0, 1)];
    let second = vec![
        error_at(1, 2),
        error_at(0, 5),
        error_at(1, 11),
        error_at(0, 3),
        ReportRecord {
            kind: ReportKind::ConditionCheckFailure,
            file_index: 0,
            row_index: 2,
            record: json!({"file": 0, "itemIndex": 2}),
        },
    ];
    for records in [first, second] {
        fx.engine
            .checkpoint(
                &fx.account_a,
                &id,
                owner,
                Duration::from_secs(60),
                progress(records),
            )
            .await
            .expect("checkpoint");
    }
    id
}

/// The seeded error records in `(file_index, row_index)` ascending order.
const PAGED_POSITIONS: [(i64, i64); 7] = [(0, 1), (0, 3), (0, 5), (0, 9), (1, 2), (1, 4), (1, 11)];

/// Paging with a limit smaller than the record count returns every record of
/// the requested kind exactly once, in `(file_index, row_index)` order, with
/// no row repeated or skipped across page boundaries.
pub async fn paged_report_reads_return_every_record_exactly_once<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let id = seed_paged_records(fx, "report_page_walk").await;

    let mut positions = Vec::new();
    let mut after = None;
    loop {
        let page = fx
            .engine
            .report_records_page(&fx.account_a, &id, ReportKind::Error, after, 3)
            .await
            .expect("report_records_page");
        assert!(page.len() <= 3, "a page must not exceed its limit");
        let Some(last) = page.last() else {
            break;
        };
        after = Some((last.file_index, last.row_index));
        for record in &page {
            assert!(
                matches!(record.kind, ReportKind::Error),
                "a page must hold only the requested kind"
            );
            assert_eq!(
                record.record["itemIndex"],
                json!(record.row_index),
                "the body rides with its own key"
            );
            positions.push((record.file_index, record.row_index));
        }
        if page.len() < 3 {
            break;
        }
    }
    assert_eq!(
        positions,
        PAGED_POSITIONS.to_vec(),
        "every error record appears exactly once, in (file_index, row_index) order"
    );

    // The same operation id under another account pages nothing: the records
    // above exist, so a page query missing the account filter would leak them.
    let foreign = fx
        .engine
        .report_records_page(&fx.account_b, &id, ReportKind::Error, None, 10)
        .await
        .expect("report_records_page");
    assert!(foreign.is_empty(), "paged reads must not cross accounts");
}

/// A page requested strictly after the last record is empty.
pub async fn a_report_page_after_the_last_record_is_empty<E: StorageEngine>(fx: &Fixture<E>) {
    let id = seed_paged_records(fx, "report_page_end").await;

    let last = *PAGED_POSITIONS.last().expect("seeded positions");
    let page = fx
        .engine
        .report_records_page(&fx.account_a, &id, ReportKind::Error, Some(last), 3)
        .await
        .expect("report_records_page");
    assert!(
        page.is_empty(),
        "after the last record there is nothing left to page"
    );
}

/// A page limit of zero is refused as a validation error; an empty page
/// would read as the end of the report.
pub async fn a_report_page_with_limit_zero_is_refused_as_validation<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let id = seed_paged_records(fx, "report_page_zero").await;

    let result = fx
        .engine
        .report_records_page(&fx.account_a, &id, ReportKind::Error, None, 0)
        .await;
    assert!(
        matches!(result, Err(StorageError::Validation(_))),
        "limit 0 must be StorageError::Validation"
    );
}

/// A report record whose body carries a NUL escape in a string value must
/// round-trip through the checkpoint and the paged read. DynamoDB string
/// values may contain U+0000, condition records embed the submitted item
/// verbatim, and the checkpoint carrying the record is the runner's only
/// path forward: a store that refuses the record body refuses every retry
/// of the same checkpoint, so the operation can never reach a terminal
/// state and its table guard never releases.
pub async fn a_report_record_with_a_nul_escape_round_trips_through_checkpoint<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "report_nul").await;
    let id = started_and_claimed(fx, &table, "owner-nul", 61_000).await;

    let body = json!({
        "itemLocation": {"inputSourceArn": "arn:aws:s3:::in-bucket/data/one.json", "itemIndex": 0},
        "errorMessages": ["The conditional request failed"],
        "newImage": {"Item": {"pk": {"S": "a\u{0}b"}}}
    });
    let record = ReportRecord {
        kind: ReportKind::Error,
        file_index: 0,
        row_index: 0,
        record: body.clone(),
    };
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "owner-nul",
            Duration::from_secs(60),
            progress(vec![record]),
        )
        .await
        .expect("a checkpoint carrying a NUL escape in a record body must commit");
    assert!(matches!(state, LeaseState::Held));

    let bodies = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert_eq!(bodies.len(), 1, "the record must read back");
    assert_eq!(
        bodies[0]["newImage"]["Item"]["pk"]["S"],
        json!("a\u{0}b"),
        "the stored body must carry the NUL byte unchanged"
    );
}

/// The report totals are zero on a fresh record and each checkpoint stores
/// the pair absolutely, like the counters; the runner owns accumulation.
pub async fn report_totals_round_trip_through_checkpoint_and_start_at_zero<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "report_totals").await;
    let owner = "owner-1";
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    assert_eq!(record.report_record_count, 0, "fresh records start at zero");
    assert_eq!(record.report_bytes, 0, "fresh records start at zero");
    let id = record.operation_id;
    let claimed = fx
        .engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(claimed.iter().any(|r| r.operation_id == id));

    let totals_progress = |count: u64, bytes: u64| Progress {
        cursor_file_index: 0,
        cursor_row_index: 10,
        processed_item_count: 10,
        failed_item_count: 0,
        processed_input_bytes: 1000,
        report_record_count: count,
        report_bytes: bytes,
        usage: json!({}),
        records: Vec::new(),
        finished_files: Vec::new(),
        abandoned_files: Vec::new(),
    };
    fx.engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            totals_progress(7, 977),
        )
        .await
        .expect("checkpoint");
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.report_record_count, 7);
    assert_eq!(row.report_bytes, 977);

    fx.engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            totals_progress(9, 1200),
        )
        .await
        .expect("checkpoint");
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(
        (row.report_record_count, row.report_bytes),
        (9, 1200),
        "the stored totals are the last checkpoint's values, not a sum"
    );
}
