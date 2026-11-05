// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint persistence and integrity: progress, counters, usage, records, and
//! finished-file entries.

use super::*;

/// A checkpoint from the lease owner persists the cursor, the counters, the
/// usage, and the report records (filtered by kind on read), and renews the
/// lease.
pub async fn checkpoint_persists_progress_counters_usage_and_records<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "checkpoint_persist").await;
    let owner = "owner-1";
    // A short first lease, then a long renewal: the renewal is what keeps the
    // second owner out past the first lease's expiry.
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    let claimed = fx
        .engine
        .claim_runnable(owner, Duration::from_millis(500), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|r| r.operation_id == record.operation_id)
    );
    let id = record.operation_id;

    let condition_record = ReportRecord {
        kind: ReportKind::ConditionCheckFailure,
        file_index: 0,
        row_index: 7,
        record: json!({"itemIndex": 7, "error": {"ErrorCode": "ConditionalCheckFailed"}}),
    };
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(vec![error_record(3), condition_record]),
        )
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Held));

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.cursor_file_index, 0);
    assert_eq!(row.cursor_row_index, 25);
    assert_eq!(row.processed_item_count, 25);
    assert_eq!(row.failed_item_count, 1);
    assert_eq!(row.processed_input_bytes, 2500);
    assert_eq!(row.usage, json!({"WriteRequestUnits": 25600}));
    assert_eq!(
        row.status, "IN_PROGRESS",
        "a checkpoint does not end the run"
    );

    let errors = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["itemIndex"], json!(3));
    let failures =
        all_report_records(fx, &fx.account_a, &id, ReportKind::ConditionCheckFailure).await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["itemIndex"], json!(7));

    // Past the original 500 ms lease, the renewal from the checkpoint still
    // holds the row.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let blocked = fx
        .engine
        .claim_runnable("owner-2", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        !blocked.iter().any(|r| r.operation_id == id),
        "the checkpoint must have renewed the lease"
    );
}

/// A checkpoint from a non-owner reports Lost and leaves no trace: the
/// cursor, the counters, and the report records move together or not at all.
pub async fn a_checkpoint_from_a_non_owner_is_lost_and_leaves_no_trace<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "checkpoint_lost").await;
    let id = started_and_claimed(fx, &table, "owner-1", 1000).await;

    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "impostor",
            Duration::from_secs(60),
            progress(vec![error_record(3)]),
        )
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Lost));

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.cursor_row_index, 0, "the cursor must not have moved");
    assert_eq!(row.processed_item_count, 0);
    assert_eq!(row.failed_item_count, 0);
    assert_eq!(
        row.lease_owner.as_deref(),
        Some("owner-1"),
        "the lease is untouched"
    );
    let records = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert!(
        records.is_empty(),
        "the refused checkpoint's records must have rolled back with it"
    );
}

/// A checkpoint that re-inserts an existing (kind, `file_index`, `row_index`)
/// key succeeds and the first record wins. A record is durable exactly when
/// the checkpoint covering its row is, so a duplicate key can only be a
/// replayed row whose record already committed; keeping the first record is
/// the second line of defence, the same as the ledger.
pub async fn a_checkpoint_reinserting_an_existing_record_key_keeps_the_first_record<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "checkpoint_duplicate").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    let record_at = |row_index: i64, marker: &str| ReportRecord {
        kind: ReportKind::Error,
        file_index: 0,
        row_index,
        record: json!({"itemIndex": row_index, "marker": marker}),
    };
    fx.engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(vec![record_at(3, "first")]),
        )
        .await
        .expect("the first checkpoint");

    // A replayed row re-produces its record with whatever the retry computed;
    // the re-insert must neither fail the checkpoint nor replace the record.
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(vec![record_at(3, "second"), record_at(5, "fresh")]),
        )
        .await
        .expect("a checkpoint replaying an existing record key must succeed");
    assert!(matches!(state, LeaseState::Held));

    let records = all_report_records(fx, &fx.account_a, &id, ReportKind::Error).await;
    assert_eq!(
        records.len(),
        2,
        "the duplicate must not add a third record"
    );
    assert_eq!(
        records[0]["marker"],
        json!("first"),
        "the first record wins at the replayed key"
    );
    assert_eq!(records[1]["itemIndex"], json!(5));
}

/// Checkpoint stores the usage value it is given, replacing the previous
/// one; the runner owns accumulation, so a merge here would double-count.
pub async fn checkpoint_usage_replaces_the_stored_value<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "usage_replace").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    for usage in [json!({"A": 1}), json!({"B": 2})] {
        fx.engine
            .checkpoint(
                &fx.account_a,
                &id,
                owner,
                Duration::from_secs(60),
                Progress {
                    cursor_file_index: 0,
                    cursor_row_index: 1,
                    processed_item_count: 1,
                    failed_item_count: 0,
                    processed_input_bytes: 100,
                    report_record_count: 0,
                    report_bytes: 0,
                    usage,
                    records: Vec::new(),
                    finished_files: Vec::new(),
                    abandoned_files: Vec::new(),
                },
            )
            .await
            .expect("checkpoint");
    }

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(
        row.usage,
        json!({"B": 2}),
        "the stored usage is the last checkpoint's value, with no key merged in"
    );
}

/// A checkpoint's `finished_files` land on the matching `input_files`
/// entries as `rows` and `etag`, read back by `get`; an entry the
/// checkpoint did not name stays without them; a finished file with no
/// tag records only its count; an index past the enumerated files is
/// refused and the whole checkpoint rolls back; a checkpoint from a
/// non-owner records nothing.
pub async fn checkpoint_records_each_finished_files_rows_and_etag<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "finished_files").await;
    let owner = "owner-1";
    let mut req = new_op(fx, &fx.account_a, &table, None, "fp-finished", 1000);
    req.input_files = vec![
        InputFile {
            path: "data/a.json".to_owned(),
            size: 100,
            rows: None,
            etag: None,
            abandon: None,
        },
        InputFile {
            path: "data/b.json".to_owned(),
            size: 200,
            rows: None,
            etag: None,
            abandon: None,
        },
        InputFile {
            path: "data/c.json".to_owned(),
            size: 300,
            rows: None,
            etag: None,
            abandon: None,
        },
    ];
    let record = created(start(fx, &fx.account_a, req, 50).await);
    let id = record.operation_id;
    fx.engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    let progress = |cursor_file: i64, finished_files: Vec<FinishedFile>| Progress {
        cursor_file_index: cursor_file,
        cursor_row_index: 0,
        processed_item_count: 0,
        failed_item_count: 0,
        processed_input_bytes: 0,
        report_record_count: 0,
        report_bytes: 0,
        usage: json!({}),
        records: Vec::new(),
        finished_files,
        abandoned_files: Vec::new(),
    };

    // File 0 finished with a tag; file 1 finished without one (its reader
    // did not reach the end through the hashing path).
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(
                2,
                vec![
                    FinishedFile {
                        index: 0,
                        rows: 40,
                        etag: Some("0123456789abcdef0123456789abcdef".to_owned()),
                    },
                    FinishedFile {
                        index: 1,
                        rows: 7,
                        etag: None,
                    },
                ],
            ),
        )
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Held));
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files.len(), 3);
    assert_eq!(row.input_files[0].path, "data/a.json");
    assert_eq!(row.input_files[0].size, 100);
    assert_eq!(row.input_files[0].rows, Some(40));
    assert_eq!(
        row.input_files[0].etag.as_deref(),
        Some("0123456789abcdef0123456789abcdef")
    );
    assert_eq!(row.input_files[1].rows, Some(7));
    assert_eq!(row.input_files[1].etag, None);
    assert_eq!(row.input_files[2].rows, None);
    assert_eq!(row.input_files[2].etag, None);
    assert_eq!(row.cursor_file_index, 2);

    // An index past the enumerated files is a runner defect: refused, and
    // the cursor move in the same checkpoint does not land either.
    let err = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(
                3,
                vec![FinishedFile {
                    index: 3,
                    rows: 1,
                    etag: None,
                }],
            ),
        )
        .await
        .expect_err("an index past the array must be refused");
    assert!(matches!(err, StorageError::Internal(_)), "got {err:?}");
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files.len(), 3, "no stray element was appended");
    assert_eq!(
        row.cursor_file_index, 2,
        "the refused checkpoint rolled back"
    );

    // A non-owner's checkpoint is lost and records nothing.
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "someone-else",
            Duration::from_secs(60),
            progress(
                3,
                vec![FinishedFile {
                    index: 2,
                    rows: 9,
                    etag: Some("ffffffffffffffffffffffffffffffff".to_owned()),
                }],
            ),
        )
        .await
        .expect("checkpoint call");
    assert!(matches!(state, LeaseState::Lost));
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files[2].rows, None);
    assert_eq!(row.input_files[2].etag, None);
}

/// A checkpoint's `abandoned_files` land on the matching `input_files`
/// entries as `abandon`, read back by `get` with the row and the kind, one
/// entry for each kind, beside a finished file in the same checkpoint; an
/// entry the checkpoint did not name stays without one; an index past the
/// enumerated files is refused and the whole checkpoint rolls back; a
/// checkpoint from a non-owner records nothing.
pub async fn checkpoint_records_each_abandoned_files_outcome<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "abandoned_files").await;
    let owner = "owner-1";
    let mut req = new_op(fx, &fx.account_a, &table, None, "fp-abandoned", 1000);
    req.input_files = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|name| InputFile {
            path: format!("data/{name}.json"),
            size: 100,
            ..InputFile::default()
        })
        .collect();
    let record = created(start(fx, &fx.account_a, req, 50).await);
    let id = record.operation_id;
    fx.engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    let progress = |cursor_file: i64, abandoned_files: Vec<AbandonedFile>| Progress {
        cursor_file_index: cursor_file,
        cursor_row_index: 0,
        processed_item_count: 0,
        failed_item_count: 0,
        processed_input_bytes: 0,
        report_record_count: 0,
        report_bytes: 0,
        usage: json!({}),
        records: Vec::new(),
        finished_files: Vec::new(),
        abandoned_files,
    };
    let abandoned = |index: usize, row: u64, kind: AbandonKind| AbandonedFile {
        index,
        row,
        kind,
        bytes: row * 10,
    };

    // Files 0 to 2 abandoned, one of each kind; file 3 finished.
    let mut both = progress(
        4,
        vec![
            abandoned(0, 7, AbandonKind::Content),
            abandoned(1, 0, AbandonKind::NotCompressed),
            abandoned(2, 12, AbandonKind::Overwritten),
        ],
    );
    both.finished_files = vec![FinishedFile {
        index: 3,
        rows: 40,
        etag: None,
    }];
    let state = fx
        .engine
        .checkpoint(&fx.account_a, &id, owner, Duration::from_secs(60), both)
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Held));
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files.len(), 5);
    let expected = [
        Some(FileAbandon {
            row: 7,
            kind: AbandonKind::Content,
            bytes: Some(70),
        }),
        Some(FileAbandon {
            row: 0,
            kind: AbandonKind::NotCompressed,
            bytes: Some(0),
        }),
        Some(FileAbandon {
            row: 12,
            kind: AbandonKind::Overwritten,
            bytes: Some(120),
        }),
        None,
        None,
    ];
    for (file, expected) in row.input_files.iter().zip(expected) {
        assert_eq!(file.abandon, expected, "{}", file.path);
        assert_eq!(file.size, 100, "{}", file.path);
    }
    assert_eq!(
        row.input_files[0].rows, None,
        "an abandoned file has no count"
    );
    assert_eq!(row.input_files[3].rows, Some(40));
    assert_eq!(row.cursor_file_index, 4);

    // An index past the enumerated files is a runner defect: refused, and
    // the cursor move in the same checkpoint does not land either.
    let err = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(5, vec![abandoned(5, 1, AbandonKind::Content)]),
        )
        .await
        .expect_err("an index past the array must be refused");
    assert!(matches!(err, StorageError::Internal(_)), "got {err:?}");
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files.len(), 5, "no stray element was appended");
    assert_eq!(
        row.cursor_file_index, 4,
        "the refused checkpoint rolled back"
    );

    // A non-owner's checkpoint is lost and records nothing.
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "someone-else",
            Duration::from_secs(60),
            progress(5, vec![abandoned(4, 3, AbandonKind::Overwritten)]),
        )
        .await
        .expect("checkpoint call");
    assert!(matches!(state, LeaseState::Lost));
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files[4].abandon, None);
}
