// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Starts that create an already-terminal operation: a missing or
//! symlinked input bucket, an owner mismatch, the replay of a token that
//! named one, and the per-hour cap on terminal-born rows. The output
//! bucket checks that stay with the first report record are here too, and
//! the busy table that refuses such a start before anything is created.

use super::*;

#[tokio::test]
async fn a_missing_input_bucket_starts_the_operation_terminal_failed() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_input(start_body(), "S3Bucket", json!("absent"));
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
    assert_eq!(description["FailureMessage"], json!(NO_SUCH_BUCKET));
    assert_eq!(description["ProcessedItemCount"], json!(0));
    assert_eq!(description["FailedItemCount"], json!(0));
    // Born terminal before enumeration: the hosted service omits
    // TotalInputBytes here (measured on the service) while keeping the counters.
    assert!(
        description.get("TotalInputBytes").is_none(),
        "TotalInputBytes must be absent on a failure born before enumeration"
    );
    assert_eq!(
        description["EndTime"], description["StartTime"],
        "the operation ends the instant it starts"
    );
    // Started and finished both count the operation, the finish under the
    // FAILED status; no worker ever sees the row.
    let started = ctx.metrics.query(&extenddb_core::metrics::MetricsQuery {
        metric: Some(extenddb_core::metrics::MetricName::BulkOperationsStarted),
        window: Some(extenddb_core::metrics::TimeWindow::AllTime),
        ..extenddb_core::metrics::MetricsQuery::default()
    });
    assert_eq!(started.len(), 1, "one Started series: {started:?}");
    assert_eq!(started[0].count, 1, "started once");
    let finished = ctx.metrics.query(&extenddb_core::metrics::MetricsQuery {
        metric: Some(extenddb_core::metrics::MetricName::BulkOperationsFinished),
        window: Some(extenddb_core::metrics::TimeWindow::AllTime),
        ..extenddb_core::metrics::MetricsQuery::default()
    });
    assert_eq!(finished.len(), 1, "one Finished series: {finished:?}");
    assert_eq!(finished[0].count, 1, "finished once");
    assert!(
        finished[0]
            .dimensions
            .contains(&extenddb_core::metrics::Dimension::Operation(
                "FAILED".to_owned()
            )),
        "the Finished dimension carries FAILED: {:?}",
        finished[0].dimensions
    );
}

#[tokio::test]
async fn a_missing_report_bucket_is_not_checked_at_start() {
    // Measured: only the input bucket is checked at Start; a missing
    // output bucket with clean rows completes. A report bucket missing at
    // render time fails the render instead.
    let h = Harness::new().await;
    // The input bucket exists (the harness staged it); only the named
    // report bucket directory is absent.
    let body = with_output(start_body(), "S3Bucket", json!("absent-reports"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
    assert!(
        description.get("FailureCode").is_none(),
        "no failure is recorded for a missing report bucket"
    );
}

/// A symlink a tenant plants at a bucket name is a missing bucket on the
/// wire, wherever it points: the operation starts terminal `FAILED` with
/// `S3NoSuchBucket` and no row is read through the link. An existence
/// check that resolved the link would instead answer `IN_PROGRESS` and
/// enumerate the link's target.
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_input_bucket_starts_the_operation_terminal_failed() {
    let h = Harness::new().await;
    // The link's target is the account's own staged input bucket, so the
    // target genuinely exists, holds files, and would enumerate cleanly:
    // a FAILED verdict can only come from the link refusal.
    let target = h.store.0.join(ACCOUNT).join(IN_BUCKET);
    std::os::unix::fs::symlink(&target, h.store.0.join(ACCOUNT).join("linked")).unwrap();
    let body = with_input(start_body(), "S3Bucket", json!("linked"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
    assert_eq!(description["FailureMessage"], json!(NO_SUCH_BUCKET));
    assert!(
        description.get("TotalInputBytes").is_none(),
        "TotalInputBytes must be absent on a failure born before enumeration"
    );
}

#[tokio::test]
async fn a_replayed_token_returns_the_terminal_failed_operation() {
    let h = Harness::new().await;
    let body = with_input(
        with_top(start_body(), "ClientToken", json!("tok-failed-replay")),
        "S3Bucket",
        json!("absent"),
    );
    let first = start_ok(&base_ctx(&h), body.clone()).await;
    let second = start_ok(&base_ctx(&h), body).await;
    assert_eq!(second["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(second["BulkOperationStatus"], json!("FAILED"));
}

#[tokio::test]
async fn a_replayed_token_wins_over_a_bucket_removed_after_the_first_start() {
    // The idempotency path is unchanged by the bucket check: the replay is
    // decided inside the start transaction, so a retry with the token of an
    // operation that started IN_PROGRESS returns that operation even when
    // the input bucket directory has been removed since. The retry must not
    // manufacture a new FAILED row.
    let h = Harness::new().await;
    let body = with_top(start_body(), "ClientToken", json!("tok-replay-wins"));
    let first = start_ok(&base_ctx(&h), body.clone()).await;
    assert_eq!(first["BulkOperationStatus"], json!("IN_PROGRESS"));
    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(IN_BUCKET)).unwrap();
    let second = start_ok(&base_ctx(&h), body).await;
    assert_eq!(second["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(
        second["BulkOperationStatus"],
        json!("IN_PROGRESS"),
        "the replay answers the original operation, not a new terminal row"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_bucket_that_is_a_symlink_starts_the_operation_terminal_failed() {
    // The bucket existence check reads symlink_metadata, so a symlink at
    // the bucket name is never followed: even one pointing at a real
    // directory full of files answers the same terminal FAILED as a
    // missing bucket, matching the enumeration, which would not follow it.
    let h = Harness::new().await;
    let account_dir = h.store.0.join(ACCOUNT);
    let real = account_dir.join("real-bucket");
    std::fs::create_dir_all(&real).unwrap();
    std::fs::write(real.join("part-0.json"), b"{\"Item\": {}}\n").unwrap();
    std::os::unix::fs::symlink(&real, account_dir.join("linked")).unwrap();
    let body = with_input(start_body(), "S3Bucket", json!("linked"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
    assert_eq!(description["FailureMessage"], json!(NO_SUCH_BUCKET));
}

#[tokio::test]
async fn the_account_directory_is_created_at_start_when_missing() {
    let h = Harness::new().await;
    let account_dir = h.store.0.join(ACCOUNT);
    std::fs::remove_dir_all(&account_dir).unwrap();
    // With the account directory gone the staged buckets are gone too, so
    // the start lands terminal FAILED; the directory itself must exist
    // again, created before the bucket check ran.
    let description = start_ok(&base_ctx(&h), start_body()).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
    let meta = std::fs::symlink_metadata(&account_dir).expect("the account directory exists");
    assert!(meta.is_dir());
}

#[tokio::test]
async fn a_missing_bucket_on_a_saturated_account_still_starts_terminal_failed() {
    // A row born terminal occupies no concurrency slot, so the account
    // limit is not consulted for it: the caller gets the failed operation,
    // not LimitExceededException.
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_concurrent_bulk_operations", json!(1))]),
    );
    start_ok(&ctx, start_body()).await;
    let body = with_input(
        with_top(start_body(), "TableName", json!("second")),
        "S3Bucket",
        json!("absent"),
    );
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
}

/// Insert one terminal-born row directly through the store with an
/// explicit start time, the way the cap's window test needs rows older
/// than the handler's real clock can produce.
async fn insert_terminal_born(h: &Harness, start_time_ms: i64) {
    let new = NewBulkOperation {
        operation_id: arn::generate_operation_id(),
        table_id: h.table_id.clone(),
        table_name: TABLE.to_owned(),
        table_arn: h.table_arn.clone(),
        client_token: None,
        client_token_hash: None,
        request_fingerprint: "fp-terminal-born".to_owned(),
        input_config: json!({"S3Bucket": "absent"}),
        output_config: json!({}),
        input_files: Vec::new(),
        max_write_request_units: None,
        performance_tier: Some("STANDARD".to_owned()),
        start_time_ms,
        total_input_bytes: None,
        initial_failure: Some(extenddb_storage::InitialFailure {
            code: "S3NoSuchBucket".to_owned(),
            message: "The specified bucket does not exist".to_owned(),
            cap: u64::MAX,
            window_start_ms: 0,
        }),
        run_context: None,
    };
    match h
        .storage
        .start_bulk_operation(ACCOUNT, new, 50)
        .await
        .expect("insert a terminal-born row")
    {
        StartOutcome::Created(_) => {}
        _ => panic!("the direct insert must create the row"),
    }
}

#[tokio::test]
async fn the_101st_missing_bucket_start_in_an_hour_is_refused() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_input(start_body(), "S3Bucket", json!("absent"));
    // The default cap admits one hundred terminal-born starts. Terminal
    // rows trip neither the per-table guard nor the account limit, so one
    // table serves them all.
    for _ in 0..100 {
        let description = start_ok(&ctx, body.clone()).await;
        assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    }
    assert_eq!(expect_throttling(start(&ctx, body).await), THROTTLED);
    // The refusal inserted nothing: the account still holds exactly the
    // hundred terminal-born rows.
    let born = h
        .storage
        .count_terminal_born_since(ACCOUNT, 0)
        .await
        .expect("count");
    assert_eq!(born, 100, "the refused request must not insert a row");
}

#[tokio::test]
async fn a_start_on_an_existing_bucket_is_neither_counted_nor_refused_by_the_cap() {
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    Harness::create_table(&h.storage, "third").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_failed_starts_per_hour", json!(1))]),
    );
    // A start on an existing bucket succeeds and does not consume the cap
    // of one: the missing-bucket start after it still lands terminal.
    let ok = start_ok(&ctx, start_body()).await;
    assert_eq!(ok["BulkOperationStatus"], json!("IN_PROGRESS"));
    let missing = with_input(
        with_top(start_body(), "TableName", json!("second")),
        "S3Bucket",
        json!("absent"),
    );
    let failed = start_ok(&ctx, missing.clone()).await;
    assert_eq!(failed["BulkOperationStatus"], json!("FAILED"));
    // The cap is now consumed, so the next missing-bucket start is
    // refused; a start on an existing bucket is still admitted.
    assert_eq!(expect_throttling(start(&ctx, missing).await), THROTTLED);
    let still_ok = start_ok(&ctx, with_top(start_body(), "TableName", json!("third"))).await;
    assert_eq!(still_ok["BulkOperationStatus"], json!("IN_PROGRESS"));
}

#[tokio::test]
async fn the_failed_start_cap_ignores_rows_outside_the_hour_window() {
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_failed_starts_per_hour", json!(1))]),
    );
    // Two terminal-born rows older than the window, inserted through the
    // store with explicit start times.
    insert_terminal_born(&h, now_ms() - 3_700_000).await;
    insert_terminal_born(&h, now_ms() - 7_200_000).await;
    // They are invisible to the cap: a fresh missing-bucket start still
    // answers the terminal FAILED operation under a cap of one.
    let body = with_input(start_body(), "S3Bucket", json!("absent"));
    let description = start_ok(&ctx, body.clone()).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    // The fresh row is inside the window, so the cap of one now refuses.
    assert_eq!(expect_throttling(start(&ctx, body).await), THROTTLED);
}

#[tokio::test]
async fn a_report_bucket_directory_that_does_not_exist_is_not_checked_at_start() {
    let h = Harness::new().await;
    // The S3Bucket member is required at Start, but whether the
    // directory it names exists is decided at the first report record,
    // not here. Only the input bucket exists; the report bucket directory
    // is gone.
    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(OUT_BUCKET)).unwrap();
    let body = with_top(
        start_body(),
        "OutputS3Configuration",
        json!({ "OutputType": ["ERROR"], "S3Bucket": OUT_BUCKET }),
    );
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
}

/// The ids of every operation the account holds, newest first.
async fn stored_operation_ids(h: &Harness) -> Vec<String> {
    let (records, _) = h
        .storage
        .list_bulk_operations(ACCOUNT, 50, None)
        .await
        .expect("list the stored operations");
    records.into_iter().map(|r| r.operation_id).collect()
}

/// With an operation running on the table, a valid Start, one naming an
/// input bucket that does not exist, one whose prefix matches no object
/// and another valid one each answer the busy message and create nothing
/// (measured on the hosted service with these four Starts). Once the
/// operation has finished, the missing-bucket Start is born `FAILED`.
#[tokio::test]
async fn every_start_on_a_busy_table_answers_busy_whatever_its_input() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    h.write_input("data/part-0.json", b"0123456789");
    let with_data = with_input(start_body(), "S3KeyPrefix", json!("data/"));
    let running = start_ok(&ctx, with_data.clone()).await;
    assert_eq!(running["BulkOperationStatus"], json!("IN_PROGRESS"));
    let running_id = description_arn(&running)
        .rsplit('/')
        .next()
        .expect("operation id")
        .to_owned();

    let missing = with_input(start_body(), "S3Bucket", json!("absent"));
    let no_match = with_input(start_body(), "S3KeyPrefix", json!("nothing-here/"));
    for (label, body) in [
        ("a valid start", with_data.clone()),
        ("a missing input bucket", missing.clone()),
        ("a prefix that matches no object", no_match),
        ("another valid start", with_data),
    ] {
        match start(&ctx, body).await {
            Err(DynamoDbError::ResourceInUseException(message)) => {
                assert_eq!(message, TABLE_BUSY, "{label}");
            }
            other => panic!("{label}: expected the busy answer, got {other:?}"),
        }
    }
    assert_eq!(
        stored_operation_ids(&h).await,
        vec![running_id.clone()],
        "the refused starts created nothing"
    );

    let claimed = h
        .storage
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(claimed.iter().any(|r| r.operation_id == running_id));
    finish_op(&h, ACCOUNT, "owner-1", &running_id, "COMPLETED").await;
    let born = start_ok(&ctx, missing).await;
    assert_eq!(born["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(born["FailureCode"], json!("S3NoSuchBucket"));
    assert_eq!(born["FailureMessage"], json!(NO_SUCH_BUCKET));
}

/// At the failed-start cap a missing-bucket Start on a table with an
/// operation running still answers busy, as the start transaction orders
/// the two; on an idle table it is throttled. Neither inserts a row.
#[tokio::test]
async fn a_missing_bucket_start_at_the_cap_answers_busy_on_a_busy_table() {
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_failed_starts_per_hour", json!(1))]),
    );
    let missing_on_second = with_input(
        with_top(start_body(), "TableName", json!("second")),
        "S3Bucket",
        json!("absent"),
    );
    let failed = start_ok(&ctx, missing_on_second.clone()).await;
    assert_eq!(failed["BulkOperationStatus"], json!("FAILED"));
    start_ok(&ctx, start_body()).await;

    let missing_on_busy = with_input(start_body(), "S3Bucket", json!("absent"));
    assert_eq!(
        expect_in_use(start(&ctx, missing_on_busy).await),
        TABLE_BUSY
    );
    assert_eq!(
        expect_throttling(start(&ctx, missing_on_second).await),
        THROTTLED
    );
    let born = h
        .storage
        .count_terminal_born_since(ACCOUNT, 0)
        .await
        .expect("count");
    assert_eq!(born, 1, "neither refused start inserted a row");
}
