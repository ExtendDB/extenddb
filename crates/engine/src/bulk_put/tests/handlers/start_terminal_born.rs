// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Starts naming a bad input location: the Start answers `IN_PROGRESS`
//! (measured on the service: HTTP 200 for a missing input bucket, three of
//! three) and the first claim ends the operation `FAILED` before a row is
//! read, with no byte member. The output bucket checks that stay with the
//! first report record are here too, and the busy table that refuses any
//! start before anything is created.

use super::*;

/// Each bad input location, as a request change and the setup it needs:
/// the Start answers `IN_PROGRESS`, then the run ends `FAILED` with
/// `S3NoSuchBucket` and both byte members absent.
#[tokio::test]
async fn a_bad_input_bucket_is_accepted_and_the_run_fails_it() {
    struct Case {
        label: &'static str,
        bucket: &'static str,
        setup: fn(&Harness),
    }
    fn none(_: &Harness) {}
    #[cfg(unix)]
    fn link_to_staged(h: &Harness) {
        // The link's target is the account's own staged input bucket, so a
        // FAILED verdict can only come from the link refusal.
        let target = h.store.0.join(ACCOUNT).join(IN_BUCKET);
        std::os::unix::fs::symlink(&target, h.store.0.join(ACCOUNT).join("linked")).unwrap();
    }
    fn remove_account(h: &Harness) {
        std::fs::remove_dir_all(h.store.0.join(ACCOUNT)).unwrap();
    }
    let mut cases = vec![
        Case {
            label: "a missing bucket",
            bucket: "absent",
            setup: none,
        },
        Case {
            label: "a missing account directory",
            bucket: IN_BUCKET,
            setup: remove_account,
        },
    ];
    #[cfg(unix)]
    cases.push(Case {
        label: "a symlinked bucket",
        bucket: "linked",
        setup: link_to_staged,
    });
    for case in cases {
        let h = Harness::new().await;
        h.write_input("part-0.json", &file_of(&plain_rows(2)));
        (case.setup)(&h);
        let ctx = base_ctx(&h);
        let body = with_input(start_body(), "S3Bucket", json!(case.bucket));
        let started = start_ok(&ctx, body).await;
        let label = case.label;
        assert_eq!(
            started["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "{label}"
        );
        for member in [
            "TotalInputBytes",
            "ProcessedInputBytes",
            "BulkOperationUsage",
        ] {
            assert!(started.get(member).is_none(), "{label}: {member} at Start");
        }

        let ended = run_started(&h, &ctx, &started).await;
        assert_eq!(ended["BulkOperationStatus"], json!("FAILED"), "{label}");
        assert_eq!(ended["FailureCode"], json!("S3NoSuchBucket"), "{label}");
        assert_eq!(ended["FailureMessage"], json!(NO_SUCH_BUCKET), "{label}");
        assert_eq!(ended["ProcessedItemCount"], json!(0), "{label}");
        for member in ["TotalInputBytes", "ProcessedInputBytes"] {
            assert!(ended.get(member).is_none(), "{label}: {member} when FAILED");
        }
        assert!(ended.get("BulkOperationUsage").is_some(), "{label}");
        if case.bucket == IN_BUCKET {
            let meta = std::fs::symlink_metadata(h.store.0.join(ACCOUNT))
                .expect("the account directory exists again");
            assert!(meta.is_dir(), "the check creates the account directory");
        }
        // No output object: the run listed nothing, so nothing is rendered
        // (measured on the service: no output object for a missing bucket).
        let reports = h.store.0.join(ACCOUNT).join(OUT_BUCKET);
        let written = std::fs::read_dir(&reports)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(written, 0, "{label}: no output object");
    }
}

#[tokio::test]
async fn a_replayed_token_returns_the_operation_its_run_failed() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_input(
        with_top(start_body(), "ClientToken", json!("tok-failed-replay")),
        "S3Bucket",
        json!("absent"),
    );
    let first = start_ok(&ctx, body.clone()).await;
    run_started(&h, &ctx, &first).await;
    let second = start_ok(&ctx, body).await;
    assert_eq!(second["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(second["BulkOperationStatus"], json!("FAILED"));
    assert!(
        second.get("BulkOperationUsage").is_none(),
        "a replayed Start answers no BulkOperationUsage (measured on the service)"
    );
}

#[tokio::test]
async fn a_replayed_token_wins_over_a_bucket_removed_after_the_first_start() {
    // A retry with the token of an operation that started IN_PROGRESS
    // returns that operation even when the input bucket directory has been
    // removed since; the retry creates nothing.
    let h = Harness::new().await;
    let body = with_top(start_body(), "ClientToken", json!("tok-replay-wins"));
    let first = start_ok(&base_ctx(&h), body.clone()).await;
    assert_eq!(first["BulkOperationStatus"], json!("IN_PROGRESS"));
    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(IN_BUCKET)).unwrap();
    let second = start_ok(&base_ctx(&h), body).await;
    assert_eq!(second["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(second["BulkOperationStatus"], json!("IN_PROGRESS"));
}

#[tokio::test]
async fn a_missing_input_bucket_takes_a_concurrency_slot_until_its_run_ends() {
    // The operation is IN_PROGRESS until its first claim, so it counts
    // against the account limit as every other running operation does.
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_concurrent_bulk_operations", json!(1))]),
    );
    let missing = with_input(start_body(), "S3Bucket", json!("absent"));
    let started = start_ok(&ctx, missing).await;
    let other = with_top(start_body(), "TableName", json!("second"));
    expect_limit_exceeded(start(&ctx, other.clone()).await);
    run_started(&h, &ctx, &started).await;
    let admitted = start_ok(&ctx, other).await;
    assert_eq!(admitted["BulkOperationStatus"], json!("IN_PROGRESS"));
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
/// (measured on the hosted service with these four Starts).
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
        ("a missing input bucket", missing),
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
        vec![running_id],
        "the refused starts created nothing"
    );
}
