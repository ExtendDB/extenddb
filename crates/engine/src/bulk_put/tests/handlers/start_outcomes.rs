// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn the_start_description_carries_every_submitted_field() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", b"0123456789");
    let input_config = json!({
        "S3Bucket": IN_BUCKET,
        "S3KeyPrefix": "orders/",
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "GZIP",
        "S3BucketOwner": ACCOUNT
    });
    let output_config = json!({
        "S3Bucket": OUT_BUCKET,
        "S3KeyPrefix": "reports/",
        "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"],
        "S3SseAlgorithm": "AES256",
        "S3SseKmsKeyId": "alias/example",
        "S3BucketOwner": ACCOUNT
    });
    let body = json!({
        "TableName": TABLE,
        "ClientToken": "load-2026-09-10-a",
        "InputS3Configuration": input_config,
        "OutputS3Configuration": output_config,
        "MaxWriteRequestUnits": 2048,
        "PerformanceTier": "ACCELERATED"
    });
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, body).await;

    let operation_arn = description_arn(&description);
    let prefix = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/");
    let id = operation_arn
        .strip_prefix(&prefix)
        .expect("the ARN carries the caller's region and account");
    let (stamp, hex) = id.split_once('-').expect("the id carries one hyphen");
    assert_eq!(stamp.len(), 14);
    assert!(stamp.bytes().all(|b| b.is_ascii_digit()));
    assert_eq!(hex.len(), 8);

    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
    assert_eq!(
        description["TableDetails"],
        json!([{ "TableName": TABLE, "TableId": h.table_id }])
    );
    // Modelled but never emitted by the service (measured on the
    // service): the table travels only in TableDetails.
    assert!(description.get("TableArn").is_none());
    assert!(description.get("TableId").is_none());
    assert_eq!(description["ClientToken"], json!("load-2026-09-10-a"));
    assert_eq!(description["InputS3Configuration"], input_config);
    assert_eq!(description["OutputS3Configuration"], output_config);
    assert_eq!(description["ProcessedItemCount"], json!(0));
    assert_eq!(description["FailedItemCount"], json!(0));
    assert_eq!(description["ProcessedInputBytes"], json!(0));
    assert_eq!(description["TotalInputBytes"], json!(10));
    assert_eq!(description["MaxWriteRequestUnits"], json!(2048));
    assert_eq!(description["PerformanceTier"], json!("ACCELERATED"));

    // StartTime is fractional seconds, not milliseconds.
    let start_time = description["StartTime"]
        .as_f64()
        .expect("StartTime is a number");
    // Whole seconds convert exactly (they fit u32 until 2106), and the
    // 300-second tolerance below does not need the milliseconds.
    let now_s = f64::from(u32::try_from(now_ms() / 1000).expect("epoch seconds fit u32"));
    assert!(
        (start_time - now_s).abs() < 300.0,
        "StartTime must be on the seconds scale: {start_time}"
    );

    let members = description.as_object().unwrap();
    for absent in [
        "EndTime",
        "BulkOperationUsage",
        "FailureCode",
        "FailureMessage",
    ] {
        assert!(
            !members.contains_key(absent),
            "{absent} must not be emitted for a running operation"
        );
    }

    // Describe renders the same description.
    let described = describe(&ctx, &operation_arn)
        .await
        .expect("describe must succeed");
    assert_eq!(described["BulkOperationDescription"], description);
}

// Start: transaction outcomes through the handler.

#[tokio::test]
async fn a_replayed_token_returns_the_same_arn_and_creates_nothing() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-replay"));
    let first = start_ok(&ctx, body.clone()).await;
    let second = start_ok(&ctx, body).await;
    assert_eq!(description_arn(&first), description_arn(&second));
    let listed = call(&ctx, "ListBulkOperations", json!({}))
        .await
        .expect("list");
    assert_eq!(
        summaries(&listed).len(),
        1,
        "the replay must create nothing"
    );
}

#[tokio::test]
async fn a_token_replayed_against_a_terminal_operation_returns_its_status_and_counters() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let body = with_top(start_body(), "ClientToken", json!("tok-terminal"));
    let first = start_ok(&ctx, body.clone()).await;

    // Drive the started operation to COMPLETED_WITH_ERRORS through the
    // runner, the way a worker would.
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    run_operation(&h.deps("runner-1", runner_config()), record)
        .await
        .expect("the run finishes");

    // The replay returns the existing operation's
    // current description with HTTP 200, whatever its status.
    let replayed = start_ok(&ctx, body).await;
    assert_eq!(description_arn(&replayed), description_arn(&first));
    assert_eq!(
        replayed["BulkOperationStatus"],
        json!("COMPLETED_WITH_ERRORS")
    );
    assert_eq!(replayed["ProcessedItemCount"], json!(3));
    assert_eq!(replayed["FailedItemCount"], json!(1));
    assert!(
        replayed["EndTime"].is_f64(),
        "the terminal description carries EndTime"
    );
}

#[tokio::test]
async fn a_replayed_token_with_different_parameters_is_a_mismatch() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-mismatch"));
    start_ok(&ctx, body.clone()).await;
    let changed = with_top(body, "MaxWriteRequestUnits", json!(4096));
    match start(&ctx, changed).await {
        Err(DynamoDbError::IdempotentParameterMismatchException(message)) => {
            assert_eq!(message, IDEMPOTENT_MISMATCH);
        }
        other => panic!("expected IdempotentParameterMismatchException, got {other:?}"),
    }
}

#[tokio::test]
async fn a_second_start_on_a_busy_table_answers_the_busy_message() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    start_ok(&ctx, start_body()).await;
    assert_eq!(expect_in_use(start(&ctx, start_body()).await), TABLE_BUSY);
}

#[tokio::test]
async fn the_guard_answers_when_the_account_limit_would_also_reject() {
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_concurrent_bulk_operations", json!(1))]),
    );
    start_ok(&ctx, start_body()).await;
    // The second start on the same table violates the guard and the account
    // limit together; the guard must answer.
    assert_eq!(expect_in_use(start(&ctx, start_body()).await), TABLE_BUSY);
}

#[tokio::test]
async fn the_account_limit_answers_across_tables() {
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    Harness::create_table(&h.storage, "third").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_concurrent_bulk_operations", json!(2))]),
    );
    start_ok(&ctx, start_body()).await;
    start_ok(&ctx, with_top(start_body(), "TableName", json!("second"))).await;
    let message = expect_limit_exceeded(
        start(&ctx, with_top(start_body(), "TableName", json!("third"))).await,
    );
    // The measured text, missing separators included, with the configured
    // limit of 2 substituted.
    assert_eq!(
        message,
        "Subscriber limit exceededService quota exceeded You have reached the maximum \
         number of concurrent bulk operations for the selected PerformanceTier, which is \
         2. Wait until other jobs finish, and try again. You can also reach out to \
         AWS Support to request a quota increase."
    );
}

#[tokio::test]
async fn a_saturated_account_is_refused_before_the_enumeration_walk() {
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[
            ("max_concurrent_bulk_operations", json!(1)),
            ("max_bulk_input_files", json!(2)),
        ]),
    );
    start_ok(&ctx, start_body()).await;

    // The second start's input bucket holds more files than the cap, so
    // an enumeration walk would answer the max_bulk_input_files message.
    // The concurrency limit message can only come from the pre-check path
    // that skips the walk.
    let bucket_dir = h.store.0.join(ACCOUNT).join("oversized");
    std::fs::create_dir_all(&bucket_dir).unwrap();
    for i in 0..3 {
        std::fs::write(bucket_dir.join(format!("part-{i}.json")), b"{}").unwrap();
    }

    let body = with_input(
        with_top(start_body(), "TableName", json!("second")),
        "S3Bucket",
        json!("oversized"),
    );
    let message = expect_limit_exceeded(start(&ctx, body).await);
    assert_eq!(
        message,
        "Subscriber limit exceededService quota exceeded You have reached the maximum \
         number of concurrent bulk operations for the selected PerformanceTier, which is \
         1. Wait until other jobs finish, and try again. You can also reach out to \
         AWS Support to request a quota increase.",
        "the account limit must answer without running the walk"
    );
}

#[tokio::test]
async fn a_replayed_token_on_a_saturated_account_returns_the_existing_operation() {
    // A replayed token is answered from its existing row before the
    // account limit, on the saturated path as well as the ordinary one:
    // the one operation counted against the limit is the operation the
    // token created, so a retry of the same start must get that operation
    // back, not LimitExceededException. A mismatched fingerprint under the
    // same saturation answers the mismatch, for the same reason.
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_concurrent_bulk_operations", json!(1))]),
    );
    let body = with_top(start_body(), "ClientToken", json!("tok-saturated"));
    let first = start_ok(&ctx, body.clone()).await;
    let first_arn = first["BulkOperationArn"]
        .as_str()
        .expect("the first start carries an ARN")
        .to_owned();

    let replayed = start_ok(&ctx, body.clone()).await;
    assert_eq!(
        replayed["BulkOperationArn"].as_str(),
        Some(first_arn.as_str()),
        "the replay must return the existing operation, not the account limit"
    );

    let changed = with_top(body, "MaxWriteRequestUnits", json!(4096));
    match start(&ctx, changed).await {
        Err(DynamoDbError::IdempotentParameterMismatchException(message)) => {
            assert_eq!(message, IDEMPOTENT_MISMATCH);
        }
        other => panic!(
            "a mismatched replay on a saturated account must answer the mismatch, got {other:?}"
        ),
    }
}

#[tokio::test]
async fn twenty_concurrent_same_token_starts_yield_one_operation() {
    let h = Harness::new().await;
    let ctx = Arc::new(base_ctx(&h));
    let body = with_top(start_body(), "ClientToken", json!("tok-race"));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let ctx = Arc::clone(&ctx);
        let body = body.clone();
        tasks.spawn(async move {
            crate::dispatch("StartBulkPutItems", body, &ctx, "127.0.0.1:0")
                .await
                .map(|result| result.body)
        });
    }
    let mut arns = HashSet::new();
    while let Some(joined) = tasks.join_next().await {
        let response = joined
            .expect("the task must not panic")
            .expect("no same-token start may answer busy or any other error");
        arns.insert(description_arn(&response["BulkOperationDescription"]));
    }
    assert_eq!(arns.len(), 1, "every start must answer the same operation");
    let listed = call(&ctx, "ListBulkOperations", json!({}))
        .await
        .expect("list");
    assert_eq!(summaries(&listed).len(), 1, "exactly one operation exists");
}

// The service replays a retry that adds a default the first request
// omitted (measured). These drive the whole handler, so the normalization
// start.rs applies before fingerprinting is pinned end to end.

/// A start body with the ClientToken set and InputCompressionType omitted.
fn tokened_body_without_compression(token: &str) -> Value {
    json!({
        "TableName": TABLE,
        "ClientToken": token,
        "InputS3Configuration": { "S3Bucket": IN_BUCKET, "InputFormat": "DYNAMODB_JSON" },
        "OutputS3Configuration": {
            "S3Bucket": OUT_BUCKET,
            "S3KeyPrefix": "run",
            "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"]
        }
    })
}

#[tokio::test]
async fn a_retry_that_adds_none_compression_replays() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = tokened_body_without_compression("tok-none");
    let first = start_ok(&ctx, body.clone()).await;
    let retry = with_input(body, "InputCompressionType", json!("NONE"));
    let replayed = start_ok(&ctx, retry).await;
    assert_eq!(description_arn(&replayed), description_arn(&first));
}

#[tokio::test]
async fn a_retry_that_drops_none_compression_replays() {
    // Normalization applies in both directions (measured): the first
    // request carries InputCompressionType NONE and the retry omits it.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-none-drop"));
    assert_eq!(
        body["InputS3Configuration"]["InputCompressionType"],
        json!("NONE"),
        "the first request carries an explicit NONE"
    );
    let first = start_ok(&ctx, body.clone()).await;
    let mut retry = body;
    retry["InputS3Configuration"]
        .as_object_mut()
        .expect("the input configuration is an object")
        .remove("InputCompressionType");
    let replayed = start_ok(&ctx, retry).await;
    assert_eq!(description_arn(&replayed), description_arn(&first));
}

#[tokio::test]
async fn a_retry_that_adds_standard_tier_replays() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-std"));
    let first = start_ok(&ctx, body.clone()).await;
    let retry = with_top(body, "PerformanceTier", json!("STANDARD"));
    let replayed = start_ok(&ctx, retry).await;
    assert_eq!(description_arn(&replayed), description_arn(&first));
}

#[tokio::test]
async fn a_retry_that_adds_own_account_bucket_owners_replays() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-owner"));
    let first = start_ok(&ctx, body.clone()).await;
    let retry = with_output(
        with_input(body, "S3BucketOwner", json!(ACCOUNT)),
        "S3BucketOwner",
        json!(ACCOUNT),
    );
    let replayed = start_ok(&ctx, retry).await;
    assert_eq!(description_arn(&replayed), description_arn(&first));
}

#[tokio::test]
async fn a_retry_that_adds_aes256_mismatches() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_top(start_body(), "ClientToken", json!("tok-aes"));
    start_ok(&ctx, body.clone()).await;
    let retry = with_output(body, "S3SseAlgorithm", json!("AES256"));
    match start(&ctx, retry).await {
        Err(DynamoDbError::IdempotentParameterMismatchException(message)) => {
            assert_eq!(message, IDEMPOTENT_MISMATCH);
        }
        other => panic!("expected IdempotentParameterMismatchException, got {other:?}"),
    }
}
