// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// PerformanceTier against the table's features (launch source of truth:
// the accelerated tier is not available on a table with a vector index).

#[tokio::test]
async fn accelerated_is_rejected_on_a_table_with_a_vector_index_and_standard_is_not() {
    let h = Harness::new().await;
    let table = format!("{TABLE}-vectors");
    Harness::create_table_with_vector(&h.storage, &table).await;
    let ctx = base_ctx(&h);

    let body = with_top(
        with_top(start_body(), "TableName", json!(table)),
        "PerformanceTier",
        json!("ACCELERATED"),
    );
    let message = expect_validation(start(&ctx, body).await);
    // Measured on the service.
    assert_eq!(
        message,
        "Invalid Request: Value provided for parameter PerformanceTier is not supported for \
         tables with vector indexes."
    );

    // STANDARD on the same table starts; the plain table accepts ACCELERATED.
    let body = with_top(
        with_top(start_body(), "TableName", json!(table)),
        "PerformanceTier",
        json!("STANDARD"),
    );
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["PerformanceTier"], json!("STANDARD"));
    let body = with_top(start_body(), "PerformanceTier", json!("ACCELERATED"));
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["PerformanceTier"], json!("ACCELERATED"));
}

// Model bounds on the S3 configuration members.

#[tokio::test]
async fn an_s3_key_prefix_above_the_model_cap_is_rejected() {
    let h = Harness::new().await;
    let prefix = "p".repeat(1025);
    let body = with_input(start_body(), "S3KeyPrefix", json!(prefix));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains(
            "at 'inputS3Configuration.s3KeyPrefix' failed to satisfy constraint: Member must \
             have length less than or equal to 1024"
        ),
        "{message}"
    );
    let body = with_output(start_body(), "S3KeyPrefix", json!("q".repeat(1025)));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("at 'outputS3Configuration.s3KeyPrefix'"),
        "{message}"
    );
    // Exactly the cap passes the length check.
    let body = with_output(start_body(), "S3KeyPrefix", json!("r".repeat(1024)));
    start_ok(&base_ctx(&h), body).await;
}

#[tokio::test]
async fn an_s3_bucket_owner_that_is_not_twelve_digits_is_rejected_with_the_pattern_message() {
    let h = Harness::new().await;
    for (owner, member) in [
        ("12345678901", "inputS3Configuration.s3BucketOwner"),
        ("1234567890123", "inputS3Configuration.s3BucketOwner"),
        ("12345678901a", "inputS3Configuration.s3BucketOwner"),
    ] {
        let body = with_input(start_body(), "S3BucketOwner", json!(owner));
        let message = expect_validation(start(&base_ctx(&h), body).await);
        assert_eq!(
            message,
            format!(
                "1 validation error detected: Value '{owner}' at '{member}' failed to satisfy \
                 constraint: Member must satisfy regular expression pattern: [0-9]{{12}}"
            )
        );
    }
    let body = with_output(start_body(), "S3BucketOwner", json!("abc"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("at 'outputS3Configuration.s3BucketOwner'"),
        "{message}"
    );
    // A well-formed foreign owner still reaches the ownership check, which
    // is a terminal-born failure rather than a validation error.
    let body = with_input(start_body(), "S3BucketOwner", json!("999999999999"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
}

#[tokio::test]
async fn an_s3_sse_kms_key_id_outside_the_model_bounds_is_rejected() {
    let h = Harness::new().await;
    // Measured for 2,049 characters; the empty key id's clause follows the
    // model's other minimum-length texts and is unmeasured.
    for (key_id, rule) in [
        (String::new(), "greater than or equal to 1"),
        ("k".repeat(2049), "less than or equal to 2048"),
    ] {
        let body = with_output(start_body(), "S3SseKmsKeyId", json!(key_id));
        let message = expect_validation(start(&base_ctx(&h), body).await);
        assert_eq!(
            message,
            format!(
                "1 validation error detected: Value '{key_id}' at \
                 'outputS3Configuration.s3SseKmsKeyId' failed to satisfy constraint: Member \
                 must have length {rule}"
            )
        );
    }
    let body = with_output(start_body(), "S3SseKmsKeyId", json!("k".repeat(2048)));
    start_ok(&base_ctx(&h), body).await;
}

// ListBulkOperations on a server without a [bulk] section reaches storage
// and answers whatever the backend answers. The Python suite's skip fixture
// relies on this order on the MongoDB and Cassandra jobs, whose test
// drivers configure no bulk store: the backend's refusal must be the
// message the client sees, not the disabled-store message.

#[tokio::test]
async fn list_without_a_bulk_store_reaches_storage_before_any_store_check() {
    let h = Harness::new().await;
    let ctx = ctx_without_store(&h);
    // SQLite supports bulk operations, so the list succeeds with an empty
    // page; a refusing backend would surface its own error here instead.
    let listed = call(&ctx, "ListBulkOperations", json!({}))
        .await
        .expect("list reaches storage even with no bulk store configured");
    assert_eq!(summaries(&listed).len(), 0);
    // Start on the same context answers the disabled-store message, so the
    // order is specific to List.
    let message = expect_validation(start(&ctx, start_body()).await);
    assert_eq!(message, BULK_DISABLED);
}

// A null or absent required member answers the hosted ValidationException
// the rest of the server uses for a null member, not the serde
// SerializationException a straight deserialize gives. Required members
// come from the API model's input shapes (2012-08-10):
// StartBulkPutItemsInput requires InputS3Configuration, OutputS3Configuration,
// TableName; InputS3Configuration requires S3Bucket and InputFormat;
// OutputS3Configuration requires OutputType; Describe and Cancel require
// BulkOperationArn.

fn null_member_message(path: &str) -> String {
    format!(
        "1 validation error detected: Value null at '{path}' failed to satisfy constraint: \
         Member must not be null"
    )
}

/// `start_body` with one top level member removed.
fn start_without_top(key: &str) -> Value {
    let mut body = start_body();
    body.as_object_mut().unwrap().remove(key);
    body
}

/// `start_body` with one member removed from `InputS3Configuration`.
fn start_without_input_member(key: &str) -> Value {
    let mut body = start_body();
    body["InputS3Configuration"]
        .as_object_mut()
        .unwrap()
        .remove(key);
    body
}

/// `start_body` with one member removed from `OutputS3Configuration`.
fn start_without_output_member(key: &str) -> Value {
    let mut body = start_body();
    body["OutputS3Configuration"]
        .as_object_mut()
        .unwrap()
        .remove(key);
    body
}

#[tokio::test]
async fn start_missing_input_configuration_answers_the_null_member_validation() {
    let h = Harness::new().await;
    let message =
        expect_validation(start(&base_ctx(&h), start_without_top("InputS3Configuration")).await);
    assert_eq!(message, null_member_message("inputS3Configuration"));
}

#[tokio::test]
async fn start_missing_output_configuration_answers_the_null_member_validation() {
    let h = Harness::new().await;
    let message =
        expect_validation(start(&base_ctx(&h), start_without_top("OutputS3Configuration")).await);
    assert_eq!(message, null_member_message("outputS3Configuration"));
}

// Measured on the service: a missing or null
// TableName answers the server's table-operation message, not the null
// member envelope, and it is answered first and alone.
const TABLE_NAME_REQUIRED: &str =
    "The parameter 'TableName' is required but was not present in the request";

#[tokio::test]
async fn start_missing_table_name_answers_the_required_parameter_message() {
    let h = Harness::new().await;
    let message = expect_validation(start(&base_ctx(&h), start_without_top("TableName")).await);
    assert_eq!(message, TABLE_NAME_REQUIRED);
}

#[tokio::test]
async fn start_missing_table_name_and_input_configuration_answers_only_table_name() {
    let h = Harness::new().await;
    let mut body = start_without_top("TableName");
    body.as_object_mut().unwrap().remove("InputS3Configuration");
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, TABLE_NAME_REQUIRED);
}

#[tokio::test]
async fn start_null_table_name_answers_the_required_parameter_message() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "TableName", Value::Null);
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, TABLE_NAME_REQUIRED);
}

#[tokio::test]
async fn start_missing_input_bucket_answers_the_null_member_validation() {
    let h = Harness::new().await;
    let message =
        expect_validation(start(&base_ctx(&h), start_without_input_member("S3Bucket")).await);
    assert_eq!(
        message,
        null_member_message("inputS3Configuration.s3Bucket")
    );
}

#[tokio::test]
async fn start_missing_input_format_answers_the_null_member_validation() {
    let h = Harness::new().await;
    let message =
        expect_validation(start(&base_ctx(&h), start_without_input_member("InputFormat")).await);
    assert_eq!(
        message,
        null_member_message("inputS3Configuration.inputFormat")
    );
}

#[tokio::test]
async fn start_missing_output_type_answers_the_null_member_validation() {
    let h = Harness::new().await;
    let message =
        expect_validation(start(&base_ctx(&h), start_without_output_member("OutputType")).await);
    assert_eq!(
        message,
        null_member_message("outputS3Configuration.outputType")
    );
}

#[tokio::test]
async fn start_two_missing_members_answer_the_combined_message_in_model_order() {
    let h = Harness::new().await;
    let mut body = start_body();
    {
        let members = body.as_object_mut().unwrap();
        members.remove("InputS3Configuration");
        members.remove("OutputS3Configuration");
    }
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "2 validation errors detected: Value null at 'inputS3Configuration' failed to satisfy \
         constraint: Member must not be null; Value null at 'outputS3Configuration' failed to \
         satisfy constraint: Member must not be null"
    );
}

// Measured on the service: a missing or null BulkOperationArn on Describe and
// Cancel answers the ARN parser's shape layer, the same message a malformed
// ARN gets.
#[tokio::test]
async fn describe_missing_arn_answers_the_arn_shape_message() {
    let h = Harness::new().await;
    let message = expect_validation(call(&base_ctx(&h), "DescribeBulkOperation", json!({})).await);
    assert_eq!(message, "Invalid Bulk operation ARN");
}

#[tokio::test]
async fn describe_null_arn_answers_the_arn_shape_message() {
    let h = Harness::new().await;
    let message = expect_validation(
        call(
            &base_ctx(&h),
            "DescribeBulkOperation",
            json!({ "BulkOperationArn": null }),
        )
        .await,
    );
    assert_eq!(message, "Invalid Bulk operation ARN");
}

#[tokio::test]
async fn cancel_missing_arn_answers_the_arn_shape_message() {
    let h = Harness::new().await;
    let message = expect_validation(call(&base_ctx(&h), "CancelBulkOperation", json!({})).await);
    assert_eq!(message, "Invalid Bulk operation ARN");
}

#[tokio::test]
async fn an_unsupported_backend_refuses_all_four_operations_with_validation() {
    let h = Harness::new().await;
    let storage: Arc<dyn StorageEngine> = Arc::new(unsupported_backend::BulkUnsupportedStorage {
        inner: Arc::clone(&h.storage),
        creating_index: None,
    });
    let ctx = ctx_with_storage(&h, storage);
    let message = unsupported_backend::UNSUPPORTED_MESSAGE;
    // Start reaches the bulk store after the table lookup (delegated to the
    // real engine) and the staged input bucket check.
    assert_eq!(expect_validation(start(&ctx, start_body()).await), message);
    let operation_arn = arn_of(ACCOUNT, "00000000000000-0000abcd");
    assert_eq!(
        expect_validation(describe(&ctx, &operation_arn).await),
        message
    );
    assert_eq!(
        expect_validation(cancel(&ctx, &operation_arn).await),
        message
    );
    assert_eq!(
        expect_validation(call(&ctx, "ListBulkOperations", json!({})).await),
        message
    );
}

// Measured on the service: a start on a table whose
// global secondary index is being created answers ResourceInUseException
// "Table has ongoing index operations, please wait and try again" in both
// creation phases, whether the table reads UPDATING or ACTIVE. The check
// runs at the table lookup, before the bulk store is reached: the double's
// store refuses everything, and the start never gets that far.
#[tokio::test]
async fn a_start_on_a_table_with_an_index_being_created_is_refused() {
    let h = Harness::new_with_gsi().await;
    let storage: Arc<dyn StorageEngine> = Arc::new(unsupported_backend::BulkUnsupportedStorage {
        inner: Arc::clone(&h.storage),
        creating_index: Some("gsi1".to_owned()),
    });
    let ctx = ctx_with_storage(&h, storage);
    let message = expect_in_use(start(&ctx, start_body()).await);
    assert_eq!(
        message,
        "Table has ongoing index operations, please wait and try again"
    );

    // The same table with every index ACTIVE passes the lookup and reaches
    // the store, so the check keys on the index status alone.
    let storage: Arc<dyn StorageEngine> = Arc::new(unsupported_backend::BulkUnsupportedStorage {
        inner: Arc::clone(&h.storage),
        creating_index: None,
    });
    let ctx = ctx_with_storage(&h, storage);
    assert_eq!(
        expect_validation(start(&ctx, start_body()).await),
        unsupported_backend::UNSUPPORTED_MESSAGE
    );
}

// TableName is typed as TableArn in the model, bounded to 1..=1024
// characters. The bound is checked at step 2 with the standard constraint
// message, so an out-of-range name never answers resource-not-found.

#[tokio::test]
async fn an_empty_table_name_fails_the_table_name_rules() {
    // Measured on the service: an empty bare name answers the table-name rules
    // (minimum 3 and the pattern) together in one envelope.
    let h = Harness::new().await;
    let body = with_top(start_body(), "TableName", json!(""));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "2 validation errors detected: Value '' at 'tableName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3; Value '' at 'tableName' failed \
         to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    );
}

#[tokio::test]
async fn a_bare_table_name_of_256_characters_fails_the_table_name_maximum() {
    let h = Harness::new().await;
    let name = "a".repeat(256);
    let body = with_top(start_body(), "TableName", json!(name));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy \
             constraint: Member must have length less than or equal to 255"
        )
    );
}

#[tokio::test]
async fn a_table_name_of_1025_characters_fails_the_length_bound() {
    let h = Harness::new().await;
    let name = "a".repeat(1025);
    let body = with_top(start_body(), "TableName", json!(name));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy \
             constraint: Member must have length less than or equal to 1024"
        ),
        "the service reports the TableArn maximum one-sided (measured on the service)"
    );
}

#[tokio::test]
async fn a_table_arn_of_1024_characters_passes_the_bound_and_meets_the_name_rule() {
    let h = Harness::new().await;
    // At the TableArn maximum the bound passes, and the name the ARN
    // resolves to is held to the table-name rule: a 1024-character ARN
    // carries a name over 255 characters and answers the measured `Table
    // name of ARN` maximum. An ARN whose name is inside the rule reaches
    // the lookup and answers resource-not-found.
    let prefix = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/");
    let bare = "a".repeat(1024 - prefix.len());
    let name = format!("{prefix}{bare}");
    let body = with_top(start_body(), "TableName", json!(name));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy \
             constraint: Table name of ARN must have length less than or equal to 255"
        )
    );
    let bare = "a".repeat(255);
    let body = with_top(start_body(), "TableName", json!(format!("{prefix}{bare}")));
    assert_eq!(
        expect_not_found(start(&base_ctx(&h), body).await),
        format!("{TABLE_NOT_FOUND}: Table: {bare} not found")
    );
}

// A Start the account concurrency cap will refuse must not pay the input
// enumeration walk first. The account limit is read before the walk; the
// authoritative check stays in the start transaction.
// A second start on a different table whose prefix holds more files than the
// file cap answers the limit, not the file cap, so the walk did not run. The
// busy-table guard is deliberately not pre-checked: a start on a busy table
// over the file cap answers the file cap today (pinned by
// the_input_file_cap_is_rejected_before_the_table_guard), so a busy-table
// pre-check would reorder that observable outcome.

#[tokio::test]
async fn a_saturated_account_is_refused_before_the_input_walk() {
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[
            ("max_concurrent_bulk_operations", json!(1)),
            ("max_bulk_input_files", json!(2)),
        ]),
    );
    // Occupy the account's one slot with a running operation on the first
    // table (the input bucket is empty, so this start enumerates nothing).
    start_ok(&ctx, start_body()).await;
    // Place more files than the file cap under a distinct prefix. If the
    // walk ran, the second start would answer the file-cap message; the
    // account-limit pre-check answers first, without reading the prefix.
    for i in 0..3 {
        h.write_input(&format!("walk/part-{i}.json"), b"0123456789");
    }
    let second = with_top(
        with_input(start_body(), "S3KeyPrefix", json!("walk/")),
        "TableName",
        json!("second"),
    );
    let message = expect_limit_exceeded(start(&ctx, second).await);
    assert!(
        message.contains("maximum number of concurrent bulk operations"),
        "the account limit answers before the walk reads the prefix: {message}"
    );
}
