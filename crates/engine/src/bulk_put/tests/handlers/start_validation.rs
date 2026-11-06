// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Start: validation order. Each request is wrong at one step and also
// wrong at a later step, so the answered error proves the earlier step ran
// first.

#[tokio::test]
async fn a_bad_enum_value_is_rejected_before_a_bad_prefix() {
    let h = Harness::new().await;
    let body = with_input(
        with_input(start_body(), "InputFormat", json!("CSV")),
        "S3KeyPrefix",
        json!("a/../b"),
    );
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value 'CSV' at 'inputS3Configuration.inputFormat' \
         failed to satisfy constraint: Member must satisfy enum value set: [ION, DYNAMODB_JSON]",
        "step 1 must answer before step 2, with the measured nested path and set order"
    );
}

#[tokio::test]
async fn a_bad_output_bucket_pattern_answers_before_an_empty_output_type() {
    // Measured on the service: an empty OutputType beside a bad output
    // bucket pattern answers the bucket pattern, a model constraint, not
    // the empty-OutputType `Invalid Request:` check.
    let h = Harness::new().await;
    let body = with_output(
        with_output(start_body(), "S3Bucket", json!("b")),
        "OutputType",
        json!([]),
    );
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value 'b' at 'outputS3Configuration.s3Bucket' failed \
         to satisfy constraint: Member must satisfy regular expression pattern: \
         ^[a-z0-9A-Z]+[\\.\\-\\w]*[a-z0-9A-Z]+$"
    );
}

#[tokio::test]
async fn a_table_name_over_1024_answers_before_a_short_prefix() {
    // A TableName of 1025 characters beside S3KeyPrefix 'x/'
    // answers only the tableName length, a model constraint, not the
    // prefix minimum, an `Invalid Request:` check.
    let h = Harness::new().await;
    let name = "a".repeat(1025);
    let body = with_top(
        with_input(start_body(), "S3KeyPrefix", json!("x/")),
        "TableName",
        json!(name),
    );
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy \
             constraint: Member must have length less than or equal to 1024"
        )
    );
}

#[tokio::test]
async fn a_bad_prefix_is_rejected_before_unsupported_ion() {
    let h = Harness::new().await;
    let body = with_input(
        with_input(start_body(), "InputFormat", json!("ION")),
        "S3KeyPrefix",
        json!("a/../b"),
    );
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, BAD_PREFIX, "step 2 must answer before step 3");
}

#[tokio::test]
async fn unsupported_ion_is_rejected_before_a_missing_store() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "InputFormat", json!("ION"));
    let message = expect_validation(start(&ctx_without_store(&h), body).await);
    assert_eq!(message, ION_REFUSED, "step 3 must answer before step 4");
}

#[tokio::test]
async fn a_bad_prefix_is_rejected_before_a_missing_store() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3KeyPrefix", json!("a/../b"));
    let message = expect_validation(start(&ctx_without_store(&h), body).await);
    assert_eq!(message, BAD_PREFIX, "step 2 must answer before step 4");
}

#[tokio::test]
async fn a_missing_store_is_rejected_before_a_missing_table() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "TableName", json!("absent"));
    let message = expect_validation(start(&ctx_without_store(&h), body).await);
    assert_eq!(message, BULK_DISABLED, "step 4 must answer before step 5");
}

#[tokio::test]
async fn a_missing_table_is_rejected_before_the_input_file_cap() {
    let h = Harness::new().await;
    for i in 0..3 {
        h.write_input(&format!("part-{i}.json"), b"0123456789");
    }
    let ctx = ctx_with_limits(&h, bulk_limits(&[("max_bulk_input_files", json!(2))]));
    let body = with_top(start_body(), "TableName", json!("absent"));
    let message = expect_not_found(start(&ctx, body).await);
    assert_eq!(
        message,
        format!("{TABLE_NOT_FOUND}: Table: absent not found"),
        "step 5 must answer before step 6"
    );
}

#[tokio::test]
async fn the_table_guard_answers_an_input_over_the_object_count_cap() {
    // The count cap is the runner's failure, not a Start refusal,
    // so a busy table is what the second start hears.
    let h = Harness::new().await;
    start_ok(&base_ctx(&h), start_body()).await;
    for i in 0..3 {
        h.write_input(&format!("part-{i}.json"), b"0123456789");
    }
    let ctx = ctx_with_limits(&h, bulk_limits(&[("max_bulk_input_files", json!(2))]));
    let message = expect_in_use(start(&ctx, start_body()).await);
    assert_eq!(message, TABLE_BUSY);
}

// The three step 2 checks the prefix chain does not reach: each pairs the
// field fault with a missing table, so the field's message proves the
// check ran before the table lookup at step 5.

#[tokio::test]
async fn an_oversized_configuration_is_rejected_before_a_missing_table() {
    let h = Harness::new().await;
    // Every string member is bounded below the total cap on its own, so the
    // total is reached through the one unbounded member, the OutputType
    // list, which accepts repeated values.
    let body = with_top(
        with_output(start_body(), "OutputType", json!(vec!["ERROR"; 1200])),
        "TableName",
        json!("absent"),
    );
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, CONFIG_TOO_BIG, "step 2 must answer before step 5");
}

#[tokio::test]
async fn a_token_over_the_byte_cap_is_rejected_before_a_missing_table() {
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_client_token_bytes", json!(64))]),
    );
    let body = with_top(
        with_top(start_body(), "ClientToken", json!("t".repeat(65))),
        "TableName",
        json!("absent"),
    );
    let message = expect_validation(start(&ctx, body).await);
    assert_eq!(
        message, "ClientToken exceeds max_bulk_client_token_bytes (64 bytes)",
        "step 2 must answer before step 5"
    );
}

#[tokio::test]
async fn a_missing_table_answers_before_the_owner_mismatch() {
    // The owner check moved from validation step 2 to the terminal-born
    // insert, which needs the table row, so the table lookup answers
    // first.
    let h = Harness::new().await;
    let body = with_top(
        with_input(start_body(), "S3BucketOwner", json!(OTHER_ACCOUNT)),
        "TableName",
        json!("absent"),
    );
    match start(&base_ctx(&h), body).await {
        Err(DynamoDbError::ResourceNotFoundException(_)) => {}
        other => panic!("expected ResourceNotFoundException, got {other:?}"),
    }
}

// Start: field constraints, each alone in an otherwise valid request.

// Measured on the service: every value under 1024 (0, 1, 1000 and 1023)
// answers the model clause with the 1024 minimum; 1024 starts and is
// echoed. Negative values answer the same clause.
#[tokio::test]
async fn max_write_request_units_below_1024_fails_the_model_minimum() {
    let h = Harness::new().await;
    for units in [-5, 0, 1, 1000, 1023] {
        let body = with_top(start_body(), "MaxWriteRequestUnits", json!(units));
        let message = expect_validation(start(&base_ctx(&h), body).await);
        assert_eq!(
            message,
            format!(
                "1 validation error detected: Value '{units}' at 'maxWriteRequestUnits' failed \
                 to satisfy constraint: Member must have value greater than or equal to 1024"
            ),
            "units {units}"
        );
    }
}

#[tokio::test]
async fn max_write_request_units_1024_is_accepted_and_echoed() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "MaxWriteRequestUnits", json!(1024));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["MaxWriteRequestUnits"], json!(1024));
}

#[tokio::test]
async fn a_client_token_containing_a_dollar_sign_is_rejected() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "ClientToken", json!("bad$token"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "the token pattern must be reported: {message}"
    );
    assert!(
        message.contains("clientToken"),
        "the field must be named: {message}"
    );
}

#[tokio::test]
async fn an_empty_client_token_is_rejected_with_the_pattern_message() {
    // The pattern's `+` requires at least one character, so the empty
    // string fails it the same way a `$` does.
    let h = Harness::new().await;
    let body = with_top(start_body(), "ClientToken", json!(""));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value '' at 'clientToken' failed to \
         satisfy constraint: Member must satisfy regular expression pattern: ^[^\\$]+$"
    );
}

#[tokio::test]
async fn a_client_token_containing_a_nul_byte_is_rejected_with_the_pattern_message() {
    // The token is stored verbatim in a text column, and PostgreSQL
    // refuses a raw NUL byte in text, so the handler rejects the byte
    // with the pattern message.
    let h = Harness::new().await;
    let body = with_top(start_body(), "ClientToken", json!("load\u{0}a"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "the token pattern must be reported: {message}"
    );
    assert!(
        message.contains("clientToken"),
        "the field must be named: {message}"
    );
}

#[tokio::test]
async fn a_configuration_string_containing_a_nul_byte_is_rejected() {
    // The recorded fields are stored as submitted, and PostgreSQL jsonb
    // refuses U+0000 in strings, so step 2 rejects the byte in any string
    // field of the two configurations.
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3SseKmsKeyId", json!("key\u{0}id"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, "S3 configuration contains an invalid character");
}

#[tokio::test]
async fn a_client_token_over_the_byte_cap_is_rejected() {
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_client_token_bytes", json!(64))]),
    );
    let body = with_top(start_body(), "ClientToken", json!("t".repeat(65)));
    let message = expect_validation(start(&ctx, body).await);
    assert_eq!(
        message,
        "ClientToken exceeds max_bulk_client_token_bytes (64 bytes)"
    );
}

#[tokio::test]
async fn a_client_token_exactly_at_the_byte_cap_is_accepted() {
    let h = Harness::new().await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_client_token_bytes", json!(64))]),
    );
    let token = "t".repeat(64);
    let body = with_top(start_body(), "ClientToken", json!(token));
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["ClientToken"], json!(token));
}

#[tokio::test]
async fn an_empty_output_type_list_is_rejected() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "OutputType", json!([]));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(message, EMPTY_OUTPUT_TYPE);
}

#[tokio::test]
async fn an_output_type_outside_the_enum_is_rejected() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "OutputType", json!(["EVERYTHING"]));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value '[EVERYTHING]' at \
         'outputS3Configuration.outputType' failed to satisfy constraint: Member must satisfy \
         constraint: [Member must satisfy enum value set: [ERROR, CONDITION_CHECK_FAILURE]]"
    );
}

#[tokio::test]
async fn an_sse_algorithm_outside_the_enum_is_rejected() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3SseAlgorithm", json!("XX"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value 'XX' at 'outputS3Configuration.s3SseAlgorithm' \
         failed to satisfy constraint: Member must satisfy enum value set: [AES256, KMS]"
    );
}

#[tokio::test]
async fn a_performance_tier_outside_the_enum_is_rejected() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "PerformanceTier", json!("NO_SUCH_TIER"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value 'NO_SUCH_TIER' at 'performanceTier' failed to \
         satisfy constraint: Member must satisfy enum value set: [ACCELERATED, STANDARD]"
    );
}

#[tokio::test]
async fn an_input_compression_type_outside_the_enum_is_rejected() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "InputCompressionType", json!("BZIP2"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "1 validation error detected: Value 'BZIP2' at \
         'inputS3Configuration.inputCompressionType' failed to satisfy constraint: Member must \
         satisfy enum value set: [ZSTD, NONE, GZIP]"
    );
}
