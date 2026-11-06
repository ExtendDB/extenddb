// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire names and enum acceptance for the bulk operation request and status types.

use super::*;

#[test]
fn start_input_deserializes_every_field_from_the_wire_names() {
    let input: StartBulkPutItemsInput = serde_json::from_value(full_start_body()).unwrap();
    assert_eq!(input.table_name, "orders");
    assert_eq!(input.client_token.as_deref(), Some("load-2026-09-10-a"));
    assert_eq!(input.max_write_request_units, Some(1000));
    assert_eq!(
        input.performance_tier,
        Some(BulkOperationPerformanceTier::Standard)
    );

    let in_cfg = &input.input_s3_configuration;
    assert_eq!(in_cfg.s3_bucket, "loads");
    assert_eq!(in_cfg.s3_key_prefix.as_deref(), Some("orders/2026-09-10/"));
    assert_eq!(in_cfg.input_format, BulkOperationInputFormat::DynamodbJson);
    assert_eq!(
        in_cfg.input_compression_type,
        Some(InputCompressionType::Gzip)
    );
    assert_eq!(in_cfg.s3_bucket_owner.as_deref(), Some("000000000000"));

    let out_cfg = &input.output_s3_configuration;
    assert_eq!(
        out_cfg.output_type,
        vec![OutputType::Error, OutputType::ConditionCheckFailure]
    );
    assert_eq!(out_cfg.s3_bucket.as_deref(), Some("loads"));
    assert_eq!(out_cfg.s3_key_prefix.as_deref(), Some("reports/"));
    assert_eq!(out_cfg.s3_bucket_owner.as_deref(), Some("000000000000"));
    assert_eq!(out_cfg.s3_sse_algorithm, Some(S3SseAlgorithm::Aes256));
    assert_eq!(out_cfg.s3_sse_kms_key_id.as_deref(), Some("alias/example"));
}

#[test]
fn start_input_without_optional_fields_deserializes_with_none() {
    let input: StartBulkPutItemsInput = serde_json::from_value(json!({
        "TableName": "orders",
        "InputS3Configuration": { "S3Bucket": "loads", "InputFormat": "DYNAMODB_JSON" },
        "OutputS3Configuration": { "OutputType": ["ERROR"] }
    }))
    .unwrap();
    assert_eq!(input.client_token, None);
    assert_eq!(input.max_write_request_units, None);
    assert_eq!(input.performance_tier, None);
    assert_eq!(input.input_s3_configuration.s3_key_prefix, None);
    assert_eq!(input.input_s3_configuration.input_compression_type, None);
    assert_eq!(input.input_s3_configuration.s3_bucket_owner, None);
    assert_eq!(input.output_s3_configuration.s3_bucket, None);
    assert_eq!(input.output_s3_configuration.s3_key_prefix, None);
    assert_eq!(input.output_s3_configuration.s3_bucket_owner, None);
    assert_eq!(input.output_s3_configuration.s3_sse_algorithm, None);
    assert_eq!(input.output_s3_configuration.s3_sse_kms_key_id, None);
}

#[test]
fn input_format_accepts_the_two_model_values() {
    assert_eq!(
        serde_json::from_value::<BulkOperationInputFormat>(json!("DYNAMODB_JSON")).unwrap(),
        BulkOperationInputFormat::DynamodbJson
    );
    assert_eq!(
        serde_json::from_value::<BulkOperationInputFormat>(json!("ION")).unwrap(),
        BulkOperationInputFormat::Ion
    );
}

#[test]
fn input_format_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<BulkOperationInputFormat>(json!("CSV"));
}

#[test]
fn compression_type_accepts_the_three_model_values() {
    assert_eq!(
        serde_json::from_value::<InputCompressionType>(json!("GZIP")).unwrap(),
        InputCompressionType::Gzip
    );
    assert_eq!(
        serde_json::from_value::<InputCompressionType>(json!("ZSTD")).unwrap(),
        InputCompressionType::Zstd
    );
    assert_eq!(
        serde_json::from_value::<InputCompressionType>(json!("NONE")).unwrap(),
        InputCompressionType::None
    );
}

#[test]
fn compression_type_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<InputCompressionType>(json!("BROTLI"));
}

#[test]
fn performance_tier_accepts_the_two_model_values() {
    assert_eq!(
        serde_json::from_value::<BulkOperationPerformanceTier>(json!("STANDARD")).unwrap(),
        BulkOperationPerformanceTier::Standard
    );
    assert_eq!(
        serde_json::from_value::<BulkOperationPerformanceTier>(json!("ACCELERATED")).unwrap(),
        BulkOperationPerformanceTier::Accelerated
    );
}

#[test]
fn performance_tier_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<BulkOperationPerformanceTier>(json!("TURBO"));
}

#[test]
fn output_type_accepts_the_two_model_values() {
    assert_eq!(
        serde_json::from_value::<OutputType>(json!("ERROR")).unwrap(),
        OutputType::Error
    );
    assert_eq!(
        serde_json::from_value::<OutputType>(json!("CONDITION_CHECK_FAILURE")).unwrap(),
        OutputType::ConditionCheckFailure
    );
}

#[test]
fn output_type_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<OutputType>(json!("WARNING"));
}

#[test]
fn sse_algorithm_accepts_the_two_model_values() {
    assert_eq!(
        serde_json::from_value::<S3SseAlgorithm>(json!("AES256")).unwrap(),
        S3SseAlgorithm::Aes256
    );
    assert_eq!(
        serde_json::from_value::<S3SseAlgorithm>(json!("KMS")).unwrap(),
        S3SseAlgorithm::Kms
    );
}

#[test]
fn sse_algorithm_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<S3SseAlgorithm>(json!("AES128"));
}

#[test]
fn is_terminal_is_true_for_exactly_the_four_terminal_statuses() {
    assert!(!BulkOperationStatus::InProgress.is_terminal());
    assert!(!BulkOperationStatus::Cancelling.is_terminal());
    assert!(BulkOperationStatus::Completed.is_terminal());
    assert!(BulkOperationStatus::CompletedWithErrors.is_terminal());
    assert!(BulkOperationStatus::Failed.is_terminal());
    assert!(BulkOperationStatus::Cancelled.is_terminal());
}

#[test]
fn status_serializes_to_the_six_wire_names() {
    let cases = [
        (BulkOperationStatus::InProgress, "IN_PROGRESS"),
        (BulkOperationStatus::Completed, "COMPLETED"),
        (
            BulkOperationStatus::CompletedWithErrors,
            "COMPLETED_WITH_ERRORS",
        ),
        (BulkOperationStatus::Cancelling, "CANCELLING"),
        (BulkOperationStatus::Cancelled, "CANCELLED"),
        (BulkOperationStatus::Failed, "FAILED"),
    ];
    for (status, wire) in cases {
        assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
    }
}

#[test]
fn list_input_deserializes_max_results_and_next_token() {
    let input: ListBulkOperationsInput = serde_json::from_value(json!({
        "MaxResults": 25,
        "NextToken": "0000018f6f7a2f47"
    }))
    .unwrap();
    assert_eq!(input.max_results, Some(25));
    assert_eq!(input.next_token.as_deref(), Some("0000018f6f7a2f47"));

    let empty: ListBulkOperationsInput = serde_json::from_value(json!({})).unwrap();
    assert_eq!(empty.max_results, None);
    assert_eq!(empty.next_token, None);
}

#[test]
fn describe_input_deserializes_the_arn() {
    let input: DescribeBulkOperationInput =
        serde_json::from_value(json!({ "BulkOperationArn": ARN })).unwrap();
    assert_eq!(input.bulk_operation_arn, ARN);
}

#[test]
fn cancel_input_deserializes_the_arn() {
    let input: CancelBulkOperationInput =
        serde_json::from_value(json!({ "BulkOperationArn": ARN })).unwrap();
    assert_eq!(input.bulk_operation_arn, ARN);
}

#[test]
fn status_accepts_the_six_wire_names() {
    let cases = [
        ("IN_PROGRESS", BulkOperationStatus::InProgress),
        ("COMPLETED", BulkOperationStatus::Completed),
        (
            "COMPLETED_WITH_ERRORS",
            BulkOperationStatus::CompletedWithErrors,
        ),
        ("CANCELLING", BulkOperationStatus::Cancelling),
        ("CANCELLED", BulkOperationStatus::Cancelled),
        ("FAILED", BulkOperationStatus::Failed),
    ];
    for (wire, status) in cases {
        assert_eq!(
            serde_json::from_value::<BulkOperationStatus>(json!(wire)).unwrap(),
            status
        );
    }
}

#[test]
fn status_rejects_a_value_outside_the_enum() {
    enum_rejection_message::<BulkOperationStatus>(json!("RUNNING"));
}

#[test]
fn negative_max_write_request_units_deserializes_for_the_handler_to_range_check() {
    // The range check (>= 1) is the handler's, at validation step 2, so the
    // wire field must accept a negative integer; an unsigned field would
    // reject it inside serde as a SerializationException instead of the
    // ValidationException the range rule produces.
    let mut body = full_start_body();
    body["MaxWriteRequestUnits"] = json!(-5);
    let input: StartBulkPutItemsInput = serde_json::from_value(body).unwrap();
    assert_eq!(input.max_write_request_units, Some(-5));
}

#[test]
fn negative_max_results_deserializes_for_the_handler_to_range_check() {
    // Same boundary as MaxWriteRequestUnits: 1 to 25 is enforced by the
    // handler, so serde must carry the negative value through.
    let input: ListBulkOperationsInput =
        serde_json::from_value(json!({ "MaxResults": -1 })).unwrap();
    assert_eq!(input.max_results, Some(-1));
}

#[test]
fn compression_type_serializes_to_the_wire_names() {
    assert_eq!(
        serde_json::to_value(InputCompressionType::Gzip).unwrap(),
        json!("GZIP")
    );
    assert_eq!(
        serde_json::to_value(InputCompressionType::Zstd).unwrap(),
        json!("ZSTD")
    );
    assert_eq!(
        serde_json::to_value(InputCompressionType::None).unwrap(),
        json!("NONE")
    );
}

#[test]
fn sse_algorithm_serializes_to_the_wire_names() {
    assert_eq!(
        serde_json::to_value(S3SseAlgorithm::Aes256).unwrap(),
        json!("AES256")
    );
    assert_eq!(
        serde_json::to_value(S3SseAlgorithm::Kms).unwrap(),
        json!("KMS")
    );
}

#[test]
fn input_format_serializes_to_the_wire_names() {
    assert_eq!(
        serde_json::to_value(BulkOperationInputFormat::DynamodbJson).unwrap(),
        json!("DYNAMODB_JSON")
    );
    assert_eq!(
        serde_json::to_value(BulkOperationInputFormat::Ion).unwrap(),
        json!("ION")
    );
}

#[test]
fn performance_tier_serializes_to_the_wire_names() {
    assert_eq!(
        serde_json::to_value(BulkOperationPerformanceTier::Standard).unwrap(),
        json!("STANDARD")
    );
    assert_eq!(
        serde_json::to_value(BulkOperationPerformanceTier::Accelerated).unwrap(),
        json!("ACCELERATED")
    );
}
