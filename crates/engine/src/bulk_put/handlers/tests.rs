// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Behaviour tests for the record renderers and the pagination cursor
//! codec, moved from the inline test module of the old handlers.rs file.

use super::*;
use serde_json::json;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "123456789012";
const OPERATION_ID: &str = "01789060589767-da9ddd4f";

fn record(status: &str) -> BulkOperationRecord {
    BulkOperationRecord {
        account_id: ACCOUNT.to_owned(),
        operation_id: OPERATION_ID.to_owned(),
        table_id: "tid-1".to_owned(),
        table_name: "orders".to_owned(),
        table_arn: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders"),
        status: status.to_owned(),
        client_token: Some("load-a".to_owned()),
        request_fingerprint: "f".repeat(64),
        input_config: json!({
            "S3Bucket": "loads",
            "S3KeyPrefix": "orders/",
            "InputFormat": "DYNAMODB_JSON",
            "InputCompressionType": "GZIP"
        }),
        output_config: json!({
            "OutputType": ["ERROR"],
            "S3Bucket": "loads",
            "S3KeyPrefix": "reports/"
        }),
        input_files: vec![InputFile {
            path: "orders/a.json".to_owned(),
            size: 4096,
            rows: None,
            etag: None,
            abandon: None,
        }],
        max_write_request_units: Some(1000),
        performance_tier: Some("STANDARD".to_owned()),
        start_time_ms: 1_789_060_589_767,
        end_time_ms: None,
        processed_item_count: 0,
        failed_item_count: 0,
        processed_input_bytes: 0,
        total_input_bytes: Some(4096),
        report_record_count: 0,
        report_bytes: 0,
        usage: json!({}),
        failure_code: None,
        failure_message: None,
        cancel_requested: false,
        cursor_file_index: 0,
        cursor_row_index: 0,
        lease_owner: None,
        lease_until_ms: None,
        run_context: None,
    }
}

#[test]
fn next_token_encodes_the_documented_hex_layout() {
    let token = encode_next_token(1_789_060_589_767, OPERATION_ID);
    // 16 digits of time plus 46 digits of id, padded with zeros to 112,
    // the service minimum (112 is a multiple of 16).
    assert_eq!(token.len(), 112);
    assert_eq!(&token[..16], "000001a08c5228c7");
    assert!(token.ends_with("00"));
    assert!(
        token
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    );
}

#[test]
fn next_token_round_trips() {
    let token = encode_next_token(1_789_060_589_767, OPERATION_ID);
    let cursor = decode_next_token(&token).expect("the encoded token must decode");
    assert_eq!(cursor.start_time_ms, 1_789_060_589_767);
    assert_eq!(cursor.operation_id, OPERATION_ID);
}

#[test]
fn next_token_answers_each_malformed_class() {
    fn token_error(token: &str) -> String {
        match decode_next_token(token).expect_err("the token must be rejected") {
            DynamoDbError::ValidationException(message) => message,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    let valid = encode_next_token(1_789_060_589_767, OPERATION_ID);
    assert_eq!(valid.len(), 112);

    // Pattern class: empty, a length not a multiple of 16, uppercase hex,
    // and non-hex characters.
    for bad in [
        String::new(),
        valid[..valid.len() - 1].to_owned(),
        valid.to_uppercase(),
        format!("{}zz", &valid[..110]),
    ] {
        assert_eq!(
            token_error(&bad),
            format!(
                "1 validation error detected: Value '{bad}' at 'nextToken' failed to satisfy \
                 constraint: Member must satisfy regular expression pattern: ([0-9a-f]{{16}})+"
            ),
            "pattern class: {bad:?}"
        );
    }

    // Model length maximum: over 1024 characters.
    let over = "0".repeat(1040);
    assert_eq!(
        token_error(&over),
        format!(
            "1 validation error detected: Value '{over}' at 'nextToken' failed to satisfy \
             constraint: Member must have length less than or equal to 1024"
        )
    );

    // Service minimum: a well-formed hex token under 112 characters.
    for bad in ["0123456789abcdef".to_owned(), valid[..96].to_owned()] {
        assert_eq!(
            token_error(&bad),
            format!(
                "Invalid Request: 1 validation error detected: Value '{bad}' at 'nextToken' \
                 failed to satisfy constraint: Member must have length greater than or equal \
                 to 112"
            ),
            "minimum class: {bad:?}"
        );
    }

    // A well-formed token of 112 or more that does not decode to a cursor.
    for bad in [
        format!("ffffffffffffffff{}", &valid[16..]), // time outside i64
        "0".repeat(112),                             // id all padding
        format!("{}{}", &valid[..16], "ff".repeat(48)), // id not UTF-8
    ] {
        assert_eq!(
            token_error(&bad),
            format!("Invalid Request: Provided nextToken is invalid: {bad}"),
            "not-a-cursor class: {bad:?}"
        );
    }
}

#[test]
fn describe_renders_an_in_progress_record_without_terminal_fields() {
    let description = describe(&record("IN_PROGRESS"), REGION).expect("the record renders");
    assert_eq!(
        description.bulk_operation_arn,
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/{OPERATION_ID}")
    );
    assert_eq!(
        description.bulk_operation_status,
        BulkOperationStatus::InProgress
    );
    let start = description.start_time.expect("start time is present");
    assert!((start - 1_789_060_589.767).abs() < 1e-6);
    assert_eq!(description.end_time, None);
    assert!(description.bulk_operation_usage.is_none());
    assert_eq!(description.failure_code, None);
    assert_eq!(description.failure_message, None);
    assert_eq!(description.client_token.as_deref(), Some("load-a"));
    assert_eq!(description.max_write_request_units, Some(1000));
    assert_eq!(
        description.performance_tier,
        Some(BulkOperationPerformanceTier::Standard)
    );
    assert_eq!(description.processed_item_count, Some(0));
    assert_eq!(description.total_input_bytes, Some(4096));
    let input = description
        .input_s3_configuration
        .expect("input configuration echoes");
    assert_eq!(input.s3_bucket, "loads");
    assert_eq!(input.s3_key_prefix.as_deref(), Some("orders/"));
    let output = description
        .output_s3_configuration
        .expect("output configuration echoes");
    assert_eq!(output.s3_bucket.as_deref(), Some("loads"));
}

#[test]
fn describe_renders_a_terminal_record_with_usage_and_failure_fields() {
    let mut terminal = record("FAILED");
    terminal.end_time_ms = Some(1_789_060_650_634);
    terminal.processed_item_count = 7;
    terminal.failed_item_count = 2;
    terminal.failure_code = Some("ValidationException".to_owned());
    terminal.failure_message = Some("Operation exceeded max_bulk_item_count (5 rows)".to_owned());
    terminal.usage = json!({
        "TableName": terminal.table_arn,
        "BulkPutBytes": 7168,
        "Table": {"BulkPutBytes": 7168},
        "GlobalSecondaryIndexes": {"gsi1": {"BulkPutBytes": 1024}},
        "LocalSecondaryIndexes": {},
        "VectorIndexes": {"vi1": {"BulkPutVectorBytes": 8192}}
    });
    let description = describe(&terminal, REGION).expect("the record renders");
    let end = description.end_time.expect("end time is present");
    assert!((end - 1_789_060_650.634).abs() < 1e-6);
    assert_eq!(description.processed_item_count, Some(7));
    assert_eq!(description.failed_item_count, Some(2));
    assert_eq!(
        description.failure_code.as_deref(),
        Some("ValidationException")
    );
    assert_eq!(
        description.failure_message.as_deref(),
        Some("Operation exceeded max_bulk_item_count (5 rows)")
    );
    let usage = description
        .bulk_operation_usage
        .expect("usage is present once terminal");
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].table_name, terminal.table_name);
    assert_eq!(usage[0].bulk_put_bytes, 7168);
    assert_eq!(usage[0].table.bulk_put_bytes, 7168);
    assert_eq!(
        usage[0].global_secondary_indexes["gsi1"].bulk_put_bytes,
        1024
    );
    assert!(usage[0].local_secondary_indexes.is_empty());
    assert_eq!(usage[0].vector_indexes["vi1"].bulk_put_vector_bytes, 8192);
}

#[test]
fn describe_renders_a_zeroed_usage_entry_for_a_terminal_record_with_empty_usage() {
    let mut terminal = record("CANCELLED");
    terminal.end_time_ms = Some(1_789_060_650_634);
    let description = describe(&terminal, REGION).expect("the record renders");
    let usage = description
        .bulk_operation_usage
        .expect("usage is present once terminal");
    assert_eq!(usage[0].bulk_put_bytes, 0);
    assert_eq!(usage[0].table_name, terminal.table_name);
    assert!(usage[0].global_secondary_indexes.is_empty());
}

#[test]
fn describe_reports_an_unknown_stored_status_as_an_internal_error() {
    let err = describe(&record("EXPLODED"), REGION).expect_err("the status must be refused");
    match err {
        DynamoDbError::InternalServerError(message) => {
            assert_eq!(message, "Internal server error");
        }
        other => panic!("expected InternalServerError, got {other:?}"),
    }
}

#[test]
fn configuration_size_cap_binds_on_the_serialized_bytes() {
    // A JSON string of n ASCII characters serializes to n + 2 bytes.
    let at_cap_halves = MAX_CONFIGURATION_BYTES / 2 - 2;
    let input = Value::String("a".repeat(at_cap_halves));
    let output = Value::String("b".repeat(at_cap_halves));
    assert!(validate_configuration_size(&input, &output).is_ok());
    let over = Value::String("b".repeat(at_cap_halves + 1));
    let err = validate_configuration_size(&input, &over)
        .expect_err("one byte past the cap must be refused");
    match err {
        DynamoDbError::ValidationException(message) => {
            assert_eq!(message, "S3 configuration exceeds 8192 bytes");
        }
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

// Fidelity: a Start that names a missing input bucket inserts the
// operation already terminal FAILED, before enumeration, so it carries
// no input total. The hosted service returns that FAILED operation
// with TotalInputBytes absent and the counters present at zero
// (measured on the service); describe renders the same shape.
#[test]
fn describe_omits_total_input_bytes_on_a_missing_bucket_failure() {
    let mut terminal = record("FAILED");
    terminal.end_time_ms = Some(terminal.start_time_ms); // born terminal
    terminal.total_input_bytes = None;
    terminal.processed_item_count = 0;
    terminal.failed_item_count = 0;
    terminal.failure_code = Some("S3NoSuchBucket".to_owned());
    terminal.failure_message = Some("The specified bucket does not exist".to_owned());

    let description = describe(&terminal, REGION).expect("the record renders");
    let wire = serde_json::to_value(&description).expect("the description serializes");

    assert_eq!(wire.get("ProcessedItemCount"), Some(&json!(0)));
    assert_eq!(wire.get("FailedItemCount"), Some(&json!(0)));
    assert!(
        wire.get("TotalInputBytes").is_none(),
        "the hosted service omits TotalInputBytes on a missing-bucket FAILED operation"
    );
    // The two byte members travel together: the missing-bucket description
    // the hosted service answered carries neither.
    assert!(
        wire.get("ProcessedInputBytes").is_none(),
        "ProcessedInputBytes is absent whenever TotalInputBytes is absent"
    );
}

// A failure on the input object count (measured on the service: 50,500
// objects under one prefix) runs for seconds on the hosted service and
// ends FAILED with neither byte member, the same shape as the missing
// bucket: the listing that would have fixed the total was never kept.
#[test]
fn describe_omits_both_byte_members_on_an_object_count_quota_failure() {
    let mut terminal = record("FAILED");
    terminal.end_time_ms = Some(terminal.start_time_ms + 15_386);
    terminal.total_input_bytes = Some(6_060_000);
    terminal.failure_code = Some("S3ObjectsCountLimitExceeded".to_owned());
    terminal.failure_message = Some(
        "You have reached the maximum number of objects allowed in a single bulk operation \
         for the specified prefix h1/, which is 50000. You can reach out to AWS Support to \
         request a quota increase."
            .to_owned(),
    );

    let description = describe(&terminal, REGION).expect("the record renders");
    let wire = serde_json::to_value(&description).expect("the description serializes");

    assert!(wire.get("TotalInputBytes").is_none());
    assert!(wire.get("ProcessedInputBytes").is_none());
    assert_eq!(wire.get("ProcessedItemCount"), Some(&json!(0)));
    assert_eq!(wire.get("FailedItemCount"), Some(&json!(0)));
}

// The per-object size quota keeps both: TotalInputBytes from the
// listing, ProcessedInputBytes 0.
#[test]
fn describe_keeps_both_byte_members_on_an_object_size_quota_failure() {
    let mut terminal = record("FAILED");
    terminal.end_time_ms = Some(terminal.start_time_ms + 8_700);
    terminal.total_input_bytes = Some(5_369_000_000);
    terminal.failure_code = Some("InputObjectTooLarge".to_owned());

    let description = describe(&terminal, REGION).expect("the record renders");
    let wire = serde_json::to_value(&description).expect("the description serializes");

    assert_eq!(wire.get("TotalInputBytes"), Some(&json!(5_369_000_000_u64)));
    assert_eq!(wire.get("ProcessedInputBytes"), Some(&json!(0)));
}

// The enumerated case keeps the field, including an enumerated total of
// zero (an empty input), so the absence above is tied to enumeration
// never having run, not to the value being zero.
#[test]
fn describe_keeps_a_zero_total_input_bytes_when_enumerated() {
    let mut enumerated = record("IN_PROGRESS");
    enumerated.total_input_bytes = Some(0);

    let description = describe(&enumerated, REGION).expect("the record renders");
    let wire = serde_json::to_value(&description).expect("the description serializes");

    assert_eq!(wire.get("TotalInputBytes"), Some(&json!(0)));
}
