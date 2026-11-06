// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared fixtures for the bulk operation type tests; the cases live in
//! `wire` (enums, inputs, status) and `description` (description, usage,
//! summary and output serialization).

use super::*;
use serde_json::json;
use std::collections::HashMap;

mod description;
mod wire;

pub(super) const ARN: &str =
    "arn:aws:dynamodb:us-east-1:000000000000:bulk-operation/01789060589767-da9ddd4f";

pub(super) const TABLE_ARN: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/orders";

pub(super) fn full_start_body() -> serde_json::Value {
    json!({
        "TableName": "orders",
        "ClientToken": "load-2026-09-10-a",
        "InputS3Configuration": {
            "S3Bucket": "loads",
            "S3KeyPrefix": "orders/2026-09-10/",
            "InputFormat": "DYNAMODB_JSON",
            "InputCompressionType": "GZIP",
            "S3BucketOwner": "000000000000"
        },
        "OutputS3Configuration": {
            "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"],
            "S3Bucket": "loads",
            "S3KeyPrefix": "reports/",
            "S3BucketOwner": "000000000000",
            "S3SseAlgorithm": "AES256",
            "S3SseKmsKeyId": "alias/example"
        },
        "MaxWriteRequestUnits": 1000,
        "PerformanceTier": "STANDARD"
    })
}

pub(super) fn input_configuration() -> InputS3Configuration {
    serde_json::from_value(full_start_body()["InputS3Configuration"].clone()).unwrap()
}

pub(super) fn output_configuration() -> OutputS3Configuration {
    serde_json::from_value(full_start_body()["OutputS3Configuration"].clone()).unwrap()
}

pub(super) fn minimal_description() -> BulkOperationDescription {
    BulkOperationDescription {
        bulk_operation_arn: ARN.to_owned(),
        bulk_operation_status: BulkOperationStatus::InProgress,
        table_details: Vec::new(),
        table_arn: None,
        table_id: None,
        client_token: None,
        input_s3_configuration: None,
        output_s3_configuration: None,
        start_time: None,
        end_time: None,
        processed_item_count: None,
        failed_item_count: None,
        processed_input_bytes: None,
        total_input_bytes: None,
        bulk_operation_usage: None,
        failure_code: None,
        failure_message: None,
        max_write_request_units: None,
        performance_tier: None,
    }
}

pub(super) fn full_description() -> BulkOperationDescription {
    BulkOperationDescription {
        bulk_operation_arn: ARN.to_owned(),
        bulk_operation_status: BulkOperationStatus::Completed,
        table_details: vec![BulkOperationTableDetail {
            table_name: "orders".to_owned(),
            table_id: "0ec72c3d-0000-0000-0000-000000000000".to_owned(),
        }],
        table_arn: Some(TABLE_ARN.to_owned()),
        table_id: Some("0ec72c3d-0000-0000-0000-000000000000".to_owned()),
        client_token: Some("load-2026-09-10-a".to_owned()),
        input_s3_configuration: Some(input_configuration()),
        output_s3_configuration: Some(output_configuration()),
        start_time: Some(1_789_060_589.767),
        end_time: Some(1_789_060_650.634),
        processed_item_count: Some(25),
        failed_item_count: Some(0),
        processed_input_bytes: Some(4096),
        total_input_bytes: Some(4096),
        bulk_operation_usage: Some(vec![usage()]),
        failure_code: Some("ValidationException".to_owned()),
        failure_message: Some("Operation exceeded max_bulk_item_count (100 rows)".to_owned()),
        max_write_request_units: Some(1000),
        performance_tier: Some(BulkOperationPerformanceTier::Standard),
    }
}

pub(super) fn usage() -> BulkOperationUsage {
    let mut gsis = HashMap::new();
    gsis.insert(
        "gsi1".to_owned(),
        BulkOperationBytes {
            bulk_put_bytes: 4096,
        },
    );
    let mut vectors = HashMap::new();
    vectors.insert(
        "vi1".to_owned(),
        BulkOperationVectorBytes {
            bulk_put_vector_bytes: 8192,
        },
    );
    BulkOperationUsage {
        table_name: "orders".to_owned(),
        bulk_put_bytes: 25600,
        table: BulkOperationBytes {
            bulk_put_bytes: 25600,
        },
        global_secondary_indexes: gsis,
        local_secondary_indexes: HashMap::new(),
        vector_indexes: vectors,
    }
}

/// The message pattern `deserialize_error` in the engine keys on to classify
/// an enum violation as a `ValidationException` rather than a
/// `SerializationException`. A derive-produced "unknown variant" message does
/// not carry it.
pub(super) fn enum_rejection_message<T: serde::de::DeserializeOwned + std::fmt::Debug>(
    value: serde_json::Value,
) -> String {
    let message = serde_json::from_value::<T>(value)
        .expect_err("a value outside the enum set must be rejected")
        .to_string();
    assert!(
        message.contains("validation error detected"),
        "enum rejection must carry the validation-error marker, got: {message}"
    );
    assert!(
        message.contains("Member must satisfy enum value set"),
        "enum rejection must name the enum value set, got: {message}"
    );
    message
}
