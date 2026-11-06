// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

mod handlers;
mod runner;
mod worker;

use crate::bulk_put::{arn, fingerprint, rows};

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::AttributeValue;
use serde_json::json;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "123456789012";
const VALID_ARN: &str =
    "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f";

/// Layer 1 of the `DescribeBulkOperation` ARN table.
const NOT_AN_ARN: &str = "Invalid Bulk operation ARN";
/// Layer 2 of the `DescribeBulkOperation` ARN table.
const BAD_ID: &str = "Invalid Request: Bulk operation ARN is invalid.";
/// The one message every parse-level row fault carries.
const ABANDON: &str = "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, NS, \
                       BS]. Remainder of the file will not be processed.";

fn validation_message(err: &DynamoDbError) -> &str {
    match err {
        DynamoDbError::ValidationException(message) => message,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

fn parse_arn_message(input: &str) -> String {
    let err = arn::parse_arn(input).expect_err("this ARN must be rejected");
    validation_message(&err).to_owned()
}

fn abandon_message(line: &str) -> String {
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Parse(message)) => message,
        other => panic!("expected a parse-level error, got {other:?}"),
    }
}

/// One attribute value nested `depth` map levels below the item.
fn nested_item_line(depth: usize) -> String {
    let mut value = String::from(r#"{"S":"leaf"}"#);
    for _ in 0..depth {
        value = format!(r#"{{"M":{{"k":{value}}}}}"#);
    }
    format!(r#"{{"Item":{{"pk":{{"S":"a"}},"deep":{value}}}}}"#)
}

fn item_message(line: &str) -> String {
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Item(message)) => message,
        other => panic!("expected an item-level error, got {other:?}"),
    }
}

// Request fingerprint.

fn base_input() -> serde_json::Value {
    json!({
        "S3Bucket": "loads",
        "S3KeyPrefix": "orders/2026-09-10/",
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "GZIP"
    })
}

fn base_output() -> serde_json::Value {
    json!({
        "S3Bucket": "loads",
        "S3KeyPrefix": "reports/",
        "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"]
    })
}

const TABLE_ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/orders";

fn base_fingerprint() -> String {
    fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    )
}

mod arn_parsing;
mod request_fingerprint;
mod row_parsing;
