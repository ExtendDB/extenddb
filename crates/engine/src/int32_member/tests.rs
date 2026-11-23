// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the int32 members of `Scan` and `Query`: how the handlers read
//! them and how a member of another JSON type is reported.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{QueryInput, ScanInput};
use serde_json::{Value, json};

use super::{QUERY_RANGES, SCAN_RANGES, saturate};
use crate::query_key_values::validate_query_request_body;
use crate::scan_request::validate_scan_request_body;

/// A Scan body with `fields` added to the table name, written as JSON text so
/// a number keeps the form the request wrote (`1.5`, `1e2`).
fn scan(fields: &str) -> Value {
    serde_json::from_str(&format!(r#"{{"TableName": "t", {fields}}}"#)).expect("JSON")
}

/// The message of a `SerializationException`, or `None` for `Ok`.
fn shape_fault(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::SerializationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// Measured 2026-10-25 (us-east-1, each request twice): `Limit: 1.5` and
/// `Limit: 1.0` return one item, `Limit: 1e2`, `Limit: 2147483648` and
/// `Limit: 9223372036854775808` are accepted, and `Segment: 1.5` of
/// `TotalSegments: 2` returns the items of segment 1. The handler reads each
/// of the three members as the service does before it deserializes the body.
#[test]
fn the_handler_reads_the_range_members_truncated_and_saturated() {
    for (fields, expected) in [
        (r#""Limit": 1.5"#, (Some(1), None, None)),
        (r#""Limit": 1.0"#, (Some(1), None, None)),
        (r#""Limit": 1e2"#, (Some(100), None, None)),
        (r#""Limit": 2147483648"#, (Some(2_147_483_647), None, None)),
        (
            r#""Limit": 9223372036854775808"#,
            (Some(2_147_483_647), None, None),
        ),
        (
            r#""Segment": 1.5, "TotalSegments": 2"#,
            (None, Some(1), Some(2)),
        ),
        (
            r#""Segment": 999999.9, "TotalSegments": 1000000.5"#,
            (None, Some(999_999), Some(1_000_000)),
        ),
        (r#""Limit": null, "Segment": null"#, (None, None, None)),
    ] {
        let mut body = scan(fields);
        assert!(validate_scan_request_body(&body).is_ok(), "{fields}");
        saturate(&mut body, &SCAN_RANGES).expect("a JSON number");
        let input: ScanInput = serde_json::from_value(body).expect("deserializes");
        assert_eq!(
            (input.limit, input.segment, input.total_segments),
            expected,
            "{fields}"
        );
    }
    let mut body = json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p", "Limit": 2.5,
        "ExpressionAttributeValues": {":p": {"S": "x"}},
    });
    assert!(validate_query_request_body(&body).is_ok());
    saturate(&mut body, &QUERY_RANGES).expect("a JSON number");
    let input: QueryInput = serde_json::from_value(body).expect("deserializes");
    assert_eq!(input.limit, Some(2));
}

/// Measured 2026-11-23 18:45 UTC (us-east-1, each request twice), each member
/// beside `Select: "BAD"`: the service reports the member's JSON type ahead of
/// the enum, with these texts, and no message for an array. The handler's
/// reader gives the same texts for a body that passes every check.
#[test]
fn a_range_member_of_another_json_type_is_a_shape_fault_with_the_service_text() {
    let string = "STRING_VALUE cannot be converted to Integer";
    let rows: [(Value, &str, &str); 4] = [
        (
            json!({"TableName": "t", "KeyConditionExpression": "pk = :p", "Limit": "5",
                   "Select": "BAD", "ExpressionAttributeValues": {":p": {"S": "p0"}}}),
            "Query",
            string,
        ),
        (
            json!({"TableName": "t", "Segment": "1", "TotalSegments": 2, "Select": "BAD"}),
            "Scan",
            string,
        ),
        (
            json!({"TableName": "t", "Limit": true, "Select": "BAD"}),
            "Scan",
            "TRUE_VALUE cannot be converted to Integer",
        ),
        (
            json!({"TableName": "t", "Limit": [1], "Select": "BAD"}),
            "Scan",
            "",
        ),
    ];
    for (body, operation, expected) in rows {
        // On Scan the Select enum of a body whose values the service can read
        // is a check of the handler (see `scan_request::SelectBody`), so the
        // request-time checks pass and the handler's read reports the member.
        let check = if operation == "Scan" {
            validate_scan_request_body(&body)
                .and_then(|()| saturate(&mut body.clone(), &SCAN_RANGES))
        } else {
            validate_query_request_body(&body)
        };
        assert_eq!(shape_fault(check).as_deref(), Some(expected), "{body}");
        let mut accepted = body.clone();
        accepted["Select"] = json!("COUNT");
        let read = if operation == "Scan" {
            validate_scan_request_body(&accepted)
                .and_then(|()| saturate(&mut accepted, &SCAN_RANGES))
        } else {
            validate_query_request_body(&accepted)
                .and_then(|()| saturate(&mut accepted, &QUERY_RANGES))
        };
        assert_eq!(shape_fault(read).as_deref(), Some(expected), "{accepted}");
    }
    // A body that passes every check is not deserialized by the checks; the
    // handler's read reports the member.
    let mut body = json!({"TableName": "t", "Limit": false});
    assert!(validate_scan_request_body(&body).is_ok());
    assert_eq!(
        shape_fault(saturate(&mut body, &SCAN_RANGES)).as_deref(),
        Some("FALSE_VALUE cannot be converted to Integer")
    );
}
