// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the int32 members of `Scan` and `Query`: how the handlers read
//! them and how a member of another JSON type is reported.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{QueryInput, ScanInput};
use serde_json::{Value, json};

use super::{
    QUERY_RANGES, RangeField, SCAN_RANGES, int32_from_decimal_text, read_floats_as_written,
    saturate,
};
use crate::query_key_values::validate_query_request_body;
use crate::scan_request::validate_scan_request_body;

/// The `Limit`, `Segment` and `TotalSegments` a handler reads.
type Members = (Option<i64>, Option<i64>, Option<i64>);

/// A Scan body with `fields` added to the table name, written as JSON text so
/// a number keeps the form the request wrote (`1.5`, `1e2`), read as the
/// server reads it: parsed, then the float members read from their digits.
fn scan(fields: &str) -> Value {
    as_written("Scan", &format!(r#"{{"TableName": "t", {fields}}}"#))
}

/// `text` parsed, then the float range members of `operation` read from the
/// digits as written, as the server does before any check.
fn as_written(operation: &str, text: &str) -> Value {
    let mut body: Value = serde_json::from_str(text).expect("JSON");
    read_floats_as_written(operation, &mut body, text.as_bytes());
    body
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
        let check = if operation == "Scan" {
            validate_scan_request_body(&body)
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

/// The reader of a number's written digits: the decimal truncated toward
/// zero, then saturated to int32, whatever the nearest double is.
#[test]
fn a_decimal_text_is_truncated_toward_zero_and_saturated() {
    for (text, expected) in [
        ("0.99999999999999999999", Some(0)),
        ("-0.99999999999999999999", Some(0)),
        ("0.9999999999999999", Some(0)),
        ("2.99999999999999999999", Some(2)),
        ("1.99999999999999999999", Some(1)),
        ("-1.99999999999999999999", Some(-1)),
        ("1000000.99999999999999999999", Some(1_000_000)),
        ("1.5", Some(1)),
        ("-1.5", Some(-1)),
        ("-0.5", Some(0)),
        ("0.0", Some(0)),
        ("-0.0", Some(0)),
        ("1e2", Some(100)),
        ("1E+2", Some(100)),
        ("1.5e1", Some(15)),
        ("123e-1", Some(12)),
        ("5e-1", Some(0)),
        ("0.5e-3", Some(0)),
        ("2147483647.9", Some(i32::MAX)),
        ("2147483648.5", Some(i32::MAX)),
        ("-2147483649.5", Some(i32::MIN)),
        ("1e300", Some(i32::MAX)),
        ("-1e300", Some(i32::MIN)),
        ("0e300", Some(0)),
        ("1e99999999999999999999", None),
        ("x", None),
    ] {
        assert_eq!(int32_from_decimal_text(text), expected, "{text}");
    }
}

/// Measured 2026-11-24 05:41 UTC (us-east-1, each request twice, a table that
/// does not exist): `Limit: 0.99999999999999999999` on `Scan` and `Query` and
/// `Limit: -0.99999999999999999999` on `Scan` answer `Value '0' at 'limit'`,
/// `TotalSegments: 0.99999999999999999999` answers `Value '0' at
/// 'totalSegments'`, and `Segment: -0.99999999999999999999` of
/// `TotalSegments: 2` and a `Query` `Limit: 2.99999999999999999999` are
/// accepted: the service truncates the written decimal, where the body parser
/// reads the nearest double. The server reads such a member from its digits
/// before any check, so the checks and the handler get the service's value,
/// and the member stays a float. An integer member is not touched.
#[test]
fn a_float_member_is_read_from_the_digits_the_request_wrote() {
    let limit_0 = "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1";
    let total_0 = limit_0.replace("'limit'", "'totalSegments'");
    let query = |fields: &str| {
        as_written(
            "Query",
            &format!(
                r#"{{"TableName": "t", "KeyConditionExpression": "pk = :p", {fields}, "ExpressionAttributeValues": {{":p": {{"S": "p0"}}}}}}"#
            ),
        )
    };
    // (operation, the fields after TableName, the refusal, the members read)
    let rows: [(&str, &str, Option<&str>, Members); 7] = [
        (
            "Scan",
            r#""Limit": 0.99999999999999999999"#,
            Some(limit_0),
            (Some(0), None, None),
        ),
        (
            "Scan",
            r#""TotalSegments": 0.99999999999999999999"#,
            Some(total_0.as_str()),
            (None, None, Some(0)),
        ),
        (
            "Scan",
            r#""Limit": -0.99999999999999999999"#,
            Some(limit_0),
            (Some(0), None, None),
        ),
        (
            "Scan",
            r#""Limit": 2.99999999999999999999, "Segment": 1.99999999999999999999, "TotalSegments": 2"#,
            None,
            (Some(2), Some(1), Some(2)),
        ),
        (
            "Scan",
            r#""Segment": -0.99999999999999999999, "TotalSegments": 2"#,
            None,
            (None, Some(0), Some(2)),
        ),
        (
            "Query",
            r#""Limit": 0.99999999999999999999"#,
            Some(limit_0),
            (Some(0), None, None),
        ),
        (
            "Query",
            r#""Limit": 2.99999999999999999999"#,
            None,
            (Some(2), None, None),
        ),
    ];
    for (operation, fields, refusal, expected) in rows {
        let (mut body, ranges): (Value, &[RangeField]) = if operation == "Scan" {
            (scan(fields), &SCAN_RANGES)
        } else {
            (query(fields), &QUERY_RANGES)
        };
        let check = if operation == "Scan" {
            validate_scan_request_body(&body)
        } else {
            validate_query_request_body(&body)
        };
        let got = match check {
            Ok(()) => None,
            Err(DynamoDbError::ValidationException(msg)) => Some(msg),
            Err(other) => panic!("unexpected error {other:?}"),
        };
        assert_eq!(got.as_deref(), refusal, "{body}");
        saturate(&mut body, ranges).expect("JSON numbers");
        let read = (
            body.get("Limit").and_then(Value::as_i64),
            body.get("Segment").and_then(Value::as_i64),
            body.get("TotalSegments").and_then(Value::as_i64),
        );
        assert_eq!(read, expected, "{body}");
    }
    // A member written with a fraction stays a float, which keeps the request
    // on the older path (the refusals above are in the older form); an
    // integer member is left as the parser read it; and the raw body of
    // another operation is not read.
    let body = scan(r#""Limit": 2.99999999999999999999, "Segment": 1.5, "TotalSegments": 2"#);
    assert_eq!(body["Limit"].as_f64(), Some(2.0));
    assert_eq!(body["Segment"].as_f64(), Some(1.0));
    assert!(body["TotalSegments"].is_i64());
    let text = r#"{"TableName": "t", "Limit": 2.99999999999999999999}"#;
    let body = as_written("GetItem", text);
    assert_eq!(body["Limit"].as_f64(), Some(3.0));
}
