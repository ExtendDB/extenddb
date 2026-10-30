// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The range constraints on `Limit`, `Segment` and `TotalSegments`.
//!
//! The rows in every test but the last are requests sent to the service on
//! 2026-10-25 (us-east-1, 09:23 to 12:56 UTC, each request twice, a Scan of an
//! existing table) with the answers it gave. The last test applies the same
//! rules to requests that were not sent.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use super::validate_scan_request_body;

const LIMIT: &str = "Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";
const SEGMENT_LOW: &str = "Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0";
const SEGMENT_HIGH: &str = "Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999";
const TOTAL_LOW: &str = "Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1";
const TOTAL_HIGH: &str = "Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000";

fn check(body: &Value) -> Option<String> {
    match validate_scan_request_body(body) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// A Scan body with `fields` added to the table name, written as JSON text so
/// a number keeps the form the request wrote (`0.0`, `1e2`).
fn scan(fields: &str) -> Value {
    serde_json::from_str(&format!(r#"{{"TableName": "t", {fields}}}"#)).expect("JSON")
}

fn one(clause: &str) -> String {
    format!("1 validation error detected: {clause}")
}

fn older(value: &str, field: &str, constraint: &str) -> String {
    format!(
        "Value '{value}' at '{field}' failed to satisfy constraint: Member must have value {constraint}"
    )
}

#[test]
fn integers_inside_int32_answer_the_newer_form_in_the_order_limit_segment_total_segments() {
    let cases = [
        (r#""Limit": 0"#, one(LIMIT)),
        (r#""Limit": -2147483648"#, one(LIMIT)),
        (r#""Segment": -1"#, one(SEGMENT_LOW)),
        (r#""Segment": -1, "TotalSegments": 2"#, one(SEGMENT_LOW)),
        (
            r#""Segment": 1000000, "TotalSegments": 1000000"#,
            one(SEGMENT_HIGH),
        ),
        (r#""TotalSegments": 0"#, one(TOTAL_LOW)),
        (r#""Segment": 0, "TotalSegments": 0"#, one(TOTAL_LOW)),
        (r#""Segment": 0, "TotalSegments": -1"#, one(TOTAL_LOW)),
        (r#""TotalSegments": 1000001"#, one(TOTAL_HIGH)),
        (
            r#""Limit": 0, "Segment": -1, "TotalSegments": 1000001"#,
            format!("3 validation errors detected: {LIMIT}; {SEGMENT_LOW}; {TOTAL_HIGH}"),
        ),
        (
            r#""Limit": 0, "TotalSegments": 0"#,
            format!("2 validation errors detected: {LIMIT}; {TOTAL_LOW}"),
        ),
        (
            r#""Segment": -1, "TotalSegments": 1000001"#,
            format!("2 validation errors detected: {SEGMENT_LOW}; {TOTAL_HIGH}"),
        ),
        (
            r#""Segment": 1000000, "TotalSegments": 0"#,
            format!("2 validation errors detected: {SEGMENT_HIGH}; {TOTAL_LOW}"),
        ),
    ];
    for (fields, expected) in cases {
        assert_eq!(check(&scan(fields)), Some(expected), "{fields}");
    }
}

#[test]
fn a_float_or_a_value_outside_int32_answers_the_older_form_with_limit_last() {
    let at_least_one = "greater than or equal to 1";
    let cases = [
        (
            r#""Limit": -2147483649"#,
            one(&older("-2147483648", "limit", at_least_one)),
        ),
        (r#""Limit": 0.0"#, one(&older("0", "limit", at_least_one))),
        (
            r#""Segment": 0, "TotalSegments": 2147483648"#,
            one(&older(
                "2147483647",
                "totalSegments",
                "less than or equal to 1000000",
            )),
        ),
        (
            r#""Limit": -2147483649, "Segment": -1"#,
            format!(
                "2 validation errors detected: {}; {}",
                older("-1", "segment", "greater than or equal to 0"),
                older("-2147483648", "limit", at_least_one)
            ),
        ),
        (
            r#""TotalSegments": 1000001, "Limit": 0.0"#,
            format!(
                "2 validation errors detected: {}; {}",
                older("1000001", "totalSegments", "less than or equal to 1000000"),
                older("0", "limit", at_least_one)
            ),
        ),
    ];
    for (fields, expected) in cases {
        assert_eq!(check(&scan(fields)), Some(expected), "{fields}");
    }
}

#[test]
fn floats_and_out_of_range_integers_that_satisfy_the_constraints_are_accepted() {
    for fields in [
        r#""Limit": 2147483648"#,
        r#""Limit": 9223372036854775808"#,
        r#""Limit": 1.5"#,
        r#""Limit": 1.0"#,
        r#""Limit": 1e2"#,
        r#""Segment": 1.5, "TotalSegments": 2"#,
    ] {
        assert_eq!(check(&scan(fields)), None, "{fields}");
    }
}

#[test]
fn an_entry_that_is_not_one_attribute_value_switches_to_the_older_form() {
    let older_limit = one(&older("0", "limit", "greater than or equal to 1"));
    for entry in [r"{}", r#"{"X": "1"}"#, r#"{"S": "x", "N": "1"}"#] {
        let fields = format!(r#""Limit": 0, "ExpressionAttributeValues": {{":a": {entry}}}"#);
        assert_eq!(
            check(&scan(&fields)).as_deref(),
            Some(older_limit.as_str()),
            "{entry}"
        );
    }
    // An empty or duplicate NS is read as an AttributeValue: the newer form.
    for entry in [r#"{"NS": []}"#, r#"{"NS": ["1", "1"]}"#] {
        let fields = format!(r#""Limit": 0, "ExpressionAttributeValues": {{":a": {entry}}}"#);
        assert_eq!(check(&scan(&fields)), Some(one(LIMIT)), "{entry}");
    }
}

/// Requests the service was not sent. The answers follow from the measured
/// ones: every fault in one message, in the order Limit, Segment,
/// TotalSegments, and nothing reported for a field inside its bounds.
#[test]
fn combinations_the_service_was_not_sent_follow_the_measured_rules() {
    let cases = [
        (r#""Limit": -1"#, one(LIMIT)),
        (r#""Segment": 0, "TotalSegments": 1000001"#, one(TOTAL_HIGH)),
        (
            r#""Limit": 0, "TotalSegments": 1000001"#,
            format!("2 validation errors detected: {LIMIT}; {TOTAL_HIGH}"),
        ),
        (
            r#""Limit": 0, "Segment": -1"#,
            format!("2 validation errors detected: {LIMIT}; {SEGMENT_LOW}"),
        ),
        (
            r#""Limit": -1, "Segment": 1000000"#,
            format!("2 validation errors detected: {LIMIT}; {SEGMENT_HIGH}"),
        ),
        (
            r#""Limit": 0, "Segment": 1000000, "TotalSegments": 0"#,
            format!("3 validation errors detected: {LIMIT}; {SEGMENT_HIGH}; {TOTAL_LOW}"),
        ),
        // A Segment that is not below TotalSegments is checked later; the
        // Limit fault is reported alone.
        (
            r#""Limit": 0, "Segment": 5, "TotalSegments": 2"#,
            one(LIMIT),
        ),
    ];
    for (fields, expected) in cases {
        assert_eq!(check(&scan(fields)), Some(expected), "{fields}");
    }
    for fields in [
        r#""Limit": 2147483647"#,
        r#""Limit": null"#,
        r#""Segment": 999999, "TotalSegments": 1000000"#,
    ] {
        assert_eq!(check(&scan(fields)), None, "{fields}");
    }
}
