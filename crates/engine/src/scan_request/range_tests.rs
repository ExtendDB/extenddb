// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The range constraints on `Limit`, `Segment` and `TotalSegments`.
//!
//! Every row is a request sent to the service (us-east-1, each request twice,
//! a Scan of an existing table) with the answer it gave: on 2026-10-25 between
//! 09:23 and 12:56 UTC, and on 2026-10-30 at 12:40 UTC, 2026-10-31 at 12:36
//! UTC, 2026-10-31 between 23:54 and 23:56 UTC, 2026-11-12 or 2026-11-23
//! (a table that does not exist) where a test says so.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::ScanInput;
use serde_json::Value;

use super::validate_scan_request_body;
use crate::int32_member::{SCAN_RANGES, read_floats_as_written, saturate};

const LIMIT: &str = "Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";
const SEGMENT_LOW: &str = "Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0";
const SEGMENT_HIGH: &str = "Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999";
const TOTAL_LOW: &str = "Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1";
const TOTAL_HIGH: &str = "Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000";
const SELECT_BAD: &str = "Value 'BAD' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]";
const RCC_BAD: &str = "Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";

fn check(body: &Value) -> Option<String> {
    match validate_scan_request_body(body) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// A Scan body with `fields` added to the table name, written as JSON text so
/// a number keeps the form the request wrote (`0.0`, `1e2`), read as the
/// server reads it: parsed, then the float range members read from their
/// digits (see `int32_member::read_floats_as_written`).
fn scan(fields: &str) -> Value {
    let text = format!(r#"{{"TableName": "t", {fields}}}"#);
    let mut body: Value = serde_json::from_str(&text).expect("JSON");
    read_floats_as_written("Scan", &mut body, text.as_bytes());
    body
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

/// Measured 2026-10-25, 2026-10-30 and 2026-11-24 05:41 UTC (the long
/// fractions, a table that does not exist): a float or a value outside int32
/// takes the older form, the value echoed as the service reads it, a
/// fraction truncated toward zero whatever the nearest double is.
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
            r#""Limit": 0.99999999999999999999"#,
            one(&older("0", "limit", at_least_one)),
        ),
        (
            r#""TotalSegments": 0.99999999999999999999"#,
            one(&older("0", "totalSegments", at_least_one)),
        ),
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

/// Measured 2026-10-25: `Limit: 1.5` and `Limit: 1.0` return one item,
/// `Limit: 1e2`, `Limit: 2147483648` and `Limit: 9223372036854775808` are
/// accepted, and `Segment: 1.5` of `TotalSegments: 2` returns the items of
/// segment 1. Measured 2026-11-24 05:41 UTC (a table that does not exist):
/// `Segment: -0.99999999999999999999` of `TotalSegments: 2` is accepted, and a
/// `Query` `Limit: 2.99999999999999999999` is, where the nearest double
/// reads as -1 and 3. The handler reads the members as the service does.
#[test]
fn floats_and_out_of_range_integers_that_satisfy_the_constraints_are_accepted() {
    for (fields, limit, segment) in [
        (r#""Limit": 2147483648"#, Some(2_147_483_647), None),
        (r#""Limit": 9223372036854775808"#, Some(2_147_483_647), None),
        (r#""Limit": 1.5"#, Some(1), None),
        (r#""Limit": 1.0"#, Some(1), None),
        (r#""Limit": 1e2"#, Some(100), None),
        (r#""Limit": 2.99999999999999999999"#, Some(2), None),
        (r#""Segment": 1.5, "TotalSegments": 2"#, None, Some(1)),
        (
            r#""Segment": 1.99999999999999999999, "TotalSegments": 2"#,
            None,
            Some(1),
        ),
        (
            r#""Segment": -0.99999999999999999999, "TotalSegments": 2"#,
            None,
            Some(0),
        ),
    ] {
        let mut body = scan(fields);
        assert_eq!(check(&body), None, "{fields}");
        saturate(&mut body, &SCAN_RANGES).expect("a JSON number");
        let input: ScanInput = serde_json::from_value(body).expect("deserializes");
        assert_eq!((input.limit, input.segment), (limit, segment), "{fields}");
    }
}

/// Measured 2026-11-12 (demohan, each row twice) and 2026-11-23 14:51 UTC: a
/// `Limit` that is a float or outside int32 puts a `Segment` fault in the
/// older form, also when the `Limit` satisfies its constraint; a `Segment`
/// outside int32 is echoed saturated; and with every member inside int32 the
/// newer form holds.
#[test]
fn a_float_or_saturated_limit_switches_a_segment_fault_to_the_older_form() {
    let segment_low = one(&older("-1", "segment", "greater than or equal to 0"));
    for fields in [
        r#""Limit": 1.5, "Segment": -1, "TotalSegments": 2"#,
        r#""Limit": 2147483648, "Segment": -1, "TotalSegments": 2"#,
        r#""Limit": 9223372036854775808, "Segment": -1, "TotalSegments": 2"#,
    ] {
        assert_eq!(
            check(&scan(fields)).as_deref(),
            Some(segment_low.as_str()),
            "{fields}"
        );
    }
    assert_eq!(
        check(&scan(
            r#""Segment": 18446744073709551615, "TotalSegments": 2"#
        )),
        Some(one(&older(
            "2147483647",
            "segment",
            "less than or equal to 999999"
        )))
    );
    assert_eq!(
        check(&scan(
            r#""Limit": 2147483647, "Segment": -1, "TotalSegments": 2"#
        )),
        Some(one(SEGMENT_LOW))
    );
}

/// Measured 2026-11-23 14:51 and 18:45 UTC (each request twice, a table that
/// does not exist): in the older form the service puts the `Select` and
/// `ReturnConsumedCapacity` enum clauses in the one message with the range
/// clauses, in the order select, totalSegments, returnConsumedCapacity,
/// segment, limit, also when only the enum clauses apply, and ahead of the
/// parameter mix and the names checks. A float `Limit` that satisfies its
/// constraint beside `{"N": "bob"}` answers the wrapped numeric text. In the
/// newer form the enums keep their places: a range fault alone, then
/// `ReturnConsumedCapacity` alone.
#[test]
fn the_enum_clauses_sit_inside_the_older_form_message() {
    let limit_0 = older("0", "limit", "greater than or equal to 1");
    let limit_min = older("-2147483648", "limit", "greater than or equal to 1");
    let scan_filter =
        r#""ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}}"#;
    let cases = [
        (
            r#""Limit": 0.0, "Select": "BAD""#.to_owned(),
            format!("2 validation errors detected: {SELECT_BAD}; {limit_0}"),
        ),
        (
            r#""Limit": 0, "ReturnConsumedCapacity": "BAD", "ExpressionAttributeValues": {":a": {}}"#
                .to_owned(),
            format!("2 validation errors detected: {RCC_BAD}; {limit_0}"),
        ),
        (
            r#""Limit": 0.0, "Segment": -1, "TotalSegments": 1000001, "Select": "BAD", "ReturnConsumedCapacity": "BAD""#.to_owned(),
            format!(
                "5 validation errors detected: {SELECT_BAD}; {}; {RCC_BAD}; {}; {limit_0}",
                older("1000001", "totalSegments", "less than or equal to 1000000"),
                older("-1", "segment", "greater than or equal to 0")
            ),
        ),
        (
            r#""Select": "BAD", "ReturnConsumedCapacity": "BAD", "FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}}"#.to_owned(),
            format!("2 validation errors detected: {SELECT_BAD}; {RCC_BAD}"),
        ),
        (
            r#""Limit": 1.5, "Select": "BAD", "ReturnConsumedCapacity": "BAD""#.to_owned(),
            format!("2 validation errors detected: {SELECT_BAD}; {RCC_BAD}"),
        ),
        (
            r#""Limit": -2147483649, "ReturnConsumedCapacity": "bad""#.to_owned(),
            format!(
                "2 validation errors detected: {}; {limit_min}",
                RCC_BAD.replace("'BAD'", "'bad'")
            ),
        ),
        // The enum clause ahead of the parameter mix, and the range clause
        // ahead of the mix and of an invalid names key.
        (
            format!(
                r#""Limit": 1.5, "Select": "BAD", {scan_filter}, "FilterExpression": "a = :a", "ExpressionAttributeValues": {{":a": {{"S": "x"}}}}"#
            ),
            one(SELECT_BAD),
        ),
        (
            format!(
                r#""Limit": 0.0, {scan_filter}, "FilterExpression": "a = :a", "ExpressionAttributeValues": {{":a": {{"S": "x"}}}}"#
            ),
            one(&limit_0),
        ),
        (
            r#""Limit": 0.0, "ExpressionAttributeNames": {"a": "b"}"#.to_owned(),
            one(&limit_0),
        ),
        (
            r#""Limit": 1.5, "FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "bob"}}"#.to_owned(),
            "ExpressionAttributeValues contains invalid value: The parameter cannot be converted to a numeric value: bob for key :n".to_owned(),
        ),
        // The newer form: the range fault alone, then ReturnConsumedCapacity alone.
        (
            r#""FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "bob"}}, "Segment": -1, "ReturnConsumedCapacity": "NOPE""#.to_owned(),
            one(SEGMENT_LOW),
        ),
        (
            r#""Limit": 0, "Select": "NOPE", "ReturnConsumedCapacity": "NOPE""#.to_owned(),
            one(LIMIT),
        ),
        (
            r#""Select": "NOPE", "ReturnConsumedCapacity": "NOPE""#.to_owned(),
            one(&RCC_BAD.replace("'BAD'", "'NOPE'")),
        ),
    ];
    for (fields, expected) in cases {
        assert_eq!(check(&scan(&fields)), Some(expected), "{fields}");
    }
}

/// Measured 2026-11-12 (demohan, each row twice) and 2026-11-23 14:51 UTC: a
/// null is a value the service cannot read as one type, as an
/// `ExpressionAttributeValues` entry, an `L` member of one or a payload in an
/// `ExclusiveStartKey` value, so a range fault beside it takes the older form.
/// `{"NULL": true}`, `{"L": []}` and a null `ExclusiveStartKey` member keep
/// the newer form.
#[test]
fn a_null_is_a_value_the_service_cannot_read() {
    let older_limit = one(&older("0", "limit", "greater than or equal to 1"));
    for fields in [
        r#""Limit": 0, "ExpressionAttributeValues": {":p": null}"#,
        r#""Limit": 0, "ExpressionAttributeValues": {":p": {"L": [null]}}"#,
        r#""Limit": 0, "ExclusiveStartKey": {"pk": {"S": null}, "sk": {"S": "s0"}}"#,
    ] {
        assert_eq!(
            check(&scan(fields)).as_deref(),
            Some(older_limit.as_str()),
            "{fields}"
        );
    }
    assert_eq!(
        check(&scan(
            r#""Segment": -1, "TotalSegments": 2, "ExpressionAttributeValues": {":p": null}"#
        )),
        Some(one(&older("-1", "segment", "greater than or equal to 0")))
    );
    for fields in [
        r#""Limit": 0, "ExpressionAttributeValues": {":p": {"NULL": true}}"#,
        r#""Limit": 0, "ExpressionAttributeValues": {":p": {"L": []}}"#,
        r#""Limit": 0, "ExclusiveStartKey": null"#,
    ] {
        assert_eq!(check(&scan(fields)), Some(one(LIMIT)), "{fields}");
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

/// Measured 2026-10-30 12:40 UTC: a fault in each of the three fields in the
/// older form is reported `totalSegments`, `segment`, `limit`, whichever field
/// is the float.
#[test]
fn three_older_form_faults_are_reported_total_segments_segment_limit() {
    let expected = format!(
        "3 validation errors detected: {}; {}; {}",
        older("1000001", "totalSegments", "less than or equal to 1000000"),
        older("-1", "segment", "greater than or equal to 0"),
        older("0", "limit", "greater than or equal to 1")
    );
    for fields in [
        r#""Limit": 0.0, "Segment": -1, "TotalSegments": 1000001"#,
        r#""Limit": 0, "Segment": -1, "TotalSegments": 1000001.0"#,
    ] {
        assert_eq!(
            check(&scan(fields)).as_deref(),
            Some(expected.as_str()),
            "{fields}"
        );
    }
}

/// Measured 2026-10-30 12:40 UTC: a negative fraction is truncated toward
/// zero, and `-0` is read as 0. Measured 2026-11-24 05:41 UTC:
/// `-0.99999999999999999999` is read as 0 too, where the nearest double is -1.
#[test]
fn a_negative_fraction_is_truncated_toward_zero() {
    for (fields, value) in [
        (r#""Limit": -1.5"#, "-1"),
        (r#""Limit": -0.5"#, "0"),
        (r#""Limit": -0"#, "0"),
        (r#""Limit": -0.99999999999999999999"#, "0"),
    ] {
        assert_eq!(
            check(&scan(fields)),
            Some(one(&older(value, "limit", "greater than or equal to 1"))),
            "{fields}"
        );
    }
}

/// Measured 2026-10-30 12:40 UTC: an unreadable member of an `L` or an `M`
/// entry, and a lower-case type descriptor, switch to the older form. Each
/// entry sits under `:d` beside a filter that reads `:p`, as sent. Measured
/// 2026-11-12: an `L` nested in an `L` does the same.
#[test]
fn an_unreadable_member_of_a_list_or_map_entry_switches_to_the_older_form() {
    let older_limit = one(&older("0", "limit", "greater than or equal to 1"));
    for entry in [
        r#"{"L": [{}]}"#,
        r#"{"M": {"k": {"X": "1"}}}"#,
        r#"{"s": "x"}"#,
        r#"{"L": [{"L": [{}]}]}"#,
    ] {
        let fields = format!(
            r#""Limit": 0, "FilterExpression": "a = :p", "ExpressionAttributeValues": {{":p": {{"S": "x"}}, ":d": {entry}}}"#
        );
        assert_eq!(
            check(&scan(&fields)).as_deref(),
            Some(older_limit.as_str()),
            "{entry}"
        );
    }
}

/// Measured 2026-10-31 between 23:54 and 23:56 UTC: an empty value or one with
/// an unknown type descriptor in `ExclusiveStartKey` or in a `ScanFilter`
/// `AttributeValueList` switches to the older form, for a `Segment` fault too;
/// a readable value keeps the newer form, also a number for an `S` key.
#[test]
fn an_unreadable_value_in_the_start_key_or_a_scan_filter_switches_to_the_older_form() {
    let older_limit = one(&older("0", "limit", "greater than or equal to 1"));
    let filter = |entry: &str| {
        format!(
            r#""ScanFilter": {{"a": {{"ComparisonOperator": "EQ", "AttributeValueList": [{entry}]}}}}"#
        )
    };
    for fields in [
        r#""ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}"#.to_owned(),
        r#""ExclusiveStartKey": {"pk": {"X": "1"}, "sk": {"S": "x"}}"#.to_owned(),
        filter("{}"),
        filter(r#"{"X": "1"}"#),
        // Measured 2026-11-23 14:51 UTC: an `M` member of a key value and an
        // `L` member of a list entry are read as the entry is.
        r#""ExclusiveStartKey": {"pk": {"M": {"k": {}}}, "sk": {"S": "s0"}}"#.to_owned(),
        filter(r#"{"L": [{}]}"#),
    ] {
        let fields = format!(r#""Limit": 0, {fields}"#);
        assert_eq!(
            check(&scan(&fields)).as_deref(),
            Some(older_limit.as_str()),
            "{fields}"
        );
    }
    let segment =
        r#""Segment": -1, "TotalSegments": 2, "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}"#;
    assert_eq!(
        check(&scan(segment)),
        Some(one(&older("-1", "segment", "greater than or equal to 0")))
    );
    for fields in [
        r#""ExclusiveStartKey": {"pk": {"N": "1"}, "sk": {"S": "x"}}"#.to_owned(),
        filter(r#"{"N": "1"}"#),
    ] {
        let fields = format!(r#""Limit": 0, {fields}"#);
        assert_eq!(check(&scan(&fields)), Some(one(LIMIT)), "{fields}");
    }
}

/// Measured 2026-10-31 12:36 UTC: `Limit: 0` beside a readable entry of each
/// type, and beside an `L` and an `M` whose members are readable, keeps the
/// newer form, as the empty and duplicate `NS` measured on 2026-10-25 do.
#[test]
fn a_readable_entry_of_any_type_keeps_the_newer_form() {
    for entry in [
        r#"{"S": "x"}"#,
        r#"{"N": "1"}"#,
        r#"{"B": "AA=="}"#,
        r#"{"SS": ["x"]}"#,
        r#"{"NS": ["1"]}"#,
        r#"{"BS": ["AA=="]}"#,
        r#"{"M": {"k": {"S": "x"}}}"#,
        r#"{"L": [{"N": "1"}, {"L": []}]}"#,
        r#"{"NULL": true}"#,
        r#"{"BOOL": false}"#,
    ] {
        let fields = format!(r#""Limit": 0, "ExpressionAttributeValues": {{":a": {entry}}}"#);
        assert_eq!(check(&scan(&fields)), Some(one(LIMIT)), "{entry}");
    }
}

/// Measured 2026-10-31 12:36 UTC: every fault in one message, in the order
/// `Limit`, `Segment`, `TotalSegments`, and nothing reported for a field inside
/// its bounds. `Segment: 0, TotalSegments: 1` was sent with `ConsistentRead: true`
/// and returned the whole table in one page.
#[test]
fn every_fault_is_reported_in_one_message_and_a_field_inside_its_bounds_is_not() {
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
        r#""Segment": 0, "TotalSegments": 1"#,
        r#""Segment": 999999, "TotalSegments": 1000000"#,
    ] {
        assert_eq!(check(&scan(fields)), None, "{fields}");
    }
}
