// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Record structure and expression-map faults, measured on the hosted
//! service.

use super::super::*;

// A fault decoding one `ExpressionAttributeValues`
// entry fails the row alone and the file continues, with the single-item
// text for the fault wrapped in the map's own message and the entry's key
// named.

#[test]
fn a_null_false_expression_value_fails_the_row_with_the_wrapped_message() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeValues": {":v": {"NULL": false}}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeValues contains invalid value: One or more parameter values were \
         invalid: Null attribute value types must have the value of true for key :v"
    );
}

#[test]
fn an_unknown_type_tag_in_an_expression_value_fails_the_row_with_the_type_tag_text() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeValues": {":v": {"XX": "1"}}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeValues contains invalid value: Expected DynamoDB type to be one of \
         [S, N, B, BOOL, NULL, L, M, SS, NS, BS] for key :v"
    );
}

#[test]
fn a_non_numeric_expression_value_fails_the_row_with_the_conversion_text() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeValues": {":v": {"N": "abc"}}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeValues contains invalid value: The parameter cannot be converted to \
         a numeric value: abc for key :v"
    );
}

#[test]
fn a_valid_expression_value_beside_a_bad_one_does_not_change_the_reported_key() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeValues": {":a": {"S": "ok"}, ":b": {"N": "abc"}}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeValues contains invalid value: The parameter cannot be converted to \
         a numeric value: abc for key :b"
    );
}

#[test]
fn the_type_tag_text_is_the_abandon_message_without_its_suffix() {
    assert_eq!(
        format!(
            "{}. Remainder of the file will not be processed.",
            rows::TYPE_TAG_MESSAGE
        ),
        rows::PARSE_ABANDON_MESSAGE
    );
}

// An `N` value inside Item whose text is not a number abandons the
// file. ExtendDB answered the item-level conversion text and kept reading.

#[test]
fn a_non_numeric_number_in_the_item_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}, "sk": {"N": "not-a-number"}}}"#),
        "Expected a valid numeric. Remainder of the file will not be processed."
    );
}

#[test]
fn a_non_numeric_number_nested_in_a_map_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}, "m": {"M": {"n": {"N": "x"}}}}}"#),
        "Expected a valid numeric. Remainder of the file will not be processed."
    );
}

// The same text inside ExpressionAttributeValues stays item-level (covered
// above): the two classes are kept apart.
#[test]
fn a_number_outside_the_supported_range_is_still_the_rows_own_fault() {
    // Unmeasured: a number of valid syntax that no magnitude accepts is
    // left to the item validators the runner runs, which report it as a row
    // fault, so the file is not abandoned at it.
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}, "n": {"N": "1e1000"}}}"#,
        1 << 20,
    )
    .expect("a magnitude fault is not a parse-level fault");
    assert!(row.is_some());
}

#[test]
fn valid_numbers_in_the_item_still_parse() {
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}, "n": {"N": "-0.5e3"}, "l": {"L": [{"N": "7"}]}}}"#,
        1 << 20,
    )
    .unwrap()
    .expect("row");
    assert_eq!(row.item["n"], AttributeValue::N("-500".to_owned()));
}

// An attribute name repeated inside the Item attribute
// map fails the row alone and names the path, at the top level and inside a
// nested M. The JSON parser kept the last value silently and the row was
// written.

#[test]
fn a_repeated_top_level_attribute_name_fails_the_row() {
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a"}, "a": {"S": "1"}, "a": {"S": "2"}}}"#),
        "One or more parameter values were invalid: Value for Item.a is ambiguous"
    );
}

#[test]
fn a_repeated_name_inside_a_map_attribute_fails_the_row_with_the_typed_path() {
    assert_eq!(
        item_message(
            r#"{"Item": {"pk": {"S": "a"}, "m": {"M": {"k": {"S": "1"}, "k": {"S": "2"}}}}}"#
        ),
        "One or more parameter values were invalid: Value for Item.m.M.k is ambiguous"
    );
}

#[test]
fn a_repeated_attribute_name_with_the_same_value_fails_the_row_too() {
    // ExtendDB's reading of the repeat rule: the repeat itself is the
    // fault, as it is for a repeated record member. An attribute
    // name repeated with equal values was not measured.
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a"}, "a": {"S": "1"}, "a": {"S": "1"}}}"#),
        "One or more parameter values were invalid: Value for Item.a is ambiguous"
    );
}

// A member given twice where one was
// expected abandons the file with the message naming the closing brace,
// and a set tag whose value is not an array names the opening
// bracket. ExtendDB answered the type-tag text for the two-tag shape and
// kept the last value for the repeats.

#[test]
fn a_record_carrying_item_twice_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}}, "Item": {"pk": {"S": "b"}}}"#),
        "Expected character }. Remainder of the file will not be processed."
    );
}

#[test]
fn a_record_carrying_item_twice_with_equal_values_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}}, "Item": {"pk": {"S": "a"}}}"#),
        "Expected character }. Remainder of the file will not be processed."
    );
}

#[test]
fn a_typed_value_with_its_tag_repeated_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "1", "S": "2"}}}"#),
        "Expected character }. Remainder of the file will not be processed."
    );
}

#[test]
fn a_typed_value_with_two_different_tags_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "1", "N": "2"}}}"#),
        "Expected character }. Remainder of the file will not be processed."
    );
}

#[test]
fn a_set_tag_whose_value_is_not_an_array_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}, "ss": {"SS": "notarray"}}}"#),
        "Expected character [. Remainder of the file will not be processed."
    );
}

#[test]
fn a_line_that_is_an_array_wrapping_a_record_still_answers_the_token_message() {
    // The tokenizer's message, not the brace one.
    assert_eq!(
        abandon_message(r#"[ {"Item": {"pk": {"S": "a"}}} ]"#),
        rows::CORRUPT_STREAM_MESSAGE
    );
}

// An empty expression map supplied with a
// ConditionExpression fails the row alone; the empty map was exempt and the
// row was written.

#[test]
fn an_empty_values_map_with_a_condition_fails_the_row() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeValues": {}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeValues must not be empty"
    );
}

#[test]
fn an_empty_names_map_with_a_condition_fails_the_row() {
    let line = concat!(
        r#"{"Item": {"pk": {"S": "a"}}, "#,
        r#""ConditionExpression": "attribute_not_exists(pk)", "#,
        r#""ExpressionAttributeNames": {}}"#
    );
    assert_eq!(
        item_message(line),
        "ExpressionAttributeNames must not be empty"
    );
}

// Unmeasured and left as it was: with no expression to reference, an empty
// map is exempt and the row is written unconditional.
#[test]
fn an_empty_values_map_without_a_condition_still_parses_unconditional() {
    let line = r#"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeValues": {}}"#;
    let row = rows::parse_row(line, 1 << 20)
        .unwrap()
        .expect("an empty map references nothing and does not trip the check");
    assert!(row.condition.is_none());
}

/// The compact JSON overhead of a one-entry values map holding an empty
/// `S`: `{":v":{"S":""}}`.
fn values_map_overhead() -> usize {
    serde_json::to_string(&json!({":v": {"S": ""}}))
        .expect("the empty-value map serializes")
        .len()
}

/// A record with one expression value whose compact values map is
/// `values_serialized` bytes and, when `names` is given, that names map;
/// the condition references the value (and the placeholder when present).
fn line_with_maps(values_serialized: usize, names: Option<&str>) -> String {
    let value = "a".repeat(values_serialized - values_map_overhead());
    match names {
        None => format!(
            "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
             \"ConditionExpression\": \"attribute_not_exists(pk) OR v = :v\", \
             \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}}}}}"
        ),
        Some(placeholder) => format!(
            "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
             \"ConditionExpression\": \"attribute_not_exists(pk) OR {placeholder} = :v\", \
             \"ExpressionAttributeNames\": {{\"{placeholder}\": \"v\"}}, \
             \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}}}}}"
        ),
    }
}

fn parse(line: &str) -> Result<Option<rows::Row>, rows::RowError> {
    rows::parse_row(line, 4 * 1024 * 1024)
}

fn expect_item_fault(line: &str, expected: &str, label: &str) {
    match parse(line) {
        Err(rows::RowError::Item(message)) => assert_eq!(message, expected, "{label}"),
        other => panic!("{label}: expected an item-level fault, got {other:?}"),
    }
}

fn expect_abandon(line: &str, label: &str) {
    match parse(line) {
        Err(rows::RowError::Parse(message)) => {
            assert_eq!(message, rows::VALUES_TOO_LARGE_MESSAGE, "{label}");
        }
        other => panic!("{label}: expected the abandon, got {other:?}"),
    }
}

// The abandon class (measured on the service, and
// ): a values map over 2,097,152 bytes of compact JSON
// abandons the file, with or without a names map, and the bound is on the
// whole map.
#[test]
fn a_values_map_over_two_mebibytes_abandons_the_file() {
    let line = line_with_maps(rows::MAX_EXPRESSION_VALUES_BYTES + 100, Some("#v"));
    assert!(
        line.len() < 4 * 1024 * 1024,
        "the row sits under the service's line cap"
    );
    expect_abandon(&line, "100 bytes over");
    expect_abandon(
        &line_with_maps(rows::MAX_EXPRESSION_VALUES_BYTES + 1, Some("#v")),
        "one byte over",
    );
    expect_abandon(
        &line_with_maps(rows::MAX_EXPRESSION_VALUES_BYTES + 1, None),
        "one byte over with no names map",
    );
}

#[test]
fn two_values_abandon_together_when_their_map_passes_the_bound() {
    // Two values of 1,100,000 bytes, each under the bound, the
    // map over it.
    let value = "a".repeat(1_100_000);
    let line = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk) OR #v = :v OR #v = :w\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"v\"}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}, \":w\": {{\"S\": \"{value}\"}}}}}}"
    );
    expect_abandon(&line, "two values over the bound together");
}

// The combined class (measured on the service): a values map of
// exactly 2,097,152 bytes beside `{"#v": "v"}` is not an abandon; the two
// maps together, the names map counted by its raw bytes (3 here), pass the
// combined bound and the row fails with the combined-size text.
#[test]
fn the_two_maps_together_over_the_combined_bound_fail_the_row() {
    expect_item_fault(
        &line_with_maps(rows::MAX_EXPRESSION_VALUES_BYTES, Some("#v")),
        rows::MAPS_TOO_LARGE_MESSAGE,
        "values at the abandon bound beside a names map",
    );
    // The edge: values plus raw names bytes exactly at the bound is not
    // this class.
    let names_raw = "#v".len() + "v".len();
    assert_eq!(names_raw, 3);
    match parse(&line_with_maps(
        rows::MAX_EXPRESSION_MAPS_BYTES - names_raw,
        Some("#v"),
    )) {
        Err(rows::RowError::Item(message)) => {
            assert_ne!(
                message,
                rows::MAPS_TOO_LARGE_MESSAGE,
                "at the bound is not the combined class"
            );
        }
        other => panic!("expected the expression-size fault at the combined edge, got {other:?}"),
    }
}

// Measurements on the service fix the names map's contribution as its raw bytes:
// values maps of 2,097,148 beside `{"#v": "v"}` and 2,097,140 beside
// `{"#vvvvvvvvv": "v"}` were refused with the expression-size text, not the
// combined-size text, which the names map's compact JSON length (10 and
// 18 bytes) would have produced.
#[test]
fn the_names_map_counts_its_raw_bytes_toward_the_combined_bound() {
    expect_item_fault(
        &line_with_maps(2_097_148, Some("#v")),
        rows::EXPRESSION_SIZE_MESSAGE,
        "a two-byte placeholder",
    );
    expect_item_fault(
        &line_with_maps(2_097_140, Some("#vvvvvvvvv")),
        rows::EXPRESSION_SIZE_MESSAGE,
        "a ten-byte placeholder",
    );
}

// The expression-size class (measured on the service): the condition text
// plus the content bytes of its values reaching 1 MiB fails the row with the
// single-item expression-size text, under the other two bounds. Below that
// the condition is evaluated.
#[test]
fn a_values_map_over_the_expression_size_edge_fails_the_row() {
    expect_item_fault(
        &line_with_maps(1_048_591, Some("#v")),
        rows::EXPRESSION_SIZE_MESSAGE,
        "over the edge",
    );
    // The edge: condition `attribute_not_exists(pk) OR #v = :v` (35 bytes) plus a
    // value of 1,048,541 bytes, 1,048,576 together.
    let cond_len = "attribute_not_exists(pk) OR #v = :v".len();
    assert_eq!(cond_len, 35);
    let edge_value = rows::MAX_EXPRESSION_SIZE_BYTES - cond_len;
    assert_eq!(edge_value, 1_048_541);
    expect_item_fault(
        &line_with_maps(edge_value + values_map_overhead(), Some("#v")),
        rows::EXPRESSION_SIZE_MESSAGE,
        "at the edge",
    );
}

#[test]
fn a_values_map_under_the_expression_size_edge_parses() {
    let cond_len = "attribute_not_exists(pk) OR #v = :v".len();
    for (value_len, label) in [
        (
            rows::MAX_EXPRESSION_SIZE_BYTES - cond_len - 1,
            "1,048,575 together",
        ),
        (1_048_539, "1,048,539 bytes"),
        (1_000_000, "1,000,000 bytes"),
        (100_000, "100,000 bytes"),
        (4_000, "4,000 bytes"),
    ] {
        assert!(
            parse(&line_with_maps(
                value_len + values_map_overhead(),
                Some("#v")
            ))
            .unwrap_or_else(|e| panic!("{label}: {e:?}"))
            .is_some(),
            "{label}"
        );
    }
}

// The bound counts the condition text: the same value beside a longer
// condition fails the row one byte sooner.
#[test]
fn the_expression_size_bound_counts_the_condition_text() {
    let short = "attribute_not_exists(pk) OR #v = :v";
    let long = "attribute_not_exists(pk) OR #v = :v OR #v = :v";
    let value_len = rows::MAX_EXPRESSION_SIZE_BYTES - long.len();
    let value = "a".repeat(value_len);
    let line = |cond: &str| {
        format!(
            "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
             \"ConditionExpression\": \"{cond}\", \
             \"ExpressionAttributeNames\": {{\"#v\": \"v\"}}, \
             \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}}}}}"
        )
    };
    expect_item_fault(
        &line(long),
        rows::EXPRESSION_SIZE_MESSAGE,
        "long condition at 1 MiB",
    );
    assert!(
        parse(&line(short))
            .expect("the short condition stays under the bound")
            .is_some()
    );
}

// The names map counts the placeholder AND the
// attribute name it maps to toward the combined 2 MiB bound on both
// expression maps. A values map of 2,097,140 serialized bytes beside a
// names map `{"#v": <20-byte attribute name>}` (placeholder 2 bytes, name
// 20 bytes, raw sum 22) and a condition over both exceeds the bound
// (2,097,162 over 2,097,152) and fails the row with the combined-size text.
// Counting only the placeholder (2 bytes) would leave 2,097,142, under the
// bound, and the row would fall to the expression-size text instead. The
// existing raw-bytes test uses a one-byte attribute name, which both the
// placeholder-only and the placeholder-plus-name rules leave under the
// bound, so it does not decide the attribute-name side; this one does.
#[test]
fn the_names_map_attribute_name_counts_toward_the_combined_bound() {
    let value = "a".repeat(2_097_140 - values_map_overhead());
    let line = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk) OR #v = :v\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"vvvvvvvvvvvvvvvvvvvv\"}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}}}}}"
    );
    expect_item_fault(
        &line,
        rows::MAPS_TOO_LARGE_MESSAGE,
        "the placeholder and the attribute name both count",
    );
}
