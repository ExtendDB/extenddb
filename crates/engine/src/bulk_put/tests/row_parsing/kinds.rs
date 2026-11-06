// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How the expression-size bound counts each value kind, measured on the
//! hosted service at the 1 MiB edge. The
//! condition is the 35-byte `attribute_not_exists(pk) OR #v = :v`, so a
//! content count of 1,048,541 reaches the bound and 1,048,540 does not.

use super::super::*;
use base64::Engine as _;

const COND: &str = "attribute_not_exists(pk) OR #v = :v";
const EDGE_IN: usize = 1024 * 1024 - 35 - 1;
const EDGE_OUT: usize = 1024 * 1024 - 35;

fn line_with_value(value: serde_json::Value) -> String {
    let values = serde_json::json!({ ":v": value });
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"{COND}\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"v\"}}, \
         \"ExpressionAttributeValues\": {values}}}"
    )
}

fn parse(line: &str) -> Result<Option<rows::Row>, rows::RowError> {
    rows::parse_row(line, 4 * 1024 * 1024)
}

fn expect_size_fault(line: &str, label: &str) {
    match parse(line) {
        Err(rows::RowError::Item(message)) => {
            assert_eq!(message, rows::EXPRESSION_SIZE_MESSAGE, "{label}");
        }
        other => panic!("{label}: expected the expression-size fault, got {other:?}"),
    }
}

fn expect_evaluated(line: &str, label: &str) {
    assert!(
        parse(line)
            .unwrap_or_else(|e| panic!("{label}: {e:?}"))
            .is_some(),
        "{label}"
    );
}

fn b64(raw_len: usize) -> String {
    base64::engine::general_purpose::STANDARD.encode(vec![0u8; raw_len])
}

// A binary value counts its decoded bytes, not its base64 text.
#[test]
fn a_binary_value_counts_its_decoded_bytes() {
    // Text of 1,048,544 chars holding 786,408 raw bytes: under the edge by raw bytes.
    let text = b64(786_408);
    assert_eq!(text.len(), 1_048_544);
    expect_evaluated(
        &line_with_value(serde_json::json!({ "B": text })),
        "binary under the edge",
    );
    // 1,048,541 raw bytes: at the edge by raw bytes.
    expect_size_fault(
        &line_with_value(serde_json::json!({ "B": b64(EDGE_OUT) })),
        "binary at the edge",
    );
    expect_evaluated(
        &line_with_value(serde_json::json!({ "B": b64(EDGE_IN) })),
        "one byte under",
    );
}

// A map counts each attribute name as well as its value.
#[test]
fn a_map_counts_its_attribute_names() {
    // A one-byte key is the byte that reaches the edge.
    let m = serde_json::json!({ "M": { "k": { "S": "a".repeat(EDGE_IN) } } });
    expect_size_fault(&line_with_value(m), "one-byte key at the edge");
    // The same value under a map with no key bytes to spare is evaluated one byte under.
    let m = serde_json::json!({ "M": { "k": { "S": "a".repeat(EDGE_IN - 1) } } });
    expect_evaluated(&line_with_value(m), "one-byte key one under");
    // A twenty-byte key beside a value ten under the edge is over by ten.
    let m = serde_json::json!({ "M": { "k".repeat(20): { "S": "a".repeat(EDGE_IN - 10) } } });
    expect_size_fault(&line_with_value(m), "twenty-byte key");
}

// List members and set members sum.
#[test]
fn list_and_set_members_sum() {
    let half = EDGE_OUT / 2;
    let rest = EDGE_OUT - half;
    let l = serde_json::json!({ "L": [ { "S": "a".repeat(half) }, { "S": "b".repeat(rest) } ] });
    expect_size_fault(&line_with_value(l), "list at the edge");
    let ss = serde_json::json!({ "SS": [ "a".repeat(half), "b".repeat(rest) ] });
    expect_size_fault(&line_with_value(ss), "string set at the edge");
    let l =
        serde_json::json!({ "L": [ { "S": "a".repeat(half) }, { "S": "b".repeat(rest - 1) } ] });
    expect_evaluated(&line_with_value(l), "list one under");
}

// Type tags never count: an `S` value at the edge is the same count whether
// read as a bare value or as the single member of a list.
#[test]
fn type_tags_do_not_count() {
    let bare = serde_json::json!({ "S": "a".repeat(EDGE_IN) });
    expect_evaluated(&line_with_value(bare), "bare S one under");
    let listed = serde_json::json!({ "L": [ { "S": "a".repeat(EDGE_IN) } ] });
    expect_evaluated(&line_with_value(listed), "listed S one under");
}

// An empty names map and a value that fails its own rule
// are judged before the size bounds; an unused placeholder is not
// a parse-time fault at all, so the size bound wins there.
#[test]
fn an_empty_names_map_and_a_bad_value_are_judged_before_the_size_bounds() {
    let big = "a".repeat(EDGE_OUT);
    let empty_names = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"{COND}\", \
         \"ExpressionAttributeNames\": {{}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{big}\"}}}}}}"
    );
    match parse(&empty_names) {
        Err(rows::RowError::Item(message)) => {
            assert_eq!(message, rows::NAMES_EMPTY_MESSAGE, "an empty names map");
        }
        other => panic!("expected the empty-names fault, got {other:?}"),
    }
    let bad_value = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk) OR #v = :v OR #v = :w\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"v\"}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{big}\"}}, \":w\": {{\"NULL\": false}}}}}}"
    );
    match parse(&bad_value) {
        Err(rows::RowError::Item(message)) => assert!(
            message.contains("Null attribute value types must have the value of true for key :w"),
            "a bad value: got {message}"
        ),
        other => panic!("expected the wrapped NULL fault, got {other:?}"),
    }
    let unused_placeholder = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"a\"}}}}, \
         \"ConditionExpression\": \"{COND}\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"v\", \"#w\": \"w\"}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{big}\"}}}}}}"
    );
    expect_size_fault(&unused_placeholder, "an unused placeholder");
}
