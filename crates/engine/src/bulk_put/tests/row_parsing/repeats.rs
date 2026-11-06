// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The set members the parser removes as repeats, and the item size they
//! carry to the runner's size check. The service stores a set with one copy
//! of each member and sizes it as submitted (measured): the item keeps one
//! copy and the row records the size of the copies removed.

use base64::Engine as _;
use extenddb_core::types::attribute_value_size;

use super::super::*;

fn row(item: serde_json::Value) -> rows::Row {
    rows::parse_row(&json!({ "Item": item }).to_string(), 4 * 1024 * 1024)
        .unwrap()
        .expect("a row")
}

fn number_size(number: &str) -> usize {
    attribute_value_size(&AttributeValue::N(number.to_owned()))
}

/// A number set member that is not a JSON string is refused by the
/// deserializer whatever its position: the dedupe keys it apart from the
/// normalized numbers, so `NS ["1", 1]` is not taken for a set with a
/// repeat of `"1"` and stored as `NS ["1"]`.
#[test]
fn a_number_set_member_that_is_not_a_string_fails_the_row_in_either_position() {
    let parse = |set: serde_json::Value| {
        rows::parse_row(
            &json!({ "Item": {"pk": {"S": "a"}, "ns": {"NS": set}} }).to_string(),
            4 * 1024 * 1024,
        )
    };
    let first = parse(json!([1, "1"])).expect_err("a number member first fails the row");
    let second = parse(json!(["1", 1])).expect_err("a number member second fails the row");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
}

#[test]
fn a_string_set_records_the_bytes_of_each_repeated_member() {
    let big = "x".repeat(250_000);
    let parsed = row(json!({"pk": {"S": "a"}, "ss": {"SS": [big, big]}}));
    assert_eq!(parsed.repeated_member_bytes, 250_000);
    assert_eq!(
        parsed.item["ss"],
        AttributeValue::SS([big].into_iter().collect())
    );
}

#[test]
fn a_number_set_records_each_removed_member_at_its_number_size() {
    let parsed = row(json!({"pk": {"S": "a"}, "ns": {"NS": ["1", "1.0", "25", "25", "7"]}}));
    assert_eq!(
        parsed.repeated_member_bytes,
        number_size("1") + number_size("25")
    );
    assert_eq!(
        parsed.item["ns"],
        AttributeValue::NS(["1", "25", "7"].map(str::to_owned).into_iter().collect())
    );
}

#[test]
fn a_binary_set_records_the_decoded_bytes_of_each_repeated_member() {
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let repeated = encode(&[7; 1000]);
    let other = encode(&[8; 10]);
    let parsed = row(json!({"pk": {"S": "a"}, "bs": {"BS": [repeated, other, repeated]}}));
    assert_eq!(parsed.repeated_member_bytes, 1000);
    assert_eq!(
        parsed.item["bs"],
        AttributeValue::BS([vec![7; 1000], vec![8; 10]].into_iter().collect())
    );
}

#[test]
fn repeats_inside_maps_and_lists_are_counted_with_the_ones_at_the_top() {
    let parsed = row(json!({
        "pk": {"S": "a"},
        "ss": {"SS": ["ab", "ab"]},
        "m": {"M": {"inner": {"SS": ["cde", "cde", "cde"]}}},
        "l": {"L": [{"SS": ["f", "f"]}, {"L": [{"NS": ["10", "1e1"]}]}]}
    }));
    assert_eq!(
        parsed.repeated_member_bytes,
        2 + 2 * 3 + 1 + number_size("10")
    );
}

#[test]
fn a_row_with_no_repeated_member_records_none() {
    let parsed = row(json!({
        "pk": {"S": "a"},
        "ss": {"SS": ["a", "b"]},
        "ns": {"NS": ["1", "2"]},
        "m": {"M": {"inner": {"SS": ["a"]}}}
    }));
    assert_eq!(parsed.repeated_member_bytes, 0);
}

#[test]
fn a_conditional_row_records_its_repeated_members_too() {
    let line = json!({
        "Item": {"pk": {"S": "a"}, "ss": {"SS": ["abc", "abc"]}},
        "ConditionExpression": "attribute_not_exists(pk)"
    })
    .to_string();
    let parsed = rows::parse_row(&line, 1 << 20).unwrap().expect("a row");
    assert!(parsed.condition.is_some());
    assert_eq!(parsed.repeated_member_bytes, 3);
}
