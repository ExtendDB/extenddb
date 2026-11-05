// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row parsing is linear in the size of the row. A row is parsed on a
//! runtime worker, so a check that compares each member of a set, or each
//! name of an attribute map, with every one before it, or that copies the
//! path of a value for every element beneath it, lets one row under the
//! 4 MiB line cap hold the worker for minutes. Each case parses on a thread
//! of its own and fails when the parse has not returned within [`BOUND`].

use std::sync::mpsc;
use std::time::Duration;

use super::super::*;

/// How long one of the rows below may take to parse.
const BOUND: Duration = Duration::from_secs(10);

/// The default bulk line cap, 4 MiB.
const MAX_LINE: usize = 4 * 1024 * 1024;

/// Parse `line` on a thread of its own and wait at most [`BOUND`] for the
/// row. A parse still running at the bound is left to finish on its thread.
fn parse_within_bound(line: String, label: &str) -> rows::Row {
    assert!(line.len() <= MAX_LINE, "{label}: the line is over the cap");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        // The receiver is gone once the bound has passed.
        let _ = sender.send(rows::parse_row(&line, MAX_LINE));
    });
    match receiver.recv_timeout(BOUND) {
        Ok(Ok(Some(row))) => row,
        Ok(other) => panic!("{label}: expected a row, got {other:?}"),
        Err(_) => panic!("{label}: the row did not parse within {BOUND:?}"),
    }
}

/// One line whose item holds `pk` and the given typed attributes.
fn item_line(attributes: impl IntoIterator<Item = (String, serde_json::Value)>) -> String {
    let mut item = serde_json::Map::new();
    item.insert("pk".to_owned(), json!({"S": "a"}));
    item.extend(attributes);
    json!({ "Item": item }).to_string()
}

fn one(name: &str, value: serde_json::Value) -> [(String, serde_json::Value); 1] {
    [(name.to_owned(), value)]
}

#[test]
fn a_string_set_of_80000_members_parses_within_the_bound() {
    let members: Vec<String> = (0..80_000).map(|i| format!("member-{i:05}")).collect();
    let row = parse_within_bound(item_line(one("s", json!({ "SS": members }))), "SS");
    let Some(AttributeValue::SS(set)) = row.item.get("s") else {
        panic!("s is a string set");
    };
    assert_eq!(set.len(), 80_000);
}

#[test]
fn a_string_set_of_80000_entries_with_each_member_twice_keeps_one_copy_of_each() {
    let members: Vec<String> = (0..80_000)
        .map(|i| format!("member-{:05}", i % 40_000))
        .collect();
    let row = parse_within_bound(item_line(one("s", json!({ "SS": members }))), "SS twice");
    let Some(AttributeValue::SS(set)) = row.item.get("s") else {
        panic!("s is a string set");
    };
    assert_eq!(set.len(), 40_000);
}

#[test]
fn a_number_set_of_80000_members_parses_within_the_bound() {
    let members: Vec<String> = (0..80_000).map(|i| i.to_string()).collect();
    let row = parse_within_bound(item_line(one("n", json!({ "NS": members }))), "NS");
    let Some(AttributeValue::NS(set)) = row.item.get("n") else {
        panic!("n is a number set");
    };
    assert_eq!(set.len(), 80_000);
}

#[test]
fn a_binary_set_of_80000_members_parses_within_the_bound() {
    use base64::Engine as _;
    let members: Vec<String> = (0_u32..80_000)
        .map(|i| base64::engine::general_purpose::STANDARD.encode(i.to_be_bytes()))
        .collect();
    let row = parse_within_bound(item_line(one("b", json!({ "BS": members }))), "BS");
    let Some(AttributeValue::BS(set)) = row.item.get("b") else {
        panic!("b is a binary set");
    };
    assert_eq!(set.len(), 80_000);
}

#[test]
fn an_item_of_80000_attribute_names_parses_within_the_bound() {
    let attributes = (0..80_000).map(|i| (format!("a{i:05}"), json!({"NULL": true})));
    let row = parse_within_bound(item_line(attributes), "item names");
    assert_eq!(row.item.len(), 80_001);
}

#[test]
fn a_map_of_80000_member_names_parses_within_the_bound() {
    let members: serde_json::Map<String, serde_json::Value> = (0..80_000)
        .map(|i| (format!("k{i:05}"), json!({"NULL": true})))
        .collect();
    let row = parse_within_bound(item_line(one("m", json!({ "M": members }))), "map names");
    let Some(AttributeValue::M(map)) = row.item.get("m") else {
        panic!("m is a map");
    };
    assert_eq!(map.len(), 80_000);
}

/// A list of 230,000 empty maps under an attribute named with 2 MiB. The
/// name is over the attribute name limit, which the runner judges after the
/// parse; the parse itself must not copy the name for each element, nor for
/// each element's map.
#[test]
fn a_list_under_a_long_attribute_name_parses_within_the_bound() {
    let name = "n".repeat(2 * 1024 * 1024);
    let elements = vec![json!({"M": {}}); 230_000];
    let row = parse_within_bound(item_line(one(&name, json!({ "L": elements }))), "list");
    let Some(AttributeValue::L(list)) = row.item.get(&name) else {
        panic!("the long name holds a list");
    };
    assert_eq!(list.len(), 230_000);
}

/// A map of 100,000 members under an attribute named with 1,000,000 bytes.
#[test]
fn a_map_under_a_long_attribute_name_parses_within_the_bound() {
    let name = "n".repeat(1_000_000);
    let members: serde_json::Map<String, serde_json::Value> = (0..100_000)
        .map(|i| (format!("k{i:05}"), json!({"NULL": true})))
        .collect();
    let row = parse_within_bound(item_line(one(&name, json!({ "M": members }))), "map");
    let Some(AttributeValue::M(map)) = row.item.get(&name) else {
        panic!("the long name holds a map");
    };
    assert_eq!(map.len(), 100_000);
}
