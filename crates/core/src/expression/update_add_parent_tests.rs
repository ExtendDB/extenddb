// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ADD on a nested path needs every element above the leaf to exist with the
//! container type the next element needs (measured 2026-10-26).

use super::*;
use crate::expression::tokenizer::tokenize;
use crate::expression::update_parser::parse_update;
use std::collections::HashMap;

const INVALID: &str = "The document path provided in the update expression is invalid for update";

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn m(entries: &[(&str, AttributeValue)]) -> AttributeValue {
    AttributeValue::M(
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    )
}

fn add(expr: &str, item: &mut BTreeMap<String, AttributeValue>) -> Result<(), DynamoDbError> {
    let actions = parse_update(&tokenize(expr)?)?;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::from([("one".to_owned(), n("1"))]));
    apply_update(&actions, item, &maps)
}

/// The callers discard the image on an error, so only the error is checked.
fn refused(expr: &str, mut item: BTreeMap<String, AttributeValue>) {
    match add(expr, &mut item) {
        Err(DynamoDbError::ValidationException(msg)) => assert_eq!(msg, INVALID, "{expr}"),
        other => panic!("{expr}: {other:?}"),
    }
}

#[test]
fn a_member_of_an_existing_map_is_added() {
    let mut item = BTreeMap::from([("m".to_owned(), m(&[]))]);
    add("ADD m.a :one", &mut item).unwrap();
    assert_eq!(item["m"], m(&[("a", n("1"))]));
}

#[test]
fn a_missing_parent_is_refused() {
    refused("ADD z.a :one", BTreeMap::new());
    refused("ADD m.a.b :one", BTreeMap::from([("m".to_owned(), m(&[]))]));
    refused(
        "ADD x :one, z.a :one",
        BTreeMap::from([("x".to_owned(), n("1"))]),
    );
}

#[test]
fn a_parent_that_is_not_a_map_is_refused() {
    let list = AttributeValue::L(vec![n("1")]);
    refused("ADD s.a :one", BTreeMap::from([("s".to_owned(), n("1"))]));
    refused(
        "ADD l.a :one",
        BTreeMap::from([("l".to_owned(), list.clone())]),
    );
    refused("ADD l[5].a :one", BTreeMap::from([("l".to_owned(), list)]));
    refused(
        "ADD m[0] :one",
        BTreeMap::from([("m".to_owned(), m(&[("a", n("1"))]))]),
    );
}

#[test]
fn a_map_inside_a_list_is_reached() {
    let mut item = BTreeMap::from([("l".to_owned(), AttributeValue::L(vec![m(&[])]))]);
    add("ADD l[0].a :one", &mut item).unwrap();
    assert_eq!(item["l"], AttributeValue::L(vec![m(&[("a", n("1"))])]));
    refused(
        "ADD l[0].a.b :one",
        BTreeMap::from([("l".to_owned(), AttributeValue::L(vec![m(&[])]))]),
    );
}
