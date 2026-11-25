// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key conditions with no equality clause on the HASH attribute. Each
//! expected message is the service's answer to the same request, as
//! `tests/python/test_query_lone_non_equality.py` records it.

use std::collections::HashMap;

use super::*;
use crate::expression::{parse_key_condition, tokenize};
use crate::types::KeyType::{Hash, Range};

type Schema = &'static [(&'static str, crate::types::KeyType)];

const TABLE: Schema = &[("pk", Hash), ("sk", Range)];
const HASH_ONLY: Schema = &[("pk", Hash)];
/// A local secondary index on `TABLE`.
const LSI: Schema = &[("pk", Hash), ("ls", Range)];
const GSI: Schema = &[("g", Hash), ("s", Range)];
const MULTI_RANGE: Schema = &[
    ("h", Hash),
    ("a", Range),
    ("b", Range),
    ("c", Range),
    ("d", Range),
];
const TWO_HASH: Schema = &[("h1", Hash), ("h2", Hash), ("r", Range)];

fn key_schema(schema: Schema) -> Vec<KeySchemaElement> {
    schema
        .iter()
        .map(|(name, key_type)| KeySchemaElement {
            attribute_name: (*name).to_owned(),
            key_type: *key_type,
        })
        .collect()
}

/// `#p`, `#s`, `#z` and `#a` stand for `pk`, `sk`, `zz` and `h1`.
fn names() -> HashMap<String, String> {
    [("p", "pk"), ("s", "sk"), ("z", "zz"), ("a", "h1")]
        .into_iter()
        .map(|(reference, attr)| (reference.to_owned(), attr.to_owned()))
        .collect()
}

fn parse(expr: &str) -> KeyCondition {
    let tokens = tokenize(expr).unwrap();
    parse_key_condition(&tokens).unwrap_or_else(|e| panic!("{expr}: {e:?}"))
}

fn refusal(expr: &str, schema: Schema) -> String {
    match parse(expr).refuse_without_hash_equality(&key_schema(schema), &names()) {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("{expr}: expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_clause_on_the_only_hash_attribute_is_not_supported() {
    for (expr, schema) in [
        ("pk > :x", TABLE),
        ("begins_with(pk, :x)", TABLE),
        ("pk BETWEEN :x AND :y", TABLE),
        ("#p > :x", TABLE),
        ("pk > :x AND sk > :y", TABLE),
        ("zz > :x AND pk > :y", TABLE),
        ("sk = :x AND pk > :y", TABLE),
        ("zz = :x AND pk > :y", TABLE),
        ("begins_with(pk, :x) AND sk = :y", TABLE),
        ("pk BETWEEN :x AND :y AND sk = :x", TABLE),
        ("begins_with(pk, :x)", HASH_ONLY),
        ("pk > :x", LSI),
        ("g > :x", GSI),
        ("g > :x AND s = :y", GSI),
    ] {
        assert_eq!(
            refusal(expr, schema),
            "Query key condition not supported",
            "{expr}"
        );
    }
}

#[test]
fn no_clause_on_the_only_hash_attribute_misses_it() {
    for (expr, schema, hash) in [
        ("sk > :x", TABLE, "pk"),
        ("z > :x", TABLE, "pk"),
        ("#s > :x", TABLE, "pk"),
        ("#z > :x", TABLE, "pk"),
        ("begins_with(#s, :x)", TABLE, "pk"),
        ("sk > :x AND zz > :y", TABLE, "pk"),
        ("ls > :x", LSI, "pk"),
        ("s > :x", GSI, "g"),
        ("s > :x AND zz > :y", GSI, "g"),
        ("pk > :x AND s > :y", GSI, "g"),
    ] {
        assert_eq!(
            refusal(expr, schema),
            format!("Query condition missed key schema element: {hash}"),
            "{expr}"
        );
    }
}

#[test]
fn a_schema_with_several_key_attributes_names_every_hash_attribute() {
    for (expr, schema, hash) in [
        ("h > :x", MULTI_RANGE, "h"),
        ("a > :x", MULTI_RANGE, "h"),
        ("z > :x", MULTI_RANGE, "h"),
        ("begins_with(a, :x)", MULTI_RANGE, "h"),
        ("a BETWEEN :x AND :y", MULTI_RANGE, "h"),
        ("h1 > :x", TWO_HASH, "h1, h2"),
        ("r > :x", TWO_HASH, "h1, h2"),
        ("#a > :x", TWO_HASH, "h1, h2"),
    ] {
        assert_eq!(
            refusal(expr, schema),
            format!(
                "HASH key attributes {hash} must have equality conditions specified in the query"
            ),
            "{expr}"
        );
    }
}

#[test]
fn lacks_hash_equality_decides_which_conditions_are_refused() {
    for (expr, schema, lacks) in [
        ("pk > :x", TABLE, true),
        ("sk = :x AND pk > :y", TABLE, true),
        ("zz = :x AND #p > :y", TABLE, true),
        ("pk = :p", TABLE, false),
        ("pk = :p AND sk > :s", TABLE, false),
        ("sk > :s AND pk = :p", TABLE, false),
        ("sk = :s AND pk = :p", TABLE, false),
        // An equality on another attribute is left to the engine's missed
        // key schema element check.
        ("sk = :x", TABLE, false),
        ("zz = :x AND sk > :y", TABLE, false),
        ("zz = :x AND yy = :y", TABLE, false),
        ("a = :a AND b = :b AND pk > :x", TABLE, false),
        ("pk = :x AND s > :y", GSI, false),
        ("h = :h AND a > :x", MULTI_RANGE, false),
    ] {
        let kc = parse(expr);
        let schema = key_schema(schema);
        assert_eq!(kc.lacks_hash_equality(&schema, &names()), lacks, "{expr}");
        assert_eq!(
            kc.refuse_without_hash_equality(&schema, &names()).is_err(),
            lacks,
            "{expr}"
        );
    }
}

#[test]
fn a_key_schema_with_no_hash_attribute_is_an_internal_error() {
    match parse("pk > :x").refuse_without_hash_equality(&key_schema(&[("sk", Range)]), &names()) {
        Err(DynamoDbError::InternalServerError(msg)) => {
            assert_eq!(msg, "key schema without a HASH attribute");
        }
        other => panic!("expected an internal error, got {other:?}"),
    }
}

/// The placeholder names of `operands_naming(attribute)` for `expr`.
fn operand_names(expr: &str, attribute: &str, names: &HashMap<String, String>) -> Vec<String> {
    parse(expr)
        .operands_naming(attribute, names)
        .into_iter()
        .map(|(_, value)| match value {
            Expr::Placeholder(name) => name.clone(),
            other => panic!("{expr}: {other:?}"),
        })
        .collect()
}

#[test]
fn operands_naming_finds_the_clause_on_an_attribute_wherever_it_is() {
    let none = HashMap::new();
    for (expr, want) in [
        ("pk > :n", vec!["n"]),
        ("pk > :n AND sk > :y", vec!["n"]),
        ("sk > :y AND pk > :n", vec!["n"]),
        ("sk = :x AND pk > :n", vec!["n"]),
        ("pk BETWEEN :n AND :m", vec!["n", "m"]),
        ("sk > :y AND pk BETWEEN :n AND :m", vec!["n", "m"]),
        ("begins_with(pk, :n)", vec!["n"]),
        ("pk = :n", vec!["n"]),
        ("sk > :y", vec![]),
        ("sk = :x AND zz > :y", vec![]),
    ] {
        assert_eq!(operand_names(expr, "pk", &none), want, "{expr}");
    }
    assert_eq!(operand_names("sk > :y AND #p > :n", "pk", &names()), ["n"]);
    // An undefined `#name` names no attribute.
    assert!(operand_names("#q > :n", "pk", &names()).is_empty());
}
