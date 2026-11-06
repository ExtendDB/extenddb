// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order of the checks `refuse_without_hash_equality` runs on a key
//! condition that skipped the classification in `handle_query`. Every
//! expected refusal was measured on the service: `pk > :x` on 2026-10-21,
//! the value type rules and `begins_with` with a second clause on
//! 2026-11-01, the rest on 2026-10-23.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, parse_key_condition, tokenize};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};

use super::refuse_without_hash_equality;

const TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter \
                             type does not match schema type";
const BEGINS_WITH_ON_N: &str = "Invalid KeyConditionExpression: Incorrect operand type for \
                                operator or function; operator or function: begins_with, \
                                operand type: N";
const ONE_PER_KEY: &str = "Invalid KeyConditionExpression: KeyConditionExpressions must only \
                           contain one condition per key";
const NOT_SUPPORTED: &str = "Query key condition not supported";
const MISSED_PK: &str = "Query condition missed key schema element: pk";
const MISSED_G: &str = "Query condition missed key schema element: g";
const HASH_H: &str = "HASH key attributes h must have equality conditions specified in the query";

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn definition(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    }
}

/// Run the checks on `expr`, with `:x` and `:y` strings and `:n` and `:m`
/// numbers.
fn check(
    expr: &str,
    key_schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) -> Result<(), String> {
    let kc = parse_key_condition(&tokenize(expr).unwrap()).unwrap();
    let values = HashMap::from([
        ("x".to_owned(), AttributeValue::S("a".to_owned())),
        ("y".to_owned(), AttributeValue::S("b".to_owned())),
        ("n".to_owned(), AttributeValue::N("1".to_owned())),
        ("m".to_owned(), AttributeValue::N("2".to_owned())),
    ]);
    let maps = ExpressionMaps::new(HashMap::new(), values);
    refuse_without_hash_equality(&kc, key_schema, definitions, &maps).map_err(|e| match e {
        DynamoDbError::ValidationException(msg) => msg,
        other => panic!("{expr}: {other:?}"),
    })
}

fn assert_refusals(
    rows: &[(&str, &str)],
    key_schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) {
    for (expr, want) in rows {
        assert_eq!(
            check(expr, key_schema, definitions).unwrap_err(),
            *want,
            "{expr}"
        );
    }
}

/// HASH `pk` and RANGE `sk`, both strings.
fn string_table() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
    (
        vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        vec![
            definition("pk", ScalarAttributeType::S),
            definition("sk", ScalarAttributeType::S),
        ],
    )
}

/// An index with HASH `g` and RANGE `s` on the table above, which defines
/// its own key attributes as well. All four are strings.
fn string_index() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
    (
        vec![key("g", KeyType::Hash), key("s", KeyType::Range)],
        ["pk", "sk", "g", "s"]
            .into_iter()
            .map(|name| definition(name, ScalarAttributeType::S))
            .collect(),
    )
}

#[test]
fn single_attribute_schema_reports_a_wrong_typed_value_before_the_wording() {
    // Row 427-8c.
    let (schema, definitions) = string_table();
    assert_refusals(
        &[("pk > :n", TYPE_MISMATCH), ("pk > :x", NOT_SUPPORTED)],
        &schema,
        &definitions,
    );
}

#[test]
fn single_attribute_schema_reports_begins_with_on_a_number_key_before_the_wording() {
    // Rows 6 and 6b.
    let schema = [key("pk", KeyType::Hash)];
    let definitions = [definition("pk", ScalarAttributeType::N)];
    assert_refusals(
        &[
            ("begins_with(pk, :n)", BEGINS_WITH_ON_N),
            ("pk > :n", NOT_SUPPORTED),
        ],
        &schema,
        &definitions,
    );
}

#[test]
fn begins_with_on_a_number_hash_attribute_is_reported_in_either_clause() {
    // HASH `pk` (N) and RANGE `sk` (S), as measured.
    let schema = [key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    let definitions = [
        definition("pk", ScalarAttributeType::N),
        definition("sk", ScalarAttributeType::S),
    ];
    assert_refusals(
        &[
            ("begins_with(pk, :n) AND sk > :x", BEGINS_WITH_ON_N),
            ("sk > :x AND begins_with(pk, :n)", BEGINS_WITH_ON_N),
        ],
        &schema,
        &definitions,
    );
}

#[test]
fn two_conditions_on_one_attribute_are_refused_before_a_wrong_typed_value() {
    let (schema, definitions) = string_table();
    assert_refusals(
        &[
            ("pk > :x AND pk < :n", ONE_PER_KEY),
            ("pk > :n AND pk < :m", ONE_PER_KEY),
        ],
        &schema,
        &definitions,
    );
}

#[test]
fn only_the_clause_on_the_hash_attribute_is_type_checked() {
    let (schema, definitions) = string_table();
    assert_refusals(
        &[
            ("pk > :n AND sk > :y", TYPE_MISMATCH),
            ("sk = :x AND pk > :n", TYPE_MISMATCH),
            ("pk BETWEEN :n AND :m", TYPE_MISMATCH),
            // An N value on the S sort key is not checked.
            ("pk > :x AND sk > :n", NOT_SUPPORTED),
        ],
        &schema,
        &definitions,
    );
}

#[test]
fn no_value_is_type_checked_without_a_clause_on_the_hash_attribute() {
    let (schema, definitions) = string_table();
    assert_refusals(
        &[("sk > :n", MISSED_PK), ("zz > :n", MISSED_PK)],
        &schema,
        &definitions,
    );
    // Measured on an index with HASH `gsi1_pk` and RANGE `SK` of a table
    // with HASH `PK`: `PK > :n` and `SK > :n` answer the missing key sentence
    // for `gsi1_pk`. Here `pk` is the table's HASH attribute and `s` the
    // index's RANGE attribute.
    let (schema, definitions) = string_index();
    assert_refusals(
        &[("pk > :n", MISSED_G), ("s > :n", MISSED_G)],
        &schema,
        &definitions,
    );
}

#[test]
fn multi_attribute_schema_reports_the_hash_wording_before_a_wrong_typed_value() {
    // Row 4.
    let mut schema = vec![key("h", KeyType::Hash)];
    let mut definitions = vec![definition("h", ScalarAttributeType::S)];
    for range in ["a", "b", "c", "d"] {
        schema.push(key(range, KeyType::Range));
        definitions.push(definition(range, ScalarAttributeType::S));
    }
    assert_refusals(&[("h > :n", HASH_H)], &schema, &definitions);
}

#[test]
fn a_key_condition_with_a_hash_equality_is_left_to_the_classification() {
    // The classification in `handle_query` has already checked the value
    // types of this condition; these checks do not repeat it.
    let (schema, definitions) = string_table();
    assert_eq!(check("pk = :n", &schema, &definitions), Ok(()));
}
