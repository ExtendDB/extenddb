// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key size limits on a `Query` key condition, reached through the
//! expression parser and through the `KeyConditions` desugaring. The rows of
//! the limits are in `extenddb_core`'s `validation::key_size` tests. The check
//! does not depend on how the key condition is arranged, so it runs on the
//! parser's output without the schema classification step.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, Condition, KeySchemaElement, KeyType};
use extenddb_core::validation::validate_query_key_sizes;

use crate::expression_helpers::parse_key_condition_expr;
use crate::legacy_filter::desugar_key_conditions;

const HASH_MSG: &str = "One or more parameter values were invalid: \
    Size of hashkey has exceeded the maximum size limit of2048 bytes";
const RANGE_MSG: &str = "One or more parameter values were invalid: \
    Aggregated size of all range keys has exceeded the size limit of 1024 bytes";

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn base_schema() -> Vec<KeySchemaElement> {
    vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)]
}

fn s(len: usize) -> AttributeValue {
    AttributeValue::S("x".repeat(len))
}

/// Parse `expr` and check it against `schema` with the given placeholder
/// values (`:name` keys are given without the colon).
fn run(
    expr: &str,
    schema: &[KeySchemaElement],
    values: &[(&str, AttributeValue)],
) -> Result<(), DynamoDbError> {
    let limits = LimitsConfig::default();
    let key_condition = parse_key_condition_expr(expr, &limits).expect("expression parses");
    let values: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    let maps = ExpressionMaps::new(HashMap::new(), values);
    validate_query_key_sizes(&key_condition, schema, &maps, &limits)
}

fn refused(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn clause_order_does_not_change_the_result() {
    let result = run(
        "sk = :v AND pk = :p",
        &base_schema(),
        &[("p", s(1)), ("v", s(1025))],
    );
    assert_eq!(refused(result), RANGE_MSG);
    let result = run(
        "sk = :v AND pk = :p",
        &base_schema(),
        &[("p", s(2049)), ("v", s(1))],
    );
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn legacy_key_conditions_are_checked_like_the_expression() {
    // Measured 2026-10-23 in us-east-1, with a 2049-byte hash value and with a
    // 1025-byte range value: the service sizes legacy `KeyConditions` with the
    // same sentences. Both syntaxes reach the check as one key condition.
    let eq = |value: AttributeValue| Condition {
        comparison_operator: "EQ".to_owned(),
        attribute_value_list: vec![value],
    };
    let schema = [("pk".to_owned(), true), ("sk".to_owned(), false)];
    let conditions = HashMap::from([("pk".to_owned(), eq(s(1))), ("sk".to_owned(), eq(s(1025)))]);
    let (kc, maps) = desugar_key_conditions(&conditions, &schema).expect("desugars");
    let result = validate_query_key_sizes(&kc, &base_schema(), &maps, &LimitsConfig::default());
    assert_eq!(refused(result), RANGE_MSG);
}
