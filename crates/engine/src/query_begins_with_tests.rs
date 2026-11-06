// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for [`refuse_begins_with_on_number`]: the type of the `begins_with`
//! value decides the refusal, not the sort key's type (measured 2026-11-06).

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    ExpressionMaps, KeyCondition, parse_key_condition, tokenize_with_limit,
};
use extenddb_core::types::{AttributeDefinition, AttributeValue, ScalarAttributeType};

use super::refuse_begins_with_on_number;
use crate::query_key_values::{KEY_CONDITION_TYPE_MISMATCH, validate_key_condition_types};

const OPERAND_TYPE: &str = "Invalid KeyConditionExpression: Incorrect operand type for operator or \
                            function; operator or function: begins_with, operand type: ";

const SORT_KEY_TYPES: [ScalarAttributeType; 3] = [
    ScalarAttributeType::S,
    ScalarAttributeType::N,
    ScalarAttributeType::B,
];

fn parse(kce: &str) -> KeyCondition {
    let tokens = tokenize_with_limit(kce, 1000).expect("tokenize");
    parse_key_condition(&tokens).expect("parse")
}

fn values(pairs: Vec<(&str, AttributeValue)>) -> ExpressionMaps {
    ExpressionMaps::new(
        HashMap::new(),
        pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
    )
}

fn message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The answer to `pk = :u AND begins_with(sk, :b)` with an S partition key and
/// a sort key of `sk_type`: the refusal, then the type check that
/// `handle_query` runs later. `None` when both pass.
fn answer(sk_type: ScalarAttributeType, value: AttributeValue) -> Option<String> {
    let key_condition = parse("pk = :u AND begins_with(sk, :b)");
    let maps = values(vec![("u", AttributeValue::S("x".to_owned())), ("b", value)]);
    let definitions = [
        AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_owned(),
            attribute_type: sk_type,
        },
    ];
    message(
        refuse_begins_with_on_number(&key_condition, &maps)
            .and_then(|()| validate_key_condition_types(&key_condition, &maps, &definitions)),
    )
}

#[test]
fn number_and_bool_values_are_refused_with_their_type_on_every_sort_key_type() {
    for sk_type in SORT_KEY_TYPES {
        for (value, type_code) in [
            (AttributeValue::N("1".to_owned()), "N"),
            (AttributeValue::Bool(true), "BOOL"),
        ] {
            assert_eq!(
                answer(sk_type, value),
                Some(format!("{OPERAND_TYPE}{type_code}")),
                "{sk_type:?} sort key"
            );
        }
    }
}

#[test]
fn string_and_binary_values_pass_to_the_type_check_on_every_sort_key_type() {
    for sk_type in SORT_KEY_TYPES {
        for (value, value_type) in [
            (AttributeValue::S("1".to_owned()), ScalarAttributeType::S),
            (AttributeValue::B(vec![1]), ScalarAttributeType::B),
        ] {
            let expected = (value_type != sk_type).then(|| KEY_CONDITION_TYPE_MISMATCH.to_owned());
            assert_eq!(
                answer(sk_type, value),
                expected,
                "{value_type:?} value on {sk_type:?} sort key"
            );
        }
    }
}

#[test]
fn a_placeholder_without_a_value_passes() {
    let key_condition = parse("pk = :u AND begins_with(sk, :zz)");
    let maps = values(vec![("u", AttributeValue::S("x".to_owned()))]);
    assert!(refuse_begins_with_on_number(&key_condition, &maps).is_ok());
}
