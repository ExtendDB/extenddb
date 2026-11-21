// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The projection and unused-name checks of both grammars. Each expectation
//! is a live SearchVectors answer, measured twice in eu-north-1 on
//! 2026-11-21.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::AttributeValue;

use super::{check, equality_conditions};

const UNUSED_U: &str =
    "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}";

fn names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn us() -> HashMap<String, AttributeValue> {
    [(":us".to_owned(), AttributeValue::S("US".to_owned()))].into()
}

fn message<T: std::fmt::Debug>(result: Result<T, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => format!("not a ValidationException: {other:?}"),
    }
}

#[test]
fn the_projection_answers_before_the_condition() {
    let limits = LimitsConfig::default();
    assert_eq!(
        message(check(
            None,
            Some(&us()),
            Some("Country = = :us"),
            Some("#p"),
            &limits
        )),
        "Invalid ProjectionExpression: An expression attribute name used in the document \
         path is not defined; attribute name: #p"
    );
}

#[test]
fn without_a_condition_a_name_must_be_used_by_the_projection() {
    let limits = LimitsConfig::default();
    let unused = names(&[("#u", "x")]);
    assert_eq!(
        message(check(Some(&unused), None, None, Some("Price"), &limits)),
        UNUSED_U
    );
    let used = names(&[("#p", "Price")]);
    assert!(check(Some(&used), None, None, Some("#p"), &limits).is_ok());
}

#[test]
fn the_equality_grammar_refuses_an_unused_name_before_an_unused_value() {
    let unused = names(&[("#u", "x")]);
    let mut values = us();
    values.insert(":w".to_owned(), AttributeValue::S("w".to_owned()));
    assert_eq!(
        message(equality_conditions(
            "Country = :us",
            Some(&unused),
            Some(&values),
            None
        )),
        UNUSED_U
    );
    let used = names(&[("#p", "Price")]);
    assert!(equality_conditions("Country = :us", Some(&used), Some(&us()), Some("#p")).is_ok());
}
