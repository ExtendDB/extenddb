// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The semantic-pass tests. Each one names the request that pinned it.

use super::{validate_condition_semantics, validate_update_semantics};
use crate::error::DynamoDbError;
use crate::expression::kind::ExpressionKind;
use crate::expression::parser::parse_condition;
use crate::expression::resolver::ExpressionMaps;
use crate::expression::tokenizer::tokenize;
use crate::expression::update_parser::parse_update_sections;
use crate::types::AttributeValue;
use std::collections::HashMap;

const U: &str = "Invalid UpdateExpression: ";
const C: &str = "Invalid ConditionExpression: ";

fn maps(names: &[(&str, &str)], values: &[(&str, AttributeValue)]) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .iter()
            .map(|(k, v)| (k.trim_start_matches('#').to_owned(), (*v).to_owned()))
            .collect::<HashMap<_, _>>(),
        values
            .iter()
            .map(|(k, v)| (k.trim_start_matches(':').to_owned(), v.clone()))
            .collect::<HashMap<_, _>>(),
    )
}

fn n() -> AttributeValue {
    AttributeValue::N("1".to_owned())
}
fn s() -> AttributeValue {
    AttributeValue::S("x".to_owned())
}
fn l() -> AttributeValue {
    AttributeValue::L(vec![])
}

fn update_err(expr: &str, m: &ExpressionMaps) -> String {
    let tokens = tokenize(expr).unwrap();
    let parsed = parse_update_sections(&tokens, Some(expr)).unwrap();
    match validate_update_semantics(&parsed, m) {
        Ok(()) => "OK".to_owned(),
        Err(DynamoDbError::ValidationException(msg)) => msg,
        Err(other) => panic!("unexpected {other:?}"),
    }
}

fn condition_err(expr: &str, m: &ExpressionMaps) -> String {
    let tokens = tokenize(expr).unwrap();
    let parsed = parse_condition(&tokens).unwrap();
    match validate_condition_semantics(&parsed, m, ExpressionKind::Condition) {
        Ok(()) => "OK".to_owned(),
        Err(DynamoDbError::ValidationException(msg)) => msg,
        Err(other) => panic!("unexpected {other:?}"),
    }
}

// Every expectation below is a response captured from Amazon DynamoDB
// on 2026-10-14 (issue #108 notes, probe-fix-round-*.py).

#[test]
fn update_refs_in_expression_order() {
    let m = maps(&[], &[(":v", n())]);
    assert_eq!(
        update_err("SET a = :q, #n = :v", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(
        update_err("SET #n = :q", &m),
        format!(
            "{U}An expression attribute name used in the document path is not defined; attribute name: #n"
        )
    );
}

#[test]
fn repeated_section_after_its_own_actions() {
    let m = maps(&[], &[(":v", n()), (":s", s()), (":n", n())]);
    assert_eq!(
        update_err("SET a = :v SET b = :q", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(
        update_err("SET a = :v SET b = :v ADD n :s", &m),
        format!("{U}The \"SET\" section can only be used once in an update expression;")
    );
    assert_eq!(
        update_err("ADD n :s ADD m :n", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(
        update_err("SET a = :n SET b = :s + :n", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator or function: +, operand type: S"
        )
    );
}

#[test]
fn function_names_are_case_sensitive() {
    let m = maps(&[], &[(":v", n())]);
    assert_eq!(
        update_err("SET x = IF_NOT_EXISTS(x, :v)", &m),
        format!("{U}Invalid function name; function: IF_NOT_EXISTS")
    );
    assert_eq!(
        update_err("SET x = foo(x, :v)", &m),
        format!("{U}Invalid function name; function: foo")
    );
    assert_eq!(
        update_err("SET x = attribute_exists(x)", &m),
        format!(
            "{U}The function is not allowed in an update expression; function: attribute_exists"
        )
    );
    assert_eq!(
        update_err("SET x = IF_NOT_EXISTS(x, :q)", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(update_err("SET x = if_not_exists(x, :v)", &m), "OK");
}

#[test]
fn nested_list_append_before_its_siblings() {
    let m = maps(&[], &[(":a", l()), (":b", l()), (":n", n())]);
    assert_eq!(
        update_err("SET l = if_not_exists(list_append(:a, :b), :q)", &m),
        format!(
            "{U}The function is not allowed to be used this way in an expression; function: list_append"
        )
    );
    assert_eq!(
        update_err("SET l = if_not_exists(list_append(:q, :b), :a)", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(
        update_err("SET l = list_append(:a, :a) + :n", &m),
        format!(
            "{U}The function is not allowed to be used this way in an expression; function: list_append"
        )
    );
    assert_eq!(
        update_err("SET l = list_append(if_not_exists(l, :a), :b)", &m),
        "OK"
    );
}

#[test]
fn operand_counts_and_document_path() {
    let m = maps(&[], &[(":a", l()), (":s", s())]);
    assert_eq!(
        update_err("SET a = if_not_exists(:a)", &m),
        format!(
            "{U}Incorrect number of operands for operator or function; operator or function: if_not_exists, number of operands: 1"
        )
    );
    assert_eq!(
        update_err("SET a = if_not_exists(a, :q, :q2)", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(
        update_err("SET l = if_not_exists(:a, :a)", &m),
        format!(
            "{U}Operator or function requires a document path; operator or function: if_not_exists"
        )
    );
    assert_eq!(
        update_err("SET l = list_append(:s, :a)", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator or function: list_append, operand type: S"
        )
    );
}

#[test]
fn add_and_delete_operand_types() {
    let m = maps(
        &[],
        &[
            (":s", s()),
            (":n", n()),
            (":b", AttributeValue::B(vec![1])),
            (
                ":ss",
                AttributeValue::SS(std::iter::once("a".to_owned()).collect()),
            ),
            (":l", l()),
        ],
    );
    assert_eq!(
        update_err("ADD n :s", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(
        update_err("ADD n :b", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: ADD, operand type: Binary, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(
        update_err("DELETE s :n", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: DELETE, operand type: NUMBER, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(
        update_err("DELETE s :l", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: DELETE, operand type: LIST, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(update_err("ADD n :n, s :ss DELETE t :ss", &m), "OK");
    assert_eq!(
        update_err("ADD n :q", &m),
        format!(
            "{U}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
}

#[test]
fn overlap_after_the_walk() {
    let m = maps(&[], &[(":v", n()), (":s", s())]);
    assert_eq!(
        update_err("SET a = :v, a.b = :v ADD n :s", &m),
        format!(
            "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
        )
    );
    assert_eq!(
        update_err("SET a = :v, a.b = :v", &m),
        format!(
            "{U}Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
        )
    );
}

#[test]
fn condition_function_names_and_positions() {
    let m = maps(&[], &[(":v", s()), (":n", n())]);
    assert_eq!(
        condition_err("ATTRIBUTE_EXISTS(pk)", &m),
        format!("{C}Invalid function name; function: ATTRIBUTE_EXISTS")
    );
    assert_eq!(
        condition_err("foo(pk) AND bar(pk)", &m),
        format!("{C}Invalid function name; function: foo")
    );
    assert_eq!(
        condition_err("if_not_exists(a, :v) = :v", &m),
        format!(
            "{C}The function is not allowed in a condition expression; function: if_not_exists"
        )
    );
    assert_eq!(
        condition_err("attribute_exists(pk) = :v", &m),
        format!(
            "{C}The function is not allowed to be used this way in an expression; function: attribute_exists"
        )
    );
    assert_eq!(
        condition_err("size(pk)", &m),
        format!(
            "{C}The function is not allowed to be used this way in an expression; function: size"
        )
    );
    assert_eq!(
        condition_err("NOT size(pk)", &m),
        format!(
            "{C}The function is not allowed to be used this way in an expression; function: size"
        )
    );
    assert_eq!(
        condition_err("size(attribute_exists(pk)) > :n", &m),
        format!(
            "{C}The function is not allowed to be used this way in an expression; function: attribute_exists"
        )
    );
    assert_eq!(
        condition_err("attribute_exists(size(pk))", &m),
        format!(
            "{C}Operator or function requires a document path; operator or function: attribute_exists"
        )
    );
    assert_eq!(
        condition_err("attribute_exists(:v)", &m),
        format!(
            "{C}Operator or function requires a document path; operator or function: attribute_exists"
        )
    );
    assert_eq!(
        condition_err("foo(#n)", &m),
        format!(
            "{C}An expression attribute name used in the document path is not defined; attribute name: #n"
        )
    );
    assert_eq!(
        condition_err(":q = #n", &m),
        format!(
            "{C}An expression attribute value used in expression is not defined; attribute value: :q"
        )
    );
    assert_eq!(
        condition_err("attribute_type(pk, :v) = :v", &m),
        format!(
            "{C}The function is not allowed to be used this way in an expression; function: attribute_type"
        )
    );
    assert_eq!(condition_err("size(pk) = size(pk)", &m), "OK");
    assert_eq!(condition_err("begins_with(:v, pk)", &m), "OK");
    assert_eq!(
        condition_err("size(pk) > :n AND attribute_exists(pk)", &m),
        "OK"
    );
}

#[test]
fn condition_operand_counts_and_types() {
    let m = maps(
        &[],
        &[
            (":n", n()),
            (":t", AttributeValue::S("S".to_owned())),
            (":xx", AttributeValue::S("XX".to_owned())),
            (":l", l()),
        ],
    );
    assert_eq!(
        condition_err("attribute_exists(pk, pk)", &m),
        format!(
            "{C}Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"
        )
    );
    assert_eq!(
        condition_err("begins_with(pk)", &m),
        format!(
            "{C}Incorrect number of operands for operator or function; operator or function: begins_with, number of operands: 1"
        )
    );
    assert_eq!(
        condition_err("size(pk, pk) > :n", &m),
        format!(
            "{C}Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"
        )
    );
    assert_eq!(
        condition_err("begins_with(pk, :n)", &m),
        format!(
            "{C}Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"
        )
    );
    assert_eq!(
        condition_err("attribute_type(pk, :n)", &m),
        format!(
            "{C}Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"
        )
    );
    assert_eq!(
        condition_err("attribute_type(pk, :xx)", &m),
        format!(
            "{C}Invalid attribute type name found; type: XX, valid types: {{S,SS,N,NS,B,BS,BOOL,NULL,L,M}}"
        )
    );
    assert_eq!(
        condition_err("pk > :l", &m),
        format!(
            "{C}Incorrect operand type for operator or function; operator or function: >, operand type: L"
        )
    );
    assert_eq!(condition_err("attribute_type(pk, :t)", &m), "OK");
}

fn key_condition_err(expr: &str) -> String {
    let tokens = tokenize(expr).unwrap();
    match crate::expression::parse_key_condition(&tokens) {
        Ok(_) => "OK".to_owned(),
        Err(DynamoDbError::ValidationException(msg)) => msg,
        Err(other) => panic!("unexpected {other:?}"),
    }
}

// Captured from Amazon DynamoDB on 2026-10-27: a condition function other than
// `size` and `begins_with` is not a key condition operator, `size` is a nested
// operation, and any other name (or `begins_with` in another case) is not a
// function.
#[test]
fn key_condition_function_names() {
    const K: &str = "Invalid KeyConditionExpression: ";
    for call in [
        "contains(sk, :v)",
        "attribute_exists(sk)",
        "attribute_not_exists(sk)",
        "attribute_type(sk, :t)",
    ] {
        assert_eq!(
            key_condition_err(&format!("pk = :p AND {call}")),
            format!("{K}Invalid operator used in KeyConditionExpression"),
            "{call}"
        );
    }
    assert_eq!(
        key_condition_err("contains(pk, :v) AND sk = :s"),
        format!("{K}Invalid operator used in KeyConditionExpression")
    );
    assert_eq!(
        key_condition_err("pk = :p AND size(sk) > :n"),
        format!("{K}KeyConditionExpressions cannot contain nested operations")
    );
    for name in ["foo", "Begins_With", "Contains"] {
        assert_eq!(
            key_condition_err(&format!("pk = :p AND {name}(sk, :v)")),
            format!("{K}Invalid function name; function: {name}"),
        );
    }
    assert_eq!(key_condition_err("pk = :p AND begins_with(sk, :v)"), "OK");
}

/// The BETWEEN bounds are checked at request time (measured 2026-10-26,
/// HBTW-1: refused on a missing key and after a false AND).
#[test]
fn between_bounds_are_checked_in_the_walk() {
    let m = maps(
        &[],
        &[
            (":hi", AttributeValue::N("5".to_owned())),
            (":lo", AttributeValue::N("1".to_owned())),
        ],
    );
    assert_eq!(
        condition_err("a BETWEEN :hi AND :lo", &m),
        format!(
            "{C}The BETWEEN operator requires upper bound to be greater than or equal to lower \
             bound; lower bound operand: AttributeValue: {{N:5}}, upper bound operand: \
             AttributeValue: {{N:1}}"
        )
    );
    assert!(condition_err("attribute_exists(b) AND a BETWEEN :hi AND :lo", &m).contains("BETWEEN"));
}
