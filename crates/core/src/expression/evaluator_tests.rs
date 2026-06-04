// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::parser::parse_condition;
use crate::expression::resolver::ExpressionMaps;
use crate::expression::tokenizer::tokenize;
use std::collections::HashMap;

fn eval(
    expr_str: &str,
    item: &BTreeMap<String, AttributeValue>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
) -> Result<bool, DynamoDbError> {
    let tokens = tokenize(expr_str)?;
    let expr = parse_condition(&tokens)?;
    let maps = ExpressionMaps::new(names, values);
    evaluate_condition(&expr, item, &maps)
}

fn simple_item() -> BTreeMap<String, AttributeValue> {
    let mut item = BTreeMap::new();
    item.insert("name".into(), AttributeValue::S("Alice".into()));
    item.insert("age".into(), AttributeValue::N("30".into()));
    item.insert("active".into(), AttributeValue::Bool(true));
    item
}

#[test]
fn string_equality_true() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("Alice".into()));
    assert!(eval("name = :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn string_equality_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("Bob".into()));
    assert!(!eval("name = :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn numeric_greater_than() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::N("25".into()));
    assert!(eval("age > :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn numeric_less_than_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::N("25".into()));
    assert!(!eval("age < :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn and_both_true() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("n".into(), AttributeValue::S("Alice".into()));
    values.insert("a".into(), AttributeValue::N("25".into()));
    assert!(eval("name = :n AND age > :a", &item, HashMap::new(), values).unwrap());
}

#[test]
fn and_one_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("n".into(), AttributeValue::S("Bob".into()));
    values.insert("a".into(), AttributeValue::N("25".into()));
    assert!(!eval("name = :n AND age > :a", &item, HashMap::new(), values).unwrap());
}

#[test]
fn or_one_true() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("n".into(), AttributeValue::S("Bob".into()));
    values.insert("a".into(), AttributeValue::N("25".into()));
    assert!(eval("name = :n OR age > :a", &item, HashMap::new(), values).unwrap());
}

#[test]
fn not_inverts() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("Bob".into()));
    assert!(eval("NOT name = :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn attribute_exists_present() {
    let item = simple_item();
    assert!(
        eval(
            "attribute_exists(name)",
            &item,
            HashMap::new(),
            HashMap::new()
        )
        .unwrap()
    );
}

#[test]
fn attribute_exists_absent() {
    let item = simple_item();
    assert!(
        !eval(
            "attribute_exists(missing)",
            &item,
            HashMap::new(),
            HashMap::new()
        )
        .unwrap()
    );
}

#[test]
fn attribute_not_exists_absent() {
    let item = simple_item();
    assert!(
        eval(
            "attribute_not_exists(missing)",
            &item,
            HashMap::new(),
            HashMap::new()
        )
        .unwrap()
    );
}

#[test]
fn begins_with_true() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("p".into(), AttributeValue::S("Ali".into()));
    assert!(eval("begins_with(name, :p)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn begins_with_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("p".into(), AttributeValue::S("Bob".into()));
    assert!(!eval("begins_with(name, :p)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn begins_with_rejects_number_operand() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("p".into(), AttributeValue::N("1".into()));
    let result = eval("begins_with(name, :p)", &item, HashMap::new(), values);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("operand type: N"));
}

#[test]
fn contains_string_substring() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("s".into(), AttributeValue::S("lic".into()));
    assert!(eval("contains(name, :s)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn contains_duplicate_operand_rejected() {
    let item = simple_item();
    let result = eval(
        "contains(name, name)",
        &item,
        HashMap::new(),
        HashMap::new(),
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("first operand must be distinct"));
}

#[test]
fn between_in_range() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("lo".into(), AttributeValue::N("20".into()));
    values.insert("hi".into(), AttributeValue::N("40".into()));
    assert!(eval("age BETWEEN :lo AND :hi", &item, HashMap::new(), values).unwrap());
}

#[test]
fn between_out_of_range() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("lo".into(), AttributeValue::N("31".into()));
    values.insert("hi".into(), AttributeValue::N("40".into()));
    assert!(!eval("age BETWEEN :lo AND :hi", &item, HashMap::new(), values).unwrap());
}

#[test]
fn in_expression_match() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("a".into(), AttributeValue::S("Alice".into()));
    values.insert("b".into(), AttributeValue::S("Bob".into()));
    assert!(eval("name IN (:a, :b)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn in_expression_no_match() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("a".into(), AttributeValue::S("Bob".into()));
    values.insert("b".into(), AttributeValue::S("Carol".into()));
    assert!(!eval("name IN (:a, :b)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn missing_attribute_comparison_returns_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("x".into()));
    assert!(!eval("missing = :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn name_ref_resolution() {
    let item = simple_item();
    let mut names = HashMap::new();
    names.insert("n".into(), "name".into());
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("Alice".into()));
    assert!(eval("#n = :v", &item, names, values).unwrap());
}

#[test]
fn size_function_in_comparison() {
    let mut item = BTreeMap::new();
    item.insert("data".into(), AttributeValue::S("hello".into()));
    let mut values = HashMap::new();
    values.insert("sz".into(), AttributeValue::N("3".into()));
    assert!(eval("size(data) > :sz", &item, HashMap::new(), values).unwrap());
}

#[test]
fn size_function_uses_utf16_code_units() {
    // "é" = 1 UTF-16 code unit, "𝄞" = 2 UTF-16 code units (surrogate pair) → total 3
    let mut item = BTreeMap::new();
    item.insert("s".into(), AttributeValue::S("é𝄞".into()));
    let mut values = HashMap::new();
    values.insert("n".into(), AttributeValue::N("3".into()));
    assert!(eval("size(s) = :n", &item, HashMap::new(), values).unwrap());
}

#[test]
fn attribute_type_check() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("t".into(), AttributeValue::S("S".into()));
    assert!(eval("attribute_type(name, :t)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn nested_map_path() {
    let mut inner = BTreeMap::new();
    inner.insert("city".into(), AttributeValue::S("NYC".into()));
    let mut item = BTreeMap::new();
    item.insert("address".into(), AttributeValue::M(inner));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("NYC".into()));
    assert!(eval("address.city = :v", &item, HashMap::new(), values).unwrap());
}

#[test]
fn contains_in_string_set() {
    let mut item = BTreeMap::new();
    let mut set = std::collections::BTreeSet::new();
    set.insert("red".into());
    set.insert("blue".into());
    item.insert("colors".into(), AttributeValue::SS(set));
    let mut values = HashMap::new();
    values.insert("c".into(), AttributeValue::S("red".into()));
    assert!(eval("contains(colors, :c)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn contains_in_list() {
    let mut item = BTreeMap::new();
    item.insert(
        "tags".into(),
        AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::S("b".into()),
        ]),
    );
    let mut values = HashMap::new();
    values.insert("t".into(), AttributeValue::S("a".into()));
    assert!(eval("contains(tags, :t)", &item, HashMap::new(), values).unwrap());
}

#[test]
fn ne_on_missing_attribute_is_true() {
    let item = BTreeMap::new();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("anything".into()));
    assert!(
        eval(
            "#a <> :v",
            &item,
            HashMap::from([("a".into(), "missing".into())]),
            values
        )
        .unwrap()
    );
}

#[test]
fn eq_on_missing_attribute_is_false() {
    let item = BTreeMap::new();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("anything".into()));
    assert!(
        !eval(
            "#a = :v",
            &item,
            HashMap::from([("a".into(), "missing".into())]),
            values
        )
        .unwrap()
    );
}

#[test]
fn lt_on_missing_attribute_is_false() {
    let item = BTreeMap::new();
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("anything".into()));
    assert!(
        !eval(
            "#a < :v",
            &item,
            HashMap::from([("a".into(), "missing".into())]),
            values
        )
        .unwrap()
    );
}

#[test]
fn ne_both_missing_is_true() {
    let item = BTreeMap::new();
    let names = HashMap::from([("a".into(), "x".into()), ("b".into(), "y".into())]);
    assert!(eval("#a <> #b", &item, names, HashMap::new()).unwrap());
}

#[test]
fn eq_both_missing_is_false() {
    let item = BTreeMap::new();
    let names = HashMap::from([("a".into(), "x".into()), ("b".into(), "y".into())]);
    assert!(!eval("#a = #b", &item, names, HashMap::new()).unwrap());
}

#[test]
fn size_of_missing_attribute_returns_false() {
    // DynamoDB: size(nonexistent) causes the comparison to evaluate to false
    // (the item is skipped), not to return 0.
    let item = simple_item(); // has "name", "age", "active" — no "missing"
    let mut values = HashMap::new();
    values.insert("zero".into(), AttributeValue::N("0".into()));
    // size(missing) = :zero should be false (not true)
    assert!(!eval("size(missing) = :zero", &item, HashMap::new(), values).unwrap());
}

#[test]
fn size_of_missing_attribute_lt_returns_false() {
    let item = simple_item();
    let mut values = HashMap::new();
    values.insert("one".into(), AttributeValue::N("1".into()));
    // size(missing) < :one should be false
    assert!(!eval("size(missing) < :one", &item, HashMap::new(), values).unwrap());
}
