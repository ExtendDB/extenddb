// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the legacy condition validator; every expected message was
//! measured against Amazon DynamoDB on 2026-10-23.

use std::collections::BTreeMap;

use super::*;

fn msg(op: &str, values: &[AttributeValue]) -> Option<String> {
    match validate_legacy_condition(op, values) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(m)) => Some(m),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

fn refusal(text: &str) -> Option<String> {
    Some(format!("{INVALID_PARAMS}{text}"))
}

fn type_refusal(op: &str, ty: &str) -> Option<String> {
    refusal(&format!(
        "ComparisonOperator {op} is not valid for {ty} AttributeValue type"
    ))
}

fn one_of_each() -> Vec<(&'static str, AttributeValue)> {
    vec![
        ("S", AttributeValue::S("a".to_owned())),
        ("N", AttributeValue::N("1".to_owned())),
        ("B", AttributeValue::B(vec![1])),
        ("SS", AttributeValue::SS(["a".to_owned()].into())),
        ("NS", AttributeValue::NS(["1".to_owned()].into())),
        ("BS", AttributeValue::BS([vec![1]].into())),
        ("BOOL", AttributeValue::Bool(true)),
        ("NULL", AttributeValue::Null),
        ("L", AttributeValue::L(Vec::new())),
        ("M", AttributeValue::M(BTreeMap::new())),
    ]
}

/// The operator by type grid measured on 2026-10-23 (ScanFilter,
/// QueryFilter and KeyConditions gave the same rows).
#[test]
fn operator_by_type_grid() {
    let accepted = |op: &str, ty: &str| match op {
        "EQ" | "NE" => true,
        "BEGINS_WITH" => matches!(ty, "S" | "B"),
        "CONTAINS" | "NOT_CONTAINS" => matches!(ty, "S" | "N" | "B" | "BOOL" | "NULL"),
        _ => matches!(ty, "S" | "N" | "B"),
    };
    for op in [
        "EQ",
        "NE",
        "LE",
        "LT",
        "GE",
        "GT",
        "CONTAINS",
        "NOT_CONTAINS",
        "BEGINS_WITH",
        "IN",
        "BETWEEN",
    ] {
        for (ty, value) in one_of_each() {
            let values = if op == "BETWEEN" {
                vec![value.clone(), value]
            } else {
                vec![value]
            };
            let expected = if accepted(op, ty) {
                None
            } else {
                type_refusal(op, ty)
            };
            assert_eq!(msg(op, &values), expected, "{op} with {ty}");
        }
    }
    for op in ["NULL", "NOT_NULL"] {
        assert_eq!(msg(op, &[]), None);
    }
}

#[test]
fn value_counts() {
    let s = AttributeValue::S("a".to_owned());
    let count = |op: &str| {
        refusal(&format!(
            "Invalid number of argument(s) for the {op} ComparisonOperator"
        ))
    };
    for op in [
        "EQ",
        "NE",
        "LE",
        "LT",
        "GE",
        "GT",
        "CONTAINS",
        "NOT_CONTAINS",
        "BEGINS_WITH",
    ] {
        assert_eq!(msg(op, &[]), count(op), "{op} with 0");
        assert_eq!(msg(op, &[s.clone(), s.clone()]), count(op), "{op} with 2");
    }
    for op in ["NULL", "NOT_NULL"] {
        assert_eq!(msg(op, std::slice::from_ref(&s)), count(op), "{op} with 1");
    }
    assert_eq!(msg("IN", &[]), count("IN"));
    assert_eq!(msg("IN", &[s.clone(), s.clone(), s.clone()]), None);
    assert_eq!(msg("BETWEEN", std::slice::from_ref(&s)), count("BETWEEN"));
    assert_eq!(
        msg("BETWEEN", &[s.clone(), s.clone(), s.clone()]),
        count("BETWEEN")
    );
    assert_eq!(
        msg("FOO", std::slice::from_ref(&s)),
        refusal("Invalid comparison operator")
    );
    assert_eq!(
        msg("eq", std::slice::from_ref(&s)),
        refusal("Invalid comparison operator")
    );
}

#[test]
fn mixed_types_and_ranges() {
    let s = AttributeValue::S("a".to_owned());
    let n = AttributeValue::N("1".to_owned());
    let l = AttributeValue::L(Vec::new());
    let b = AttributeValue::Bool(true);
    let same = refusal("AttributeValues inside AttributeValueList must be of same type");
    // The first value's type is checked; the rest are held to it.
    assert_eq!(msg("BETWEEN", &[s.clone(), n.clone()]), same);
    assert_eq!(msg("BETWEEN", &[n.clone(), l.clone()]), same);
    assert_eq!(
        msg("BETWEEN", &[l.clone(), n.clone()]),
        type_refusal("BETWEEN", "L")
    );
    assert_eq!(
        msg("BETWEEN", &[b, l.clone()]),
        type_refusal("BETWEEN", "BOOL")
    );
    assert_eq!(msg("IN", &[s.clone(), n.clone()]), same);
    assert_eq!(msg("IN", &[l, s.clone()]), type_refusal("IN", "L"));
    assert_eq!(msg("IN", &[s.clone(), AttributeValue::L(Vec::new())]), same);
    let range = refusal(
        "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound",
    );
    assert_eq!(
        msg("BETWEEN", &[AttributeValue::N("2".to_owned()), n.clone()]),
        range
    );
    assert_eq!(
        msg(
            "BETWEEN",
            &[
                AttributeValue::N("10".to_owned()),
                AttributeValue::N("9".to_owned())
            ]
        ),
        range
    );
    assert_eq!(
        msg("BETWEEN", &[AttributeValue::S("b".to_owned()), s.clone()]),
        range
    );
    assert_eq!(
        msg("BETWEEN", &[n.clone(), AttributeValue::N("10".to_owned())]),
        None
    );
    assert_eq!(msg("BETWEEN", &[n.clone(), n]), None);
}

#[test]
fn desugar_filter_refuses_before_desugaring() {
    let mut filter = HashMap::new();
    filter.insert(
        "f".to_owned(),
        Condition {
            comparison_operator: "BEGINS_WITH".to_owned(),
            attribute_value_list: vec![AttributeValue::N("1".to_owned())],
        },
    );
    let err = desugar_filter(&filter, ConditionalOperator::And).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == type_refusal("BEGINS_WITH", "N")),
        "{err:?}"
    );
}

#[test]
fn key_conditions_refuse_non_indexable_operators_after_the_value_checks() {
    let schema = vec![("pk".to_owned(), true), ("sk".to_owned(), false)];
    let eq = |v: AttributeValue| Condition {
        comparison_operator: "EQ".to_owned(),
        attribute_value_list: vec![v],
    };
    for (op, values) in [
        ("NE", vec![AttributeValue::S("a".to_owned())]),
        ("NULL", Vec::new()),
        ("NOT_NULL", Vec::new()),
        ("CONTAINS", vec![AttributeValue::S("a".to_owned())]),
        ("NOT_CONTAINS", vec![AttributeValue::N("1".to_owned())]),
        (
            "IN",
            vec![
                AttributeValue::S("a".to_owned()),
                AttributeValue::S("b".to_owned()),
            ],
        ),
    ] {
        let mut kc = HashMap::new();
        kc.insert("pk".to_owned(), eq(AttributeValue::S("p".to_owned())));
        kc.insert(
            "sk".to_owned(),
            Condition {
                comparison_operator: op.to_owned(),
                attribute_value_list: values,
            },
        );
        let err = desugar_key_conditions(&kc, &schema).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == refusal("Attempted conditional constraint is not an indexable operation")),
            "{op}: {err:?}"
        );
    }
    // A value the operator does not take is reported first, as is a count.
    let mut kc = HashMap::new();
    kc.insert("pk".to_owned(), eq(AttributeValue::S("p".to_owned())));
    kc.insert(
        "sk".to_owned(),
        Condition {
            comparison_operator: "CONTAINS".to_owned(),
            attribute_value_list: vec![AttributeValue::L(Vec::new())],
        },
    );
    let err = desugar_key_conditions(&kc, &schema).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == type_refusal("CONTAINS", "L")),
        "{err:?}"
    );
}
