// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the legacy condition validator; every expected message was
//! measured against Amazon DynamoDB on 2026-10-23 and 2026-10-24.

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

fn cond(op: &str, values: Vec<AttributeValue>) -> Condition {
    Condition {
        comparison_operator: op.to_owned(),
        attribute_value_list: values,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn kc(entries: Vec<(&str, Condition)>) -> HashMap<String, Condition> {
    entries
        .into_iter()
        .map(|(name, c)| (name.to_owned(), c))
        .collect()
}

fn schema(
    keys: &[(&str, KeyType, ScalarAttributeType)],
) -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
    (
        keys.iter()
            .map(|(name, key_type, _)| KeySchemaElement {
                attribute_name: (*name).to_owned(),
                key_type: *key_type,
            })
            .collect(),
        keys.iter()
            .map(|(name, _, ty)| AttributeDefinition {
                attribute_name: (*name).to_owned(),
                attribute_type: *ty,
            })
            .collect(),
    )
}

fn ss_schema() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
    schema(&[
        ("pk", KeyType::Hash, ScalarAttributeType::S),
        ("sk", KeyType::Range, ScalarAttributeType::S),
    ])
}

fn key_error(
    conditions: &HashMap<String, Condition>,
    (ks, defs): &(Vec<KeySchemaElement>, Vec<AttributeDefinition>),
) -> Option<String> {
    match desugar_key_conditions(conditions, ks, defs) {
        Ok(_) => None,
        Err(DynamoDbError::ValidationException(m)) => Some(m),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

const SCHEMA_TYPE: &str = "Condition parameter type does not match schema type";
const NOT_INDEXABLE: &str = "Attempted conditional constraint is not an indexable operation";

fn empty_key(name: &str) -> Option<String> {
    Some(format!(
        "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: {name}"
    ))
}

/// A number that does not parse is reported before the operator name, the
/// count, the operator type, the same-type check and the range, inside sets,
/// lists and maps too (measured 2026-10-24).
#[test]
fn numbers_are_checked_before_the_operator_checks() {
    let bad = |raw: &str| {
        Some(format!(
            "The parameter cannot be converted to a numeric value: {raw}"
        ))
    };
    assert_eq!(msg("EQ", &[n("abc")]), bad("abc"));
    assert_eq!(msg("EQ", &[n("12e")]), bad("12e"));
    assert_eq!(msg("BEGINS_WITH", &[n("abc")]), bad("abc"));
    assert_eq!(msg("FOO", &[n("abc")]), bad("abc"));
    assert_eq!(msg("EQ", &[n("abc"), s("x")]), bad("abc"));
    assert_eq!(msg("EQ", &[s("x"), n("abc")]), bad("abc"));
    assert_eq!(msg("IN", &[s("x"), n("abc")]), bad("abc"));
    assert_eq!(msg("NULL", &[n("abc")]), bad("abc"));
    assert_eq!(msg("BETWEEN", &[n("2"), n("abc")]), bad("abc"));
    assert_eq!(msg("BETWEEN", &[n("abc"), n("2")]), bad("abc"));
    assert_eq!(
        msg(
            "EQ",
            &[AttributeValue::NS(
                ["1".to_owned(), "abc".to_owned()].into()
            )]
        ),
        bad("abc")
    );
    assert_eq!(msg("EQ", &[AttributeValue::L(vec![n("abc")])]), bad("abc"));
    assert_eq!(
        msg(
            "EQ",
            &[AttributeValue::M([("k".to_owned(), n("abc"))].into())]
        ),
        bad("abc")
    );
    assert_eq!(msg("LT", &[AttributeValue::L(vec![n("abc")])]), bad("abc"));
    assert_eq!(
        msg("EQ", &[n("1e400")]),
        Some(
            "Number overflow. Attempting to store a number with magnitude larger than supported range"
                .to_owned()
        )
    );
    assert_eq!(msg("EQ", &[n("1")]), None);
    assert_eq!(msg("EQ", &[n("-1.5e3")]), None);
}

#[test]
fn key_conditions_refuse_non_indexable_operators_on_any_key_at_request_time() {
    for (op, values) in [
        ("NE", vec![s("a")]),
        ("NULL", Vec::new()),
        ("NOT_NULL", Vec::new()),
        ("CONTAINS", vec![s("a")]),
        ("NOT_CONTAINS", vec![n("1")]),
        ("IN", vec![s("a"), s("b")]),
    ] {
        let on_sk = kc(vec![
            ("pk", cond("EQ", vec![s("p")])),
            ("sk", cond(op, values.clone())),
        ]);
        let on_pk = kc(vec![("pk", cond(op, values))]);
        for conditions in [&on_sk, &on_pk] {
            let err = validate_key_conditions(conditions).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == refusal(NOT_INDEXABLE)),
                "{op}: {err:?}"
            );
        }
    }
    // A value the operator does not take is reported first, as is a count.
    let typed = kc(vec![
        ("pk", cond("EQ", vec![s("p")])),
        ("sk", cond("CONTAINS", vec![AttributeValue::L(Vec::new())])),
    ]);
    let err = validate_key_conditions(&typed).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == type_refusal("CONTAINS", "L")),
        "{err:?}"
    );
    // Non-indexable is reported ahead of a schema type mismatch on the other key.
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![n("1")])),
                ("sk", cond("NE", vec![s("a")]))
            ]),
            &ss_schema()
        ),
        refusal(NOT_INDEXABLE)
    );
    assert!(validate_key_conditions(&kc(vec![("pk", cond("EQ", vec![s("p")]))])).is_ok());
}

#[test]
fn key_conditions_refuse_a_value_of_the_wrong_schema_type() {
    let ss = ss_schema();
    let sn = schema(&[
        ("pk", KeyType::Hash, ScalarAttributeType::S),
        ("sk", KeyType::Range, ScalarAttributeType::N),
    ]);
    let pk_s = || ("pk", cond("EQ", vec![s("p")]));
    for hash in [
        n("1"),
        AttributeValue::B(vec![1]),
        AttributeValue::Bool(true),
    ] {
        assert_eq!(
            key_error(&kc(vec![("pk", cond("EQ", vec![hash]))]), &ss),
            refusal(SCHEMA_TYPE)
        );
    }
    for range in [
        cond("EQ", vec![n("1")]),
        cond("LE", vec![AttributeValue::B(vec![1])]),
        cond("BETWEEN", vec![n("1"), n("2")]),
        cond("BEGINS_WITH", vec![AttributeValue::B(vec![1])]),
    ] {
        assert_eq!(
            key_error(&kc(vec![pk_s(), ("sk", range)]), &ss),
            refusal(SCHEMA_TYPE)
        );
    }
    for range in [
        cond("EQ", vec![s("abc")]),
        cond("BEGINS_WITH", vec![s("1")]),
        cond("BETWEEN", vec![s("a"), s("b")]),
        cond("LT", vec![s("5")]),
    ] {
        assert_eq!(
            key_error(&kc(vec![pk_s(), ("sk", range)]), &sn),
            refusal(SCHEMA_TYPE)
        );
    }
    // Both keys wrong: one message, no attribute named.
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![n("1")])),
                ("sk", cond("EQ", vec![n("1")]))
            ]),
            &ss
        ),
        refusal(SCHEMA_TYPE)
    );
    // The hash key's type is checked before its operator; a well-typed hash key
    // with a non-EQ operator is refused before the range key's type.
    assert_eq!(
        key_error(&kc(vec![("pk", cond("LT", vec![n("1")]))]), &ss),
        refusal(SCHEMA_TYPE)
    );
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("LT", vec![s("p")])),
                ("sk", cond("EQ", vec![n("1")]))
            ]),
            &ss
        ),
        Some("Query key condition not supported".to_owned())
    );
    // Type before a condition on a non-key attribute; an absent hash key first.
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![n("1")])),
                ("foo", cond("EQ", vec![s("x")]))
            ]),
            &ss
        ),
        refusal(SCHEMA_TYPE)
    );
    assert_eq!(
        key_error(&kc(vec![("sk", cond("EQ", vec![n("1")]))]), &ss),
        Some("Query condition missed key schema element: pk".to_owned())
    );
    // Well-typed conditions pass.
    assert!(key_error(&kc(vec![pk_s(), ("sk", cond("EQ", vec![n("1")]))]), &sn).is_none());
    assert!(
        key_error(
            &kc(vec![pk_s(), ("sk", cond("BETWEEN", vec![s("a"), s("b")]))]),
            &ss
        )
        .is_none()
    );
}

#[test]
fn key_conditions_refuse_an_empty_string_after_the_schema_checks() {
    let ss = ss_schema();
    let pk_s = || ("pk", cond("EQ", vec![s("p")]));
    assert_eq!(
        key_error(&kc(vec![("pk", cond("EQ", vec![s("")]))]), &ss),
        empty_key("pk")
    );
    for op in ["EQ", "LE", "LT", "GE", "GT", "BEGINS_WITH"] {
        assert_eq!(
            key_error(&kc(vec![pk_s(), ("sk", cond(op, vec![s("")]))]), &ss),
            empty_key("sk"),
            "{op}"
        );
    }
    assert_eq!(
        key_error(
            &kc(vec![pk_s(), ("sk", cond("BETWEEN", vec![s(""), s("a")]))]),
            &ss
        ),
        empty_key("sk")
    );
    assert_eq!(
        key_error(
            &kc(vec![pk_s(), ("sk", cond("BETWEEN", vec![s(""), s("")]))]),
            &ss
        ),
        empty_key("sk")
    );
    // Hash key named first; schema type, operator and non-key checks come first.
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![s("")])),
                ("sk", cond("EQ", vec![s("")]))
            ]),
            &ss
        ),
        empty_key("pk")
    );
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![s("")])),
                ("sk", cond("EQ", vec![n("1")]))
            ]),
            &ss
        ),
        refusal(SCHEMA_TYPE)
    );
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![n("1")])),
                ("sk", cond("EQ", vec![s("")]))
            ]),
            &ss
        ),
        refusal(SCHEMA_TYPE)
    );
    assert_eq!(
        key_error(&kc(vec![("pk", cond("LT", vec![s("")]))]), &ss),
        Some("Query key condition not supported".to_owned())
    );
    assert_eq!(
        key_error(
            &kc(vec![
                ("pk", cond("EQ", vec![s("")])),
                ("foo", cond("EQ", vec![s("x")]))
            ]),
            &ss
        ),
        Some("Query condition missed key schema element: pk".to_owned())
    );
    // An inverted range is a request-time error and wins over the empty bound.
    assert_eq!(
        key_error(
            &kc(vec![pk_s(), ("sk", cond("BETWEEN", vec![s("a"), s("")]))]),
            &ss
        ),
        refusal(
            "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
        )
    );
    // An empty B on an S key is a schema type mismatch.
    assert_eq!(
        key_error(
            &kc(vec![(
                "pk",
                cond("EQ", vec![AttributeValue::B(Vec::new())])
            )]),
            &ss
        ),
        refusal(SCHEMA_TYPE)
    );
}

#[test]
fn query_filter_may_not_name_a_key_of_the_queried_schema() {
    let (ks, _) = ss_schema();
    let on_key = |name: &str| {
        Some(format!(
            "QueryFilter can only contain non-primary key attributes: Primary key attribute: {name}"
        ))
    };
    let err = |filter: &HashMap<String, Condition>| match reject_query_filter_on_key(filter, &ks) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(m)) => Some(m),
        Err(other) => panic!("unexpected error {other:?}"),
    };
    assert_eq!(
        err(&kc(vec![("pk", cond("EQ", vec![s("x")]))])),
        on_key("pk")
    );
    assert_eq!(
        err(&kc(vec![("sk", cond("EQ", vec![s("x")]))])),
        on_key("sk")
    );
    assert_eq!(
        err(&kc(vec![("pk", cond("NOT_NULL", vec![]))])),
        on_key("pk")
    );
    // Hash key named first whatever the map order.
    assert_eq!(
        err(&kc(vec![
            ("sk", cond("EQ", vec![s("x")])),
            ("pk", cond("EQ", vec![s("x")]))
        ])),
        on_key("pk")
    );
    assert_eq!(err(&kc(vec![("zz", cond("EQ", vec![s("x")]))])), None);
    // Only the queried schema's keys count: a GSI schema frees the table keys.
    let (gsi, _) = schema(&[
        ("gpk", KeyType::Hash, ScalarAttributeType::S),
        ("gsk", KeyType::Range, ScalarAttributeType::S),
    ]);
    assert!(reject_query_filter_on_key(&kc(vec![("pk", cond("EQ", vec![s("x")]))]), &gsi).is_ok());
    assert!(
        reject_query_filter_on_key(&kc(vec![("gsk", cond("EQ", vec![s("x")]))]), &gsi).is_err()
    );
}
