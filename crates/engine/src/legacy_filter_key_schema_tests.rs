// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the legacy `KeyConditions` and `QueryFilter` checks that need
//! the key schema. Every expected message was measured against Amazon
//! DynamoDB on 2026-10-23, 2026-10-24 and 2026-11-12.

use super::*;

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
    // The handler runs this before the table lookup, so non-indexable is
    // reported ahead of a schema type mismatch on the other key.
    let err = validate_key_conditions(&kc(vec![
        ("pk", cond("EQ", vec![n("1")])),
        ("sk", cond("NE", vec![s("a")])),
    ]))
    .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == refusal(NOT_INDEXABLE)),
        "{err:?}"
    );
    assert!(validate_key_conditions(&kc(vec![("pk", cond("EQ", vec![s("p")]))])).is_ok());
    // The range key desugaring gives the same refusal for such an operator.
    let err =
        desugar_sk_condition(&cond("NE", vec![s("a")]), "sk", &mut HashMap::new()).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == refusal(NOT_INDEXABLE)),
        "{err:?}"
    );
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
    // B keys: a B value passes and an S value is refused (measured
    // 2026-11-12 on a B/B table).
    let bb = schema(&[
        ("pk", KeyType::Hash, ScalarAttributeType::B),
        ("sk", KeyType::Range, ScalarAttributeType::B),
    ]);
    let pk_b = || ("pk", cond("EQ", vec![AttributeValue::B(vec![1])]));
    assert_eq!(
        key_error(&kc(vec![("pk", cond("EQ", vec![s("x")]))]), &bb),
        refusal(SCHEMA_TYPE)
    );
    assert_eq!(
        key_error(&kc(vec![pk_b(), ("sk", cond("EQ", vec![s("x")]))]), &bb),
        refusal(SCHEMA_TYPE)
    );
    assert!(key_error(&kc(vec![pk_b()]), &bb).is_none());
}

#[test]
fn key_conditions_refuse_an_empty_key_value() {
    let ss = ss_schema();
    let pk_s = || ("pk", cond("EQ", vec![s("p")]));
    let empty = |conditions: &HashMap<String, Condition>, ks: &[KeySchemaElement]| {
        match reject_empty_key_values(conditions, ks) {
            Ok(()) => None,
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    };
    assert_eq!(
        empty(&kc(vec![("pk", cond("EQ", vec![s("")]))]), &ss.0),
        empty_key("pk")
    );
    for op in ["EQ", "LE", "LT", "GE", "GT", "BEGINS_WITH"] {
        assert_eq!(
            empty(&kc(vec![pk_s(), ("sk", cond(op, vec![s("")]))]), &ss.0),
            empty_key("sk"),
            "{op}"
        );
    }
    for bounds in [vec![s(""), s("a")], vec![s(""), s("")]] {
        assert_eq!(
            empty(&kc(vec![pk_s(), ("sk", cond("BETWEEN", bounds))]), &ss.0),
            empty_key("sk")
        );
    }
    // Both keys empty: the service names either one (measured 2026-11-12);
    // this names the first in key schema order.
    assert_eq!(
        empty(
            &kc(vec![
                ("pk", cond("EQ", vec![s("")])),
                ("sk", cond("EQ", vec![s("")]))
            ]),
            &ss.0
        ),
        empty_key("pk")
    );
    // Every HASH and RANGE attribute of a multi-attribute key schema
    // (measured 2026-11-12), and no attribute outside the key schema.
    let (multi, _) = schema(&[
        ("h1", KeyType::Hash, ScalarAttributeType::S),
        ("h2", KeyType::Hash, ScalarAttributeType::S),
        ("r1", KeyType::Range, ScalarAttributeType::S),
        ("r2", KeyType::Range, ScalarAttributeType::S),
    ]);
    let eq = |v: &str| cond("EQ", vec![s(v)]);
    for (name, conditions) in [
        ("h1", kc(vec![("h1", eq("")), ("h2", eq("b"))])),
        ("h2", kc(vec![("h1", eq("a")), ("h2", eq(""))])),
        (
            "r1",
            kc(vec![
                ("h1", eq("a")),
                ("h2", eq("b")),
                ("r1", cond("BEGINS_WITH", vec![s("")])),
            ]),
        ),
        (
            "r2",
            kc(vec![
                ("h1", eq("a")),
                ("h2", eq("b")),
                ("r1", eq("c")),
                ("r2", eq("")),
            ]),
        ),
    ] {
        assert_eq!(empty(&conditions, &multi), empty_key(name));
    }
    assert_eq!(empty(&kc(vec![pk_s(), ("foo", eq(""))]), &ss.0), None);
    // An empty B on a B key names the binary kind (measured 2026-11-12).
    let (bb, _) = schema(&[
        ("pk", KeyType::Hash, ScalarAttributeType::B),
        ("sk", KeyType::Range, ScalarAttributeType::B),
    ]);
    let empty_binary = |name: &str| {
        Some(format!(
            "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: {name}"
        ))
    };
    let pk_b = || ("pk", cond("EQ", vec![AttributeValue::B(vec![1])]));
    assert_eq!(
        empty(
            &kc(vec![(
                "pk",
                cond("EQ", vec![AttributeValue::B(Vec::new())])
            )]),
            &bb
        ),
        empty_binary("pk")
    );
    for sk in [
        cond("EQ", vec![AttributeValue::B(Vec::new())]),
        cond("BEGINS_WITH", vec![AttributeValue::B(Vec::new())]),
        cond(
            "BETWEEN",
            vec![AttributeValue::B(Vec::new()), AttributeValue::B(vec![5])],
        ),
    ] {
        assert_eq!(
            empty(&kc(vec![pk_b(), ("sk", sk)]), &bb),
            empty_binary("sk")
        );
    }
    // An inverted range is a request-time error, reported before the empty
    // bound is looked at.
    let err = validate_key_conditions(&kc(vec![
        pk_s(),
        ("sk", cond("BETWEEN", vec![s("a"), s("")])),
    ]))
    .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if Some(m.clone()) == refusal(
            "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
        )),
        "{err:?}"
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
