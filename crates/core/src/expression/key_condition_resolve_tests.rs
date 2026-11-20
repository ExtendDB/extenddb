// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::tokenize;
use crate::types::{KeySchemaElement, KeyType};
use std::collections::HashMap;

#[test]
fn resolve_pk_sk_swaps_when_reversed() {
    // "sk = :sk AND pk = :pk" — parser assigns first Eq as PK
    let tokens = tokenize("sk = :sk AND pk = :pk").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("sk".to_owned())]);

    kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare {
            path,
            op: CompareOp::Eq,
            ..
        }) => {
            assert_eq!(path, &vec![PathElement::Attribute("sk".to_owned())]);
        }
        other => panic!("expected Eq SK condition, got {other:?}"),
    }
}

#[test]
fn resolve_pk_sk_no_swap_when_correct() {
    let tokens = tokenize("pk = :pk AND sk = :sk").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);

    kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
}

#[test]
fn resolve_pk_sk_with_name_refs() {
    let tokens = tokenize("#s = :sk AND #p = :pk").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();

    let mut names = HashMap::new();
    names.insert("s".to_owned(), "sortkey".to_owned());
    names.insert("p".to_owned(), "partkey".to_owned());

    kc.resolve_pk_sk("partkey", &names).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("#p".to_owned())]);
}

#[test]
fn resolve_pk_sk_noop_for_non_eq_sk() {
    // "pk = :pk AND sk > :val" — SK is not Eq, no ambiguity
    let tokens = tokenize("pk = :pk AND sk > :val").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
}

#[test]
fn resolve_pk_sk_noop_for_single_clause() {
    let tokens = tokenize("pk = :pk").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
}

#[test]
fn resolve_multipart_reclassifies() {
    let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 = :v3").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let names = HashMap::new();
    kc.resolve_multipart(&["pk1", "pk2"], &["sk1"], &names)
        .unwrap();

    // After resolve_multipart, pk_path + extra_pk should have pk1 and pk2
    let pk_count = 1 + kc.extra_pk_conditions.len();
    assert_eq!(pk_count, 2);
    // sk_condition should be the sk1 equality
    assert!(kc.sk_condition.is_some());
}

#[test]
fn resolve_multipart_with_range() {
    let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 > :v3").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let names = HashMap::new();
    kc.resolve_multipart(&["pk1", "pk2"], &["sk1"], &names)
        .unwrap();

    assert_eq!(1 + kc.extra_pk_conditions.len(), 2);
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare {
            op: CompareOp::Gt, ..
        }) => {}
        other => panic!("expected Gt SK condition, got {other:?}"),
    }
}

#[test]
fn resolve_multipart_reorders_to_schema_order() {
    // Expression order: pk2, pk1, sk — reversed from key schema order [pk1, pk2]
    let tokens = tokenize("pk2 = :v2 AND pk1 = :v1 AND sk = :v3").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let names = HashMap::new();
    kc.resolve_multipart(&["pk1", "pk2"], &["sk"], &names)
        .unwrap();

    // After resolve_multipart, pk_path should be pk1 (first in schema)
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
    // extra_pk_conditions should have pk2
    assert_eq!(kc.extra_pk_conditions.len(), 1);
    assert_eq!(
        kc.extra_pk_conditions[0].0,
        vec![PathElement::Attribute("pk2".to_owned())]
    );
}

#[test]
fn resolve_multipart_multi_range() {
    // 2 HASH + 2 RANGE, all equality conditions
    let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 = :v3 AND sk2 = :v4").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let names = HashMap::new();
    kc.resolve_multipart(&["pk1", "pk2"], &["sk1", "sk2"], &names)
        .unwrap();

    // 2 PK conditions
    assert_eq!(1 + kc.extra_pk_conditions.len(), 2);
    // Primary SK condition is sk1
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare {
            path,
            op: CompareOp::Eq,
            ..
        }) => {
            assert_eq!(path, &vec![PathElement::Attribute("sk1".to_owned())]);
        }
        other => panic!("expected Eq SK condition on sk1, got {other:?}"),
    }
    // Extra SK condition is sk2
    assert_eq!(kc.extra_sk_conditions.len(), 1);
    assert_eq!(
        kc.extra_sk_conditions[0].0,
        vec![PathElement::Attribute("sk2".to_owned())]
    );
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

#[test]
fn resolve_key_schema_reads_a_multi_attribute_schema_by_name() {
    // Authorization reads the partition key value from `pk_path` after this
    // call, so it must name the first HASH attribute for every clause order
    // the engine serves.
    let two_hash = [
        key("g1", KeyType::Hash),
        key("g2", KeyType::Hash),
        key("gs", KeyType::Range),
    ];
    let two_range = [
        key("h", KeyType::Hash),
        key("a", KeyType::Range),
        key("b", KeyType::Range),
    ];
    for (schema, expr) in [
        (&two_hash[..], "g2 = :g AND g1 = :v AND gs = :x"),
        (&two_hash[..], "gs = :x AND g1 = :v AND g2 = :g"),
        (&two_hash[..], "g2 = :g AND g1 = :v AND gs > :lo"),
        (&two_range[..], "a = :a AND h = :v AND b = :c"),
        (&two_range[..], "b = :c AND h = :v AND a = :a"),
        (&two_range[..], "b > :lo AND a = :a AND h = :v"),
        (&two_range[..], "begins_with(b, :p) AND a = :a AND h = :v"),
    ] {
        let tokens = tokenize(expr).unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_key_schema(schema, &HashMap::new())
            .unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        let hash = &schema[0].attribute_name;
        assert_eq!(
            kc.pk_path,
            vec![PathElement::Attribute(hash.clone())],
            "{expr}"
        );
        assert_eq!(kc.pk_value, Expr::Placeholder("v".to_owned()), "{expr}");
    }
}

#[test]
fn resolve_key_schema_checks_values_before_resolve_pk_sk() {
    let schema = [key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    let tokens = tokenize("sk BETWEEN :lo AND :hi AND pk = :p").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let mut checked = Vec::new();
    kc.resolve_key_schema_with(&schema, &HashMap::new(), |_, value| {
        checked.push(format!("{value:?}"));
        Ok(())
    })
    .unwrap();
    assert_eq!(checked.len(), 3, "{checked:?}");
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);

    let tokens = tokenize("sk = :s AND pk = :p").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    let err = kc
        .resolve_key_schema_with(&schema, &HashMap::new(), |_, _| {
            Err(validation_err("checked"))
        })
        .unwrap_err();
    assert!(matches!(&err, DynamoDbError::ValidationException(msg) if msg == "checked"));
    // The failed check left the parser's assignment in place.
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("sk".to_owned())]);

    kc.resolve_key_schema(&schema, &HashMap::new()).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
}
