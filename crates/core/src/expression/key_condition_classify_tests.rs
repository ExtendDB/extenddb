// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::tokenize;
use std::collections::HashMap;

/// The index key schema the single-HASH cases use: HASH `h`, then RANGE
/// `a`, `b`, `c`, `d`.
const ONE_HASH: [&str; 1] = ["h"];
const FOUR_RANGE: [&str; 4] = ["a", "b", "c", "d"];

/// Parse `expr` and classify it against `h` + `a, b, c, d`.
fn classify_one_hash(expr: &str) -> Result<KeyCondition, DynamoDbError> {
    classify_with_names(expr, &HashMap::new())
}

fn classify_with_names(
    expr: &str,
    names: &HashMap<String, String>,
) -> Result<KeyCondition, DynamoDbError> {
    let tokens = tokenize(expr).unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_multipart(&ONE_HASH, &FOUR_RANGE, names)?;
    Ok(kc)
}

/// The attribute names of `extra_sk_conditions`, in order.
fn extra_sk_names(kc: &KeyCondition) -> Vec<String> {
    kc.extra_sk_conditions
        .iter()
        .map(|(path, _)| match path.first() {
            Some(PathElement::Attribute(name)) => name.clone(),
            other => panic!("unexpected path {other:?}"),
        })
        .collect()
}

/// The sort-key condition's attribute and a label for its operator.
fn sk_shape(kc: &KeyCondition) -> Option<(String, &'static str)> {
    let cond = kc.sk_condition.as_ref()?;
    let name = match cond.path().first() {
        Some(PathElement::Attribute(name)) => name.clone(),
        other => panic!("unexpected path {other:?}"),
    };
    let op = match cond {
        SortKeyCondition::Compare { op, .. } => match op {
            CompareOp::Eq => "=",
            CompareOp::Ne => "<>",
            CompareOp::Lt => "<",
            CompareOp::Le => "<=",
            CompareOp::Gt => ">",
            CompareOp::Ge => ">=",
        },
        SortKeyCondition::Between { .. } => "BETWEEN",
        SortKeyCondition::BeginsWith { .. } => "begins_with",
    };
    Some((name, op))
}

fn assert_range_gap(expr: &str, without_eq: &str, present: &str) {
    let err = classify_one_hash(expr).unwrap_err();
    let want = format!(
        "RANGE key attributes {without_eq} must have equality conditions specified in the \
         query because a condition is present on key attribute {present}"
    );
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
        "expr {expr}: got {err:?}, want {want}"
    );
}

#[test]
fn one_hash_multi_range_hash_only_condition_is_accepted() {
    let kc = classify_one_hash("h = :h").unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("h".to_owned())]);
    assert!(kc.extra_pk_conditions.is_empty());
    assert!(kc.sk_condition.is_none());
    assert!(kc.extra_sk_conditions.is_empty());
}

#[test]
fn one_hash_multi_range_equality_prefixes_are_accepted() {
    // Equalities on a leading run of the RANGE attributes, up to all four.
    for (expr, extra) in [
        ("h = :h AND a = :a", vec![]),
        ("h = :h AND a = :a AND b = :b", vec!["b"]),
        ("h = :h AND a = :a AND b = :b AND c = :c", vec!["b", "c"]),
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND d = :d",
            vec!["b", "c", "d"],
        ),
    ] {
        let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(
            kc.pk_path,
            vec![PathElement::Attribute("h".to_owned())],
            "{expr}"
        );
        assert!(kc.extra_pk_conditions.is_empty(), "{expr}");
        assert_eq!(sk_shape(&kc), Some(("a".to_owned(), "=")), "{expr}");
        assert_eq!(extra_sk_names(&kc), extra, "{expr}");
    }
}

#[test]
fn one_hash_multi_range_clause_order_is_free() {
    // The defect: the first clause was taken as the partition key, so
    // `b = :b AND h = :h AND a = :a` was refused as missing `h`.
    for expr in [
        "b = :b AND h = :h AND a = :a",
        "a = :a AND b = :b AND h = :h",
        "b = :b AND a = :a AND h = :h",
    ] {
        let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(
            kc.pk_path,
            vec![PathElement::Attribute("h".to_owned())],
            "{expr}"
        );
        assert_eq!(sk_shape(&kc), Some(("a".to_owned(), "=")), "{expr}");
        assert_eq!(extra_sk_names(&kc), vec!["b"], "{expr}");
    }
}

#[test]
fn one_hash_multi_range_operator_on_the_last_given_attribute_is_accepted() {
    for (expr, sk, extra) in [
        ("h = :h AND a > :a", ("a", ">"), vec![]),
        ("h = :h AND a = :a AND b < :b", ("b", "<"), vec!["a"]),
        (
            "h = :h AND a = :a AND b = :b AND c > :c",
            ("c", ">"),
            vec!["a", "b"],
        ),
        (
            "h = :h AND a = :a AND begins_with(b, :b)",
            ("b", "begins_with"),
            vec!["a"],
        ),
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND begins_with(d, :d)",
            ("d", "begins_with"),
            vec!["a", "b", "c"],
        ),
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND d BETWEEN :x AND :y",
            ("d", "BETWEEN"),
            vec!["a", "b", "c"],
        ),
        (
            "c > :c AND b = :b AND h = :h AND a = :a",
            ("c", ">"),
            vec!["a", "b"],
        ),
    ] {
        let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(
            kc.pk_path,
            vec![PathElement::Attribute("h".to_owned())],
            "{expr}"
        );
        assert_eq!(sk_shape(&kc), Some((sk.0.to_owned(), sk.1)), "{expr}");
        assert_eq!(extra_sk_names(&kc), extra, "{expr}");
    }
}

#[test]
fn one_hash_multi_range_gap_is_refused() {
    // A condition on a RANGE attribute needs an equality on every RANGE
    // attribute before it. The defect accepted `h = :h AND b = :b` and
    // returned no items.
    assert_range_gap("h = :h AND b = :b", "a", "b");
    assert_range_gap("h = :h AND a = :a AND c = :c", "b", "c");
    assert_range_gap("h = :h AND a = :a AND b = :b AND d > :d", "c", "d");
}

#[test]
fn one_hash_multi_range_operator_before_a_later_condition_is_refused() {
    assert_range_gap("h = :h AND a = :a AND b > :b AND c = :c", "b", "c");
    assert_range_gap("h = :h AND begins_with(a, :a) AND b = :b", "a", "b");
}

#[test]
fn one_hash_multi_range_missing_hash_is_refused() {
    for expr in [
        "a = :a AND b = :b AND c = :c",
        "a = :a AND b = :b",
        "a = :a",
    ] {
        let err = classify_one_hash(expr).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "HASH key attributes h must have equality conditions specified in the query"),
            "expr {expr}: got {err:?}"
        );
    }
}

/// Classify against HASH `h1`, `h2` and RANGE `r`.
fn classify_two_hash(expr: &str) -> Result<KeyCondition, DynamoDbError> {
    let tokens = tokenize(expr).unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_multipart(&["h1", "h2"], &["r"], &HashMap::new())?;
    Ok(kc)
}

#[test]
fn two_hash_missing_hash_names_every_missing_attribute() {
    for (expr, missing) in [
        ("h1 = :x", "h2"),
        ("h2 = :x", "h1"),
        ("h1 = :x AND r = :y", "h2"),
        ("r = :y", "h1, h2"),
        ("h1 = :x AND h2 > :y", "h2"),
    ] {
        let err = classify_two_hash(expr).unwrap_err();
        let want = format!(
            "HASH key attributes {missing} must have equality conditions specified in the query"
        );
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "expr {expr}: got {err:?}"
        );
    }
    assert!(classify_two_hash("h1 = :x AND h2 = :y").is_ok());
    assert!(classify_two_hash("h1 = :x AND h2 = :y AND r > :z").is_ok());
}

#[test]
fn a_second_condition_on_one_key_attribute_is_refused() {
    const WANT: &str = "Invalid KeyConditionExpression: KeyConditionExpressions must only \
                        contain one condition per key";
    for expr in [
        "h = :x AND h = :y",
        "h = :x AND h = :y AND a = :z",
        "h = :x AND a = :y AND a = :z",
        "h = :h AND a = :a AND a > :x",
        "h = :h AND a > :x AND a = :a",
        "h = :h AND a = :a AND b = :n AND b > :lo",
        "h = :h AND h > :x",
        "h = :h AND h = :y AND c = :c",
        "a = :a AND a = :y",
    ] {
        let err = classify_one_hash(expr).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == WANT),
            "expr {expr}: got {err:?}"
        );
    }
    for expr in [
        "h1 = :x AND h1 = :y AND h2 = :z",
        "h1 = :x AND h1 = :y",
        "h1 = :x AND h2 = :n AND r = :r AND r > :y",
        "h1 = :x AND r = :r AND r = :y",
    ] {
        let err = classify_two_hash(expr).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == WANT),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn a_condition_on_a_non_key_attribute_names_the_first_uncovered_range_attribute() {
    for (expr, missing) in [
        ("h = :h AND z = :z", "a"),
        ("h = :h AND z = :z AND y = :y", "a"),
        ("h = :h AND z > :z", "a"),
        ("h = :h AND a = :a AND z = :z", "b"),
        ("h = :h AND a = :a AND z > :z", "b"),
        ("h = :h AND a > :a AND z = :z", "b"),
        ("h = :h AND a = :a AND b = :b AND z > :z", "c"),
        ("h = :h AND a = :a AND b = :b AND c = :c AND z = :z", "d"),
    ] {
        let err = classify_one_hash(expr).unwrap_err();
        let want = format!("Query condition missed key schema element: {missing}");
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "expr {expr}: got {err:?}"
        );
    }
    let err = classify_two_hash("h1 = :x AND h2 = :y AND z = :z").unwrap_err();
    assert!(matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "Query condition missed key schema element: r"));
    // A gap is reported before the non-key attribute.
    let err = classify_one_hash("h = :h AND b = :b AND z = :z").unwrap_err();
    assert!(matches!(&err, DynamoDbError::ValidationException(msg)
        if msg.starts_with("RANGE key attributes a must have equality conditions")));
    // A missing HASH attribute is reported before the non-key attribute.
    let err = classify_two_hash("h1 = :x AND z = :z").unwrap_err();
    assert!(matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "HASH key attributes h2 must have equality conditions specified in the query"));
}

#[test]
fn more_conditions_than_key_attributes_are_refused_by_count() {
    for (expr, n) in [
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z = :z",
            6,
        ),
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z > :z",
            6,
        ),
        (
            "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND PK = :p",
            6,
        ),
    ] {
        let err = classify_one_hash(expr).unwrap_err();
        let want = format!(
            "The number of query conditions ({n}) exceeds the number of key attributes \
             defined in the schema (5)"
        );
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "expr {expr}: got {err:?}"
        );
    }
    let err = classify_two_hash("h1 = :x AND h2 = :y AND r = :r AND z = :z").unwrap_err();
    assert!(matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "The number of query conditions (4) exceeds the number of key attributes defined in the schema (3)"));
}

#[test]
fn the_count_is_refused_before_a_missing_hash_attribute_or_a_range_gap() {
    // Each count case above has every key attribute, so they also pass
    // with the count check moved after the missing-HASH and RANGE-gap
    // checks. These lack the HASH attribute or leave a RANGE gap.
    let count = |n: usize, attrs: usize| {
        format!(
            "The number of query conditions ({n}) exceeds the number of key attributes \
             defined in the schema ({attrs})"
        )
    };
    for expr in [
        "a = :a AND b = :b AND c = :c AND d = :d AND z1 = :z AND z2 = :y",
        "h = :h AND b = :b AND c = :c AND d = :d AND z1 = :z AND z2 = :y",
    ] {
        let err = classify_one_hash(expr).unwrap_err();
        let want = count(6, 5);
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "expr {expr}: got {err:?}"
        );
    }
    let expr = "a = :a AND b = :b AND c = :c AND z1 = :z AND z2 = :y";
    let err = classify_two_hash(expr).unwrap_err();
    let want = count(5, 3);
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
        "expr {expr}: got {err:?}"
    );
    // The same five clauses on HASH `h` and RANGE `a`, `b`, `c`, `d` do not
    // exceed its five key attributes, so the missing HASH attribute is
    // reported.
    let err = classify_one_hash(expr).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "HASH key attributes h must have equality conditions specified in the query"),
        "expr {expr}: got {err:?}"
    );
}

#[test]
fn one_hash_multi_range_resolves_name_references() {
    let names: HashMap<String, String> = [("hh", "h"), ("aa", "a"), ("bb", "b")]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let kc = classify_with_names("#bb > :b AND #hh = :h AND #aa = :a", &names).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("#hh".to_owned())]);
    assert_eq!(sk_shape(&kc), Some(("#bb".to_owned(), ">")));
    assert_eq!(extra_sk_names(&kc), vec!["#aa"]);

    let err = classify_with_names("#hh = :h AND #bb = :b", &names).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
            if msg.starts_with("RANGE key attributes a must have")),
        "got {err:?}"
    );
}

#[test]
fn one_hash_one_range_schema_is_left_unchanged() {
    let tokens = tokenize("sk = :s AND pk = :p").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_multipart(&["pk"], &["sk"], &HashMap::new())
        .unwrap();
    // Still the parser's assignment; resolve_pk_sk handles this schema.
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("sk".to_owned())]);
}

#[test]
fn multi_hash_partition_key_need_not_come_first() {
    // With two HASH attributes the first clause used to be checked as the
    // first HASH attribute before reclassification, so this order was
    // refused as missing `pk1`.
    let tokens = tokenize("sk = :s AND pk1 = :a AND pk2 = :b").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_multipart(&["pk1", "pk2"], &["sk"], &HashMap::new())
        .unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
    assert_eq!(
        kc.extra_pk_conditions[0].0,
        vec![PathElement::Attribute("pk2".to_owned())]
    );
    assert_eq!(sk_shape(&kc), Some(("sk".to_owned(), "=")));
    assert!(kc.extra_sk_conditions.is_empty());
}

#[test]
fn multi_hash_multi_range_operator_after_equality_keeps_both() {
    // The non-equality on the second RANGE attribute is the sort-key
    // condition, and the equality on the first is carried with it.
    let tokens = tokenize("pk1 = :a AND pk2 = :b AND s1 = :x AND s2 > :y").unwrap();
    let mut kc = parse_key_condition(&tokens).unwrap();
    kc.resolve_multipart(&["pk1", "pk2"], &["s1", "s2"], &HashMap::new())
        .unwrap();
    assert_eq!(sk_shape(&kc), Some(("s2".to_owned(), ">")));
    assert_eq!(extra_sk_names(&kc), vec!["s1"]);
}
