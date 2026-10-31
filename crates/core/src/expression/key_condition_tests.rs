// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::tokenize;
use crate::types::{KeySchemaElement, KeyType};
use std::collections::HashMap;

#[test]
fn key_condition_redundant_parens_rejected_with_canonical_message() {
    for expr in ["((pk = :v))", "((#pk = :pk)) AND (#sk = :sk)"] {
        let tokens = tokenize(expr).unwrap();
        let err = parse_key_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid KeyConditionExpression: The expression has redundant parentheses;"),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn key_condition_valid_parens_accepted() {
    for expr in [
        "(pk = :v)",
        "(#pk = :pk) AND (#sk = :sk)",
        "(#pk = :pk AND #sk = :sk)",
        "(#pk = :pk AND (#sk = :sk))",
    ] {
        let tokens = tokenize(expr).unwrap();
        assert!(
            parse_key_condition(&tokens).is_ok(),
            "expr {expr} should parse"
        );
    }
}

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
fn parse_three_clause_multipart() {
    // pk1 = :v1 AND pk2 = :v2 AND sk = :v3
    let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk = :v3").unwrap();
    let kc = parse_key_condition(&tokens).unwrap();
    // First clause is pk_path, one extra_pk, one sk
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
    assert_eq!(kc.extra_pk_conditions.len(), 1);
    assert!(kc.sk_condition.is_some());
}

#[test]
fn parse_four_clause_multipart() {
    let tokens = tokenize("a = :a AND b = :b AND c = :c AND d = :d").unwrap();
    let kc = parse_key_condition(&tokens).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("a".to_owned())]);
    assert_eq!(kc.extra_pk_conditions.len(), 2);
    assert!(kc.sk_condition.is_some());
}

#[test]
fn parse_multipart_with_range_condition() {
    // pk1 = :v1 AND pk2 = :v2 AND sk > :v3
    let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk > :v3").unwrap();
    let kc = parse_key_condition(&tokens).unwrap();
    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
    // pk2 is in extra_pk_conditions (will be reclassified by resolve_multipart)
    assert_eq!(kc.extra_pk_conditions.len(), 1);
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare {
            op: CompareOp::Gt, ..
        }) => {}
        other => panic!("expected Gt SK condition, got {other:?}"),
    }
}

#[test]
fn parse_reversed_sort_key_comparison() {
    let tokens = tokenize("pk = :pk AND :lo <= sk").unwrap();
    let kc = parse_key_condition(&tokens).unwrap();

    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare {
            path,
            op: CompareOp::Ge,
            value: Expr::Placeholder(name),
        }) => {
            assert_eq!(path, &vec![PathElement::Attribute("sk".to_owned())]);
            assert_eq!(name, "lo");
        }
        other => panic!("expected reversed Ge SK condition, got {other:?}"),
    }
}

#[test]
fn parse_reversed_partition_key_equality() {
    let tokens = tokenize(":pk = pk").unwrap();
    let kc = parse_key_condition(&tokens).unwrap();

    assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    assert_eq!(kc.pk_value, Expr::Placeholder("pk".to_owned()));
}

#[test]
fn reverse_comparator_maps_each_operator_and_is_involutive() {
    use CompareOp::{Eq, Ge, Gt, Le, Lt, Ne};

    let cases = [(Eq, Eq), (Ne, Ne), (Lt, Gt), (Le, Ge), (Gt, Lt), (Ge, Le)];

    for (op, reversed) in cases {
        assert_eq!(reverse_comparator(op), reversed);
        assert_eq!(reverse_comparator(reverse_comparator(op)), op);
    }
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

const NESTED_KEY_ERR: &str = "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes";

#[test]
fn nested_dot_path_on_key_rejected() {
    for expr in [
        "pk.foo = :v",
        "pk = :p AND sk.foo = :v",
        "begins_with(sk.foo, :v)",
        "foo.bar = :v",
    ] {
        let tokens = tokenize(expr).unwrap();
        let err = parse_key_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn nested_path_in_all_clause_shapes_rejected() {
    // Exercises every `raw_clause_path` arm (Compare/Between/BeginsWith) and a
    // 3-clause multi-part shape with a nested middle clause. Multi-part keys
    // are an ExtendDB extension real DynamoDB cannot express, hence a
    // parser-level test. The reject loop runs before classification, so a
    // nested clause in any position is rejected up front.
    for expr in [
        "pk.foo > :v",                            // single non-eq (Compare arm)
        "pk = :p AND sk.foo BETWEEN :a AND :b",   // Between arm
        "pk = :p AND begins_with(sk[0], :v)",     // BeginsWith arm, index
        "pk = :p AND :v < sk.foo",                // reversed value-first
        "pk1 = :v1 AND pk2.x = :v2 AND sk = :v3", // multi-part, nested middle
    ] {
        let tokens = tokenize(expr).unwrap();
        let err = parse_key_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn index_path_on_key_rejected() {
    for expr in ["pk[0] = :v", "pk = :p AND sk[0] = :v"] {
        let tokens = tokenize(expr).unwrap();
        let err = parse_key_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn nested_path_via_name_placeholder_rejected() {
    // The check is structural (path length), so it fires before #name
    // resolution.
    let tokens = tokenize("#d.foo = :v").unwrap();
    let err = parse_key_condition(&tokens).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
        "got {err:?}"
    );
}

#[test]
fn top_level_key_paths_accepted() {
    for expr in ["pk = :v", "pk = :p AND sk = :s", "#pk = :p AND #sk = :s"] {
        let tokens = tokenize(expr).unwrap();
        assert!(
            parse_key_condition(&tokens).is_ok(),
            "expr {expr} should parse"
        );
    }
}

/// One expected clause: attribute, operand placeholder, equality.
type ExpectedClause = (&'static str, &'static str, bool);

/// The attribute and operand of each clause, and whether it is an
/// equality, which is what authorization reads to find the partition value
/// of a key condition the engine rejects for its operator.
#[test]
fn clause_operands_name_every_single_operand_clause() {
    let cases: &[(&str, &[ExpectedClause])] = &[
        ("pk = :v", &[("pk", "v", true)]),
        (
            "pk = :v AND sk = :s",
            &[("pk", "v", true), ("sk", "s", true)],
        ),
        ("pk > :v", &[("pk", "v", false)]),
        (":v > pk", &[("pk", "v", false)]),
        ("begins_with(pk, :v)", &[("pk", "v", false)]),
        ("(pk >= :v)", &[("pk", "v", false)]),
        ("#p <= :v", &[("#p", "v", false)]),
        (
            "pk = :v AND sk < :s",
            &[("pk", "v", true), ("sk", "s", false)],
        ),
        (
            "pk > :v AND sk = :s",
            &[("pk", "v", false), ("sk", "s", true)],
        ),
        (
            "pk < :v AND sk > :s",
            &[("pk", "v", false), ("sk", "s", false)],
        ),
        (
            "pk = :u AND pk > :v",
            &[("pk", "u", true), ("pk", "v", false)],
        ),
        // BETWEEN names two operands, so it contributes none.
        ("pk BETWEEN :lo AND :hi", &[]),
        ("pk = :v AND sk BETWEEN :lo AND :hi", &[("pk", "v", true)]),
    ];
    for (expr, want) in cases {
        let tokens = tokenize(expr).unwrap();
        let got = key_condition_clause_operands(&tokens).unwrap();
        let got: Vec<(String, String, bool)> = got
            .into_iter()
            .map(|clause| {
                let name = match &clause.path[0] {
                    PathElement::Attribute(n) => n.clone(),
                    PathElement::Index(i) => format!("[{i}]"),
                };
                let value = match clause.value {
                    Expr::Placeholder(n) => n,
                    other => format!("{other:?}"),
                };
                (name, value, clause.equality)
            })
            .collect();
        let want: Vec<(String, String, bool)> = want
            .iter()
            .map(|(a, v, e)| ((*a).to_owned(), (*v).to_owned(), *e))
            .collect();
        assert_eq!(got, want, "expr {expr}");
    }
}

/// The same syntax errors and unsupported constructs `parse_key_condition`
/// rejects before classification, with the same error: both read the clauses
/// through one parser.
#[test]
fn clause_operands_reject_what_the_parser_rejects() {
    for expr in [
        "pk :: :v",
        "pk > :v OR sk = :s",
        "((pk = :v))",
        "pk.sub > :v",
        "pk <> :v",
        "pk >",
    ] {
        let Ok(tokens) = tokenize(expr) else {
            continue;
        };
        let Err(operands_err) = key_condition_clause_operands(&tokens) else {
            panic!("expr {expr} should be rejected");
        };
        let Err(parser_err) = parse_key_condition(&tokens) else {
            panic!("expr {expr} should be rejected by parse_key_condition");
        };
        assert_eq!(
            format!("{operands_err:?}"),
            format!("{parser_err:?}"),
            "expr {expr}"
        );
    }
}

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

// Syntax error wording measured 2026-10-25 (us-east-1) on Query.

fn kce_msg(src: &str) -> String {
    match parse_key_condition_from(&tokenize(src).unwrap(), src) {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn repeated_operator_quotes_the_source_window() {
    // [d22] `pk = = :v`
    assert_eq!(
        kce_msg("pk = = :v"),
        "Invalid KeyConditionExpression: Syntax error; token: \"=\", near: \"= = :v\""
    );
    assert_eq!(
        kce_msg("pk = :v AND sk = = :s"),
        "Invalid KeyConditionExpression: Syntax error; token: \"=\", near: \"= = :s\""
    );
}

#[test]
fn a_lower_case_keyword_is_shown_as_written() {
    // Measured 2026-10-25: the token and the window are the source text.
    assert_eq!(
        kce_msg("pk = :p and and sk = :s"),
        "Invalid KeyConditionExpression: Syntax error; token: \"and\", near: \"and and sk\""
    );
}

#[test]
fn trailing_keyword_is_eof_with_the_keyword_as_window() {
    // [d23] `pk = :v AND`
    assert_eq!(
        kce_msg("pk = :v AND"),
        "Invalid KeyConditionExpression: Syntax error; token: \"<EOF>\", near: \"AND\""
    );
    assert_eq!(
        kce_msg("pk ="),
        "Invalid KeyConditionExpression: Syntax error; token: \"<EOF>\", near: \"=\""
    );
}

#[test]
fn stripped_outer_parentheses_keep_the_source_window() {
    // The window is taken from the full token list, so the stripped
    // parenthesis does not shift the quoted text.
    assert_eq!(
        kce_msg("(pk = = :v)"),
        "Invalid KeyConditionExpression: Syntax error; token: \"=\", near: \"= = :v\""
    );
}

#[test]
fn other_key_condition_texts_are_unchanged() {
    assert_eq!(
        kce_msg("pk = :v OR sk = :s"),
        "Invalid KeyConditionExpression: only AND is supported between key conditions"
    );
    assert_eq!(
        kce_msg("pk"),
        "Invalid KeyConditionExpression: expected operator after operand"
    );
    assert_eq!(
        kce_msg("pk = :v AND sk = sk2"),
        "Invalid KeyConditionExpression: key condition values must be expression attribute value placeholders"
    );
}
