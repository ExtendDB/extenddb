// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::tokenize;
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
    kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

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
    kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

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
    kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

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
    kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

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
