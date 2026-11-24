// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::tokenize;

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
fn a_fault_at_the_first_token_keeps_leading_whitespace() {
    // Measured 2026-10-25: the window starts at the start of the expression.
    assert_eq!(
        kce_msg("  = pk = :v"),
        "Invalid KeyConditionExpression: Syntax error; token: \"=\", near: \"  = pk\""
    );
}

#[test]
fn or_and_not_answer_the_invalid_operator_refusal() {
    // Measured 2026-11-19 (us-east-1): [x25], [x20], [x21].
    for src in [
        "pk = :v OR sk = :s",
        "NOT pk = :p",
        "pk = :p AND NOT sk = :s",
    ] {
        assert_eq!(
            kce_msg(src),
            "Invalid KeyConditionExpression: Invalid operator used in KeyConditionExpression",
            "{src}"
        );
    }
}

#[test]
fn other_key_condition_texts_are_unchanged() {
    assert_eq!(
        kce_msg("pk"),
        "Invalid KeyConditionExpression: expected operator after operand"
    );
    assert_eq!(
        kce_msg("pk = :v AND sk = sk2"),
        "Invalid KeyConditionExpression: key condition values must be expression attribute value placeholders"
    );
}
