// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::ast::{CompareOp, Expr, PathElement};
use crate::expression::tokenizer::tokenize;

fn parse(input: &str) -> Result<Expr, DynamoDbError> {
    let tokens = tokenize(input)?;
    parse_condition(&tokens)
}

#[test]
fn parse_simple_comparison() {
    let expr = parse("price > :min").unwrap();
    assert!(matches!(
        expr,
        Expr::Compare {
            op: CompareOp::Gt,
            ..
        }
    ));
}

#[test]
fn parse_and_expression() {
    let expr = parse("a = :v1 AND b = :v2").unwrap();
    assert!(matches!(expr, Expr::And(_, _)));
}

#[test]
fn parse_or_expression() {
    let expr = parse("a = :v1 OR b = :v2").unwrap();
    assert!(matches!(expr, Expr::Or(_, _)));
}

#[test]
fn parse_not_expression() {
    let expr = parse("NOT a = :v").unwrap();
    assert!(matches!(expr, Expr::Not(_)));
}

#[test]
fn parse_parenthesized_expression() {
    let expr = parse("(a = :v1 OR b = :v2) AND c = :v3").unwrap();
    assert!(matches!(expr, Expr::And(_, _)));
}

#[test]
fn parse_between_expression() {
    let expr = parse("price BETWEEN :lo AND :hi").unwrap();
    assert!(matches!(expr, Expr::Between { .. }));
}

#[test]
fn parse_in_expression() {
    let expr = parse("status IN (:a, :b, :c)").unwrap();
    if let Expr::In { list, .. } = &expr {
        assert_eq!(list.len(), 3);
    } else {
        panic!("Expected In expression");
    }
}

#[test]
fn parse_attribute_exists() {
    let expr = parse("attribute_exists(myattr)").unwrap();
    assert!(matches!(expr, Expr::Function { .. }));
}

#[test]
fn parse_attribute_not_exists() {
    let expr = parse("attribute_not_exists(#pk)").unwrap();
    if let Expr::Function { name, args } = &expr {
        assert_eq!(name, "attribute_not_exists");
        assert_eq!(args.len(), 1);
    } else {
        panic!("Expected Function expression");
    }
}

#[test]
fn parse_begins_with() {
    let expr = parse("begins_with(#n, :prefix)").unwrap();
    if let Expr::Function { name, args } = &expr {
        assert_eq!(name, "begins_with");
        assert_eq!(args.len(), 2);
    } else {
        panic!("Expected Function expression");
    }
}

#[test]
fn parse_contains() {
    let expr = parse("contains(tags, :tag)").unwrap();
    if let Expr::Function { name, .. } = &expr {
        assert_eq!(name, "contains");
    } else {
        panic!("Expected Function expression");
    }
}

#[test]
fn parse_size_in_comparison() {
    let expr = parse("size(mylist) > :limit").unwrap();
    if let Expr::Compare { left, op, .. } = &expr {
        assert!(matches!(op, CompareOp::Gt));
        assert!(matches!(left.as_ref(), Expr::Function { name, .. } if name == "size"));
    } else {
        panic!("Expected Compare expression with size function");
    }
}

#[test]
fn parse_nested_path() {
    let expr = parse("address.city = :city").unwrap();
    if let Expr::Compare { left, .. } = &expr {
        if let Expr::Path(elements) = left.as_ref() {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], PathElement::Attribute("address".into()));
            assert_eq!(elements[1], PathElement::Attribute("city".into()));
        } else {
            panic!("Expected Path expression");
        }
    } else {
        panic!("Expected Compare expression");
    }
}

#[test]
fn parse_list_index_path() {
    let expr = parse("items[0] = :v").unwrap();
    if let Expr::Compare { left, .. } = &expr {
        if let Expr::Path(elements) = left.as_ref() {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[1], PathElement::Index(0));
        } else {
            panic!("Expected Path expression");
        }
    } else {
        panic!("Expected Compare expression");
    }
}

#[test]
fn parse_all_comparators() {
    for (input, expected_op) in [
        ("a = :v", CompareOp::Eq),
        ("a <> :v", CompareOp::Ne),
        ("a < :v", CompareOp::Lt),
        ("a <= :v", CompareOp::Le),
        ("a > :v", CompareOp::Gt),
        ("a >= :v", CompareOp::Ge),
    ] {
        let expr = parse(input).unwrap();
        if let Expr::Compare { op, .. } = &expr {
            assert_eq!(*op, expected_op, "Failed for input: {input}");
        } else {
            panic!("Expected Compare for input: {input}");
        }
    }
}

#[test]
fn parse_empty_tokens_rejected() {
    let err = parse_condition(&[]).unwrap_err();
    assert!(matches!(err, DynamoDbError::ValidationException(_)));
}

#[test]
fn parse_trailing_tokens_rejected() {
    // An operand after a complete condition: measured 2026-10-25 (d11,
    // `#v = :v #v = :v` answers token "#v", near ":v #v =").
    let tokens = tokenize("a = :v b").unwrap();
    let err = parse_condition(&tokens).unwrap_err();
    assert!(matches!(err, DynamoDbError::ValidationException(msg)
        if msg == "Invalid ConditionExpression: Syntax error; token: \"b\", near: \":v b\""));
}

#[test]
fn parse_depth_limit_exceeded() {
    // Build deeply nested NOT NOT NOT ... a = :v
    let deep = "NOT ".repeat(200) + "a = :v";
    let tokens = tokenize(&deep).unwrap();
    let err = parse_condition_with_depth_limit(&tokens, 150).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("depth exceeded"))
    );
}

#[test]
fn redundant_parens_rejected() {
    let cases = [
        "((a = :v))",
        "(((a = :v)))",
        "((a = :v AND b = :v2))",
        "((NOT (a = :v)))",
        "(a = :v) AND ((b = :v2))",
    ];
    for expr in cases {
        let tokens = tokenize(expr).unwrap();
        let err = parse_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg.contains("redundant parentheses")),
            "Expected redundant parens error for: {expr}, got: {err:?}"
        );
    }
}

#[test]
fn valid_parens_accepted() {
    let cases = [
        "(a = :v)",
        "(a = :v) AND (b = :v2)",
        "((a = :v) AND (b = :v2))",
        "(NOT (a = :v))",
    ];
    for expr in cases {
        let tokens = tokenize(expr).unwrap();
        assert!(
            parse_condition(&tokens).is_ok(),
            "Expected valid parse for: {expr}"
        );
    }
}

#[test]
fn redundant_parens_use_canonical_message() {
    for expr in [
        "((a = :v))",
        "(((a = :v)))",
        "((a = :v AND b = :v2))",
        "((NOT (a = :v)))",
    ] {
        let tokens = tokenize(expr).unwrap();
        let err = parse_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid ConditionExpression: The expression has redundant parentheses;"),
            "expr {expr}: got {err:?}"
        );
    }
}

#[test]
fn a_token_other_than_a_parenthesis_after_in_answers_the_syntax_error() {
    // Measured 2026-11-19 (us-east-1), probe cases in the first column.
    for (case, src, token, near) in [
        ("x17", "#v IN IN (:v)", "IN", "IN IN ("),
        ("i01", "#v IN :v", ":v", "IN :v"),
        ("i02", "#v IN", "<EOF>", "IN"),
    ] {
        let err = parse_condition_from(&tokenize(src).unwrap(), src, 150).unwrap_err();
        let want = format!(
            "Invalid ConditionExpression: Syntax error; token: \"{token}\", near: \"{near}\""
        );
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "{case}: got {err:?}"
        );
    }
}

#[test]
fn the_classified_parse_tells_a_syntax_error_from_the_other_refusals() {
    // The syntax variant carries the service's wording; every other
    // refusal is the other variant with the parser's own text.
    for (src, token, near) in [
        ("#v = = :v", "=", "= = :v"),
        ("#v = :v AND", "<EOF>", "AND"),
        ("#v = :v #v = :v", "#v", ":v #v ="),
        ("= #v = :v", "=", "= #v"),
        ("#v IN :v", ":v", "IN :v"),
    ] {
        let fault = parse_condition_classified(&tokenize(src).unwrap(), src, 150).unwrap_err();
        let want = format!(
            "Invalid ConditionExpression: Syntax error; token: \"{token}\", near: \"{near}\""
        );
        assert!(
            matches!(&fault, ConditionFault::Syntax(DynamoDbError::ValidationException(msg))
                if *msg == want),
            "{src}: got {fault:?}"
        );
    }
    for (src, text) in [
        ("((#v = :v))", "The expression has redundant parentheses;"),
        ("#v", "expected comparison operator or function call"),
        ("(#v = :v", "expected ')'"),
        ("#v = :v, #v = :v", "unexpected token at position 3"),
    ] {
        let fault = parse_condition_classified(&tokenize(src).unwrap(), src, 150).unwrap_err();
        let want = format!("Invalid ConditionExpression: {text}");
        assert!(
            matches!(&fault, ConditionFault::Other(DynamoDbError::ValidationException(msg))
                if *msg == want),
            "{src}: got {fault:?}"
        );
    }
    // Depth is the other variant too, with the depth limit of the call.
    let src = "NOT NOT #v = :v";
    let fault = parse_condition_classified(&tokenize(src).unwrap(), src, 1).unwrap_err();
    assert!(
        matches!(&fault, ConditionFault::Other(DynamoDbError::ValidationException(msg))
            if msg == "Invalid ConditionExpression: expression nesting depth exceeded"),
        "got {fault:?}"
    );
}
