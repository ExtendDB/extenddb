// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression parser.
//!
//! Parses a token stream into an expression AST. Supports comparisons,
//! AND/OR/NOT, `attribute_exists`, and `attribute_not_exists`.
//!
//! Grammar (Phase 3 subset):
//! ```text
//! condition     → or_expr
//! or_expr       → and_expr ( OR and_expr )*
//! and_expr      → not_expr ( AND not_expr )*
//! not_expr      → NOT not_expr | primary
//! primary       → comparison | function_call | '(' condition ')'
//! comparison    → operand comparator operand
//! operand       → path | placeholder
//! path          → (ident | name_ref) ( '.' (ident | name_ref) | '[' number ']' )*
//! function_call → function_name '(' operand ( ',' operand )* ')'
//! ```

use super::ast::{CompareOp, Expr};
use super::parser_common;
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// Parse a condition expression token stream into an AST.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_condition(tokens: &[Token]) -> Result<Expr, DynamoDbError> {
    parse_condition_with_depth_limit(tokens, 150)
}

/// Parse a condition expression with an explicit depth limit.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or depth exceeded.
pub fn parse_condition_with_depth_limit(
    tokens: &[Token],
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    parser_common::check_redundant_parens(tokens)
        .map_err(|body| validation_err(&format!("Invalid ConditionExpression: {body}")))?;
    let mut pos = 0;
    let mut depth = 0;
    let expr = parse_or(tokens, &mut pos, &mut depth, max_depth)?;
    if pos < tokens.len() {
        return Err(validation_err(&format!(
            "Invalid ConditionExpression: unexpected token at position {pos}"
        )));
    }
    Ok(expr)
}

fn parse_or(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    let mut left = parse_and(tokens, pos, depth, max_depth)?;
    while *pos < tokens.len() && tokens[*pos] == Token::Or {
        *pos += 1;
        let right = parse_and(tokens, pos, depth, max_depth)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    let mut left = parse_not(tokens, pos, depth, max_depth)?;
    while *pos < tokens.len() && tokens[*pos] == Token::And {
        *pos += 1;
        let right = parse_not(tokens, pos, depth, max_depth)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    if *pos < tokens.len() && tokens[*pos] == Token::Not {
        *pos += 1;
        *depth += 1;
        if *depth > max_depth {
            return Err(validation_err(
                "Invalid ConditionExpression: expression nesting depth exceeded",
            ));
        }
        let expr = parse_not(tokens, pos, depth, max_depth)?;
        *depth -= 1;
        return Ok(Expr::Not(Box::new(expr)));
    }
    parse_primary(tokens, pos, depth, max_depth)
}

fn parse_primary(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid ConditionExpression: unexpected end of expression",
        ));
    }

    // Parenthesized expression
    if tokens[*pos] == Token::LParen {
        *pos += 1;
        *depth += 1;
        if *depth > max_depth {
            return Err(validation_err(
                "Invalid ConditionExpression: expression nesting depth exceeded",
            ));
        }
        let expr = parse_or(tokens, pos, depth, max_depth)?;
        *depth -= 1;
        parser_common::expect_token(tokens, pos, &Token::RParen, ")", "ConditionExpression")?;
        return Ok(expr);
    }

    // Try function call: ident followed by '('
    if let Token::Ident(name) = &tokens[*pos] {
        let fn_name_lower = name.to_ascii_lowercase();
        if is_function_name(&fn_name_lower)
            && *pos + 1 < tokens.len()
            && tokens[*pos + 1] == Token::LParen
        {
            return parse_function_call(tokens, pos);
        }
    }

    // Operand — then check for comparator, BETWEEN, or IN
    let left = parse_operand(tokens, pos)?;

    if *pos < tokens.len() {
        if let Some(op) = try_comparator(&tokens[*pos]) {
            *pos += 1;
            let right = parse_operand(tokens, pos)?;
            return Ok(Expr::Compare {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }

        // BETWEEN operand AND operand
        if tokens[*pos] == Token::Between {
            *pos += 1;
            let low = parse_operand(tokens, pos)?;
            parser_common::expect_token(tokens, pos, &Token::And, "AND", "ConditionExpression")?;
            let high = parse_operand(tokens, pos)?;
            return Ok(Expr::Between {
                operand: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
            });
        }

        // IN (operand, operand, ...)
        if tokens[*pos] == Token::In {
            *pos += 1;
            parser_common::expect_token(tokens, pos, &Token::LParen, "(", "ConditionExpression")?;
            let mut list = vec![parse_operand(tokens, pos)?];
            while *pos < tokens.len() && tokens[*pos] == Token::Comma {
                *pos += 1;
                list.push(parse_operand(tokens, pos)?);
            }
            parser_common::expect_token(tokens, pos, &Token::RParen, ")", "ConditionExpression")?;
            return Ok(Expr::In {
                operand: Box::new(left),
                list,
            });
        }
    }

    // Bare operand is not a valid condition on its own
    Err(validation_err(
        "Invalid ConditionExpression: expected comparison operator or function call",
    ))
}

fn parse_operand(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid ConditionExpression: expected operand",
        ));
    }

    match &tokens[*pos] {
        Token::Placeholder(name) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
        Token::Ident(name) => {
            // size(path) is a function that returns a value, usable as an operand
            if name.eq_ignore_ascii_case("size")
                && *pos + 1 < tokens.len()
                && tokens[*pos + 1] == Token::LParen
            {
                return parse_function_call(tokens, pos);
            }
            let elements = parser_common::parse_path(tokens, pos)?;
            Ok(Expr::Path(elements))
        }
        Token::NameRef(_) => {
            let elements = parser_common::parse_path(tokens, pos)?;
            Ok(Expr::Path(elements))
        }
        _ => Err(validation_err(
            "Invalid ConditionExpression: expected attribute path or value placeholder",
        )),
    }
}

fn parse_function_call(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    let name = match &tokens[*pos] {
        Token::Ident(n) => n.to_ascii_lowercase(),
        _ => {
            return Err(validation_err(
                "Invalid ConditionExpression: expected function name",
            ));
        }
    };
    *pos += 1;

    parser_common::expect_token(tokens, pos, &Token::LParen, "(", "ConditionExpression")?;

    let mut args = Vec::new();
    if *pos < tokens.len() && tokens[*pos] != Token::RParen {
        args.push(parse_operand(tokens, pos)?);
        while *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
            args.push(parse_operand(tokens, pos)?);
        }
    }

    parser_common::expect_token(tokens, pos, &Token::RParen, ")", "ConditionExpression")?;

    Ok(Expr::Function { name, args })
}

fn try_comparator(token: &Token) -> Option<CompareOp> {
    match token {
        Token::Eq => Some(CompareOp::Eq),
        Token::Ne => Some(CompareOp::Ne),
        Token::Lt => Some(CompareOp::Lt),
        Token::Le => Some(CompareOp::Le),
        Token::Gt => Some(CompareOp::Gt),
        Token::Ge => Some(CompareOp::Ge),
        _ => None,
    }
}

fn is_function_name(name: &str) -> bool {
    matches!(
        name,
        "attribute_exists" | "attribute_not_exists" | "attribute_type" | "begins_with" | "contains"
    )
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.to_owned())
}

#[cfg(test)]
mod tests {
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
        let tokens = tokenize("a = :v b").unwrap();
        let err = parse_condition(&tokens).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("unexpected token"))
        );
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
}
