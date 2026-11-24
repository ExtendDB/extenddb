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
//!
//! A parenthesized condition, a `NOT` and a `size` call read as an operand
//! each nest one level, up to `max_depth`.

use super::ast::{CompareOp, Expr};
use super::kind::ExpressionKind;
use super::parser_common;
use super::syntax_error::{is_operator_or_keyword, syntax_error};
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// Why the condition parser refused a token stream. A caller that reads a
/// key condition with the condition grammar first keeps a `Syntax` refusal
/// and leaves any other refusal to the key condition parser, which reports
/// it in its own words.
#[derive(Debug)]
pub enum ConditionFault {
    /// A repeated or misplaced operator, keyword or path, worded as the
    /// service words it (`Syntax error; token: "=", near: "= = :v"`).
    Syntax(DynamoDbError),
    /// Any other refusal: redundant parentheses, the nesting depth, a bare
    /// operand, a token the grammar has no rule for.
    Other(DynamoDbError),
}

impl From<ConditionFault> for DynamoDbError {
    fn from(fault: ConditionFault) -> Self {
        match fault {
            ConditionFault::Syntax(err) | ConditionFault::Other(err) => err,
        }
    }
}

impl From<DynamoDbError> for ConditionFault {
    fn from(err: DynamoDbError) -> Self {
        Self::Other(err)
    }
}

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
    parse_condition_from(tokens, "", max_depth)
}

/// Parse a condition expression whose tokens came from `source`, so a syntax
/// error can quote the source window around the offending token as Amazon
/// `DynamoDB` does (`Syntax error; token: "=", near: "= = :v"`). An empty
/// `source` joins the token texts instead.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or depth exceeded.
pub fn parse_condition_from(
    tokens: &[Token],
    source: &str,
    max_depth: usize,
) -> Result<Expr, DynamoDbError> {
    parse_condition_classified(tokens, source, max_depth).map_err(DynamoDbError::from)
}

/// Parse as [`parse_condition_from`] does, and tell a syntax error in the
/// service's wording apart from the parser's other refusals.
///
/// # Errors
///
/// Returns [`ConditionFault::Syntax`] for a repeated or misplaced operator,
/// keyword or path, and [`ConditionFault::Other`] for every other refusal.
pub fn parse_condition_classified(
    tokens: &[Token],
    source: &str,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    parser_common::check_redundant_parens(tokens)
        .map_err(|body| validation_err(&format!("Invalid ConditionExpression: {body}")))?;
    let mut pos = 0;
    let mut depth = 0;
    let expr = parse_or(tokens, source, &mut pos, &mut depth, max_depth)?;
    if pos < tokens.len() {
        // A token after a complete condition. Measured 2026-10-25 for an
        // operator (`#v = :v =`) and a path (`#v = :v #v = :v`); other tokens
        // keep the previous text.
        if is_operator_or_keyword(&tokens[pos])
            || matches!(tokens[pos], Token::Ident(_) | Token::NameRef(_))
        {
            return Err(syntax_fault(source, tokens, pos));
        }
        return Err(validation_err(&format!(
            "Invalid ConditionExpression: unexpected token at position {pos}"
        )));
    }
    Ok(expr)
}

fn parse_or(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    let mut left = parse_and(tokens, src, pos, depth, max_depth)?;
    while *pos < tokens.len() && tokens[*pos] == Token::Or {
        *pos += 1;
        let right = parse_and(tokens, src, pos, depth, max_depth)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    let mut left = parse_not(tokens, src, pos, depth, max_depth)?;
    while *pos < tokens.len() && tokens[*pos] == Token::And {
        *pos += 1;
        let right = parse_not(tokens, src, pos, depth, max_depth)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    if *pos < tokens.len() && tokens[*pos] == Token::Not {
        *pos += 1;
        *depth += 1;
        if *depth > max_depth {
            return Err(validation_err(
                "Invalid ConditionExpression: expression nesting depth exceeded",
            ));
        }
        let expr = parse_not(tokens, src, pos, depth, max_depth)?;
        *depth -= 1;
        return Ok(Expr::Not(Box::new(expr)));
    }
    parse_primary(tokens, src, pos, depth, max_depth)
}

fn parse_primary(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    if *pos >= tokens.len() {
        if tokens.is_empty() {
            return Err(validation_err(
                "Invalid ConditionExpression: unexpected end of expression",
            ));
        }
        // Input ends where a condition is required (`#v = :v AND`): measured
        // 2026-10-25 as token "<EOF>" with the last token as the window.
        return Err(syntax_fault(src, tokens, *pos));
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
        let expr = parse_or(tokens, src, pos, depth, max_depth)?;
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
            return parse_function_call(tokens, src, pos, depth, max_depth);
        }
    }

    // Operand — then check for comparator, BETWEEN, or IN
    let left = parse_operand(tokens, src, pos, depth, max_depth)?;

    if *pos < tokens.len() {
        if let Some(op) = try_comparator(&tokens[*pos]) {
            *pos += 1;
            let right = parse_operand(tokens, src, pos, depth, max_depth)?;
            return Ok(Expr::Compare {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }

        // BETWEEN operand AND operand
        if tokens[*pos] == Token::Between {
            *pos += 1;
            let low = parse_operand(tokens, src, pos, depth, max_depth)?;
            parser_common::expect_token(tokens, pos, &Token::And, "AND", "ConditionExpression")?;
            let high = parse_operand(tokens, src, pos, depth, max_depth)?;
            return Ok(Expr::Between {
                operand: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
            });
        }

        // IN (operand, operand, ...)
        if tokens[*pos] == Token::In {
            *pos += 1;
            // Any token but `(` after IN, and the end of input, answer the
            // syntax error (measured 2026-11-19: `#v IN IN (:v)` answers token
            // "IN", near "IN IN ("; `#v IN :v` token ":v"; `#v IN` token
            // "<EOF>", near "IN").
            if tokens.get(*pos) != Some(&Token::LParen) {
                return Err(syntax_fault(src, tokens, *pos));
            }
            *pos += 1;
            let mut list = vec![parse_operand(tokens, src, pos, depth, max_depth)?];
            while *pos < tokens.len() && tokens[*pos] == Token::Comma {
                *pos += 1;
                list.push(parse_operand(tokens, src, pos, depth, max_depth)?);
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

fn parse_operand(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
    if *pos >= tokens.len() {
        // Input ends where an operand is required (`#v = :v AND NOT`).
        return Err(syntax_fault(src, tokens, *pos));
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
                *depth += 1;
                if *depth > max_depth {
                    return Err(validation_err(
                        "Invalid ConditionExpression: expression nesting depth exceeded",
                    ));
                }
                let call = parse_function_call(tokens, src, pos, depth, max_depth)?;
                *depth -= 1;
                return Ok(call);
            }
            let elements = parser_common::parse_path(tokens, pos)?;
            Ok(Expr::Path(elements))
        }
        Token::NameRef(_) => {
            let elements = parser_common::parse_path(tokens, pos)?;
            Ok(Expr::Path(elements))
        }
        // An operator or keyword where an operand is required (`#v = = :v`,
        // `#v = :v AND AND #v = :v`, `AND #v = :v`): measured 2026-10-25.
        token if is_operator_or_keyword(token) => Err(syntax_fault(src, tokens, *pos)),
        _ => Err(validation_err(
            "Invalid ConditionExpression: expected attribute path or value placeholder",
        )),
    }
}

fn parse_function_call(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    depth: &mut usize,
    max_depth: usize,
) -> Result<Expr, ConditionFault> {
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
        args.push(parse_operand(tokens, src, pos, depth, max_depth)?);
        while *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
            args.push(parse_operand(tokens, src, pos, depth, max_depth)?);
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

fn validation_err(msg: &str) -> ConditionFault {
    ConditionFault::Other(DynamoDbError::ValidationException(msg.to_owned()))
}

/// The service's syntax error at `pos`, as a [`ConditionFault::Syntax`].
fn syntax_fault(src: &str, tokens: &[Token], pos: usize) -> ConditionFault {
    ConditionFault::Syntax(syntax_error(ExpressionKind::Condition, src, tokens, pos))
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
