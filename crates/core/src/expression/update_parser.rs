// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Update expression parser.
//!
//! Parses `UpdateExpression` strings into a list of `UpdateAction`s.
//! Phase 3 supports SET and REMOVE actions.
//!
//! Grammar:
//! ```text
//! update_expr → action_clause ( action_clause )*
//! action_clause → SET set_action ( ',' set_action )*
//!               | REMOVE remove_action ( ',' remove_action )*
//! set_action → path '=' value_expr
//! value_expr → operand ( ('+' | '-') operand )?
//! remove_action → path
//! ```
//!
//! A parenthesized value and a function call each nest one level. Past
//! `max_expression_depth` levels the expression is refused before the parser
//! recurses further, so the stack a parse takes stays bounded whatever the
//! size and token limits allow.

use super::ast::{ArithOp, Expr, UpdateAction};
use super::kind::ExpressionKind;
use super::parser_common;
use super::syntax_error::{is_operator_or_keyword, syntax_error};
use super::tokenizer::Token;
use crate::error::DynamoDbError;

const KIND: ExpressionKind = ExpressionKind::Update;

/// The nesting depth `parse_update` allows, the default of
/// `max_expression_depth`.
const DEFAULT_MAX_DEPTH: usize = 150;

/// The nesting levels a parse has entered, and the most it may enter.
struct Nesting {
    depth: usize,
    max_depth: usize,
}

impl Nesting {
    fn new(max_depth: usize) -> Self {
        Self {
            depth: 0,
            max_depth,
        }
    }

    /// Enter one level, refusing the expression past `max_depth`.
    fn enter(&mut self) -> Result<(), DynamoDbError> {
        self.depth += 1;
        if self.depth > self.max_depth {
            return Err(validation_err(
                "Invalid UpdateExpression: expression nesting depth exceeded",
            ));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }
}

/// Parse an update expression token stream into a list of actions, with the
/// default depth limit.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or depth exceeded.
pub fn parse_update(tokens: &[Token]) -> Result<Vec<UpdateAction>, DynamoDbError> {
    parse_update_from(tokens, "", DEFAULT_MAX_DEPTH)
}

/// Parse an update expression whose tokens came from `source`, nesting at
/// most `max_depth` levels of parentheses and function calls. A syntax error
/// quotes the source window around the offending token as Amazon `DynamoDB`
/// does. An empty `source` joins the token texts instead.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or depth exceeded.
pub fn parse_update_from(
    tokens: &[Token],
    source: &str,
    max_depth: usize,
) -> Result<Vec<UpdateAction>, DynamoDbError> {
    parser_common::check_redundant_parens(tokens).map_err(|msg| {
        DynamoDbError::ValidationException(format!("Invalid UpdateExpression: {msg}"))
    })?;

    let mut pos = 0;
    let mut actions = Vec::new();
    let mut nesting = Nesting::new(max_depth);

    while pos < tokens.len() {
        match &tokens[pos] {
            Token::Set => {
                pos += 1;
                parse_set_actions(tokens, source, &mut pos, &mut actions, &mut nesting)?;
            }
            Token::Remove => {
                pos += 1;
                parse_remove_actions(tokens, source, &mut pos, &mut actions)?;
            }
            Token::Add => {
                pos += 1;
                parse_add_actions(tokens, source, &mut pos, &mut actions)?;
            }
            Token::Delete => {
                pos += 1;
                parse_delete_actions(tokens, source, &mut pos, &mut actions)?;
            }
            // A token where a section keyword is required, such as the second
            // `+` of `SET #v = :v + :v + :v`, which answers token "+", near
            // ":v + :v" (measured 2026-10-25).
            _ => return Err(syntax_error(KIND, source, tokens, pos)),
        }
    }

    if actions.is_empty() {
        return Err(validation_err(
            "Invalid UpdateExpression: expression must contain at least one action",
        ));
    }

    Ok(actions)
}

/// Parse the path that starts an action. Input that ends here (`REMOVE`,
/// `SET #v = :v,`) or an operator here (`SET = :v`) is a syntax error in the
/// service's wording (measured 2026-10-25).
fn parse_action_path(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
) -> Result<Vec<super::ast::PathElement>, DynamoDbError> {
    match tokens.get(*pos) {
        None => Err(syntax_error(KIND, src, tokens, *pos)),
        Some(token) if is_operator_or_keyword(token) => Err(syntax_error(KIND, src, tokens, *pos)),
        _ => parser_common::parse_path(tokens, pos),
    }
}

fn parse_set_actions(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
    nesting: &mut Nesting,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parse_action_path(tokens, src, pos)?;
        // `SET #v :v` answers token ":v", near "#v :v" (measured 2026-10-25);
        // end of input and an operator or keyword here follow the same rule.
        // Any other token keeps the previous text.
        match tokens.get(*pos) {
            Some(Token::Eq) => {}
            None | Some(Token::Placeholder(_)) => {
                return Err(syntax_error(KIND, src, tokens, *pos));
            }
            Some(token) if is_operator_or_keyword(token) => {
                return Err(syntax_error(KIND, src, tokens, *pos));
            }
            Some(_) => {
                parser_common::expect_token(tokens, pos, &Token::Eq, "=", "UpdateExpression")?;
            }
        }
        *pos += 1;
        let value = parse_value_expr(tokens, src, pos, nesting)?;
        actions.push(UpdateAction::Set { path, value });

        if *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    Ok(())
}

fn parse_remove_actions(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parse_action_path(tokens, src, pos)?;
        actions.push(UpdateAction::Remove { path });

        if *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    Ok(())
}

/// Parse a SET value expression: `operand` or `operand +/- operand`.
fn parse_value_expr(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    nesting: &mut Nesting,
) -> Result<Expr, DynamoDbError> {
    let left = parse_set_operand(tokens, src, pos, nesting)?;

    if *pos < tokens.len() {
        let arith_op = match &tokens[*pos] {
            Token::Plus => Some(ArithOp::Add),
            Token::Minus => Some(ArithOp::Sub),
            _ => None,
        };
        if let Some(op) = arith_op {
            *pos += 1;
            let right = parse_set_operand(tokens, src, pos, nesting)?;
            return Ok(Expr::Arithmetic {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
    }

    Ok(left)
}

/// Parse a SET operand: path, placeholder, or function call (`if_not_exists`, `list_append`).
fn parse_set_operand(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    nesting: &mut Nesting,
) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        // `SET #v = :v +` answers token "<EOF>", near "+" (measured 2026-10-25).
        return Err(syntax_error(KIND, src, tokens, *pos));
    }

    match &tokens[*pos] {
        Token::Placeholder(name) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
        Token::Ident(name) => {
            let fn_lower = name.to_ascii_lowercase();
            if is_set_function(&fn_lower)
                && *pos + 1 < tokens.len()
                && tokens[*pos + 1] == Token::LParen
            {
                nesting.enter()?;
                let call = parse_set_function_call(tokens, src, pos, nesting)?;
                nesting.leave();
                Ok(call)
            } else {
                parser_common::parse_path(tokens, pos).map(Expr::Path)
            }
        }
        Token::NameRef(_) => parser_common::parse_path(tokens, pos).map(Expr::Path),
        Token::LParen => {
            *pos += 1; // consume '('
            nesting.enter()?;
            let expr = parse_value_expr(tokens, src, pos, nesting)?;
            nesting.leave();
            if *pos >= tokens.len() || tokens[*pos] != Token::RParen {
                return Err(validation_err(
                    "Invalid UpdateExpression: expected closing ')'",
                ));
            }
            *pos += 1; // consume ')'
            Ok(expr)
        }
        // An operator where an operand is required: `SET #v = = :v` answers
        // token "=", near "= = :v"; `SET #v = :v + + :v` answers token "+",
        // near "+ + :v" (measured 2026-10-25).
        token if is_operator_or_keyword(token) => Err(syntax_error(KIND, src, tokens, *pos)),
        _ => Err(validation_err(
            "Invalid UpdateExpression: expected path, placeholder, or function",
        )),
    }
}

fn parse_set_function_call(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    nesting: &mut Nesting,
) -> Result<Expr, DynamoDbError> {
    let name = match &tokens[*pos] {
        Token::Ident(n) => n.to_ascii_lowercase(),
        _ => {
            return Err(validation_err(
                "Invalid UpdateExpression: expected function name",
            ));
        }
    };
    *pos += 1;
    parser_common::expect_token(tokens, pos, &Token::LParen, "(", "UpdateExpression")?;

    let mut args = Vec::new();
    if *pos < tokens.len() && tokens[*pos] != Token::RParen {
        args.push(parse_set_operand(tokens, src, pos, nesting)?);
        while *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
            args.push(parse_set_operand(tokens, src, pos, nesting)?);
        }
    }

    parser_common::expect_token(tokens, pos, &Token::RParen, ")", "UpdateExpression")?;
    Ok(Expr::Function { name, args })
}

fn is_set_function(name: &str) -> bool {
    matches!(name, "if_not_exists" | "list_append")
}

/// Parse ADD actions: `ADD path value (, path value)*`
fn parse_add_actions(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parse_action_path(tokens, src, pos)?;
        let value = parse_add_delete_value(tokens, src, pos)?;
        actions.push(UpdateAction::Add { path, value });
        if *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    Ok(())
}

/// Parse DELETE actions: `DELETE path value (, path value)*`
fn parse_delete_actions(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parse_action_path(tokens, src, pos)?;
        let value = parse_add_delete_value(tokens, src, pos)?;
        actions.push(UpdateAction::Delete { path, value });
        if *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    Ok(())
}

/// Parse the value of an ADD or DELETE action, which must be a placeholder.
/// Any other token, and the end of input, answer the service's syntax error
/// (measured 2026-11-19: `ADD #v = :v` and `DELETE #v = :v` answer token
/// "=", near "#v = :v"; `ADD #v` answers token "<EOF>", near "#v"; `ADD #v
/// #w` answers token "#w", near "#v #w").
fn parse_add_delete_value(
    tokens: &[Token],
    src: &str,
    pos: &mut usize,
) -> Result<Expr, DynamoDbError> {
    match tokens.get(*pos) {
        Some(Token::Placeholder(name)) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
        _ => Err(syntax_error(KIND, src, tokens, *pos)),
    }
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.to_owned())
}

#[cfg(test)]
#[path = "update_parser_tests.rs"]
mod tests;
