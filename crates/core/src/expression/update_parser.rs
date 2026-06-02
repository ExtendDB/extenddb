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

use super::ast::{ArithOp, Expr, UpdateAction};
use super::parser_common;
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// Parse an update expression token stream into a list of actions.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_update(tokens: &[Token]) -> Result<Vec<UpdateAction>, DynamoDbError> {
    parser_common::check_redundant_parens(tokens).map_err(|msg| {
        DynamoDbError::ValidationException(format!("Invalid UpdateExpression: {msg}"))
    })?;

    let mut pos = 0;
    let mut actions = Vec::new();

    while pos < tokens.len() {
        match &tokens[pos] {
            Token::Set => {
                pos += 1;
                parse_set_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Remove => {
                pos += 1;
                parse_remove_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Add => {
                pos += 1;
                parse_add_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Delete => {
                pos += 1;
                parse_delete_actions(tokens, &mut pos, &mut actions)?;
            }
            _ => {
                return Err(validation_err(
                    "Invalid UpdateExpression: expected SET, REMOVE, ADD, or DELETE",
                ));
            }
        }
    }

    if actions.is_empty() {
        return Err(validation_err(
            "Invalid UpdateExpression: expression must contain at least one action",
        ));
    }

    Ok(actions)
}

/// Parse an update expression with DynamoDB-compatible syntax error messages.
pub fn parse_update_from(
    tokens: &[Token],
    source: &str,
) -> Result<Vec<UpdateAction>, DynamoDbError> {
    parser_common::check_redundant_parens(tokens).map_err(|msg| {
        DynamoDbError::ValidationException(format!("Invalid UpdateExpression: {msg}"))
    })?;

    let mut pos = 0;
    let mut actions = Vec::new();

    while pos < tokens.len() {
        match &tokens[pos] {
            Token::Set => {
                pos += 1;
                parse_set_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Remove => {
                pos += 1;
                parse_remove_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Add => {
                pos += 1;
                parse_add_actions(tokens, &mut pos, &mut actions)?;
            }
            Token::Delete => {
                pos += 1;
                parse_delete_actions(tokens, &mut pos, &mut actions)?;
            }
            token => {
                let token_text = token_display_text(token);
                let near = build_near_context(source, &token_text);
                return Err(validation_err(&format!(
                    "Invalid UpdateExpression: Syntax error; token: \"{token_text}\", near: \"{near}\""
                )));
            }
        }
    }

    if actions.is_empty() {
        return Err(validation_err(
            "Invalid UpdateExpression: expression must contain at least one action",
        ));
    }

    Ok(actions)
}

fn token_display_text(token: &Token) -> String {
    match token {
        Token::Ident(s) => s.clone(),
        Token::NameRef(s) => format!("#{s}"),
        Token::Placeholder(s) => format!(":{s}"),
        Token::Eq => "=".to_owned(),
        Token::Ne => "<>".to_owned(),
        Token::Lt => "<".to_owned(),
        Token::Le => "<=".to_owned(),
        Token::Gt => ">".to_owned(),
        Token::Ge => ">=".to_owned(),
        Token::Plus => "+".to_owned(),
        Token::Minus => "-".to_owned(),
        Token::Comma => ",".to_owned(),
        Token::Dot => ".".to_owned(),
        Token::LBracket => "[".to_owned(),
        Token::RBracket => "]".to_owned(),
        Token::LParen => "(".to_owned(),
        Token::RParen => ")".to_owned(),
        Token::And => "AND".to_owned(),
        Token::Or => "OR".to_owned(),
        Token::Not => "NOT".to_owned(),
        Token::Between => "BETWEEN".to_owned(),
        Token::In => "IN".to_owned(),
        Token::Set => "SET".to_owned(),
        Token::Remove => "REMOVE".to_owned(),
        Token::Add => "ADD".to_owned(),
        Token::Delete => "DELETE".to_owned(),
    }
}

fn build_near_context(source: &str, token_text: &str) -> String {
    if source.is_empty() {
        return token_text.to_owned();
    }
    if let Some(start) = source.find(token_text) {
        let end = std::cmp::min(start + token_text.len() + 7, source.len());
        source[start..end].trim_end().to_owned()
    } else {
        token_text.to_owned()
    }
}

fn parse_set_actions(
    tokens: &[Token],
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parser_common::parse_path(tokens, pos)?;
        parser_common::expect_token(tokens, pos, &Token::Eq, "=", "UpdateExpression")?;
        let value = parse_value_expr(tokens, pos)?;
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
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parser_common::parse_path(tokens, pos)?;
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
fn parse_value_expr(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    let left = parse_set_operand(tokens, pos)?;

    if *pos < tokens.len() {
        let arith_op = match &tokens[*pos] {
            Token::Plus => Some(ArithOp::Add),
            Token::Minus => Some(ArithOp::Sub),
            _ => None,
        };
        if let Some(op) = arith_op {
            *pos += 1;
            let right = parse_set_operand(tokens, pos)?;
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
fn parse_set_operand(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err("Invalid UpdateExpression: expected operand"));
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
                parse_set_function_call(tokens, pos)
            } else {
                parser_common::parse_path(tokens, pos).map(Expr::Path)
            }
        }
        Token::NameRef(_) => parser_common::parse_path(tokens, pos).map(Expr::Path),
        Token::LParen => {
            *pos += 1; // consume '('
            let expr = parse_value_expr(tokens, pos)?;
            if *pos >= tokens.len() || tokens[*pos] != Token::RParen {
                return Err(validation_err(
                    "Invalid UpdateExpression: expected closing ')'",
                ));
            }
            *pos += 1; // consume ')'
            Ok(expr)
        }
        _ => Err(validation_err(
            "Invalid UpdateExpression: expected path, placeholder, or function",
        )),
    }
}

fn parse_set_function_call(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
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
        args.push(parse_set_operand(tokens, pos)?);
        while *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
            args.push(parse_set_operand(tokens, pos)?);
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
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parser_common::parse_path(tokens, pos)?;
        let value = parse_add_delete_value(tokens, pos)?;
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
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parser_common::parse_path(tokens, pos)?;
        let value = parse_add_delete_value(tokens, pos)?;
        actions.push(UpdateAction::Delete { path, value });
        if *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    Ok(())
}

/// Parse a value for ADD/DELETE — only placeholders are valid.
fn parse_add_delete_value(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid UpdateExpression: expected value for ADD/DELETE action",
        ));
    }
    match &tokens[*pos] {
        Token::Placeholder(name) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
        _ => Err(validation_err(
            "Invalid UpdateExpression: ADD/DELETE value must be a placeholder",
        )),
    }
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::ast::{ArithOp, Expr, PathElement, UpdateAction};
    use crate::expression::tokenizer::tokenize;

    fn parse(input: &str) -> Result<Vec<UpdateAction>, DynamoDbError> {
        let tokens = tokenize(input)?;
        parse_update(&tokens)
    }

    #[test]
    fn parse_simple_set() {
        let actions = parse("SET price = :p").unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], UpdateAction::Set { path, .. } if path.len() == 1));
    }

    #[test]
    fn parse_multiple_set_actions() {
        let actions = parse("SET a = :v1, b = :v2, c = :v3").unwrap();
        assert_eq!(actions.len(), 3);
        assert!(
            actions
                .iter()
                .all(|a| matches!(a, UpdateAction::Set { .. }))
        );
    }

    #[test]
    fn parse_set_with_arithmetic_add() {
        let actions = parse("SET price = price + :inc").unwrap();
        assert_eq!(actions.len(), 1);
        if let UpdateAction::Set { value, .. } = &actions[0] {
            assert!(matches!(
                value,
                Expr::Arithmetic {
                    op: ArithOp::Add,
                    ..
                }
            ));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_set_with_arithmetic_sub() {
        let actions = parse("SET stock = stock - :dec").unwrap();
        if let UpdateAction::Set { value, .. } = &actions[0] {
            assert!(matches!(
                value,
                Expr::Arithmetic {
                    op: ArithOp::Sub,
                    ..
                }
            ));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_set_with_if_not_exists() {
        let actions = parse("SET price = if_not_exists(price, :default)").unwrap();
        if let UpdateAction::Set { value, .. } = &actions[0] {
            if let Expr::Function { name, args } = value {
                assert_eq!(name, "if_not_exists");
                assert_eq!(args.len(), 2);
            } else {
                panic!("Expected Function expression");
            }
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_set_with_list_append() {
        let actions = parse("SET tags = list_append(tags, :newtags)").unwrap();
        if let UpdateAction::Set { value, .. } = &actions[0] {
            assert!(matches!(value, Expr::Function { name, .. } if name == "list_append"));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_simple_remove() {
        let actions = parse("REMOVE oldattr").unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], UpdateAction::Remove { .. }));
    }

    #[test]
    fn parse_multiple_remove() {
        let actions = parse("REMOVE a, b, c").unwrap();
        assert_eq!(actions.len(), 3);
    }

    #[test]
    fn parse_set_and_remove_combined() {
        let actions = parse("SET a = :v REMOVE b").unwrap();
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], UpdateAction::Set { .. }));
        assert!(matches!(&actions[1], UpdateAction::Remove { .. }));
    }

    #[test]
    fn parse_add_action() {
        let actions = parse("ADD counter :inc").unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], UpdateAction::Add { .. }));
    }

    #[test]
    fn parse_delete_action() {
        let actions = parse("DELETE colors :remove_colors").unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], UpdateAction::Delete { .. }));
    }

    #[test]
    fn parse_all_four_action_types() {
        let actions = parse("SET a = :v ADD b :inc DELETE c :rm REMOVE d").unwrap();
        assert_eq!(actions.len(), 4);
    }

    #[test]
    fn parse_nested_path_in_set() {
        let actions = parse("SET address.city = :city").unwrap();
        if let UpdateAction::Set { path, .. } = &actions[0] {
            assert_eq!(path.len(), 2);
            assert_eq!(path[0], PathElement::Attribute("address".into()));
            assert_eq!(path[1], PathElement::Attribute("city".into()));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_list_index_in_set() {
        let actions = parse("SET items[0] = :v").unwrap();
        if let UpdateAction::Set { path, .. } = &actions[0] {
            assert_eq!(path.len(), 2);
            assert_eq!(path[1], PathElement::Index(0));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn parse_empty_expression_rejected() {
        let tokens = tokenize("").unwrap();
        // Empty tokens — parse_update should fail
        let err = parse_update(&tokens).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("at least one action"))
        );
    }

    #[test]
    fn parse_invalid_action_keyword_rejected() {
        let err = parse("price = :v").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("expected SET"))
        );
    }

    #[test]
    fn parse_name_ref_in_path() {
        let actions = parse("SET #n = :v").unwrap();
        if let UpdateAction::Set { path, .. } = &actions[0] {
            assert_eq!(path[0], PathElement::Attribute("#n".into()));
        } else {
            panic!("Expected Set action");
        }
    }

    #[test]
    fn redundant_parens_rejected() {
        let err = parse("SET c = ((c - :v))").unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg.contains("redundant parentheses")),
            "Expected redundant parens error, got: {err:?}"
        );
    }

    #[test]
    fn single_parens_accepted() {
        let actions = parse("SET c = (c - :v)").unwrap();
        assert!(matches!(&actions[0], UpdateAction::Set { .. }));
    }
}
