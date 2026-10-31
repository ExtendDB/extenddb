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
use super::kind::ExpressionKind;
use super::parser_common;
use super::syntax_error::{is_operator_or_keyword, syntax_error};
use super::tokenizer::Token;
use crate::error::DynamoDbError;

const KIND: ExpressionKind = ExpressionKind::Update;

/// Parse an update expression token stream into a list of actions.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_update(tokens: &[Token]) -> Result<Vec<UpdateAction>, DynamoDbError> {
    parse_update_from(tokens, "")
}

/// Parse an update expression whose tokens came from `source`, so a syntax
/// error can quote the source window around the offending token as Amazon
/// `DynamoDB` does. An empty `source` joins the token texts instead.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
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
                parse_set_actions(tokens, source, &mut pos, &mut actions)?;
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
        let value = parse_value_expr(tokens, src, pos)?;
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
fn parse_value_expr(tokens: &[Token], src: &str, pos: &mut usize) -> Result<Expr, DynamoDbError> {
    let left = parse_set_operand(tokens, src, pos)?;

    if *pos < tokens.len() {
        let arith_op = match &tokens[*pos] {
            Token::Plus => Some(ArithOp::Add),
            Token::Minus => Some(ArithOp::Sub),
            _ => None,
        };
        if let Some(op) = arith_op {
            *pos += 1;
            let right = parse_set_operand(tokens, src, pos)?;
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
fn parse_set_operand(tokens: &[Token], src: &str, pos: &mut usize) -> Result<Expr, DynamoDbError> {
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
                parse_set_function_call(tokens, src, pos)
            } else {
                parser_common::parse_path(tokens, pos).map(Expr::Path)
            }
        }
        Token::NameRef(_) => parser_common::parse_path(tokens, pos).map(Expr::Path),
        Token::LParen => {
            *pos += 1; // consume '('
            let expr = parse_value_expr(tokens, src, pos)?;
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
        args.push(parse_set_operand(tokens, src, pos)?);
        while *pos < tokens.len() && tokens[*pos] == Token::Comma {
            *pos += 1;
            args.push(parse_set_operand(tokens, src, pos)?);
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
    src: &str,
    pos: &mut usize,
    actions: &mut Vec<UpdateAction>,
) -> Result<(), DynamoDbError> {
    loop {
        let path = parse_action_path(tokens, src, pos)?;
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
        // A token where a section keyword is required is the service's syntax
        // error; without a source the window joins the token texts.
        let err = parse("price = :v").unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(msg)
            if msg == "Invalid UpdateExpression: Syntax error; token: \"price\", near: \"price =\""));
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

    #[test]
    fn unmatched_paren_rejected() {
        // Opening paren with no matching close must be a ValidationException,
        // not a panic or a silently-accepted expression.
        let err = parse("SET c = (c - :v").unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg.contains("closing")),
            "Expected unmatched-paren error, got: {err:?}"
        );
    }

    #[test]
    fn unmatched_close_paren_rejected() {
        // A stray closing paren with no matching open must also be rejected.
        let err = parse("SET c = c - :v)").unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(_)),
            "Expected validation error for stray ')', got: {err:?}"
        );
    }

    // Syntax error wording measured 2026-10-25 (us-east-1) on UpdateItem.

    fn upd_msg(src: &str) -> String {
        match parse_update_from(&tokenize(src).unwrap(), src) {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    fn syntax(token: &str, near: &str) -> String {
        format!("Invalid UpdateExpression: Syntax error; token: \"{token}\", near: \"{near}\"")
    }

    #[test]
    fn measured_update_syntax_errors() {
        assert_eq!(upd_msg("SET #v = = :v"), syntax("=", "= = :v")); // [d24]
        assert_eq!(upd_msg("SET #v = :v + + :v"), syntax("+", "+ + :v")); // [d25]
        assert_eq!(upd_msg("SET #v = :v,"), syntax("<EOF>", ",")); // [d26]
        assert_eq!(upd_msg("SET #v = :v + :v + :v"), syntax("+", ":v + :v")); // [b07]
        assert_eq!(upd_msg("SET #v = :v +"), syntax("<EOF>", "+")); // [u08]
        assert_eq!(upd_msg("SET #v = :v REMOVE"), syntax("<EOF>", "REMOVE")); // [u10]
        assert_eq!(upd_msg("SET = :v"), syntax("=", "SET = :v")); // [u11]
        assert_eq!(upd_msg("SET #v :v"), syntax(":v", "#v :v")); // [u12]
    }

    #[test]
    fn other_update_texts_are_unchanged() {
        assert_eq!(
            upd_msg("SET #v #w = :v"),
            "Invalid UpdateExpression: expected '='"
        );
        assert_eq!(
            upd_msg("SET #v = (:v"),
            "Invalid UpdateExpression: expected closing ')'"
        );
    }
}
