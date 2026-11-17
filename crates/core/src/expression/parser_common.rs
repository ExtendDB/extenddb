// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared parsing utilities for condition and update expression parsers.

use super::ast::PathElement;
use super::tokenizer::{SYNTAX_ERROR, Token, syntax_error};
use super::update_parser::token_display_text;
use crate::error::DynamoDbError;

/// The start of Amazon DynamoDB's refusal of a list index out of range.
const RANGE_REFUSAL: &str = "List index is not within the allowable range; index: ";

/// Parse a document path from a token stream.
///
/// Handles `ident`, `#name`, `.attr`, and `[index]` syntax.
/// Used by both the condition expression parser and the update expression parser.
///
/// An index of two or more digits that starts with `0` is a syntax error (see
/// [`leading_zero_error`]). An index above 2147483647 parses here, and
/// [`check_list_indexes`] refuses it once the whole expression parses.
pub fn parse_path(tokens: &[Token], pos: &mut usize) -> Result<Vec<PathElement>, DynamoDbError> {
    let mut elements = Vec::new();

    match &tokens.get(*pos) {
        Some(Token::Ident(name)) => {
            elements.push(PathElement::Attribute(name.clone()));
            *pos += 1;
        }
        Some(Token::NameRef(name)) => {
            elements.push(PathElement::Attribute(format!("#{name}")));
            *pos += 1;
        }
        _ => {
            return Err(validation_err("expected attribute name"));
        }
    }

    while *pos < tokens.len() {
        if tokens[*pos] == Token::Dot {
            *pos += 1;
            match &tokens.get(*pos) {
                Some(Token::Ident(name)) => {
                    elements.push(PathElement::Attribute(name.clone()));
                    *pos += 1;
                }
                Some(Token::NameRef(name)) => {
                    elements.push(PathElement::Attribute(format!("#{name}")));
                    *pos += 1;
                }
                _ => {
                    return Err(validation_err("expected attribute name after '.'"));
                }
            }
        } else if tokens[*pos] == Token::LBracket {
            *pos += 1;
            match tokens.get(*pos) {
                Some(Token::Ident(digits)) if digits.bytes().all(|b| b.is_ascii_digit()) => {
                    if digits.len() > 1 && digits.starts_with('0') {
                        return Err(leading_zero_error(digits, tokens.get(*pos + 1)));
                    }
                    elements.push(PathElement::Index(digits.parse().unwrap_or(usize::MAX)));
                    *pos += 1;
                }
                _ => return Err(validation_err("expected numeric index in brackets")),
            }
            expect_token(tokens, pos, &Token::RBracket, "]", "expression")?;
        } else {
            break;
        }
    }

    Ok(elements)
}

/// Refuse the first list index of an expression above 2147483647, the largest
/// one Amazon DynamoDB takes, with the index as the request wrote it. Run it
/// once the expression parses: DynamoDB answers a syntax error anywhere in the
/// expression first (measured 2026-11-17: `l[99999999999], l[01]` gets the
/// syntax error of `l[01]`).
pub fn check_list_indexes(tokens: &[Token]) -> Result<(), DynamoDbError> {
    let refused = tokens.windows(3).find_map(|window| match window {
        [Token::LBracket, Token::Ident(index), Token::RBracket]
            if index.parse::<i32>().is_err() =>
        {
            Some(index)
        }
        _ => None,
    });
    refused.map_or(Ok(()), |index| {
        Err(validation_err(&list_index_range_refusal(index)))
    })
}

pub(crate) fn list_index_range_refusal(index: &str) -> String {
    format!("{RANGE_REFUSAL}[{index}]")
}

/// Whether `body`, the text after the `Invalid ...Expression:` label of a
/// parse error, is a refusal of a list index: [`check_list_indexes`], or the
/// syntax error of a leading zero, the one syntax error `parse_path` gives.
pub fn is_list_index_refusal(body: &str) -> bool {
    body.starts_with(RANGE_REFUSAL) || body.starts_with(SYNTAX_ERROR)
}

/// The syntax error of a list index of two or more digits that starts with
/// `0`. Amazon DynamoDB reads an index as `0` or as a number that starts with
/// 1 to 9, so `l[01]` is the index `0` followed by the token `1` (measured
/// 2026-11-14 and 2026-11-17). The near text is the token before, the token
/// and the token after. `next` is the token after the digits. The tokens keep
/// no white space, so `l[01 ]` is near `01]` here and near `01 ]` on DynamoDB.
fn leading_zero_error(digits: &str, next: Option<&Token>) -> DynamoDbError {
    fn first(text: &str) -> (&str, &str) {
        text.split_at(if text.starts_with('0') { 1 } else { text.len() })
    }
    let (token, rest) = first(&digits[1..]);
    let after = if rest.is_empty() {
        next.map(token_display_text).unwrap_or_default()
    } else {
        first(rest).0.to_owned()
    };
    validation_err(&syntax_error(token, &format!("0{token}{after}")))
}

/// Expect a specific token at the current position.
pub fn expect_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
    label: &str,
    context: &str,
) -> Result<(), DynamoDbError> {
    if *pos >= tokens.len() || tokens[*pos] != *expected {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid {context}: expected '{label}'"
        )));
    }
    *pos += 1;
    Ok(())
}

/// Reject redundant parentheses: a parenthesised group whose
/// entire content is itself a single parenthesised group, such as `((x))`.
/// Returns the bare error body so each parser can prefix its own expression type.
pub fn check_redundant_parens(tokens: &[Token]) -> Result<(), String> {
    // Single-pass stack: one bool per unmatched '('.
    // true = the preceding token was also '(' (double-open).
    // On ')', if popped is true AND the next token is ')', the group is redundant.
    let mut stack: Vec<bool> = Vec::new();

    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => {
                stack.push(i > 0 && tokens[i - 1] == Token::LParen);
            }
            Token::RParen => {
                if let Some(true) = stack.pop()
                    && tokens.get(i + 1) == Some(&Token::RParen)
                {
                    return Err("The expression has redundant parentheses;".to_owned());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("Invalid expression: {msg}"))
}

#[cfg(test)]
#[path = "parser_common_tests.rs"]
mod tests;
