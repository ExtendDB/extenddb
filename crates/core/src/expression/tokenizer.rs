// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression tokenizer — converts expression strings into token streams.
//!
//! Handles the complete `DynamoDB` expression grammar including identifiers,
//! `#name` references, `:value` placeholders, operators, and keywords.

use crate::error::DynamoDbError;

/// Token produced by the tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Bare identifier (attribute name).
    Ident(String),
    /// `#name` reference.
    NameRef(String),
    /// `:value` placeholder.
    Placeholder(String),
    // Operators
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Comma,
    Dot,
    LBracket,
    RBracket,
    LParen,
    RParen,
    // Keywords (case-insensitive)
    And,
    Or,
    Not,
    Between,
    In,
    Set,
    Remove,
    Add,
    Delete,
}

/// Tokenize an expression string into a sequence of tokens.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for invalid characters or
/// unterminated tokens.
pub fn tokenize(input: &str) -> Result<Vec<Token>, DynamoDbError> {
    tokenize_with_limit(input, 4096)
}

/// Tokenize with an explicit token count limit.
///
/// # Errors
///
/// Returns `ValidationException` if the token count exceeds `max_tokens`.
pub fn tokenize_with_limit(input: &str, max_tokens: usize) -> Result<Vec<Token>, DynamoDbError> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    let mut prev_start: Option<usize> = None;
    while i < bytes.len() {
        let tok_start = i;
        let before = tokens.len();
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b'(' => {
                push_token(&mut tokens, Token::LParen, max_tokens)?;
                i += 1;
            }
            b')' => {
                push_token(&mut tokens, Token::RParen, max_tokens)?;
                i += 1;
            }
            b'[' => {
                push_token(&mut tokens, Token::LBracket, max_tokens)?;
                i += 1;
            }
            b']' => {
                push_token(&mut tokens, Token::RBracket, max_tokens)?;
                i += 1;
            }
            b',' => {
                push_token(&mut tokens, Token::Comma, max_tokens)?;
                i += 1;
            }
            b'.' => {
                push_token(&mut tokens, Token::Dot, max_tokens)?;
                i += 1;
            }
            b'+' => {
                push_token(&mut tokens, Token::Plus, max_tokens)?;
                i += 1;
            }
            b'-' => {
                push_token(&mut tokens, Token::Minus, max_tokens)?;
                i += 1;
            }
            b'=' => {
                push_token(&mut tokens, Token::Eq, max_tokens)?;
                i += 1;
            }
            b'<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                    push_token(&mut tokens, Token::Ne, max_tokens)?;
                    i += 2;
                } else if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    push_token(&mut tokens, Token::Le, max_tokens)?;
                    i += 2;
                } else {
                    push_token(&mut tokens, Token::Lt, max_tokens)?;
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    push_token(&mut tokens, Token::Ge, max_tokens)?;
                    i += 2;
                } else {
                    push_token(&mut tokens, Token::Gt, max_tokens)?;
                    i += 1;
                }
            }
            b'#' => {
                i += 1;
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                if i == start {
                    return Err(validation_err(
                        "Invalid expression: empty name reference '#'",
                    ));
                }
                push_token(
                    &mut tokens,
                    Token::NameRef(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            b':' => {
                i += 1;
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                if i == start {
                    return Err(validation_err(
                        "Invalid expression: empty value placeholder ':'",
                    ));
                }
                push_token(
                    &mut tokens,
                    Token::Placeholder(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            c if is_ident_start(c) => {
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                let word = &input[start..i];
                push_token(&mut tokens, keyword_or_ident(word), max_tokens)?;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                // List index numbers are parsed as identifiers for bracket access
                push_token(
                    &mut tokens,
                    Token::Ident(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            _ => {
                let (ch, near) = unknown_character(input, i, prev_start);
                return Err(validation_err(&format!(
                    "Syntax error; token: \"{ch}\", near: \"{near}\""
                )));
            }
        }
        if tokens.len() > before {
            prev_start = Some(tok_start);
        }
    }

    Ok(tokens)
}

/// Tokenize with an expression type prefix for DynamoDB-compatible error messages.
///
/// On invalid characters, produces: `"Invalid {expr_type}: Syntax error; token: "{tok}", near: "{near}"`
/// On empty input, produces: `"Invalid {expr_type}: The expression can not be empty;"`
pub fn tokenize_for(
    input: &str,
    max_tokens: usize,
    kind: super::ExpressionKind,
) -> Result<Vec<Token>, DynamoDbError> {
    if input.is_empty() {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid {kind}: The expression can not be empty;"
        )));
    }
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    let mut prev_start: Option<usize> = None;
    while i < bytes.len() {
        let tok_start = i;
        let before = tokens.len();
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b'(' => {
                push_token(&mut tokens, Token::LParen, max_tokens)?;
                i += 1;
            }
            b')' => {
                push_token(&mut tokens, Token::RParen, max_tokens)?;
                i += 1;
            }
            b'[' => {
                push_token(&mut tokens, Token::LBracket, max_tokens)?;
                i += 1;
            }
            b']' => {
                push_token(&mut tokens, Token::RBracket, max_tokens)?;
                i += 1;
            }
            b',' => {
                push_token(&mut tokens, Token::Comma, max_tokens)?;
                i += 1;
            }
            b'.' => {
                push_token(&mut tokens, Token::Dot, max_tokens)?;
                i += 1;
            }
            b'+' => {
                push_token(&mut tokens, Token::Plus, max_tokens)?;
                i += 1;
            }
            b'-' => {
                push_token(&mut tokens, Token::Minus, max_tokens)?;
                i += 1;
            }
            b'=' => {
                push_token(&mut tokens, Token::Eq, max_tokens)?;
                i += 1;
            }
            b'<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                    push_token(&mut tokens, Token::Ne, max_tokens)?;
                    i += 2;
                } else if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    push_token(&mut tokens, Token::Le, max_tokens)?;
                    i += 2;
                } else {
                    push_token(&mut tokens, Token::Lt, max_tokens)?;
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    push_token(&mut tokens, Token::Ge, max_tokens)?;
                    i += 2;
                } else {
                    push_token(&mut tokens, Token::Gt, max_tokens)?;
                    i += 1;
                }
            }
            b'#' => {
                i += 1;
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                if i == start {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid {kind}: Syntax error; token: \"#\", near: \"#\""
                    )));
                }
                push_token(
                    &mut tokens,
                    Token::NameRef(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            b':' => {
                i += 1;
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                if i == start {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid {kind}: Syntax error; token: \":\", near: \":\""
                    )));
                }
                push_token(
                    &mut tokens,
                    Token::Placeholder(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            c if is_ident_start(c) => {
                let start = i;
                while i < bytes.len() && is_ident_char(bytes[i]) {
                    i += 1;
                }
                let word = &input[start..i];
                push_token(&mut tokens, keyword_or_ident(word), max_tokens)?;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                push_token(
                    &mut tokens,
                    Token::Ident(input[start..i].to_owned()),
                    max_tokens,
                )?;
            }
            _ => {
                let (ch, near) = unknown_character(input, i, prev_start);
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid {kind}: Syntax error; token: \"{ch}\", near: \"{near}\""
                )));
            }
        }
        if tokens.len() > before {
            prev_start = Some(tok_start);
        }
    }
    Ok(tokens)
}

fn push_token(
    tokens: &mut Vec<Token>,
    token: Token,
    max_tokens: usize,
) -> Result<(), DynamoDbError> {
    if tokens.len() >= max_tokens {
        return Err(validation_err(&format!(
            "Expression exceeds maximum token count ({max_tokens})"
        )));
    }
    tokens.push(token);
    Ok(())
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// The token text and the `near` window the service reports for a character
/// the expression grammar does not accept, at byte offset `at` of `input`.
///
/// Measured 2026-10-25 on FilterExpression, ConditionExpression,
/// KeyConditionExpression, UpdateExpression and ProjectionExpression: the
/// token is the character as sent (a whole code point, `é` or an emoji), and
/// the window is the source text from the start of the previous token to the
/// end of the next one, whitespace kept, each dropped when absent. Every
/// character outside the grammar is a token of its own, so `#v = ééé` answers
/// `token: "é", near: "= éé"`, and a name stops at the first such character,
/// so `abéc = :v` answers `near: "abéc"`.
///
/// `prev_start` is the byte offset where the previous token starts.
fn unknown_character(input: &str, at: usize, prev_start: Option<usize>) -> (String, String) {
    let ch = input[at..].chars().next().unwrap_or('?');
    let after = at + ch.len_utf8();
    let end = next_token_end(input, after).unwrap_or(after);
    let start = prev_start.unwrap_or(at);
    (ch.to_string(), input[start..end].to_owned())
}

/// The byte offset where the next token after `from` ends, skipping the
/// tokenizer's whitespace, or `None` at the end of input. Mirrors the token
/// rules of [`tokenize_with_limit`], with a character outside the grammar
/// counted as one token.
fn next_token_end(input: &str, from: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut i = from;
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    let first = *bytes.get(i)?;
    let run = |mut j: usize, pred: fn(u8) -> bool| {
        while j < bytes.len() && pred(bytes[j]) {
            j += 1;
        }
        j
    };
    Some(match first {
        b'<' if matches!(bytes.get(i + 1), Some(b'>' | b'=')) => i + 2,
        b'>' if bytes.get(i + 1) == Some(&b'=') => i + 2,
        b'(' | b')' | b'[' | b']' | b',' | b'.' | b'+' | b'-' | b'=' | b'<' | b'>' => i + 1,
        b'#' | b':' => run(i + 1, is_ident_char),
        c if is_ident_start(c) => run(i, is_ident_char),
        c if c.is_ascii_digit() => run(i, |b| b.is_ascii_digit()),
        _ => i + input[i..].chars().next().map_or(1, char::len_utf8),
    })
}

fn keyword_or_ident(word: &str) -> Token {
    match word.to_ascii_uppercase().as_str() {
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "SET" => Token::Set,
        "REMOVE" => Token::Remove,
        "BETWEEN" => Token::Between,
        "IN" => Token::In,
        "ADD" => Token::Add,
        "DELETE" => Token::Delete,
        _ => Token::Ident(word.to_owned()),
    }
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple_comparison() {
        let tokens = tokenize("price > :min").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("price".into()),
                Token::Gt,
                Token::Placeholder("min".into()),
            ]
        );
    }

    #[test]
    fn tokenize_name_ref_and_placeholder() {
        let tokens = tokenize("#n = :v").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::NameRef("n".into()),
                Token::Eq,
                Token::Placeholder("v".into())
            ]
        );
    }

    #[test]
    fn tokenize_all_operators() {
        let tokens = tokenize("= <> < <= > >=").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Eq,
                Token::Ne,
                Token::Lt,
                Token::Le,
                Token::Gt,
                Token::Ge
            ]
        );
    }

    #[test]
    fn tokenize_keywords_case_insensitive() {
        let tokens = tokenize("SET REMOVE add DELETE And Or Not Between In").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Set,
                Token::Remove,
                Token::Add,
                Token::Delete,
                Token::And,
                Token::Or,
                Token::Not,
                Token::Between,
                Token::In,
            ]
        );
    }

    #[test]
    fn tokenize_path_with_index() {
        let tokens = tokenize("items[0].name").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("items".into()),
                Token::LBracket,
                Token::Ident("0".into()),
                Token::RBracket,
                Token::Dot,
                Token::Ident("name".into()),
            ]
        );
    }

    #[test]
    fn tokenize_arithmetic() {
        let tokens = tokenize("price + :tax - :discount").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("price".into()),
                Token::Plus,
                Token::Placeholder("tax".into()),
                Token::Minus,
                Token::Placeholder("discount".into()),
            ]
        );
    }

    #[test]
    fn tokenize_empty_name_ref_rejected() {
        let err = tokenize("# = :v").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("empty name reference"))
        );
    }

    #[test]
    fn tokenize_empty_placeholder_rejected() {
        let err = tokenize("a = :").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("empty value placeholder"))
        );
    }

    #[test]
    fn tokenize_invalid_character_rejected() {
        let err = tokenize("a @ b").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("Syntax error; token:"))
        );
    }

    #[test]
    fn tokenize_exceeds_token_limit() {
        let err = tokenize_with_limit("a = b AND c = d", 3).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("maximum token count"))
        );
    }

    #[test]
    fn tokenize_whitespace_variants() {
        let tokens = tokenize("a\t=\n:v\r").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("a".into()),
                Token::Eq,
                Token::Placeholder("v".into())
            ]
        );
    }

    #[test]
    fn tokenize_empty_input() {
        let tokens = tokenize("").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_function_call() {
        let tokens = tokenize("attribute_exists(#pk)").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("attribute_exists".into()),
                Token::LParen,
                Token::NameRef("pk".into()),
                Token::RParen,
            ]
        );
    }

    #[test]
    fn tokenize_underscore_ident() {
        let tokens = tokenize("_my_attr = :v").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("_my_attr".into()),
                Token::Eq,
                Token::Placeholder("v".into()),
            ]
        );
    }

    #[test]
    fn tokenize_for_multibyte_utf8_does_not_panic() {
        // Emoji (4-byte UTF-8) should produce a validation error, not a panic.
        let err =
            tokenize_for("a = 😀", 4096, super::super::ExpressionKind::Condition).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(ref msg) if msg.contains("Syntax error"))
        );

        // Accented character (2-byte UTF-8)
        let err = tokenize_for("café", 4096, super::super::ExpressionKind::Filter).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(ref msg) if msg.contains("Syntax error"))
        );

        // Multi-byte at the very end
        let err = tokenize_for("x + ñ", 4096, super::super::ExpressionKind::Update).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(ref msg) if msg.contains("Syntax error"))
        );
    }

    /// The token and window the service reports for a character outside the
    /// grammar (measured 2026-10-25), as `(expression, token, near)`.
    const UNKNOWN_CHARACTER_ROWS: &[(&str, &str, &str)] = &[
        ("#v = ééé", "é", "= éé"),
        ("#v = é", "é", "= é"),
        ("é = :v", "é", "é ="),
        ("#v = :v AND é", "é", "AND é"),
        ("#v = é #v", "é", "= é #v"),
        ("#v = 日本語", "日", "= 日本"),
        ("#v = 😀x", "😀", "= 😀x"),
        ("#v = $x", "$", "= $x"),
        ("#v = @", "@", "= @"),
        ("#v = aé", "é", "aé"),
        ("abéc = :v", "é", "abéc"),
        ("#longname = é", "é", "= é"),
        ("#v = é é", "é", "= é é"),
        ("(#v = é)", "é", "= é)"),
        ("contains(#v, é)", "é", ", é)"),
        ("#v = :v !", "!", ":v !"),
        ("#v =\té", "é", "=\té"),
        ("#v =é", "é", "=é"),
        ("pk = :v AND é", "é", "AND é"),
        ("SET #v = éé", "é", "= éé"),
        ("#v, $x", "$", ", $x"),
    ];

    #[test]
    fn unknown_character_reports_the_character_and_the_service_window() {
        for (expr, token, near) in UNKNOWN_CHARACTER_ROWS {
            let expected = format!("Syntax error; token: \"{token}\", near: \"{near}\"");
            let Err(DynamoDbError::ValidationException(msg)) = tokenize(expr) else {
                panic!("{expr}: expected a syntax error");
            };
            assert_eq!(msg, expected, "{expr}");
            let Err(DynamoDbError::ValidationException(msg)) =
                tokenize_for(expr, 4096, super::super::ExpressionKind::Filter)
            else {
                panic!("{expr}: expected a syntax error");
            };
            assert_eq!(
                msg,
                format!("Invalid FilterExpression: {expected}"),
                "{expr}"
            );
        }
    }
}
