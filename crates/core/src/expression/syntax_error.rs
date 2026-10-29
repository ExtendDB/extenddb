// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Syntax error; token: "<t>", near: "<window>"` wording Amazon `DynamoDB`
//! uses when a parser meets a token it cannot accept.
//!
//! Measured 2026-10-25 (us-east-1) on `ConditionExpression`, `FilterExpression`,
//! `KeyConditionExpression`, `UpdateExpression` and `ProjectionExpression`:
//! the token is the offending token's text, or `<EOF>` when the input ends
//! where a token is required; the window is the source text from the start of
//! the token before the offending one to the end of the token after it, with
//! the source's own whitespace kept (`#v==:v` answers `near: "==:v"`) and a
//! missing neighbour dropped (`= #v = :v` answers `near: "= #v"`, `#v = :v =`
//! answers `near: ":v ="`). For `<EOF>` the window is the last token's text
//! (`#v = :v AND` answers `token: "<EOF>", near: "AND"`).

use std::ops::Range;

use super::kind::ExpressionKind;
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// The text a token is shown as in a syntax error when the source it came
/// from is not available. With a source, the token is shown as written.
#[must_use]
pub fn token_text(token: &Token) -> String {
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

/// Whether `token` is an operator (`=`, `<>`, `<`, `<=`, `>`, `>=`, `+`, `-`) or
/// a keyword (`AND`, `OR`, `NOT`, `BETWEEN`, `IN`, `SET`, `REMOVE`, `ADD`,
/// `DELETE`): the token classes measured to answer the service's syntax error
/// when they appear where an operand or a connector is required. Punctuation
/// and operands are not in the measured set and keep their previous texts.
#[must_use]
pub fn is_operator_or_keyword(token: &Token) -> bool {
    !matches!(
        token,
        Token::Ident(_)
            | Token::NameRef(_)
            | Token::Placeholder(_)
            | Token::Comma
            | Token::Dot
            | Token::LBracket
            | Token::RBracket
            | Token::LParen
            | Token::RParen
    )
}

/// The byte length a token occupies in the source it was read from. Keywords
/// are matched case-insensitively by the tokenizer, so their length is the
/// length of the keyword; every other token's text is reproduced verbatim.
fn token_len(token: &Token) -> usize {
    match token {
        Token::Ident(s) => s.len(),
        Token::NameRef(s) | Token::Placeholder(s) => 1 + s.len(),
        _ => token_text(token).len(),
    }
}

/// Byte ranges of `tokens` in `source`, recovered by walking the source with
/// the tokenizer's whitespace rule (space, tab, newline, carriage return).
/// Returns `None` when the tokens do not line up with the source (a caller
/// that passed a source the tokens did not come from), so the window falls
/// back to the token texts.
fn token_spans(source: &str, tokens: &[Token]) -> Option<Vec<Range<usize>>> {
    let bytes = source.as_bytes();
    let mut spans = Vec::with_capacity(tokens.len());
    let mut i = 0;
    for token in tokens {
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        let end = i + token_len(token);
        if end > bytes.len() {
            return None;
        }
        spans.push(i..end);
        i = end;
    }
    Some(spans)
}

/// Build the service's syntax error for the token at `pos` (or `<EOF>` when
/// `pos == tokens.len()`), prefixed with `Invalid <kind>:`.
///
/// `source` is the expression text the tokens came from; an empty source
/// (a caller holding only tokens) joins the token texts with single spaces.
#[must_use]
pub fn syntax_error(
    kind: ExpressionKind,
    source: &str,
    tokens: &[Token],
    pos: usize,
) -> DynamoDbError {
    // The token is shown as the client wrote it: a lower-case `and` answers
    // `token: "and"` (measured 2026-10-25 on FilterExpression,
    // ConditionExpression and KeyConditionExpression).
    let spans = (!source.is_empty())
        .then(|| token_spans(source, tokens))
        .flatten();
    let token = match (tokens.get(pos), &spans) {
        (None, _) => "<EOF>".to_owned(),
        (Some(_), Some(spans)) => source[spans[pos].clone()].to_owned(),
        (Some(t), None) => token_text(t),
    };
    let near = near_window(source, tokens, pos);
    DynamoDbError::ValidationException(format!(
        "Invalid {kind}: Syntax error; token: \"{token}\", near: \"{near}\""
    ))
}

/// The window of source text around the token at `pos`: from the previous
/// token to the next one, each dropped when absent. When the faulty token is
/// the first one, the window starts at the start of the source, leading
/// whitespace kept (measured 2026-10-25: `  = #v = :v` answers
/// `near: "  = #v"`). At end of input the window is the last token alone.
fn near_window(source: &str, tokens: &[Token], pos: usize) -> String {
    if tokens.is_empty() {
        return String::new();
    }
    let (first, last) = if pos >= tokens.len() {
        (tokens.len() - 1, tokens.len() - 1)
    } else {
        (pos.saturating_sub(1), (pos + 1).min(tokens.len() - 1))
    };
    if let Some(spans) = (!source.is_empty())
        .then(|| token_spans(source, tokens))
        .flatten()
    {
        let start = if pos == 0 { 0 } else { spans[first].start };
        return source[start..spans[last].end].to_owned();
    }
    tokens[first..=last]
        .iter()
        .map(token_text)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::tokenizer::tokenize;

    fn msg(kind: ExpressionKind, source: &str, pos: usize) -> String {
        let tokens = tokenize(source).unwrap();
        match syntax_error(kind, source, &tokens, pos) {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("{other:?}"),
        }
    }

    // Expected texts measured 2026-10-25 (us-east-1), probe cases in brackets.

    #[test]
    fn repeated_operator_in_the_middle_shows_three_tokens() {
        // [d01] `#v = = :v`: the second `=` is the offending token.
        assert_eq!(
            msg(ExpressionKind::Condition, "#v = = :v", 2),
            "Invalid ConditionExpression: Syntax error; token: \"=\", near: \"= = :v\""
        );
    }

    #[test]
    fn window_keeps_the_source_whitespace() {
        // [d17] no spaces, [d18] a tab, [d12] longer tokens.
        assert!(msg(ExpressionKind::Condition, "#v==:v", 2).ends_with("near: \"==:v\""));
        assert!(msg(ExpressionKind::Condition, "#v =\t= :v", 2).ends_with("near: \"=\t= :v\""));
        assert!(
            msg(ExpressionKind::Condition, "#longname = = :longvalue", 2)
                .ends_with("near: \"= = :longvalue\"")
        );
    }

    #[test]
    fn missing_neighbours_are_dropped() {
        // [d03] at the start, [d04] at the end, [d29] alone, [d30] two tokens.
        assert!(msg(ExpressionKind::Condition, "= #v = :v", 0).ends_with("near: \"= #v\""));
        assert!(msg(ExpressionKind::Condition, "#v = :v =", 3).ends_with("near: \":v =\""));
        assert!(msg(ExpressionKind::Condition, "=", 0).ends_with("near: \"=\""));
        assert!(msg(ExpressionKind::Condition, "= =", 0).ends_with("near: \"= =\""));
    }

    #[test]
    fn a_fault_at_the_first_token_keeps_leading_whitespace() {
        // Measured 2026-10-25 on FilterExpression, KeyConditionExpression and
        // ProjectionExpression.
        assert!(msg(ExpressionKind::Filter, "  = #v = :v", 0).ends_with("near: \"  = #v\""));
        assert!(msg(ExpressionKind::Filter, "\t= #v = :v", 0).ends_with("near: \"\t= #v\""));
        assert!(msg(ExpressionKind::Filter, "  AND #v = :v", 0).ends_with("near: \"  AND #v\""));
        assert!(msg(ExpressionKind::KeyCondition, "  = pk = :v", 0).ends_with("near: \"  = pk\""));
        assert!(msg(ExpressionKind::Projection, "  , #v", 0).ends_with("near: \"  , #v\""));
        assert!(msg(ExpressionKind::Filter, "= #v = :v", 0).ends_with("near: \"= #v\""));
    }

    #[test]
    fn end_of_input_is_eof_with_the_last_token() {
        // [d05] trailing AND, [d26] trailing comma on an update.
        assert_eq!(
            msg(ExpressionKind::Condition, "#v = :v AND", 4),
            "Invalid ConditionExpression: Syntax error; token: \"<EOF>\", near: \"AND\""
        );
        assert_eq!(
            msg(ExpressionKind::Update, "SET #v = :v,", 5),
            "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \",\""
        );
    }

    #[test]
    fn keyword_tokens_show_the_source_text() {
        // [d07] doubled AND; a lower-case keyword (measured 2026-10-25) is
        // shown as written, in the token and in the window.
        assert_eq!(
            msg(ExpressionKind::Condition, "#v = :v AND AND #v = :v", 4),
            "Invalid ConditionExpression: Syntax error; token: \"AND\", near: \"AND AND #v\""
        );
        assert_eq!(
            msg(ExpressionKind::Filter, "#v = :v and and #v = :v", 4),
            "Invalid FilterExpression: Syntax error; token: \"and\", near: \"and and #v\""
        );
    }

    #[test]
    fn without_a_source_the_window_joins_token_texts() {
        let tokens = tokenize("#v  =  =  :v").unwrap();
        let DynamoDbError::ValidationException(m) =
            syntax_error(ExpressionKind::Filter, "", &tokens, 2)
        else {
            panic!()
        };
        assert_eq!(
            m,
            "Invalid FilterExpression: Syntax error; token: \"=\", near: \"= = :v\""
        );
    }
}
