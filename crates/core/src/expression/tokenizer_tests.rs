// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

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
    let err = tokenize_for("a = 😀", 4096, super::super::ExpressionKind::Condition).unwrap_err();
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
    // No token before the character: the window starts at the start.
    ("  é = :v", "é", "  é ="),
    ("\té = :v", "é", "\té ="),
    ("\n\né = :v", "é", "\n\né ="),
    (" é", "é", " é"),
    ("  $ = :v", "$", "  $ ="),
    ("   éé = :v", "é", "   éé"),
    ("  é SET #v = :v", "é", "  é SET"),
    (" #v = :v AND é", "é", "AND é"),
    (" (é)", "é", "(é)"),
    // U+0000 is removed from the token and the window.
    ("#v = :v AND \0", "", "AND "),
    ("#v = \0x", "", "= x"),
    ("\0#v = :v", "", "#v"),
    ("#v = é\0", "é", "= é"),
    ("#v\0 = :v", "", "#v ="),
    ("#v = :v AND \0\0", "", "AND "),
    ("a\0b = :v", "", "ab"),
    // The next token: two-character operators, a name run, a digit run.
    ("#v é<> :v", "é", "#v é<>"),
    ("#v é<= :v", "é", "#v é<="),
    ("#v é>= :v", "é", "#v é>="),
    ("#v <>é", "é", "<>é"),
    ("#v = é abc", "é", "= é abc"),
    ("#v[é12] = :v", "é", "[é12"),
    ("#v BETWEEN é AND :v", "é", "BETWEEN é AND"),
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
