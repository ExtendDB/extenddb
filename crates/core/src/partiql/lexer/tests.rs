// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the PartiQL lexer.

use super::*;

fn kinds(input: &str) -> Vec<TokenKind> {
    lex(input).unwrap().into_iter().map(|t| t.kind).collect()
}

#[test]
fn line_comment_reads_as_whitespace() {
    assert_eq!(
        kinds("a -- note\nb -- trailing"),
        vec![
            TokenKind::Ident("a".to_owned()),
            TokenKind::Ident("b".to_owned()),
            TokenKind::Eof
        ]
    );
}

#[test]
fn block_comment_reads_as_whitespace() {
    assert_eq!(
        kinds("a /* note\n more */ b /**/"),
        vec![
            TokenKind::Ident("a".to_owned()),
            TokenKind::Ident("b".to_owned()),
            TokenKind::Eof
        ]
    );
}

#[test]
fn a_comment_keeps_later_spans_on_their_line() {
    let tokens = lex("-- note\n  b").unwrap();
    assert_eq!((tokens[0].line, tokens[0].column), (2, 3));
}

#[test]
fn unterminated_block_comment_is_a_lex_error() {
    assert!(lex("a /* note").is_err());
    assert!(lex("a /* note *").is_err());
}

#[test]
fn hash_is_a_lex_error() {
    assert!(lex("a # note").is_err());
}

#[test]
fn a_single_minus_stays_an_operator() {
    assert_eq!(
        kinds("a - 1"),
        vec![
            TokenKind::Ident("a".to_owned()),
            TokenKind::Minus,
            TokenKind::Number("1".to_owned()),
            TokenKind::Eof
        ]
    );
}

#[test]
fn a_number_may_start_with_its_decimal_point() {
    assert_eq!(
        kinds(".73e1 .5"),
        vec![
            TokenKind::Number(".73e1".to_owned()),
            TokenKind::Number(".5".to_owned()),
            TokenKind::Eof
        ]
    );
}

fn numbers(input: &str) -> Vec<String> {
    kinds(input)
        .into_iter()
        .map(|k| match k {
            TokenKind::Number(n) => n,
            TokenKind::Ident(w) => format!("word {w}"),
            TokenKind::Dot => ".".to_owned(),
            TokenKind::Eof => "end".to_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_complete_number_ends_where_a_second_dot_or_a_letter_starts() {
    assert_eq!(numbers("1.2.3"), ["1.2", ".3", "end"]);
    assert_eq!(numbers("1e5.5"), ["1e5", ".5", "end"]);
    assert_eq!(numbers("1e-5.2"), ["1e-5", ".2", "end"]);
    assert_eq!(numbers("1e5e5"), ["1e5", "word e5", "end"]);
    assert_eq!(numbers("1.2e3e4"), ["1.2e3", "word e4", "end"]);
    assert_eq!(numbers("1_000"), ["1", "word _000", "end"]);
    assert_eq!(numbers("1.2a"), ["1.2", "word a", "end"]);
    assert_eq!(numbers("1.2.e3"), ["1.2", ".", "word e3", "end"]);
    assert_eq!(numbers("1.2..3"), ["1.2", ".", ".3", "end"]);
    assert_eq!(numbers("..5"), [".", ".5", "end"]);
}

#[test]
fn numbers_the_service_reads() {
    for text in ["1.e3", "1.", ".5", "00.5", "1e05", "1E5", "1.5e+3"] {
        assert_eq!(numbers(text), [text, "end"], "{text}");
    }
}

#[test]
fn a_dot_after_the_numbers_own_dot_or_an_exponent_without_digits_does_not_lex() {
    for text in [
        "1..2", "5..", "1..", "1...2", "1.5e", "1e+", "5.e", "1ee5", "1e.5",
    ] {
        assert!(lex(text).is_err(), "{text}");
    }
}

#[test]
fn a_word_is_ascii_letters_digits_underscore_and_dollar() {
    assert_eq!(numbers("$x a$b_1"), ["word $x", "word a$b_1", "end"]);
    assert!(lex("café = 1").is_err());
}

#[test]
fn an_at_sign_before_a_word_reads_as_the_word() {
    let tokens = lex("@n = 1").unwrap();
    assert_eq!(tokens[0].kind, TokenKind::Ident("n".to_owned()));
    assert_eq!((tokens[0].column, tokens[0].length), (1, 2));
    assert!(lex("@ n").is_err());
}

#[test]
fn percent_slash_and_concatenation_are_operators() {
    assert_eq!(
        kinds("a % b / c || d"),
        vec![
            TokenKind::Ident("a".to_owned()),
            TokenKind::Percent,
            TokenKind::Ident("b".to_owned()),
            TokenKind::Slash,
            TokenKind::Ident("c".to_owned()),
            TokenKind::Concat,
            TokenKind::Ident("d".to_owned()),
            TokenKind::Eof
        ]
    );
    for text in ["a | b", "a ^ b", "a & b", "~a", "!a"] {
        assert!(lex(text).is_err(), "{text}");
    }
}

#[test]
fn a_backtick_literal_is_one_token() {
    let tokens = lex("s = `1`").unwrap();
    assert_eq!(tokens[2].kind, TokenKind::Ion("1".to_owned()));
    assert_eq!((tokens[2].column, tokens[2].length), (5, 3));
    assert!(lex("s = `1").is_err());
}

#[test]
fn a_dot_before_a_name_stays_the_path_dot() {
    assert_eq!(
        kinds("a.b"),
        vec![
            TokenKind::Ident("a".to_owned()),
            TokenKind::Dot,
            TokenKind::Ident("b".to_owned()),
            TokenKind::Eof
        ]
    );
}
