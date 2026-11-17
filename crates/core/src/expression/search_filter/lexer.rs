// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tokens of a search condition, with their byte spans.
//!
//! The spans exist for the syntax error message. The service names the
//! offending token and the text near it: the token before, the whitespace
//! touching the offending token, the offending token, and the token after.
//! `a == :s` gives `Syntax error; token: "=", near: "== :s"`.

/// The kind of a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Kind {
    /// A bare attribute or function name.
    Name,
    /// `#name`.
    NameRef,
    /// `:value`.
    ValueRef,
    /// A run of digits.
    Number,
    And,
    Or,
    Not,
    Between,
    In,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Plus,
    Minus,
    /// A character no token starts with, a leading byte-order mark included.
    Unknown,
    /// The end of the expression.
    Eof,
}

/// A token and the bytes of the expression it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Tok {
    pub(super) kind: Kind,
    pub(super) start: usize,
    pub(super) end: usize,
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn keyword(word: &str) -> Option<Kind> {
    match word.to_ascii_uppercase().as_str() {
        "AND" => Some(Kind::And),
        "OR" => Some(Kind::Or),
        "NOT" => Some(Kind::Not),
        "BETWEEN" => Some(Kind::Between),
        "IN" => Some(Kind::In),
        _ => None,
    }
}

/// Split `src` into tokens, ending with one [`Kind::Eof`].
///
/// Never fails: a character that starts no token becomes [`Kind::Unknown`],
/// which the parser reports as a syntax error at that character.
pub(super) fn lex(src: &str) -> Vec<Tok> {
    let bytes = src.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let b = bytes[i];
        let single = |kind| Some((kind, 1));
        let fixed = match b {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
                continue;
            }
            b'(' => single(Kind::LParen),
            b')' => single(Kind::RParen),
            b'[' => single(Kind::LBracket),
            b']' => single(Kind::RBracket),
            b',' => single(Kind::Comma),
            b'.' => single(Kind::Dot),
            b'+' => single(Kind::Plus),
            b'-' => single(Kind::Minus),
            b'=' => single(Kind::Eq),
            b'<' => match bytes.get(i + 1) {
                Some(b'>') => Some((Kind::Ne, 2)),
                Some(b'=') => Some((Kind::Le, 2)),
                _ => single(Kind::Lt),
            },
            b'>' => match bytes.get(i + 1) {
                Some(b'=') => Some((Kind::Ge, 2)),
                _ => single(Kind::Gt),
            },
            _ => None,
        };
        if let Some((kind, len)) = fixed {
            i += len;
            toks.push(Tok {
                kind,
                start,
                end: i,
            });
            continue;
        }
        let kind = match b {
            b'#' | b':' if bytes.get(i + 1).copied().is_some_and(is_word_byte) => {
                i += 1;
                while i < bytes.len() && is_word_byte(bytes[i]) {
                    i += 1;
                }
                if b == b'#' {
                    Kind::NameRef
                } else {
                    Kind::ValueRef
                }
            }
            b if b.is_ascii_digit() => {
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                Kind::Number
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                while i < bytes.len() && is_word_byte(bytes[i]) {
                    i += 1;
                }
                keyword(&src[start..i]).unwrap_or(Kind::Name)
            }
            _ => {
                // One whole character, which may take several bytes.
                let len = src[i..].chars().next().map_or(1, char::len_utf8);
                i += len;
                Kind::Unknown
            }
        };
        toks.push(Tok {
            kind,
            start,
            end: i,
        });
    }
    toks.push(Tok {
        kind: Kind::Eof,
        start: src.len(),
        end: src.len(),
    });
    toks
}

/// The syntax error body for the token at `at`, without the expression
/// prefix: `Syntax error; token: "<text>", near: "<near>"`.
///
/// `near` runs from the start of the token before (or from the start of the
/// expression when there is none) to the end of the offending token, then on
/// to the end of the token after, which takes in the whitespace between them.
/// The end of the expression reads as `<EOF>` in `token` and as nothing in
/// `near`. NUL characters are dropped from the message, as the service drops
/// them.
pub(super) fn syntax_error(src: &str, toks: &[Tok], at: usize) -> String {
    let cur = &toks[at];
    let near_start = at
        .checked_sub(1)
        .and_then(|prev| toks.get(prev))
        .map_or(0, |prev| prev.start);
    let near_end = if cur.kind == Kind::Eof {
        cur.end
    } else {
        toks.get(at + 1).map_or(cur.end, |next| next.end)
    };
    let token = if cur.kind == Kind::Eof {
        "<EOF>"
    } else {
        &src[cur.start..cur.end]
    };
    let near = &src[near_start..near_end];
    format!("Syntax error; token: \"{token}\", near: \"{near}\"").replace('\0', "")
}
