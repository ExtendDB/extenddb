// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL lexer.
//!
//! Quoting rules match the service: single quotes delimit a string literal,
//! double quotes delimit an identifier, backticks delimit an Ion literal. `?`
//! is a positional parameter, `<<` and `>>` bracket a set (bag) literal. `--`
//! starts a comment that runs to the end of the line and `/* ... */` brackets
//! a comment; both read as whitespace. A token carries a 1-based
//! `line:column:length` span so the parser can build the service's exact
//! `... at <l:c:len>` messages.
//!
//! An unquoted word is ASCII letters, digits, `_` and `$`, not starting with a
//! digit, and `@` before a word reads as that word (measured 2026-11-02: `$x`
//! and `@n` are names, `café` does not lex). `%`, `/` and `||` are operators
//! the parser reads and then refuses.
//!
//! A number is digits, an optional dot with optional digits, then an optional
//! exponent that must have digits; it may also start with its decimal point
//! (`.5`). What follows a complete number starts a new token, except a second
//! dot right after the number's own dot, and an exponent with no digits: both
//! fail to lex (measured 2026-11-02 on 38 shapes: `1.2.3` is two numbers,
//! `1..2`, `1e+` and `5.e` do not lex, `1.e3` and `00.5` are numbers).
//!
//! The service reports every lexing failure (a character it does not read such
//! as `#`, an unterminated comment, string or quoted identifier) with nothing
//! after the "wasn't well formed" prefix (measured 2026-11-02), so the parser
//! drops [`LexError::message`]; the message is kept for diagnostics only.

mod words;

pub use words::{WordClass, is_function_keyword, word_class};

/// A lexical token with its 1-based line, column and length in the source.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub column: usize,
    pub length: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// A bare identifier or keyword (case-insensitive match happens in the parser).
    Ident(String),
    /// A double-quoted identifier.
    QuotedIdent(String),
    /// A single-quoted string literal.
    String(String),
    /// A numeric literal, kept as text to preserve precision.
    Number(String),
    /// A backtick-quoted Ion literal, the text between the backticks.
    Ion(String),
    Param,
    Comma,
    Dot,
    Star,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LDoubleAngle,
    RDoubleAngle,
    LBrace,
    RBrace,
    Colon,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Percent,
    Slash,
    /// `||`
    Concat,
    Semicolon,
    Eof,
}

/// A lexing error with the span that triggered it.
#[derive(Debug, Clone, PartialEq)]
pub struct LexError {
    pub message: String,
    pub line: usize,
    pub column: usize,
    pub length: usize,
}

/// Tokenize a PartiQL statement.
///
/// # Errors
///
/// Returns a [`LexError`] on an unterminated string or identifier or an
/// unrecognized character.
pub fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    let mut line = 1;
    let mut col = 1;

    let advance = |i: &mut usize, line: &mut usize, col: &mut usize, n: usize, chars: &[char]| {
        for _ in 0..n {
            if chars.get(*i) == Some(&'\n') {
                *line += 1;
                *col = 1;
            } else {
                *col += 1;
            }
            *i += 1;
        }
    };

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            advance(&mut i, &mut line, &mut col, 1, &chars);
            continue;
        }
        let (start_line, start_col) = (line, col);
        // `-- ...` runs to the end of the line.
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            let mut j = i + 2;
            while j < chars.len() && chars[j] != '\n' && chars[j] != '\r' {
                j += 1;
            }
            let len = j - i;
            advance(&mut i, &mut line, &mut col, len, &chars);
            continue;
        }
        // `/* ... */`; one with no closing `*/` is a lexing failure.
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut j = i + 2;
            while j + 1 < chars.len() && !(chars[j] == '*' && chars[j + 1] == '/') {
                j += 1;
            }
            if j + 1 >= chars.len() {
                return Err(LexError {
                    message: "unterminated block comment".to_owned(),
                    line: start_line,
                    column: start_col,
                    length: chars.len() - i,
                });
            }
            let len = j + 2 - i;
            advance(&mut i, &mut line, &mut col, len, &chars);
            continue;
        }
        // A number written from its decimal point (`.5`) reads as a number;
        // any other `.` is the path dot.
        if c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit) {
            let len =
                number_len(&chars, i).ok_or_else(|| malformed_number(start_line, start_col))?;
            tokens.push(Token {
                kind: TokenKind::Number(chars[i..i + len].iter().collect()),
                line: start_line,
                column: start_col,
                length: len,
            });
            advance(&mut i, &mut line, &mut col, len, &chars);
            continue;
        }
        match c {
            '\'' => {
                // String literal; '' is an escaped single quote.
                let mut s = String::new();
                let mut j = i + 1;
                loop {
                    match chars.get(j) {
                        None => {
                            return Err(LexError {
                                message: "unterminated string literal".to_owned(),
                                line: start_line,
                                column: start_col,
                                length: j - i,
                            });
                        }
                        Some('\'') if chars.get(j + 1) == Some(&'\'') => {
                            s.push('\'');
                            j += 2;
                        }
                        Some('\'') => {
                            j += 1;
                            break;
                        }
                        Some(ch) => {
                            s.push(*ch);
                            j += 1;
                        }
                    }
                }
                let len = j - i;
                tokens.push(Token {
                    kind: TokenKind::String(s),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            '"' => {
                let mut s = String::new();
                let mut j = i + 1;
                loop {
                    match chars.get(j) {
                        None => {
                            return Err(LexError {
                                message: "unterminated quoted identifier".to_owned(),
                                line: start_line,
                                column: start_col,
                                length: j - i,
                            });
                        }
                        Some('"') if chars.get(j + 1) == Some(&'"') => {
                            s.push('"');
                            j += 2;
                        }
                        Some('"') => {
                            j += 1;
                            break;
                        }
                        Some(ch) => {
                            s.push(*ch);
                            j += 1;
                        }
                    }
                }
                let len = j - i;
                tokens.push(Token {
                    kind: TokenKind::QuotedIdent(s),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            '0'..='9' => {
                let len =
                    number_len(&chars, i).ok_or_else(|| malformed_number(start_line, start_col))?;
                let text: String = chars[i..i + len].iter().collect();
                tokens.push(Token {
                    kind: TokenKind::Number(text),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            c if is_word_start(c) => {
                let len = word_len(&chars, i);
                let text: String = chars[i..i + len].iter().collect();
                tokens.push(Token {
                    kind: TokenKind::Ident(text),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            // `@name` reads as the name; the span keeps the `@`.
            '@' if chars.get(i + 1).copied().is_some_and(is_word_start) => {
                let len = 1 + word_len(&chars, i + 1);
                let text: String = chars[i + 1..i + len].iter().collect();
                tokens.push(Token {
                    kind: TokenKind::Ident(text),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            '`' => {
                let Some(close) = chars[i + 1..].iter().position(|&ch| ch == '`') else {
                    return Err(LexError {
                        message: "unterminated Ion literal".to_owned(),
                        line: start_line,
                        column: start_col,
                        length: chars.len() - i,
                    });
                };
                let len = close + 2;
                tokens.push(Token {
                    kind: TokenKind::Ion(chars[i + 1..i + 1 + close].iter().collect()),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            _ => {
                // Multi- and single-character operators.
                let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
                let (kind, len) = match two.as_str() {
                    "<<" => (TokenKind::LDoubleAngle, 2),
                    ">>" => (TokenKind::RDoubleAngle, 2),
                    "<=" => (TokenKind::Le, 2),
                    ">=" => (TokenKind::Ge, 2),
                    "<>" => (TokenKind::Ne, 2),
                    "!=" => (TokenKind::Ne, 2),
                    "||" => (TokenKind::Concat, 2),
                    _ => {
                        let k = match c {
                            ',' => TokenKind::Comma,
                            '.' => TokenKind::Dot,
                            '*' => TokenKind::Star,
                            '(' => TokenKind::LParen,
                            ')' => TokenKind::RParen,
                            '[' => TokenKind::LBracket,
                            ']' => TokenKind::RBracket,
                            '{' => TokenKind::LBrace,
                            '}' => TokenKind::RBrace,
                            ':' => TokenKind::Colon,
                            '=' => TokenKind::Eq,
                            '<' => TokenKind::Lt,
                            '>' => TokenKind::Gt,
                            '+' => TokenKind::Plus,
                            '-' => TokenKind::Minus,
                            '%' => TokenKind::Percent,
                            '/' => TokenKind::Slash,
                            '?' => TokenKind::Param,
                            ';' => TokenKind::Semicolon,
                            other => {
                                return Err(LexError {
                                    message: format!("unexpected character '{other}'"),
                                    line: start_line,
                                    column: start_col,
                                    length: 1,
                                });
                            }
                        };
                        (k, 1)
                    }
                };
                tokens.push(Token {
                    kind,
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
        }
    }
    tokens.push(Token {
        kind: TokenKind::Eof,
        line,
        column: col,
        length: 0,
    });
    Ok(tokens)
}

/// The length of the number literal starting at `start` (a digit, or a dot
/// with a digit after it), or `None` when the text there does not lex: a dot
/// right after the number's own dot (`1..2`), or an exponent with no digits
/// (`1e+`, `1ee5`). See the module notes. The text is handed on as written;
/// converting it to a number is the parser's and engine's job.
fn number_len(chars: &[char], start: usize) -> Option<usize> {
    let digits_from = |mut j: usize| {
        while chars.get(j).is_some_and(char::is_ascii_digit) {
            j += 1;
        }
        j
    };
    let mut j = digits_from(start);
    if chars.get(j) == Some(&'.') {
        let after = digits_from(j + 1);
        if after == j + 1 && chars.get(after) == Some(&'.') {
            return None;
        }
        j = after;
    }
    if matches!(chars.get(j), Some('e' | 'E')) {
        let mut k = j + 1;
        if matches!(chars.get(k), Some('+' | '-')) {
            k += 1;
        }
        let end = digits_from(k);
        if end == k {
            return None;
        }
        j = end;
    }
    Some(j - start)
}

fn malformed_number(line: usize, column: usize) -> LexError {
    LexError {
        message: "malformed number".to_owned(),
        line,
        column,
        length: 1,
    }
}

fn is_word_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == '$'
}

/// The length of the unquoted word starting at `start`.
fn word_len(chars: &[char], start: usize) -> usize {
    chars[start..]
        .iter()
        .take_while(|c| c.is_ascii_alphanumeric() || **c == '_' || **c == '$')
        .count()
}

#[cfg(test)]
mod tests;
