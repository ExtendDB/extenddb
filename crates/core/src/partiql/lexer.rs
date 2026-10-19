// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL lexer.
//!
//! Quoting rules match the service: single quotes delimit a string literal,
//! double quotes delimit an identifier. `?` is a positional parameter, `<<` and
//! `>>` bracket a set (bag) literal. A parse error carries a 1-based
//! `line:column:length` span so the parser can build the service's exact
//! `... at <l:c:len>` messages.

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
                let mut j = i;
                while j < chars.len()
                    && (chars[j].is_ascii_digit()
                        || chars[j] == '.'
                        || chars[j] == 'e'
                        || chars[j] == 'E'
                        || ((chars[j] == '+' || chars[j] == '-')
                            && j > i
                            && (chars[j - 1] == 'e' || chars[j - 1] == 'E')))
                {
                    j += 1;
                }
                let text: String = chars[i..j].iter().collect();
                let len = j - i;
                tokens.push(Token {
                    kind: TokenKind::Number(text),
                    line: start_line,
                    column: start_col,
                    length: len,
                });
                advance(&mut i, &mut line, &mut col, len, &chars);
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut j = i;
                while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                let text: String = chars[i..j].iter().collect();
                let len = j - i;
                tokens.push(Token {
                    kind: TokenKind::Ident(text),
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
