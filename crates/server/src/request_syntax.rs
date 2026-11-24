// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The JSON syntax check a request body gets before authentication.
//!
//! [`skip_value`] reads past a body's first JSON value without building it
//! and without recursing, with the grammar the service applies before
//! authentication: JSON's, except in the three lexemes where the service is
//! looser (whitespace, numbers and the four bytes after `\u`; each is
//! measured in the doc comments below). `request_parse::check_request_syntax`
//! turns its answer into the server's refusal.

/// Whether `b` can be part of a number as the service reads one. The service
/// takes a run of these bytes that starts with a digit or `-` as one number.
pub(crate) fn number_byte(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
}

/// A cursor over a request body, for [`skip_value`].
struct Scan<'a> {
    body: &'a [u8],
    at: usize,
}

impl Scan<'_> {
    /// The byte at the cursor after any whitespace, or `None` at the end of
    /// the body. Besides JSON's space, TAB, LF and CR, the service skips NUL,
    /// VT and FF between tokens and refuses SOH there (measured 2026-11-24,
    /// us-east-1: unsigned, a 200 wrapper `PutItem` with one of the three
    /// before a value is a `MissingAuthenticationTokenException`, and with
    /// SOH there a `SerializationException`).
    fn token(&mut self) -> Option<u8> {
        while let Some(&b) = self.body.get(self.at) {
            if !matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x00 | 0x0b | 0x0c) {
                return Some(b);
            }
            self.at += 1;
        }
        None
    }

    /// The byte at the cursor, which moves past it.
    fn next(&mut self) -> Option<u8> {
        let b = *self.body.get(self.at)?;
        self.at += 1;
        Some(b)
    }

    /// Skip a string literal from after its opening quote to after its
    /// closing quote. As in JSON, a raw control byte, or an escape letter
    /// other than `"`, `\`, `/`, `b`, `f`, `n`, `r`, `t` and `u`, is a fault.
    /// The four bytes after `\u` are not read as hex digits: the service
    /// takes any byte from `0` to `g` (0x30 to 0x67) there and refuses the
    /// others (measured 2026-11-24, us-east-1, unsigned, 200 wrappers:
    /// `\u12G4`, `\u::::`, `\u@@@@`, `\u` and four backslashes, `\u` and four
    /// backticks, `\uffff` and `\ugggg` are a
    /// `MissingAuthenticationTokenException`; `\u/123`, `\u12.4`, `\u12"x`,
    /// `\uhhhh`, `\uzzzz`, `\u~~~~`, `\u` and four SOH bytes, and `\u` and four
    /// bytes above 0x7F are a `SerializationException`).
    fn skip_string(&mut self) -> Result<(), &'static str> {
        loop {
            match self.next().ok_or("unterminated string")? {
                b'"' => return Ok(()),
                b'\\' => match self.next().ok_or("unterminated string")? {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                    b'u' => {
                        for _ in 0..4 {
                            if !matches!(self.next(), Some(b'0'..=b'g')) {
                                return Err("invalid unicode escape");
                            }
                        }
                    }
                    _ => return Err("invalid escape"),
                },
                0x00..=0x1f => return Err("control character in string"),
                _ => {}
            }
        }
    }

    /// Skip an object key and the colon after it.
    fn skip_key(&mut self) -> Result<(), &'static str> {
        if self.token() != Some(b'"') {
            return Err("key must be a string");
        }
        self.at += 1;
        self.skip_string()?;
        if self.token() != Some(b':') {
            return Err("expected `:`");
        }
        self.at += 1;
        Ok(())
    }

    /// Skip `word`, which must start at the cursor.
    fn skip_literal(&mut self, word: &[u8]) -> Result<(), &'static str> {
        if !self.body[self.at..].starts_with(word) {
            return Err("invalid literal");
        }
        self.at += word.len();
        Ok(())
    }
}

/// Read past the first JSON value of `body` without building it. The grammar
/// is JSON's, as `serde_json` applies it, except in the three lexemes where
/// the service's check before authentication is looser: whitespace also takes
/// NUL, VT and FF ([`Scan::token`]); a number is `-` or a digit and then any
/// run of [`number_byte`]s, so `01`, `1.`, `1e` and `-` pass (measured
/// 2026-11-24, us-east-1, at 63 and 20000 wrappers: unsigned, each is a
/// `MissingAuthenticationTokenException`); and the four bytes after `\u` may
/// be any byte from `0` to `g` ([`Scan::skip_string`]). Bytes after the value
/// are not read. The closers of the open containers sit on a heap stack, one
/// byte per level, so the loop never recurses.
pub(crate) fn skip_value(body: &[u8]) -> Result<(), &'static str> {
    let mut scan = Scan { body, at: 0 };
    let mut open: Vec<u8> = Vec::new();
    'value: loop {
        match scan.token().ok_or("expected value")? {
            opener @ (b'{' | b'[') => {
                scan.at += 1;
                let closer = if opener == b'{' { b'}' } else { b']' };
                if scan.token() == Some(closer) {
                    scan.at += 1;
                } else {
                    open.push(closer);
                    if opener == b'{' {
                        scan.skip_key()?;
                    }
                    continue 'value;
                }
            }
            b'"' => {
                scan.at += 1;
                scan.skip_string()?;
            }
            b'-' | b'0'..=b'9' => {
                scan.at += 1;
                while scan.body.get(scan.at).is_some_and(|&b| number_byte(b)) {
                    scan.at += 1;
                }
            }
            b't' => scan.skip_literal(b"true")?,
            b'f' => scan.skip_literal(b"false")?,
            b'n' => scan.skip_literal(b"null")?,
            _ => return Err("expected value"),
        }
        // The value is complete: close containers until a comma starts the
        // next member, or the outermost container closes.
        loop {
            let Some(&closer) = open.last() else {
                return Ok(());
            };
            match scan.token() {
                Some(b',') => {
                    scan.at += 1;
                    if closer == b'}' {
                        scan.skip_key()?;
                    }
                    continue 'value;
                }
                Some(b) if b == closer => {
                    scan.at += 1;
                    open.pop();
                }
                Some(_) => return Err("expected `,` or the end of the container"),
                None => return Err("unterminated container"),
            }
        }
    }
}
