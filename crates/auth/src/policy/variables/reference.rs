// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM's grammar for a `${...}` policy variable reference, as a policy write
//! checks it and as evaluation reads a value the write accepts.
//!
//! Measured on IAM policy writes and with the IAM policy simulator on
//! 2026-11-09, two samples per request shape (issue #834):
//!
//! - After `${`, Java whitespace (space, tab, newline, U+000B, U+000C,
//!   U+000D) is skipped and the next character belongs to the reference text
//!   whatever it is, `}` included. From there each `'` opens or closes a
//!   quoted span, and the first `}` outside a quoted span closes the
//!   reference. So `a${}}`, `a${ }}` and `a${}x}` are closed references, and
//!   `a${}`, `a${ }`, `a${'}}` and `a${aws:username, '}'` are not closed.
//! - The text is split at each `,` outside a quoted span. One part: after
//!   removing Java whitespace at both ends, an escape (`*`, `?`, `$`) or a key
//!   name. Two parts: a key name trimmed of every code point up to U+0020,
//!   then a default trimmed of Java whitespace that is one single-quoted
//!   string in which `'` appears only doubled. A key name holds no `'` and no
//!   Java whitespace; in the two-part form it also holds no `$` unless it is
//!   the escape `$`. Three or more parts are refused.
//! - At evaluation an escape decides before a default: `${*, 'x'}` is a
//!   literal `*`. A doubled `''` in a default is one `'`. A reference whose
//!   text starts with `}` names no key the request carries and its default is
//!   not used, so `a${}, 'x'}` matches nothing (as `StringEquals` on a tag
//!   `ax` and as `Resource` `table/${}, 'x'}` on `table/x`), the same reading
//!   as before the grammar.

use super::{Part, Reference, trim_up_to_space};

fn is_java_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{0b}' | '\u{0c}')
}

fn java_trim(text: &str) -> &str {
    text.trim_matches(is_java_whitespace)
}

/// The byte offset in `after_start`, the text after a `${`, of the `}` that
/// closes the reference, or `None` when nothing closes it.
pub(in crate::policy) fn reference_end(after_start: &str) -> Option<usize> {
    let skipped = after_start.len() - after_start.trim_start_matches(is_java_whitespace).len();
    let first = after_start[skipped..].chars().next()?;
    let mut quoted = first == '\'';
    let from = skipped + first.len_utf8();
    for (i, b) in after_start[from..].bytes().enumerate() {
        match b {
            b'\'' => quoted = !quoted,
            b'}' if !quoted => return Some(from + i),
            _ => {}
        }
    }
    None
}

/// The parts of reference text `inner`, split at each `,` outside a quoted
/// span.
fn fields(inner: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut quoted = false;
    let mut start = 0;
    for (i, b) in inner.bytes().enumerate() {
        match b {
            b'\'' => quoted = !quoted,
            b',' if !quoted => {
                out.push(&inner[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&inner[start..]);
    out
}

fn is_escape(text: &str) -> bool {
    matches!(text, "*" | "?" | "$")
}

fn is_name(name: &str) -> bool {
    !name.is_empty() && !name.chars().any(|c| c == '\'' || is_java_whitespace(c))
}

/// The text inside `quoted` when it is one single-quoted string in which a
/// `'` appears only doubled.
fn quoted_default(quoted: &str) -> Option<&str> {
    let inner = quoted.strip_prefix('\'')?.strip_suffix('\'')?;
    let mut rest = inner;
    while let Some(i) = rest.find('\'') {
        rest = rest[i + 1..].strip_prefix('\'')?;
    }
    Some(inner)
}

/// Whether the text between `${` and its closing `}` is a reference IAM
/// accepts at write.
pub(in crate::policy) fn is_well_formed(inner: &str) -> bool {
    if is_escape(java_trim(inner)) {
        return true;
    }
    match fields(inner).as_slice() {
        [name] => is_name(java_trim(name)),
        [name, default] => {
            let name = trim_up_to_space(name);
            is_name(name)
                && (name == "$" || !name.contains('$'))
                && quoted_default(java_trim(default)).is_some()
        }
        _ => false,
    }
}

/// Whether `value` holds a `${...}` reference IAM refuses at write: one that
/// is not closed, or one whose text is not well formed.
#[must_use]
pub fn has_invalid_reference(value: &str) -> bool {
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = reference_end(after) else {
            return true;
        };
        if !is_well_formed(&after[..end]) {
            return true;
        }
        rest = &after[end + 1..];
    }
    false
}

/// Read the text of a reference that [`is_well_formed`] accepts.
pub(super) fn parse(inner: &str) -> Reference<'_> {
    let escape = |text: &str| Reference::Escape(text.as_bytes()[0]);
    if java_trim(inner).starts_with('}') {
        return Reference::Key {
            name: trim_up_to_space(inner),
            default: None,
            doubled_quotes: true,
        };
    }
    match fields(inner).as_slice() {
        [name, default] => {
            let name = trim_up_to_space(name);
            if is_escape(name) {
                return escape(name);
            }
            Reference::Key {
                name,
                default: quoted_default(java_trim(default)),
                doubled_quotes: true,
            }
        }
        _ => {
            let name = trim_up_to_space(inner);
            if is_escape(name) {
                return escape(name);
            }
            Reference::Key {
                name,
                default: None,
                doubled_quotes: true,
            }
        }
    }
}

/// Parse the text between `${` and `}`.
///
/// The text is trimmed with [`trim_up_to_space`](super::trim_up_to_space) first. Then `*`, `?` and
/// `$` are the escapes, and `name, 'default'` is the default form, where the
/// name and the quoted default are trimmed again and the text inside the
/// quotes is kept as written. Anything else, including a `,` whose right side
/// is not single-quoted, is taken as the key name.
///
/// This is the reading of a value that holds a reference a policy write
/// refuses ([`has_invalid_reference`]), which only a stored
/// document can hold; it is unchanged so such a document evaluates as before.
pub(super) fn legacy(inner: &str) -> Reference<'_> {
    let inner = trim_up_to_space(inner);
    match inner {
        "*" => return Reference::Escape(b'*'),
        "?" => return Reference::Escape(b'?'),
        "$" => return Reference::Escape(b'$'),
        _ => {}
    }
    if let Some((name, default)) = inner.split_once(',') {
        let default = trim_up_to_space(default);
        if default.len() >= 2 && default.starts_with('\'') && default.ends_with('\'') {
            return Reference::Key {
                name: trim_up_to_space(name),
                default: Some(&default[1..default.len() - 1]),
                doubled_quotes: false,
            };
        }
    }
    Reference::Key {
        name: inner,
        default: None,
        doubled_quotes: false,
    }
}

/// Split `value` into text and references, ending each reference with
/// `end_of` and reading it with `read`.
pub(super) fn split<'a>(
    value: &'a str,
    end_of: impl Fn(&str) -> Option<usize>,
    read: impl Fn(&'a str) -> Reference<'a>,
) -> Vec<Part<'a>> {
    let mut out = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push(Part::Text(&rest[..start]));
        let after_start = &rest[start + 2..];
        match end_of(after_start) {
            Some(end) => {
                let raw = &rest[start..start + 3 + end];
                out.push(Part::Ref(raw, read(&after_start[..end])));
                rest = &after_start[end + 1..];
            }
            None => {
                out.push(Part::Text(&rest[start..]));
                rest = "";
            }
        }
    }
    out.push(Part::Text(rest));
    out
}
