// SPDX-License-Identifier: Apache-2.0

//! Escape for the two characters a storage engine may not be able to hold
//! verbatim in a document.
//!
//! DynamoDB accepts the character U+0000 anywhere a string appears: partition
//! and sort keys, index keys, attribute values, map keys, attribute names. Two
//! of the storage engines cannot store it as is in a document. PostgreSQL
//! `jsonb` rejects the `\u0000` escape, and BSON field names are C strings and
//! end at the first NUL. PostgreSQL `TEXT` rejects the byte too (`invalid byte
//! sequence for encoding "UTF8": 0x00`); the PostgreSQL key columns are not
//! escaped, and that backend refuses a key value that holds U+0000.
//!
//! The escape maps each affected character to a two-character sequence that
//! contains no NUL:
//!
//! | input  | output        |
//! |--------|---------------|
//! | U+0000 | U+0001 U+0001 |
//! | U+0001 | U+0001 U+0002 |
//!
//! Every other character passes through unchanged, so a string without either
//! character encodes to itself and the stored form of ordinary data does not
//! change. The mapping is applied per character, so `unescape(escape(s)) == s`
//! for every `s` and two different strings never escape to the same text; the
//! tests below pin both.
//!
//! Decoding is lenient about a U+0001 that is not followed by U+0001 or U+0002:
//! it is returned as a literal U+0001. A string written before this escape
//! existed that contained U+0001 immediately followed by U+0001 or U+0002
//! would decode differently. On PostgreSQL, catalog migration 13,
//! `escape_control_chars`, re-encodes such documents so that case cannot arise
//! after it has run; on MongoDB only a document marked as escaped is decoded.

use std::borrow::Cow;

const ESC: char = '\u{1}';
const NUL_TAIL: char = '\u{1}';
const ESC_TAIL: char = '\u{2}';

/// Whether `s` contains a character the escape changes.
#[must_use]
pub fn needs_escape(s: &str) -> bool {
    s.bytes().any(|b| b == 0 || b == 1)
}

/// Escape U+0000 and U+0001 in `s`. Borrows when there is nothing to change.
#[must_use]
pub fn escape_control(s: &str) -> Cow<'_, str> {
    if !needs_escape(s) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\u{0}' => {
                out.push(ESC);
                out.push(NUL_TAIL);
            }
            '\u{1}' => {
                out.push(ESC);
                out.push(ESC_TAIL);
            }
            other => out.push(other),
        }
    }
    Cow::Owned(out)
}

/// Reverse [`escape_control`]. Borrows when there is nothing to change. A
/// U+0001 not followed by U+0001 or U+0002 is kept as a literal U+0001 (see
/// the module documentation for why).
#[must_use]
pub fn unescape_control(s: &str) -> Cow<'_, str> {
    if !s.bytes().any(|b| b == 1) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == ESC {
            match chars.peek() {
                Some(&NUL_TAIL) => {
                    chars.next();
                    out.push('\u{0}');
                }
                Some(&ESC_TAIL) => {
                    chars.next();
                    out.push('\u{1}');
                }
                _ => out.push('\u{1}'),
            }
        } else {
            out.push(ch);
        }
    }
    Cow::Owned(out)
}

/// Apply [`escape_control`] to every string in a JSON tree: object keys and
/// string values, at every depth. Used where the whole document lands in a
/// store that rejects U+0000 in any string (PostgreSQL `jsonb`).
#[must_use]
pub fn escape_json_strings(v: serde_json::Value) -> serde_json::Value {
    map_json(v, true, &|s| escape_control(s).into_owned())
}

/// Reverse [`escape_json_strings`].
#[must_use]
pub fn unescape_json_strings(v: serde_json::Value) -> serde_json::Value {
    map_json(v, true, &|s| unescape_control(s).into_owned())
}

/// Apply [`escape_control`] to every object key in a JSON tree, leaving string
/// values alone. Used where only field names are restricted (BSON).
#[must_use]
pub fn escape_json_keys(v: serde_json::Value) -> serde_json::Value {
    map_json(v, false, &|s| escape_control(s).into_owned())
}

/// Reverse [`escape_json_keys`].
#[must_use]
pub fn unescape_json_keys(v: serde_json::Value) -> serde_json::Value {
    map_json(v, false, &|s| unescape_control(s).into_owned())
}

fn map_json(
    v: serde_json::Value,
    values_too: bool,
    f: &dyn Fn(&str) -> String,
) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::String(s) if values_too => {
            if needs_escape(&s) {
                Value::String(f(&s))
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| map_json(item, values_too, f))
                .collect(),
        ),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                let key = if needs_escape(&k) { f(&k) } else { k };
                out.insert(key, map_json(val, values_too, f));
            }
            Value::Object(out)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every string over the alphabet {NUL, U+0001, U+0002, 'a'} up to the
    /// given length. Small alphabet, exhaustive: the escape only looks at
    /// these three code points and everything else is identity.
    fn corpus(max_len: usize) -> Vec<String> {
        let alphabet = ['\u{0}', '\u{1}', '\u{2}', 'a'];
        let mut out = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..max_len {
            let mut next = Vec::new();
            for s in &frontier {
                for ch in alphabet {
                    let mut t = s.clone();
                    t.push(ch);
                    next.push(t);
                }
            }
            out.extend(next.iter().cloned());
            frontier = next;
        }
        out
    }

    #[test]
    fn plain_strings_are_borrowed_unchanged() {
        for s in ["", "a", "hello", "\u{2}\u{3}", "\u{e9}\u{4e2d}", "a\tb\nc"] {
            assert!(matches!(escape_control(s), Cow::Borrowed(_)), "{s:?}");
            assert!(matches!(unescape_control(s), Cow::Borrowed(_)), "{s:?}");
            assert_eq!(escape_control(s), s);
            assert_eq!(unescape_control(s), s);
        }
    }

    #[test]
    fn table_of_the_two_mappings() {
        assert_eq!(escape_control("\u{0}"), "\u{1}\u{1}");
        assert_eq!(escape_control("\u{1}"), "\u{1}\u{2}");
        assert_eq!(escape_control("a\u{0}b"), "a\u{1}\u{1}b");
        assert_eq!(escape_control("\u{0}\u{1}"), "\u{1}\u{1}\u{1}\u{2}");
        assert_eq!(unescape_control("\u{1}\u{1}"), "\u{0}");
        assert_eq!(unescape_control("\u{1}\u{2}"), "\u{1}");
    }

    #[test]
    fn escaped_output_never_contains_nul() {
        for s in corpus(5) {
            assert!(!escape_control(&s).contains('\u{0}'), "{s:?}");
        }
    }

    #[test]
    fn round_trips_every_string_in_the_corpus() {
        for s in corpus(5) {
            assert_eq!(unescape_control(&escape_control(&s)), s, "{s:?}");
        }
        for s in [
            "caf\u{e9}\u{0}\u{4e2d}",
            "\u{0}\u{0}\u{0}",
            "\u{1}\u{1}\u{1}",
            "x\u{1}\u{0}\u{2}y",
        ] {
            assert_eq!(unescape_control(&escape_control(s)), s, "{s:?}");
        }
    }

    #[test]
    fn escaped_forms_are_distinct_for_distinct_inputs() {
        let corpus = corpus(4);
        let mut seen = std::collections::HashMap::new();
        for s in &corpus {
            let e = escape_control(s).into_owned();
            if let Some(prev) = seen.insert(e.clone(), s.clone()) {
                panic!("collision: {prev:?} and {s:?} both escape to {e:?}");
            }
        }
    }

    #[test]
    fn lenient_decode_keeps_a_stray_escape_character_literal() {
        // A row written before the escape existed can carry a raw U+0001
        // followed by anything other than U+0001 or U+0002.
        assert_eq!(unescape_control("a\u{1}b"), "a\u{1}b");
        assert_eq!(unescape_control("\u{1}"), "\u{1}");
        assert_eq!(unescape_control("\u{1}\u{3}"), "\u{1}\u{3}");
        assert_eq!(unescape_control("x\u{1}"), "x\u{1}");
    }

    #[test]
    fn json_strings_variant_touches_keys_and_values_at_every_depth() {
        let item = json!({
            "a\u{0}b": {"S": "v\u{0}"},
            "m": {"M": {"k\u{1}": {"S": "\u{0}"}, "plain": {"N": "1"}}},
            "l": {"L": [{"S": "\u{0}x"}, {"SS": ["\u{0}", "y"]}]},
            "b": {"B": "AAEC"}
        });
        let escaped = escape_json_strings(item.clone());
        assert!(!escaped.to_string().contains("\\u0000"), "{escaped}");
        assert_eq!(escaped["a\u{1}\u{1}b"]["S"], json!("v\u{1}\u{1}"));
        assert_eq!(escaped["m"]["M"]["k\u{1}\u{2}"]["S"], json!("\u{1}\u{1}"));
        assert_eq!(escaped["l"]["L"][1]["SS"][0], json!("\u{1}\u{1}"));
        assert_eq!(escaped["b"]["B"], json!("AAEC"));
        assert_eq!(unescape_json_strings(escaped), item);
    }

    #[test]
    fn json_keys_variant_leaves_values_alone() {
        let item = json!({
            "a\u{0}b": {"S": "v\u{0}"},
            "m": {"M": {"k\u{0}": {"L": [{"S": "\u{0}"}]}}}
        });
        let escaped = escape_json_keys(item.clone());
        assert_eq!(escaped["a\u{1}\u{1}b"]["S"], json!("v\u{0}"));
        assert_eq!(
            escaped["m"]["M"]["k\u{1}\u{1}"]["L"][0]["S"],
            json!("\u{0}")
        );
        assert_eq!(unescape_json_keys(escaped), item);
    }

    #[test]
    fn json_without_control_characters_is_unchanged() {
        let item = json!({"pk": {"S": "p"}, "n": {"N": "1"}, "m": {"M": {"k": {"S": "v"}}}});
        assert_eq!(escape_json_strings(item.clone()), item);
        assert_eq!(escape_json_keys(item.clone()), item);
        assert_eq!(unescape_json_strings(item.clone()), item);
        assert_eq!(unescape_json_keys(item.clone()), item);
    }
}
