// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 13's copy of the escape that the item documents are stored
//! through (`extenddb_storage::util::escape_json_strings`): U+0000 as U+0001
//! U+0001, U+0001 as U+0001 U+0002, every other character as it is. It is
//! kept in this module so that the module's checksum covers it, and a later
//! change to the write path's escape cannot change what this migration
//! writes.

use serde_json::Value;

/// The escaped form of one string, or `None` when it holds neither
/// character.
fn escape_str(s: &str) -> Option<String> {
    if !s.bytes().any(|b| b == 0 || b == 1) {
        return None;
    }
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\u{0}' => out.push_str("\u{1}\u{1}"),
            '\u{1}' => out.push_str("\u{1}\u{2}"),
            other => out.push(other),
        }
    }
    Some(out)
}

/// The escaped form of a document: every object key and string value, at
/// every depth.
pub(crate) fn escape_document(v: Value) -> Value {
    match v {
        Value::String(s) => match escape_str(&s) {
            Some(t) => Value::String(t),
            None => Value::String(s),
        },
        Value::Array(items) => Value::Array(items.into_iter().map(escape_document).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                let key = escape_str(&k).unwrap_or(k);
                out.insert(key, escape_document(val));
            }
            Value::Object(out)
        }
        other => other,
    }
}
