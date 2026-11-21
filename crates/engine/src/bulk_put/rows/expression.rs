// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! One record's `ExpressionAttributeValues`: the per-entry decode and the
//! texts a failed entry reports.
//!
//! The map is held as raw JSON while the line parses and each entry is
//! decoded here, so a fault in one entry fails its row alone instead of
//! abandoning the file (measured on the hosted service). The record text
//! wraps the single-item text for the same
//! fault:
//!
//! ```text
//! ExpressionAttributeValues contains invalid value: <inner> for key <key>
//! ```
//!
//! The inner texts come from the code the single-item path uses: the
//! `AttributeValue` deserializer for a value rule and for an unknown type
//! tag, and the shared number validator for a number that does not
//! convert, which the deserializer stores raw and leaves to this layer.

use std::collections::HashMap;

use extenddb_core::types::AttributeValue;
use extenddb_core::validation::number::validate_and_normalize_number;
use serde_json::{Map, Value};

use super::{RowError, TYPE_TAG_MESSAGE, VALUE_RULE_PREFIX};

/// The prefix of the deserializer messages for a value that carries no type
/// tag or more than one. The single-item path reports the first of them
/// under the same wrapper this module builds, so both are passed through as
/// the inner text (neither case is measured for a record's expression map).
const SUPPLIED_VALUE_PREFIX: &str = "Supplied AttributeValue";

/// The serialized JSON length of the values map in bytes, counted without
/// holding the bytes: the map re-serialized without whitespace. The bound
/// is on the whole map, not on one value (measured on the service: two
/// values of 1,100,000 bytes are refused together), and it counts the JSON
/// text of each value, so a binary value counts its base64 text.
/// The edge sits at 2 MiB of this length.
pub(super) fn values_serialized_len(values: &Map<String, Value>) -> usize {
    serialized_len(values)
}

/// The bytes the names map adds to the combined bound on both expression
/// maps: the placeholder and the attribute name of each entry, without
/// JSON punctuation. Measured on the service beside a `{"#v":"v"}` map: a
/// values map of 2,097,152 bytes fails the row with the combined-size text
///  and one of 2,097,148 bytes fails it with the expression-size
/// text instead, so the map adds between 1 and 4 bytes; beside a
/// `{"#vvvvvvvvv":"v"}` map a values map of 2,097,140 bytes is likewise not
/// the combined class, so that map adds at most 12. The raw
/// bytes (3 and 11) fit both; the compact JSON text (10 and 18) fits
/// neither.
pub(super) fn names_combined_len(names: &HashMap<String, String>) -> usize {
    names
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum()
}

/// The bytes of the values themselves, as the expression-size bound counts
/// them. Measured on the service with the 35-byte condition
/// `attribute_not_exists(pk) OR #v = :v` at the 1 MiB edge: an `S` value
/// counts its bytes; a `B` value counts its decoded bytes, not its base64
/// text (text of 1,048,544 characters for 786,408 raw bytes was evaluated;
/// 1,048,541 raw bytes was refused); an `M` counts each attribute name as
/// well as its value (a one-byte key is the byte that reaches the edge);
/// the members of an `L` and of an `SS` sum. Type
/// tags and JSON punctuation never count. `N`, `NS` and `BS` were not
/// measured at the edge (a number cannot reach it) and follow the same
/// rule: digits as text, binary members as decoded bytes. The values are
/// decoded before the bound is judged, so a binary text that does
/// not decode has already failed the row by then and counts nothing here.
pub(super) fn values_content_len(values: &Map<String, Value>) -> usize {
    use base64::Engine as _;
    fn decoded_len(text: &str) -> usize {
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_or(0, |bytes| bytes.len())
    }
    fn typed(value: &Value) -> usize {
        let Some(map) = value.as_object() else {
            return 0;
        };
        let Some((tag, inner)) = map.iter().next() else {
            return 0;
        };
        match (tag.as_str(), inner) {
            ("S" | "N", Value::String(s)) => s.len(),
            ("B", Value::String(s)) => decoded_len(s),
            ("SS" | "NS", Value::Array(items)) => {
                items.iter().filter_map(Value::as_str).map(str::len).sum()
            }
            ("BS", Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(decoded_len)
                .sum(),
            ("L", Value::Array(items)) => items.iter().map(typed).sum(),
            ("M", Value::Object(entries)) => entries
                .iter()
                .map(|(name, value)| name.len() + typed(value))
                .sum(),
            _ => 0,
        }
    }
    values.values().map(typed).sum()
}

fn serialized_len<T: serde::Serialize>(value: &T) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    // The writer cannot fail and a map of JSON values always serializes.
    if serde_json::to_writer(&mut counter, value).is_err() {
        return 0;
    }
    counter.0
}

/// Decode every entry of `ExpressionAttributeValues`.
///
/// Entries are visited in key order (the raw map is sorted), so a record
/// with two bad entries reports the lower key, as the single-item number
/// check does for the same map.
///
/// # Errors
///
/// Returns [`RowError::Item`] naming the first entry that fails, with the
/// wrapped text; the file continues at the next row.
pub(super) fn decode_values(
    raw: Map<String, Value>,
) -> Result<HashMap<String, AttributeValue>, RowError> {
    let mut decoded = HashMap::with_capacity(raw.len());
    for (key, value) in raw {
        let value = decode_value(&key, value)?;
        decoded.insert(key, value);
    }
    Ok(decoded)
}

/// Decode one entry: the deserializer first, then the number conversion it
/// defers.
fn decode_value(key: &str, value: Value) -> Result<AttributeValue, RowError> {
    let decoded: AttributeValue = serde_json::from_value(value).map_err(|error| {
        let message = error.to_string();
        if message.starts_with(VALUE_RULE_PREFIX) || message.starts_with(SUPPLIED_VALUE_PREFIX) {
            fault(key, &message)
        } else {
            // Every other deserializer message is a value whose type tag
            // the service does not know; its text names the tags it takes
            // (measured).
            fault(key, TYPE_TAG_MESSAGE)
        }
    })?;
    check_numbers(&decoded).map_err(|message| fault(key, &message))?;
    Ok(decoded)
}

/// The record text for a failed entry: the single-item text for the fault,
/// wrapped in the map's own message, naming the entry's key.
fn fault(key: &str, inner: &str) -> RowError {
    RowError::Item(format!(
        "ExpressionAttributeValues contains invalid value: {inner} for key {key}"
    ))
}

/// Whether every number in a decoded value converts, through the shared
/// number validator so the text is the one the single-item path answers
/// (`The parameter cannot be converted to a numeric value: <input>`,
/// measured for a top-level `N`). The walk reaches `NS` members
/// and numbers inside `L` and `M`, where the validator the single-item path
/// runs reaches them; those positions are unmeasured for a record.
fn check_numbers(value: &AttributeValue) -> Result<(), String> {
    match value {
        AttributeValue::N(number) => convert(number),
        AttributeValue::NS(set) => set.iter().try_for_each(|number| convert(number)),
        AttributeValue::L(list) => list.iter().try_for_each(check_numbers),
        AttributeValue::M(map) => map.values().try_for_each(check_numbers),
        _ => Ok(()),
    }
}

fn convert(number: &str) -> Result<(), String> {
    validate_and_normalize_number(number)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
