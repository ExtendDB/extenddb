// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Input row parsing for bulk put operations.
//!
//! Each input file is newline-delimited JSON, one object per line, with a
//! required `Item` and optional `ConditionExpression`,
//! `ExpressionAttributeNames`, and `ExpressionAttributeValues`. Item
//! deserialization is the same `Item` (and through it `AttributeValue`)
//! `Deserialize` path that `import_export_io::read_dynamodb_json` relies
//! on, so a bad type tag is rejected identically in both.
//!
//! A row failure falls into one of two classes, which the runner treats
//! differently:
//!
//! - [`RowError::Parse`] is a parse-level fault: malformed or truncated
//!   JSON, a missing or non-object `Item`, an attribute value that fails
//!   to decode (an unknown type tag, a binary value that is not base64),
//!   a line over the byte cap, or a condition field of the wrong JSON
//!   type. It abandons the rest of the file, so its message says so.
//! - [`RowError::Item`] is an item-level fault the parser can see before
//!   the runner does: a decoded attribute value that fails validation (an
//!   empty set, a `NULL` that is not `true`), a string or attribute name
//!   containing U+0000, an `ExpressionAttributeValues` entry that fails to
//!   decode, or expression maps supplied without a `ConditionExpression`.
//!   It fails the row alone and the file continues, carrying the `PutItem`
//!   message for the same fault. A repeated set member is not a fault on
//!   this path: the set is stored deduplicated, and the item size counts
//!   the set as submitted, repeated members included (both measured on the
//!   service).
//!
//! The line between the two classes for attribute values: a value of the
//! wrong JSON shape (no type tag, more than one tag, an unknown tag, a set
//! that is not an array, an `N` that is not a number, a binary value that
//! is not base64) did not decode and is parse-level, several of them with
//! their own measured message; a value of the right shape that failed a
//! DynamoDB value rule (an empty set, measured on the service as item-level
//! with the `PutItem` text; a `NULL` that is not `true`) decoded and is
//! item-level. The second class is exactly the set of deserializer
//! messages that begin with the `PutItem` prefix [`VALUE_RULE_PREFIX`],
//! plus the repeated attribute name the deserializer cannot see.
//!
//! Item-level checks the parser cannot make (key presence and types, item
//! size, nesting depth, condition-expression syntax) stay the runner's,
//! through the `PutItem` validators; the condition strings are handed to
//! the `PutItem` parsing functions unexamined.

mod expression;
mod messages;
mod scan;

pub(crate) use messages::*;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use base64::Engine as _;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, Item, attribute_value_size};
use extenddb_core::validation::number::validate_and_normalize_number;
use serde::Deserialize;

/// The condition attached to one input row, carried verbatim for the
/// runner to hand to the `PutItem` parsing functions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowCondition {
    pub expression: String,
    pub names: Option<HashMap<String, String>>,
    pub values: Option<HashMap<String, AttributeValue>>,
}

/// One parsed input row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Row {
    pub item: Item,
    pub condition: Option<RowCondition>,
    /// The item size of the set members the parser removed as repeats. The
    /// item holds one copy of each member; the size check counts the set as
    /// submitted (see [`Row::item_limits`]).
    pub repeated_member_bytes: usize,
}

impl Row {
    /// The limits the item checks run against. The item size counts a set
    /// as submitted, repeated members included, though the set is stored
    /// with one copy of each member (measured on the service: an `SS` of two
    /// identical 250,000-byte strings fails the row with the item size
    /// message). The item holds one copy, so the bytes of the removed copies
    /// come off the item size limit. That is the same comparison as adding
    /// them to the item's size, and it is made by the `PutItem` validator,
    /// so the size check keeps its place in the order of the item checks.
    pub(crate) fn item_limits<'a>(&self, limits: &'a LimitsConfig) -> Cow<'a, LimitsConfig> {
        if self.repeated_member_bytes == 0 {
            return Cow::Borrowed(limits);
        }
        Cow::Owned(LimitsConfig {
            max_item_size_bytes: limits
                .max_item_size_bytes
                .saturating_sub(self.repeated_member_bytes),
            ..limits.clone()
        })
    }
}

/// A row failure, in one of two classes the runner treats differently.
///
/// - `Parse` abandons the remainder of the file; its string is
///   [`PARSE_ABANDON_MESSAGE`], or the dedicated message for a missing or
///   non-object `Item`.
/// - `Item` fails the row alone and the file continues; its string is the
///   `PutItem` message for the fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowError {
    Parse(String),
    Item(String),
}

/// The wire shape of one input line. A top-level key outside the four is
/// refused before this shape is read (see [`UNKNOWN_FIELD_MESSAGE`]).
/// `Item` is held as a raw JSON value so a missing one and a non-object one
/// answer their own messages (measured); the attribute values are
/// deserialized in a second step, whose failure keeps the shared abandon
/// message. `ExpressionAttributeValues` is held as a raw JSON object for
/// the same reason: a fault in one of its entries fails the row alone
/// (measured on the service), so its entries are decoded
/// after the line parses, by [`expression::decode_values`]. A values field
/// that is not a JSON object still fails here.
#[derive(Deserialize)]
struct RawRow {
    #[serde(rename = "Item")]
    item: Option<serde_json::Value>,
    #[serde(rename = "ConditionExpression")]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames")]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues")]
    expression_attribute_values: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Parse one input line.
///
/// Returns `Ok(None)` for a blank or whitespace-only line, which is
/// skipped without touching any counter.
///
/// Returns [`RowError::Parse`] for every parse-level cause: a line longer
/// than `max_line_bytes`, JSON that does not parse or is truncated, a
/// missing or non-object `Item`, a record member given twice, an
/// `ExpressionAttributeValues` map over [`MAX_EXPRESSION_VALUES_BYTES`]
/// (each with its own measured message), an attribute value that fails to
/// decode (unknown type tag, more than one tag, an empty value, an `N`
/// that is not a number, a set tag that is not an array, a binary value
/// that is not base64), or a condition field of the wrong JSON type (a
/// non-string `ConditionExpression`, a non-object names or values map).
/// The tree has no `PutItem` message for a malformed expression map, only
/// for a missing or unreferenced one, so a wrong-type field is classified
/// parse-level.
///
/// An empty-string `ConditionExpression` is treated as absent, mirroring
/// `handle_put_item` (`is_some_and(|s| !s.is_empty())`): a bare empty
/// expression parses unconditional, and an empty expression alongside a
/// non-empty names or values map fails item-level exactly as a missing
/// one does.
///
/// Returns [`RowError::Item`] when an attribute value decoded and failed a
/// validation rule (an empty string, number or binary set; a duplicate set
/// member), carrying the `PutItem` message for it; when an attribute name
/// is repeated in the same attribute map (measured on the service); when
/// an attribute name
/// or string value contains U+0000 ([`NUL_IN_STRING_MESSAGE`]); when an
/// `ExpressionAttributeValues` entry fails to decode or carries a number
/// that does not convert, carrying that fault's single-item text wrapped in
/// the map's own message (measured on the service); and when
/// `ExpressionAttributeNames` or `ExpressionAttributeValues` is supplied
/// with no `ConditionExpression` that could reference them. The parser can
/// see these faults, and each fails the row alone rather than abandoning
/// the file. For the expression maps the message is the one
/// `validate_expression_param_usage` produces for the same case; names
/// wins when both are present, matching that function's order. An empty
/// map supplied WITH a `ConditionExpression` fails the row with
/// [`NAMES_EMPTY_MESSAGE`] or [`VALUES_EMPTY_MESSAGE`] (measured on the
/// service); an empty map with no expression is exempt,
/// matching `validate_expression_param_usage`. The two maps serializing
/// together over [`MAX_EXPRESSION_MAPS_BYTES`] fail the row with
/// [`MAPS_TOO_LARGE_MESSAGE`] (measured on the service, which also fixes
/// the names map's contribution).
///
/// A row missing a key attribute or exceeding the item size limit parses
/// here; those faults are item-level and are the runner's, through the
/// `PutItem` validators.
pub(crate) fn parse_row(line: &str, max_line_bytes: usize) -> Result<Option<Row>, RowError> {
    if line.len() > max_line_bytes {
        return Err(parse_error());
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    // An attribute value nested at or beyond the parse bound abandons the
    // file with the long nesting message (measured on the service: 63 levels is
    // an item-level fault, 64 abandons). The bound is checked on the raw
    // text before parsing: the JSON parser's own recursion limit (128) sits
    // below the bracket depth of 63 attribute levels, so parsing runs with
    // that limit lifted, and this scan is what bounds the recursion.
    if bracket_depth(trimmed.as_bytes()) >= MAX_PARSE_BRACKET_DEPTH {
        return Err(RowError::Parse(NESTING_ABANDON_MESSAGE.to_owned()));
    }
    // A line that is not JSON at all answers the tokenizer's message, not
    // the type-tag one (measured on the service with plain text and with a
    // truncated object; the September type-tag measurement was for an
    // unknown tag inside well-formed JSON). A complete object followed by
    // more text on the line is the same class.
    let mut deserializer = serde_json::Deserializer::from_str(trimmed);
    deserializer.disable_recursion_limit();
    // The line is read into a tree that keeps a repeated object member
    // rather than collapsing it, so the item's structure can be judged
    // before its values are decoded.
    let node = scan::Node::deserialize(&mut deserializer)
        .and_then(|node| deserializer.end().map(|()| node))
        .map_err(|_| RowError::Parse(CORRUPT_STREAM_MESSAGE.to_owned()))?;
    let value = node.to_value();
    // A line that parses as JSON but is not a record object, an array
    // wrapping a record among them, answers the tokenizer's message
    // (measured on the service, on the array form; the other
    // non-object forms were not measured and are read the same way).
    if !value.is_object() {
        return Err(RowError::Parse(CORRUPT_STREAM_MESSAGE.to_owned()));
    }
    // A top-level key outside the record shape abandons the file, and the
    // check runs before the missing-`Item` one: a record whose `Item` is
    // misspelled answers this message (measured on the service).
    if let Some(object) = value.as_object()
        && object
            .keys()
            .any(|key| !RECORD_KEYS.contains(&key.as_str()))
    {
        return Err(RowError::Parse(UNKNOWN_FIELD_MESSAGE.to_owned()));
    }
    // A top-level member given twice abandons the file (measured
    // ). Its precedence against the
    // unknown-field check was not measured; the unknown-field check keeps
    // the position its own measurement gave it.
    scan::check_record_members(&node)?;
    // Well-formed JSON of the wrong shape (a condition field of the wrong
    // JSON type) keeps the shared abandon message (ExtendDB's choice;
    // unmeasured).
    let raw: RawRow = serde_json::from_value(value).map_err(|_| parse_error())?;
    let Some(item_value) = raw.item else {
        return Err(RowError::Parse(ITEM_MISSING_MESSAGE.to_owned()));
    };
    if !item_value.is_object() {
        return Err(RowError::Parse(ITEM_NOT_OBJECT_MESSAGE.to_owned()));
    }
    // The size of the values map is judged before any value in the record
    // is decoded, and the file is abandoned at the row (measured
    // ). Its precedence against the item's own
    // parse-level faults was not measured; both abandon the file.
    let values_len = raw
        .expression_attribute_values
        .as_ref()
        .map_or(0, expression::values_serialized_len);
    if values_len > MAX_EXPRESSION_VALUES_BYTES {
        return Err(RowError::Parse(VALUES_TOO_LARGE_MESSAGE.to_owned()));
    }
    // The item's structure, judged before its values are decoded: the
    // faults here are the ones the shared deserializer reports with another
    // class or not at all.
    scan::check_item(&node)?;
    // The bulk path accepts a set with a repeated member and stores the set
    // deduplicated (measured on the service: `["a", "a"]` is written as `["a"]`,
    // processed and not failed), where `PutItem` refuses the same value.
    // The repeat is removed before the shared deserializer sees it, and its
    // bytes are kept for the item size check, which counts it (measured).
    let mut item_value = item_value;
    let repeated_member_bytes = dedupe_set_members(&mut item_value);
    // A value that decoded and failed a rule (an empty set, a `NULL` that is
    // not `true`) is the row's fault alone and carries the message `PutItem`
    // answers for it; a value that did not decode (no or two type tags, an
    // unknown tag, a binary value that is not base64) abandons the file,
    // with the base64 case carrying its own measured message.
    let item: Item = serde_json::from_value(item_value).map_err(|e| {
        let message = e.to_string();
        if message.starts_with(VALUE_RULE_PREFIX) {
            RowError::Item(message)
        } else if message.contains("base64") {
            RowError::Parse(BINARY_NOT_BASE64_MESSAGE.to_owned())
        } else {
            parse_error()
        }
    })?;
    if item_contains_nul(&item) {
        return Err(RowError::Item(NUL_IN_STRING_MESSAGE.to_owned()));
    }
    // An empty-string expression counts as absent, like in handle_put_item.
    let expression = raw
        .condition_expression
        .filter(|expression| !expression.is_empty());
    let Some(expression) = expression else {
        // No expression can reference the maps. Names wins when both are
        // present and empty maps are exempt, matching
        // validate_expression_param_usage.
        if raw
            .expression_attribute_names
            .is_some_and(|m| !m.is_empty())
        {
            return Err(RowError::Item(NAMES_WITHOUT_EXPRESSION_MESSAGE.to_owned()));
        }
        if raw
            .expression_attribute_values
            .is_some_and(|m| !m.is_empty())
        {
            return Err(RowError::Item(VALUES_WITHOUT_EXPRESSION_MESSAGE.to_owned()));
        }
        return Ok(Some(Row {
            item,
            condition: None,
            repeated_member_bytes,
        }));
    };
    // An expression is present, so an empty map is the row's fault rather
    // than a map with nothing to reference: the names case first, the order
    // the without-expression checks below use (measured on the service one map
    // at a time; a record carrying both empty is
    // unmeasured).
    if raw
        .expression_attribute_names
        .as_ref()
        .is_some_and(HashMap::is_empty)
    {
        return Err(RowError::Item(NAMES_EMPTY_MESSAGE.to_owned()));
    }
    if raw
        .expression_attribute_values
        .as_ref()
        .is_some_and(serde_json::Map::is_empty)
    {
        return Err(RowError::Item(VALUES_EMPTY_MESSAGE.to_owned()));
    }
    // The sizes the two bounds below judge are measured on the raw maps,
    // then the values are decoded, then the bounds are judged: a value that
    // fails its own rule fails the row with the wrapped text even when the
    // map is over the expression-size edge (measured on the service: a
    // NULL false beside a 1 MiB value answers the wrapped NULL text).
    // Decoding here rather than inside the line shape keeps a fault in one
    // entry the row's alone (measured on the service).
    let content_len = raw
        .expression_attribute_values
        .as_ref()
        .map_or(0, expression::values_content_len);
    let names_len = raw
        .expression_attribute_names
        .as_ref()
        .map_or(0, expression::names_combined_len);
    let decoded = match raw.expression_attribute_values {
        Some(values) => Some(expression::decode_values(values)?),
        None => None,
    };
    // Two item-level bounds sit under the abandon bound (measured on the
    // service). The two maps together, the names map counted by its raw
    // bytes, over 2 MiB fail the row with the combined-size text (the edge
    // is exact at 2,097,152); under that, the condition text plus the
    // content of its values reaching 1 MiB fails the row with the
    // single-item expression-size text. An empty names map and a bad value
    // are judged before them, an unused placeholder after (judged by the
    // runner's expression parsing).
    if values_len + names_len > MAX_EXPRESSION_MAPS_BYTES {
        return Err(RowError::Item(MAPS_TOO_LARGE_MESSAGE.to_owned()));
    }
    if expression.len() + content_len >= MAX_EXPRESSION_SIZE_BYTES {
        return Err(RowError::Item(EXPRESSION_SIZE_MESSAGE.to_owned()));
    }
    Ok(Some(Row {
        item,
        condition: Some(RowCondition {
            expression,
            names: raw.expression_attribute_names,
            values: decoded,
        }),
        repeated_member_bytes,
    }))
}

fn parse_error() -> RowError {
    RowError::Parse(PARSE_ABANDON_MESSAGE.to_owned())
}

/// The deepest bracket nesting of a JSON line, counting `{` and `[` outside
/// string literals. Brackets inside a string, and escaped quotes inside it,
/// are skipped. Unbalanced text saturates at zero and is left for the
/// parser to refuse.
fn bracket_depth(line: &[u8]) -> usize {
    let mut depth = 0usize;
    let mut deepest = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in line {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Remove repeated members from every `SS`, `NS` and `BS` value in the
/// item's JSON, at any depth, keeping first occurrences in order, and
/// return the item size of the members removed. `SS` members are compared
/// as text; `BS` members as the bytes they decode to; `NS` members as
/// numbers, so numerically equal members written differently collapse to
/// one. The walk follows the item's structure (an attribute map holds typed
/// values; an `M` holds another attribute map; an `L` holds typed values)
/// rather than matching key names, so an attribute that is itself named
/// `SS`, `NS` or `BS` is treated as an attribute and its value is still
/// reached (measured on the service: the service stores a set under an
/// attribute named `SS` deduplicated, like any other set). Only a set array
/// is touched; anything malformed is left for the deserializer to judge.
fn dedupe_set_members(item: &mut serde_json::Value) -> usize {
    let serde_json::Value::Object(attributes) = item else {
        return 0;
    };
    attributes.values_mut().map(dedupe_typed_value).sum()
}

/// One typed attribute value: a single-key object whose key is the type tag.
/// A removed member is sized the way `item_size_bytes` sizes the copy that
/// is kept: a string by its bytes, a binary value by its decoded bytes, a
/// number by the number size of its normalized form. A string or binary
/// set member of the wrong shape counts nothing: its first copy is kept,
/// and the deserializer refuses the set.
fn dedupe_typed_value(typed: &mut serde_json::Value) -> usize {
    let serde_json::Value::Object(map) = typed else {
        return 0;
    };
    if map.len() != 1 {
        return 0;
    }
    let Some((tag, inner)) = map.iter_mut().next() else {
        return 0;
    };
    match (tag.as_str(), inner) {
        // String set members are compared as text, the way the service
        // dedupes them (measured on the service).
        ("SS", serde_json::Value::Array(members)) => retain_first(
            members,
            |member| match member.as_str() {
                Some(text) => MemberKey::Text(text.to_owned()),
                None => MemberKey::Json(member.to_string()),
            },
            |key| match key {
                MemberKey::Text(text) => text.len(),
                MemberKey::Bytes(_) | MemberKey::Json(_) => 0,
            },
        ),
        // Binary set members are compared as the bytes they decode to, with
        // the decoder the deserializer uses. That decoder accepts one text
        // for each byte string, so this compares the texts; a repeated
        // member is stored once (measured on the service). A member that
        // does not decode is keyed by its text and left for the
        // deserializer to refuse.
        ("BS", serde_json::Value::Array(members)) => retain_first(
            members,
            |member| match member.as_str() {
                Some(text) => base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .map_or_else(|_| MemberKey::Text(text.to_owned()), MemberKey::Bytes),
                None => MemberKey::Json(member.to_string()),
            },
            |key| match key {
                MemberKey::Bytes(bytes) => bytes.len(),
                MemberKey::Text(_) | MemberKey::Json(_) => 0,
            },
        ),
        // Number set members are compared as numbers, so members written
        // differently but numerically equal (for example "1" and "1.0")
        // count as one and the first is kept: the service stores
        // NS ["1", "1.0"] as NS ["1"] with the operation COMPLETED (measured
        // on the service), where comparing the members as text would leave
        // both and the deserializer would then refuse the row as a duplicate
        // set. The dedupe key is the normalization ExtendDB already applies
        // to a number on write, so it matches what the deserializer would
        // store; a member that does not normalize is keyed by its raw text,
        // and one that is not a string by its JSON text in a key space of
        // its own, so it is never taken for a repeat of a string member and
        // reaches the deserializer, which refuses it, wherever it stands.
        ("NS", serde_json::Value::Array(members)) => retain_first(
            members,
            |member| match member.as_str() {
                Some(s) => MemberKey::Text(
                    validate_and_normalize_number(s).unwrap_or_else(|_| s.to_owned()),
                ),
                None => MemberKey::Json(member.to_string()),
            },
            |key| match key {
                MemberKey::Text(number) => attribute_value_size(&AttributeValue::N(number.clone())),
                MemberKey::Bytes(_) | MemberKey::Json(_) => 0,
            },
        ),
        ("M", nested) => dedupe_set_members(nested),
        ("L", serde_json::Value::Array(elements)) => {
            elements.iter_mut().map(dedupe_typed_value).sum()
        }
        _ => 0,
    }
}

/// What a set member is compared by when its set is deduplicated.
#[derive(PartialEq, Eq, Hash)]
enum MemberKey {
    /// The member's text: an `SS` member, the normalized number of an `NS`
    /// member, or a `BS` member that does not decode.
    Text(String),
    /// The bytes a `BS` member decodes to.
    Bytes(Vec<u8>),
    /// A member that is not a JSON string, by its JSON text.
    Json(String),
}

/// Keep the first member of each key, in order, and return the sum of
/// `size` over the members removed. The keys seen are held in a hash set,
/// so the pass is linear in the number of members.
fn retain_first(
    members: &mut Vec<serde_json::Value>,
    key: impl Fn(&serde_json::Value) -> MemberKey,
    size: impl Fn(&MemberKey) -> usize,
) -> usize {
    let mut seen = HashSet::with_capacity(members.len());
    let mut removed = 0;
    members.retain(|member| {
        let key = key(member);
        if seen.contains(&key) {
            removed += size(&key);
            false
        } else {
            seen.insert(key)
        }
    });
    removed
}

/// Whether any attribute name or string-typed value in the item, at any
/// depth of maps and lists, contains U+0000. Number strings are covered by
/// the number validator (the character is not a digit) and binary values
/// are bytes, so only `S`, `SS` members and map keys are walked.
fn item_contains_nul(item: &Item) -> bool {
    item.iter()
        .any(|(name, value)| name.contains('\0') || value_contains_nul(value))
}

fn value_contains_nul(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::S(s) => s.contains('\0'),
        AttributeValue::SS(set) => set.iter().any(|s| s.contains('\0')),
        AttributeValue::L(list) => list.iter().any(value_contains_nul),
        AttributeValue::M(map) => map
            .iter()
            .any(|(name, value)| name.contains('\0') || value_contains_nul(value)),
        _ => false,
    }
}
