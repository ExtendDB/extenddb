// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Whether a stored item can be restored: the check a backup job runs on
//! every item it reads, before the item is written to a data file, and the
//! check PR 9a's restore binds for every item it reads back.
//!
//! The write paths let some items in that a restore's `PutItem`-shaped write
//! would refuse: an `N` value that is not a valid number (`1E126`, `abc`;
//! #618 names the four paths that store one unvalidated: `BatchWriteItem`,
//! `TransactWriteItems`, `UpdateItem` on an absent item, `ImportTable`), an
//! item over the configured size limit stored under a larger one, a key
//! attribute or attribute name over this instance's limits stored under
//! larger ones, and an attribute name over the limit that `UpdateItem` and
//! a `TransactWriteItems` `Update` stored unchecked before every write path
//! checked it (#433). The key-shape rules (a key attribute missing, of the wrong
//! type or empty, an `N` key not in normalized form) cover rows of earlier
//! builds and rows edited outside the wire: every write path of this tree
//! refuses such a key, and the snapshot streams hand every valid `N` over
//! normalized, so the normalized-form branch is unreachable from a
//! snapshot; it stays because it is the format's own rule
//! ([`crate::backup_format::check_key_value`]). A backup that carried such
//! an item could not be restored in full, so the job refuses the backup
//! instead and names the item (its key), the attribute path and the reason.
//! The base key alone is checked: an index key attribute of the wrong type
//! or empty is left to the restore's index build.
//!
//! One walk over the item: every `N` and every `NS` member is parsed once
//! on the accepting path (a key `N` that fails the key rule is parsed a
//! second time to classify the failure), the nesting depth is counted as it
//! goes, every attribute name is measured against `max_attribute_name_bytes`
//! and the key attributes are checked with the backup format's own key rule
//! and against the hash and range key size limits, the rules the wire's
//! `check_key_size` and `check_attribute_name_size` apply, so a key that
//! passes here is one a key-ordered file's range can carry and one the
//! target's write accepts. The item's size is one call on the whole item
//! (`item_size_bytes`). The request path's `validate_put_item` stays what it
//! is: it returns the wire error with no attribute path and is not called
//! here.
//!
//! Every untrusted text an error carries (a key value, an attribute name, a
//! path segment, a number the wire's reason echoes) is bounded on its own
//! ([`SHOWN_VALUE_BYTES`]; a key value at [`SHOWN_KEY_VALUE_BYTES`]) and
//! rendered as a JSON string's content, with the line separators U+0085,
//! U+2028 and U+2029 escaped beside the control characters, so neither a
//! control character nor a Unicode line separator in a name or a number
//! can end the log line it is written in, and a key of any
//! legal size appears whole while only an oversized value is cut. The
//! bound is per text, so the whole message is bounded by the item's own
//! size times a small factor (a byte renders as at most six), not by a
//! constant: a path of thirty nested maps with names at the bound is about
//! 127 KB.

use std::borrow::Cow;
use std::fmt;

use crate::backup_format::{FormatError, check_key_value, hash_key_text, shown};
use crate::limits::LimitsConfig;
use crate::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
    item_size_bytes,
};
use crate::validation::number::validate_and_normalize_number;

/// Longest rendering of one untrusted text in a [`RestorableItemError`]
/// other than a key value (an attribute name, a path segment, a set member,
/// the number a reason echoes), in the bytes the message writes: the text
/// as a JSON string's content, quotes not counted. A round figure that
/// keeps a path segment to four times the longest key attribute name
/// (255 bytes), after which a name is cut.
pub const SHOWN_VALUE_BYTES: usize = 4096;

/// Longest rendering of a key attribute's value, in the same measure: six
/// times the 2048-byte hash key limit, the most a byte escapes to
/// (`\u00XX`), so a key of any legal size renders whole whatever its
/// bytes, and the 2732-character base64 of a 2048-byte `B` key fits.
pub const SHOWN_KEY_VALUE_BYTES: usize = 6 * 2048;

/// The prefix kept of a text over its bound, in the same measure, followed
/// by `... <N bytes>` with the text's length.
const SHOWN_VALUE_PREFIX_BYTES: usize = 1024;

/// The instance's limits a restore would hold an item to, for
/// [`validate_restorable_item`]: the item size, the hash and range key
/// sizes and the attribute name size of `LimitsConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestorableLimits {
    pub max_item_size_bytes: usize,
    pub max_partition_key_size_bytes: usize,
    pub max_sort_key_size_bytes: usize,
    pub max_attribute_name_bytes: usize,
}

impl From<&LimitsConfig> for RestorableLimits {
    fn from(limits: &LimitsConfig) -> Self {
        Self {
            max_item_size_bytes: limits.max_item_size_bytes,
            max_partition_key_size_bytes: limits.max_partition_key_size_bytes,
            max_sort_key_size_bytes: limits.max_sort_key_size_bytes,
            max_attribute_name_bytes: limits.max_attribute_name_bytes,
        }
    }
}

/// One key attribute of a [`RestorableKeySchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyAttribute {
    name: String,
    ty: ScalarAttributeType,
    key_type: KeyType,
}

/// The key attributes of a table, resolved once from its key schema and
/// attribute definitions, for [`validate_restorable_item`]. Unlike
/// `KeyOrder`, which only a hash-only or single-range-key table has, every
/// table has one of these: a multi-part key is a list of more than two
/// attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorableKeySchema {
    attributes: Vec<KeyAttribute>,
}

impl RestorableKeySchema {
    /// Resolve the key attributes, in key-schema order.
    ///
    /// # Errors
    ///
    /// The name of a key attribute that has no attribute definition, or more
    /// than one; `CreateTable` and the manifest reader both refuse such a
    /// schema, so a table's own schema never fails here.
    pub fn resolve(
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
    ) -> Result<Self, String> {
        let mut attributes = Vec::with_capacity(key_schema.len());
        for element in key_schema {
            let name = element.attribute_name.as_str();
            let mut matching = attribute_definitions
                .iter()
                .filter(|d| d.attribute_name == name)
                .map(|d| d.attribute_type);
            let ty = matching.next().ok_or_else(|| {
                format!(
                    "key attribute {:?} has no attribute definition",
                    shown(name)
                )
            })?;
            if matching.next().is_some() {
                return Err(format!(
                    "key attribute {:?} has more than one attribute definition",
                    shown(name)
                ));
            }
            attributes.push(KeyAttribute {
                name: name.to_owned(),
                ty,
                key_type: element.key_type,
            });
        }
        Ok(Self { attributes })
    }

    /// Names of the key attributes, in key-schema order.
    pub fn attribute_names(&self) -> impl Iterator<Item = &str> {
        self.attributes.iter().map(|a| a.name.as_str())
    }

    /// Whether `name` is a key attribute.
    fn is_key(&self, name: &str) -> bool {
        self.attributes.iter().any(|a| a.name == name)
    }

    /// The key attributes of `item` as compact JSON (`{"pk":{"S":"a"}}`)
    /// for an error message or a log line, each name bounded on its own
    /// ([`SHOWN_VALUE_BYTES`]) and each value on its own
    /// ([`SHOWN_KEY_VALUE_BYTES`]), so every key attribute appears and only
    /// an oversized value is cut; a key attribute the item lacks is left
    /// out. A `B` value is its base64 text, as the format writes it; a value
    /// that is not a scalar (a key attribute of the wrong type) is its own
    /// JSON, cut to a JSON string when oversized.
    #[must_use]
    pub fn shown_key(&self, item: &Item) -> String {
        let mut out = String::from("{");
        let mut first = true;
        for attribute in &self.attributes {
            let Some(value) = item.get(&attribute.name) else {
                continue;
            };
            if !first {
                out.push(',');
            }
            first = false;
            push_json_string(&mut out, &attribute.name, SHOWN_VALUE_BYTES);
            out.push(':');
            let tag = match value {
                AttributeValue::S(_) => Some("S"),
                AttributeValue::N(_) => Some("N"),
                AttributeValue::B(_) => Some("B"),
                _ => None,
            };
            match (tag, hash_key_text(value)) {
                (Some(tag), Some(text)) => {
                    out.push_str("{\"");
                    out.push_str(tag);
                    out.push_str("\":");
                    push_json_string(&mut out, &text, SHOWN_KEY_VALUE_BYTES);
                    out.push('}');
                }
                _ => {
                    let text = serde_json::to_string(value).unwrap_or_default();
                    if text.len() <= SHOWN_KEY_VALUE_BYTES {
                        out.push_str(&text);
                    } else {
                        push_json_string(&mut out, &text, SHOWN_KEY_VALUE_BYTES);
                    }
                }
            }
        }
        out.push('}');
        out
    }
}

/// Append `s` to `out` as a JSON string: quotes around [`bounded_text`]
/// with `bound`.
fn push_json_string(out: &mut String, s: &str, bound: usize) {
    out.push('"');
    out.push_str(&bounded_text(s, bound));
    out.push('"');
}

/// Bytes a message writes for `c` inside a JSON string: `\"`, `\\` and
/// the five short escapes take two, the other control characters and the
/// three line separators `\u00XX`-style escapes of six, everything else
/// its UTF-8 length.
fn json_escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if (c as u32) < 0x20 || is_line_separator(c) => 6,
        c => c.len_utf8(),
    }
}

/// The Unicode line separators `serde_json` leaves raw and a reader that
/// breaks lines the Unicode way (Python's `str.splitlines`) breaks on:
/// NEXT LINE, LINE SEPARATOR and PARAGRAPH SEPARATOR.
fn is_line_separator(c: char) -> bool {
    matches!(c, '\u{85}' | '\u{2028}' | '\u{2029}')
}

/// `s` as a JSON string's content: the escapes `serde_json` writes, then
/// the three line separators as `\u0085`, `\u2028` and `\u2029` (valid JSON
/// escapes `serde_json` does not write), no quotes, so neither a control
/// character nor a line separator in a tenant's text can stand as a line
/// of its own in a log; borrowed when nothing in it escapes.
fn json_escaped(s: &str) -> Cow<'_, str> {
    if s.chars().all(|c| json_escaped_len(c) == c.len_utf8()) {
        return Cow::Borrowed(s);
    }
    let mut quoted = serde_json::to_string(s).unwrap_or_default();
    quoted.pop();
    if !quoted.is_empty() {
        quoted.remove(0);
    }
    if quoted.chars().any(is_line_separator) {
        quoted = quoted
            .replace('\u{85}', r"\u0085")
            .replace('\u{2028}', r"\u2028")
            .replace('\u{2029}', r"\u2029");
    }
    Cow::Owned(quoted)
}

/// `s` as a message writes it, a JSON string's content ([`json_escaped`]),
/// bounded: whole when its escaped text is at most `bound` bytes, otherwise
/// a prefix of at most [`SHOWN_VALUE_PREFIX_BYTES`] of escaped text and
/// `... <N bytes>` with the text's length.
fn bounded_text(s: &str, bound: usize) -> Cow<'_, str> {
    let mut rendered = 0;
    let mut prefix = 0;
    for c in s.chars() {
        rendered += json_escaped_len(c);
        if rendered > bound {
            return Cow::Owned(format!(
                "{}... <{} bytes>",
                json_escaped(&s[..prefix]),
                s.len()
            ));
        }
        if rendered <= SHOWN_VALUE_PREFIX_BYTES {
            prefix += c.len_utf8();
        }
    }
    json_escaped(s)
}

/// Why an item cannot be restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestorableItemReason {
    /// An `N` value, or a member of an `NS`, that is not a valid `DynamoDB`
    /// number: the wire's own reason text, with the member appended for an
    /// `NS` when the text does not already end in it (the #618 case; a
    /// FAILED backup names the issue when its first refused item has this
    /// reason).
    InvalidNumber(String),
    /// A key attribute missing, of the wrong type, empty, an `N` key not in
    /// normalized form (the backup format's key rule's reason text), or a
    /// key value over the hash or range key size limit.
    KeyShape(String),
    /// An attribute name (the path's last segment) longer than the
    /// attribute name limit.
    AttributeNameTooLong { size: usize, max: usize },
    /// The item is larger than the configured limit.
    TooLarge { size: usize, max: usize },
    /// A map or list nests deeper than the item nesting limit.
    TooDeep,
    /// The item's data file line would exceed the format's line cap
    /// (`MAX_LINE_BYTES`): the backup job measures the line of an item over
    /// the default item limit, the largest the cap is sized for, and stops
    /// measuring once the cap is passed, so the line's own length is not
    /// known.
    LineTooLong { cap: usize },
}

impl fmt::Display for RestorableItemReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNumber(reason) | Self::KeyShape(reason) => f.write_str(reason),
            Self::AttributeNameTooLong { size, max } => {
                write!(
                    f,
                    "attribute name of {size} bytes exceeds the limit of {max} bytes"
                )
            }
            Self::TooLarge { size, max } => {
                write!(f, "item size {size} bytes exceeds the limit of {max} bytes")
            }
            Self::TooDeep => write!(
                f,
                "attributes nest beyond {} levels",
                super::MAX_ITEM_NESTING_DEPTH
            ),
            Self::LineTooLong { cap } => {
                write!(f, "data file line exceeds the line cap of {cap} bytes")
            }
        }
    }
}

/// An item [`validate_restorable_item`] refused: which item (its key
/// attributes as compact JSON, each name and value bounded), where (the
/// attribute path, `a.b[2].c` for a member of a list inside a map, each
/// segment bounded, empty for a rule about the whole item) and why.
/// `Display` writes `key {key}, attribute {path}: {reason}`, without the
/// attribute clause when the path is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorableItemError {
    pub key: String,
    pub path: String,
    pub reason: RestorableItemReason,
}

impl fmt::Display for RestorableItemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            write!(f, "key {}: {}", self.key, self.reason)
        } else {
            write!(
                f,
                "key {}, attribute {}: {}",
                self.key, self.path, self.reason
            )
        }
    }
}

impl std::error::Error for RestorableItemError {}

/// Check that `item` is one a restore under `limits` can write: its key
/// attributes present with the declared types, not empty, an `N` key
/// normalized, each within its key size limit; every attribute name within
/// the name limit; every `N` and `NS` member a valid number; no map or list
/// nested beyond the item nesting limit; the item within the item size
/// limit. The first failure, in that order (key attributes in key-schema
/// order, then the other attributes in name order, then the size), is the
/// error.
///
/// # Errors
///
/// [`RestorableItemError`] naming the item's key, the attribute path and the
/// reason.
pub fn validate_restorable_item(
    item: &Item,
    key: &RestorableKeySchema,
    limits: &RestorableLimits,
) -> Result<(), RestorableItemError> {
    let refuse = |path: String, reason: RestorableItemReason| RestorableItemError {
        key: key.shown_key(item),
        path,
        reason,
    };
    for attribute in &key.attributes {
        let name = attribute.name.as_str();
        let path = || bounded_text(name, SHOWN_VALUE_BYTES).into_owned();
        if name.len() > limits.max_attribute_name_bytes {
            return Err(refuse(
                path(),
                RestorableItemReason::AttributeNameTooLong {
                    size: name.len(),
                    max: limits.max_attribute_name_bytes,
                },
            ));
        }
        let Some(value) = item.get(name) else {
            return Err(refuse(
                path(),
                RestorableItemReason::KeyShape(format!("key attribute {:?} missing", shown(name))),
            ));
        };
        if let Err(e) = check_key_value(name, attribute.ty, value) {
            let reason = match e {
                FormatError::InvalidKey(reason) => reason,
                other => other.to_string(),
            };
            // Classified by the declared type first: a text that is not a
            // number under an `N` key is the #618 case; under any other
            // declared type it is a key of the wrong type.
            let reason = match (value, attribute.ty) {
                (AttributeValue::N(n), ScalarAttributeType::N) => {
                    match validate_and_normalize_number(n) {
                        Err(wire) => RestorableItemReason::InvalidNumber(number_reason(n, &wire)),
                        Ok(_) => RestorableItemReason::KeyShape(reason),
                    }
                }
                _ => RestorableItemReason::KeyShape(reason),
            };
            return Err(refuse(path(), reason));
        }
        let (max, kind) = match attribute.key_type {
            KeyType::Hash => (limits.max_partition_key_size_bytes, "hash"),
            KeyType::Range => (limits.max_sort_key_size_bytes, "range"),
        };
        let size = super::key_size::key_value_byte_size(value);
        if size > max {
            return Err(refuse(
                path(),
                RestorableItemReason::KeyShape(format!(
                    "key attribute {:?} has {size} bytes; the {kind} key limit is {max} bytes",
                    shown(name)
                )),
            ));
        }
    }
    let mut path = String::new();
    for (name, value) in item {
        if key.is_key(name) {
            continue;
        }
        path.clear();
        push_segment(&mut path, name);
        if let Err(reason) =
            check_name(name, limits).and_then(|()| walk(value, &mut path, 0, limits))
        {
            return Err(refuse(std::mem::take(&mut path), reason));
        }
    }
    let size = item_size_bytes(item);
    if size > limits.max_item_size_bytes {
        return Err(refuse(
            String::new(),
            RestorableItemReason::TooLarge {
                size,
                max: limits.max_item_size_bytes,
            },
        ));
    }
    Ok(())
}

/// The attribute name rule the wire's `check_attribute_name_size` applies,
/// at the top level and for every map member.
fn check_name(name: &str, limits: &RestorableLimits) -> Result<(), RestorableItemReason> {
    if name.len() > limits.max_attribute_name_bytes {
        return Err(RestorableItemReason::AttributeNameTooLong {
            size: name.len(),
            max: limits.max_attribute_name_bytes,
        });
    }
    Ok(())
}

/// Append one attribute name to `path`, bounded and escaped
/// ([`bounded_text`]).
fn push_segment(path: &mut String, name: &str) {
    path.push_str(&bounded_text(name, SHOWN_VALUE_BYTES));
}

/// Visit `value` and everything under it. On an error `path` names the
/// offending value; otherwise it is left as it was on entry.
fn walk(
    value: &AttributeValue,
    path: &mut String,
    depth: usize,
    limits: &RestorableLimits,
) -> Result<(), RestorableItemReason> {
    match value {
        AttributeValue::N(n) => number(n),
        AttributeValue::NS(set) => {
            for (i, n) in set.iter().enumerate() {
                if let Err(RestorableItemReason::InvalidNumber(text)) = number(n) {
                    path.push_str(&format!("[{i}]"));
                    let member = bounded_text(n, SHOWN_VALUE_BYTES);
                    let text = if text.ends_with(&*member) {
                        text
                    } else {
                        format!("{text} (member {member})")
                    };
                    return Err(RestorableItemReason::InvalidNumber(text));
                }
            }
            Ok(())
        }
        AttributeValue::L(list) => {
            let next = depth + 1;
            if next >= super::MAX_ITEM_NESTING_DEPTH {
                return Err(RestorableItemReason::TooDeep);
            }
            let len = path.len();
            for (i, v) in list.iter().enumerate() {
                path.push_str(&format!("[{i}]"));
                walk(v, path, next, limits)?;
                path.truncate(len);
            }
            Ok(())
        }
        AttributeValue::M(map) => {
            let next = depth + 1;
            if next >= super::MAX_ITEM_NESTING_DEPTH {
                return Err(RestorableItemReason::TooDeep);
            }
            let len = path.len();
            for (name, v) in map {
                path.push('.');
                push_segment(path, name);
                check_name(name, limits)?;
                walk(v, path, next, limits)?;
                path.truncate(len);
            }
            Ok(())
        }
        AttributeValue::S(_)
        | AttributeValue::B(_)
        | AttributeValue::SS(_)
        | AttributeValue::BS(_)
        | AttributeValue::Bool(_)
        | AttributeValue::Null => Ok(()),
    }
}

fn number(n: &str) -> Result<(), RestorableItemReason> {
    validate_and_normalize_number(n)
        .map(drop)
        .map_err(|e| RestorableItemReason::InvalidNumber(number_reason(n, &e)))
}

/// The wire's reason for `n`, which ends in `n` for a malformed number: that
/// echo is bounded and escaped like every other untrusted text
/// ([`bounded_text`]), so the text is the wire's byte for byte up to
/// [`SHOWN_VALUE_BYTES`] for an input with nothing to escape.
fn number_reason(n: &str, e: &crate::error::DynamoDbError) -> String {
    let text = e.message();
    match text.strip_suffix(n) {
        Some(head) => format!("{head}{}", bounded_text(n, SHOWN_VALUE_BYTES)),
        None => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;
    use std::collections::{BTreeMap, BTreeSet};

    const MAX: usize = 400 * 1024;

    /// The default limits with `max_item_size_bytes` at `max`.
    fn limits(max: usize) -> RestorableLimits {
        RestorableLimits {
            max_item_size_bytes: max,
            ..RestorableLimits::from(&LimitsConfig::default())
        }
    }

    fn element(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn definition(name: &str, ty: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ty,
        }
    }

    /// `pk` S alone.
    fn hash_only() -> RestorableKeySchema {
        RestorableKeySchema::resolve(
            &[element("pk", KeyType::Hash)],
            &[definition("pk", ScalarAttributeType::S)],
        )
        .unwrap()
    }

    /// `pk` S, `sk` N.
    fn composite() -> RestorableKeySchema {
        RestorableKeySchema::resolve(
            &[element("pk", KeyType::Hash), element("sk", KeyType::Range)],
            &[
                definition("pk", ScalarAttributeType::S),
                definition("sk", ScalarAttributeType::N),
            ],
        )
        .unwrap()
    }

    /// `pk` S, `sk` N, `sk2` B: a key `KeyOrder` cannot resolve.
    fn multi_part() -> RestorableKeySchema {
        RestorableKeySchema::resolve(
            &[
                element("pk", KeyType::Hash),
                element("sk", KeyType::Range),
                element("sk2", KeyType::Range),
            ],
            &[
                definition("pk", ScalarAttributeType::S),
                definition("sk", ScalarAttributeType::N),
                definition("sk2", ScalarAttributeType::B),
            ],
        )
        .unwrap()
    }

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    fn n(v: &str) -> AttributeValue {
        AttributeValue::N(v.to_owned())
    }

    fn item(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn refused(item: &Item, key: &RestorableKeySchema, max: usize) -> RestorableItemError {
        validate_restorable_item(item, key, &limits(max)).expect_err("refused")
    }

    #[test]
    fn a_plain_item_passes_on_every_key_shape() {
        let it = item(&[
            ("pk", s("a")),
            ("sk", n("1")),
            ("sk2", AttributeValue::B(vec![1])),
            ("x", n("1E125")),
            (
                "y",
                AttributeValue::NS(BTreeSet::from(["1".to_owned(), "2.5".to_owned()])),
            ),
        ]);
        validate_restorable_item(&it, &hash_only(), &limits(MAX)).unwrap();
        validate_restorable_item(&it, &composite(), &limits(MAX)).unwrap();
        validate_restorable_item(&it, &multi_part(), &limits(MAX)).unwrap();
    }

    #[test]
    fn an_overflowing_number_in_a_non_key_attribute_is_refused_with_its_path() {
        let it = item(&[("pk", s("a")), ("amount", n("1E126"))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.key, r#"{"pk":{"S":"a"}}"#);
        assert_eq!(e.path, "amount");
        let RestorableItemReason::InvalidNumber(reason) = &e.reason else {
            panic!("{e:?}");
        };
        assert!(reason.starts_with("Number overflow"), "{reason}");
        assert_eq!(
            e.to_string(),
            format!(r#"key {{"pk":{{"S":"a"}}}}, attribute amount: {reason}"#)
        );
    }

    #[test]
    fn a_non_numeric_text_is_refused_with_the_wire_reason() {
        let it = item(&[("pk", s("a")), ("amount", n("abc"))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "amount");
        assert_eq!(
            e.reason,
            RestorableItemReason::InvalidNumber(
                "The parameter cannot be converted to a numeric value: abc".to_owned()
            )
        );
    }

    #[test]
    fn a_nested_number_names_its_path_through_maps_and_lists() {
        let inner = AttributeValue::M(BTreeMap::from([("c".to_owned(), n("abc"))]));
        let list = AttributeValue::L(vec![n("1"), n("2"), inner]);
        let a = AttributeValue::M(BTreeMap::from([("b".to_owned(), list)]));
        let it = item(&[("pk", s("a")), ("a", a)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "a.b[2].c");
        assert!(matches!(e.reason, RestorableItemReason::InvalidNumber(_)));
    }

    /// A set member's reason carries the member, since the position is the
    /// sorted set's and the overflow text alone does not name the value; a
    /// text that already ends in the member (the malformed case) is left as
    /// it is.
    #[test]
    fn a_number_set_member_names_its_position_and_the_member() {
        let set = AttributeValue::NS(BTreeSet::from([
            "1".to_owned(),
            "2".to_owned(),
            "x".to_owned(),
        ]));
        let it = item(&[("pk", s("a")), ("ns", set)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "ns[2]");
        assert_eq!(
            e.reason,
            RestorableItemReason::InvalidNumber(
                "The parameter cannot be converted to a numeric value: x".to_owned()
            )
        );
        let set = AttributeValue::NS(BTreeSet::from(["01E126".to_owned(), "1E126".to_owned()]));
        let it = item(&[("pk", s("a")), ("ns", set)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "ns[0]");
        let RestorableItemReason::InvalidNumber(reason) = &e.reason else {
            panic!("{e:?}");
        };
        assert!(reason.starts_with("Number overflow"), "{reason}");
        assert!(reason.ends_with(" (member 01E126)"), "{reason}");
    }

    /// The path is rebuilt from the attribute for each sibling: a list whose
    /// first member is a valid map and whose second member is bad names the
    /// second, not a path that still carries the first's members.
    #[test]
    fn the_path_does_not_carry_a_sibling_that_passed() {
        let ok = AttributeValue::M(BTreeMap::from([("deep".to_owned(), n("1"))]));
        let list = AttributeValue::L(vec![ok, n("abc")]);
        let it = item(&[("pk", s("a")), ("l", list)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "l[1]");
    }

    #[test]
    fn validation_does_not_stop_at_the_first_valid_number() {
        // Three valid numbers before the bad one, in name order.
        let it = item(&[
            ("pk", s("a")),
            ("n1", n("1")),
            ("n2", n("2")),
            ("n3", n("3")),
            ("n4", n("1E126")),
        ]);
        assert_eq!(refused(&it, &hash_only(), MAX).path, "n4");
    }

    #[test]
    fn an_item_over_the_size_limit_is_refused_without_a_path() {
        let it = item(&[("pk", s("a")), ("blob", s(&"x".repeat(100)))]);
        let size = item_size_bytes(&it);
        validate_restorable_item(&it, &hash_only(), &limits(size)).unwrap();
        let e = refused(&it, &hash_only(), size - 1);
        assert_eq!(e.path, "");
        assert_eq!(
            e.reason,
            RestorableItemReason::TooLarge {
                size,
                max: size - 1
            }
        );
        assert_eq!(
            e.to_string(),
            format!(
                r#"key {{"pk":{{"S":"a"}}}}: item size {size} bytes exceeds the limit of {} bytes"#,
                size - 1
            )
        );
    }

    #[test]
    fn nesting_at_the_limit_is_refused_and_one_short_of_it_passes() {
        let nest = |levels: usize| {
            let mut v = n("1");
            for _ in 0..levels {
                v = AttributeValue::M(BTreeMap::from([("m".to_owned(), v)]));
            }
            item(&[("pk", s("a")), ("deep", v)])
        };
        let limit = super::super::MAX_ITEM_NESTING_DEPTH;
        validate_restorable_item(&nest(limit - 1), &hash_only(), &limits(MAX)).unwrap();
        let e = refused(&nest(limit), &hash_only(), MAX);
        assert_eq!(e.reason, RestorableItemReason::TooDeep);
        assert_eq!(e.path, format!("deep{}", ".m".repeat(limit - 1)));
        // The same rule the request path applies.
        super::super::validate_item_nesting_depth(&nest(limit - 1)).unwrap();
        super::super::validate_item_nesting_depth(&nest(limit)).unwrap_err();
    }

    #[test]
    fn a_missing_key_attribute_is_refused() {
        let it = item(&[("pk", s("a"))]);
        let e = refused(&it, &composite(), MAX);
        assert_eq!(e.path, "sk");
        assert_eq!(e.key, r#"{"pk":{"S":"a"}}"#);
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(r#"key attribute "sk" missing"#.to_owned())
        );
    }

    #[test]
    fn a_key_attribute_of_the_wrong_type_is_refused() {
        let it = item(&[("pk", s("a")), ("sk", s("1"))]);
        let e = refused(&it, &composite(), MAX);
        assert_eq!(e.path, "sk");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(
                r#"key attribute "sk" is S, the schema declares N"#.to_owned()
            )
        );
        assert_eq!(e.key, r#"{"pk":{"S":"a"},"sk":{"S":"1"}}"#);
    }

    /// A text that is not a number under a key declared `S` is a key of the
    /// wrong type, not the #618 case: the declared type decides first.
    #[test]
    fn a_non_numeric_n_under_an_s_key_is_a_key_shape_error() {
        let it = item(&[("pk", n("abc"))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "pk");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(
                r#"key attribute "pk" is N, the schema declares S"#.to_owned()
            )
        );
    }

    #[test]
    fn an_empty_key_value_is_refused() {
        let it = item(&[("pk", s(""))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "pk");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(r#"key attribute "pk" is empty"#.to_owned())
        );
    }

    /// A bad number in the range key is the #618 case, not a key-shape one:
    /// the backup's FAILED reason names the issue by this variant.
    #[test]
    fn an_invalid_number_in_a_range_key_is_an_invalid_number() {
        for bad in ["abc", "1E126"] {
            let it = item(&[("pk", s("a")), ("sk", n(bad))]);
            let e = refused(&it, &composite(), MAX);
            assert_eq!(e.path, "sk");
            assert!(
                matches!(e.reason, RestorableItemReason::InvalidNumber(_)),
                "{bad}: {e:?}"
            );
        }
    }

    #[test]
    fn a_valid_but_unnormalized_number_key_is_a_key_shape_error() {
        let it = item(&[("pk", s("a")), ("sk", n("1.0"))]);
        let e = refused(&it, &composite(), MAX);
        assert_eq!(e.path, "sk");
        let RestorableItemReason::KeyShape(reason) = &e.reason else {
            panic!("{e:?}");
        };
        assert!(reason.contains("not in normalized form"), "{reason}");
    }

    #[test]
    fn a_multi_part_key_is_checked_attribute_by_attribute() {
        let it = item(&[
            ("pk", s("a")),
            ("sk", n("1")),
            ("sk2", AttributeValue::B(vec![])),
        ]);
        let e = refused(&it, &multi_part(), MAX);
        assert_eq!(e.path, "sk2");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(r#"key attribute "sk2" is empty"#.to_owned())
        );
        assert_eq!(e.key, r#"{"pk":{"S":"a"},"sk":{"N":"1"},"sk2":{"B":""}}"#);
    }

    #[test]
    fn key_attributes_are_checked_before_the_other_attributes() {
        let it = item(&[("amount", n("abc")), ("pk", s(""))]);
        assert_eq!(refused(&it, &hash_only(), MAX).path, "pk");
    }

    #[test]
    fn a_schema_whose_key_attribute_lacks_a_definition_does_not_resolve() {
        let err = RestorableKeySchema::resolve(&[element("pk", KeyType::Hash)], &[]).unwrap_err();
        assert_eq!(err, r#"key attribute "pk" has no attribute definition"#);
        let err = RestorableKeySchema::resolve(
            &[element("pk", KeyType::Hash)],
            &[
                definition("pk", ScalarAttributeType::S),
                definition("pk", ScalarAttributeType::N),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            r#"key attribute "pk" has more than one attribute definition"#
        );
    }

    /// The key sizes against the instance's limits, the rule the wire's
    /// `check_key_size` applies: at the limit the key passes, one byte over
    /// it is refused, hash and range each against their own limit; a `B`
    /// key's size is its bytes.
    #[test]
    fn a_key_over_its_size_limit_is_refused_at_the_boundary() {
        let limits = RestorableLimits {
            max_partition_key_size_bytes: 10,
            max_sort_key_size_bytes: 4,
            ..limits(MAX)
        };
        let ok = item(&[("pk", s(&"p".repeat(10))), ("sk", n("1234"))]);
        validate_restorable_item(&ok, &composite(), &limits).unwrap();
        let hash = item(&[("pk", s(&"p".repeat(11))), ("sk", n("1"))]);
        let e = validate_restorable_item(&hash, &composite(), &limits).unwrap_err();
        assert_eq!(e.path, "pk");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(
                r#"key attribute "pk" has 11 bytes; the hash key limit is 10 bytes"#.to_owned()
            )
        );
        let range = item(&[("pk", s("p")), ("sk", n("12345"))]);
        let e = validate_restorable_item(&range, &composite(), &limits).unwrap_err();
        assert_eq!(e.path, "sk");
        assert_eq!(
            e.reason,
            RestorableItemReason::KeyShape(
                r#"key attribute "sk" has 5 bytes; the range key limit is 4 bytes"#.to_owned()
            )
        );
        let binary = item(&[
            ("pk", s("p")),
            ("sk", n("1")),
            ("sk2", AttributeValue::B(vec![0; 5])),
        ]);
        let e = validate_restorable_item(&binary, &multi_part(), &limits).unwrap_err();
        assert_eq!(e.path, "sk2");
        assert!(
            e.to_string().contains("has 5 bytes; the range key limit"),
            "{e}"
        );
        // The defaults: a 2048-byte hash key and a 1024-byte range key pass.
        let largest = item(&[("pk", s(&"p".repeat(2048))), ("sk", n(&"1".repeat(38)))]);
        validate_restorable_item(&largest, &composite(), &self::limits(MAX)).unwrap();
        let over = item(&[("pk", s(&"p".repeat(2049))), ("sk", n("1"))]);
        assert!(validate_restorable_item(&over, &composite(), &self::limits(MAX)).is_err());
    }

    /// The attribute name rule the wire's `check_attribute_name_size`
    /// applies: a name at the limit passes, one byte over is refused, at the
    /// top level, inside a map and for a key attribute.
    #[test]
    fn an_attribute_name_over_its_limit_is_refused_at_the_boundary() {
        let limits = RestorableLimits {
            max_attribute_name_bytes: 6,
            ..limits(MAX)
        };
        let ok = item(&[("pk", s("a")), ("abcdef", n("1"))]);
        validate_restorable_item(&ok, &hash_only(), &limits).unwrap();
        let top = item(&[("pk", s("a")), ("abcdefg", n("1"))]);
        let e = validate_restorable_item(&top, &hash_only(), &limits).unwrap_err();
        assert_eq!(e.path, "abcdefg");
        assert_eq!(
            e.reason,
            RestorableItemReason::AttributeNameTooLong { size: 7, max: 6 }
        );
        assert_eq!(
            e.to_string(),
            r#"key {"pk":{"S":"a"}}, attribute abcdefg: attribute name of 7 bytes exceeds the limit of 6 bytes"#
        );
        let nested = AttributeValue::M(BTreeMap::from([("abcdefg".to_owned(), n("1"))]));
        let it = item(&[("pk", s("a")), ("m", nested)]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(e.path, "m.abcdefg");
        assert_eq!(
            e.reason,
            RestorableItemReason::AttributeNameTooLong { size: 7, max: 6 }
        );
        let key = RestorableKeySchema::resolve(
            &[element("abcdefg", KeyType::Hash)],
            &[definition("abcdefg", ScalarAttributeType::S)],
        )
        .unwrap();
        let it = item(&[("abcdefg", s("a"))]);
        let e = validate_restorable_item(&it, &key, &limits).unwrap_err();
        assert_eq!(e.path, "abcdefg");
        assert_eq!(
            e.reason,
            RestorableItemReason::AttributeNameTooLong { size: 7, max: 6 }
        );
    }

    /// The limits are the instance's: `RestorableLimits` carries the four
    /// sizes of `LimitsConfig`.
    #[test]
    fn restorable_limits_are_built_from_the_limits_config() {
        let config = LimitsConfig {
            max_item_size_bytes: 1,
            max_partition_key_size_bytes: 2,
            max_sort_key_size_bytes: 3,
            max_attribute_name_bytes: 4,
            ..LimitsConfig::default()
        };
        assert_eq!(
            RestorableLimits::from(&config),
            RestorableLimits {
                max_item_size_bytes: 1,
                max_partition_key_size_bytes: 2,
                max_sort_key_size_bytes: 3,
                max_attribute_name_bytes: 4,
            }
        );
    }

    /// Keys of ordinary size are shown whole, every attribute present, as
    /// `Display` writes them: a UUID hash key with a ten-digit range key, a
    /// 58-byte hash key with range `1`, a 2048-byte hash key with a
    /// 1024-byte range key (the largest legal key), and a hash key of
    /// quotes, whose escaped text is twice its bytes.
    #[test]
    fn the_shown_key_carries_every_attribute_of_a_legal_key_whole() {
        let uuid = "39518456-7a1b-4c2d-9e3f-0a1b2c3d832c";
        let it = item(&[
            ("pk", s(uuid)),
            ("sk", n("1761955200")),
            ("amount", n("abc")),
        ]);
        let e = refused(&it, &composite(), MAX);
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{uuid}"}},"sk":{{"N":"1761955200"}}}}"#)
        );
        assert_eq!(
            e.to_string(),
            format!(
                r#"key {{"pk":{{"S":"{uuid}"}},"sk":{{"N":"1761955200"}}}}, attribute amount: The parameter cannot be converted to a numeric value: abc"#
            )
        );
        let tenant = "tenant-42/orders/2026-10-31/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        assert_eq!(tenant.len(), 58);
        let it = item(&[("pk", s(tenant)), ("sk", n("1")), ("amount", n("abc"))]);
        let e = refused(&it, &composite(), MAX);
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{tenant}"}},"sk":{{"N":"1"}}}}"#)
        );
        let hash = "h".repeat(2048);
        let range = "r".repeat(1024);
        let key = RestorableKeySchema::resolve(
            &[element("pk", KeyType::Hash), element("sk", KeyType::Range)],
            &[
                definition("pk", ScalarAttributeType::S),
                definition("sk", ScalarAttributeType::S),
            ],
        )
        .unwrap();
        let it = item(&[("pk", s(&hash)), ("sk", s(&range)), ("amount", n("abc"))]);
        let e = refused(&it, &key, MAX);
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{hash}"}},"sk":{{"S":"{range}"}}}}"#)
        );
        let quotes = "\"".repeat(2048);
        let it = item(&[("pk", s(&quotes)), ("amount", n("abc"))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(
            e.key,
            format!(
                r#"{{"pk":{{"S":{}}}}}"#,
                serde_json::to_string(&quotes).unwrap()
            )
        );
        // A `B` key is its base64 text, the format's rendering.
        let it = item(&[
            ("pk", s("a")),
            ("sk", n("1")),
            ("sk2", AttributeValue::B(vec![0xff; 2048])),
            ("amount", n("abc")),
        ]);
        let e = refused(&it, &multi_part(), MAX);
        assert!(
            e.key
                .ends_with(&format!(r#""sk2":{{"B":"{}"}}}}"#, "/".repeat(2730) + "8=")),
            "{}",
            e.key
        );
    }

    /// Only an oversized value is cut, and on its own: a 13,000-byte hash
    /// key (over `SHOWN_KEY_VALUE_BYTES`, under a raised key limit) is cut
    /// to its prefix and length while the range key beside it is shown
    /// whole; a 3000-byte key, over the service's limit but under the
    /// bound, is whole; the bound is on the escaped text, so a key of 6145
    /// quotes (12,290 escaped bytes) is cut and a key of 6144 is not; a cut
    /// value is still a JSON string. A key attribute's name is bounded at
    /// `SHOWN_VALUE_BYTES` (one of 4097 bytes is cut), and a key attribute
    /// holding a list is its JSON, cut to a JSON string when oversized.
    #[test]
    fn an_oversized_key_value_is_cut_on_its_own() {
        let limits = RestorableLimits {
            max_partition_key_size_bytes: 20_000,
            ..limits(10)
        };
        let long = "k".repeat(13_000);
        let it = item(&[("pk", s(&long)), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert!(matches!(
            e.reason,
            RestorableItemReason::TooLarge { max: 10, .. }
        ));
        assert_eq!(
            e.key,
            format!(
                r#"{{"pk":{{"S":"{}... <13000 bytes>"}},"sk":{{"N":"7"}}}}"#,
                "k".repeat(SHOWN_VALUE_PREFIX_BYTES)
            )
        );
        let over_the_service_limit = "k".repeat(3000);
        let it = item(&[("pk", s(&over_the_service_limit)), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{over_the_service_limit}"}},"sk":{{"N":"7"}}}}"#)
        );
        let parsed: serde_json::Value = serde_json::from_str(&e.key).unwrap();
        assert_eq!(parsed["sk"]["N"], "7");
        let plain = "p".repeat(SHOWN_KEY_VALUE_BYTES);
        let it = item(&[("pk", s(&plain)), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{plain}"}},"sk":{{"N":"7"}}}}"#)
        );
        let quotes = "\"".repeat(SHOWN_KEY_VALUE_BYTES / 2);
        let it = item(&[("pk", s(&quotes)), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert_eq!(
            e.key,
            format!(
                r#"{{"pk":{{"S":{}}},"sk":{{"N":"7"}}}}"#,
                serde_json::to_string(&quotes).unwrap()
            )
        );
        let quotes = "\"".repeat(SHOWN_KEY_VALUE_BYTES / 2 + 1);
        let it = item(&[("pk", s(&quotes)), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert!(e.key.contains("... <6145 bytes>"), "{}", e.key);
        assert!(e.key.ends_with(r#","sk":{"N":"7"}}"#), "{}", e.key);
        let parsed: serde_json::Value = serde_json::from_str(&e.key).unwrap();
        assert_eq!(
            parsed["pk"]["S"].as_str().unwrap().len(),
            512 + "... <6145 bytes>".len()
        );
        // The whole key stays bounded whatever the key: two 100,000-byte
        // values and a 100,000-byte name.
        let huge = "x".repeat(100_000);
        let key = RestorableKeySchema::resolve(
            &[element(&huge, KeyType::Hash), element("sk", KeyType::Range)],
            &[
                definition(&huge, ScalarAttributeType::S),
                definition("sk", ScalarAttributeType::S),
            ],
        )
        .unwrap();
        let it = item(&[(&huge, s(&huge)), ("sk", s(&huge))]);
        let limits = RestorableLimits {
            max_partition_key_size_bytes: 200_000,
            max_sort_key_size_bytes: 200_000,
            max_attribute_name_bytes: 200_000,
            ..self::limits(10)
        };
        let e = validate_restorable_item(&it, &key, &limits).unwrap_err();
        assert!(
            e.key.len() < 3 * (SHOWN_VALUE_PREFIX_BYTES + 40),
            "{}",
            e.key.len()
        );
        assert_eq!(e.key.matches("... <100000 bytes>").count(), 3);
        // A key attribute's name is bounded at `SHOWN_VALUE_BYTES`, the
        // bound a name gets, not the key value's: one of 4097 bytes is cut.
        let long_name = "k".repeat(SHOWN_VALUE_BYTES + 1);
        let key = RestorableKeySchema::resolve(
            &[element(&long_name, KeyType::Hash)],
            &[definition(&long_name, ScalarAttributeType::S)],
        )
        .unwrap();
        let it = item(&[(long_name.as_str(), s("a")), ("amount", n("abc"))]);
        let limits = RestorableLimits {
            max_attribute_name_bytes: 10_000,
            ..self::limits(MAX)
        };
        let e = validate_restorable_item(&it, &key, &limits).unwrap_err();
        assert_eq!(e.path, "amount");
        assert_eq!(
            e.key,
            format!(
                r#"{{"{}... <4097 bytes>":{{"S":"a"}}}}"#,
                "k".repeat(SHOWN_VALUE_PREFIX_BYTES)
            )
        );
        // A key attribute holding a value that is not a scalar (a list of
        // 20,000 bytes under `pk`) renders as its JSON cut to a JSON string:
        // the key still parses and stays under 1,200 bytes.
        let list = AttributeValue::L(vec![s(&"x".repeat(20_000))]);
        let list_json_len = serde_json::to_string(&list).unwrap().len();
        assert!(list_json_len > SHOWN_KEY_VALUE_BYTES);
        let it = item(&[("pk", list), ("sk", n("7"))]);
        let e = validate_restorable_item(&it, &composite(), &self::limits(MAX)).unwrap_err();
        assert!(e.key.len() < 1200, "{}", e.key.len());
        let parsed: serde_json::Value = serde_json::from_str(&e.key).unwrap();
        assert!(
            parsed["pk"]
                .as_str()
                .unwrap()
                .ends_with(&format!("... <{list_json_len} bytes>")),
            "{}",
            e.key
        );
        assert_eq!(parsed["sk"]["N"], "7");
    }

    /// The attribute path is bounded segment by segment with the same rule:
    /// a 60,000-byte attribute name holding a map with a 60,000-byte member
    /// gives a path of two cut segments, each with its length; a name of
    /// `SHOWN_VALUE_BYTES` bytes is whole and one byte more is cut.
    #[test]
    fn the_attribute_path_is_bounded_segment_by_segment() {
        let limits = RestorableLimits {
            max_attribute_name_bytes: 100_000,
            ..limits(MAX)
        };
        let long_name = "a".repeat(60_000);
        let long_member = "b".repeat(60_000);
        let inner = AttributeValue::M(BTreeMap::from([(long_member, n("abc"))]));
        let it = item(&[("pk", s("a")), (&long_name, inner)]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(
            e.path,
            format!(
                "{}... <60000 bytes>.{}... <60000 bytes>",
                "a".repeat(SHOWN_VALUE_PREFIX_BYTES),
                "b".repeat(SHOWN_VALUE_PREFIX_BYTES)
            )
        );
        assert!(e.to_string().len() < 3000, "{}", e.to_string().len());
        let whole = "w".repeat(SHOWN_VALUE_BYTES);
        let it = item(&[("pk", s("a")), (&whole, n("abc"))]);
        assert_eq!(
            validate_restorable_item(&it, &hash_only(), &limits)
                .unwrap_err()
                .path,
            whole
        );
        let over = "w".repeat(SHOWN_VALUE_BYTES + 1);
        let it = item(&[("pk", s("a")), (&over, n("abc"))]);
        assert_eq!(
            validate_restorable_item(&it, &hash_only(), &limits)
                .unwrap_err()
                .path,
            format!("{}... <4097 bytes>", "w".repeat(SHOWN_VALUE_PREFIX_BYTES))
        );
        // A set member over the bound is cut in the reason, as a plain `N`
        // over it is: the wire's echo of the input is the one text that can
        // grow with the item.
        let member = format!("1E{}", "9".repeat(SHOWN_VALUE_BYTES));
        let set = AttributeValue::NS(BTreeSet::from([member.clone()]));
        let it = item(&[("pk", s("a")), ("ns", set)]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert!(e.to_string().len() < 2000, "{}", e.to_string().len());
        assert!(
            e.to_string()
                .ends_with(&format!("<{} bytes>", member.len())),
            "{e}"
        );
        let it = item(&[("pk", s("a")), ("n", n(&member))]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(
            e.reason,
            RestorableItemReason::InvalidNumber(format!(
                "The parameter cannot be converted to a numeric value: {}... <{} bytes>",
                &member[..SHOWN_VALUE_PREFIX_BYTES],
                member.len()
            ))
        );
        // In a key the value is echoed twice: whole in the key (under the
        // key bound) and cut in the reason.
        let it = item(&[("pk", s("a")), ("sk", n(&member))]);
        let e = validate_restorable_item(&it, &composite(), &limits).unwrap_err();
        assert!(
            e.to_string().len() < member.len() + SHOWN_VALUE_PREFIX_BYTES + 400,
            "{}",
            e.to_string().len()
        );
        assert!(e.key.contains(&member), "{}", e.key);
        assert_eq!(e.to_string().matches("... <4098 bytes>").count(), 1, "{e}");
    }

    /// `LineTooLong`, the reason the backup job's feeder builds for an item
    /// whose data file line would exceed the cap, names the cap: the
    /// measurement stops at the cap, so the line's own length is unknown.
    #[test]
    fn a_line_too_long_reason_names_the_cap() {
        let e = RestorableItemError {
            key: r#"{"pk":{"S":"a"}}"#.to_owned(),
            path: String::new(),
            reason: RestorableItemReason::LineTooLong { cap: 33_554_432 },
        };
        assert_eq!(
            e.to_string(),
            r#"key {"pk":{"S":"a"}}: data file line exceeds the line cap of 33554432 bytes"#
        );
    }

    /// A control character, or one of the line separators U+0085, U+2028 and
    /// U+2029, in an attribute name or in the number the wire's reason
    /// echoes is written escaped, as a JSON string's content (`\n`, `\t`,
    /// `\"`, `\\`, `\u00XX`, `\u2028`), so a tenant's item cannot add a
    /// line of its own to the log: the message holds no raw newline and no
    /// separator, and the key beside it is the same escaped rendering.
    #[test]
    fn a_control_character_in_a_name_or_a_number_is_escaped_in_the_message() {
        let it = item(&[
            ("pk", s("a\nb")),
            ("amount", n("abc\nFORGED LINE: looks like another record")),
        ]);
        let e = refused(&it, &hash_only(), MAX);
        let message = e.to_string();
        assert!(!message.contains('\n'), "{message:?}");
        assert_eq!(
            message,
            r#"key {"pk":{"S":"a\nb"}}, attribute amount: The parameter cannot be converted to a numeric value: abc\nFORGED LINE: looks like another record"#
        );
        let name = "att\nFORGED LINE TWO\t\"quoted\" \\ \u{1}";
        let it = item(&[("pk", s("a")), (name, n("abc"))]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, r#"att\nFORGED LINE TWO\t\"quoted\" \\ \u0001"#);
        assert!(!e.to_string().contains('\n'), "{e}");
        // In a map, in a set member and in a key attribute's name the same.
        let set = AttributeValue::NS(BTreeSet::from(["1".to_owned(), "x\ny".to_owned()]));
        let it = item(&[("pk", s("a")), ("ns", set)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, "ns[1]");
        assert_eq!(
            e.reason,
            RestorableItemReason::InvalidNumber(
                r"The parameter cannot be converted to a numeric value: x\ny".to_owned()
            )
        );
        let inner = AttributeValue::M(BTreeMap::from([("c\rd".to_owned(), n("abc"))]));
        let it = item(&[("pk", s("a")), ("m\u{c}", inner)]);
        let e = refused(&it, &hash_only(), MAX);
        assert_eq!(e.path, r"m\f.c\rd");
        let key = RestorableKeySchema::resolve(
            &[element("p\nk", KeyType::Hash)],
            &[definition("p\nk", ScalarAttributeType::S)],
        )
        .unwrap();
        let it = item(&[("p\nk", s(""))]);
        let e = validate_restorable_item(&it, &key, &limits(MAX)).unwrap_err();
        assert_eq!(e.path, r"p\nk");
        assert_eq!(e.key, r#"{"p\nk":{"S":""}}"#);
        // The three Unicode line separators, which `serde_json` leaves raw
        // and a reader that breaks lines the Unicode way breaks on, in a
        // key value, a name and a number: each is written as its `\uXXXX`
        // escape and none reaches the message.
        let separators = "\u{85}\u{2028}\u{2029}";
        let name = format!("att{separators}");
        let it = item(&[
            ("pk", s(&format!("a{separators}b"))),
            (name.as_str(), n(&format!("abc{separators}FORGED LINE"))),
        ]);
        let e = refused(&it, &hash_only(), MAX);
        let message = e.to_string();
        assert!(
            !message
                .chars()
                .any(|c| matches!(c, '\u{85}' | '\u{2028}' | '\u{2029}')),
            "{message:?}"
        );
        assert_eq!(e.key, r#"{"pk":{"S":"a\u0085\u2028\u2029b"}}"#);
        assert_eq!(e.path, r"att\u0085\u2028\u2029");
        assert_eq!(
            e.reason,
            RestorableItemReason::InvalidNumber(
                r"The parameter cannot be converted to a numeric value: abc\u0085\u2028\u2029FORGED LINE"
                    .to_owned()
            )
        );
    }

    /// A key value is bounded at `SHOWN_KEY_VALUE_BYTES`, six times the hash
    /// key limit: a 700-byte hash key of U+0001 (4,200 escaped bytes, over
    /// the bound a name gets) is shown whole, and a 2049-byte one (12,294
    /// escaped bytes, over the legal key size) is cut.
    #[test]
    fn a_legal_key_of_control_characters_is_shown_whole() {
        let limits = RestorableLimits {
            max_partition_key_size_bytes: 4096,
            ..limits(MAX)
        };
        let legal = "\u{1}".repeat(700);
        let it = item(&[("pk", s(&legal)), ("amount", n("abc"))]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(e.path, "amount");
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{}"}}}}"#, r"\u0001".repeat(700))
        );
        let largest = "\u{1}".repeat(2048);
        let it = item(&[("pk", s(&largest)), ("amount", n("abc"))]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(
            e.key,
            format!(r#"{{"pk":{{"S":"{}"}}}}"#, r"\u0001".repeat(2048))
        );
        let over = "\u{1}".repeat(2049);
        let it = item(&[("pk", s(&over)), ("amount", n("abc"))]);
        let e = validate_restorable_item(&it, &hash_only(), &limits).unwrap_err();
        assert_eq!(
            e.key,
            format!(
                r#"{{"pk":{{"S":"{}... <2049 bytes>"}}}}"#,
                r"\u0001".repeat(SHOWN_VALUE_PREFIX_BYTES / 6)
            )
        );
        let parsed: serde_json::Value = serde_json::from_str(&e.key).unwrap();
        assert_eq!(parsed["pk"]["S"].as_str().unwrap().len(), 170 + 16);
    }

    /// The depth rule through lists, and through lists and maps mixed: at
    /// the limit refused, one short of it passed, the path naming each
    /// level, and the request path's rule agreeing on every shape.
    #[test]
    fn nesting_through_lists_and_mixed_levels_is_measured_the_same() {
        let limit = super::super::MAX_ITEM_NESTING_DEPTH;
        let lists = |levels: usize| {
            let mut v = n("1");
            for _ in 0..levels {
                v = AttributeValue::L(vec![v]);
            }
            item(&[("pk", s("a")), ("deep", v)])
        };
        let mixed = |levels: usize| {
            let mut v = n("1");
            for i in 0..levels {
                v = if i % 2 == 0 {
                    AttributeValue::L(vec![v])
                } else {
                    AttributeValue::M(BTreeMap::from([("m".to_owned(), v)]))
                };
            }
            item(&[("pk", s("a")), ("deep", v)])
        };
        for (what, nest) in [
            ("lists", &lists as &dyn Fn(usize) -> Item),
            ("mixed", &mixed as &dyn Fn(usize) -> Item),
        ] {
            validate_restorable_item(&nest(limit - 1), &hash_only(), &limits(MAX))
                .unwrap_or_else(|e| panic!("{what}, one short of the limit: {e}"));
            let e = refused(&nest(limit), &hash_only(), MAX);
            assert_eq!(e.reason, RestorableItemReason::TooDeep, "{what}");
            super::super::validate_item_nesting_depth(&nest(limit - 1))
                .unwrap_or_else(|e| panic!("{what}: the request path refuses one short: {e}"));
            super::super::validate_item_nesting_depth(&nest(limit)).unwrap_err();
        }
        let e = refused(&lists(limit), &hash_only(), MAX);
        assert_eq!(e.path, format!("deep{}", "[0]".repeat(limit - 1)));
        let e = refused(&mixed(limit), &hash_only(), MAX);
        // Built inside out: the outermost level is the last one wrapped.
        let mut path = "deep".to_owned();
        for i in (0..limit - 1).rev() {
            path.push_str(if (i + 1) % 2 == 0 { "[0]" } else { ".m" });
        }
        assert_eq!(e.path, path);
    }
}
