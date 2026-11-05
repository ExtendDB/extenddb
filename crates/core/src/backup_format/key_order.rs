// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The one definition of base-table key order for the backup format.
//!
//! A `key_ordered` backup promises its items ascend in this order within each
//! data file and from one file to the next:
//!
//! - Hash key: the text encoding [`hash_key_text`] produces (`S` as is, `N`
//!   as the string held, `B` as standard base64 with `=` padding), compared
//!   as UTF-8 bytes. A number hash key therefore sorts as text (`"10"`
//!   before `"9"`), and a binary one by its base64 text, not its bytes
//!   (`[0xff]` is `/w==` and sorts before `[0x00]`, `AA==`); this is how the
//!   storage layer keys rows and the format inherits it rather than
//!   defining a second order. The base64 text of a `B` hash key is the very
//!   string that appears in the JSON of a `key_range` endpoint or a data
//!   file line (`{"B":"/w=="}`), so a reader compares what it sees.
//!
//!   [`KeyOrder::compare`] takes an `N` value as written; it does not
//!   normalize. The wire `AttributeValue` deserializer rewrites every valid
//!   number it parses to its normalized form (`5.0` to `5`, `1.5E3` to
//!   `1500`), which covers request items and data file lines alike, so a
//!   stored valid number holds the normalized string. It keeps an invalid
//!   number as written, and whether that reaches a backend depends on the
//!   operation. `PutItem` refuses it: its validation runs the item number
//!   check (`validate_item_numbers`) before a backend stores the item.
//!   `BatchWriteItem` and `TransactWriteItems` do not, and store an invalid
//!   `N` key value as written (`1E126`, `abc` and `1E-131` are stored and
//!   returned by `Scan` as written; #618). `UpdateItem` on an absent item,
//!   which creates the item from its `Key`, and `ImportTable` do not run
//!   that check on the key either; `UpdateItem` cannot change the key of an
//!   existing item, and a restore validates every item. So a table can hold
//!   an `N` key that [`KeyOrder::validate_key_item`] refuses.
//!
//!   A key-ordered writer that meets such a stored key refuses the backup
//!   with an error naming the key. The format cannot carry the key as a
//!   `key_range` endpoint ([`KeyOrder::validate_key_item`] refuses it, so
//!   [`super::BackupDocuments::build`] cannot write the manifest), and the
//!   restore's item validation would refuse the item anyway. The writer
//!   passes every item to a [`super::KeyRangeTracker`], which checks the
//!   item's key as [`KeyOrder::endpoint_key`] does and refuses such a key
//!   wherever it falls in a file, not only at a file's first or last item;
//!   [`super::KeyRangeCheck`] is that tracker plus the recorded range, so a
//!   reader refuses a key-ordered file holding such a key as well.
//!
//!   A `key_range` endpoint is read differently: the manifest parser reads
//!   its `N` strings as written, before the `AttributeValue` deserializer
//!   would rewrite them, and refuses one not in normalized form
//!   ([`super::KeyRange`]), so the stored bytes and the parsed range agree;
//!   [`KeyOrder::validate_key_item`] refuses the same for an endpoint built
//!   in code. A caller that builds items in code and compares them must
//!   normalize its `N` values first.
//! - Range key: `S` as UTF-8 bytes, `B` as raw bytes (a shorter prefix
//!   first), `N` by exact decimal value.
//!
//! What another implementation must reproduce to compute this order (the
//! Rust code is `crate::validation::number::validate_and_normalize_number`
//! and `KeyOrder::compare`; the test `doc_examples_normalize_as_stated`
//! pins the examples below):
//!
//! - `N` normalization. Accepted input: an optional `-` or `+`, digits with
//!   at most one `.` and at least one digit, an optional `e` or `E` followed
//!   by an optionally signed decimal exponent that fits a signed 32-bit
//!   integer (`-2147483648` to `2147483647`); no whitespace, no other
//!   characters. An exponent outside that range is refused whatever the
//!   value would be: `0E2147483648` and `0E-2147483649` are refused, while
//!   `0E2147483647` and `0.0E-2147483648` are `0`. The value must have at most 38 significant digits after
//!   leading and trailing zeros are dropped and a magnitude within
//!   10^-130 to just under 10^126 (`1E-130` and `9.9E125` are accepted,
//!   `1E-131` and `1E126` refused). Output: plain decimal without an
//!   exponent, a `-` only for a negative value, no leading zeros except the
//!   single `0` before a `.`, no trailing fractional zeros and no trailing
//!   `.`, zero as `0` (`-0`, `-0.0` and `0E5` are all `0`). So `00042` is
//!   `42`, `1.50` is `1.5`, `1.5E3` is `1500`, `+1` is `1`, `.5` is `0.5`,
//!   `1E+125` is `1` followed by 125 zeros (126 characters), and `1E-130` is
//!   `0.` followed by 129 zeros and `1` (132 characters). A hash-key `N`
//!   compares as this string, bytewise, so the whole order of `N` hash keys
//!   depends on these rules.
//! - `B`: standard base64 (`A`-`Z`, `a`-`z`, `0`-`9`, `+`, `/`) with `=`
//!   padding in canonical form, not the URL-safe alphabet and not unpadded.
//! - `S`: compared as the UTF-8 bytes of the string, not as code points,
//!   UTF-16 code units (`JavaScript`'s default string comparison) or a
//!   collation; `é` sorts after `z`.
//! - `N` range keys: exact decimal comparison (`BigDecimal` here).
//!   Doubles are insufficient: two keys that first differ in the 38th
//!   significant digit are distinct keys in this order and equal as `f64`.
//!
//! The SQL backends can produce this order from their key columns, but only
//! with the right collation on the query, whatever the column declares:
//!
//! - SQLite: `ORDER BY pk, sk_s` (or `sk_b`) with the default `BINARY`
//!   collation is memcmp; `ORDER BY pk, sk_n` is numeric because `sk_n` holds
//!   `encode_orderable_number`'s order-preserving text encoding. The
//!   `storage-sqlite` test `number_key_order_matches_backup_format` binds
//!   that encoding to [`KeyOrder::compare`].
//! - Postgres: `ORDER BY pk COLLATE "C", sk_s COLLATE "C"`; `sk_n` is
//!   `NUMERIC` and `sk_b` is `bytea`, both already in the format's order.
//!   Tables created before the key columns were declared `COLLATE "C"` keep
//!   the database collation (`en_US.utf8` in the default setups, where
//!   `a < B` and punctuation is weak), so a writer that omits the collation
//!   on `pk` or `sk_s` emits a file that is not in this order.
//!
//! Backends that cannot produce this order write `unordered`. The manifest
//! parser binds [`KeyOrder::compare`] for its `key_range` checks, and
//! [`super::KeyRangeTracker`] (the writer's) and [`super::KeyRangeCheck`]
//! (the reader's, run by [`super::ExtenddbManifest::reader`] over each
//! file's items) compare in the same order for ascent (the tracker across
//! every file it spans, the check within its file) and the endpoints, with
//! the hash key's text bytewise and the range key parsed
//! once per item; the storage layer's
//! `pk_to_text` binds [`hash_key_text`], so for the hash key the writer, the
//! reader, and the row key agree by construction. (Other copies of the
//! `S`/`N`/`B`-to-text mapping remain, none on the backup path: hash keys
//! for stream shard assignment in `storage-postgres/src/data/tx_helpers.rs`,
//! `storage-sqlite/src/data/tx_helpers.rs`,
//! `storage-cassandra/src/stream_util.rs` and
//! `storage-mongodb/src/data_engine.rs`; sort keys in
//! `storage-mongodb/src/data/mod.rs` (`sk_to_text`, a verbatim copy of
//! [`hash_key_text`]), `storage-mongodb/src/data_engine.rs`
//! and `storage-cassandra/src/data/transactions.rs`; binary values in
//! `storage-mongodb/src/condition.rs`. Binding them is a follow-up.)
//! Range-key agreement rests on each backend's column order matching the
//! rules above: the `storage-sqlite` test named above binds `sk_n`, and the
//! storage layer's snapshot conformance check orders with
//! [`KeyOrder::compare`].

use std::borrow::Cow;
use std::cmp::Ordering;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bigdecimal::BigDecimal;

use super::FormatError;
use super::error::truncated;
use crate::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use crate::validation::number::validate_and_normalize_number;

/// Text encoding of a hash key value: the row key the SQL backends store.
/// `None` for a value that cannot be a hash key.
#[must_use]
pub fn hash_key_text(value: &AttributeValue) -> Option<Cow<'_, str>> {
    match value {
        AttributeValue::S(s) => Some(Cow::Borrowed(s)),
        AttributeValue::N(n) => Some(Cow::Borrowed(n)),
        AttributeValue::B(b) => Some(Cow::Owned(BASE64.encode(b))),
        _ => None,
    }
}

/// A table's key shape, resolved once from its key schema and attribute
/// definitions, able to compare and validate items' keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOrder {
    hash: (String, ScalarAttributeType),
    range: Option<(String, ScalarAttributeType)>,
}

impl KeyOrder {
    /// Resolve the key shape. Only hash-only and single-range-key tables have
    /// a format-defined order; a multi-part range key returns
    /// [`FormatError::UnorderableKeySchema`], and a writer for such a table
    /// writes `unordered`.
    ///
    /// # Errors
    ///
    /// [`FormatError::UnorderableKeySchema`] when the schema has no hash key,
    /// more than one hash or range key, a range key listed before the hash
    /// key (`DynamoDB` requires `HASH` first), a range key with the hash
    /// key's name, a key attribute without a definition, or an attribute
    /// defined more than once.
    pub fn resolve(
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
    ) -> Result<Self, FormatError> {
        let type_of = |name: &str| {
            let mut matching = attribute_definitions
                .iter()
                .filter(|d| d.attribute_name == name)
                .map(|d| d.attribute_type);
            let ty = matching.next().ok_or_else(|| {
                FormatError::UnorderableKeySchema(format!(
                    "key attribute {:?} has no attribute definition",
                    truncated(name)
                ))
            })?;
            if matching.next().is_some() {
                return Err(FormatError::UnorderableKeySchema(format!(
                    "key attribute {:?} has more than one attribute definition",
                    truncated(name)
                )));
            }
            Ok(ty)
        };
        let mut hash = None;
        let mut range = None;
        for element in key_schema {
            if element.key_type == KeyType::Range && hash.is_none() {
                return Err(FormatError::UnorderableKeySchema(
                    "the range key is listed before the hash key".to_owned(),
                ));
            }
            let slot = match element.key_type {
                KeyType::Hash => &mut hash,
                KeyType::Range => &mut range,
            };
            if slot.is_some() {
                return Err(FormatError::UnorderableKeySchema(
                    "multi-part keys have no format-defined order".to_owned(),
                ));
            }
            *slot = Some((
                element.attribute_name.clone(),
                type_of(&element.attribute_name)?,
            ));
        }
        let hash: (String, ScalarAttributeType) = hash.ok_or_else(|| {
            FormatError::UnorderableKeySchema("key schema has no hash key".to_owned())
        })?;
        if range.as_ref().is_some_and(|(name, _)| *name == hash.0) {
            return Err(FormatError::UnorderableKeySchema(format!(
                "the range key and the hash key are both {:?}",
                truncated(&hash.0)
            )));
        }
        Ok(Self { hash, range })
    }

    /// Names of the key attributes, hash first.
    pub fn attribute_names(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.hash.0.as_str()).chain(self.range.iter().map(|(n, _)| n.as_str()))
    }

    /// Check that `key` holds exactly the key attributes with the declared
    /// types and nothing else: the shape [`super::KeyRange`] carries. An `N`
    /// value must be a valid `DynamoDB` number in its normalized form, and
    /// an `S` or `B` value must not be empty (`DynamoDB` refuses an empty key
    /// value). A parsed manifest's `N` endpoints were already checked for
    /// normalized form as written ([`super::KeyRange`]), so for parsed input
    /// this refuses invalid numbers (`1E200`, `1_000`, 39 significant
    /// digits) and the normalized-form check guards items built in code. Key
    /// lengths (2048 bytes for a hash key, 1024 for a range key) are not
    /// checked here; the restore's item validation checks them.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidKey`] naming what is wrong; for an invalid
    /// number, the number validation's reason (`Number overflow. ...`).
    pub fn validate_key_item(&self, key: &Item) -> Result<(), FormatError> {
        let expected: usize = 1 + usize::from(self.range.is_some());
        if key.len() != expected {
            return Err(FormatError::InvalidKey(format!(
                "key has {} attributes, the key schema has {expected}",
                key.len()
            )));
        }
        for (name, ty) in std::iter::once(&self.hash).chain(self.range.iter()) {
            let value = key.get(name).ok_or_else(|| {
                FormatError::InvalidKey(format!("key attribute {:?} missing", truncated(name)))
            })?;
            check_key_value(name, *ty, value)?;
        }
        Ok(())
    }

    /// The key attributes of `item` checked as [`Self::validate_key_item`]
    /// checks a key, without building one: the values borrowed from `item`,
    /// an `N` range key parsed once for comparison. Other attributes of
    /// `item` are not looked at. The errors are [`Self::endpoint_key`]'s.
    pub(super) fn checked_key<'a>(&self, item: &'a Item) -> Result<CheckedKey<'a>, FormatError> {
        let (hash, range) = self.key_values(item)?;
        let range = match (&self.range, range) {
            (Some((name, _)), Some(value)) => Some(match value {
                AttributeValue::S(s) => RangeKey::S(s),
                AttributeValue::B(b) => RangeKey::B(b),
                AttributeValue::N(n) => RangeKey::N(n, Cow::Owned(parse_number(name, n)?)),
                _ => unreachable!("check_key_value accepts S, N and B only"),
            }),
            _ => None,
        };
        Ok(CheckedKey {
            hash,
            hash_text: hash_key_text(hash).expect("check_key_value accepts S, N and B only"),
            range,
        })
    }

    /// The hash and range key values of `item`, each checked with
    /// [`check_key_value`]; an error names the key (bounded).
    fn key_values<'a>(
        &self,
        item: &'a Item,
    ) -> Result<(&'a AttributeValue, Option<&'a AttributeValue>), FormatError> {
        let value_of = |name: &str| {
            item.get(name).ok_or_else(|| {
                FormatError::InvalidKey(format!("item has no key attribute {:?}", truncated(name)))
            })
        };
        let hash = value_of(&self.hash.0)?;
        let range = match &self.range {
            Some((name, _)) => Some(value_of(name)?),
            None => None,
        };
        let checked = check_key_value(&self.hash.0, self.hash.1, hash).and_then(|()| {
            match (&self.range, range) {
                (Some((name, ty)), Some(value)) => check_key_value(name, *ty, value),
                _ => Ok(()),
            }
        });
        checked.map_err(|e| match e {
            FormatError::InvalidKey(reason) => {
                let shown = serde_json::to_string(&self.key_item(hash, range)).unwrap_or_default();
                FormatError::InvalidKey(format!(
                    "item key {} cannot be a key_range endpoint: {reason}",
                    truncated(&shown)
                ))
            }
            other => other,
        })?;
        Ok((hash, range))
    }

    /// A key-only item of the given values.
    fn key_item(&self, hash: &AttributeValue, range: Option<&AttributeValue>) -> Item {
        let mut key = Item::new();
        key.insert(self.hash.0.clone(), hash.clone());
        if let (Some((name, _)), Some(value)) = (&self.range, range) {
            key.insert(name.clone(), value.clone());
        }
        key
    }

    /// The key of `item` as a [`super::KeyRange`] endpoint and as
    /// [`super::KeyRangeCheck`] compares it: the key attributes only (other
    /// attributes dropped), checked with [`Self::validate_key_item`]. A
    /// key-ordered writer does not call it per item:
    /// [`super::KeyRangeTracker`] makes the same check on each item without
    /// building the key, and builds the first and last as the file's range;
    /// an error refuses the backup (module docs: a stored `N` key such as
    /// `abc` cannot be an endpoint, and the restore would refuse the item).
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidKey`] when a key attribute is missing or the
    /// key fails [`Self::validate_key_item`]; the message names the key
    /// (bounded) and the reason.
    pub fn endpoint_key(&self, item: &Item) -> Result<Item, FormatError> {
        let (hash, range) = self.key_values(item)?;
        Ok(self.key_item(hash, range))
    }

    /// Compare two items by their keys. Both must pass
    /// [`Self::validate_key_item`] on their key attributes (extra attributes
    /// are ignored here, so whole items can be compared).
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidKey`] when a key attribute is missing, has the
    /// wrong type, or (for `N`) does not parse as a number. The last case is
    /// defensive: a value that passed [`Self::validate_key_item`] always
    /// parses.
    pub fn compare(&self, a: &Item, b: &Item) -> Result<Ordering, FormatError> {
        let ha = hash_text(a, &self.hash)?;
        let hb = hash_text(b, &self.hash)?;
        match ha.as_bytes().cmp(hb.as_bytes()) {
            Ordering::Equal => {}
            other => return Ok(other),
        }
        let Some((name, ty)) = &self.range else {
            return Ok(Ordering::Equal);
        };
        let missing =
            || FormatError::InvalidKey(format!("range key {:?} missing", truncated(name)));
        let ra = a.get(name).ok_or_else(missing)?;
        let rb = b.get(name).ok_or_else(missing)?;
        match (ty, ra, rb) {
            (ScalarAttributeType::S, AttributeValue::S(x), AttributeValue::S(y)) => {
                Ok(x.as_bytes().cmp(y.as_bytes()))
            }
            (ScalarAttributeType::B, AttributeValue::B(x), AttributeValue::B(y)) => Ok(x.cmp(y)),
            (ScalarAttributeType::N, AttributeValue::N(x), AttributeValue::N(y)) => {
                Ok(parse_number(name, x)?.cmp(&parse_number(name, y)?))
            }
            _ => Err(FormatError::InvalidKey(format!(
                "range key {:?} does not match its declared type {ty:?}",
                truncated(name)
            ))),
        }
    }
}

fn hash_text<'a>(
    item: &'a Item,
    (name, ty): &(String, ScalarAttributeType),
) -> Result<Cow<'a, str>, FormatError> {
    let shown = || truncated(name);
    let value = item
        .get(name)
        .ok_or_else(|| FormatError::InvalidKey(format!("hash key {:?} missing", shown())))?;
    let actual = match value {
        AttributeValue::S(_) => ScalarAttributeType::S,
        AttributeValue::N(_) => ScalarAttributeType::N,
        AttributeValue::B(_) => ScalarAttributeType::B,
        _ => {
            return Err(FormatError::InvalidKey(format!(
                "hash key {:?} is not a scalar S, N or B",
                shown()
            )));
        }
    };
    if actual != *ty {
        return Err(FormatError::InvalidKey(format!(
            "hash key {:?} is {actual:?}, the schema declares {ty:?}",
            shown()
        )));
    }
    Ok(hash_key_text(value).expect("scalar checked above"))
}

/// An `N` range key value as a decimal, for comparison.
fn parse_number(name: &str, n: &str) -> Result<BigDecimal, FormatError> {
    BigDecimal::from_str(n).map_err(|e| {
        FormatError::InvalidKey(format!(
            "range key {:?} value {:?}: {}",
            truncated(name),
            truncated(n),
            truncated(&e.to_string())
        ))
    })
}

/// One key attribute's value checked as [`KeyOrder::validate_key_item`]
/// checks it: the declared scalar type, not empty, and for `N` a valid
/// number in normalized form. The error is the reason alone.
fn check_key_value(
    name: &str,
    ty: ScalarAttributeType,
    value: &AttributeValue,
) -> Result<(), FormatError> {
    let shown = || truncated(name);
    let empty = match value {
        AttributeValue::S(s) => s.is_empty(),
        AttributeValue::B(b) => b.is_empty(),
        _ => false,
    };
    if empty {
        return Err(FormatError::InvalidKey(format!(
            "key attribute {:?} is empty",
            shown()
        )));
    }
    let actual = match value {
        AttributeValue::S(_) => ScalarAttributeType::S,
        AttributeValue::N(n) => {
            let normalized = validate_and_normalize_number(n).map_err(|e| {
                // The reason for a malformed number ends in the input, which
                // is shown once, bounded, before it; the other reasons
                // (overflow, underflow, too many digits) are fixed texts.
                let reason = e.to_string();
                let reason = match reason.strip_suffix(n.as_str()) {
                    Some(rest) => rest.strip_suffix(": ").unwrap_or(rest).to_owned(),
                    None => reason,
                };
                FormatError::InvalidKey(format!(
                    "key attribute {:?} value {:?} is not a valid number: {reason}",
                    shown(),
                    truncated(n),
                ))
            })?;
            if normalized != *n {
                return Err(FormatError::InvalidKey(format!(
                    "key attribute {:?} value {:?} is not in normalized form ({:?})",
                    shown(),
                    truncated(n),
                    truncated(&normalized)
                )));
            }
            ScalarAttributeType::N
        }
        AttributeValue::B(_) => ScalarAttributeType::B,
        _ => {
            return Err(FormatError::InvalidKey(format!(
                "key attribute {:?} is not a scalar S, N or B",
                shown()
            )));
        }
    };
    if actual != ty {
        return Err(FormatError::InvalidKey(format!(
            "key attribute {:?} is {actual:?}, the schema declares {ty:?}",
            shown()
        )));
    }
    Ok(())
}

/// An item's key, checked by [`KeyOrder::checked_key`]: the values borrowed
/// from the item and an `N` range key parsed once, so the key-range checks
/// compare keys without building a key [`Item`]. [`HeldKey::as_checked`]
/// gives the same view of a held key.
pub(super) struct CheckedKey<'a> {
    hash: &'a AttributeValue,
    hash_text: Cow<'a, str>,
    range: Option<RangeKey<'a>>,
}

enum RangeKey<'a> {
    S(&'a str),
    B(&'a [u8]),
    N(&'a str, Cow<'a, BigDecimal>),
}

impl CheckedKey<'_> {
    /// [`KeyOrder::compare`]'s order: the hash key's text bytewise, and the
    /// range key only when the texts are equal (`S` and `B` bytewise, `N` by
    /// value), so a key in another partition is decided by the text alone.
    pub(super) fn cmp(&self, other: &CheckedKey<'_>) -> Ordering {
        let range = || match (&self.range, &other.range) {
            (Some(RangeKey::S(x)), Some(RangeKey::S(y))) => x.as_bytes().cmp(y.as_bytes()),
            (Some(RangeKey::B(x)), Some(RangeKey::B(y))) => x.cmp(y),
            (Some(RangeKey::N(_, x)), Some(RangeKey::N(_, y))) => x.cmp(y),
            // Keys checked under one `KeyOrder` have one shape.
            (x, y) => x.is_some().cmp(&y.is_some()),
        };
        self.hash_text
            .as_bytes()
            .cmp(other.hash_text.as_bytes())
            .then_with(range)
    }
}

/// A key the key-range checks keep from one item to the next: a
/// [`CheckedKey`] owned, with the hash key's text and an `N` range key's
/// decimal kept, so comparing with it parses nothing. [`Self::assign`]
/// reuses its storage.
#[derive(Debug, Clone)]
pub(super) struct HeldKey {
    hash: AttributeValue,
    /// The hash key's text when it is not the value's own string (`B`).
    hash_b64: Option<String>,
    range: Option<HeldRange>,
}

#[derive(Debug, Clone)]
enum HeldRange {
    S(String),
    B(Vec<u8>),
    N(String, BigDecimal),
}

impl HeldRange {
    fn new(range: RangeKey<'_>) -> Self {
        match range {
            RangeKey::S(s) => Self::S(s.to_owned()),
            RangeKey::B(b) => Self::B(b.to_vec()),
            RangeKey::N(n, d) => Self::N(n.to_owned(), d.into_owned()),
        }
    }
}

fn set_string(dst: &mut String, src: &str) {
    dst.clear();
    dst.push_str(src);
}

impl HeldKey {
    pub(super) fn new(key: CheckedKey<'_>) -> Self {
        Self {
            hash_b64: matches!(key.hash, AttributeValue::B(_)).then(|| key.hash_text.into_owned()),
            hash: key.hash.clone(),
            range: key.range.map(HeldRange::new),
        }
    }

    /// Hold `key` instead, in this key's storage. Keys checked under one
    /// [`KeyOrder`] have one shape, so every allocation is reused.
    pub(super) fn assign(&mut self, key: CheckedKey<'_>) {
        match (&mut self.hash, key.hash) {
            (AttributeValue::S(dst), AttributeValue::S(src))
            | (AttributeValue::N(dst), AttributeValue::N(src)) => set_string(dst, src),
            (AttributeValue::B(dst), AttributeValue::B(src)) => dst.clone_from(src),
            (dst, src) => *dst = src.clone(),
        }
        match (&mut self.hash_b64, matches!(key.hash, AttributeValue::B(_))) {
            (Some(text), true) => set_string(text, &key.hash_text),
            (text, binary) => *text = binary.then(|| key.hash_text.into_owned()),
        }
        match (&mut self.range, key.range) {
            (Some(HeldRange::S(dst)), Some(RangeKey::S(src))) => set_string(dst, src),
            (Some(HeldRange::B(dst)), Some(RangeKey::B(src))) => {
                dst.clear();
                dst.extend_from_slice(src);
            }
            (Some(HeldRange::N(dst, decimal)), Some(RangeKey::N(src, parsed))) => {
                set_string(dst, src);
                *decimal = parsed.into_owned();
            }
            (dst, src) => *dst = src.map(HeldRange::new),
        }
    }

    pub(super) fn as_checked(&self) -> CheckedKey<'_> {
        let hash_text = match (&self.hash_b64, &self.hash) {
            (Some(text), _) => text.as_str(),
            (None, AttributeValue::S(s) | AttributeValue::N(s)) => s.as_str(),
            (None, _) => "",
        };
        CheckedKey {
            hash: &self.hash,
            hash_text: Cow::Borrowed(hash_text),
            range: self.range.as_ref().map(|r| match r {
                HeldRange::S(s) => RangeKey::S(s),
                HeldRange::B(b) => RangeKey::B(b),
                HeldRange::N(n, d) => RangeKey::N(n, Cow::Borrowed(d)),
            }),
        }
    }

    /// The key as a key-only [`Item`], the shape of a [`super::KeyRange`]
    /// endpoint.
    pub(super) fn to_item(&self, order: &KeyOrder) -> Item {
        let mut key = Item::new();
        key.insert(order.hash.0.clone(), self.hash.clone());
        if let (Some((name, _)), Some(range)) = (&order.range, &self.range) {
            let value = match range {
                HeldRange::S(s) => AttributeValue::S(s.clone()),
                HeldRange::B(b) => AttributeValue::B(b.clone()),
                HeldRange::N(n, _) => AttributeValue::N(n.clone()),
            };
            key.insert(name.clone(), value);
        }
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(range: Option<ScalarAttributeType>) -> KeyOrder {
        let mut ks = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let mut defs = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        if let Some(ty) = range {
            ks.push(KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            });
            defs.push(AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ty,
            });
        }
        KeyOrder::resolve(&ks, &defs).unwrap()
    }

    fn item(pk: &str, sk: Option<AttributeValue>) -> Item {
        let mut m = Item::new();
        m.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        if let Some(sk) = sk {
            m.insert("sk".to_owned(), sk);
        }
        m
    }

    #[test]
    fn hash_key_text_matches_storage_encoding() {
        assert_eq!(hash_key_text(&AttributeValue::S("a".into())).unwrap(), "a");
        assert_eq!(
            hash_key_text(&AttributeValue::N("10".into())).unwrap(),
            "10"
        );
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0xde, 0xad])).unwrap(),
            "3q0="
        );
        // Standard alphabet with padding: `+` and `/` distinguish it from
        // URL-safe base64, and the storage row-key test pins the same bytes.
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0xfb, 0xff, 0xbf])).unwrap(),
            "+/+/"
        );
        assert_eq!(hash_key_text(&AttributeValue::B(vec![1])).unwrap(), "AQ==");
        assert!(hash_key_text(&AttributeValue::Bool(true)).is_none());
    }

    /// The hash key's declared type is enforced on both sides of a compare,
    /// not only the range key's.
    #[test]
    fn compare_rejects_hash_key_type_mismatch() {
        let order = schema(None);
        let good = item("p", None);
        let mut bad = Item::new();
        bad.insert("pk".to_owned(), AttributeValue::N("1".into()));
        assert!(order.compare(&good, &bad).is_err());
        assert!(order.compare(&bad, &good).is_err());
    }

    /// A `B` hash key sorts by its base64 text: `[0xff]` (`/w==`) before
    /// `[0x00]` (`AA==`), the opposite of the raw bytes. A backend writing a
    /// key-ordered file has to reproduce this order.
    #[test]
    fn binary_hash_keys_sort_by_base64_text() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let defs = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::B,
        }];
        let order = KeyOrder::resolve(&ks, &defs).unwrap();
        let b = |bytes: &[u8]| {
            let mut m = Item::new();
            m.insert("pk".to_owned(), AttributeValue::B(bytes.to_vec()));
            m
        };
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0x00])).unwrap(),
            "AA=="
        );
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0xff])).unwrap(),
            "/w=="
        );
        assert_eq!(
            order.compare(&b(&[0xff]), &b(&[0x00])).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            order.compare(&b(&[0x00]), &b(&[0xff])).unwrap(),
            Ordering::Greater
        );
        // `+` (0x2b) sorts before `/` (0x2f) and digits before letters.
        assert_eq!(
            order.compare(&b(&[0xfb]), &b(&[0xff])).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            order.compare(&b(&[0xd3]), &b(&[0x00])).unwrap(),
            Ordering::Less
        );
        // With a range key the hash key still decides first.
        let mut ks = ks;
        ks.push(KeySchemaElement {
            attribute_name: "sk".to_owned(),
            key_type: KeyType::Range,
        });
        let mut defs = defs;
        defs.push(AttributeDefinition {
            attribute_name: "sk".to_owned(),
            attribute_type: ScalarAttributeType::B,
        });
        let order = KeyOrder::resolve(&ks, &defs).unwrap();
        let bb = |h: u8, r: u8| {
            let mut m = b(&[h]);
            m.insert("sk".to_owned(), AttributeValue::B(vec![r]));
            m
        };
        assert_eq!(
            order.compare(&bb(0xff, 0xff), &bb(0x00, 0x00)).unwrap(),
            Ordering::Less
        );
    }

    /// Messages that name an attribute or quote a value from a manifest are
    /// bounded.
    #[test]
    fn untrusted_names_and_values_are_bounded_in_errors() {
        for n in [81, 300, 1000, 1_000_000] {
            let name = "a".repeat(n);
            let ks = vec![KeySchemaElement {
                attribute_name: name.clone(),
                key_type: KeyType::Hash,
            }];
            let err = KeyOrder::resolve(&ks, &[]).unwrap_err();
            assert!(
                err.to_string().len() < 200,
                "{n}: {}",
                err.to_string().len()
            );
            let defs = vec![AttributeDefinition {
                attribute_name: name.clone(),
                attribute_type: ScalarAttributeType::N,
            }];
            let order = KeyOrder::resolve(&ks, &defs).unwrap();
            let mut key = Item::new();
            key.insert(name.clone(), AttributeValue::N("9".repeat(n)));
            for err in [
                order.validate_key_item(&key).unwrap_err(),
                order
                    .validate_key_item(&Item::from([("x".to_owned(), AttributeValue::Null)]))
                    .unwrap_err(),
                order.compare(&Item::new(), &key).unwrap_err(),
            ] {
                // The bound holds the two bounded fields and a fixed reason
                // (the longest, 64 bytes, is the number validation's "more
                // than 38 significant digits").
                assert!(
                    err.to_string().len() < 300,
                    "{n}: {}",
                    err.to_string().len()
                );
            }
            let mut wrong = Item::new();
            wrong.insert(name.clone(), AttributeValue::S("x".repeat(n)));
            let err = order.validate_key_item(&wrong).unwrap_err();
            assert!(
                err.to_string().len() < 200,
                "{n}: {}",
                err.to_string().len()
            );
            // A range key with an unparseable value, through compare.
            let order = schema(Some(ScalarAttributeType::N));
            let bad = item("p", Some(AttributeValue::N("x".repeat(n))));
            let err = order.compare(&bad, &bad).unwrap_err();
            assert!(
                err.to_string().len() < 250,
                "{n}: {}",
                err.to_string().len()
            );
        }
    }

    #[test]
    fn number_hash_keys_sort_as_text() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let defs = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::N,
        }];
        let order = KeyOrder::resolve(&ks, &defs).unwrap();
        let n = |s: &str| {
            let mut m = Item::new();
            m.insert("pk".to_owned(), AttributeValue::N(s.to_owned()));
            m
        };
        assert_eq!(order.compare(&n("10"), &n("9")).unwrap(), Ordering::Less);
    }

    #[test]
    fn number_range_keys_sort_numerically() {
        let order = schema(Some(ScalarAttributeType::N));
        let a = item("p", Some(AttributeValue::N("9".into())));
        let b = item("p", Some(AttributeValue::N("10".into())));
        let c = item("p", Some(AttributeValue::N("-1.5".into())));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
        assert_eq!(order.compare(&c, &a).unwrap(), Ordering::Less);
        assert_eq!(order.compare(&a, &a).unwrap(), Ordering::Equal);
    }

    #[test]
    fn string_and_binary_range_keys_sort_bytewise() {
        let order = schema(Some(ScalarAttributeType::S));
        let a = item("p", Some(AttributeValue::S("B".into())));
        let b = item("p", Some(AttributeValue::S("a".into())));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
        // Bytewise, not length-first: "ab" < "b".
        let a = item("p", Some(AttributeValue::S("ab".into())));
        let b = item("p", Some(AttributeValue::S("b".into())));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
        let order = schema(Some(ScalarAttributeType::B));
        let a = item("p", Some(AttributeValue::B(vec![1, 2])));
        let b = item("p", Some(AttributeValue::B(vec![1, 2, 0])));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
        // Raw bytes, not their base64 text ("AA==" sorts after "/w=="), and
        // not length-first.
        let a = item("p", Some(AttributeValue::B(vec![0x00])));
        let b = item("p", Some(AttributeValue::B(vec![0xff])));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
        let a = item("p", Some(AttributeValue::B(vec![0x01, 0x00])));
        let b = item("p", Some(AttributeValue::B(vec![0x02])));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
    }

    #[test]
    fn hash_key_decides_before_range_key() {
        let order = schema(Some(ScalarAttributeType::N));
        let a = item("a", Some(AttributeValue::N("99".into())));
        let b = item("b", Some(AttributeValue::N("1".into())));
        assert_eq!(order.compare(&a, &b).unwrap(), Ordering::Less);
    }

    #[test]
    fn validate_key_item_rejects_wrong_shape() {
        let order = schema(Some(ScalarAttributeType::N));
        order
            .validate_key_item(&item("p", Some(AttributeValue::N("1".into()))))
            .unwrap();
        assert!(order.validate_key_item(&item("p", None)).is_err());
        assert!(
            order
                .validate_key_item(&item("p", Some(AttributeValue::S("1".into()))))
                .is_err()
        );
        let mut extra = item("p", Some(AttributeValue::N("1".into())));
        extra.insert("v".to_owned(), AttributeValue::Null);
        assert!(order.validate_key_item(&extra).is_err());
    }

    /// `N` endpoints must be valid `DynamoDB` numbers in normalized form;
    /// `BigDecimal::from_str` alone would accept all of these.
    #[test]
    fn validate_key_item_rejects_non_dynamodb_numbers() {
        let order = schema(Some(ScalarAttributeType::N));
        for bad in [
            "1E200",
            "1_000",
            "123456789012345678901234567890123456789",
            "1.0",
            "+1",
            ".5",
            "1.",
            "-0",
            "abc",
        ] {
            let err = order
                .validate_key_item(&item("p", Some(AttributeValue::N(bad.into()))))
                .err()
                .unwrap_or_else(|| panic!("{bad:?} must be rejected"));
            assert!(matches!(err, FormatError::InvalidKey(_)), "{bad:?}: {err}");
        }
        for good in ["0", "1", "-1.5", "1E+125", "1E-130", "10"] {
            let normalized = validate_and_normalize_number(good).unwrap();
            order
                .validate_key_item(&item("p", Some(AttributeValue::N(normalized.clone()))))
                .unwrap_or_else(|e| panic!("{normalized:?} (from {good:?}) rejected: {e}"));
        }
    }

    /// `endpoint_key` keeps the key attributes only and refuses, naming the
    /// key, what `validate_key_item` refuses: the stored `N` keys
    /// `BatchWriteItem` and `TransactWriteItems` accept (#618) among them.
    #[test]
    fn endpoint_key_takes_the_key_and_refuses_what_cannot_be_an_endpoint() {
        let order = schema(Some(ScalarAttributeType::N));
        let mut whole = item("p", Some(AttributeValue::N("1.5".into())));
        whole.insert("v".to_owned(), AttributeValue::S("x".into()));
        assert_eq!(
            order.endpoint_key(&whole).unwrap(),
            item("p", Some(AttributeValue::N("1.5".into())))
        );
        for bad in ["abc", "1E126", "1E-131", "5.0"] {
            let err = order
                .endpoint_key(&item("p", Some(AttributeValue::N(bad.into()))))
                .unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidKey(m)
                    if m.contains(&format!(r#""sk":{{"N":"{bad}"}}"#))
                        && m.contains("cannot be a key_range endpoint")),
                "{bad}: {err}"
            );
        }
        let err = order.endpoint_key(&item("p", None)).unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("no key attribute \"sk\"")),
            "{err}"
        );
        // The message stays bounded for a long key.
        let long = item(&"p".repeat(5000), Some(AttributeValue::N("abc".into())));
        let err = order.endpoint_key(&long).unwrap_err();
        assert!(err.to_string().len() < 400, "{}", err.to_string().len());
    }

    #[test]
    fn duplicate_attribute_definitions_are_unorderable() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let defs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
        ];
        let err = KeyOrder::resolve(&ks, &defs).unwrap_err();
        assert!(matches!(err, FormatError::UnorderableKeySchema(_)), "{err}");
        assert!(err.to_string().contains("more than one"), "{err}");
    }

    #[test]
    fn compare_rejects_type_mismatch_and_bad_numbers() {
        let order = schema(Some(ScalarAttributeType::N));
        let a = item("p", Some(AttributeValue::N("1".into())));
        let bad = item("p", Some(AttributeValue::S("1".into())));
        assert!(order.compare(&a, &bad).is_err());
        let nan = item("p", Some(AttributeValue::N("abc".into())));
        assert!(order.compare(&a, &nan).is_err());
    }

    #[test]
    fn multi_part_keys_are_unorderable() {
        let ks = vec![
            KeySchemaElement {
                attribute_name: "a".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "b".to_owned(),
                key_type: KeyType::Hash,
            },
        ];
        let defs = vec![
            AttributeDefinition {
                attribute_name: "a".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "b".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];
        assert!(matches!(
            KeyOrder::resolve(&ks, &defs),
            Err(FormatError::UnorderableKeySchema(_))
        ));
    }

    #[test]
    fn range_key_named_like_the_hash_key_is_refused() {
        let ks = [
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let defs = [AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let err = KeyOrder::resolve(&ks, &defs).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnorderableKeySchema(m) if m.contains("both")),
            "{err}"
        );
    }

    #[test]
    fn range_key_before_the_hash_key_is_refused() {
        let ks = [
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
        ];
        let defs = [
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];
        let err = KeyOrder::resolve(&ks, &defs).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnorderableKeySchema(m) if m.contains("before")),
            "{err}"
        );
        // The same elements HASH first resolve.
        KeyOrder::resolve(&[ks[1].clone(), ks[0].clone()], &defs).unwrap();
    }

    /// Two `N` range keys that an `f64` cannot tell apart: they first differ
    /// in the 38th significant digit, far beyond the 17 an `f64` holds, so a
    /// comparator that went through floating point would call them equal.
    #[test]
    fn number_range_keys_differing_beyond_seventeen_digits_are_ordered() {
        let order = schema(Some(ScalarAttributeType::N));
        let low = "1234567890123456789012345678901234567";
        let high = "1234567890123456789012345678901234568";
        let lo = item("p", Some(AttributeValue::N(low.to_owned())));
        let hi = item("p", Some(AttributeValue::N(high.to_owned())));
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(low.parse::<f64>().unwrap(), high.parse::<f64>().unwrap());
        }
        assert_eq!(order.compare(&lo, &hi).unwrap(), Ordering::Less);
        assert_eq!(order.compare(&hi, &lo).unwrap(), Ordering::Greater);
        // And small magnitudes alike.
        let lo = item(
            "p",
            Some(AttributeValue::N("1.0000000000000000000001E-100".into())),
        );
        let hi = item(
            "p",
            Some(AttributeValue::N("1.0000000000000000000002E-100".into())),
        );
        assert_eq!(order.compare(&lo, &hi).unwrap(), Ordering::Less);
    }

    /// The module doc's normalization and comparison examples, pinned so
    /// the stated rules and the code cannot drift apart: each input's
    /// normalized form, the two refused magnitudes, the exponent's 32-bit
    /// range, the long forms'
    /// lengths, the base64 alphabet, UTF-8 byte order of `S`, and the
    /// 38th-digit `N` range-key pair.
    #[test]
    fn doc_examples_normalize_as_stated() {
        for (input, normalized) in [
            ("00042", "42"),
            ("1.50", "1.5"),
            ("1.5E3", "1500"),
            ("+1", "1"),
            (".5", "0.5"),
            ("-0", "0"),
            ("-0.0", "0"),
            ("0E5", "0"),
            ("5.0", "5"),
            ("1e2", "100"),
            ("-1.5", "-1.5"),
            ("9.9E125", &format!("99{}", "0".repeat(124))),
        ] {
            assert_eq!(
                validate_and_normalize_number(input).unwrap(),
                normalized,
                "{input}"
            );
        }
        let big = validate_and_normalize_number("1E+125").unwrap();
        assert_eq!(big, format!("1{}", "0".repeat(125)));
        assert_eq!(big.len(), 126);
        let small = validate_and_normalize_number("1E-130").unwrap();
        assert_eq!(small, format!("0.{}1", "0".repeat(129)));
        assert_eq!(small.len(), 132);
        assert!(validate_and_normalize_number("1E-131").is_err());
        assert!(validate_and_normalize_number("1E126").is_err());
        assert!(validate_and_normalize_number(&"9".repeat(38)).is_ok());
        assert!(validate_and_normalize_number(&"9".repeat(39)).is_err());
        for refused in [" 5", "5 ", "1_000", "0x10", ".", "1e", "NaN", ""] {
            assert!(
                validate_and_normalize_number(refused).is_err(),
                "{refused:?}"
            );
        }
        // The exponent is a signed 32-bit integer: one past either end is
        // refused even where the value would be zero, and the ends are read.
        for refused in ["0E2147483648", "0E-2147483649", "1E2147483648"] {
            assert!(
                validate_and_normalize_number(refused).is_err(),
                "{refused:?}"
            );
        }
        for zero in [
            "0E2147483647",
            "0E-2147483648",
            "0E+2147483647",
            "0.0E-2147483648",
        ] {
            assert_eq!(validate_and_normalize_number(zero).unwrap(), "0", "{zero}");
        }
        // A non-zero value with an exponent at either end is refused by its
        // magnitude, without overflow in the exponent arithmetic.
        assert!(validate_and_normalize_number("10E2147483647").is_err());
        assert!(validate_and_normalize_number("0.1E-2147483648").is_err());
        // Normalized output is a fixed point of normalization.
        for s in ["42", "1.5", "0", &big, &small] {
            assert_eq!(validate_and_normalize_number(s).unwrap(), s);
        }
        // `B`: the standard alphabet with padding, so `+` and `/` appear and
        // the text is what a JSON endpoint carries.
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0xfb, 0xff, 0xbf])).unwrap(),
            "+/+/"
        );
        assert_eq!(
            hash_key_text(&AttributeValue::B(vec![0xff])).unwrap(),
            "/w=="
        );
        // `S`: UTF-8 bytes; `é` (0xc3 0xa9) sorts after `z` (0x7a).
        let order = schema(Some(ScalarAttributeType::S));
        let e = item("p", Some(AttributeValue::S("é".into())));
        let z = item("p", Some(AttributeValue::S("z".into())));
        assert_eq!(order.compare(&z, &e).unwrap(), Ordering::Less);
        let order = schema(None);
        assert_eq!(
            order.compare(&item("z", None), &item("é", None)).unwrap(),
            Ordering::Less
        );
        // `N` range keys: exact decimal; 38 significant digits are kept.
        let order = schema(Some(ScalarAttributeType::N));
        let low = "1234567890123456789012345678901234567";
        let high = "1234567890123456789012345678901234568";
        assert_eq!(
            order
                .compare(
                    &item("p", Some(AttributeValue::N(low.into()))),
                    &item("p", Some(AttributeValue::N(high.into())))
                )
                .unwrap(),
            Ordering::Less
        );
    }
}
