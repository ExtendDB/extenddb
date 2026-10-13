// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The one definition of base-table key order for the backup format.
//!
//! A `key_ordered` backup promises its items ascend in this order within each
//! data file and from one file to the next:
//!
//! - Hash key: the text encoding [`hash_key_text`] produces (`S` as is, `N`
//!   as its normalized decimal string, `B` as standard base64), compared
//!   bytewise. A number hash key therefore sorts as text (`"10"` before
//!   `"9"`); this is how the storage layer keys rows and the format inherits
//!   it rather than defining a second order.
//! - Range key: `S` bytewise, `N` by numeric value, `B` bytewise.
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
//! Backends that cannot produce this order write `unordered`. The restore
//! path binds [`KeyOrder::compare`] for its overlap and monotonicity checks,
//! and the storage layer's `pk_to_text` binds [`hash_key_text`], so for the
//! hash key the writer, the reader, and the row key agree by construction.
//! (Two other copies of the hash-key text mapping remain in the Postgres and
//! SQLite `tx_helpers` for stream shard assignment; they are not on the
//! backup path.) Range-key agreement rests on each backend's column order
//! matching the rules above, which the tests named here check.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bigdecimal::BigDecimal;

use super::FormatError;
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
    /// more than one hash or range key, a key attribute without a definition,
    /// or an attribute defined more than once.
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
                    "key attribute {name:?} has no attribute definition"
                ))
            })?;
            if matching.next().is_some() {
                return Err(FormatError::UnorderableKeySchema(format!(
                    "key attribute {name:?} has more than one attribute definition"
                )));
            }
            Ok(ty)
        };
        let mut hash = None;
        let mut range = None;
        for element in key_schema {
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
        let hash = hash.ok_or_else(|| {
            FormatError::UnorderableKeySchema("key schema has no hash key".to_owned())
        })?;
        Ok(Self { hash, range })
    }

    /// Names of the key attributes, hash first.
    pub fn attribute_names(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.hash.0.as_str()).chain(self.range.iter().map(|(n, _)| n.as_str()))
    }

    /// Check that `key` holds exactly the key attributes with the declared
    /// types and nothing else: the shape [`super::KeyRange`] carries. An `N`
    /// value must be a valid `DynamoDB` number in its normalized form (the
    /// form the deserializer and the row key produce), so an endpoint such as
    /// `1E200`, `1_000` or 39 significant digits is refused here rather than
    /// parsed leniently by [`Self::compare`].
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidKey`] naming what is wrong.
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
                FormatError::InvalidKey(format!("key attribute {name:?} missing"))
            })?;
            let actual = match value {
                AttributeValue::S(_) => ScalarAttributeType::S,
                AttributeValue::N(n) => {
                    let normalized = validate_and_normalize_number(n).map_err(|e| {
                        FormatError::InvalidKey(format!(
                            "key attribute {name:?} value {n:?} is not a valid number: {e}"
                        ))
                    })?;
                    if normalized != *n {
                        return Err(FormatError::InvalidKey(format!(
                            "key attribute {name:?} value {n:?} is not in normalized form \
                             ({normalized:?})"
                        )));
                    }
                    ScalarAttributeType::N
                }
                AttributeValue::B(_) => ScalarAttributeType::B,
                _ => {
                    return Err(FormatError::InvalidKey(format!(
                        "key attribute {name:?} is not a scalar S, N or B"
                    )));
                }
            };
            if actual != *ty {
                return Err(FormatError::InvalidKey(format!(
                    "key attribute {name:?} is {actual:?}, the schema declares {ty:?}"
                )));
            }
        }
        Ok(())
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
        let ra = a
            .get(name)
            .ok_or_else(|| FormatError::InvalidKey(format!("range key {name:?} missing")))?;
        let rb = b
            .get(name)
            .ok_or_else(|| FormatError::InvalidKey(format!("range key {name:?} missing")))?;
        match (ty, ra, rb) {
            (ScalarAttributeType::S, AttributeValue::S(x), AttributeValue::S(y)) => {
                Ok(x.as_bytes().cmp(y.as_bytes()))
            }
            (ScalarAttributeType::B, AttributeValue::B(x), AttributeValue::B(y)) => Ok(x.cmp(y)),
            (ScalarAttributeType::N, AttributeValue::N(x), AttributeValue::N(y)) => {
                let parse = |s: &str| {
                    BigDecimal::from_str(s).map_err(|e| {
                        FormatError::InvalidKey(format!("range key {name:?} value {s:?}: {e}"))
                    })
                };
                Ok(parse(x)?.cmp(&parse(y)?))
            }
            _ => Err(FormatError::InvalidKey(format!(
                "range key {name:?} does not match its declared type {ty:?}"
            ))),
        }
    }
}

fn hash_text<'a>(
    item: &'a Item,
    (name, ty): &(String, ScalarAttributeType),
) -> Result<Cow<'a, str>, FormatError> {
    let value = item
        .get(name)
        .ok_or_else(|| FormatError::InvalidKey(format!("hash key {name:?} missing")))?;
    let actual = match value {
        AttributeValue::S(_) => ScalarAttributeType::S,
        AttributeValue::N(_) => ScalarAttributeType::N,
        AttributeValue::B(_) => ScalarAttributeType::B,
        _ => {
            return Err(FormatError::InvalidKey(format!(
                "hash key {name:?} is not a scalar S, N or B"
            )));
        }
    };
    if actual != *ty {
        return Err(FormatError::InvalidKey(format!(
            "hash key {name:?} is {actual:?}, the schema declares {ty:?}"
        )));
    }
    Ok(hash_key_text(value).expect("scalar checked above"))
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
        assert!(hash_key_text(&AttributeValue::Bool(true)).is_none());
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
        let order = schema(Some(ScalarAttributeType::B));
        let a = item("p", Some(AttributeValue::B(vec![1, 2])));
        let b = item("p", Some(AttributeValue::B(vec![1, 2, 0])));
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
}
