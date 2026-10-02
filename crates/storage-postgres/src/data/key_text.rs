// SPDX-License-Identifier: Apache-2.0

//! Key text for the `PostgreSQL` key columns.
//!
//! The shared helpers in `extenddb_storage::util` turn key attribute values into
//! the text the SQL backends bind against `pk`, `sk_s`, `base_pk`, and the other
//! string key columns. `PostgreSQL` `TEXT` cannot hold the byte 0x00, which
//! DynamoDB allows anywhere in a string key, so this module wraps those helpers
//! and passes every string component through the order-preserving escape from
//! `extenddb_storage::util::escape_control`. Numbers and base64 binaries contain
//! neither U+0000 nor U+0001 and are unchanged. The escape preserves byte order
//! and prefixes, so the `COLLATE "C"` comparisons, `BETWEEN`, `begins_with`,
//! and row-comparison cursors that run on these columns return the same rows
//! they would on the raw text.
//!
//! Every site in this crate that produces key column text imports these three
//! functions instead of the shared ones, so the write path, the read path, index
//! rows, cursors, and the propagation queue all agree on the stored form.

use std::borrow::Cow;

use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{self, SortKeyValue, encode_netstring_composite, escape_control};

/// Partition key attribute value as escaped column text.
pub(crate) fn pk_to_text(value: &AttributeValue) -> Result<Cow<'_, str>, StorageError> {
    Ok(match util::pk_to_text(value)? {
        Cow::Borrowed(s) => escape_control(s),
        Cow::Owned(s) => Cow::Owned(escape_control(&s).into_owned()),
    })
}

/// Composite partition key as escaped column text.
///
/// Mirrors `extenddb_storage::util::composite_pk_to_text`: a single HASH
/// attribute is its own text, several are netstring-joined. Each part is
/// escaped before joining, which is also what `query_scan` does when it builds
/// the same text from a key condition, so the two agree byte for byte.
pub(crate) fn composite_pk_to_text(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<String, StorageError> {
    let hash_elements: Vec<_> = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .collect();
    if hash_elements.len() == 1 {
        let val = item
            .get(&hash_elements[0].attribute_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        return Ok(pk_to_text(val)?.into_owned());
    }
    let mut parts = Vec::with_capacity(hash_elements.len());
    for ks in &hash_elements {
        let val = item.get(&ks.attribute_name).ok_or_else(|| {
            StorageError::Internal(format!(
                "missing partition key attribute {}",
                ks.attribute_name
            ))
        })?;
        parts.push(pk_to_text(val)?.into_owned());
    }
    Ok(encode_netstring_composite(&parts))
}

/// Sort key attribute value as a typed bind value, string keys escaped.
pub(crate) fn parse_sk(
    value: &AttributeValue,
    sk_type: ScalarAttributeType,
) -> Result<SortKeyValue, StorageError> {
    Ok(match util::parse_sk(value, sk_type)? {
        SortKeyValue::S(s) => SortKeyValue::S(escape_control(&s).into_owned()),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ks(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }
    }

    #[test]
    fn string_keys_are_escaped_and_other_types_are_not() {
        assert_eq!(
            pk_to_text(&AttributeValue::S("a\u{0}b".into())).unwrap(),
            "a\u{1}\u{1}b"
        );
        assert_eq!(
            pk_to_text(&AttributeValue::S("plain".into())).unwrap(),
            "plain"
        );
        assert_eq!(
            pk_to_text(&AttributeValue::N("12.5".into())).unwrap(),
            "12.5"
        );
        assert_eq!(
            pk_to_text(&AttributeValue::B(vec![0, 1, 2])).unwrap(),
            util::pk_to_text(&AttributeValue::B(vec![0, 1, 2])).unwrap()
        );
        match parse_sk(&AttributeValue::S("\u{1}".into()), ScalarAttributeType::S).unwrap() {
            SortKeyValue::S(s) => assert_eq!(s, "\u{1}\u{2}"),
            _ => panic!("string sort key"),
        }
        match parse_sk(&AttributeValue::B(vec![0]), ScalarAttributeType::B).unwrap() {
            SortKeyValue::B(b) => assert_eq!(b, vec![0]),
            _ => panic!("binary sort key"),
        }
    }

    #[test]
    fn composite_text_escapes_each_part_before_joining() {
        let mut item = Item::new();
        item.insert("h1".to_owned(), AttributeValue::S("a\u{0}".into()));
        item.insert("h2".to_owned(), AttributeValue::S("b".into()));
        let text = composite_pk_to_text(&item, &[ks("h1"), ks("h2")]).unwrap();
        // The escaped part is three bytes, and the netstring length says so.
        assert_eq!(text, "3:a\u{1}\u{1},1:b,");
        assert!(!text.contains('\u{0}'));
        // Single hash attribute: the escaped value itself.
        let text = composite_pk_to_text(&item, &[ks("h1")]).unwrap();
        assert_eq!(text, "a\u{1}\u{1}");
    }

    #[test]
    fn stored_text_never_contains_nul() {
        for raw in ["\u{0}", "a\u{0}b", "\u{0}\u{1}\u{2}", "x\u{1}\u{1}y"] {
            let av = AttributeValue::S(raw.into());
            let text = pk_to_text(&av).unwrap();
            assert!(!text.contains('\u{0}'), "{raw:?} -> {text:?}");
        }
    }
}
