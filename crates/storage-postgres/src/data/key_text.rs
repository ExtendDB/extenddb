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
        parts.push(util::pk_to_text(val)?);
    }
    Ok(join_escaped_parts(&parts))
}

/// The stored text of a composite partition key whose parts, as
/// `extenddb_storage::util::pk_to_text` renders each HASH attribute, are
/// `raw_parts`: each part escaped, then netstring-joined. The netstring length
/// is that of the escaped part.
///
/// [`composite_pk_to_text`] builds every composite key column through this
/// function, and catalog migration 6, `escape_control_chars`, rebuilds legacy
/// composite key columns through it ([`reescape_legacy_composite`]).
pub(crate) fn join_escaped_parts<S: AsRef<str>>(raw_parts: &[S]) -> String {
    let parts: Vec<String> = raw_parts
        .iter()
        .map(|p| escape_control(p.as_ref()).into_owned())
        .collect();
    encode_netstring_composite(&parts)
}

/// Split a netstring-joined composite key (`<len>:<text>,` per part, `len`
/// the part's length in bytes) into its parts. `None` when `text` is not a
/// sequence of well-formed netstrings.
pub(crate) fn split_netstring_composite(text: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let colon = rest.find(':')?;
        let digits = &rest[..colon];
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let len: usize = digits.parse().ok()?;
        let start = colon + 1;
        let end = start.checked_add(len)?;
        let part = rest.get(start..end)?;
        if rest.as_bytes().get(end) != Some(&b',') {
            return None;
        }
        parts.push(part);
        rest = &rest[end + 1..];
    }
    (!parts.is_empty()).then_some(parts)
}

/// The stored form of a composite key column that a build without the escape
/// wrote: its netstring parts hold raw text, so each is escaped and the parts
/// are joined again with the new lengths, as [`composite_pk_to_text`] writes the
/// same key. Escaping the whole text instead would leave every length prefix
/// counting the raw part. `None` when `legacy` is not a netstring.
pub(crate) fn reescape_legacy_composite(legacy: &str) -> Option<String> {
    split_netstring_composite(legacy).map(|parts| join_escaped_parts(&parts))
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
    fn netstring_split_inverts_the_join() {
        let cases: [&[&str]; 5] = [
            &["a", "b"],
            &["", "x"],
            &["1:a,", ",:", "z"],
            &["a\u{1}\u{2}", "\u{e9}t\u{e9}", "12"],
            &["only"],
        ];
        for parts in cases {
            let owned: Vec<String> = parts.iter().map(|p| (*p).to_owned()).collect();
            let joined = encode_netstring_composite(&owned);
            assert_eq!(
                split_netstring_composite(&joined).as_deref(),
                Some(parts),
                "{joined:?}"
            );
        }
        for bad in [
            "",
            "a",
            "1:ab,",
            "2:a,",
            "x:ab,",
            "+1:a,",
            "1:a",
            "1:a,2",
            "1:\u{e9},",
        ] {
            assert_eq!(split_netstring_composite(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_legacy_composite_key_is_rebuilt_as_the_write_path_builds_it() {
        // The case from the review: a raw U+0001 in the first of two HASH
        // parts. Escaping the whole text would give 2:a\x01\x02,1:b, with a
        // length prefix that no longer matches its part.
        let legacy = "2:a\u{1},1:b,";
        let rebuilt = reescape_legacy_composite(legacy).expect("a netstring");
        assert_eq!(rebuilt, "3:a\u{1}\u{2},1:b,");

        let mut item = Item::new();
        item.insert("h1".to_owned(), AttributeValue::S("a\u{1}".into()));
        item.insert("h2".to_owned(), AttributeValue::S("b".into()));
        assert_eq!(
            composite_pk_to_text(&item, &[ks("h1"), ks("h2")]).unwrap(),
            rebuilt
        );

        // Parts without U+0001 keep their text and their lengths.
        assert_eq!(
            reescape_legacy_composite("1:x,2:yz,").as_deref(),
            Some("1:x,2:yz,")
        );
        assert_eq!(reescape_legacy_composite("a\u{1}b"), None);
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
