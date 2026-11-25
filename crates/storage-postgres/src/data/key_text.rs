// SPDX-License-Identifier: Apache-2.0

//! Key text for the `PostgreSQL` key columns.
//!
//! The shared helpers in `extenddb_storage::util` turn key attribute values into
//! the text the SQL backends bind against `pk`, `sk_s`, `base_pk`, and the other
//! string key columns. `PostgreSQL` `TEXT` cannot hold the byte 0x00, which
//! DynamoDB allows anywhere in a string key. The key columns hold the text the
//! shared helpers give, as every earlier release stored it, and these wrappers
//! refuse a string key value that holds U+0000 with a `ValidationException`
//! ([`NUL_IN_KEY`]) instead of a statement that fails. Numbers and base64
//! binaries never hold U+0000. The item documents in `item_data` hold U+0000
//! through the escape of `data::item_to_json`; the key columns are not escaped,
//! so a key keeps the bytes and the length an earlier release stored for it.
//!
//! Every site in this crate that produces key column text imports these three
//! functions instead of the shared ones, so a write, a read, an index row and a
//! cursor refuse the same keys. A write also refuses an item whose key for one
//! of the table's indexes holds U+0000 ([`refuse_nul_index_keys`]), before its
//! first statement, because the queue writes an asynchronous index's rows after
//! the write commits.

use std::borrow::Cow;

use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, ScalarAttributeType};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{self, SortKeyValue};

use super::index::{IndexMeta, item_has_index_keys};

/// The answer to a key value that holds U+0000.
pub(crate) const NUL_IN_KEY: &str = "Key attribute values cannot contain the character \
     U+0000 on the PostgreSQL storage backend, whose text columns cannot hold the byte 0x00. \
     Remove the character from the key value, or use another storage backend.";

/// Refuse key text that holds U+0000 ([`NUL_IN_KEY`]).
fn refuse_nul(text: &str) -> Result<(), StorageError> {
    if text.as_bytes().contains(&0) {
        return Err(StorageError::Validation(NUL_IN_KEY.to_owned()));
    }
    Ok(())
}

/// Partition key attribute value as column text.
pub(crate) fn pk_to_text(value: &AttributeValue) -> Result<Cow<'_, str>, StorageError> {
    let text = util::pk_to_text(value)?;
    refuse_nul(&text)?;
    Ok(text)
}

/// Composite partition key as column text: a single HASH attribute is its own
/// text, several are netstring-joined (`extenddb_storage::util::composite_pk_to_text`).
/// The join holds U+0000 exactly when a part does.
pub(crate) fn composite_pk_to_text(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<String, StorageError> {
    let text = util::composite_pk_to_text(item, key_schema)?;
    refuse_nul(&text)?;
    Ok(text)
}

/// Sort key attribute value as a typed bind value.
pub(crate) fn parse_sk(
    value: &AttributeValue,
    sk_type: ScalarAttributeType,
) -> Result<SortKeyValue, StorageError> {
    let sk = util::parse_sk(value, sk_type)?;
    if let SortKeyValue::S(s) = &sk {
        refuse_nul(s)?;
    }
    Ok(sk)
}

/// Whether one of the attributes of `key_schema` that `item` holds is a string
/// with U+0000.
pub(crate) fn holds_nul_key(item: &Item, key_schema: &[KeySchemaElement]) -> bool {
    key_schema.iter().any(
        |ks| matches!(item.get(&ks.attribute_name), Some(AttributeValue::S(s)) if s.contains('\0')),
    )
}

/// Refuse `item` when its key under `key_schema` holds U+0000, with the error
/// the key text functions return.
pub(crate) fn refuse_nul_key(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<(), StorageError> {
    if holds_nul_key(item, key_schema) {
        return Err(StorageError::Validation(NUL_IN_KEY.to_owned()));
    }
    Ok(())
}

/// Refuse a written item that holds every key attribute of one of `indexes`
/// and whose key for that index holds U+0000. An index row that the queue or
/// a backfill writes later leaves such an item out (`gsi_build::indexable`),
/// as it leaves out an item written before the index existed.
pub(crate) fn refuse_nul_index_keys(
    item: &Item,
    indexes: &[IndexMeta],
) -> Result<(), StorageError> {
    for idx in indexes {
        if item_has_index_keys(item, &idx.key_schema) {
            refuse_nul_key(item, &idx.key_schema)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::KeyType;

    use super::*;

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn refused<T>(result: Result<T, StorageError>) -> String {
        match result {
            Err(StorageError::Validation(message)) => message,
            Err(other) => panic!("expected the refusal, got {other:?}"),
            Ok(_) => panic!("expected the refusal, got a key text"),
        }
    }

    const MESSAGE: &str = "Key attribute values cannot contain the character U+0000 on the \
         PostgreSQL storage backend, whose text columns cannot hold the byte 0x00. Remove the \
         character from the key value, or use another storage backend.";

    #[test]
    fn a_string_key_with_u0000_is_refused() {
        assert_eq!(
            refused(pk_to_text(&AttributeValue::S("a\u{0}b".into()))),
            MESSAGE
        );
        assert_eq!(
            refused(parse_sk(
                &AttributeValue::S("\u{0}".into()),
                ScalarAttributeType::S
            )),
            MESSAGE
        );
        let mut item = Item::new();
        item.insert("h1".to_owned(), AttributeValue::S("a".into()));
        item.insert("h2".to_owned(), AttributeValue::S("b\u{0}".into()));
        let schema = [ks("h1", KeyType::Hash), ks("h2", KeyType::Hash)];
        assert_eq!(refused(composite_pk_to_text(&item, &schema)), MESSAGE);
        assert_eq!(refused(refuse_nul_key(&item, &schema)), MESSAGE);
        assert_eq!(refused(composite_pk_to_text(&item, &schema[1..])), MESSAGE);
    }

    #[test]
    fn other_keys_keep_the_text_of_the_shared_helpers() {
        // U+0001 is stored as it is, as every earlier release stored it.
        for value in [
            AttributeValue::S("a\u{1}\u{2}b".into()),
            AttributeValue::S("plain".into()),
            AttributeValue::N("12.5".into()),
            AttributeValue::B(vec![0, 1, 2]),
        ] {
            assert_eq!(
                pk_to_text(&value).unwrap(),
                util::pk_to_text(&value).unwrap(),
                "{value:?}"
            );
        }
        match parse_sk(&AttributeValue::S("\u{1}".into()), ScalarAttributeType::S).unwrap() {
            SortKeyValue::S(s) => assert_eq!(s, "\u{1}"),
            _ => panic!("string sort key"),
        }
        match parse_sk(&AttributeValue::B(vec![0]), ScalarAttributeType::B).unwrap() {
            SortKeyValue::B(b) => assert_eq!(b, vec![0]),
            _ => panic!("binary sort key"),
        }
        let mut item = Item::new();
        item.insert("h1".to_owned(), AttributeValue::S("a\u{1}".into()));
        item.insert("h2".to_owned(), AttributeValue::S("b".into()));
        item.insert("v".to_owned(), AttributeValue::S("\u{0}".into()));
        let schema = [ks("h1", KeyType::Hash), ks("h2", KeyType::Hash)];
        assert_eq!(
            composite_pk_to_text(&item, &schema).unwrap(),
            "2:a\u{1},1:b,"
        );
        // A U+0000 outside the key does not concern the key.
        assert!(refuse_nul_key(&item, &schema).is_ok());
        assert!(refuse_nul_key(&item, &[ks("v", KeyType::Range)]).is_err());
    }
}
