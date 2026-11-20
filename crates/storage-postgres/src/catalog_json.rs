// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored form of the `jsonb` documents that carry attribute names.
//!
//! DynamoDB accepts U+0000 in an attribute name, a key attribute name
//! included, and `DescribeTable` returns it byte for byte (measured
//! 2026-09-18 and 2026-10-24, and again on 2026-11-04). PostgreSQL `jsonb`
//! refuses the `\u0000` escape (`unsupported Unicode escape sequence`), so a
//! document that holds the character cannot be stored. Every document below
//! is written through [`to_catalog_json`] or [`escape_json`], which replace
//! the two affected characters with sequences `jsonb` accepts, and read
//! through [`from_catalog_json`] or [`unescape_json`], which reverse them:
//!
//! | character | stored as     |
//! |-----------|---------------|
//! | U+0000    | U+0001 U+0001 |
//! | U+0001    | U+0001 U+0002 |
//!
//! Every other character is stored as itself, so a name without either
//! character is stored as it was before the escape existed. The escape
//! applies to every string and object key of a document, so a string that is
//! not an attribute name and holds one of the two characters is stored the
//! same way and reads back as written. It is the escape the item documents
//! use, `extenddb_storage::util::escape_json_strings` and
//! `unescape_json_strings`.
//!
//! The documents: in the catalog database `tables.key_schema`,
//! `tables.attribute_definitions`, `indexes.key_schema`, `indexes.projection`,
//! `backups.key_schema`, `backups.attribute_definitions`,
//! `backups.vector_indexes`, `backup_definitions.definition`,
//! `vector_indexes.vector_attribute`, `vector_indexes.search_schema`,
//! `vector_indexes.projection` and `backup_index.source_table_details_json`;
//! in the data database
//! `gsi_pending.index_context`. Releases before catalog migration 11
//! (`attribute_name_escape`) stored these documents raw, and a raw name that
//! holds U+0001 followed by U+0001 or U+0002 would read back as a different
//! name, so migration 11 rewrites every stored document through the escape
//! once (`migrations/m0011_attribute_name_escape.rs`).
//!
//! The item documents (`item_data`, the stream record images, the images in
//! `gsi_pending`) are stored through the same escape: `data::item_to_json`
//! and `data::json_to_item` call these functions, and the records in
//! `stream_pending` and `stream_records` are written and read through them.
//! Migration 11 leaves those documents as they are; migration 13
//! (`escape_control_chars`) rewrites them.
//!
//! Decoding reads a U+0001 that is not followed by U+0001 or U+0002 as
//! itself. The encoder never writes one.

use extenddb_storage::error::StorageError;
use extenddb_storage::util::{escape_json_strings, unescape_json_strings};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Serialize `value` into its stored form.
pub(crate) fn to_catalog_json<T: Serialize + ?Sized>(value: &T) -> Result<Value, StorageError> {
    serde_json::to_value(value)
        .map(escape_json)
        .map_err(|e| StorageError::Internal(e.to_string()))
}

/// Deserialize a document written by [`to_catalog_json`] or [`escape_json`].
pub(crate) fn from_catalog_json<T: DeserializeOwned>(v: Value) -> Result<T, StorageError> {
    serde_json::from_value(unescape_json(v)).map_err(|e| StorageError::Internal(e.to_string()))
}

/// The stored form of a document that is already a JSON tree.
pub(crate) fn escape_json(v: Value) -> Value {
    escape_json_strings(v)
}

/// A stored document with the escape reversed, for a caller that reads its
/// fields without deserializing it.
pub(crate) fn unescape_json(v: Value) -> Value {
    unescape_json_strings(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    use serde_json::json;

    fn key_schema(name: &str) -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    #[test]
    fn a_name_without_either_character_is_stored_as_itself() {
        let ks = key_schema("pk");
        let stored = to_catalog_json(&ks).unwrap();
        assert_eq!(stored, serde_json::to_value(&ks).unwrap());
        let back: Vec<KeySchemaElement> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ks);
    }

    #[test]
    fn u0000_in_a_key_attribute_name_is_stored_without_u0000_and_read_back() {
        let ks = key_schema("p\u{0}k");
        let stored = to_catalog_json(&ks).unwrap();
        let text = stored.to_string();
        assert!(!text.contains("\\u0000"), "{text}");
        assert_eq!(stored[0]["AttributeName"], json!("p\u{1}\u{1}k"));
        let back: Vec<KeySchemaElement> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ks);
    }

    #[test]
    fn u0001_in_an_attribute_definition_is_stored_as_a_pair_and_read_back() {
        let ad = vec![AttributeDefinition {
            attribute_name: "a\u{1}b\u{1}\u{1}c\u{0}".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let stored = to_catalog_json(&ad).unwrap();
        assert_eq!(
            stored[0]["AttributeName"],
            json!("a\u{1}\u{2}b\u{1}\u{2}\u{1}\u{2}c\u{1}\u{1}")
        );
        let back: Vec<AttributeDefinition> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ad);
    }

    #[test]
    fn a_u0001_without_a_tail_reads_as_itself() {
        let stored = json!([{"AttributeName": "a\u{1}z", "KeyType": "HASH"}]);
        let back: Vec<KeySchemaElement> = from_catalog_json(stored).unwrap();
        assert_eq!(back[0].attribute_name, "a\u{1}z");
    }

    #[test]
    fn object_keys_and_nested_strings_are_escaped() {
        let v = json!({"k\u{0}": {"NonKeyAttributes": ["n\u{0}", "plain"]}, "n": 1});
        let stored = escape_json(v.clone());
        assert_eq!(
            stored,
            json!({"k\u{1}\u{1}": {"NonKeyAttributes": ["n\u{1}\u{1}", "plain"]}, "n": 1})
        );
        assert_eq!(unescape_json(stored), v);
    }
}
