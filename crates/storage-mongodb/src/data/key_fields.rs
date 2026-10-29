// SPDX-License-Identifier: Apache-2.0

//! Key attributes of a base-table document, for the two places that build a
//! document from something other than a full item.
//!
//! [`item_to_document`] writes a base-table document as `{ _id, pk, sk_*,
//! item_data, _v }`, with `_id` the key text and the key attributes inside
//! `item_data`. An UpdateItem that creates an item through a native MongoDB
//! upsert starts from the key filter (`pk` and `sk_*`) and the update
//! operators, so it needs the `_id` and the `item_data` key attributes added
//! with `$setOnInsert`: [`insert_only_key_fields`] builds that document.
//!
//! Before that was done, such an upsert stored a document with a generated
//! ObjectId `_id` and no key attributes in `item_data` (issue #436).
//! [`repaired_document`] rebuilds a document of that shape into the shape
//! `item_to_document` writes, reading the key values back from the top-level
//! `pk` and `sk_*` fields.

use base64::Engine as _;
use bson::{Bson, Document};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::sk_info;

use super::{document_to_item, item_to_document, sk_suffix};

/// The `$setOnInsert` document for an upsert whose filter is [`super::pk_filter`]
/// on `key`: `_id` as `item_to_document` writes it, and every key attribute
/// under `item_data.<name>`.
///
/// The filter's own equality fields (`pk`, `sk_*`) are not repeated; MongoDB
/// copies them into the inserted document itself.
pub fn insert_only_key_fields(
    key: &Item,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let key_doc = item_to_document(key, key_schema, attribute_definitions)?;
    let mut fields = Document::new();
    let id = key_doc
        .get("_id")
        .cloned()
        .ok_or_else(|| StorageError::Internal("key document has no _id".to_owned()))?;
    fields.insert("_id", id);
    for ks in key_schema {
        let value = key.get(&ks.attribute_name).ok_or_else(|| {
            StorageError::Internal(format!("missing key attribute {}", ks.attribute_name))
        })?;
        let value_json =
            serde_json::to_value(value).map_err(|e| StorageError::Internal(e.to_string()))?;
        let value_bson =
            bson::to_bson(&value_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        fields.insert(format!("item_data.{}", ks.attribute_name), value_bson);
    }
    Ok(fields)
}

/// Whether `doc` was stored by the upsert described in the module comment: an
/// `_id` that is not the key text and key attributes missing from `item_data`.
#[must_use]
pub fn is_stored_without_key(doc: &Document) -> bool {
    matches!(doc.get("_id"), Some(Bson::ObjectId(_)))
}

/// Rebuild a document stored without its key into the shape
/// `item_to_document` writes: the same `item_data` with the key attributes
/// added, `_id` as the key text, and `_v` carried over.
///
/// The key values come from the top-level `pk` text and `sk_s`/`sk_n`/`sk_b`
/// field, typed by `attribute_definitions`. A document whose top-level key
/// fields are missing or of the wrong type is an error; the caller leaves it
/// in place.
pub fn repaired_document(
    doc: &Document,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let mut item = document_to_item(doc)?;

    let pk_name = key_schema
        .iter()
        .find(|ks| ks.key_type == KeyType::Hash)
        .map(|ks| ks.attribute_name.as_str())
        .ok_or_else(|| StorageError::Internal("key schema has no partition key".to_owned()))?;
    let pk_type = attribute_type(attribute_definitions, pk_name)?;
    let pk_text = doc
        .get_str("pk")
        .map_err(|_| StorageError::Internal("document has no pk text".to_owned()))?;
    item.insert(pk_name.to_owned(), pk_value(pk_type, pk_text)?);

    if let Some((sk_name, sk_type)) = sk_info(key_schema, attribute_definitions) {
        let field = format!("sk_{}", sk_suffix(sk_type));
        let value = doc
            .get(&field)
            .ok_or_else(|| StorageError::Internal(format!("document has no {field}")))?;
        item.insert(sk_name.to_owned(), sk_value(sk_type, value, &field)?);
    }

    let mut repaired = item_to_document(&item, key_schema, attribute_definitions)?;
    if let Some(version) = doc.get("_v") {
        repaired.insert("_v", version.clone());
    }
    Ok(repaired)
}

fn attribute_type(
    attribute_definitions: &[AttributeDefinition],
    name: &str,
) -> Result<ScalarAttributeType, StorageError> {
    attribute_definitions
        .iter()
        .find(|d| d.attribute_name == name)
        .map(|d| d.attribute_type)
        .ok_or_else(|| StorageError::Internal(format!("no attribute definition for {name}")))
}

/// The partition key attribute from its `pk` text, inverting
/// `extenddb_storage::util::pk_to_text`.
fn pk_value(pk_type: ScalarAttributeType, text: &str) -> Result<AttributeValue, StorageError> {
    Ok(match pk_type {
        ScalarAttributeType::S => AttributeValue::S(text.to_owned()),
        ScalarAttributeType::N => AttributeValue::N(text.to_owned()),
        ScalarAttributeType::B => AttributeValue::B(
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(|e| StorageError::Internal(format!("pk text is not base64: {e}")))?,
        ),
    })
}

/// The sort key attribute from its typed `sk_*` field, inverting
/// `insert_typed_sk`.
fn sk_value(
    sk_type: ScalarAttributeType,
    value: &Bson,
    field: &str,
) -> Result<AttributeValue, StorageError> {
    match (sk_type, value) {
        (ScalarAttributeType::S, Bson::String(s)) => Ok(AttributeValue::S(s.clone())),
        (ScalarAttributeType::N, Bson::Decimal128(d)) => Ok(AttributeValue::N(d.to_string())),
        (ScalarAttributeType::B, Bson::String(hex)) => Ok(AttributeValue::B(hex_to_bytes(hex)?)),
        _ => Err(StorageError::Internal(format!(
            "{field} holds a {} value, not the sort key type",
            value.element_type() as u8
        ))),
    }
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, StorageError> {
    let bytes = hex.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(StorageError::Internal("sk_b hex has odd length".to_owned()));
    }
    bytes
        .chunks(2)
        .map(|pair| {
            let s = std::str::from_utf8(pair).map_err(|e| StorageError::Internal(e.to_string()))?;
            u8::from_str_radix(s, 16)
                .map_err(|e| StorageError::Internal(format!("sk_b is not hex: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn def(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type,
        }
    }

    fn hash_schema() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
        (
            vec![key("pk", KeyType::Hash)],
            vec![def("pk", ScalarAttributeType::S)],
        )
    }

    fn range_schema(
        sk_type: ScalarAttributeType,
    ) -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
        (
            vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
            vec![def("pk", ScalarAttributeType::S), def("sk", sk_type)],
        )
    }

    fn item(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    #[test]
    fn insert_only_fields_carry_the_id_and_the_key_attributes_for_a_hash_key() {
        let (schema, defs) = hash_schema();
        let key = item(&[("pk", AttributeValue::S("u1".to_owned()))]);
        let fields = insert_only_key_fields(&key, &schema, &defs).expect("fields");
        assert_eq!(fields, doc! { "_id": "u1", "item_data.pk": { "S": "u1" } });
    }

    #[test]
    fn insert_only_fields_carry_both_key_attributes_for_a_range_key() {
        let (schema, defs) = range_schema(ScalarAttributeType::N);
        let key = item(&[
            ("pk", AttributeValue::S("u1".to_owned())),
            ("sk", AttributeValue::N("42".to_owned())),
        ]);
        let fields = insert_only_key_fields(&key, &schema, &defs).expect("fields");
        let full = item_to_document(&key, &schema, &defs).expect("document");
        assert_eq!(fields.get("_id"), full.get("_id"));
        assert_eq!(
            fields.get("item_data.pk"),
            Some(&bson::bson!({ "S": "u1" }))
        );
        assert_eq!(
            fields.get("item_data.sk"),
            Some(&bson::bson!({ "N": "42" }))
        );
        assert_eq!(
            fields.len(),
            3,
            "no pk or sk_n field: the filter supplies those"
        );
    }

    /// The document MongoDB inserts for the pre-fix upsert on a hash table,
    /// as measured on MongoDB 7.0: the filter's `pk`, the `$set` attribute,
    /// the `$inc` version, and a generated `_id`.
    fn stored_without_key_hash() -> Document {
        doc! {
            "_id": bson::oid::ObjectId::new(),
            "pk": "u1",
            "item_data": { "w": { "S": "upd" } },
            "_v": 3_i64,
        }
    }

    #[test]
    fn a_generated_object_id_marks_a_document_stored_without_its_key() {
        let (schema, defs) = hash_schema();
        assert!(is_stored_without_key(&stored_without_key_hash()));
        let good = item_to_document(
            &item(&[("pk", AttributeValue::S("u1".to_owned()))]),
            &schema,
            &defs,
        )
        .expect("document");
        assert!(!is_stored_without_key(&good));
    }

    #[test]
    fn repair_restores_a_hash_key_and_keeps_attributes_and_version() {
        let (schema, defs) = hash_schema();
        let repaired =
            repaired_document(&stored_without_key_hash(), &schema, &defs).expect("repair");
        let want_item = item(&[
            ("pk", AttributeValue::S("u1".to_owned())),
            ("w", AttributeValue::S("upd".to_owned())),
        ]);
        let mut want = item_to_document(&want_item, &schema, &defs).expect("document");
        want.insert("_v", 3_i64);
        assert_eq!(repaired, want);
    }

    #[test]
    fn repair_restores_string_numeric_and_binary_sort_keys() {
        let cases = [
            (ScalarAttributeType::S, AttributeValue::S("s1".to_owned())),
            (ScalarAttributeType::N, AttributeValue::N("12.5".to_owned())),
            (
                ScalarAttributeType::B,
                AttributeValue::B(vec![0x01, 0xff, 0x10]),
            ),
        ];
        for (sk_type, sk) in cases {
            let (schema, defs) = range_schema(sk_type);
            let want_item = item(&[
                ("pk", AttributeValue::S("u1".to_owned())),
                ("sk", sk.clone()),
                ("w", AttributeValue::S("upd".to_owned())),
            ]);
            let want = item_to_document(&want_item, &schema, &defs).expect("document");
            // The pre-fix upsert copied the filter's pk and sk_* fields and
            // set only the non-key attribute.
            let sk_field = format!("sk_{}", sk_suffix(sk_type));
            let bad = doc! {
                "_id": bson::oid::ObjectId::new(),
                "pk": "u1",
                &sk_field: want.get(&sk_field).cloned().expect("sort key field"),
                "item_data": { "w": { "S": "upd" } },
            };
            let repaired = repaired_document(&bad, &schema, &defs).expect("repair");
            assert_eq!(repaired, want, "{sk_type:?} sort key");
            assert_eq!(
                document_to_item(&repaired).expect("item"),
                want_item,
                "{sk_type:?} sort key item"
            );
        }
    }

    #[test]
    fn repair_refuses_a_document_whose_sort_key_field_has_the_wrong_type() {
        let (schema, defs) = range_schema(ScalarAttributeType::N);
        let bad = doc! {
            "_id": bson::oid::ObjectId::new(),
            "pk": "u1",
            "sk_n": "not a decimal",
            "item_data": { "w": { "S": "upd" } },
        };
        assert!(repaired_document(&bad, &schema, &defs).is_err());
    }
}
