// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data engine helpers for the `MongoDB` backend.
//!
//! Contains document conversion, collection naming, and key extraction utilities.

use std::collections::BTreeMap;

use bson::{Bson, Document, doc};

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    composite_pk_to_text, encode_netstring_composite, pk_to_text, sk_info,
};

/// Returns the `MongoDB` collection name for a `DynamoDB` table.
pub fn data_collection_name(table_id: &str) -> String {
    format!("_ddb_{table_id}")
}

/// Build the mongo document `_id` for a composite (partition + sort) key.
///
/// Uses netstring encoding — `<len>:<part>,<len>:<part>,` — so the boundary
/// between `pk` and `sk` is unambiguous regardless of the contents of
/// either. A naive `"{pk}#{sk}"` scheme collides when `pk` or `sk` contains
/// the delimiter (e.g., `pk="a#b", sk="c"` and `pk="a", sk="b#c"` both
/// produce `"a#b#c"`).
#[must_use]
pub fn composite_id(pk_text: &str, sk_text: &str) -> String {
    encode_netstring_composite(&[pk_text.to_owned(), sk_text.to_owned()])
}

/// Returns the `MongoDB` collection name for a secondary index.
pub fn index_collection_name(index_id: &str) -> String {
    format!("_ddb_{index_id}")
}

/// Convert a `DynamoDB` Item to a `MongoDB` BSON document for storage.
///
/// Document structure: `{ _id, pk, sk_s/sk_n/sk_b, item_data }`
pub fn item_to_document(
    item: &Item,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let pk_text = composite_pk_to_text(item, key_schema)?;

    // Serialize the full item as item_data
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
    let item_bson = bson::to_bson(&item_json).map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut doc = Document::new();

    // Build the _id field
    if let Some((sk_name, sk_type)) = sk_info(key_schema, attribute_definitions) {
        let sk_value = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk_text = sk_to_text(sk_value)?;
        // Netstring-encoded composite _id — see composite_id() for why the
        // naive "{pk}#{sk}" form is collision-prone.
        doc.insert("_id", composite_id(&pk_text, &sk_text));
        doc.insert("pk", pk_text);

        // Insert the typed sort key field
        match sk_type {
            ScalarAttributeType::S => {
                if let AttributeValue::S(s) = sk_value {
                    doc.insert("sk_s", s.clone());
                }
            }
            ScalarAttributeType::N => {
                if let AttributeValue::N(n) = sk_value {
                    // Store as Decimal128 for correct numeric ordering.
                    // Values that exceed Decimal128's 34 significant digits (or
                    // any parse failure) are rejected rather than downcasting to
                    // f64, which would silently lose precision and can produce
                    // incorrect ordering. DynamoDB supports up to 38 digits; this
                    // limitation is documented in docs/differences-from-dynamodb.md.
                    let d = n.parse::<bson::Decimal128>().map_err(|_| {
                        StorageError::Validation(format!(
                            "Numeric sort key value '{n}' exceeds supported precision (Decimal128, 34 significant digits)"
                        ))
                    })?;
                    doc.insert("sk_n", d);
                }
            }
            ScalarAttributeType::B => {
                if let AttributeValue::B(b) = sk_value {
                    doc.insert(
                        "sk_b",
                        bson::Binary {
                            subtype: bson::spec::BinarySubtype::Generic,
                            bytes: b.clone(),
                        },
                    );
                }
            }
        }
    } else {
        // PK-only table
        doc.insert("_id", pk_text.clone());
        doc.insert("pk", pk_text);
    }

    doc.insert("item_data", item_bson);
    Ok(doc)
}

/// Convert a `MongoDB` document back to a `DynamoDB` Item.
pub fn document_to_item(doc: &Document) -> Result<Item, StorageError> {
    let item_data = doc
        .get("item_data")
        .ok_or_else(|| StorageError::Internal("Document missing item_data field".to_string()))?;

    let json_value: serde_json::Value = bson::from_bson(item_data.clone())
        .map_err(|e| StorageError::Internal(format!("BSON to JSON conversion error: {e}")))?;

    let item: Item = serde_json::from_value(json_value)
        .map_err(|e| StorageError::Internal(format!("JSON to Item conversion error: {e}")))?;

    Ok(item)
}

/// Convert a sort key value to text for use in the _id field.
fn sk_to_text(value: &AttributeValue) -> Result<String, StorageError> {
    match value {
        AttributeValue::S(s) => Ok(s.clone()),
        AttributeValue::N(n) => Ok(n.clone()),
        AttributeValue::B(b) => {
            use base64::Engine;
            Ok(base64::engine::general_purpose::STANDARD.encode(b))
        }
        _ => Err(StorageError::Internal(
            "sort key must be S, N, or B".to_owned(),
        )),
    }
}

/// Build a primary key filter for `MongoDB` queries.
pub fn pk_filter(
    key: &Item,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let pk_text = composite_pk_to_text(key, key_schema)?;
    let mut filter = doc! { "pk": &pk_text };

    if let Some((sk_name, sk_type)) = sk_info(key_schema, attribute_definitions) {
        let sk_value = key
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key in key".to_owned()))?;
        match sk_type {
            ScalarAttributeType::S => {
                if let AttributeValue::S(s) = sk_value {
                    filter.insert("sk_s", s.clone());
                }
            }
            ScalarAttributeType::N => {
                if let AttributeValue::N(n) = sk_value {
                    let d = n.parse::<bson::Decimal128>().map_err(|_| {
                        StorageError::Validation(format!(
                            "Numeric key value '{n}' exceeds supported precision (Decimal128, 34 significant digits)"
                        ))
                    })?;
                    filter.insert("sk_n", d);
                }
            }
            ScalarAttributeType::B => {
                if let AttributeValue::B(b) = sk_value {
                    filter.insert(
                        "sk_b",
                        bson::Binary {
                            subtype: bson::spec::BinarySubtype::Generic,
                            bytes: b.clone(),
                        },
                    );
                }
            }
        }
    }

    Ok(filter)
}

/// Get the sort key column name for a table.
pub fn sk_field_name(
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Option<&'static str> {
    sk_info(key_schema, attribute_definitions).map(|(_, sk_type)| match sk_type {
        ScalarAttributeType::S => "sk_s",
        ScalarAttributeType::N => "sk_n",
        ScalarAttributeType::B => "sk_b",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_pk_str_sk_num() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
        (
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_owned(),
                    key_type: KeyType::Range,
                },
            ],
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_owned(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
        )
    }

    #[test]
    fn item_to_document_rejects_numeric_sort_key_exceeding_decimal128() {
        let (schema, attrs) = schema_pk_str_sk_num();
        // 35 significant digits — exceeds Decimal128's 34-digit precision.
        let over_precision = "1".to_owned() + &"2".repeat(34);
        assert_eq!(
            over_precision
                .chars()
                .filter(|c| c.is_ascii_digit())
                .count(),
            35
        );

        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("x".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N(over_precision.clone()));

        let err = item_to_document(&item, &schema, &attrs).unwrap_err();
        match err {
            StorageError::Validation(msg) => {
                assert!(msg.contains(&over_precision));
                assert!(msg.contains("Decimal128"));
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    #[test]
    fn item_to_document_accepts_numeric_sort_key_at_decimal128_boundary() {
        let (schema, attrs) = schema_pk_str_sk_num();
        // 34 significant digits — at the Decimal128 boundary.
        let at_boundary = "1".repeat(34);

        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("x".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N(at_boundary));

        assert!(item_to_document(&item, &schema, &attrs).is_ok());
    }

    #[test]
    fn pk_filter_rejects_numeric_sort_key_exceeding_decimal128() {
        let (schema, attrs) = schema_pk_str_sk_num();
        let over_precision = "1".to_owned() + &"2".repeat(34);
        let mut key = Item::new();
        key.insert("pk".to_owned(), AttributeValue::S("x".to_owned()));
        key.insert("sk".to_owned(), AttributeValue::N(over_precision));

        let err = pk_filter(&key, &schema, &attrs).unwrap_err();
        assert!(matches!(err, StorageError::Validation(_)));
    }

    #[test]
    fn composite_id_disambiguates_delimiter_in_pk_or_sk() {
        // Two items whose naive "{pk}#{sk}" strings would collide must
        // produce distinct netstring-encoded _ids.
        let a = composite_id("a#b", "c");
        let b = composite_id("a", "b#c");
        assert_ne!(
            a, b,
            "composite _id must not collide on delimiter-containing keys"
        );
    }

    #[test]
    fn composite_id_stable_on_normal_inputs() {
        // Reasonable inputs still round-trip through netstring cleanly.
        assert_eq!(
            composite_id("user1", "2024-01-01"),
            "5:user1,10:2024-01-01,"
        );
        assert_eq!(composite_id("", "sk"), "0:,2:sk,");
        assert_eq!(composite_id("pk", ""), "2:pk,0:,");
    }

    #[test]
    fn composite_id_is_written_by_item_to_document() {
        let (schema, attrs) = schema_pk_str_sk_num();
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("user1".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N("42".to_owned()));

        let doc = item_to_document(&item, &schema, &attrs).unwrap();
        let id = doc.get_str("_id").unwrap();
        assert!(
            id.starts_with("5:user1,"),
            "expected netstring-encoded _id, got {id:?}"
        );
    }
}
