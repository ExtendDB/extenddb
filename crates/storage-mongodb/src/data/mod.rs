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
use extenddb_storage::util::{composite_pk_to_text, pk_to_text, sk_info};

/// Returns the `MongoDB` collection name for a `DynamoDB` table.
pub fn data_collection_name(table_id: &str) -> String {
    format!("_ddb_{table_id}")
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
        doc.insert("_id", format!("{pk_text}#{sk_text}"));
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
                    // Store as Decimal128 for proper numeric ordering
                    match n.parse::<bson::Decimal128>() {
                        Ok(d) => {
                            doc.insert("sk_n", d);
                        }
                        Err(_) => {
                            // Fallback: try parsing as f64
                            if let Ok(f) = n.parse::<f64>() {
                                doc.insert("sk_n", f);
                            } else {
                                return Err(StorageError::Internal(format!(
                                    "Cannot convert sort key '{n}' to numeric BSON type"
                                )));
                            }
                        }
                    }
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
                    match n.parse::<bson::Decimal128>() {
                        Ok(d) => {
                            filter.insert("sk_n", d);
                        }
                        Err(_) => {
                            if let Ok(f) = n.parse::<f64>() {
                                filter.insert("sk_n", f);
                            }
                        }
                    }
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
