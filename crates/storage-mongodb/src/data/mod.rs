// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data engine helpers for the `MongoDB` backend.
//!
//! Contains document conversion, collection naming, and key extraction utilities.

use bson::{Document, doc};

#[cfg(test)]
use extenddb_core::types::KeyType;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    composite_pk_to_text, encode_netstring_composite, escape_json_keys, sk_info, unescape_json_keys,
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

/// Convert an item (base item or projected index item) to the BSON stored
/// under `item_data`.
///
/// BSON field names are C strings and cannot contain U+0000, while `DynamoDB`
/// accepts it in attribute names and map keys. Every object key in the item's
/// JSON form is run through the shared order-preserving escape (U+0000 and
/// U+0001 become two-character sequences, see
/// `extenddb_storage::util::escape_control`) before the BSON conversion, so
/// the stored document never carries a NUL field name. String values are left
/// alone; BSON strings are length-prefixed and hold U+0000 verbatim.
///
/// Every conversion of an item to `item_data` BSON must go through this
/// function or [`json_to_item_data`]; a direct `bson::to_bson` on item JSON
/// fails in the driver at write time when a name contains U+0000.
/// [`item_data_to_json`] reverses the escape on read.
pub(crate) fn item_to_item_data(item: &Item) -> Result<bson::Bson, StorageError> {
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
    json_to_item_data(item_json)
}

/// JSON-tree variant of [`item_to_item_data`], for payloads that carry items
/// nested inside another structure (stream records with key/image maps).
pub(crate) fn json_to_item_data(json: serde_json::Value) -> Result<bson::Bson, StorageError> {
    let escaped = escape_json_keys(json);
    bson::to_bson(&escaped).map_err(|e| StorageError::Internal(e.to_string()))
}

/// Reverse of [`json_to_item_data`]: BSON read from storage back to JSON with
/// the object-key escape undone.
pub(crate) fn item_data_to_json(value: &bson::Bson) -> Result<serde_json::Value, StorageError> {
    let json: serde_json::Value = bson::from_bson(value.clone())
        .map_err(|e| StorageError::Internal(format!("BSON to JSON conversion error: {e}")))?;
    Ok(unescape_json_keys(json))
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

    // Serialize the full item as item_data (object keys escaped, see
    // item_to_item_data).
    let item_bson = item_to_item_data(item)?;

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
        let sk_field = format!("sk_{}", sk_suffix(sk_type));
        insert_typed_sk(&mut doc, &sk_field, sk_type, sk_value)?;
    } else {
        // PK-only table
        doc.insert("_id", pk_text.clone());
        doc.insert("pk", pk_text);
    }

    doc.insert("item_data", item_bson);
    Ok(doc)
}

/// Insert a typed sort-key value into a document under the given field name.
///
/// Shared between base tables (`sk_s`/`sk_n`/`sk_b`) and index documents
/// carrying the base table's sort key (`base_sk_s`/`base_sk_n`/`base_sk_b`).
fn insert_typed_sk(
    doc: &mut Document,
    field: &str,
    sk_type: ScalarAttributeType,
    sk_value: &AttributeValue,
) -> Result<(), StorageError> {
    match (sk_type, sk_value) {
        (ScalarAttributeType::S, AttributeValue::S(s)) => {
            doc.insert(field, s.clone());
        }
        (ScalarAttributeType::N, AttributeValue::N(n)) => {
            // Store as Decimal128 for correct numeric ordering. Values that
            // exceed Decimal128's 34 significant digits are rejected rather
            // than downcasting to f64, which would silently lose precision.
            let d = n.parse::<bson::Decimal128>().map_err(|_| {
                StorageError::Validation(format!(
                    "Numeric sort key value '{n}' exceeds supported precision (Decimal128, 34 significant digits)"
                ))
            })?;
            doc.insert(field, d);
        }
        (ScalarAttributeType::B, AttributeValue::B(b)) => {
            // Store as hex string, not BSON Binary. See `binary_sk_to_hex`
            // for the rationale — MongoDB's Binary sort order diverges
            // from DDB's unsigned-lex byte order for unequal-length
            // values (D-M5 / RFC-0003 §1.4).
            doc.insert(field, binary_sk_to_hex(b));
        }
        _ => {
            // Mismatched types are silently skipped — matches the existing
            // behavior of item_to_document. Callers can rely on
            // validate_index_keys / validate_item_keys upstream to reject
            // these before write.
        }
    }
    Ok(())
}

/// Build a `MongoDB` index-collection document.
///
/// Index documents differ from base-table documents in two ways:
///
/// 1. The `_id` incorporates the base-table primary key in addition to the
///    index primary key. GSI keys are non-unique — multiple base items can
///    share identical `(index_pk, index_sk)` values. Encoding the base key
///    into `_id` gives each index entry a unique identity keyed to the base
///    item it describes.
///
/// 2. The document carries the base-table key attributes as first-class
///    fields — `base_pk` (text) and `base_sk_s`/`base_sk_n`/`base_sk_b`
///    (typed). This lets index pagination form a compound cursor
///    `(index_sk, base_pk, base_sk)` without traversing the JSON
///    `item_data` payload for base-key values.
///
/// The `item_data` payload is unchanged — it is the full projected item
/// as serialized by `AttributeValue`.
///
/// `projected` is the item projected into the index (see `project_item` in
/// `data_engine.rs`). It must contain both the index-key attributes and
/// the base-table key attributes.
pub fn index_document(
    projected: &Item,
    idx_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let idx_pk_text = composite_pk_to_text(projected, idx_key_schema)?;
    let base_pk_text = composite_pk_to_text(projected, base_key_schema)?;

    let idx_sk = sk_info(idx_key_schema, attribute_definitions);
    let base_sk = sk_info(base_key_schema, attribute_definitions);

    // Build the netstring composite _id. Order:
    //   [index_pk, index_sk_or_"", base_pk, base_sk_or_""]
    // Netstring parts are self-delimiting, so absent sk components encode as
    // "0:," and the boundary is preserved.
    let idx_sk_text = match idx_sk {
        Some((sk_name, _)) => projected
            .get(sk_name)
            .map(sk_to_text)
            .transpose()?
            .unwrap_or_default(),
        None => String::new(),
    };
    let base_sk_text = match base_sk {
        Some((sk_name, _)) => projected
            .get(sk_name)
            .map(sk_to_text)
            .transpose()?
            .unwrap_or_default(),
        None => String::new(),
    };
    let id = encode_netstring_composite(&[
        idx_pk_text.clone(),
        idx_sk_text,
        base_pk_text.clone(),
        base_sk_text,
    ]);

    let mut doc = Document::new();
    doc.insert("_id", id);
    doc.insert("pk", &idx_pk_text);
    doc.insert("base_pk", &base_pk_text);

    if let Some((sk_name, sk_type)) = idx_sk
        && let Some(sk_value) = projected.get(sk_name)
    {
        let field = format!("sk_{}", sk_suffix(sk_type));
        insert_typed_sk(&mut doc, &field, sk_type, sk_value)?;
    }
    if let Some((sk_name, sk_type)) = base_sk
        && let Some(sk_value) = projected.get(sk_name)
    {
        let field = format!("base_sk_{}", sk_suffix(sk_type));
        insert_typed_sk(&mut doc, &field, sk_type, sk_value)?;
    }

    let item_bson = item_to_item_data(projected)?;
    doc.insert("item_data", item_bson);

    Ok(doc)
}

/// Build a delete filter for a specific index entry.
///
/// The filter must match the exact base item's index entry, so it needs
/// both the index-key and base-key components — a filter on index keys
/// alone would delete every base item's entry that shares those index
/// keys (silent data loss on GSIs with duplicate keys). Returns a filter
/// on `(pk, sk?, base_pk, base_sk?)` — the same tuple that composes
/// the `_id`, but we filter on the individual fields so mongo can use
/// per-field indexes if present.
pub fn index_entry_filter(
    projected: &Item,
    idx_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<Document, StorageError> {
    let idx_pk_text = composite_pk_to_text(projected, idx_key_schema)?;
    let base_pk_text = composite_pk_to_text(projected, base_key_schema)?;
    let mut filter = doc! {
        "pk": idx_pk_text,
        "base_pk": base_pk_text,
    };

    if let Some((sk_name, sk_type)) = sk_info(idx_key_schema, attribute_definitions)
        && let Some(sk_value) = projected.get(sk_name)
    {
        let field = format!("sk_{}", sk_suffix(sk_type));
        insert_typed_sk(&mut filter, &field, sk_type, sk_value)?;
    }
    if let Some((sk_name, sk_type)) = sk_info(base_key_schema, attribute_definitions)
        && let Some(sk_value) = projected.get(sk_name)
    {
        let field = format!("base_sk_{}", sk_suffix(sk_type));
        insert_typed_sk(&mut filter, &field, sk_type, sk_value)?;
    }
    Ok(filter)
}

/// Sort-key column suffix for a scalar attribute type. Shared by index and
/// base-key field naming.
#[must_use]
pub fn sk_suffix(sk_type: ScalarAttributeType) -> &'static str {
    match sk_type {
        ScalarAttributeType::S => "s",
        ScalarAttributeType::N => "n",
        ScalarAttributeType::B => "b",
    }
}

/// Convert a `MongoDB` document back to a `DynamoDB` Item.
pub fn document_to_item(doc: &Document) -> Result<Item, StorageError> {
    let item_data = doc
        .get("item_data")
        .ok_or_else(|| StorageError::Internal("Document missing item_data field".to_string()))?;

    let json_value = item_data_to_json(item_data)?;

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

/// Encode a byte slice as a lowercase hex string.
///
/// Used to store binary sort keys as strings in the typed
/// `sk_b`/`base_sk_b` fields. Lexicographic comparison of hex-encoded
/// strings preserves DynamoDB's unsigned-lex byte order — MongoDB's
/// native BSON Binary comparison is length-first-then-content, which
/// diverges from DDB for values of different lengths (e.g., DDB says
/// `[0x01,0xFF] < [0x02]`; BSON Binary reverses that). Hex strings
/// also make `begins_with` implementable as a plain string range
/// filter instead of a full-partition post-fetch scan. RFC-0003 §1.4.
#[must_use]
pub fn binary_sk_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
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
                    // Hex-encoded string, matching how insert_typed_sk
                    // writes sk_b — see D-M5.
                    filter.insert("sk_b", binary_sk_to_hex(b));
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

    /// Walk a BSON tree and assert no document field name contains U+0000.
    fn assert_no_nul_field_names(value: &bson::Bson) {
        match value {
            bson::Bson::Document(doc) => {
                for (k, v) in doc {
                    assert!(!k.contains('\u{0}'), "field name contains NUL: {k:?}");
                    assert_no_nul_field_names(v);
                }
            }
            bson::Bson::Array(items) => {
                for item in items {
                    assert_no_nul_field_names(item);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn item_document_round_trips_control_characters_in_names() {
        use std::collections::BTreeMap;

        let (schema, attrs) = schema_pk_str_sk_num();

        let mut inner = BTreeMap::new();
        inner.insert(
            "deep\u{0}key".to_owned(),
            AttributeValue::S("v\u{0}".to_owned()),
        );
        let mut mid = BTreeMap::new();
        mid.insert("mid\u{1}key".to_owned(), AttributeValue::M(inner));
        let mut in_list = BTreeMap::new();
        in_list.insert(
            "list\u{0}\u{1}key".to_owned(),
            AttributeValue::S("w".to_owned()),
        );

        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("p".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N("1".to_owned()));
        item.insert(
            "top\u{0}\u{1}name".to_owned(),
            AttributeValue::S("x".to_owned()),
        );
        item.insert("map".to_owned(), AttributeValue::M(mid));
        item.insert(
            "list".to_owned(),
            AttributeValue::L(vec![AttributeValue::M(in_list)]),
        );

        let doc = item_to_document(&item, &schema, &attrs).unwrap();
        assert_no_nul_field_names(&bson::Bson::Document(doc.clone()));

        let back = document_to_item(&doc).unwrap();
        assert_eq!(back, item);
    }

    #[test]
    fn plain_item_document_stores_raw_field_names() {
        use std::collections::BTreeMap;

        let (schema, attrs) = schema_pk_str_sk_num();

        let mut inner = BTreeMap::new();
        inner.insert("k".to_owned(), AttributeValue::S("v".to_owned()));
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("p".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N("1".to_owned()));
        item.insert("attr".to_owned(), AttributeValue::M(inner));

        let doc = item_to_document(&item, &schema, &attrs).unwrap();
        let item_data = doc.get_document("item_data").unwrap();
        let names: Vec<&str> = item_data.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["attr", "pk", "sk"]);
        let attr_m = item_data
            .get_document("attr")
            .unwrap()
            .get_document("M")
            .unwrap();
        let map_keys: Vec<&str> = attr_m.keys().map(String::as_str).collect();
        assert_eq!(map_keys, vec!["k"]);
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

    // ── index_document / index_entry_filter ─────────────────────────

    fn base_schema_composite() -> (Vec<KeySchemaElement>, Vec<AttributeDefinition>) {
        (
            vec![
                KeySchemaElement {
                    attribute_name: "customer_id".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "order_id".to_owned(),
                    key_type: KeyType::Range,
                },
            ],
            vec![
                AttributeDefinition {
                    attribute_name: "customer_id".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "order_id".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "status".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "priority".to_owned(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
        )
    }

    fn gsi_schema_hash_range() -> Vec<KeySchemaElement> {
        vec![
            KeySchemaElement {
                attribute_name: "status".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "priority".to_owned(),
                key_type: KeyType::Range,
            },
        ]
    }

    fn item_with(customer_id: &str, order_id: &str, status: &str, priority: &str) -> Item {
        let mut item = Item::new();
        item.insert(
            "customer_id".to_owned(),
            AttributeValue::S(customer_id.to_owned()),
        );
        item.insert(
            "order_id".to_owned(),
            AttributeValue::S(order_id.to_owned()),
        );
        item.insert("status".to_owned(), AttributeValue::S(status.to_owned()));
        item.insert(
            "priority".to_owned(),
            AttributeValue::N(priority.to_owned()),
        );
        item
    }

    #[test]
    fn index_document_encodes_base_keys() {
        let (base_schema, attrs) = base_schema_composite();
        let idx_schema = gsi_schema_hash_range();
        let item = item_with("cust1", "order1", "pending", "5");

        let doc = index_document(&item, &idx_schema, &base_schema, &attrs).unwrap();
        assert_eq!(doc.get_str("pk").unwrap(), "pending");
        assert_eq!(doc.get_str("base_pk").unwrap(), "cust1");
        // Index sk (N) is Decimal128, base sk (S) is a plain string.
        assert!(doc.get("sk_n").is_some(), "expected sk_n field");
        assert_eq!(doc.get_str("base_sk_s").unwrap(), "order1");
    }

    #[test]
    fn index_document_id_disambiguates_duplicate_index_keys() {
        // Two base items sharing (status, priority) but different base keys
        // must produce distinct index _ids. Without base keys in _id both
        // upserts would write to the same document — the D-C1 data-loss bug.
        let (base_schema, attrs) = base_schema_composite();
        let idx_schema = gsi_schema_hash_range();

        let a = item_with("custA", "orderA", "pending", "5");
        let b = item_with("custB", "orderB", "pending", "5");

        let da = index_document(&a, &idx_schema, &base_schema, &attrs).unwrap();
        let db = index_document(&b, &idx_schema, &base_schema, &attrs).unwrap();
        assert_ne!(da.get_str("_id").unwrap(), db.get_str("_id").unwrap());
    }

    #[test]
    fn index_entry_filter_matches_own_document() {
        // The filter built for a projected item must select exactly that
        // item's index document — same _id, base_pk, and base_sk fields.
        let (base_schema, attrs) = base_schema_composite();
        let idx_schema = gsi_schema_hash_range();
        let item = item_with("cust1", "order1", "pending", "5");

        let doc = index_document(&item, &idx_schema, &base_schema, &attrs).unwrap();
        let filter = index_entry_filter(&item, &idx_schema, &base_schema, &attrs).unwrap();
        // Every filter field must appear in the doc with the same value.
        for (k, v) in filter.iter() {
            let actual = doc.get(k).expect("filter field missing on doc");
            assert_eq!(v, actual, "filter field {k} mismatch");
        }
    }

    #[test]
    fn index_document_supports_hash_only_gsi_on_composite_base() {
        // R-2 shape: hash-only GSI on a composite base table. The doc must
        // still carry base_pk and base_sk so pagination can tie-break.
        let (base_schema, attrs) = base_schema_composite();
        let idx_schema = vec![KeySchemaElement {
            attribute_name: "status".to_owned(),
            key_type: KeyType::Hash,
        }];
        let item = item_with("cust1", "order1", "pending", "5");

        let doc = index_document(&item, &idx_schema, &base_schema, &attrs).unwrap();
        assert_eq!(doc.get_str("pk").unwrap(), "pending");
        assert!(
            doc.get("sk_s").is_none() && doc.get("sk_n").is_none() && doc.get("sk_b").is_none()
        );
        assert_eq!(doc.get_str("base_pk").unwrap(), "cust1");
        assert_eq!(doc.get_str("base_sk_s").unwrap(), "order1");
    }

    #[test]
    fn binary_sk_to_hex_preserves_ddb_byte_order() {
        // DynamoDB compares binary sort keys as unsigned lex bytes.
        // The stored hex-string form must preserve that ordering under
        // MongoDB's default lexicographic string comparison — verify
        // both same-length and cross-length cases.
        let a = binary_sk_to_hex(&[0x01, 0xff]);
        let b = binary_sk_to_hex(&[0x02]);
        // DDB: [0x01, 0xff] < [0x02]. Hex: "01ff" < "02".
        assert!(a < b, "{a} < {b}");

        // Shorter-prefix rule: [0x01] < [0x01, 0x00] in DDB.
        let a = binary_sk_to_hex(&[0x01]);
        let b = binary_sk_to_hex(&[0x01, 0x00]);
        assert!(a < b);

        // Same first byte, longer runner in DDB.
        let a = binary_sk_to_hex(&[0x01, 0x00, 0x00]);
        let b = binary_sk_to_hex(&[0x02]);
        assert!(a < b);

        // Empty is the smallest.
        let empty = binary_sk_to_hex(&[]);
        let single = binary_sk_to_hex(&[0x00]);
        assert!(empty < single);
    }
}
