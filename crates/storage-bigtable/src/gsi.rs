// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Global Secondary Index support via shadow tables.
//!
//! Each GSI gets its own BigTable table named `<base_data_table>_g<8hex>`
//! (8 hex chars from a stable hash of the index name to fit BigTable's
//! 50-char table-id limit).
//!
//! GSI Index Row Key layout:
//! - Single-HASH GSI partition prefix: `[gsi_pk_tag:1][gsi_pk_len:4][gsi_pk_bytes]`
//! - Multi-HASH GSI partition prefix (`2..=4` HASH keys):
//!   `[0x4D ('M'):1][inner_len:4][([tag_i:1][len_i:4][bytes_i])...]`
//! - Followed by zero or more GSI sort-key segments (`0..=4` RANGE keys in schema order):
//!   `([gsi_sk_tag_j:1][escape_encoded(gsi_sk_bytes_j)][0x00 0x00])...`
//! - Followed by base table key suffix:
//!   `[base_pk_tag:1][base_pk_len:4][base_pk_bytes] ([base_sk_tag:1][base_sk_bytes])?`
//!
//! Non-terminal `gsi_sk` segments are encoded using FoundationDB tuple
//! order-preserving escape encoding (`0x00 -> 0x00 0xFF`, terminated with `0x00 0x00`),
//! so Bigtable's natural row-key byte order matches DynamoDB's lexicographic order
//! across all `RANGE` attributes and base-key tie-breakers. The terminal `base_sk`
//! segment remains raw bytes to the end of the row key.

use std::collections::BTreeMap;

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, Projection, ProjectionType,
};
use extenddb_storage::error::StorageError;

use crate::data::encoding::row_key;

/// Order-preserving escape encoding for non-terminal byte segments
/// (FoundationDB tuple encoding pattern). Replaces each `0x00` byte with `0x00 0xFF`.
pub fn escape_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 4);
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out
}

/// Order-preserving escape encoding terminated with `0x00 0x00`.
pub fn escape_bytes_terminated(bytes: &[u8]) -> Vec<u8> {
    let mut out = escape_bytes(bytes);
    out.push(0x00);
    out.push(0x00);
    out
}

/// Decode an escape-encoded segment from a byte slice starting at `idx`.
/// Returns the unescaped bytes and the new index immediately after `0x00 0x00`.
#[cfg(any(test, feature = "test-hooks"))]
pub fn unescape_bytes_terminated(
    bytes: &[u8],
    mut idx: usize,
) -> Result<(Vec<u8>, usize), StorageError> {
    let mut out = Vec::new();
    while idx < bytes.len() {
        if bytes[idx] == 0x00 {
            if idx + 1 >= bytes.len() {
                return Err(StorageError::Internal(
                    "malformed escaped bytes: trailing 0x00".into(),
                ));
            }
            match bytes[idx + 1] {
                0x00 => {
                    // 0x00 0x00 is the segment terminator
                    return Ok((out, idx + 2));
                }
                0xFF => {
                    // 0x00 0xFF is an escaped literal 0x00
                    out.push(0x00);
                    idx += 2;
                }
                other => {
                    return Err(StorageError::Internal(format!(
                        "malformed escaped bytes: 0x00 followed by invalid byte 0x{other:02X}"
                    )));
                }
            }
        } else {
            out.push(bytes[idx]);
            idx += 1;
        }
    }
    Err(StorageError::Internal(
        "malformed escaped bytes: missing terminator 0x00 0x00".into(),
    ))
}

/// Derive the BigTable shadow-table id for a given (base_data_table, index_name).
pub fn shadow_table_id(base_data_table: &str, index_name: &str) -> String {
    // FNV-1a over the index name → 8 hex chars; deterministic and fits in
    // the BigTable table-name length budget.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in index_name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{base_data_table}_g{:08x}", (h & 0xFFFF_FFFF) as u32)
}

/// Returns `true` when `table_id_short` names a GSI shadow table (`<base>_g<8hex>`)
/// rather than a base data table (`t<16hex>`).
#[must_use]
pub fn is_shadow_table_id(table_id_short: &str) -> bool {
    table_id_short.contains("_g")
}

/// Outer partition-prefix tag for multi-HASH (`2..=4` HASH keys) GSI shadow rows.
/// Distinct from `S` (`0x53`), `N` (`0x4E`), and `B` (`0x42`).
pub const TAG_MULTI_HASH: u8 = 0x4D;

fn tag_bytes_for(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    row_key::sk_tag_and_bytes(av)
}

/// Encode `1..=4` HASH attribute values (in key-schema order) into a self-delimiting
/// `[tag:1][len:4][payload]` partition prefix.
///
/// For a single HASH key, this matches `row_key::pk_range_start` byte-for-byte.
/// For multiple HASH keys, each part is length-framed inside a `TAG_MULTI_HASH`
/// (`0x4D`) outer `[tag:1][len:4][inner]` envelope so `pk_prefix_slice` and
/// parallel-scan CRC32 segment hashing work uniformly.
pub fn encode_gsi_pk_prefix(hash_vals: &[&AttributeValue]) -> Result<Vec<u8>, StorageError> {
    match hash_vals {
        [] => Err(StorageError::Validation("GSI missing HASH key".into())),
        [single] => {
            let (tag, bytes) = tag_bytes_for(single)?;
            let mut out = Vec::with_capacity(5 + bytes.len());
            out.push(tag);
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
            Ok(out)
        }
        many => {
            let mut inner = Vec::with_capacity(many.len() * 16);
            for v in many {
                let (tag, bytes) = tag_bytes_for(v)?;
                inner.push(tag);
                inner.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                inner.extend_from_slice(&bytes);
            }
            let mut out = Vec::with_capacity(5 + inner.len());
            out.push(TAG_MULTI_HASH);
            out.extend_from_slice(&(inner.len() as u32).to_be_bytes());
            out.extend_from_slice(&inner);
            Ok(out)
        }
    }
}

/// Build the shadow row key for a given item, knowing both the GSI key schema
/// and the base table's key schema. Returns None if the item lacks any of the
/// GSI key attributes (sparse-index semantics — that item simply doesn't get
/// a shadow entry).
pub fn shadow_row_key_for_item(
    item: &Item,
    gsi_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
) -> Result<Option<Vec<u8>>, StorageError> {
    let mut hash_vals: Vec<&AttributeValue> = Vec::with_capacity(4);
    for ks in gsi_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
    {
        let Some(val) = item.get(&ks.attribute_name) else {
            return Ok(None);
        };
        hash_vals.push(val);
    }
    if hash_vals.is_empty() {
        return Err(StorageError::Validation("GSI missing HASH key".into()));
    }

    let mut range_vals: Vec<&AttributeValue> = Vec::with_capacity(4);
    for ks in gsi_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
    {
        let Some(val) = item.get(&ks.attribute_name) else {
            return Ok(None);
        };
        range_vals.push(val);
    }

    let mut out = encode_gsi_pk_prefix(&hash_vals)?;

    // GSI sort keys (`0..=4` RANGE attributes in schema order) — each non-terminal,
    // escape-encoded with `0x00 0x00` terminator.
    for sk_val in range_vals {
        let (tag, bytes) = tag_bytes_for(sk_val)?;
        out.push(tag);
        let escaped = escape_bytes_terminated(&bytes);
        out.extend_from_slice(&escaped);
    }

    // Base partition key (HASH) — required (it's a key attr).
    let base_pk = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("base table missing HASH key".into()))?;
    let pk_val = item.get(&base_pk.attribute_name).ok_or_else(|| {
        StorageError::Validation(format!("item missing key attr {}", base_pk.attribute_name))
    })?;
    let (tag, bytes) = tag_bytes_for(pk_val)?;
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);

    // Base sort key (optional) — terminal, raw bytes to end of key.
    if let Some(base_sk) = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
    {
        let sk_val = item.get(&base_sk.attribute_name).ok_or_else(|| {
            StorageError::Validation(format!("item missing key attr {}", base_sk.attribute_name))
        })?;
        let (tag, bytes) = tag_bytes_for(sk_val)?;
        out.push(tag);
        out.extend_from_slice(&bytes);
    }

    if out.len() > row_key::MAX_ROW_KEY_SIZE {
        return Err(StorageError::Validation(format!(
            "encoded GSI shadow row key size ({} bytes) exceeds Bigtable maximum limit of {} bytes",
            out.len(),
            row_key::MAX_ROW_KEY_SIZE
        )));
    }

    Ok(Some(out))
}

#[cfg(any(test, feature = "test-hooks"))]
fn decode_attr_value(tag: u8, bytes: &[u8]) -> Result<AttributeValue, StorageError> {
    match tag {
        0x53 => Ok(AttributeValue::S(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| StorageError::Internal(format!("decode S: {e}")))?,
        )),
        0x42 => Ok(AttributeValue::B(bytes.to_vec())),
        0x4E => Ok(AttributeValue::N(crate::data::encoding::number::decode(
            bytes,
        )?)),
        other => Err(StorageError::Internal(format!(
            "unknown tag: 0x{other:02X}"
        ))),
    }
}

/// Decode a GSI shadow row key back into an Item containing both the GSI key
/// attributes and the base table key attributes.
#[cfg(any(test, feature = "test-hooks"))]
pub fn decode_shadow_row_key(
    key: &[u8],
    gsi_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
) -> Result<Item, StorageError> {
    if key.is_empty() {
        return Err(StorageError::Internal("empty shadow row key".into()));
    }
    let mut idx = 0;
    let mut item = Item::new();

    // 1. Decode GSI PK(s)
    let hash_elems: Vec<&KeySchemaElement> = gsi_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .collect();
    if hash_elems.is_empty() {
        return Err(StorageError::Validation("GSI missing HASH key".into()));
    }
    if key.len() < idx + 5 {
        return Err(StorageError::Internal(
            "malformed shadow row key (GSI PK header)".into(),
        ));
    }
    let gsi_pk_tag = key[idx];
    idx += 1;
    let gsi_pk_len = u32::from_be_bytes(
        key[idx..idx + 4]
            .try_into()
            .map_err(|_| StorageError::Internal("failed to convert slice to array".into()))?,
    ) as usize;
    idx += 4;
    if key.len() < idx + gsi_pk_len {
        return Err(StorageError::Internal(
            "malformed shadow row key (GSI PK bytes)".into(),
        ));
    }
    let gsi_pk_bytes = &key[idx..idx + gsi_pk_len];
    idx += gsi_pk_len;

    if hash_elems.len() == 1 {
        let gsi_pk_val = decode_attr_value(gsi_pk_tag, gsi_pk_bytes)?;
        item.insert(hash_elems[0].attribute_name.clone(), gsi_pk_val);
    } else {
        if gsi_pk_tag != TAG_MULTI_HASH {
            return Err(StorageError::Internal(format!(
                "expected multi-HASH tag 0x{TAG_MULTI_HASH:02X}, got 0x{gsi_pk_tag:02X}"
            )));
        }
        let mut inner_idx = 0usize;
        for he in &hash_elems {
            if gsi_pk_bytes.len() < inner_idx + 5 {
                return Err(StorageError::Internal(
                    "malformed multi-HASH GSI PK part header".into(),
                ));
            }
            let p_tag = gsi_pk_bytes[inner_idx];
            inner_idx += 1;
            let p_len = u32::from_be_bytes(
                gsi_pk_bytes[inner_idx..inner_idx + 4]
                    .try_into()
                    .map_err(|_| StorageError::Internal("multi-HASH part len".into()))?,
            ) as usize;
            inner_idx += 4;
            if gsi_pk_bytes.len() < inner_idx + p_len {
                return Err(StorageError::Internal(
                    "malformed multi-HASH GSI PK part bytes".into(),
                ));
            }
            let p_val = decode_attr_value(p_tag, &gsi_pk_bytes[inner_idx..inner_idx + p_len])?;
            inner_idx += p_len;
            item.insert(he.attribute_name.clone(), p_val);
        }
        if inner_idx != gsi_pk_bytes.len() {
            return Err(StorageError::Internal(
                "trailing bytes in multi-HASH GSI PK envelope".into(),
            ));
        }
    }

    // 2. Decode GSI SK(s) (in schema order)
    for gsi_sk in gsi_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
    {
        if key.len() < idx + 1 {
            return Err(StorageError::Internal(
                "missing GSI SK tag in shadow row key".into(),
            ));
        }
        let gsi_sk_tag = key[idx];
        idx += 1;
        let (unescaped_bytes, new_idx) = unescape_bytes_terminated(key, idx)?;
        idx = new_idx;
        let gsi_sk_val = decode_attr_value(gsi_sk_tag, &unescaped_bytes)?;
        item.insert(gsi_sk.attribute_name.clone(), gsi_sk_val);
    }

    // 3. Decode Base PK
    let base_pk = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("base table missing HASH key".into()))?;
    if key.len() < idx + 5 {
        return Err(StorageError::Internal(
            "malformed shadow row key (Base PK header)".into(),
        ));
    }
    let base_pk_tag = key[idx];
    idx += 1;
    let base_pk_len = u32::from_be_bytes(
        key[idx..idx + 4]
            .try_into()
            .map_err(|_| StorageError::Internal("failed to convert slice to array".into()))?,
    ) as usize;
    idx += 4;
    if key.len() < idx + base_pk_len {
        return Err(StorageError::Internal(
            "malformed shadow row key (Base PK bytes)".into(),
        ));
    }
    let base_pk_bytes = &key[idx..idx + base_pk_len];
    idx += base_pk_len;
    let base_pk_val = decode_attr_value(base_pk_tag, base_pk_bytes)?;
    item.insert(base_pk.attribute_name.clone(), base_pk_val);

    // 4. Decode Base SK (if present)
    if let Some(base_sk) = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
    {
        if key.len() < idx + 1 {
            return Err(StorageError::Internal(
                "missing Base SK tag in shadow row key".into(),
            ));
        }
        let base_sk_tag = key[idx];
        idx += 1;
        let base_sk_bytes = &key[idx..];
        let base_sk_val = decode_attr_value(base_sk_tag, base_sk_bytes)?;
        item.insert(base_sk.attribute_name.clone(), base_sk_val);
    }

    Ok(item)
}

/// Project a base item down to exactly the attributes a shadow row needs,
/// per the GSI's `Projection`:
/// - `ALL`        → pass-through (every attribute).
/// - `KEYS_ONLY`  → base-table key attrs + GSI key attrs.
/// - `INCLUDE`    → base+GSI keys plus the configured `NonKeyAttributes`.
pub fn project_for_shadow(
    item: &Item,
    projection: &Projection,
    base_key_schema: &[KeySchemaElement],
    gsi_key_schema: &[KeySchemaElement],
) -> Item {
    if matches!(projection.projection_type, ProjectionType::All) {
        return item.clone();
    }
    let mut out: Item = BTreeMap::new();
    let mut copy_named = |name: &str| {
        if let Some(v) = item.get(name) {
            out.insert(name.to_owned(), v.clone());
        }
    };
    for ks in base_key_schema {
        copy_named(&ks.attribute_name);
    }
    for ks in gsi_key_schema {
        copy_named(&ks.attribute_name);
    }
    if matches!(projection.projection_type, ProjectionType::Include)
        && let Some(extras) = &projection.non_key_attributes
    {
        for name in extras {
            copy_named(name);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::KeyType;

    fn ks(name: &str, kt: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.into(),
            key_type: kt,
        }
    }

    #[test]
    fn test_gsi_sk_sort_order_no_inversion() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        let mk_item = |gsi_sk_val: &str, pk_val: &str| {
            let mut item = BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S(pk_val.to_string()));
            item.insert(
                "gsi_pk".to_string(),
                AttributeValue::S("common_gsi_pk".to_string()),
            );
            item.insert(
                "gsi_sk".to_string(),
                AttributeValue::S(gsi_sk_val.to_string()),
            );
            item
        };

        let key_a = shadow_row_key_for_item(&mk_item("A", "base1"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let key_ab = shadow_row_key_for_item(&mk_item("AB", "base1"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let key_b = shadow_row_key_for_item(&mk_item("B", "base1"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();

        assert!(
            key_a < key_ab,
            "sort inversion: 'A' should sort before 'AB'"
        );
        assert!(key_ab < key_b, "'AB' should sort before 'B'");
    }

    #[test]
    fn test_gsi_sk_with_embedded_zeros() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        let mk_item = |gsi_sk_bytes: Vec<u8>| {
            let mut item = BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S("base1".to_string()));
            item.insert(
                "gsi_pk".to_string(),
                AttributeValue::S("common_gsi_pk".to_string()),
            );
            item.insert("gsi_sk".to_string(), AttributeValue::B(gsi_sk_bytes));
            item
        };

        let k1 = shadow_row_key_for_item(&mk_item(vec![0x41]), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k2 = shadow_row_key_for_item(&mk_item(vec![0x41, 0x00]), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k3 = shadow_row_key_for_item(&mk_item(vec![0x41, 0x00, 0x01]), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k4 = shadow_row_key_for_item(&mk_item(vec![0x41, 0x01]), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();

        assert!(k1 < k2);
        assert!(k2 < k3);
        assert!(k3 < k4);
    }

    #[test]
    fn test_gsi_number_sk_sorting() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        let mk_item = |n: &str| {
            let mut item = BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S("base1".to_string()));
            item.insert("gsi_pk".to_string(), AttributeValue::S("g1".to_string()));
            item.insert("gsi_sk".to_string(), AttributeValue::N(n.to_string()));
            item
        };

        let series = ["-100", "-1.5", "-1", "0", "0.5", "1", "1.5", "100", "1e10"];
        let keys: Vec<Vec<u8>> = series
            .iter()
            .map(|s| {
                shadow_row_key_for_item(&mk_item(s), &gsi_ks, &base_ks)
                    .unwrap()
                    .unwrap()
            })
            .collect();

        for w in keys.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn test_decode_shadow_row_key_round_trip() {
        let base_ks = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        let mut item = BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::S("my_base_pk".into()));
        item.insert("sk".to_string(), AttributeValue::N("99.9".into()));
        item.insert("gsi_pk".to_string(), AttributeValue::B(vec![1, 2, 3]));
        item.insert(
            "gsi_sk".to_string(),
            AttributeValue::S("gsi_sort_val".into()),
        );

        let key = shadow_row_key_for_item(&item, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let decoded = decode_shadow_row_key(&key, &gsi_ks, &base_ks).unwrap();

        assert_eq!(item.get("pk"), decoded.get("pk"));
        assert_eq!(item.get("gsi_pk"), decoded.get("gsi_pk"));
        assert_eq!(item.get("gsi_sk"), decoded.get("gsi_sk"));

        let re_encoded = shadow_row_key_for_item(&decoded, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        assert_eq!(key, re_encoded);
    }

    #[test]
    fn test_shadow_row_key_unambiguous_boundaries() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        let mk_item = |gsi_sk: &str, base_pk: &str| {
            let mut item = BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S(base_pk.into()));
            item.insert("gsi_pk".to_string(), AttributeValue::S("gsi_p".into()));
            item.insert("gsi_sk".to_string(), AttributeValue::S(gsi_sk.into()));
            item
        };

        let key1 = shadow_row_key_for_item(&mk_item("A", "B"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let key2 = shadow_row_key_for_item(&mk_item("AB", ""), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        assert_ne!(key1, key2);

        let dec1 = decode_shadow_row_key(&key1, &gsi_ks, &base_ks).unwrap();
        let dec2 = decode_shadow_row_key(&key2, &gsi_ks, &base_ks).unwrap();
        assert_eq!(dec1.get("gsi_sk"), Some(&AttributeValue::S("A".into())));
        assert_eq!(dec1.get("pk"), Some(&AttributeValue::S("B".into())));
        assert_eq!(dec2.get("gsi_sk"), Some(&AttributeValue::S("AB".into())));
        assert_eq!(dec2.get("pk"), Some(&AttributeValue::S("".into())));
    }

    #[test]
    fn test_shadow_row_key_4kb_limit() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash)];

        let mut item = BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::S("pk_val".to_string()));
        item.insert("gsi_pk".to_string(), AttributeValue::S("x".repeat(5000)));

        let res = shadow_row_key_for_item(&item, &gsi_ks, &base_ks);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match err {
            StorageError::Validation(msg) => {
                assert!(msg.contains("exceeds Bigtable maximum limit of 4096 bytes"));
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    #[test]
    fn test_multi_hash_and_multi_range_shadow_keys_order_and_round_trip() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![
            ks("h1", KeyType::Hash),
            ks("h2", KeyType::Hash),
            ks("r1", KeyType::Range),
            ks("r2", KeyType::Range),
            ks("r3", KeyType::Range),
        ];

        // Delimiter safety across composite HASH keys: ("ab", "c") != ("a", "bc").
        let mk_hash = |h1: &str, h2: &str| {
            let mut it = BTreeMap::new();
            it.insert("pk".into(), AttributeValue::S("p1".into()));
            it.insert("h1".into(), AttributeValue::S(h1.into()));
            it.insert("h2".into(), AttributeValue::S(h2.into()));
            it.insert("r1".into(), AttributeValue::S("T".into()));
            it.insert("r2".into(), AttributeValue::S("live".into()));
            it.insert("r3".into(), AttributeValue::N("10".into()));
            it
        };
        let k_ab_c = shadow_row_key_for_item(&mk_hash("ab", "c"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k_a_bc = shadow_row_key_for_item(&mk_hash("a", "bc"), &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        assert_ne!(k_ab_c, k_a_bc);

        // Sparse-index rule: missing any HASH or RANGE attribute omits the entry.
        let mut missing_h2 = mk_hash("a", "b");
        missing_h2.remove("h2");
        assert!(
            shadow_row_key_for_item(&missing_h2, &gsi_ks, &base_ks)
                .unwrap()
                .is_none()
        );
        let mut missing_r3 = mk_hash("a", "b");
        missing_r3.remove("r3");
        assert!(
            shadow_row_key_for_item(&missing_r3, &gsi_ks, &base_ks)
                .unwrap()
                .is_none()
        );

        // Round-trip decode of multi-HASH + multi-RANGE shadow key.
        let orig = mk_hash("tenant:1", "region\x00us");
        let enc = shadow_row_key_for_item(&orig, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let dec = decode_shadow_row_key(&enc, &gsi_ks, &base_ks).unwrap();
        for attr in ["pk", "h1", "h2", "r1", "r2"] {
            assert_eq!(orig.get(attr), dec.get(attr), "mismatch on {attr}");
        }
        match (orig.get("r3"), dec.get("r3")) {
            (Some(AttributeValue::N(a)), Some(AttributeValue::N(b))) => {
                use std::str::FromStr;
                assert_eq!(
                    bigdecimal::BigDecimal::from_str(a).unwrap(),
                    bigdecimal::BigDecimal::from_str(b).unwrap(),
                    "numeric mismatch on r3"
                );
            }
            other => panic!("expected numeric r3 round-trip, got {other:?}"),
        }

        // Lexicographic order across (r1, r2, r3, pk).
        let mk_row = |pk: &str, r1: &str, r2: &str, r3: &str| {
            let mut it = BTreeMap::new();
            it.insert("pk".into(), AttributeValue::S(pk.into()));
            it.insert("h1".into(), AttributeValue::S("t1".into()));
            it.insert("h2".into(), AttributeValue::S("us".into()));
            it.insert("r1".into(), AttributeValue::S(r1.into()));
            it.insert("r2".into(), AttributeValue::S(r2.into()));
            it.insert("r3".into(), AttributeValue::N(r3.into()));
            shadow_row_key_for_item(&it, &gsi_ks, &base_ks)
                .unwrap()
                .unwrap()
        };
        let ordered = [
            mk_row("p5", "F", "live", "100"),
            mk_row("p4", "T", "draft", "50"),
            mk_row("p3", "T", "live", "-5"),
            mk_row("p2", "T", "live", "2"),
            mk_row("p1", "T", "live", "10"),
        ];
        for w in ordered.windows(2) {
            assert!(w[0] < w[1]);
        }
    }
}
