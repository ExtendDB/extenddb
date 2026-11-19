// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Global Secondary Index support via shadow tables.
//!
//! Each GSI gets its own BigTable table named `<base_data_table>_g<8hex>`
//! (8 hex chars from a stable hash of the index name to fit BigTable's
//! 50-char table-id limit).
//!
//! GSI Index Row Key layout:
//! `[gsi_pk_tag:1][gsi_pk_len:4][gsi_pk_bytes] [gsi_sk_tag:1][escape_encoded(gsi_sk_bytes)][0x00 0x00] [base_pk_tag:1][base_pk_len:4][base_pk_bytes] [base_sk_tag:1][base_sk_bytes]`
//!
//! The non-terminal `gsi_sk` segment is encoded using FoundationDB tuple
//! order-preserving escape encoding (0x00 -> 0x00 0xFF, terminated with 0x00 0x00).
//! The terminal `base_sk` segment remains raw bytes to the end of the row key.

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

fn tag_bytes_for(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    row_key::sk_tag_and_bytes(av)
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
    let mut out = Vec::with_capacity(64);

    // GSI partition key (HASH) — required for the index entry to exist.
    let gsi_pk = gsi_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("GSI missing HASH key".into()))?;
    let Some(pk_val) = item.get(&gsi_pk.attribute_name) else {
        return Ok(None);
    };
    let (tag, bytes) = tag_bytes_for(pk_val)?;
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);

    // GSI sort key (optional) — non-terminal, escape-encoded with 0x00 0x00 terminator.
    if let Some(gsi_sk) = gsi_key_schema.iter().find(|k| k.key_type == KeyType::Range) {
        let Some(sk_val) = item.get(&gsi_sk.attribute_name) else {
            return Ok(None);
        };
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

    // 1. Decode GSI PK
    let gsi_pk = gsi_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("GSI missing HASH key".into()))?;
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
    let gsi_pk_val = decode_attr_value(gsi_pk_tag, gsi_pk_bytes)?;
    item.insert(gsi_pk.attribute_name.clone(), gsi_pk_val);

    // 2. Decode GSI SK (if present)
    if let Some(gsi_sk) = gsi_key_schema.iter().find(|k| k.key_type == KeyType::Range) {
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

/// Build the prefix used to scan all shadow entries matching `gsi_pk` (and
/// optionally a `gsi_sk` operator). Used by Query against an index.
pub fn shadow_prefix_for_pk(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let (tag, bytes) = tag_bytes_for(pk)?;
    let mut out = Vec::with_capacity(1 + 4 + bytes.len());
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(out)
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

/// Decode shadow-row cells back into an Item map. The base-key columns are
/// already inside `item` (we projected ALL on write) so the caller gets the
/// item directly.
pub fn decode_shadow_cells(
    cells: Vec<bigtable_rs::bigtable::RowCell>,
) -> Result<Option<Item>, StorageError> {
    let mut item: Item = BTreeMap::new();
    for c in cells {
        if c.family_name == crate::data::item_ops::FAMILY_DATA {
            let attr = String::from_utf8(c.qualifier)
                .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
            item.insert(attr, crate::data::encoding::cell::decode(&c.value)?);
        }
    }
    if item.is_empty() {
        Ok(None)
    } else {
        Ok(Some(item))
    }
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
        // Test "A" vs "AB" sort ordering (maintainer B3 issue)
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

        // "A" MUST sort before "AB", and "AB" before "B"
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
        // Distinct (gsi_sk, base_pk) pairs must NEVER produce the same shadow row key
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
}
