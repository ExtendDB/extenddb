// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Secondary Index (GSI & LSI) shadow-table encoding and projection helpers.
//!
//! Each GSI gets its own Bigtable table named `<base_data_table>_g<8hex>` and
//! each LSI gets its own Bigtable table named `<base_data_table>_l<8hex>`
//! (8 hex chars from FNV-1a over the index name, fitting Bigtable's 50-char
//! table-ID limit).
//!
//! Shadow Row Key layout (bounded to <= 3,276 bytes for all single-RANGE
//! `DynamoDB` key combinations, well within Bigtable's 4,096-byte row-key limit):
//! - Index partition digest header (`37` bytes):
//!   `[TAG_PK_DIGEST (0x44):1][32u32.to_be_bytes():4][sha256(index_hash_keys):32]`
//! - Zero or more index sort-key segments (`0..=4` RANGE keys in schema order):
//!   `([index_sk_tag_j:1][escape_encoded(index_sk_bytes_j)][0x00 0x00])...`
//! - Order-preserving, size-bounded base partition-key tie-breaker (`<= 163` bytes):
//!   `[base_pk_tag:1][escape_encoded(base_pk_bytes[..min(64)])][0x00, 0x00|0x01][sha256(base_pk):32]`
//! - Optional base sort-key suffix (terminal, raw bytes to end of row key):
//!   `([base_sk_tag:1][base_sk_bytes])?`
//!
//! Full attribute values (including unhashed index/base partition keys) are
//! stored in family `d` cells, while the fixed-size SHA-256 digests keep even
//! maximum-size keys (`2,048 B` `pk` + `1,024 B` `sk` + `2,048 B` `gsi_pk` +
//! `1,024 B` `gsi_sk`) safely below Bigtable's 4 KiB row-key ceiling.

use std::collections::BTreeMap;

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, Projection, ProjectionType,
};
use extenddb_storage::error::StorageError;
use sha2::{Digest, Sha256};

use crate::data::encoding::row_key;

/// Outer partition-prefix tag for index shadow rows (`'D' = 0x44`), distinct
/// from base-table scalar tags `S` (`0x53`), `N` (`0x4E`), and `B` (`0x42`).
pub const TAG_PK_DIGEST: u8 = 0x44;

/// Byte length of a SHA-256 partition-key digest.
pub const PK_DIGEST_LEN: usize = 32;

/// Order-preserving escape encoding for non-terminal byte segments
/// (`FoundationDB` tuple encoding pattern). Replaces each `0x00` byte with `0x00 0xFF`.
#[must_use]
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
#[must_use]
pub fn escape_bytes_terminated(bytes: &[u8]) -> Vec<u8> {
    let mut out = escape_bytes(bytes);
    out.push(0x00);
    out.push(0x00);
    out
}

/// Decode an escape-encoded segment from a byte slice starting at `idx`.
/// Returns the unescaped bytes and the new index immediately after `0x00 0x00`.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] if the escaped segment is truncated,
/// missing its `0x00 0x00` terminator, or contains an invalid escape byte.
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
                0x00 => return Ok((out, idx + 2)),
                0xFF => {
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

fn fnv1a32(name: &str) -> u32 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h & 0xFFFF_FFFF) as u32
}

/// Derive the Bigtable shadow-table ID for a GSI `(base_data_table, index_name)`.
#[must_use]
pub fn shadow_table_id(base_data_table: &str, index_name: &str) -> String {
    format!("{base_data_table}_g{:08x}", fnv1a32(index_name))
}

/// Derive the Bigtable shadow-table ID for an LSI `(base_data_table, index_name)`.
#[must_use]
pub fn lsi_shadow_table_id(base_data_table: &str, index_name: &str) -> String {
    format!("{base_data_table}_l{:08x}", fnv1a32(index_name))
}

/// Returns `true` when `table_id_short` names a GSI (`<base>_g<8hex>`) or LSI
/// (`<base>_l<8hex>`) shadow table rather than a base data table (`t<16hex>`).
#[must_use]
pub fn is_shadow_table_id(table_id_short: &str) -> bool {
    table_id_short.contains("_g") || table_id_short.contains("_l")
}

fn tag_bytes_for(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    row_key::sk_tag_and_bytes(av)
}

/// Compute a canonical, domain-separated 32-byte SHA-256 digest over `1..=4`
/// partition-key scalar attribute values in key-schema order.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if `hash_vals` is empty or contains a non-scalar attribute.
pub fn hash_partition_values(
    hash_vals: &[&AttributeValue],
) -> Result<[u8; PK_DIGEST_LEN], StorageError> {
    if hash_vals.is_empty() {
        return Err(StorageError::Validation("index missing HASH key".into()));
    }
    let mut hasher = Sha256::new();
    let count = u32::try_from(hash_vals.len()).unwrap_or(u32::MAX);
    hasher.update(count.to_be_bytes());
    for v in hash_vals {
        let (tag, bytes) = tag_bytes_for(v)?;
        let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        hasher.update([tag]);
        hasher.update(len.to_be_bytes());
        hasher.update(&bytes);
    }
    Ok(hasher.finalize().into())
}

/// Encode `1..=4` index HASH attribute values (in key-schema order) into a
/// self-delimiting 37-byte `[TAG_PK_DIGEST:1][32u32:4][sha256:32]` prefix.
///
/// Because bytes `1..5` carry the exact payload length (`32`), `pk_prefix_slice`
/// and parallel-scan segment hashing treat base-table and shadow-table row keys
/// uniformly.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if `hash_vals` is empty or contains a non-scalar attribute.
pub fn encode_gsi_pk_prefix(hash_vals: &[&AttributeValue]) -> Result<Vec<u8>, StorageError> {
    let digest = hash_partition_values(hash_vals)?;
    let mut out = Vec::with_capacity(5 + PK_DIGEST_LEN);
    out.push(TAG_PK_DIGEST);
    out.extend_from_slice(&u32::try_from(PK_DIGEST_LEN).unwrap_or(32).to_be_bytes());
    out.extend_from_slice(&digest);
    Ok(out)
}

/// Build the shadow row key for an item on a GSI or LSI shadow table.
///
/// Returns `Ok(None)` when the item lacks any of the index key attributes
/// (sparse-index semantics). Validates that the encoded shadow row key never
/// exceeds Bigtable's 4,096-byte row-key limit so callers can validate all
/// shadow keys before committing the base row.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if index or base key schemas are invalid,
/// key attributes have non-scalar types, or the encoded shadow row key exceeds 4 KiB.
pub fn shadow_row_key_for_item(
    item: &Item,
    index_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
) -> Result<Option<Vec<u8>>, StorageError> {
    const BASE_PK_SORT_PREFIX_CAP: usize = 64;

    let mut hash_vals: Vec<&AttributeValue> = Vec::with_capacity(4);
    for ks in index_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
    {
        let Some(val) = item.get(&ks.attribute_name) else {
            return Ok(None);
        };
        hash_vals.push(val);
    }
    if hash_vals.is_empty() {
        return Err(StorageError::Validation("index missing HASH key".into()));
    }

    let mut range_vals: Vec<&AttributeValue> = Vec::with_capacity(4);
    for ks in index_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
    {
        let Some(val) = item.get(&ks.attribute_name) else {
            return Ok(None);
        };
        range_vals.push(val);
    }

    let mut out = encode_gsi_pk_prefix(&hash_vals)?;

    // Index sort keys (`0..=4` RANGE attributes in schema order), escape-encoded
    // with `0x00 0x00` terminators so byte order matches DynamoDB sort order.
    for sk_val in range_vals {
        let (tag, bytes) = tag_bytes_for(sk_val)?;
        out.push(tag);
        out.extend_from_slice(&escape_bytes_terminated(&bytes));
    }

    // Base partition key (HASH) tie-breaker: leading 64 escaped bytes (preserving
    // exact DynamoDB lexicographic base-PK order for keys <= 64 bytes) followed by
    // `[0x00, 0x00|0x01]` and the 32-byte SHA-256 digest (`<= 163` bytes total).
    let base_pk = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("base table missing HASH key".into()))?;
    let pk_val = item.get(&base_pk.attribute_name).ok_or_else(|| {
        StorageError::Validation(format!("item missing key attr {}", base_pk.attribute_name))
    })?;
    let (base_pk_tag, base_pk_bytes) = tag_bytes_for(pk_val)?;
    let prefix_len = base_pk_bytes.len().min(BASE_PK_SORT_PREFIX_CAP);
    out.push(base_pk_tag);
    out.extend_from_slice(&escape_bytes(&base_pk_bytes[..prefix_len]));
    out.push(0x00);
    out.push(u8::from(base_pk_bytes.len() > BASE_PK_SORT_PREFIX_CAP));
    let base_pk_digest = hash_partition_values(&[pk_val])?;
    out.extend_from_slice(&base_pk_digest);

    // Base sort key (optional): terminal, raw bytes to end of row key.
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
            "encoded index shadow row key size ({} bytes) exceeds Bigtable maximum limit of {} bytes",
            out.len(),
            row_key::MAX_ROW_KEY_SIZE
        )));
    }

    Ok(Some(out))
}

/// Project a base item down to the attributes stored in a GSI or LSI shadow row:
/// - `ALL`        -> every attribute of `item`
/// - `KEYS_ONLY`  -> base-table key attributes + index key attributes
/// - `INCLUDE`    -> base + index key attributes + configured `NonKeyAttributes`
#[must_use]
pub fn project_for_shadow(
    item: &Item,
    projection: &Projection,
    base_key_schema: &[KeySchemaElement],
    index_key_schema: &[KeySchemaElement],
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
    for ks in index_key_schema {
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

        assert!(key_a < key_ab, "'A' must sort before 'AB'");
        assert!(key_ab < key_b, "'AB' must sort before 'B'");
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
    fn test_max_dynamodb_key_sizes_fit_under_4kb_bigtable_limit() {
        let base_ks = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let gsi_ks = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];

        // Worst-case valid DynamoDB item: 2,048-byte PK, 1,024-byte SK,
        // 2,048-byte GSI PK, and 1,024-byte GSI SK filled entirely with 0x00
        // (which doubles to 2,048 bytes under escape encoding).
        let mut item = BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::B(vec![0xAB; 2048]));
        item.insert("sk".to_string(), AttributeValue::B(vec![0xCD; 1024]));
        item.insert("gsi_pk".to_string(), AttributeValue::B(vec![0xEF; 2048]));
        item.insert("gsi_sk".to_string(), AttributeValue::B(vec![0x00; 1024]));

        let encoded = shadow_row_key_for_item(&item, &gsi_ks, &base_ks)
            .expect("valid max-size keys must succeed")
            .expect("item contains all index keys");
        assert_eq!(
            encoded.len(),
            37 + (1 + 2048 + 2) + (1 + 64 + 2 + 32) + (1 + 1024)
        );
        assert!(encoded.len() <= row_key::MAX_ROW_KEY_SIZE);

        // Even if the 2,048-byte base PK is filled entirely with 0x00 (which
        // doubles its 64-byte sort prefix to 128 bytes), total size is 3,276 <= 4,096.
        item.insert("pk".to_string(), AttributeValue::B(vec![0x00; 2048]));
        let worst_encoded = shadow_row_key_for_item(&item, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        assert_eq!(worst_encoded.len(), 3276);
        assert!(worst_encoded.len() <= row_key::MAX_ROW_KEY_SIZE);

        // Equal (gsi_pk, gsi_sk) items sort in lexicographical base-PK order:
        let mut p1 = item.clone();
        p1.insert("pk".to_string(), AttributeValue::S("alpha".to_string()));
        let mut p2 = item.clone();
        p2.insert("pk".to_string(), AttributeValue::S("alpha_2".to_string()));
        let mut p3 = item.clone();
        p3.insert("pk".to_string(), AttributeValue::S("beta".to_string()));
        let k1 = shadow_row_key_for_item(&p1, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k2 = shadow_row_key_for_item(&p2, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        let k3 = shadow_row_key_for_item(&p3, &gsi_ks, &base_ks)
            .unwrap()
            .unwrap();
        assert!(k1 < k2 && k2 < k3);
    }

    #[test]
    fn test_multi_range_shadow_row_key_exceeding_4kb_is_rejected_cleanly() {
        let base_ks = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let gsi_ks = vec![
            ks("h1", KeyType::Hash),
            ks("r1", KeyType::Range),
            ks("r2", KeyType::Range),
            ks("r3", KeyType::Range),
        ];

        let mut item = BTreeMap::new();
        item.insert("pk".into(), AttributeValue::S("p".into()));
        item.insert("sk".into(), AttributeValue::B(vec![0x11; 1024]));
        item.insert("h1".into(), AttributeValue::S("h".into()));
        item.insert("r1".into(), AttributeValue::B(vec![0x00; 1024]));
        item.insert("r2".into(), AttributeValue::B(vec![0x00; 1024]));
        item.insert("r3".into(), AttributeValue::B(vec![0x00; 1024]));

        let err = shadow_row_key_for_item(&item, &gsi_ks, &base_ks).unwrap_err();
        assert!(matches!(err, StorageError::Validation(msg) if msg.contains("4096 bytes")));
    }

    #[test]
    fn test_multi_hash_and_multi_range_shadow_keys_order_and_delimit() {
        let base_ks = vec![ks("pk", KeyType::Hash)];
        let gsi_ks = vec![
            ks("h1", KeyType::Hash),
            ks("h2", KeyType::Hash),
            ks("r1", KeyType::Range),
            ks("r2", KeyType::Range),
            ks("r3", KeyType::Range),
        ];

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

    #[test]
    fn test_shadow_table_ids_distinguish_gsi_and_lsi() {
        let gsi = shadow_table_id("t0123456789abcdef", "idx");
        let lsi = lsi_shadow_table_id("t0123456789abcdef", "idx");
        assert_ne!(gsi, lsi);
        assert!(is_shadow_table_id(&gsi));
        assert!(is_shadow_table_id(&lsi));
        assert!(!is_shadow_table_id("t0123456789abcdef"));
    }
}
