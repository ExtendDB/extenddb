// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Encoding and decoding for `__extenddb_ttl_index__` entries:
//! - Sharded TTL expiration keys (`[shard_id:1 in 0..16][expiry_be8:8]...`)
//! - Delayed version-only tombstone reclamation queue keys
//!   (`b"tomb:"[ready_ms_be8:8][sha256:32]`, byte 0 = `0x74 >= 16`, strictly
//!   disjoint from all TTL shards and bounded to 45 bytes regardless of row-key size).

use extenddb_storage::error::StorageError;
use sha2::{Digest, Sha256};

pub const TTL_INDEX_TABLE: &str = "__extenddb_ttl_index__";
pub const NUM_SHARDS: u8 = 16;

/// Prefix for delayed version-only row tombstone reclamation entries in
/// `__extenddb_ttl_index__`. Byte `0` (`b't' = 0x74`) is strictly greater than
/// `NUM_SHARDS - 1` (`0x0F`), keeping the two keyspaces completely disjoint.
pub const TOMBSTONE_GC_PREFIX: &[u8; 5] = b"tomb:";

/// Qualifier in family `"d"` holding the serialized tombstone target payload.
pub const TOMBSTONE_GC_QUALIFIER: &[u8] = b"g";

/// Build the TTL index row key.
#[must_use]
pub fn encode_ttl_key(
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
) -> Vec<u8> {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in base_row_key {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    #[allow(clippy::cast_possible_truncation)]
    let shard_id = (h % u64::from(NUM_SHARDS)) as u8;

    let mut out = Vec::with_capacity(
        1 + 8 + 1 + account_id.len() + 1 + table_name.len() + base_row_key.len(),
    );
    out.push(shard_id);
    out.extend_from_slice(&expiry.to_be_bytes());
    #[allow(clippy::cast_possible_truncation)]
    out.push(account_id.len() as u8);
    out.extend_from_slice(account_id.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    out.push(table_name.len() as u8);
    out.extend_from_slice(table_name.as_bytes());
    out.extend_from_slice(base_row_key);
    out
}

/// Decode a TTL index row key back into `(shard_id, expiry, account_id, table_name, base_row_key)`.
pub fn decode_ttl_key(key: &[u8]) -> Result<(u8, i64, &str, &str, &[u8]), StorageError> {
    if key.len() < 1 + 8 + 1 + 1 {
        return Err(StorageError::Internal("malformed TTL key".into()));
    }
    let shard_id = key[0];
    let expiry_bytes: [u8; 8] = key[1..9]
        .try_into()
        .map_err(|_| StorageError::Internal("malformed TTL key (expiry)".into()))?;
    let expiry = i64::from_be_bytes(expiry_bytes);

    let acct_len = key[9] as usize;
    if key.len() < 9 + 1 + acct_len + 1 {
        return Err(StorageError::Internal("malformed TTL key (account)".into()));
    }
    let account_id = std::str::from_utf8(&key[10..10 + acct_len])
        .map_err(|e| StorageError::Internal(format!("decode account: {e}")))?;

    let table_len_idx = 10 + acct_len;
    let table_len = key[table_len_idx] as usize;
    if key.len() < table_len_idx + 1 + table_len {
        return Err(StorageError::Internal("malformed TTL key (table)".into()));
    }
    let table_name = std::str::from_utf8(&key[table_len_idx + 1..table_len_idx + 1 + table_len])
        .map_err(|e| StorageError::Internal(format!("decode table: {e}")))?;

    let base_key_idx = table_len_idx + 1 + table_len;
    let base_row_key = &key[base_key_idx..];

    Ok((shard_id, expiry, account_id, table_name, base_row_key))
}

/// Encode a fixed 45-byte tombstone GC queue key ordered by `ready_ms`:
/// `b"tomb:" (5B) || ready_ms_be8 (8B) || sha256(table_short || 0x00 || raw_row_key) (32B)`.
#[must_use]
pub fn encode_tombstone_gc_key(ready_ms: u64, table_short: &str, raw_row_key: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(table_short.as_bytes());
    hasher.update([0x00]);
    hasher.update(raw_row_key);
    let digest = hasher.finalize();

    let mut out = Vec::with_capacity(5 + 8 + 32);
    out.extend_from_slice(TOMBSTONE_GC_PREFIX);
    out.extend_from_slice(&ready_ms.to_be_bytes());
    out.extend_from_slice(&digest);
    out
}

/// Exclusive upper bound key `b"tomb:" || (now_ms + 1)_be8` for scanning due
/// tombstone reclamation entries.
#[must_use]
pub fn tombstone_gc_scan_end_exclusive(now_ms: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + 8);
    out.extend_from_slice(TOMBSTONE_GC_PREFIX);
    out.extend_from_slice(&now_ms.saturating_add(1).to_be_bytes());
    out
}

/// Encode `(expected_version, table_short, raw_row_key)` into the `d:g` cell value.
#[must_use]
pub fn encode_tombstone_gc_value(
    expected_version: u64,
    table_short: &str,
    raw_row_key: &[u8],
) -> Vec<u8> {
    let tbytes = table_short.as_bytes();
    let tlen = u16::try_from(tbytes.len()).unwrap_or(u16::MAX);
    let mut out = Vec::with_capacity(8 + 2 + tbytes.len() + raw_row_key.len());
    out.extend_from_slice(&expected_version.to_be_bytes());
    out.extend_from_slice(&tlen.to_be_bytes());
    out.extend_from_slice(&tbytes[..usize::from(tlen)]);
    out.extend_from_slice(raw_row_key);
    out
}

/// Decode `(expected_version, table_short, raw_row_key)` from a `d:g` cell value.
#[must_use]
pub fn decode_tombstone_gc_value(val: &[u8]) -> Option<(u64, &str, &[u8])> {
    if val.len() < 10 {
        return None;
    }
    let expected_version = u64::from_be_bytes(val[0..8].try_into().ok()?);
    let tlen = usize::from(u16::from_be_bytes(val[8..10].try_into().ok()?));
    if val.len() < 10 + tlen {
        return None;
    }
    let table_short = std::str::from_utf8(&val[10..10 + tlen]).ok()?;
    let raw_row_key = &val[10 + tlen..];
    Some((expected_version, table_short, raw_row_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_key_roundtrip() {
        let account_id = "test-account";
        let table_name = "test-table";
        let base_row_key = b"my-base-row-key";
        let expiry = 1_719_692_400i64;

        let key = encode_ttl_key(account_id, table_name, base_row_key, expiry);
        let (shard_id, decoded_expiry, decoded_acct, decoded_table, decoded_base_key) =
            decode_ttl_key(&key).unwrap();

        assert!(shard_id < NUM_SHARDS);
        assert_eq!(decoded_expiry, expiry);
        assert_eq!(decoded_acct, account_id);
        assert_eq!(decoded_table, table_name);
        assert_eq!(decoded_base_key, base_row_key);
    }

    #[test]
    fn test_ttl_key_lexicographical_sorting() {
        let account_id = "test-account";
        let table_name = "test-table";
        let base_row_key = b"row-1";

        let key_early = encode_ttl_key(account_id, table_name, base_row_key, 1000);
        let key_late = encode_ttl_key(account_id, table_name, base_row_key, 2000);

        assert!(key_early < key_late);
    }

    #[test]
    fn test_tombstone_gc_keys_are_disjoint_bounded_and_roundtrip_4kb_keys() {
        let max_row_key = vec![0xFFu8; 4_096];
        let k1 = encode_tombstone_gc_key(1_000, "t0123456789abcdef_gdeadbeef", &max_row_key);
        let k2 = encode_tombstone_gc_key(2_000, "t0123456789abcdef_gdeadbeef", &max_row_key);
        assert_eq!(k1.len(), 45);
        assert_eq!(k2.len(), 45);
        assert!(k1[0] >= NUM_SHARDS);
        assert!(k1 < k2);
        assert!(k1 < tombstone_gc_scan_end_exclusive(1_000));
        assert!(k2 >= tombstone_gc_scan_end_exclusive(1_000));

        let val = encode_tombstone_gc_value(987_654_321, "t0123456789abcdef", &max_row_key);
        let (ver, tbl, rk) = decode_tombstone_gc_value(&val).unwrap();
        assert_eq!(ver, 987_654_321);
        assert_eq!(tbl, "t0123456789abcdef");
        assert_eq!(rk, max_row_key.as_slice());
    }
}
