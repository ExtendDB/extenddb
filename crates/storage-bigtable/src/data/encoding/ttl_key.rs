// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Encoding and decoding for `__extenddb_ttl_index__` entries:
//! - Sharded TTL expiration keys (`[shard_id:1 in 0..16][expiry_be8:8]...`)
//! - Delayed version-only tombstone reclamation queue keys
//!   (`b"tomb:"[ready_ms_be8:8][sha256:32]`, byte 0 = `0x74 >= 16`, strictly
//!   disjoint from all TTL shards and bounded to 45 bytes regardless of row-key size).

use extenddb_storage::error::StorageError;
use sha2::{Digest, Sha256};

use crate::data::encoding::row_key::MAX_ROW_KEY_SIZE;

pub const TTL_INDEX_TABLE: &str = "__extenddb_ttl_index__";
pub const NUM_SHARDS: u8 = 16;

/// Magic header for `d:""` TTL index cell values carrying `(table_id, base_row_key)`.
const TTL_VALUE_MAGIC: &[u8; 4] = b"tv1:";

/// Tag byte (`'D' = 0x44`, disjoint from valid base-table row-key tags
/// `0x53`/`0x4E`/`0x42`/`0x4D`) marking a 33-byte `[0x44][SHA-256(base_row_key):32]`
/// suffix in [`encode_ttl_key`] when raw concatenation would exceed 4,096 bytes.
const TAG_TTL_DIGEST: u8 = 0x44;

/// Prefix for delayed version-only row tombstone reclamation entries in
/// `__extenddb_ttl_index__`. Byte `0` (`b't' = 0x74`) is strictly greater than
/// `NUM_SHARDS - 1` (`0x0F`), keeping the two keyspaces completely disjoint.
pub const TOMBSTONE_GC_PREFIX: &[u8; 5] = b"tomb:";

/// Qualifier in family `"d"` holding the serialized tombstone target payload.
pub const TOMBSTONE_GC_QUALIFIER: &[u8] = b"g";

/// Parsed TTL index entry combining the row key and optional `d:""` cell payload.
pub type DecodedTtlEntry<'a> = (u8, i64, &'a str, &'a str, Option<&'a str>, &'a [u8]);

/// Build the TTL index row key, bounding total row-key length to Bigtable's
/// `4,096`-byte ceiling by substituting `SHA-256(base_row_key)` whenever raw
/// concatenation would exceed `MAX_ROW_KEY_SIZE` (full `base_row_key` and
/// `table_id` are stored in the `d:""` cell payload via [`encode_ttl_value`]).
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
    let shard_id = u8::try_from(h % u64::from(NUM_SHARDS)).unwrap_or(0);

    let acct_bytes = &account_id.as_bytes()[..account_id.len().min(usize::from(u8::MAX))];
    let table_bytes = &table_name.as_bytes()[..table_name.len().min(usize::from(u8::MAX))];
    let header_len = 1 + 8 + 1 + acct_bytes.len() + 1 + table_bytes.len();
    let needs_digest = header_len.saturating_add(base_row_key.len()) > MAX_ROW_KEY_SIZE;
    let suffix_len = if needs_digest { 33 } else { base_row_key.len() };

    let mut out = Vec::with_capacity(header_len + suffix_len);
    out.push(shard_id);
    out.extend_from_slice(&expiry.to_be_bytes());
    out.push(u8::try_from(acct_bytes.len()).unwrap_or(u8::MAX));
    out.extend_from_slice(acct_bytes);
    out.push(u8::try_from(table_bytes.len()).unwrap_or(u8::MAX));
    out.extend_from_slice(table_bytes);
    if needs_digest {
        let digest = Sha256::digest(base_row_key);
        out.push(TAG_TTL_DIGEST);
        out.extend_from_slice(&digest);
    } else {
        out.extend_from_slice(base_row_key);
    }
    out
}

/// Encode `(table_id, base_row_key)` into the `d:""` TTL index cell value so
/// `ttl_worker` recovers `base_row_key` even when [`encode_ttl_key`] digested a
/// wide key and immediately retires orphaned entries if a deleted table name is
/// reused under a new `table_id`.
#[must_use]
pub fn encode_ttl_value(table_id: Option<&str>, base_row_key: &[u8]) -> Vec<u8> {
    let tid_bytes = table_id.unwrap_or("").as_bytes();
    let tid_len = u16::try_from(tid_bytes.len()).unwrap_or(u16::MAX);
    let mut out =
        Vec::with_capacity(TTL_VALUE_MAGIC.len() + 2 + usize::from(tid_len) + base_row_key.len());
    out.extend_from_slice(TTL_VALUE_MAGIC);
    out.extend_from_slice(&tid_len.to_be_bytes());
    out.extend_from_slice(&tid_bytes[..usize::from(tid_len)]);
    out.extend_from_slice(base_row_key);
    out
}

/// Decode a TTL index row key back into `(shard_id, expiry, account_id, table_name, key_suffix)`.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] if `key` is truncated or contains invalid UTF-8
/// in `account_id` or `table_name`.
pub fn decode_ttl_key(key: &[u8]) -> Result<(u8, i64, &str, &str, &[u8]), StorageError> {
    if key.len() < 1 + 8 + 1 + 1 {
        return Err(StorageError::Internal("malformed TTL key".into()));
    }
    let shard_id = key[0];
    let expiry_bytes: [u8; 8] = key[1..9]
        .try_into()
        .map_err(|_| StorageError::Internal("malformed TTL key (expiry)".into()))?;
    let expiry = i64::from_be_bytes(expiry_bytes);

    let acct_len = usize::from(key[9]);
    if key.len() < 9 + 1 + acct_len + 1 {
        return Err(StorageError::Internal("malformed TTL key (account)".into()));
    }
    let account_id = std::str::from_utf8(&key[10..10 + acct_len])
        .map_err(|e| StorageError::Internal(format!("decode account: {e}")))?;

    let table_len_idx = 10 + acct_len;
    let table_len = usize::from(key[table_len_idx]);
    if key.len() < table_len_idx + 1 + table_len {
        return Err(StorageError::Internal("malformed TTL key (table)".into()));
    }
    let table_name = std::str::from_utf8(&key[table_len_idx + 1..table_len_idx + 1 + table_len])
        .map_err(|e| StorageError::Internal(format!("decode table: {e}")))?;

    let base_key_idx = table_len_idx + 1 + table_len;
    let base_row_key = &key[base_key_idx..];

    Ok((shard_id, expiry, account_id, table_name, base_row_key))
}

/// Decode a TTL index `(raw_key, cell_value)` pair into
/// `(shard_id, expiry, account_id, table_name, table_id_opt, base_row_key)`,
/// falling back to `raw_key`'s trailing bytes only when `raw_key` carries an
/// inline (`<= 4,096`-byte) `base_row_key` and `cell_value` is empty.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] if `raw_key` or a `tv1:` `cell_value` is
/// malformed, or if `raw_key` carries a digested suffix without a `tv1:` payload.
pub fn decode_ttl_entry<'a>(
    raw_key: &'a [u8],
    cell_value: &'a [u8],
) -> Result<DecodedTtlEntry<'a>, StorageError> {
    let (shard_id, expiry, account_id, table_name, key_suffix) = decode_ttl_key(raw_key)?;
    if let Some(rest) = cell_value.strip_prefix(TTL_VALUE_MAGIC) {
        if rest.len() < 2 {
            return Err(StorageError::Internal("malformed TTL value header".into()));
        }
        let tid_len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        if rest.len() < 2 + tid_len {
            return Err(StorageError::Internal(
                "malformed TTL value table_id".into(),
            ));
        }
        let tid_str = std::str::from_utf8(&rest[2..2 + tid_len])
            .map_err(|e| StorageError::Internal(format!("decode TTL table_id: {e}")))?;
        let tid_opt = (!tid_str.is_empty()).then_some(tid_str);
        let base_rk = &rest[2 + tid_len..];
        Ok((shard_id, expiry, account_id, table_name, tid_opt, base_rk))
    } else if key_suffix.len() == 33 && key_suffix.first() == Some(&TAG_TTL_DIGEST) {
        Err(StorageError::Internal(
            "digested TTL key missing tv1: cell payload".into(),
        ))
    } else {
        Ok((shard_id, expiry, account_id, table_name, None, key_suffix))
    }
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

    #[test]
    fn test_ttl_key_bounds_4kb_base_keys_and_decodes_table_id_payload() {
        let max_base_key = vec![0xABu8; 4_096];
        let ttl_rk = encode_ttl_key(
            "000000000000",
            "wide-ttl-table",
            &max_base_key,
            1_750_000_000,
        );
        assert!(
            ttl_rk.len() <= MAX_ROW_KEY_SIZE,
            "TTL row key exceeded 4096 bytes: {}",
            ttl_rk.len()
        );
        assert!(ttl_rk[0] < NUM_SHARDS);

        let payload = encode_ttl_value(Some("tbl-uuid-1234"), &max_base_key);
        let (shard, exp, acct, tbl, tid, decoded_rk) =
            decode_ttl_entry(&ttl_rk, &payload).expect("decode_ttl_entry");
        assert!(shard < NUM_SHARDS);
        assert_eq!(exp, 1_750_000_000);
        assert_eq!(acct, "000000000000");
        assert_eq!(tbl, "wide-ttl-table");
        assert_eq!(tid, Some("tbl-uuid-1234"));
        assert_eq!(decoded_rk, max_base_key.as_slice());

        // Empty legacy cell payload falls back to the inline row-key suffix,
        // whereas a digested (`0x44`) row key without a `tv1:` payload is rejected.
        assert!(
            decode_ttl_entry(&ttl_rk, b"").is_err(),
            "digested TTL row key with empty payload must fail rather than returning SHA-256 bytes"
        );
        let small_rk = encode_ttl_key("000000000000", "tbl", b"short-key", 1_750_000_001);
        let (_, _, _, _, tid_legacy, rk_legacy) =
            decode_ttl_entry(&small_rk, b"").expect("legacy empty payload");
        assert_eq!(tid_legacy, None);
        assert_eq!(rk_legacy, b"short-key");
    }
}
