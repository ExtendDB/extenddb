//! Encoding and decoding for the scalable TTL index row keys.

use extenddb_storage::error::StorageError;

pub const TTL_INDEX_TABLE: &str = "__extenddb_ttl_index__";
pub const NUM_SHARDS: u8 = 16;

/// Build the TTL index row key.
pub fn encode_ttl_key(
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
) -> Vec<u8> {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in base_row_key {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let shard_id = (h % NUM_SHARDS as u64) as u8;

    let mut out = Vec::with_capacity(1 + 8 + 1 + account_id.len() + 1 + table_name.len() + base_row_key.len());
    out.push(shard_id);
    out.extend_from_slice(&expiry.to_be_bytes());
    out.push(account_id.len() as u8);
    out.extend_from_slice(account_id.as_bytes());
    out.push(table_name.len() as u8);
    out.extend_from_slice(table_name.as_bytes());
    out.extend_from_slice(base_row_key);
    out
}

/// Decode a TTL index row key back into its components.
/// Returns (shard_id, expiry, account_id, table_name, base_row_key)
pub fn decode_ttl_key<'a>(
    key: &'a [u8],
) -> Result<(u8, i64, &'a str, &'a str, &'a [u8]), StorageError> {
    if key.len() < 1 + 8 + 1 + 1 {
        return Err(StorageError::Internal("malformed TTL key".into()));
    }
    let shard_id = key[0];
    let expiry_bytes: [u8; 8] = key[1..9].try_into().map_err(|_| StorageError::Internal("malformed TTL key (expiry)".into()))?;
    let expiry = i64::from_be_bytes(expiry_bytes);
    
    let acct_len = key[9] as usize;
    if key.len() < 9 + 1 + acct_len + 1 {
        return Err(StorageError::Internal("malformed TTL key (account)".into()));
    }
    let account_id = std::str::from_utf8(&key[10..10+acct_len])
        .map_err(|e| StorageError::Internal(format!("decode account: {e}")))?;
    
    let table_len_idx = 10 + acct_len;
    let table_len = key[table_len_idx] as usize;
    if key.len() < table_len_idx + 1 + table_len {
        return Err(StorageError::Internal("malformed TTL key (table)".into()));
    }
    let table_name = std::str::from_utf8(&key[table_len_idx+1..table_len_idx+1+table_len])
        .map_err(|e| StorageError::Internal(format!("decode table: {e}")))?;
    
    let base_key_idx = table_len_idx + 1 + table_len;
    let base_row_key = &key[base_key_idx..];

    Ok((shard_id, expiry, account_id, table_name, base_row_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_key_roundtrip() {
        let account_id = "test-account";
        let table_name = "test-table";
        let base_row_key = b"my-base-row-key";
        let expiry = 1719692400i64; // Some timestamp

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

        // Same shard (if base_row_key is the same), different expiries.
        let key_early = encode_ttl_key(account_id, table_name, base_row_key, 1000);
        let key_late = encode_ttl_key(account_id, table_name, base_row_key, 2000);

        assert!(key_early < key_late);
    }
}
