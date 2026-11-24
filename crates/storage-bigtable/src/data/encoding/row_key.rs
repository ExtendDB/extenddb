// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row-key encoding.
//!
//! Format:
//! ```text
//! Composite:  [pk_tag:1] [pk_len:u32BE] [pk_bytes]  [sk_tag:1] [sk_bytes]
//! PK-only:    [pk_tag:1] [pk_len:u32BE] [pk_bytes]
//! ```
//! - `pk_tag` / `sk_tag`: `0x53`=S, `0x4E`=N, `0x42`=B
//! - PK length-prefix groups rows with the same PK contiguously in Bigtable's
//!   lex-sorted row-key space; SK bytes preserve DDB's intra-partition sort
//!   order.
//! - For S/B the SK bytes are raw. For N the SK bytes are the
//!   lex-preserving decimal encoding produced by `encoding::number`.

use super::number;
use extenddb_core::types::{AttributeValue, KeySchemaElement, KeyType};
use extenddb_storage::error::StorageError;

const TAG_S: u8 = 0x53;
const TAG_N: u8 = 0x4E;
const TAG_B: u8 = 0x42;

/// Maximum allowed row key size in Cloud Bigtable (4KB = 4096 bytes).
pub const MAX_ROW_KEY_SIZE: usize = 4096;

/// Exclusive upper bound for a raw byte prefix, or `None` when `prefix` is
/// empty or every byte is `0xFF`.
///
/// Any byte sequence `k` starting with `prefix` satisfies `prefix <= k < upper`,
/// and any byte sequence not starting with `prefix` lies outside `[prefix, upper)`.
#[must_use]
pub fn binary_prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let pos = prefix.iter().rposition(|&b| b < 0xFF)?;
    let mut upper = prefix[..=pos].to_vec();
    upper[pos] += 1;
    Some(upper)
}

/// Smallest valid (`<= MAX_ROW_KEY_SIZE`-byte) Bigtable row key strictly greater
/// than `key` in lexicographic byte order, or `None` if none exists.
#[must_use]
pub fn next_valid_row_key_after(key: &[u8]) -> Option<Vec<u8>> {
    if key.len() < MAX_ROW_KEY_SIZE {
        let mut next = Vec::with_capacity(key.len() + 1);
        next.extend_from_slice(key);
        next.push(0x00);
        Some(next)
    } else {
        binary_prefix_upper_bound(&key[..MAX_ROW_KEY_SIZE])
    }
}

/// Clamp a lexicographic range `(start_closed, end, end_open)` to the domain of
/// valid Bigtable row keys (`len <= MAX_ROW_KEY_SIZE`), returning `None` if no
/// valid row key can fall inside the range.
///
/// For any valid row key `x` (`|x| <= M`) and bound `B` with `|B| > M` and
/// `H = B[..M]`: `x <= H <=> x < B <=> x <= B`, and `x > H <=> x >= B`.
#[must_use]
pub fn clamp_range_to_max_row_key(
    start: Vec<u8>,
    end: Vec<u8>,
    end_open: bool,
) -> Option<(Vec<u8>, Vec<u8>, bool)> {
    if start > end || (end_open && start == end) {
        return None;
    }
    let s = if start.len() <= MAX_ROW_KEY_SIZE {
        start
    } else {
        binary_prefix_upper_bound(&start[..MAX_ROW_KEY_SIZE])?
    };
    let (e, open) = if end.len() <= MAX_ROW_KEY_SIZE {
        (end, end_open)
    } else {
        (end[..MAX_ROW_KEY_SIZE].to_vec(), false)
    };
    if s > e || (open && s == e) {
        None
    } else {
        Some((s, e, open))
    }
}

fn tag_for(av: &AttributeValue) -> Result<u8, StorageError> {
    match av {
        AttributeValue::S(_) => Ok(TAG_S),
        AttributeValue::N(_) => Ok(TAG_N),
        AttributeValue::B(_) => Ok(TAG_B),
        other => Err(StorageError::Validation(format!(
            "key attribute must be S/N/B, got {other:?}"
        ))),
    }
}

/// Encoded bytes for a key attribute, without the type tag. For S/B raw; for N
/// the lex-preserving decimal encoding.
fn encoded_key_bytes(av: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    match av {
        AttributeValue::S(s) => Ok(s.as_bytes().to_vec()),
        AttributeValue::N(n) => number::encode(n),
        AttributeValue::B(b) => Ok(b.clone()),
        other => Err(StorageError::Validation(format!(
            "key attribute must be S/N/B, got {other:?}"
        ))),
    }
}

/// `(tag, bytes)` pair for an SK value — used by query/scan to construct range
/// bounds without re-deriving the encoding rules.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if `av` is not a scalar `S`, `N`, or `B` key value.
pub fn sk_tag_and_bytes(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    Ok((tag_for(av)?, encoded_key_bytes(av)?))
}

/// Encode a base-table item's primary key (`HASH` + optional `RANGE`) into its
/// Bigtable row key, enforcing Bigtable's 4,096-byte row-key ceiling.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if required key attributes are missing,
/// have non-scalar types, or exceed `MAX_ROW_KEY_SIZE`.
pub fn encode_key(
    item: &extenddb_core::types::Item,
    key_schema: &[KeySchemaElement],
) -> Result<Vec<u8>, StorageError> {
    let pk_name = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .map(|k| &k.attribute_name)
        .ok_or_else(|| StorageError::Validation("key schema missing HASH".into()))?;
    let sk_name = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
        .map(|k| &k.attribute_name);

    let pk = item
        .get(pk_name)
        .ok_or_else(|| StorageError::Validation(format!("missing key attr {pk_name}")))?;
    let pk_tag = tag_for(pk)?;
    let pk_bytes = encoded_key_bytes(pk)?;
    let pk_len = u32::try_from(pk_bytes.len())
        .map_err(|_| StorageError::Validation("partition key exceeds u32 length".into()))?;

    let mut out = Vec::with_capacity(1 + 4 + pk_bytes.len() + 1 + 64);
    out.push(pk_tag);
    out.extend_from_slice(&pk_len.to_be_bytes());
    out.extend_from_slice(&pk_bytes);

    if let Some(sk_name) = sk_name {
        let sk = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Validation(format!("missing key attr {sk_name}")))?;
        let (sk_tag, sk_bytes) = sk_tag_and_bytes(sk)?;
        out.push(sk_tag);
        out.extend_from_slice(&sk_bytes);
    }
    if out.len() > MAX_ROW_KEY_SIZE {
        return Err(StorageError::Validation(format!(
            "encoded row key size ({} bytes) exceeds Bigtable maximum limit of {} bytes",
            out.len(),
            MAX_ROW_KEY_SIZE
        )));
    }
    Ok(out)
}

/// Lower bound for a Query against a single partition.
///
/// # Errors
///
/// Returns [`StorageError::Validation`] if `pk` is not a valid scalar key attribute.
pub fn pk_range_start(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let pk_bytes = encoded_key_bytes(pk)?;
    let pk_len = u32::try_from(pk_bytes.len())
        .map_err(|_| StorageError::Validation("partition key exceeds u32 length".into()))?;
    let mut out = Vec::with_capacity(1 + 4 + pk_bytes.len());
    out.push(tag_for(pk)?);
    out.extend_from_slice(&pk_len.to_be_bytes());
    out.extend_from_slice(&pk_bytes);
    Ok(out)
}

/// Exclusive upper bound (`EndKeyOpen`) for a Query against a single partition.
///
/// Because byte `0` of `pk_range_start` is always a type tag (`0x42`/`0x4E`/`0x53`
/// `< 0xFF`), `binary_prefix_upper_bound` always succeeds and never exceeds
/// `pk_range_start(pk).len()`.
///
/// # Errors
///
/// Returns [`StorageError`] if `pk` cannot be encoded as a partition key.
pub fn pk_range_end_exclusive(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let start = pk_range_start(pk)?;
    binary_prefix_upper_bound(&start)
        .ok_or_else(|| StorageError::Internal("all-0xFF PK prefix".into()))
}

/// Decode a base-table Bigtable row key back into its key attributes.
///
/// # Errors
///
/// Returns [`StorageError`] if `key` is truncated or `key_schema` lacks a `HASH` key.
pub fn decode_key(
    key: &[u8],
    key_schema: &[KeySchemaElement],
) -> Result<extenddb_core::types::Item, StorageError> {
    let pk_name = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .map(|k| &k.attribute_name)
        .ok_or_else(|| StorageError::Validation("key schema missing HASH".into()))?;
    let sk_name = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
        .map(|k| &k.attribute_name);

    if key.is_empty() {
        return Err(StorageError::Internal("empty row key".into()));
    }

    let mut idx = 0;

    // Decode PK
    let pk_tag = key[idx];
    idx += 1;
    if key.len() < idx + 4 {
        return Err(StorageError::Internal("malformed row key (PK len)".into()));
    }
    let pk_len = u32::from_be_bytes(
        key[idx..idx + 4]
            .try_into()
            .map_err(|_| StorageError::Internal("failed to convert slice to array".into()))?,
    ) as usize;
    idx += 4;
    if key.len() < idx + pk_len {
        return Err(StorageError::Internal(
            "malformed row key (PK bytes)".into(),
        ));
    }
    let pk_bytes = &key[idx..idx + pk_len];
    idx += pk_len;

    let pk_val = match pk_tag {
        TAG_S => AttributeValue::S(
            String::from_utf8(pk_bytes.to_vec())
                .map_err(|e| StorageError::Internal(format!("decode PK S: {e}")))?,
        ),
        TAG_B => AttributeValue::B(pk_bytes.to_vec()),
        TAG_N => AttributeValue::N(number::decode(pk_bytes)?),
        _ => return Err(StorageError::Internal(format!("unknown PK tag: {pk_tag}"))),
    };

    let mut item = extenddb_core::types::Item::new();
    item.insert(pk_name.clone(), pk_val);

    // Decode SK if present in schema
    if let Some(sk_name) = sk_name {
        if key.len() < idx + 1 {
            return Err(StorageError::Internal("missing SK in row key".into()));
        }
        let sk_tag = key[idx];
        idx += 1;
        let sk_bytes = &key[idx..];

        let sk_val = match sk_tag {
            TAG_S => AttributeValue::S(
                String::from_utf8(sk_bytes.to_vec())
                    .map_err(|e| StorageError::Internal(format!("decode SK S: {e}")))?,
            ),
            TAG_B => AttributeValue::B(sk_bytes.to_vec()),
            TAG_N => AttributeValue::N(number::decode(sk_bytes)?),
            _ => return Err(StorageError::Internal(format!("unknown SK tag: {sk_tag}"))),
        };
        item.insert(sk_name.clone(), sk_val);
    }

    Ok(item)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, KeyType};

    fn ks(name: &str, kt: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.into(),
            key_type: kt,
        }
    }

    #[test]
    fn pk_only_encoding() {
        let schema = vec![ks("pk", KeyType::Hash)];
        let mut item = std::collections::BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::S("hello".into()));
        let key = encode_key(&item, &schema).unwrap();
        assert_eq!(&key[0..1], &[0x53]);
        assert_eq!(&key[1..5], &(5u32).to_be_bytes());
        assert_eq!(&key[5..], b"hello");
    }

    #[test]
    fn composite_encoding_orders_within_partition() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let mk = |sk: &str| {
            let mut item = std::collections::BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S("p".into()));
            item.insert("sk".to_string(), AttributeValue::S(sk.into()));
            encode_key(&item, &schema).unwrap()
        };
        assert!(mk("aa") < mk("abc"));
        assert!(mk("abc") < mk("z"));
    }

    #[test]
    fn n_composite_sk_orders_numerically() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let mk = |sk: &str| {
            let mut item = std::collections::BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S("p".into()));
            item.insert("sk".to_string(), AttributeValue::N(sk.into()));
            encode_key(&item, &schema).unwrap()
        };
        // Lex-compares the encoded rows in DDB's numeric order.
        let series = ["-100", "-1.5", "-1", "0", "0.5", "1", "1.5", "100", "1e10"];
        for w in series.windows(2) {
            assert!(mk(w[0]) < mk(w[1]), "{} < {}", w[0], w[1]);
        }
    }

    #[test]
    fn n_pk_encoding_round_trips_via_helpers() {
        let pk = AttributeValue::N("42".into());
        let lo = pk_range_start(&pk).unwrap();
        let hi = pk_range_end_exclusive(&pk).unwrap();
        assert!(lo < hi);
        assert_eq!(lo[0], TAG_N);

        // Partition with all-0xFF binary PK still produces an exact short exclusive bound.
        let ff_pk = AttributeValue::B(vec![0xFF; 32]);
        let ff_lo = pk_range_start(&ff_pk).unwrap();
        let ff_hi = pk_range_end_exclusive(&ff_pk).unwrap();
        assert!(ff_lo < ff_hi);
        assert!(ff_hi.len() < ff_lo.len());
        let mut max_sk_row = ff_lo.clone();
        max_sk_row.push(TAG_B);
        max_sk_row.extend_from_slice(&[0xFF; 64]);
        assert!(
            max_sk_row.as_slice() >= ff_lo.as_slice() && max_sk_row.as_slice() < ff_hi.as_slice()
        );
    }

    #[test]
    fn row_key_successor_and_clamping_respect_4kb_limit() {
        let short = vec![TAG_S, 0x41];
        assert_eq!(
            next_valid_row_key_after(&short),
            Some(vec![TAG_S, 0x41, 0x00])
        );

        let mut at_max = vec![TAG_S; MAX_ROW_KEY_SIZE];
        at_max[MAX_ROW_KEY_SIZE - 1] = 0xFF;
        let succ = next_valid_row_key_after(&at_max).unwrap();
        assert_eq!(succ.len(), MAX_ROW_KEY_SIZE - 1);
        assert_eq!(*succ.last().unwrap(), TAG_S + 1);

        // Overlong exact comparison [overlong, overlong] closed clamps to None.
        let mut overlong = vec![TAG_S; MAX_ROW_KEY_SIZE + 5];
        overlong[MAX_ROW_KEY_SIZE] = 0x00;
        assert_eq!(
            clamp_range_to_max_row_key(overlong.clone(), overlong.clone(), false),
            None
        );
        // [short, overlong) clamps to [short, overlong[..4096]] closed.
        let (cs, ce, copen) =
            clamp_range_to_max_row_key(short.clone(), overlong.clone(), true).unwrap();
        assert_eq!(cs, short);
        assert_eq!(ce.len(), MAX_ROW_KEY_SIZE);
        assert!(!copen);
    }

    #[test]
    fn round_trip_key_decoding() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let mut item = std::collections::BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::S("partition".into()));
        item.insert("sk".to_string(), AttributeValue::N("123.45".into()));

        let key = encode_key(&item, &schema).unwrap();
        let decoded = decode_key(&key, &schema).unwrap();

        assert_eq!(item.get("pk"), decoded.get("pk"));
        let enc_orig = encode_key(&item, &schema).unwrap();
        let enc_dec = encode_key(&decoded, &schema).unwrap();
        assert_eq!(enc_orig, enc_dec);
    }

    #[test]
    fn round_trip_key_decoding_with_binary_and_number() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let mut item = std::collections::BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::B(vec![1, 2, 3, 4]));
        item.insert("sk".to_string(), AttributeValue::N("-99.99".into()));

        let key = encode_key(&item, &schema).unwrap();
        let decoded = decode_key(&key, &schema).unwrap();

        assert_eq!(item.get("pk"), decoded.get("pk"));

        let enc_orig = encode_key(&item, &schema).unwrap();
        let enc_dec = encode_key(&decoded, &schema).unwrap();
        assert_eq!(enc_orig, enc_dec);
    }

    #[test]
    fn rejects_key_exceeding_4kb_limit() {
        let schema = vec![ks("pk", KeyType::Hash)];
        let mut item = std::collections::BTreeMap::new();
        let big_str = "a".repeat(4096);
        item.insert("pk".to_string(), AttributeValue::S(big_str));
        let res = encode_key(&item, &schema);
        assert!(res.is_err());
        match res.unwrap_err() {
            StorageError::Validation(msg) => {
                assert!(msg.contains("exceeds Bigtable maximum limit of 4096 bytes"));
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }
}
