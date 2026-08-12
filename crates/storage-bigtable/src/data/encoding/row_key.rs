//! Row-key encoding.
//!
//! Format:
//! ```text
//! Composite:  [pk_tag:1] [pk_len:u32BE] [pk_bytes]  [sk_tag:1] [sk_bytes]
//! PK-only:    [pk_tag:1] [pk_len:u32BE] [pk_bytes]
//! ```
//! - `pk_tag` / `sk_tag`: `0x53`=S, `0x4E`=N, `0x42`=B
//! - PK length-prefix groups rows with the same PK contiguously in BigTable's
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

/// One past the largest possible SK encoding byte. Used to construct exclusive
/// upper bounds for partition-scan ranges; 16 copies make for-all-practical-
/// purposes "unreachable".
const HI_FILL: [u8; 16] = [0xFF; 16];

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
pub fn sk_tag_and_bytes(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    Ok((tag_for(av)?, encoded_key_bytes(av)?))
}

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

    let mut out = Vec::with_capacity(1 + 4 + pk_bytes.len() + 1 + 64);
    out.push(pk_tag);
    out.extend_from_slice(&(pk_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&pk_bytes);

    if let Some(sk_name) = sk_name {
        let sk = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Validation(format!("missing key attr {sk_name}")))?;
        let (sk_tag, sk_bytes) = sk_tag_and_bytes(sk)?;
        out.push(sk_tag);
        out.extend_from_slice(&sk_bytes);
    }
    Ok(out)
}

/// Lower bound for a Query against a single partition.
pub fn pk_range_start(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let pk_bytes = encoded_key_bytes(pk)?;
    let mut out = Vec::with_capacity(1 + 4 + pk_bytes.len());
    out.push(tag_for(pk)?);
    out.extend_from_slice(&(pk_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&pk_bytes);
    Ok(out)
}

/// Exclusive upper bound for a Query against a single partition.
pub fn pk_range_end_inclusive(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let mut out = pk_range_start(pk)?;
    out.push(0xFF);
    out.extend_from_slice(&HI_FILL);
    Ok(out)
}

/// Append the "one past any possible SK" trailer to a row-key prefix. Useful
/// for constructing exclusive upper bounds in Query SK conditions.
pub fn append_sk_upper_trailer(buf: &mut Vec<u8>) {
    buf.push(0xFF);
    buf.extend_from_slice(&HI_FILL);
}

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
        key[idx..idx+4]
            .try_into()
            .map_err(|_| StorageError::Internal("failed to convert slice to array".into()))?
    ) as usize;
    idx += 4;
    if key.len() < idx + pk_len {
        return Err(StorageError::Internal("malformed row key (PK bytes)".into()));
    }
    let pk_bytes = &key[idx..idx+pk_len];
    idx += pk_len;
    
    let pk_val = match pk_tag {
        TAG_S => AttributeValue::S(String::from_utf8(pk_bytes.to_vec())
            .map_err(|e| StorageError::Internal(format!("decode PK S: {e}")))?),
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
            TAG_S => AttributeValue::S(String::from_utf8(sk_bytes.to_vec())
                .map_err(|e| StorageError::Internal(format!("decode SK S: {e}")))?),
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
        let hi = pk_range_end_inclusive(&pk).unwrap();
        assert!(lo < hi);
        assert_eq!(lo[0], TAG_N);
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
}
