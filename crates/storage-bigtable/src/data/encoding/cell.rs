//! Cell value encoding.
//!
//! Per-attribute cells with a 1-byte type tag + payload.
//!
//! ```text
//! S    0x01 + raw UTF-8 bytes
//! N    0x02 + raw UTF-8 bytes
//! B    0x03 + raw bytes
//! BOOL 0x04 + (0x00 | 0x01)
//! NULL 0x05 + (empty payload)
//! SS   0x06 + JSON-serialized BTreeSet<String>
//! NS   0x07 + JSON-serialized BTreeSet<String>  (DDB N is string-encoded)
//! BS   0x08 + JSON-serialized Vec<base64 String> for binary-safe transport
//! L    0x09 + JSON-serialized Vec<AttributeValue> (DDB-spec form)
//! M    0x0A + JSON-serialized BTreeMap<String, AttributeValue>
//! ```
//!
//! Scalars stay hand-rolled (tiny + simple). Sets / L / M use serde_json
//! under their tag bytes — leverages AttributeValue's existing serde impls
//! which produce DDB-spec JSON, and decoders are symmetric.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use extenddb_core::types::AttributeValue;
use extenddb_storage::error::StorageError;

pub const TAG_S: u8 = 0x01;
pub const TAG_N: u8 = 0x02;
pub const TAG_B: u8 = 0x03;
pub const TAG_BOOL: u8 = 0x04;
pub const TAG_NULL: u8 = 0x05;
pub const TAG_SS: u8 = 0x06;
pub const TAG_NS: u8 = 0x07;
pub const TAG_BS: u8 = 0x08;
pub const TAG_L: u8 = 0x09;
pub const TAG_M: u8 = 0x0A;

pub fn encode(av: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    Ok(match av {
        AttributeValue::S(s) => {
            let mut out = Vec::with_capacity(1 + s.len());
            out.push(TAG_S);
            out.extend_from_slice(s.as_bytes());
            out
        }
        AttributeValue::N(s) => {
            let mut out = Vec::with_capacity(1 + s.len());
            out.push(TAG_N);
            out.extend_from_slice(s.as_bytes());
            out
        }
        AttributeValue::B(b) => {
            let mut out = Vec::with_capacity(1 + b.len());
            out.push(TAG_B);
            out.extend_from_slice(b);
            out
        }
        AttributeValue::Bool(b) => vec![TAG_BOOL, if *b { 1 } else { 0 }],
        AttributeValue::Null => vec![TAG_NULL],
        AttributeValue::SS(set) => {
            let body = serde_json::to_vec(set).map_err(|e| {
                StorageError::Internal(format!("encode SS: {e}"))
            })?;
            let mut out = Vec::with_capacity(1 + body.len());
            out.push(TAG_SS);
            out.extend_from_slice(&body);
            out
        }
        AttributeValue::NS(set) => {
            let body = serde_json::to_vec(set).map_err(|e| {
                StorageError::Internal(format!("encode NS: {e}"))
            })?;
            let mut out = Vec::with_capacity(1 + body.len());
            out.push(TAG_NS);
            out.extend_from_slice(&body);
            out
        }
        AttributeValue::BS(set) => {
            // Convert each Vec<u8> to base64 so JSON can carry it; the
            // BTreeSet ordering is over Vec<u8>, which is consistent across
            // sessions, so the decoded set matches the encoded one.
            let encoded: Vec<String> = set
                .iter()
                .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                .collect();
            let body = serde_json::to_vec(&encoded).map_err(|e| {
                StorageError::Internal(format!("encode BS: {e}"))
            })?;
            let mut out = Vec::with_capacity(1 + body.len());
            out.push(TAG_BS);
            out.extend_from_slice(&body);
            out
        }
        AttributeValue::L(list) => {
            // AttributeValue's serde Serialize emits {"S": "..."} etc. — the
            // DDB-spec wire form — which the Deserialize impl reverses.
            let body = serde_json::to_vec(list).map_err(|e| {
                StorageError::Internal(format!("encode L: {e}"))
            })?;
            let mut out = Vec::with_capacity(1 + body.len());
            out.push(TAG_L);
            out.extend_from_slice(&body);
            out
        }
        AttributeValue::M(map) => {
            let body = serde_json::to_vec(map).map_err(|e| {
                StorageError::Internal(format!("encode M: {e}"))
            })?;
            let mut out = Vec::with_capacity(1 + body.len());
            out.push(TAG_M);
            out.extend_from_slice(&body);
            out
        }
    })
}

pub fn decode(bytes: &[u8]) -> Result<AttributeValue, StorageError> {
    let tag = *bytes.first().ok_or_else(|| {
        StorageError::Internal("cell payload empty (no type tag)".into())
    })?;
    let payload = &bytes[1..];
    Ok(match tag {
        TAG_S => AttributeValue::S(
            String::from_utf8(payload.to_vec())
                .map_err(|e| StorageError::Internal(format!("decode S: {e}")))?,
        ),
        TAG_N => AttributeValue::N(
            String::from_utf8(payload.to_vec())
                .map_err(|e| StorageError::Internal(format!("decode N: {e}")))?,
        ),
        TAG_B => AttributeValue::B(payload.to_vec()),
        TAG_BOOL => AttributeValue::Bool(payload.first().copied().unwrap_or(0) != 0),
        TAG_NULL => AttributeValue::Null,
        TAG_SS => {
            let set: BTreeSet<String> = serde_json::from_slice(payload)
                .map_err(|e| StorageError::Internal(format!("decode SS: {e}")))?;
            AttributeValue::SS(set)
        }
        TAG_NS => {
            let set: BTreeSet<String> = serde_json::from_slice(payload)
                .map_err(|e| StorageError::Internal(format!("decode NS: {e}")))?;
            AttributeValue::NS(set)
        }
        TAG_BS => {
            let encoded: Vec<String> = serde_json::from_slice(payload)
                .map_err(|e| StorageError::Internal(format!("decode BS: {e}")))?;
            let mut set: BTreeSet<Vec<u8>> = BTreeSet::new();
            for s in encoded {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&s)
                    .map_err(|e| StorageError::Internal(format!("decode BS b64: {e}")))?;
                set.insert(bytes);
            }
            AttributeValue::BS(set)
        }
        TAG_L => {
            let list: Vec<AttributeValue> = serde_json::from_slice(payload)
                .map_err(|e| StorageError::Internal(format!("decode L: {e}")))?;
            AttributeValue::L(list)
        }
        TAG_M => {
            let map: BTreeMap<String, AttributeValue> = serde_json::from_slice(payload)
                .map_err(|e| StorageError::Internal(format!("decode M: {e}")))?;
            AttributeValue::M(map)
        }
        unknown => {
            return Err(StorageError::Internal(format!(
                "unknown cell type tag {unknown:#x}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(av: AttributeValue) {
        let enc = encode(&av).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(av, dec);
    }

    #[test]
    fn roundtrip_scalars() {
        rt(AttributeValue::S("hello".into()));
        rt(AttributeValue::N("3.14159".into()));
        rt(AttributeValue::B(vec![0, 1, 2, 255]));
        rt(AttributeValue::Bool(true));
        rt(AttributeValue::Bool(false));
        rt(AttributeValue::Null);
    }

    #[test]
    fn n_preserves_string_form() {
        let enc = encode(&AttributeValue::N("3.14159".into())).unwrap();
        let dec = decode(&enc).unwrap();
        if let AttributeValue::N(s) = dec {
            assert_eq!(s, "3.14159"); // ddbtest::test_decimal_roundtrip relies on this
        } else {
            panic!("expected N");
        }
    }

    #[test]
    fn roundtrip_sets() {
        let mut ss = std::collections::BTreeSet::new();
        ss.insert("a".to_string());
        ss.insert("b".to_string());
        ss.insert("c".to_string());
        rt(AttributeValue::SS(ss));

        let mut ns = std::collections::BTreeSet::new();
        ns.insert("1".to_string());
        ns.insert("2".to_string());
        rt(AttributeValue::NS(ns));

        let mut bs = std::collections::BTreeSet::new();
        bs.insert(vec![0u8, 1, 2]);
        bs.insert(vec![3u8, 4, 5]);
        rt(AttributeValue::BS(bs));
    }

    #[test]
    fn roundtrip_list_recursive() {
        let l = AttributeValue::L(vec![
            AttributeValue::S("x".into()),
            AttributeValue::N("42".into()),
            AttributeValue::Bool(true),
            AttributeValue::L(vec![AttributeValue::Null, AttributeValue::S("nested".into())]),
        ]);
        rt(l);
    }

    #[test]
    fn roundtrip_map_recursive() {
        let mut inner = std::collections::BTreeMap::new();
        inner.insert("city".to_string(), AttributeValue::S("Brooklyn".into()));
        inner.insert("zip".to_string(), AttributeValue::N("11201".into()));

        let mut outer = std::collections::BTreeMap::new();
        outer.insert("name".to_string(), AttributeValue::S("Alice".into()));
        outer.insert("age".to_string(), AttributeValue::N("30".into()));
        outer.insert("address".to_string(), AttributeValue::M(inner));
        outer.insert(
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("dev".into()),
                AttributeValue::S("rust".into()),
            ]),
        );
        rt(AttributeValue::M(outer));
    }
}
