// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Real, round-trip-tested encoding of ExtendDB items onto S3 Object Annotations.
//!
//! This is the direct analog of the Route 53 backend's TXT-record chunking
//! (PR #54). There, the 255-byte limit on a single TXT character-string forced
//! item bodies to spill across sibling resource records keyed by sequence
//! number. Here the analogous constraint is the **1 MB per-named-annotation**
//! ceiling, so item bodies spill across sibling *annotations* keyed by sequence
//! number, reassembled on read.
//!
//! The mapping:
//!
//! - A sentinel S3 object is the "table" (analogous to #54's hosted zone). Its
//!   default key is [`DEFAULT_TABLE_OBJECT_KEY`].
//! - Each item is one logical annotation: the annotation **name** encodes the
//!   partition/sort key, and the annotation **value** is the JSON-serialized
//!   item body.
//! - Items larger than 1 MB are split across sibling annotations keyed by
//!   sequence number (`<key>#0001`, `<key>#0002`, …), reassembled on read.
//!
//! The body is base64-encoded before chunking. base64 output is pure ASCII, so
//! it can be split at any byte boundary without tearing a UTF-8 code point —
//! exactly the property #54 relied on when slicing into 255-byte TXT strings.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;

/// Default S3 object key that plays the role of a "table".
///
/// Analogous to PR #54's hosted zone: the sentinel object whose annotations
/// collectively constitute the table's contents. The key is, of course, a
/// rebuttal.
pub const DEFAULT_TABLE_OBJECT_KEY: &str = ".well-actually";

/// Maximum size of a single named annotation value: 1 MB, per the S3 Object
/// Annotations limits. Item bodies larger than this spill across siblings.
pub const MAX_ANNOTATION_VALUE_BYTES: usize = 1024 * 1024;

/// Maximum number of named annotations attached to a single object: 1,000, per
/// the S3 Object Annotations limits. Because each item is encoded as one or
/// more annotations on the sentinel object, a table holds at most 1,000 items —
/// fewer when any item exceeds [`MAX_ANNOTATION_VALUE_BYTES`] and spills across
/// multiple annotations.
pub const MAX_ANNOTATIONS_PER_OBJECT: usize = 1000;

/// Separator between the encoded partition key and encoded sort key within an
/// annotation name. `~` is not part of the URL-safe base64 alphabet, so it can
/// never appear inside an encoded key component.
const KEY_SEPARATOR: char = '~';

/// Separator between the encoded key and the chunk sequence number. `#` is also
/// outside the base64 alphabet, so the sequence suffix is unambiguous.
const SEQUENCE_SEPARATOR: char = '#';

/// Errors produced while encoding items to, or decoding items from, annotations.
#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    /// An annotation name (or key component) was not valid base64.
    #[error("annotation name is not valid base64: {0}")]
    InvalidBase64(String),
    /// An annotation name did not match the `<key>#NNNN` shape.
    #[error("annotation name is malformed: {0}")]
    MalformedName(String),
    /// A decoded key component or body was not valid UTF-8.
    #[error("decoded value is not valid UTF-8")]
    InvalidUtf8,
    /// No annotations were supplied to reassemble an item.
    #[error("no annotations supplied for item")]
    Empty,
    /// The chunk sequence numbers were not a contiguous `1..=n` run.
    #[error("annotation chunks do not form a contiguous sequence")]
    NonContiguousChunks,
    /// The item would require more annotations than an object can hold.
    #[error("item too large: {chunks} chunks exceeds the {max}-annotation per-object limit")]
    TooManyChunks { chunks: usize, max: usize },
}

/// The partition (and optional sort) key of an item, before encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemKey {
    /// The partition key value, rendered as a string.
    pub partition_key: String,
    /// The sort key value, if the table has a sort key.
    pub sort_key: Option<String>,
}

/// A single named annotation as it would be written via `PutObjectAnnotation`
/// and read back via `GetObjectAnnotation` / `ListObjectAnnotations`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotation {
    /// The annotation name: `<encoded-key>#NNNN`.
    pub name: String,
    /// The annotation value: a slice of the base64-encoded item body. Lands in
    /// the `text_value` column of the Iceberg annotation table.
    pub value: String,
}

/// Encode an item key into the shared base of its annotation names.
///
/// The partition and sort key are base64-encoded independently and joined with
/// [`KEY_SEPARATOR`], so either may contain arbitrary bytes (including `#` or
/// `~`) without colliding with the name grammar.
#[must_use]
pub fn encode_key(key: &ItemKey) -> String {
    let pk = B64.encode(key.partition_key.as_bytes());
    match &key.sort_key {
        Some(sk) => format!("{pk}{KEY_SEPARATOR}{}", B64.encode(sk.as_bytes())),
        None => pk,
    }
}

/// Decode the shared base of an annotation name back into an [`ItemKey`].
pub fn decode_key(encoded: &str) -> Result<ItemKey, EncodingError> {
    match encoded.split_once(KEY_SEPARATOR) {
        Some((pk_b64, sk_b64)) => Ok(ItemKey {
            partition_key: decode_component(pk_b64)?,
            sort_key: Some(decode_component(sk_b64)?),
        }),
        None => Ok(ItemKey {
            partition_key: decode_component(encoded)?,
            sort_key: None,
        }),
    }
}

fn decode_component(b64: &str) -> Result<String, EncodingError> {
    let bytes = B64
        .decode(b64)
        .map_err(|e| EncodingError::InvalidBase64(e.to_string()))?;
    String::from_utf8(bytes).map_err(|_| EncodingError::InvalidUtf8)
}

/// Encode an item into one or more annotations.
///
/// `body` is the JSON-serialized item body (the analog of #54's item JSON). It
/// is base64-encoded — keeping the payload ASCII so it can be split at any byte
/// boundary — then chunked into `<=` [`MAX_ANNOTATION_VALUE_BYTES`] pieces. Each
/// chunk becomes one named annotation `"<encoded-key>#NNNN"`, written via
/// `PutObjectAnnotation`.
///
/// # Errors
///
/// Returns [`EncodingError::TooManyChunks`] if the body would need more than
/// [`MAX_ANNOTATIONS_PER_OBJECT`] annotations to store.
pub fn encode_item(key: &ItemKey, body: &str) -> Result<Vec<Annotation>, EncodingError> {
    let base = encode_key(key);
    let encoded = B64.encode(body.as_bytes());
    // base64 output is pure ASCII, so each byte slice is itself valid UTF-8 and
    // a valid base64 fragment of the whole — the same invariant that let #54
    // tear the payload across 255-byte TXT strings.
    let bytes = encoded.as_bytes();

    let chunk_count = bytes.len().div_ceil(MAX_ANNOTATION_VALUE_BYTES).max(1);
    if chunk_count > MAX_ANNOTATIONS_PER_OBJECT {
        return Err(EncodingError::TooManyChunks {
            chunks: chunk_count,
            max: MAX_ANNOTATIONS_PER_OBJECT,
        });
    }

    // An empty body still occupies one annotation, so a present-but-empty item
    // round-trips rather than vanishing.
    if bytes.is_empty() {
        return Ok(vec![Annotation {
            name: format!("{base}{SEQUENCE_SEPARATOR}0001"),
            value: String::new(),
        }]);
    }

    let annotations = bytes
        .chunks(MAX_ANNOTATION_VALUE_BYTES)
        .enumerate()
        .map(|(i, chunk)| Annotation {
            name: format!("{base}{SEQUENCE_SEPARATOR}{seq:04}", seq = i + 1),
            // `chunk` is a slice of ASCII base64 output, so this never fails.
            value: String::from_utf8(chunk.to_vec()).expect("base64 output is ASCII"),
        })
        .collect();

    Ok(annotations)
}

/// Reassemble an item body from its annotations.
///
/// Sorts the chunks by sequence number, concatenates the base64 fragments, and
/// decodes. The annotations may arrive in any order, because
/// `ListObjectAnnotations` makes no ordering guarantee. Returns the item key
/// (decoded from the shared name base) and the JSON body.
///
/// # Errors
///
/// Returns [`EncodingError`] if the names are malformed, belong to different
/// items, are not a contiguous sequence, or do not base64-decode to UTF-8.
pub fn decode_item(annotations: &[Annotation]) -> Result<(ItemKey, String), EncodingError> {
    if annotations.is_empty() {
        return Err(EncodingError::Empty);
    }

    // (sequence, base-key, value)
    let mut chunks: Vec<(usize, &str, &str)> = Vec::with_capacity(annotations.len());
    for ann in annotations {
        let (base, seq_str) = ann
            .name
            .rsplit_once(SEQUENCE_SEPARATOR)
            .ok_or_else(|| EncodingError::MalformedName(ann.name.clone()))?;
        let seq: usize = seq_str
            .parse()
            .map_err(|_| EncodingError::MalformedName(ann.name.clone()))?;
        chunks.push((seq, base, ann.value.as_str()));
    }

    let base = chunks[0].1;
    if chunks.iter().any(|(_, b, _)| *b != base) {
        return Err(EncodingError::MalformedName(
            "annotations belong to different items".to_owned(),
        ));
    }

    chunks.sort_by_key(|(seq, _, _)| *seq);
    for (idx, (seq, _, _)) in chunks.iter().enumerate() {
        if *seq != idx + 1 {
            return Err(EncodingError::NonContiguousChunks);
        }
    }

    let mut encoded = String::new();
    for (_, _, value) in &chunks {
        encoded.push_str(value);
    }

    let bytes = B64
        .decode(encoded.as_bytes())
        .map_err(|e| EncodingError::InvalidBase64(e.to_string()))?;
    let body = String::from_utf8(bytes).map_err(|_| EncodingError::InvalidUtf8)?;
    let key = decode_key(base)?;

    Ok((key, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_small_item_single_annotation() {
        // A small item fits in a single annotation: one PutObjectAnnotation.
        let key = ItemKey {
            partition_key: "user#42".to_owned(),
            sort_key: Some("profile".to_owned()),
        };
        let body = r#"{"id":"42","name":"Corey","tier":"gold"}"#;

        let annotations = encode_item(&key, body).expect("encode");
        assert_eq!(annotations.len(), 1, "small item should be one annotation");
        assert!(annotations[0].name.ends_with("#0001"));
        assert!(annotations[0].value.len() <= MAX_ANNOTATION_VALUE_BYTES);

        let (decoded_key, decoded_body) = decode_item(&annotations).expect("decode");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn round_trip_partition_key_only() {
        let key = ItemKey {
            partition_key: "lonely-partition".to_owned(),
            sort_key: None,
        };
        let body = r#"{"value":1}"#;

        let annotations = encode_item(&key, body).expect("encode");
        let (decoded_key, decoded_body) = decode_item(&annotations).expect("decode");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn round_trip_multi_megabyte_item_chunks() {
        // A multi-MB item spills across sibling annotations (one
        // PutObjectAnnotation per chunk), reassembled on read.
        let key = ItemKey {
            partition_key: "big".to_owned(),
            sort_key: Some("blob".to_owned()),
        };
        // ~3 MB of body → ~4 MB of base64 → at least four 1 MB annotations.
        let payload = "x".repeat(3 * 1024 * 1024);
        let body = format!(r#"{{"blob":"{payload}"}}"#);

        let annotations = encode_item(&key, &body).expect("encode");
        assert!(
            annotations.len() >= 2,
            "multi-MB item must span multiple annotations, got {}",
            annotations.len()
        );
        for ann in &annotations {
            assert!(
                ann.value.len() <= MAX_ANNOTATION_VALUE_BYTES,
                "no annotation may exceed the 1 MB limit"
            );
        }

        let (decoded_key, decoded_body) = decode_item(&annotations).expect("decode");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn decode_is_order_independent() {
        // ListObjectAnnotations makes no ordering promise; reassembly must not
        // depend on the order chunks come back in.
        let key = ItemKey {
            partition_key: "shuffled".to_owned(),
            sort_key: None,
        };
        let body = format!(r#"{{"blob":"{}"}}"#, "y".repeat(2 * 1024 * 1024));

        let mut annotations = encode_item(&key, &body).expect("encode");
        annotations.reverse();

        let (decoded_key, decoded_body) = decode_item(&annotations).expect("decode");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn empty_body_round_trips() {
        let key = ItemKey {
            partition_key: "k".to_owned(),
            sort_key: None,
        };
        let annotations = encode_item(&key, "").expect("encode");
        assert_eq!(annotations.len(), 1);
        let (decoded_key, decoded_body) = decode_item(&annotations).expect("decode");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_body, "");
    }
}
