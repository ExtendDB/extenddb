// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data file encoding and decoding.
//!
//! A data file is gzip-compressed newline-delimited JSON, one
//! `{"Item": <DynamoDB JSON item>}` object per line, the encoding Amazon
//! `DynamoDB` writes for `DYNAMODB_JSON` exports and the same line encoding
//! the engine's `ExportTableToPointInTime` handler produces (a
//! `serde_json::json!({"Item": item})` wrapper serialized compactly, then a
//! newline), so the two agree byte for byte.
//!
//! Checksums: `sha256` and `md5` are computed over the compressed bytes, the
//! object as stored, which is what `extenddb-manifest.json` records per data
//! file and what `manifest-files.json` records as `md5Checksum`. Item and
//! uncompressed byte counts are computed over the line stream.

use std::io::{BufRead, BufReader, Write};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FormatError;
use crate::types::Item;

/// Maximum length of a single decompressed data-file line, in bytes.
///
/// One line holds one item wrapped as `{"Item": <DynamoDB JSON item>}` plus a
/// trailing newline. A `DynamoDB` item is capped at 400 KB. The `DYNAMODB_JSON`
/// wire form inflates that: every binary (`B`, `BS`) value is base64 (about
/// 4/3), attribute names and the `{"S":...}` / `{"N":...}` type tags repeat per
/// value, and the `{"Item":...}` wrapper adds a fixed frame. Four megabytes is
/// an order of magnitude above the worst realistic expansion of a 400 KB item,
/// so it never rejects a legitimate line, while it bounds the memory a single
/// `next_item` call can allocate from a malicious or corrupt gzip stream (a
/// gzip member can expand about 1030 to 1, so an unbounded read of a small
/// object can otherwise allocate gigabytes). A line at or over the cap yields
/// [`FormatError::LineTooLong`] before the whole line is materialized.
pub const MAX_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Lowercase hex encoding of a byte slice.
pub(super) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Strip a single trailing `\n` and an optional preceding `\r` from a line.
fn strip_trailing_newline(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// The single-key wrapper each data file line holds.
#[derive(Serialize, Deserialize)]
struct ItemLine {
    #[serde(rename = "Item")]
    item: Item,
}

/// Checksums and counts produced while encoding a data file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileChecksums {
    /// Number of items encoded.
    pub item_count: u64,
    /// Lowercase hex sha256 of the compressed bytes.
    pub sha256_hex: String,
    /// Base64 MD5 of the compressed bytes, the `md5Checksum` encoding
    /// `manifest-files.json` uses.
    pub md5_base64: String,
    /// Size of the compressed output.
    pub compressed_bytes: u64,
    /// Total size of the uncompressed lines, newlines included.
    pub uncompressed_bytes: u64,
}

/// What a decoder should verify. Only the members that are `Some` are
/// checked, so a caller holding just the `md5Checksum` from a
/// `manifest-files.json` line can still verify what it has.
#[derive(Debug, Clone, Default)]
pub struct DataFileExpectations {
    /// Expected lowercase hex sha256 of the compressed bytes.
    pub sha256_hex: Option<String>,
    /// Expected base64 MD5 of the compressed bytes.
    pub md5_base64: Option<String>,
    /// Expected item count.
    pub item_count: Option<u64>,
    /// Expected uncompressed byte count.
    pub uncompressed_bytes: Option<u64>,
}

impl From<&DataFileChecksums> for DataFileExpectations {
    fn from(checksums: &DataFileChecksums) -> Self {
        Self {
            sha256_hex: Some(checksums.sha256_hex.clone()),
            md5_base64: Some(checksums.md5_base64.clone()),
            item_count: Some(checksums.item_count),
            uncompressed_bytes: Some(checksums.uncompressed_bytes),
        }
    }
}

/// Encode items into a gzip data file, returning the compressed bytes and
/// the checksums a manifest records for them.
///
/// # Errors
///
/// Returns [`FormatError::Json`] when an item fails to serialize and
/// [`FormatError::Encode`] when compression fails.
pub fn encode_data_file<I>(items: I) -> Result<(Vec<u8>, DataFileChecksums), FormatError>
where
    I: IntoIterator<Item = Item>,
{
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut item_count: u64 = 0;
    let mut uncompressed_bytes: u64 = 0;

    for item in items {
        // The exact line encoding the engine's export handler produces.
        let wrapper = serde_json::json!({ "Item": item });
        let mut line = serde_json::to_string(&wrapper)?;
        line.push('\n');
        encoder
            .write_all(line.as_bytes())
            .map_err(|e| FormatError::Encode(e.to_string()))?;
        item_count += 1;
        uncompressed_bytes += line.len() as u64;
    }

    let compressed = encoder
        .finish()
        .map_err(|e| FormatError::Encode(e.to_string()))?;

    let checksums = DataFileChecksums {
        item_count,
        sha256_hex: hex_encode(&Sha256::digest(&compressed)),
        md5_base64: BASE64.encode(Md5::digest(&compressed)),
        compressed_bytes: compressed.len() as u64,
        uncompressed_bytes,
    };
    Ok((compressed, checksums))
}

/// Streaming decoder for a data file.
///
/// The checksums over the compressed bytes are verified eagerly on
/// construction; items then stream back one line at a time, and the item and
/// uncompressed byte counts are verified when the stream ends.
pub struct DataFileDecoder<'a> {
    reader: BufReader<MultiGzDecoder<&'a [u8]>>,
    expectations: DataFileExpectations,
    items_decoded: u64,
    bytes_decoded: u64,
    finished: bool,
}

impl<'a> DataFileDecoder<'a> {
    /// Verify the compressed-byte checksums and open the stream.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ChecksumMismatch`] when a checksum in
    /// `expectations` does not match `compressed`.
    pub fn new(
        compressed: &'a [u8],
        expectations: DataFileExpectations,
    ) -> Result<Self, FormatError> {
        if let Some(expected) = &expectations.sha256_hex {
            let computed = hex_encode(&Sha256::digest(compressed));
            if &computed != expected {
                return Err(FormatError::ChecksumMismatch {
                    subject: "data file sha256",
                    expected: expected.clone(),
                    computed,
                });
            }
        }
        if let Some(expected) = &expectations.md5_base64 {
            let computed = BASE64.encode(Md5::digest(compressed));
            if &computed != expected {
                return Err(FormatError::ChecksumMismatch {
                    subject: "data file md5",
                    expected: expected.clone(),
                    computed,
                });
            }
        }
        Ok(Self {
            reader: BufReader::new(MultiGzDecoder::new(compressed)),
            expectations,
            items_decoded: 0,
            bytes_decoded: 0,
            finished: false,
        })
    }

    /// Decode the next item, or verify the counts and return `Ok(None)` at
    /// the end of the stream.
    ///
    /// The line is read through a bounded loop that refuses to materialize
    /// more than [`MAX_LINE_BYTES`], so a malicious or corrupt gzip member
    /// cannot drive unbounded memory. When
    /// [`DataFileExpectations::uncompressed_bytes`] is set it is also enforced
    /// incrementally: the running total is checked against the expectation as
    /// each line is consumed, so an oversized stream is rejected before the
    /// whole of it is read rather than only at clean end of stream.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Decode`] when decompression fails,
    /// [`FormatError::LineTooLong`] when a single line reaches the cap,
    /// [`FormatError::Json`] when a line does not parse, and
    /// [`FormatError::ItemCountMismatch`] or [`FormatError::ByteCountMismatch`]
    /// when the stream ends with counts that differ from the expectations
    /// (the byte count is also enforced mid-stream once it is exceeded).
    pub fn next_item(&mut self) -> Result<Option<Item>, FormatError> {
        if self.finished {
            return Ok(None);
        }
        let mut line: Vec<u8> = Vec::new();
        let read = self.read_capped_line(&mut line)?;
        if read == 0 {
            self.finished = true;
            if let Some(expected) = self.expectations.item_count
                && expected != self.items_decoded
            {
                return Err(FormatError::ItemCountMismatch {
                    expected,
                    actual: self.items_decoded,
                });
            }
            if let Some(expected) = self.expectations.uncompressed_bytes
                && expected != self.bytes_decoded
            {
                return Err(FormatError::ByteCountMismatch {
                    expected,
                    actual: self.bytes_decoded,
                });
            }
            return Ok(None);
        }
        self.bytes_decoded += read as u64;
        // Enforce the recorded uncompressed size incrementally: once the
        // running total passes the expectation the stream is oversized, so
        // reject it here rather than continuing to read.
        if let Some(expected) = self.expectations.uncompressed_bytes
            && self.bytes_decoded > expected
        {
            self.finished = true;
            return Err(FormatError::ByteCountMismatch {
                expected,
                actual: self.bytes_decoded,
            });
        }
        let text = std::str::from_utf8(strip_trailing_newline(&line))
            .map_err(|e| FormatError::Decode(e.to_string()))?;
        let parsed: ItemLine = serde_json::from_str(text)?;
        self.items_decoded += 1;
        Ok(Some(parsed.item))
    }

    /// Read one newline-terminated line into `line`, appending at most
    /// [`MAX_LINE_BYTES`] bytes (newline included) before erroring.
    ///
    /// Returns the number of bytes read, `0` at end of stream. The read walks
    /// the `BufReader`'s own buffer with `fill_buf` / `consume`, so peak
    /// allocation is the growing line (capped) plus the reader's fixed
    /// buffer, never the whole compressed member expanded at once.
    fn read_capped_line(&mut self, line: &mut Vec<u8>) -> Result<usize, FormatError> {
        loop {
            let available = self
                .reader
                .fill_buf()
                .map_err(|e| FormatError::Decode(e.to_string()))?;
            if available.is_empty() {
                return Ok(line.len());
            }
            if let Some(nl) = available.iter().position(|&b| b == b'\n') {
                let want = nl + 1;
                if line.len() + want > MAX_LINE_BYTES {
                    return Err(FormatError::LineTooLong {
                        cap: MAX_LINE_BYTES,
                    });
                }
                line.extend_from_slice(&available[..want]);
                self.reader.consume(want);
                return Ok(line.len());
            }
            let take = available.len();
            if line.len() + take > MAX_LINE_BYTES {
                return Err(FormatError::LineTooLong {
                    cap: MAX_LINE_BYTES,
                });
            }
            line.extend_from_slice(available);
            self.reader.consume(take);
        }
    }
}

impl Iterator for DataFileDecoder<'_> {
    type Item = Result<Item, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

/// Decode a whole data file, verifying every expectation that is set.
///
/// # Errors
///
/// Propagates every error [`DataFileDecoder`] can produce.
pub fn decode_data_file(
    compressed: &[u8],
    expectations: DataFileExpectations,
) -> Result<Vec<Item>, FormatError> {
    let mut decoder = DataFileDecoder::new(compressed, expectations)?;
    let mut items = Vec::new();
    while let Some(item) = decoder.next_item()? {
        items.push(item);
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::types::AttributeValue;

    fn item(entries: Vec<(&str, AttributeValue)>) -> Item {
        entries
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect()
    }

    fn string_set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    fn byte_set(values: &[&[u8]]) -> BTreeSet<Vec<u8>> {
        values.iter().map(|b| b.to_vec()).collect()
    }

    /// A map nested to the given depth, ending in a string leaf.
    fn nested_map(depth: usize) -> AttributeValue {
        let mut value = AttributeValue::S("leaf".to_owned());
        for level in (0..depth).rev() {
            let mut map = BTreeMap::new();
            map.insert(format!("level{level}"), value);
            value = AttributeValue::M(map);
        }
        value
    }

    /// Items covering every `DynamoDB` attribute type, with numbers at the
    /// 38-significant-digit maximum and in negative form. Number strings are
    /// written in the normalized form the `N` deserializer produces, so a
    /// round trip compares equal.
    fn all_types_items() -> Vec<Item> {
        vec![
            item(vec![
                ("pk", AttributeValue::S("item-1".to_owned())),
                (
                    "n_38_digits",
                    AttributeValue::N("99999999999999999999999999999999999999".to_owned()),
                ),
                (
                    "n_negative",
                    AttributeValue::N("-12345678901234567890.123456789".to_owned()),
                ),
                (
                    "n_large_magnitude",
                    AttributeValue::N(format!("1{}", "0".repeat(100))),
                ),
                ("n_small", AttributeValue::N("0.001".to_owned())),
                ("b", AttributeValue::B(vec![0xde, 0xad, 0xbe, 0xef])),
                ("bool_true", AttributeValue::Bool(true)),
                ("null", AttributeValue::Null),
            ]),
            item(vec![
                ("pk", AttributeValue::S("item-2".to_owned())),
                ("ss", AttributeValue::SS(string_set(&["a", "b", "c"]))),
                (
                    "ns",
                    AttributeValue::NS(
                        ["-1", "0.5", "42"]
                            .iter()
                            .map(|s| (*s).to_owned())
                            .collect(),
                    ),
                ),
                (
                    "bs",
                    AttributeValue::BS(byte_set(&[b"one".as_slice(), b"two".as_slice()])),
                ),
                (
                    "l",
                    AttributeValue::L(vec![
                        AttributeValue::N("1".to_owned()),
                        AttributeValue::S("x".to_owned()),
                        AttributeValue::Bool(false),
                        AttributeValue::Null,
                        AttributeValue::L(vec![]),
                    ]),
                ),
                ("m_depth_32", nested_map(32)),
            ]),
        ]
    }

    #[test]
    fn encode_decode_round_trip_all_types() {
        let items = all_types_items();
        let (compressed, checksums) = encode_data_file(items.clone()).unwrap();
        assert_eq!(checksums.item_count, 2);
        assert_eq!(checksums.compressed_bytes, compressed.len() as u64);
        assert!(checksums.uncompressed_bytes > checksums.compressed_bytes / 4);

        let decoded = decode_data_file(&compressed, (&checksums).into()).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn exponent_form_numbers_decode_to_normalized_values() {
        // Exponent and shorthand forms are valid on the wire; the N
        // deserializer normalizes them, so a file holding them decodes to the
        // plain forms.
        let line = concat!(
            r#"{"Item":{"e_neg":{"N":"-2.5e-2"},"e_pos":{"N":"1.5E3"},"#,
            r#""pk":{"S":"exp"}}}"#,
            "\n",
        );
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(line.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let decoded = decode_data_file(&compressed, DataFileExpectations::default()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].get("e_pos"),
            Some(&AttributeValue::N("1500".to_owned()))
        );
        assert_eq!(
            decoded[0].get("e_neg"),
            Some(&AttributeValue::N("-0.025".to_owned()))
        );
    }

    #[test]
    fn line_encoding_matches_export_handler_shape() {
        // One item, then decompress and compare against the exact string the
        // engine's export handler would write for it.
        let one = item(vec![
            ("a", AttributeValue::N("1".to_owned())),
            ("pk", AttributeValue::S("x".to_owned())),
        ]);
        let (compressed, _) = encode_data_file(vec![one.clone()]).unwrap();

        let mut reader = BufReader::new(MultiGzDecoder::new(compressed.as_slice()));
        let mut text = String::new();
        std::io::Read::read_to_string(&mut reader, &mut text).unwrap();

        let wrapper = serde_json::json!({ "Item": one });
        let expected = format!("{}\n", serde_json::to_string(&wrapper).unwrap());
        assert_eq!(text, expected);
        assert_eq!(
            text,
            "{\"Item\":{\"a\":{\"N\":\"1\"},\"pk\":{\"S\":\"x\"}}}\n"
        );
    }

    #[test]
    fn empty_file_round_trip() {
        let (compressed, checksums) = encode_data_file(Vec::<Item>::new()).unwrap();
        assert_eq!(checksums.item_count, 0);
        assert_eq!(checksums.uncompressed_bytes, 0);
        let decoded = decode_data_file(&compressed, (&checksums).into()).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn tampered_compressed_bytes_fail_sha256() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let last = compressed.len() - 1;
        compressed[last] ^= 0xff;
        let err = decode_data_file(&compressed, (&checksums).into()).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "data file sha256",
                ..
            }
        ));
    }

    #[test]
    fn tampered_compressed_bytes_fail_md5_when_only_md5_given() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        compressed[0] ^= 0x01;
        let expectations = DataFileExpectations {
            md5_base64: Some(checksums.md5_base64),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "data file md5",
                ..
            }
        ));
    }

    #[test]
    fn wrong_item_count_is_detected() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let expectations = DataFileExpectations {
            item_count: Some(checksums.item_count + 1),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ItemCountMismatch {
                expected: 3,
                actual: 2
            }
        ));
    }

    #[test]
    fn wrong_uncompressed_byte_count_is_detected() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let expectations = DataFileExpectations {
            uncompressed_bytes: Some(checksums.uncompressed_bytes + 1),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(err, FormatError::ByteCountMismatch { .. }));
    }

    #[test]
    fn decoder_streams_via_iterator() {
        let items = all_types_items();
        let (compressed, checksums) = encode_data_file(items.clone()).unwrap();
        let decoder = DataFileDecoder::new(&compressed, (&checksums).into()).unwrap();
        let streamed: Result<Vec<Item>, FormatError> = decoder.collect();
        assert_eq!(streamed.unwrap(), items);
    }

    #[test]
    fn truncated_gzip_stream_is_an_error() {
        let (compressed, _) = encode_data_file(all_types_items()).unwrap();
        let truncated = &compressed[..compressed.len() / 2];
        let err = decode_data_file(truncated, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::Decode(_)));
    }

    #[test]
    fn malformed_line_is_an_error() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"{\"NotItem\":{}}\n").unwrap();
        let compressed = encoder.finish().unwrap();
        let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::Json(_)));
    }

    #[test]
    fn md5_checksum_encoding_matches_manifest_files_shape() {
        // 16 MD5 bytes encode to 24 base64 characters with padding, the
        // shape manifest-files.json carries.
        let (_, checksums) = encode_data_file(all_types_items()).unwrap();
        assert_eq!(checksums.md5_base64.len(), 24);
        assert!(checksums.md5_base64.ends_with("=="));
        assert_eq!(checksums.sha256_hex.len(), 64);
    }

    /// Read the process high-water resident set size in kibibytes from
    /// `/proc/self/status` (`VmHWM`).
    #[cfg(target_os = "linux")]
    fn peak_rss_kib() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let kib = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap();
                return kib;
            }
        }
        panic!("VmHWM not found in /proc/self/status");
    }

    #[test]
    fn single_oversized_line_hits_the_cap_without_materializing_it() {
        // A gzip whose one line is far larger than the cap: highly
        // compressible repeated bytes with no newline until the very end.
        // Uncompressed this is 64 MiB; compressed it is tiny. A decoder that
        // read the whole line into memory would allocate the full 64 MiB.
        let uncompressed_len = 64 * 1024 * 1024;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let block = vec![b'a'; 1024 * 1024];
        let mut written = 0usize;
        while written < uncompressed_len {
            encoder.write_all(&block).unwrap();
            written += block.len();
        }
        // No trailing newline: the whole thing is one line.
        let compressed = encoder.finish().unwrap();
        assert!(
            compressed.len() < 512 * 1024,
            "fixture should be small compressed, was {}",
            compressed.len()
        );

        #[cfg(target_os = "linux")]
        let before = peak_rss_kib();

        let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
        assert!(
            matches!(err, FormatError::LineTooLong { cap } if cap == MAX_LINE_BYTES),
            "expected LineTooLong, got {err:?}"
        );

        // On Linux, prove the decoder never materialized the 64 MiB line:
        // peak RSS growth across the call must stay well under it. The cap is
        // 4 MiB plus the reader buffer, so a generous ceiling of 32 MiB
        // growth still fails loudly if the whole line were buffered.
        #[cfg(target_os = "linux")]
        {
            let after = peak_rss_kib();
            let growth_kib = after.saturating_sub(before);
            println!(
                "bomb test peak RSS: before={before} KiB, after={after} KiB, growth={growth_kib} KiB"
            );
            assert!(
                growth_kib < 32 * 1024,
                "peak RSS grew {growth_kib} KiB decoding a 64 MiB bomb; \
                 the line was not bounded"
            );
        }
    }

    #[test]
    fn oversized_stream_is_rejected_before_full_read_when_byte_count_is_known() {
        // Many small lines whose running total will exceed a deliberately
        // low expectation; the decoder must stop mid-stream, not at EOF.
        let items: Vec<Item> = (0..1000)
            .map(|i| item(vec![("pk", AttributeValue::S(format!("item-{i}")))]))
            .collect();
        let (compressed, _) = encode_data_file(items).unwrap();
        let expectations = DataFileExpectations {
            uncompressed_bytes: Some(64),
            ..Default::default()
        };
        let mut decoder = DataFileDecoder::new(&compressed, expectations).unwrap();
        let mut err = None;
        // Pull items until the incremental byte-count check fires.
        loop {
            match decoder.next_item() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert!(
            matches!(
                err,
                Some(FormatError::ByteCountMismatch { expected: 64, .. })
            ),
            "expected a mid-stream ByteCountMismatch, got {err:?}"
        );
    }

    #[test]
    fn multi_member_gzip_decodes_all_members() {
        // Two independent gzip members concatenated: MultiGzDecoder must read
        // both, GzDecoder would stop after the first.
        let first = all_types_items();
        let second = vec![
            item(vec![("pk", AttributeValue::S("m2-a".to_owned()))]),
            item(vec![("pk", AttributeValue::S("m2-b".to_owned()))]),
        ];
        let (mut member_one, _) = encode_data_file(first.clone()).unwrap();
        let (member_two, _) = encode_data_file(second.clone()).unwrap();
        member_one.extend_from_slice(&member_two);

        let decoded = decode_data_file(&member_one, DataFileExpectations::default()).unwrap();
        let mut expected = first;
        expected.extend(second);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn line_exactly_at_cap_boundary_is_rejected() {
        // A single line whose length reaches the cap must error rather than
        // parse. Build a valid-ish long line of filler with no newline.
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let block = vec![b'x'; 1024 * 1024];
        let mut written = 0usize;
        while written <= MAX_LINE_BYTES {
            encoder.write_all(&block).unwrap();
            written += block.len();
        }
        let compressed = encoder.finish().unwrap();
        let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::LineTooLong { .. }));
    }
}
