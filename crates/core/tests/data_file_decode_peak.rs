// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Peak memory of decoding one line at the cap: a probe, ignored by default.
//!
//! The line cap bounds the bytes the decoder reads for one line, not the item
//! the line decodes to. This test builds a line of empty strings
//! (`{"S":""},`, 9 bytes per element) at exactly [`MAX_LINE_BYTES`], decodes
//! it, and prints the growth of the process high-water resident set
//! (`VmHWM`). It is its own binary with one test so nothing else in the
//! process moves the high-water mark, and it is ignored because it allocates
//! gigabytes. Run it with
//!
//! ```sh
//! cargo test -p extenddb-core --test data_file_decode_peak -- --ignored --nocapture
//! ```
//!
//! The decode-memory bound, the shapes measured and their figures are stated
//! once, in the docs of `DataFileDecoder`; this probe is one of their
//! sources. The non-ignored `data_file_nested_decode_memory` test holds the
//! bound for a nested item.

use std::io::Write;

use extenddb_core::backup_format::{
    DataFileExpectations, MAX_LINE_BYTES, decode_data_file, encode_item_line,
};
use extenddb_core::types::{AttributeValue, Item};
use flate2::Compression;
use flate2::write::GzEncoder;

#[cfg(target_os = "linux")]
fn peak_rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .expect("VmHWM in /proc/self/status")
}

#[test]
#[ignore = "allocates gigabytes; a measurement, run explicitly"]
fn decode_peak_of_a_line_of_empty_strings_at_the_cap() {
    const PREFIX: &[u8] = b"{\"Item\":{\"l\":{\"L\":[";
    const ELEMENT: &[u8] = b"{\"S\":\"\"}";
    const SUFFIX: &[u8] = b"]},\"pk\":{\"S\":\"p\"}}}\n";
    // PREFIX + n elements + (n - 1) commas + SUFFIX <= the cap.
    let n = (MAX_LINE_BYTES - PREFIX.len() - SUFFIX.len() + 1) / (ELEMENT.len() + 1);
    let mut line = Vec::with_capacity(MAX_LINE_BYTES);
    line.extend_from_slice(PREFIX);
    for i in 0..n {
        if i > 0 {
            line.push(b',');
        }
        line.extend_from_slice(ELEMENT);
    }
    line.extend_from_slice(SUFFIX);
    assert!(line.len() <= MAX_LINE_BYTES && line.len() + ELEMENT.len() + 1 > MAX_LINE_BYTES);

    // The line is the encoder's own encoding of that item (checked on a
    // short list, so the probe does not build the big item twice).
    let small: Item = [
        ("pk".to_owned(), AttributeValue::S("p".to_owned())),
        (
            "l".to_owned(),
            AttributeValue::L(vec![AttributeValue::S(String::new()); 2]),
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        encode_item_line(&small).unwrap(),
        b"{\"Item\":{\"l\":{\"L\":[{\"S\":\"\"},{\"S\":\"\"}]},\"pk\":{\"S\":\"p\"}}}\n"
    );

    let compressed = {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&line).unwrap();
        encoder.finish().unwrap()
    };
    let line_len = line.len();
    drop(line);

    #[cfg(target_os = "linux")]
    let before = peak_rss_kib();
    let items = decode_data_file(&compressed, DataFileExpectations::default()).unwrap();
    assert_eq!(items.len(), 1);
    assert!(matches!(items[0].get("l"), Some(AttributeValue::L(l)) if l.len() == n));
    #[cfg(target_os = "linux")]
    {
        let after = peak_rss_kib();
        let growth_kib = after.saturating_sub(before);
        #[allow(clippy::cast_precision_loss)]
        let per_line_byte = (growth_kib * 1024) as f64 / line_len as f64;
        println!(
            "decode peak: {n} empty strings, line {line_len} bytes, compressed {} bytes, \
             VmHWM before={before} KiB after={after} KiB growth={growth_kib} KiB \
             ({per_line_byte:.1} bytes per line byte)",
            compressed.len()
        );
    }
    drop(items);
}
