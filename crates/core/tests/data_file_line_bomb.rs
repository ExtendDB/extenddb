// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data file decoder refuses an over-long line without materializing it.
//!
//! This binary holds one test on purpose: the check reads the process-wide
//! high-water resident set (`VmHWM`), which only rises, so any other test in
//! the same process that allocated more first would hide a decoder that
//! buffers the whole line. Keep it alone.

use std::io::Write;

use extenddb_core::backup_format::{
    DataFileExpectations, FormatError, GZIP_LEVEL, MAX_LINE_BYTES, decode_data_file,
};
use flate2::Compression;
use flate2::write::GzEncoder;

/// The process high-water resident set size in kibibytes (`VmHWM` in
/// `/proc/self/status`).
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
fn single_oversized_line_hits_the_cap_without_materializing_it() {
    // One line of 256 MiB with no newline: eight times the cap. Built as 256
    // gzip members of 1 MiB each (`MultiGzDecoder` reads them as one stream,
    // and the line runs on across members), so the fixture costs one 1 MiB
    // compression and stays small.
    const MIB: usize = 1024 * 1024;
    let member = {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(GZIP_LEVEL));
        encoder.write_all(&vec![b'a'; MIB]).unwrap();
        encoder.finish().unwrap()
    };
    let compressed = member.repeat(256);
    let uncompressed = 256 * MIB;
    assert!(uncompressed >= 8 * MAX_LINE_BYTES);
    assert!(
        compressed.len() < 2 * MIB,
        "fixture should be small compressed, was {}",
        compressed.len()
    );
    drop(member);

    #[cfg(target_os = "linux")]
    let before = peak_rss_kib();

    let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
    assert!(
        matches!(err, FormatError::LineTooLong { cap } if cap == MAX_LINE_BYTES),
        "expected LineTooLong, got {err:?}"
    );

    // The decoder holds at most the capped line plus fixed buffers. A
    // decoder that read the whole line before checking it would hold 256 MiB
    // (and up to twice that while its buffer grows), so a ceiling of twice
    // the cap separates the two with room on both sides.
    #[cfg(target_os = "linux")]
    {
        let after = peak_rss_kib();
        let growth_kib = after.saturating_sub(before);
        println!(
            "bomb test peak RSS: before={before} KiB, after={after} KiB, growth={growth_kib} KiB, \
             cap={} KiB",
            MAX_LINE_BYTES / 1024
        );
        let ceiling_kib = u64::try_from(2 * MAX_LINE_BYTES / 1024).unwrap();
        assert!(
            growth_kib < ceiling_kib,
            "peak RSS grew {growth_kib} KiB decoding a {} MiB line; the line was not bounded",
            uncompressed / MIB
        );
    }
}
