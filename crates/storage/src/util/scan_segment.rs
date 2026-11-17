// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which segment of a parallel Scan holds a partition key.

/// The segment of a `total_segments`-way parallel Scan that holds every item
/// of the partition key whose stored text is `pk`: CRC32 of the text modulo
/// the count. The SQLite and MongoDB backends call it from their scan
/// predicate and their start-key check.
#[must_use]
pub fn scan_segment(pk: &[u8], total_segments: i64) -> i64 {
    // The engine admits 1 to 1,000,000 segments. The clamp keeps the function
    // total, so a count of 0 reaching it from SQL cannot divide by zero.
    let total = u32::try_from(total_segments.max(1)).unwrap_or(u32::MAX);
    i64::from(crc32fast::hash(pk) % total)
}
