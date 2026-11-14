// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Decoding a deeply nested item costs no more memory than a flat one.
//!
//! The wire `AttributeValue` deserializer used to convert each `L` element
//! and `M` value with `serde_json::from_value(v.clone())`, so every enclosing
//! level held its own copy of the subtree while the level below it was
//! converted, and peak memory grew with the nesting depth. This binary
//! decodes an item nested to `DynamoDB`'s limit, one `PutItem` accepts at the
//! default limits, and bounds the decoder's peak heap by a multiple of the
//! line. [`DataFileDecoder`]'s docs state the bound and the figures behind
//! it.
//!
//! The binary holds one test and counts the heap with its own global
//! allocator, so nothing else in the process moves the peak. It measures
//! requested bytes, not resident memory, so the result does not depend on
//! the platform's allocator.
//!
//! [`DataFileDecoder`]: extenddb_core::backup_format::DataFileDecoder

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use extenddb_core::backup_format::{DataFileDecoder, encode_data_file};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, Item, item_size_bytes};
use extenddb_core::validation::validate_item_nesting_depth;

/// Counts live heap bytes and their high-water mark since the last reset.
struct CountingAllocator;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call is forwarded to `System` unchanged; the counters only
// observe the sizes.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc` is passed on as is.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc` is passed on as is.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// 30 wrappers, `M` and `L` in turn, around a list of empty strings: 31
/// containers, the maximum, and with the leaf strings the 32 levels
/// `DynamoDB` allows. Both kinds, so a copy in either arm of the
/// deserializer shows.
const DEPTH: usize = 31;
/// Empty strings in the innermost list, as many as keep the item within the
/// default 409,600-byte item limit.
const LEAVES: usize = 409_458;

/// Peak heap growth allowed while decoding, per line byte. Measured at 76
/// for this item with `serde_json`'s default `BTreeMap` (`cargo test -p
/// extenddb-core`, the default binary) and 51 when `preserve_order` is on
/// (CI's `cargo test --workspace`, where the MongoDB backend's `bson`
/// enables it and `indexmap` replaces the map), and about 1,200 when either
/// the `L` or the `M` arm copies its subtree again. The bound leaves about
/// 1.5x headroom over the larger figure for allocator-visible changes in
/// `serde_json` or in std's `BTreeMap` node layout, which is the dominant
/// term, and still fails by a factor of 10 on a copy in one arm.
const PEAK_PER_LINE_BYTE: usize = 120;

#[test]
fn nested_item_decodes_within_a_small_multiple_of_its_line() {
    let mut value = AttributeValue::L(vec![AttributeValue::S(String::new()); LEAVES]);
    for level in 1..DEPTH {
        value = if level % 2 == 1 {
            AttributeValue::M([("m".to_owned(), value)].into_iter().collect())
        } else {
            AttributeValue::L(vec![value])
        };
    }
    let item: Item = [
        ("pk".to_owned(), AttributeValue::S("p".to_owned())),
        ("l".to_owned(), value),
    ]
    .into_iter()
    .collect();
    // An item the request path accepts at the default limits, so the
    // encoder writes it into a backup; not a forged line.
    validate_item_nesting_depth(&item).expect("31 containers and the leaf strings are 32 levels");
    let size = item_size_bytes(&item);
    assert!(
        size <= LimitsConfig::default().max_item_size_bytes,
        "{size}"
    );
    let (compressed, checksums) = encode_data_file([item]).unwrap();
    let line_bytes = usize::try_from(checksums.uncompressed_bytes).unwrap();
    assert!(line_bytes > 3_600_000, "{line_bytes}");

    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let mut decoder = DataFileDecoder::new(&compressed[..], (&checksums).into());
    let restored = decoder.next_item().unwrap().unwrap();
    assert!(decoder.next_item().unwrap().is_none());
    drop(decoder);
    let peak = PEAK.load(Ordering::Relaxed) - base;

    let mut level = restored.get("l").unwrap();
    for _ in 1..DEPTH {
        level = match level {
            AttributeValue::L(list) if list.len() == 1 => &list[0],
            AttributeValue::M(map) if map.len() == 1 => &map["m"],
            other => panic!("expected a one-member L or M, got {other:?}"),
        };
    }
    assert!(matches!(level, AttributeValue::L(list) if list.len() == LEAVES));

    println!(
        "nested decode: depth {DEPTH}, {LEAVES} leaves, line {line_bytes} bytes, \
         peak heap growth {peak} bytes ({} per line byte)",
        peak / line_bytes
    );
    assert!(
        peak < PEAK_PER_LINE_BYTE * line_bytes,
        "decoding a {line_bytes}-byte line raised the heap by {peak} bytes, \
         more than {PEAK_PER_LINE_BYTE} per line byte: a nested value is copied per level"
    );
}
