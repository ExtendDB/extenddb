// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Generated requests from a fixed seed: each verifies, changes to its
//! unsigned headers still verify, and each change `mutate` makes to it is
//! refused.
//!
//! A generated request is a POST to `/` with no query string, signed for the
//! server's region (a request signed for another region is refused before
//! its signature is compared; `reject` checks those). Its header values are
//! visible ASCII words joined by runs of spaces, never empty, and each header
//! is sent once, except that the first unsigned header may be sent twice.

use std::cell::RefCell;

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::test_runner::{Config, RngAlgorithm, RngSeed, TestCaseError, TestRunner};

use crate::corpus::{self, ENDPOINTS, ITEM_TOKENS, SDK_HEADERS, TARGETS, UNSIGNED_HEADERS};
use crate::harness::{Key, SERVER_REGION, Shape, Tally};
use crate::mutate::{self, MARGIN, SKEW_WINDOW};

/// Requests generated per run.
const CASES: u32 = 256;
/// The seed every run starts from.
const SEED: u64 = 0x5167_7634_2026_1103;
/// The widest clock offset a generated client signs with, in seconds.
const MAX_OFFSET: i64 = SKEW_WINDOW - MARGIN;
/// Name spellings drawn per request, one per header line.
const SPELLINGS: usize = 12;

/// Whitespace written between JSON tokens.
const GAPS: &[&str] = &["", " ", "   ", "\t", "\n", "\r\n", " \t\n "];
const TABLES: &[&str] = &["Music", "t", "orders-2026", "A.b_c-9"];

fn word() -> impl Strategy<Value = String> {
    vec(0x21_u8..=0x7e, 1..10).prop_map(|bytes| String::from_utf8(bytes).expect("visible ASCII"))
}

/// Visible ASCII words joined by one to three spaces, with up to two spaces
/// before and after.
fn header_value() -> impl Strategy<Value = String> {
    (0_usize..3, vec((1_usize..4, word()), 1..4), 0_usize..3).prop_map(|(lead, words, trail)| {
        let mut value = " ".repeat(lead);
        for (index, (gap, word)) in words.iter().enumerate() {
            if index > 0 {
                value.push_str(&" ".repeat(*gap));
            }
            value.push_str(word);
        }
        value.push_str(&" ".repeat(trail));
        value
    })
}

/// A JSON string value: any characters, or one of the multi-byte samples.
fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        vec(any::<char>(), 0..16).prop_map(|chars| chars.into_iter().collect()),
        select(corpus::MULTIBYTE).prop_map(str::to_owned),
    ]
}

/// An empty body, or a PutItem object with generated whitespace between its
/// tokens.
fn body() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => Just(Vec::new()),
        6 => (vec(select(GAPS), ITEM_TOKENS + 1), select(TABLES), text())
            .prop_map(|(gaps, table, value)| corpus::spaced_item(&gaps, table, &value)),
    ]
}

fn shape() -> impl Strategy<Value = Shape> {
    let request = (
        prop_oneof![Just(Key::LongTerm), Just(Key::Session)],
        select(ENDPOINTS),
        any::<bool>(),
        Just(SERVER_REGION),
        select(TARGETS),
    );
    let signing = (any::<bool>(), any::<bool>(), -MAX_OFFSET..=MAX_OFFSET);
    let signed = (
        vec(any::<bool>(), SDK_HEADERS.len()),
        vec(header_value(), SDK_HEADERS.len()),
    );
    let unsigned = (
        vec(any::<bool>(), UNSIGNED_HEADERS.len()),
        vec(header_value(), UNSIGNED_HEADERS.len()),
        any::<bool>(),
    );
    let spelling = (vec(0_u8..4, SPELLINGS), 0_usize..3, 0_usize..3);
    (request, signing, signed, unsigned, spelling, body()).prop_map(
        |(
            (key, endpoint, explicit_host, region, target),
            (content_sha256, sdk_exclusions, clock_offset),
            (sign_mask, sign_values),
            (send_mask, send_values, repeat),
            (styles, lead, trail),
            body,
        )| {
            let mut shape = Shape::new(key, endpoint, region, target, body);
            shape.content_sha256 = content_sha256;
            shape.sdk_exclusions = sdk_exclusions;
            shape.clock_offset = clock_offset;
            for (name, value) in &mut shape.signed {
                if name != "content-length" {
                    *value = format!("{}{value}{}", " ".repeat(lead), " ".repeat(trail));
                }
            }
            if explicit_host {
                shape
                    .signed
                    .insert(0, ("host".to_owned(), endpoint.host.to_owned()));
            }
            for ((&(name, _), on), value) in SDK_HEADERS.iter().zip(sign_mask).zip(sign_values) {
                if on {
                    shape.signed.push((name.to_owned(), value));
                }
            }
            for ((&(name, _), on), value) in UNSIGNED_HEADERS.iter().zip(send_mask).zip(send_values)
            {
                if on {
                    shape.unsigned.push((name.to_owned(), value));
                }
            }
            if repeat && let Some((name, value)) = shape.unsigned.first().cloned() {
                shape
                    .unsigned
                    .push((name, format!("{} again", value.trim())));
            }
            for ((name, _), style) in shape
                .signed
                .iter_mut()
                .chain(shape.unsigned.iter_mut())
                .zip(styles.iter().cycle())
            {
                *name = corpus::spell(name, *style);
            }
            shape
        },
    )
}

#[test]
fn generated_requests_verify_and_every_change_is_refused() {
    let config = Config {
        cases: CASES,
        failure_persistence: None,
        rng_algorithm: RngAlgorithm::ChaCha,
        rng_seed: RngSeed::Fixed(SEED),
        ..Config::default()
    };
    let tally = RefCell::new(Tally::default());
    let mut runner = TestRunner::new(config);
    let result = runner.run(&shape(), |shape| {
        mutate::check_shape(
            &shape,
            &mut tally.borrow_mut(),
            "accepted: generated request",
        )
        .map_err(TestCaseError::fail)
    });
    tally
        .borrow()
        .report("generated_requests_verify_and_every_change_is_refused");
    if let Err(failure) = result {
        panic!("{failure}");
    }
}
