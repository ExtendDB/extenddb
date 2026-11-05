// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the ExtendDB parser fuzz targets.
//!
//! These turn fuzzer bytes into the structured inputs the parsers and
//! evaluators take: HTTP header maps, a request split into headers and body,
//! items, and expression maps. They live here, outside the product crates, so
//! the six fuzz targets can share them. Nothing here changes product behavior.
//!
//! Byte consumption is intentionally simple and front to back: a single byte
//! gives a small count or selector, then that many sub values are read. At end
//! of input `arbitrary` yields zero bytes, so every count collapses to zero and
//! every generator terminates.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arbitrary::{Arbitrary, Unstructured};
use http::{HeaderMap, HeaderName, HeaderValue};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_core::validation::number::validate_and_normalize_number;

/// Maximum attribute-value nesting a generator produces. The server accepts up
/// to 32 levels. A lower cap keeps each generated input small while still
/// exercising nested list and map walks.
const MAX_DEPTH: u8 = 5;

/// One byte from the fuzzer. At end of input `arbitrary` returns zero.
fn byte(u: &mut Unstructured) -> u8 {
    u8::arbitrary(u).unwrap_or(0)
}

/// Up to 31 raw bytes, length chosen by one control byte.
fn bytes_vec(u: &mut Unstructured) -> Vec<u8> {
    let len = (byte(u) & 0x1f) as usize;
    u.bytes(len).map(<[u8]>::to_vec).unwrap_or_default()
}

/// A short string built from raw bytes, lossily decoded.
fn arb_string(u: &mut Unstructured) -> String {
    String::from_utf8_lossy(&bytes_vec(u)).into_owned()
}

const IDENT_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";

/// A 1 to 8 character identifier made of expression-name characters, so a
/// placeholder or name reference the fuzzer puts in an expression has a chance
/// of resolving against the generated maps.
fn arb_ident(u: &mut Unstructured) -> String {
    let len = (byte(u) & 0x7) as usize + 1;
    let raw = u.bytes(len).map(<[u8]>::to_vec).unwrap_or_default();
    if raw.is_empty() {
        return "a".to_owned();
    }
    raw.iter()
        .map(|b| IDENT_CHARS[(*b as usize) % IDENT_CHARS.len()] as char)
        .collect()
}

/// Generate a number string, either a valid integer or arbitrary text, and
/// pass it through `validate_and_normalize_number`. Returns the normalized
/// form, or `None` when that function refuses the string.
/// fuzz_targets/expression_evaluate.rs describes the filter.
fn arb_number(u: &mut Unstructured) -> Option<String> {
    let raw = if byte(u) & 1 == 0 {
        i64::arbitrary(u).unwrap_or(0).to_string()
    } else {
        arb_string(u)
    };
    validate_and_normalize_number(&raw).ok()
}

fn arb_string_set(u: &mut Unstructured) -> BTreeSet<String> {
    let n = (byte(u) & 0x3) as usize + 1;
    (0..n).map(|_| arb_string(u)).collect()
}

/// A number set made of the numbers `arb_number` keeps, or `None` when it
/// keeps none, because a set may not be empty.
fn arb_number_set(u: &mut Unstructured) -> Option<BTreeSet<String>> {
    let n = (byte(u) & 0x3) as usize + 1;
    let set: BTreeSet<String> = (0..n).filter_map(|_| arb_number(u)).collect();
    (!set.is_empty()).then_some(set)
}

fn arb_binary_set(u: &mut Unstructured) -> BTreeSet<Vec<u8>> {
    let n = (byte(u) & 0x3) as usize + 1;
    (0..n).map(|_| bytes_vec(u)).collect()
}

fn arb_list(u: &mut Unstructured, depth: u8) -> Vec<AttributeValue> {
    let n = (byte(u) & 0x3) as usize;
    (0..n)
        .filter_map(|_| arb_attribute_value(u, depth + 1))
        .collect()
}

fn arb_value_map(u: &mut Unstructured, depth: u8) -> BTreeMap<String, AttributeValue> {
    let n = (byte(u) & 0x3) as usize;
    (0..n)
        .filter_map(|_| {
            let name = arb_string(u);
            arb_attribute_value(u, depth + 1).map(|value| (name, value))
        })
        .collect()
}

/// Build one `AttributeValue`, or `None` for a number or number set that
/// `arb_number` drops. A list or map leaves out the members that are `None`.
/// Recursive variants (`L`, `M`) are only offered below the depth cap. Set
/// variants are always non-empty, which is what the wire deserializer would
/// accept.
pub fn arb_attribute_value(u: &mut Unstructured, depth: u8) -> Option<AttributeValue> {
    let variants: u8 = if depth < MAX_DEPTH { 10 } else { 8 };
    let value = match byte(u) % variants {
        0 => AttributeValue::S(arb_string(u)),
        1 => AttributeValue::N(arb_number(u)?),
        2 => AttributeValue::B(bytes_vec(u)),
        3 => AttributeValue::Bool(byte(u) & 1 == 1),
        4 => AttributeValue::Null,
        5 => AttributeValue::SS(arb_string_set(u)),
        6 => AttributeValue::NS(arb_number_set(u)?),
        7 => AttributeValue::BS(arb_binary_set(u)),
        8 => AttributeValue::L(arb_list(u, depth)),
        _ => AttributeValue::M(arb_value_map(u, depth)),
    };
    Some(value)
}

/// Build an item of up to seven attributes. An attribute whose value is `None`
/// is left out.
pub fn arb_item(u: &mut Unstructured) -> Item {
    let n = (byte(u) & 0x7) as usize;
    (0..n)
        .filter_map(|_| {
            let name = arb_string(u);
            arb_attribute_value(u, 0).map(|value| (name, value))
        })
        .collect()
}

/// Build the resolved `ExpressionAttributeNames` and `ExpressionAttributeValues`
/// maps. Keys are bare (no `#` or `:` prefix), matching what the engine stores.
/// A value that is `None` is left out.
pub fn arb_expression_maps(u: &mut Unstructured) -> ExpressionMaps {
    let name_count = (byte(u) & 0x7) as usize;
    let mut names = HashMap::new();
    for _ in 0..name_count {
        names.insert(arb_ident(u), arb_string(u));
    }
    let value_count = (byte(u) & 0x7) as usize;
    let mut values = HashMap::new();
    for _ in 0..value_count {
        let key = arb_ident(u);
        if let Some(value) = arb_attribute_value(u, 0) {
            values.insert(key, value);
        }
    }
    ExpressionMaps::new(names, values)
}

/// Build a header map of up to three arbitrary name and value pairs. Bytes that
/// are not a valid header name or value are skipped, as the HTTP layer would
/// reject them before these parsers ever run.
pub fn build_header_map(u: &mut Unstructured) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let count = (byte(u) & 0x3) as usize;
    for _ in 0..count {
        let name = bytes_vec(u);
        let value = bytes_vec(u);
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(&name),
            HeaderValue::from_bytes(&value),
        ) {
            headers.insert(n, v);
        }
    }
    headers
}

/// Set one fixed-name header from raw bytes, skipping bytes that are not a valid
/// header value.
pub fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &[u8]) {
    if let Ok(v) = HeaderValue::from_bytes(value) {
        headers.insert(HeaderName::from_static(name), v);
    }
}

/// Split a `sigv4_verify` input into request headers and a body, using the
/// line layout described in fuzz_targets/sigv4_verify.rs. The first line is
/// stored as the `authorization` header. Each following line up to the first
/// empty line is one `name:value` header, appended in order. A line that is
/// not a valid header name and value is skipped, as the HTTP layer would
/// reject it. The bytes after the empty line are the body.
/// The most request headers hyper's HTTP/1 parser accepts by default.
const MAX_HEADERS: usize = 100;

pub fn request_from_lines(data: &[u8]) -> (HeaderMap, &[u8]) {
    let (head, body) = match data.windows(2).position(|pair| pair == b"\n\n") {
        Some(end) => (&data[..end], &data[end + 2..]),
        None => (data, &data[data.len()..]),
    };
    let mut lines = head.split(|b| *b == b'\n');
    let mut headers = HeaderMap::new();
    if let Some(authorization) = lines.next() {
        insert_header(&mut headers, "authorization", authorization);
    }
    // hyper parses at most MAX_HEADERS request headers, and the server keeps
    // that default, so lines past the limit are ignored.
    for line in lines.take(MAX_HEADERS - 1) {
        let Some(colon) = line.iter().position(|b| *b == b':') else {
            continue;
        };
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(&line[..colon]),
            HeaderValue::from_bytes(trim_ows(&line[colon + 1..])),
        ) {
            headers.append(name, value);
        }
    }
    (headers, body)
}

/// Strip the spaces and tabs HTTP allows around a header value.
fn trim_ows(mut value: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = value {
        value = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = value {
        value = rest;
    }
    value
}
