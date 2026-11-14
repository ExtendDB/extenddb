// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the server authentication path. In crates/auth/src/lib.rs
// authenticate calls sigv4::verify::validate_timestamp on the request headers,
// which reads the x-amz-date header and parses an ISO-8601 basic timestamp. The
// handler feeds the request's own headers. Here the x-amz-date value is chosen
// by the fuzzer; a few other arbitrary headers are added, which the parser
// ignores.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let mut headers = extenddb_fuzz::build_header_map(&mut u);
    let date = u.take_rest();
    extenddb_fuzz::insert_header(&mut headers, "x-amz-date", date);
    let _ = extenddb_auth::sigv4::verify::validate_timestamp(&headers);
});
