// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the server authentication path. In crates/auth/src/lib.rs
// authenticate calls sigv4::verify::validate_timestamp on the request headers,
// which reads the x-amz-date header and parses an ISO-8601 basic timestamp. It
// reads no other header, so the whole input is the x-amz-date value. An input
// that is not a valid header value leaves the header out, as the HTTP layer
// would reject it before authentication runs.

#![no_main]

use http::HeaderMap;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut headers = HeaderMap::new();
    extenddb_fuzz::insert_header(&mut headers, "x-amz-date", data);
    let _ = extenddb_auth::sigv4::verify::validate_timestamp(&headers);
});
