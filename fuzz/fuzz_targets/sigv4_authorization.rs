// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the server authentication entry point. In crates/server/src/handler.rs
// the handler calls state.auth.authenticate, which in crates/auth/src/lib.rs
// calls sigv4::parse::parse_authorization on the bytes of the Authorization
// header. parse_authorization caps the header at 8 KB itself, so no extra bound
// is applied here.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The header arrives as the bytes of an HTTP header value, in a request
    // with or without a date header.
    for has_date in [false, true] {
        let _ = extenddb_auth::sigv4::parse::parse_authorization(data, has_date);
    }
});
