// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the server authentication entry point. In crates/server/src/handler.rs
// the handler calls state.auth.authenticate, which in crates/auth/src/lib.rs
// calls sigv4::parse::parse_authorization on the Authorization header string.
// parse_authorization caps the header at 8 KB itself, so no extra bound is
// applied here.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The header arrives as an HTTP header value, which is read as a string.
    if let Ok(header) = std::str::from_utf8(data) {
        let _ = extenddb_auth::sigv4::parse::parse_authorization(header);
    }
});
