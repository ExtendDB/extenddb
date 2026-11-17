// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors BuiltinAuthProvider::authenticate in crates/auth/src/lib.rs. It reads
// the Authorization header from the request headers as a string, parses it
// with sigv4::parse::parse_authorization, and calls
// sigv4::verify::verify_signature with "POST", "/", an empty query string, the
// request headers and the request body. Here a fixed secret key takes the
// place of the credential store lookup. The timestamp window check is left
// out because it depends on the clock; the sigv4_timestamp target covers it.
//
// Input layout, with lines separated by '\n':
//   - The first line is the Authorization header value. The target also
//     stores it in the header map as authorization, as the server's map
//     holds it.
//   - Each following line, up to the first empty line, is one request header
//     written as name:value. Spaces and tabs around the value are trimmed.
//   - The bytes after the empty line are the request body.
// A line that is not a valid header name and value is skipped, as the HTTP
// layer would reject it before authentication runs.
//
// The seed in fuzz/seeds/sigv4_verify carries a valid signature for its own
// headers and body under this secret key, so it reaches the Ok(()) return of
// verify_signature.

#![no_main]

use libfuzzer_sys::fuzz_target;

use extenddb_auth::sigv4::parse::parse_authorization;
use extenddb_auth::sigv4::verify::verify_signature;

// The AWS documentation example secret key. It is not a real credential; it
// only has to be a fixed string so the derived signature is deterministic.
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

fuzz_target!(|data: &[u8]| {
    let (headers, body) = extenddb_fuzz::request_from_lines(data);
    // authenticate rejects a request whose Authorization header is missing or
    // is not a string before it parses anything.
    let Some(header) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
        return;
    };
    let Ok(parsed) = parse_authorization(header) else {
        return;
    };
    let _ = verify_signature(&parsed, SECRET_KEY, "POST", "/", "", &headers, body);
});
