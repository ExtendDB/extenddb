// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The signature covers the request's path and query string.

use axum::http::Uri;
use extenddb_core::error::DynamoDbError;

use crate::corpus;
use crate::harness::{
    Endpoint, Key, SIGNATURE_MISMATCH, Tally, authenticate_at, server_canonical_request_at, sign,
};

/// URIs a client signs. Each path and query string has a canonical form other
/// than that of `/`: empty, `.` and `..` segments, percent-encoded and
/// reserved characters in the path, and query strings out of order, with a
/// repeated name, a name without a value, an empty pair, `+` and encoded
/// characters.
const URIS: &[&str] = &[
    "https://localhost:18443/ping",
    "https://localhost:18443/other/path",
    "https://localhost:18443/?a=b",
    "https://localhost:18443/ping?x=1",
    "https://localhost:18443/a//b/./c/../d/",
    "https://localhost:18443/a/.",
    "https://localhost:18443/%7Euser/a%20b:c",
    "https://localhost:18443/?b=2&a=1&a=0&&c",
    "https://localhost:18443/?k=v%2Fw&s=a+b&t=%7E",
];

#[test]
fn the_signature_covers_the_path_and_the_query_string() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for &uri in URIS {
            let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
            shape.endpoint = Endpoint {
                uri,
                host: "localhost:18443",
            };
            let signed = sign(&shape);
            let target: Uri = uri.parse().expect("a URI");
            let (path, query) = (target.path(), target.query().unwrap_or(""));
            let server = server_canonical_request_at(path, query, &signed.wire);
            assert_eq!(server, signed.canonical_request, "{key:?} {uri}");
            authenticate_at(path, query, &signed.wire)
                .unwrap_or_else(|err| panic!("{key:?} {uri}: {err:?}"));
            tally.add("accepted at the signed path and query string");

            let other_query = if query.is_empty() { "x=1" } else { "" };
            for (other_path, other_query) in [("/", ""), (path, other_query)] {
                match authenticate_at(other_path, other_query, &signed.wire) {
                    Err(DynamoDbError::InvalidSignatureException(message))
                        if message.starts_with(SIGNATURE_MISMATCH) =>
                    {
                        tally.add("refused at another path or query string");
                    }
                    other => panic!("{key:?} {uri} sent to {other_path}?{other_query}: {other:?}"),
                }
            }
        }
    }
    tally.report("the_signature_covers_the_path_and_the_query_string");
}
