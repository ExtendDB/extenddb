// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `x-amz-target` must be named in `SignedHeaders`.
//!
//! The server dispatches on `x-amz-target`, so a signature that does not
//! cover it would let the holder of one signed request send its body as
//! another operation.

use super::tests::{SECRET, SIGNED_BODY, assert_signature_mismatch, make_headers, sign};
use super::*;

fn assert_target_not_signed(result: Result<(), DynamoDbError>, what: &str) {
    match result {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert_eq!(msg, TARGET_NOT_SIGNED, "{what}");
        }
        other => panic!("{what}: expected InvalidSignatureException, got: {other:?}"),
    }
}

/// A request signed correctly over every header but `x-amz-target` is
/// refused, for the DynamoDB and the DynamoDB Streams prefixes.
#[test]
fn reject_target_left_out_of_signed_headers() {
    for target in [
        "DynamoDB_20120810.GetItem",
        "DynamoDBStreams_20120810.GetRecords",
    ] {
        let mut headers = make_headers("20260415T120000Z");
        headers.insert("x-amz-target", target.parse().unwrap());
        let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
        assert_target_not_signed(
            verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY),
            target,
        );
    }
}

/// A GetItem signed without `x-amz-target`, sent as DeleteItem with the
/// same body and signature, is refused.
#[test]
fn reject_operation_changed_when_target_was_not_signed() {
    let mut headers = make_headers("20260415T120000Z");
    let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
    headers.insert(
        "x-amz-target",
        "DynamoDB_20120810.DeleteItem".parse().unwrap(),
    );
    assert_target_not_signed(
        verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY),
        "GetItem sent as DeleteItem",
    );
}

/// The name must match exactly: a longer name that starts with
/// `x-amz-target` does not count.
#[test]
fn reject_signed_headers_naming_only_a_longer_name() {
    let headers = make_headers("20260415T120000Z");
    let parsed = sign(
        &headers,
        "content-type;host;x-amz-date;x-amz-targets",
        SIGNED_BODY,
    );
    assert_target_not_signed(
        verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY),
        "x-amz-targets",
    );
}

/// With `x-amz-target` signed, the request verifies, and changing the
/// operation after signing is a plain signature mismatch.
#[test]
fn signed_target_changed_after_signing_is_a_signature_mismatch() {
    let mut headers = make_headers("20260415T120000Z");
    let parsed = sign(
        &headers,
        "content-type;host;x-amz-date;x-amz-target",
        SIGNED_BODY,
    );
    verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY).unwrap();
    headers.insert(
        "x-amz-target",
        "DynamoDB_20120810.DeleteItem".parse().unwrap(),
    );
    assert_signature_mismatch(verify_signature(
        &parsed,
        SECRET,
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    ));
}

/// A mixed-case `X-Amz-Target` in `SignedHeaders` counts as signed, and the
/// header is still covered by the signature: changing the operation after
/// signing is a signature mismatch, not a bypass.
#[test]
fn mixed_case_signed_target_is_accepted_and_covered_by_the_signature() {
    let mut headers = make_headers("20260415T120000Z");
    let parsed = sign(
        &headers,
        "content-type;host;x-amz-date;X-Amz-Target",
        SIGNED_BODY,
    );
    verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY).unwrap();
    headers.insert(
        "x-amz-target",
        "DynamoDB_20120810.DeleteItem".parse().unwrap(),
    );
    assert_signature_mismatch(verify_signature(
        &parsed,
        SECRET,
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    ));
}

/// The `Host` check still runs first: a list that names neither header
/// gets the `Host` refusal, as before this check existed.
#[test]
fn missing_host_is_reported_before_missing_target() {
    let headers = make_headers("20260415T120000Z");
    let parsed = sign(&headers, "content-type;x-amz-date", SIGNED_BODY);
    match verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY) {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(msg.starts_with("\"Host\""), "{msg}");
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}
