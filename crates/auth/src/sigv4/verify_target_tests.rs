// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `x-amz-target` must be named in `SignedHeaders`.
//!
//! The server dispatches on `x-amz-target`, so a signature that does not
//! cover it would let the holder of one signed request send its body as
//! another operation.

use super::tests::{SECRET, SIGNED_BODY, assert_signature_mismatch, make_headers, sign};
use super::*;

/// The refusal for an unsigned `x-amz-target`: an `InvalidSignatureException`
/// whose message names `X-Amz-Target` and is not the signature mismatch. The
/// message wording itself is not pinned, because the service's wording for
/// this case has not been measured.
fn assert_target_not_signed(result: Result<(), DynamoDbError>, what: &str) {
    match result {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(msg.contains("X-Amz-Target"), "{what}: {msg}");
            assert!(!msg.contains("does not match"), "{what}: {msg}");
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

/// The name must match exactly. A name that only looks like it does not
/// count, whether it is longer, shorter or has a prefix; the bare name is the
/// control.
#[test]
fn only_the_exact_name_satisfies_the_check() {
    for (name, accepted) in [
        ("x-amz-targets", false),
        ("x-amz-targe", false),
        ("foo-x-amz-target", false),
        ("x-amz-target", true),
    ] {
        let headers = make_headers("20260415T120000Z");
        let signed = format!("content-type;host;x-amz-date;{name}");
        let parsed = sign(&headers, &signed, SIGNED_BODY);
        let result = verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY);
        if accepted {
            result.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        } else {
            assert_target_not_signed(result, name);
        }
    }
}

/// The check runs before the signature is compared. A request with a wrong
/// secret and an unsigned `x-amz-target` gets the `X-Amz-Target` refusal, not
/// the signature mismatch, so the refusal does not depend on the secret.
#[test]
fn unsigned_target_is_refused_before_the_signature_is_compared() {
    let headers = make_headers("20260415T120000Z");
    let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
    assert_target_not_signed(
        verify_signature(
            &parsed,
            "not-the-signing-secret",
            "POST",
            "/",
            "",
            &headers,
            SIGNED_BODY,
        ),
        "wrong secret",
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
