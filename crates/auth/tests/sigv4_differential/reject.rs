// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A request changed after `aws-sigv4` signed it, in a part the signature
//! covers, or signed for another region or another service, is refused.

use extenddb_auth::sigv4::{parse, verify};
use extenddb_core::error::DynamoDbError;

use crate::corpus::{self, REGIONS};
use crate::harness::{
    Key, Message, Refusal, SERVER_REGION, SIGNATURE_MISMATCH, Tally, WRONG_REGION, WRONG_SERVICE,
    check_accepted, check_refused, server_canonical_request, sign,
};
use crate::mutate;

/// Every change `mutate` makes, to each client shape, with and without
/// `x-amz-content-sha256`, for both keys.
#[test]
fn every_change_after_signing_is_refused() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (shape_name, shape) in corpus::client_shapes(key, corpus::PUT_ITEM) {
            for content_sha256 in [false, true] {
                let mut shape = shape.clone();
                shape.content_sha256 = content_sha256;
                if let Err(err) = mutate::check_shape(&shape, &mut tally, "accepted: client shape")
                {
                    panic!("{key:?}, {shape_name}, sha256 {content_sha256}: {err}");
                }
            }
        }
    }
    tally.report("every_change_after_signing_is_refused");
}

/// One body byte changed in every fixed body, with and without
/// `x-amz-content-sha256` carrying the hash of the body that was signed.
#[test]
fn one_body_byte_changed_is_refused() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (body_name, body) in corpus::small_bodies()
            .into_iter()
            .chain(corpus::large_bodies())
        {
            for content_sha256 in [false, true] {
                let mut shape = corpus::base(key, body.clone());
                shape.content_sha256 = content_sha256;
                let signed = sign(&shape);
                for mutant in mutate::body_mutants(&signed) {
                    if let Err(err) = check_refused(&mutant.wire, mutant.expected, mutant.message) {
                        panic!(
                            "{key:?}, {body_name}, sha256 {content_sha256}, {}: {err}",
                            mutant.label
                        );
                    }
                    tally.add(mutant.category);
                }
            }
        }
    }
    tally.report("one_body_byte_changed_is_refused");
}

/// `authenticate` hashes and verifies a 16 MiB body, and refuses it with one
/// byte changed. The HTTP layer enforces the server's body size limit before
/// `authenticate` runs, so this test does not check that limit.
#[test]
fn a_16_mib_body_is_hashed_and_verified_and_one_byte_changed_is_refused() {
    let mut shape = corpus::base(Key::LongTerm, corpus::sized_item(corpus::BODY_LIMIT, "a"));
    shape.content_sha256 = true;
    let signed = sign(&shape);
    check_accepted(&signed).unwrap();
    let mut changed = signed.wire.clone();
    let middle = changed.body.len() / 2;
    changed.body[middle] ^= 0x01;
    check_refused(
        &changed,
        Refusal::Signature,
        Message::Prefix(SIGNATURE_MISMATCH),
    )
    .unwrap();
    eprintln!(
        "a_16_mib_body_is_hashed_and_verified_and_one_byte_changed_is_refused: \
         accepted: 16 MiB body: 1, refused: one byte changed: 1"
    );
}

/// A request `aws-sigv4` signed for each region in `REGIONS` other than the
/// server's. Its signature matches the credential scope it carries, so the
/// region check is the only check that refuses it, and the refusal is the
/// whole `WRONG_REGION` message: the service names no region, and the scope's
/// service is right. `accept::the_servers_region_verifies` checks the
/// server's own region.
#[test]
fn every_other_region_is_refused_by_the_region_check() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for region in REGIONS.iter().filter(|region| **region != SERVER_REGION) {
            let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
            (*region).clone_into(&mut shape.region);
            let signed = sign(&shape);
            assert_eq!(
                server_canonical_request(&signed.wire),
                signed.canonical_request,
                "{key:?} in {region}: the server builds the canonical request aws-sigv4 signed"
            );
            if let Err(err) = check_refused(
                &signed.wire,
                Refusal::Signature,
                Message::Exact(WRONG_REGION),
            ) {
                panic!("{key:?} in {region}: {err}");
            }
            tally.add("refused: region");
        }
    }
    tally.report("every_other_region_is_refused_by_the_region_check");
}

/// A request `aws-sigv4` signed for another service. Its signature matches
/// the credential scope it carries, so the service check is the only check
/// that refuses it.
#[test]
fn signed_for_another_service_is_refused_by_the_service_check() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
        shape.service = "s3";
        let signed = sign(&shape);
        assert_eq!(
            server_canonical_request(&signed.wire),
            signed.canonical_request,
            "{key:?}: the server builds the canonical request aws-sigv4 signed"
        );
        if let Err(err) = check_refused(
            &signed.wire,
            Refusal::Signature,
            Message::Prefix(WRONG_SERVICE),
        ) {
            panic!("{key:?}, signed for s3: {err}");
        }
        tally.add("signed for another service");
    }
    tally.report("signed_for_another_service_is_refused_by_the_service_check");
}

/// The HTTP layer routes only POST requests for `/` to `authenticate`, which
/// passes POST, `/` and an empty query string to `verify_signature`. These
/// calls pass other values to `verify_signature` to show the signature covers
/// the method, the path and the query string.
#[test]
fn method_path_and_query_string_are_covered_by_the_signature() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (shape_name, shape) in corpus::client_shapes(key, corpus::PUT_ITEM) {
            let signed = sign(&shape);
            let header = signed.wire.value("authorization").unwrap();
            let parsed = parse::parse_authorization(header).unwrap();
            let headers = signed.wire.header_map();
            let verify = |method: &str, path: &str, query: &str| {
                verify::verify_signature(
                    &parsed,
                    key.secret(),
                    method,
                    path,
                    query,
                    &headers,
                    &signed.wire.body,
                )
            };
            verify("POST", "/", "").unwrap_or_else(|err| panic!("{shape_name}: {err:?}"));
            let changes = [
                ("method changed", "GET", "/", ""),
                ("method changed", "PUT", "/", ""),
                ("path changed", "POST", "/x", ""),
                ("path changed", "POST", "//", ""),
                ("query string changed", "POST", "/", "a=b"),
                ("query string changed", "POST", "/", "Action=ListTables"),
            ];
            for (category, method, path, query) in changes {
                match verify(method, path, query) {
                    Err(DynamoDbError::InvalidSignatureException(message))
                        if message.starts_with(SIGNATURE_MISMATCH) =>
                    {
                        tally.add(category);
                    }
                    other => panic!("{shape_name}, {method} {path}?{query}: {other:?}"),
                }
            }
        }
    }
    tally.report("method_path_and_query_string_are_covered_by_the_signature");
}
