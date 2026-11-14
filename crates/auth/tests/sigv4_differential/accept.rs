// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Requests `aws-sigv4` signed the way DynamoDB clients send them verify, and
//! the server's canonical request for each is the one `aws-sigv4` signed.

use crate::corpus::{self, ENDPOINTS, REGIONS, SDK_HEADERS};
use crate::harness::{Key, SERVER_REGION, Shape, Tally, check_accepted, sign};
use crate::mutate::{MARGIN, SKEW_WINDOW};

fn accept(shape: &Shape, what: &str, tally: &mut Tally, category: &'static str) {
    if let Err(err) = check_accepted(&sign(shape)) {
        panic!("{what}: {err}");
    }
    tally.add(category);
}

/// Each client shape, with and without `x-amz-content-sha256`, for both keys
/// and every small body.
#[test]
fn client_shapes_verify() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (body_name, body) in corpus::small_bodies() {
            for (shape_name, shape) in corpus::client_shapes(key, &body) {
                for content_sha256 in [false, true] {
                    let mut shape = shape.clone();
                    shape.content_sha256 = content_sha256;
                    let what =
                        format!("{key:?}, {shape_name}, {body_name}, sha256 {content_sha256}");
                    accept(&shape, &what, &mut tally, "accepted: client shape");
                }
            }
        }
    }
    tally.report("client_shapes_verify");
}

/// A request signed for the server's region verifies. The server refuses a
/// request signed for any other region in `REGIONS`;
/// `reject::every_other_region_is_refused_by_the_region_check` checks those.
#[test]
fn the_servers_region_verifies() {
    assert!(
        REGIONS.contains(&SERVER_REGION),
        "the server's region is a region clients sign for"
    );
    let mut tally = Tally::default();
    for key in Key::ALL {
        let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
        SERVER_REGION.clone_into(&mut shape.region);
        accept(
            &shape,
            &format!("{key:?} in {SERVER_REGION}"),
            &mut tally,
            "accepted: the server's region",
        );
    }
    tally.report("the_servers_region_verifies");
}

/// Each endpoint, with Host taken from the URI and with Host set by the
/// client.
#[test]
fn every_endpoint_verifies() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for endpoint in ENDPOINTS {
            for explicit_host in [false, true] {
                let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
                shape.endpoint = *endpoint;
                if explicit_host {
                    shape = shape.with_signed("host", endpoint.host);
                }
                let what = format!("{key:?} to {}, host set {explicit_host}", endpoint.uri);
                accept(&shape, &what, &mut tally, "accepted: endpoint");
            }
        }
    }
    tally.report("every_endpoint_verifies");
}

/// Rewrites a header value's spacing.
type Respace = fn(&str) -> String;

/// Signed values with leading, trailing and repeated inner spaces.
#[test]
fn header_values_with_spaces_verify() {
    let spacings: [(&str, Respace); 4] = [
        ("leading spaces", |value| format!("   {value}")),
        ("trailing spaces", |value| format!("{value}  ")),
        ("repeated inner spaces", |value| value.replace(' ', "    ")),
        ("all three", |value| {
            format!("  {}   ", value.replace(' ', "  "))
        }),
    ];
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (spacing, respace) in spacings {
            let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
            for (name, value) in SDK_HEADERS {
                shape = shape.with_signed(name, value);
            }
            for (name, value) in &mut shape.signed {
                if name != "content-length" {
                    *value = respace(value);
                }
            }
            accept(
                &shape,
                &format!("{key:?}, {spacing}"),
                &mut tally,
                "accepted: spaced header values",
            );
        }
    }
    tally.report("header_values_with_spaces_verify");
}

/// Header names in Title-Case, upper case and alternating case, signed and
/// unsigned.
#[test]
fn mixed_case_header_names_verify() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for style in 1..4 {
            for (shape_name, shape) in corpus::client_shapes(key, corpus::PUT_ITEM) {
                let host = shape.endpoint.host;
                let mut shape = shape.with_signed("host", host);
                for (name, _) in shape.signed.iter_mut().chain(shape.unsigned.iter_mut()) {
                    *name = corpus::spell(name, style);
                }
                accept(
                    &shape,
                    &format!("{key:?}, {shape_name}, style {style}"),
                    &mut tally,
                    "accepted: mixed-case names",
                );
            }
        }
    }
    tally.report("mixed_case_header_names_verify");
}

/// Large bodies, with and without `x-amz-content-sha256`.
#[test]
fn large_bodies_verify() {
    let mut tally = Tally::default();
    for key in Key::ALL {
        for (body_name, body) in corpus::large_bodies() {
            for content_sha256 in [false, true] {
                let mut shape = corpus::base(key, body.clone());
                shape.content_sha256 = content_sha256;
                let what = format!("{key:?}, {body_name}, sha256 {content_sha256}");
                accept(&shape, &what, &mut tally, "accepted: large body");
            }
        }
    }
    tally.report("large_bodies_verify");
}

/// A client clock ahead of or behind the server's, inside the skew window.
#[test]
fn clock_offsets_inside_the_window_verify() {
    let edge = SKEW_WINDOW - MARGIN;
    let mut tally = Tally::default();
    for key in Key::ALL {
        for offset in [-edge, -60, 0, 60, edge] {
            let mut shape = corpus::base(key, corpus::PUT_ITEM.to_vec());
            shape.clock_offset = offset;
            accept(
                &shape,
                &format!("{key:?}, clock {offset} s"),
                &mut tally,
                "accepted: clock offset inside the window",
            );
        }
    }
    tally.report("clock_offsets_inside_the_window_verify");
}
