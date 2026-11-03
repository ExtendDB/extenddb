// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Changes made to a request after `aws-sigv4` signed it, each with the
//! refusal it must get, and changes the signature does not cover.

use std::time::SystemTime;

use crate::harness::{
    Authorization, EXPIRED, INVALID_TOKEN, Key, MISSING_DATE, OTHER_TOKEN, Refusal,
    SCOPE_DATE_MISMATCH, SIGNATURE_MISMATCH, Shape, Signed, Tally, WRONG_SERVICE, Wire, amz_date,
    check_accepted, check_accepted_as, check_refused, shift, sign,
};

/// Seconds the server allows between `X-Amz-Date` and its clock.
pub const SKEW_WINDOW: i64 = 15 * 60;
/// Seconds kept between a case and the window's edge, so the time a slow run
/// spends between signing and checking cannot move a case across it.
pub const MARGIN: i64 = 60;

const DAY: i64 = 24 * 60 * 60;

/// A request changed after signing and the refusal it must get.
pub struct Mutant {
    pub category: &'static str,
    pub label: String,
    pub wire: Wire,
    pub expected: Refusal,
    /// The start of the refusal message, which names the check that must
    /// refuse the request.
    pub message: &'static str,
}

fn changed(wire: &Wire, change: impl FnOnce(&mut Wire)) -> Wire {
    let mut wire = wire.clone();
    change(&mut wire);
    wire
}

/// `value` with its first character other than a space replaced.
fn first_character_replaced(value: &str) -> String {
    let (index, current) = value
        .char_indices()
        .find(|(_, c)| *c != ' ')
        .expect("a signed value has a character other than a space");
    let replacement = if current == 'a' { 'b' } else { 'a' };
    format!(
        "{}{replacement}{}",
        &value[..index],
        &value[index + current.len_utf8()..]
    )
}

/// Changes to `signed` after signing: each signed header's value changed and
/// each signed header dropped, one more name in `SignedHeaders`, one body
/// byte changed, `X-Amz-Date` moved, the credential scope's date, region and
/// service changed, and the signature truncated, extended, upper-cased or
/// with one digit changed.
pub fn mutants(signed: &Signed) -> Vec<Mutant> {
    let mut out = Vec::new();
    header_mutants(signed, &mut out);
    signed_headers_mutants(signed, &mut out);
    out.extend(body_mutants(signed));
    date_mutants(signed, &mut out);
    scope_mutants(signed, &mut out);
    signature_mutants(signed, &mut out);
    out
}

fn header_mutants(signed: &Signed, out: &mut Vec<Mutant>) {
    let wire = &signed.wire;
    for name in wire.authorization().signed_headers {
        let value = wire
            .value(&name)
            .unwrap_or_else(|| panic!("the signed header {name} is sent"))
            .to_owned();
        let token = name == "x-amz-security-token";
        // X-Amz-Date is moved by whole seconds in `date_mutants` instead.
        if name != "x-amz-date" {
            let category = if token {
                "session token changed after signing"
            } else {
                "signed header value changed"
            };
            out.push(Mutant {
                category,
                label: format!("{name}: a character appended"),
                wire: changed(wire, |wire| wire.replace(&name, format!("{value}x"))),
                expected: Refusal::Signature,
                message: SIGNATURE_MISMATCH,
            });
            out.push(Mutant {
                category,
                label: format!("{name}: the first character replaced"),
                wire: changed(wire, |wire| {
                    wire.replace(&name, first_character_replaced(&value));
                }),
                expected: Refusal::Signature,
                message: SIGNATURE_MISMATCH,
            });
        }
        let (category, expected, message) = match name.as_str() {
            "x-amz-date" => ("signed header dropped", Refusal::Incomplete, MISSING_DATE),
            "x-amz-security-token" => (
                "session token removed after signing",
                Refusal::Signature,
                SIGNATURE_MISMATCH,
            ),
            _ => (
                "signed header dropped",
                Refusal::Signature,
                SIGNATURE_MISMATCH,
            ),
        };
        out.push(Mutant {
            category,
            label: format!("{name}: dropped"),
            wire: changed(wire, |wire| wire.remove(&name)),
            expected,
            message,
        });
    }
}

/// `SignedHeaders` names one more header: one the request does not carry, and
/// each one the request carries unsigned.
fn signed_headers_mutants(signed: &Signed, out: &mut Vec<Mutant>) {
    let wire = &signed.wire;
    let authorization = wire.authorization();
    let mut extras = vec!["x-amz-not-sent".to_owned()];
    extras.extend(wire.unsigned_names());
    for extra in extras {
        let mut listed = authorization.clone();
        listed.signed_headers.push(extra.clone());
        listed.signed_headers.sort();
        out.push(Mutant {
            category: "extra name in SignedHeaders",
            label: format!("{extra} listed in SignedHeaders"),
            wire: changed(wire, |wire| wire.set_authorization(&listed)),
            expected: Refusal::Signature,
            message: SIGNATURE_MISMATCH,
        });
    }
}

/// One body byte changed (the first, the middle and the last), or one byte
/// added to an empty body. When the request carries `x-amz-content-sha256`
/// it still names the hash of the body that was signed.
pub fn body_mutants(signed: &Signed) -> Vec<Mutant> {
    let wire = &signed.wire;
    let category = if wire.value("x-amz-content-sha256").is_some() {
        "body byte changed, x-amz-content-sha256 kept"
    } else {
        "body byte changed"
    };
    let length = wire.body.len();
    if length == 0 {
        return vec![Mutant {
            category,
            label: "a byte added to the empty body".to_owned(),
            wire: changed(wire, |wire| wire.body.push(b'{')),
            expected: Refusal::Signature,
            message: SIGNATURE_MISMATCH,
        }];
    }
    let mut positions = vec![0, length / 2, length - 1];
    positions.dedup();
    positions
        .into_iter()
        .map(|position| Mutant {
            category,
            label: format!("body byte {position} of {length} changed"),
            wire: changed(wire, |wire| wire.body[position] ^= 0x01),
            expected: Refusal::Signature,
            message: SIGNATURE_MISMATCH,
        })
        .collect()
}

/// `X-Amz-Date` moved one second either way, and moved past the skew window
/// either way from the server's clock.
fn date_mutants(signed: &Signed, out: &mut Vec<Mutant>) {
    let scope_date = signed.wire.authorization().date;
    for seconds in [-1, 1] {
        let date = amz_date(shift(signed.time, seconds));
        // Moved across midnight UTC, X-Amz-Date is on a different day from
        // the credential scope, and the scope date check refuses the request
        // before the signature is compared.
        let message = if date[..8] == scope_date {
            SIGNATURE_MISMATCH
        } else {
            SCOPE_DATE_MISMATCH
        };
        out.push(Mutant {
            category: "x-amz-date moved one second",
            label: format!("x-amz-date moved {seconds} s to {date}"),
            wire: changed(&signed.wire, |wire| {
                wire.replace("x-amz-date", date.clone())
            }),
            expected: Refusal::Signature,
            message,
        });
    }
    let now = SystemTime::now();
    for seconds in [-(SKEW_WINDOW + MARGIN), SKEW_WINDOW + MARGIN] {
        let date = amz_date(shift(now, seconds));
        out.push(Mutant {
            category: "x-amz-date moved past the skew window",
            label: format!("x-amz-date set {seconds} s from now, to {date}"),
            wire: changed(&signed.wire, |wire| {
                wire.replace("x-amz-date", date.clone())
            }),
            expected: Refusal::Client,
            message: EXPIRED,
        });
    }
}

/// `signed` with its `Authorization` value changed by `change`, refused with
/// `InvalidSignatureException` and a message starting with `message`.
fn authorization_mutant(
    signed: &Signed,
    category: &'static str,
    label: String,
    message: &'static str,
    change: impl FnOnce(&mut Authorization),
) -> Mutant {
    let mut parts = signed.wire.authorization();
    change(&mut parts);
    Mutant {
        category,
        label,
        wire: changed(&signed.wire, |wire| wire.set_authorization(&parts)),
        expected: Refusal::Signature,
        message,
    }
}

/// The credential scope's date, region or service changed.
///
/// The server checks the scope's service and date before it compares the
/// signature. It has no check of its own for the region, so a changed region
/// is refused by the signature. `aws-sigv4` takes the scope date and
/// `X-Amz-Date` from one time, so a changed scope date also breaks the
/// signature, and the scope date check refuses it first.
fn scope_mutants(signed: &Signed, out: &mut Vec<Mutant>) {
    for days in [-1, 1] {
        let date = amz_date(shift(signed.time, days * DAY))[..8].to_owned();
        out.push(authorization_mutant(
            signed,
            "credential scope date changed, refused by the scope date check",
            format!("scope date {date}"),
            SCOPE_DATE_MISMATCH,
            |parts| parts.date = date,
        ));
    }
    let region = if signed.wire.authorization().region == "us-west-2" {
        "us-east-1"
    } else {
        "us-west-2"
    };
    out.push(authorization_mutant(
        signed,
        "credential scope region changed, refused by the signature",
        format!("scope region {region}"),
        SIGNATURE_MISMATCH,
        |parts| region.clone_into(&mut parts.region),
    ));
    for service in ["s3", "streams.dynamodb", "DynamoDB"] {
        out.push(authorization_mutant(
            signed,
            "credential scope service changed",
            format!("scope service {service}"),
            WRONG_SERVICE,
            |parts| service.clone_into(&mut parts.service),
        ));
    }
}

/// The signature truncated, extended, upper-cased or with one digit changed.
fn signature_mutants(signed: &Signed, out: &mut Vec<Mutant>) {
    let authorization = signed.wire.authorization();
    let signature = &authorization.signature;
    let last = signature.chars().last().expect("a signature");
    let mut variants = vec![
        (
            "signature truncated",
            signature[..signature.len() - 1].to_owned(),
        ),
        ("signature extended", format!("{signature}0")),
        (
            "signature digit changed",
            format!(
                "{}{}",
                &signature[..signature.len() - 1],
                if last == '0' { '1' } else { '0' }
            ),
        ),
    ];
    let upper = signature.to_ascii_uppercase();
    // A signature of digits alone reads the same upper-cased.
    if upper != *signature {
        variants.push(("signature upper-cased", upper));
    }
    for (category, changed_signature) in variants {
        out.push(authorization_mutant(
            signed,
            category,
            format!("signature {changed_signature}"),
            SIGNATURE_MISMATCH,
            |parts| parts.signature = changed_signature,
        ));
    }
}

/// `shape` signed with credentials the store does not hold for its key, or
/// at a time outside the skew window.
pub fn resigned(shape: &Shape) -> Vec<Mutant> {
    let mut variants = Vec::new();
    let mut other_secret = shape.clone();
    other_secret.signing_secret = shape.key.other_secret().to_owned();
    variants.push((
        "wrong secret",
        "signed with another secret",
        other_secret,
        Refusal::Signature,
        SIGNATURE_MISMATCH,
    ));
    for offset in [-(SKEW_WINDOW + MARGIN), SKEW_WINDOW + MARGIN] {
        let mut outside = shape.clone();
        outside.clock_offset = offset;
        variants.push((
            "signed outside the skew window",
            "signed with a clock outside the window",
            outside,
            Refusal::Client,
            EXPIRED,
        ));
    }
    if shape.key == Key::Session {
        let mut other_token = shape.clone();
        other_token.signing_token = Some(OTHER_TOKEN.to_owned());
        variants.push((
            "session token changed before signing",
            "signed and sent with another token",
            other_token,
            Refusal::Client,
            INVALID_TOKEN,
        ));
        let mut no_token = shape.clone();
        no_token.signing_token = None;
        variants.push((
            "session token removed before signing",
            "signed and sent without the token",
            no_token,
            Refusal::Client,
            INVALID_TOKEN,
        ));
    }
    variants
        .into_iter()
        .map(|(category, label, shape, expected, message)| Mutant {
            category,
            label: label.to_owned(),
            wire: sign(&shape).wire,
            expected,
            message,
        })
        .collect()
}

/// Changes to headers the signature does not cover: each unsigned header
/// changed and dropped, and one header added.
pub fn unsigned_changes(signed: &Signed) -> Vec<(&'static str, Wire)> {
    let wire = &signed.wire;
    let mut out = Vec::new();
    for name in wire.unsigned_names() {
        out.push((
            "unsigned header changed after signing",
            changed(wire, |wire| {
                wire.replace(&name, "changed after signing".to_owned());
            }),
        ));
        out.push((
            "unsigned header dropped after signing",
            changed(wire, |wire| wire.remove(&name)),
        ));
    }
    out.push((
        "unsigned header added after signing",
        changed(wire, |wire| {
            wire.headers
                .push(("x-added-after-signing".to_owned(), "1".to_owned()));
        }),
    ));
    out
}

/// Sign `shape` and check that it verifies, that changes to its unsigned
/// headers still verify, and that each request `mutants` and `resigned` make
/// from it is refused by the check its message names.
pub fn check_shape(shape: &Shape, tally: &mut Tally, category: &'static str) -> Result<(), String> {
    let signed = sign(shape);
    check_accepted(&signed)?;
    tally.add(category);
    for (category, wire) in unsigned_changes(&signed) {
        check_accepted_as(&signed, &wire).map_err(|err| format!("{category}: {err}"))?;
        tally.add(category);
    }
    for mutant in mutants(&signed).into_iter().chain(resigned(shape)) {
        check_refused(&mutant.wire, mutant.expected, mutant.message)
            .map_err(|err| format!("{}: {err}", mutant.label))?;
        tally.add(mutant.category);
    }
    Ok(())
}
