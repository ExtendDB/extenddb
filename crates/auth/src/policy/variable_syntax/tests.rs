// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `${...}` reference write check. The expectations follow
//! live IAM measurements taken on 2026-11-09, two samples each: every value
//! class under string and Arn operators and in `Resource` and `NotResource`
//! for each `Version`, and each other operator family with one value of the
//! class. The rows for the other operator families extend those cells to the
//! other values of the same class.

use super::*;
use extenddb_core::limits::DEFAULT_MAX_POLICY_DOCUMENT_BYTES;
use serde_json::json;

/// Unterminated on every path: no `}` follows the `${`.
pub(super) const UNTERMINATED: &[&str] = &[
    "a${",
    "${",
    "a${aws:username",
    "${aws:username}${",
    "a$${",
    "a${aws:username, 'x'",
];

/// Not closed under IAM's reading, but closed when each `${` ends at the next
/// `}`: refused by identity policies and `2012-10-17` trust policies, stored
/// by a `2008-10-17` trust Arn check, which closes at the first `}`.
pub(super) const UNTERMINATED_QUOTED: &[&str] = &[
    "a${'}}",
    "a${}'}",
    "a${'}",
    "a${x'}",
    "a'${'}",
    "a${aws:username, '}'",
    "a${aws:username, 'x''}'",
];

/// Closed but not well formed: refused on the identity paths and in
/// `2012-10-17` trust policies, stored by a `2008-10-17` trust policy.
pub(super) const MALFORMED: &[&str] = &[
    "a${}",
    "a${ }",
    "a${'x'}",
    "a${'}'}",
    "a${aws:username, \"x\"}",
    "a${aws:username, \"}\"}",
    "a${aws:username,}",
    "a${, 'x'}",
    "a${ , 'x'}",
    "a${,}",
    "a${aws:username, 'x' , 'y'}",
    "a${'x', 'y'}",
    "a${a,b}",
    "a${a, b}",
    "a${a,b, 'x'}",
    "a${aws user}",
    "a${a b, 'x'}",
    "a${a\tb}",
    "a${a\nb}",
    "a${a\rb}",
    "a${a\u{0b}b}",
    "a${a\u{0c}b}",
    "a${a'b'}",
    "a${aws:username, 'x',}",
    "a${aws:username, 'x'y}",
    "a${aws:username, 'x' 'y'}",
    "a${,}}",
    "a${}' }'}",
    "a${}${a, 'x'}",
    "a${}${aws:username, '}'}",
    "a${\u{1} \u{1}}",
    "a${\u{1}, 'x'}",
    "a${aws:username,\u{1}'x'\u{1}}",
    "a${aws:username, \u{1}'x'}",
    "a${aws:username,'x'\u{1}}",
    "a${a$b, 'x'}",
    "a${$x, 'y'}",
];

pub(super) const TERMINATED: &[&str] = &[
    "a",
    "a$",
    "a}",
    "a'}${x}",
    "a${*}",
    "a${aws:username}",
    "a${aws:username, 'x'}",
    "a${aws:username}}",
    "a${${aws:username}}",
    "a${aws:username, '}x'}",
    "a${aws:username, '}'}",
    "a${aws:username, 'x${'}",
    "a${aws:username, ''}",
    "a${aws:username, '\\'}'",
    "a${aws:username,'x'}",
    "a${aws:username , 'x'}",
    "a${aws:username,  'x'}",
    "a${aws:username, 'x' }",
    "a${aws:username, 'x'\t}",
    "a${aws:username,\t'x'}",
    "a${aws:username, ' '}",
    "a${aws:username, 'a,b'}",
    "a${aws:username, 'x''y'}",
    "a${aws:username, ''''}",
    "a${aws:username, 'x'''}",
    "a${aws:username, '''x'}",
    "a${aws:username, 'x'''''}",
    "a${aws:username,'}'}",
    "a${ aws:username }",
    "a${\taws:username\t}",
    "a${aws:username\n}",
    "a${\u{1}aws:username}",
    "a${ * }",
    "a${ ? }",
    "a${ $ }",
    "a${?}",
    "a${$}",
    "a${*x}",
    "a${**}",
    "a${?x}",
    "a${$x}",
    "a${*, 'x'}",
    "a${:}",
    "a${a$b}",
    "a${a{b}",
    "a${a\"b}",
    "a${a\\b}",
    "a${a/b}",
    "a${a-b}",
    "a${a\u{1}b}",
    "a${a\u{1f}b}",
    "a${a\u{7f}b}",
    "a${a\u{a0}b}",
    "a${}}",
    "a${}x}",
    "a${ }}",
    "a${\t}}",
    "a${}}}",
    "a${}${}",
    "a${}${a}",
    "a${}${a}${b}",
    "a${ }${aws:username}",
    "a${}${aws:username}",
    "a${}:}",
    "a${}, 'x'}",
    "a${}a, 'x'}",
    "a${}x, 'y'}",
    "a${}, '}'}",
    "a${}aws:username, '}'}",
    "a${\u{1}}",
    "a${\u{1f}}",
    "a${\u{1}\u{1}}",
    "a${ \u{1} }",
    "a${aws:username\u{1},'x'}",
    "a${\u{1}aws:username, 'x'}",
    "a${a{b, 'x'}",
    "a${$, 'x'}",
    "a${?, 'x'}",
    "a${*, 'x'}",
    "a${ * , 'x'}",
];

pub(super) const ARN: &str = "arn:aws:iam::123456789012:user/";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue/";

pub(super) fn all_unterminated() -> impl Iterator<Item = &'static str> {
    UNTERMINATED.iter().chain(UNTERMINATED_QUOTED).copied()
}

/// Every value an identity write and a `2012-10-17` trust write refuse.
pub(super) fn all_invalid() -> impl Iterator<Item = &'static str> {
    all_unterminated().chain(MALFORMED.iter().copied())
}

#[test]
fn scan_finds_every_unterminated_reference() {
    for v in all_unterminated() {
        assert!(has_invalid_reference(v), "{v}");
        assert!(has_invalid_reference(&format!("{ARN}{v}")), "{v}");
    }
    for v in TERMINATED {
        assert!(!has_invalid_reference(v), "{v}");
        assert!(!has_invalid_reference(&format!("{ARN}{v}")), "{v}");
    }
    for v in MALFORMED {
        assert!(has_invalid_reference(v), "{v}");
        assert!(!has_unclosed_brace(v), "{v}");
    }
    for v in UNTERMINATED_QUOTED {
        assert!(!has_unclosed_brace(v), "{v}");
    }
    for v in UNTERMINATED {
        assert!(has_unclosed_brace(v), "{v}");
    }
    assert!(!has_invalid_reference("\u{e9}${x}\u{1F600}"));
    assert!(has_invalid_reference("\u{e9}${x\u{1F600}"));
}

fn identity(version: &str, op: &str, key: &str, value: &Value) -> String {
    json!({
        "Version": version,
        "Statement": [{
            "Effect": "Deny",
            "Action": "dynamodb:GetItem",
            "Resource": "*",
            "Condition": {op: {key: value}},
        }],
    })
    .to_string()
}

fn identity_resource(version: &str, field: &str, value: &str) -> String {
    json!({
        "Version": version,
        "Statement": [{"Effect": "Deny", "Action": "dynamodb:GetItem", field: value}],
    })
    .to_string()
}

fn identity_statements(version: &str, statements: &Value) -> String {
    json!({"Version": version, "Statement": statements}).to_string()
}

fn write_error(json: &str) -> String {
    PolicyDocument::from_json_for_write(json, DEFAULT_MAX_POLICY_DOCUMENT_BYTES)
        .expect_err(json)
        .0
}

const STRING_OPS: &[&str] = &[
    "StringEquals",
    "StringLike",
    "StringNotEquals",
    "StringNotLike",
    "StringEqualsIgnoreCase",
    "StringEqualsIfExists",
    "ForAnyValue:StringEquals",
    "ForAllValues:StringLike",
];
const ARN_OPS: &[&str] = &["ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike"];

#[test]
fn identity_2012_refuses_with_syntax_error() {
    for v in all_invalid() {
        for op in STRING_OPS {
            let e = write_error(&identity(
                "2012-10-17",
                op,
                "aws:ResourceTag/owner",
                &json!(v),
            ));
            assert!(e.starts_with(SYNTAX_ERROR), "{op} {v}: {e}");
        }
        for op in ARN_OPS {
            let doc = identity(
                "2012-10-17",
                op,
                "aws:PrincipalArn",
                &json!(format!("{ARN}{v}")),
            );
            assert!(write_error(&doc).starts_with(SYNTAX_ERROR), "{op} {v}");
        }
        for (op, key, value) in [
            ("NumericEquals", "aws:MultiFactorAuthAge", format!("1{v}")),
            (
                "DateEquals",
                "aws:CurrentTime",
                format!("2026-01-01T00:00:00Z{v}"),
            ),
            ("IpAddress", "aws:SourceIp", format!("10.0.0.0/8{v}")),
            ("Bool", "aws:SecureTransport", format!("true{v}")),
            ("Null", "aws:ResourceTag/owner", format!("true{v}")),
        ] {
            let doc = identity("2012-10-17", op, key, &json!(value));
            assert!(write_error(&doc).starts_with(SYNTAX_ERROR), "{op} {value}");
        }
        for field in ["Resource", "NotResource"] {
            let doc = identity_resource("2012-10-17", field, &format!("{TABLE}{v}"));
            assert!(write_error(&doc).starts_with(SYNTAX_ERROR), "{field} {v}");
        }
    }
}

#[test]
fn identity_2008_refuses_with_iam_messages() {
    for v in all_invalid() {
        for op in STRING_OPS {
            let e = write_error(&identity(
                "2008-10-17",
                op,
                "aws:ResourceTag/owner",
                &json!(v),
            ));
            assert!(e.starts_with(VARIABLE_NOT_WELL_FORMED), "{op} {v}: {e}");
        }
        for op in ARN_OPS {
            let doc = identity(
                "2008-10-17",
                op,
                "aws:PrincipalArn",
                &json!(format!("{ARN}{v}")),
            );
            assert!(
                write_error(&doc).starts_with(VARIABLE_NOT_WELL_FORMED),
                "{op} {v}"
            );
        }
        for (op, key, value) in [
            ("NumericEquals", "aws:MultiFactorAuthAge", format!("1{v}")),
            (
                "DateEquals",
                "aws:CurrentTime",
                format!("2026-01-01T00:00:00Z{v}"),
            ),
        ] {
            let doc = identity("2008-10-17", op, key, &json!(value));
            assert!(
                write_error(&doc).starts_with(LEGACY_PARSING),
                "{op} {value}"
            );
        }
        for (op, key) in [
            ("Bool", "aws:SecureTransport"),
            ("Null", "aws:ResourceTag/owner"),
        ] {
            let doc = identity("2008-10-17", op, key, &json!(format!("true{v}")));
            assert!(
                write_error(&doc).starts_with(VARIABLE_NOT_WELL_FORMED),
                "{op} {v}"
            );
        }
    }
}

/// Under `2008-10-17` a pattern gets the Resource message when no later `}`
/// closes its `${`, quoted or not, and the variable message when only a
/// quoted `}` follows.
#[test]
fn identity_2008_pattern_message_follows_the_unquoted_close() {
    for field in ["Resource", "NotResource"] {
        for v in UNTERMINATED.iter().chain(&["${'x"]) {
            let pattern = format!("{TABLE}{v}");
            let doc = identity_resource("2008-10-17", field, &pattern);
            assert_eq!(
                write_error(&doc),
                format!(
                    "MalformedPolicyDocument: Resource {pattern} must be in ARN format or \"*\"."
                ),
                "{field} {v}"
            );
        }
        for v in UNTERMINATED_QUOTED
            .iter()
            .chain(MALFORMED)
            .chain(&["${'}x"])
        {
            let pattern = format!("{TABLE}{v}");
            let doc = identity_resource("2008-10-17", field, &pattern);
            assert_eq!(
                write_error(&doc),
                format!("{VARIABLE_NOT_WELL_FORMED} ({field} holds {pattern:?})"),
                "{field} {v}"
            );
        }
    }
}

/// Under `2008-10-17` the ARN field that holds the first `$` decides the
/// message: the service, region and account fields of a pattern, and every
/// field before the resource field of an Arn value, fail legacy parsing.
#[test]
fn identity_2008_message_follows_the_arn_field() {
    let resource_format =
        |p: &str| format!("MalformedPolicyDocument: Resource {p} must be in ARN format or \"*\".");
    for (pattern, legacy) in [
        ("arn:${x:dynamodb:us-east-1:123456789012:table/t", false),
        ("arn:aws:dyn${x:us-east-1:123456789012:table/t", true),
        ("arn:aws:dynamodb:us-${x:123456789012:table/t", true),
        ("arn:aws:dynamodb:us-east-1:12${x:table/t", true),
        ("arn:aws:dynamodb:us-east-1:123456789012:table/${x", false),
    ] {
        let e = write_error(&identity_resource("2008-10-17", "Resource", pattern));
        if legacy {
            assert!(e.starts_with(LEGACY_PARSING), "{pattern}: {e}");
        } else {
            assert_eq!(e, resource_format(pattern));
        }
    }
    for (value, legacy) in [
        ("arn:${x:iam::123456789012:user/a", true),
        ("arn:aws:i${x::123456789012:user/a", true),
        ("arn:aws:iam:${x:123456789012:user/a", true),
        ("arn:aws:iam::${x:user/a", true),
        ("${aws:PrincipalArn", true),
        ("arn:aws:iam${x", true),
        ("arn:aws:iam::123456789012:user/${x", false),
    ] {
        let e = write_error(&identity(
            "2008-10-17",
            "ArnLike",
            "aws:PrincipalArn",
            &json!(value),
        ));
        let expected = if legacy {
            LEGACY_PARSING
        } else {
            VARIABLE_NOT_WELL_FORMED
        };
        assert!(e.starts_with(expected), "{value}: {e}");
    }
}

#[test]
fn identity_accepts_terminated_values() {
    for version in ["2012-10-17", "2008-10-17"] {
        for v in TERMINATED {
            for op in STRING_OPS {
                let doc = identity(version, op, "aws:ResourceTag/owner", &json!(v));
                assert!(
                    PolicyDocument::from_json_for_write(&doc, DEFAULT_MAX_POLICY_DOCUMENT_BYTES)
                        .is_ok(),
                    "{version} {op} {v}"
                );
            }
            for op in ARN_OPS {
                let doc = identity(version, op, "aws:PrincipalArn", &json!(format!("{ARN}{v}")));
                assert!(
                    PolicyDocument::from_json_for_write(&doc, DEFAULT_MAX_POLICY_DOCUMENT_BYTES)
                        .is_ok(),
                    "{version} {op} {v}"
                );
            }
            let doc = identity_resource(version, "Resource", &format!("{TABLE}{v}"));
            assert!(
                PolicyDocument::from_json_for_write(&doc, DEFAULT_MAX_POLICY_DOCUMENT_BYTES)
                    .is_ok(),
                "{version} {v}"
            );
        }
    }
}

fn deny(extra: &Value) -> Value {
    let mut s = json!({"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*"});
    for (k, v) in extra.as_object().expect("object") {
        s[k] = v.clone();
    }
    s
}

#[test]
fn identity_checks_every_array_element_and_statement_but_not_keys() {
    for (version, message) in [
        ("2012-10-17", SYNTAX_ERROR),
        ("2008-10-17", VARIABLE_NOT_WELL_FORMED),
    ] {
        let doc = identity(
            version,
            "StringEquals",
            "aws:ResourceTag/owner",
            &json!(["a", "b${"]),
        );
        assert!(write_error(&doc).starts_with(message), "{version} array");

        let doc = identity_statements(
            version,
            &json!([
                deny(&json!({"Condition": {"StringEquals": {"aws:ResourceTag/owner": "a"}}})),
                deny(&json!({"Condition": {"StringEquals": {"aws:ResourceTag/owner": "a${"}}})),
            ]),
        );
        assert!(
            write_error(&doc).starts_with(message),
            "{version} second statement"
        );

        let doc = identity(version, "StringEquals", "aws:ResourceTag/${x", &json!("a"));
        assert!(
            PolicyDocument::from_json_for_write(&doc, DEFAULT_MAX_POLICY_DOCUMENT_BYTES).is_ok(),
            "{version} key"
        );
    }
    let doc = identity_statements(
        "2012-10-17",
        &json!([deny(&json!({"Resource": [TABLE, format!("{TABLE}a${{")]}))]),
    );
    assert!(write_error(&doc).starts_with(SYNTAX_ERROR));
}

/// Under `2012-10-17` the variable check wins over the Sid, CIDR and
/// structural checks. Under `2008-10-17` a value that fails legacy parsing
/// wins anywhere in the document, the Sid check comes next, and the other
/// variable messages come last.
#[test]
fn identity_precedence_follows_the_version() {
    let bad_sid = |version: &str, cond: Value| {
        identity_statements(
            version,
            &json!([deny(&json!({"Sid": "bad-sid", "Condition": cond}))]),
        )
    };
    let str_cond = json!({"StringEquals": {"aws:ResourceTag/owner": "a${"}});
    let num_cond = json!({"NumericEquals": {"aws:MultiFactorAuthAge": "1${"}});
    assert!(write_error(&bad_sid("2012-10-17", str_cond.clone())).starts_with(SYNTAX_ERROR));
    assert!(write_error(&bad_sid("2008-10-17", str_cond.clone())).contains("Sid"));
    assert!(write_error(&bad_sid("2008-10-17", num_cond.clone())).starts_with(LEGACY_PARSING));

    let cidr = |version: &str| {
        identity_statements(
            version,
            &json!([deny(&json!({"Condition": {
                "IpAddress": {"aws:SourceIp": "300.0.0.0/8"},
                "StringEquals": {"aws:ResourceTag/owner": "a${"}}}))]),
        )
    };
    assert!(write_error(&cidr("2012-10-17")).starts_with(SYNTAX_ERROR));
    assert!(write_error(&cidr("2008-10-17")).contains("CIDR"));

    let structural = identity_statements(
        "2012-10-17",
        &json!([deny(
            &json!({"NotAction": "dynamodb:Query", "Condition": str_cond})
        )]),
    );
    assert!(write_error(&structural).starts_with(SYNTAX_ERROR));

    for first in [
        deny(&json!({"Condition": str_cond})),
        deny(&json!({"Resource": format!("{TABLE}a${{")})),
    ] {
        let doc = identity_statements(
            "2008-10-17",
            &json!([first, deny(&json!({"Condition": num_cond}))]),
        );
        assert!(write_error(&doc).starts_with(LEGACY_PARSING), "{doc}");
    }
}

#[test]
fn stored_document_keeps_parsing() {
    for version in ["2012-10-17", "2008-10-17"] {
        let doc = identity(
            version,
            "StringEquals",
            "aws:ResourceTag/owner",
            &json!("a${"),
        );
        assert!(PolicyDocument::from_json(&doc).is_ok(), "{version}");
    }
}

/// Under `2008-10-17` a pattern whose first `$` lies in its partition field
/// gets the Partition message; in one statement an unclosed `${` in any
/// pattern wins over a malformed reference, and across statements the first
/// statement decides.
#[test]
fn identity_2008_partition_and_pattern_order() {
    let p = "arn:${}:dynamodb:us-east-1:123456789012:table/t";
    assert_eq!(
        write_error(&identity_resource("2008-10-17", "Resource", p)),
        format!("MalformedPolicyDocument: Partition \"${{}}\" is not valid for resource \"{p}\".")
    );
    assert!(write_error(&identity_resource("2012-10-17", "Resource", p)).starts_with(SYNTAX_ERROR));
    let unclosed = format!("{TABLE}b${{");
    let malformed = format!("{TABLE}a${{}}");
    let resource_format =
        format!("MalformedPolicyDocument: Resource {unclosed} must be in ARN format or \"*\".");
    for array in [json!([malformed, unclosed]), json!([unclosed, malformed])] {
        let doc = identity_statements("2008-10-17", &json!([deny(&json!({"Resource": array}))]));
        assert_eq!(write_error(&doc), resource_format);
    }
    let doc = identity_statements(
        "2008-10-17",
        &json!([
            deny(&json!({"Resource": malformed})),
            deny(&json!({"Resource": unclosed})),
        ]),
    );
    assert!(write_error(&doc).starts_with(VARIABLE_NOT_WELL_FORMED));
    let doc = identity_statements(
        "2008-10-17",
        &json!([deny(&json!({"Resource": unclosed,
            "Condition": {"StringEquals": {"aws:ResourceTag/owner": "a${}"}}}))]),
    );
    assert_eq!(write_error(&doc), resource_format);
    let doc = identity_statements(
        "2008-10-17",
        &json!([deny(&json!({"Sid": "bad-sid", "Resource": p}))]),
    );
    assert!(write_error(&doc).contains("Sid"));
}
