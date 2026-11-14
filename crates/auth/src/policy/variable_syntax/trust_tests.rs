// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the trust policy checks of the `${...}` write check. The
//! expectations follow live IAM measurements on `UpdateAssumeRolePolicy` and
//! `CreateRole` taken on 2026-11-09, two samples each.

use super::tests::{ARN, MALFORMED, TERMINATED, UNTERMINATED, UNTERMINATED_QUOTED, all_invalid};
use super::*;
use serde_json::json;

fn trust(version: Option<&str>, op: &str, key: &str, value: &str) -> Value {
    let mut doc = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "sts:AssumeRole",
            "Condition": {op: {key: value}},
        }],
    });
    if let Some(v) = version {
        doc["Version"] = json!(v);
    }
    doc
}

#[test]
fn trust_2012_refuses_every_value_kind() {
    for v in all_invalid() {
        for (op, key, value) in [
            ("StringEquals", "sts:ExternalId", v.to_owned()),
            ("StringLike", "sts:ExternalId", v.to_owned()),
            ("NumericEquals", "aws:MultiFactorAuthAge", format!("1{v}")),
            ("Bool", "aws:MultiFactorAuthPresent", format!("true{v}")),
            ("ArnLike", "aws:PrincipalArn", format!("{ARN}{v}")),
            ("ArnEquals", "aws:PrincipalArn", format!("{ARN}{v}")),
        ] {
            let doc = trust(Some("2012-10-17"), op, key, &value);
            let e = trust_variable_error(&doc).unwrap_or_else(|| panic!("{op} {value}"));
            assert!(e.starts_with(TRUST_INVALID_VARIABLES), "{e}");
            assert_eq!(trust_arn_value_error(&doc), None);
        }
    }
    for v in TERMINATED {
        let doc = trust(Some("2012-10-17"), "StringEquals", "sts:ExternalId", v);
        assert_eq!(trust_variable_error(&doc), None, "{v}");
    }
}

#[test]
fn trust_2008_and_unversioned_refuse_only_arn_values() {
    for version in [Some("2008-10-17"), None] {
        for v in all_invalid() {
            let doc = trust(version, "StringEquals", "sts:ExternalId", v);
            assert_eq!(trust_variable_error(&doc), None);
            assert_eq!(trust_arn_value_error(&doc), None, "{version:?} {v}");
        }
        for v in UNTERMINATED {
            for op in [
                "ArnLike",
                "ArnEquals",
                "ArnNotLikeIfExists",
                "ForAnyValue:ArnLike",
            ] {
                let doc = trust(version, op, "aws:PrincipalArn", &format!("{ARN}{v}"));
                assert!(
                    trust_arn_value_error(&doc).is_some(),
                    "{version:?} {op} {v}"
                );
            }
        }
        for v in TERMINATED
            .iter()
            .chain(UNTERMINATED_QUOTED)
            .chain(MALFORMED)
        {
            let doc = trust(version, "ArnLike", "aws:PrincipalArn", &format!("{ARN}{v}"));
            assert_eq!(trust_arn_value_error(&doc), None, "{version:?} {v}");
        }
    }
}

#[test]
fn trust_arn_message_escapes_as_iam_does() {
    let cases = [
        ("a${", r"a\${"),
        ("a$${", r"a$\${"),
        ("a$b${", r"a$b\${"),
        ("a\"${", r#"a\\\"\${"#),
        ("a\\${", r"a\\\\\${"),
        ("a'${", r"a\\\'\${"),
        ("a\t${", r"a\\t\${"),
        ("a\u{1}${", r"a\\001\${"),
        ("a\u{e9}${", r"a\\303\\251\${"),
    ];
    for (tail, shown) in cases {
        let doc = trust(
            Some("2008-10-17"),
            "ArnLike",
            "aws:PrincipalArn",
            &format!("{ARN}{tail}"),
        );
        let expected = format!(
            r#"MalformedPolicyDocument: value used for ARN condition is incorrect type: STRING\nstring: \"arn:aws:iam::123456789012:user\/{shown}\"\n"#
        );
        assert_eq!(
            trust_arn_value_error(&doc).as_deref(),
            Some(expected.as_str()),
            "{tail}"
        );
    }
}
