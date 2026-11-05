// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use serde_json::json;

use super::*;
use crate::policy::document::{PolicyDocument, parse_conditions};

/// Accepted by `CreatePolicy` (twice under each of `ArnEquals`, `ArnNotEquals`,
/// `ArnLike`, `ArnNotLike`, `ForAnyValue:ArnLike`, `ArnNotLikeIfExists`) or
/// by `PutRolePolicy`, 2026-10-30.
const IDENTITY_ACCEPTED: &[&str] = &[
    "*",
    "*:*",
    "*:*:*:*:*:*",
    "a*",
    "arn",
    "arn:*",
    "arn:*:iam::123456789012:user/alice",
    "arn:aws",
    "arn:aws:dynamodb:*",
    "arn:aws:dynamodb:*:*:table/*",
    "arn:aws:dynamodb:us-east-1:123456789012",
    "arn:aws:dynamodb:us-east-1:123456789012:",
    "arn:aws:dynamodb:us-east-1:123456789012:table/a:b",
    "arn:aws:dynamodb:us-east-1:123456789012:table/t",
    "arn:aws:i*",
    "arn:aws:ia",
    "arn:aws:iam",
    "arn:aws:iam:",
    "arn:aws:iam:*",
    "arn:aws:iam:us-east-1",
    "arn:aws:iam::123456789012:user/${aws:username}",
    "arn:aws:iam::123456789012:user/*",
    "arn:aws:iam::123456789012:user/alice",
    "arn:aws:iam::9641",
    "arn:aws:iam::964157134968",
    "arn:aws:iam::964157134968:",
    "arn:aws:iam::964157134968:role/*",
    "arn:aws:iam::964157134968:role/extenddb-test-665-e1:x",
];

/// Rejected by `CreatePolicy` or `PutRolePolicy` with `MalformedPolicyDocument`
/// and "The policy failed legacy parsing", 2026-10-30.
const IDENTITY_REJECTED: &[&str] = &[
    "",
    "arn:",
    "arn::",
    "arn:aws:",
    "arn:aws:${aws:username}:us-east-1:123456789012:table/t",
    "arn:aws:dynamodb:${aws:RequestedRegion}:123456789012:table/t",
    "ARN:aws:iam::123456789012:user/alice",
    "arn:*:role/extenddb-test-665-e1",
    "arn::dynamodb:us-east-1:123456789012:table/t",
    "arn:aws::us-east-1:123456789012:table/t",
    "arn:aws:iam:*:role/extenddb-test-665-e1",
    "arn:aws:iam::${aws:PrincipalAccount}:user/alice",
    "arn:aws:iam:role/extenddb-test-665-e1",
    "b*",
    "foo:*",
    "foo:aws:iam::123456789012:user/alice",
];

/// Accepted by `UpdateAssumeRolePolicy` in a trust policy, 2026-10-30.
const TRUST_ACCEPTED: &[&str] = &[
    "*:*:*:*:*:*",
    "arn:*:iam::123456789012:user/alice",
    "arn::dynamodb:us-east-1:123456789012:table/t",
    "arn:aws:dynamodb:*:*:table/*",
    "arn:aws:dynamodb:us-east-1:123456789012:",
    "arn:aws:dynamodb:us-east-1:123456789012:table/a:b",
    "arn:aws:iam::123456789012:user/${aws:username}",
    "arn:aws:iam::123456789012:user/*",
    "arn:aws:iam::123456789012:user/alice",
];

/// Rejected by `CreateRole` or `UpdateAssumeRolePolicy` in a trust policy,
/// 2026-10-30.
const TRUST_REJECTED: &[&str] = &[
    "",
    "*",
    "arn:aws:dynamodb:us-east-1:123456789012",
    "arn:aws:iam:*",
];

const ROLE_ARN: &str = "arn:aws:iam::964157134968:role/extenddb-test-665-e1";

/// `ArnEquals` on `aws:PrincipalArn` for a session of `ROLE_ARN`, measured
/// twice per pattern with `DescribeTable` on 2026-10-30.
const MATCH_MEASURED: &[(&str, bool)] = &[
    ("*", false),
    ("*:*", false),
    ("a*", false),
    ("arn", false),
    ("arn:*", true),
    ("arn:aws", true),
    ("arn:aws:dynamodb:*", false),
    ("arn:aws:i*", true),
    ("arn:aws:ia", false),
    ("arn:aws:iam", true),
    ("arn:aws:iam:", true),
    ("arn:aws:iam:*", true),
    ("arn:aws:iam:us-east-1", false),
    ("arn:aws:iam::9641", false),
    ("arn:aws:iam::964157134968", true),
    ("arn:aws:iam::964157134968:", false),
    ("arn:aws:iam::964157134968:role/*", true),
    ("arn:aws:iam::964157134968:role/extenddb-test-665-*", true),
    (
        "arn:aws:iam::964157134968:role/extenddb-test-665-e1:x",
        false,
    ),
    ("arn:*:iam::964157134968:role/extenddb-test-665-e1", true),
    ("*:*:*:*:*:*", true),
];

#[test]
fn identity_check_accepts_what_the_service_accepted() {
    for v in IDENTITY_ACCEPTED {
        assert!(is_valid_identity_arn_value(v), "should accept {v:?}");
    }
}

#[test]
fn identity_check_rejects_what_the_service_rejected() {
    for v in IDENTITY_REJECTED {
        assert!(!is_valid_identity_arn_value(v), "should reject {v:?}");
    }
}

#[test]
fn trust_check_accepts_what_the_service_accepted() {
    for v in TRUST_ACCEPTED {
        assert!(is_valid_trust_arn_value(v), "should accept {v:?}");
    }
}

#[test]
fn trust_check_rejects_what_the_service_rejected() {
    for v in TRUST_REJECTED {
        assert!(!is_valid_trust_arn_value(v), "should reject {v:?}");
    }
}

#[test]
fn trust_message_matches_the_service_text() {
    assert_eq!(
        trust_arn_value_message("arn:aws:iam:*"),
        r#"value used for ARN condition is incorrect type: STRING\nstring: \"arn:aws:iam:*\"\n"#
    );
}

#[test]
fn matcher_matches_what_the_service_matched() {
    for (pattern, expected) in MATCH_MEASURED {
        assert_eq!(
            arn_condition_match(pattern, ROLE_ARN),
            *expected,
            "pattern {pattern:?}"
        );
    }
}

#[test]
fn matcher_needs_a_six_field_context_value() {
    assert!(!arn_condition_match("arn:aws", "arn:aws:iam"));
    assert!(!arn_condition_match("*:*:*:*:*:*", "notanarn"));
}

#[test]
fn matcher_compares_a_short_pattern_with_the_context_first_field() {
    assert!(!arn_condition_match(
        "arn:aws",
        "foo:aws:iam::964157134968:role/r"
    ));
}

#[test]
fn operator_names_cover_every_form() {
    for name in [
        "ArnEquals",
        "ArnNotEquals",
        "ArnLike",
        "ArnNotLike",
        "ForAnyValue:ArnLike",
        "ForAllValues:ArnNotEquals",
        "ArnNotLikeIfExists",
        "ForAnyValue:ArnEqualsIfExists",
    ] {
        assert!(is_arn_operator_name(name), "{name}");
    }
    for name in [
        "StringEquals",
        "ArnEqualsX",
        "arnequals",
        "ForAnyValue:StringLike",
    ] {
        assert!(!is_arn_operator_name(name), "{name}");
    }
}

fn identity_doc(effect: &str, op: &str, value: &serde_json::Value) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Action": "dynamodb:GetItem",
            "Resource": "*",
            "Condition": {op: {"aws:PrincipalArn": value}}
        }]
    })
    .to_string()
}

#[test]
fn from_json_rejects_a_bad_value_under_every_operator_form_and_effect() {
    for op in [
        "ArnEquals",
        "ArnNotEquals",
        "ArnLike",
        "ArnNotLike",
        "ForAnyValue:ArnLike",
        "ForAllValues:ArnLike",
        "ForAllValues:ArnEquals",
        "ArnNotLikeIfExists",
    ] {
        for effect in ["Allow", "Deny"] {
            let err = PolicyDocument::from_json(&identity_doc(effect, op, &json!("")))
                .expect_err("empty value must not parse");
            assert!(
                err.0.starts_with(IDENTITY_ARN_VALUE_ERROR),
                "{op} {effect}: {err}"
            );
            assert!(err.0.contains("aws:PrincipalArn"), "{err}");
        }
    }
}

#[test]
fn from_json_rejects_a_list_holding_one_bad_value() {
    let value = json!(["arn:aws:iam::123456789012:user/alice", "foo:*"]);
    let err = PolicyDocument::from_json(&identity_doc("Allow", "ArnNotEquals", &value))
        .expect_err("list with a bad value must not parse");
    assert!(err.0.contains("\"foo:*\""), "{err}");
}

#[test]
fn from_json_accepts_values_the_service_accepts() {
    let value = json!(["*", "arn:aws:iam:*", "arn:aws:iam::123456789012:user/alice"]);
    assert!(PolicyDocument::from_json(&identity_doc("Allow", "ArnNotEquals", &value)).is_ok());
    assert!(PolicyDocument::from_json_for_write(&identity_doc("Deny", "ArnLike", &value)).is_ok());
}

#[test]
fn from_json_leaves_other_operators_alone() {
    assert!(PolicyDocument::from_json(&identity_doc("Allow", "StringEquals", &json!(""))).is_ok());
}

#[test]
fn trust_conditions_with_a_short_value_are_flagged() {
    let short = parse_conditions(&json!({"ArnNotEquals": {"aws:PrincipalArn": "*"}})).unwrap();
    assert!(holds_invalid_trust_arn_value(&short));
    let wrapped = parse_conditions(
        &json!({"ForAnyValue:ArnLikeIfExists": {"aws:PrincipalArn": ["arn:aws:iam:*"]}}),
    )
    .unwrap();
    assert!(holds_invalid_trust_arn_value(&wrapped));
    let full = parse_conditions(
        &json!({"ArnNotEquals": {"aws:PrincipalArn": "arn::dynamodb:us-east-1:123456789012:table/t"}}),
    )
    .unwrap();
    assert!(!holds_invalid_trust_arn_value(&full));
    let other = parse_conditions(&json!({"StringEquals": {"aws:PrincipalArn": "*"}})).unwrap();
    assert!(!holds_invalid_trust_arn_value(&other));
}

fn trust_doc(condition: &serde_json::Value) -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
             "Action": "sts:AssumeRole"},
            {"Effect": "Deny", "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
             "Action": "sts:AssumeRole", "Condition": condition}
        ]
    })
}

#[test]
fn trust_write_error_carries_the_service_code_and_message() {
    let doc = trust_doc(
        &json!({"ArnNotEquals": {"aws:PrincipalArn": ["arn:aws:iam::123456789012:user/a", "*"]}}),
    );
    assert_eq!(
        trust_policy_arn_value_error(&doc).as_deref(),
        Some(
            r#"MalformedPolicyDocument: value used for ARN condition is incorrect type: STRING\nstring: \"*\"\n"#
        )
    );
    let ok = trust_doc(&json!({"ArnNotEquals": {"aws:PrincipalArn": "*:*:*:*:*:*"}}));
    assert_eq!(trust_policy_arn_value_error(&ok), None);
}

#[test]
fn trust_write_check_reads_a_number_or_boolean_as_its_text() {
    // `UpdateAssumeRolePolicy` rejected 123, true and 1.5 under `ArnEquals`,
    // twice each, with the value's text in the message.
    for (value, text) in [
        (json!(123), "123"),
        (json!(true), "true"),
        (json!(1.5), "1.5"),
        (json!(["arn:aws:iam::123456789012:user/a", 123]), "123"),
    ] {
        let doc = trust_doc(&json!({"ArnEquals": {"aws:PrincipalArn": value}}));
        assert_eq!(
            trust_policy_arn_value_error(&doc),
            Some(format!(
                r#"MalformedPolicyDocument: value used for ARN condition is incorrect type: STRING\nstring: \"{text}\"\n"#
            )),
            "{text}"
        );
        assert_eq!(invalid_trust_arn_value(&doc).as_deref(), Some(text));
    }
}

#[test]
fn stored_trust_value_scan_reads_a_single_statement_object() {
    let doc = json!({"Statement": {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
        "Action": "sts:AssumeRole", "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:iam"}}}});
    assert_eq!(
        invalid_trust_arn_value(&doc).as_deref(),
        Some("arn:aws:iam")
    );
    assert_eq!(invalid_trust_arn_value(&json!({"Statement": []})), None);
}
