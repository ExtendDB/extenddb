// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use serde_json::json;

use super::*;
use crate::policy::document::{PolicyDocument, parse_conditions};
use crate::policy::variables::arn_value_lacks_fields;

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
    // `PutRolePolicy`, 2026-10-30 21:13 round, twice each.
    "arn:aws/x:iam::123456789012:root",
    "arn:${aws:username}:iam::123456789012:root",
    "arn:aws:IAM::123456789012:root",
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
fn trust_message_names_the_key_and_the_value() {
    assert_eq!(
        trust_arn_value_message("aws:PrincipalArn", "arn:aws:iam:*"),
        r#"Arn condition on aws:PrincipalArn holds "arn:aws:iam:*", which does not have the six fields of an ARN"#
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

/// `pattern` as tokens: each `(text, written)` part is written in the policy
/// when `written` is true and comes from a variable's value when it is false.
fn tokens(parts: &[(&str, bool)]) -> Vec<Token> {
    parts
        .iter()
        .flat_map(|(text, written)| {
            text.encode_utf16().map(move |u| {
                if *written {
                    Token::written(u)
                } else {
                    Token::Unit(u)
                }
            })
        })
        .collect()
}

#[test]
fn token_matcher_matches_a_written_pattern_as_the_service_matched() {
    for (pattern, expected) in MATCH_MEASURED {
        assert_eq!(
            arn_condition_match_tokens(&tokens(&[(pattern, true)]), ROLE_ARN),
            *expected,
            "pattern {pattern:?}"
        );
    }
}

#[test]
fn a_substituted_star_or_colon_is_a_character_of_its_field() {
    // Written in full, `arn:aws:iam::*:role/...` matches any account and
    // `arn:aws:iam:*` every iam ARN.
    assert!(arn_condition_match(
        "arn:aws:iam::*:role/extenddb-test-665-e1",
        ROLE_ARN
    ));
    assert!(arn_condition_match("arn:aws:iam:*", ROLE_ARN));
    // A `*` from a variable in the account field is the character `*`.
    let star = tokens(&[
        ("arn:aws:iam::", true),
        ("*", false),
        (":role/extenddb-test-665-e1", true),
    ]);
    assert!(!arn_condition_match_tokens(&star, ROLE_ARN));
    // A `:` from a variable does not split `aws:iam` into two fields, so the
    // pattern has three fields and its partition field is `aws:iam`.
    let colon = tokens(&[("arn:", true), ("aws:iam", false), (":*", true)]);
    assert!(!arn_condition_match_tokens(&colon, ROLE_ARN));
    // A `*` from a variable is no bare `*` either.
    assert!(!arn_condition_match_tokens(&[Token::Unit(0x2A)], ROLE_ARN));
    assert!(!arn_condition_match_tokens(&[Token::Star], ROLE_ARN));
}

#[test]
fn identity_check_rejects_every_value_that_lacks_written_fields() {
    // Every value of one to four of these pieces joined by `:`: each value
    // `arn_value_lacks_fields` reports must fail the write check, so the two
    // field counts agree on every value a write accepts.
    const PIECES: [&str; 8] = [
        "arn",
        "aws",
        "",
        "*",
        "${a}",
        "${a:b}",
        "${a:b:c:d:e}",
        "x/y",
    ];
    let mut values = vec![String::new()];
    let mut layer = vec![String::new()];
    for _ in 0..4 {
        let mut next = Vec::new();
        for prefix in &layer {
            for piece in PIECES {
                let v = if prefix.is_empty() {
                    piece.to_owned()
                } else {
                    format!("{prefix}:{piece}")
                };
                next.push(v);
            }
        }
        values.extend(next.iter().cloned());
        layer = next;
    }
    let mut reported = 0;
    for v in &values {
        if arn_value_lacks_fields(v) {
            reported += 1;
            assert!(!is_valid_identity_arn_value(v), "{v:?}");
        }
    }
    assert!(reported > 100, "{reported}");
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
fn the_write_check_rejects_a_bad_value_under_every_operator_form_and_effect() {
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
            let doc = identity_doc(effect, op, &json!(""));
            let err = PolicyDocument::from_json_for_write(&doc)
                .expect_err("empty value must not be written");
            assert_eq!(
                err.0,
                r#"Arn condition on aws:PrincipalArn holds "": its first field does not match arn"#,
                "{op} {effect}"
            );
            // A stored one still loads; its condition fails closed instead.
            assert!(PolicyDocument::from_json(&doc).is_ok(), "{op} {effect}");
        }
    }
}

#[test]
fn the_write_check_rejects_a_list_holding_one_bad_value() {
    let value = json!(["arn:aws:iam::123456789012:user/alice", "foo:*"]);
    let doc = identity_doc("Allow", "ArnNotEquals", &value);
    let err = PolicyDocument::from_json_for_write(&doc)
        .expect_err("list with a bad value must not be written");
    assert!(err.0.contains("\"foo:*\""), "{err}");
    assert!(PolicyDocument::from_json(&doc).is_ok());
}

#[test]
fn the_write_check_names_what_is_wrong() {
    for (value, problem) in [
        ("b*", "its first field does not match arn"),
        ("arn::", "its partition field is empty"),
        ("arn:aws:", "its service field is empty"),
        (
            "arn:aws/x:iam::123456789012:root",
            "its partition field holds /",
        ),
        (
            "arn:${aws:username}:iam::123456789012:root",
            "its partition field holds a policy variable",
        ),
        ("arn:*:role/x", "its service field holds /"),
        (
            "arn:aws:dynamodb:${aws:RequestedRegion}:123456789012:table/t",
            "its region field holds a policy variable",
        ),
        (
            "arn:aws:iam::${aws:PrincipalAccount}:user/alice",
            "its account field holds a policy variable",
        ),
        (
            "arn:aws:IAM::123456789012:root",
            "its service field holds an upper-case letter",
        ),
    ] {
        assert_eq!(
            identity_arn_value_problem(value).as_deref(),
            Some(problem),
            "{value:?}"
        );
    }
    assert_eq!(identity_arn_value_problem("arn:aws:iam:*"), None);
}

/// Accepted by `CreatePolicy` or `PutRolePolicy`, 2026-10-30; how the
/// service evaluates them was not measured.
const UNMEASURED_SHORT: &[&str] = &[
    "*:aws:iam:*",
    "a*:aws:iam:*",
    "ar?:aws:iam",
    "*:aws:iam::123456789012",
];

#[test]
fn a_short_pattern_with_a_wildcard_first_field_is_refused_at_write() {
    for v in UNMEASURED_SHORT {
        assert!(is_valid_identity_arn_value(v), "the service accepts {v:?}");
        assert!(is_unmeasured_short_pattern(v), "{v:?}");
        let err = PolicyDocument::from_json_for_write(&identity_doc("Deny", "ArnLike", &json!(v)))
            .expect_err("refused at write");
        assert_eq!(
            err.0,
            format!(
                "Arn condition on aws:PrincipalArn holds {v:?}: ExtendDB does not accept a value \
                 of two to five fields whose first field holds a wildcard"
            )
        );
    }
    // `*:*` was measured to match nothing, and the six-field and one-field
    // wildcard values were measured too.
    for v in ["*:*", "*", "a*", "*:*:*:*:*:*", "arn:*"] {
        assert!(!is_unmeasured_short_pattern(v), "{v:?}");
        assert!(
            PolicyDocument::from_json_for_write(&identity_doc("Deny", "ArnLike", &json!(v)))
                .is_ok(),
            "{v:?}"
        );
    }
}

#[test]
fn only_values_with_no_meaning_or_no_measurement_fail_closed() {
    for v in [
        "",
        "foo:*",
        "b*",
        "ARN:aws:iam::123456789012:user/alice",
        "${aws:PrincipalArn}",
        "arn:aws:iam::${aws:PrincipalAccount}",
        "*:aws:iam:*",
        "ar?:aws:iam",
    ] {
        for expanded in [true, false] {
            assert!(fails_closed_at_evaluation(v, expanded), "{v:?} {expanded}");
        }
        // Each is one the write check refuses.
        assert!(
            PolicyDocument::from_json_for_write(&identity_doc("Deny", "ArnLike", &json!(v)))
                .is_err(),
            "{v:?}"
        );
    }
    // Values whose expansion has two to five fields fail closed only where
    // they are expanded. The write check refuses each.
    for v in [
        "arn:aws:iam::${aws:PrincipalAccount, '}:'}",
        "*:${x, '}::::'}",
        "arn:aws:iam::${}:}",
    ] {
        assert!(fails_closed_at_evaluation(v, true), "{v:?}");
        assert!(!fails_closed_at_evaluation(v, false), "{v:?}");
        assert!(
            PolicyDocument::from_json_for_write(&identity_doc("Deny", "ArnLike", &json!(v)))
                .is_err(),
            "{v:?}"
        );
    }
    for v in [
        "*",
        "*:*",
        "a*",
        "arn:aws:iam:*",
        "arn::dynamodb:us-east-1:123456789012:table/t",
        "arn:*:role/x",
        "arn:aws:IAM::123456789012:root",
        "arn:aws:iam::${aws:PrincipalAccount}:user/${aws:username}",
        "arn:aws:iam::123456789012:user/${aws:username}",
        // A first field that holds a variable is left to the matcher. Its
        // references hold every `:`, so its expansion has one field.
        "${x, '}:::::'}",
    ] {
        for expanded in [true, false] {
            assert!(!fails_closed_at_evaluation(v, expanded), "{v:?} {expanded}");
        }
    }
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
    assert_eq!(
        invalid_trust_arn_value(&short),
        Some(("aws:PrincipalArn", "*"))
    );
    let wrapped = parse_conditions(
        &json!({"ForAnyValue:ArnLikeIfExists": {"aws:SourceArn": ["arn:aws:iam:*"]}}),
    )
    .unwrap();
    assert_eq!(
        invalid_trust_arn_value(&wrapped),
        Some(("aws:SourceArn", "arn:aws:iam:*"))
    );
    let full = parse_conditions(
        &json!({"ArnNotEquals": {"aws:PrincipalArn": "arn::dynamodb:us-east-1:123456789012:table/t"}}),
    )
    .unwrap();
    assert_eq!(invalid_trust_arn_value(&full), None);
    let other = parse_conditions(&json!({"StringEquals": {"aws:PrincipalArn": "*"}})).unwrap();
    assert_eq!(invalid_trust_arn_value(&other), None);
}

#[test]
fn the_session_policy_check_runs_the_arn_check_and_not_the_version_or_sid_check() {
    let doc = |version: &str, sid: &str, value: &str| {
        json!({"Version": version, "Statement": [{
            "Sid": sid, "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnLike": {"aws:PrincipalArn": value}}}]})
        .to_string()
    };
    let other = doc("2020-01-01", "Read-Only", "arn:aws:iam:*");
    assert!(PolicyDocument::from_json_for_write(&other).is_err());
    assert!(PolicyDocument::from_json_for_session_policy(&other).is_ok());
    let err = PolicyDocument::from_json_for_session_policy(&doc("2012-10-17", "", ""))
        .expect_err("an empty Arn value is refused");
    assert_eq!(
        err.0,
        r#"Arn condition on aws:PrincipalArn holds "": its first field does not match arn"#
    );
    assert!(
        PolicyDocument::from_json_for_session_policy(&doc("2012-10-17", "", "*:aws:iam:*"))
            .is_err()
    );
}

#[test]
fn a_trust_value_that_fails_closed_at_evaluation_is_found() {
    let value = "ARN:aws:iam::123456789012:user/x";
    let first_field =
        parse_conditions(&json!({"ArnNotEquals": {"aws:PrincipalArn": value}})).unwrap();
    assert_eq!(invalid_trust_arn_value(&first_field), None);
    let short = parse_conditions(&json!({"ArnLike": {"aws:SourceArn": "arn:aws:iam:*"}})).unwrap();
    let full = parse_conditions(
        &json!({"ArnEquals": {"aws:PrincipalArn": "arn:aws:iam::123456789012:user/x"}}),
    )
    .unwrap();
    for version_2012 in [true, false] {
        assert_eq!(
            fail_closed_trust_arn_value(&first_field, version_2012),
            Some(("aws:PrincipalArn", value))
        );
        assert_eq!(
            fail_closed_trust_arn_value(&short, version_2012),
            Some(("aws:SourceArn", "arn:aws:iam:*"))
        );
        assert_eq!(fail_closed_trust_arn_value(&full, version_2012), None);
    }
    // A value whose expansion has five fields has six fields at write, and
    // fails closed only in a `2012-10-17` trust policy, where it expands.
    let quoted = "arn:aws:iam::${aws:PrincipalAccount, '}:'}";
    let expands = parse_conditions(&json!({"ArnLike": {"aws:PrincipalArn": quoted}})).unwrap();
    assert_eq!(invalid_trust_arn_value(&expands), None);
    assert_eq!(
        fail_closed_trust_arn_value(&expands, true),
        Some(("aws:PrincipalArn", quoted))
    );
    assert_eq!(fail_closed_trust_arn_value(&expands, false), None);
}
