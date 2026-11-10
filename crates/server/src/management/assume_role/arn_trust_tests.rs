// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Arn condition values in role trust policies, at write and at evaluation.
//!
//! Live IAM rejects at `CreateRole` and `UpdateAssumeRolePolicy` a trust
//! policy whose Arn condition value does not split into six ARN fields, with
//! `MalformedPolicyDocument` and a message naming the value, and it reads a
//! JSON number or boolean as its text, so it rejects one the same way. It
//! accepted every six-field value tried, an empty partition included. Measured
//! on 2026-10-30 under `ArnEquals`, `ArnNotEquals`, `ArnLike` and
//! `ArnNotLike`. ExtendDB's message is its own and names the key and the
//! value. A shorter value can only be one stored before ExtendDB checked it,
//! and it fails closed: a Deny blocks and an Allow does not grant. AssumeRole
//! refuses a session policy that an identity policy write refuses.

use super::tests::ctx;
use super::{
    check_session_policy, stored_trust_arn_value, trust_decision, trust_decision_for,
    validate_trust_policy, warn_on_stored_trust_arn_value,
};
use extenddb_auth::policy::document::PolicyDocument;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

const CALLER: &str = "arn:aws:iam::123456789012:user/alice";

/// Values the service rejected in a trust policy Arn condition.
const SHORT: [&str; 4] = [
    "*",
    "",
    "arn:aws:dynamodb:us-east-1:123456789012",
    "arn:aws:iam:*",
];

fn stmt(effect: &str, principal: &str, condition: Option<Value>) -> Value {
    let mut s = json!({"Effect": effect, "Principal": {"AWS": principal},
        "Action": "sts:AssumeRole"});
    if let Some(c) = condition {
        s["Condition"] = c;
    }
    s
}

fn arn_cond(op: &str, value: &str) -> Value {
    json!({op: {"aws:PrincipalArn": value}})
}

fn doc(statements: &[Value]) -> Value {
    json!({"Version": "2012-10-17", "Statement": statements})
}

/// The trust write error for `value` on `aws:PrincipalArn`.
fn short_error(value: &str) -> String {
    format!(
        "Arn condition on aws:PrincipalArn holds {value:?}, which does not have the six fields \
         of an ARN"
    )
}

#[test]
fn write_rejects_a_short_value_naming_it() {
    for op in ["ArnEquals", "ArnNotEquals"] {
        for v in SHORT {
            let d = doc(&[stmt("Allow", "*", Some(arn_cond(op, v)))]);
            assert_eq!(validate_trust_policy(&d), Err(short_error(v)), "{op} {v:?}");
        }
    }
}

#[test]
fn write_reads_each_operator_entry_on_its_own() {
    // `BinaryEquals` is an IAM operator the evaluator does not parse; the Arn
    // value beside it is still checked, as IAM checks it.
    let cond = json!({
        "BinaryEquals": {"aws:PrincipalTag/x": "AAAA"},
        "ArnLike": {"aws:PrincipalArn": "arn:aws:iam:*"}
    });
    let d = doc(&[stmt("Allow", "*", Some(cond))]);
    assert_eq!(validate_trust_policy(&d), Err(short_error("arn:aws:iam:*")));
}

#[test]
fn write_rejects_a_number_or_boolean_naming_its_text() {
    // Measured at `UpdateAssumeRolePolicy` under `ArnEquals`, twice each:
    // 123, true and 1.5 were rejected with their text in the message.
    for (value, text) in [
        (json!(123), "123"),
        (json!(true), "true"),
        (json!(1.5), "1.5"),
        (json!(["arn:aws:iam::123456789012:user/alice", 123]), "123"),
    ] {
        let cond = json!({"ArnEquals": {"aws:PrincipalArn": value}});
        let d = doc(&[stmt("Allow", "*", Some(cond))]);
        assert_eq!(validate_trust_policy(&d), Err(short_error(text)), "{text}");
    }
}

#[test]
fn write_accepts_six_field_values() {
    for v in [
        "arn:aws:iam::123456789012:user/alice",
        "arn:aws:iam::123456789012:user/${aws:username}",
        "arn::dynamodb:us-east-1:123456789012:table/t",
        "*:*:*:*:*:*",
        "arn:aws:dynamodb:us-east-1:123456789012:",
    ] {
        let d = doc(&[stmt("Allow", "*", Some(arn_cond("ArnEquals", v)))]);
        assert_eq!(validate_trust_policy(&d), Ok(()), "{v:?}");
    }
}

#[test]
fn stored_short_value_under_an_allow_does_not_grant() {
    // With the IAM matcher alone each of these holds for the caller (a short
    // value that matches nothing negates to true), so the Allow would grant.
    for op in ["ArnNotEquals", "ArnNotLike", "ArnNotEqualsIfExists"] {
        for v in ["*", "", "arn:aws:dynamodb:*"] {
            let s = [stmt("Allow", "*", Some(arn_cond(op, v)))];
            assert!(!trust_decision(&s, CALLER, &ctx(), false), "{op} {v:?}");
        }
    }
}

#[test]
fn stored_short_value_under_a_deny_blocks() {
    // With the IAM matcher alone none of these matches the caller, so the
    // Deny would not apply and the unconditional Allow would grant.
    for op in ["ArnEquals", "ArnLike", "ForAnyValue:ArnLike"] {
        for v in ["*", "", "arn:aws:dynamodb:*"] {
            let s = [
                stmt("Allow", CALLER, None),
                stmt("Deny", "*", Some(arn_cond(op, v))),
            ];
            assert!(!trust_decision(&s, CALLER, &ctx(), false), "{op} {v:?}");
        }
    }
}

#[test]
fn six_field_values_still_evaluate() {
    let bob = "arn:aws:iam::123456789012:user/bob";
    let allow = [stmt("Allow", "*", Some(arn_cond("ArnNotEquals", bob)))];
    assert!(trust_decision(&allow, CALLER, &ctx(), false));
    let deny_other = [
        stmt("Allow", CALLER, None),
        stmt("Deny", "*", Some(arn_cond("ArnEquals", bob))),
    ];
    assert!(trust_decision(&deny_other, CALLER, &ctx(), false));
    let deny_caller = [
        stmt("Allow", CALLER, None),
        stmt(
            "Deny",
            "*",
            Some(arn_cond("ArnLike", "arn:aws:iam::*:user/ali*")),
        ),
    ];
    assert!(!trust_decision(&deny_caller, CALLER, &ctx(), false));
}

#[test]
fn the_stored_value_scan_reads_as_the_evaluator_reads() {
    // A single statement object, a number read as its text, and a statement
    // whose block the evaluator cannot parse, which fails closed anyway.
    let single = json!({"Statement": {"Effect": "Allow", "Principal": {"AWS": "*"},
        "Action": "sts:AssumeRole", "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:iam"}}}});
    assert_eq!(
        stored_trust_arn_value(&single),
        Some(("aws:SourceArn".to_owned(), "arn:aws:iam".to_owned()))
    );
    let number = doc(&[stmt(
        "Allow",
        "*",
        Some(json!({"ArnEquals": {"aws:PrincipalArn": 123}})),
    )]);
    assert_eq!(
        stored_trust_arn_value(&number),
        Some(("aws:PrincipalArn".to_owned(), "123".to_owned()))
    );
    let six = doc(&[stmt("Allow", "*", Some(arn_cond("ArnEquals", CALLER)))]);
    assert_eq!(stored_trust_arn_value(&six), None);
    assert_eq!(stored_trust_arn_value(&json!({"Statement": []})), None);
}

/// Collects the formatted log lines written while a test's subscriber is set.
#[derive(Clone, Default)]
struct LogLines(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogLines {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogLines {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// The warning lines `warn_on_stored_trust_arn_value` logs for `policy`. A
/// second registered subscriber makes tracing compute each callsite's
/// interest from every subscriber, and the call runs once before this test's
/// subscriber is set, so the callsite is registered.
fn warnings_for(policy: &Value) -> Vec<String> {
    let _second = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::WARN)
            .finish(),
    );
    warn_on_stored_trust_arn_value(policy, "r", "123456789012");
    let lines = LogLines::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(lines.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        warn_on_stored_trust_arn_value(policy, "r", "123456789012");
    });
    let text = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
    text.lines().map(str::to_owned).collect()
}

#[test]
fn a_stored_short_value_logs_one_warning_naming_the_role() {
    let short = doc(&[
        stmt("Allow", CALLER, None),
        stmt("Deny", "*", Some(arn_cond("ArnLike", "*"))),
    ]);
    let lines = warnings_for(&short);
    assert_eq!(lines.len(), 1, "{lines:#?}");
    assert!(
        lines[0].contains(
            r#"trust policy of role r in account 123456789012 holds Arn condition value "*" on aws:PrincipalArn"#
        ),
        "{lines:#?}"
    );
    let six = doc(&[stmt("Allow", "*", Some(arn_cond("ArnEquals", CALLER)))]);
    assert!(warnings_for(&six).is_empty());
}

#[test]
fn assume_role_refuses_a_session_policy_the_write_check_refuses() {
    let session_policy = |value: &str| {
        json!({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
            "Condition": {"ArnLike": {"aws:PrincipalArn": value}}}]})
    };
    for (value, problem) in [
        ("", "its first field does not match arn"),
        (
            "arn:aws:iam::${aws:PrincipalAccount}:user/a",
            "its account field holds a policy variable",
        ),
        (
            "*:aws:iam:*",
            "ExtendDB does not accept a value of two to five fields whose first field holds a \
             wildcard",
        ),
    ] {
        assert_eq!(
            check_session_policy(&session_policy(value)),
            Err(format!(
                "MalformedPolicyDocument: Arn condition on aws:PrincipalArn holds {value:?}: \
                 {problem}"
            )),
            "{value:?}"
        );
    }
    // A malformed CIDR, which does not load at request time either.
    let cidr = json!({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
        "Condition": {"IpAddress": {"aws:SourceIp": "nope"}}}]});
    assert!(
        check_session_policy(&cidr)
            .is_err_and(|m| m.starts_with("MalformedPolicyDocument: malformed CIDR")),
    );
    for value in [
        "arn:aws:iam:*",
        "*",
        "arn:aws:iam::123456789012:user/${aws:username}",
    ] {
        assert_eq!(
            check_session_policy(&session_policy(value)),
            Ok(()),
            "{value:?}"
        );
    }
}

/// A session policy that allows `dynamodb:GetItem` on `*`.
fn read_session_policy() -> Value {
    json!({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}]})
}

#[test]
fn assume_role_accepts_a_session_policy_whose_sid_a_policy_write_refuses() {
    // A policy write refuses this `Sid`. The request path loads the session
    // policy, and AssumeRole issued the session before its session policy
    // check, so it keeps issuing it.
    let mut sp = read_session_policy();
    sp["Statement"][0]["Sid"] = json!("Read-Only");
    assert!(PolicyDocument::from_json_for_write(&sp.to_string()).is_err());
    assert!(PolicyDocument::from_json(&sp.to_string()).is_ok());
    assert_eq!(check_session_policy(&sp), Ok(()));
}

#[test]
fn assume_role_accepts_a_session_policy_whose_version_a_policy_write_refuses() {
    let mut sp = read_session_policy();
    sp["Version"] = json!("2020-01-01");
    assert!(PolicyDocument::from_json_for_write(&sp.to_string()).is_err());
    assert!(PolicyDocument::from_json(&sp.to_string()).is_ok());
    assert_eq!(check_session_policy(&sp), Ok(()));
}

#[test]
fn a_six_field_value_whose_first_field_is_not_arn_fails_closed_and_is_named() {
    // IAM accepted this shape in a trust policy (UpdateAssumeRolePolicy,
    // 2026-10-30), and so does the trust write check. Its statement fails
    // closed, and the AssumeRole warning names it.
    let value = "ARN:aws:iam::123456789012:user/x";
    let allow = [stmt("Allow", "*", Some(arn_cond("ArnNotEquals", value)))];
    assert_eq!(validate_trust_policy(&doc(&allow)), Ok(()));
    assert!(!trust_decision(&allow, CALLER, &ctx(), false));
    let deny = [
        stmt("Allow", CALLER, None),
        stmt("Deny", "*", Some(arn_cond("ArnEquals", value))),
    ];
    assert!(!trust_decision(&deny, CALLER, &ctx(), false));
    assert_eq!(
        stored_trust_arn_value(&doc(&allow)),
        Some(("aws:PrincipalArn".to_owned(), value.to_owned()))
    );
    let lines = warnings_for(&doc(&allow));
    assert_eq!(lines.len(), 1, "{lines:#?}");
    assert!(
        lines[0].contains(&format!(
            "holds Arn condition value {value:?} on aws:PrincipalArn, so its statement fails \
             closed"
        )),
        "{lines:#?}"
    );
}

#[test]
fn a_first_field_that_holds_a_reference_is_left_to_the_matcher() {
    // With `x` absent the default `}:::::` gives one field, which matches
    // nothing, so `ArnNotLike` holds for every caller. The IAM policy
    // simulator allowed an `Allow` gated on `ArnNotLike` with this value in
    // an identity policy, two samples per `Version` (issue #834). The trust
    // write check accepts the value, which has six fields, and the AssumeRole
    // warning does not name it.
    let value = "${x, '}:::::'}";
    let allow = [stmt("Allow", "*", Some(arn_cond("ArnNotLike", value)))];
    assert_eq!(validate_trust_policy(&doc(&allow)), Ok(()));
    for version_2012 in [true, false] {
        assert!(
            trust_decision_for(&allow, CALLER, &ctx(), false, version_2012),
            "version_2012 {version_2012}"
        );
    }
    assert_eq!(stored_trust_arn_value(&doc(&allow)), None);
}

/// A federated user of the caller's account. Its ARN has the service `sts`,
/// so the five fields `arn:aws:iam::123456789012` do not match it.
const FEDERATED: &str = "arn:aws:sts::123456789012:federated-user/carol";

/// A value whose quoted default holds `}:`. Counted with each `${` ending at
/// the next `}`, as IAM counts the fields of an Arn value, it has five written
/// `:`, and the trust write check sees six fields. The evaluator reads the
/// reference up to its last `}`, so for a caller in account 123456789012 the
/// value expands to `arn:aws:iam::123456789012`, which has five fields. How
/// IAM evaluates such a value was not measured.
const QUOTED_COLON: &str = "arn:aws:iam::${aws:PrincipalAccount, '}:'}";

/// For `ArnLike` and `ArnNotLike` and for each effect, whether alice and the
/// federated user, both in account 123456789012, may assume a role whose
/// trust policy holds the effect gated on the operator with `value`, with
/// `Version` `2012-10-17` when `version_2012` holds and `2008-10-17`
/// otherwise. A `Deny` comes with an unconditional `Allow`.
fn by_caller(version_2012: bool, value: &str) -> Vec<(&'static str, &'static str, [bool; 2])> {
    let mut federated = ctx();
    federated.principal_arn = FEDERATED.to_owned();
    let mut rows = Vec::new();
    for op in ["ArnLike", "ArnNotLike"] {
        for effect in ["Allow", "Deny"] {
            let gated = stmt(effect, "*", Some(arn_cond(op, value)));
            let statements = if effect == "Deny" {
                vec![stmt("Allow", "*", None), gated]
            } else {
                vec![gated]
            };
            let decisions = [
                trust_decision_for(&statements, CALLER, &ctx(), false, version_2012),
                trust_decision_for(&statements, FEDERATED, &federated, false, version_2012),
            ];
            rows.push((op, effect, decisions));
        }
    }
    rows
}

#[test]
fn a_value_whose_quoted_default_leaves_five_fields_fails_closed() {
    // The trust write check accepts the value. Its statement fails closed
    // under both operators and both effects, for a caller the five fields
    // match and for one they do not, and the AssumeRole warning names it.
    let d = doc(&[stmt("Allow", "*", Some(arn_cond("ArnLike", QUOTED_COLON)))]);
    assert_eq!(validate_trust_policy(&d), Ok(()));
    let closed = [false, false];
    assert_eq!(
        by_caller(true, QUOTED_COLON),
        [
            ("ArnLike", "Allow", closed),
            ("ArnLike", "Deny", closed),
            ("ArnNotLike", "Allow", closed),
            ("ArnNotLike", "Deny", closed),
        ]
    );
    assert_eq!(
        stored_trust_arn_value(&d),
        Some(("aws:PrincipalArn".to_owned(), QUOTED_COLON.to_owned()))
    );
}

#[test]
fn a_value_whose_quoted_default_holds_a_colon_is_compared_as_written_in_a_2008_document() {
    // A `2008-10-17` trust policy expands no variable, so the value has six
    // fields and its account field is the text `${aws`, which no caller's ARN
    // holds.
    let mut d = doc(&[stmt("Allow", "*", Some(arn_cond("ArnLike", QUOTED_COLON)))]);
    d["Version"] = json!("2008-10-17");
    assert_eq!(validate_trust_policy(&d), Ok(()));
    assert_eq!(
        by_caller(false, QUOTED_COLON),
        [
            ("ArnLike", "Allow", [false, false]),
            ("ArnLike", "Deny", [true, true]),
            ("ArnNotLike", "Allow", [true, true]),
            ("ArnNotLike", "Deny", [false, false]),
        ]
    );
    assert_eq!(stored_trust_arn_value(&d), None);
}
