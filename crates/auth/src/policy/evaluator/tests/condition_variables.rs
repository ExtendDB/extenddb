// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy variables in condition values (issue #534).
//!
//! One test per cell measured with the IAM policy simulator on 2026-10-24:
//! `Allow dynamodb:*` on `*` beside a `Deny dynamodb:GetItem` on `*` with the
//! condition under test, action `dynamodb:GetItem`. Cells E1 to E9b are
//! measured; the tests after them apply the nearest measured rule to shapes
//! the cells do not cover and say so. The cells measured on 2026-10-25,
//! including a wildcard that comes from a substituted value, are in
//! `condition_variables_operators.rs`.

use super::*;

const TEAM: &str = "aws:PrincipalTag/team";
const OWNER: &str = "aws:ResourceTag/owner";
const SRC: &str = "aws:SourceArn";
const USER: &str = "arn:aws:iam::123456789012:user/";
const BLUE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue";

fn allow_all() -> PolicyDocument {
    parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
        }]}"#,
    )
}

/// `Allow dynamodb:* on *` beside `Deny dynamodb:GetItem on *` if
/// `operator key: value`, in a document with the given `Version`.
fn deny_if(version: Option<&str>, operator: &str, key: &str, value: &str) -> Vec<PolicyDocument> {
    let version = version.map_or(String::new(), |v| format!(r#""Version":"{v}","#));
    vec![
        allow_all(),
        parse(&format!(
            r#"{{{version}"Statement":[{{
                "Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
                "Condition":{{"{operator}":{{"{key}":"{value}"}}}}
            }}]}}"#
        )),
    ]
}

fn get(policies: &[PolicyDocument], ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(policies, None, None, "dynamodb:GetItem", BLUE, ctx)
}

fn team(value: &str) -> Ctx {
    Ctx::empty().with(TEAM, vec![value])
}

fn owner(value: &str) -> Ctx {
    Ctx::empty().with(OWNER, vec![value])
}

const V2012: Option<&str> = Some("2012-10-17");
const V2008: Option<&str> = Some("2008-10-17");

// --- Rule 2: escapes in a 2012-10-17 document ---

#[test]
fn e1_string_like_escaped_star_does_not_match_text() {
    let p = deny_if(V2012, "StringLike", TEAM, "a${*}");
    assert_eq!(get(&p, &team("abc")), AuthzDecision::Allow);
}

#[test]
fn e2_string_like_escaped_star_matches_literal_star() {
    let p = deny_if(V2012, "StringLike", TEAM, "a${*}");
    assert_eq!(get(&p, &team("a*")), AuthzDecision::Deny);
}

#[test]
fn e2b_string_like_written_star_is_a_wildcard() {
    let p = deny_if(V2012, "StringLike", TEAM, "a*");
    assert_eq!(get(&p, &team("abc")), AuthzDecision::Deny);
}

#[test]
fn e3_string_equals_escaped_question_is_the_character() {
    let p = deny_if(V2012, "StringEquals", TEAM, "${?}");
    assert_eq!(get(&p, &team("?")), AuthzDecision::Deny);
}

#[test]
fn e3b_string_like_escaped_question_does_not_match_one_character() {
    let p = deny_if(V2012, "StringLike", TEAM, "a${?}");
    assert_eq!(get(&p, &team("ab")), AuthzDecision::Allow);
}

#[test]
fn e3c_string_like_escaped_question_matches_literal_question() {
    let p = deny_if(V2012, "StringLike", TEAM, "a${?}");
    assert_eq!(get(&p, &team("a?")), AuthzDecision::Deny);
}

#[test]
fn e4_string_equals_escaped_dollar_is_the_character() {
    let p = deny_if(V2012, "StringEquals", TEAM, "${$}");
    assert_eq!(get(&p, &team("$")), AuthzDecision::Deny);
}

#[test]
fn e4b_escaped_dollar_result_is_not_expanded_again() {
    let p = deny_if(V2012, "StringEquals", TEAM, "a${$}{x}");
    assert_eq!(get(&p, &team("a${x}")), AuthzDecision::Deny);
}

// --- Rule 1: 2008-10-17 and no-Version documents compare the text as written ---

#[test]
fn e5_version_2008_escape_is_not_the_character() {
    let p = deny_if(V2008, "StringEquals", TEAM, "${*}");
    assert_eq!(get(&p, &team("*")), AuthzDecision::Allow);
}

#[test]
fn e5b_version_2008_escape_is_literal_text() {
    let p = deny_if(V2008, "StringEquals", TEAM, "${*}");
    assert_eq!(get(&p, &team("${*}")), AuthzDecision::Deny);
}

#[test]
fn e6_no_version_variable_is_not_expanded() {
    let p = deny_if(None, "StringEquals", OWNER, "${aws:username}");
    let ctx = owner("alice").with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
}

#[test]
fn e6b_no_version_variable_is_literal_text() {
    let p = deny_if(None, "StringEquals", OWNER, "${aws:username}");
    let ctx = owner("${aws:username}").with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

#[test]
fn e6c_version_2008_variable_is_literal_text() {
    let p = deny_if(V2008, "StringEquals", OWNER, "${aws:username}");
    let ctx = owner("${aws:username}").with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

#[test]
fn version_2012_variable_is_expanded() {
    // The issue's second row: the same statement as E6 under 2012-10-17.
    let p = deny_if(V2012, "StringEquals", OWNER, "${aws:username}");
    let ctx = owner("alice").with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

// --- Rule 2 for ArnLike ---

#[test]
fn e7_arn_like_escaped_star_does_not_match_text() {
    let p = deny_if(V2012, "ArnLike", SRC, &format!("{USER}a${{*}}"));
    let ctx = Ctx::empty().with(SRC, vec![&format!("{USER}abc")]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
}

#[test]
fn e7b_arn_like_escaped_star_matches_literal_star() {
    let p = deny_if(V2012, "ArnLike", SRC, &format!("{USER}a${{*}}"));
    let ctx = Ctx::empty().with(SRC, vec![&format!("{USER}a*")]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

// --- Rule 3: the default form ---

#[test]
fn e8_default_form_expands_when_key_absent() {
    let p = deny_if(V2012, "StringEquals", OWNER, "${aws:username, 'anon'}");
    assert_eq!(get(&p, &owner("anon")), AuthzDecision::Deny);
}

#[test]
fn e8b_version_2008_default_form_is_literal_text() {
    let p = deny_if(V2008, "StringEquals", OWNER, "${aws:username, 'anon'}");
    assert_eq!(get(&p, &owner("anon")), AuthzDecision::Allow);
}

// --- Rule 4: an unresolved variable matches nothing ---

#[test]
fn e9_unresolved_variable_does_not_match_its_own_text() {
    let p = deny_if(V2012, "StringEquals", OWNER, "${aws:username}");
    assert_eq!(get(&p, &owner("${aws:username}")), AuthzDecision::Allow);
}

#[test]
fn e9b_unresolved_variable_negated_operator_holds() {
    let p = deny_if(V2012, "StringNotEquals", OWNER, "${aws:username}");
    assert_eq!(get(&p, &owner("alice")), AuthzDecision::Deny);
}

// --- Derived from the cells above; not measured ---

#[test]
fn derived_unresolved_variable_among_several_values_is_skipped() {
    // Rule 4 per value: the resolved value still matches, the unresolved one
    // matches nothing.
    let p = vec![
        allow_all(),
        parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
                "Condition":{"StringEquals":{"aws:ResourceTag/owner":["${aws:username}","bob"]}}
            }]}"#,
        ),
    ];
    assert_eq!(get(&p, &owner("bob")), AuthzDecision::Deny);
    assert_eq!(get(&p, &owner("${aws:username}")), AuthzDecision::Allow);
    assert_eq!(get(&p, &owner("alice")), AuthzDecision::Allow);
}

#[test]
fn derived_unresolved_variable_negated_like_and_ignore_case_hold() {
    // Rule 4 for the other negative string operators.
    let p = deny_if(V2012, "StringNotLike", OWNER, "${aws:username}");
    assert_eq!(get(&p, &owner("alice")), AuthzDecision::Deny);
    let p = deny_if(V2012, "StringNotEqualsIgnoreCase", OWNER, "${aws:username}");
    assert_eq!(get(&p, &owner("alice")), AuthzDecision::Deny);
    let p = deny_if(V2012, "StringLike", OWNER, "${aws:username}");
    assert_eq!(get(&p, &owner("${aws:username}")), AuthzDecision::Allow);
}

#[test]
fn derived_ignore_case_operators_read_escapes() {
    // Rule 2 for StringEqualsIgnoreCase: the escape is the character and the
    // comparison stays case-insensitive on the rest.
    let p = deny_if(V2012, "StringEqualsIgnoreCase", TEAM, "A${*}");
    assert_eq!(get(&p, &team("a*")), AuthzDecision::Deny);
    assert_eq!(get(&p, &team("abc")), AuthzDecision::Allow);
}

#[test]
fn derived_set_operators_with_unresolved_variable() {
    // Rule 4 under ForAnyValue and ForAllValues on a multivalued key: the
    // unresolved value matches none of the context values, so a positive
    // operator fails and a negative one holds.
    let p = deny_if(
        V2012,
        "ForAnyValue:StringEquals",
        "dynamodb:LeadingKeys",
        "${aws:username}",
    );
    let ctx = Ctx::empty().with("dynamodb:LeadingKeys", vec!["${aws:username}", "k"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
    let p = deny_if(
        V2012,
        "ForAllValues:StringNotEquals",
        "dynamodb:LeadingKeys",
        "${aws:username}",
    );
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

#[test]
fn derived_set_operator_escaped_star_is_literal() {
    // Rule 2 under ForAnyValue:StringLike.
    let p = deny_if(
        V2012,
        "ForAnyValue:StringLike",
        "dynamodb:LeadingKeys",
        "a${*}",
    );
    let ctx = Ctx::empty().with("dynamodb:LeadingKeys", vec!["abc", "axy"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
    let ctx = Ctx::empty().with("dynamodb:LeadingKeys", vec!["abc", "a*"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
}

#[test]
fn derived_date_and_address_values_with_a_variable_are_not_expanded() {
    use crate::policy::condition::{ConditionMatch, evaluate_statement_conditions};
    use crate::policy::document::parse_conditions;
    // IAM's operator reference lists policy variables as not supported for
    // the date and IP address operators. The value is compared as written, the
    // rule measured for `NumericEquals` (cell C4a), so it parses as neither a
    // date nor an address and the condition does not match.
    let p = deny_if(
        V2012,
        "DateEquals",
        "aws:CurrentTime",
        "${aws:PrincipalTag/when}",
    );
    let ctx = Ctx::empty()
        .with("aws:CurrentTime", vec!["2026-10-28T00:00:00Z"])
        .with("aws:PrincipalTag/when", vec!["2026-10-28T00:00:00Z"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow, "DateEquals");
    // An identity policy with this `IpAddress` value does not parse (a
    // malformed CIDR). A trust policy's conditions go through
    // `parse_conditions` alone, so the trust evaluator reaches this value.
    let conditions = parse_conditions(&serde_json::json!({
        "IpAddress": {"aws:SourceIp": "${aws:PrincipalTag/net}"}
    }))
    .unwrap();
    let ctx = Ctx::empty()
        .with("aws:SourceIp", vec!["10.1.2.3"])
        .with("aws:PrincipalTag/net", vec!["10.0.0.0/8"]);
    assert_eq!(
        evaluate_statement_conditions(&conditions, true, &ctx),
        ConditionMatch::NoMatch,
        "IpAddress"
    );
}

#[test]
fn version_2008_string_like_wildcards_still_match() {
    // Rule 1 leaves written wildcards as wildcards: only variable expansion is
    // off in a 2008-10-17 document.
    let p = deny_if(V2008, "StringLike", TEAM, "a*");
    assert_eq!(get(&p, &team("abc")), AuthzDecision::Deny);
}

#[test]
fn permissions_boundary_and_session_policy_take_their_own_version() {
    // The flag comes from the document being evaluated: a 2008-10-17 boundary
    // compares its condition value as written while the identity policy is
    // 2012-10-17.
    let identity = allow_all();
    let boundary_2008 = parse(
        r#"{"Version":"2008-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
            "Condition":{"StringEquals":{"aws:ResourceTag/owner":"${aws:username}"}}
        }]}"#,
    );
    let ctx = owner("alice").with("aws:username", vec!["alice"]);
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&identity),
            Some(&boundary_2008),
            None,
            "dynamodb:GetItem",
            BLUE,
            &ctx
        ),
        AuthzDecision::Deny
    );
    let session_2012 = parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
            "Condition":{"StringEquals":{"aws:ResourceTag/owner":"${aws:username}"}}
        }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&identity),
            None,
            Some(&session_2012),
            "dynamodb:GetItem",
            BLUE,
            &ctx
        ),
        AuthzDecision::Allow
    );
}

#[test]
fn explicit_deny_matches_arc_takes_the_document_version() {
    let arcs = |docs: Vec<PolicyDocument>| -> Vec<std::sync::Arc<PolicyDocument>> {
        docs.into_iter().map(std::sync::Arc::new).collect()
    };
    let ctx = owner("alice").with("aws:username", vec!["alice"]);
    let deny_2008 = arcs(deny_if(V2008, "StringEquals", OWNER, "${aws:username}"));
    assert!(!explicit_deny_matches_arc(
        &deny_2008,
        None,
        None,
        "dynamodb:GetItem",
        BLUE,
        &ctx
    ));
    let deny_2012 = arcs(deny_if(V2012, "StringEquals", OWNER, "${aws:username}"));
    assert!(explicit_deny_matches_arc(
        &deny_2012,
        None,
        None,
        "dynamodb:GetItem",
        BLUE,
        &ctx
    ));
}
