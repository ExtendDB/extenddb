// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for policy variable parsing, expansion and token matching.

use super::*;
use std::collections::HashMap;

struct Ctx(HashMap<String, Vec<String>>);

impl Ctx {
    fn empty() -> Self {
        Self(HashMap::new())
    }

    fn with(mut self, key: &str, values: &[&str]) -> Self {
        self.0.insert(
            key.to_owned(),
            values.iter().map(|v| (*v).to_owned()).collect(),
        );
        self
    }
}

impl ConditionContext for Ctx {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        self.0
            .get(key)
            .map(|v| v.iter().map(String::as_str).collect())
    }

    fn is_multivalued_key(&self, _key: &str) -> bool {
        false
    }
}

const TEAM: &str = "aws:PrincipalTag/team";
const BLUE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue";

fn team_blue() -> Ctx {
    Ctx::empty().with(TEAM, &["blue"])
}

// --- PolicyDocument::expands_policy_variables ---

#[test]
fn only_version_2012_expands_variables() {
    let stmt = r#""Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]"#;
    let v2012 =
        PolicyDocument::from_json(&format!(r#"{{"Version":"2012-10-17",{stmt}}}"#)).unwrap();
    assert!(v2012.expands_policy_variables());
    let v2008 =
        PolicyDocument::from_json(&format!(r#"{{"Version":"2008-10-17",{stmt}}}"#)).unwrap();
    assert_eq!(v2008.version.as_deref(), Some("2008-10-17"));
    assert!(!v2008.expands_policy_variables());
    let none = PolicyDocument::from_json(&format!("{{{stmt}}}")).unwrap();
    assert_eq!(none.version, None);
    assert!(!none.expands_policy_variables());
}

#[test]
fn a_write_refuses_a_version_that_is_not_one_of_the_two_values() {
    let stmt = r#""Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]"#;
    for version in [
        r#""2012-10-18""#,
        r#""2012-10-17 ""#,
        "20121017",
        "null",
        r#"["2012-10-17"]"#,
        r#""2012-10-17T""#,
    ] {
        let json = format!(r#"{{"Version":{version},{stmt}}}"#);
        let refused = PolicyDocument::from_json_for_write(&json)
            .err()
            .map(|e| e.0);
        let expected =
            format!("invalid Version (must be the string 2012-10-17 or 2008-10-17): {version}");
        assert_eq!(refused, Some(expected), "{version}");
        // A stored document holding the value still parses, and does not expand.
        let stored = PolicyDocument::from_json(&json).unwrap();
        assert!(!stored.expands_policy_variables(), "{version}");
    }
    for version in ["2012-10-17", "2008-10-17"] {
        let json = format!(r#"{{"Version":"{version}",{stmt}}}"#);
        assert!(
            PolicyDocument::from_json_for_write(&json).is_ok(),
            "{version}"
        );
    }
    // A missing Version is refused by each write path before parsing.
    assert!(PolicyDocument::from_json_for_write(&format!("{{{stmt}}}")).is_ok());
}

// --- parse_reference ---

#[test]
fn parse_plain_key() {
    assert_eq!(
        parse_reference("aws:username"),
        Reference::Key {
            name: "aws:username",
            default: None
        }
    );
}

#[test]
fn parse_default_form_with_and_without_space() {
    for inner in [
        "aws:username, 'anon'",
        "aws:username,'anon'",
        " aws:username , 'anon' ",
    ] {
        assert_eq!(
            parse_reference(inner),
            Reference::Key {
                name: "aws:username",
                default: Some("anon")
            },
            "{inner}"
        );
    }
}

#[test]
fn parse_comma_without_quotes_is_key_text() {
    assert_eq!(
        parse_reference("a, b"),
        Reference::Key {
            name: "a, b",
            default: None
        }
    );
}

#[test]
fn parse_escapes() {
    assert_eq!(parse_reference("*"), Reference::Escape(b'*'));
    assert_eq!(parse_reference("?"), Reference::Escape(b'?'));
    assert_eq!(parse_reference("$"), Reference::Escape(b'$'));
}

// --- resource_pattern_matches ---

#[test]
fn no_variable_is_arn_match() {
    let ctx = Ctx::empty();
    assert!(resource_pattern_matches(
        "arn:aws:dynamodb:*:*:table/bl*",
        BLUE,
        true,
        &ctx
    ));
    assert!(resource_pattern_matches("*", BLUE, true, &ctx));
    assert!(!resource_pattern_matches(
        "arn:aws:dynamodb:*:*:table/red",
        BLUE,
        true,
        &ctx
    ));
}

#[test]
fn variable_expands_when_enabled_only() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
    assert!(resource_pattern_matches(pattern, BLUE, true, &team_blue()));
    assert!(!resource_pattern_matches(
        pattern,
        BLUE,
        false,
        &team_blue()
    ));
}

#[test]
fn unresolved_variable_matches_nothing() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
    assert!(!resource_pattern_matches(
        pattern,
        BLUE,
        true,
        &Ctx::empty()
    ));
    assert!(!resource_pattern_matches(
        "${aws:PrincipalTag/team}*",
        BLUE,
        true,
        &Ctx::empty()
    ));
}

#[test]
fn default_used_when_key_absent() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team, 'blue'}";
    assert!(resource_pattern_matches(pattern, BLUE, true, &Ctx::empty()));
    let red = Ctx::empty().with(TEAM, &["red"]);
    assert!(!resource_pattern_matches(pattern, BLUE, true, &red));
}

#[test]
fn wildcard_after_variable_still_matches() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}*";
    let blue_x = "arn:aws:dynamodb:us-east-1:123456789012:table/blue-x";
    assert!(resource_pattern_matches(
        pattern,
        blue_x,
        true,
        &team_blue()
    ));
    assert!(!resource_pattern_matches(
        pattern,
        "arn:aws:dynamodb:us-east-1:123456789012:table/red",
        true,
        &team_blue()
    ));
}

#[test]
fn wildcard_inside_variable_value_is_literal() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
    let star = Ctx::empty().with(TEAM, &["*"]);
    assert!(!resource_pattern_matches(pattern, BLUE, true, &star));
    assert!(resource_pattern_matches(
        pattern,
        "arn:aws:dynamodb:us-east-1:123456789012:table/*",
        true,
        &star
    ));
}

#[test]
fn escapes_are_literal_characters() {
    let ctx = Ctx::empty();
    let star = "arn:aws:dynamodb:us-east-1:123456789012:table/${*}";
    assert!(!resource_pattern_matches(star, BLUE, true, &ctx));
    assert!(resource_pattern_matches(
        star,
        "arn:aws:dynamodb:us-east-1:123456789012:table/*",
        true,
        &ctx
    ));
    let question = "arn:aws:dynamodb:us-east-1:123456789012:table/${?}";
    assert!(!resource_pattern_matches(
        question,
        "arn:aws:dynamodb:us-east-1:123456789012:table/x",
        true,
        &ctx
    ));
    assert!(resource_pattern_matches(
        question,
        "arn:aws:dynamodb:us-east-1:123456789012:table/?",
        true,
        &ctx
    ));
    let dollar = "arn:aws:dynamodb:us-east-1:123456789012:table/${$}x";
    assert!(resource_pattern_matches(
        dollar,
        "arn:aws:dynamodb:us-east-1:123456789012:table/$x",
        true,
        &ctx
    ));
}

#[test]
fn a_present_single_valued_key_with_several_values_uses_the_first() {
    // The request context lists one value for a single-valued key; a second
    // value can only come from a test context. The first is used, as before.
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
    let ctx = Ctx::empty().with(TEAM, &["blue", "red"]);
    assert!(resource_pattern_matches(pattern, BLUE, true, &ctx));
}

#[test]
fn present_key_with_no_values_expands_empty() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
    let ctx = Ctx::empty().with(TEAM, &[]);
    assert!(resource_pattern_matches(
        pattern,
        "arn:aws:dynamodb:us-east-1:123456789012:table/",
        true,
        &ctx
    ));
    assert!(!resource_pattern_matches(pattern, BLUE, true, &ctx));
}

#[test]
fn field_count_rule_holds_after_expansion() {
    let pattern = "arn:aws:dynamodb:${aws:RequestedRegion}:*:table/blue";
    let ctx = Ctx::empty().with("aws:RequestedRegion", &["us-east-1"]);
    assert!(resource_pattern_matches(pattern, BLUE, true, &ctx));
    assert!(!resource_pattern_matches(
        pattern,
        "arn:aws:dynamodb:us-east-1:table/blue",
        true,
        &ctx
    ));
}

#[test]
fn unclosed_brace_is_literal_text() {
    let pattern = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws";
    assert!(resource_pattern_matches(
        pattern,
        "arn:aws:dynamodb:us-east-1:123456789012:table/${aws",
        true,
        &Ctx::empty()
    ));
    assert!(!resource_pattern_matches(
        pattern,
        BLUE,
        true,
        &Ctx::empty()
    ));
}

// --- expand_condition_value ---

#[test]
fn condition_value_expands_variable_and_default() {
    let ctx = team_blue();
    assert_eq!(
        expand_condition_value("team-${aws:PrincipalTag/team}", &ctx),
        "team-blue"
    );
    assert_eq!(
        expand_condition_value("${aws:username, 'anon'}", &ctx),
        "anon"
    );
    assert_eq!(
        expand_condition_value("${aws:PrincipalTag/team, 'none'}", &ctx),
        "blue"
    );
}

#[test]
fn condition_value_leaves_unresolved_and_escapes_literal() {
    let ctx = Ctx::empty();
    assert_eq!(
        expand_condition_value("${aws:username}", &ctx),
        "${aws:username}"
    );
    assert_eq!(
        expand_condition_value("a${*}b${?}c${$}", &ctx),
        "a${*}b${?}c${$}"
    );
    assert_eq!(expand_condition_value("plain", &ctx), "plain");
    assert_eq!(expand_condition_value("${open", &ctx), "${open");
}

#[test]
fn condition_value_present_key_without_values_is_empty() {
    let ctx = Ctx::empty().with(TEAM, &[]);
    assert_eq!(
        expand_condition_value("x${aws:PrincipalTag/team}y", &ctx),
        "xy"
    );
}
