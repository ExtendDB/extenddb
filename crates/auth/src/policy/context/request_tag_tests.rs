// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `RequestContext` resolution of `aws:TagKeys` and `aws:RequestTag/<key>`.

use super::*;

fn ctx_with(tags: &[(&str, &str)], keys: &[&str]) -> RequestContext {
    RequestContext::build(
        HashMap::new(),
        HashMap::new(),
        false,
        RequestParams {
            request_tags: Some(
                tags.iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ),
            request_tag_keys: Some(keys.iter().map(|k| (*k).to_owned()).collect()),
            ..Default::default()
        },
    )
}

#[test]
fn no_tag_fields_leave_both_keys_absent() {
    let ctx = RequestContext::build(
        HashMap::new(),
        HashMap::new(),
        false,
        RequestParams::default(),
    );
    assert_eq!(ctx.resolve_key("aws:TagKeys"), None);
    assert_eq!(ctx.resolve_key("aws:RequestTag/env"), None);
    assert!(!ctx.is_unresolvable_key("aws:TagKeys"));
    assert!(!ctx.is_unresolvable_key("aws:RequestTag/env"));
}

#[test]
fn empty_tag_set_and_empty_key_list_leave_both_keys_absent() {
    // An operation that carries a tag field with nothing in it populates
    // neither key, so a Null:true condition on either sees an absent key.
    let ctx = ctx_with(&[], &[]);
    assert_eq!(ctx.request_tags, None);
    assert_eq!(ctx.request_tag_keys, None);
    assert_eq!(ctx.resolve_key("aws:TagKeys"), None);
    assert_eq!(ctx.resolve_key("aws:RequestTag/env"), None);
}

#[test]
fn present_tags_resolve_both_keys() {
    let ctx = ctx_with(&[("env", "prod"), ("owner", "ops")], &["env", "owner"]);
    assert_eq!(ctx.resolve_key("aws:TagKeys"), Some(vec!["env", "owner"]));
    assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec!["prod"]));
    assert_eq!(ctx.resolve_key("aws:RequestTag/owner"), Some(vec!["ops"]));
    // The prefix and the tag name are matched without regard to case.
    assert_eq!(ctx.resolve_key("AWS:TAGKEYS"), Some(vec!["env", "owner"]));
    assert_eq!(ctx.resolve_key("aws:requesttag/ENV"), Some(vec!["prod"]));
    assert_eq!(ctx.resolve_key("aws:RequestTag/missing"), None);
    assert!(!ctx.is_unresolvable_key("aws:RequestTag/missing"));
}

#[test]
fn tagkeys_is_multivalued_and_the_request_tag_is_not() {
    let ctx = ctx_with(&[("env", "prod")], &["env"]);
    assert!(ctx.is_multivalued_key("aws:TagKeys"));
    assert!(ctx.is_multivalued_key("AWS:TAGKEYS"));
    assert!(ctx.is_multivalued_key("dynamodb:LeadingKeys"));
    assert!(ctx.is_multivalued_key("dynamodb:Attributes"));
    assert!(!ctx.is_multivalued_key("aws:RequestTag/env"));
    assert!(!ctx.is_multivalued_key("aws:PrincipalTag/env"));
}

#[test]
fn absent_key_negated_rule_applies_to_tagkeys_only() {
    let ctx = ctx_with(&[], &[]);
    assert!(ctx.bare_negated_matches_absent_multivalued_key("aws:TagKeys"));
    assert!(ctx.bare_negated_matches_absent_multivalued_key("aws:tagkeys"));
    assert!(!ctx.bare_negated_matches_absent_multivalued_key("dynamodb:LeadingKeys"));
    assert!(!ctx.bare_negated_matches_absent_multivalued_key("dynamodb:Attributes"));
}

#[test]
fn case_variant_keys_with_one_value_resolve_to_it() {
    // Env=prod and ENV=prod fold to one name and carry one value: the key
    // reads prod whichever spelling the policy uses.
    let ctx = ctx_with(&[("Env", "prod"), ("ENV", "prod")], &["Env", "ENV"]);
    assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec!["prod"]));
    assert_eq!(ctx.resolve_key("aws:RequestTag/Env"), Some(vec!["prod"]));
    assert!(!ctx.is_unresolvable_key("aws:RequestTag/env"));
}

#[test]
fn case_variant_keys_with_differing_values_fail_closed() {
    // Env=prod and env=dev are stored as two tags by the engine. The key has
    // no single value, so it resolves to nothing and is reported unresolvable:
    // a Deny on it blocks and an Allow on it does not grant. Both orders.
    for tags in [
        [("Env", "prod"), ("env", "dev")],
        [("env", "dev"), ("Env", "prod")],
    ] {
        let ctx = ctx_with(&tags, &["Env", "env"]);
        for key in [
            "aws:RequestTag/env",
            "aws:RequestTag/Env",
            "aws:RequestTag/ENV",
        ] {
            assert_eq!(ctx.resolve_key(key), None, "{key} resolves to nothing");
            assert!(ctx.is_unresolvable_key(key), "{key} fails closed");
        }
        // Another tag on the same request is unaffected.
        let ctx = ctx_with(&[("Env", "prod"), ("env", "dev"), ("owner", "ops")], &[]);
        assert_eq!(ctx.resolve_key("aws:RequestTag/owner"), Some(vec!["ops"]));
        assert!(!ctx.is_unresolvable_key("aws:RequestTag/owner"));
    }
}

#[test]
fn ambiguous_request_tag_fails_the_statement_closed() {
    use crate::policy::condition::{ConditionMatch, evaluate_statement_conditions};
    use crate::policy::document::{Condition, ConditionOperator};

    let ctx = ctx_with(&[("Env", "prod"), ("env", "dev")], &["Env", "env"]);
    let deny_on_prod = [Condition {
        operator: ConditionOperator::StringEquals,
        key: "aws:RequestTag/env".to_owned(),
        values: vec!["prod".to_owned()],
    }];
    assert_eq!(
        evaluate_statement_conditions(&deny_on_prod, &ctx),
        ConditionMatch::Unresolvable
    );
    // The same statement against a request naming the tag once reads its value.
    let one = ctx_with(&[("env", "prod")], &["env"]);
    assert_eq!(
        evaluate_statement_conditions(&deny_on_prod, &one),
        ConditionMatch::Match
    );
}

#[test]
fn ambiguous_request_tag_as_a_policy_variable_fails_the_statement_closed() {
    use crate::policy::condition::{ConditionMatch, evaluate_statement_conditions};
    use crate::policy::document::{Condition, ConditionOperator};

    let mut resource = HashMap::new();
    resource.insert("env".to_owned(), "prod".to_owned());
    let with_resource_tag = |tags: &[(&str, &str)]| {
        let mut ctx = ctx_with(tags, &[]);
        ctx.resource_tags = resource.clone();
        ctx
    };
    let deny = [Condition {
        operator: ConditionOperator::StringEquals,
        key: "aws:ResourceTag/env".to_owned(),
        values: vec!["${aws:RequestTag/env}".to_owned()],
    }];
    // One spelling expands to its value.
    for tags in [[("env", "prod")], [("Env", "prod")]] {
        assert_eq!(
            evaluate_statement_conditions(&deny, &with_resource_tag(&tags)),
            ConditionMatch::Match
        );
    }
    // Two spellings with differing values have no value to expand to.
    for tags in [
        [("Env", "prod"), ("env", "dev")],
        [("env", "dev"), ("Env", "prod")],
    ] {
        assert_eq!(
            evaluate_statement_conditions(&deny, &with_resource_tag(&tags)),
            ConditionMatch::Unresolvable
        );
    }
    // The variable inside a longer value, and a negated operator, fail closed too.
    let embedded = [Condition {
        operator: ConditionOperator::StringNotEquals,
        key: "aws:ResourceTag/env".to_owned(),
        values: vec!["x-${aws:RequestTag/env}-y".to_owned()],
    }];
    assert_eq!(
        evaluate_statement_conditions(
            &embedded,
            &with_resource_tag(&[("Env", "prod"), ("env", "dev")])
        ),
        ConditionMatch::Unresolvable
    );
    // An absent request tag is not ambiguous: the variable stays literal, as before.
    assert_eq!(
        evaluate_statement_conditions(&deny, &with_resource_tag(&[("team", "x")])),
        ConditionMatch::NoMatch
    );
    // A brace that never closes names no variable.
    let unclosed = [Condition {
        operator: ConditionOperator::StringEquals,
        key: "aws:ResourceTag/env".to_owned(),
        values: vec!["${aws:RequestTag/env".to_owned()],
    }];
    assert_eq!(
        evaluate_statement_conditions(
            &unclosed,
            &with_resource_tag(&[("Env", "prod"), ("env", "dev")])
        ),
        ConditionMatch::NoMatch
    );
}

#[test]
fn policy_variable_naming_aws_tagkeys_is_left_literal_for_every_tag_order() {
    use crate::policy::condition::{ConditionMatch, evaluate_statement_conditions};
    use crate::policy::document::{Condition, ConditionOperator};

    // A Deny on aws:ResourceTag/team equal to ${aws:TagKeys} on a table tagged
    // team=env. The variable names a multivalued key, so it stays literal and
    // the statement applies to neither order of the request's tag keys.
    let deny = [Condition {
        operator: ConditionOperator::StringEquals,
        key: "aws:ResourceTag/team".to_owned(),
        values: vec!["${aws:TagKeys}".to_owned()],
    }];
    for keys in [["env", "owner"], ["owner", "env"]] {
        let mut ctx = ctx_with(&[("env", "x"), ("owner", "x")], &keys);
        ctx.resource_tags
            .insert("team".to_owned(), "env".to_owned());
        assert_eq!(
            evaluate_statement_conditions(&deny, &ctx),
            ConditionMatch::NoMatch,
            "tag keys {keys:?}"
        );
    }
}

/// An `AssumeRoleContext` for an IAM user caller with the given principal tags
/// and session tags.
fn assume_role_ctx(principal: &[(&str, &str)], session: &[(&str, &str)]) -> AssumeRoleContext {
    let owned = |tags: &[(&str, &str)]| -> HashMap<String, String> {
        tags.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    };
    AssumeRoleContext {
        principal_tags: owned(principal),
        external_id: None,
        session_name: Some("probe".to_owned()),
        principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
        principal_account: Some("123456789012".to_owned()),
        request_tags: owned(session),
        region: Some("us-east-1".to_owned()),
        secure_transport: true,
        principal_type: Some("User".to_owned()),
        username: Some("alice".to_owned()),
        source_ip: None,
        user_agent: None,
        userid: None,
        resource_account: Some("123456789012".to_owned()),
        current_time: "2026-01-01T00:00:00Z".to_owned(),
        epoch_time: "1767225600".to_owned(),
    }
}

#[test]
fn trust_policy_variable_naming_aws_tagkeys_is_left_literal() {
    use crate::policy::condition::{ConditionMatch, evaluate_statement_conditions};
    use crate::policy::document::{Condition, ConditionOperator};

    // AssumeRoleContext reports aws:TagKeys as multivalued, so a trust policy
    // condition value ${aws:TagKeys} stays literal text. Expanded, it would be
    // the first session tag key in the map's iteration order, which a test
    // cannot choose. One context holds both session tag keys in one map, and
    // each of its two principal tags holds one key, so an expansion to either
    // key matches the condition on the principal tag that holds it.
    let compare_with_tag_keys = |principal_tag: &str| {
        [Condition {
            operator: ConditionOperator::StringEquals,
            key: format!("aws:PrincipalTag/{principal_tag}"),
            values: vec!["${aws:TagKeys}".to_owned()],
        }]
    };
    let session = [("env", "x"), ("owner", "x")];
    let ctx = assume_role_ctx(&[("key_env", "env"), ("key_owner", "owner")], &session);
    for tag in ["key_env", "key_owner"] {
        assert_eq!(
            evaluate_statement_conditions(&compare_with_tag_keys(tag), &ctx),
            ConditionMatch::NoMatch,
            "${{aws:TagKeys}} expanded to the session tag key in {tag}"
        );
    }
    // The variable is text, so it equals the same text.
    let ctx = assume_role_ctx(&[("team", "${aws:TagKeys}")], &session);
    assert_eq!(
        evaluate_statement_conditions(&compare_with_tag_keys("team"), &ctx),
        ConditionMatch::Match
    );
}

#[test]
fn an_empty_request_tag_value_is_a_present_key() {
    use crate::policy::condition::evaluate_condition;
    use crate::policy::document::{Condition, ConditionOperator};

    // An empty tag value is a value, so the key is present: Null:false matches
    // and Null:true does not.
    let ctx = ctx_with(&[("env", "")], &["env"]);
    assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec![""]));
    assert!(!ctx.is_unresolvable_key("aws:RequestTag/env"));
    let null = |value: &str| Condition {
        operator: ConditionOperator::Null,
        key: "aws:RequestTag/env".to_owned(),
        values: vec![value.to_owned()],
    };
    assert!(evaluate_condition(&null("false"), &ctx));
    assert!(!evaluate_condition(&null("true"), &ctx));
}

#[test]
fn request_tag_names_match_beyond_ascii_case() {
    // A tag name that differs from the policy's only in the case of a non-ASCII
    // letter matches: a Deny on aws:RequestTag/Étag applies to a request that
    // carries étag (iam simulate-custom-policy, 2026-10-27, two samples).
    //
    // ExtendDB folds the sharp s to ss as well, so Straße and STRASSE are one
    // name here. On the service they are two: the same Deny does not apply to
    // the other spelling, on iam simulate-custom-policy and on a resource policy
    // on a DynamoDB table (2026-10-27, two samples each). That difference is
    // not asserted here.
    let ctx = ctx_with(&[("étag", "x")], &["étag"]);
    assert_eq!(ctx.resolve_key("aws:RequestTag/étag"), Some(vec!["x"]));
    assert_eq!(ctx.resolve_key("aws:RequestTag/Étag"), Some(vec!["x"]));
    let ctx = ctx_with(&[("Étag", "x")], &["Étag"]);
    assert_eq!(ctx.resolve_key("aws:RequestTag/étag"), Some(vec!["x"]));
    // Two spellings that differ in case and carry differing values are ambiguous.
    let ctx = ctx_with(&[("Étag", "x"), ("étag", "y")], &["Étag", "étag"]);
    assert_eq!(ctx.resolve_key("aws:RequestTag/étag"), None);
    assert!(ctx.is_unresolvable_key("aws:RequestTag/étag"));
}

#[test]
fn request_tag_lookups_do_not_scan_the_request_tags() {
    // A request body can carry tens of thousands of tags and a policy can read
    // aws:RequestTag/<name> in many conditions. Each lookup probes the index
    // built with the context, so 3,000 lookups against 20,000 tags take
    // milliseconds. Folding every tag name on every lookup folds 60 million
    // names, which takes seconds even in a release build.
    let tags: HashMap<String, String> = (0..20_000)
        .map(|i| (format!("t{i}"), "v".to_owned()))
        .collect();
    let ctx = RequestContext::build(
        HashMap::new(),
        HashMap::new(),
        false,
        RequestParams {
            request_tags: Some(tags),
            ..Default::default()
        },
    );
    let started = std::time::Instant::now();
    for i in 0..1_500 {
        let key = format!("aws:RequestTag/absent{i}");
        assert_eq!(ctx.resolve_key(&key), None);
        assert!(!ctx.is_unresolvable_key(&key));
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "3,000 lookups against 20,000 tags took {elapsed:?}"
    );
    assert_eq!(ctx.resolve_key("aws:RequestTag/t19999"), Some(vec!["v"]));
}

#[test]
fn principal_and_resource_tag_lookup_keep_the_exact_first_rule() {
    // Only aws:RequestTag changed. The neighbouring tag keys still resolve a
    // byte-exact stored key first and otherwise the smallest fold-matching key.
    let mut principal = HashMap::new();
    principal.insert("Env".to_owned(), "prod".to_owned());
    principal.insert("env".to_owned(), "dev".to_owned());
    let ctx = RequestContext::build(
        principal.clone(),
        principal,
        false,
        RequestParams::default(),
    );
    assert_eq!(ctx.resolve_key("aws:PrincipalTag/env"), Some(vec!["dev"]));
    assert_eq!(ctx.resolve_key("aws:PrincipalTag/Env"), Some(vec!["prod"]));
    assert_eq!(ctx.resolve_key("aws:PrincipalTag/ENV"), Some(vec!["prod"]));
    assert_eq!(ctx.resolve_key("aws:ResourceTag/env"), Some(vec!["dev"]));
    assert!(!ctx.is_unresolvable_key("aws:PrincipalTag/ENV"));
}
