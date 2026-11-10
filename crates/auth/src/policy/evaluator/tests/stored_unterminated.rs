// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A policy stored before writes refused an unterminated `${` (issue #834)
//! keeps evaluating as before: the value is compared as the literal text, so
//! a `Deny` gated on `StringEquals aws:ResourceTag/owner: "a${"` applies to a
//! table tagged `owner=a${` and not to one tagged `owner=a`. IAM never holds
//! such a policy, so this pins ExtendDB's own behaviour for stored documents.

use super::*;

const OWNER: &str = "aws:ResourceTag/owner";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue";

fn stored(version: &str) -> Vec<PolicyDocument> {
    vec![parse(&format!(
        r#"{{"Version":"{version}","Statement":[
            {{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}},
            {{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
              "Condition":{{"StringEquals":{{"{OWNER}":"a${{"}}}}}}]}}"#
    ))]
}

fn get(policies: &[PolicyDocument], owner: &str) -> AuthzDecision {
    evaluate_policies(
        policies,
        None,
        None,
        "dynamodb:GetItem",
        TABLE,
        &Ctx::empty().with(OWNER, vec![owner]),
    )
}

#[test]
fn a_stored_unterminated_value_is_compared_as_written() {
    for version in ["2012-10-17", "2008-10-17"] {
        let p = stored(version);
        assert_eq!(get(&p, "a${"), AuthzDecision::Deny, "{version}");
        assert_eq!(get(&p, "a"), AuthzDecision::Allow, "{version}");
    }
}

/// A stored value that holds a reference a write refuses is read as before
/// the write check: each `${` ends at the next `}`, quotes or not. Here
/// `a${aws:username, '}'` reads as the key `aws:username, '` (absent, no
/// default) and then the text `'`, so the value matches no tag and the `Deny`
/// never applies.
#[test]
fn a_stored_refused_reference_reads_as_before() {
    let p = vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[
            {{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}},
            {{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
              "Condition":{{"StringEquals":{{"{OWNER}":"a${{aws:username, '}}'"}}}}}}]}}"#
    ))];
    for owner in ["a", "a}", "a'", "a${aws:username, '}'"] {
        assert_eq!(get(&p, owner), AuthzDecision::Allow, "{owner}");
    }
}
