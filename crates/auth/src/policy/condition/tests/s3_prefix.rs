// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `s3:prefix` on the real `RequestContext`.
//!
//! An `s3:ListBucket` evaluation carries the listing prefix as `s3:prefix`;
//! every other evaluation leaves the key absent. These tests pin the
//! resolution, the absent-key semantics when no prefix is set, and the
//! `StringLike` and `Null` forms a listing grant is written with.

use std::collections::HashMap;

use crate::policy::condition::evaluate_condition;
use crate::policy::context::{ConditionContext, RequestContext, RequestParams};
use crate::policy::document::{Condition, ConditionOperator};

/// A request whose `s3:prefix` is `prefix`, or absent when `prefix` is `None`.
fn ctx(prefix: Option<&str>) -> RequestContext {
    RequestContext::build(
        HashMap::new(),
        HashMap::new(),
        false,
        RequestParams {
            s3_prefix: prefix.map(ToOwned::to_owned),
            ..Default::default()
        },
    )
}

fn prefix_cond(op: ConditionOperator, values: &[&str]) -> Condition {
    Condition {
        operator: op,
        key: "s3:prefix".to_owned(),
        values: values.iter().map(|s| (*s).to_owned()).collect(),
    }
}

#[test]
fn s3_prefix_resolves_to_the_listing_prefix() {
    let ctx = ctx(Some("loads/2026/"));
    assert_eq!(ctx.resolve_key("s3:prefix"), Some(vec!["loads/2026/"]));
    // The key name is matched without regard to case, like every other key
    // this context resolves.
    assert_eq!(ctx.resolve_key("S3:Prefix"), Some(vec!["loads/2026/"]));
    // A single-valued key: a bare operator applies to it.
    assert!(!ctx.is_multivalued_key("s3:prefix"));
}

#[test]
fn s3_prefix_is_absent_when_unset_and_present_when_empty() {
    let unset = ctx(None);
    assert_eq!(unset.resolve_key("s3:prefix"), None);
    // Absent, not unresolvable: absent-key semantics apply and the key never
    // fails closed.
    assert!(!unset.is_unresolvable_key("s3:prefix"));
    // An empty prefix, a listing of the whole bucket, is a present key whose
    // value is the empty string.
    assert_eq!(ctx(Some("")).resolve_key("s3:prefix"), Some(vec![""]));
}

#[test]
fn string_like_on_s3_prefix_matches_the_prefix_as_given() {
    let grant = prefix_cond(ConditionOperator::StringLike, &["loads/*"]);
    assert!(evaluate_condition(&grant, &ctx(Some("loads/"))));
    assert!(evaluate_condition(&grant, &ctx(Some("loads/2026/orders"))));
    assert!(!evaluate_condition(&grant, &ctx(Some("other/"))));
    // The prefix is compared as given: without the slash it does not match.
    assert!(!evaluate_condition(&grant, &ctx(Some("loads"))));
    // An empty prefix does not match, and neither does an absent key.
    assert!(!evaluate_condition(&grant, &ctx(Some(""))));
    assert!(!evaluate_condition(&grant, &ctx(None)));
}

#[test]
fn null_on_s3_prefix_tells_an_absent_key_from_an_empty_prefix() {
    let absent = prefix_cond(ConditionOperator::Null, &["true"]);
    assert!(evaluate_condition(&absent, &ctx(None)));
    assert!(!evaluate_condition(&absent, &ctx(Some(""))));
    assert!(!evaluate_condition(&absent, &ctx(Some("loads/"))));
}
