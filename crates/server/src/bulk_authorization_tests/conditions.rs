// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition keys on the bulk operations. A bulk-operation resource never
//! carries tags, and a table may carry none, so a resource tag key is absent
//! from every Describe, Cancel and List request and from a Start on an
//! untagged table. Under IAM's rule a negated operator on an absent key
//! evaluates true, so a Deny gated on one applies; a positive operator on an
//! absent key does not match, so a Deny gated on one does not.

use super::*;

/// Allow every action, and Deny `deny_action` on every resource when the
/// condition block `condition` holds.
fn allow_all_with_conditional_deny(deny_action: &str, condition: Value) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": deny_action, "Resource": "*", "Condition": condition}
        ]
    })
    .to_string()
}

#[tokio::test]
async fn a_deny_with_a_negated_operator_on_an_absent_tag_binds_on_describe() {
    let policy = allow_all_with_conditional_deny(
        "dynamodb:DescribeBulkOperation",
        json!({"StringNotEquals": {"aws:ResourceTag/env": "prod"}}),
    );
    let (state, _) = state_with_policies(vec![policy], false).await;
    let own = operation_arn(REGION, ACCOUNT);
    assert_denied(
        authorize(&state, "DescribeBulkOperation", &arn_body(&own)).await,
        "StringNotEquals on a tag the operation cannot carry",
    );
}

#[tokio::test]
async fn a_deny_with_each_negated_operator_on_an_absent_key_binds_on_cancel_and_list() {
    let conditions = [
        json!({"StringNotEquals": {"aws:ResourceTag/env": "prod"}}),
        json!({"StringNotLike": {"aws:ResourceTag/env": "pr*"}}),
        json!({"StringNotEqualsIgnoreCase": {"aws:ResourceTag/env": "PROD"}}),
        json!({"StringNotEqualsIfExists": {"aws:ResourceTag/env": "prod"}}),
    ];
    let own = operation_arn(REGION, ACCOUNT);
    for condition in conditions {
        let policy = allow_all_with_conditional_deny("dynamodb:*", condition.clone());
        let (state, _) = state_with_policies(vec![policy], false).await;
        assert_denied(
            authorize(&state, "CancelBulkOperation", &arn_body(&own)).await,
            &format!("Cancel under {condition}"),
        );
        assert_denied(
            authorize(&state, "ListBulkOperations", &json!({})).await,
            &format!("List under {condition}"),
        );
    }
}

#[tokio::test]
async fn a_deny_with_a_negated_operator_on_an_absent_tag_binds_on_start() {
    let policy = allow_all_with_conditional_deny(
        "dynamodb:BulkPutItems",
        json!({"StringNotEquals": {"aws:ResourceTag/env": "prod"}}),
    );
    let (state, _) = state_with_policies(vec![policy], false).await;
    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "StringNotEquals on a tag the untagged table does not carry",
    );
}

#[tokio::test]
async fn a_deny_with_a_positive_operator_on_an_absent_tag_does_not_bind() {
    let policy = allow_all_with_conditional_deny(
        "dynamodb:*",
        json!({"StringEquals": {"aws:ResourceTag/env": "prod"}}),
    );
    let (state, _) = state_with_policies(vec![policy], false).await;
    let own = operation_arn(REGION, ACCOUNT);
    authorize(&state, "DescribeBulkOperation", &arn_body(&own))
        .await
        .expect("StringEquals on an absent key does not match, so the Deny does not apply");
    authorize(&state, "StartBulkPutItems", &start_body(TABLE))
        .await
        .expect("the same holds for a Start on an untagged table");
}
