// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization of Describe, Cancel and List against bulk operation ARNs:
//! foreign accounts and regions, the two invalid-ARN layers, and the wildcard
//! resource.

use super::*;

#[tokio::test]
async fn a_describe_with_a_foreign_account_arn_is_denied_with_no_policy_evaluated() {
    // The policy allows everything, so the denial can only come from the
    // account check ahead of evaluation; the fetch counter proves no
    // policy and no storage row was consulted, so the answer does not
    // depend on whether the foreign operation exists.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let foreign = operation_arn(REGION, OTHER_ACCOUNT);
    match authorize(&state, "DescribeBulkOperation", &arn_body(&foreign)).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(
                message,
                format!(
                    "User: arn:aws:iam::{ACCOUNT}:user/{USER} is not authorized \
                     to perform: dynamodb:DescribeBulkOperation on resource: {foreign}"
                )
            );
        }
        other => panic!("expected the denial, got {other:?}"),
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "a foreign ARN must not be evaluated against any policy"
    );
}

#[tokio::test]
async fn a_cancel_with_a_foreign_region_arn_is_not_found_with_no_policy_evaluated() {
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let foreign = operation_arn("eu-west-1", ACCOUNT);
    match authorize(&state, "CancelBulkOperation", &arn_body(&foreign)).await {
        Err(DynamoDbError::ResourceNotFoundException(message)) => {
            assert_eq!(
                message,
                format!("Bulk operation with arn: {foreign} does not exist")
            );
        }
        other => panic!("expected the not-found answer, got {other:?}"),
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_describe_with_a_layer_1_invalid_arn_is_rejected_before_any_evaluation() {
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let message = validation_message(
        authorize(&state, "DescribeBulkOperation", &arn_body("orders")).await,
        "a plain word in BulkOperationArn",
    );
    assert_eq!(message, NOT_AN_ARN);
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_describe_with_a_layer_2_invalid_arn_is_rejected_before_any_evaluation() {
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let uuid_shaped = format!(
        "arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
    );
    let message = validation_message(
        authorize(&state, "DescribeBulkOperation", &arn_body(&uuid_shaped)).await,
        "a uuid-shaped operation id",
    );
    assert_eq!(message, BAD_ID);
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_deny_on_the_bulk_operation_wildcard_cannot_be_evaded_by_respelling_the_arn() {
    // Policy resource matching is literal, so a Deny written against the
    // documented ARN shape can never match a partition or service
    // respelling. The parse therefore refuses a respelling at layer 2,
    // before any policy is fetched; only the canonical spelling reaches
    // evaluation, where the Deny binds.
    let wildcard = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*");
    let (state, fetches) =
        state_with_policies(vec![allow_all_with_deny("dynamodb:*", &wildcard)], false).await;

    for respelled in [
        format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/{OPERATION_ID}"),
        format!("arn:aws:s3:{REGION}:{ACCOUNT}:bulk-operation/{OPERATION_ID}"),
    ] {
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            let message = validation_message(
                authorize(&state, operation, &arn_body(&respelled)).await,
                "a respelled bulk operation ARN",
            );
            assert_eq!(message, BAD_ID);
        }
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "a respelled ARN must be refused before any policy fetch"
    );

    let canonical = operation_arn(REGION, ACCOUNT);
    assert_denied(
        authorize(&state, "DescribeBulkOperation", &arn_body(&canonical)).await,
        "the Deny binds against the canonical Describe spelling",
    );
    assert_denied(
        authorize(&state, "CancelBulkOperation", &arn_body(&canonical)).await,
        "the Deny binds against the canonical Cancel spelling",
    );
}

#[tokio::test]
async fn a_describe_with_the_callers_own_arn_is_evaluated_against_that_arn() {
    // Allowed on the exact operation ARN: the resource evaluated must be
    // the ARN itself, so this passes; a wildcard-table resource derivation
    // would deny it.
    let own = operation_arn(REGION, ACCOUNT);
    let (state, _fetches) =
        state_with_policies(vec![allow("dynamodb:DescribeBulkOperation", &own)], false).await;
    authorize(&state, "DescribeBulkOperation", &arn_body(&own))
        .await
        .expect("the caller's own ARN is the evaluated resource");

    // The same policy does not grant Cancel.
    let (state, _fetches) =
        state_with_policies(vec![allow("dynamodb:DescribeBulkOperation", &own)], false).await;
    assert_denied(
        authorize(&state, "CancelBulkOperation", &arn_body(&own)).await,
        "Describe alone must not grant Cancel",
    );
}

// ListBulkOperations: the resource is the account's bulk-operation wildcard.

#[tokio::test]
async fn list_is_evaluated_against_the_wildcard_bulk_operation_resource() {
    let wildcard = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*");
    let (state, _fetches) =
        state_with_policies(vec![allow("dynamodb:ListBulkOperations", &wildcard)], false).await;
    authorize(&state, "ListBulkOperations", &json!({}))
        .await
        .expect("an allow on the bulk-operation wildcard grants List");

    // An allow on every table does not reach the bulk-operation resource,
    // so a derivation that still evaluates List against `table/*` fails
    // here.
    let (state, _fetches) = state_with_policies(
        vec![allow(
            "dynamodb:ListBulkOperations",
            &format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/*"),
        )],
        false,
    )
    .await;
    assert_denied(
        authorize(&state, "ListBulkOperations", &json!({})).await,
        "a table wildcard must not grant List over bulk operations",
    );
}

// Developer mode: the early return sits above every bulk branch.
