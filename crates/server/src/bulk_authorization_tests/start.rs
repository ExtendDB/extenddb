// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization of StartBulkPutItems: the two required actions, table ARN
//! spellings and accounts, and the dev-mode bypass.

use super::*;

#[tokio::test]
async fn a_user_allowed_only_put_item_is_denied_start_bulk_put_items() {
    let (state, _fetches) = state_with_policies(
        vec![allow("dynamodb:PutItem", &table_arn(ACCOUNT, TABLE))],
        false,
    )
    .await;
    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "PutItem alone must not grant StartBulkPutItems",
    );
}

#[tokio::test]
async fn start_requires_both_the_control_plane_and_the_data_plane_action() {
    // The data-plane grant alone: the control-plane check runs first and
    // its denial names the account-scoped action and the wildcard.
    let (state, _fetches) = state_with_policies(
        vec![allow("dynamodb:BulkPutItems", &table_arn(ACCOUNT, TABLE))],
        false,
    )
    .await;
    let message = denial_message(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "BulkPutItems alone",
    );
    assert!(
        message.contains("dynamodb:StartBulkOperation") && message.contains(&operation_wildcard()),
        "the denial names the control-plane action and the wildcard: {message}"
    );

    // The control-plane grant alone: the data-plane check fails and its
    // denial names the table-scoped action and the table ARN.
    let (state, _fetches) = state_with_policies(
        vec![allow("dynamodb:StartBulkOperation", &operation_wildcard())],
        false,
    )
    .await;
    let message = denial_message(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "StartBulkOperation alone",
    );
    assert!(
        message.contains("dynamodb:BulkPutItems") && message.contains(&table_arn(ACCOUNT, TABLE)),
        "the denial names the data-plane action and the table: {message}"
    );

    // Both grants pass.
    let (state, _fetches) = state_with_policies(start_grants(TABLE), false).await;
    authorize(&state, "StartBulkPutItems", &start_body(TABLE))
        .await
        .expect("both grants authorize the start");
}

#[tokio::test]
async fn a_grant_spelled_as_the_operation_name_does_not_authorize_a_start() {
    // `dynamodb:StartBulkPutItems` is not an action the service defines.
    let (state, _fetches) = state_with_policies(
        vec![
            allow("dynamodb:StartBulkPutItems", &table_arn(ACCOUNT, TABLE)),
            allow("dynamodb:StartBulkPutItems", &operation_wildcard()),
        ],
        false,
    )
    .await;
    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "the operation name is not an IAM action for this feature",
    );
}

#[tokio::test]
async fn a_deny_on_the_control_plane_action_blocks_every_start_in_the_account() {
    let (state, _fetches) = state_with_policies(
        vec![allow_all_with_deny(
            "dynamodb:StartBulkOperation",
            &operation_wildcard(),
        )],
        false,
    )
    .await;
    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(TABLE)).await,
        "a wildcard deny on the control-plane action",
    );
    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(OTHER_TABLE)).await,
        "the deny is account scoped, so the other table is denied too",
    );
}

#[tokio::test]
async fn start_allowed_on_table_a_is_denied_on_table_b_for_both_spellings() {
    let (state, fetches) = state_with_policies(start_grants(TABLE), false).await;

    authorize(&state, "StartBulkPutItems", &start_body(TABLE))
        .await
        .expect("the bare name of the allowed table passes");
    // The positive half of the fetch counter: an evaluated request fetches
    // policies at least once. The zero-fetch assertions elsewhere in this
    // file prove absence with the same counter, and would pass vacuously
    // if evaluation ever stopped routing through the counting store.
    assert!(
        fetches.load(Ordering::SeqCst) > 0,
        "an evaluated request fetches policies at least once"
    );
    authorize(
        &state,
        "StartBulkPutItems",
        &start_body(&table_arn(ACCOUNT, TABLE)),
    )
    .await
    .expect("the own-ARN spelling of the allowed table passes");

    assert_denied(
        authorize(&state, "StartBulkPutItems", &start_body(OTHER_TABLE)).await,
        "the bare name of the other table",
    );
    assert_denied(
        authorize(
            &state,
            "StartBulkPutItems",
            &start_body(&table_arn(ACCOUNT, OTHER_TABLE)),
        )
        .await,
        "the own-ARN spelling of the other table",
    );
}

#[tokio::test]
async fn an_explicit_deny_on_the_table_binds_against_the_own_arn_spelling() {
    let (state, _fetches) = state_with_policies(
        vec![allow_all_with_deny(
            "dynamodb:BulkPutItems",
            &table_arn(ACCOUNT, TABLE),
        )],
        false,
    )
    .await;
    assert_denied(
        authorize(
            &state,
            "StartBulkPutItems",
            &start_body(&table_arn(ACCOUNT, TABLE)),
        )
        .await,
        "the ARN spelling must resolve to the denied table's resource",
    );
}

#[tokio::test]
async fn table_name_as_another_accounts_arn_is_denied_before_any_authorization_call() {
    // The account and region checks of validation step 2 run ahead of
    // policy evaluation, so a foreign table ARN never reaches a policy.
    // The policy here allows everything and the fetch counter stays at
    // zero, so the denial can only come from the check itself; the
    // message is the engine's own denial string (measured), not the
    // evaluated-denial format.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let foreign = table_arn(OTHER_ACCOUNT, TABLE);
    match authorize(&state, "StartBulkPutItems", &start_body(&foreign)).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "no policy fetch may run for a foreign table ARN"
    );
}

/// Insert `OTHER_ACCOUNT` into the test's catalog, so the account a foreign
/// ARN names exists in this deployment.
async fn add_other_account(state: &AppState) {
    state
        .catalog_store
        .as_ref()
        .expect("a catalog")
        .create_account(OTHER_ACCOUNT, "other")
        .await
        .expect("create the other account");
}

#[tokio::test]
async fn table_name_as_an_existing_accounts_arn_is_denied_before_any_authorization_call() {
    // The same answer when the other account exists in this deployment: the
    // bulk derivation answers the ARN, not the cross-account refusal of the
    // other operations, which would name `dynamodb:StartBulkPutItems`.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    add_other_account(&state).await;
    let foreign = table_arn(OTHER_ACCOUNT, TABLE);
    match authorize(&state, "StartBulkPutItems", &start_body(&foreign)).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "no policy fetch may run for a table ARN of an existing account"
    );
}

#[tokio::test]
async fn another_accounts_index_arn_fails_validation_when_the_account_exists() {
    // An index ARN is not a table ARN, so the derivation refuses it as an
    // invalid spelling before it compares accounts.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    add_other_account(&state).await;
    let index = format!("{}/index/gsi", table_arn(OTHER_ACCOUNT, TABLE));
    let message = validation_message(
        authorize(&state, "StartBulkPutItems", &start_body(&index)).await,
        "another account's index ARN",
    );
    assert_eq!(message, format!("Invalid table ARN: {index}"));
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn table_name_as_another_regions_arn_fails_validation_before_any_authorization_call() {
    // Measured: another region on the caller's own table ARN is a step 2
    // validation failure; the zero fetch count proves it is answered
    // before any policy is fetched, like the foreign-account denial.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let foreign = format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/{TABLE}");
    let message = validation_message(
        authorize(&state, "StartBulkPutItems", &start_body(&foreign)).await,
        "a foreign-region table ARN",
    );
    assert_eq!(
        message,
        format!("1 validation error detected: Invalid AWS region in '{foreign}'")
    );
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "no policy fetch may run for a foreign-region table ARN"
    );
}

#[tokio::test]
async fn an_own_index_arn_answers_the_measured_format_text_before_any_authorization_call() {
    // Measured: the service answers an own index ARN with the
    // resource-name format clause, the text the handler carries; the
    // authorization layer runs first on the request path, so it must
    // answer the same text, before any policy is fetched.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    let index = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}/index/gsi");
    let message = validation_message(
        authorize(&state, "StartBulkPutItems", &start_body(&index)).await,
        "an own index ARN",
    );
    assert_eq!(
        message,
        format!(
            "1 validation error detected: Value '{index}' at 'tableName' failed to satisfy \
             constraint: Valid ARN format is \
             'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
             where Resource name must have length less than or equal to 255, Resource name \
             must have length greater than or equal to 3, Resource name must satisfy regular \
             expression pattern: [a-zA-Z0-9_.-]+"
        )
    );
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_own_arn_whose_name_breaks_the_rule_fails_validation_before_any_authorization_call() {
    // Measured: one `Table name of ARN` clause, length before pattern.
    let (state, fetches) = state_with_policies(vec![allow("dynamodb:*", "*")], false).await;
    for (name, rule) in [
        ("ab", "must have length greater than or equal to 3"),
        ("a*", "must have length greater than or equal to 3"),
        (
            "deni*",
            "must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
        ),
    ] {
        let value = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{name}");
        let message = validation_message(
            authorize(&state, "StartBulkPutItems", &start_body(&value)).await,
            name,
        );
        assert_eq!(
            message,
            format!(
                "1 validation error detected: Value '{value}' at 'tableName' failed to satisfy \
                 constraint: Table name of ARN {rule}"
            )
        );
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

// DescribeBulkOperation and CancelBulkOperation: the resource is the body's
// operation ARN, with the two parse layers and the account check answered
// before any policy evaluation.

#[tokio::test]
async fn dev_mode_passes_all_four_operations_without_evaluation() {
    let (state, fetches) = state_with_policies(Vec::new(), true).await;
    let own = operation_arn(REGION, ACCOUNT);

    authorize(&state, "StartBulkPutItems", &start_body(TABLE))
        .await
        .expect("dev mode passes Start");
    authorize(&state, "DescribeBulkOperation", &arn_body(&own))
        .await
        .expect("dev mode passes Describe");
    authorize(&state, "CancelBulkOperation", &arn_body(&own))
        .await
        .expect("dev mode passes Cancel");
    authorize(&state, "ListBulkOperations", &json!({}))
        .await
        .expect("dev mode passes List");

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "dev mode evaluates no policy for any of the four"
    );
}
