// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// CancelBulkOperation.

#[tokio::test]
async fn cancel_moves_in_progress_to_cancelling_with_the_two_field_shape() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let arn = description_arn(&description);
    let response = cancel(&ctx, &arn).await.expect("cancel");
    assert_eq!(
        response,
        json!({ "BulkOperationArn": arn, "BulkOperationStatus": "CANCELLING" }),
        "the response carries exactly the two modelled fields"
    );
}

#[tokio::test]
async fn a_second_cancel_returns_cancelling_again() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let arn = description_arn(&description);
    cancel(&ctx, &arn).await.expect("first cancel");
    let response = cancel(&ctx, &arn).await.expect("second cancel");
    assert_eq!(response["BulkOperationStatus"], json!("CANCELLING"));
}

#[tokio::test]
async fn cancel_on_each_terminal_status_names_that_status() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let statuses = ["COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCELLED"];
    let mut ops = Vec::new();
    for (i, _) in statuses.iter().enumerate() {
        ops.push(
            raw_op(
                &h,
                ACCOUNT,
                u32::try_from(i).unwrap(),
                1000 + i64::try_from(i).unwrap(),
            )
            .await,
        );
    }
    let claimed = h
        .storage
        .claim_runnable("finisher", Duration::from_secs(60), 100, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), statuses.len());
    for (op, status) in ops.iter().zip(statuses) {
        finish_op(&h, ACCOUNT, "finisher", op, status).await;
    }
    for (op, status) in ops.iter().zip(statuses) {
        let message = expect_validation(cancel(&ctx, &arn_of(ACCOUNT, op)).await);
        assert_eq!(
            message,
            format!("Invalid Request: Operation already in terminal state: {status}")
        );
    }
}

#[tokio::test]
async fn cancel_rejects_a_plain_string_arn() {
    let h = Harness::new().await;
    assert_eq!(
        expect_validation(cancel(&base_ctx(&h), "orders").await),
        NOT_AN_ARN
    );
}

#[tokio::test]
async fn cancel_rejects_a_uuid_shaped_operation_id() {
    let h = Harness::new().await;
    let arn = format!(
        "arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/\
         f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
    );
    assert_eq!(expect_validation(cancel(&base_ctx(&h), &arn).await), BAD_ID);
}

#[tokio::test]
async fn cancel_answers_not_found_for_an_unknown_operation() {
    let h = Harness::new().await;
    let arn = arn_of(ACCOUNT, "01789060589767-da9ddd4f");
    assert_eq!(
        expect_not_found(cancel(&base_ctx(&h), &arn).await),
        format!("Bulk operation with arn: {arn} does not exist")
    );
}

#[tokio::test]
async fn cancel_denies_another_accounts_operation() {
    let h = Harness::new().await;
    h.catalog
        .create_account(OTHER_ACCOUNT, "handler-tests")
        .await
        .expect("create account");
    let foreign_op = raw_op(&h, OTHER_ACCOUNT, 1, 1_700_000_000_000).await;
    let arn = arn_of(OTHER_ACCOUNT, &foreign_op);
    match cancel(&base_ctx(&h), &arn).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_answers_not_found_for_another_region() {
    let h = Harness::new().await;
    let op = raw_op(&h, ACCOUNT, 1, 1_700_000_000_000).await;
    let arn = format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:bulk-operation/{op}");
    // The operation exists in the caller's account; the foreign region in
    // the ARN must answer the third layer anyway, as Describe does.
    assert_eq!(
        expect_not_found(cancel(&base_ctx(&h), &arn).await),
        format!("Bulk operation with arn: {arn} does not exist")
    );
}

#[tokio::test]
async fn a_cancel_racing_the_terminal_transition_never_answers_wrongly() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    let files = h.enumerate().await;
    let op = h
        .start(files, stored_input_config(), stored_output_config())
        .await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The runner processes every row, writes its final checkpoint, and
    // pends inside the terminal transition.
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.pend_finish = Some(Arc::clone(&notify));
    let mut config = runner_config();
    config.lease = Duration::from_millis(100);
    let deps = h.deps_with_storage("runner-1", config, Arc::new(faulting));
    tokio::select! {
        _ = run_operation(&deps, record) => {
            panic!("the runner must not finish while the terminal write pends")
        }
        () = notify.notified() => {}
    }

    // The cancel lands after the runner passed its last batch boundary and
    // before the terminal transition; it must answer CANCELLING, never an
    // error and never a terminal answer.
    let ctx = base_ctx(&h);
    let arn = arn_of(ACCOUNT, &op);
    let response = cancel(&ctx, &arn).await.expect("the cancel must succeed");
    assert_eq!(response["BulkOperationStatus"], json!("CANCELLING"));

    // The first runner died in the pend; after the lease expires a new
    // claim resumes the CANCELLING operation and the conditional finish
    // lands CANCELLED regardless of the outcome the runner computed.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    run_operation(&h.deps("runner-2", runner_config()), record)
        .await
        .expect("resumed run");
    let described = describe(&ctx, &arn).await.expect("describe");
    let description = &described["BulkOperationDescription"];
    assert_eq!(description["BulkOperationStatus"], json!("CANCELLED"));
    assert_eq!(
        description["ProcessedItemCount"],
        json!(30),
        "the counters keep every processed row"
    );
}
