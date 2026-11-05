// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// DescribeBulkOperation.

#[tokio::test]
async fn describe_rejects_a_plain_string_arn() {
    let h = Harness::new().await;
    assert_eq!(
        expect_validation(describe(&base_ctx(&h), "orders").await),
        NOT_AN_ARN
    );
}

#[tokio::test]
async fn describe_rejects_a_uuid_shaped_operation_id() {
    let h = Harness::new().await;
    let arn = format!(
        "arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/\
         f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
    );
    assert_eq!(
        expect_validation(describe(&base_ctx(&h), &arn).await),
        BAD_ID
    );
}

#[tokio::test]
async fn a_respelled_bulk_operation_arn_is_refused_at_layer_two() {
    let h = Harness::new().await;
    // A real operation, so only the partition or the service respelling is
    // at fault: the id would resolve if the string reached layer three,
    // and a respelling no policy can name must never get that far.
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let canonical = description_arn(&description);
    let op = canonical
        .rsplit_once('/')
        .expect("the ARN carries the operation id")
        .1
        .to_owned();
    let respelled = [
        format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/{op}"),
        format!("arn:aws:s3:{REGION}:{ACCOUNT}:bulk-operation/{op}"),
    ];
    for arn in &respelled {
        assert_eq!(
            expect_validation(describe(&ctx, arn).await),
            BAD_ID,
            "describe must refuse {arn} with the layer two message"
        );
        assert_eq!(
            expect_validation(cancel(&ctx, arn).await),
            BAD_ID,
            "cancel must refuse {arn} with the layer two message"
        );
    }
    // The canonical spelling of the same operation still resolves.
    let described = describe(&ctx, &canonical)
        .await
        .expect("the canonical ARN resolves");
    assert!(
        described["BulkOperationDescription"].is_object(),
        "the canonical spelling answers the description"
    );
}

#[tokio::test]
async fn describe_answers_not_found_for_an_unknown_operation() {
    let h = Harness::new().await;
    let arn = arn_of(ACCOUNT, "01789060589767-da9ddd4f");
    assert_eq!(
        expect_not_found(describe(&base_ctx(&h), &arn).await),
        format!("Bulk operation with arn: {arn} does not exist")
    );
}

#[tokio::test]
async fn describe_denies_another_accounts_operation() {
    let h = Harness::new().await;
    h.catalog
        .create_account(OTHER_ACCOUNT, "handler-tests")
        .await
        .expect("create account");
    let foreign_op = raw_op(&h, OTHER_ACCOUNT, 1, 1_700_000_000_000).await;
    let arn = arn_of(OTHER_ACCOUNT, &foreign_op);
    // The operation exists in the other account; the answer is decided
    // before storage is consulted, so it must not depend on that.
    match describe(&base_ctx(&h), &arn).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
}

#[tokio::test]
async fn a_foreign_account_is_answered_before_the_operation_id_shape() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    // Measured on the service: a foreign-account ARN carrying the
    // id `nope` is denied, while the caller's own account with the same id
    // answers the layer two validation message. Neither account holds an
    // operation with that id, so only the order of the two checks decides
    // which answer comes back.
    let foreign = arn_of(OTHER_ACCOUNT, "nope");
    for result in [describe(&ctx, &foreign).await, cancel(&ctx, &foreign).await] {
        match result {
            Err(DynamoDbError::AccessDeniedException(message)) => {
                assert_eq!(message, "Access is denied");
            }
            other => panic!("expected AccessDeniedException, got {other:?}"),
        }
    }
    let own = arn_of(ACCOUNT, "nope");
    assert_eq!(expect_validation(describe(&ctx, &own).await), BAD_ID);
    assert_eq!(expect_validation(cancel(&ctx, &own).await), BAD_ID);
}

#[tokio::test]
async fn a_foreign_account_outranks_a_foreign_region() {
    // An ARN naming both another account and another region
    // answers the account's AccessDeniedException. The account check runs
    // before the region check, so a foreign account wins over a foreign
    // region; the answer is decided before storage is consulted.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let arn = format!(
        "arn:aws:dynamodb:us-west-2:{OTHER_ACCOUNT}:bulk-operation/01789060589767-da9ddd4f"
    );
    for result in [describe(&ctx, &arn).await, cancel(&ctx, &arn).await] {
        match result {
            Err(DynamoDbError::AccessDeniedException(message)) => {
                assert_eq!(message, "Access is denied");
            }
            other => panic!("expected AccessDeniedException, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn describe_answers_not_found_for_another_region() {
    let h = Harness::new().await;
    let op = raw_op(&h, ACCOUNT, 1, 1_700_000_000_000).await;
    let arn = format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:bulk-operation/{op}");
    assert_eq!(
        expect_not_found(describe(&base_ctx(&h), &arn).await),
        format!("Bulk operation with arn: {arn} does not exist")
    );
}

#[tokio::test]
async fn a_terminal_operation_description_carries_end_time_and_usage() {
    let h = Harness::new().await;
    let rows = vec![row(0), row_missing_sk(1), row(2)];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            stored_input_config(),
            stored_output_config(),
            runner_config(),
        )
        .await;
    let arn = arn_of(ACCOUNT, &record.operation_id);
    let response = describe(&base_ctx(&h), &arn).await.expect("describe");
    let description = &response["BulkOperationDescription"];
    assert_eq!(
        description["BulkOperationStatus"],
        json!("COMPLETED_WITH_ERRORS")
    );
    // ProcessedItemCount counts rows read and attempted, including the
    // failed row.
    assert_eq!(description["ProcessedItemCount"], json!(3));
    assert_eq!(description["FailedItemCount"], json!(1));
    let start_time = description["StartTime"].as_f64().expect("StartTime");
    let end_time = description["EndTime"].as_f64().expect("EndTime");
    assert!(end_time >= start_time);
    let usage = description["BulkOperationUsage"]
        .as_array()
        .expect("BulkOperationUsage is a list");
    assert_eq!(usage.len(), 1, "one usage entry for the table");
    assert!(
        usage[0]["BulkPutBytes"].as_u64().expect("BulkPutBytes") > 0,
        "the usage entry counts the written bytes"
    );
    let members = description.as_object().unwrap();
    assert!(!members.contains_key("FailureCode"));
    assert!(!members.contains_key("FailureMessage"));
}

#[tokio::test]
async fn a_failed_operation_description_carries_the_failure_fields() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(5)));
    let files = h.enumerate().await;
    let mut config = runner_config();
    config.max_item_count = 2;
    let record = h
        .run_to_end(files, stored_input_config(), stored_output_config(), config)
        .await;
    let arn = arn_of(ACCOUNT, &record.operation_id);
    let response = describe(&base_ctx(&h), &arn).await.expect("describe");
    let description = &response["BulkOperationDescription"];
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("ValidationException"));
    assert_eq!(
        description["FailureMessage"],
        json!("Operation exceeded max_bulk_item_count (2 rows)")
    );
    assert!(description["EndTime"].is_f64(), "EndTime must be present");
}

#[tokio::test]
async fn an_unknown_stored_status_answers_internal_server_error() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let arn = description_arn(&description);
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.doctor_status = Some("MYSTERY".to_owned());
    let doctored = ctx_with_storage(&h, Arc::new(faulting));
    match describe(&doctored, &arn).await {
        Err(DynamoDbError::InternalServerError(_)) => {}
        other => panic!("expected InternalServerError, got {other:?}"),
    }
}

#[tokio::test]
async fn a_negative_stored_counter_answers_internal_server_error_on_describe() {
    // The backend record readers refuse a negative counter with a storage
    // error (the storage contract pins each column); this test pins the
    // wire mapping of that refusal. The counter is planted through a
    // claimed checkpoint, which binds the figures it is handed, so the
    // corrupt row reaches the real reader on the describe path.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let arn = description_arn(&description);
    let record = h.claim("corruptor", Duration::from_secs(60)).await;
    let state = h
        .storage
        .checkpoint(
            ACCOUNT,
            &record.operation_id,
            "corruptor",
            Duration::from_secs(60),
            Progress {
                cursor_file_index: 0,
                cursor_row_index: 0,
                processed_item_count: -1,
                failed_item_count: 0,
                processed_input_bytes: 0,
                report_record_count: 0,
                report_bytes: 0,
                usage: json!({}),
                records: Vec::new(),
                finished_files: Vec::new(),
                abandoned_files: Vec::new(),
            },
        )
        .await
        .expect("the checkpoint itself writes whatever it is handed");
    assert!(matches!(state, LeaseState::Held), "the lease is held");
    match describe(&ctx, &arn).await {
        Err(DynamoDbError::InternalServerError(_)) => {}
        other => panic!("expected InternalServerError, got {other:?}"),
    }
}
