// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn an_input_bucket_owner_of_another_account_starts_the_operation_terminal_failed() {
    // Measured: the mismatch fails the operation, not the request. The
    // token is consumed like any other terminal-born start.
    let h = Harness::new().await;
    let body = with_input(
        with_top(start_body(), "ClientToken", json!("tok-owner-mismatch")),
        "S3BucketOwner",
        json!(OTHER_ACCOUNT),
    );
    let description = start_ok(&base_ctx(&h), body.clone()).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
    assert_eq!(description["FailureMessage"], json!("Access Denied"));
    assert_eq!(description["ProcessedItemCount"], json!(0));
    assert_eq!(description["EndTime"], description["StartTime"]);
    let replay = start_ok(&base_ctx(&h), body).await;
    assert_eq!(
        replay["BulkOperationArn"], description["BulkOperationArn"],
        "the token is consumed and replays to the failed operation"
    );
}

#[tokio::test]
async fn an_input_owner_mismatch_answers_before_the_bucket_existence_check() {
    // Ownership is decided before existence, so a caller naming another
    // owner cannot learn whether the bucket exists: a missing bucket with
    // a mismatched owner answers S3AccessDenied, not S3NoSuchBucket.
    let h = Harness::new().await;
    let body = with_input(
        with_input(start_body(), "S3BucketOwner", json!(OTHER_ACCOUNT)),
        "S3Bucket",
        json!("absent"),
    );
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
}

#[tokio::test]
async fn an_output_bucket_owner_of_another_account_is_not_checked_at_start() {
    // The output configuration is checked at the first report record, not
    // at Start (measured: a mismatched output owner with clean rows
    // completes).
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3BucketOwner", json!(OTHER_ACCOUNT));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
}

#[tokio::test]
async fn the_description_carries_the_table_name_and_id_in_table_details() {
    // Measured on the service: the Start response and every
    // Describe carry TableDetails as a one-entry list of the bare table
    // name and the table id.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let expected = json!([{ "TableName": TABLE, "TableId": h.table_id }]);
    assert_eq!(description["TableDetails"], expected);
    let described = describe(&ctx, &description_arn(&description))
        .await
        .expect("the operation describes");
    assert_eq!(
        described["BulkOperationDescription"]["TableDetails"], expected,
        "every Describe carries the same entry"
    );
}

#[tokio::test]
async fn an_absent_bucket_owner_is_echoed_as_the_callers_account() {
    // Measured on the service: with S3BucketOwner omitted on both
    // configurations, the description echoes the caller's account id on
    // both, and an absent InputCompressionType is echoed as NONE.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let mut body = start_body();
    body["InputS3Configuration"]
        .as_object_mut()
        .unwrap()
        .remove("InputCompressionType");
    let description = start_ok(&ctx, body).await;
    assert_eq!(
        description["InputS3Configuration"]["S3BucketOwner"],
        json!(ACCOUNT)
    );
    assert_eq!(
        description["OutputS3Configuration"]["S3BucketOwner"],
        json!(ACCOUNT)
    );
    assert_eq!(
        description["InputS3Configuration"]["InputCompressionType"],
        json!("NONE")
    );
    // The resolved values are stored, so a later Describe renders them
    // without re-deriving anything.
    let described = describe(&ctx, &description_arn(&description))
        .await
        .expect("the operation describes");
    let described = &described["BulkOperationDescription"];
    assert_eq!(
        described["InputS3Configuration"]["S3BucketOwner"],
        json!(ACCOUNT)
    );
    assert_eq!(
        described["OutputS3Configuration"]["S3BucketOwner"],
        json!(ACCOUNT)
    );
    assert_eq!(
        described["InputS3Configuration"]["InputCompressionType"],
        json!("NONE")
    );
}

#[tokio::test]
async fn supplying_the_own_account_bucket_owner_replays_the_request_that_omitted_it() {
    // The own-account S3BucketOwner is a service default a retry may add
    // without changing the operation (measured), so the fingerprint
    // normalizes it to absent and the replay returns the existing
    // operation rather than a parameter mismatch.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let token = "owner-resolution";
    let first = with_top(start_body(), "ClientToken", json!(token));
    let first_desc = start_ok(&ctx, first).await;
    let second = with_input(
        with_top(start_body(), "ClientToken", json!(token)),
        "S3BucketOwner",
        json!(ACCOUNT),
    );
    let replayed = start_ok(&ctx, second).await;
    assert_eq!(description_arn(&replayed), description_arn(&first_desc));
}

#[tokio::test]
async fn an_oversized_stored_configuration_is_rejected() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "OutputType", json!(vec!["ERROR"; 1200]));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        CONFIG_TOO_BIG
    );
}

#[tokio::test]
async fn a_bare_table_name_is_accepted() {
    let h = Harness::new().await;
    let description = start_ok(&base_ctx(&h), start_body()).await;
    assert_eq!(description["TableDetails"][0]["TableName"], json!(TABLE));
}

#[tokio::test]
async fn a_matching_table_arn_is_accepted() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "TableName", json!(h.table_arn.clone()));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["TableDetails"][0]["TableName"], json!(TABLE));
}

#[tokio::test]
async fn a_foreign_account_table_arn_is_denied() {
    // Measured: the answer is the denial, not a validation message, and
    // it is decided before the table is looked up.
    let h = Harness::new().await;
    let foreign = format!("arn:aws:dynamodb:{REGION}:{OTHER_ACCOUNT}:table/{TABLE}");
    let body = with_top(start_body(), "TableName", json!(foreign.clone()));
    match start(&base_ctx(&h), body).await {
        Err(DynamoDbError::AccessDeniedException(message)) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
}

#[tokio::test]
async fn a_foreign_region_table_arn_fails_the_region_validation() {
    // Measured: the caller's own table ARN respelled to another region is
    // a validation failure at step 2.
    let h = Harness::new().await;
    let foreign = format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/{TABLE}");
    let body = with_top(start_body(), "TableName", json!(foreign.clone()));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        format!("1 validation error detected: Invalid AWS region in '{foreign}'")
    );
}

#[tokio::test]
async fn ion_input_format_is_refused() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "InputFormat", json!("ION"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        ION_REFUSED
    );
}

/// `InputCompressionType` ZSTD is accepted and echoed, as GZIP is (measured
/// on the service: a zstd input is read and its compressed size is
/// `TotalInputBytes`).
#[tokio::test]
async fn zstd_compression_is_accepted_and_echoed() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "InputCompressionType", json!("ZSTD"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(
        description["InputS3Configuration"]["InputCompressionType"],
        json!("ZSTD")
    );
}

#[tokio::test]
async fn a_missing_store_answers_the_disabled_message() {
    let h = Harness::new().await;
    assert_eq!(
        expect_validation(start(&ctx_without_store(&h), start_body()).await),
        BULK_DISABLED
    );
}

#[tokio::test]
async fn a_missing_table_answers_the_table_not_found_error() {
    let h = Harness::new().await;
    let body = with_top(start_body(), "TableName", json!("absent"));
    assert_eq!(
        expect_not_found(start(&base_ctx(&h), body).await),
        format!("{TABLE_NOT_FOUND}: Table: absent not found")
    );
}

#[tokio::test]
async fn a_table_that_is_not_active_answers_the_in_use_error() {
    let h = Harness::new().await;
    // A non-zero control-plane delay leaves the new table CREATING, and no
    // control-plane worker runs in these tests to mature it.
    h.catalog
        .set_setting("control_plane_delay_seconds", "3600")
        .await
        .expect("set delay");
    Harness::create_table(&h.storage, "pending").await;
    h.catalog
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("reset delay");
    let body = with_top(start_body(), "TableName", json!("pending"));
    assert_eq!(
        expect_in_use(start(&base_ctx(&h), body).await),
        "Table is being created, updated, or deleted: pending"
    );
}

#[tokio::test]
async fn a_table_being_deleted_answers_the_table_not_found_validation() {
    // Measured on the service: a start against a table in DELETING
    // answers 400 ValidationException with this text, both immediately
    // after DeleteTable and three seconds later, not the in-use error a
    // CREATING table gets.
    let h = Harness::new().await;
    Harness::create_table(&h.storage, "going").await;
    // A delay of a second or more leaves the delete at DELETING with a
    // scheduled transition, and no control-plane worker runs here to
    // complete it.
    h.catalog
        .set_setting("control_plane_delay_seconds", "3600")
        .await
        .expect("set delay");
    let deleted = h
        .storage
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "going".to_owned(),
            },
        )
        .await
        .expect("delete table");
    h.catalog
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("reset delay");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting,
        "the delay must leave the table DELETING"
    );
    let body = with_top(start_body(), "TableName", json!("going"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        "Invalid Request: Table not found"
    );
}

#[tokio::test]
async fn an_input_over_the_object_count_cap_is_accepted_at_start() {
    // Measured on the service: the Start is accepted and the
    // operation ends FAILED with S3ObjectsCountLimitExceeded (the runner's
    // check); the walk stops one object past the cap, so the record
    // carries max plus one files for the runner to count.
    let h = Harness::new().await;
    for i in 0..5 {
        h.write_input(&format!("part-{i}.json"), b"0123456789");
    }
    let ctx = ctx_with_limits(&h, bulk_limits(&[("max_bulk_input_files", json!(2))]));
    let description = start_ok(&ctx, start_body()).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
    let operation_id = description["BulkOperationArn"]
        .as_str()
        .and_then(|arn| arn.rsplit('/').next())
        .expect("operation id");
    let record = h
        .storage
        .get_bulk_operation(ACCOUNT, operation_id)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(record.input_files.len(), 3, "the cap plus one");
}

#[tokio::test]
async fn the_input_byte_cap_answers_the_byte_message() {
    let h = Harness::new().await;
    h.write_input("part-0.json", b"01234567");
    h.write_input("part-1.json", b"01234567");
    let ctx = ctx_with_limits(&h, bulk_limits(&[("max_bulk_input_bytes", json!(10))]));
    assert_eq!(
        expect_validation(start(&ctx, start_body()).await),
        "Input exceeds max_bulk_input_bytes (10 bytes)"
    );
}

#[tokio::test]
async fn the_byte_cap_still_refuses_when_the_count_cap_is_also_passed() {
    // The count cap is no longer a Start refusal, so with both passed the
    // enumerated total is what answers.
    let h = Harness::new().await;
    for i in 0..3 {
        h.write_input(&format!("part-{i}.json"), b"01234567");
    }
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[
            ("max_bulk_input_files", json!(2)),
            ("max_bulk_input_bytes", json!(10)),
        ]),
    );
    assert_eq!(
        expect_validation(start(&ctx, start_body()).await),
        "Input exceeds max_bulk_input_bytes (10 bytes)"
    );
}

#[tokio::test]
async fn total_input_bytes_is_the_enumerated_sum() {
    let h = Harness::new().await;
    h.write_input("part-0.json", b"0123456789");
    h.write_input("part-1.json", b"01234567890123456789");
    let description = start_ok(&base_ctx(&h), start_body()).await;
    assert_eq!(description["TotalInputBytes"], json!(30));
}

#[tokio::test]
async fn an_absent_performance_tier_echoes_standard() {
    let h = Harness::new().await;
    let description = start_ok(&base_ctx(&h), start_body()).await;
    assert_eq!(description["PerformanceTier"], json!("STANDARD"));
}

#[tokio::test]
async fn an_empty_input_answers_in_progress_with_zero_total_bytes() {
    let h = Harness::new().await;
    // The bucket exists (the harness staged it) and is empty; enumeration
    // matches no files.
    let description = start_ok(&base_ctx(&h), start_body()).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
    assert_eq!(description["TotalInputBytes"], json!(0));
}
