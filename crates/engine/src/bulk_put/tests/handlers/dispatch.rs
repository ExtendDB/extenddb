// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Dispatch.

#[test]
fn the_four_operation_names_are_known_operations() {
    for name in [
        "StartBulkPutItems",
        "DescribeBulkOperation",
        "ListBulkOperations",
        "CancelBulkOperation",
    ] {
        assert!(crate::is_known_operation(name), "{name} must be known");
    }
}

#[tokio::test]
async fn a_non_object_body_answers_the_standard_serialization_error() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    for operation in [
        "StartBulkPutItems",
        "DescribeBulkOperation",
        "ListBulkOperations",
        "CancelBulkOperation",
    ] {
        match call(&ctx, operation, json!(7)).await {
            Err(DynamoDbError::SerializationException(message)) => assert!(
                message.starts_with("Start of structure or map found where not expected"),
                "{operation}: {message}"
            ),
            other => panic!("{operation}: expected SerializationException, got {other:?}"),
        }
    }
}

// Fault-order parity with the bulk runner.

#[tokio::test]
async fn put_item_reports_the_condition_message_when_the_key_is_also_missing() {
    // The bulk runner pins its fault order to handle_put_item's: the
    // condition fields resolve before the item validates, so the
    // expression message wins when a row carries both faults. This is the
    // PutItem half of that parity, driven with the same sk-less item and
    // unparseable expression the runner unit test uses.
    let h = Harness::new().await;
    let expected = resolve_condition(
        Some("attribute_not_exists(("),
        None,
        None,
        None,
        None,
        &LimitsConfig::default(),
    )
    .expect_err("the condition expression is invalid")
    .to_string();
    assert!(
        expected.starts_with("Invalid ConditionExpression"),
        "the shared core names the condition fault: {expected}"
    );
    let body = json!({
        "TableName": TABLE,
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression": "attribute_not_exists(("
    });
    let message = expect_validation(call(&base_ctx(&h), "PutItem", body).await);
    assert_eq!(
        message, expected,
        "the condition message must win over the missing key"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_at_the_bucket_name_starts_the_operation_terminal_failed() {
    let h = Harness::new().await;
    // A real directory with an input file, reachable only through a
    // symlink planted at the bucket name. The existence check reads the
    // link itself and answers as if the bucket were missing; a check that
    // followed the link would find a directory inside the caller's own
    // subtree and let the operation proceed.
    let elsewhere = h.store.0.join(ACCOUNT).join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("part-000.json"), b"{}\n").unwrap();
    std::os::unix::fs::symlink(&elsewhere, h.store.0.join(ACCOUNT).join("linked")).unwrap();
    let body = with_input(start_body(), "S3Bucket", json!("linked"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
}
