// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::bulk_put::authorize::S3Action;
use crate::bulk_put::tests::runner::{FakeAuthorizer, caller, denied};

// The caller's s3:ListBucket on the input bucket, decided at Start before the
// store's own bucket check (measured on the service: a bucket policy denying
// the listing fails the operation S3AccessDenied with no byte members and no
// manifest). Here the row is born terminal.

const BUCKET_ARN: &str = "arn:aws:s3:::loads";

fn checked_ctx(h: &Harness, authorizer: &Arc<FakeAuthorizer>) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.bulk_authorizer = Some(Arc::clone(authorizer) as Arc<dyn BulkAuthorizer>);
    ctx.bulk_principal = Some(caller());
    ctx
}

async fn stored(h: &Harness, description: &Value) -> extenddb_storage::BulkOperationRecord {
    let operation_arn = description_arn(description);
    let id = operation_arn.rsplit('/').next().expect("an operation id");
    h.storage
        .get_bulk_operation(ACCOUNT, id)
        .await
        .expect("get bulk operation")
        .expect("the operation exists")
}

#[tokio::test]
async fn a_denied_listing_is_born_failed_with_no_byte_members() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(2)));
    let fake = Arc::new(FakeAuthorizer::denying(S3Action::ListBucket, BUCKET_ARN));
    let description = start_ok(&checked_ctx(&h, &fake), start_body()).await;

    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
    assert_eq!(
        description["FailureMessage"],
        json!(denied("s3:ListBucket", BUCKET_ARN)),
        "S3's spelling: the bucket ARN quoted, no prefix, no tail"
    );
    assert!(description.get("TotalInputBytes").is_none());
    assert!(description.get("ProcessedInputBytes").is_none());
    assert_eq!(
        fake.checks(),
        vec![(
            S3Action::ListBucket,
            BUCKET_ARN.to_owned(),
            Some(String::new())
        )],
        "an absent S3KeyPrefix is the empty s3:prefix"
    );
    let record = stored(&h, &description).await;
    assert!(record.input_files.is_empty(), "nothing is enumerated");
}

#[tokio::test]
async fn the_listing_check_carries_the_request_prefix() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let body = with_input(start_body(), "S3KeyPrefix", json!("orders/"));
    let description = start_ok(&checked_ctx(&h, &fake), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
    assert_eq!(
        fake.checks(),
        vec![(
            S3Action::ListBucket,
            BUCKET_ARN.to_owned(),
            Some("orders/".to_owned())
        )]
    );
}

#[tokio::test]
async fn an_allowed_start_records_the_caller_for_the_runner() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let description = start_ok(&checked_ctx(&h, &fake), start_body()).await;
    let record = stored(&h, &description).await;
    assert_eq!(
        record.run_context,
        Some(json!({
            "store": "filesystem",
            "principal": serde_json::to_value(caller()).unwrap(),
        }))
    );
    assert!(
        !description.to_string().contains("principal"),
        "the response carries no caller"
    );
}

#[tokio::test]
async fn the_listing_is_decided_before_the_owner_rule() {
    let h = Harness::new().await;
    let fake = Arc::new(FakeAuthorizer::denying(S3Action::ListBucket, BUCKET_ARN));
    let body = with_input(start_body(), "S3BucketOwner", json!(OTHER_ACCOUNT));
    let description = start_ok(&checked_ctx(&h, &fake), body).await;
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
    assert_eq!(
        description["FailureMessage"],
        json!(denied("s3:ListBucket", BUCKET_ARN))
    );
}

#[tokio::test]
async fn an_authorizer_with_no_caller_fails_closed_at_start() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let mut ctx = checked_ctx(&h, &fake);
    ctx.bulk_principal = None;
    let description = start_ok(&ctx, start_body()).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
    assert_eq!(description["FailureMessage"], json!("Access Denied"));
    assert!(fake.checks().is_empty());
}

#[tokio::test]
async fn a_listing_that_cannot_be_evaluated_creates_no_operation() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer {
        unavailable: true,
        ..FakeAuthorizer::default()
    });
    let ctx = checked_ctx(&h, &fake);
    match start(&ctx, start_body()).await {
        Err(DynamoDbError::InternalServerError(message)) => {
            assert_eq!(message, "Internal server error");
        }
        other => panic!("expected InternalServerError, got {other:?}"),
    }
    let listed = call(&ctx, "ListBulkOperations", json!({}))
        .await
        .expect("list");
    assert_eq!(listed["BulkOperationsSummaryList"], json!([]));
}
