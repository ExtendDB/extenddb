// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::bulk_put::authorize::S3Action;
use crate::bulk_put::tests::runner::{FakeAuthorizer, caller, denied};

// The caller's s3:ListBucket on the input bucket, decided on the first
// claim before the store's own bucket check (measured on the service: a
// bucket policy denying the listing fails the operation S3AccessDenied with
// no byte members and no manifest). The Start checks nothing.

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

/// Each listing the run may not make ends it `FAILED` with no byte member
/// and nothing listed. The ListBucket denial is decided before the owner
/// rule, and an authorizer with no recorded caller fails closed without a
/// check.
#[tokio::test]
async fn a_denied_listing_fails_the_run_with_no_byte_members() {
    let s3_denied = denied("s3:ListBucket", BUCKET_ARN);
    for (label, owner, principal, message, checks) in [
        ("a denied listing", None, true, s3_denied.as_str(), 1),
        (
            "a denied listing and another owner",
            Some(OTHER_ACCOUNT),
            true,
            s3_denied.as_str(),
            1,
        ),
        ("no recorded caller", None, false, "Access Denied", 0),
    ] {
        let h = Harness::new().await;
        h.write_input("part-0.json", &file_of(&plain_rows(2)));
        let fake = Arc::new(FakeAuthorizer::denying(S3Action::ListBucket, BUCKET_ARN));
        let mut ctx = checked_ctx(&h, &fake);
        if !principal {
            ctx.bulk_principal = None;
        }
        let body = match owner {
            Some(owner) => with_input(start_body(), "S3BucketOwner", json!(owner)),
            None => start_body(),
        };
        let started = start_ok(&ctx, body).await;
        assert_eq!(
            started["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "{label}"
        );
        assert!(fake.checks().is_empty(), "{label}: Start checks nothing");

        let description = run_started(&h, &ctx, &started).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("FAILED"),
            "{label}"
        );
        assert_eq!(
            description["FailureCode"],
            json!("S3AccessDenied"),
            "{label}"
        );
        assert_eq!(description["FailureMessage"], json!(message), "{label}");
        assert!(description.get("TotalInputBytes").is_none(), "{label}");
        assert!(description.get("ProcessedInputBytes").is_none(), "{label}");
        let expected: Vec<_> = (0..checks)
            .map(|_| {
                (
                    S3Action::ListBucket,
                    BUCKET_ARN.to_owned(),
                    Some(String::new()),
                )
            })
            .collect();
        assert_eq!(
            fake.checks(),
            expected,
            "{label}: an absent S3KeyPrefix is the empty s3:prefix"
        );
        let record = stored(&h, &description).await;
        assert!(record.input_files.is_empty(), "{label}: nothing is listed");
    }
}

#[tokio::test]
async fn the_listing_check_carries_the_request_prefix() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let ctx = checked_ctx(&h, &fake);
    let body = with_input(start_body(), "S3KeyPrefix", json!("orders/"));
    let started = start_ok(&ctx, body).await;
    assert_eq!(started["BulkOperationStatus"], json!("IN_PROGRESS"));
    let description = run_started(&h, &ctx, &started).await;
    assert_eq!(description["BulkOperationStatus"], json!("COMPLETED"));
    assert_eq!(
        fake.checks().first(),
        Some(&(
            S3Action::ListBucket,
            BUCKET_ARN.to_owned(),
            Some("orders/".to_owned())
        ))
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
async fn a_listing_that_cannot_be_evaluated_fails_the_run_internally() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer {
        unavailable: true,
        ..FakeAuthorizer::default()
    });
    let ctx = checked_ctx(&h, &fake);
    let started = start_ok(&ctx, start_body()).await;
    let description = run_started(&h, &ctx, &started).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("InternalServerError"));
    assert_eq!(
        description["FailureMessage"],
        json!("Internal server error")
    );
}
