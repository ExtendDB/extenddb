// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The run context Start records on the operation for its runner: the store
// the operation started on. No response renders it.

async fn stored_run_context(h: &Harness, description: &Value) -> Option<Value> {
    let operation_arn = description_arn(description);
    let id = operation_arn
        .rsplit('/')
        .next()
        .expect("the ARN ends with the operation id");
    h.storage
        .get_bulk_operation(ACCOUNT, id)
        .await
        .expect("get bulk operation")
        .expect("the operation exists")
        .run_context
}

#[tokio::test]
async fn start_records_the_store_and_no_response_renders_it() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(1)));
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    assert_eq!(
        stored_run_context(&h, &description).await,
        Some(json!({"store": "filesystem"}))
    );

    let operation_arn = description_arn(&description);
    let described = describe(&ctx, &operation_arn).await.expect("describe");
    let listed = call(&ctx, "ListBulkOperations", json!({}))
        .await
        .expect("list");
    for (what, body) in [
        ("start", &description),
        ("describe", &described),
        ("list", &listed),
    ] {
        let text = body.to_string();
        assert!(
            !text.contains("filesystem") && !text.to_lowercase().contains("context"),
            "the {what} response carries no run context: {text}"
        );
    }
}

#[tokio::test]
async fn a_terminal_born_start_records_the_store_too() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let body = with_input(start_body(), "S3Bucket", json!("absent-loads"));
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(
        stored_run_context(&h, &description).await,
        Some(json!({"store": "filesystem"}))
    );
}
