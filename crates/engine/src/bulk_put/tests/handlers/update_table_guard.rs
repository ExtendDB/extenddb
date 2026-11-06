// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// UpdateTable while a bulk operation is in progress on the table (measured on
// the hosted service).

const INDEX_CREATE_REFUSED: &str = "Attempt to change a resource which is still in use: Index \
                                    creation is not allowed while table has ongoing bulk \
                                    operation: orders";

fn gsi_create_body(table: &str) -> Value {
    json!({
        "TableName": table,
        "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "gsi_new",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }
        }]
    })
}

async fn update_table(ctx: &OperationContext, body: Value) -> Result<Value, DynamoDbError> {
    call(ctx, "UpdateTable", body).await
}

#[tokio::test]
async fn index_creation_is_refused_while_an_operation_runs_and_allowed_after_it_finishes() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    let operation_id = description["BulkOperationArn"]
        .as_str()
        .and_then(|arn| arn.rsplit('/').next())
        .expect("operation id")
        .to_owned();

    // ResourceInUseException with the measured text and the table
    // name appended.
    let refused = expect_in_use(update_table(&ctx, gsi_create_body(TABLE)).await);
    assert_eq!(refused, INDEX_CREATE_REFUSED);

    // Another table of the account is not held by this operation.
    Harness::create_table(&h.storage, "other").await;
    update_table(&ctx, gsi_create_body("other"))
        .await
        .expect("an index on an unrelated table is created");

    // Once the operation is terminal the same request on the bound table
    // is accepted.
    let claimed = h
        .storage
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(claimed.iter().any(|r| r.operation_id == operation_id));
    finish_op(&h, ACCOUNT, "owner-1", &operation_id, "COMPLETED").await;
    update_table(&ctx, gsi_create_body(TABLE))
        .await
        .expect("index creation after the operation finished");
}

#[tokio::test]
async fn index_deletion_is_accepted_while_an_operation_runs() {
    // A GSI delete during a run is accepted.
    let h = Harness::new_with_gsi().await;
    let ctx = base_ctx(&h);
    start_ok(&ctx, start_body()).await;
    let response = update_table(
        &ctx,
        json!({
            "TableName": TABLE,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "gsi1"}}]
        }),
    )
    .await
    .expect("index deletion during a run");
    assert!(response["TableDescription"].is_object());
}

#[tokio::test]
async fn an_operation_on_a_deleted_table_does_not_refuse_an_index_on_its_successor() {
    // The operation stays IN_PROGRESS on the old table's id after the
    // table is deleted; a new table of the same name has a new id.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    let description = start_ok(&ctx, start_body()).await;
    assert_eq!(
        description["BulkOperationStatus"], "IN_PROGRESS",
        "{description}"
    );
    h.storage
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("delete the table");
    let new_id = Harness::create_table(&h.storage, TABLE).await;
    assert_ne!(new_id, h.table_id, "the new table has a new id");

    let response = update_table(&ctx, gsi_create_body(TABLE))
        .await
        .expect("an index on the new table is created");
    assert!(response["TableDescription"].is_object());
}
