// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The expression-map checks of `SearchVectors` answer ahead of a missing
//! table and a dimension mismatch, under either grammar.
//!
//! Measured on DynamoDB on 2026-11-23, two runs each: an empty
//! `ExpressionAttributeValues` or `ExpressionAttributeNames` answers
//! "<map> must not be empty" against a missing table and with a two-element
//! `SearchVector` on a four-dimension index; with valid maps the same
//! requests answer ResourceNotFoundException and the dimension message.

use std::time::Duration;

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::SqliteConfig;
use tempfile::TempDir;

use crate::OperationContext;

/// A context on a bootstrapped SQLite catalog in `dir`, where a table is
/// created ACTIVE.
async fn context(dir: &TempDir) -> OperationContext {
    let config = SqliteConfig {
        path: dir.path().join("catalog.sqlite").display().to_string(),
        pool_size: 2,
    };
    let mut options = ServerComponentsOptions::default();
    options.bootstrap_if_uninitialized = true;
    options.release = "test";
    let components =
        (extenddb_storage_sqlite::backend().server_components)(&config, "us-east-1", options)
            .await
            .unwrap_or_else(|e| panic!("server components: {e:?}"));
    let account_id = components
        .catalog_store
        .default_account_id()
        .await
        .unwrap()
        .expect("the bootstrapped account");
    components
        .catalog_store
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .unwrap();
    crate::test_context::context_on(components.engine, &account_id)
}

/// A table "tbl" with a four-dimension vector index "vix" whose HASH element
/// is `Country`, once the index serves searches.
async fn table(ctx: &OperationContext) {
    crate::handle_create_table(
        json!({
            "TableName": "tbl",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "Country", "AttributeType": "S"}
            ],
            "VectorIndexes": [{
                "IndexName": "vix",
                "Dimensions": 4,
                "DistanceFunction": "COSINE",
                "VectorAttribute": {"AttributeName": "emb"},
                "SearchSchema": [
                    {"AttributeName": "Country", "SearchSchemaElementType": "HASH"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }),
        ctx,
    )
    .await
    .expect("CreateTable");
    let probe = request("tbl", 4, Some(json!({":us": {"S": "US"}})), None);
    for _ in 0..200 {
        match crate::handle_search_vectors(probe.clone(), ctx).await {
            Ok(_) => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("the vector index did not become ACTIVE");
}

fn request(table: &str, dims: usize, values: Option<Value>, names: Option<Value>) -> Value {
    let mut body = json!({
        "TableName": table,
        "IndexName": "vix",
        "SearchVector": vec![json!({"N": "1"}); dims],
        "TopK": 5,
        "SearchConditionExpression": "Country = :us",
    });
    if let Some(values) = values {
        body["ExpressionAttributeValues"] = values;
    }
    if let Some(names) = names {
        body["ExpressionAttributeNames"] = names;
    }
    body
}

async fn answer(ctx: &OperationContext, body: Value) -> DynamoDbError {
    match crate::handle_search_vectors(body, ctx).await {
        Ok(_) => panic!("the request was answered"),
        Err(e) => e,
    }
}

fn validation(message: &str) -> impl Fn(&DynamoDbError) -> bool + '_ {
    move |e| matches!(e, DynamoDbError::ValidationException(m) if m == message)
}

fn us() -> Option<Value> {
    Some(json!({":us": {"S": "US"}}))
}

#[tokio::test]
async fn an_empty_map_answers_before_a_missing_table() {
    let dir = TempDir::new().unwrap();
    let ctx = context(&dir).await;
    let values_empty = answer(&ctx, request("missing", 4, Some(json!({})), None)).await;
    assert!(
        validation("ExpressionAttributeValues must not be empty")(&values_empty),
        "{values_empty:?}"
    );
    let names_empty = answer(&ctx, request("missing", 4, us(), Some(json!({})))).await;
    assert!(
        validation("ExpressionAttributeNames must not be empty")(&names_empty),
        "{names_empty:?}"
    );
    let missing = answer(&ctx, request("missing", 4, us(), None)).await;
    assert!(
        matches!(missing, DynamoDbError::ResourceNotFoundException(_)),
        "{missing:?}"
    );
}

#[tokio::test]
async fn an_empty_map_answers_before_a_dimension_mismatch() {
    let dir = TempDir::new().unwrap();
    let ctx = context(&dir).await;
    table(&ctx).await;
    let values_empty = answer(&ctx, request("tbl", 2, Some(json!({})), None)).await;
    assert!(
        validation("ExpressionAttributeValues must not be empty")(&values_empty),
        "{values_empty:?}"
    );
    let names_empty = answer(&ctx, request("tbl", 2, us(), Some(json!({})))).await;
    assert!(
        validation("ExpressionAttributeNames must not be empty")(&names_empty),
        "{names_empty:?}"
    );
    let mismatch = answer(&ctx, request("tbl", 2, us(), None)).await;
    assert!(
        validation("Input search vector dimension 2 does not match vector index dimension 4")(
            &mismatch
        ),
        "{mismatch:?}"
    );
}
