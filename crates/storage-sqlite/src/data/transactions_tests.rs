// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `transact_get_items` on a key element whose type does not match the
//! table's key schema (issue #701). Measured on DynamoDB (us-east-1) on
//! 2026-10-31: the transaction is cancelled with a `ValidationError` reason
//! carrying `The provided key element does not match the schema`.

use extenddb_core::types::{AttributeValue, CreateTableInput, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TransactGetOp};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    // Zero control-plane delay: the table is ACTIVE at create time, since no
    // transition poller runs inside a unit test.
    sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
    )
    .execute(&engine.pool)
    .await
    .expect("delay");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    engine
}

async fn hash_table(engine: &SqliteEngine) -> TableKeyInfo {
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    let desc = engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create");
    TableKeyInfo {
        table_name: "t".to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_wrong_typed_key_cancels_with_the_schema_message() {
    let engine = engine().await;
    let key_info = hash_table(&engine).await;
    let mut key = Item::new();
    key.insert("pk".to_owned(), AttributeValue::N("1".to_owned()));

    let err = engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &key,
        }])
        .await
        .expect_err("a wrong-typed key must cancel the transaction");
    let StorageError::TransactionCanceled(reasons) = err else {
        panic!("expected TransactionCanceled, got {err:?}");
    };
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert_eq!(reasons[0].code, "ValidationError");
    assert_eq!(
        reasons[0].message.as_deref(),
        Some("The provided key element does not match the schema")
    );
}
