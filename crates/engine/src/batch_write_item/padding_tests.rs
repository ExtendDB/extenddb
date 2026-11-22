// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which operations read a `B` value with its padding left out or cut short.
//! Measured on the service in us-east-1 on 2026-11-22, each request twice,
//! on a table keyed on `pk` (S) and one keyed on `pk` (B): `BatchWriteItem`
//! refuses such a value, and such a `BS` member, in a `PutRequest` item and in
//! a `DeleteRequest` key with the text of its length, and every other
//! operation reads it as the padded value. `BatchWriteItem` reads `Y=Q=`
//! through the second reading and stores `//Q=`. The rows send each request
//! through [`dispatch`] and read a written item back with a `GetItem`.

use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::TempDir;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::SqliteConfig;

use crate::{AuthorizedKeyInfo, BackupRuntime, OperationContext, dispatch};

/// The table keyed on `pk` (S), whose items are read back.
const S_TABLE: &str = "keyed-on-s";
/// The table keyed on `pk` (B).
const B_TABLE: &str = "keyed-on-b";

/// The service's answer: the message of its `SerializationException`, or the
/// `B` text of the item read back, when the request writes one.
type Answer = Result<Option<String>, String>;

fn length(n: usize) -> Answer {
    Err(format!(
        "Base64 encoded length is expected a multiple of 4 bytes but found: {n}"
    ))
}

fn stored(text: &str) -> Answer {
    Ok(Some(text.to_owned()))
}

fn batch(table: &str, request: Value) -> Value {
    json!({"RequestItems": {table: [request]}})
}

fn put(item: Value) -> Value {
    json!({"PutRequest": {"Item": item}})
}

/// An item of the S table: its row as the key and `b` as the value.
fn item(row: &str, b: Value) -> Value {
    json!({"pk": {"S": row}, "b": b})
}

/// The key `YQ` of the B table.
fn b_key() -> Value {
    json!({"pk": {"B": "YQ"}})
}

/// A context on a bootstrapped `SqliteEngine` in a scratch directory, in the
/// account it bootstrapped, creating its tables ACTIVE. The directory lives
/// as long as the returned guard.
pub(super) async fn context() -> (OperationContext, TempDir) {
    let dir = tempfile::tempdir().expect("a scratch directory");
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
        .expect("the catalog is read")
        .expect("the bootstrapped account");
    components
        .catalog_store
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("the delay is set");
    let ctx = OperationContext {
        storage: components.engine,
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(account_id),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: AuthorizedKeyInfo::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: None,
    };
    (ctx, dir)
}

#[tokio::test]
async fn batch_write_item_alone_reads_a_binary_only_with_its_padding() {
    #[rustfmt::skip]
    let rows: [(&str, &str, Value, Answer); 13] = [
        ("w1", "BatchWriteItem", batch(S_TABLE, put(item("w1", json!({"B": "YQ"})))), length(2)),
        ("w2", "BatchWriteItem", batch(S_TABLE, put(item("w2", json!({"B": "YQ="})))), length(3)),
        ("w3", "BatchWriteItem", batch(S_TABLE, put(item("w3", json!({"B": "Y=Q="})))), stored("//Q=")),
        ("w4", "BatchWriteItem", batch(S_TABLE, put(item("w4", json!({"BS": ["YQ"]})))), length(2)),
        ("w5", "BatchWriteItem", batch(B_TABLE, json!({"DeleteRequest": {"Key": b_key()}})), length(2)),
        ("w6", "BatchWriteItem", batch(B_TABLE, put(json!({"pk": {"B": "Yg"}}))), length(2)),
        ("w7", "BatchGetItem", json!({"RequestItems": {B_TABLE: {"Keys": [b_key()]}}}), Ok(None)),
        ("w8", "TransactGetItems",
         json!({"TransactItems": [{"Get": {"TableName": B_TABLE, "Key": b_key()}}]}), Ok(None)),
        ("w9", "GetItem", json!({"TableName": B_TABLE, "Key": b_key()}), Ok(None)),
        ("w10", "DeleteItem", json!({"TableName": B_TABLE, "Key": b_key()}), Ok(None)),
        ("w11", "TransactWriteItems",
         json!({"TransactItems": [{"Put": {"TableName": S_TABLE, "Item": item("w11", json!({"B": "YQ"}))}}]}),
         stored("YQ==")),
        ("w12", "TransactWriteItems",
         json!({"TransactItems": [{"Delete": {"TableName": B_TABLE, "Key": b_key()}}]}), Ok(None)),
        ("w13", "UpdateItem",
         json!({"TableName": S_TABLE, "Key": {"pk": {"S": "w13"}},
                "UpdateExpression": "SET b = :v", "ExpressionAttributeValues": {":v": {"B": "YQ"}}}),
         stored("YQ==")),
    ];
    let (ctx, _dir) = context().await;
    for (table, kind) in [(S_TABLE, "S"), (B_TABLE, "B")] {
        let body = json!({
            "TableName": table,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": kind}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        });
        if let Err(e) = dispatch("CreateTable", body, &ctx, "").await {
            panic!("{table}: {e:?}");
        }
    }
    let mut wrong = Vec::new();
    for (row, operation, body, want) in rows {
        let got = match dispatch(operation, body, &ctx, "").await {
            Ok(_) => Ok(read_back(&ctx, row).await),
            Err(DynamoDbError::SerializationException(message)) => Err(message),
            Err(other) => Err(format!("{other:?}")),
        };
        if got != want {
            wrong.push(format!("{row}: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The `B` text of attribute `b` of the item `row` in the S table, if any.
async fn read_back(ctx: &OperationContext, row: &str) -> Option<String> {
    let body = json!({"TableName": S_TABLE, "Key": {"pk": {"S": row}}});
    let result = dispatch("GetItem", body, ctx, "").await.expect(row);
    result.body["Item"]["b"]["B"]
        .as_str()
        .map(ToOwned::to_owned)
}
