// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `TableStatus` an `UpdateTable` answers, and what the table serves
//! while that status lasts, against the SQLite backend in process. The
//! expected answers are Amazon DynamoDB's, measured in us-east-1 on
//! 2026-11-13, 2026-11-14 and 2026-11-17 on tables keyed on one S attribute.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_storage::server_components::ServerComponentsOptions;
use serde_json::{Value, json};

use crate::{AuthorizedKeyInfo, BackupRuntime, OperationContext, dispatch};

const KEY: &str = "pk";
const SERVER: &str = "127.0.0.1:8000";

/// A context over a fresh in-memory catalog whose `control_plane_delay_seconds`
/// is 0, so a created table is ACTIVE at once, and the catalog's settings, to
/// set the delay before an update.
async fn context() -> (OperationContext, Arc<dyn extenddb_storage::CatalogStore>) {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 1,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        "us-east-1",
        ServerComponentsOptions::default(),
    )
    .await
    .expect("an in-memory catalog opens");
    let settings = components.catalog_store;
    set_delay(&settings, "0").await;
    let account_id = settings
        .default_account_id()
        .await
        .expect("the default account is read")
        .expect("the catalog has a default account");
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
    (ctx, settings)
}

async fn set_delay(settings: &Arc<dyn extenddb_storage::CatalogStore>, delay: &str) {
    settings
        .set_setting("control_plane_delay_seconds", delay)
        .await
        .expect("the delay is set");
}

async fn call(
    ctx: &OperationContext,
    operation: &str,
    body: Value,
) -> Result<Value, DynamoDbError> {
    dispatch(operation, body, ctx, SERVER)
        .await
        .map(|result| result.body)
}

async fn describe_status(ctx: &OperationContext, table: &str) -> String {
    let desc = call(ctx, "DescribeTable", json!({"TableName": table}))
        .await
        .expect("DescribeTable");
    desc["Table"]["TableStatus"]
        .as_str()
        .expect("a status")
        .to_owned()
}

/// `{"TableName": table}` with `members` added.
fn request(table: &str, members: &Value) -> Value {
    let mut body = json!({"TableName": table});
    for (k, v) in members.as_object().expect("members") {
        body[k] = v.clone();
    }
    body
}

/// Create `table`, keyed on one S attribute, with `members` added to the
/// request. With no delay set it is ACTIVE once this returns.
async fn create(ctx: &OperationContext, table: &str, members: &Value) {
    let mut body = request(table, members);
    body["KeySchema"] = json!([{"AttributeName": KEY, "KeyType": "HASH"}]);
    body["AttributeDefinitions"] = json!([{"AttributeName": KEY, "AttributeType": "S"}]);
    call(ctx, "CreateTable", body).await.expect("CreateTable");
    assert_eq!(describe_status(ctx, table).await, "ACTIVE", "{table}");
}

async fn update(
    ctx: &OperationContext,
    table: &str,
    members: &Value,
) -> Result<Value, DynamoDbError> {
    call(ctx, "UpdateTable", request(table, members)).await
}

fn provisioned_5() -> Value {
    json!({"BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}})
}

fn on_demand() -> Value {
    json!({"BillingMode": "PAY_PER_REQUEST"})
}

/// One change of each kind that opens the window: the table before the
/// request and the request's members. The service answered UPDATING, and its
/// DescribeTable right after reported UPDATING too.
fn rows() -> [(Value, Value); 4] {
    [
        (
            provisioned_5(),
            json!({"ProvisionedThroughput": {"ReadCapacityUnits": 9, "WriteCapacityUnits": 11}}),
        ),
        (provisioned_5(), json!({"BillingMode": "PAY_PER_REQUEST"})),
        (
            on_demand(),
            json!({"TableClass": "STANDARD_INFREQUENT_ACCESS"}),
        ),
        (
            on_demand(),
            json!({"StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}}),
        ),
    ]
}

/// Each row: UPDATING in the response and in the DescribeTable right after,
/// and ACTIVE once `control_plane_delay_seconds` has passed.
#[tokio::test]
async fn each_change_answers_updating_until_the_window_ends() {
    let (ctx, settings) = context().await;
    for (i, (before, members)) in rows().into_iter().enumerate() {
        let table = format!("tbl{i}");
        set_delay(&settings, "0").await;
        create(&ctx, &table, &before).await;
        set_delay(&settings, "1").await;
        let response = update(&ctx, &table, &members)
            .await
            .unwrap_or_else(|e| panic!("row {i} {members}: {e:?}"));
        assert_eq!(
            response["TableDescription"]["TableStatus"], "UPDATING",
            "row {i} {members}: the response"
        );
        assert_eq!(
            describe_status(&ctx, &table).await,
            "UPDATING",
            "row {i} {members}: DescribeTable right after"
        );
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            describe_status(&ctx, &table).await,
            "ACTIVE",
            "row {i} {members}: after the window"
        );
    }
}

/// Inside a capacity change's window: another capacity change is refused
/// with the service's IOPS sentence; a `DeletionProtectionEnabled` change
/// answers ACTIVE while DescribeTable still says UPDATING; the item
/// operations, UpdateTimeToLive and CreateBackup are served.
#[tokio::test]
async fn inside_a_window_a_capacity_change_is_refused_and_the_table_is_served() {
    let (ctx, settings) = context().await;
    create(&ctx, "tbl", &provisioned_5()).await;
    set_delay(&settings, "3600").await;
    update(
        &ctx,
        "tbl",
        &json!({"ProvisionedThroughput": {"ReadCapacityUnits": 6, "WriteCapacityUnits": 6}}),
    )
    .await
    .expect("the first change");
    assert_eq!(describe_status(&ctx, "tbl").await, "UPDATING");

    let iops = "Attempt to change a resource which is still in use: Table IOPS are currently \
                being updated. Table: tbl";
    for members in [
        json!({"ProvisionedThroughput": {"ReadCapacityUnits": 20, "WriteCapacityUnits": 20}}),
        json!({"BillingMode": "PAY_PER_REQUEST"}),
    ] {
        let err = update(&ctx, "tbl", &members)
            .await
            .expect_err("refused inside the window");
        assert!(
            matches!(&err, DynamoDbError::ResourceInUseException(m) if m == iops),
            "{members}: {err:?}"
        );
    }

    for (operation, body, pointer, expected) in [
        (
            "UpdateTable",
            json!({"TableName": "tbl", "DeletionProtectionEnabled": true}),
            "/TableDescription/TableStatus",
            json!("ACTIVE"),
        ),
        (
            "PutItem",
            json!({"TableName": "tbl", "Item": {KEY: {"S": "a"}}}),
            "",
            json!({}),
        ),
        (
            "GetItem",
            json!({"TableName": "tbl", "Key": {KEY: {"S": "a"}}}),
            "/Item/pk/S",
            json!("a"),
        ),
        (
            "UpdateTimeToLive",
            json!({"TableName": "tbl",
                "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "ttl"}}),
            "/TimeToLiveSpecification/AttributeName",
            json!("ttl"),
        ),
        (
            "CreateBackup",
            json!({"TableName": "tbl", "BackupName": "bkp"}),
            "/BackupDetails/BackupName",
            json!("bkp"),
        ),
    ] {
        let out = call(&ctx, operation, body)
            .await
            .unwrap_or_else(|e| panic!("{operation} inside the window: {e:?}"));
        assert_eq!(out.pointer(pointer), Some(&expected), "{operation}: {out}");
        assert_eq!(
            describe_status(&ctx, "tbl").await,
            "UPDATING",
            "after {operation}"
        );
    }
}

/// With no control-plane delay the change applies at once and the response
/// says ACTIVE, as CreateTable answers ACTIVE at once then.
#[tokio::test]
async fn a_zero_delay_answers_active_at_once() {
    let (ctx, _settings) = context().await;
    create(&ctx, "tbl", &on_demand()).await;
    let response = update(
        &ctx,
        "tbl",
        &json!({"TableClass": "STANDARD_INFREQUENT_ACCESS"}),
    )
    .await
    .expect("UpdateTable");
    assert_eq!(response["TableDescription"]["TableStatus"], "ACTIVE");
    assert_eq!(describe_status(&ctx, "tbl").await, "ACTIVE");
}
