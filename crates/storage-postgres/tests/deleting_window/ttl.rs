// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTimeToLive` while a table is `DELETING`, on the window the parent
//! module opens. Amazon DynamoDB runs the ordinary TTL rules on a `DELETING`
//! table (measured 2026-10-25), so the catalog write is admitted beside
//! `ACTIVE` and refused for the other statuses. The delete removes the data
//! table with its index, so the engine sends no index DDL for such a table.

use std::sync::Arc;

use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use serde_json::json;

use super::{
    ACCOUNT, Scratch, TABLE, base_conn, create_table_named, open_window, scratch, skip,
    start_deleting, status_of,
};

/// UpdateTimeToLive during the window writes the catalog row, the way the
/// service runs its ordinary TTL rules there (measured 2026-10-25), and leaves
/// the status alone. The disable direction is served too.
#[tokio::test]
async fn update_ttl_during_the_window_is_served() {
    if base_conn().is_none() {
        return skip("update_ttl_during_the_window_is_served");
    }
    let s = scratch().await;
    open_window(&s).await;

    s.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect("UpdateTimeToLive on a DELETING table");
    let attr: Option<String> = sqlx::query_scalar(
        "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.catalog)
    .await
    .expect("read the TTL attribute");
    assert_eq!(attr.as_deref(), Some("ttl"));
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING"),
        "the write must not change the status"
    );

    s.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", false)
        .await
        .expect("the disable direction during the window");
    let attr: Option<String> = sqlx::query_scalar(
        "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.catalog)
    .await
    .expect("read the TTL attribute");
    assert_eq!(attr, None);

    s.cleanup().await;
}

/// A CREATING table keeps the TTL refusal: the window admits DELETING only,
/// and a table with no row at all is still reported as absent.
#[tokio::test]
async fn update_ttl_outside_active_and_deleting_keeps_its_refusal() {
    if base_conn().is_none() {
        return skip("update_ttl_outside_active_and_deleting_keeps_its_refusal");
    }
    let s = scratch().await;
    open_window(&s).await;
    sqlx::query(
        "UPDATE tables SET table_status = 'CREATING' WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .execute(&s.catalog)
    .await
    .expect("mark CREATING");

    let err = s
        .engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect_err("UpdateTimeToLive on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );

    let err = s
        .engine
        .update_ttl(ACCOUNT, "t_absent", "ttl", true)
        .await
        .expect_err("UpdateTimeToLive on a missing table");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == "t_absent"),
        "expected TableNotFound(\"t_absent\"), got {err:?}"
    );

    s.cleanup().await;
}

/// The TTL index names in the scratch data database, which holds one table.
async fn ttl_indexes(s: &Scratch) -> Vec<String> {
    sqlx::query_scalar("SELECT indexname FROM pg_indexes WHERE indexname LIKE 'idx\\_ttl\\_%'")
        .fetch_all(s.engine.data_pool())
        .await
        .expect("list the TTL indexes")
}

/// UpdateTimeToLive disabling TTL during the window, through the engine
/// handler, sends no DROP INDEX: the index built while the table was ACTIVE is
/// still there when the call returns, the catalog write landed, and the row is
/// still DELETING.
#[tokio::test]
async fn update_ttl_disable_during_the_window_sends_no_drop_index() {
    if base_conn().is_none() {
        return skip("update_ttl_disable_during_the_window_sends_no_drop_index");
    }
    let s = scratch().await;
    create_table_named(&s, TABLE).await;
    s.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect("enable TTL while ACTIVE");
    s.engine
        .create_ttl_index(ACCOUNT, TABLE, "ttl")
        .await
        .expect("build the TTL index while ACTIVE");
    let built = ttl_indexes(&s).await;
    assert_eq!(built.len(), 1, "one TTL index before the delete: {built:?}");
    start_deleting(&s).await;

    let ctx = extenddb_engine::OperationContext {
        storage: Arc::new(s.engine.clone()),
        limits: Arc::new(extenddb_core::limits::LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: extenddb_engine::AuthorizedKeyInfo::default(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    };
    let out = extenddb_engine::handle_update_time_to_live(
        json!({"TableName": TABLE,
               "TimeToLiveSpecification": {"Enabled": false, "AttributeName": "ttl"}}),
        &ctx,
    )
    .await
    .expect("UpdateTimeToLive disable during the window");
    assert_eq!(
        out["TimeToLiveSpecification"],
        json!({"Enabled": false, "AttributeName": "ttl"})
    );
    drop(ctx);

    assert_eq!(ttl_indexes(&s).await, built, "no DROP INDEX was sent");
    let attr: Option<String> = sqlx::query_scalar(
        "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.catalog)
    .await
    .expect("read the TTL attribute");
    assert_eq!(attr, None, "the catalog write landed");
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING")
    );

    s.cleanup().await;
}
