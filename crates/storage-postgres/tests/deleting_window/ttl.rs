// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTimeToLive` while a table is `DELETING`, on the window the parent
//! module opens. Amazon DynamoDB runs the ordinary TTL rules on a `DELETING`
//! table (measured 2026-10-25), so the catalog write is admitted beside
//! `ACTIVE` and refused for the other statuses. The delete removes the data
//! table with its index, so the engine sends no index DDL for such a table.

use std::sync::Arc;

use extenddb_storage::MetadataEngine;
use serde_json::json;

use super::{
    ACCOUNT, Scratch, TABLE, base_conn, create_table_named, open_window, scratch, skip,
    start_deleting, status_of,
};

/// UpdateTimeToLive by the status of `TABLE`'s catalog row, on one window,
/// row after row. DELETING is admitted beside ACTIVE in both directions, the
/// way the service runs its ordinary TTL rules in the window (measured
/// 2026-10-25): the write lands on the row and leaves the status alone. A
/// CREATING table keeps `TableNotActive` and a missing one `TableNotFound`,
/// ExtendDB's current answers: the service answers `ResourceNotFoundException`
/// for a CREATING table (measured 2026-11-20).
#[tokio::test]
async fn update_ttl_by_table_status() {
    // (status of `TABLE`, table named, the calls' `enabled` in order, the
    // stored `ttl_attribute` after them or the last call's error)
    type Row = (
        &'static str,
        &'static str,
        &'static [bool],
        Result<Option<&'static str>, String>,
    );
    if base_conn().is_none() {
        return skip("update_ttl_by_table_status");
    }
    let s = scratch().await;
    open_window(&s).await;

    let rows: [Row; 4] = [
        ("DELETING", TABLE, &[true], Ok(Some("ttl"))),
        ("DELETING", TABLE, &[true, false], Ok(None)),
        (
            "CREATING",
            TABLE,
            &[true],
            Err(format!("TableNotActive({TABLE:?})")),
        ),
        (
            "DELETING",
            "t_absent",
            &[true],
            Err("TableNotFound(\"t_absent\")".to_owned()),
        ),
    ];
    for (status, table, calls, expected) in rows {
        let row = format!("{status} {table} {calls:?}");
        sqlx::query(
            "UPDATE tables SET table_status = $1 WHERE account_id = $2 AND table_name = $3",
        )
        .bind(status)
        .bind(ACCOUNT)
        .bind(TABLE)
        .execute(&s.catalog)
        .await
        .expect("set the status");
        let (last, earlier) = calls.split_last().expect("one call at least");
        for &enabled in earlier {
            s.engine
                .update_ttl(ACCOUNT, table, "ttl", enabled)
                .await
                .unwrap_or_else(|e| panic!("{row}: an earlier call failed: {e:?}"));
        }
        let result = s.engine.update_ttl(ACCOUNT, table, "ttl", *last).await;
        match expected {
            Ok(attribute) => {
                result.unwrap_or_else(|e| panic!("{row}: {e:?}"));
                let stored: Option<String> = sqlx::query_scalar(
                    "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
                )
                .bind(ACCOUNT)
                .bind(table)
                .fetch_one(&s.catalog)
                .await
                .expect("read the TTL attribute");
                assert_eq!(stored.as_deref(), attribute, "{row}");
            }
            Err(error) => assert_eq!(format!("{:?}", result.expect_err(&row)), error, "{row}"),
        }
        assert_eq!(
            status_of(&s.catalog, TABLE).await.as_deref(),
            Some(status),
            "{row}: the write must not change the status"
        );
    }

    s.cleanup().await;
}

/// The TTL index names in the scratch data database, which holds one table.
async fn ttl_indexes(s: &Scratch) -> Vec<String> {
    sqlx::query_scalar("SELECT indexname FROM pg_indexes WHERE indexname LIKE 'idx\\_ttl\\_%'")
        .fetch_all(s.engine.data_pool())
        .await
        .expect("list the TTL indexes")
}

/// The engine's request context over the scratch engine.
pub(super) fn engine_context(s: &Scratch) -> extenddb_engine::OperationContext {
    extenddb_engine::OperationContext {
        storage: Arc::new(s.engine.clone()),
        backup: Arc::new(extenddb_engine::BackupRuntime::new(1)),
        limits: Arc::new(extenddb_core::limits::LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: extenddb_engine::AuthorizedKeyInfo::default(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
        item_authorizer: None,
    }
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

    let ctx = engine_context(&s);
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
