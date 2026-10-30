// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct tests for what the control plane answers while a table is
//! `DELETING`.
//!
//! Amazon DynamoDB answers a second `DeleteTable` and an `UpdateTable` in that
//! window with `ResourceInUseException` and the sentence `Attempt to change a
//! resource which is still in use: Table is being deleted: <name>` (measured
//! 2026-10-24). On this backend `DeleteTable` drops the data and the catalog
//! row in one call and never sets `DELETING` itself; the abandoned-restore
//! sweep does. The window is produced here by setting the row's status
//! directly.
//!
//! The catalog keyspace is shared by every test in this binary, and the
//! control-plane sweep (`process_control_plane_transitions`) removes any
//! `DELETING` row whose `status_transition_at` has passed, then clears the
//! table's TTL entries in the account keyspace. A fresh table carries its
//! activation deadline in that column, so a row left as `DELETING` with it
//! would be swept by whichever test runs the sweep next; after this test's
//! account keyspace is dropped, that sweep fails on the missing keyspace and
//! takes an unrelated test down with it. `set_status` therefore clears the
//! column, as activation does, which keeps the row out of the sweep, and each
//! test removes its row before it returns.

use extenddb_core::types::{DeleteTableInput, UpdateTableInput};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestAccount, TestTable, setup_engine, test_config};

const TABLE: &str = "DeletingWindowTable";

/// Sets the row's status and clears its transition deadline, so the
/// control-plane sweep leaves the row alone for the rest of the test.
async fn set_status(engine: &CassandraEngine, account_id: &str, status: &str) {
    let query = format!(
        "UPDATE {}.tables SET table_status = ?, status_transition_at = null \
         WHERE account_id = ? AND table_name = ? IF EXISTS",
        engine.catalog_keyspace()
    );
    engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(status, account_id, TABLE))
        .await
        .expect("set the table status");
}

/// Removes the catalog row the test set to a non-ACTIVE status; the account
/// keyspace goes with the `TestAccount`.
async fn remove_row(engine: &CassandraEngine, account_id: &str) {
    let query = format!(
        "DELETE FROM {}.tables WHERE account_id = ? AND table_name = ? IF EXISTS",
        engine.catalog_keyspace()
    );
    engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, TABLE))
        .await
        .expect("remove the table row");
}

async fn status_of(engine: &CassandraEngine, account_id: &str) -> Option<String> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT table_status FROM {}.tables WHERE account_id = ? AND table_name = ?",
        engine.catalog_keyspace()
    );
    let rows = engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, TABLE))
        .await
        .expect("read the row")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default();
    rows.first()
        .map(|row| row.get_r_by_name("table_status").expect("table_status"))
}

fn update_input() -> UpdateTableInput {
    UpdateTableInput {
        table_name: TABLE.to_owned(),
        billing_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: Some(true),
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        table_throughput_mode: None,
        vector_index_updates: None,
    }
}

/// An engine, an account, an ACTIVE `TABLE`, and its row set to `DELETING`.
async fn open_window() -> (CassandraEngine, TestAccount, TestTable) {
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let table = TestTable::with_account(&engine, &account.account_id, TABLE, false).await;
    set_status(&engine, &account.account_id, "DELETING").await;
    (engine, account, table)
}

/// A second DeleteTable during the window reports the table as being deleted,
/// not as absent, and does not run the drop: the row stays.
#[tokio::test]
async fn a_second_delete_table_during_the_window_is_table_being_deleted() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account, _table) = open_window().await;

    let err = engine
        .delete_table(
            &account.account_id,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect_err("DeleteTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    assert_eq!(
        status_of(&engine, &account.account_id).await.as_deref(),
        Some("DELETING")
    );
    remove_row(&engine, &account.account_id).await;
}

/// UpdateTable during the window reports the table as being deleted, not with
/// the sentence for the other non-ACTIVE statuses.
#[tokio::test]
async fn update_table_during_the_window_is_table_being_deleted() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account, _table) = open_window().await;

    let err = engine
        .update_table(&account.account_id, update_input())
        .await
        .expect_err("UpdateTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    remove_row(&engine, &account.account_id).await;
}

/// A CREATING table keeps the existing answer: the new sentence is for
/// DELETING only.
#[tokio::test]
async fn update_table_on_a_creating_table_keeps_table_not_active() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account, _table) = open_window().await;
    set_status(&engine, &account.account_id, "CREATING").await;

    let err = engine
        .update_table(&account.account_id, update_input())
        .await
        .expect_err("UpdateTable on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );
    remove_row(&engine, &account.account_id).await;
}

/// UpdateTimeToLive during the window writes the catalog row, the way the
/// service runs its ordinary TTL rules there (measured 2026-10-25), and leaves
/// the status alone; the other non-ACTIVE statuses keep their refusal. The
/// enable is undone before the row is removed so the account keyspace's TTL
/// entries do not outlive the test.
#[tokio::test]
async fn update_ttl_during_the_window_is_served() {
    use extenddb_core::types::TimeToLiveStatus;
    use extenddb_storage::MetadataEngine;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account, _table) = open_window().await;

    engine
        .update_ttl(&account.account_id, TABLE, "ttl", true)
        .await
        .expect("UpdateTimeToLive on a DELETING table");
    let desc = engine
        .describe_ttl(&account.account_id, TABLE)
        .await
        .expect("the TTL description");
    assert_eq!(desc.attribute_name.as_deref(), Some("ttl"));
    assert_ne!(desc.time_to_live_status, TimeToLiveStatus::Disabled);
    assert_eq!(
        status_of(&engine, &account.account_id).await.as_deref(),
        Some("DELETING"),
        "the write must not change the status"
    );

    engine
        .update_ttl(&account.account_id, TABLE, "ttl", false)
        .await
        .expect("the disable direction during the window");

    set_status(&engine, &account.account_id, "CREATING").await;
    let err = engine
        .update_ttl(&account.account_id, TABLE, "ttl", true)
        .await
        .expect_err("UpdateTimeToLive on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );

    remove_row(&engine, &account.account_id).await;
}
