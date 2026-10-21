// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for transaction ledger operations.

use std::sync::Arc;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine as _;
use extenddb_storage::error::StorageError;
use extenddb_storage_cassandra::data::transaction_ledger::TransactionState;
use uuid::Uuid;

use crate::helpers::{ensure_test_account, setup_engine, unique_test_account};

#[tokio::test]
async fn test_write_and_read_ledger_entry() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let txn_id = Uuid::new_v4();
    let started_at = 1000000;
    let items_blob = r#"[{"table":"t1","pk":"a","sk":"b"}]"#;

    // Write ledger entry
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Preparing,
            started_at,
            Some("client-token-1"),
            Some("fingerprint-1"),
            items_blob,
        )
        .await
        .expect("Write ledger entry failed");

    // Read it back
    let entry = engine
        .read_ledger_entry(&keyspace, txn_id)
        .await
        .expect("Read ledger entry failed")
        .expect("Entry should exist");

    assert_eq!(entry.txn_id, txn_id);
    assert_eq!(entry.state, "PREPARING");
    assert_eq!(entry.started_at, started_at);
    assert_eq!(entry.client_token, Some("client-token-1".to_string()));
    assert_eq!(entry.request_fingerprint, Some("fingerprint-1".to_string()));
    assert_eq!(entry.items_blob, items_blob);
}

#[tokio::test]
async fn test_write_duplicate_txn_id_fails() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let txn_id = Uuid::new_v4();
    let items_blob = r#"[{"table":"t1"}]"#;

    // First write succeeds
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Preparing,
            1000000,
            None,
            None,
            items_blob,
        )
        .await
        .expect("First write should succeed");

    // Second write with same txn_id fails
    let result = engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Preparing,
            2000000,
            None,
            None,
            items_blob,
        )
        .await;

    assert!(result.is_err(), "Duplicate txn_id should fail");
    match result {
        Err(StorageError::Internal(msg)) => {
            assert!(msg.contains("already exists"), "Error message: {}", msg);
        }
        _ => panic!("Expected Internal error with 'already exists'"),
    }
}

#[tokio::test]
async fn test_update_ledger_state() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let txn_id = Uuid::new_v4();
    let items_blob = r#"[{"table":"t1"}]"#;

    // Create entry
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Preparing,
            1000000,
            None,
            None,
            items_blob,
        )
        .await
        .unwrap();

    // Update state to committing
    engine
        .update_ledger_state(&keyspace, txn_id, TransactionState::Committing)
        .await
        .expect("Update state failed");

    // Verify state changed
    let entry = engine
        .read_ledger_entry(&keyspace, txn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.state, "COMMITTING");

    // Update to cancelling
    engine
        .update_ledger_state(&keyspace, txn_id, TransactionState::Cancelling)
        .await
        .unwrap();

    let entry = engine
        .read_ledger_entry(&keyspace, txn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.state, "CANCELLING");
}

#[tokio::test]
async fn test_delete_ledger_entry() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let txn_id = Uuid::new_v4();
    let items_blob = r#"[{"table":"t1"}]"#;

    // Create entry
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Preparing,
            1000000,
            None,
            None,
            items_blob,
        )
        .await
        .unwrap();

    // Verify it exists
    assert!(
        engine
            .read_ledger_entry(&keyspace, txn_id)
            .await
            .unwrap()
            .is_some()
    );

    // Delete it
    engine
        .delete_ledger_entry(&keyspace, txn_id)
        .await
        .expect("Delete failed");

    // Verify it's gone
    assert!(
        engine
            .read_ledger_entry(&keyspace, txn_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_scan_old_transactions() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let items_blob = r#"[{"table":"t1"}]"#;

    // Create transactions with different timestamps
    let old_txn_1 = Uuid::new_v4();
    let old_txn_2 = Uuid::new_v4();
    let recent_txn = Uuid::new_v4();

    engine
        .write_ledger_entry(
            &keyspace,
            old_txn_1,
            TransactionState::Preparing,
            1000,
            None,
            None,
            items_blob,
        )
        .await
        .unwrap();
    engine
        .write_ledger_entry(
            &keyspace,
            old_txn_2,
            TransactionState::Committing,
            2000,
            None,
            None,
            items_blob,
        )
        .await
        .unwrap();
    engine
        .write_ledger_entry(
            &keyspace,
            recent_txn,
            TransactionState::Preparing,
            100000,
            None,
            None,
            items_blob,
        )
        .await
        .unwrap();

    // Scan for transactions older than 50000
    let old_txns = engine
        .scan_old_transactions(&keyspace, 50000)
        .await
        .expect("Scan failed");

    assert_eq!(old_txns.len(), 2, "Should find 2 old transactions");

    let old_ids: Vec<Uuid> = old_txns.iter().map(|e| e.txn_id).collect();
    assert!(old_ids.contains(&old_txn_1));
    assert!(old_ids.contains(&old_txn_2));
    assert!(!old_ids.contains(&recent_txn));
}

#[tokio::test]
async fn test_read_nonexistent_ledger_entry() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id).await.unwrap();
    let keyspace = engine.account_keyspace(&account_id);

    let txn_id = Uuid::new_v4();
    let entry = engine
        .read_ledger_entry(&keyspace, txn_id)
        .await
        .expect("Query should succeed");

    assert!(entry.is_none(), "Should return None for nonexistent entry");
}

// ---------------------------------------------------------------------------
// COMMITTING recovery and secondary-index rows.
// ---------------------------------------------------------------------------

/// Read the base row's `version` and `prepared_txn_id` for a hash-key item.
async fn base_row_state(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    keyspace: &str,
    table_id: &str,
    pk: &str,
) -> (Option<i64>, Option<Uuid>) {
    use cdrs_tokio::types::IntoRustByName as _;
    let table = format!("items_{}", table_id.replace('-', "_"));
    let cql = format!("SELECT version, prepared_txn_id FROM {keyspace}.{table} WHERE pk = ?");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(pk),
        "base_row_state",
    )
    .await
    .unwrap();
    let row = rows.into_iter().next().expect("row");
    (
        row.get_by_name("version").ok().flatten(),
        row.get_by_name("prepared_txn_id").ok().flatten(),
    )
}

/// GSI keys of every index row for item `id`, read straight from the index table.
async fn gsi_rows_for(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    keyspace: &str,
    table_id: &str,
    gsi_name: &str,
    id: &str,
) -> Vec<String> {
    use cdrs_tokio::types::IntoRustByName as _;
    let catalog = engine.catalog_keyspace();
    let cql =
        format!("SELECT index_id FROM {catalog}.indexes WHERE table_id = ? AND index_name = ?");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(table_id, gsi_name),
        "gsi_rows_for",
    )
    .await
    .unwrap();
    let index_id: String = rows[0].get_r_by_name("index_id").unwrap();
    let idx_table = extenddb_storage_cassandra::data::ddl::index_table_name(&index_id);
    let cql = format!("SELECT pk, base_pk FROM {keyspace}.{idx_table}");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(),
        "gsi_rows_for",
    )
    .await
    .unwrap();
    let mut keys: Vec<String> = rows
        .iter()
        .filter(|row| {
            let base_pk: String = row.get_r_by_name("base_pk").unwrap();
            base_pk == id
        })
        .map(|row| {
            let pk: String = row.get_r_by_name("pk").unwrap();
            pk
        })
        .collect();
    keys.sort();
    keys
}

fn ledger_put_op(table_id: &str, pk: &str, pre: &Item, committed: &Item) -> String {
    serde_json::json!([{
        "op": "PUT",
        "table_id": table_id,
        "pk": pk,
        "sk_col": null,
        "sk_val": null,
        "item_data": serde_json::to_string(committed).unwrap(),
        "pre_commit_item_data": serde_json::to_string(pre).unwrap(),
    }])
    .to_string()
}

/// Move the just-created table to ACTIVE (there is no control-plane worker in
/// the direct tests) and make its GSI synchronous so index rows are readable
/// right after each write.
async fn activate_with_sync_gsi(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    table_id: &str,
    gsi_name: &str,
) {
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
    let catalog = engine.catalog_keyspace();
    let cql = format!(
        "UPDATE {catalog}.indexes SET propagation_delay_ms = ? WHERE table_id = ? AND index_name = ?"
    );
    extenddb_storage_cassandra::cassandra_util::execute::<StorageError>(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(0_i32, table_id, gsi_name),
        "set_gsi_delay",
    )
    .await
    .unwrap();
}

fn gsi_item(id: &str, gpk: &str, val: &str) -> Item {
    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S(id.to_string()));
    item.insert("gpk".to_string(), AttributeValue::S(gpk.to_string()));
    item.insert("val".to_string(), AttributeValue::S(val.to_string()));
    item
}

#[tokio::test]
async fn test_committing_recovery_leaves_a_row_a_later_writer_owns_alone() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // A transaction committed K2 over K1 and crashed before deleting its
    // ledger entry; a later ordinary write then moved the item to K3. Recovery
    // finds a COMMITTING entry whose commit fence no longer applies. It must
    // not write K2's index row: the later writer owns the row and its index
    // batch, and a K2 row stamped above K3's would be a permanent phantom.
    let engine = Arc::new(setup_engine().await);
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "RecoveryStaleTable", "GsiIdx", "gpk").await;
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let table_id = table.key_info.table_id.clone();
    activate_with_sync_gsi(&engine, &table_id, "GsiIdx").await;
    let maps = ExpressionMaps::default();

    let k1 = gsi_item("rec", "K1", "1");
    let k2 = gsi_item("rec", "K2", "2");
    let k3 = gsi_item("rec", "K3", "3");
    engine
        .put_item(&table.key_info, k1.clone(), false, None, &maps, None)
        .await
        .unwrap();
    engine
        .put_item(&table.key_info, k3.clone(), false, None, &maps, None)
        .await
        .unwrap();
    let (version_before, prepared) = base_row_state(&engine, &keyspace, &table_id, "rec").await;
    assert!(prepared.is_none());
    assert_eq!(
        gsi_rows_for(&engine, &keyspace, &table_id, "GsiIdx", "rec").await,
        vec!["K3"]
    );

    let txn_id = Uuid::new_v4();
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Committing,
            0,
            None,
            None,
            &ledger_put_op(&table_id, "rec", &k1, &k2),
        )
        .await
        .unwrap();
    engine.recover_transaction(&keyspace, txn_id).await.unwrap();

    let (version_after, _) = base_row_state(&engine, &keyspace, &table_id, "rec").await;
    assert_eq!(
        version_after, version_before,
        "recovery rewrote a row it does not own"
    );
    assert_eq!(
        gsi_rows_for(&engine, &keyspace, &table_id, "GsiIdx", "rec").await,
        vec!["K3"],
        "recovery resurrected the stale committed image's index row"
    );
    assert!(
        engine
            .read_ledger_entry(&keyspace, txn_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_committing_recovery_repairs_the_index_rows_of_a_row_it_commits() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // The row is still fenced by the crashed transaction: PREPARE marked it
    // and the commit never landed. Recovery commits K2 with a concrete version
    // and replaces K1's index row with K2's.
    let engine = Arc::new(setup_engine().await);
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "RecoveryOwnedTable", "GsiIdx", "gpk").await;
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let table_id = table.key_info.table_id.clone();
    activate_with_sync_gsi(&engine, &table_id, "GsiIdx").await;
    let maps = ExpressionMaps::default();

    let k1 = gsi_item("own", "K1", "1");
    let k2 = gsi_item("own", "K2", "2");
    engine
        .put_item(&table.key_info, k1.clone(), false, None, &maps, None)
        .await
        .unwrap();
    let (version_before, _) = base_row_state(&engine, &keyspace, &table_id, "own").await;

    let txn_id = Uuid::new_v4();
    let data_table = format!("items_{}", table_id.replace('-', "_"));
    let fence = format!(
        "UPDATE {keyspace}.{data_table} SET prepared_txn_id = ?, prepared_txn_timestamp = ? WHERE pk = ? IF prepared_txn_id = null"
    );
    extenddb_storage_cassandra::cassandra_util::execute::<StorageError>(
        &engine.session_arc(),
        &fence,
        cdrs_tokio::query_values!(txn_id, 1_i64, "own"),
        "prepare by hand",
    )
    .await
    .unwrap();
    engine
        .write_ledger_entry(
            &keyspace,
            txn_id,
            TransactionState::Committing,
            0,
            None,
            None,
            &ledger_put_op(&table_id, "own", &k1, &k2),
        )
        .await
        .unwrap();
    engine.recover_transaction(&keyspace, txn_id).await.unwrap();

    let (version_after, prepared) = base_row_state(&engine, &keyspace, &table_id, "own").await;
    assert!(prepared.is_none(), "recovery left the fence in place");
    assert!(
        version_after.is_some() && version_after > version_before,
        "{version_after:?} vs {version_before:?}"
    );
    assert_eq!(
        gsi_rows_for(&engine, &keyspace, &table_id, "GsiIdx", "own").await,
        vec!["K2"]
    );
    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S("own".to_string()));
    let got = engine.get_item(&table.key_info, &key).await.unwrap();
    assert_eq!(
        got.and_then(|i| i.get("gpk").cloned()),
        Some(AttributeValue::S("K2".to_string()))
    );
}
