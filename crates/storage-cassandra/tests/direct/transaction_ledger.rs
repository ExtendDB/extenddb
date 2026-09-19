// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for transaction ledger operations.

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
