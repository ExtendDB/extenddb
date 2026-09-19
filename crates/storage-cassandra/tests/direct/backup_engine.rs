// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for Cassandra backup and restore operations.

use std::collections::BTreeMap;
use std::time::Duration;

use extenddb_core::types::{AttributeValue, DeleteTableInput, DescribeTableInput, TableStatus};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, DataEngine, TableEngine};

use crate::helpers::{TestAccount, TestTable, setup_engine, unique_test_account};

fn item(id: &str, sort: Option<&str>, value: &str) -> BTreeMap<String, AttributeValue> {
    let mut item = BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        item.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    item.insert("value".to_owned(), AttributeValue::S(value.to_owned()));
    item
}

fn key(id: &str, sort: Option<&str>) -> BTreeMap<String, AttributeValue> {
    let mut key = BTreeMap::new();
    key.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        key.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    key
}

async fn activate_tables(engine: &extenddb_storage_cassandra::CassandraEngine) {
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

#[tokio::test]
async fn test_backup_describe_list_delete_and_account_scope() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "BackupLifecycle", false).await;
    activate_tables(&engine).await;

    for (id, value) in [("one", "first"), ("two", "second")] {
        engine
            .put_item(
                &table.key_info,
                item(id, None, value),
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("seed item");
    }

    let details = engine
        .create_backup(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "daily",
        )
        .await
        .expect("create backup");
    assert_eq!(details.backup_name, "daily");
    assert_eq!(details.backup_status, "AVAILABLE");
    assert_eq!(details.backup_type, "USER");
    assert!(details.backup_arn.contains(&format!(
        ":{}:table/{}/backup/",
        table.key_info.account_id, table.key_info.table_name
    )));

    let description = engine
        .describe_backup(&table.key_info.account_id, &details.backup_arn)
        .await
        .expect("describe backup");
    assert_eq!(
        description.source_table_details.table_id,
        table.key_info.table_id
    );
    assert_eq!(description.source_table_details.item_count, 2);
    assert_eq!(
        description.source_table_details.key_schema,
        table.key_info.key_schema
    );

    let table_backups = engine
        .list_backups(&table.key_info.account_id, Some(&table.key_info.table_name))
        .await
        .expect("list table backups");
    assert_eq!(table_backups.len(), 1);
    assert_eq!(table_backups[0].backup_arn, details.backup_arn);
    assert_eq!(
        engine
            .list_backups(&table.key_info.account_id, None)
            .await
            .expect("list account backups")
            .len(),
        1
    );

    let foreign = unique_test_account();
    let foreign_error = engine
        .describe_backup(&foreign, &details.backup_arn)
        .await
        .expect_err("another account must not resolve the backup");
    assert!(
        matches!(foreign_error, StorageError::Validation(message) if message.contains("Backup not found"))
    );

    let deleted = engine
        .delete_backup(&table.key_info.account_id, &details.backup_arn)
        .await
        .expect("delete backup");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(
        engine
            .describe_backup(&table.key_info.account_id, &details.backup_arn)
            .await
            .is_err()
    );
    assert!(
        engine
            .list_backups(&table.key_info.account_id, None)
            .await
            .expect("list after delete")
            .is_empty()
    );
}

#[tokio::test]
async fn test_restore_uses_immutable_snapshot_with_sort_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "BackupRestoreSource", true).await;
    activate_tables(&engine).await;

    let original_one = item("partition", Some("one"), "before-backup");
    let original_two = item("partition", Some("two"), "preserved");
    for original in [&original_one, &original_two] {
        engine
            .put_item(
                &table.key_info,
                original.clone(),
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("seed source item");
    }

    let backup = engine
        .create_backup(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "immutable",
        )
        .await
        .expect("create backup");

    engine
        .put_item(
            &table.key_info,
            item("partition", Some("one"), "after-backup"),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("mutate source item");
    engine
        .put_item(
            &table.key_info,
            item("partition", Some("three"), "new-after-backup"),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("add source item");

    let target = "BackupRestoreTarget";
    let initial = engine
        .restore_table_from_backup(&table.key_info.account_id, target, &backup.backup_arn)
        .await
        .expect("restore backup");
    assert!(matches!(
        initial.table_status,
        TableStatus::Creating | TableStatus::Active
    ));

    let target_description = engine
        .describe_table(
            &table.key_info.account_id,
            DescribeTableInput {
                table_name: target.to_owned(),
            },
        )
        .await
        .expect("describe restored table");
    assert_eq!(target_description.table_status, TableStatus::Active);

    let restored_key_info = engine
        .table_key_info(&table.key_info.account_id, target)
        .await
        .expect("restored table key info");
    let restored_one = engine
        .get_item(&restored_key_info, &key("partition", Some("one")))
        .await
        .expect("get first restored item")
        .expect("first restored item exists");
    assert_eq!(restored_one.get("value"), original_one.get("value"));
    let restored_two = engine
        .get_item(&restored_key_info, &key("partition", Some("two")))
        .await
        .expect("get second restored item")
        .expect("second restored item exists");
    assert_eq!(restored_two.get("value"), original_two.get("value"));
    assert!(
        engine
            .get_item(&restored_key_info, &key("partition", Some("three")))
            .await
            .expect("get post-backup item")
            .is_none()
    );

    engine
        .delete_table(
            &table.key_info.account_id,
            DeleteTableInput {
                table_name: target.to_owned(),
            },
        )
        .await
        .expect("delete restored table");
    engine
        .delete_backup(&table.key_info.account_id, &backup.backup_arn)
        .await
        .expect("delete backup");
}

#[tokio::test]
async fn test_table_name_filter_spans_table_recreation() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_test").await;
    let table_name = "BackupRecreatedSource";
    let source = std::mem::ManuallyDrop::new(
        TestTable::with_account(&engine, &account.account_id, table_name, false).await,
    );
    activate_tables(&engine).await;

    engine
        .put_item(
            &source.key_info,
            item("original", None, "from-old-schema"),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("seed original table");
    let backup = engine
        .create_backup(&account.account_id, table_name, "before-recreate")
        .await
        .expect("backup original table");
    let original_table_id = source.key_info.table_id.clone();

    engine
        .delete_table(
            &account.account_id,
            DeleteTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("delete original table");
    activate_tables(&engine).await;

    // Reuse the source name with a different schema. ListBackups is name-based,
    // but restore must continue to use the immutable schema stored in the backup.
    let replacement = TestTable::with_account(&engine, &account.account_id, table_name, true).await;
    activate_tables(&engine).await;
    assert_ne!(replacement.key_info.table_id, original_table_id);
    assert_eq!(replacement.key_info.key_schema.len(), 2);

    let listed = engine
        .list_backups(&account.account_id, Some(table_name))
        .await
        .expect("list backups across table recreation");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].backup_arn, backup.backup_arn);

    let target = "BackupRecreatedRestore";
    engine
        .restore_table_from_backup(&account.account_id, target, &backup.backup_arn)
        .await
        .expect("restore backup from original table incarnation");
    let restored_key_info = engine
        .table_key_info(&account.account_id, target)
        .await
        .expect("restored table key info");
    assert_eq!(restored_key_info.key_schema.len(), 1);
    let restored = engine
        .get_item(&restored_key_info, &key("original", None))
        .await
        .expect("read restored item")
        .expect("restored item exists");
    assert_eq!(
        restored.get("value"),
        Some(&AttributeValue::S("from-old-schema".to_owned()))
    );

    engine
        .delete_table(
            &account.account_id,
            DeleteTableInput {
                table_name: target.to_owned(),
            },
        )
        .await
        .expect("delete restored table");
    engine
        .delete_backup(&account.account_id, &backup.backup_arn)
        .await
        .expect("delete backup");
}

#[tokio::test]
async fn test_continuous_backup_state_and_pitr_restore_rejection() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ContinuousBackupState", false).await;

    let initial = engine
        .describe_continuous_backups(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("describe default continuous backup state");
    assert_eq!(initial.continuous_backups_status, "ENABLED");
    let initial_pitr = initial.point_in_time_recovery_description.unwrap();
    assert_eq!(initial_pitr.point_in_time_recovery_status, "DISABLED");
    assert!(initial_pitr.earliest_restorable_date_time.is_none());

    let enabled = engine
        .update_continuous_backups(&table.key_info.account_id, &table.key_info.table_name, true)
        .await
        .expect("enable PITR state");
    let enabled_pitr = enabled.point_in_time_recovery_description.unwrap();
    assert_eq!(enabled_pitr.point_in_time_recovery_status, "ENABLED");
    assert!(enabled_pitr.earliest_restorable_date_time.is_some());
    assert!(enabled_pitr.latest_restorable_date_time.is_some());

    let restore_error = engine
        .restore_table_to_point_in_time(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "UnsupportedPitrTarget",
        )
        .await
        .expect_err("PITR restore must not fake a current-time snapshot");
    assert!(
        matches!(restore_error, StorageError::Validation(message) if message.contains("not yet supported"))
    );
}
