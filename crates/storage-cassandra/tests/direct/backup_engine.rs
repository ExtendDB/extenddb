// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for Cassandra backup and restore operations.
//!
//! Restores go through the same steps the engine runs
//! (`backup_restore_source`, `ensure_restorable`, `restore_input`, then
//! `restore_table_from_backup`), because the backend now receives the
//! resolved `CreateTableInput` rather than the backup ARN alone.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiInput, KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType,
    ProvisionedThroughput, ScalarAttributeType, TableDescription, TableKeyInfo, TableStatus,
    TimeToLiveStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupEngine, DataEngine, RestoreRequest, RestoreTableOverrides, TableEngine,
};
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;

use crate::helpers::{TestAccount, TestTable, setup_engine, unique_test_account};

type Item = BTreeMap<String, AttributeValue>;

pub(crate) fn item(id: &str, sort: Option<&str>, value: &str) -> Item {
    let mut item = BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        item.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    item.insert("value".to_owned(), AttributeValue::S(value.to_owned()));
    item
}

pub(crate) fn key(id: &str, sort: Option<&str>) -> Item {
    let mut key = BTreeMap::new();
    key.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        key.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    key
}

pub(crate) fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

pub(crate) fn attr(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    }
}

pub(crate) async fn activate_tables(engine: &CassandraEngine) {
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

/// Create `input` in `account_id`, activate it, and return its key info.
pub(crate) async fn create_active_table(
    engine: &CassandraEngine,
    account_id: &str,
    input: CreateTableInput,
) -> TableKeyInfo {
    let name = input.table_name.clone();
    engine
        .create_table(account_id, input)
        .await
        .expect("create table");
    activate_tables(engine).await;
    engine
        .table_key_info(account_id, &name)
        .await
        .expect("source key info")
}

/// Make every GSI of a table synchronous, so the source's indexes are
/// complete without the GSI propagation workers (not running in these tests).
pub(crate) async fn make_gsis_synchronous(
    engine: &CassandraEngine,
    table_id: &str,
    names: &[&str],
) {
    let query = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        engine.catalog_keyspace()
    );
    for name in names {
        engine
            .session()
            .query_with_values(&query, cdrs_tokio::query_values!(table_id, *name))
            .await
            .expect("set GSI synchronous");
    }
}

pub(crate) async fn put_all(engine: &CassandraEngine, key_info: &TableKeyInfo, items: Vec<Item>) {
    let mut writes = futures::stream::iter(items)
        .map(|item| engine.put_item(key_info, item, false, None, &Default::default(), None))
        .buffer_unordered(32);
    while let Some(result) = writes.next().await {
        result.expect("seed item");
    }
}

pub(crate) async fn describe(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> TableDescription {
    engine
        .describe_table(
            account_id,
            DescribeTableInput {
                table_name: name.to_owned(),
            },
        )
        .await
        .expect("describe table")
}

/// The ARN of a backup that does not exist, with a well-formed id: `arn`
/// with its last hex digit changed.
fn flip_last_hex(arn: &str) -> String {
    let (head, last) = arn.split_at(arn.len() - 1);
    format!("{head}{}", if last == "0" { "1" } else { "0" })
}

/// Restore `backup_arn` into `target` the way the engine does.
pub(crate) async fn restore(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    target: &str,
    overrides: &RestoreTableOverrides,
) -> Result<TableDescription, StorageError> {
    let source = engine.backup_restore_source(account_id, backup_arn).await?;
    source.ensure_restorable()?;
    let input = source.restore_input(target, overrides);
    engine
        .restore_table_from_backup(
            account_id,
            RestoreRequest {
                backup_arn: backup_arn.to_owned(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input,
            },
        )
        .await
}

/// Poll until `name` is ACTIVE; panic after `timeout`.
pub(crate) async fn wait_active(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
    timeout: Duration,
) -> TableDescription {
    let deadline = Instant::now() + timeout;
    loop {
        let description = describe(engine, account_id, name).await;
        if description.table_status == TableStatus::Active {
            return description;
        }
        assert!(
            Instant::now() < deadline,
            "restore target {name} still {:?} after {timeout:?}",
            description.table_status
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn scan_count(engine: &CassandraEngine, key_info: &TableKeyInfo) -> usize {
    let mut count = 0;
    let mut start: Option<Item> = None;
    loop {
        let (items, next) = engine
            .scan(key_info, None, start.as_ref(), None, None, None)
            .await
            .expect("scan");
        count += items.len();
        match next {
            Some(next) => start = Some(next),
            None => return count,
        }
    }
}

/// Every item a Query on `index` for `pk_attr = value` returns, all pages.
pub(crate) async fn query_index(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    index: &str,
    pk_attr: &str,
    value: AttributeValue,
) -> Vec<Item> {
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute(pk_attr.to_owned())],
        pk_value: Expr::Placeholder(":v".to_owned()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };
    let mut maps = ExpressionMaps::default();
    maps.values.insert(":v".to_owned(), value);
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let (items, next) = engine
            .query(
                key_info,
                &condition,
                &maps,
                true,
                None,
                start.as_ref(),
                Some(index),
            )
            .await
            .expect("query index");
        out.extend(items);
        match next {
            Some(next) => start = Some(next),
            None => return out,
        }
    }
}

/// A source table of `n` items with no secondary indexes, for the copy-timing
/// tests. Returns (account, key info, backup ARN).
async fn backed_up_plain_table(
    engine: &CassandraEngine,
    name: &str,
    n: usize,
) -> (TestAccount, TableKeyInfo, String) {
    let account = TestAccount::new(engine, "extenddb_ttl_test").await;
    let key_info = create_active_table(
        engine,
        &account.account_id,
        CreateTableInput {
            table_name: name.to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        engine,
        &key_info,
        (0..n)
            .map(|i| item(&format!("item-{i:05}"), None, &"x".repeat(200)))
            .collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account.account_id, name, "copy-timing")
        .await
        .expect("create backup");
    (account, key_info, backup.backup_arn)
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
    // The shared id: 14-digit epoch ms, and BackupCreationDateTime is that
    // instant to the millisecond.
    let embedded_ms = extenddb_storage::util::backup_id_epoch_millis(&details.backup_arn)
        .expect("ARN carries the shared backup id shape");
    #[allow(clippy::cast_precision_loss)]
    let embedded_seconds = embedded_ms as f64 / 1000.0;
    assert_eq!(details.backup_creation_date_time, embedded_seconds);

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
    assert_eq!(
        description.backup_details.backup_creation_date_time,
        embedded_seconds
    );

    let table_backups = engine
        .list_backups(&table.key_info.account_id, Some(&table.key_info.table_name))
        .await
        .expect("list table backups");
    assert_eq!(table_backups.len(), 1);
    assert_eq!(table_backups[0].backup_arn, details.backup_arn);
    assert_eq!(table_backups[0].table_id, table.key_info.table_id);
    let account_backups = engine
        .list_backups(&table.key_info.account_id, None)
        .await
        .expect("list account backups");
    assert_eq!(account_backups.len(), 1);
    assert_eq!(account_backups[0].table_id, table.key_info.table_id);
    assert_eq!(
        account_backups[0].backup_creation_date_time,
        embedded_seconds
    );

    let foreign = unique_test_account();
    let foreign_error = engine
        .describe_backup(&foreign, &details.backup_arn)
        .await
        .expect_err("another account must not resolve the backup");
    assert!(
        matches!(foreign_error, StorageError::Validation(message) if message.contains("Backup not found"))
    );
    let foreign_source = engine
        .backup_restore_source(&foreign, &details.backup_arn)
        .await
        .expect_err("another account must not resolve the restore source");
    assert!(
        matches!(foreign_source, StorageError::Validation(message) if message.contains("Backup not found"))
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
    let gone = engine
        .backup_restore_source(&table.key_info.account_id, &details.backup_arn)
        .await
        .expect_err("a deleted backup is not a restore source");
    assert!(matches!(gone, StorageError::Validation(m) if m.contains("Backup not found")));
}

#[tokio::test]
async fn test_restore_uses_immutable_snapshot_with_sort_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "BackupRestoreSource", true).await;
    activate_tables(&engine).await;
    let account_id = table.key_info.account_id.clone();

    let original_one = item("partition", Some("one"), "before-backup");
    let original_two = item("partition", Some("two"), "preserved");
    put_all(
        &engine,
        &table.key_info,
        vec![original_one.clone(), original_two.clone()],
    )
    .await;

    let backup = engine
        .create_backup(&account_id, &table.key_info.table_name, "immutable")
        .await
        .expect("create backup");

    put_all(
        &engine,
        &table.key_info,
        vec![
            item("partition", Some("one"), "after-backup"),
            item("partition", Some("three"), "new-after-backup"),
        ],
    )
    .await;

    let target = "BackupRestoreTarget";
    let initial = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore backup");
    assert_eq!(initial.table_status, TableStatus::Creating);

    let target_description =
        wait_active(&engine, &account_id, target, Duration::from_secs(60)).await;
    assert_eq!(target_description.item_count, 2);

    let restored_key_info = engine
        .table_key_info(&account_id, target)
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

    // A second restore into the same name is refused with the bare name.
    let taken = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect_err("the target name is taken");
    assert!(matches!(taken, StorageError::TableAlreadyExists(ref n) if n == target));

    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: target.to_owned(),
            },
        )
        .await
        .expect("delete restored table");
    engine
        .delete_backup(&account_id, &backup.backup_arn)
        .await
        .expect("delete backup");
}

#[tokio::test]
async fn test_table_name_filter_spans_table_recreation() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
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
    assert_eq!(listed[0].table_id, original_table_id);

    let target = "BackupRecreatedRestore";
    restore(
        &engine,
        &account.account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore backup from original table incarnation");
    wait_active(
        &engine,
        &account.account_id,
        target,
        Duration::from_secs(60),
    )
    .await;
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
async fn test_continuous_backup_state() {
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
}

/// (a) A table with a GSI (sparse, INCLUDE projection) and an LSI, more than
/// one backup bucket of items: the restore reaches ACTIVE with every item,
/// and a Query on each index returns exactly what it returns on the source.
#[tokio::test]
async fn test_restore_reproduces_gsi_and_lsi_contents() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: i64 = 200; // 64 per bucket: 4 buckets
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "IndexedSource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash), ks("sort", KeyType::Range)],
            attribute_definitions: vec![
                attr("id", ScalarAttributeType::S),
                attr("sort", ScalarAttributeType::S),
                attr("gpk", ScalarAttributeType::S),
                attr("gsk", ScalarAttributeType::N),
                attr("lsk", ScalarAttributeType::N),
            ],
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: Some(vec![GsiInput {
                index_name: "ByGroup".to_owned(),
                key_schema: vec![ks("gpk", KeyType::Hash), ks("gsk", KeyType::Range)],
                projection: Projection {
                    projection_type: ProjectionType::Include,
                    non_key_attributes: Some(vec!["value".to_owned()]),
                },
                provisioned_throughput: None,
            }]),
            local_secondary_indexes: Some(vec![LsiInput {
                index_name: "ByLocal".to_owned(),
                key_schema: vec![ks("id", KeyType::Hash), ks("lsk", KeyType::Range)],
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            }]),
            ..Default::default()
        },
    )
    .await;
    make_gsis_synchronous(&engine, &source.table_id, &["ByGroup"]).await;

    let mut items = Vec::new();
    for i in 0..ITEMS {
        let mut it = item(
            &format!("p{}", i % 7),
            Some(&format!("s{i:04}")),
            &format!("v{i}"),
        );
        it.insert("lsk".to_owned(), AttributeValue::N((1000 - i).to_string()));
        it.insert("extra".to_owned(), AttributeValue::S(format!("e{i}")));
        // Every tenth item is absent from the sparse GSI.
        if i % 10 != 0 {
            it.insert("gpk".to_owned(), AttributeValue::S(format!("g{}", i % 5)));
            it.insert("gsk".to_owned(), AttributeValue::N(i.to_string()));
        }
        items.push(it);
    }
    put_all(&engine, &source, items).await;

    let backup = engine
        .create_backup(&account_id, "IndexedSource", "indexed")
        .await
        .expect("create backup");
    let recorded = engine
        .backup_restore_source(&account_id, &backup.backup_arn)
        .await
        .expect("restore source");
    let definition = recorded.definition.as_ref().expect("definition recorded");
    assert_eq!(definition.global_secondary_indexes.len(), 1);
    assert_eq!(definition.local_secondary_indexes.len(), 1);
    assert_eq!(definition.billing_mode, "PAY_PER_REQUEST");
    assert_eq!(
        definition
            .time_to_live
            .as_ref()
            .map(|t| t.time_to_live_status),
        Some(TimeToLiveStatus::Disabled)
    );
    assert_eq!(recorded.item_count, ITEMS);

    let target = "IndexedTarget";
    let initial = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    assert_eq!(initial.table_status, TableStatus::Creating);
    assert_eq!(
        initial.global_secondary_indexes.as_ref().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        initial.local_secondary_indexes.as_ref().map(Vec::len),
        Some(1)
    );

    let active = wait_active(&engine, &account_id, target, Duration::from_secs(120)).await;
    assert_eq!(active.item_count, ITEMS);
    let restored = engine
        .table_key_info(&account_id, target)
        .await
        .expect("target key info");
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let expected_items = ITEMS as usize;
    assert_eq!(scan_count(&engine, &restored).await, expected_items);

    // Liveness: the source indexes are populated, so equality below is not
    // two empty answers.
    let mut gsi_total = 0;
    for g in 0..5 {
        let value = AttributeValue::S(format!("g{g}"));
        let on_source = query_index(&engine, &source, "ByGroup", "gpk", value.clone()).await;
        let on_target = query_index(&engine, &restored, "ByGroup", "gpk", value).await;
        assert!(!on_source.is_empty(), "source GSI g{g} is empty");
        assert_eq!(
            on_target, on_source,
            "GSI ByGroup g{g} differs after restore"
        );
        assert!(
            on_target.iter().all(|it| !it.contains_key("extra")),
            "INCLUDE projection must not carry unprojected attributes"
        );
        gsi_total += on_source.len();
    }
    assert_eq!(
        gsi_total, 180,
        "sparse GSI holds the 9 in 10 items with gpk"
    );
    let mut lsi_total = 0;
    for p in 0..7 {
        let value = AttributeValue::S(format!("p{p}"));
        let on_source = query_index(&engine, &source, "ByLocal", "id", value.clone()).await;
        let on_target = query_index(&engine, &restored, "ByLocal", "id", value).await;
        assert!(!on_source.is_empty(), "source LSI p{p} is empty");
        assert_eq!(
            on_target, on_source,
            "LSI ByLocal p{p} differs after restore"
        );
        lsi_total += on_source.len();
    }
    assert_eq!(lsi_total, expected_items);
}

/// (b) Overrides resolved by `restore_input` reach the target as given.
#[tokio::test]
async fn test_restore_applies_billing_overrides() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();

    // PAY_PER_REQUEST source -> PROVISIONED 7/9.
    let on_demand = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "OnDemandSource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &on_demand,
        (0..5).map(|i| item(&format!("a{i}"), None, "v")).collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "OnDemandSource", "od")
        .await
        .expect("backup on-demand source");
    let overrides = RestoreTableOverrides {
        billing_mode: Some(BillingMode::Provisioned),
        provisioned_throughput: Some(ProvisionedThroughput {
            read_capacity_units: 7,
            write_capacity_units: 9,
        }),
        sse_specification: None,
    };
    restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "ToProvisioned",
        &overrides,
    )
    .await
    .expect("restore with PROVISIONED override");
    let target = wait_active(
        &engine,
        &account_id,
        "ToProvisioned",
        Duration::from_secs(60),
    )
    .await;
    assert!(
        target.billing_mode_summary.is_none(),
        "target must be PROVISIONED"
    );
    assert_eq!(target.provisioned_throughput.read_capacity_units, 7);
    assert_eq!(target.provisioned_throughput.write_capacity_units, 9);
    assert_eq!(target.item_count, 5);

    // PROVISIONED 3/4 source -> PAY_PER_REQUEST.
    let provisioned = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "ProvisionedSource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 3,
                write_capacity_units: 4,
            }),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &provisioned,
        (0..3).map(|i| item(&format!("b{i}"), None, "v")).collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "ProvisionedSource", "pv")
        .await
        .expect("backup provisioned source");
    let source = engine
        .backup_restore_source(&account_id, &backup.backup_arn)
        .await
        .expect("restore source");
    assert_eq!(source.billing_mode, "PROVISIONED");
    assert_eq!(
        source.provisioned_throughput,
        Some(ProvisionedThroughput {
            read_capacity_units: 3,
            write_capacity_units: 4
        })
    );
    let overrides = RestoreTableOverrides {
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    };
    restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "ToOnDemand",
        &overrides,
    )
    .await
    .expect("restore with PAY_PER_REQUEST override");
    let target = wait_active(&engine, &account_id, "ToOnDemand", Duration::from_secs(60)).await;
    assert_eq!(
        target.billing_mode_summary.as_ref().map(|b| b.billing_mode),
        Some(BillingMode::PayPerRequest)
    );
    assert_eq!(target.provisioned_throughput.read_capacity_units, 0);
    assert_eq!(target.item_count, 3);

    // Without an override the source's own billing is kept.
    restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "KeepsProvisioned",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore without overrides");
    let target = wait_active(
        &engine,
        &account_id,
        "KeepsProvisioned",
        Duration::from_secs(60),
    )
    .await;
    assert!(target.billing_mode_summary.is_none());
    assert_eq!(target.provisioned_throughput.read_capacity_units, 3);
    assert_eq!(target.provisioned_throughput.write_capacity_units, 4);
}

/// (c) The call returns while the copy is still running, and the copy does
/// not depend on the request future or the engine handle that served it:
/// the engine that took the request is dropped right after it returns, and
/// the target still reaches ACTIVE with every item.
#[tokio::test]
async fn test_restore_returns_creating_and_survives_the_request() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 10_000;
    let observer = setup_engine().await;
    let (account, _source, backup_arn) =
        backed_up_plain_table(&observer, "DetachedSource", ITEMS).await;
    let account_id = account.account_id.clone();

    let request_engine = setup_engine().await;
    let returned = restore(
        &request_engine,
        &account_id,
        &backup_arn,
        "DetachedTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    drop(request_engine);
    assert_eq!(returned.table_status, TableStatus::Creating);

    // The targeted assertion: the copy has not finished when the call
    // returns. An implementation that awaits the copy inside the call
    // returns an already-ACTIVE table here.
    let right_after = describe(&observer, &account_id, "DetachedTarget").await;
    assert_eq!(
        right_after.table_status,
        TableStatus::Creating,
        "the restore call must return before the copy completes"
    );

    let active = wait_active(
        &observer,
        &account_id,
        "DetachedTarget",
        Duration::from_secs(180),
    )
    .await;
    #[allow(clippy::cast_possible_wrap)]
    let expected = ITEMS as i64;
    assert_eq!(active.item_count, expected);
    let key_info = observer
        .table_key_info(&account_id, "DetachedTarget")
        .await
        .expect("target key info");
    assert_eq!(scan_count(&observer, &key_info).await, ITEMS);
}

/// (c) A request dropped mid-flight (a client that timed out) still produces
/// a complete restore: the future is polled once, which starts the restore,
/// and dropped while it is still pending; the target appears and reaches
/// ACTIVE with every item.
#[tokio::test]
async fn test_dropped_restore_request_still_reaches_active() {
    use futures::FutureExt;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 10_000;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) =
        backed_up_plain_table(&engine, "DroppedSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let source = engine
        .backup_restore_source(&account_id, &backup_arn)
        .await
        .expect("restore source");
    source.ensure_restorable().expect("restorable");
    let input = source.restore_input("DroppedTarget", &RestoreTableOverrides::default());
    let request = engine.restore_table_from_backup(
        &account_id,
        RestoreRequest {
            backup_arn: backup_arn.clone(),
            source_table_arn: source.source_table_arn.clone(),
            restore_date_time: source.backup_creation_date_time,
            input,
        },
    );
    assert!(
        request.now_or_never().is_none(),
        "the first poll must not complete the restore request"
    );

    // The dropped request's restore must still create the target ...
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "DroppedTarget".to_owned(),
                },
            )
            .await
        {
            Ok(_) => break,
            Err(StorageError::TableNotFound(_)) => {
                assert!(
                    Instant::now() < deadline,
                    "the dropped request's restore never created its target"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("describe target: {error:?}"),
        }
    }
    // ... and finish it.
    let active = wait_active(
        &engine,
        &account_id,
        "DroppedTarget",
        Duration::from_secs(180),
    )
    .await;
    #[allow(clippy::cast_possible_wrap)]
    let expected = ITEMS as i64;
    assert_eq!(active.item_count, expected);
    let key_info = engine
        .table_key_info(&account_id, "DroppedTarget")
        .await
        .expect("target key info");
    assert_eq!(scan_count(&engine, &key_info).await, ITEMS);
}

/// (d) While the copy runs, DeleteBackup of the source is BackupInUse,
/// DeleteTable of the target is refused, and DescribeTable reports the
/// registration; once ACTIVE both deletes succeed.
#[tokio::test]
async fn test_backup_and_target_are_held_while_restoring() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 10_000;
    let engine = setup_engine().await;
    let (account, source, backup_arn) = backed_up_plain_table(&engine, "HeldSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let source_backup = engine
        .backup_restore_source(&account_id, &backup_arn)
        .await
        .expect("restore source");

    restore(
        &engine,
        &account_id,
        &backup_arn,
        "HeldTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");

    let in_use = engine
        .delete_backup(&account_id, &backup_arn)
        .await
        .expect_err("DeleteBackup during the copy");
    assert!(
        matches!(in_use, StorageError::BackupInUse(ref m) if m.contains("HeldTarget")),
        "got {in_use:?}"
    );
    let refused = engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "HeldTarget".to_owned(),
            },
        )
        .await
        .expect_err("DeleteTable during the copy");
    assert!(
        matches!(refused, StorageError::IndexesInUse(ref m) if m.contains("being restored")),
        "got {refused:?}"
    );
    // Liveness: both refusals happened while the restore was in progress, not
    // after it had finished.
    let during = describe(&engine, &account_id, "HeldTarget").await;
    assert_eq!(during.table_status, TableStatus::Creating);
    let summary = during
        .restore_summary
        .expect("restore summary while CREATING");
    assert_eq!(
        summary.source_backup_arn.as_deref(),
        Some(backup_arn.as_str())
    );
    let source_table_arn = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: source.table_name.clone(),
            },
        )
        .await
        .expect("describe source")
        .table_arn;
    assert_eq!(
        summary.source_table_arn.as_deref(),
        Some(source_table_arn.as_str())
    );
    assert_eq!(
        summary.restore_date_time,
        source_backup.backup_creation_date_time
    );
    assert!(summary.restore_in_progress);

    let active = wait_active(&engine, &account_id, "HeldTarget", Duration::from_secs(180)).await;
    #[allow(clippy::cast_possible_wrap)]
    let expected = ITEMS as i64;
    assert_eq!(active.item_count, expected);
    assert!(
        !active
            .restore_summary
            .expect("still registered")
            .restore_in_progress,
        "the backend reports the registration; the engine hides it once ACTIVE"
    );

    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "HeldTarget".to_owned(),
            },
        )
        .await
        .expect("DeleteTable after ACTIVE");
    engine
        .delete_backup(&account_id, &backup_arn)
        .await
        .expect("DeleteBackup after ACTIVE");
}

/// While a restore's target is CREATING: a second restore of the
/// same backup into another name is `BackupInUse` naming the target and
/// creates nothing, and a restore of another backup, or of the same backup,
/// into the target's name is `TableBeingRestored` naming the target and its
/// source backup. Once the target is ACTIVE a
/// restore of the backup is accepted and a restore into the name is
/// `TableAlreadyExists`. The registration is one lightweight transaction
/// conditioned on `active_restores = null`, so two simultaneous restores of
/// one backup admit exactly one.
#[tokio::test]
async fn test_a_restore_is_refused_while_a_target_of_its_backup_is_creating() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Enough items that the first copy outlasts the refusals below, which
    // each take a schema round trip on this backend.
    const ITEMS: usize = 10_000;
    let engine = setup_engine().await;
    let (account, source, b1) = backed_up_plain_table(&engine, "InUseSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let b2 = engine
        .create_backup(&account_id, &source.table_name, "second")
        .await
        .expect("second backup")
        .backup_arn;

    let overrides = RestoreTableOverrides::default();
    restore(&engine, &account_id, &b1, "InUseTarget", &overrides)
        .await
        .expect("first restore");
    let second = restore(&engine, &account_id, &b1, "InUseTarget2", &overrides).await;
    assert!(
        matches!(second, Err(StorageError::BackupInUse(ref m)) if m.contains("InUseTarget")),
        "a second restore of a backup with a CREATING target: {second:?}"
    );
    let gone = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: "InUseTarget2".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(gone, Err(StorageError::TableNotFound(_))),
        "the refused restore left no target: {gone:?}"
    );
    let into_target = restore(&engine, &account_id, &b2, "InUseTarget", &overrides).await;
    match into_target {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "InUseTarget");
            assert_eq!(backup_arn, b1);
        }
        other => panic!("a restore into a CREATING restore target: {other:?}"),
    }
    // The same backup into the same name: the name is what is refused, not
    // the backup (the service answers TableInUseException).
    let same = restore(&engine, &account_id, &b1, "InUseTarget", &overrides).await;
    match same {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "InUseTarget");
            assert_eq!(backup_arn, b1);
        }
        other => panic!("the same backup into its CREATING target: {other:?}"),
    }
    // The name is checked before the backup: a backup that does not exist,
    // restored into that name, is refused as the name too.
    let source = engine
        .backup_restore_source(&account_id, &b2)
        .await
        .expect("source");
    let missing = engine
        .restore_table_from_backup(
            &account_id,
            RestoreRequest {
                backup_arn: flip_last_hex(&b2),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input: source.restore_input("InUseTarget", &overrides),
            },
        )
        .await;
    match missing {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "InUseTarget");
            assert_eq!(backup_arn, b1);
        }
        other => panic!("a missing backup into a CREATING restore target: {other:?}"),
    }
    // Liveness: the refusals happened while the first restore was CREATING.
    assert_eq!(
        describe(&engine, &account_id, "InUseTarget")
            .await
            .table_status,
        TableStatus::Creating
    );

    wait_active(
        &engine,
        &account_id,
        "InUseTarget",
        Duration::from_secs(180),
    )
    .await;
    restore(&engine, &account_id, &b1, "InUseTarget2", &overrides)
        .await
        .expect("a restore once the first target is ACTIVE");
    let taken = restore(&engine, &account_id, &b2, "InUseTarget", &overrides).await;
    assert!(
        matches!(taken, Err(StorageError::TableAlreadyExists(ref n)) if n == "InUseTarget"),
        "{taken:?}"
    );
    wait_active(
        &engine,
        &account_id,
        "InUseTarget2",
        Duration::from_secs(180),
    )
    .await;

    // Simultaneous, against a held registration: a CREATING restore target
    // of b2 is put in the catalog by hand and registered on b2 the way
    // register_restore does, so the registration is held for as long as the
    // test wants it (a real copy of b2 is over before a second target's
    // schema is created). Four restores of b2 fired at once are all turned
    // away as BackupInUse naming that target, and none creates a target.
    // Once the registration is released, a restore of b2 is admitted.
    let held_id = uuid::Uuid::new_v4().to_string();
    let catalog = engine.catalog_keyspace();
    engine
        .session()
        .query_with_values(
            &format!(
                "INSERT INTO {catalog}.tables \
                 (account_id, table_name, table_id, table_status, restore_source_backup_arn) \
                 VALUES (?, ?, ?, 'CREATING', ?)"
            ),
            cdrs_tokio::query_values!(
                account_id.as_str(),
                "HeldTarget",
                held_id.as_str(),
                b2.as_str()
            ),
        )
        .await
        .expect("held target row");
    let registered = extenddb_storage_cassandra::cassandra_util::query_lwt(
        engine.session(),
        &format!(
            "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? \
             IF backup_status = 'AVAILABLE' AND active_restores = null"
        ),
        cdrs_tokio::query_values!(
            held_id.as_str(),
            "HeldTarget",
            account_id.as_str(),
            b2.as_str()
        ),
    )
    .await
    .expect("register the held restore");
    assert!(
        extenddb_storage_cassandra::cassandra_util::lwt_applied(&registered).unwrap(),
        "b2 had no registration before the held one"
    );
    let results = futures::future::join_all((0..4).map(|i| {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let b2 = b2.clone();
        async move {
            restore(
                &engine,
                &account_id,
                &b2,
                &format!("SimTarget{i}"),
                &RestoreTableOverrides::default(),
            )
            .await
        }
    }))
    .await;
    for (i, result) in results.iter().enumerate() {
        assert!(
            matches!(result, Err(StorageError::BackupInUse(m)) if m.contains("HeldTarget")),
            "restore {i} against the held registration: {result:?}"
        );
        let gone = engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: format!("SimTarget{i}"),
                },
            )
            .await;
        assert!(
            matches!(gone, Err(StorageError::TableNotFound(_))),
            "refused restore {i} left a target: {gone:?}"
        );
    }
    engine
        .session()
        .query_with_values(
            &format!("DELETE FROM {catalog}.tables WHERE account_id = ? AND table_name = ?"),
            cdrs_tokio::query_values!(account_id.as_str(), "HeldTarget"),
        )
        .await
        .expect("remove the held target row");
    // The registration is now stale (its target is gone); the next restore's
    // backup check clears it and the restore is admitted.
    restore(&engine, &account_id, &b2, "SimTarget0", &overrides)
        .await
        .expect("a restore once the held registration is released");
    wait_active(&engine, &account_id, "SimTarget0", Duration::from_secs(180)).await;
}

/// (e) A backup row with no definition (written before V005) restores as a
/// legacy backup: keys and billing only, no secondary indexes.
#[tokio::test]
async fn test_backup_without_definition_restores_as_legacy() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "LegacySource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![
                attr("id", ScalarAttributeType::S),
                attr("gpk", ScalarAttributeType::S),
            ],
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: Some(vec![GsiInput {
                index_name: "LegacyGsi".to_owned(),
                key_schema: vec![ks("gpk", KeyType::Hash)],
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }]),
            ..Default::default()
        },
    )
    .await;
    make_gsis_synchronous(&engine, &source.table_id, &["LegacyGsi"]).await;
    put_all(
        &engine,
        &source,
        (0..70)
            .map(|i| {
                let mut it = item(&format!("l{i}"), None, "v");
                it.insert("gpk".to_owned(), AttributeValue::S(format!("g{}", i % 3)));
                it
            })
            .collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "LegacySource", "legacy")
        .await
        .expect("create backup");

    // What a row written by the build before V005 looks like: no definition.
    let strip = format!(
        "UPDATE {}.backups_by_arn SET definition = null WHERE account_id = ? AND backup_arn = ? IF EXISTS",
        engine.catalog_keyspace()
    );
    engine
        .session()
        .query_with_values(
            &strip,
            cdrs_tokio::query_values!(account_id.as_str(), backup.backup_arn.as_str()),
        )
        .await
        .expect("strip definition");

    let legacy = engine
        .backup_restore_source(&account_id, &backup.backup_arn)
        .await
        .expect("legacy restore source");
    assert!(legacy.definition.is_none());
    assert_eq!(legacy.billing_mode, "PAY_PER_REQUEST");
    assert_eq!(legacy.legacy_vector_index_count, 0);

    restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "LegacyTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore legacy backup");
    let active = wait_active(
        &engine,
        &account_id,
        "LegacyTarget",
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(active.item_count, 70);
    assert!(
        active.global_secondary_indexes.is_none(),
        "legacy restores keys only"
    );
    assert!(active.local_secondary_indexes.is_none());
    assert_eq!(
        active.billing_mode_summary.as_ref().map(|b| b.billing_mode),
        Some(BillingMode::PayPerRequest)
    );
}

/// A restore whose copy stopped heartbeating is removed by the sweep, and
/// the backup it held becomes deletable. Ownership is taken away from the
/// running copy (as if its process had died and another had claimed it) by
/// rewriting the owner and back-dating the heartbeat.
#[tokio::test]
async fn test_sweep_removes_abandoned_restore_target() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 10_000;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) =
        backed_up_plain_table(&engine, "AbandonedSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "AbandonedTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");

    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    let orphan = format!(
        "UPDATE {}.tables SET restore_owner = ?, restore_heartbeat = ? \
         WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'",
        engine.catalog_keyspace()
    );
    let result = extenddb_storage_cassandra::cassandra_util::query_lwt(
        engine.session(),
        &orphan,
        cdrs_tokio::query_values!(
            uuid::Uuid::new_v4(),
            stale,
            account_id.as_str(),
            "AbandonedTarget",
            target.table_id.as_str()
        ),
    )
    .await
    .expect("orphan the target");
    assert!(
        extenddb_storage_cassandra::cassandra_util::lwt_applied(&result).unwrap(),
        "the copy must still be running when the target is orphaned"
    );

    // A fresh heartbeat is not swept.
    let not_yet = engine
        .sweep_abandoned_restores(Duration::from_secs(600))
        .await
        .expect("sweep");
    assert!(!not_yet.contains(&"AbandonedTarget".to_owned()));

    let removed = engine
        .sweep_abandoned_restores(Duration::from_secs(60))
        .await
        .expect("sweep");
    assert!(
        removed.contains(&"AbandonedTarget".to_owned()),
        "removed {removed:?}"
    );
    let gone = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: "AbandonedTarget".to_owned(),
            },
        )
        .await
        .expect_err("swept target is gone");
    assert!(matches!(gone, StorageError::TableNotFound(_)));
    // The orphaned copy notices it lost the target and does not resurrect it.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "AbandonedTarget".to_owned(),
                },
            )
            .await
            .is_err()
    );
    engine
        .delete_backup(&account_id, &backup_arn)
        .await
        .expect("the abandoned restore no longer holds the backup");
}

/// Restore of a backup that is not AVAILABLE is refused before any target is
/// created, and a missing backup leaves no table behind.
#[tokio::test]
async fn test_restore_of_deleted_backup_is_not_found() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "GoneSource", 3).await;
    let source = engine
        .backup_restore_source(&account.account_id, &backup_arn)
        .await
        .expect("source");
    let input = source.restore_input("GoneTarget", &RestoreTableOverrides::default());
    engine
        .delete_backup(&account.account_id, &backup_arn)
        .await
        .expect("delete backup");
    let error = engine
        .restore_table_from_backup(
            &account.account_id,
            RestoreRequest {
                backup_arn: backup_arn.clone(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input,
            },
        )
        .await
        .expect_err("restore of a deleted backup");
    assert!(matches!(error, StorageError::Validation(ref m) if m.contains("Backup not found")));
    assert!(
        engine
            .describe_table(
                &account.account_id,
                DescribeTableInput {
                    table_name: "GoneTarget".to_owned(),
                },
            )
            .await
            .is_err()
    );
}

// ── Isolated catalogs (own keyspace prefix) ────────────────────────────────

/// Catalog migrations V001-V004: the catalog shape before V005 added
/// `backups_by_arn.definition/active_restores` and `tables.restore_*`.
const CATALOG_THROUGH_V004: &[(&str, &str)] = &[
    (
        "V001__initial_schema.cql",
        include_str!("../../migrations/catalog/V001__initial_schema.cql"),
    ),
    (
        "V002__backup_restore.cql",
        include_str!("../../migrations/catalog/V002__backup_restore.cql"),
    ),
    (
        "V003__account_scoped_idempotency.cql",
        include_str!("../../migrations/catalog/V003__account_scoped_idempotency.cql"),
    ),
    (
        "V004__ttl_backfill_cursor.cql",
        include_str!("../../migrations/catalog/V004__ttl_backfill_cursor.cql"),
    ),
];

/// An engine on its own keyspace prefix (so its sweeps and workers touch no
/// other test's tables) with one provisioned account. With `through_v004`
/// the catalog has run only catalog migrations V001-V004, as a catalog not
/// yet migrated by `extenddb migrate --yes`; otherwise every migration.
async fn isolated_engine(
    tag: &str,
    through_v004: bool,
) -> (
    std::sync::Arc<CassandraEngine>,
    String,
    crate::helpers::DropKeyspacesOnDrop,
) {
    let mut config = crate::helpers::test_config();
    config.keyspace_prefix = format!("f4{tag}_{}", &crate::helpers::unique_test_id()[..10]);
    config.cached_connection_string = None;
    config.ensure_cached_connection_string();
    let engine = std::sync::Arc::new(CassandraEngine::new(&config, "us-east-1").await.unwrap());
    let session = engine.session_arc();
    let account_id = unique_test_account();
    let catalog_ks = engine.catalog_keyspace();
    let account_ks = engine.account_keyspace(&account_id);
    let drop_keyspaces =
        crate::helpers::DropKeyspacesOnDrop(vec![catalog_ks.clone(), account_ks.clone()]);
    for ks in [&catalog_ks, &account_ks] {
        engine.create_keyspace(ks).await.unwrap();
        crate::helpers::ensure_keyspace_rf(&engine, ks)
            .await
            .unwrap();
    }
    if through_v004 {
        // Statement by statement, each qualified with the catalog keyspace by
        // the function the migration runner uses, recording versions 1-4 in
        // schema_history as the runner did for a 0.0.3 catalog.
        for (filename, sql) in CATALOG_THROUGH_V004 {
            let statements = extenddb_storage_cassandra::migrations::migration_statements(
                &catalog_ks,
                filename,
                sql,
            )
            .expect(filename);
            for statement in statements {
                session.query(statement).await.expect("V001-V004 statement");
            }
        }
        for version in 1..=4_i32 {
            session
                .query_with_values(
                    format!(
                        "INSERT INTO {catalog_ks}.schema_history (version, description, applied_at) \
                         VALUES (?, 'through_v004', toTimestamp(now()))"
                    ),
                    cdrs_tokio::query_values!(version),
                )
                .await
                .expect("record V001-V004");
        }
    } else {
        extenddb_storage_cassandra::migrations::run_catalog_migrations(&session, &catalog_ks)
            .await
            .unwrap();
    }
    extenddb_storage_cassandra::migrations::run_data_migrations(&session, &account_ks)
        .await
        .unwrap();
    session
        .query_with_values(
            format!(
                "INSERT INTO {catalog_ks}.accounts (account_id, account_name, created_at) \
                 VALUES (?, ?, toTimestamp(now()))"
            ),
            cdrs_tokio::query_values!(account_id.as_str(), "F4 Test"),
        )
        .await
        .unwrap();
    (engine, account_id, drop_keyspaces)
}

/// The columns catalog migration V005 adds are absent from `keyspace`.
async fn assert_catalog_lacks_v005(engine: &CassandraEngine, keyspace: &str) {
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        "columns",
    )
    .await
    .expect("read schema");
    let columns: Vec<String> = rows
        .iter()
        .map(|row| {
            extenddb_storage_cassandra::cassandra_util::get_column::<String, StorageError>(
                row,
                "column_name",
                "columns",
            )
            .unwrap()
        })
        .collect();
    assert!(columns.iter().any(|c| c == "table_status"), "{columns:?}");
    for v005 in [
        "restore_source_backup_arn",
        "restore_heartbeat",
        "active_restores",
        "definition",
    ] {
        assert!(!columns.iter().any(|c| c == v005), "{v005} present");
    }
}

/// On a catalog that has not run V005, ordinary table and item operations
/// work; only backup and restore need V005. (CreateTable used to name the
/// V005 restore columns in its INSERT for every table, which CQL rejects.)
#[tokio::test]
async fn test_table_operations_work_on_a_catalog_without_v005() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("v4", true).await;
    assert_catalog_lacks_v005(&engine, &engine.catalog_keyspace()).await;

    let created = engine
        .create_table(
            &account_id,
            CreateTableInput {
                table_name: "Unmigrated".to_owned(),
                key_schema: vec![ks("id", KeyType::Hash)],
                attribute_definitions: vec![
                    attr("id", ScalarAttributeType::S),
                    attr("g", ScalarAttributeType::S),
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "ByG".to_owned(),
                    key_schema: vec![ks("g", KeyType::Hash)],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                }]),
                ..Default::default()
            },
        )
        .await
        .expect("CreateTable on a catalog without V005");
    assert!(created.restore_summary.is_none());
    activate_tables(&engine).await;
    let described = describe(&engine, &account_id, "Unmigrated").await;
    assert_eq!(described.table_status, TableStatus::Active);
    assert!(described.restore_summary.is_none());

    let key_info = engine
        .table_key_info(&account_id, "Unmigrated")
        .await
        .expect("key info");
    engine
        .put_item(
            &key_info,
            item("a", None, "v"),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("PutItem");
    let got = engine
        .get_item(&key_info, &key("a", None))
        .await
        .expect("GetItem");
    assert!(got.is_some());
    assert_eq!(scan_count(&engine, &key_info).await, 1);

    // Backup needs V005. CreateBackup, DeleteBackup and RestoreTableFromBackup
    // are refused with a message naming the file and `migrate` (they failed
    // with an internal `Undefined column name ...`); nothing is created.
    let assert_refused = |what: &str, result: Result<String, StorageError>| match result {
        Err(StorageError::Unsupported(m)) => {
            assert!(
                m.contains("V005__backup_definitions_and_restore_registration.cql")
                    && m.contains("extenddb migrate --yes")
                    && !m.contains("V004"),
                "{what}: {m}"
            );
        }
        other => panic!("{what}: {other:?}"),
    };
    assert_refused(
        "CreateBackup",
        engine
            .create_backup(&account_id, "Unmigrated", "needs-v005")
            .await
            .map(|d| d.backup_arn),
    );
    let some_arn = extenddb_storage::util::backup_arn(
        "us-east-1",
        &account_id,
        "Unmigrated",
        &extenddb_storage::util::new_backup_id(),
    );
    assert_refused(
        "DeleteBackup",
        engine
            .delete_backup(&account_id, &some_arn)
            .await
            .map(|d| d.backup_details.backup_arn),
    );
    assert_refused(
        "RestoreTableFromBackup",
        engine
            .restore_table_from_backup(
                &account_id,
                RestoreRequest {
                    backup_arn: some_arn.clone(),
                    source_table_arn: described.table_arn.clone(),
                    restore_date_time: 0.0,
                    input: CreateTableInput {
                        table_name: "UnmigratedTarget".to_owned(),
                        key_schema: vec![ks("id", KeyType::Hash)],
                        attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
                        billing_mode: Some(BillingMode::PayPerRequest),
                        ..Default::default()
                    },
                },
            )
            .await
            .map(|d| d.table_name),
    );
    assert!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "UnmigratedTarget".to_owned()
                }
            )
            .await
            .is_err(),
        "a refused restore creates no target"
    );

    // Liveness: once V005 is applied (as `migrate --yes` does, the runner
    // skipping the recorded files), the same engine backs up without a
    // restart.
    extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        &engine.catalog_keyspace(),
    )
    .await
    .expect("apply V005");
    let backup = engine
        .create_backup(&account_id, "Unmigrated", "after-v005")
        .await
        .expect("CreateBackup after V005");
    engine
        .delete_backup(&account_id, &backup.backup_arn)
        .await
        .expect("DeleteBackup after V005");

    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "Unmigrated".to_owned(),
            },
        )
        .await
        .expect("DeleteTable on a catalog without V005");
}

/// Back-date the heartbeat of the CREATING restore target `name` by two
/// minutes and give it a foreign owner, as if the process copying into it had
/// died; asserts the copy was still running (the LWT applied).
async fn orphan_restore_target(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
    table_id: &str,
) {
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    let orphan = format!(
        "UPDATE {}.tables SET restore_owner = ?, restore_heartbeat = ? \
         WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'",
        engine.catalog_keyspace()
    );
    let result = extenddb_storage_cassandra::cassandra_util::query_lwt(
        engine.session(),
        &orphan,
        cdrs_tokio::query_values!(uuid::Uuid::new_v4(), stale, account_id, name, table_id),
    )
    .await
    .expect("orphan the target");
    assert!(
        extenddb_storage_cassandra::cassandra_util::lwt_applied(&result).unwrap(),
        "the copy must still be running when the target is orphaned"
    );
}

/// The server's sweep worker sweeps once as soon as it starts (not one
/// interval later) and returns promptly once shutdown is signalled.
#[tokio::test]
async fn test_restore_sweep_worker_sweeps_at_startup_and_stops_on_shutdown() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("sw", false).await;
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "SweepSource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &source,
        (0..1_500)
            .map(|i| item(&format!("item-{i:05}"), None, &"x".repeat(200)))
            .collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "SweepSource", "sweep")
        .await
        .expect("create backup");
    let target = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "SweepTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    orphan_restore_target(&engine, &account_id, "SweepTarget", &target.table_id).await;

    let shutdown = extenddb_storage::CancellationToken::new();
    let started = Instant::now();
    let worker = tokio::spawn(extenddb_storage_cassandra::poll_abandoned_restores(
        std::sync::Arc::clone(&engine),
        shutdown.clone(),
    ));
    // Well inside RESTORE_SWEEP_INTERVAL (15 s): only a sweep at startup
    // removes the target this soon.
    let deadline = started + Duration::from_secs(8);
    loop {
        let described = engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "SweepTarget".to_owned(),
                },
            )
            .await;
        if matches!(described, Err(StorageError::TableNotFound(_))) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the abandoned target was not swept within 8 s of the worker starting: {:?}",
            described.map(|d| d.table_status)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .expect("the sweep worker did not stop within 3 s of shutdown")
        .expect("sweep worker panicked");
}

/// The restore heartbeat runs while the target's data and index tables are
/// created, not only once the copy starts: with many indexes that DDL (each
/// statement waiting for schema agreement) can outlast RESTORE_STALE_AFTER on
/// a slow cluster, and a heartbeat still at its insert value would let a
/// sweep remove the live target.
#[tokio::test]
async fn test_restore_heartbeat_runs_during_target_ddl() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("hb", false).await;
    let mut attribute_definitions = vec![
        attr("id", ScalarAttributeType::S),
        attr("sort", ScalarAttributeType::S),
    ];
    let mut gsis = Vec::new();
    let mut lsis = Vec::new();
    for i in 0..5 {
        attribute_definitions.push(attr(&format!("g{i}"), ScalarAttributeType::S));
        attribute_definitions.push(attr(&format!("l{i}"), ScalarAttributeType::S));
        gsis.push(GsiInput {
            index_name: format!("G{i}"),
            key_schema: vec![ks(&format!("g{i}"), KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
        lsis.push(LsiInput {
            index_name: format!("L{i}"),
            key_schema: vec![
                ks("id", KeyType::Hash),
                ks(&format!("l{i}"), KeyType::Range),
            ],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        });
    }
    create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "ManyIndexes".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash), ks("sort", KeyType::Range)],
            attribute_definitions,
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: Some(gsis),
            local_secondary_indexes: Some(lsis),
            ..Default::default()
        },
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "ManyIndexes", "ddl")
        .await
        .expect("create backup");

    // Watch the target row while the restore request runs: when it first
    // appears, and when its heartbeat first moves past its insert value.
    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        let watching = std::sync::Arc::clone(&watching);
        tokio::spawn(async move {
            let query = format!(
                "SELECT created_at, restore_heartbeat FROM {}.tables \
                 WHERE account_id = ? AND table_name = ?",
                engine.catalog_keyspace()
            );
            let (mut row_seen, mut refreshed) = (None, None);
            while watching.load(std::sync::atomic::Ordering::Acquire) && refreshed.is_none() {
                let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
                    &engine.session_arc(),
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), "ManyIndexesRestored"),
                    "watch",
                )
                .await
                .expect("watch target");
                if let Some(row) = rows.first() {
                    use cdrs_tokio::types::IntoRustByName;
                    let observed = Instant::now();
                    row_seen.get_or_insert(observed);
                    let created: Option<i64> = row.get_by_name("created_at").ok().flatten();
                    let heartbeat: Option<i64> =
                        row.get_by_name("restore_heartbeat").ok().flatten();
                    if heartbeat > created {
                        refreshed = Some(observed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (row_seen, refreshed)
        })
    };
    let started = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "ManyIndexesRestored",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    let returned = Instant::now();
    watching.store(false, std::sync::atomic::Ordering::Release);
    let (row_seen, refreshed) = watcher.await.expect("watcher");
    assert_eq!(started.table_status, TableStatus::Creating);
    let row_seen = row_seen.expect("the target row was never observed before the restore returned");
    // Precondition: the DDL after the insert took long enough to tell a
    // heartbeat started at the insert from one started after registration.
    let ddl = returned - row_seen;
    assert!(
        ddl >= Duration::from_millis(800),
        "target DDL took only {ddl:?}; too short for this test to discriminate"
    );
    let refreshed = refreshed.expect("the heartbeat was not refreshed before the restore returned");
    eprintln!(
        "target DDL {ddl:?}; heartbeat first refreshed {:?} before the restore returned",
        returned - refreshed
    );
    assert!(
        returned - refreshed >= Duration::from_millis(500),
        "the heartbeat was first refreshed only {:?} before the restore returned (DDL {ddl:?})",
        returned - refreshed
    );
    wait_active(
        &engine,
        &account_id,
        "ManyIndexesRestored",
        Duration::from_secs(60),
    )
    .await;
}
