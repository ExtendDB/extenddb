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

use extenddb_core::backup_format::BackupArn;
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiInput, KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType,
    ProvisionedThroughput, ScalarAttributeType, TableDescription, TableKeyInfo, TableStatus,
    TimeToLiveStatus,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, RestoreTargetKey, StatusFields,
    StoreIdentity, StoreKind, TimeBounds, UNOWNED_GENERATION, WriteOutcome,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, DataEngine, JobPermit, LegacyBackupReader, RestoreRequest,
    RestoreTableOverrides, TableEngine,
};
use extenddb_storage_cassandra::CassandraEngine;
use futures::{StreamExt, TryStreamExt};

use crate::helpers::{TestAccount, TestTable, setup_engine, unique_test_account};

type Item = BTreeMap<String, AttributeValue>;

fn item(id: &str, sort: Option<&str>, value: &str) -> Item {
    let mut item = BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        item.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    item.insert("value".to_owned(), AttributeValue::S(value.to_owned()));
    item
}

fn key(id: &str, sort: Option<&str>) -> Item {
    let mut key = BTreeMap::new();
    key.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    if let Some(sort) = sort {
        key.insert("sort".to_owned(), AttributeValue::S(sort.to_owned()));
    }
    key
}

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn attr(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    }
}

async fn activate_tables(engine: &CassandraEngine) {
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

/// Create `input` in `account_id`, activate it, and return its key info.
async fn create_active_table(
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
async fn make_gsis_synchronous(engine: &CassandraEngine, table_id: &str, names: &[&str]) {
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

async fn put_all(engine: &CassandraEngine, key_info: &TableKeyInfo, items: Vec<Item>) {
    let mut writes = futures::stream::iter(items)
        .map(|item| engine.put_item(key_info, item, false, None, &Default::default(), None))
        .buffer_unordered(32);
    while let Some(result) = writes.next().await {
        result.expect("seed item");
    }
}

async fn describe(engine: &CassandraEngine, account_id: &str, name: &str) -> TableDescription {
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
async fn restore(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    target: &str,
    overrides: &RestoreTableOverrides,
) -> Result<TableDescription, StorageError> {
    let source = engine.restore_source(account_id, backup_arn).await?;
    source.ensure_restorable()?;
    let input = source.restore_input(target, overrides);
    engine
        .restore(
            account_id,
            RestoreRequest {
                backup_arn: backup_arn.to_owned(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input,
            },
            JobPermit::detached(),
        )
        .await
}

/// Poll until `name` is ACTIVE; panic after `timeout`.
async fn wait_active(
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

async fn scan_count(engine: &CassandraEngine, key_info: &TableKeyInfo) -> usize {
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
async fn query_index(
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
        .create(
            &account.account_id,
            name,
            "copy-timing",
            JobPermit::detached(),
        )
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
        .create(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "daily",
            JobPermit::detached(),
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
        .describe(&table.key_info.account_id, &details.backup_arn)
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
        .list(&table.key_info.account_id, Some(&table.key_info.table_name))
        .await
        .expect("list table backups");
    assert_eq!(table_backups.len(), 1);
    assert_eq!(table_backups[0].backup_arn, details.backup_arn);
    assert_eq!(table_backups[0].table_id, table.key_info.table_id);
    let account_backups = engine
        .list(&table.key_info.account_id, None)
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
        .describe(&foreign, &details.backup_arn)
        .await
        .expect_err("another account must not resolve the backup");
    assert!(
        matches!(foreign_error, StorageError::Validation(message) if message.contains("Backup not found"))
    );
    let foreign_source = engine
        .restore_source(&foreign, &details.backup_arn)
        .await
        .expect_err("another account must not resolve the restore source");
    assert!(
        matches!(foreign_source, StorageError::Validation(message) if message.contains("Backup not found"))
    );

    let deleted = engine
        .delete(&table.key_info.account_id, &details.backup_arn)
        .await
        .expect("delete backup");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(
        engine
            .describe(&table.key_info.account_id, &details.backup_arn)
            .await
            .is_err()
    );
    assert!(
        engine
            .list(&table.key_info.account_id, None)
            .await
            .expect("list after delete")
            .is_empty()
    );
    let gone = engine
        .restore_source(&table.key_info.account_id, &details.backup_arn)
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
        .create(
            &account_id,
            &table.key_info.table_name,
            "immutable",
            JobPermit::detached(),
        )
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
        .delete(&account_id, &backup.backup_arn)
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
        .create(
            &account.account_id,
            table_name,
            "before-recreate",
            JobPermit::detached(),
        )
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
        .list(&account.account_id, Some(table_name))
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
        .delete(&account.account_id, &backup.backup_arn)
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
        .create(
            &account_id,
            "IndexedSource",
            "indexed",
            JobPermit::detached(),
        )
        .await
        .expect("create backup");
    let recorded = engine
        .restore_source(&account_id, &backup.backup_arn)
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
        .create(&account_id, "OnDemandSource", "od", JobPermit::detached())
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
        .create(
            &account_id,
            "ProvisionedSource",
            "pv",
            JobPermit::detached(),
        )
        .await
        .expect("backup provisioned source");
    let source = engine
        .restore_source(&account_id, &backup.backup_arn)
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
    const ITEMS: usize = 1_500;
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
    const ITEMS: usize = 1_500;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) =
        backed_up_plain_table(&engine, "DroppedSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let source = engine
        .restore_source(&account_id, &backup_arn)
        .await
        .expect("restore source");
    source.ensure_restorable().expect("restorable");
    let input = source.restore_input("DroppedTarget", &RestoreTableOverrides::default());
    let request = engine.restore(
        &account_id,
        RestoreRequest {
            backup_arn: backup_arn.clone(),
            source_table_arn: source.source_table_arn.clone(),
            restore_date_time: source.backup_creation_date_time,
            input,
        },
        JobPermit::detached(),
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
    const ITEMS: usize = 1_500;
    let engine = setup_engine().await;
    let (account, source, backup_arn) = backed_up_plain_table(&engine, "HeldSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let source_backup = engine
        .restore_source(&account_id, &backup_arn)
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
        .delete(&account_id, &backup_arn)
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
        .delete(&account_id, &backup_arn)
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
        .create(
            &account_id,
            &source.table_name,
            "second",
            JobPermit::detached(),
        )
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
        .restore_source(&account_id, &b2)
        .await
        .expect("source");
    let missing = engine
        .restore(
            &account_id,
            RestoreRequest {
                backup_arn: flip_last_hex(&b2),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input: source.restore_input("InUseTarget", &overrides),
            },
            JobPermit::detached(),
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

    // Simultaneous: four restores of b2 started together, exactly one admitted.
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
    let accepted: Vec<usize> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.is_ok().then_some(i))
        .collect();
    assert_eq!(accepted.len(), 1, "exactly one is accepted: {results:?}");
    for (i, result) in results.iter().enumerate() {
        if i != accepted[0] {
            assert!(
                matches!(result, Err(StorageError::BackupInUse(_))),
                "restore {i}: {result:?}"
            );
        }
    }
    wait_active(
        &engine,
        &account_id,
        &format!("SimTarget{}", accepted[0]),
        Duration::from_secs(180),
    )
    .await;
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
        .create(&account_id, "LegacySource", "legacy", JobPermit::detached())
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
        .restore_source(&account_id, &backup.backup_arn)
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

/// A restore whose owner died is found by `list_dead_restore_jobs` only once
/// its heartbeat is older than the stale window, and is then failed and
/// removed through `fail_restore_job` and `remove_restore_target`, after
/// which the backup it held becomes deletable. Ownership is taken away from
/// the running copy (as if its process had died and another had claimed the
/// target) by rewriting the generation and back-dating the heartbeat; the
/// copy notices and does not resurrect the target.
#[tokio::test]
async fn test_abandoned_restore_is_removed_only_when_its_heartbeat_is_stale() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 1_500;
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
    orphan_restore_target(&engine, &account_id, "AbandonedTarget", &target.table_id).await;
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "AbandonedTarget".to_owned(),
        table_id: target.table_id.clone(),
    };

    // A heartbeat two minutes old is not stale under a ten-minute window.
    let not_yet = engine
        .list_dead_restore_jobs(Duration::from_secs(600))
        .await
        .expect("list");
    assert!(
        !not_yet.iter().any(|job| job.target == key),
        "a heartbeat younger than the window is not dead: {not_yet:?}"
    );
    let dead = engine
        .list_dead_restore_jobs(Duration::from_secs(60))
        .await
        .expect("list");
    let job = dead
        .iter()
        .find(|job| job.target == key)
        .unwrap_or_else(|| panic!("the orphaned target is dead under a 60 s window: {dead:?}"));
    assert_eq!(job.source_backup_arn, backup_arn);
    assert_eq!(job.status, JobStatus::Running);
    assert!(
        engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail"),
        "the record was RUNNING"
    );
    assert!(
        !engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail again"),
        "a FAILED record is not RUNNING"
    );
    assert!(
        engine.remove_restore_target(&key).await.expect("remove"),
        "the target was a CREATING restore target"
    );
    assert!(
        !engine
            .remove_restore_target(&key)
            .await
            .expect("remove again"),
        "a removed target is not one"
    );
    let gone = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: "AbandonedTarget".to_owned(),
            },
        )
        .await
        .expect_err("removed target is gone");
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
        .delete(&account_id, &backup_arn)
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
        .restore_source(&account.account_id, &backup_arn)
        .await
        .expect("source");
    let input = source.restore_input("GoneTarget", &RestoreTableOverrides::default());
    engine
        .delete(&account.account_id, &backup_arn)
        .await
        .expect("delete backup");
    let error = engine
        .restore(
            &account.account_id,
            RestoreRequest {
                backup_arn: backup_arn.clone(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input,
            },
            JobPermit::detached(),
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

/// Catalog migrations V001-V005: the catalog shape before V006 added the
/// backup catalog index (`backups_by_arn.store_*`, `backup_index_by_status`,
/// the `tables.restore_*` job record, `backup_maintenance`, `catalog_id`).
const CATALOG_THROUGH_V005: &[(&str, &str)] = &[
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
    (
        "V005__backup_definitions_and_restore_registration.cql",
        include_str!(
            "../../migrations/catalog/V005__backup_definitions_and_restore_registration.cql"
        ),
    ),
];

/// An engine on its own keyspace prefix (so its catalog scans see no other
/// test's tables) with one provisioned account. With `through_v005` the
/// catalog has run only catalog migrations V001-V005, as a catalog not yet
/// migrated by `extenddb migrate --yes`; otherwise every migration.
async fn isolated_engine(
    tag: &str,
    through_v005: bool,
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
    if through_v005 {
        // Statement by statement, each qualified with the catalog keyspace by
        // the function the migration runner uses, recording versions 1-5 in
        // schema_history as the runner did for a 0.0.4 catalog.
        for (filename, sql) in CATALOG_THROUGH_V005 {
            let statements = extenddb_storage_cassandra::migrations::migration_statements(
                &catalog_ks,
                filename,
                sql,
            )
            .expect(filename);
            for statement in statements {
                session.query(statement).await.expect("V001-V005 statement");
            }
        }
        for version in 1..=5_i32 {
            session
                .query_with_values(
                    format!(
                        "INSERT INTO {catalog_ks}.schema_history (version, description, applied_at) \
                         VALUES (?, 'through_v005', toTimestamp(now()))"
                    ),
                    cdrs_tokio::query_values!(version),
                )
                .await
                .expect("record V001-V005");
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

/// The columns and tables catalog migration V006 adds are absent from
/// `keyspace`, and V005's are present.
async fn assert_catalog_lacks_v006(engine: &CassandraEngine, keyspace: &str) {
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
    for v005 in ["restore_source_backup_arn", "active_restores", "definition"] {
        assert!(columns.iter().any(|c| c == v005), "{v005} absent");
    }
    for v006 in [
        "restore_status",
        "restore_owner_generation",
        "restore_heartbeat_at",
        "store_kind",
        "source_table_details_json",
    ] {
        assert!(!columns.iter().any(|c| c == v006), "{v006} present");
    }
    let tables = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        "tables",
    )
    .await
    .expect("read schema");
    let names: Vec<String> = tables
        .iter()
        .map(|row| {
            extenddb_storage_cassandra::cassandra_util::get_column::<String, StorageError>(
                row,
                "table_name",
                "tables",
            )
            .unwrap()
        })
        .collect();
    for v006 in ["backup_index_by_status", "backup_maintenance"] {
        assert!(!names.iter().any(|n| n == v006), "{v006} present");
    }
    assert!(
        engine.catalog_id().await.is_err(),
        "a catalog without V006 has no catalog id"
    );
    // `serve` does not start on it: the factory's guard refuses the 0.0.4
    // catalog with the command that brings it forward.
    let refusal = engine
        .require_served_catalog()
        .await
        .expect_err("serve refuses a catalog behind the binary")
        .to_string();
    assert!(
        refusal.contains("at version 0.0.4")
            && refusal.contains(extenddb_storage_cassandra::CATALOG_VERSION)
            && refusal.contains("extenddb migrate --yes"),
        "{refusal}"
    );
}

/// On a catalog that has not run V006, ordinary table and item operations
/// work; only backup and restore need V006. (CreateTable used to name the
/// restore columns in its INSERT for every table, which CQL rejects.)
#[tokio::test]
async fn test_table_operations_work_on_a_catalog_without_v006() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("v5", true).await;
    assert_catalog_lacks_v006(&engine, &engine.catalog_keyspace()).await;

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

    // Backup needs V006. CreateBackup, DeleteBackup and RestoreTableFromBackup
    // are refused with a message naming the file and `migrate` (they failed
    // with an internal `Undefined column name ...`); nothing is created.
    let assert_refused = |what: &str, result: Result<String, StorageError>| match result {
        Err(StorageError::Unsupported(m)) => {
            assert!(
                m.contains("V006__backup_catalog_index.cql")
                    && m.contains("extenddb migrate --yes")
                    && !m.contains("V005"),
                "{what}: {m}"
            );
        }
        other => panic!("{what}: {other:?}"),
    };
    assert_refused(
        "CreateBackup",
        engine
            .create(
                &account_id,
                "Unmigrated",
                "needs-v006",
                JobPermit::detached(),
            )
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
            .delete(&account_id, &some_arn)
            .await
            .map(|d| d.backup_details.backup_arn),
    );
    assert_refused(
        "RestoreTableFromBackup",
        engine
            .restore(
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
                JobPermit::detached(),
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

    // Liveness: once V006 is applied (as `migrate --yes` does, the runner
    // skipping the recorded files), the same engine backs up without a
    // restart, and the catalog has its id.
    extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        &engine.catalog_keyspace(),
    )
    .await
    .expect("apply V006");
    assert!(
        uuid::Uuid::parse_str(&engine.catalog_id().await.expect("catalog id")).is_ok(),
        "the migration writes a catalog id"
    );
    engine
        .require_served_catalog()
        .await
        .expect("serve accepts the migrated catalog");
    let backup = engine
        .create(
            &account_id,
            "Unmigrated",
            "after-v006",
            JobPermit::detached(),
        )
        .await
        .expect("CreateBackup after V006");
    engine
        .delete(&account_id, &backup.backup_arn)
        .await
        .expect("DeleteBackup after V006");

    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "Unmigrated".to_owned(),
            },
        )
        .await
        .expect("DeleteTable on a catalog without V006");
}

/// Back-date the heartbeat of the CREATING restore target `name` by two
/// minutes and give its job record another generation, as if the process
/// copying into it had died and another had claimed it; asserts the copy was
/// still running (the LWT applied).
async fn orphan_restore_target(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
    table_id: &str,
) {
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    let orphan = format!(
        "UPDATE {}.tables SET restore_owner_generation = ?, restore_heartbeat_at = ? \
         WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'",
        engine.catalog_keyspace()
    );
    let foreign_generation = 1_i64;
    let result = extenddb_storage_cassandra::cassandra_util::query_lwt(
        engine.session(),
        &orphan,
        cdrs_tokio::query_values!(foreign_generation, stale, account_id, name, table_id),
    )
    .await
    .expect("orphan the target");
    assert!(
        extenddb_storage_cassandra::cassandra_util::lwt_applied(&result).unwrap(),
        "the copy must still be running when the target is orphaned"
    );
}

/// The restore heartbeat runs while the target's data and index tables are
/// created, not only once the copy starts: with many indexes that DDL (each
/// statement waiting for schema agreement) can outlast the reconciler's
/// stale window on a slow cluster, and a heartbeat still at its insert value
/// would let the reconciler remove the live target.
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
        .create(&account_id, "ManyIndexes", "ddl", JobPermit::detached())
        .await
        .expect("create backup");

    // Watch the target row while the restore request runs: when it first
    // appears, and when its heartbeat is first written after the insert (the
    // column's write time moves past the row's, which the insert set).
    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        let watching = std::sync::Arc::clone(&watching);
        tokio::spawn(async move {
            let query = format!(
                "SELECT WRITETIME(restore_heartbeat_at) AS beat, WRITETIME(table_id) AS inserted \
                 FROM {}.tables WHERE account_id = ? AND table_name = ?",
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
                    let beat: Option<i64> = row.get_by_name("beat").ok().flatten();
                    let inserted: Option<i64> = row.get_by_name("inserted").ok().flatten();
                    if let (Some(beat), Some(inserted)) = (beat, inserted)
                        && beat > inserted
                    {
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

// ── The catalog index ───────────────────────────────────────────────────────

/// A store identity no other test's rows carry, so a store-scoped listing
/// in the shared catalog keyspace sees only this test's rows.
fn unique_store() -> StoreIdentity {
    StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!(
            "/var/lib/extenddb/backups-{}",
            crate::helpers::unique_test_id()
        ),
        store_id: "store-1".to_owned(),
    }
}

/// A row for a backup of `table` in `account` minted now, with the manifest
/// fixture's source table.
fn index_row(account: &str, table: &str, name: &str, store: &StoreIdentity) -> BackupRow {
    let backup_id = extenddb_storage::util::new_backup_id();
    let backup_arn = extenddb_storage::util::backup_arn("us-east-1", account, table, &backup_id);
    let manifest: serde_json::Value = serde_json::from_str(include_str!(
        "../../../core/testdata/extenddb-manifest/v1.json"
    ))
    .expect("fixture");
    BackupRow {
        backup_id: backup_id.clone(),
        account_id: account.to_owned(),
        table_name: table.to_owned(),
        backup_arn,
        backup_name: name.to_owned(),
        status: BackupStatus::Creating,
        store: store.clone(),
        size_bytes: 0,
        item_count: 0,
        created_at: extenddb_storage::util::backup_creation_date_time(&backup_id).unwrap(),
        completed_at: None,
        failure_reason: None,
        source_table_details_json: manifest["source_table"].clone(),
        owner_generation: 0,
        heartbeat_at: 0.0,
    }
}

fn arn_of(row: &BackupRow) -> BackupArn {
    BackupArn::parse(&row.backup_arn).unwrap()
}

/// Run `cql` with `values` as a lightweight transaction and assert it applied.
async fn apply(engine: &CassandraEngine, cql: &str, values: cdrs_tokio::query::QueryValues) {
    let result =
        extenddb_storage_cassandra::cassandra_util::query_lwt(engine.session(), cql, values)
            .await
            .expect("lwt");
    assert!(
        extenddb_storage_cassandra::cassandra_util::lwt_applied(&result).unwrap(),
        "{cql}"
    );
}

/// The claim: a second claim of the same backup is refused while the row
/// exists, the heartbeat confirms the claim and refreshes the row, a status
/// write is conditioned on the generation and the `from` status, and the row
/// is read back by account, backup id, table and ARN.
#[tokio::test]
async fn test_claim_backup_excludes_a_second_owner_and_fences_its_writes() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    assert!(
        uuid::Uuid::parse_str(&engine.catalog_id().await.expect("id")).is_ok(),
        "the catalog has an id from the migration"
    );
    let store = unique_store();
    let row = index_row(&account_id, "Music", "nightly", &store);
    let claim = engine.claim_backup(&row).await.expect("claim");
    assert_ne!(claim.generation(), UNOWNED_GENERATION);
    let second = engine.claim_backup(&row).await.err();
    assert!(
        matches!(second, Some(StorageError::AlreadyExists(_))),
        "{second:?}"
    );
    let stored = engine
        .read_backup_row(&arn_of(&row))
        .await
        .expect("read")
        .expect("the row exists");
    assert_eq!(stored.status, BackupStatus::Creating);
    assert_eq!(stored.owner_generation, claim.generation());
    assert_eq!(stored.store, store);
    assert_eq!(stored.created_at, row.created_at);
    assert_eq!(stored.backup_id, row.backup_id);
    assert_eq!(
        stored.source_table_details_json,
        row.source_table_details_json
    );
    let before = stored.heartbeat_at;
    assert!(before > 0.0, "the insert stamps the heartbeat");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    let after = engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap()
        .heartbeat_at;
    assert!(after > before, "{after} > {before}");

    // The same id under another account, table or region finds nothing; the
    // legacy reader does not see the row either.
    let other_account = unique_test_account();
    let other_account_arn =
        BackupArn::new("aws", "us-east-1", &other_account, "Music", &row.backup_id).unwrap();
    assert!(
        engine
            .read_backup_row(&other_account_arn)
            .await
            .unwrap()
            .is_none()
    );
    let other_table =
        BackupArn::new("aws", "us-east-1", &account_id, "Other", &row.backup_id).unwrap();
    assert!(
        engine
            .read_backup_row(&other_table)
            .await
            .unwrap()
            .is_none()
    );
    let other_region =
        BackupArn::new("aws", "eu-west-1", &account_id, "Music", &row.backup_id).unwrap();
    assert!(
        engine
            .read_backup_row(&other_region)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        matches!(
            engine.describe(&account_id, &row.backup_arn).await,
            Err(StorageError::Validation(ref m)) if m.contains("Backup not found")
        ),
        "an index row is not an in-catalog backup"
    );

    // A write from the wrong state matches nothing; the right one matches
    // once, and the row then carries what it set.
    let fields = StatusFields {
        size_bytes: Some(1234),
        item_count: Some(5),
        failure_reason: None,
        completed: true,
    };
    assert_eq!(
        claim
            .set_status(JobStatus::Running, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    assert_eq!(
        claim
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    assert_eq!(
        claim
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    let read = claim.read().await.expect("read");
    assert_eq!(read.status, JobStatus::Available);
    assert_eq!(read.owner_generation, claim.generation());
    let stored = engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, BackupStatus::Available);
    assert_eq!(stored.size_bytes, 1234);
    assert_eq!(stored.item_count, 5);
    assert!(stored.completed_at.is_some());
    // The status index followed the row.
    let available: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(available.len(), 1, "{available:?}");
    assert!(
        engine
            .list_backup_rows(Some(BackupStatus::Creating), &store)
            .try_collect::<Vec<BackupRow>>()
            .await
            .unwrap()
            .is_empty()
    );
    // The legacy listing does not show the index row.
    assert!(
        engine
            .list(&account_id, None)
            .await
            .expect("legacy list")
            .is_empty()
    );

    // A row another generation took over confirms nothing for this claim.
    apply(
        &engine,
        &format!(
            "UPDATE {}.backups_by_arn SET owner_generation = ? \
             WHERE account_id = ? AND backup_arn = ? IF owner_generation = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            i64::try_from(claim.generation()).unwrap() + 1,
            account_id.as_str(),
            row.backup_arn.as_str(),
            i64::try_from(claim.generation()).unwrap()
        ),
    )
    .await;
    assert_eq!(claim.heartbeat().await.expect("heartbeat"), Heartbeat::Lost);
    assert_eq!(
        claim
            .set_status(
                JobStatus::Available,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    drop(claim);
    // The row is the claim: a successor can take it only through its insert,
    // which the existing row refuses.
    let third = engine.claim_backup(&row).await.err();
    assert!(
        matches!(third, Some(StorageError::AlreadyExists(_))),
        "{third:?}"
    );
}

/// DeleteBackup's first step and the tombstone writes: only an AVAILABLE row
/// moves to DELETING; a CREATING row answers its status; a backup a CREATING
/// restore target reads answers the target's name; DELETING moves to DELETED
/// once; a DELETED row is removed once, with its list and status entries; a
/// missing row is "Backup not found".
#[tokio::test]
async fn test_start_backup_delete_checks_the_status_and_the_in_use_rule_under_the_lock() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = index_row(&account_id, "Music", "nightly", &store);
    let key = row.key();
    assert!(matches!(
        engine.start_backup_delete(&key).await,
        Err(StorageError::Validation(m)) if m.contains("Backup not found")
    ));
    let claim = engine.claim_backup(&row).await.expect("claim");
    assert_eq!(
        engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Status(BackupStatus::Creating)
    );
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    assert_eq!(engine.backup_in_use(&key).await.unwrap(), None);

    // A CREATING restore target registered from the backup: in use, naming
    // the target; an ACTIVE one is not, and its registration is cleared.
    let target = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "DelTarget".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    let catalog = engine.catalog_keyspace();
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'CREATING', status_transition_at = null, \
             restore_source_backup_arn = ? WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(
            row.backup_arn.as_str(),
            account_id.as_str(),
            "DelTarget",
            target.table_id.as_str()
        ),
    )
    .await;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS"
        ),
        cdrs_tokio::query_values!(
            target.table_id.as_str(),
            "DelTarget",
            account_id.as_str(),
            row.backup_arn.as_str()
        ),
    )
    .await;
    assert_eq!(
        engine.backup_in_use(&key).await.unwrap().as_deref(),
        Some("DelTarget")
    );
    assert_eq!(
        engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::InUse("DelTarget".to_owned())
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available,
        "a refused delete leaves the row AVAILABLE"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'ACTIVE' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "DelTarget", target.table_id.as_str()),
    )
    .await;
    assert_eq!(engine.backup_in_use(&key).await.unwrap(), None);

    assert_eq!(
        engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Deleting
    );
    assert_eq!(
        engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Status(BackupStatus::Deleting),
        "a retry finds the row DELETING"
    );
    assert!(
        !engine.remove_backup_row(&key).await.unwrap(),
        "DELETING is not a tombstone"
    );
    assert!(engine.set_backup_deleted(&key).await.unwrap());
    assert!(!engine.set_backup_deleted(&key).await.unwrap());
    let stored = engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, BackupStatus::Deleted);
    assert!(stored.completed_at.is_some());
    let deleted: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Deleted), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1, "the status index followed: {deleted:?}");
    assert!(engine.remove_backup_row(&key).await.unwrap());
    assert!(!engine.remove_backup_row(&key).await.unwrap());
    assert!(
        engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .list_backup_rows(None, &store)
            .try_collect::<Vec<BackupRow>>()
            .await
            .unwrap()
            .is_empty(),
        "the status entry went with the row"
    );
    assert!(
        engine
            .list_account_backup_rows(&account_id, None, &TimeBounds::default())
            .try_collect::<Vec<BackupRow>>()
            .await
            .unwrap()
            .is_empty(),
        "the list entries went with the row"
    );
    drop(claim);
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "DelTarget".to_owned(),
            },
        )
        .await
        .expect("delete the target");
}

/// The listings: by account with an optional table and the time bounds
/// (lower inclusive, upper exclusive) in (created_at, ARN) order, and by
/// store and status.
#[tokio::test]
async fn test_the_catalog_listings_are_scoped_and_ordered() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let other = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let other_id = other.account_id.clone();
    let store = unique_store();
    let mut rows = Vec::new();
    for (account, table, name) in [
        (account_id.as_str(), "Music", "a"),
        (account_id.as_str(), "Music", "b"),
        (account_id.as_str(), "Other", "c"),
        (other_id.as_str(), "Music", "d"),
    ] {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let row = index_row(account, table, name, &store);
        let claim = engine.claim_backup(&row).await.expect("claim");
        if name == "b" {
            assert_eq!(
                claim
                    .set_status(
                        JobStatus::Creating,
                        JobStatus::Failed,
                        &StatusFields::default()
                    )
                    .await
                    .unwrap(),
                WriteOutcome::Matched
            );
        }
        rows.push((row, claim));
    }
    let names = |listed: Vec<BackupRow>| -> Vec<String> {
        listed.into_iter().map(|r| r.backup_name).collect()
    };
    let all: Vec<BackupRow> = engine
        .list_account_backup_rows(&account_id, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        names(all.clone()),
        ["a", "b", "c"],
        "every status, in creation order"
    );
    assert_eq!(all[1].status, BackupStatus::Failed);
    let music: Vec<BackupRow> = engine
        .list_account_backup_rows(&account_id, Some("Music"), &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(music), ["a", "b"]);
    let bounds = TimeBounds {
        lower: Some(rows[1].0.created_at),
        upper: Some(rows[2].0.created_at),
    };
    let bounded: Vec<BackupRow> = engine
        .list_account_backup_rows(&account_id, None, &bounds)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(bounded), ["b"], "lower inclusive, upper exclusive");
    let others: Vec<BackupRow> = engine
        .list_account_backup_rows(&other_id, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(others), ["d"]);

    let by_store: Vec<BackupRow> = engine
        .list_backup_rows(None, &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(by_store), ["a", "b", "c", "d"]);
    let failed: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Failed), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(failed), ["b"]);
    let elsewhere = StoreIdentity {
        store_id: "store-2".to_owned(),
        ..store.clone()
    };
    let none: Vec<BackupRow> = engine
        .list_backup_rows(None, &elsewhere)
        .try_collect()
        .await
        .unwrap();
    assert!(none.is_empty(), "another store's rows are not listed");
    drop(rows);
}

/// The restore job record of a target, as `(status, kind, generation,
/// heartbeat_at ms, completed_at ms)`.
async fn job_record(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
) {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT restore_status, restore_kind, restore_owner_generation, restore_heartbeat_at, \
             restore_completed_at FROM {}.tables WHERE account_id = ? AND table_name = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id, name),
        "job record",
    )
    .await
    .expect("read the record");
    let row = rows.first().expect("the target exists");
    (
        row.get_by_name("restore_status").ok().flatten(),
        row.get_by_name("restore_kind").ok().flatten(),
        row.get_by_name("restore_owner_generation").ok().flatten(),
        row.get_by_name("restore_heartbeat_at").ok().flatten(),
        row.get_by_name("restore_completed_at").ok().flatten(),
    )
}

/// A catalog at the shape before V006 holding a finished restore and a
/// restore the stop interrupted (its target CREATING) migrates with the
/// registrations backfilled: the finished one DONE, the interrupted one
/// RUNNING with the heartbeat at the epoch and the unowned generation, so the
/// reconciler's first pass fails it and removes its target. A second run of
/// the migration keeps the catalog id.
#[tokio::test]
async fn test_migration_v006_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one()
 {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("bf", false).await;
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "BfSource".to_owned(),
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
        (0..40).map(|i| item(&format!("i{i}"), None, "v")).collect(),
    )
    .await;
    let backup = engine
        .create(&account_id, "BfSource", "b", JobPermit::detached())
        .await
        .expect("backup");
    let overrides = RestoreTableOverrides::default();
    let finished = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "BfDone",
        &overrides,
    )
    .await
    .expect("restore");
    wait_active(&engine, &account_id, "BfDone", Duration::from_secs(60)).await;
    let interrupted = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "BfCut",
        &overrides,
    )
    .await
    .expect("restore");
    wait_active(&engine, &account_id, "BfCut", Duration::from_secs(60)).await;
    let catalog_id = engine.catalog_id().await.expect("id");

    // Back to the shape before V006: the registrations as the previous
    // release wrote them (no job record), the interrupted target CREATING,
    // and the migration forgotten.
    let catalog = engine.catalog_keyspace();
    for (name, table_id) in [
        ("BfDone", finished.table_id.as_str()),
        ("BfCut", interrupted.table_id.as_str()),
    ] {
        apply(
            &engine,
            &format!(
                "UPDATE {catalog}.tables SET restore_status = null, restore_kind = null, \
                 restore_owner_generation = null, restore_heartbeat_at = null, \
                 restore_completed_at = null WHERE account_id = ? AND table_name = ? \
                 IF table_id = ?"
            ),
            cdrs_tokio::query_values!(account_id.as_str(), name, table_id),
        )
        .await;
    }
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'CREATING', status_transition_at = null \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "BfCut", interrupted.table_id.as_str()),
    )
    .await;
    engine
        .session()
        .query(format!(
            "DELETE FROM {catalog}.schema_history WHERE version = 6"
        ))
        .await
        .expect("forget V006");
    extenddb_storage_cassandra::migrations::run_catalog_migrations(&engine.session_arc(), &catalog)
        .await
        .expect("migrate again");

    let (status, kind, generation, heartbeat, completed) =
        job_record(&engine, &account_id, "BfCut").await;
    assert_eq!(status.as_deref(), Some("RUNNING"));
    assert_eq!(kind.as_deref(), Some("legacy"));
    assert_eq!(generation, Some(i64::try_from(UNOWNED_GENERATION).unwrap()));
    assert_eq!(heartbeat, Some(0), "the epoch, not null");
    assert_eq!(completed, None);
    let (status, kind, _, _, completed) = job_record(&engine, &account_id, "BfDone").await;
    assert_eq!(status.as_deref(), Some("DONE"));
    assert_eq!(kind.as_deref(), Some("legacy"));
    #[allow(clippy::cast_possible_truncation)]
    let created_ms = (finished.creation_date_time * 1_000.0).round() as i64;
    assert_eq!(
        completed,
        Some(created_ms),
        "completed when the table was created"
    );
    assert_eq!(
        engine.catalog_id().await.expect("id"),
        catalog_id,
        "a second run keeps the catalog id"
    );

    // The first reconciler pass: the interrupted restore is dead.
    let dead = engine
        .list_dead_restore_jobs(Duration::from_secs(300))
        .await
        .expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(
        dead[0].target,
        RestoreTargetKey {
            account_id: account_id.clone(),
            table_name: "BfCut".to_owned(),
            table_id: interrupted.table_id.clone(),
        }
    );
    assert_eq!(dead[0].owner_generation, UNOWNED_GENERATION);
    assert_eq!(dead[0].source_backup_arn, backup.backup_arn);
    assert!(
        engine
            .fail_restore_job(&dead[0].target, "owner dead")
            .await
            .unwrap()
    );
    assert!(engine.remove_restore_target(&dead[0].target).await.unwrap());
    assert!(matches!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "BfCut".to_owned(),
                },
            )
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(
        engine.table_key_info(&account_id, "BfCut").await.is_err(),
        "the target's data went with it"
    );
    assert_eq!(
        describe(&engine, &account_id, "BfDone").await.table_status,
        TableStatus::Active
    );
    assert_eq!(
        describe(&engine, &account_id, "BfSource")
            .await
            .table_status,
        TableStatus::Active
    );
    engine
        .delete(&account_id, &backup.backup_arn)
        .await
        .expect("the removed target released the backup");
}

/// The legacy restore registers through the job record: RUNNING under the
/// claim's generation with a fresh heartbeat while the copy runs, DONE with
/// the ACTIVE flip.
#[tokio::test]
async fn test_a_legacy_restore_registers_a_running_job_record_and_finishes_it_done() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 1_500;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "JrSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "JrTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("registers");
    let (status, kind, generation, heartbeat, completed) =
        job_record(&engine, &account_id, "JrTarget").await;
    assert_eq!(status.as_deref(), Some("RUNNING"));
    assert_eq!(kind.as_deref(), Some("legacy"));
    assert_ne!(generation, Some(i64::try_from(UNOWNED_GENERATION).unwrap()));
    assert!(generation.is_some_and(|g| g > 0));
    let now = extenddb_storage_cassandra::cassandra_util::now_millis();
    let age = now - heartbeat.expect("a heartbeat");
    assert!(age.abs() < 60_000, "a fresh heartbeat: {age} ms old");
    assert_eq!(completed, None);
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "JrTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    assert!(
        !engine
            .list_dead_restore_jobs(Duration::from_secs(300))
            .await
            .unwrap()
            .iter()
            .any(|job| job.target == key),
        "a running restore is not dead"
    );
    wait_active(&engine, &account_id, "JrTarget", Duration::from_secs(180)).await;
    let (status, _, generation_after, _, completed) =
        job_record(&engine, &account_id, "JrTarget").await;
    assert_eq!(status.as_deref(), Some("DONE"));
    assert_eq!(generation_after, generation, "DONE under the same claim");
    assert!(completed.is_some());
    assert!(
        !engine.fail_restore_job(&key, "late").await.unwrap(),
        "a DONE record is not RUNNING"
    );
    assert!(
        !engine.remove_restore_target(&key).await.unwrap(),
        "an ACTIVE table is not a CREATING restore target"
    );
}

/// An in-catalog CreateBackup whose process died leaves a CREATING row whose
/// heartbeat goes stale. `abandoned_creating_backups` reports it only once
/// the heartbeat is older than the window, exactly once (the DELETING scan
/// runs before the CREATING one, so the row it just fenced is not read
/// again), fencing the row to DELETING under a fresh generation so the dead
/// create's heartbeat no longer applies; the reader's `delete` then removes
/// the row, its list entries and every payload bucket the row did not yet
/// account for.
#[tokio::test]
async fn test_an_abandoned_in_catalog_create_is_reported_once_stale_and_deleted() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 200;
    const DEAD_GENERATION: u64 = 4242;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) =
        backed_up_plain_table(&engine, "AbandonedCreate", ITEMS).await;
    let account_id = account.account_id.clone();
    let abandoned = extenddb_storage::backup_catalog::AbandonedLegacyBackup {
        account_id: account_id.clone(),
        backup_arn: backup_arn.clone(),
    };
    let reported = |stale_after: Duration| {
        let engine = &engine;
        let abandoned = &abandoned;
        async move {
            engine
                .abandoned_creating_backups(stale_after)
                .await
                .unwrap()
                .into_iter()
                .filter(|row| row == abandoned)
                .count()
        }
    };
    // A live, finished backup is not reported.
    assert_eq!(reported(Duration::from_secs(60)).await, 0);
    // What a create that died mid-copy leaves: a CREATING row counting no
    // items yet, owned by the dead create's generation, its payload buckets
    // written, its heartbeat two minutes old.
    let catalog = engine.catalog_keyspace();
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET backup_status = 'CREATING', item_count = 0, \
             owner_generation = ? WHERE account_id = ? AND backup_arn = ? \
             IF backup_status = 'AVAILABLE'"
        ),
        cdrs_tokio::query_values!(
            i64::try_from(DEAD_GENERATION).unwrap(),
            account_id.as_str(),
            backup_arn.as_str()
        ),
    )
    .await;
    assert!(
        extenddb_storage_cassandra::heartbeat_backup_row(
            &engine,
            &account_id,
            &backup_arn,
            DEAD_GENERATION
        )
        .await
        .unwrap(),
        "until the row is fenced, the create's own heartbeat applies"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET heartbeat_at = ? \
             WHERE account_id = ? AND backup_arn = ? IF backup_status = 'CREATING'"
        ),
        cdrs_tokio::query_values!(stale, account_id.as_str(), backup_arn.as_str()),
    )
    .await;
    assert!(
        matches!(
            engine.delete(&account_id, &backup_arn).await,
            Err(StorageError::Validation(ref m)) if m.contains("Backup not found")
        ),
        "a client's delete of a backup still being created is not found"
    );
    assert_eq!(
        reported(Duration::from_secs(600)).await,
        0,
        "a heartbeat younger than the window is not dead"
    );
    assert_eq!(
        reported(Duration::from_secs(60)).await,
        1,
        "a heartbeat older than the window is dead, reported once"
    );
    assert!(
        !extenddb_storage_cassandra::heartbeat_backup_row(
            &engine,
            &account_id,
            &backup_arn,
            DEAD_GENERATION
        )
        .await
        .unwrap(),
        "the fence took the row under a fresh generation: the dead create's heartbeat is lost"
    );
    let deleted = engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the reconciler's delete finishes the fenced row");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert_eq!(
        reported(Duration::from_secs(60)).await,
        0,
        "a deleted row is not reported again"
    );
    assert!(engine.list(&account_id, None).await.unwrap().is_empty());
    // The payload is gone, bucket by bucket past the count the row carried.
    let probe = format!(
        "SELECT item_index FROM {}.backup_items WHERE backup_arn = ? AND bucket = ? LIMIT 1",
        engine.account_keyspace(&account_id)
    );
    for bucket in 0..4_i32 {
        let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
            &engine.session_arc(),
            &probe,
            cdrs_tokio::query_values!(backup_arn.as_str(), bucket),
            "probe",
        )
        .await
        .unwrap();
        assert!(rows.is_empty(), "bucket {bucket} still holds items");
    }
}

/// The `restore_completed_at` of a target's job record, in epoch ms.
async fn completed_at(engine: &CassandraEngine, account_id: &str, name: &str) -> Option<i64> {
    job_record(engine, account_id, name).await.4
}

/// The dead jobs under `stale_after` that name `key`.
async fn dead_jobs_of(
    engine: &CassandraEngine,
    key: &RestoreTargetKey,
    stale_after: Duration,
) -> Vec<extenddb_storage::backup_catalog::RestoreJobRow> {
    engine
        .list_dead_restore_jobs(stale_after)
        .await
        .expect("list dead jobs")
        .into_iter()
        .filter(|job| &job.target == key)
        .collect()
}

/// A restore whose FAILED mark landed but whose target removal did not (the
/// target still CREATING) is listed by `list_dead_restore_jobs` as FAILED
/// once the record is stale: its `restore_completed_at`, or its heartbeat
/// where none was stamped, older than the window. While fresh it is left to
/// the writer still on its failure path. `remove_restore_target` then removes
/// the target, with the FAILED record as it was. A FAILED record whose table
/// is ACTIVE is not listed; one whose table is DELETING with its registration
/// is.
#[tokio::test]
async fn test_a_failed_record_whose_target_removal_did_not_land_is_listed_once_stale_and_removed() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 1_500;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "FtSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let catalog = engine.catalog_keyspace();

    // A finished restore: an ACTIVE table whose record is then marked FAILED
    // and aged by hand. Not a CREATING target, so never listed.
    let done = restore(
        &engine,
        &account_id,
        &backup_arn,
        "FtDone",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    wait_active(&engine, &account_id, "FtDone", Duration::from_secs(120)).await;
    let done_key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "FtDone".to_owned(),
        table_id: done.table_id.clone(),
    };
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET restore_status = 'FAILED', restore_completed_at = 0 \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "FtDone", done.table_id.as_str()),
    )
    .await;
    assert!(
        dead_jobs_of(&engine, &done_key, Duration::from_secs(1))
            .await
            .is_empty(),
        "a FAILED record of an ACTIVE table is not listed"
    );

    // The shape the duty exists for: the owner died, the reconciler's FAILED
    // write landed, the removal did not.
    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "FtTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    orphan_restore_target(&engine, &account_id, "FtTarget", &target.table_id).await;
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "FtTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    assert!(
        engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail")
    );
    let failed_at = completed_at(&engine, &account_id, "FtTarget")
        .await
        .expect("the FAILED write stamps completed_at");
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .is_empty(),
        "a fresh FAILED record is left to the writer on its failure path"
    );

    // Two minutes later (on the record's clock): stale under a 60 s window,
    // not under a 10 minute one.
    let stale = failed_at - 120_000;
    let set_completed = |value: Option<i64>| {
        let engine = &engine;
        let catalog = &catalog;
        let account_id = &account_id;
        let table_id = &target.table_id;
        async move {
            apply(
                engine,
                &format!(
                    "UPDATE {catalog}.tables SET restore_completed_at = ? \
                     WHERE account_id = ? AND table_name = ? IF table_id = ?"
                ),
                cdrs_tokio::query_values!(
                    value,
                    account_id.as_str(),
                    "FtTarget",
                    table_id.as_str()
                ),
            )
            .await;
        }
    };
    set_completed(Some(stale)).await;
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(600))
            .await
            .is_empty(),
        "not stale under a window longer than its age"
    );
    let listed = dead_jobs_of(&engine, &key, Duration::from_secs(60)).await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].status, JobStatus::Failed);
    assert_eq!(
        listed[0].kind,
        extenddb_storage::backup_catalog::RestoreKind::Legacy
    );
    assert_eq!(listed[0].source_backup_arn, backup_arn);

    // No completion stamp: the record is aged by its heartbeat (two minutes
    // old since it was orphaned).
    set_completed(None).await;
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(600))
            .await
            .is_empty(),
        "aged by the heartbeat: not stale under 10 minutes"
    );
    assert_eq!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .len(),
        1,
        "aged by the heartbeat: stale under 60 s"
    );

    // A DELETING table with its registration (the removal's first write
    // landed) is listed too; the removal of that state is
    // `test_a_failed_record_whose_target_is_deleting_is_listed_once_stale_and_its_removal_resumed`.
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'DELETING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "FtTarget", target.table_id.as_str()),
    )
    .await;
    assert_eq!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .len(),
        1,
        "a FAILED record of a DELETING table with its registration is listed"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'CREATING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "FtTarget", target.table_id.as_str()),
    )
    .await;

    // The duty: the target removed, the FAILED record not written again.
    assert!(
        !engine
            .fail_restore_job(&key, "again")
            .await
            .expect("fail again"),
        "a FAILED record is not RUNNING: no second FAILED write"
    );
    assert!(
        engine.remove_restore_target(&key).await.expect("remove"),
        "the target was a CREATING restore target"
    );
    assert!(matches!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "FtTarget".to_owned(),
                },
            )
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .is_empty(),
        "a removed target has no record"
    );
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the removed target released the backup");
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "FtDone".to_owned(),
            },
        )
        .await
        .expect("delete the finished target");
}

/// The removal of a FAILED record's target is two writes: the claim from
/// CREATING to DELETING, then the drop. A target the claim moved to DELETING
/// whose drop then failed (the row DELETING with its registration and FAILED
/// record) is listed by `list_dead_restore_jobs` as FAILED once the record is
/// stale, left alone while fresh, and `remove_restore_target` resumes the
/// removal from the drop: the table, its data table and its record are gone,
/// the backup is released and the name is free. Three DELETING rows the
/// resume rule must leave alone: a restored table with a DONE record that a
/// client is deleting (its registration still on the row), which is neither
/// listed nor removed and keeps its data; the DELETING+FAILED row named by a
/// key with another `table_id`, on which `remove_restore_target` drops
/// nothing; and a DELETING row without a registration (a client's
/// DeleteTable in flight), neither listed nor removed.
#[tokio::test]
async fn test_a_failed_record_whose_target_is_deleting_is_listed_once_stale_and_its_removal_resumed()
 {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 1_500;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "PaSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let catalog = engine.catalog_keyspace();

    // A restored table, ACTIVE with a DONE record, that a client's
    // DeleteTable moved to DELETING: the registration is on the row and the
    // record is not FAILED, so the row belongs to that delete.
    let done = restore(
        &engine,
        &account_id,
        &backup_arn,
        "PaDone",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore the table a client deletes");
    wait_active(&engine, &account_id, "PaDone", Duration::from_secs(60)).await;
    let done_key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "PaDone".to_owned(),
        table_id: done.table_id.clone(),
    };
    assert_eq!(
        job_record(&engine, &account_id, "PaDone")
            .await
            .0
            .as_deref(),
        Some("DONE"),
        "the activated restore's record is DONE"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'DELETING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'ACTIVE'"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "PaDone", done.table_id.as_str()),
    )
    .await;
    let done_data_table = format!("items_{}", done.table_id.replace('-', "_"));
    assert!(
        dead_jobs_of(&engine, &done_key, Duration::from_secs(1))
            .await
            .is_empty(),
        "a DONE record of a DELETING table is not listed"
    );
    assert!(
        !engine
            .remove_restore_target(&done_key)
            .await
            .expect("remove a restored table a client is deleting"),
        "a DELETING restored table whose record is DONE belongs to the client's delete"
    );
    assert!(
        data_table_exists(&engine, &account_id, &done_data_table).await,
        "the client's delete keeps the restored table's data"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'ACTIVE' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "PaDone", done.table_id.as_str()),
    )
    .await;
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "PaDone".to_owned(),
            },
        )
        .await
        .expect("delete the restored table");

    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "PaTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    orphan_restore_target(&engine, &account_id, "PaTarget", &target.table_id).await;
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "PaTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    assert!(
        engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail")
    );
    let failed_at = completed_at(&engine, &account_id, "PaTarget")
        .await
        .expect("the FAILED write stamps completed_at");
    // The removal's claim landed and its drop did not.
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'DELETING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "PaTarget", target.table_id.as_str()),
    )
    .await;
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .is_empty(),
        "a fresh FAILED record of a DELETING target is left to the remover on its path"
    );

    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET restore_completed_at = ? \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(
            failed_at - 120_000,
            account_id.as_str(),
            "PaTarget",
            target.table_id.as_str()
        ),
    )
    .await;
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(600))
            .await
            .is_empty(),
        "not stale under a window longer than its age"
    );
    let listed = dead_jobs_of(&engine, &key, Duration::from_secs(60)).await;
    assert_eq!(
        listed.len(),
        1,
        "a stale FAILED record of a DELETING target with its registration is listed: {listed:?}"
    );
    assert_eq!(listed[0].status, JobStatus::Failed);
    assert_eq!(listed[0].source_backup_arn, backup_arn);

    // The duty resumes the drop, on the row's own table id only: a key with
    // another id (a table re-created under the name) touches nothing.
    let data_table = format!("items_{}", target.table_id.replace('-', "_"));
    assert!(
        data_table_exists(&engine, &account_id, &data_table).await,
        "the target's data table exists before the removal"
    );
    let other_id_key = RestoreTargetKey {
        table_id: "not-this-id".to_owned(),
        ..key.clone()
    };
    assert!(
        !engine
            .remove_restore_target(&other_id_key)
            .await
            .expect("remove under another table id"),
        "a key naming another table id removes nothing from a DELETING target"
    );
    assert!(
        data_table_exists(&engine, &account_id, &data_table).await,
        "the data table is kept under a key with another table id"
    );
    assert_eq!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .len(),
        1,
        "the record is still listed after the mismatched removal"
    );
    assert!(
        engine.remove_restore_target(&key).await.expect("remove"),
        "a DELETING target with its registration and FAILED record is removed"
    );
    assert!(matches!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "PaTarget".to_owned(),
                },
            )
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(
        !data_table_exists(&engine, &account_id, &data_table).await,
        "the target's data table is dropped"
    );
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(60))
            .await
            .is_empty(),
        "a removed target has no record"
    );
    assert!(
        !engine
            .remove_restore_target(&key)
            .await
            .expect("remove again"),
        "nothing left to remove"
    );
    // The name is free, and the backup released.
    let reused = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "PaTarget".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    assert_ne!(reused.table_id, target.table_id);
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the removed target released the backup");

    // A DELETING row without a registration is a client's DeleteTable in
    // flight: not a restore target, so not listed and not removed.
    let plain_key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "PaTarget".to_owned(),
        table_id: reused.table_id.clone(),
    };
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'DELETING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'ACTIVE'"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "PaTarget", reused.table_id.as_str()),
    )
    .await;
    assert!(
        dead_jobs_of(&engine, &plain_key, Duration::from_secs(1))
            .await
            .is_empty(),
        "a DELETING table that is not a restore target is not listed"
    );
    assert!(
        !engine
            .remove_restore_target(&plain_key)
            .await
            .expect("remove a plain table"),
        "a DELETING table without a registration is not removed"
    );
    assert!(
        data_table_exists(
            &engine,
            &account_id,
            &format!("items_{}", reused.table_id.replace('-', "_"))
        )
        .await,
        "the client's delete keeps its table"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'ACTIVE' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "PaTarget", reused.table_id.as_str()),
    )
    .await;
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "PaTarget".to_owned(),
            },
        )
        .await
        .expect("delete the re-created table");
}

/// Whether `table` exists in `account_id`'s keyspace.
async fn data_table_exists(engine: &CassandraEngine, account_id: &str, table: &str) -> bool {
    !extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
        cdrs_tokio::query_values!(engine.account_keyspace(account_id), table),
        "schema",
    )
    .await
    .expect("read the schema")
    .is_empty()
}

#[cfg(feature = "test-hooks")]
/// The write times of a `backups_by_arn` row's `heartbeat_at` and of a column
/// only the insert writes (`backup_name`), as `(heartbeat, inserted)`, for
/// the one row of `account_id`, with the row's status.
async fn create_row_write_times(
    engine: &CassandraEngine,
    account_id: &str,
) -> Option<(Option<i64>, Option<i64>, String)> {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT WRITETIME(heartbeat_at) AS beat, WRITETIME(backup_name) AS inserted, \
             backup_status FROM {}.backups_by_arn WHERE account_id = ? ALLOW FILTERING",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id),
        "create row",
    )
    .await
    .expect("read the create's row");
    let row = rows.first()?;
    Some((
        row.get_by_name("beat").ok().flatten(),
        row.get_by_name("inserted").ok().flatten(),
        row.get_r_by_name("backup_status").expect("status"),
    ))
}

#[cfg(feature = "test-hooks")]
/// The in-catalog create heartbeats its CREATING row while the items copy:
/// `heartbeat_at` is written again after the insert, while the row is still
/// CREATING, and the published row carries the last beat.
#[tokio::test]
async fn test_the_in_catalog_create_heartbeats_its_row_while_the_items_copy() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 3_000;
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_heartbeat_interval(
        &account_id,
        Duration::from_millis(100),
    );
    let key_info = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "Beating".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &key_info,
        (0..ITEMS)
            .map(|i| item(&format!("item-{i:05}"), None, &"x".repeat(200)))
            .collect(),
    )
    .await;

    // Watch the row while the create runs: the first time its heartbeat's
    // write time passes the insert's while the row is still CREATING.
    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        let watching = std::sync::Arc::clone(&watching);
        tokio::spawn(async move {
            let mut refreshed_while_creating = false;
            while watching.load(std::sync::atomic::Ordering::Acquire) && !refreshed_while_creating {
                if let Some((Some(beat), Some(inserted), status)) =
                    create_row_write_times(&engine, &account_id).await
                    && beat > inserted
                    && status == "CREATING"
                {
                    refreshed_while_creating = true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            refreshed_while_creating
        })
    };
    let created = engine
        .create(&account_id, "Beating", "beats", JobPermit::detached())
        .await
        .expect("create backup");
    watching.store(false, std::sync::atomic::Ordering::Release);
    assert!(
        watcher.await.expect("watcher"),
        "heartbeat_at was written again while the row was CREATING"
    );
    let (beat, inserted, status) = create_row_write_times(&engine, &account_id)
        .await
        .expect("the published row");
    assert_eq!(status, "AVAILABLE");
    assert!(
        beat.zip(inserted)
            .is_some_and(|(beat, inserted)| beat > inserted),
        "the published row carries the last beat: {beat:?} vs {inserted:?}"
    );
    engine
        .delete(&account_id, &created.backup_arn)
        .await
        .expect("delete backup");
}

#[cfg(feature = "test-hooks")]
/// Items of `backup_arn`'s payload in `account_id`'s keyspace, all buckets.
async fn payload_item_count(engine: &CassandraEngine, account_id: &str, backup_arn: &str) -> i64 {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT COUNT(*) AS n FROM {}.backup_items WHERE backup_arn = ? ALLOW FILTERING",
            engine.account_keyspace(account_id)
        ),
        cdrs_tokio::query_values!(backup_arn),
        "payload count",
    )
    .await
    .expect("count the payload");
    rows.first()
        .and_then(|row| row.get_by_name("n").ok().flatten())
        .unwrap_or(0)
}

/// The `backup_status` of the `backups_by_arn` row `backup_arn`, if there is
/// one.
#[cfg(feature = "test-hooks")]
async fn backup_status_of(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
) -> Option<String> {
    use cdrs_tokio::types::IntoRustByName;
    extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT backup_status FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id, backup_arn),
        "backup status",
    )
    .await
    .expect("read the row")
    .first()
    .and_then(|row| row.get_by_name("backup_status").ok().flatten())
}

#[cfg(feature = "test-hooks")]
/// Three heartbeat errors in a row stop an in-catalog create within the
/// bucket it is writing: the create fails naming the unconfirmed claim, its
/// row stays CREATING for the reconciler, and the payload ends at a bucket
/// boundary short of the table. Two errors followed by a confirmed beat do
/// not stop it.
#[tokio::test]
async fn test_three_heartbeat_errors_stop_a_create_within_a_bucket_and_two_do_not() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: i64 = 3_000;
    const BUCKET: i64 = 64;
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_heartbeat_interval(
        &account_id,
        Duration::from_millis(10),
    );
    let key_info = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "Erring".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &key_info,
        (0..ITEMS)
            .map(|i| item(&format!("item-{i:05}"), None, &"x".repeat(200)))
            .collect(),
    )
    .await;

    // The create starts with confirmed beats; once its first bucket is on
    // disk, the next three beats fail.
    let create = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move {
            engine
                .create(&account_id, "Erring", "errs", JobPermit::detached())
                .await
        })
    };
    let backup_arn = first_written_backup_arn(&engine, &account_id).await;
    extenddb_storage_cassandra::test_hooks::fail_heartbeats(&account_id, 3);
    let error = create
        .await
        .expect("create task")
        .expect_err("three heartbeat errors in a row stop the create");
    assert!(
        matches!(error, StorageError::Internal(ref m) if m.contains("could not be confirmed")),
        "{error}"
    );
    assert_eq!(
        backup_status_of(&engine, &account_id, &backup_arn).await,
        Some("CREATING".to_owned()),
        "the row is left for the reconciler"
    );
    let written = payload_item_count(&engine, &account_id, &backup_arn).await;
    assert!(
        written > 0 && written < ITEMS,
        "the copy stopped part way: {written} of {ITEMS}"
    );
    assert_eq!(
        written % BUCKET,
        0,
        "the copy polls its claim per bucket, so it stopped at a bucket boundary: {written}"
    );
    // The reconciler finishes the row once its heartbeat is stale.
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    apply(
        &engine,
        &format!(
            "UPDATE {}.backups_by_arn SET heartbeat_at = ? \
             WHERE account_id = ? AND backup_arn = ? IF backup_status = 'CREATING'",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(stale, account_id.as_str(), backup_arn.as_str()),
    )
    .await;
    assert_eq!(
        engine
            .abandoned_creating_backups(Duration::from_secs(60))
            .await
            .unwrap()
            .iter()
            .filter(|row| row.backup_arn == backup_arn)
            .count(),
        1
    );
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the reconciler's delete finishes the stopped create");
    assert_eq!(
        payload_item_count(&engine, &account_id, &backup_arn).await,
        0
    );

    // Two errors, then confirmed beats: the create finishes. The errors are
    // armed once the copy is writing, and both are seen taken by a beat
    // while the payload is still short of the table, so the create met two
    // errors in a row where three would have stopped it.
    let create = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move {
            engine
                .create(&account_id, "Erring", "two-errors", JobPermit::detached())
                .await
        })
    };
    let second_arn = first_written_backup_arn(&engine, &account_id).await;
    extenddb_storage_cassandra::test_hooks::fail_heartbeats(&account_id, 2);
    let deadline = Instant::now() + Duration::from_secs(30);
    while extenddb_storage_cassandra::test_hooks::pending_heartbeat_errors(&account_id) > 0 {
        assert!(
            Instant::now() < deadline,
            "the two erring beats never fired"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let written_at_second_error = payload_item_count(&engine, &account_id, &second_arn).await;
    assert!(
        written_at_second_error < ITEMS,
        "both erring beats fired while the copy was still writing: {written_at_second_error} \
         of {ITEMS}"
    );
    let created = create
        .await
        .expect("create task")
        .expect("two heartbeat errors do not stop a create");
    assert_eq!(created.backup_arn, second_arn);
    assert_eq!(created.backup_status, "AVAILABLE");
    assert_eq!(
        payload_item_count(&engine, &account_id, &created.backup_arn).await,
        ITEMS
    );
    engine
        .delete(&account_id, &created.backup_arn)
        .await
        .expect("delete backup");
}

#[cfg(feature = "test-hooks")]
/// Wait for the first `backup_items` row in `account_id`'s keyspace and
/// return its backup ARN: the create whose copy has begun writing.
async fn first_written_backup_arn(engine: &CassandraEngine, account_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "the create never started writing"
        );
        if let Some(arn) = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
            &engine.session_arc(),
            &format!(
                "SELECT backup_arn FROM {}.backup_items LIMIT 1",
                engine.account_keyspace(account_id)
            ),
            cdrs_tokio::query_values!(),
            "first item",
        )
        .await
        .expect("probe")
        .first()
        .map(|row| {
            extenddb_storage_cassandra::cassandra_util::get_column::<String, StorageError>(
                row,
                "backup_arn",
                "first item",
            )
            .unwrap()
        }) {
            return arn;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[cfg(feature = "test-hooks")]
/// A restore that registers on the backup between DeleteBackup's in-use read
/// and its DELETING claim is what the claim's own `active_restores = null`
/// condition is for: the claim does not apply, the delete answers
/// BackupInUse naming the target, and the backup stays AVAILABLE; the same
/// for `start_backup_delete` on a catalog index row.
#[tokio::test]
async fn test_a_restore_registering_between_the_in_use_read_and_the_claim_keeps_the_backup() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 1_500;
    let engine = std::sync::Arc::new(setup_engine().await);
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "RaceSource", ITEMS).await;
    let account_id = account.account_id.clone();

    // The legacy delete, paused after its in-use read found no restore.
    let pause = extenddb_storage_cassandra::test_hooks::pause_before_delete_claim(&backup_arn);
    let delete = {
        let engine = std::sync::Arc::clone(&engine);
        let (account_id, backup_arn) = (account_id.clone(), backup_arn.clone());
        tokio::spawn(async move { engine.delete(&account_id, &backup_arn).await })
    };
    pause.reached().await;
    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "RaceTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("the restore registers while the delete is between its read and its claim");
    assert_eq!(target.table_status, TableStatus::Creating);
    pause.release();
    let refused = delete
        .await
        .expect("delete task")
        .expect_err("the claim's active_restores = null condition refuses the delete");
    assert!(
        matches!(refused, StorageError::BackupInUse(ref m) if m.contains("RaceTarget")),
        "{refused}"
    );
    assert_eq!(
        backup_status_of(&engine, &account_id, &backup_arn).await,
        Some("AVAILABLE".to_owned()),
        "a refused delete leaves the row"
    );
    wait_active(&engine, &account_id, "RaceTarget", Duration::from_secs(120)).await;
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the finished restore released the backup");

    // `start_backup_delete` on an index row: the registration lands in the
    // same gap; the claim answers InUse and the row stays AVAILABLE.
    let store = unique_store();
    let row = index_row(&account_id, "RaceSource", "nightly", &store);
    let key = row.key();
    let claim = engine.claim_backup(&row).await.expect("claim");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    let pause = extenddb_storage_cassandra::test_hooks::pause_before_delete_claim(&row.backup_arn);
    let start = {
        let engine = std::sync::Arc::clone(&engine);
        let key = key.clone();
        tokio::spawn(async move { engine.start_backup_delete(&key).await })
    };
    pause.reached().await;
    let catalog = engine.catalog_keyspace();
    let registered = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "RaceIndexTarget".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'CREATING', status_transition_at = null, \
             restore_source_backup_arn = ? WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(
            row.backup_arn.as_str(),
            account_id.as_str(),
            "RaceIndexTarget",
            registered.table_id.as_str()
        ),
    )
    .await;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? IF backup_status = 'AVAILABLE'"
        ),
        cdrs_tokio::query_values!(
            registered.table_id.as_str(),
            "RaceIndexTarget",
            account_id.as_str(),
            row.backup_arn.as_str()
        ),
    )
    .await;
    pause.release();
    assert_eq!(
        start.await.expect("start task").unwrap(),
        DeleteStart::InUse("RaceIndexTarget".to_owned())
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available,
        "a refused delete leaves the row AVAILABLE"
    );
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET table_status = 'ACTIVE' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(
            account_id.as_str(),
            "RaceIndexTarget",
            registered.table_id.as_str()
        ),
    )
    .await;
    assert_eq!(
        engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Deleting
    );
    assert!(engine.set_backup_deleted(&key).await.unwrap());
    assert!(engine.remove_backup_row(&key).await.unwrap());
    drop(claim);
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "RaceIndexTarget".to_owned(),
            },
        )
        .await
        .expect("delete the target");
}

/// A V005 catalog whose V006 step fails (a `tables` row carrying a restore
/// registration and no `table_id`, which the backfill cannot key) stays at
/// 0.0.4 with V006 unrecorded, so `serve` refuses it and `migrate` runs the
/// file again; once the cause is gone the second run lands at 0.0.5 with
/// V006 recorded. A fresh catalog lands there in one run.
#[tokio::test]
async fn test_a_migration_that_fails_in_its_step_leaves_the_version_behind_and_resumes() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    async fn catalog_state(engine: &CassandraEngine, catalog: &str) -> (Option<String>, Vec<i32>) {
        use cdrs_tokio::types::IntoRustByName;
        let session = engine.session_arc();
        let version = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
            &session,
            &format!("SELECT value FROM {catalog}.settings WHERE key = 'catalog_version'"),
            cdrs_tokio::query_values!(),
            "version",
        )
        .await
        .unwrap()
        .first()
        .and_then(|row| {
            let value: Option<String> = row.get_by_name("value").ok().flatten();
            value
        });
        let mut recorded: Vec<i32> =
            extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
                &session,
                &format!("SELECT version FROM {catalog}.schema_history"),
                cdrs_tokio::query_values!(),
                "history",
            )
            .await
            .unwrap()
            .iter()
            .map(|row| {
                let version: i32 = row.get_r_by_name("version").unwrap();
                version
            })
            .collect();
        recorded.sort_unstable();
        (version, recorded)
    }

    let (fresh, _account, _drop_fresh) = isolated_engine("mv", false).await;
    assert_eq!(
        catalog_state(&fresh, &fresh.catalog_keyspace()).await,
        (
            Some(extenddb_storage_cassandra::CATALOG_VERSION.to_owned()),
            vec![1, 2, 3, 4, 5, 6]
        ),
        "a fresh catalog is at the binary's version with every file recorded"
    );

    let (engine, account_id, _drop_keyspaces) = isolated_engine("mf", true).await;
    let catalog = engine.catalog_keyspace();
    assert_eq!(
        catalog_state(&engine, &catalog).await,
        (Some("0.0.4".to_owned()), vec![1, 2, 3, 4, 5])
    );
    // A registration the step cannot key.
    engine
        .session()
        .query_with_values(
            format!(
                "INSERT INTO {catalog}.tables (account_id, table_name, table_status, \
                 restore_source_backup_arn) VALUES (?, 'Unkeyed', 'ACTIVE', 'arn:aws:dynamodb:us-east-1:000000000000:table/T/backup/01700000000000-abc')"
            ),
            cdrs_tokio::query_values!(account_id.as_str()),
        )
        .await
        .expect("a row without a table_id");
    let _failed = extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        &catalog,
    )
    .await
    .expect_err("the step fails on the unkeyed row");
    assert_eq!(
        catalog_state(&engine, &catalog).await,
        (Some("0.0.4".to_owned()), vec![1, 2, 3, 4, 5]),
        "a failed step leaves the version and the history where they were"
    );
    let refusal = engine
        .require_served_catalog()
        .await
        .expect_err("serve refuses the half-migrated catalog")
        .to_string();
    assert!(
        refusal.contains("at version 0.0.4") && refusal.contains("extenddb migrate --yes"),
        "{refusal}"
    );
    assert!(
        matches!(
            engine.create(&account_id, "Unkeyed", "b", JobPermit::detached()).await,
            Err(StorageError::Unsupported(ref m)) if m.contains("V006__backup_catalog_index.cql")
        ),
        "a backup operation is refused until the migration is recorded"
    );

    // The cause gone, the next run applies the file again and lands.
    engine
        .session()
        .query_with_values(
            format!("DELETE FROM {catalog}.tables WHERE account_id = ? AND table_name = 'Unkeyed'"),
            cdrs_tokio::query_values!(account_id.as_str()),
        )
        .await
        .expect("remove the row");
    extenddb_storage_cassandra::migrations::run_catalog_migrations(&engine.session_arc(), &catalog)
        .await
        .expect("the second run succeeds");
    assert_eq!(
        catalog_state(&engine, &catalog).await,
        (
            Some(extenddb_storage_cassandra::CATALOG_VERSION.to_owned()),
            vec![1, 2, 3, 4, 5, 6]
        )
    );
    engine
        .require_served_catalog()
        .await
        .expect("serve accepts the migrated catalog");
    assert!(
        uuid::Uuid::parse_str(&engine.catalog_id().await.expect("catalog id")).is_ok(),
        "the step wrote the catalog id"
    );
}

/// `read_store_ids`: the distinct ids of the rows for a root, of every
/// status, scoped to the kind and the root, read from the status partitions.
#[tokio::test]
async fn test_read_store_ids_lists_the_distinct_ids_of_a_root() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let root = store.store_root.clone();
    let ids = |kind: StoreKind| {
        let engine = &engine;
        let root = root.clone();
        async move { engine.read_store_ids(kind, &root).await.expect("ids") }
    };
    assert!(ids(StoreKind::Filesystem).await.is_empty());
    let first = index_row(&account_id, "Music", "a", &store);
    let _c1 = engine.claim_backup(&first).await.expect("claim");
    let second = index_row(&account_id, "Music", "b", &store);
    let c2 = engine.claim_backup(&second).await.expect("claim");
    assert_eq!(
        c2.set_status(
            JobStatus::Creating,
            JobStatus::Failed,
            &StatusFields::default()
        )
        .await
        .expect("fail"),
        WriteOutcome::Matched
    );
    assert_eq!(
        ids(StoreKind::Filesystem).await,
        ["store-1"],
        "two rows of one store, of two statuses, are one id"
    );
    let mut other_store = store.clone();
    other_store.store_id = "store-2".to_owned();
    let other = index_row(&account_id, "Songs", "c", &other_store);
    let _c3 = engine.claim_backup(&other).await.expect("claim");
    assert_eq!(ids(StoreKind::Filesystem).await, ["store-1", "store-2"]);
    assert!(
        ids(StoreKind::S3).await.is_empty(),
        "another kind at the same root is another store"
    );
    assert!(
        engine
            .read_store_ids(StoreKind::Filesystem, "/elsewhere")
            .await
            .expect("ids")
            .is_empty()
    );
}

/// `remove_dead_creating_backup_row`: a CREATING row whose heartbeat is older
/// than the window is removed outright, its status-index entry with it; one
/// with a live owner, one that is not CREATING, and one that is gone are left,
/// answering `false`.
#[tokio::test]
async fn test_a_dead_creating_row_is_removed_and_a_live_or_moved_one_is_kept() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let stale = Duration::from_secs(300);
    let ten_minutes_ago = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        - 600_000;
    let age = |backup_arn: String, generation: u64| {
        let engine = &engine;
        let account_id = account_id.clone();
        async move {
            apply(
                engine,
                &format!(
                    "UPDATE {}.backups_by_arn SET heartbeat_at = ? \
                     WHERE account_id = ? AND backup_arn = ? IF owner_generation = ?",
                    engine.catalog_keyspace()
                ),
                cdrs_tokio::query_values!(
                    ten_minutes_ago,
                    account_id.as_str(),
                    backup_arn.as_str(),
                    i64::try_from(generation).unwrap()
                ),
            )
            .await;
        }
    };
    let creating = index_row(&account_id, "Music", "dead-a", &store);
    let key = creating.key();
    let claim = engine.claim_backup(&creating).await.expect("claim");
    assert!(
        !engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap(),
        "a live owner keeps its row"
    );
    age(creating.backup_arn.clone(), claim.generation()).await;
    assert!(
        engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap()
    );
    assert!(
        engine
            .read_backup_row(&arn_of(&creating))
            .await
            .unwrap()
            .is_none()
    );
    let creating_rows: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Creating), &store)
        .try_collect()
        .await
        .unwrap();
    assert!(
        creating_rows.is_empty(),
        "the status index entry went with the row: {creating_rows:?}"
    );
    assert!(
        !engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap(),
        "gone"
    );
    drop(claim);

    let available = index_row(&account_id, "Music", "dead-b", &store);
    let claim = engine.claim_backup(&available).await.expect("claim");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .expect("available"),
        WriteOutcome::Matched
    );
    age(available.backup_arn.clone(), claim.generation()).await;
    assert!(
        !engine
            .remove_dead_creating_backup_row(&available.key(), stale)
            .await
            .unwrap(),
        "only a CREATING row"
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&available))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available
    );
    drop(claim);
}

/// The maintenance claim: taken once, refused to a second holder naming the
/// first, heartbeated on the coordinator's clock, released; a holder whose
/// heartbeat is older than the stale window is dead, so its claim is not
/// reported held and is taken over with a lightweight transaction on its
/// generation, and its handle finds the claim lost. The catalog is shared by
/// this binary's tests, so the test releases what it takes.
#[tokio::test]
async fn test_the_maintenance_claim_excludes_a_second_holder_and_expires_when_stale() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let stale = Duration::from_secs(300);
    let check = || engine.check_maintenance_claim(stale);
    // Another test of this binary never holds the claim; a stale row an
    // interrupted run left is taken over below like any dead holder's.
    let first = engine
        .maintenance_claim("purge on host-a", stale)
        .await
        .expect("claim");
    assert_ne!(first.generation(), UNOWNED_GENERATION);
    assert!(check().await.expect("check"), "a live claim is held");
    match engine.maintenance_claim("purge on host-b", stale).await {
        Err(StorageError::AlreadyExists(m)) => assert!(m.contains("purge on host-a"), "{m}"),
        Err(other) => panic!("a second holder is refused with AlreadyExists: {other:?}"),
        Ok(_) => panic!("a second holder is refused"),
    }
    assert_eq!(
        first.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    first.release().await.expect("release");
    assert!(!check().await.expect("check"), "released");
    first.release().await.expect("a second release is a no-op");

    // A dead holder: its heartbeat aged past the window.
    let dead = engine
        .maintenance_claim("purge on host-c", stale)
        .await
        .expect("claim");
    let aged = format!(
        "UPDATE {}.backup_maintenance SET heartbeat_at = ? WHERE claim = 'claim' \
         IF owner_generation = ?",
        engine.catalog_keyspace()
    );
    let ten_minutes_ago = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        - 600_000;
    apply(
        &engine,
        &aged,
        cdrs_tokio::query_values!(ten_minutes_ago, i64::try_from(dead.generation()).unwrap()),
    )
    .await;
    assert!(!check().await.expect("check"), "a stale claim is not held");
    let taker = engine
        .maintenance_claim("purge on host-d", stale)
        .await
        .expect("a dead holder's claim is taken over");
    assert_ne!(taker.generation(), dead.generation());
    assert!(check().await.expect("check"));
    assert_eq!(
        dead.heartbeat().await.expect("heartbeat"),
        Heartbeat::Lost,
        "the dead holder's handle finds the claim lost"
    );
    dead.release()
        .await
        .expect("a release by a lost holder changes nothing");
    assert!(check().await.expect("check"), "the taker still holds it");
    taker.release().await.expect("release");
    assert!(!check().await.expect("check"));
}
