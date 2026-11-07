// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for Cassandra backup and restore operations.
//!
//! Restores go through the same steps the engine runs
//! (`backup_restore_source`, `ensure_restorable`, `restore_input`, then
//! `restore_table_from_backup`), because the backend now receives the
//! resolved `CreateTableInput` rather than the backup ARN alone.

use std::collections::{BTreeMap, BTreeSet};
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
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, RestoreKind,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity, StoreKind,
    TimeBounds, UNOWNED_GENERATION, WriteOutcome, backup_in_use_message,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId};
use extenddb_storage::{
    BackupCatalog, BulkLoadFile, DataEngine, Finished, JobPermit, LegacyBackupReader,
    RestoreEngine, RestoreRequest, RestoreTableOverrides, TableEngine,
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
        matches!(in_use, StorageError::BackupInUse(ref m) if m.contains(&backup_arn)),
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
        matches!(second, Err(StorageError::BackupInUse(ref m)) if m.contains(&b1)),
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
            .fail_restore_job(&key, job.owner_generation, job.heartbeat_at, "owner dead")
            .await
            .expect("fail"),
        "the record was RUNNING"
    );
    assert!(
        !engine
            .fail_restore_job(&key, job.owner_generation, job.heartbeat_at, "owner dead")
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

/// An engine on its own keyspace prefix (so its catalog scans see no other
/// test's tables) with one account. With `before_migration_6` the catalog
/// has run catalog migrations 1 to 5 only, as a catalog not yet migrated by
/// `extenddb migrate --yes`, and the account is not provisioned (see
/// [`provision_account`]); otherwise every migration has run and the account
/// is provisioned.
async fn isolated_engine(
    tag: &str,
    before_migration_6: bool,
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
    let through = if before_migration_6 {
        MigrationId::DropContinuousBackups
    } else {
        MigrationId::LATEST
    };
    extenddb_storage_cassandra::migrations::migrate_catalog_keyspace_through(
        session.clone(),
        &config.keyspace_prefix,
        "test",
        std::time::Duration::from_secs(120),
        through,
    )
    .await
    .unwrap();
    if !before_migration_6 {
        provision_account(&engine, &account_id).await;
    }
    (engine, account_id, drop_keyspaces)
}

/// Run the account migrations on `account_id`'s keyspace, which exists, and
/// record the account in the catalog, as CreateAccount does. The account
/// run applies the catalog's pending migrations as well, so a test of a
/// catalog behind the binary provisions its account only once the catalog
/// is migrated.
async fn provision_account(engine: &CassandraEngine, account_id: &str) {
    let session = engine.session_arc();
    let catalog_ks = engine.catalog_keyspace();
    extenddb_storage_cassandra::migrations::migrate_account(
        session.clone(),
        &prefix_of(engine),
        &engine.account_keyspace(account_id),
        "test",
    )
    .await
    .unwrap();
    session
        .query_with_values(
            format!(
                "INSERT INTO {catalog_ks}.accounts (account_id, account_name, created_at) \
                 VALUES (?, ?, toTimestamp(now()))"
            ),
            cdrs_tokio::query_values!(account_id, "F4 Test"),
        )
        .await
        .unwrap();
}

/// The columns and tables catalog migration 6 adds are absent from
/// `keyspace`, and migration 1's restore registration columns are present.
async fn assert_catalog_lacks_migration_6(engine: &CassandraEngine, keyspace: &str) {
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
    for baseline in ["restore_source_backup_arn", "active_restores", "definition"] {
        assert!(columns.iter().any(|c| c == baseline), "{baseline} absent");
    }
    for added in [
        "restore_status",
        "restore_owner_generation",
        "restore_heartbeat_at",
        "store_kind",
        "source_table_details_json",
    ] {
        assert!(!columns.iter().any(|c| c == added), "{added} present");
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
    for added in ["backup_index_by_status", "backup_maintenance"] {
        assert!(!names.iter().any(|n| n == added), "{added} present");
    }
    assert!(
        engine.catalog_id().await.is_err(),
        "a catalog without migration 6 has no catalog id"
    );
}

/// `serve` does not start on a catalog without migration 6: the serving
/// registration reads the ledgers and refuses a catalog behind the binary,
/// naming the pending migration, and leaves no registration behind. Once
/// `migrate --yes` has applied it, the same deployment registers, the
/// catalog has its id, and backups work.
#[tokio::test]
async fn test_serve_refuses_a_catalog_without_migration_6_until_it_is_migrated() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("v5", true).await;
    let catalog = engine.catalog_keyspace();
    let prefix = prefix_of(&engine);
    assert_catalog_lacks_migration_6(&engine, &catalog).await;

    let refused = extenddb_storage_cassandra::migrations::register_server(
        engine.session_arc(),
        &prefix,
        "t",
        "test",
    )
    .await
    .err()
    .expect("serve refuses a catalog behind the binary");
    match &refused {
        MigrationError::Status(CatalogStatus::Behind { version, pending }) => {
            assert_eq!(*version, Some(MigrationId::DropContinuousBackups.number()));
            assert!(
                pending
                    .iter()
                    .any(|p| p.id == MigrationId::BackupCatalogIndex.number()),
                "{refused}"
            );
        }
        other => panic!("expected a Behind refusal, got {other}"),
    }
    assert!(
        refused.to_string().contains("backup_catalog_index"),
        "{refused}"
    );

    // Liveness: once migration 6 is applied, as `migrate --yes` applies it,
    // the deployment registers, the catalog has its id, and the same engine
    // backs up without a restart.
    extenddb_storage_cassandra::migrations::migrate_catalog_keyspace(
        engine.session_arc(),
        &prefix,
        "test",
        std::time::Duration::from_secs(120),
    )
    .await
    .expect("apply migration 6");
    provision_account(&engine, &account_id).await;
    assert!(
        uuid::Uuid::parse_str(&engine.catalog_id().await.expect("catalog id")).is_ok(),
        "the migration writes a catalog id"
    );
    let registration = extenddb_storage_cassandra::migrations::register_server(
        engine.session_arc(),
        &prefix,
        "t",
        "test",
    )
    .await
    .expect("serve accepts the migrated catalog");
    extenddb_storage::hooks::ServingRegistrar::release(&registration).await;
    let created = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "Migrated".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(&engine, &created, vec![item("a", None, "v")]).await;
    let backup = engine
        .create(
            &account_id,
            "Migrated",
            "after-migration-6",
            JobPermit::detached(),
        )
        .await
        .expect("CreateBackup after migration 6");
    engine
        .delete(&account_id, &backup.backup_arn)
        .await
        .expect("DeleteBackup after migration 6");
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

/// The names of the tables in `keyspace`, read from `system_schema`.
async fn table_names_in(engine: &CassandraEngine, keyspace: &str) -> BTreeSet<String> {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        "list tables",
    )
    .await
    .expect("list tables");
    rows.iter()
        .map(|row| {
            let name: Option<String> = row.get_by_name("table_name").ok().flatten();
            name.expect("table_name")
        })
        .collect()
}

/// The restore heartbeat runs while the target's data and index tables are
/// created, not only once the copy starts: with many indexes that DDL (each
/// statement waiting for schema agreement) can outlast the reconciler's
/// stale window on a slow cluster, and a heartbeat still at its insert value
/// would let the reconciler remove the live target.
///
/// The check is on order, not time, so it holds at any DDL speed: when the
/// heartbeat first moves past its insert value, not all of the target's
/// tables exist yet.
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

    // The source table's data and index tables share the account keyspace
    // with the target's; any table not among these is the target's.
    let account_keyspace = engine.account_keyspace(&account_id);
    let source_tables = std::sync::Arc::new(table_names_in(&engine, &account_keyspace).await);

    // Watch the target row while the restore request runs: whether it
    // appears, and how many target tables exist when its heartbeat first
    // moves past its insert value.
    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let engine = std::sync::Arc::clone(&engine);
        let account_id = account_id.clone();
        let account_keyspace = account_keyspace.clone();
        let source_tables = std::sync::Arc::clone(&source_tables);
        let watching = std::sync::Arc::clone(&watching);
        tokio::spawn(async move {
            let query = format!(
                "SELECT WRITETIME(restore_heartbeat_at) AS beat, WRITETIME(table_id) AS inserted \
                 FROM {}.tables WHERE account_id = ? AND table_name = ?",
                engine.catalog_keyspace()
            );
            let mut row_seen = false;
            let mut target_tables_at_first_beat = None;
            while watching.load(std::sync::atomic::Ordering::Acquire)
                && target_tables_at_first_beat.is_none()
            {
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
                    row_seen = true;
                    let beat: Option<i64> = row.get_by_name("beat").ok().flatten();
                    let inserted: Option<i64> = row.get_by_name("inserted").ok().flatten();
                    if let (Some(beat), Some(inserted)) = (beat, inserted)
                        && beat > inserted
                    {
                        let tables = table_names_in(&engine, &account_keyspace).await;
                        target_tables_at_first_beat =
                            Some(tables.difference(&source_tables).count());
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (row_seen, target_tables_at_first_beat)
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
    watching.store(false, std::sync::atomic::Ordering::Release);
    let (row_seen, target_tables_at_first_beat) = watcher.await.expect("watcher");
    assert_eq!(started.table_status, TableStatus::Creating);
    assert!(
        row_seen,
        "the target row was never observed before the restore returned"
    );
    // The restore returns once the target's DDL is done: its data table and
    // one table per index (5 GSIs + 5 LSIs).
    let target_table_count = table_names_in(&engine, &account_keyspace)
        .await
        .difference(&source_tables)
        .count();
    assert_eq!(
        target_table_count, 11,
        "the target should have a data table and 10 index tables"
    );
    let target_tables_at_first_beat = target_tables_at_first_beat
        .expect("the heartbeat was not refreshed before the restore returned");
    eprintln!(
        "heartbeat first refreshed when {target_tables_at_first_beat} of \
         {target_table_count} target tables existed"
    );
    assert!(
        target_tables_at_first_beat < target_table_count,
        "the heartbeat was first refreshed only after all {target_table_count} target \
         tables existed"
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

/// Delete `rows` from `backups_by_arn` and their entries from the status
/// index and the two listings, whatever their status, so a test that wrote
/// rows outside a claim leaves none in the shared catalog keyspace.
async fn remove_index_rows(engine: &CassandraEngine, rows: &[&BackupRow]) {
    let catalog = engine.catalog_keyspace();
    for row in rows {
        let created_at = (row.created_at * 1_000.0).round() as i64;
        for (cql, values) in [
            (
                format!(
                    "DELETE FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(row.account_id.as_str(), row.backup_arn.as_str()),
            ),
            (
                format!(
                    "DELETE FROM {catalog}.backups_by_account \
                     WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    created_at,
                    row.backup_arn.as_str()
                ),
            ),
            (
                format!(
                    "DELETE FROM {catalog}.backups_by_table WHERE account_id = ? \
                     AND table_name = ? AND created_at = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    row.table_name.as_str(),
                    created_at,
                    row.backup_arn.as_str()
                ),
            ),
        ] {
            engine
                .session()
                .query_with_values(&cql, values)
                .await
                .expect("remove a backup row");
        }
        for status in [
            BackupStatus::Creating,
            BackupStatus::Available,
            BackupStatus::Deleting,
            BackupStatus::Deleted,
            BackupStatus::Failed,
        ] {
            engine
                .session()
                .query_with_values(
                    &format!(
                        "DELETE FROM {catalog}.backup_index_by_status \
                         WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
                    ),
                    cdrs_tokio::query_values!(
                        row.store.store_root.as_str(),
                        status.as_str(),
                        row.backup_arn.as_str()
                    ),
                )
                .await
                .expect("remove a status entry");
        }
    }
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
        source_table_details_json: None,
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
        engine.forget_available_backup_row(&key).await.unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::InUse("DelTarget".to_owned()),
        "the outright removal keeps the in-use rule"
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
    assert_eq!(
        engine.forget_available_backup_row(&key).await.unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::Status(BackupStatus::Deleting),
        "only an AVAILABLE row is removed outright"
    );
    // An AVAILABLE row goes outright, read and listed nowhere, with no
    // tombstone; a second removal finds no row.
    let mut adopted = index_row(&account_id, "Music", "adopted", &store);
    adopted.status = BackupStatus::Available;
    assert!(engine.insert_backup_row(&adopted).await.expect("insert"));
    assert_eq!(
        engine
            .forget_available_backup_row(&adopted.key())
            .await
            .unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::Removed
    );
    assert!(
        engine
            .read_backup_row(&arn_of(&adopted))
            .await
            .unwrap()
            .is_none()
    );
    for status in [None, Some(BackupStatus::Available)] {
        let listed: Vec<BackupRow> = engine
            .list_backup_rows(status, &adopted.store)
            .try_collect()
            .await
            .unwrap();
        assert!(
            !listed.iter().any(|r| r.backup_arn == adopted.backup_arn),
            "{status:?}"
        );
    }
    let by_account: Vec<BackupRow> = engine
        .list_account_backup_rows(&adopted.account_id, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert!(
        !by_account
            .iter()
            .any(|r| r.backup_arn == adopted.backup_arn)
    );
    assert_eq!(
        raw_index_entries(&engine, &adopted).await,
        0,
        "the list and status entries went with the row"
    );
    assert!(matches!(
        engine.forget_available_backup_row(&adopted.key()).await,
        Err(StorageError::Validation(m)) if m.contains("Backup not found")
    ));
    assert!(
        !engine
            .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
            .await
            .unwrap(),
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
    // A row another writer moved to another store after the caller read it
    // is left by a removal that names the store read.
    let moved_to = StoreIdentity {
        store_kind: row.store.store_kind,
        store_root: format!("{}-moved", row.store.store_root),
        store_id: "moved-1".to_owned(),
    };
    assert_eq!(
        engine
            .rewrite_store(
                row.store.store_kind,
                &row.store.store_root,
                std::slice::from_ref(&key),
                &moved_to
            )
            .await
            .unwrap(),
        1
    );
    assert!(
        !engine
            .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
            .await
            .unwrap(),
        "a row moved to another store is left"
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .map(|r| (r.status, r.store)),
        Some((BackupStatus::Deleted, moved_to.clone()))
    );
    // The same root under the other kind is another store: a removal that
    // names the kind it read leaves the row.
    let other_kind = StoreIdentity {
        store_kind: if moved_to.store_kind == StoreKind::S3 {
            StoreKind::Filesystem
        } else {
            StoreKind::S3
        },
        ..moved_to.clone()
    };
    assert_eq!(
        engine
            .rewrite_store(
                moved_to.store_kind,
                &moved_to.store_root,
                std::slice::from_ref(&key),
                &other_kind
            )
            .await
            .unwrap(),
        1
    );
    assert!(
        !engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap(),
        "a row moved to the other kind is left"
    );
    let moved_to = other_kind;
    assert!(
        engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap()
    );
    assert!(
        !engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap()
    );
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

/// A catalog at the shape before migration 6 holding a finished restore and a
/// restore the stop interrupted (its target CREATING) migrates with the
/// registrations backfilled: the finished one DONE, the interrupted one
/// RUNNING with the heartbeat at the epoch and the unowned generation, so the
/// reconciler's first pass fails it and removes its target. A second run of
/// the migration keeps the catalog id.
#[tokio::test]
async fn test_migration_6_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one()
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

    // Back to the shape before migration 6: the registrations as the
    // previous release wrote them (no job record), the interrupted target
    // CREATING, and the migration forgotten.
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
    let prefix = prefix_of(&engine);
    forget_catalog_migration(&engine, &prefix, MigrationId::BackupCatalogIndex).await;
    let report = extenddb_storage_cassandra::migrations::migrate_catalog_keyspace(
        engine.session_arc(),
        &prefix,
        "test",
        Duration::from_secs(120),
    )
    .await
    .expect("migrate again");
    let applied: Vec<(u32, Vec<String>)> = report
        .applied
        .iter()
        .map(|step| (step.id, step.notes.clone()))
        .collect();
    assert_eq!(
        applied,
        [(
            MigrationId::BackupCatalogIndex.number(),
            vec![
                "backfilled 2 restore registration(s) as legacy restores, 1 of them RUNNING \
                 with a dead owner for the reconciler to remove"
                    .to_owned()
            ]
        )],
        "the catalog id is kept"
    );

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
            .fail_restore_job(
                &dead[0].target,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
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
        !engine
            .fail_restore_job(
                &key,
                u64::try_from(generation_after.unwrap()).unwrap(),
                extenddb_storage_cassandra::cassandra_util::millis_to_seconds_f64(
                    heartbeat.expect("a heartbeat")
                ),
                "late"
            )
            .await
            .unwrap(),
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
        removed: false,
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
    let dead = dead_jobs_of(&engine, &key, Duration::from_secs(60)).await;
    assert_eq!(dead.len(), 1, "the orphaned record is listed: {dead:?}");
    assert!(
        engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
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
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "again"
            )
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
    let dead = dead_jobs_of(&engine, &key, Duration::from_secs(60)).await;
    assert_eq!(dead.len(), 1, "the orphaned record is listed: {dead:?}");
    assert!(
        engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
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

/// How many of `row`'s three index entries (`backups_by_account`,
/// `backups_by_table`, `backup_index_by_status` under its status) are in
/// the tables, read directly: every listing re-reads the authoritative row
/// and skips an entry whose row is gone, so only a direct read sees one.
async fn raw_index_entries(engine: &CassandraEngine, row: &BackupRow) -> usize {
    let catalog = engine.catalog_keyspace();
    let created_at = (row.created_at * 1_000.0).round() as i64;
    let mut found = 0;
    for (cql, values) in [
        (
            format!(
                "SELECT backup_arn FROM {catalog}.backups_by_account \
                 WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(row.account_id.as_str(), created_at, row.backup_arn.as_str()),
        ),
        (
            format!(
                "SELECT backup_arn FROM {catalog}.backups_by_table WHERE account_id = ? \
                 AND table_name = ? AND created_at = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                row.account_id.as_str(),
                row.table_name.as_str(),
                created_at,
                row.backup_arn.as_str()
            ),
        ),
        (
            format!(
                "SELECT backup_arn FROM {catalog}.backup_index_by_status \
                 WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                row.store.store_root.as_str(),
                row.status.as_str(),
                row.backup_arn.as_str()
            ),
        ),
    ] {
        let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
            &engine.session_arc(),
            &cql,
            values,
            "index entry",
        )
        .await
        .expect("read an index entry");
        found += rows.len();
    }
    found
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
        matches!(refused, StorageError::BackupInUse(ref m) if m.contains(&backup_arn)),
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
    assert!(
        engine
            .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
            .await
            .unwrap()
    );
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

/// The keyspace prefix `engine` was built with.
fn prefix_of(engine: &CassandraEngine) -> String {
    engine
        .catalog_keyspace()
        .strip_suffix("_catalog")
        .expect("catalog keyspace name")
        .to_owned()
}

/// Remove migration `id`'s ledger row from the catalog keyspace under the
/// migration lock, as `migrate --forget` does, so the next run applies it
/// again.
async fn forget_catalog_migration(engine: &CassandraEngine, prefix: &str, id: MigrationId) {
    use extenddb_storage_cassandra::migrations::{CassandraMigrations, LeaseTimes, MigrationLock};
    let lock = MigrationLock::acquire(
        engine.session_arc(),
        &engine.catalog_keyspace(),
        LeaseTimes::DEFAULT,
        Duration::from_secs(120),
    )
    .await
    .expect("take the lock");
    let forgotten = {
        let migrations = CassandraMigrations::locked_catalog_only(&lock, prefix);
        extenddb_storage::migrations::Runner::new(&migrations, "test")
            .forget(id.number())
            .await
    };
    lock.release().await;
    forgotten.expect("forget the migration");
}

/// A catalog at migration 5 whose migration 6 fails in its backfill (a
/// `tables` row carrying a restore registration and no `table_id`, which the
/// backfill cannot key) stays behind with migration 6 unrecorded, so `serve`
/// refuses it and the next `migrate --yes` runs the body again; once the
/// cause is gone the second run records it and the catalog is current. A
/// fresh catalog is current at once.
#[tokio::test]
async fn test_a_migration_6_that_fails_in_its_backfill_stays_pending_and_resumes() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let current = CatalogStatus::Current {
        version: MigrationId::LATEST.number(),
    };
    let (fresh, _account, _drop_fresh) = isolated_engine("mv", false).await;
    assert_eq!(
        extenddb_storage_cassandra::migrations::catalog_status(
            fresh.session_arc(),
            &prefix_of(&fresh)
        )
        .await
        .expect("status"),
        current,
        "a fresh catalog has every migration recorded"
    );

    let (engine, account_id, _drop_keyspaces) = isolated_engine("mf", true).await;
    let catalog = engine.catalog_keyspace();
    let prefix = prefix_of(&engine);
    // Behind at migration 5 with migration 6 pending (and 7, which does not
    // apply on Cassandra and is recorded after it).
    let behind_at_5 = |status: CatalogStatus| match status {
        CatalogStatus::Behind { version, pending } => {
            version == Some(MigrationId::DropContinuousBackups.number())
                && pending
                    .iter()
                    .any(|p| p.id == MigrationId::BackupCatalogIndex.number())
                && pending.iter().all(|p| {
                    p.id == MigrationId::BackupCatalogIndex.number()
                        || p.id == MigrationId::RestoreProgress.number()
                })
        }
        _ => false,
    };
    let status = || async {
        extenddb_storage_cassandra::migrations::catalog_status(engine.session_arc(), &prefix)
            .await
            .expect("status")
    };
    assert!(behind_at_5(status().await));
    // A registration the backfill cannot key.
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
    let migrate = || {
        extenddb_storage_cassandra::migrations::migrate_catalog_keyspace(
            engine.session_arc(),
            &prefix,
            "test",
            Duration::from_secs(120),
        )
    };
    let failed = migrate()
        .await
        .expect_err("the backfill fails on the unkeyed row");
    assert!(failed.to_string().contains("table_id"), "{failed}");
    assert!(
        behind_at_5(status().await),
        "a failed body leaves migration 6 unrecorded"
    );
    assert!(
        matches!(
            extenddb_storage_cassandra::migrations::register_server(
                engine.session_arc(),
                &prefix,
                "t",
                "test"
            )
            .await,
            Err(MigrationError::Status(CatalogStatus::Behind { .. }))
        ),
        "serve refuses the half-migrated catalog"
    );

    // The cause gone, the next run applies the body again and records it.
    engine
        .session()
        .query_with_values(
            format!("DELETE FROM {catalog}.tables WHERE account_id = ? AND table_name = 'Unkeyed'"),
            cdrs_tokio::query_values!(account_id.as_str()),
        )
        .await
        .expect("remove the row");
    migrate().await.expect("the second run succeeds");
    assert_eq!(status().await, current);
    let registration = extenddb_storage_cassandra::migrations::register_server(
        engine.session_arc(),
        &prefix,
        "t",
        "test",
    )
    .await
    .expect("serve accepts the migrated catalog");
    extenddb_storage::hooks::ServingRegistrar::release(&registration).await;
    assert!(
        uuid::Uuid::parse_str(&engine.catalog_id().await.expect("catalog id")).is_ok(),
        "the body wrote the catalog id"
    );
}

/// Items in the data table of a restore target, whatever its status.
#[cfg(feature = "test-hooks")]
async fn target_item_count(engine: &CassandraEngine, account_id: &str, table_id: &str) -> i64 {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT COUNT(*) AS n FROM {}.items_{}",
            engine.account_keyspace(account_id),
            table_id.replace('-', "_")
        ),
        cdrs_tokio::query_values!(),
        "target count",
    )
    .await
    .expect("count the target");
    rows.first()
        .and_then(|row| row.get_by_name("n").ok().flatten())
        .unwrap_or(0)
}

/// Start a restore of `backup_arn` whose jobs heartbeat every 100 ms into
/// `target`, and return its description once the copy has written something
/// but is far from done.
#[cfg(feature = "test-hooks")]
async fn restore_mid_copy(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    target: &str,
    items: i64,
) -> TableDescription {
    extenddb_storage_cassandra::test_hooks::set_heartbeat_interval(
        account_id,
        Duration::from_millis(100),
    );
    let desc = restore(
        engine,
        account_id,
        backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let copied = target_item_count(engine, account_id, &desc.table_id).await;
        if copied > 0 {
            assert!(
                copied < items / 2,
                "the copy is already {copied} of {items} items in"
            );
            break;
        }
        assert!(Instant::now() < deadline, "the copy did not start");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    desc
}

/// Wait until the target's item count stops growing, and return it. Panics
/// if it is still growing after 30 s.
#[cfg(feature = "test-hooks")]
async fn settled_item_count(engine: &CassandraEngine, account_id: &str, table_id: &str) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = target_item_count(engine, account_id, table_id).await;
    loop {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let now = target_item_count(engine, account_id, table_id).await;
        if now == last {
            return now;
        }
        assert!(Instant::now() < deadline, "the copy keeps writing");
        last = now;
    }
}

#[cfg(feature = "test-hooks")]
/// A FAILED write on a running restore's record stops the copy at its next
/// heartbeat, as on the other three backends: the heartbeat requires
/// `restore_status = 'RUNNING'`, so it is not applied, the copy stops within
/// a bucket, the target is left CREATING under the FAILED record with fewer
/// items than the backup, the copy's own cleanup touches neither, and the
/// FAILED-target duty then lists the pair and removes the target. The FAILED
/// is written under the record's own generation (the shape a job's failure
/// path writes, and what the reviewer wrote by hand), so the status term is
/// the one condition that stops the heartbeat; without it the heartbeat keeps
/// applying over the FAILED record and the copy writes every item.
#[tokio::test]
async fn test_a_failed_record_stops_the_restore_copy_at_its_next_heartbeat() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 20_000;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "FcSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let target = restore_mid_copy(
        &engine,
        &account_id,
        &backup_arn,
        "FcTarget",
        i64::try_from(ITEMS).unwrap(),
    )
    .await;
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "FcTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    let (status, _, generation, _, _) = job_record(&engine, &account_id, "FcTarget").await;
    assert_eq!(status.as_deref(), Some("RUNNING"));
    // FAILED under the record's own generation: the status alone changes.
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_status = 'FAILED', restore_failure_reason = 'owner dead', \
             restore_completed_at = toTimestamp(now()) \
             WHERE account_id = ? AND table_name = ? IF table_id = ? AND restore_status = 'RUNNING'",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            account_id.as_str(),
            "FcTarget",
            target.table_id.as_str()
        ),
    )
    .await;
    let copied = settled_item_count(&engine, &account_id, &target.table_id).await;
    assert!(
        copied < i64::try_from(ITEMS).unwrap(),
        "the copy stopped short: {copied} of {ITEMS} items"
    );
    let described = describe(&engine, &account_id, "FcTarget").await;
    assert_eq!(
        described.table_status,
        TableStatus::Creating,
        "the target is left to the reconciler, not flipped ACTIVE and not removed by the copy"
    );
    let (status, _, after, _, completed) = job_record(&engine, &account_id, "FcTarget").await;
    assert_eq!(
        status.as_deref(),
        Some("FAILED"),
        "the record is left as written"
    );
    assert_eq!(
        after, generation,
        "the stopped copy writes nothing on its row"
    );
    assert!(completed.is_some());

    // The FAILED-target duty: listed once the record is stale, removed.
    let dead = dead_jobs_of(&engine, &key, Duration::ZERO).await;
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].status, JobStatus::Failed);
    assert!(engine.remove_restore_target(&key).await.expect("remove"));
    assert!(
        matches!(
            engine
                .describe_table(
                    &account_id,
                    extenddb_core::types::DescribeTableInput {
                        table_name: "FcTarget".to_owned()
                    }
                )
                .await,
            Err(StorageError::TableNotFound(_))
        ),
        "the target is gone"
    );
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the backup is free once the target is gone");
}

#[cfg(feature = "test-hooks")]
/// A target claimed to DELETING mid-copy (the removal's first write) stops
/// the copy at its next heartbeat: the heartbeat requires `table_status =
/// 'CREATING'`, so it is not applied, the copy stops within a bucket and
/// writes nothing more into a table whose owner is dropping it, and the copy's
/// own cleanup touches neither the row nor the data. Without the condition
/// the heartbeat keeps applying and the copy writes every item.
#[tokio::test]
async fn test_a_target_claimed_to_deleting_stops_the_restore_copy_at_its_next_heartbeat() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 20_000;
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "DcSource", ITEMS).await;
    let account_id = account.account_id.clone();
    let target = restore_mid_copy(
        &engine,
        &account_id,
        &backup_arn,
        "DcTarget",
        i64::try_from(ITEMS).unwrap(),
    )
    .await;
    let (status, _, generation, _, _) = job_record(&engine, &account_id, "DcTarget").await;
    assert_eq!(status.as_deref(), Some("RUNNING"));
    // The removal's claim: CREATING to DELETING, as `remove_restore_target`'s
    // first write makes it.
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET table_status = 'DELETING' \
             WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "DcTarget", target.table_id.as_str()),
    )
    .await;
    let copied = settled_item_count(&engine, &account_id, &target.table_id).await;
    assert!(
        copied < i64::try_from(ITEMS).unwrap(),
        "the copy stopped short: {copied} of {ITEMS} items"
    );
    let (status, _, after, _, _) = job_record(&engine, &account_id, "DcTarget").await;
    assert_eq!(
        status.as_deref(),
        Some("RUNNING"),
        "the stopped copy writes nothing on a row it no longer owns"
    );
    assert_eq!(after, generation);
    let (table_status,): (Option<String>,) = {
        use cdrs_tokio::types::IntoRustByName;
        let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
            &engine.session_arc(),
            &format!(
                "SELECT table_status FROM {}.tables WHERE account_id = ? AND table_name = ?",
                engine.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(account_id.as_str(), "DcTarget"),
            "table status",
        )
        .await
        .expect("read");
        (rows
            .first()
            .and_then(|r| r.get_by_name("table_status").ok().flatten()),)
    };
    assert_eq!(
        table_status.as_deref(),
        Some("DELETING"),
        "the removal's claim stands"
    );

    // The removal resumes from the drop (the state the FAILED-target duty
    // lists when the record is FAILED); here the record is RUNNING, so finish
    // the removal by hand: mark the record FAILED under its claim, then let
    // `remove_restore_target` resume from the drop.
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "DcTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_status = 'FAILED', \
             restore_completed_at = toTimestamp(now()) \
             WHERE account_id = ? AND table_name = ? IF table_id = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "DcTarget", target.table_id.as_str()),
    )
    .await;
    assert!(engine.remove_restore_target(&key).await.expect("remove"));
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the backup is free once the target is gone");
}

#[cfg(feature = "test-hooks")]
/// The abandoned-create fence is conditioned on the heartbeat the scan read:
/// a create whose heartbeat lands between the scan and the fence keeps its
/// CREATING row under its own generation, and is not reported. The scan is
/// paused after the read by a hook; the create's heartbeat lands in the gap;
/// the fence matches nothing. Without the `heartbeat_at = ?` condition the
/// fence would move a row whose owner just proved alive to DELETING.
#[tokio::test]
async fn test_a_create_that_heartbeats_between_the_scan_and_the_fence_keeps_its_row() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 100;
    const LIVE_GENERATION: u64 = 4343;
    let engine = std::sync::Arc::new(setup_engine().await);
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "HfSource", ITEMS).await;
    let account_id = account.account_id.clone();
    // A CREATING row whose heartbeat is two minutes old when the scan reads it.
    let catalog = engine.catalog_keyspace();
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET backup_status = 'CREATING', item_count = 0, \
             owner_generation = ?, heartbeat_at = ? WHERE account_id = ? AND backup_arn = ? \
             IF backup_status = 'AVAILABLE'"
        ),
        cdrs_tokio::query_values!(
            i64::try_from(LIVE_GENERATION).unwrap(),
            stale,
            account_id.as_str(),
            backup_arn.as_str()
        ),
    )
    .await;
    let pause = extenddb_storage_cassandra::test_hooks::pause_before_abandoned_fence(&backup_arn);
    let scan = {
        let engine = std::sync::Arc::clone(&engine);
        tokio::spawn(async move {
            engine
                .abandoned_creating_backups(Duration::from_secs(60))
                .await
        })
    };
    pause.reached().await;
    // The create's heartbeat lands after the scan read the row.
    assert!(
        extenddb_storage_cassandra::heartbeat_backup_row(
            &engine,
            &account_id,
            &backup_arn,
            LIVE_GENERATION
        )
        .await
        .unwrap()
    );
    pause.release();
    let reported = scan.await.unwrap().expect("scan");
    assert!(
        !reported.iter().any(|a| a.backup_arn == backup_arn),
        "a row whose heartbeat landed after the scan is not reported: {reported:?}"
    );
    assert_eq!(
        backup_status_of(&engine, &account_id, &backup_arn)
            .await
            .as_deref(),
        Some("CREATING"),
        "the fence matched nothing"
    );
    assert!(
        extenddb_storage_cassandra::heartbeat_backup_row(
            &engine,
            &account_id,
            &backup_arn,
            LIVE_GENERATION
        )
        .await
        .unwrap(),
        "the row is still the create's: its heartbeat applies"
    );
    // The same row, stale again with no heartbeat in the gap: fenced and
    // reported, and the create's heartbeat is lost.
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET heartbeat_at = ? \
             WHERE account_id = ? AND backup_arn = ? IF backup_status = 'CREATING'"
        ),
        cdrs_tokio::query_values!(stale, account_id.as_str(), backup_arn.as_str()),
    )
    .await;
    let reported = engine
        .abandoned_creating_backups(Duration::from_secs(60))
        .await
        .expect("scan");
    assert!(reported.iter().any(|a| a.backup_arn == backup_arn));
    assert!(
        !extenddb_storage_cassandra::heartbeat_backup_row(
            &engine,
            &account_id,
            &backup_arn,
            LIVE_GENERATION
        )
        .await
        .unwrap()
    );
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the reconciler's delete finishes the fenced row");
}

/// A stale CREATING catalog index row (`store_kind` set: a store-backed
/// backup whose writer died, which a later release's reconciler resolves) is
/// skipped by the legacy scan: it is not reported, not fenced to DELETING,
/// and keeps its generation, so the legacy reader's `delete` never reaches a
/// row of the other format. Without the skip the scan would fence it and hand
/// it to `delete`.
#[tokio::test]
async fn test_a_stale_creating_index_row_is_skipped_by_the_abandoned_create_scan() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = index_row(&account_id, "Music", "nightly", &store);
    let claim = engine.claim_backup(&row).await.expect("claim");
    let generation = claim.generation();
    // The writer died: its heartbeat is two minutes old.
    let catalog = engine.catalog_keyspace();
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET heartbeat_at = ? \
             WHERE account_id = ? AND backup_arn = ? IF owner_generation = ?"
        ),
        cdrs_tokio::query_values!(
            stale,
            account_id.as_str(),
            row.backup_arn.as_str(),
            i64::try_from(generation).unwrap()
        ),
    )
    .await;
    let reported = engine
        .abandoned_creating_backups(Duration::from_secs(60))
        .await
        .expect("scan");
    assert!(
        !reported.iter().any(|a| a.backup_arn == row.backup_arn),
        "an index row is not the legacy scan's: {reported:?}"
    );
    let stored = engine
        .read_backup_row(&arn_of(&row))
        .await
        .expect("read")
        .expect("the row is still there");
    assert_eq!(
        stored.status,
        BackupStatus::Creating,
        "not fenced to DELETING"
    );
    assert_eq!(
        stored.owner_generation, generation,
        "the writer's claim stands"
    );
    assert!(
        matches!(
            engine.delete(&account_id, &row.backup_arn).await,
            Err(StorageError::Validation(ref m)) if m.contains("Backup not found")
        ),
        "the legacy reader does not see an index row"
    );
    assert!(
        engine
            .remove_backup_row(&row.key(), row.store.store_kind, &row.store.store_root)
            .await
            .is_ok()
    );
}

/// `fail_restore_job` is conditioned on the `owner_generation` and
/// `restore_heartbeat_at` the listing read: a heartbeat that lands between
/// the listing and the write keeps the record its owner's, and with the
/// listed values current the write lands under a fresh generation.
#[tokio::test]
async fn test_the_failed_write_matches_only_the_listed_generation_and_heartbeat() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _source, backup_arn) = backed_up_plain_table(&engine, "FwSource", 50).await;
    let account_id = account.account_id.clone();
    let target = restore(
        &engine,
        &account_id,
        &backup_arn,
        "FwTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    wait_active(&engine, &account_id, "FwTarget", Duration::from_secs(180)).await;
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: "FwTarget".to_owned(),
        table_id: target.table_id.clone(),
    };
    let catalog = engine.catalog_keyspace();
    let crash = |heartbeat: i64| {
        let engine = &engine;
        let catalog = &catalog;
        let account_id = &account_id;
        let table_id = &target.table_id;
        async move {
            apply(
                engine,
                &format!(
                    "UPDATE {catalog}.tables SET table_status = 'CREATING', \
                     restore_status = 'RUNNING', restore_completed_at = null, \
                     restore_heartbeat_at = ? WHERE account_id = ? AND table_name = ? \
                     IF table_id = ?"
                ),
                cdrs_tokio::query_values!(
                    heartbeat,
                    account_id.as_str(),
                    "FwTarget",
                    table_id.as_str()
                ),
            )
            .await;
        }
    };
    let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 600_000;
    crash(stale).await;
    let dead = dead_jobs_of(&engine, &key, Duration::from_secs(300)).await;
    assert_eq!(dead.len(), 1, "{dead:?}");
    let (_, _, generation, _, _) = job_record(&engine, &account_id, "FwTarget").await;
    assert_eq!(
        i64::try_from(dead[0].owner_generation).unwrap(),
        generation.expect("generation")
    );

    // The owner's heartbeat lands after the listing.
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.tables SET restore_heartbeat_at = toTimestamp(now()) \
             WHERE account_id = ? AND table_name = ? IF table_id = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), "FwTarget", target.table_id.as_str()),
    )
    .await;
    assert!(
        !engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail"),
        "a heartbeat after the listing keeps the record its owner's"
    );
    let (status, _, after, _, _) = job_record(&engine, &account_id, "FwTarget").await;
    assert_eq!(status.as_deref(), Some("RUNNING"));
    assert_eq!(after, generation, "the generation is untouched");
    assert!(
        dead_jobs_of(&engine, &key, Duration::from_secs(300))
            .await
            .is_empty()
    );

    // Listed values that are still current: the write lands, under a fresh
    // generation.
    crash(stale).await;
    let dead = dead_jobs_of(&engine, &key, Duration::from_secs(300)).await;
    assert_eq!(dead.len(), 1);
    assert!(
        engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail")
    );
    let (status, _, fenced, _, completed) = job_record(&engine, &account_id, "FwTarget").await;
    assert_eq!(status.as_deref(), Some("FAILED"));
    assert_ne!(
        fenced, generation,
        "the FAILED write carries a fresh generation"
    );
    assert!(completed.is_some());
    assert!(engine.remove_restore_target(&key).await.expect("remove"));
    engine
        .delete(&account_id, &backup_arn)
        .await
        .expect("the backup is free");
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

/// A row of `index_row(account, table, name, ..)` with `status`, under the
/// store of `kind`, `root` and `id`.
fn row_at(
    account: &str,
    name: &str,
    status: BackupStatus,
    (kind, root, id): (StoreKind, &str, &str),
) -> BackupRow {
    let store = StoreIdentity {
        store_kind: kind,
        store_root: root.to_owned(),
        store_id: id.to_owned(),
    };
    let mut row = index_row(account, "Music", name, &store);
    row.status = status;
    row.size_bytes = 10;
    row.item_count = 2;
    row
}

/// The ARNs of `rows` in the listings' (`created_at`, `backup_arn`) order.
fn in_listing_order(rows: &[&BackupRow]) -> Vec<String> {
    let mut rows = rows.to_vec();
    rows.sort_by(|a, b| {
        a.created_at
            .total_cmp(&b.created_at)
            .then_with(|| a.backup_arn.cmp(&b.backup_arn))
    });
    rows.into_iter().map(|r| r.backup_arn.clone()).collect()
}

/// The kind and id the status entry of `arn` under `root` and `status`
/// carries, read at quorum; `None` when there is no such entry.
async fn status_entry(
    engine: &CassandraEngine,
    root: &str,
    status: BackupStatus,
    arn: &str,
) -> Option<(String, String)> {
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT store_kind, store_id FROM {}.backup_index_by_status \
             WHERE store_root = ? AND backup_status = ? AND backup_arn = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(root, status.as_str(), arn),
        "status entry",
    )
    .await
    .expect("read a status entry");
    rows.first().map(|row| {
        use cdrs_tokio::types::IntoRustByName;
        let kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
        let id: Option<String> = row.get_by_name("store_id").ok().flatten();
        (kind.unwrap_or_default(), id.unwrap_or_default())
    })
}

/// The rows a by-root listing returns, by ARN.
async fn by_root(engine: &CassandraEngine, kind: StoreKind, root: &str) -> Vec<String> {
    let rows: Vec<BackupRow> = engine
        .list_backup_rows_by_root(kind, root, None)
        .try_collect()
        .await
        .expect("listing");
    rows.into_iter().map(|r| r.backup_arn).collect()
}

/// `list_backup_rows_by_root`: every row of the kind and root, whatever its
/// id, of every status or of one, whole and in the listings' order, read
/// from the root's status partitions and re-read; nothing of a root that
/// differs by one byte (a trailing `/`, one character fewer), of an S3 root
/// that differs by its `?endpoint=` suffix, or of the other kind at the same
/// label.
#[tokio::test]
async fn test_list_backup_rows_by_root_returns_every_id_and_status_of_the_root_and_nothing_else() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let root = format!("/srv/backups-{tag}");
    let at = |id| (StoreKind::Filesystem, root.as_str(), id);
    let mut of_root = Vec::new();
    for (i, status) in [
        BackupStatus::Available,
        BackupStatus::Deleting,
        BackupStatus::Deleted,
        BackupStatus::Failed,
    ]
    .into_iter()
    .enumerate()
    {
        let id = if i % 2 == 0 { "store-1" } else { "store-2" };
        of_root.push(row_at(&account_id, &format!("r{i}"), status, at(id)));
    }
    // Inserted newest first: the order asserted is the listing's.
    for row in of_root.iter().rev() {
        assert!(engine.insert_backup_row(row).await.expect("insert"));
    }
    let creating = row_at(&account_id, "job", BackupStatus::Creating, at("store-1"));
    let claim = engine.claim_backup(&creating).await.expect("claim");
    let slash = format!("{root}/");
    let shorter = root[..root.len() - 1].to_owned();
    let s3 = format!("s3://bucket-{tag}/prefix");
    let s3_endpoint = format!("{s3}?endpoint=http://127.0.0.1:9200");
    let others = [
        row_at(
            &account_id,
            "slash",
            BackupStatus::Available,
            (StoreKind::Filesystem, &slash, "store-1"),
        ),
        row_at(
            &account_id,
            "shorter",
            BackupStatus::Available,
            (StoreKind::Filesystem, &shorter, "store-1"),
        ),
        row_at(
            &account_id,
            "kind",
            BackupStatus::Available,
            (StoreKind::S3, &root, "store-1"),
        ),
        row_at(
            &account_id,
            "s3",
            BackupStatus::Available,
            (StoreKind::S3, &s3, "store-3"),
        ),
        row_at(
            &account_id,
            "endpoint",
            BackupStatus::Available,
            (StoreKind::S3, &s3_endpoint, "store-3"),
        ),
    ];
    for row in &others {
        assert!(engine.insert_backup_row(row).await.expect("insert"));
    }
    let listed = |kind: StoreKind, root: String, status: Option<BackupStatus>| {
        let engine = &engine;
        async move {
            let rows: Vec<BackupRow> = engine
                .list_backup_rows_by_root(kind, &root, status)
                .try_collect()
                .await
                .expect("listing");
            rows
        }
    };
    let arns =
        |rows: Vec<BackupRow>| -> Vec<String> { rows.into_iter().map(|r| r.backup_arn).collect() };
    let mut all: Vec<&BackupRow> = of_root.iter().collect();
    all.push(&creating);
    let whole = listed(StoreKind::Filesystem, root.clone(), None).await;
    assert_eq!(arns(whole.clone()), in_listing_order(&all));
    for row in &whole {
        assert_eq!(
            Some(row),
            engine
                .read_backup_row(&arn_of(row))
                .await
                .expect("read")
                .as_ref(),
            "the listing returns rows whole"
        );
    }
    for status in BackupStatus::ALL {
        let of_status: Vec<&BackupRow> =
            all.iter().copied().filter(|r| r.status == status).collect();
        assert_eq!(
            arns(listed(StoreKind::Filesystem, root.clone(), Some(status)).await),
            in_listing_order(&of_status),
            "{status}"
        );
    }
    for (kind, label, row) in [
        (StoreKind::Filesystem, slash.as_str(), &others[0]),
        (StoreKind::Filesystem, shorter.as_str(), &others[1]),
        (StoreKind::S3, root.as_str(), &others[2]),
        (StoreKind::S3, s3.as_str(), &others[3]),
        (StoreKind::S3, s3_endpoint.as_str(), &others[4]),
    ] {
        assert_eq!(
            arns(listed(kind, label.to_owned(), None).await),
            std::slice::from_ref(&row.backup_arn),
            "{kind} {label}"
        );
    }
    drop(claim);
    let mut every: Vec<&BackupRow> = all.clone();
    every.extend(others.iter());
    remove_index_rows(&engine, &every).await;
}

/// `rewrite_store`: of the keys given, the rows still of the old kind and
/// root and not CREATING get the new store, with every other column left,
/// and are counted; their status entries move from the old root's
/// partition to the new root's, under the same status, carrying the new
/// kind and id. A CREATING row, a row of another root, a row of the other
/// kind at the same label and a key with no row are left and not counted; a
/// second call counts 0; a row another writer moved to a third root before
/// the call is left there.
#[tokio::test]
async fn test_rewrite_store_moves_only_the_old_roots_rows_that_are_not_creating() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let mut movable = Vec::new();
    for (i, status) in [
        BackupStatus::Available,
        BackupStatus::Deleting,
        BackupStatus::Deleted,
        BackupStatus::Failed,
    ]
    .into_iter()
    .enumerate()
    {
        movable.push(row_at(&account_id, &format!("m{i}"), status, old));
    }
    let moved_away = row_at(&account_id, "away", BackupStatus::Available, old);
    let other_root_label = format!("/srv/backups-other-{tag}");
    let other_root = row_at(
        &account_id,
        "other-root",
        BackupStatus::Available,
        (StoreKind::Filesystem, &other_root_label, "old-1"),
    );
    let other_kind = row_at(
        &account_id,
        "other-kind",
        BackupStatus::Available,
        (StoreKind::S3, &old_root, "old-1"),
    );
    for row in movable
        .iter()
        .chain([&moved_away, &other_root, &other_kind])
    {
        assert!(engine.insert_backup_row(row).await.expect("insert"));
    }
    let creating = row_at(&account_id, "job", BackupStatus::Creating, old);
    let claim = engine.claim_backup(&creating).await.expect("claim");
    let stored = |row: &BackupRow| {
        let engine = &engine;
        let arn = arn_of(row);
        async move {
            engine
                .read_backup_row(&arn)
                .await
                .expect("read")
                .expect("the row exists")
        }
    };
    let mut before = Vec::new();
    for row in &movable {
        before.push(stored(row).await);
    }
    // Another writer moves one row to a third root first.
    let third = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-third-{tag}"),
        store_id: "third-1".to_owned(),
    };
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[moved_away.key()], &third)
            .await
            .expect("rewrite"),
        1
    );
    let mut ghost = row_at(&account_id, "ghost", BackupStatus::Available, old).key();
    ghost.backup_arn = format!("{}-ghost", ghost.backup_arn);
    let keys: Vec<extenddb_storage::backup_catalog::BackupKey> = movable
        .iter()
        .chain([&creating, &moved_away, &other_root, &other_kind])
        .map(BackupRow::key)
        .chain([ghost])
        .collect();
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &keys, &to)
            .await
            .expect("rewrite"),
        4,
        "the four rows of the old root that are not CREATING"
    );
    for (row, before) in movable.iter().zip(&before) {
        assert_eq!(
            stored(row).await,
            BackupRow {
                store: to.clone(),
                ..before.clone()
            },
            "only the store changed"
        );
        assert_eq!(
            status_entry(&engine, &to.store_root, row.status, &row.backup_arn).await,
            Some(("filesystem".to_owned(), "new-1".to_owned())),
            "{}: the new root's entry",
            row.status
        );
        assert_eq!(
            status_entry(&engine, old.1, row.status, &row.backup_arn).await,
            None,
            "{}: the old root's entry is gone",
            row.status
        );
    }
    let creating_now = stored(&creating).await;
    assert_eq!(
        (creating_now.status, creating_now.store.store_root.as_str()),
        (BackupStatus::Creating, old.1)
    );
    assert_eq!(creating_now.owner_generation, claim.generation());
    assert_eq!(stored(&moved_away).await.store, third);
    assert_eq!(stored(&other_root).await.store, other_root.store);
    assert_eq!(stored(&other_kind).await.store, other_kind.store);
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &keys, &to)
            .await
            .expect("rewrite"),
        0,
        "a second call finds nothing left to move"
    );
    let listed: Vec<BackupRow> = engine
        .list_backup_rows(None, &to)
        .try_collect()
        .await
        .expect("listing");
    let movable_refs: Vec<&BackupRow> = movable.iter().collect();
    assert_eq!(
        listed.into_iter().map(|r| r.backup_arn).collect::<Vec<_>>(),
        in_listing_order(&movable_refs)
    );
    assert_eq!(
        by_root(&engine, old.0, old.1).await,
        std::slice::from_ref(&creating.backup_arn)
    );
    assert_eq!(
        engine.read_store_ids(old.0, old.1).await.expect("ids"),
        ["old-1"],
        "the CREATING row keeps the old root, and no other entry is left there"
    );
    drop(claim);
    let mut cleanup: Vec<BackupRow> = Vec::new();
    for row in movable.iter().chain([&moved_away]) {
        for store in [&to, &third] {
            cleanup.push(BackupRow {
                store: store.clone(),
                ..row.clone()
            });
        }
    }
    let mut every: Vec<&BackupRow> = cleanup.iter().collect();
    every.extend(
        movable
            .iter()
            .chain([&creating, &moved_away, &other_root, &other_kind]),
    );
    remove_index_rows(&engine, &every).await;
}

/// `summarize_store_roots`: one count per kind, root, id and status entry,
/// in the one order; two ids of one root, two statuses of one id, two roots
/// and the two kinds are each counted apart. The catalog keyspace is shared,
/// so only this test's roots are compared.
#[tokio::test]
async fn test_summarize_store_roots_counts_the_rows_of_each_store_and_status() {
    use extenddb_storage::backup_catalog::StoreRootCount;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let (root_a, root_b, root_s3) = (
        format!("/srv/a-{tag}"),
        format!("/srv/b-{tag}"),
        format!("s3://bucket-{tag}/a"),
    );
    let a1 = (StoreKind::Filesystem, root_a.as_str(), "id-1");
    let a2 = (StoreKind::Filesystem, root_a.as_str(), "id-2");
    let b1 = (StoreKind::Filesystem, root_b.as_str(), "id-1");
    let s3 = (StoreKind::S3, root_s3.as_str(), "id-1");
    let rows = [
        row_at(&account_id, "1", BackupStatus::Available, a1),
        row_at(&account_id, "2", BackupStatus::Available, a1),
        row_at(&account_id, "3", BackupStatus::Failed, a1),
        row_at(&account_id, "4", BackupStatus::Available, a2),
        row_at(&account_id, "5", BackupStatus::Deleting, b1),
        row_at(&account_id, "7", BackupStatus::Available, b1),
        row_at(&account_id, "6", BackupStatus::Available, s3),
    ];
    for row in &rows {
        assert!(engine.insert_backup_row(row).await.expect("insert"));
    }
    let creating = row_at(&account_id, "job", BackupStatus::Creating, b1);
    let claim = engine.claim_backup(&creating).await.expect("claim");
    let line = |(kind, root, id): (StoreKind, &str, &str), status, rows| StoreRootCount {
        store: StoreIdentity {
            store_kind: kind,
            store_root: root.to_owned(),
            store_id: id.to_owned(),
        },
        status,
        rows,
    };
    let ours: Vec<StoreRootCount> = engine
        .summarize_store_roots()
        .await
        .expect("summary")
        .into_iter()
        .filter(|c| [&root_a, &root_b, &root_s3].contains(&&c.store.store_root))
        .collect();
    assert_eq!(
        ours,
        [
            line(a1, BackupStatus::Available, 2),
            line(a1, BackupStatus::Failed, 1),
            line(a2, BackupStatus::Available, 1),
            line(b1, BackupStatus::Creating, 1),
            line(b1, BackupStatus::Available, 1),
            line(b1, BackupStatus::Deleting, 1),
            line(s3, BackupStatus::Available, 1),
        ]
    );
    drop(claim);
    let mut every: Vec<&BackupRow> = rows.iter().collect();
    every.push(&creating);
    remove_index_rows(&engine, &every).await;
}

/// A rewrite whose answer is lost is settled by a serial read: one whose
/// transaction ran finds the row under the new root, counts it and removes
/// the old root's entry; one whose transaction never ran finds the row under
/// the old root, sends it once more and counts it; a lost answer on every
/// attempt is an error that leaves the row under the old root and listed
/// there.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_an_unknown_rewrite_outcome_is_settled_by_a_serial_read() {
    use extenddb_storage_cassandra::test_hooks::{ProgressWriteFault, fail_rewrite_writes};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let mut cleanup = Vec::new();
    for (name, fault) in [
        ("landed", ProgressWriteFault::Landed),
        ("lost", ProgressWriteFault::Lost),
    ] {
        let row = row_at(&account_id, name, BackupStatus::Available, old);
        assert!(engine.insert_backup_row(&row).await.expect("insert"));
        fail_rewrite_writes(&row.backup_arn, &[fault]);
        assert_eq!(
            engine
                .rewrite_store(old.0, old.1, &[row.key()], &to)
                .await
                .expect("rewrite"),
            1,
            "{name}"
        );
        let stored = engine.read_backup_row(&arn_of(&row)).await.unwrap();
        assert_eq!(stored.map(|r| r.store), Some(to.clone()), "{name}");
        assert_eq!(
            status_entry(&engine, old.1, row.status, &row.backup_arn).await,
            None,
            "{name}: the old root's entry is gone"
        );
        assert!(
            status_entry(&engine, &to.store_root, row.status, &row.backup_arn)
                .await
                .is_some(),
            "{name}: the new root's entry"
        );
        cleanup.push(row);
    }

    let unknown = row_at(&account_id, "unknown", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&unknown).await.expect("insert"));
    fail_rewrite_writes(&unknown.backup_arn, &[ProgressWriteFault::Lost; 3]);
    let failed = engine
        .rewrite_store(old.0, old.1, &[unknown.key()], &to)
        .await
        .expect_err("a lost answer on every attempt");
    assert!(
        matches!(&failed, StorageError::Internal(m) if m.contains("unknown outcome")),
        "{failed}"
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&unknown))
            .await
            .unwrap()
            .map(|r| r.store.store_root),
        Some(old_root.clone())
    );
    assert_eq!(
        by_root(&engine, old.0, old.1).await,
        std::slice::from_ref(&unknown.backup_arn)
    );
    cleanup.push(unknown);
    let moved: Vec<BackupRow> = cleanup
        .iter()
        .map(|row| BackupRow {
            store: to.clone(),
            ..row.clone()
        })
        .collect();
    let every: Vec<&BackupRow> = cleanup.iter().chain(moved.iter()).collect();
    remove_index_rows(&engine, &every).await;
}

/// A rewrite that stops between its writes leaves the row listed under one
/// root: stopped after its first write (the new root's entry), the row
/// still names the old root and is listed there and not under the new one,
/// and the next call moves it; stopped after its transaction, the row names
/// the new root and is listed there, and the old root's entry is left, which
/// the old root's listing skips and `read_store_ids` of the old root still
/// counts.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_stopped_rewrite_leaves_the_row_listed_under_one_root() {
    use extenddb_storage_cassandra::test_hooks::{RewriteStop, stop_rewrite};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let listed_under_new = |row: &BackupRow| {
        let engine = &engine;
        let to = &to;
        let arn = row.backup_arn.clone();
        async move {
            let rows: Vec<BackupRow> = engine
                .list_backup_rows(None, to)
                .try_collect()
                .await
                .expect("listing");
            rows.iter().any(|r| r.backup_arn == arn)
        }
    };

    let first = row_at(&account_id, "first", BackupStatus::Failed, old);
    assert!(engine.insert_backup_row(&first).await.expect("insert"));
    stop_rewrite(&first.backup_arn, RewriteStop::AfterNewEntry);
    engine
        .rewrite_store(old.0, old.1, &[first.key()], &to)
        .await
        .expect_err("stopped after the first write");
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&first))
            .await
            .unwrap()
            .map(|r| r.store.store_root),
        Some(old_root.clone())
    );
    assert_eq!(
        by_root(&engine, old.0, old.1).await,
        std::slice::from_ref(&first.backup_arn),
        "still listed under the old root"
    );
    assert!(
        !listed_under_new(&first).await,
        "the new root's entry is skipped"
    );
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[first.key()], &to)
            .await
            .expect("rewrite"),
        1,
        "the next call moves it"
    );
    assert!(listed_under_new(&first).await);
    assert!(by_root(&engine, old.0, old.1).await.is_empty());

    let second = row_at(&account_id, "second", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&second).await.expect("insert"));
    stop_rewrite(&second.backup_arn, RewriteStop::AfterRow);
    engine
        .rewrite_store(old.0, old.1, &[second.key()], &to)
        .await
        .expect_err("stopped after the transaction");
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&second))
            .await
            .unwrap()
            .map(|r| r.store),
        Some(to.clone())
    );
    assert!(listed_under_new(&second).await, "listed under the new root");
    assert!(
        by_root(&engine, old.0, old.1).await.is_empty(),
        "the old root's listing skips the entry left"
    );
    assert!(
        status_entry(&engine, old.1, second.status, &second.backup_arn)
            .await
            .is_some(),
        "the old root's entry is left"
    );
    assert_eq!(
        engine.read_store_ids(old.0, old.1).await.expect("ids"),
        ["old-1"],
        "an entry left counts for the old root's ids"
    );
    let moved: Vec<BackupRow> = [&first, &second]
        .into_iter()
        .map(|row| BackupRow {
            store: to.clone(),
            ..row.clone()
        })
        .collect();
    let every: Vec<&BackupRow> = [&first, &second].into_iter().chain(moved.iter()).collect();
    remove_index_rows(&engine, &every).await;
}

/// The stores of a test's rows moved from `old` to `to`, for the cleanup:
/// each row under both roots, so every status entry of either is removed.
#[cfg(feature = "test-hooks")]
fn under_both(rows: &[&BackupRow], to: &StoreIdentity) -> Vec<BackupRow> {
    rows.iter()
        .flat_map(|row| {
            [
                (*row).clone(),
                BackupRow {
                    store: to.clone(),
                    ..(*row).clone()
                },
            ]
        })
        .collect()
}

/// Whether `list_backup_rows(Some(status), store)` returns `arn`.
#[cfg(feature = "test-hooks")]
async fn listed_with(
    engine: &CassandraEngine,
    status: BackupStatus,
    store: &StoreIdentity,
    arn: &str,
) -> bool {
    let rows: Vec<BackupRow> = engine
        .list_backup_rows(Some(status), store)
        .try_collect()
        .await
        .expect("listing");
    rows.iter().any(|r| r.backup_arn == arn)
}

/// A DeleteBackup or the deletion's DELETED write that read the row before
/// `rewrite_store` moved it, and writes after: each one's lightweight
/// transaction carries the store it read, so it does not apply to the moved
/// row; the row is read again and written under the root it now names, and
/// its status entry moves there. The row ends DELETING (then DELETED) under
/// the new root and is listed by the new store's listing of that status,
/// with no entry left under either root for the status it had.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_delete_that_read_the_row_before_a_rewrite_moves_its_entry_under_the_new_root() {
    use extenddb_storage_cassandra::test_hooks::{
        pause_before_delete_claim, pause_before_deleted_write,
    };
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let new_entry = Some(("filesystem".to_owned(), "new-1".to_owned()));

    // DeleteBackup: read AVAILABLE under the old root, then the move.
    let available = row_at(&account_id, "deleted", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&available).await.expect("insert"));
    let pause = pause_before_delete_claim(&available.backup_arn);
    let start = {
        let engine = std::sync::Arc::clone(&engine);
        let key = available.key();
        tokio::spawn(async move { engine.start_backup_delete(&key).await })
    };
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[available.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    pause.release();
    assert_eq!(
        start.await.expect("delete task").expect("delete"),
        DeleteStart::Deleting
    );
    let stored = engine
        .read_backup_row(&arn_of(&available))
        .await
        .unwrap()
        .expect("the row exists");
    assert_eq!(
        (stored.status, &stored.store),
        (BackupStatus::Deleting, &to)
    );
    assert!(
        listed_with(&engine, BackupStatus::Deleting, &to, &available.backup_arn).await,
        "the new store's DELETING listing returns the row"
    );
    let arn = available.backup_arn.as_str();
    assert_eq!(
        status_entry(&engine, &to.store_root, BackupStatus::Deleting, arn).await,
        new_entry
    );
    for (root, status) in [
        (to.store_root.as_str(), BackupStatus::Available),
        (old.1, BackupStatus::Available),
        (old.1, BackupStatus::Deleting),
    ] {
        assert_eq!(
            status_entry(&engine, root, status, arn).await,
            None,
            "{root} {status}"
        );
    }

    // The DELETED write: read DELETING under the old root, then the move.
    let deleting = row_at(&account_id, "finished", BackupStatus::Deleting, old);
    assert!(engine.insert_backup_row(&deleting).await.expect("insert"));
    let pause = pause_before_deleted_write(&deleting.backup_arn);
    let finish = {
        let engine = std::sync::Arc::clone(&engine);
        let key = deleting.key();
        tokio::spawn(async move { engine.set_backup_deleted(&key).await })
    };
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[deleting.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    pause.release();
    assert!(finish.await.expect("task").expect("set DELETED"));
    let stored = engine
        .read_backup_row(&arn_of(&deleting))
        .await
        .unwrap()
        .expect("the row exists");
    assert_eq!((stored.status, &stored.store), (BackupStatus::Deleted, &to));
    assert!(
        listed_with(&engine, BackupStatus::Deleted, &to, &deleting.backup_arn).await,
        "the new store's DELETED listing returns the row"
    );
    let arn = deleting.backup_arn.as_str();
    assert_eq!(
        status_entry(&engine, &to.store_root, BackupStatus::Deleted, arn).await,
        new_entry
    );
    for (root, status) in [
        (to.store_root.as_str(), BackupStatus::Deleting),
        (old.1, BackupStatus::Deleting),
        (old.1, BackupStatus::Deleted),
    ] {
        assert_eq!(
            status_entry(&engine, root, status, arn).await,
            None,
            "{root} {status}"
        );
    }
    let every = under_both(&[&available, &deleting], &to);
    remove_index_rows(&engine, &every.iter().collect::<Vec<_>>()).await;
}

/// A rewrite whose row another writer changes between the new root's entry
/// and the lightweight transaction. A DeleteBackup in the gap: the
/// transaction, conditioned on the status read, does not apply; the serial
/// read finds the row still under the old root, the new entry is removed,
/// and the next attempt moves the DELETING row, which is then listed under
/// the new root with its one entry there. Another move of the row to the
/// same store in the gap: the serial read after the transaction that did not
/// apply finds the row already under the new store, so the call counts it
/// and keeps the new entry, the row's only correct one.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_rewrite_whose_row_changes_before_its_transaction_reads_the_row_again() {
    use extenddb_storage_cassandra::test_hooks::pause_before_rewrite_write;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let rewrite = |row: &BackupRow| {
        let engine = std::sync::Arc::clone(&engine);
        let key = row.key();
        let (to, root) = (to.clone(), old_root.clone());
        tokio::spawn(async move {
            engine
                .rewrite_store(StoreKind::Filesystem, &root, &[key], &to)
                .await
        })
    };

    let deleted = row_at(&account_id, "deleted", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&deleted).await.expect("insert"));
    let pause = pause_before_rewrite_write(&deleted.backup_arn);
    let moving = rewrite(&deleted);
    pause.reached().await;
    assert_eq!(
        engine
            .start_backup_delete(&deleted.key())
            .await
            .expect("delete"),
        DeleteStart::Deleting
    );
    pause.release();
    assert_eq!(moving.await.expect("task").expect("rewrite"), 1);
    let stored = engine
        .read_backup_row(&arn_of(&deleted))
        .await
        .unwrap()
        .expect("the row exists");
    assert_eq!(
        (stored.status, &stored.store),
        (BackupStatus::Deleting, &to)
    );
    assert!(listed_with(&engine, BackupStatus::Deleting, &to, &deleted.backup_arn).await);
    let arn = deleted.backup_arn.as_str();
    assert!(
        status_entry(&engine, &to.store_root, BackupStatus::Deleting, arn)
            .await
            .is_some()
    );
    for (root, status) in [
        (to.store_root.as_str(), BackupStatus::Available),
        (old.1, BackupStatus::Available),
        (old.1, BackupStatus::Deleting),
    ] {
        assert_eq!(
            status_entry(&engine, root, status, arn).await,
            None,
            "{root} {status}: only the new root's DELETING entry is left"
        );
    }

    let raced = row_at(&account_id, "raced", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&raced).await.expect("insert"));
    let pause = pause_before_rewrite_write(&raced.backup_arn);
    let first = rewrite(&raced);
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[raced.key()], &to)
            .await
            .expect("the other move"),
        1
    );
    pause.release();
    assert_eq!(
        first.await.expect("task").expect("rewrite"),
        1,
        "the row already under the new store counts as this call's write"
    );
    assert!(
        listed_with(&engine, BackupStatus::Available, &to, &raced.backup_arn).await,
        "the new root's entry is kept"
    );
    assert_eq!(
        status_entry(&engine, old.1, BackupStatus::Available, &raced.backup_arn).await,
        None
    );
    let every = under_both(&[&deleted, &raced], &to);
    remove_index_rows(&engine, &every.iter().collect::<Vec<_>>()).await;
}

/// `list_backup_rows_by_root` lists a row once, under its own status and
/// kind: a second entry of the row under another status of the same root
/// is passed over (each row is read once), and an entry whose kind is not
/// its row's is skipped on the re-read.
#[tokio::test]
async fn test_list_backup_rows_by_root_skips_an_entry_whose_row_has_another_status_or_kind() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let root = format!("/srv/backups-{tag}");
    let row = row_at(
        &account_id,
        "row",
        BackupStatus::Available,
        (StoreKind::Filesystem, &root, "id-1"),
    );
    let s3_row = row_at(
        &account_id,
        "s3",
        BackupStatus::Available,
        (StoreKind::S3, &root, "id-1"),
    );
    for r in [&row, &s3_row] {
        assert!(engine.insert_backup_row(r).await.expect("insert"));
    }
    let catalog = engine.catalog_keyspace();
    let entry = |status: BackupStatus, arn: &str| {
        (
            format!(
                "INSERT INTO {catalog}.backup_index_by_status \
                 (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
                 VALUES (?, ?, ?, ?, 'filesystem', 'id-1')"
            ),
            cdrs_tokio::query_values!(
                root.as_str(),
                status.as_str(),
                arn.to_owned(),
                account_id.as_str()
            ),
        )
    };
    // A stale entry of the filesystem row under FAILED, and the S3 row's
    // AVAILABLE entry overwritten with the filesystem kind.
    for (cql, values) in [
        entry(BackupStatus::Failed, &row.backup_arn),
        entry(BackupStatus::Available, &s3_row.backup_arn),
    ] {
        engine
            .session()
            .query_with_values(&cql, values)
            .await
            .expect("write an entry");
    }
    assert_eq!(
        by_root(&engine, StoreKind::Filesystem, &root).await,
        std::slice::from_ref(&row.backup_arn),
        "listed once, and the S3 row not at all"
    );
    for status in [BackupStatus::Failed, BackupStatus::Available] {
        let rows: Vec<BackupRow> = engine
            .list_backup_rows_by_root(StoreKind::Filesystem, &root, Some(status))
            .try_collect()
            .await
            .expect("listing");
        let expected: &[String] = if status == BackupStatus::Available {
            std::slice::from_ref(&row.backup_arn)
        } else {
            &[]
        };
        assert_eq!(
            rows.into_iter().map(|r| r.backup_arn).collect::<Vec<_>>(),
            expected,
            "{status}"
        );
    }
    remove_index_rows(&engine, &[&row, &s3_row]).await;
}

/// After a rewrite stopped between its transaction and the old entry's
/// removal, `summarize_store_roots` counts the row under both roots: the
/// new root's entry and the old root's entry left behind, which the summary
/// does not verify against the row.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_the_summary_counts_the_old_entry_a_stopped_rewrite_leaves() {
    use extenddb_storage::backup_catalog::StoreRootCount;
    use extenddb_storage_cassandra::test_hooks::{RewriteStop, stop_rewrite};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let row = row_at(&account_id, "stopped", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&row).await.expect("insert"));
    stop_rewrite(&row.backup_arn, RewriteStop::AfterRow);
    engine
        .rewrite_store(old.0, old.1, &[row.key()], &to)
        .await
        .expect_err("stopped after the transaction");
    let ours: Vec<StoreRootCount> = engine
        .summarize_store_roots()
        .await
        .expect("summary")
        .into_iter()
        .filter(|c| {
            [old_root.as_str(), to.store_root.as_str()].contains(&c.store.store_root.as_str())
        })
        .collect();
    let mut expected = vec![
        StoreRootCount {
            store: row.store.clone(),
            status: BackupStatus::Available,
            rows: 1,
        },
        StoreRootCount {
            store: to.clone(),
            status: BackupStatus::Available,
            rows: 1,
        },
    ];
    StoreRootCount::sort(&mut expected);
    assert_eq!(ours, expected, "the orphan entry counts for the old root");
    let every = under_both(&[&row], &to);
    remove_index_rows(&engine, &every.iter().collect::<Vec<_>>()).await;
}

/// `insert_backup_row`: the row lands as given, unowned, with `completed_at`
/// and `heartbeat_at` on the coordinator's clock, its status entry and both
/// list rows written (`list_backup_rows` reads the first, the account and
/// table listings the others); a second insert and an insert over a CREATING
/// row a claim holds answer `false` and change nothing; a claim over the
/// inserted row is refused. The rule is per ARN spelling: the same id under
/// another region is another row.
#[tokio::test]
async fn test_insert_backup_row_inserts_an_unowned_row_only_when_absent() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut adopted = index_row(&account_id, "Music", "adopted", &store);
    adopted.status = BackupStatus::Available;
    adopted.size_bytes = 1234;
    adopted.item_count = 5;
    // Ignored: the insert sets these three itself.
    adopted.owner_generation = 99;
    adopted.completed_at = None;
    adopted.heartbeat_at = 0.0;
    assert!(engine.insert_backup_row(&adopted).await.expect("insert"));
    let stored = engine
        .read_backup_row(&arn_of(&adopted))
        .await
        .expect("read")
        .expect("the row exists");
    assert_eq!(stored.owner_generation, UNOWNED_GENERATION);
    let completed_at = stored.completed_at.expect("completed_at is set");
    for at in [completed_at, stored.heartbeat_at] {
        assert!((at - adopted.created_at).abs() < 300.0, "{at}");
    }
    assert!(
        (stored.created_at - adopted.created_at).abs() < 1e-3,
        "{} is the id's instant {}",
        stored.created_at,
        adopted.created_at
    );
    assert_eq!(
        BackupRow {
            owner_generation: 99,
            completed_at: None,
            heartbeat_at: 0.0,
            created_at: adopted.created_at,
            ..stored.clone()
        },
        adopted,
        "every other column is the row as given"
    );

    // A second insert of the key, with other values, writes nothing.
    let mut again = adopted.clone();
    again.backup_name = "again".to_owned();
    again.size_bytes = 1;
    again.status = BackupStatus::Failed;
    assert!(!engine.insert_backup_row(&again).await.expect("insert"));
    assert_eq!(
        engine.read_backup_row(&arn_of(&adopted)).await.unwrap(),
        Some(stored.clone()),
        "the refused inserts changed nothing"
    );
    let claimed = engine.claim_backup(&adopted).await.err();
    assert!(
        matches!(claimed, Some(StorageError::AlreadyExists(_))),
        "{claimed:?}"
    );

    // Over a CREATING row a live claim holds.
    let creating = index_row(&account_id, "Music", "job", &store);
    let claim = engine.claim_backup(&creating).await.expect("claim");
    let mut over = creating.clone();
    over.status = BackupStatus::Available;
    assert!(!engine.insert_backup_row(&over).await.expect("insert"));
    let job_row = engine
        .read_backup_row(&arn_of(&creating))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job_row.status, BackupStatus::Creating);
    assert_eq!(job_row.owner_generation, claim.generation());

    // Read and listed like a created row.
    let arns =
        |rows: Vec<BackupRow>| -> Vec<String> { rows.into_iter().map(|r| r.backup_arn).collect() };
    let available: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(available, std::slice::from_ref(&stored));
    let creating_rows: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Creating), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        arns(creating_rows),
        std::slice::from_ref(&creating.backup_arn)
    );
    for table in [None, Some("Music")] {
        let listed: Vec<BackupRow> = engine
            .list_account_backup_rows(&account_id, table, &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert!(
            listed.contains(&stored),
            "{table:?}: {:?}",
            arns(listed.clone())
        );
    }
    drop(claim);

    // Cassandra indexes no backup id: another spelling of the ARN inserts.
    let mut respelled = adopted.clone();
    respelled.backup_arn =
        BackupArn::new("aws", "eu-west-1", &account_id, "Music", &adopted.backup_id)
            .unwrap()
            .to_arn_string();
    assert!(engine.insert_backup_row(&respelled).await.expect("insert"));
    assert!(
        engine
            .read_backup_row(&arn_of(&respelled))
            .await
            .unwrap()
            .is_some()
    );
    remove_index_rows(&engine, &[&adopted, &respelled, &creating]).await;
}

/// Delete the three index entries of `row` and keep its `backups_by_arn`
/// row: the state an insert leaves when it stops, or its batch fails,
/// between the two writes.
async fn drop_index_entries(engine: &CassandraEngine, row: &BackupRow) {
    let catalog = engine.catalog_keyspace();
    let created_at = (row.created_at * 1_000.0).round() as i64;
    for (cql, values) in [
        (
            format!(
                "DELETE FROM {catalog}.backups_by_account \
                 WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(row.account_id.as_str(), created_at, row.backup_arn.as_str()),
        ),
        (
            format!(
                "DELETE FROM {catalog}.backups_by_table WHERE account_id = ? \
                 AND table_name = ? AND created_at = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                row.account_id.as_str(),
                row.table_name.as_str(),
                created_at,
                row.backup_arn.as_str()
            ),
        ),
        (
            format!(
                "DELETE FROM {catalog}.backup_index_by_status \
                 WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                row.store.store_root.as_str(),
                row.status.as_str(),
                row.backup_arn.as_str()
            ),
        ),
    ] {
        engine
            .session()
            .query_with_values(&cql, values)
            .await
            .expect("remove an index entry");
    }
}

/// Which of the three listings return `row`: the store's status listing
/// (`list_backup_rows`), the account listing and the table listing.
async fn listed_in(engine: &CassandraEngine, row: &BackupRow) -> [bool; 3] {
    let by_status: Vec<BackupRow> = engine
        .list_backup_rows(Some(row.status), &row.store)
        .try_collect()
        .await
        .expect("status listing");
    let mut listed = [by_status.iter().any(|r| r.backup_arn == row.backup_arn); 3];
    for (slot, table) in [(1, None), (2, Some(row.table_name.as_str()))] {
        let rows: Vec<BackupRow> = engine
            .list_account_backup_rows(&row.account_id, table, &TimeBounds::default())
            .try_collect()
            .await
            .expect("account listing");
        listed[slot] = rows.iter().any(|r| r.backup_arn == row.backup_arn);
    }
    listed
}

/// An insert that stopped between its row and its index entries leaves an
/// AVAILABLE row no listing returns. The next insert of the same row (the
/// next sync of the same verified prefix) finds the row its own, writes the
/// entries and answers `true`; an insert with other values still answers
/// `false` and writes nothing.
#[tokio::test]
async fn test_insert_backup_row_completes_an_insert_that_stopped_before_its_entries() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut row = index_row(&account_id, "Music", "torn", &store);
    row.status = BackupStatus::Available;
    row.size_bytes = 77;
    row.item_count = 3;
    assert!(engine.insert_backup_row(&row).await.expect("insert"));
    drop_index_entries(&engine, &row).await;
    assert_eq!(
        listed_in(&engine, &row).await,
        [false; 3],
        "the torn state: the row is listed nowhere"
    );

    assert!(
        engine.insert_backup_row(&row).await.expect("insert"),
        "the second insert of the same row completes the first"
    );
    assert_eq!(listed_in(&engine, &row).await, [true; 3]);
    let stored = engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .expect("the row exists");
    assert_eq!(stored.owner_generation, UNOWNED_GENERATION);

    let mut other = row.clone();
    other.size_bytes = 1;
    drop_index_entries(&engine, &row).await;
    assert!(
        !engine.insert_backup_row(&other).await.expect("insert"),
        "a row with other values is one that existed"
    );
    assert_eq!(listed_in(&engine, &row).await, [false; 3]);
    remove_index_rows(&engine, &[&row]).await;
}

/// An insert at an ARN an earlier release's in-catalog backup holds (a
/// `backups_by_arn` row with no store kind, which the index read does not
/// return) answers `false` and writes no list or status entry: the
/// `NotMatched` arm's serial read finds no index row, and that is not this
/// insert's row.
#[tokio::test]
async fn test_an_insert_over_an_in_catalog_backup_answers_false_and_writes_no_entry() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut row = index_row(&account_id, "Music", "legacy", &store);
    row.status = BackupStatus::Available;
    let catalog = engine.catalog_keyspace();
    engine
        .session()
        .query_with_values(
            &format!(
                "INSERT INTO {catalog}.backups_by_arn (account_id, backup_arn, backup_id, \
                 backup_name, table_name, backup_status) VALUES (?, ?, ?, ?, ?, 'AVAILABLE')"
            ),
            cdrs_tokio::query_values!(
                row.account_id.as_str(),
                row.backup_arn.as_str(),
                row.backup_id.as_str(),
                "legacy",
                "Music"
            ),
        )
        .await
        .expect("an in-catalog backup's row");
    let inserted = engine.insert_backup_row(&row).await.expect("insert");
    let entries = raw_index_entries(&engine, &row).await;
    remove_index_rows(&engine, &[&row]).await;
    assert!(
        !inserted,
        "an insert over an in-catalog backup answered true"
    );
    assert_eq!(
        entries, 0,
        "an insert over an in-catalog backup wrote entries"
    );
}

/// An insert whose answer is lost is settled by a serial read: one whose
/// transaction ran finds its row and answers `true` with its entries
/// written; one whose transaction never ran finds no row, sends it once
/// more and answers `true`; one that finds a row a claim holds answers
/// `false` and changes nothing; two lost answers with no row are an error
/// and leave no row.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_an_unknown_insert_outcome_is_settled_by_a_serial_read() {
    use extenddb_storage_cassandra::test_hooks::{ProgressWriteFault, fail_insert_writes};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let available = |name: &str| {
        let mut row = index_row(&account_id, "Music", name, &store);
        row.status = BackupStatus::Available;
        row
    };

    // The transaction ran; its answer was lost.
    let landed = available("landed");
    fail_insert_writes(&landed.backup_arn, &[ProgressWriteFault::Landed]);
    assert!(engine.insert_backup_row(&landed).await.expect("insert"));
    assert_eq!(listed_in(&engine, &landed).await, [true; 3]);

    // The transaction never ran: the write is sent once more.
    let lost = available("lost");
    fail_insert_writes(&lost.backup_arn, &[ProgressWriteFault::Lost]);
    assert!(engine.insert_backup_row(&lost).await.expect("insert"));
    assert_eq!(listed_in(&engine, &lost).await, [true; 3]);
    let stored = engine.read_backup_row(&arn_of(&lost)).await.unwrap();
    assert_eq!(stored.map(|r| r.owner_generation), Some(UNOWNED_GENERATION));

    // A row a claim holds is there: the insert is refused and changes nothing.
    let creating = index_row(&account_id, "Music", "job", &store);
    let claim = engine.claim_backup(&creating).await.expect("claim");
    let mut over = creating.clone();
    over.status = BackupStatus::Available;
    fail_insert_writes(&over.backup_arn, &[ProgressWriteFault::Lost]);
    assert!(!engine.insert_backup_row(&over).await.expect("insert"));
    let job_row = engine
        .read_backup_row(&arn_of(&creating))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (job_row.status, job_row.owner_generation),
        (BackupStatus::Creating, claim.generation())
    );
    assert!(
        !listed_in(&engine, &over).await[0],
        "no AVAILABLE status entry"
    );
    drop(claim);

    // Two lost answers and no row: an unknown outcome is an error.
    let unknown = available("unknown");
    fail_insert_writes(
        &unknown.backup_arn,
        &[ProgressWriteFault::Lost, ProgressWriteFault::Lost],
    );
    let failed = engine
        .insert_backup_row(&unknown)
        .await
        .expect_err("two lost answers and no row");
    assert!(
        matches!(&failed, StorageError::Internal(m) if m.contains("unknown outcome")),
        "{failed}"
    );
    assert_eq!(
        engine.read_backup_row(&arn_of(&unknown)).await.unwrap(),
        None
    );
    remove_index_rows(&engine, &[&landed, &lost, &creating, &unknown]).await;
}

/// A failed batch of index entries is sent once more; one that fails twice
/// is an error that leaves the row unlisted, and the next insert of the row
/// writes the entries.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_failed_index_entry_batch_is_sent_once_more() {
    use extenddb_storage_cassandra::test_hooks::fail_index_entry_batches;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let available = |name: &str| {
        let mut row = index_row(&account_id, "Music", name, &store);
        row.status = BackupStatus::Available;
        row
    };

    let once = available("once");
    fail_index_entry_batches(&once.backup_arn, 1);
    assert!(engine.insert_backup_row(&once).await.expect("insert"));
    assert_eq!(listed_in(&engine, &once).await, [true; 3]);

    let twice = available("twice");
    fail_index_entry_batches(&twice.backup_arn, 2);
    let failed = engine
        .insert_backup_row(&twice)
        .await
        .expect_err("a batch that fails twice");
    assert!(
        matches!(&failed, StorageError::Internal(m) if m.contains("index entry batch")),
        "{failed}"
    );
    assert!(
        engine
            .read_backup_row(&arn_of(&twice))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(listed_in(&engine, &twice).await, [false; 3]);
    assert!(engine.insert_backup_row(&twice).await.expect("insert"));
    assert_eq!(listed_in(&engine, &twice).await, [true; 3]);
    remove_index_rows(&engine, &[&once, &twice]).await;
}

/// A removal whose answer is lost is settled by a serial read: one whose
/// transaction ran finds no row and answers `Removed` with the entries
/// removed; one whose transaction never ran finds the row, runs once more and
/// answers `Removed`; a lost answer on every attempt is an error that leaves
/// the row AVAILABLE and listed.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_an_unknown_forget_outcome_is_settled_by_a_serial_read() {
    use extenddb_storage::backup_catalog::ForgetRow;
    use extenddb_storage_cassandra::test_hooks::{ProgressWriteFault, fail_forget_writes};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let adopted = |name: &str| {
        let mut row = index_row(&account_id, "Music", name, &store);
        row.status = BackupStatus::Available;
        row
    };

    for (name, fault) in [
        ("landed", ProgressWriteFault::Landed),
        ("lost", ProgressWriteFault::Lost),
    ] {
        let row = adopted(name);
        assert!(engine.insert_backup_row(&row).await.expect("insert"));
        fail_forget_writes(&row.backup_arn, &[fault]);
        assert_eq!(
            engine
                .forget_available_backup_row(&row.key())
                .await
                .unwrap(),
            ForgetRow::Removed,
            "{name}"
        );
        assert_eq!(
            engine.read_backup_row(&arn_of(&row)).await.unwrap(),
            None,
            "{name}"
        );
        assert_eq!(raw_index_entries(&engine, &row).await, 0, "{name}");
    }

    let unknown = adopted("unknown");
    assert!(engine.insert_backup_row(&unknown).await.expect("insert"));
    fail_forget_writes(&unknown.backup_arn, &[ProgressWriteFault::Lost; 3]);
    let failed = engine
        .forget_available_backup_row(&unknown.key())
        .await
        .expect_err("a lost answer on every attempt");
    assert!(
        matches!(&failed, StorageError::Internal(m) if m.contains("unknown outcome")),
        "{failed}"
    );
    assert_eq!(
        engine
            .read_backup_row(&arn_of(&unknown))
            .await
            .unwrap()
            .map(|r| r.status),
        Some(BackupStatus::Available)
    );
    assert_eq!(listed_in(&engine, &unknown).await, [true; 3]);
    remove_index_rows(&engine, &[&unknown]).await;
}

/// A removal whose entry batch fails after the row is gone answers `Removed`:
/// the row is read and listed nowhere, the entries it left are skipped by
/// every listing, and a second removal finds no row.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_forget_whose_entry_removal_fails_answers_removed() {
    use extenddb_storage::backup_catalog::ForgetRow;
    use extenddb_storage_cassandra::test_hooks::fail_index_entry_removals;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut row = index_row(&account_id, "Music", "orphaned", &store);
    row.status = BackupStatus::Available;
    assert!(engine.insert_backup_row(&row).await.expect("insert"));
    fail_index_entry_removals(&row.backup_arn, 1);
    assert_eq!(
        engine
            .forget_available_backup_row(&row.key())
            .await
            .unwrap(),
        ForgetRow::Removed
    );
    assert_eq!(engine.read_backup_row(&arn_of(&row)).await.unwrap(), None);
    assert_eq!(
        raw_index_entries(&engine, &row).await,
        3,
        "the failed batch left the entries"
    );
    assert_eq!(
        listed_in(&engine, &row).await,
        [false; 3],
        "every listing skips an entry whose row is gone"
    );
    assert!(matches!(
        engine.forget_available_backup_row(&row.key()).await,
        Err(StorageError::Validation(m)) if m.contains("Backup not found")
    ));
    remove_index_rows(&engine, &[&row]).await;
}

/// The in-use rule runs before the removal's write and clears a registration
/// whose target is no longer being restored: a row whose `active_restores`
/// still names a finished (ACTIVE) target is removed at once. Without the
/// rule the write's `active_restores = null` condition refuses every attempt
/// and the removal answers `InUse` naming the finished table.
#[tokio::test]
async fn test_a_forget_clears_a_finished_restores_registration_and_removes_the_row() {
    use extenddb_storage::backup_catalog::ForgetRow;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut row = index_row(&account_id, "Music", "finished", &store);
    row.status = BackupStatus::Available;
    assert!(engine.insert_backup_row(&row).await.expect("insert"));
    let finished = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "FinishedTarget".to_owned(),
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
            "UPDATE {}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            finished.table_id.as_str(),
            "FinishedTarget",
            account_id.as_str(),
            row.backup_arn.as_str()
        ),
    )
    .await;
    assert_eq!(
        engine
            .forget_available_backup_row(&row.key())
            .await
            .unwrap(),
        ForgetRow::Removed
    );
    assert_eq!(engine.read_backup_row(&arn_of(&row)).await.unwrap(), None);
    assert_eq!(raw_index_entries(&engine, &row).await, 0);
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

/// `list_backup_rows_older_than`: the rows of a store with a status whose
/// named clock column is older than the age on the coordinator's clock. A
/// row FAILED after its heartbeat aged is listed on the heartbeat clock and
/// not on the completed clock until that window too has passed (waited out
/// on the coordinator's clock, not hand-dated); a row with no `completed_at`
/// is never listed on the completed clock; another status and another store
/// are not listed.
#[tokio::test]
async fn test_rows_older_than_are_listed_on_the_named_clock() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let window = Duration::from_secs(1);
    let listed = |status: BackupStatus, clock: StaleClock, age: Duration| {
        let engine = &engine;
        let store = &store;
        async move {
            let mut arns: Vec<String> = engine
                .list_backup_rows_older_than(status, store, clock, age)
                .map_ok(|row| row.backup_arn)
                .try_collect()
                .await
                .expect("listing");
            arns.sort();
            arns
        }
    };
    let completed_failure = StatusFields {
        failure_reason: Some("owner dead".to_owned()),
        completed: true,
        ..StatusFields::default()
    };
    let open_failure = StatusFields {
        failure_reason: Some("owner dead".to_owned()),
        completed: false,
        ..StatusFields::default()
    };
    let stamped = index_row(&account_id, "Music", "stamped", &store);
    let unstamped = index_row(&account_id, "Music", "unstamped", &store);
    let creating = index_row(&account_id, "Music", "creating", &store);
    let mut other_store = store.clone();
    other_store.store_id = "store-2".to_owned();
    let elsewhere = index_row(&account_id, "Music", "elsewhere", &other_store);
    let stamped_claim = engine.claim_backup(&stamped).await.expect("claim");
    let unstamped_claim = engine.claim_backup(&unstamped).await.expect("claim");
    let _creating_claim = engine.claim_backup(&creating).await.expect("claim");
    let elsewhere_claim = engine.claim_backup(&elsewhere).await.expect("claim");
    assert_eq!(
        elsewhere_claim
            .set_status(JobStatus::Creating, JobStatus::Failed, &completed_failure)
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    assert!(
        listed(BackupStatus::Failed, StaleClock::Heartbeat, Duration::ZERO)
            .await
            .is_empty(),
        "nothing of this store is FAILED yet"
    );

    // The heartbeats age past the window; then the two rows fail, one with a
    // completion time and one without.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert_eq!(
        stamped_claim
            .set_status(JobStatus::Creating, JobStatus::Failed, &completed_failure)
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    assert_eq!(
        unstamped_claim
            .set_status(JobStatus::Creating, JobStatus::Failed, &open_failure)
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let mut both = vec![stamped.backup_arn.clone(), unstamped.backup_arn.clone()];
    both.sort();
    assert_eq!(
        listed(BackupStatus::Failed, StaleClock::Heartbeat, window).await,
        both,
        "a row just failed is listed on the heartbeat clock: its dead owner's beat is stale"
    );
    assert!(
        listed(BackupStatus::Failed, StaleClock::Completed, window)
            .await
            .is_empty(),
        "the completion time is an instant old"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        listed(BackupStatus::Failed, StaleClock::Completed, Duration::ZERO).await,
        std::slice::from_ref(&stamped.backup_arn),
        "a row with no completed_at is not listed on the completed clock"
    );
    assert!(
        listed(
            BackupStatus::Failed,
            StaleClock::Heartbeat,
            Duration::from_secs(300)
        )
        .await
        .is_empty(),
        "a long window lists nothing"
    );
    assert_eq!(
        listed(BackupStatus::Creating, StaleClock::Heartbeat, window).await,
        std::slice::from_ref(&creating.backup_arn),
        "the CREATING row is listed under its own status only"
    );
    assert!(
        listed(
            BackupStatus::Creating,
            StaleClock::Completed,
            Duration::ZERO
        )
        .await
        .is_empty(),
        "a CREATING row has no completion time"
    );
    assert!(
        listed(BackupStatus::Deleted, StaleClock::Completed, Duration::ZERO)
            .await
            .is_empty()
    );

    // The completed window crossed on the coordinator's clock.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert_eq!(
        listed(BackupStatus::Failed, StaleClock::Completed, window).await,
        std::slice::from_ref(&stamped.backup_arn)
    );
    assert_eq!(
        listed(BackupStatus::Failed, StaleClock::Heartbeat, window).await,
        both,
        "the other store's FAILED row is not listed"
    );
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

/// Two takers racing for one dead holder's row see one win: each reads the
/// stale row (the test hook holds both after their read, so neither sees
/// the other's write first) and runs `UPDATE ... IF owner_generation = <old>`,
/// and the lightweight transaction matches once. With `IF EXISTS` in its
/// place both would win and two commands would hold one claim. The loser is
/// refused naming a holder, and the winner's handle heartbeats.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_two_takers_of_one_stale_maintenance_claim_see_one_win() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let stale = Duration::from_secs(300);
    let dead = engine
        .maintenance_claim("purge on host-dead", stale)
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
    assert!(
        !engine.check_maintenance_claim(stale).await.expect("check"),
        "a stale claim is not held"
    );
    let pause = extenddb_storage_cassandra::test_hooks::pause_before_maintenance_takeover();
    let (a, b, ()) = tokio::join!(
        engine.maintenance_claim("purge on host-a", stale),
        engine.maintenance_claim("purge on host-b", stale),
        async {
            pause.reached().await;
            pause.reached().await;
            extenddb_storage_cassandra::test_hooks::clear_maintenance_takeover_pause();
            pause.release();
            pause.release();
        },
    );
    let (winner, loser) = match (a, b) {
        (Ok(w), Err(l)) | (Err(l), Ok(w)) => (w, l),
        (Ok(_), Ok(_)) => panic!("two takers both won one stale claim"),
        (Err(a), Err(b)) => panic!("neither taker won: {a:?} / {b:?}"),
    };
    match loser {
        StorageError::AlreadyExists(m) => assert!(m.contains("purge on host-"), "{m}"),
        other => panic!("the loser is refused with AlreadyExists: {other:?}"),
    }
    assert_ne!(winner.generation(), dead.generation());
    assert_eq!(
        winner.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    assert_eq!(
        dead.heartbeat().await.expect("heartbeat"),
        Heartbeat::Lost,
        "the dead holder's handle finds the claim lost"
    );
    assert!(engine.check_maintenance_claim(stale).await.expect("check"));
    winner.release().await.expect("release");
    assert!(!engine.check_maintenance_claim(stale).await.expect("check"));
}

/// An insert whose answer is lost after it committed (the test hook turns
/// the applied answer into `Unknown`) is settled by the re-read: the row
/// carries the call's generation, so the call holds the claim, heartbeats it
/// and releases it. Without the re-read the command would be refused while
/// its own row, which nobody heartbeats, refused every job until it went
/// stale.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_maintenance_insert_whose_answer_is_lost_is_settled_by_the_re_read() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let stale = Duration::from_secs(300);
    assert!(
        !engine.check_maintenance_claim(stale).await.expect("check"),
        "no claim is held before the test"
    );
    extenddb_storage_cassandra::test_hooks::lose_next_maintenance_insert_answer();
    let claim = engine
        .maintenance_claim("purge on host-lost", stale)
        .await
        .expect("an insert whose answer was lost is this call's claim once re-read");
    assert!(
        engine.check_maintenance_claim(stale).await.expect("check"),
        "the committed insert is held"
    );
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT holder, owner_generation FROM {}.backup_maintenance WHERE claim = 'claim'",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(),
        "maintenance claim",
    )
    .await
    .expect("read");
    use cdrs_tokio::types::IntoRustByName;
    let holder: Option<String> = rows
        .first()
        .and_then(|r| r.get_by_name("holder").ok().flatten());
    let generation: Option<i64> = rows
        .first()
        .and_then(|r| r.get_by_name("owner_generation").ok().flatten());
    assert_eq!(holder.as_deref(), Some("purge on host-lost"));
    assert_eq!(generation, Some(i64::try_from(claim.generation()).unwrap()));
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    claim.release().await.expect("release");
    assert!(!engine.check_maintenance_claim(stale).await.expect("check"));
}

/// `newest_available_backup_row` reads the status index's AVAILABLE
/// partition and one row: the newest AVAILABLE row in the listing's order,
/// none while the store has no AVAILABLE row, and never a row of another
/// store or status. `list_running_restore_jobs` lists a RUNNING record by
/// status alone, one whose heartbeat is ahead of the coordinator's clock
/// included, which the dead-job listing under a zero window does not list.
#[tokio::test]
async fn test_the_newest_available_row_and_the_running_restores_are_read_by_status() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    assert!(
        engine
            .newest_available_backup_row(&store)
            .await
            .expect("read")
            .is_none(),
        "no AVAILABLE row yet"
    );
    let publish = |row: BackupRow| {
        let engine = &engine;
        async move {
            let claim = engine.claim_backup(&row).await.expect("claim");
            assert_eq!(
                claim
                    .set_status(
                        JobStatus::Creating,
                        JobStatus::Available,
                        &StatusFields::default()
                    )
                    .await
                    .expect("publish"),
                WriteOutcome::Matched
            );
        }
    };
    publish(index_row(&account_id, "Music", "older", &store)).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    publish(index_row(&account_id, "Music", "newer", &store)).await;
    let creating = index_row(&account_id, "Music", "creating", &store);
    let _creating = engine.claim_backup(&creating).await.expect("claim");
    let mut other_store = store.clone();
    other_store.store_id = "store-2".to_owned();
    publish(index_row(&account_id, "Songs", "elsewhere", &other_store)).await;
    let newest = engine
        .newest_available_backup_row(&store)
        .await
        .expect("read")
        .expect("one AVAILABLE row");
    let listed: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .try_collect()
        .await
        .expect("list");
    assert_eq!(listed.len(), 2);
    assert_eq!(newest.backup_arn, listed.last().unwrap().backup_arn);
    assert_eq!(newest.backup_name, "newer");

    // A RUNNING restore record stamped a minute ahead of the coordinator.
    let table = TestTable::with_account(&engine, &account_id, "rr_target", false).await;
    let table_id = table.key_info.table_id.clone();
    let ahead = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 60_000;
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_status = 'RUNNING', restore_kind = 'legacy', \
             restore_owner_generation = 7, restore_heartbeat_at = ?, \
             restore_source_backup_arn = ? WHERE account_id = ? AND table_name = ? \
             IF table_id = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            ahead,
            newest.backup_arn.as_str(),
            account_id.as_str(),
            "rr_target",
            table_id.as_str()
        ),
    )
    .await;
    let running = engine
        .list_running_restore_jobs()
        .await
        .expect("list running");
    let ours: Vec<&extenddb_storage::backup_catalog::RestoreJobRow> = running
        .iter()
        .filter(|job| job.target.account_id == account_id)
        .collect();
    assert_eq!(ours.len(), 1, "{running:?}");
    assert_eq!(ours[0].target.table_name, "rr_target");
    assert_eq!(ours[0].target.table_id, table_id);
    assert_eq!(ours[0].status, JobStatus::Running);
    assert!(
        !engine
            .list_dead_restore_jobs(Duration::ZERO)
            .await
            .expect("list dead")
            .iter()
            .any(|job| job.target.account_id == account_id),
        "the age-based listing misses a heartbeat ahead of the clock"
    );
}
/// The index row of `row`, read back through the catalog.
async fn stored_index_row(engine: &CassandraEngine, row: &BackupRow) -> BackupRow {
    engine
        .read_backup_row(&arn_of(row))
        .await
        .expect("read")
        .expect("the row exists")
}

/// `claim_dead_backup`: refused while the owner's heartbeat is within the
/// window, on the coordinator's clock; once the window has passed (waited
/// out, not hand-dated) the row is taken over under a fresh generation with
/// its heartbeat left as the dead owner wrote it, the old claim finds its
/// heartbeat lost and its writes unmatched, and the successor's AVAILABLE
/// write carries the source table and a completion time and moves the status
/// index. A successor that heartbeats is a live owner to a third taker; a row
/// that is not CREATING, and a missing row, answer `None`.
#[tokio::test]
async fn test_claim_dead_backup_takes_over_a_stale_row_and_refuses_a_live_one() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = index_row(&account_id, "Music", "dead-owner", &store);
    let key = row.key();
    let window = Duration::from_secs(1);
    assert!(
        engine
            .claim_dead_backup(&key, window)
            .await
            .expect("claim")
            .is_none(),
        "a missing row"
    );
    let owner = engine.claim_backup(&row).await.expect("claim");
    let before = stored_index_row(&engine, &row).await;
    assert!(
        engine
            .claim_dead_backup(&key, Duration::from_secs(300))
            .await
            .expect("claim")
            .is_none(),
        "a live owner within a long window"
    );
    assert!(
        engine
            .claim_dead_backup(&key, window)
            .await
            .expect("claim")
            .is_none(),
        "a heartbeat written an instant ago is within a one-second window"
    );
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor = engine
        .claim_dead_backup(&key, window)
        .await
        .expect("claim")
        .expect("the window passed on the coordinator's clock: taken over");
    assert_ne!(successor.generation(), owner.generation());
    assert_ne!(successor.generation(), UNOWNED_GENERATION);
    let after = stored_index_row(&engine, &row).await;
    assert_eq!(after.owner_generation, successor.generation());
    assert_eq!(after.status, BackupStatus::Creating);
    assert_eq!(
        after.heartbeat_at, before.heartbeat_at,
        "the takeover left the heartbeat as the dead owner wrote it"
    );
    assert_eq!(
        owner.heartbeat().await.expect("heartbeat"),
        Heartbeat::Lost,
        "the old owner's heartbeat finds another generation"
    );
    assert_eq!(
        owner
            .set_status(
                JobStatus::Creating,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::NotMatched,
        "the old generation's write is fenced"
    );

    // The successor heartbeats: a third taker finds a live owner.
    assert_eq!(
        successor.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    assert!(
        engine
            .claim_dead_backup(&key, window)
            .await
            .expect("claim")
            .is_none(),
        "the successor's heartbeat is fresh"
    );

    // The successor's AVAILABLE write carries the manifest's source table.
    let mut source_table = row.source_table_details_json.clone();
    source_table["item_count"] = serde_json::json!(41);
    let fields = StatusFields {
        size_bytes: Some(4_096),
        item_count: Some(41),
        failure_reason: None,
        source_table_details_json: Some(source_table.clone()),
        completed: true,
    };
    assert_eq!(
        successor
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let available = stored_index_row(&engine, &row).await;
    assert_eq!(available.status, BackupStatus::Available);
    assert_eq!(available.size_bytes, 4_096);
    assert_eq!(available.item_count, 41);
    assert_eq!(
        available.source_table_details_json, source_table,
        "the write applied the source table"
    );
    assert!(available.completed_at.is_some(), "completed is stamped");
    let listed: Vec<BackupRow> = engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "the status index followed: {listed:?}");
    assert!(
        engine
            .claim_dead_backup(&key, Duration::ZERO)
            .await
            .expect("claim")
            .is_none(),
        "an AVAILABLE row is not taken over, whatever its heartbeat's age"
    );

    // A FAILED write without the member leaves the stored source table.
    let failed = index_row(&account_id, "Music", "dead-owner-failed", &store);
    let claim = engine.claim_backup(&failed).await.expect("claim");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Failed,
                &StatusFields {
                    failure_reason: Some("owner dead".to_owned()),
                    completed: true,
                    ..StatusFields::default()
                }
            )
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let stored = stored_index_row(&engine, &failed).await;
    assert_eq!(
        stored.source_table_details_json,
        failed.source_table_details_json
    );
    assert_eq!(stored.failure_reason.as_deref(), Some("owner dead"));
}

/// The `active_restores` of a `backups_by_arn` row, as the map's entries.
async fn registrations(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
) -> std::collections::HashMap<String, String> {
    use cdrs_tokio::types::AsRust;
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT active_restores FROM {}.backups_by_arn \
             WHERE account_id = ? AND backup_arn = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id, backup_arn),
        "registrations",
    )
    .await
    .expect("read the row");
    let map: Option<cdrs_tokio::types::map::Map> = rows
        .first()
        .expect("the row exists")
        .get_by_name("active_restores")
        .ok()
        .flatten();
    map.map(|map| map.as_rust().expect("map").unwrap_or_default())
        .unwrap_or_default()
}

/// `sweep_restore_registrations`: an entry whose target is a CREATING
/// restore target of the backup with the registered table id is kept; one
/// whose target is ACTIVE, one whose target restores from another backup and
/// one whose target has no row are removed, and the swept backup's ARN is
/// returned once. A second sweep finds nothing to do.
#[tokio::test]
async fn test_the_registrations_sweep_removes_stale_entries_and_keeps_a_live_one() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine("sw", false).await;
    let store = unique_store();
    let catalog = engine.catalog_keyspace();
    let row = index_row(&account_id, "Music", "registered", &store);
    let claim = engine.claim_backup(&row).await.expect("claim");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let other = index_row(&account_id, "Music", "other", &store);
    let other_claim = engine.claim_backup(&other).await.expect("claim");
    assert_eq!(
        other_claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let mut targets = Vec::new();
    for name in ["SwLive", "SwActive", "SwForeign"] {
        targets.push(
            create_active_table(
                &engine,
                &account_id,
                CreateTableInput {
                    table_name: name.to_owned(),
                    key_schema: vec![ks("id", KeyType::Hash)],
                    attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    let (live, active, foreign) = (&targets[0], &targets[1], &targets[2]);
    // A CREATING target restoring from the backup; a CREATING target
    // restoring from the other backup; an ACTIVE table; a name with no row.
    for (target, source) in [(live, &row), (foreign, &other)] {
        apply(
            &engine,
            &format!(
                "UPDATE {catalog}.tables SET table_status = 'CREATING', \
                 status_transition_at = null, restore_source_backup_arn = ? \
                 WHERE account_id = ? AND table_name = ? IF table_id = ?"
            ),
            cdrs_tokio::query_values!(
                source.backup_arn.as_str(),
                account_id.as_str(),
                target.table_name.as_str(),
                target.table_id.as_str()
            ),
        )
        .await;
    }
    let missing_id = uuid::Uuid::new_v4().to_string();
    for (table_id, name) in [
        (live.table_id.as_str(), "SwLive"),
        (active.table_id.as_str(), "SwActive"),
        (foreign.table_id.as_str(), "SwForeign"),
        (missing_id.as_str(), "SwMissing"),
    ] {
        apply(
            &engine,
            &format!(
                "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
                 WHERE account_id = ? AND backup_arn = ? IF EXISTS"
            ),
            cdrs_tokio::query_values!(table_id, name, account_id.as_str(), row.backup_arn.as_str()),
        )
        .await;
    }
    // The other backup registers its own live restore: nothing to sweep.
    apply(
        &engine,
        &format!(
            "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS"
        ),
        cdrs_tokio::query_values!(
            foreign.table_id.as_str(),
            "SwForeign",
            account_id.as_str(),
            other.backup_arn.as_str()
        ),
    )
    .await;
    assert_eq!(
        registrations(&engine, &account_id, &row.backup_arn)
            .await
            .len(),
        4
    );

    let swept = engine.sweep_restore_registrations().await.expect("sweep");
    assert_eq!(swept, std::slice::from_ref(&row.backup_arn));
    let left = registrations(&engine, &account_id, &row.backup_arn).await;
    assert_eq!(
        left,
        std::collections::HashMap::from([(live.table_id.clone(), "SwLive".to_owned())]),
        "the live entry is kept and the three stale ones are removed"
    );
    assert_eq!(
        registrations(&engine, &account_id, &other.backup_arn).await,
        std::collections::HashMap::from([(foreign.table_id.clone(), "SwForeign".to_owned())]),
        "the other backup's live entry is untouched"
    );
    assert!(
        engine
            .sweep_restore_registrations()
            .await
            .expect("sweep")
            .is_empty(),
        "a second sweep finds nothing"
    );
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        Some("SwLive".to_owned()),
        "the kept entry is the in-use rule's"
    );
}

// ── Store-backed (manifest) restore ──────────────────────────────────────────

/// An AVAILABLE catalog-index backup row (`store_kind` set) for `table`, the
/// row a store-backed restore registers against.
async fn available_index_backup(
    engine: &CassandraEngine,
    account_id: &str,
    table: &str,
    name: &str,
    store: &StoreIdentity,
) -> BackupRow {
    let row = index_row(account_id, table, name, store);
    let claim = engine.claim_backup(&row).await.expect("claim backup");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields {
                    completed: true,
                    ..StatusFields::default()
                }
            )
            .await
            .expect("make the backup AVAILABLE"),
        WriteOutcome::Matched
    );
    row
}

/// A hash-only `CreateTableInput` for a restore target.
fn manifest_target_input(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![ks("id", KeyType::Hash)],
        attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    }
}

/// The registration a store-backed restore of `row` writes, with `spool_host`.
fn manifest_registration(row: &BackupRow, spool_host: Option<&str>) -> RestoreRegistrationRow {
    RestoreRegistrationRow {
        backup: row.key(),
        source_table_arn: format!(
            "arn:aws:dynamodb:us-east-1:{}:table/{}",
            row.account_id, row.table_name
        ),
        restore_date_time: row.created_at,
        store: row.store.clone(),
        spool_host: spool_host.map(str::to_owned),
        owner_generation: UNOWNED_GENERATION,
    }
}

/// A hash-keyed item with key `k{i:05}`.
fn id_item(i: usize) -> Item {
    let mut it = BTreeMap::new();
    it.insert("id".to_owned(), AttributeValue::S(format!("k{i:05}")));
    it.insert("v".to_owned(), AttributeValue::S(format!("val{i}")));
    it
}

/// The items `lo..hi` as a `bulk_load` stream.
fn item_stream(
    lo: usize,
    hi: usize,
) -> futures::stream::BoxStream<'static, Result<Item, StorageError>> {
    let items: Vec<Item> = (lo..hi).map(id_item).collect();
    futures::stream::iter(items.into_iter().map(Ok)).boxed()
}

/// `claim_restore` registers a CREATING target with a RUNNING `manifest`
/// record carrying the store, the spool host and the claim's generation, the
/// `active_restores` entry on the backup, and a `RestoreSummary` on the
/// description; the claim heartbeats and reads the record.
#[tokio::test]
async fn test_claim_restore_registers_a_manifest_target_with_its_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let registration = manifest_registration(&row, Some("host-a:/var/spool"));

    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("RManifest"),
            &registration,
        )
        .await
        .expect("the registration creates the target and the record");
    assert_eq!(desc.table_status, TableStatus::Creating);
    let summary = desc.restore_summary.as_ref().expect("a RestoreSummary");
    assert_eq!(
        summary.source_backup_arn.as_deref(),
        Some(row.backup_arn.as_str())
    );
    assert!(summary.restore_in_progress);
    assert_ne!(claim.generation(), UNOWNED_GENERATION);

    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let record = engine
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("the record exists");
    assert_eq!(record.kind, RestoreKind::Manifest);
    assert_eq!(record.status, JobStatus::Running);
    assert_eq!(record.owner_generation, claim.generation());
    assert_eq!(record.store.as_ref(), Some(&store));
    assert_eq!(record.spool_host.as_deref(), Some("host-a:/var/spool"));
    assert_eq!(record.source_backup_arn, row.backup_arn);

    // The backup is in use by the target, and the heartbeat confirms and the
    // read returns RUNNING.
    assert_eq!(
        engine.backup_in_use(&row.key()).await.unwrap().as_deref(),
        Some("RManifest")
    );
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    assert_eq!(claim.read().await.expect("read").status, JobStatus::Running);
}

/// The two orderings against `start_backup_delete` on the backup's
/// `backup_index` row: a registration that lands first leaves the backup in
/// use; a DeleteBackup that lands first leaves the registration with no
/// backup to read.
#[tokio::test]
async fn test_claim_restore_and_start_backup_delete_order_on_the_backup_index_row() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();

    // Registration wins: DeleteBackup finds the backup in use by the target.
    let store = unique_store();
    let won = available_index_backup(&engine, &account_id, "Music", "a", &store).await;
    let (desc, _claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("WonTarget"),
            &manifest_registration(&won, None),
        )
        .await
        .expect("register");
    assert_eq!(desc.table_status, TableStatus::Creating);
    assert_eq!(
        engine.start_backup_delete(&won.key()).await.unwrap(),
        DeleteStart::InUse("WonTarget".to_owned())
    );

    // DeleteBackup wins: the registration finds the backup gone.
    let lost = available_index_backup(&engine, &account_id, "Music", "b", &store).await;
    assert_eq!(
        engine.start_backup_delete(&lost.key()).await.unwrap(),
        DeleteStart::Deleting
    );
    let refused = engine
        .claim_restore(
            &account_id,
            manifest_target_input("LostTarget"),
            &manifest_registration(&lost, None),
        )
        .await
        .err()
        .expect("a registration against a DELETING backup is Backup not found");
    assert!(
        matches!(refused, StorageError::Validation(ref m) if m.contains("Backup not found")),
        "{refused}"
    );
    // The lost registration left no target behind.
    assert!(
        engine
            .describe_table(
                &account_id,
                DescribeTableInput {
                    table_name: "LostTarget".to_owned()
                }
            )
            .await
            .is_err()
    );
}

/// A second restore of a backup whose target is CREATING is `BackupInUse`,
/// and a restore into a taken name is `TableAlreadyExists`.
#[tokio::test]
async fn test_a_second_restore_is_in_use_and_a_taken_name_is_refused() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;

    let (_desc, _claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("FirstTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("the first restore registers");

    // A second restore of the same backup, into a different name, is in use.
    let in_use = engine
        .claim_restore(
            &account_id,
            manifest_target_input("SecondTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .err()
        .expect("the backup is in use by the first restore");
    // The service's text names the backup, not the target.
    assert!(
        matches!(in_use, StorageError::BackupInUse(ref m) if *m == backup_in_use_message(&row.backup_arn)),
        "{in_use}"
    );

    // A restore into an existing table's name is TableAlreadyExists.
    create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "Taken".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    let other = available_index_backup(&engine, &account_id, "Music", "other", &store).await;
    let taken = engine
        .claim_restore(
            &account_id,
            manifest_target_input("Taken"),
            &manifest_registration(&other, None),
        )
        .await
        .err()
        .expect("the name is taken");
    assert!(
        matches!(taken, StorageError::TableAlreadyExists(ref name) if name == "Taken"),
        "{taken}"
    );
}

/// `claim_restore_dead`: a live owner, and a legacy record, are not taken
/// over; a stale manifest record is, under a fresh generation, with the
/// claimant's `restore_spool_host` written and `restore_heartbeat_at`
/// refreshed to the coordinator's clock in the same lightweight transaction,
/// so a second taker right after the first (before the successor's first
/// beat) finds the record live and is refused while the first taker's beat
/// confirms; the old claim's writes are then fenced.
#[tokio::test]
async fn test_claim_restore_dead_takes_over_a_stale_manifest_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, owner) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("Takeover"),
            &manifest_registration(&row, Some("owner:/s")),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let window = Duration::from_secs(1);
    let before = engine
        .read_restore_job(&key)
        .await
        .unwrap()
        .unwrap()
        .heartbeat_at;
    assert!(
        engine
            .claim_restore_dead(&key, Duration::from_secs(300), Some("other:/s"))
            .await
            .expect("claim")
            .is_none(),
        "a live owner within a long window is not taken over"
    );

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor = engine
        .claim_restore_dead(&key, window, Some("successor:/spool"))
        .await
        .expect("claim")
        .expect("the window passed: taken over");
    assert_ne!(successor.generation(), owner.generation());
    let after = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(after.owner_generation, successor.generation());
    assert_eq!(after.status, JobStatus::Running);
    assert_eq!(
        after.spool_host.as_deref(),
        Some("successor:/spool"),
        "the takeover wrote the claimant's spool host"
    );
    // A second taker right after the first, before the successor's first
    // beat: its serial read finds the fresh heartbeat and it is refused; the
    // first taker's beat confirms.
    let second = engine
        .claim_restore_dead(&key, window, Some("second:/s"))
        .await
        .expect("a second takeover attempt");
    assert!(
        second.is_none(),
        "a second taker right after the first is refused: {:?}",
        second.as_ref().map(|c| c.generation())
    );
    assert_eq!(
        successor.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed,
        "the first taker keeps the record"
    );
    assert!(
        after.heartbeat_at > before + 1.0,
        "the takeover refreshed the heartbeat: {} was {before}",
        after.heartbeat_at
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("epoch")
        .as_secs_f64();
    assert!(
        (now - after.heartbeat_at).abs() < 5.0,
        "the refreshed heartbeat is the coordinator's now: {} vs {now}",
        after.heartbeat_at
    );
    assert_eq!(owner.heartbeat().await.expect("heartbeat"), Heartbeat::Lost);
    assert_eq!(
        owner
            .set_status(
                JobStatus::Running,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::NotMatched,
        "the old generation's write is fenced"
    );

    // The successor heartbeats again: a third taker finds it live.
    assert_eq!(
        successor.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    assert!(
        engine
            .claim_restore_dead(&key, window, Some("third:/s"))
            .await
            .expect("claim")
            .is_none(),
        "the successor's heartbeat is fresh"
    );

    // A legacy record is not a manifest restore's to take, whatever its age.
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_kind = 'legacy' \
             WHERE account_id = ? AND table_name = ? IF table_id = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            account_id.as_str(),
            key.table_name.as_str(),
            key.table_id.as_str()
        ),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert!(
        engine
            .claim_restore_dead(&key, window, Some("x:/s"))
            .await
            .expect("claim")
            .is_none(),
        "a legacy record is not taken over by the manifest takeover"
    );
}

/// `claim_restore_dead` refuses the takeover when a heartbeat lands between
/// its serial read and its lightweight transaction: the LWT conditions on the
/// heartbeat it read, which the beat has moved.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_claim_restore_dead_refuses_a_beat_between_the_read_and_the_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, owner) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("BeatRace"),
            &manifest_registration(&row, Some("owner:/s")),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    tokio::time::sleep(Duration::from_millis(1_300)).await;

    let pause =
        extenddb_storage_cassandra::test_hooks::pause_before_restore_takeover(&key.table_id);
    let takeover = {
        let engine = engine.clone();
        let key = key.clone();
        tokio::spawn(async move {
            engine
                .claim_restore_dead(&key, Duration::from_secs(1), Some("successor:/s"))
                .await
        })
    };
    pause.reached().await;
    // A beat lands after the takeover read its stale heartbeat.
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_heartbeat_at = toTimestamp(now()) \
             WHERE account_id = ? AND table_name = ? IF table_id = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            account_id.as_str(),
            key.table_name.as_str(),
            key.table_id.as_str()
        ),
    )
    .await;
    pause.release();
    assert!(
        takeover
            .await
            .expect("takeover task")
            .expect("claim")
            .is_none(),
        "the beat between the read and the write refuses the takeover"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        record.owner_generation,
        owner.generation(),
        "the record is still the original owner's"
    );
    assert_eq!(
        record.spool_host.as_deref(),
        Some("owner:/s"),
        "the refused takeover did not rewrite the spool host"
    );
}

/// A record whose backup does not list its target (the owner died between
/// the target's creation and the `active_restores` registration): the
/// takeover re-applies the entry, so the backup is in use for the resumed
/// load and `start_backup_delete` is refused.
#[tokio::test]
async fn test_claim_restore_dead_reapplies_a_missing_active_restores_entry() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, owner) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("Unlisted"),
            &manifest_registration(&row, Some("owner:/s")),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // The crash's shape: the target and its RUNNING record exist, the
    // backup's entry does not.
    apply(
        &engine,
        &format!(
            "DELETE active_restores[?] FROM {}.backups_by_arn \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            key.table_id.as_str(),
            account_id.as_str(),
            row.backup_arn.as_str()
        ),
    )
    .await;
    assert!(
        registrations(&engine, &account_id, &row.backup_arn)
            .await
            .is_empty(),
        "the entry is gone"
    );
    drop(owner);
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor = engine
        .claim_restore_dead(&key, Duration::from_secs(1), Some("successor:/s"))
        .await
        .expect("claim")
        .expect("the stale record is taken over");
    let entries = registrations(&engine, &account_id, &row.backup_arn).await;
    assert_eq!(
        entries.get(&key.table_id).map(String::as_str),
        Some("Unlisted"),
        "the takeover re-applied the backup's entry for the target: {entries:?}"
    );
    assert_eq!(
        engine.start_backup_delete(&row.key()).await.unwrap(),
        DeleteStart::InUse("Unlisted".to_owned()),
        "the backup is in use for the resumed load"
    );
    assert_eq!(
        engine.backup_in_use(&row.key()).await.unwrap().as_deref(),
        Some("Unlisted")
    );
    drop(successor);
}

/// A DeleteBackup that lands between a registration's unfenced pre-read and
/// its `active_restores` lightweight transaction: the transaction's own
/// `backup_status = 'AVAILABLE'` condition refuses the registration, which
/// answers `Backup not found` and leaves no target and no entry.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_delete_between_the_registrations_pre_read_and_its_fence_leaves_no_target() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "raced", &store).await;
    let pause =
        extenddb_storage_cassandra::test_hooks::pause_before_restore_registration(&row.backup_arn);
    let registration = {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let registration = manifest_registration(&row, None);
        tokio::spawn(async move {
            engine
                .claim_restore(
                    &account_id,
                    manifest_target_input("RacedTarget"),
                    &registration,
                )
                .await
        })
    };
    // The registration has read the backup AVAILABLE and created the target;
    // the delete lands before its fence.
    pause.reached().await;
    assert_eq!(
        engine.start_backup_delete(&row.key()).await.unwrap(),
        DeleteStart::Deleting,
        "nothing holds the backup before the registration's fence"
    );
    pause.release();
    let refused = registration
        .await
        .expect("registration task")
        .err()
        .expect("the fence refuses the registration");
    assert!(
        matches!(refused, StorageError::Validation(ref m) if m.contains("Backup not found")),
        "{refused}"
    );
    assert!(
        matches!(
            engine
                .describe_table(
                    &account_id,
                    DescribeTableInput {
                        table_name: "RacedTarget".to_owned()
                    }
                )
                .await,
            Err(StorageError::TableNotFound(_))
        ),
        "the refused registration left no target"
    );
    assert!(
        registrations(&engine, &account_id, &row.backup_arn)
            .await
            .is_empty(),
        "no entry on the DELETING backup"
    );
}

/// Two concurrent registrations of one backup leave one target: the in-use
/// pre-read is not atomic with the `active_restores` lightweight
/// transaction, so both can pass it (the first held before its fence while
/// the second runs whole), but the fence's `active_restores = null` admits
/// one; the other finds the registered target in its re-read, is
/// `BackupInUse` naming it, and removes its own target.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_two_concurrent_registrations_of_one_backup_leave_one_target() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "twice", &store).await;
    let pause =
        extenddb_storage_cassandra::test_hooks::pause_before_restore_registration(&row.backup_arn);
    let first = {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let registration = manifest_registration(&row, None);
        tokio::spawn(async move {
            engine
                .claim_restore(
                    &account_id,
                    manifest_target_input("FirstOfTwo"),
                    &registration,
                )
                .await
        })
    };
    // The first registration has passed the pre-read (no restore registered)
    // and created its target; the second runs whole before the first's fence.
    pause.reached().await;
    let (second_desc, second_claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("SecondOfTwo"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("the second registration, whole before the first's fence");
    pause.release();
    let refused = first
        .await
        .expect("registration task")
        .err()
        .expect("the first's fence finds the second registered");
    assert!(
        matches!(refused, StorageError::BackupInUse(ref m) if *m == backup_in_use_message(&row.backup_arn)),
        "{refused}"
    );
    assert!(
        matches!(
            engine
                .describe_table(
                    &account_id,
                    DescribeTableInput {
                        table_name: "FirstOfTwo".to_owned()
                    }
                )
                .await,
            Err(StorageError::TableNotFound(_))
        ),
        "the refused registration left no target"
    );
    assert_eq!(
        registrations(&engine, &account_id, &row.backup_arn).await,
        std::collections::HashMap::from([(second_desc.table_id.clone(), "SecondOfTwo".to_owned())]),
        "one entry on the backup, the second's"
    );
    assert_eq!(
        engine
            .backup_in_use(&row.key())
            .await
            .expect("in use")
            .as_deref(),
        Some("SecondOfTwo")
    );
    assert_eq!(
        second_claim.heartbeat().await.expect("beat"),
        Heartbeat::Confirmed,
        "the second registration's claim is live"
    );
}

/// A registration whose record a reconciler took over during its target's
/// DDL (no heartbeat runs there, so a DDL that outlasts the stale window
/// leaves the record dead) removes nothing: its `active_restores` transaction
/// finds the successor's entry, and its removal of the target is conditioned
/// on its own generation, which the takeover moved. The target stays, the
/// record is the successor's, and the registration answers an error naming
/// the lost claim rather than the refusal.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_registration_taken_over_during_its_ddl_leaves_the_successors_target() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "slowddl", &store).await;
    let pause =
        extenddb_storage_cassandra::test_hooks::pause_before_restore_registration(&row.backup_arn);
    let registration = {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let registration = manifest_registration(&row, Some("registrant:/s"));
        tokio::spawn(async move {
            engine
                .claim_restore(
                    &account_id,
                    composite_target_input("SlowDdl"),
                    &registration,
                )
                .await
        })
    };
    // The target's row and DDL exist; the registration is held before its
    // `active_restores` transaction, as a DDL past the stale window would
    // hold it. The record's heartbeat is the row insert's.
    pause.reached().await;
    let desc = describe(&engine, &account_id, "SlowDdl").await;
    assert_eq!(desc.table_status, TableStatus::Creating);
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let registrant_generation = engine
        .read_restore_job(&key)
        .await
        .unwrap()
        .unwrap()
        .owner_generation;
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor = engine
        .claim_restore_dead(&key, Duration::from_secs(1), Some("successor:/s"))
        .await
        .expect("claim")
        .expect("the registration's record is stale and taken over");
    assert_ne!(successor.generation(), registrant_generation);
    assert_eq!(
        registrations(&engine, &account_id, &row.backup_arn).await,
        std::collections::HashMap::from([(key.table_id.clone(), "SlowDdl".to_owned())]),
        "the takeover re-applied the target's entry"
    );

    pause.release();
    let outcome = registration.await.expect("registration task");

    // The successor's target, record and registration stand.
    let target = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: "SlowDdl".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(&target, Ok(desc) if desc.table_status == TableStatus::Creating),
        "the registrant's refusal path left the successor's target in place: {target:?}"
    );
    let lost = outcome
        .err()
        .expect("the registration does not return a claim on a record another owns");
    assert!(
        matches!(&lost, StorageError::Internal(m) if m.contains("lost its record to a takeover")),
        "the registration names the lost claim, not the refusal: {lost}"
    );
    let record = engine
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("the record stands");
    assert_eq!(record.status, JobStatus::Running);
    assert_eq!(record.owner_generation, successor.generation());
    assert_eq!(record.spool_host.as_deref(), Some("successor:/s"));
    assert_eq!(
        registrations(&engine, &account_id, &row.backup_arn).await,
        std::collections::HashMap::from([(key.table_id.clone(), "SlowDdl".to_owned())]),
        "the successor's entry on the backup stands"
    );
    assert_eq!(
        successor.heartbeat().await.expect("beat"),
        Heartbeat::Confirmed
    );
    // The successor loads into the target the registrant left: its data and
    // index tables are there.
    assert_eq!(
        engine
            .bulk_load(
                &key,
                &*successor,
                BulkLoadFile {
                    file_index: 0,
                    start_at: 0,
                    reapplying: false,
                },
                futures::stream::iter((0..6).map(composite_item).map(Ok)).boxed(),
            )
            .await
            .expect("the successor's load"),
        6
    );
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 6);
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        3
    );
}

/// A composite-key restore target with a GSI and an LSI.
fn composite_target_input(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![ks("id", KeyType::Hash), ks("sk", KeyType::Range)],
        attribute_definitions: vec![
            attr("id", ScalarAttributeType::S),
            attr("sk", ScalarAttributeType::N),
            attr("g", ScalarAttributeType::S),
            attr("l", ScalarAttributeType::S),
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gix".to_owned(),
            key_schema: vec![ks("g", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "lix".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash), ks("l", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }]),
        ..Default::default()
    }
}

/// Item `i` of the composite fixture: `id` one of five, `sk` the index,
/// every second item carrying the GSI's key `g`, every third the LSI's `l`.
fn composite_item(i: usize) -> Item {
    let mut it = BTreeMap::new();
    it.insert("id".to_owned(), AttributeValue::S(format!("p{}", i % 5)));
    it.insert("sk".to_owned(), AttributeValue::N(i.to_string()));
    if i.is_multiple_of(2) {
        it.insert("g".to_owned(), AttributeValue::S(format!("g{}", i % 4)));
    }
    if i.is_multiple_of(3) {
        it.insert("l".to_owned(), AttributeValue::S(format!("l{i}")));
    }
    it
}

/// The rows of the target's secondary index `index_name`.
async fn index_rows(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
    index_name: &str,
) -> i64 {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT index_id FROM {}.indexes WHERE table_id = ? AND index_name = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(table_id, index_name),
        "index id",
    )
    .await
    .expect("read the index");
    let index_id: String = rows
        .first()
        .expect("the index exists")
        .get_by_name("index_id")
        .ok()
        .flatten()
        .expect("index_id");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT COUNT(*) AS c FROM {}.{}",
            engine.account_keyspace(account_id),
            extenddb_storage_cassandra::data::ddl::index_table_name(&index_id)
        ),
        cdrs_tokio::query_values!(),
        "index rows",
    )
    .await
    .expect("count the index rows");
    rows.first()
        .expect("a count")
        .get_by_name("c")
        .ok()
        .flatten()
        .expect("c")
}

/// Per item of the composite fixture (keyed by its `sk`), the base row's
/// `version` when `index_name` is `None`, otherwise the write timestamp of
/// the item's row in that secondary index (`WRITETIME(item_data)`).
#[cfg(feature = "test-hooks")]
async fn stamps_by_sk(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
    index_name: Option<&str>,
) -> BTreeMap<String, i64> {
    use cdrs_tokio::types::IntoRustByName;
    let keyspace = engine.account_keyspace(account_id);
    let cql = match index_name {
        None => format!(
            "SELECT item_data, version AS stamp FROM {keyspace}.items_{}",
            table_id.replace('-', "_")
        ),
        Some(index_name) => {
            let rows =
                extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
                    &engine.session_arc(),
                    &format!(
                        "SELECT index_id FROM {}.indexes WHERE table_id = ? AND index_name = ?",
                        engine.catalog_keyspace()
                    ),
                    cdrs_tokio::query_values!(table_id, index_name),
                    "index id",
                )
                .await
                .expect("read the index");
            let index_id: String = rows
                .first()
                .expect("the index exists")
                .get_by_name("index_id")
                .ok()
                .flatten()
                .expect("index_id");
            format!(
                "SELECT item_data, WRITETIME(item_data) AS stamp FROM {keyspace}.{}",
                extenddb_storage_cassandra::data::ddl::index_table_name(&index_id)
            )
        }
    };
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(),
        "stamps",
    )
    .await
    .expect("read the stamps");
    rows.into_iter()
        .map(|row| {
            let json: String = row.get_r_by_name("item_data").expect("item_data");
            let image: Item = serde_json::from_str(&json).expect("an item image");
            let Some(AttributeValue::N(sk)) = image.get("sk") else {
                panic!("no sk in {image:?}");
            };
            let stamp: i64 = row.get_r_by_name("stamp").expect("a stamp");
            (sk.clone(), stamp)
        })
        .collect()
}

/// Every GSI and LSI row of the composite target carries its base row's
/// `version` as its write timestamp, the stamp the write path gives the
/// index rows of each version it stores (ADR-0027); returns the base
/// versions.
#[cfg(feature = "test-hooks")]
async fn assert_index_rows_carry_the_base_versions(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
) -> BTreeMap<String, i64> {
    let versions = stamps_by_sk(engine, account_id, table_id, None).await;
    for index_name in ["gix", "lix"] {
        let stamps = stamps_by_sk(engine, account_id, table_id, Some(index_name)).await;
        assert!(!stamps.is_empty(), "{index_name} has rows");
        for (sk, stamp) in &stamps {
            assert_eq!(
                Some(stamp),
                versions.get(sk),
                "the {index_name} row of item {sk} carries its base row's version"
            );
        }
    }
    versions
}

/// `bulk_load` into a composite-key target with a GSI and an LSI: a file
/// resumed after its first three items (`start_at: 3`) applies exactly the
/// rest and records `start_at` plus the items applied as the file's total;
/// the GSI and LSI rows are written with the base rows; an empty file, and a
/// file resumed at its end, record their progress.
#[tokio::test]
async fn test_bulk_load_resumes_mid_file_with_its_index_rows_and_records_an_empty_file() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            composite_target_input("Composite"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register a composite target");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // File 0 resumed after three of its ten items.
    let rest: Vec<Item> = (3..10).map(composite_item).collect();
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 3,
                reapplying: false,
            },
            futures::stream::iter(rest.into_iter().map(Ok)).boxed(),
        )
        .await
        .expect("the rest of file 0");
    assert_eq!(applied, 7, "exactly the rest");
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.file_index, Some(0));
    assert_eq!(
        record.items_applied,
        Some(10),
        "the running total is start_at plus the items applied"
    );
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 7);
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        3,
        "the GSI rows of items 4, 6 and 8"
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        3,
        "the LSI rows of items 3, 6 and 9"
    );

    // An empty file records its progress as complete.
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 0,
                reapplying: false,
            },
            futures::stream::iter(Vec::<Result<Item, StorageError>>::new()).boxed(),
        )
        .await
        .expect("an empty file");
    assert_eq!(applied, 0);
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(1), Some(0)),
        "an empty file's progress is recorded"
    );
    // A file resumed at its end: `start_at` is the recorded total.
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 2,
                start_at: 4,
                reapplying: false,
            },
            futures::stream::iter(Vec::<Result<Item, StorageError>>::new()).boxed(),
        )
        .await
        .expect("a file resumed at its end");
    assert_eq!(applied, 0);
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(2), Some(4))
    );

    // Another account's key, and the target once ACTIVE, name no CREATING
    // restore target: `count_restore_target` is `TableNotFound`.
    let foreign = RestoreTargetKey {
        account_id: "111111111111".to_owned(),
        ..key.clone()
    };
    assert!(matches!(
        engine.count_restore_target(&foreign).await,
        Err(StorageError::TableNotFound(_))
    ));
    assert_eq!(
        engine
            .finish_restore(&key, &*claim, 7, 4_096)
            .await
            .expect("finish"),
        Finished::Active
    );
    assert!(
        matches!(
            engine.count_restore_target(&key).await,
            Err(StorageError::TableNotFound(_))
        ),
        "an ACTIVE table is not a CREATING restore target"
    );
}

/// A file resumed mid-way (`start_at: 3`) with more than one batch left:
/// the progress written after each batch inside the loop is `start_at` plus
/// the items applied so far, as the write after the tail is, so a second
/// crash between two of the resumed file's batches resumes from a running
/// total that counts the items the first run applied. Three items per batch,
/// seven items: the record reads 6 after the first batch, 9 after the
/// second, 10 at the end.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_mid_file_resume_records_start_at_in_every_batch_progress() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 3);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("ResumedBatches"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // The stream reads the record when asked for the first item of the
    // second and of the third batch: by then the previous batch's progress
    // is written.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_by_stream = std::sync::Arc::clone(&seen);
    let reader = engine.clone();
    let read_key = key.clone();
    let rest: futures::stream::BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::unfold(3usize, move |i| {
            let reader = reader.clone();
            let key = read_key.clone();
            let seen = std::sync::Arc::clone(&seen_by_stream);
            async move {
                if i >= 10 {
                    return None;
                }
                if i == 6 || i == 9 {
                    let record = reader.read_restore_job(&key).await.unwrap().unwrap();
                    seen.lock()
                        .expect("seen")
                        .push((record.file_index, record.items_applied));
                }
                Some((Ok(id_item(i)), i + 1))
            }
        }));
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 3,
                reapplying: false,
            },
            rest,
        )
        .await
        .expect("the rest of file 0");
    assert_eq!(applied, 7, "exactly the rest");
    assert_eq!(
        *seen.lock().expect("seen"),
        vec![(Some(0), Some(6)), (Some(0), Some(9))],
        "each batch's progress is start_at plus the items applied so far"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(10)),
        "the tail's progress is the file's total"
    );
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 7);
}

/// A progress lightweight transaction whose outcome is unknown (a write
/// timeout, a lost connection) does not fail the file: the record is re-read
/// at LOCAL_SERIAL. One whose transaction committed (the answer alone was
/// lost) is found applied and the file goes on; one whose transaction never
/// ran is found carrying the earlier progress and is written once more; a
/// second unknown outcome the record does not carry fails the file with an
/// error naming it, and the batches before it stand with their progress.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_an_unknown_progress_write_outcome_is_decided_by_a_re_read() {
    use extenddb_storage_cassandra::test_hooks::ProgressWriteFault;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 3);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("UnknownProgress"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };

    // File 0, three batches: the first batch's write lands but its answer is
    // lost (the re-read finds the values written), the second's never runs
    // (the re-read finds the first batch's values; written once more), the
    // tail's runs as usual. The file completes with every item and its
    // progress.
    extenddb_storage_cassandra::test_hooks::fail_progress_writes(
        &account_id,
        &[ProgressWriteFault::Landed, ProgressWriteFault::Lost],
    );
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 7),
        )
        .await
        .expect("an unknown outcome the record decides does not fail the file");
    assert_eq!(applied, 7);
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(7)),
        "the file's progress is recorded"
    );
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 7);

    // File 1: the first batch's write is lost twice over; the second unknown
    // outcome fails the file. The batch's items landed (the writes are
    // idempotent upserts a resume rewrites) and the record carries file 0's
    // progress, so a resume re-applies file 1 from its start.
    extenddb_storage_cassandra::test_hooks::fail_progress_writes(
        &account_id,
        &[ProgressWriteFault::Lost, ProgressWriteFault::Lost],
    );
    let failed = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 0,
                reapplying: false,
            },
            item_stream(7, 14),
        )
        .await
        .expect_err("two unknown outcomes the record does not carry fail the file");
    assert!(
        matches!(&failed, StorageError::Internal(m) if m.contains("had no known outcome twice")),
        "{failed}"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(7)),
        "file 1 recorded no progress"
    );
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count"),
        10,
        "the first batch of file 1 landed before its progress write"
    );
    assert_eq!(
        claim.heartbeat().await.expect("beat"),
        Heartbeat::Confirmed,
        "the claim is this job's throughout"
    );
}

/// A batch closes at the first item whose text takes it past
/// `RESTORE_BATCH_BYTES`, long before the count cap: a file of 100 items of
/// about 350 KB each (35 MB of text against the 32 MiB bound) is written in
/// two batches, the first's progress recorded before the stream hands over
/// its last item.
#[tokio::test]
async fn test_a_batch_closes_at_the_byte_bound_before_the_count_cap() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const N: usize = 100;
    const TEXT: usize = 350_000;
    const {
        assert!(N < extenddb_storage::restore_engine::RESTORE_BATCH_ITEMS);
        // 100 items cross the byte bound once.
        assert!(
            N * TEXT > extenddb_storage::restore_engine::RESTORE_BATCH_BYTES
                && (N - 2) * TEXT < 2 * extenddb_storage::restore_engine::RESTORE_BATCH_BYTES
        );
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("BigItems"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // The stream reads the record when asked for the last item: by then the
    // byte bound has closed the first batch and recorded its progress.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let seen_by_stream = std::sync::Arc::clone(&seen);
    let reader = engine.clone();
    let read_key = key.clone();
    let big: futures::stream::BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::unfold(0usize, move |i| {
            let reader = reader.clone();
            let key = read_key.clone();
            let seen = std::sync::Arc::clone(&seen_by_stream);
            async move {
                if i >= N {
                    return None;
                }
                if i == N - 1 {
                    let record = reader.read_restore_job(&key).await.unwrap().unwrap();
                    *seen.lock().expect("seen") = Some((record.file_index, record.items_applied));
                }
                let mut item = id_item(i);
                item.insert("v".to_owned(), AttributeValue::S("x".repeat(TEXT)));
                Some((Ok(item), i + 1))
            }
        }));
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            big,
        )
        .await
        .expect("bulk_load");
    assert_eq!(applied as usize, N);
    let (file_index, items_applied) = seen
        .lock()
        .expect("seen")
        .expect("the stream read the record at its last item");
    assert_eq!(file_index, Some(0), "the first batch recorded its progress");
    let first_batch = items_applied.expect("the first batch's total") as usize;
    assert!(
        0 < first_batch && first_batch < N,
        "the first batch closed at the byte bound with {first_batch} of {N} items"
    );
    assert!(
        first_batch * TEXT
            < extenddb_storage::restore_engine::RESTORE_BATCH_BYTES + TEXT + 1_000 * N,
        "the batch closed at the first item past the bound: {first_batch} items"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(N as u64))
    );
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count") as usize,
        N
    );
}

/// A claim lost between two batches of one file (the record's generation
/// moved by a takeover while the load ran): the read before the next batch
/// finds the record no longer this claim's, the file ends as a lost claim,
/// and the second batch's items never land.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_bulk_load_stops_at_a_lost_claim_between_two_batches() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 3);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("LostMidFile"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // The stream hands the first batch's three items, then moves the
    // record's generation when asked for the first item of the second.
    let mover = engine.clone();
    let moved_key = key.clone();
    let generation = i64::try_from(claim.generation()).unwrap();
    let stream: futures::stream::BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::unfold(0usize, move |i| {
            let mover = mover.clone();
            let key = moved_key.clone();
            async move {
                if i >= 7 {
                    return None;
                }
                if i == 3 {
                    apply(
                        &mover,
                        &format!(
                            "UPDATE {}.tables SET restore_owner_generation = ? \
                             WHERE account_id = ? AND table_name = ? \
                             IF table_id = ? AND restore_owner_generation = ?",
                            mover.catalog_keyspace()
                        ),
                        cdrs_tokio::query_values!(
                            generation + 1,
                            key.account_id.as_str(),
                            key.table_name.as_str(),
                            key.table_id.as_str(),
                            generation
                        ),
                    )
                    .await;
                }
                Some((Ok(id_item(i)), i + 1))
            }
        }));
    let out = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream,
        )
        .await;
    assert!(
        matches!(&out, Err(StorageError::Internal(m)) if m.contains("claim on the target was lost")),
        "the read before the second batch finds another owner: {out:?}"
    );
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count"),
        3,
        "the first batch's items alone landed"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(3)),
        "the first batch's progress stands"
    );
}

/// `bulk_load` crosses a batch boundary, recording the file's running total
/// after each batch, and a re-applied batch changes nothing observable: the
/// writes are idempotent upserts of the base rows and of each GSI and LSI row
/// through the ordinary put path (an index row whose key did not change is
/// rewritten by its `INSERT` alone, and each index row carries its base row's
/// `version` as its write timestamp, ADR-0027), so the base count and both
/// index row counts equal those of the first load, after the last batch alone
/// is re-applied (`start_at: 8`, a resume's shape) and after the whole file
/// is, and every index row's stamp follows its base row's version.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_bulk_load_crosses_a_batch_boundary_and_a_reapply_changes_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    // Four items per batch, so twelve items cross two boundaries.
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 4);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            composite_target_input("BulkTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let composite_items = |range: std::ops::Range<usize>| {
        futures::stream::iter(range.map(composite_item).map(Ok)).boxed()
    };
    // Items 0, 2, 4, 6, 8 and 10 carry the GSI's key; 0, 3, 6 and 9 the LSI's.
    let (gsi_rows, lsi_rows) = (6, 4);

    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            composite_items(0..12),
        )
        .await
        .expect("bulk_load");
    assert_eq!(applied, 12);
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.file_index, Some(0));
    assert_eq!(record.items_applied, Some(12));
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 12);
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        gsi_rows
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        lsi_rows
    );
    let loaded =
        assert_index_rows_carry_the_base_versions(&engine, &account_id, &key.table_id).await;
    assert_eq!(loaded.len(), 12);

    // The last batch re-applied (a resume whose progress write was lost):
    // the same base and index rows.
    let reapplied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 8,
                reapplying: true,
            },
            composite_items(8..12),
        )
        .await
        .expect("bulk_load re-apply of the last batch");
    assert_eq!(reapplied, 4);
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 12);
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        gsi_rows,
        "a re-applied batch keeps every GSI row"
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        lsi_rows,
        "a re-applied batch keeps every LSI row"
    );
    // The re-applied items' base rows moved to a later version and their
    // index rows with them; the others kept both.
    let reapplied_versions =
        assert_index_rows_carry_the_base_versions(&engine, &account_id, &key.table_id).await;
    for (sk, version) in &reapplied_versions {
        if sk.parse::<usize>().expect("an integer sk") >= 8 {
            assert!(version > &loaded[sk], "item {sk} was written again");
        } else {
            assert_eq!(version, &loaded[sk], "item {sk} was not written again");
        }
    }

    // The whole file re-applied: the count stays 12 and the index rows stand.
    let reapplied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: true,
            },
            composite_items(0..12),
        )
        .await
        .expect("bulk_load re-apply");
    assert_eq!(reapplied, 12);
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count"),
        12,
        "a re-applied file changes nothing"
    );
    assert_eq!(
        (
            index_rows(&engine, &account_id, &key.table_id, "gix").await,
            index_rows(&engine, &account_id, &key.table_id, "lix").await
        ),
        (gsi_rows, lsi_rows),
        "a re-applied file keeps every index row"
    );
    assert_index_rows_carry_the_base_versions(&engine, &account_id, &key.table_id).await;
}

/// An unordered backup carrying one key in two data files applies both items
/// (the second overwrites the first), so `count_restore_target` is one short
/// of the manifest's total: the shortfall is the duplicate, which the
/// engine's count check turns into a FAILED restore.
#[tokio::test]
async fn test_an_unordered_duplicate_key_is_caught_by_the_count_alone() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("DupTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // File 0: items 0..6. File 1: items 6..12 and item 0 again (the backup
    // holds k00000 twice). The manifest's total is 13 items.
    engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 6),
        )
        .await
        .expect("file 0");
    let file_one: Vec<Item> = (6..12)
        .map(id_item)
        .chain(std::iter::once(id_item(0)))
        .collect();
    engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 0,
                reapplying: false,
            },
            futures::stream::iter(file_one.into_iter().map(Ok)).boxed(),
        )
        .await
        .expect("file 1");
    let count = engine.count_restore_target(&key).await.expect("count");
    assert_eq!(count, 12, "the duplicated key collapsed into one row");
    assert!(count < 13, "the count is one short of the manifest's 13");
}

/// `bulk_load` stops with a lost-claim error when the record's generation has
/// moved (the reconciler took it over): the progress lightweight transaction
/// matches no RUNNING record under the claim's generation.
#[tokio::test]
async fn test_bulk_load_is_refused_when_the_claim_was_lost() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("LostTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // Another owner bumps the generation.
    apply(
        &engine,
        &format!(
            "UPDATE {}.tables SET restore_owner_generation = ? \
             WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND restore_owner_generation = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            i64::try_from(claim.generation()).unwrap() + 1,
            account_id.as_str(),
            key.table_name.as_str(),
            key.table_id.as_str(),
            i64::try_from(claim.generation()).unwrap()
        ),
    )
    .await;
    let error = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 3),
        )
        .await
        .expect_err("the progress write finds another owner");
    assert!(
        error.to_string().contains("claim on the target was lost"),
        "{error}"
    );
}

/// `finish_restore` flips the target ACTIVE and marks the record DONE, and a
/// stale generation's `finish_restore` is `NotMatched`.
#[tokio::test]
async fn test_finish_restore_flips_the_target_and_refuses_a_stale_generation() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("FinishTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 5),
        )
        .await
        .expect("bulk_load");
    let count = engine.count_restore_target(&key).await.expect("count");
    assert_eq!(count, 5);

    assert_eq!(
        engine
            .finish_restore(&key, &*claim, count, 4_096)
            .await
            .expect("finish"),
        Finished::Active
    );
    let described = describe(&engine, &account_id, "FinishTarget").await;
    assert_eq!(described.table_status, TableStatus::Active);
    assert_eq!(described.item_count, 5);

    // A second flip, the record no longer RUNNING, matches nothing.
    assert!(matches!(
        engine
            .finish_restore(&key, &*claim, count, 4_096)
            .await
            .expect("finish again"),
        Finished::NotMatched(_)
    ));
}

/// After a claim writes the record FAILED (leaving `table_status` alone),
/// `remove_restore_target` performs the two-write removal (CREATING to
/// DELETING, then the drop) and the target is gone.
#[tokio::test]
async fn test_remove_restore_target_after_a_failed_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("FailTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    assert_eq!(
        claim
            .set_status(
                JobStatus::Running,
                JobStatus::Failed,
                &StatusFields {
                    failure_reason: Some("a data file failed".to_owned()),
                    completed: true,
                    ..StatusFields::default()
                }
            )
            .await
            .expect("fail the record"),
        WriteOutcome::Matched
    );
    // The FAILED write left the table CREATING; the record carries the reason.
    assert_eq!(
        describe(&engine, &account_id, "FailTarget")
            .await
            .table_status,
        TableStatus::Creating,
        "the FAILED write leaves table_status alone for the removal's two-write step"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.status, JobStatus::Failed);
    assert_eq!(record.failure_reason.as_deref(), Some("a data file failed"));

    assert!(
        engine.remove_restore_target(&key).await.expect("remove"),
        "the two-write removal of a FAILED target succeeds"
    );
    assert!(
        matches!(
            engine
                .describe_table(
                    &account_id,
                    DescribeTableInput {
                        table_name: "FailTarget".to_owned()
                    }
                )
                .await,
            Err(StorageError::TableNotFound(_))
        ),
        "the target is gone"
    );
    assert!(
        engine.read_restore_job(&key).await.expect("read").is_none(),
        "the record went with the target"
    );
}

/// The liveness test inside the takeover write: with the owner's beat
/// landing between `claim_dead_backup`'s `LOCAL_SERIAL` read (which found
/// the heartbeat stale) and its LWT, the LWT's `heartbeat_at = <read>`
/// condition fails and the takeover answers `None`; the row keeps the
/// owner's generation and the owner's next beat confirms. Without that
/// condition the staleness comparison alone would decide, and a live owner's
/// job would be failed by the reconciler.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_beat_between_the_takeovers_read_and_its_write_refuses_the_takeover() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = index_row(&account_id, "Music", "beat-in-the-gap", &store);
    let key = row.key();
    let window = Duration::from_secs(1);
    let owner = engine.claim_backup(&row).await.expect("claim");
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let pause =
        extenddb_storage_cassandra::test_hooks::pause_before_takeover_write(&row.backup_arn);
    let taker = {
        let engine = engine.clone();
        let key = key.clone();
        tokio::spawn(async move { engine.claim_dead_backup(&key, window).await })
    };
    pause.reached().await;
    // The read found the beat stale; the owner beats now.
    assert_eq!(
        owner.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    pause.release();
    let taken = taker.await.expect("the taker's task").expect("claim");
    assert!(
        taken.is_none(),
        "the LWT's heartbeat condition refuses a row whose owner beat in the gap"
    );
    let after = stored_index_row(&engine, &row).await;
    assert_eq!(after.owner_generation, owner.generation());
    assert_eq!(after.status, BackupStatus::Creating);
    assert_eq!(
        owner.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed,
        "the owner's claim is intact"
    );
    // Without the beat, the same stale row is taken over.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor = engine
        .claim_dead_backup(&key, window)
        .await
        .expect("claim")
        .expect("taken over once no beat lands in the gap");
    assert_ne!(successor.generation(), owner.generation());
}

// ── The batch's timestamp fence, the first batch resumed without progress, the
// ── readiness check's index tables, the in-use rule of a running restore ───────

/// The items of a target's data table (rows with an `item_data`: a delete
/// leaves the partition's static delete stamp behind, so `COUNT(*)` would
/// count a deleted item's partition), counted whatever the table's status
/// (`count_restore_target` refuses a target no longer CREATING).
#[cfg(feature = "test-hooks")]
async fn base_rows(engine: &CassandraEngine, account_id: &str, table_id: &str) -> i64 {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT COUNT(item_data) AS c FROM {}.items_{}",
            engine.account_keyspace(account_id),
            table_id.replace('-', "_")
        ),
        cdrs_tokio::query_values!(),
        "base rows",
    )
    .await
    .expect("count the base rows");
    rows.first()
        .expect("a count")
        .get_by_name("c")
        .ok()
        .flatten()
        .expect("c")
}

/// Microseconds since the epoch on this host's clock, the domain of the
/// restore's write timestamps.
#[cfg(feature = "test-hooks")]
fn micros_now() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_micros(),
    )
    .expect("fits")
}

/// The batch fence (the module doc of `restore_engine`): a writer paused
/// between a batch's claim check and the batch's writes, taken over, whose
/// successor re-applies the batch, flips the target ACTIVE and deletes two
/// of its items, lands its batch when released at the timestamp it read
/// before the check, below every write and delete the successor made: the
/// deleted items stay deleted, their GSI and LSI rows stay gone, and the
/// surviving rows carry the successor's versions, not the stalled writer's.
/// Without the timestamp (the writes stamped when they land) the stalled
/// batch would resurrect the deleted items and their index rows.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_batch_landed_after_a_takeover_loses_to_the_successors_writes_and_deletes() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 4);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, owner) = engine
        .claim_restore(
            &account_id,
            composite_target_input("Fenced"),
            &manifest_registration(&row, Some("owner:/s")),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let composite_items = |range: std::ops::Range<usize>| {
        futures::stream::iter(range.map(composite_item).map(Ok)).boxed()
    };

    // The owner's one batch of four, held after its claim check.
    let pause =
        extenddb_storage_cassandra::test_hooks::pause_after_restore_claim_check(&key.table_id);
    let stalled = {
        let engine = engine.clone();
        let key = key.clone();
        tokio::spawn(async move {
            engine
                .bulk_load(
                    &key,
                    &*owner,
                    BulkLoadFile {
                        file_index: 0,
                        start_at: 0,
                        reapplying: false,
                    },
                    composite_items(0..4),
                )
                .await
        })
    };
    pause.reached().await;
    assert_eq!(
        base_rows(&engine, &account_id, &key.table_id).await,
        0,
        "nothing is written before the check"
    );

    // The owner's heartbeat (the check refreshed it) goes stale; the
    // successor takes the record over, writes the batch itself at later
    // timestamps, finishes, and two items are deleted through the ACTIVE
    // table.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let successor_from = micros_now();
    let successor = engine
        .claim_restore_dead(&key, Duration::from_secs(1), Some("successor:/s"))
        .await
        .expect("claim")
        .expect("the stale record is taken over");
    let applied = engine
        .bulk_load(
            &key,
            &*successor,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: true,
            },
            composite_items(0..4),
        )
        .await
        .expect("the successor's load");
    assert_eq!(applied, 4);
    assert_eq!(
        engine
            .finish_restore(&key, &*successor, 4, 1_024)
            .await
            .expect("finish"),
        Finished::Active
    );
    let key_info = engine
        .table_key_info(&account_id, "Fenced")
        .await
        .expect("the ACTIVE target's key info");
    // The deletes' GSI tombstones are written in the delete's own batch (no
    // propagation worker runs in these tests).
    make_gsis_synchronous(&engine, &key.table_id, &["gix"]).await;
    // Items 0 and 2 carry the GSI's key `g`; item 0 the LSI's `l` as well.
    for i in [0usize, 2] {
        let mut k = BTreeMap::new();
        k.insert("id".to_owned(), AttributeValue::S(format!("p{}", i % 5)));
        k.insert("sk".to_owned(), AttributeValue::N(i.to_string()));
        engine
            .delete_item(&key_info, &k, false, None, &ExpressionMaps::default(), None)
            .await
            .expect("delete through the ACTIVE table");
    }
    assert_eq!(base_rows(&engine, &account_id, &key.table_id).await, 2);
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        0
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        1
    );

    // The stalled writer lands its batch now. Its progress write then finds
    // the record no longer its own (the lost claim); its writes carry the
    // timestamp read before its check, so they change nothing.
    pause.release();
    let out = stalled.await.expect("the owner's task");
    assert!(
        matches!(&out, Err(StorageError::Internal(m)) if m.contains("claim on the target was lost")),
        "the stalled writer's progress write reports the lost claim: {out:?}"
    );
    assert_eq!(
        base_rows(&engine, &account_id, &key.table_id).await,
        2,
        "the deleted items stay deleted"
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        0,
        "the deleted items' GSI rows stay gone"
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        1,
        "the deleted item's LSI row stays gone"
    );
    for i in [0usize, 2] {
        let mut k = BTreeMap::new();
        k.insert("id".to_owned(), AttributeValue::S(format!("p{}", i % 5)));
        k.insert("sk".to_owned(), AttributeValue::N(i.to_string()));
        assert!(
            engine.get_item(&key_info, &k).await.expect("get").is_none(),
            "item {i} was not resurrected"
        );
    }
    let versions = live_versions_by_sk(&engine, &account_id, &key.table_id).await;
    assert_eq!(versions.keys().collect::<Vec<_>>(), ["1", "3"]);
    for (sk, version) in &versions {
        assert!(
            *version >= successor_from,
            "item {sk} carries the successor's version ({version}), not the stalled writer's \
             (before {successor_from})"
        );
    }
    // The surviving LSI row (item 3's) carries its base row's version.
    assert_eq!(
        stamps_by_sk(&engine, &account_id, &key.table_id, Some("lix")).await,
        BTreeMap::from([("3".to_owned(), versions["3"])])
    );
}

/// Per live item of the composite fixture (keyed by its `sk`), the base
/// row's `version`; a deleted item's partition, left with its static delete
/// stamp alone, is skipped.
#[cfg(feature = "test-hooks")]
async fn live_versions_by_sk(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
) -> BTreeMap<String, i64> {
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT item_data, version FROM {}.items_{}",
            engine.account_keyspace(account_id),
            table_id.replace('-', "_")
        ),
        cdrs_tokio::query_values!(),
        "versions",
    )
    .await
    .expect("read the versions");
    rows.into_iter()
        .filter_map(|row| {
            let json: Option<String> = row.get_by_name("item_data").ok().flatten();
            let image: Item = serde_json::from_str(&json?).expect("an item image");
            let Some(AttributeValue::N(sk)) = image.get("sk") else {
                panic!("no sk in {image:?}");
            };
            let version: i64 = row.get_r_by_name("version").expect("a version");
            Some((sk.clone(), version))
        })
        .collect()
}

/// A first batch whose writes landed and whose progress was never recorded
/// (here the progress lightweight transaction's outcome is unknown twice and
/// the load fails, the state a crash before the first progress record
/// leaves) is resumed with the first file re-applied (`reapplying`, from
/// `start_at: 0`): the upserts rewrite the landed rows and write the rest,
/// and the base count and both index-row counts are exact. The engine
/// takes this path for a target holding no more than the backup's total
/// (`a_half_applied_first_file_with_no_progress_is_resumed_with_upserts`).
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_first_batch_applied_without_progress_resumes_to_the_exact_count() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    extenddb_storage_cassandra::test_hooks::set_restore_batch_size(&account_id, 3);
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            composite_target_input("HalfFirst"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let composite_items = |range: std::ops::Range<usize>| {
        futures::stream::iter(range.map(composite_item).map(Ok)).boxed()
    };
    // The first batch's writes land; its progress write is lost twice and the
    // load fails with no progress recorded.
    extenddb_storage_cassandra::test_hooks::fail_progress_writes(
        &account_id,
        &[
            extenddb_storage_cassandra::test_hooks::ProgressWriteFault::Lost,
            extenddb_storage_cassandra::test_hooks::ProgressWriteFault::Lost,
        ],
    );
    let out = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            composite_items(0..7),
        )
        .await;
    assert!(
        matches!(&out, Err(StorageError::Internal(m)) if m.contains("no known outcome twice")),
        "{out:?}"
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (None, None),
        "no progress is recorded"
    );
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count"),
        3,
        "the first batch's items landed"
    );

    // The resume: the whole first file re-applied with upserts.
    let applied = engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: true,
            },
            composite_items(0..7),
        )
        .await
        .expect("the resume's load");
    assert_eq!(applied, 7);
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 7);
    // Items 0, 2, 4 and 6 carry `g`; 0, 3 and 6 carry `l`.
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "gix").await,
        4
    );
    assert_eq!(
        index_rows(&engine, &account_id, &key.table_id, "lix").await,
        3
    );
    let record = engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        (record.file_index, record.items_applied),
        (Some(0), Some(7))
    );
    assert_index_rows_carry_the_base_versions(&engine, &account_id, &key.table_id).await;
}

/// `restore_target_ready` reads the index tables as well as the data table:
/// a target whose GSI table is missing (the registration creates the data
/// table and then each index table in its own statement, so a crash between
/// them leaves this) is not ready, where the data table alone would pass.
#[tokio::test]
async fn test_restore_target_ready_requires_every_index_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    let (desc, _claim) = engine
        .claim_restore(
            &account_id,
            composite_target_input("NoGsiTable"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    assert!(
        engine.restore_target_ready(&key).await.expect("ready"),
        "the registration created every table"
    );
    // The GSI's table is dropped, as a crash between the data table's DDL
    // and the index table's would leave it.
    use cdrs_tokio::types::IntoRustByName;
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT index_id FROM {}.indexes WHERE table_id = ? AND index_name = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(key.table_id.as_str(), "gix"),
        "index id",
    )
    .await
    .expect("read the index");
    let index_id: String = rows
        .first()
        .expect("the GSI exists")
        .get_by_name("index_id")
        .ok()
        .flatten()
        .expect("index_id");
    engine
        .session()
        .query(format!(
            "DROP TABLE {}.{}",
            engine.account_keyspace(&account_id),
            extenddb_storage_cassandra::data::ddl::index_table_name(&index_id)
        ))
        .await
        .expect("drop the GSI's table");
    assert!(
        !engine.restore_target_ready(&key).await.expect("ready"),
        "a target missing its GSI table is not ready"
    );
}

/// The in-use rule's input on Cassandra while a store-backed restore of the
/// backup runs: `backup_in_use` names the CREATING target, twice (the
/// engine reads it before it validates a second restore's overrides, so an
/// unknown GSI override, a throughput override and a plain restore are each
/// `BackupInUseException`, as on the other backends); it names the target
/// still when the backup's `active_restores` entry has not landed (the
/// registration writes the target row first), through the target rows; and
/// it names nothing once the target is ACTIVE.
#[tokio::test]
async fn test_a_running_store_backed_restore_keeps_its_backup_in_use_until_active() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = available_index_backup(&engine, &account_id, "Music", "nightly", &store).await;
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        None
    );
    let (desc, claim) = engine
        .claim_restore(
            &account_id,
            manifest_target_input("InUseTarget"),
            &manifest_registration(&row, None),
        )
        .await
        .expect("register");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    for _ in 0..2 {
        assert_eq!(
            engine.backup_in_use(&row.key()).await.expect("in use"),
            Some("InUseTarget".to_owned()),
            "a running store-backed restore keeps the backup in use"
        );
        let second = engine
            .claim_restore(
                &account_id,
                manifest_target_input("SecondTarget"),
                &manifest_registration(&row, None),
            )
            .await
            .err()
            .expect("a second restore is refused");
        assert!(
            matches!(second, StorageError::BackupInUse(ref m) if *m == backup_in_use_message(&row.backup_arn)),
            "{second}"
        );
    }
    // The registration's `active_restores` entry removed: the target row
    // alone keeps the backup in use.
    apply(
        &engine,
        &format!(
            "DELETE active_restores[?] FROM {}.backups_by_arn \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(
            key.table_id.as_str(),
            account_id.as_str(),
            row.backup_arn.as_str()
        ),
    )
    .await;
    assert!(
        registrations(&engine, &account_id, &row.backup_arn)
            .await
            .is_empty()
    );
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        Some("InUseTarget".to_owned()),
        "the CREATING target row keeps the backup in use before the map entry lands"
    );
    // ACTIVE: the backup is free.
    assert_eq!(
        engine
            .finish_restore(&key, &*claim, 0, 0)
            .await
            .expect("finish"),
        Finished::Active
    );
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        None
    );
}

/// Deleters whose lightweight transaction lands before a `rewrite_store`
/// of their row and whose entry batch lands after it: DeleteBackup's
/// DELETING and the deletion's DELETED each leave an entry under the old
/// root with no row behind it, which the old root's ids and the summary
/// count. `remove_orphan_status_entries` of the old root deletes both,
/// checked against the rows, and leaves the moved rows listed under the new
/// root. A tombstone removal that read its row before the rewrite is
/// conditioned on the store it read, so it leaves the moved row.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_the_entries_deleters_leave_behind_a_rewrite_are_swept_from_the_old_root() {
    use extenddb_storage::backup_catalog::StatusEntrySweep;
    use extenddb_storage_cassandra::test_hooks::{
        pause_after_delete_claim, pause_after_deleted_write, pause_before_tombstone_removal,
    };
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let old_entry = Some(("filesystem".to_owned(), "old-1".to_owned()));

    // DeleteBackup: its DELETING claim applies, then the move, then its
    // entry batch.
    let available = row_at(&account_id, "deleting", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&available).await.expect("insert"));
    let pause = pause_after_delete_claim(&available.backup_arn);
    let start = {
        let engine = std::sync::Arc::clone(&engine);
        let key = available.key();
        tokio::spawn(async move { engine.start_backup_delete(&key).await })
    };
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[available.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    pause.release();
    assert_eq!(
        start.await.expect("delete task").expect("delete"),
        DeleteStart::Deleting
    );
    assert_eq!(
        status_entry(
            &engine,
            old.1,
            BackupStatus::Deleting,
            &available.backup_arn
        )
        .await,
        old_entry,
        "the batch wrote the DELETING entry under the old root"
    );

    // The DELETED write: its transaction applies, then the move, then its
    // entry batch.
    let deleting = row_at(&account_id, "deleted", BackupStatus::Deleting, old);
    assert!(engine.insert_backup_row(&deleting).await.expect("insert"));
    let pause = pause_after_deleted_write(&deleting.backup_arn);
    let finish = {
        let engine = std::sync::Arc::clone(&engine);
        let key = deleting.key();
        tokio::spawn(async move { engine.set_backup_deleted(&key).await })
    };
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[deleting.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    pause.release();
    assert!(finish.await.expect("task").expect("set DELETED"));
    assert_eq!(
        status_entry(&engine, old.1, BackupStatus::Deleted, &deleting.backup_arn).await,
        old_entry
    );

    // The tombstone removal: read before the move, its transaction after.
    let tombstone = row_at(&account_id, "tombstone", BackupStatus::Deleted, old);
    assert!(engine.insert_backup_row(&tombstone).await.expect("insert"));
    let pause = pause_before_tombstone_removal(&tombstone.backup_arn);
    let removal = {
        let engine = std::sync::Arc::clone(&engine);
        let key = tombstone.key();
        let root = old_root.clone();
        tokio::spawn(async move {
            engine
                .remove_backup_row(&key, StoreKind::Filesystem, &root)
                .await
        })
    };
    pause.reached().await;
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[tombstone.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    pause.release();
    assert!(
        !removal.await.expect("task").expect("removal"),
        "the removal names the store it read"
    );
    assert!(
        listed_with(&engine, BackupStatus::Deleted, &to, &tombstone.backup_arn).await,
        "the moved tombstone is the new store's"
    );

    assert_eq!(
        engine.read_store_ids(old.0, old.1).await.unwrap(),
        ["old-1"],
        "the old root's ids count the two entries"
    );
    assert!(by_root(&engine, old.0, old.1).await.is_empty());
    assert_eq!(
        engine
            .remove_orphan_status_entries(old.0, old.1)
            .await
            .unwrap(),
        StatusEntrySweep {
            removed: 2,
            repaired: 0
        }
    );
    assert!(
        engine
            .read_store_ids(old.0, old.1)
            .await
            .unwrap()
            .is_empty()
    );
    let summary: Vec<_> = engine
        .summarize_store_roots()
        .await
        .unwrap()
        .into_iter()
        .filter(|c| c.store.store_root == old_root)
        .collect();
    assert!(summary.is_empty(), "{summary:?}");
    for (status, row) in [
        (BackupStatus::Deleting, &available),
        (BackupStatus::Deleted, &deleting),
        (BackupStatus::Deleted, &tombstone),
    ] {
        assert!(
            listed_with(&engine, status, &to, &row.backup_arn).await,
            "{} is still listed under the new root",
            row.backup_arn
        );
    }
    assert_eq!(
        engine
            .remove_orphan_status_entries(to.store_kind, &to.store_root)
            .await
            .unwrap(),
        StatusEntrySweep::default(),
        "the new root's entries have their rows"
    );
    let every = under_both(&[&available, &deleting, &tombstone], &to);
    let mut all: Vec<BackupRow> = every.clone();
    for row in &every {
        for status in [
            BackupStatus::Available,
            BackupStatus::Deleting,
            BackupStatus::Deleted,
        ] {
            all.push(BackupRow {
                status,
                ..row.clone()
            });
        }
    }
    remove_index_rows(&engine, &all.iter().collect::<Vec<_>>()).await;
}

/// A row whose only status entry is under an earlier status: a DeleteBackup
/// that stopped between its applied DELETING transaction and its entry
/// batch (the row DELETING, its entry AVAILABLE), and a job whose AVAILABLE
/// transaction applied and whose entry batch failed (the row AVAILABLE, its
/// entry CREATING). The status-scoped `list_backup_rows` lists neither;
/// `list_backup_rows_by_root` lists each under its own status, and a row
/// with two entries (the `doubled` row's extra DELETED entry) once.
/// `rewrite_store` of what the by-root listing returns moves the DELETING
/// row, after which the old root lists it no more. The sweep of the old
/// root then deletes the moved row's stale entry (its row names another
/// store) and repairs the others: each row's entry under its own status is
/// written and the stale one deleted, so `list_backup_rows` lists them.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_row_whose_entry_is_under_an_earlier_status_is_listed_moved_and_repaired() {
    use extenddb_storage::backup_catalog::StatusEntrySweep;
    use extenddb_storage_cassandra::test_hooks::pause_after_delete_claim;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = std::sync::Arc::new(setup_engine().await);
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let tag = crate::helpers::unique_test_id();
    let old_root = format!("/srv/backups-old-{tag}");
    let old = (StoreKind::Filesystem, old_root.as_str(), "old-1");
    let to = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: format!("/srv/backups-new-{tag}"),
        store_id: "new-1".to_owned(),
    };
    let catalog = engine.catalog_keyspace();
    let raw = |cql: String, values: cdrs_tokio::query::QueryValues| {
        let engine = std::sync::Arc::clone(&engine);
        async move {
            engine
                .session()
                .query_with_values(&cql, values)
                .await
                .expect("a raw write");
        }
    };

    // DeleteBackup stopped after its DELETING transaction, before its batch.
    let deleting = row_at(&account_id, "deleting", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&deleting).await.expect("insert"));
    let pause = pause_after_delete_claim(&deleting.backup_arn);
    let start = {
        let engine = std::sync::Arc::clone(&engine);
        let key = deleting.key();
        tokio::spawn(async move { engine.start_backup_delete(&key).await })
    };
    pause.reached().await;
    start.abort();
    let _ = start.await;

    // A job's AVAILABLE transaction applied, its entry batch failed.
    let job = row_at(&account_id, "job", BackupStatus::Creating, old);
    let claim = engine.claim_backup(&job).await.expect("claim");
    raw(
        format!(
            "UPDATE {catalog}.backups_by_arn SET backup_status = 'AVAILABLE' \
             WHERE account_id = ? AND backup_arn = ?"
        ),
        cdrs_tokio::query_values!(account_id.as_str(), job.backup_arn.as_str()),
    )
    .await;
    drop(claim);

    // An AVAILABLE row with an extra DELETED entry beside its own.
    let doubled = row_at(&account_id, "doubled", BackupStatus::Available, old);
    assert!(engine.insert_backup_row(&doubled).await.expect("insert"));
    raw(
        format!(
            "INSERT INTO {catalog}.backup_index_by_status \
             (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
             VALUES (?, 'DELETED', ?, ?, 'filesystem', 'old-1')"
        ),
        cdrs_tokio::query_values!(
            old_root.as_str(),
            doubled.backup_arn.as_str(),
            account_id.as_str()
        ),
    )
    .await;

    let old_store = deleting.store.clone();
    assert!(
        !listed_with(
            &engine,
            BackupStatus::Deleting,
            &old_store,
            &deleting.backup_arn
        )
        .await
    );
    assert!(
        !listed_with(
            &engine,
            BackupStatus::Available,
            &old_store,
            &job.backup_arn
        )
        .await
    );
    let listed: Vec<BackupRow> = engine
        .list_backup_rows_by_root(old.0, old.1, None)
        .try_collect()
        .await
        .expect("listing");
    let mut statuses: Vec<(String, BackupStatus)> = listed
        .iter()
        .map(|r| (r.backup_arn.clone(), r.status))
        .collect();
    statuses.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = vec![
        (deleting.backup_arn.clone(), BackupStatus::Deleting),
        (job.backup_arn.clone(), BackupStatus::Available),
        (doubled.backup_arn.clone(), BackupStatus::Available),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(statuses, expected, "each row once, under its own status");
    let only_deleting: Vec<String> = engine
        .list_backup_rows_by_root(old.0, old.1, Some(BackupStatus::Deleting))
        .map_ok(|r| r.backup_arn)
        .try_collect()
        .await
        .expect("listing");
    assert_eq!(only_deleting, std::slice::from_ref(&deleting.backup_arn));

    // The move of what the by-root listing returns: the DELETING row moves
    // (the job and the doubled row are left out of the keys here).
    assert_eq!(
        engine
            .rewrite_store(old.0, old.1, &[deleting.key()], &to)
            .await
            .expect("rewrite"),
        1
    );
    let mut left = by_root(&engine, old.0, old.1).await;
    left.sort();
    let mut expected_left = vec![job.backup_arn.clone(), doubled.backup_arn.clone()];
    expected_left.sort();
    assert_eq!(
        left, expected_left,
        "the moved row is no longer the old root's"
    );
    assert!(listed_with(&engine, BackupStatus::Deleting, &to, &deleting.backup_arn).await);

    assert_eq!(
        engine
            .remove_orphan_status_entries(old.0, old.1)
            .await
            .unwrap(),
        StatusEntrySweep {
            removed: 1,
            repaired: 2
        }
    );
    assert!(
        listed_with(
            &engine,
            BackupStatus::Available,
            &old_store,
            &job.backup_arn
        )
        .await,
        "the job row's entry is under AVAILABLE now"
    );
    for (status, arn) in [
        (BackupStatus::Available, &deleting.backup_arn),
        (BackupStatus::Creating, &job.backup_arn),
        (BackupStatus::Deleted, &doubled.backup_arn),
    ] {
        assert_eq!(status_entry(&engine, old.1, status, arn).await, None);
    }
    assert!(
        status_entry(&engine, old.1, BackupStatus::Available, &doubled.backup_arn)
            .await
            .is_some()
    );
    assert_eq!(
        engine
            .remove_orphan_status_entries(old.0, old.1)
            .await
            .unwrap(),
        StatusEntrySweep::default(),
        "a second sweep finds nothing"
    );
    let every = under_both(&[&deleting, &job, &doubled], &to);
    remove_index_rows(&engine, &every.iter().collect::<Vec<_>>()).await;
}

// --- `extenddb backup migrate-legacy`: the item stream, `list_all`, the
// conversion of an in-catalog row into its index row.

/// The index row of the store copy of the in-catalog backup `backup_arn`, as
/// the migration builds it.
#[cfg(feature = "test-hooks")]
async fn migrated_row(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    store: &StoreIdentity,
) -> BackupRow {
    let source = engine
        .restore_source(account_id, backup_arn)
        .await
        .expect("source");
    let table = extenddb_storage::backup_definition::source_table_from_legacy(
        &source,
        extenddb_storage::backup_definition::LegacyVectorState::Unsupported,
        "us-east-1",
        account_id,
    )
    .expect("source table");
    let description = engine
        .describe(account_id, backup_arn)
        .await
        .expect("describe");
    BackupRow {
        backup_id: BackupArn::parse(backup_arn)
            .expect("arn")
            .backup_id()
            .to_owned(),
        account_id: account_id.to_owned(),
        table_name: source.source_table_name.clone(),
        backup_arn: backup_arn.to_owned(),
        backup_name: description.backup_details.backup_name,
        status: BackupStatus::Available,
        store: store.clone(),
        size_bytes: 4321,
        item_count: source.item_count,
        created_at: source.backup_creation_date_time,
        completed_at: None,
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&table).expect("json"),
        owner_generation: 77,
        heartbeat_at: 0.0,
    }
}

/// `item_data` of `backup_arn`'s payload rows in (bucket, `item_index`)
/// order, the order the create wrote them.
#[cfg(feature = "test-hooks")]
async fn payload_in_order(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    buckets: i32,
) -> Vec<String> {
    use cdrs_tokio::types::IntoRustByName;
    let mut items = Vec::new();
    for bucket in 0..buckets {
        let rows = extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
            &engine.session_arc(),
            &format!(
                "SELECT item_data FROM {}.backup_items WHERE backup_arn = ? AND bucket = ?",
                engine.account_keyspace(account_id)
            ),
            cdrs_tokio::query_values!(backup_arn, bucket),
            "payload",
        )
        .await
        .expect("read a bucket");
        for row in rows {
            let data: String = row.get_r_by_name("item_data").expect("item_data");
            items.push(data);
        }
    }
    items
}

/// The stream yields every item of every bucket, in bucket and
/// `item_index` order, past several pages of each bucket and a last bucket
/// that ends on a page bound (200 items: three full buckets of 64 and one
/// of 8, read in pages of 8), with the backup's name, source and the vector
/// state of a backend without vector support; a missing backup is refused.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_legacy_items_stream_every_bucket_in_pages() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, key_info, backup_arn) = backed_up_plain_table(&engine, "LmSource", 200).await;
    let account_id = account.account_id.clone();
    let opened = engine.items(&account_id, &backup_arn).await.expect("items");
    assert_eq!(opened.backup_name, "copy-timing");
    assert_eq!(
        opened.source,
        engine
            .restore_source(&account_id, &backup_arn)
            .await
            .expect("source")
    );
    assert_eq!(
        opened.vector_state,
        extenddb_storage::backup_definition::LegacyVectorState::Unsupported
    );
    let streamed: Vec<String> = opened
        .items
        .map(|item| serde_json::to_string(&item.expect("an item")).expect("text"))
        .collect()
        .await;
    let stored: Vec<String> = payload_in_order(&engine, &account_id, &backup_arn, 5)
        .await
        .iter()
        .map(|text| {
            let item: Item = serde_json::from_str(text).expect("item");
            serde_json::to_string(&item).expect("text")
        })
        .collect();
    assert_eq!(stored.len(), 200);
    assert_eq!(streamed.len(), 200);
    assert_eq!(
        streamed, stored,
        "every item once, in bucket and index order"
    );
    assert_eq!(scan_count(&engine, &key_info).await, 200);

    let err = engine
        .items(&account_id, &flip_last_hex(&backup_arn))
        .await
        .err();
    assert!(
        matches!(&err, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
        "{err:?}"
    );
}

/// `list_all` reads every account's in-catalog rows and leaves out the
/// index rows and the DELETED rows. The catalog keyspace is shared with
/// other tests, so the listing is checked for this test's two accounts.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_list_all_lists_the_in_catalog_rows_of_every_account() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmList", 3).await;
    let account_id = account.account_id.clone();
    let other = unique_test_account();
    let catalog = engine.catalog_keyspace();
    let raw = |account: String, arn: String, status: &'static str, created_at: i64| {
        let engine = &engine;
        let catalog = catalog.clone();
        async move {
            engine
                .session()
                .query_with_values(
                    &format!(
                        "INSERT INTO {catalog}.backups_by_arn (account_id, backup_arn, \
                         backup_name, table_name, backup_status, created_at) \
                         VALUES (?, ?, 'raw', 'LmList', ?, ?)"
                    ),
                    cdrs_tokio::query_values!(account.as_str(), arn.as_str(), status, created_at),
                )
                .await
                .expect("an in-catalog row");
        }
    };
    let other_arn = extenddb_storage::util::backup_arn(
        "us-east-1",
        &other,
        "LmList",
        &extenddb_storage::util::new_backup_id(),
    );
    let deleted_arn = extenddb_storage::util::backup_arn(
        "us-east-1",
        &account_id,
        "LmList",
        &extenddb_storage::util::new_backup_id(),
    );
    raw(other.clone(), other_arn.clone(), "DELETING", i64::MAX / 2).await;
    raw(account_id.clone(), deleted_arn.clone(), "DELETED", 1).await;
    let mut index = index_row(&account_id, "LmList", "index", &unique_store());
    index.status = BackupStatus::Available;
    assert!(engine.insert_backup_row(&index).await.expect("insert"));

    let listed: Vec<_> = engine
        .list_all()
        .await
        .expect("list")
        .into_iter()
        .filter(|k| k.account_id == account_id || k.account_id == other)
        .collect();
    remove_index_rows(&engine, &[&index]).await;
    engine
        .session()
        .query_with_values(
            &format!(
                "DELETE FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(other.as_str(), other_arn.as_str()),
        )
        .await
        .expect("remove the raw row");
    assert_eq!(
        listed,
        vec![
            extenddb_storage::backup_catalog::LegacyBackupKey {
                account_id: account_id.clone(),
                backup_arn: backup_arn.clone(),
                status: BackupStatus::Available,
            },
            extenddb_storage::backup_catalog::LegacyBackupKey {
                account_id: other.clone(),
                backup_arn: other_arn.clone(),
                status: BackupStatus::Deleting,
            },
        ]
    );
}

/// The replacement converts the in-catalog row into its index row in place:
/// the index reader reads it unowned with the store and the source table,
/// its list and status entries are written, its buckets deleted, and the
/// in-catalog reader no longer sees it (describe, list, delete). A second
/// call is `AlreadyReplaced`; one with another store is `AlreadyExists`.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_replacement_converts_the_in_catalog_row_in_place() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmConvert", 100).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = migrated_row(&engine, &account_id, &backup_arn, &store).await;
    assert_eq!(
        payload_item_count(&engine, &account_id, &backup_arn).await,
        100
    );
    let answer = engine.replace_with_index_row(&row).await;
    let read = engine.read_backup_row(&arn_of(&row)).await.expect("read");
    let listed = listed_in(&engine, &row).await;
    let payload = payload_item_count(&engine, &account_id, &backup_arn).await;
    let legacy_describe = engine.describe(&account_id, &backup_arn).await.err();
    let legacy_list = engine.list(&account_id, None).await.expect("list");
    let legacy_delete = engine.delete(&account_id, &backup_arn).await.err();
    let mut moved = row.clone();
    moved.store = unique_store();
    let again = engine.replace_with_index_row(&row).await;
    let elsewhere = engine.replace_with_index_row(&moved).await.err();
    remove_index_rows(&engine, &[&row]).await;

    assert_eq!(
        answer.expect("replace"),
        extenddb_storage::backup_catalog::Replaced::Replaced
    );
    let read = read.expect("the index row");
    assert_eq!(read.owner_generation, UNOWNED_GENERATION);
    assert_eq!(read.status, BackupStatus::Available);
    assert_eq!(read.store, store);
    assert_eq!(read.backup_id, row.backup_id);
    assert_eq!(read.backup_name, "copy-timing");
    assert_eq!(read.size_bytes, 4321);
    assert_eq!(read.item_count, 100);
    assert!(read.completed_at.is_some());
    assert!((read.created_at - row.created_at).abs() < 1e-3);
    assert_eq!(
        read.source_table().expect("source"),
        row.source_table().expect("source")
    );
    assert_eq!(listed, [true; 3], "status, account and table entries");
    assert_eq!(payload, 0, "the buckets are deleted");
    assert!(
        matches!(&legacy_describe, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
        "{legacy_describe:?}"
    );
    assert!(
        legacy_list.iter().all(|s| s.backup_arn != backup_arn),
        "{legacy_list:?}"
    );
    assert!(
        matches!(&legacy_delete, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
        "{legacy_delete:?}"
    );
    assert_eq!(
        again.expect("replace again"),
        extenddb_storage::backup_catalog::Replaced::AlreadyReplaced
    );
    assert!(
        matches!(&elsewhere, Some(StorageError::AlreadyExists(_))),
        "{elsewhere:?}"
    );
}

/// The replacement refuses, changing nothing, while a restore target of the
/// backup is CREATING (its registration is in `active_restores`), and when
/// the backup is DELETED or missing.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_replacement_refuses_an_in_use_or_unavailable_backup_and_changes_nothing() {
    use extenddb_storage::backup_catalog::Replaced;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmRefuse", 10).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = migrated_row(&engine, &account_id, &backup_arn, &store).await;
    restore(
        &engine,
        &account_id,
        &backup_arn,
        "LmTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    let target = wait_active(&engine, &account_id, "LmTarget", Duration::from_secs(120)).await;
    // The target back in CREATING with its registration, as while its copy
    // runs.
    let catalog = engine.catalog_keyspace();
    for (cql, values) in [
        (
            format!(
                "UPDATE {catalog}.tables SET table_status = 'CREATING' \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id.as_str(), "LmTarget"),
        ),
        (
            format!(
                "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
                 WHERE account_id = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                target.table_id.as_str(),
                "LmTarget",
                account_id.as_str(),
                backup_arn.as_str()
            ),
        ),
    ] {
        engine
            .session()
            .query_with_values(&cql, values)
            .await
            .expect("hold the target");
    }
    let in_use = engine.replace_with_index_row(&row).await;
    let read = engine.read_backup_row(&arn_of(&row)).await.expect("read");
    let payload = payload_item_count(&engine, &account_id, &backup_arn).await;
    let status = backup_status_of(&engine, &account_id, &backup_arn).await;
    engine
        .session()
        .query_with_values(
            &format!(
                "UPDATE {catalog}.tables SET table_status = 'ACTIVE' \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id.as_str(), "LmTarget"),
        )
        .await
        .expect("release the target");
    assert_eq!(
        in_use.expect("replace"),
        Replaced::InUse("LmTarget".to_owned())
    );
    assert!(read.is_none(), "still an in-catalog row");
    assert_eq!(payload, 10, "the buckets are kept");
    assert_eq!(status.as_deref(), Some("AVAILABLE"));

    // DELETED, then missing.
    let second = engine
        .create(&account_id, "LmRefuse", "second", JobPermit::detached())
        .await
        .expect("second backup")
        .backup_arn;
    let second_row = migrated_row(&engine, &account_id, &second, &store).await;
    engine.delete(&account_id, &second).await.expect("delete");
    assert_eq!(
        engine
            .replace_with_index_row(&second_row)
            .await
            .expect("replace"),
        Replaced::NotAvailable("the in-catalog backup is DELETED".to_owned())
    );
    let mut missing = second_row.clone();
    missing.backup_arn = flip_last_hex(&second);
    assert!(matches!(
        engine.replace_with_index_row(&missing).await.expect("replace"),
        Replaced::NotAvailable(m) if m.contains("Backup not found")
    ));
}

/// A lost answer of the conversion is settled by a serial read (the
/// transaction ran: `Replaced`; it never ran: sent again, `Replaced`); a
/// stop after the conversion leaves the index row with no entries and its
/// buckets, which the next call finishes as `AlreadyReplaced`; and a
/// DeleteBackup that read the in-catalog row before the conversion claims
/// nothing after it.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_conversion_whose_answer_is_lost_or_that_stopped_is_settled() {
    use extenddb_storage::backup_catalog::Replaced;
    use extenddb_storage_cassandra::test_hooks::{
        ProgressWriteFault, fail_convert_writes, pause_before_delete_claim, stop_convert_after_row,
    };
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, first) = backed_up_plain_table(&engine, "LmSettle", 70).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let mut backups = vec![first];
    for name in ["second", "third", "fourth"] {
        backups.push(
            engine
                .create(&account_id, "LmSettle", name, JobPermit::detached())
                .await
                .expect("backup")
                .backup_arn,
        );
    }
    let mut rows = Vec::new();
    for arn in &backups {
        rows.push(migrated_row(&engine, &account_id, arn, &store).await);
    }

    // The transaction ran; its answer was lost.
    fail_convert_writes(&rows[0].backup_arn, &[ProgressWriteFault::Landed]);
    let landed = engine.replace_with_index_row(&rows[0]).await;
    let landed_read = engine
        .read_backup_row(&arn_of(&rows[0]))
        .await
        .expect("read");
    let landed_listed = listed_in(&engine, &rows[0]).await;
    let landed_payload = payload_item_count(&engine, &account_id, &backups[0]).await;

    // The transaction never ran: sent again.
    fail_convert_writes(&rows[1].backup_arn, &[ProgressWriteFault::Lost]);
    let lost = engine.replace_with_index_row(&rows[1]).await;
    let lost_read = engine
        .read_backup_row(&arn_of(&rows[1]))
        .await
        .expect("read");
    let lost_payload = payload_item_count(&engine, &account_id, &backups[1]).await;

    // A stop after the conversion, then the next call.
    stop_convert_after_row(&rows[2].backup_arn);
    let stopped = engine.replace_with_index_row(&rows[2]).await.err();
    let stopped_read = engine
        .read_backup_row(&arn_of(&rows[2]))
        .await
        .expect("read");
    let stopped_listed = listed_in(&engine, &rows[2]).await;
    let stopped_payload = payload_item_count(&engine, &account_id, &backups[2]).await;
    let stopped_describe = engine.describe(&account_id, &backups[2]).await.err();
    let finished = engine.replace_with_index_row(&rows[2]).await;
    let finished_listed = listed_in(&engine, &rows[2]).await;
    let finished_payload = payload_item_count(&engine, &account_id, &backups[2]).await;

    // A DeleteBackup paused between its read and its claim while the
    // conversion lands.
    let pause = pause_before_delete_claim(&backups[3]);
    let deleting = {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let arn = backups[3].clone();
        tokio::spawn(async move { engine.delete(&account_id, &arn).await })
    };
    pause.reached().await;
    let converted = engine.replace_with_index_row(&rows[3]).await;
    pause.release();
    let deleted = deleting.await.expect("delete task");
    let after_delete = engine
        .read_backup_row(&arn_of(&rows[3]))
        .await
        .expect("read");

    remove_index_rows(&engine, &rows.iter().collect::<Vec<_>>()).await;

    assert_eq!(landed.expect("landed"), Replaced::Replaced);
    assert_eq!(landed_read.map(|r| r.store), Some(store.clone()));
    assert_eq!(landed_listed, [true; 3]);
    assert_eq!(landed_payload, 0);

    assert_eq!(lost.expect("lost"), Replaced::Replaced);
    assert_eq!(lost_read.map(|r| r.store), Some(store.clone()));
    assert_eq!(lost_payload, 0);

    assert!(
        matches!(&stopped, Some(StorageError::Internal(m)) if m.contains("stopped after its row")),
        "{stopped:?}"
    );
    assert_eq!(stopped_read.map(|r| r.store), Some(store.clone()));
    assert_eq!(
        stopped_listed,
        [false, true, true],
        "no status entry yet; the list rows are the in-catalog backup's, keyed alike"
    );
    assert_eq!(stopped_payload, 70, "the buckets are still there");
    assert!(
        matches!(&stopped_describe, Some(StorageError::Validation(_))),
        "the in-catalog reader no longer reads it: {stopped_describe:?}"
    );
    assert_eq!(finished.expect("finished"), Replaced::AlreadyReplaced);
    assert_eq!(finished_listed, [true; 3]);
    assert_eq!(finished_payload, 0);

    assert_eq!(converted.expect("converted"), Replaced::Replaced);
    assert!(
        matches!(&deleted, Err(StorageError::Validation(m)) if m.contains("Backup not found")),
        "{deleted:?}"
    );
    let after_delete = after_delete.expect("the index row");
    assert_eq!(after_delete.status, BackupStatus::Available);
    assert_eq!(after_delete.store, store);
}

/// The `active_restores` of the `backups_by_arn` row `backup_arn`: whether
/// a registration is on it.
#[cfg(feature = "test-hooks")]
async fn has_registration(engine: &CassandraEngine, account_id: &str, backup_arn: &str) -> bool {
    use cdrs_tokio::types::IntoRustByName;
    extenddb_storage_cassandra::cassandra_util::query_rows_quorum::<StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT active_restores FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(account_id, backup_arn),
        "registrations",
    )
    .await
    .expect("read the row")
    .first()
    .and_then(|row| {
        IntoRustByName::<cdrs_tokio::types::map::Map>::get_by_name(row, "active_restores")
            .ok()
            .flatten()
    })
    .is_some()
}

/// A legacy restore that read the in-catalog backup before its row was
/// converted, and registers after the conversion (its target's DDL in
/// between), is refused: the registration's condition carries
/// `store_kind = null`, so it does not land on the index row. The restore
/// answers not found, its target is removed, and the index row carries no
/// registration.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_legacy_restore_registering_after_a_conversion_is_refused() {
    use extenddb_storage::backup_catalog::Replaced;
    use extenddb_storage_cassandra::test_hooks::pause_before_legacy_restore_registration;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmLate", 20).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = migrated_row(&engine, &account_id, &backup_arn, &store).await;
    let pause = pause_before_legacy_restore_registration(&backup_arn);
    let restoring = {
        let engine = engine.clone();
        let account_id = account_id.clone();
        let backup_arn = backup_arn.clone();
        tokio::spawn(async move {
            restore(
                &engine,
                &account_id,
                &backup_arn,
                "LmLateTarget",
                &RestoreTableOverrides::default(),
            )
            .await
        })
    };
    pause.reached().await;
    let converted = engine.replace_with_index_row(&row).await;
    pause.release();
    let restored = restoring.await.expect("restore task");
    let registered = has_registration(&engine, &account_id, &backup_arn).await;
    let target = engine
        .describe_table(
            &account_id,
            DescribeTableInput {
                table_name: "LmLateTarget".to_owned(),
            },
        )
        .await;
    let read = engine.read_backup_row(&arn_of(&row)).await.expect("read");
    remove_index_rows(&engine, &[&row]).await;
    assert_eq!(converted.expect("convert"), Replaced::Replaced);
    assert!(
        matches!(&restored, Err(StorageError::Validation(m)) if m.contains("Backup not found")),
        "{:?}",
        restored.map(|d| d.table_status)
    );
    assert!(!registered, "no registration on the index row");
    assert!(target.is_err(), "the target is removed: {target:?}");
    assert_eq!(read.map(|r| r.status), Some(BackupStatus::Available));
}

/// A restore's registration that lands between the conversion's read of
/// the row (its in-use check) and its lightweight transaction: the
/// transaction's own `active_restores = null` condition refuses it, and the
/// next read answers `InUse` with the in-catalog backup kept whole.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_a_registration_between_the_conversions_read_and_its_transaction_refuses_it() {
    use extenddb_storage::backup_catalog::Replaced;
    use extenddb_storage_cassandra::test_hooks::pause_before_convert;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmGap", 10).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = migrated_row(&engine, &account_id, &backup_arn, &store).await;
    restore(
        &engine,
        &account_id,
        &backup_arn,
        "LmGapTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    let target = wait_active(
        &engine,
        &account_id,
        "LmGapTarget",
        Duration::from_secs(120),
    )
    .await;
    let catalog = engine.catalog_keyspace();
    let set_target = |status: &'static str| {
        let engine = engine.clone();
        let catalog = catalog.clone();
        let account_id = account_id.clone();
        async move {
            engine
                .session()
                .query_with_values(
                    &format!(
                        "UPDATE {catalog}.tables SET table_status = ? \
                         WHERE account_id = ? AND table_name = ?"
                    ),
                    cdrs_tokio::query_values!(status, account_id.as_str(), "LmGapTarget"),
                )
                .await
                .expect("target status");
        }
    };
    set_target("CREATING").await;
    let pause = pause_before_convert(&backup_arn);
    let converting = {
        let engine = engine.clone();
        let row = row.clone();
        tokio::spawn(async move { engine.replace_with_index_row(&row).await })
    };
    pause.reached().await;
    engine
        .session()
        .query_with_values(
            &format!(
                "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
                 WHERE account_id = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(
                target.table_id.as_str(),
                "LmGapTarget",
                account_id.as_str(),
                backup_arn.as_str()
            ),
        )
        .await
        .expect("a registration in the gap");
    pause.release();
    let answer = converting.await.expect("convert task");
    let read = engine.read_backup_row(&arn_of(&row)).await.expect("read");
    let payload = payload_item_count(&engine, &account_id, &backup_arn).await;
    set_target("ACTIVE").await;
    if read.is_some() {
        remove_index_rows(&engine, &[&row]).await;
    }
    assert_eq!(
        answer.expect("convert"),
        Replaced::InUse("LmGapTarget".to_owned())
    );
    assert!(read.is_none(), "still an in-catalog row");
    assert_eq!(payload, 10, "the buckets are kept");
}

/// A conversion that stopped after its row, then a DeleteBackup of the
/// migrated backup (its index row DELETED): the next replacement call
/// deletes the item buckets, writes no status entry for the deleted row,
/// and answers `NotAvailable`.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_finishing_a_conversion_of_a_backup_deleted_since_writes_no_entry() {
    use extenddb_storage::backup_catalog::Replaced;
    use extenddb_storage_cassandra::test_hooks::stop_convert_after_row;
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, backup_arn) = backed_up_plain_table(&engine, "LmGoneSince", 70).await;
    let account_id = account.account_id.clone();
    let store = unique_store();
    let row = migrated_row(&engine, &account_id, &backup_arn, &store).await;
    stop_convert_after_row(&backup_arn);
    let stopped = engine.replace_with_index_row(&row).await;
    engine
        .session()
        .query_with_values(
            &format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETED' \
                 WHERE account_id = ? AND backup_arn = ?",
                engine.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
        )
        .await
        .expect("the migrated backup deleted");
    let finished = engine.replace_with_index_row(&row).await;
    let payload = payload_item_count(&engine, &account_id, &backup_arn).await;
    let listed = listed_in(&engine, &row).await;
    remove_index_rows(&engine, &[&row]).await;
    assert!(stopped.is_err(), "{stopped:?}");
    assert_eq!(
        finished.expect("finish"),
        Replaced::NotAvailable("the backup is DELETED in the store".to_owned())
    );
    assert_eq!(payload, 0, "the buckets are deleted");
    assert!(!listed[0], "no AVAILABLE status entry for a deleted row");
}

/// `holder_status` answers for an in-catalog row in any status, DELETED
/// included, and for none a conversion turned into an index row;
/// `list_deleted` lists the DELETED in-catalog rows, not a converted one.
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_holder_status_and_list_deleted_tell_a_delete_from_a_conversion() {
    use extenddb_storage::backup_catalog::{LegacyBackupKey, Replaced};
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, _, kept) = backed_up_plain_table(&engine, "LmHolder", 3).await;
    let account_id = account.account_id.clone();
    let mut created = Vec::new();
    for name in ["gone", "moved"] {
        created.push(
            engine
                .create(&account_id, "LmHolder", name, JobPermit::detached())
                .await
                .expect("backup")
                .backup_arn,
        );
    }
    let (deleted, converted) = (created[0].clone(), created[1].clone());
    engine.delete(&account_id, &deleted).await.expect("delete");
    let row = migrated_row(&engine, &account_id, &converted, &unique_store()).await;
    let replaced = engine.replace_with_index_row(&row).await;
    let statuses = [
        engine
            .holder_status(&account_id, &kept)
            .await
            .expect("holder"),
        engine
            .holder_status(&account_id, &deleted)
            .await
            .expect("holder"),
        engine
            .holder_status(&account_id, &converted)
            .await
            .expect("holder"),
        engine
            .holder_status(&unique_test_account(), &kept)
            .await
            .expect("holder"),
    ];
    let listed: Vec<LegacyBackupKey> = engine
        .list_deleted()
        .await
        .expect("deleted")
        .into_iter()
        .filter(|k| k.account_id == account_id)
        .collect();
    remove_index_rows(&engine, &[&row]).await;
    assert_eq!(replaced.expect("replace"), Replaced::Replaced);
    assert_eq!(
        statuses,
        [
            Some(BackupStatus::Available),
            Some(BackupStatus::Deleted),
            None,
            None
        ]
    );
    assert_eq!(
        listed,
        vec![LegacyBackupKey {
            account_id: account_id.clone(),
            backup_arn: deleted,
            status: BackupStatus::Deleted,
        }]
    );
}
