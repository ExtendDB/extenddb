// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Restore copy timing against a live Cassandra.
//!
//! Ignored by default: the numbers from a single-node container are only
//! meaningful relative to each other on the same machine, so these run
//! manually around a change to the restore copy, never in CI:
//!
//! ```text
//! cargo test -p extenddb-storage-cassandra --test direct_integration --release -- \
//!     --ignored --test-threads=1 --nocapture restore_timing
//! ```
//!
//! Each case backs up a table, restores it, reports the time from the
//! `CREATING` response to `ACTIVE`, and checks the restored table against the
//! source: item count, every item, and every index row.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, GsiInput, KeyType, LsiInput, Projection,
    ProjectionType, ScalarAttributeType, TableKeyInfo, TableStatus,
};
use extenddb_storage::{BackupEngine, DataEngine, RestoreTableOverrides, TableEngine};
use extenddb_storage_cassandra::CassandraEngine;

use crate::backup_engine::{
    attr, create_active_table, describe, item, ks, make_gsis_synchronous, put_all, query_index,
    restore, scan_count,
};
use crate::helpers::{TestAccount, setup_engine};

type Item = BTreeMap<String, AttributeValue>;

/// Poll `name` every 50 ms until it is ACTIVE; the time taken.
async fn time_to_active(engine: &CassandraEngine, account_id: &str, name: &str) -> Duration {
    let started = Instant::now();
    loop {
        let description = describe(engine, account_id, name).await;
        if description.table_status == TableStatus::Active {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "restore target {name} still {:?} after 600 s",
            description.table_status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every item of `key_info`, by key, all pages.
async fn scan_all(engine: &CassandraEngine, key_info: &TableKeyInfo) -> BTreeMap<String, Item> {
    let mut out = BTreeMap::new();
    let mut start: Option<Item> = None;
    loop {
        let (items, next) = engine
            .scan(key_info, None, start.as_ref(), None, None, None)
            .await
            .expect("scan");
        for it in items {
            let mut key = String::new();
            for ks in &key_info.key_schema {
                key.push_str(&format!("{:?}|", it.get(&ks.attribute_name)));
            }
            out.insert(key, it);
        }
        match next {
            Some(next) => start = Some(next),
            None => return out,
        }
    }
}

async fn back_up(engine: &CassandraEngine, account_id: &str, name: &str) -> String {
    let started = Instant::now();
    let backup = engine
        .create_backup(account_id, name, "timing")
        .await
        .expect("create backup");
    println!("  CreateBackup of {name}: {:?}", started.elapsed());
    backup.backup_arn
}

async fn restore_and_time(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    target: &str,
) -> TableKeyInfo {
    let initial = restore(
        engine,
        account_id,
        backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    assert_eq!(initial.table_status, TableStatus::Creating);
    let elapsed = time_to_active(engine, account_id, target).await;
    println!("  {target} ACTIVE after {elapsed:?}");
    engine
        .table_key_info(account_id, target)
        .await
        .expect("target key info")
}

#[tokio::test]
#[ignore]
async fn restore_timing_60k_plain() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 60_000;
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "TimingPlain".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    let started = Instant::now();
    put_all(
        &engine,
        &source,
        (0..ITEMS)
            .map(|i| item(&format!("item-{i:06}"), None, &"x".repeat(200)))
            .collect(),
    )
    .await;
    println!("  seeded {ITEMS} items in {:?}", started.elapsed());
    let backup_arn = back_up(&engine, &account_id, "TimingPlain").await;

    let restored = restore_and_time(&engine, &account_id, &backup_arn, "TimingPlainTarget").await;

    let description = describe(&engine, &account_id, "TimingPlainTarget").await;
    assert_eq!(description.item_count, ITEMS as i64);
    assert_eq!(scan_count(&engine, &restored).await, ITEMS);
    let on_source = scan_all(&engine, &source).await;
    let on_target = scan_all(&engine, &restored).await;
    assert_eq!(on_source.len(), ITEMS);
    assert_eq!(
        on_target, on_source,
        "restored items differ from the source"
    );
    println!("  {ITEMS} items compared equal");
}

#[tokio::test]
#[ignore]
async fn restore_timing_20k_indexed() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const ITEMS: usize = 20_000;
    const GROUPS: usize = 49;
    const PARTITIONS: usize = 200;
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "TimingIndexed".to_owned(),
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
    let mut items = Vec::with_capacity(ITEMS);
    for i in 0..ITEMS {
        let mut it = item(
            &format!("p{:04}", i % PARTITIONS),
            Some(&format!("s{i:06}")),
            &"v".repeat(100),
        );
        it.insert("lsk".to_owned(), AttributeValue::N((ITEMS - i).to_string()));
        it.insert("extra".to_owned(), AttributeValue::S(format!("e{i}")));
        if i % 10 != 0 {
            it.insert(
                "gpk".to_owned(),
                AttributeValue::S(format!("g{:03}", i % GROUPS)),
            );
            it.insert("gsk".to_owned(), AttributeValue::N(i.to_string()));
        }
        items.push(it);
    }
    let started = Instant::now();
    put_all(&engine, &source, items).await;
    println!("  seeded {ITEMS} indexed items in {:?}", started.elapsed());
    let backup_arn = back_up(&engine, &account_id, "TimingIndexed").await;

    let restored = restore_and_time(&engine, &account_id, &backup_arn, "TimingIndexedTarget").await;

    let description = describe(&engine, &account_id, "TimingIndexedTarget").await;
    assert_eq!(description.item_count, ITEMS as i64);
    let on_source = scan_all(&engine, &source).await;
    let on_target = scan_all(&engine, &restored).await;
    assert_eq!(on_source.len(), ITEMS);
    assert_eq!(
        on_target, on_source,
        "restored items differ from the source"
    );

    let mut gsi_rows = 0;
    for g in 0..GROUPS {
        let value = AttributeValue::S(format!("g{g:03}"));
        let on_source = query_index(&engine, &source, "ByGroup", "gpk", value.clone()).await;
        let on_target = query_index(&engine, &restored, "ByGroup", "gpk", value).await;
        assert!(!on_source.is_empty(), "source GSI g{g} is empty");
        assert_eq!(
            on_target, on_source,
            "GSI ByGroup g{g} differs after restore"
        );
        gsi_rows += on_source.len();
    }
    assert_eq!(gsi_rows, ITEMS - ITEMS / 10, "sparse GSI row count");
    let mut lsi_rows = 0;
    for p in 0..PARTITIONS {
        let value = AttributeValue::S(format!("p{p:04}"));
        let on_source = query_index(&engine, &source, "ByLocal", "id", value.clone()).await;
        let on_target = query_index(&engine, &restored, "ByLocal", "id", value).await;
        assert!(!on_source.is_empty(), "source LSI p{p} is empty");
        assert_eq!(
            on_target, on_source,
            "LSI ByLocal p{p} differs after restore"
        );
        lsi_rows += on_source.len();
    }
    assert_eq!(lsi_rows, ITEMS);
    println!("  {ITEMS} items, {gsi_rows} GSI rows and {lsi_rows} LSI rows compared equal");
}
