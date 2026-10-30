// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for the restore copy's write path: what a
//! restored row leaves behind, and that the ordinary write paths (`PutItem`
//! with and without a condition, `UpdateItem`, `DeleteItem`,
//! `TransactWriteItems`) read, fence and move restored items and their index
//! rows the way they do items the put path wrote.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, GsiInput, KeyType, LsiInput, Projection,
    ProjectionType, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TableKeyInfo,
    TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupEngine, DataEngine, RestoreTableOverrides, TableEngine, TransactWriteOp,
};
use extenddb_storage_cassandra::CassandraEngine;

use crate::backup_engine::{
    attr, create_active_table, item, key, ks, make_gsis_synchronous, put_all, query_index, restore,
    wait_active,
};
use crate::helpers::{TestAccount, setup_engine};

type Item = BTreeMap<String, AttributeValue>;

pub(crate) fn s(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_owned())
}

pub(crate) fn n(value: i64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

fn path(attr: &str) -> Expr {
    Expr::Path(vec![PathElement::Attribute(attr.to_owned())])
}

/// `attr = :v` with `:v` bound to `value`.
fn equals(attr: &str, value: AttributeValue) -> (Expr, ExpressionMaps) {
    let condition = Expr::Compare {
        left: Box::new(path(attr)),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":v".to_owned())),
    };
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), value);
    (condition, ExpressionMaps::new(HashMap::new(), values))
}

/// The raw `version` column of a restored row, read with the row's text keys.
async fn row_version(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    id: &str,
    sort: Option<&str>,
) -> Option<i64> {
    let keyspace = format!("extenddb_ttl_test_account_{}", key_info.account_id);
    let table = format!("items_{}", key_info.table_id.replace('-', "_"));
    let rows = match sort {
        Some(sort) => {
            let cql = format!("SELECT version FROM {keyspace}.{table} WHERE pk = ? AND sk_s = ?");
            engine
                .session()
                .query_with_values(&cql, cdrs_tokio::query_values!(id, sort))
                .await
        }
        None => {
            let cql = format!("SELECT version FROM {keyspace}.{table} WHERE pk = ?");
            engine
                .session()
                .query_with_values(&cql, cdrs_tokio::query_values!(id))
                .await
        }
    }
    .expect("read version")
    .response_body()
    .expect("version body")
    .into_rows()
    .unwrap_or_default();
    rows.into_iter()
        .next()
        .and_then(|row| row.get_by_name("version").ok().flatten())
}

/// An indexed source table (base `id` + `sort`, GSI `ByGroup` on `gpk` +
/// `gsk` projecting `value`, LSI `ByLocal` on `id` + `lsk` projecting all),
/// seeded with `items`, backed up and restored into `target`. Returns the
/// account and the restored table's key info.
pub(crate) async fn restored_indexed_table(
    engine: &CassandraEngine,
    items: Vec<Item>,
    target: &str,
) -> (TestAccount, TableKeyInfo) {
    let account = TestAccount::new(engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        engine,
        &account_id,
        CreateTableInput {
            table_name: format!("{target}Source"),
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
    make_gsis_synchronous(engine, &source.table_id, &["ByGroup"]).await;
    put_all(engine, &source, items).await;
    let backup = engine
        .create_backup(&account_id, &source.table_name, "restore-write")
        .await
        .expect("create backup");
    let initial = restore(
        engine,
        &account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    assert_eq!(initial.table_status, TableStatus::Creating);
    wait_active(engine, &account_id, target, Duration::from_secs(120)).await;
    let restored = engine
        .table_key_info(&account_id, target)
        .await
        .expect("target key info");
    // Restored GSIs are synchronous in these tests as well, so a later write's
    // index row lands before it is queried.
    make_gsis_synchronous(engine, &restored.table_id, &["ByGroup"]).await;
    (account, restored)
}

fn indexed_item(id: &str, sort: &str, group: &str, gsk: i64, lsk: i64, value: &str) -> Item {
    let mut it = item(id, Some(sort), value);
    it.insert("gpk".to_owned(), s(group));
    it.insert("gsk".to_owned(), n(gsk));
    it.insert("lsk".to_owned(), n(lsk));
    it
}

/// The ids (`id|sort`) a Query on `index` for `pk_attr = value` returns.
async fn index_keys(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    index: &str,
    pk_attr: &str,
    value: &str,
) -> Vec<String> {
    let mut keys: Vec<String> = query_index(engine, key_info, index, pk_attr, s(value))
        .await
        .into_iter()
        .map(|it| {
            format!(
                "{}|{}",
                match it.get("id") {
                    Some(AttributeValue::S(v)) => v.clone(),
                    other => format!("{other:?}"),
                },
                match it.get("sort") {
                    Some(AttributeValue::S(v)) => v.clone(),
                    other => format!("{other:?}"),
                }
            )
        })
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn test_restored_rows_carry_a_version_and_take_every_later_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let items = vec![
        indexed_item("a", "1", "g1", 1, 10, "v-a1"),
        indexed_item("a", "2", "g1", 2, 20, "v-a2"),
        indexed_item("b", "1", "g2", 3, 30, "v-b1"),
        indexed_item("b", "2", "g2", 4, 40, "v-b2"),
        indexed_item("c", "1", "g3", 5, 50, "v-c1"),
        // No GSI keys: absent from the sparse GSI, present in the LSI.
        {
            let mut it = item("c", Some("2"), "v-c2");
            it.insert("lsk".to_owned(), n(60));
            it
        },
    ];
    let (_account, restored) = restored_indexed_table(&engine, items, "RestoreWrite").await;
    let maps = ExpressionMaps::default();

    // Every restored row carries a version in the microsecond clock domain.
    for (id, sort) in [("a", "1"), ("b", "2"), ("c", "2")] {
        let version = row_version(&engine, &restored, id, Some(sort)).await;
        assert!(
            version.is_some_and(|v| v > 1_000_000_000_000_000),
            "restored row {id}/{sort} version {version:?} is not a clock reading"
        );
    }
    assert_eq!(
        index_keys(&engine, &restored, "ByGroup", "gpk", "g1").await,
        ["a|1", "a|2"]
    );
    assert_eq!(
        index_keys(&engine, &restored, "ByLocal", "id", "c").await,
        ["c|1", "c|2"]
    );

    // UpdateItem moves a/1 from g1 to g9 and changes its LSI key; the old
    // index rows go, the new ones arrive, the projection carries the new value.
    let before = row_version(&engine, &restored, "a", Some("1")).await;
    let mut set_values = HashMap::new();
    set_values.insert(":g".to_owned(), s("g9"));
    set_values.insert(":l".to_owned(), n(11));
    set_values.insert(":val".to_owned(), s("v-a1-updated"));
    let set_maps = ExpressionMaps::new(HashMap::new(), set_values);
    let actions = [
        UpdateAction::Set {
            path: vec![PathElement::Attribute("gpk".to_owned())],
            value: Expr::Placeholder(":g".to_owned()),
        },
        UpdateAction::Set {
            path: vec![PathElement::Attribute("lsk".to_owned())],
            value: Expr::Placeholder(":l".to_owned()),
        },
        UpdateAction::Set {
            path: vec![PathElement::Attribute("value".to_owned())],
            value: Expr::Placeholder(":val".to_owned()),
        },
    ];
    let (old, new) = engine
        .update_item(
            &restored,
            &key("a", Some("1")),
            &actions,
            true,
            true,
            None,
            &set_maps,
            None,
        )
        .await
        .expect("update restored item");
    assert_eq!(
        old.as_ref().and_then(|it| it.get("value")),
        Some(&s("v-a1"))
    );
    assert_eq!(
        new.as_ref().and_then(|it| it.get("value")),
        Some(&s("v-a1-updated"))
    );
    let after = row_version(&engine, &restored, "a", Some("1")).await;
    assert!(
        after > before,
        "version must advance: {before:?} -> {after:?}"
    );
    assert_eq!(
        index_keys(&engine, &restored, "ByGroup", "gpk", "g1").await,
        ["a|2"]
    );
    let moved = query_index(&engine, &restored, "ByGroup", "gpk", s("g9")).await;
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].get("value"), Some(&s("v-a1-updated")));
    assert_eq!(moved[0].get("gsk"), Some(&n(1)));
    let local = query_index(&engine, &restored, "ByLocal", "id", s("a")).await;
    let lsks: Vec<&AttributeValue> = local.iter().filter_map(|it| it.get("lsk")).collect();
    assert_eq!(lsks, [&n(11), &n(20)]);

    // A conditional PutItem reads the restored image: the condition on the
    // restored value holds, the condition on a wrong value fails and returns
    // the restored item.
    let (holds, holds_maps) = equals("value", s("v-b1"));
    let mut replacement = indexed_item("b", "1", "g2", 3, 30, "v-b1-put");
    replacement.remove("gsk");
    replacement.insert("gsk".to_owned(), n(33));
    engine
        .put_item(
            &restored,
            replacement.clone(),
            false,
            Some(&holds),
            &holds_maps,
            None,
        )
        .await
        .expect("conditional put over a restored item");
    assert_eq!(
        engine
            .get_item(&restored, &key("b", Some("1")))
            .await
            .expect("get b/1"),
        Some(replacement)
    );
    let (fails, fails_maps) = equals("value", s("v-b2-wrong"));
    let failed = engine
        .put_item(
            &restored,
            indexed_item("b", "2", "g2", 4, 40, "never"),
            false,
            Some(&fails),
            &fails_maps,
            None,
        )
        .await;
    match failed {
        Err(StorageError::ConditionFailed(Some(existing))) => {
            assert_eq!(existing.get("value"), Some(&s("v-b2")));
        }
        other => panic!("expected ConditionFailed with the restored item, got {other:?}"),
    }
    let in_g2 = query_index(&engine, &restored, "ByGroup", "gpk", s("g2")).await;
    let gsks: Vec<&AttributeValue> = in_g2.iter().filter_map(|it| it.get("gsk")).collect();
    assert_eq!(
        gsks,
        [&n(4), &n(33)],
        "old b/1 GSI row at gsk=3 must be gone"
    );

    // attribute_not_exists on a restored key fails: the row exists.
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![path("id")],
    };
    let refused = engine
        .put_item(
            &restored,
            indexed_item("c", "1", "g3", 5, 50, "never"),
            false,
            Some(&not_exists),
            &maps,
            None,
        )
        .await;
    assert!(
        matches!(refused, Err(StorageError::ConditionFailed(_))),
        "attribute_not_exists must see the restored row, got {refused:?}"
    );

    // DeleteItem with a condition on the restored value removes the row and
    // its index rows.
    let (holds, holds_maps) = equals("value", s("v-a2"));
    let deleted = engine
        .delete_item(
            &restored,
            &key("a", Some("2")),
            true,
            Some(&holds),
            &holds_maps,
            None,
        )
        .await
        .expect("conditional delete of a restored item");
    assert_eq!(
        deleted.and_then(|it| it.get("value").cloned()),
        Some(s("v-a2"))
    );
    assert_eq!(
        engine
            .get_item(&restored, &key("a", Some("2")))
            .await
            .expect("get a/2"),
        None
    );
    assert!(
        index_keys(&engine, &restored, "ByGroup", "gpk", "g1")
            .await
            .is_empty()
    );
    let local = query_index(&engine, &restored, "ByLocal", "id", s("a")).await;
    assert_eq!(local.len(), 1, "a/2's LSI row must be gone");

    // TransactWriteItems: a condition check on one restored item and a put
    // over another commit together; the put gives c/2 its first GSI row and
    // rewrites its LSI row in place.
    let (check, check_maps) = equals("value", s("v-c1"));
    let c2 = indexed_item("c", "2", "g7", 7, 60, "v-c2-txn");
    let c1_key = key("c", Some("1"));
    let ops = vec![
        TransactWriteOp::ConditionCheck {
            key_info: &restored,
            key: &c1_key,
            condition: &check,
            maps: &check_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
        TransactWriteOp::Put {
            key_info: &restored,
            item: &c2,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];
    engine
        .transact_write_items(&ops, None)
        .await
        .expect("transaction over restored items");
    assert_eq!(
        engine
            .get_item(&restored, &key("c", Some("2")))
            .await
            .expect("get c/2"),
        Some(c2)
    );
    assert_eq!(
        index_keys(&engine, &restored, "ByGroup", "gpk", "g7").await,
        ["c|2"]
    );
    let local = query_index(&engine, &restored, "ByLocal", "id", s("c")).await;
    let values: Vec<&AttributeValue> = local.iter().filter_map(|it| it.get("value")).collect();
    assert_eq!(values, [&s("v-c1"), &s("v-c2-txn")]);
}

#[tokio::test]
async fn test_restored_pk_only_rows_take_later_writes() {
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
            table_name: "RestoreWritePlainSource".to_owned(),
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
        (0..70)
            .map(|i| item(&format!("k{i:02}"), None, &format!("v{i}")))
            .collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "RestoreWritePlainSource", "plain")
        .await
        .expect("create backup");
    let target = "RestoreWritePlain";
    restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        target,
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore");
    let active = wait_active(&engine, &account_id, target, Duration::from_secs(120)).await;
    assert_eq!(active.item_count, 70);
    let restored = engine
        .table_key_info(&account_id, target)
        .await
        .expect("target key info");
    let maps = ExpressionMaps::default();

    // Items from the first and the second payload bucket (64 per bucket).
    for id in ["k00", "k63", "k64", "k69"] {
        assert!(
            row_version(&engine, &restored, id, None)
                .await
                .is_some_and(|v| v > 1_000_000_000_000_000),
            "{id} has no clock version"
        );
    }

    // Unconditional put over a restored row returns the restored image.
    let old = engine
        .put_item(&restored, item("k00", None, "p0"), true, None, &maps, None)
        .await
        .expect("put over restored");
    assert_eq!(old.and_then(|it| it.get("value").cloned()), Some(s("v0")));

    // UpdateItem with a condition on the restored value.
    let (holds, holds_maps) = equals("value", s("v63"));
    let actions = [UpdateAction::Set {
        path: vec![PathElement::Attribute("value".to_owned())],
        value: Expr::Placeholder(":v".to_owned()),
    }];
    let before = row_version(&engine, &restored, "k63", None).await;
    let (_, new) = engine
        .update_item(
            &restored,
            &key("k63", None),
            &actions,
            false,
            true,
            Some(&holds),
            &holds_maps,
            None,
        )
        .await
        .expect("conditional update of a restored item");
    assert_eq!(new.and_then(|it| it.get("value").cloned()), Some(s("v63")));
    let after = row_version(&engine, &restored, "k63", None).await;
    assert!(
        after > before,
        "version must advance: {before:?} -> {after:?}"
    );

    // attribute_not_exists(id) is refused on a restored key.
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![path("id")],
    };
    let refused = engine
        .put_item(
            &restored,
            item("k64", None, "never"),
            false,
            Some(&not_exists),
            &maps,
            None,
        )
        .await;
    assert!(
        matches!(refused, Err(StorageError::ConditionFailed(_))),
        "attribute_not_exists must see the restored row, got {refused:?}"
    );

    // A failing condition returns the restored item; a delete then removes it.
    let (fails, fails_maps) = equals("value", s("other"));
    let failed = engine
        .delete_item(
            &restored,
            &key("k69", None),
            true,
            Some(&fails),
            &fails_maps,
            None,
        )
        .await;
    match failed {
        Err(StorageError::ConditionFailed(Some(existing))) => {
            assert_eq!(existing.get("value"), Some(&s("v69")));
        }
        other => panic!("expected ConditionFailed with the restored item, got {other:?}"),
    }
    let deleted = engine
        .delete_item(&restored, &key("k69", None), true, None, &maps, None)
        .await
        .expect("delete restored item");
    assert_eq!(
        deleted.and_then(|it| it.get("value").cloned()),
        Some(s("v69"))
    );
    assert_eq!(
        engine
            .get_item(&restored, &key("k69", None))
            .await
            .expect("get k69"),
        None
    );
}
