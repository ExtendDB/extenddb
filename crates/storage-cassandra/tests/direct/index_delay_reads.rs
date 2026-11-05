// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How often the write paths read `index_propagation_delay_ms` (#407).
//!
//! A write reads the setting at most once per write call, and only when some
//! GSI of the table follows it. A retried write does not read it again.
//! BatchWriteItem makes one write call per item, as on SQLite and PostgreSQL.
//!
//! Each test runs on an engine of its own, so
//! `CassandraEngine::index_delay_reads` counts only its own writes.

use crate::helpers::{IsolatedAccount, IsolatedEngine, isolated_account, isolated_engine};
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, CreateTableInput, GsiInput, Item, KeySchemaElement,
    KeyType, LsiInput, Projection, ProjectionType, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::{DataEngine as _, MetadataEngine as _, TransactWriteOp};

const GSI: &str = "GsiIdx";

fn all_projected() -> Projection {
    Projection {
        projection_type: ProjectionType::All,
        non_key_attributes: None,
    }
}

/// A table keyed `id` (S), with a GSI on `gpk` (S) when `with_gsi` and an
/// LSI on `lsk` (S) under sort key `sk` (S) when `with_lsi`.
fn table_input(name: &str, with_gsi: bool, with_lsi: bool) -> CreateTableInput {
    let attribute = |name: &str| AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: ScalarAttributeType::S,
    };
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    };
    let mut key_schema = vec![key("id", KeyType::Hash)];
    let mut attribute_definitions = vec![attribute("id")];
    if with_lsi {
        key_schema.push(key("sk", KeyType::Range));
        attribute_definitions.push(attribute("sk"));
        attribute_definitions.push(attribute("lsk"));
    }
    if with_gsi {
        attribute_definitions.push(attribute("gpk"));
    }
    CreateTableInput {
        table_name: name.to_string(),
        key_schema,
        attribute_definitions,
        local_secondary_indexes: with_lsi.then(|| {
            vec![LsiInput {
                index_name: "LsiIdx".to_string(),
                key_schema: vec![key("id", KeyType::Hash), key("lsk", KeyType::Range)],
                projection: all_projected(),
            }]
        }),
        global_secondary_indexes: with_gsi.then(|| {
            vec![GsiInput {
                index_name: GSI.to_string(),
                key_schema: vec![key("gpk", KeyType::Hash)],
                projection: all_projected(),
                provisioned_throughput: None,
            }]
        }),
        vector_indexes: None,
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        stream_specification: None,
        sse_specification: None,
        tags: None,
        table_class: None,
        deletion_protection_enabled: None,
    }
}

fn item(id: &str) -> Item {
    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S(id.to_string()));
    item.insert("gpk".to_string(), AttributeValue::S("g".to_string()));
    item
}

fn key(id: &str) -> Item {
    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S(id.to_string()));
    key
}

async fn put(isolated: &IsolatedEngine, account: &IsolatedAccount, item: Item) {
    isolated
        .engine
        .put_item(
            &account.key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put_item");
}

/// An engine with the setting at 0 and one account whose table has a GSI
/// that follows it, TTL enabled, and item `held` written with a TTL claim
/// on it, so every write to `held` loses its claim and retries until it
/// gives up. 0 keeps the GSI synchronous, which TTL requires; the GSI still
/// follows the setting, so a write still reads it.
async fn claimed_item_fixture() -> (IsolatedEngine, IsolatedAccount) {
    let isolated = isolated_engine("idr").await;
    isolated
        .settings()
        .set_setting("index_propagation_delay_ms", "0")
        .await
        .unwrap();
    let account = isolated_account(&isolated, table_input("DelayReads", true, false)).await;
    let engine = &isolated.engine;
    let (account_id, table_name) = (&account.key_info.account_id, &account.key_info.table_name);
    // A new table is CREATING until the control plane moves it on.
    let mut enabled = false;
    for _ in 0..50 {
        engine.process_control_plane_transitions().await.unwrap();
        match engine
            .update_ttl(account_id, table_name, "expires_at", true)
            .await
        {
            Ok(_) => {
                enabled = true;
                break;
            }
            Err(StorageError::TableNotActive(_)) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("update_ttl: {e:?}"),
        }
    }
    assert!(enabled, "the table did not become ACTIVE");
    engine
        .create_ttl_index(account_id, table_name, "expires_at")
        .await
        .unwrap();

    let held = item("held");
    put(&isolated, &account, held.clone()).await;
    engine
        .acquire_ttl_mutation_claim(&account.key_info, &key("held"), Some(&held))
        .await
        .expect("claim the item");
    (isolated, account)
}

fn assert_gave_up<T: std::fmt::Debug>(result: Result<T, StorageError>, write: &str) {
    assert!(
        matches!(result, Err(StorageError::TransactionConflict(_))),
        "{write} on a claimed item must retry until it gives up, got {result:?}"
    );
}

/// A PutItem that retries until it gives up reads the setting once.
#[tokio::test]
async fn retried_put_item_reads_the_setting_once() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (isolated, account) = claimed_item_fixture().await;
    let before = isolated.engine.index_delay_reads();

    let result = isolated
        .engine
        .put_item(
            &account.key_info,
            item("held"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await;

    assert_gave_up(result, "PutItem");
    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        1,
        "a PutItem read the setting once per attempt"
    );
}

/// A DeleteItem that retries until it gives up reads the setting once.
#[tokio::test]
async fn retried_delete_item_reads_the_setting_once() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (isolated, account) = claimed_item_fixture().await;
    let before = isolated.engine.index_delay_reads();

    let result = isolated
        .engine
        .delete_item(
            &account.key_info,
            &key("held"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await;

    assert_gave_up(result, "DeleteItem");
    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        1,
        "a DeleteItem read the setting once per attempt"
    );
}

/// An UpdateItem that retries until it gives up reads the setting once.
#[tokio::test]
async fn retried_update_item_reads_the_setting_once() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (isolated, account) = claimed_item_fixture().await;
    let before = isolated.engine.index_delay_reads();

    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("n".to_string())],
        value: Expr::Placeholder(":n".to_string()),
    }];
    let mut values = std::collections::HashMap::new();
    values.insert(":n".to_string(), AttributeValue::N("1".to_string()));
    let maps = ExpressionMaps::new(std::collections::HashMap::new(), values);
    let result = isolated
        .engine
        .update_item(
            &account.key_info,
            &key("held"),
            &actions,
            false,
            false,
            None,
            &maps,
            None,
        )
        .await;

    assert_gave_up(result, "UpdateItem");
    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        1,
        "an UpdateItem read the setting once per attempt"
    );
}

/// A TransactWriteItems of several puts reads the setting once.
#[tokio::test]
async fn transaction_reads_the_setting_once() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const PUTS: usize = 5;
    let isolated = isolated_engine("idr").await;
    let account = isolated_account(&isolated, table_input("DelayReadsTxn", true, false)).await;
    let items: Vec<Item> = (0..PUTS).map(|i| item(&format!("t{i}"))).collect();
    let maps = ExpressionMaps::default();
    let ops: Vec<TransactWriteOp<'_>> = items
        .iter()
        .map(|item| TransactWriteOp::Put {
            key_info: &account.key_info,
            item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        })
        .collect();
    let before = isolated.engine.index_delay_reads();

    isolated
        .engine
        .transact_write_items(&ops, None)
        .await
        .expect("transact_write_items");

    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        1,
        "a transaction of {PUTS} puts did not read the setting exactly once"
    );
}

/// A write to a table whose only index is an LSI reads nothing: an LSI is
/// always synchronous.
#[tokio::test]
async fn lsi_only_table_reads_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let isolated = isolated_engine("idr").await;
    let account = isolated_account(&isolated, table_input("DelayReadsLsi", false, true)).await;
    let mut lsi_item = Item::new();
    lsi_item.insert("id".to_string(), AttributeValue::S("a".to_string()));
    lsi_item.insert("sk".to_string(), AttributeValue::S("s".to_string()));
    lsi_item.insert("lsk".to_string(), AttributeValue::S("l".to_string()));
    let before = isolated.engine.index_delay_reads();

    put(&isolated, &account, lsi_item).await;

    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        0,
        "a write to an LSI-only table read the setting"
    );
}

/// A write to a table whose only GSI sets a delay of its own reads nothing:
/// the GSI uses its own delay.
#[tokio::test]
async fn gsi_with_its_own_delay_reads_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let isolated = isolated_engine("idr").await;
    let account = isolated_account(&isolated, table_input("DelayReadsOwn", true, false)).await;
    isolated
        .engine
        .session()
        .query_with_values(
            format!(
                "UPDATE {}.indexes SET propagation_delay_ms = ? \
                 WHERE table_id = ? AND index_name = ?",
                isolated.engine.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(0_i32, account.key_info.table_id.as_str(), GSI),
        )
        .await
        .unwrap();
    let before = isolated.engine.index_delay_reads();

    put(&isolated, &account, item("own")).await;

    assert_eq!(
        isolated.engine.index_delay_reads() - before,
        0,
        "a write to a table whose GSI sets its own delay read the setting"
    );
}
