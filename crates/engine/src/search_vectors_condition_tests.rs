// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which conditions reach the backend as equality pairs and which as an item
//! filter, for an account the extended grammar is enabled for.
//!
//! The service compares inline-filter strings and binary values exactly and
//! inline-filter numbers as binary64. Measured on DynamoDB on 2026-11-23 with
//! conditions of equalities only, two runs: `Price = 9007199254740993` matches
//! a stored 9007199254740992, and `Cat = "A"` does not match a stored "a".

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpError, OpResult, SettingsStore};
use extenddb_storage_sqlite::SqliteEngine;
use futures::future::BoxFuture;

use super::{Scope, parse, resolve};

struct Store(Mutex<String>);

impl SettingsStore for Store {
    fn get_setting(&self, _key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let value = self.0.lock().unwrap().clone();
        Box::pin(async move { Ok(Some(value)) })
    }

    fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

async fn server(setting: &str) -> Arc<dyn StorageEngine> {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    );
    crate::install_search_filter_settings(&storage, Arc::new(Store(Mutex::new(setting.into()))));
    storage
}

fn values() -> HashMap<String, AttributeValue> {
    [
        (":c", AttributeValue::S("US".to_owned())),
        (":s", AttributeValue::S("a".to_owned())),
        (":t", AttributeValue::B(vec![1])),
        (":p", AttributeValue::N("9007199254740993".to_owned())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

fn schema(hash: bool) -> Vec<SearchSchemaElement> {
    let element = |name: &str, element_type| SearchSchemaElement {
        attribute_name: name.to_owned(),
        element_type,
    };
    let mut schema = vec![
        element("Price", SearchSchemaElementType::InlineFilter),
        element("Cat", SearchSchemaElementType::InlineFilter),
        element("Tag", SearchSchemaElementType::InlineFilter),
    ];
    if hash {
        schema.insert(0, element("Country", SearchSchemaElementType::Hash));
    }
    schema
}

fn definitions() -> Vec<AttributeDefinition> {
    [
        ("Country", ScalarAttributeType::S),
        ("Price", ScalarAttributeType::N),
        ("Cat", ScalarAttributeType::S),
        ("Tag", ScalarAttributeType::B),
    ]
    .into_iter()
    .map(|(name, attribute_type)| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    })
    .collect()
}

/// What the backend receives: the HASH value, the equality pairs, and
/// whether it gets an item filter. The filter, when present, is applied to
/// `item`.
type Routed = (
    Option<AttributeValue>,
    Vec<(String, AttributeValue)>,
    Option<bool>,
);

async fn routed(setting: &str, hash: bool, expr: &str, item: &Item) -> Routed {
    let storage = server(setting).await;
    let served = storage
        .as_vector_search()
        .expect("SQLite serves vector search");
    let values: HashMap<String, AttributeValue> = values()
        .into_iter()
        .filter(|(k, _)| expr.contains(k.as_str()))
        .collect();
    let parsed = parse(
        &storage,
        served,
        "111111111111",
        Some(expr),
        None,
        Some(&values),
        None,
    )
    .await
    .unwrap();
    let schema = schema(hash);
    let (hash_key, filters, item_filter): Scope<'_> = resolve(
        &parsed,
        Some(&schema),
        &definitions(),
        &storage,
        "111111111111",
    )
    .await
    .unwrap();
    (
        hash_key.map(|(_, v)| v.clone()),
        filters
            .into_iter()
            .map(|(n, v)| (n.to_owned(), v.clone()))
            .collect(),
        item_filter.map(|f| f.matches(item).unwrap()),
    )
}

/// An item whose `Price` equals `:p` in binary64 and not exactly.
fn item() -> Item {
    [
        ("Country", AttributeValue::S("US".to_owned())),
        ("Price", AttributeValue::N("9007199254740992".to_owned())),
        ("Cat", AttributeValue::S("a".to_owned())),
        ("Tag", AttributeValue::B(vec![1])),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

fn us() -> Option<AttributeValue> {
    Some(AttributeValue::S("US".to_owned()))
}

fn pair(name: &str, value: &str) -> Vec<(String, AttributeValue)> {
    values()
        .get(value)
        .map(|v| vec![(name.to_owned(), v.clone())])
        .unwrap()
}

#[tokio::test]
async fn string_and_binary_equalities_stay_equality_pairs() {
    assert_eq!(
        routed("all", true, "Country = :c AND Cat = :s", &item()).await,
        (us(), pair("Cat", ":s"), None)
    );
    let tag_then_cat = [pair("Tag", ":t"), pair("Cat", ":s")].concat();
    assert_eq!(
        routed(
            "all",
            true,
            "Tag = :t AND Country = :c AND Cat = :s",
            &item()
        )
        .await,
        (us(), tag_then_cat, None)
    );
    let cat_then_tag = [pair("Cat", ":s"), pair("Tag", ":t")].concat();
    assert_eq!(
        routed("all", false, "Cat = :s AND Tag = :t", &item()).await,
        (None, cat_then_tag, None)
    );
}

#[tokio::test]
async fn the_hash_equality_alone_needs_no_filter() {
    assert_eq!(
        routed("all", true, "Country = :c", &item()).await,
        (us(), Vec::new(), None)
    );
}

#[tokio::test]
async fn a_number_equality_goes_to_the_item_filter_and_compares_as_binary64() {
    assert_eq!(
        routed("all", true, "Country = :c AND Price = :p", &item()).await,
        (us(), Vec::new(), Some(true))
    );
    assert_eq!(
        routed(
            "all",
            true,
            "Country = :c AND Cat = :s AND Price = :p",
            &item()
        )
        .await,
        (us(), Vec::new(), Some(true))
    );
    assert_eq!(
        routed("all", false, "Price = :p", &item()).await,
        (None, Vec::new(), Some(true))
    );
}

#[tokio::test]
async fn a_condition_beyond_equality_goes_to_the_item_filter() {
    for expr in [
        "Country = :c AND Cat <> :s",
        "Country = :c AND (Cat = :s OR Tag = :t)",
        "Country = :c AND attribute_exists(Cat)",
        "Country = :c AND Cat IN (:s)",
    ] {
        let (hash_key, filters, filter) = routed("all", true, expr, &item()).await;
        assert_eq!((hash_key, filters), (us(), Vec::new()), "{expr}");
        assert!(filter.is_some(), "{expr}");
    }
}

#[tokio::test]
async fn an_account_the_setting_leaves_out_keeps_the_equality_grammar() {
    assert_eq!(
        routed("off", true, "Country = :c AND Price = :p", &item()).await,
        (us(), pair("Price", ":p"), None)
    );
}

/// A settings store whose every read fails, counting the reads.
#[derive(Default)]
struct Failing(AtomicUsize);

impl SettingsStore for Failing {
    fn get_setting(&self, _key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(OpError::Internal("connection reset".to_owned())) })
    }

    fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// The scope, or the error, of `expr` on a server whose settings store
/// fails every read, and the number of reads it got.
async fn without_the_setting(
    expr: &str,
    names: Option<&HashMap<String, String>>,
) -> (Result<Routed, String>, usize) {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    );
    let store = Arc::new(Failing::default());
    crate::install_search_filter_settings(&storage, store.clone());
    let served = storage
        .as_vector_search()
        .expect("SQLite serves vector search");
    let values: HashMap<String, AttributeValue> = values()
        .into_iter()
        .filter(|(k, _)| expr.contains(k.as_str()))
        .collect();
    let schema = schema(true);
    let answer = async {
        let parsed = parse(
            &storage,
            served,
            "111111111111",
            Some(expr),
            names,
            Some(&values),
            None,
        )
        .await?;
        let (hash_key, filters, item_filter) = resolve(
            &parsed,
            Some(&schema),
            &definitions(),
            &storage,
            "111111111111",
        )
        .await?;
        Ok::<Routed, extenddb_core::error::DynamoDbError>((
            hash_key.map(|(_, v)| v.clone()),
            filters
                .into_iter()
                .map(|(n, v)| (n.to_owned(), v.clone()))
                .collect(),
            item_filter.map(|f| f.matches(&item()).unwrap()),
        ))
    }
    .await
    .map_err(|e| e.message().to_owned());
    (answer, store.0.load(Ordering::SeqCst))
}

/// A condition that both grammars answer alike is served without a read of
/// the setting, so a setting that cannot be read does not refuse it.
#[tokio::test]
async fn a_condition_both_grammars_answer_alike_does_not_read_the_setting() {
    assert_eq!(
        without_the_setting("Country = :c AND Cat = :s", None).await,
        (Ok((us(), pair("Cat", ":s"), None)), 0)
    );
    assert_eq!(
        without_the_setting("Country = :c AND Tag = :t", None).await,
        (Ok((us(), pair("Tag", ":t"), None)), 0)
    );
    // the same schema refusal under both grammars
    assert_eq!(
        without_the_setting("Cat = :s", None).await,
        (
            Err("SearchConditionExpression must have \
                 all HASH attributes in configured SearchSchema"
                .to_owned()),
            0
        )
    );
    // the same unused-names refusal under both grammars
    let names: HashMap<String, String> = [("#u".to_owned(), "x".to_owned())].into();
    assert_eq!(
        without_the_setting("Country = :c", Some(&names)).await,
        (
            Err(
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"
                    .to_owned()
            ),
            0
        )
    );
}

/// A condition whose answer depends on the grammar reads the setting, and a
/// read that fails before any read answered is a retryable fault.
#[tokio::test]
async fn a_condition_the_grammars_answer_differently_reads_the_setting() {
    let unavailable = Err("Service is temporarily unavailable".to_owned());
    for expr in ["Country = :c AND Cat <> :s", "Country = :c AND Price = :p"] {
        assert_eq!(
            without_the_setting(expr, None).await,
            (unavailable.clone(), 1),
            "{expr}"
        );
    }
}
