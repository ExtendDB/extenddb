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
use std::sync::{Arc, Mutex};

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpResult, SettingsStore};
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
    let (hash_key, filters, item_filter): Scope<'_> =
        resolve(&parsed, Some(&schema), &definitions()).unwrap();
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
