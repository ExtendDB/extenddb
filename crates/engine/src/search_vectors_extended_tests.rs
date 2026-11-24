// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which requests the extended grammar parses, and the filter it hands the
//! backend.
//!
//! Each test installs its store against a storage engine of its own.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};
use extenddb_storage::management_store::{OpError, OpResult, SettingsStore};
use extenddb_storage::{
    BoxedFuture, StorageEngine, VectorSearch, VectorSearchEngine, VectorSearchOutput,
    VectorSearchResult,
};
use extenddb_storage_sqlite::SqliteEngine;
use futures::future::BoxFuture;

use super::imp::{parse_request, resolve_scope};
use super::install_search_filter_settings;

const CALLER: &str = "111111111111";

/// A settings store holding one optional value for every key, or failing
/// every read.
struct Store(Mutex<Result<Option<String>, ()>>);

impl SettingsStore for Store {
    fn get_setting(&self, _key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let answer = self.0.lock().unwrap().clone();
        Box::pin(async move { answer.map_err(|()| OpError::Internal("unreachable".to_owned())) })
    }

    fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// A storage engine whose server reads `value` for the setting.
async fn install(value: Result<Option<&str>, ()>) -> Arc<dyn StorageEngine> {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    );
    let value = value.map(|v| v.map(str::to_owned));
    install_search_filter_settings(&storage, Arc::new(Store(Mutex::new(value))));
    storage
}

struct Served;

impl VectorSearchEngine for Served {
    fn search_vectors(&self, _req: VectorSearch<'_>) -> BoxedFuture<'_, VectorSearchResult> {
        Box::pin(async {
            Ok(VectorSearchOutput {
                hits: Vec::new(),
                distance_function: extenddb_core::types::DistanceFunction::Cosine,
            })
        })
    }
}

fn values() -> HashMap<String, AttributeValue> {
    [
        (":c".to_owned(), AttributeValue::S("US".to_owned())),
        (":p".to_owned(), AttributeValue::N("10".to_owned())),
    ]
    .into()
}

const INEQUALITY: &str = "Country = :c AND Price > :p";

async fn parsed(
    storage: &Arc<dyn StorageEngine>,
    expr: Option<&str>,
) -> Result<bool, DynamoDbError> {
    let values = values();
    parse_request(storage, &Served, CALLER, expr, None, Some(&values), None)
        .await
        .map(|parsed| parsed.is_some())
}

#[tokio::test]
async fn the_setting_selects_the_accounts() {
    for (value, expected) in [
        // an absent or empty value is every account
        (Ok(None), true),
        (Ok(Some("")), true),
        (Ok(Some("off")), false),
        (Ok(Some("all")), true),
        (Ok(Some("111111111111")), true),
        (Ok(Some("222222222222,111111111111")), true),
        (Ok(Some("222222222222")), false),
        // a value that does not parse is logged and reads as every account
        (Ok(Some("on")), true),
    ] {
        let storage = install(value).await;
        assert_eq!(
            parsed(&storage, Some(INEQUALITY)).await.unwrap(),
            expected,
            "{value:?}"
        );
    }
}

#[tokio::test]
async fn a_store_that_fails_answers_a_retryable_fault() {
    let storage = install(Err(())).await;
    let err = parsed(&storage, Some(INEQUALITY)).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ServiceUnavailable(m)
            if m == "Service is temporarily unavailable"),
        "{err:?}"
    );
}

#[tokio::test]
async fn without_an_installed_store_every_account_gets_the_grammar() {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    );
    assert!(parsed(&storage, Some(INEQUALITY)).await.unwrap());
}

#[tokio::test]
async fn no_expression_is_left_to_the_equality_path() {
    let storage = install(Ok(Some("all"))).await;
    assert!(!parsed(&storage, None).await.unwrap());
}

#[tokio::test]
async fn an_enabled_account_gets_the_extended_messages() {
    let storage = install(Ok(Some("all"))).await;
    let err = parsed(
        &storage,
        Some("Country = :c AND Price > :p AND begins_with(Category, :c)"),
    )
    .await
    .unwrap_err();
    let DynamoDbError::ValidationException(message) = err else {
        panic!("expected ValidationException, got {err:?}");
    };
    assert_eq!(
        message,
        "Invalid SearchConditionExpression: The function is not allowed in a search \
         condition; function: begins_with"
    );
}

#[tokio::test]
async fn a_name_neither_expression_uses_is_refused() {
    let storage = install(Ok(Some("all"))).await;
    let values = values();
    let names: HashMap<String, String> = [("#u".to_owned(), "x".to_owned())].into();
    let err = parse_request(
        &storage,
        &Served,
        CALLER,
        Some(INEQUALITY),
        Some(&names),
        Some(&values),
        None,
    )
    .await
    .err()
    .expect("refused");
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m)
            if m == "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"),
        "{err:?}"
    );
    let used: HashMap<String, String> = [("#u".to_owned(), "Price".to_owned())].into();
    let projected = parse_request(
        &storage,
        &Served,
        CALLER,
        Some(INEQUALITY),
        Some(&used),
        Some(&values),
        Some("#u"),
    )
    .await;
    assert!(projected.is_ok_and(|e| e.is_some()));
}

fn item(country: &str, price: &str) -> Item {
    [
        ("Country".to_owned(), AttributeValue::S(country.to_owned())),
        ("Price".to_owned(), AttributeValue::N(price.to_owned())),
    ]
    .into()
}

#[tokio::test]
async fn the_scope_selects_the_hash_partition_and_filters_items() {
    let storage = install(Ok(Some("all"))).await;
    let values = values();
    let extended = parse_request(
        &storage,
        &Served,
        CALLER,
        Some(INEQUALITY),
        None,
        Some(&values),
        None,
    )
    .await
    .unwrap()
    .expect("parsed with the extended grammar");
    let schema = [
        SearchSchemaElement {
            attribute_name: "Country".to_owned(),
            element_type: SearchSchemaElementType::Hash,
        },
        SearchSchemaElement {
            attribute_name: "Price".to_owned(),
            element_type: SearchSchemaElementType::InlineFilter,
        },
    ];
    let definitions = [
        AttributeDefinition {
            attribute_name: "Country".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "Price".to_owned(),
            attribute_type: ScalarAttributeType::N,
        },
    ];
    let (hash_key, filter) = resolve_scope(&extended, Some(&schema), &definitions).unwrap();
    assert_eq!(
        hash_key,
        Some(("Country", &AttributeValue::S("US".to_owned())))
    );
    assert!(filter.matches(&item("US", "11")).unwrap());
    assert!(!filter.matches(&item("US", "10")).unwrap());
    assert!(!filter.matches(&item("CA", "11")).unwrap());
}
