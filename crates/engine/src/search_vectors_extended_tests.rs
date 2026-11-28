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
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpError, OpResult, SettingsStore};
use futures::future::BoxFuture;

use super::imp::{enabled_for, parse, resolve_scope};
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
    let storage = crate::test_context::memory_engine().await;
    let value = value.map(|v| v.map(str::to_owned));
    install_search_filter_settings(&storage, Arc::new(Store(Mutex::new(value))));
    storage
}

fn values() -> HashMap<String, AttributeValue> {
    [
        (":c".to_owned(), AttributeValue::S("US".to_owned())),
        (":p".to_owned(), AttributeValue::N("10".to_owned())),
    ]
    .into()
}

const INEQUALITY: &str = "Country = :c AND Price > :p";

async fn enabled(storage: &Arc<dyn StorageEngine>) -> Result<bool, DynamoDbError> {
    enabled_for(storage, CALLER).await
}

static LIMITS: std::sync::LazyLock<extenddb_core::limits::LimitsConfig> =
    std::sync::LazyLock::new(extenddb_core::limits::LimitsConfig::default);

fn parsed(expr: &str) -> Result<super::imp::Extended, DynamoDbError> {
    parse(
        expr,
        None,
        Some(&values()),
        None,
        &extenddb_core::limits::LimitsConfig::default(),
    )
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
        // a value that does not parse is logged and enables no account
        (Ok(Some("on")), false),
    ] {
        let storage = install(value).await;
        assert_eq!(enabled(&storage).await.unwrap(), expected, "{value:?}");
    }
}

#[tokio::test]
async fn a_store_that_fails_answers_a_retryable_fault() {
    let storage = install(Err(())).await;
    let err = enabled(&storage).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ServiceUnavailable(m)
            if m == "Service is temporarily unavailable"),
        "{err:?}"
    );
}

#[tokio::test]
async fn without_an_installed_store_every_account_gets_the_grammar() {
    let storage = crate::test_context::memory_engine().await;
    assert!(enabled(&storage).await.unwrap());
}

#[test]
fn the_extended_grammar_gives_its_own_messages() {
    let Err(err) = parsed("Country = :c AND Price > :p AND begins_with(Category, :c)") else {
        panic!("refused");
    };
    let DynamoDbError::ValidationException(message) = err else {
        panic!("expected ValidationException, got {err:?}");
    };
    assert_eq!(
        message,
        "Invalid SearchConditionExpression: The function is not allowed in a search \
         condition; function: begins_with"
    );
}

#[test]
fn a_name_neither_expression_uses_is_refused() {
    let values = values();
    let names: HashMap<String, String> = [("#u".to_owned(), "x".to_owned())].into();
    let err = parse(INEQUALITY, Some(&names), Some(&values), None, &LIMITS)
        .err()
        .expect("refused");
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m)
            if m == "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"),
        "{err:?}"
    );
    let used: HashMap<String, String> = [("#u".to_owned(), "Price".to_owned())].into();
    assert!(parse(INEQUALITY, Some(&used), Some(&values), Some("#u"), &LIMITS).is_ok());
}

fn item(country: &str, price: &str) -> Item {
    [
        ("Country".to_owned(), AttributeValue::S(country.to_owned())),
        ("Price".to_owned(), AttributeValue::N(price.to_owned())),
    ]
    .into()
}

#[test]
fn the_scope_selects_the_hash_partition_and_filters_items() {
    let Ok(extended) = parsed(INEQUALITY) else {
        panic!("parsed with the extended grammar");
    };
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
