// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A change of `search_inequality_filters` through the management API
//! reaches the next search at once, without waiting for the kept read of
//! the setting to expire.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use extenddb_core::limits::LimitsConfig;
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpResult, SettingsStore};
use extenddb_storage_sqlite::SqliteEngine;
use futures::future::BoxFuture;
use serde_json::json;

use super::set_setting;

/// A settings store that keeps what is written to it.
#[derive(Default)]
struct Store(Mutex<HashMap<String, String>>);

impl SettingsStore for Store {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let value = self.0.lock().unwrap().get(key).cloned();
        Box::pin(async move { Ok(value) })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_owned());
        Box::pin(async { Ok(()) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Whether the server takes `Price > :p` as a search condition, which only
/// the extended grammar accepts.
async fn takes_an_inequality(storage: &Arc<dyn StorageEngine>) -> bool {
    let body = json!({
        "TableName": "tbl",
        "IndexName": "vidx",
        "SearchVector": [{"N": "1"}, {"N": "0"}],
        "TopK": 1,
        "SearchConditionExpression": "Price > :p",
        "ExpressionAttributeValues": {":p": {"N": "5"}},
    });
    extenddb_engine::check_search_vectors_request(
        &body,
        storage,
        "123456789012",
        &LimitsConfig::default(),
    )
    .await
    .is_ok()
}

#[tokio::test]
async fn a_change_through_the_management_api_reaches_the_next_search() {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    );
    let store = Arc::new(Store::default());
    extenddb_engine::install_search_filter_settings(&storage, store.clone());
    let key = extenddb_core::settings_keys::SEARCH_INEQUALITY_FILTERS;

    set_setting(&*store, key, "off").await.unwrap();
    assert!(!takes_an_inequality(&storage).await);
    // Well within the one second a read of the setting is kept: only the
    // change through the management API can make the next search read it.
    set_setting(&*store, key, "all").await.unwrap();
    assert!(takes_an_inequality(&storage).await);
    set_setting(&*store, key, "off").await.unwrap();
    assert!(!takes_an_inequality(&storage).await);
}
