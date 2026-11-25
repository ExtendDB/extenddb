// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A vector search with a search condition parsed by the extended grammar.
//!
//! The backend applies the condition to each candidate before ranking, so
//! `top_k` counts matching items only. The match semantics are the
//! FilterExpression ones. The two that the cases below turn on, a missing
//! attribute satisfying `<>` and `1E2` equal to `100`, were measured against
//! the live service in eu-north-1 on 2026-11-19, each request twice, with the
//! items of the wire tests.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::expression::search_filter::{SearchFilter, parse_search_filter};
use extenddb_core::types::{AttributeValue, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{VectorItemFilter, VectorSearch, VectorSearchEngine};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

struct Matcher(SearchFilter);

impl VectorItemFilter for Matcher {
    fn matches(&self, item: &Item) -> Result<bool, StorageError> {
        self.0
            .matches(item)
            .map_err(|e| StorageError::Internal(format!("{e:?}")))
    }
}

/// A table whose index has HASH `Country` (S) and inline filter `Price` (N),
/// holding items at increasing distance from the query `[1, 0]`.
async fn fixture() -> (SqliteEngine, TableKeyInfo) {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 4_096_000)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0')
         ON CONFLICT(key) DO UPDATE SET value = '0'",
    )
    .execute(&engine.pool)
    .await
    .expect("delay 0");
    let input = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "Country", "AttributeType": "S"},
            {"AttributeName": "Price", "AttributeType": "N"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {"AttributeName": "emb"},
            "SearchSchema": [
                {"AttributeName": "Country", "SearchSchemaElementType": "HASH"},
                {"AttributeName": "Price", "SearchSchemaElementType": "INLINE_FILTER"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE account_id = ?")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("activate");
    let key_info = engine
        .fetch_table_key_info(ACCOUNT, "t")
        .await
        .expect("key info");
    for (id, country, slope, price) in [
        ("n1", "US", "0", Some("5")),
        ("n2", "US", "0.1", Some("10")),
        ("n3", "US", "0.2", None),
        ("n4", "US", "0.3", Some("1E2")),
        ("n5", "US", "0.4", Some("15")),
        ("c1", "CA", "0.05", Some("50")),
    ] {
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
        item.insert("Country".to_owned(), AttributeValue::S(country.to_owned()));
        item.insert(
            "emb".to_owned(),
            AttributeValue::L(vec![
                AttributeValue::N("1".to_owned()),
                AttributeValue::N(slope.to_owned()),
            ]),
        );
        if let Some(p) = price {
            item.insert("Price".to_owned(), AttributeValue::N(p.to_owned()));
        }
        engine
            .put_item_impl(
                &key_info,
                item,
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put item");
    }
    (engine, key_info)
}

fn values() -> HashMap<String, AttributeValue> {
    [
        (":us", AttributeValue::S("US".to_owned())),
        (":p5", AttributeValue::N("5".to_owned())),
        (":p10", AttributeValue::N("10".to_owned())),
        (":p100", AttributeValue::N("100".to_owned())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

/// The ids `expr` returns with `top_k`, nearest first.
async fn search(
    engine: &SqliteEngine,
    key_info: &TableKeyInfo,
    expr: &str,
    top_k: i64,
) -> Vec<String> {
    let all = values();
    let used: HashMap<String, AttributeValue> = all
        .iter()
        .filter(|(k, _)| {
            expr.match_indices(k.as_str()).any(|(at, _)| {
                !expr[at + k.len()..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric())
            })
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let filter = parse_search_filter(
        expr,
        None,
        Some(&used),
        50,
        &extenddb_core::limits::LimitsConfig::default(),
    )
    .expect("parse");
    let schema = &key_info.vector_indexes[0].search_schema;
    let definitions = &key_info.attribute_definitions;
    let hash = filter
        .validate_against_schema(Some(schema.as_slice()), definitions)
        .expect("schema")
        .map(|(name, value)| (name.to_owned(), value.clone()));
    let item_filter: Arc<dyn VectorItemFilter> = Arc::new(Matcher(filter));
    let out = engine
        .search_vectors(VectorSearch {
            key_info,
            index_name: "vix",
            query_vector: &[1.0, 0.0],
            top_k,
            hash_key: hash.as_ref().map(|(n, v)| (n.as_str(), v)),
            filters: &[],
            item_filter: Some(item_filter),
        })
        .await
        .expect("search");
    out.hits
        .iter()
        .map(|hit| match hit.item.get("id") {
            Some(AttributeValue::S(id)) => id.clone(),
            other => panic!("id: {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn top_k_counts_matching_items_only() {
    let (engine, key_info) = fixture().await;
    // unfiltered, the two nearest would be n1 and n2
    assert_eq!(
        search(&engine, &key_info, "Country = :us AND Price > :p5", 2).await,
        ["n2", "n4"]
    );
}

#[tokio::test]
async fn the_hash_value_scopes_the_search() {
    let (engine, key_info) = fixture().await;
    assert_eq!(
        search(&engine, &key_info, "Country = :us AND Price >= :p5", 10).await,
        ["n1", "n2", "n4", "n5"]
    );
}

/// A missing Price satisfies `<>`, and `1E2` equals `100`.
#[tokio::test]
async fn a_missing_attribute_and_a_number_written_two_ways() {
    let (engine, key_info) = fixture().await;
    assert_eq!(
        search(&engine, &key_info, "Country = :us AND Price <> :p10", 10).await,
        ["n1", "n3", "n4", "n5"]
    );
    assert_eq!(
        search(
            &engine,
            &key_info,
            "Country = :us AND (attribute_not_exists(Price) OR Price = :p100)",
            10
        )
        .await,
        ["n3", "n4"]
    );
    assert_eq!(
        search(
            &engine,
            &key_info,
            "Country = :us AND NOT Price BETWEEN :p5 AND :p10",
            10
        )
        .await,
        ["n3", "n4", "n5"]
    );
}
