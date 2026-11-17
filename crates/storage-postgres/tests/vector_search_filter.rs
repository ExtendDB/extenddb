// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A vector search with a search condition parsed by the extended grammar,
//! on PostgreSQL.
//!
//! The backend streams the rows in score order and keeps the first `top_k`
//! that satisfy the condition, so `top_k` counts matching items only. The
//! match semantics are the FilterExpression ones; the cases that turn on a
//! missing attribute or a number written two ways are not yet measured
//! against the live service.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database,
//! for a role that may create databases and the pgvector extension. Without
//! it the tests report a skip and pass. The tests use one database pair,
//! `ineq_vf_catalog` and `ineq_vf_data`, created on first use and kept, and a
//! table of their own that they delete when they pass.

#![cfg(feature = "search-inequality-filters")]

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::expression::search_filter::{SearchFilter, parse_search_filter};
use extenddb_core::types::{AttributeValue, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::migrations::Runner;
use extenddb_storage::{
    DataEngine, TableEngine, VectorItemFilter, VectorSearch, VectorSearchEngine,
};
use extenddb_storage_postgres::migrations::{
    PostgresMigrations, PostgresTarget, try_create_vector_extension,
};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const CATALOG: &str = "ineq_vf_catalog";
const DATA: &str = "ineq_vf_data";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Matcher(SearchFilter);

impl VectorItemFilter for Matcher {
    fn matches(&self, item: &Item) -> Result<bool, StorageError> {
        self.0
            .matches(item)
            .map_err(|e| StorageError::Internal(format!("{e:?}")))
    }
}

fn base() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

/// The engine on the database pair, migrated, with a fresh table `name`
/// whose index has HASH `Country` (S) and inline filter `Price` (N).
async fn fixture(base: &str, name: &str) -> Option<(PostgresEngine, TableKeyInfo)> {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the server");
    for db in [CATALOG, DATA] {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(db)
                .fetch_one(&admin)
                .await
                .expect("look up the database");
        if !exists {
            sqlx::query(&format!("CREATE DATABASE \"{db}\""))
                .execute(&admin)
                .await
                .expect("create the database");
        }
    }
    admin.close().await;
    let (catalog_url, data_url) = (format!("{base}/{CATALOG}"), format!("{base}/{DATA}"));
    let line = try_create_vector_extension(&data_url.parse().expect("a URL"), None).await;
    if !line.starts_with("pgvector available") {
        eprintln!("SKIP: {line}");
        return None;
    }
    let target = PostgresTarget::new(catalog_url.parse().expect("a URL"), DATA)
        .with_data_connection_row(&data_url);
    let runner = PostgresMigrations::open_unlocked(&target)
        .await
        .expect("open the migration runner");
    let report = Runner::new(&runner, "test").migrate().await;
    runner.close().await;
    report.expect("migrate");
    let catalog = PgPoolOptions::new()
        .max_connections(1)
        .connect(&catalog_url)
        .await
        .expect("connect to the catalog");
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, '0') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .execute(&catalog)
        .await
        .expect("write a setting");
    }
    sqlx::query(
        "INSERT INTO accounts (account_id, account_name) VALUES ($1, 'ineq') \
         ON CONFLICT DO NOTHING",
    )
    .bind(ACCOUNT)
    .execute(&catalog)
    .await
    .expect("seed the account");
    catalog.close().await;
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url,
            pool_size: 4,
            max_item_size_bytes: 400_000,
        },
        "us-east-1",
    )
    .await
    .expect("open the engine");
    let input = serde_json::from_value(json!({
        "TableName": name,
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
        .create_table(ACCOUNT, input)
        .await
        .expect("create the table");
    let key_info = wait_active(&engine, name).await;
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
            .put_item(
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
    Some((engine, key_info))
}

async fn wait_active(engine: &PostgresEngine, name: &str) -> TableKeyInfo {
    for _ in 0..100 {
        if let Ok(key_info) = engine.table_key_info(ACCOUNT, name).await
            && key_info.vector_indexes.len() == 1
        {
            let desc = engine
                .describe_table(
                    ACCOUNT,
                    extenddb_core::types::DescribeTableInput {
                        table_name: name.to_owned(),
                    },
                )
                .await
                .expect("describe");
            let ready = desc
                .vector_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .all(|vi| vi.index_status.is_active());
            if ready {
                return key_info;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the vector index of {name} did not become ACTIVE in 10 s");
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

async fn search(
    engine: &PostgresEngine,
    key_info: &TableKeyInfo,
    expr: &str,
    top_k: i64,
) -> Vec<String> {
    let used: HashMap<String, AttributeValue> = values()
        .into_iter()
        .filter(|(k, _)| {
            expr.match_indices(k.as_str()).any(|(at, _)| {
                !expr[at + k.len()..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric())
            })
        })
        .collect();
    let filter = parse_search_filter(expr, None, Some(&used), 50).expect("parse");
    let hash = filter
        .validate_against_schema(
            Some(key_info.vector_indexes[0].search_schema.as_slice()),
            &key_info.attribute_definitions,
        )
        .expect("schema")
        .map(|(n, v)| (n.to_owned(), v.clone()));
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
async fn conditions_are_evaluated_before_the_top_k_cut() {
    let Some(base) = base() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let name = format!("vf_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let Some((engine, key_info)) = fixture(&base, &name).await else {
        return;
    };
    // unfiltered, the two nearest would be n1 and n2
    assert_eq!(
        search(&engine, &key_info, "Country = :us AND Price > :p5", 2).await,
        ["n2", "n4"]
    );
    assert_eq!(
        search(&engine, &key_info, "Country = :us AND Price >= :p5", 10).await,
        ["n1", "n2", "n4", "n5"]
    );
    // not yet measured: a missing Price under <>, and 1E2 equal to 100
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
    engine
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: name.clone(),
            },
        )
        .await
        .expect("delete the table");
}
