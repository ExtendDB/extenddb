// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A PostgreSQL engine on a database pair of its own, with one table whose
//! vector index has HASH `Country` (S) and inline filter `Price` (N), for the
//! vector search condition tests.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database,
//! for a role that may create databases and the pgvector extension. Without
//! it a test reports a skip and passes. [`run`] creates the pair, runs the
//! test body and drops both databases afterwards, also when the body or the
//! setup panics.

#![allow(dead_code)]

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
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
use futures::FutureExt;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

pub const ACCOUNT: &str = "123456789012";
pub const TABLE: &str = "vf";

/// What a test body receives.
pub struct Fixture {
    pub engine: PostgresEngine,
    pub key_info: TableKeyInfo,
    /// The data database's URL, for statements a test runs beside the engine.
    pub data_url: String,
}

/// One item: id, Country, the two vector components, and Price if any.
pub type Row<'a> = (&'a str, &'a str, f64, f64, Option<&'a str>);

pub struct Matcher(pub SearchFilter);

impl VectorItemFilter for Matcher {
    fn matches(&self, item: &Item) -> Result<bool, StorageError> {
        self.0
            .matches(item)
            .map_err(|e| StorageError::Internal(format!("{e:?}")))
    }
}

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

/// Create the database pair, load `rows` into a table whose index uses
/// `distance`, run `body`, then drop the pair whatever `body` did.
pub async fn run<B>(distance: &str, rows: &[Row<'_>], body: B)
where
    B: AsyncFnOnce(&Fixture),
{
    let Some(base) = base() else {
        return;
    };
    let id = &uuid::Uuid::new_v4().simple().to_string()[..12];
    let (catalog, data) = (format!("vf_{id}_catalog"), format!("vf_{id}_data"));
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the server");
    let outcome = AssertUnwindSafe(async {
        for db in [&catalog, &data] {
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE \"{db}\"")))
                .execute(&admin)
                .await
                .expect("create the database");
        }
        if let Some(fixture) = setup(&base, &catalog, &data, distance, rows).await {
            body(&fixture).await;
        }
    })
    .catch_unwind()
    .await;
    for db in [&catalog, &data] {
        // FORCE ends the engine's pooled connections, which a panic leaves
        // open.
        let dropped = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"
        )))
        .execute(&admin)
        .await;
        if let Err(e) = dropped {
            eprintln!("could not drop {db}: {e}");
        }
    }
    admin.close().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn setup(
    base: &str,
    catalog: &str,
    data: &str,
    distance: &str,
    rows: &[Row<'_>],
) -> Option<Fixture> {
    let (catalog_url, data_url) = (format!("{base}/{catalog}"), format!("{base}/{data}"));
    let line = try_create_vector_extension(&data_url.parse().expect("a URL"), None).await;
    if !line.starts_with("pgvector available") {
        eprintln!("SKIP: {line}");
        return None;
    }
    let target = PostgresTarget::new(catalog_url.parse().expect("a URL"), data)
        .with_data_connection_row(&data_url);
    let runner = PostgresMigrations::open_unlocked(&target)
        .await
        .expect("open the migration runner");
    let report = Runner::new(&runner, "test").migrate().await;
    runner.close().await;
    report.expect("migrate");
    let pool = PgPoolOptions::new()
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
        .execute(&pool)
        .await
        .expect("write a setting");
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'ineq')")
        .bind(ACCOUNT)
        .execute(&pool)
        .await
        .expect("seed the account");
    pool.close().await;
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
        "TableName": TABLE,
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
            "DistanceFunction": distance,
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
    let key_info = wait_active(&engine).await;
    for &(id, country, x, y, price) in rows {
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
        item.insert("Country".to_owned(), AttributeValue::S(country.to_owned()));
        item.insert(
            "emb".to_owned(),
            AttributeValue::L(vec![
                AttributeValue::N(x.to_string()),
                AttributeValue::N(y.to_string()),
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
    Some(Fixture {
        engine,
        key_info,
        data_url,
    })
}

async fn wait_active(engine: &PostgresEngine) -> TableKeyInfo {
    for _ in 0..100 {
        if let Ok(key_info) = engine.table_key_info(ACCOUNT, TABLE).await
            && key_info.vector_indexes.len() == 1
        {
            let desc = engine
                .describe_table(
                    ACCOUNT,
                    extenddb_core::types::DescribeTableInput {
                        table_name: TABLE.to_owned(),
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
    panic!("the vector index of {TABLE} did not become ACTIVE in 10 s");
}

/// Parse `expr` with the values among `values` that it names, and check it
/// against the index's search schema. Returns the filter and the HASH pair.
pub fn parse(
    fixture: &Fixture,
    expr: &str,
    values: &HashMap<String, AttributeValue>,
) -> (SearchFilter, Option<(String, AttributeValue)>) {
    let used: HashMap<String, AttributeValue> = values
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
    let hash = filter
        .validate_against_schema(
            Some(fixture.key_info.vector_indexes[0].search_schema.as_slice()),
            &fixture.key_info.attribute_definitions,
        )
        .expect("schema")
        .map(|(n, v)| (n.to_owned(), v.clone()));
    (filter, hash)
}

/// Search with `item_filter`, or without a condition when it is `None`, and
/// return the hits' ids and scores in order.
pub async fn search_with(
    fixture: &Fixture,
    hash: Option<(&str, &AttributeValue)>,
    item_filter: Option<Arc<dyn VectorItemFilter>>,
    query: &[f32],
    top_k: i64,
) -> Vec<(String, f64)> {
    let out = fixture
        .engine
        .search_vectors(VectorSearch {
            key_info: &fixture.key_info,
            index_name: "vix",
            query_vector: query,
            top_k,
            hash_key: hash,
            filters: &[],
            item_filter,
        })
        .await
        .expect("search");
    out.hits
        .iter()
        .map(|hit| match hit.item.get("id") {
            Some(AttributeValue::S(id)) => (id.clone(), hit.score),
            other => panic!("id: {other:?}"),
        })
        .collect()
}

/// Search with the condition `expr`, and return the hits' ids and scores.
pub async fn search(
    fixture: &Fixture,
    expr: &str,
    values: &HashMap<String, AttributeValue>,
    query: &[f32],
    top_k: i64,
) -> Vec<(String, f64)> {
    let (filter, hash) = parse(fixture, expr, values);
    let item_filter: Arc<dyn VectorItemFilter> = Arc::new(Matcher(filter));
    search_with(
        fixture,
        hash.as_ref().map(|(n, v)| (n.as_str(), v)),
        Some(item_filter),
        query,
        top_k,
    )
    .await
}

/// The ids of `hits`.
pub fn ids(hits: &[(String, f64)]) -> Vec<&str> {
    hits.iter().map(|(id, _)| id.as_str()).collect()
}

/// Named values: each `(name, number)` becomes `:name` bound to that number,
/// each `(name, string)` with a leading `S:` to that string.
pub fn values(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|&(name, value)| {
            let value = match value.strip_prefix("S:") {
                Some(s) => AttributeValue::S(s.to_owned()),
                None => AttributeValue::N(value.to_owned()),
            };
            (format!(":{name}"), value)
        })
        .collect()
}
