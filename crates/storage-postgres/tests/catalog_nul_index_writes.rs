// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Item writes through indexes whose names hold U+0000 or U+0001.
//!
//! The write paths read an index's key schema and projection from the
//! catalog, where they are stored through the escape of `catalog_json`, and
//! a write to an index with a propagation delay stores the index definition
//! in `gsi_pending.index_context` for the queue worker. Each test writes
//! items to a table whose GSI is keyed on a raw U+0001 pair and projects a
//! name that holds U+0001 U+0002: a decoder that read either stored form as
//! the name would find no such attribute on the items, and the index would
//! stay empty. A second GSI is keyed on a U+0000 name that the items do not
//! carry, so every write with a delay stores a context that holds U+0000.
//! An item attribute name that holds U+0000 is item data, which the PostgreSQL
//! item columns cannot hold yet, so no item here carries one.
//!
//! See `catalog_nul_support` for the database these tests need.

mod catalog_nul_support;

use std::collections::BTreeMap;
use std::time::Duration;

use catalog_nul_support::{
    ACCOUNT, Options, Scratch, base_conn, include, key, s_attr, scratch, set, skip, stored_text,
};
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, DescribeTableInput, DistanceFunction, GsiInput,
    Item, KeyType, Projection, ProjectionType, SearchSchemaElement, SearchSchemaElementType,
    TableKeyInfo, UpdateTableInput, VectorAttribute, VectorIndexSpecification,
};
use extenddb_storage::{DataEngine, TableEngine, VectorSearch, VectorSearchEngine};

/// The key of the GSI the items carry: a raw U+0001 pair.
const GK: &str = "g\u{1}\u{1}k";
/// A projected name: U+0001 U+0002, the stored form of U+0001.
const NK: &str = "n\u{1}\u{2}";
/// The key of the GSI no item carries: U+0000.
const ZK: &str = "z\u{0}k";

fn table_input(table: &str, with_gsis: bool) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash)],
        attribute_definitions: if with_gsis {
            vec![s_attr("pk"), s_attr(GK), s_attr(ZK)]
        } else {
            vec![s_attr("pk")]
        },
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: with_gsis.then(|| {
            vec![
                GsiInput {
                    index_name: "by_u1".to_owned(),
                    key_schema: vec![key(GK, KeyType::Hash)],
                    projection: include(&[NK]),
                    provisioned_throughput: None,
                },
                GsiInput {
                    index_name: "by_u0".to_owned(),
                    key_schema: vec![key(ZK, KeyType::Hash)],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                },
            ]
        }),
        ..Default::default()
    }
}

fn item(n: u32) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{n}"))),
        (GK.to_owned(), AttributeValue::S(format!("g{}", n % 2))),
        (NK.to_owned(), AttributeValue::S(format!("projected {n}"))),
        (
            "other".to_owned(),
            AttributeValue::S("not projected".to_owned()),
        ),
    ])
}

async fn put(s: &Scratch, key_info: &TableKeyInfo, it: Item) {
    s.engine
        .put_item(key_info, it, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("PutItem");
}

async fn index_id(s: &Scratch, table: &str, index: &str) -> String {
    sqlx::query_scalar(
        "SELECT i.index_id FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE t.table_name = $1 AND i.index_name = $2",
    )
    .bind(table)
    .bind(index)
    .fetch_one(&s.catalog)
    .await
    .expect("look up the index id")
}

/// The attribute names of every row of an index, sorted, one row per entry.
async fn index_rows(s: &Scratch, index_id: &str) -> Vec<Vec<String>> {
    let rows: Vec<serde_json::Value> =
        sqlx::query_scalar(&format!("SELECT item_data FROM \"_ddb_{index_id}\""))
            .fetch_all(&s.data)
            .await
            .expect("read an index table");
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut names: Vec<String> =
                row.as_object().expect("an item").keys().cloned().collect();
            names.sort();
            names
        })
        .collect();
    out.sort();
    out
}

/// Wait until the queue holds no row for the table.
async fn wait_queue_empty(s: &Scratch, table_id: &str) {
    for _ in 0..500 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM gsi_pending WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(&s.data)
            .await
            .expect("count the queued rows");
        if n == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the propagation queue did not drain");
}

/// What `by_u1` holds for three items: the keys and the projected name.
fn expected_rows() -> Vec<Vec<String>> {
    let mut names = vec![GK.to_owned(), NK.to_owned(), "pk".to_owned()];
    names.sort();
    vec![names; 3]
}

#[tokio::test]
async fn writes_maintain_indexes_keyed_on_these_names_with_and_without_a_delay() {
    let test = "writes_maintain_indexes_keyed_on_these_names_with_and_without_a_delay";
    if base_conn().is_none() {
        return skip(test);
    }
    let options = Options {
        gsi_workers: true,
        ..Options::default()
    };
    let s = scratch(test, options).await.expect("a scratch");
    for delay in ["0", "200"] {
        set(&s.catalog, "index_propagation_delay_ms", delay).await;
        let table = format!("writes_{delay}");
        s.engine
            .create_table(ACCOUNT, table_input(&table, true))
            .await
            .expect("CreateTable");
        let key_info = s
            .engine
            .table_key_info(ACCOUNT, &table)
            .await
            .expect("table_key_info");
        for n in 0..3 {
            put(&s, &key_info, item(n)).await;
        }
        if delay != "0" {
            // The context of each queued row holds the U+0000 name, stored
            // through the escape.
            let contexts: Vec<String> = sqlx::query_scalar(
                "SELECT index_context::text FROM gsi_pending WHERE table_id = $1",
            )
            .bind(&key_info.table_id)
            .fetch_all(&s.data)
            .await
            .expect("read the queued contexts");
            for context in &contexts {
                assert!(!context.contains("\\u0000"), "{context}");
            }
            wait_queue_empty(&s, &key_info.table_id).await;
        }
        let by_u1 = index_id(&s, &table, "by_u1").await;
        assert_eq!(
            index_rows(&s, &by_u1).await,
            expected_rows(),
            "delay {delay}"
        );
        let by_u0 = index_id(&s, &table, "by_u0").await;
        assert!(index_rows(&s, &by_u0).await.is_empty(), "delay {delay}");
    }
}

#[tokio::test]
async fn a_backfill_and_the_recovery_sweep_read_the_names_from_the_catalog() {
    let test = "a_backfill_and_the_recovery_sweep_read_the_names_from_the_catalog";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let table = "backfill";
    s.engine
        .create_table(ACCOUNT, table_input(table, false))
        .await
        .expect("CreateTable");
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, table)
        .await
        .expect("table_key_info");
    for n in 0..3 {
        put(&s, &key_info, item(n)).await;
    }
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": table,
        "AttributeDefinitions": [{"AttributeName": GK, "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "by_u1",
            "KeySchema": [{"AttributeName": GK, "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [NK]}
        }}]
    }))
    .expect("an UpdateTable request");
    s.engine
        .update_table(ACCOUNT, input)
        .await
        .expect("UpdateTable adding the GSI");
    let by_u1 = index_id(&s, table, "by_u1").await;
    assert_eq!(index_rows(&s, &by_u1).await, expected_rows());

    // What a build that died leaves: the index CREATING and its rows gone.
    // The recovery sweep reads the definition from the catalog.
    sqlx::query("UPDATE indexes SET index_status = 'CREATING' WHERE index_id = $1")
        .bind(&by_u1)
        .execute(&s.catalog)
        .await
        .expect("mark the index CREATING");
    sqlx::query(&format!("DELETE FROM \"_ddb_{by_u1}\""))
        .execute(&s.data)
        .await
        .expect("remove the index rows");
    // The build that ran inside UpdateTable held its ownership lock on a
    // connection of its own, released when the server notices the
    // connection closed; until then the sweep skips the index.
    let mut finished = 0;
    for _ in 0..50 {
        finished = extenddb_storage_postgres::reconcile_creating_gsis(&s.engine)
            .await
            .expect("run the recovery sweep");
        if finished > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(finished, 1);
    assert_eq!(index_rows(&s, &by_u1).await, expected_rows());
}

#[tokio::test]
async fn a_vector_index_keeps_its_attribute_names() {
    let test = "a_vector_index_keeps_its_attribute_names";
    if base_conn().is_none() {
        return skip(test);
    }
    let options = Options {
        pgvector: true,
        ..Options::default()
    };
    let Some(s) = scratch(test, options).await else {
        return;
    };
    // The vector attribute and the search schema's HASH attribute.
    let (vector, hash) = ("v\u{1}\u{1}", "t\u{1}\u{2}");
    let input = CreateTableInput {
        table_name: "vectors".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash)],
        attribute_definitions: vec![s_attr("pk"), s_attr(hash)],
        billing_mode: Some(BillingMode::PayPerRequest),
        vector_indexes: Some(vec![VectorIndexSpecification {
            index_name: "vidx".to_owned(),
            dimensions: 2,
            distance_function: DistanceFunction::Cosine,
            vector_attribute: VectorAttribute {
                attribute_name: vector.to_owned(),
            },
            search_schema: Some(vec![SearchSchemaElement {
                attribute_name: hash.to_owned(),
                element_type: SearchSchemaElementType::Hash,
            }]),
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }]),
        ..Default::default()
    };
    s.engine
        .create_table(ACCOUNT, input)
        .await
        .expect("CreateTable with a vector index");
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT vector_attribute::text || ' ' || search_schema::text FROM vector_indexes \
         WHERE index_name = 'vidx'",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("read the vector index row");
    for name in [vector, hash] {
        assert!(
            stored.contains(&format!("\"{}\"", stored_text(name))),
            "{stored}"
        );
    }
    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "vectors".to_owned(),
            },
        )
        .await
        .expect("DescribeTable");
    let vi = &desc.vector_indexes.expect("vector indexes")[0];
    assert_eq!(vi.vector_attribute.attribute_name, vector);
    let schema = vi.search_schema.as_ref().expect("a search schema");
    assert_eq!(schema[0].attribute_name, hash);

    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "vectors")
        .await
        .expect("table_key_info");
    assert_eq!(key_info.vector_indexes[0].vector_attribute_name, vector);
    let tenant = AttributeValue::S("a".to_owned());
    let it: Item = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S("p1".to_owned())),
        (hash.to_owned(), tenant.clone()),
        (
            vector.to_owned(),
            AttributeValue::L(vec![
                AttributeValue::N("1".to_owned()),
                AttributeValue::N("0".to_owned()),
            ]),
        ),
    ]);
    put(&s, &key_info, it).await;
    // The search reads the vector attribute's name from the catalog and puts
    // the stored vector back under it.
    let found = s
        .engine
        .search_vectors(VectorSearch {
            key_info: &key_info,
            index_name: "vidx",
            query_vector: &[1.0, 0.0],
            top_k: 5,
            hash_key: Some((hash, &tenant)),
            filters: &[],
        })
        .await
        .expect("SearchVectors");
    assert_eq!(found.hits.len(), 1);
    assert!(
        found.hits[0].item.contains_key(vector),
        "{:?}",
        found.hits[0].item
    );
}
