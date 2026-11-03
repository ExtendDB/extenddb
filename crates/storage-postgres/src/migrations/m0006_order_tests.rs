// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live checks of migration 6's ordered key rewrite, with the scratch database
//! and the skip convention of `m0006_tests`. The tables are created by the DDL
//! `CreateTable` runs, so their primary keys are the production ones. Each
//! test stores legacy keys that differ only by a U+0002 after a U+0001, the
//! shorter one first in the heap, the order a one-statement rewrite fails on.
//! `item_data` is `{}`: the tests check the key columns.

//! Live checks with the scratch database and the skip convention of
//! `migrations::live_reencode`. The tables are created by the DDL
//! `CreateTable` runs, so their primary keys are the production ones. Each
//! test stores legacy keys that differ only by a U+0002 after a U+0001,
//! the shorter one first in the heap, the order a one-statement rewrite
//! fails on. `item_data` is `{}`: the tests check the key columns.

use extenddb_core::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use extenddb_storage::util::escape_control;
use sqlx::PgPool;

use super::m0006_escape_control_chars::{BATCH, rewrite_longest_first};
use super::m0006_tests::{base_conn, run_pool, scratch, skip};
use crate::PostgresEngine;
use crate::data::key_text::reescape_legacy_composite;

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn string_attributes(key_schema: &[KeySchemaElement]) -> Vec<AttributeDefinition> {
    key_schema
        .iter()
        .map(|k| AttributeDefinition {
            attribute_name: k.attribute_name.clone(),
            attribute_type: ScalarAttributeType::S,
        })
        .collect()
}

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("json")
}

/// The account, the `tables` row and the data table of table `id`.
async fn table(db: &PgPool, id: &str, key_schema: &[KeySchemaElement]) {
    let attributes = string_attributes(key_schema);
    sqlx::query(
        "INSERT INTO accounts (account_id, account_name) VALUES ('111122223333', 'order') \
         ON CONFLICT DO NOTHING",
    )
    .execute(db)
    .await
    .expect("seed the account");
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
         table_status, table_arn, table_id) VALUES ('111122223333', $1, $2, $3, 'ACTIVE', \
         'arn:aws:dynamodb:us-east-1:111122223333:table/' || $1, $1)",
    )
    .bind(id)
    .bind(json(&key_schema))
    .bind(json(&attributes))
    .execute(db)
    .await
    .expect("seed the tables row");
    let mut tx = db.begin().await.expect("begin");
    PostgresEngine::create_data_table(&mut tx, id, key_schema, &attributes)
        .await
        .expect("create the data table");
    tx.commit().await.expect("commit");
}

/// The `indexes` row and the data table of GSI `id` on table `table_id`.
async fn index(
    db: &PgPool,
    table_id: &str,
    base_key_schema: &[KeySchemaElement],
    id: &str,
    key_schema: &[KeySchemaElement],
) {
    sqlx::query(
        "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, \
         projection) VALUES ($1, $2, $2, 'GSI', $3, '{\"ProjectionType\": \"ALL\"}')",
    )
    .bind(table_id)
    .bind(id)
    .bind(json(&key_schema))
    .execute(db)
    .await
    .expect("seed the indexes row");
    let mut tx = db.begin().await.expect("begin");
    PostgresEngine::create_index_data_table(
        &mut tx,
        id,
        key_schema,
        &string_attributes(key_schema),
        base_key_schema,
        &string_attributes(base_key_schema),
    )
    .await
    .expect("create the index data table");
    tx.commit().await.expect("commit");
}

/// Insert rows into `_ddb_<id>` one at a time, in the order given, which
/// is their heap order in a new table.
async fn insert(db: &PgPool, id: &str, columns: &[&str], rows: &[Vec<String>]) {
    let names: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    let params: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO \"_ddb_{id}\" ({}, item_data) VALUES ({}, '{{}}')",
        names.join(", "),
        params.join(", ")
    );
    for row in rows {
        let mut q = sqlx::query(&sql);
        for value in row {
            q = q.bind(value);
        }
        q.execute(db).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_owned()).collect()
}

/// Every value of `col` in `_ddb_<id>`, sorted.
async fn column(db: &PgPool, id: &str, col: &str) -> Vec<String> {
    let sql = format!("SELECT \"{col}\" FROM \"_ddb_{id}\"");
    let mut values: Vec<String> = sqlx::query_scalar(&sql)
        .fetch_all(db)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    values.sort();
    values
}

/// The escape of each legacy text, sorted: what 004 must leave.
fn escaped<S: AsRef<str>>(legacy: &[S]) -> Vec<String> {
    let mut values: Vec<String> = legacy
        .iter()
        .map(|t| escape_control(t.as_ref()).into_owned())
        .collect();
    values.sort();
    values
}

fn rebuilt<S: AsRef<str>>(legacy: &[S]) -> Vec<String> {
    let mut values: Vec<String> = legacy
        .iter()
        .map(|t| reescape_legacy_composite(t.as_ref()).expect("a netstring"))
        .collect();
    values.sort();
    values
}

async fn migrate(db: &PgPool) {
    run_pool(db).await.expect("004 completes");
}

#[tokio::test]
async fn rewrites_a_partition_key_pair_longest_first() {
    if base_conn().is_none() {
        skip("rewrites_a_partition_key_pair_longest_first");
        return;
    }
    let s = scratch().await;
    table(
        &s.db,
        "tp",
        &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
    )
    .await;
    let legacy = ["a\u{1}", "a\u{1}\u{2}"];
    let rows: Vec<Vec<String>> = legacy.iter().map(|pk| row(&[pk, "s"])).collect();
    insert(&s.db, "tp", &["pk", "sk_s"], &rows).await;
    migrate(&s.db).await;
    assert_eq!(column(&s.db, "tp", "pk").await, escaped(&legacy));
    s.cleanup().await;
}

#[tokio::test]
async fn rewrites_a_sort_key_pair_longest_first() {
    if base_conn().is_none() {
        skip("rewrites_a_sort_key_pair_longest_first");
        return;
    }
    let s = scratch().await;
    table(
        &s.db,
        "ts",
        &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
    )
    .await;
    let legacy = ["s\u{1}", "s\u{1}\u{2}"];
    let rows: Vec<Vec<String>> = legacy.iter().map(|sk| row(&["p", sk])).collect();
    insert(&s.db, "ts", &["pk", "sk_s"], &rows).await;
    migrate(&s.db).await;
    assert_eq!(column(&s.db, "ts", "sk_s").await, escaped(&legacy));
    assert_eq!(column(&s.db, "ts", "pk").await, vec!["p", "p"]);
    s.cleanup().await;
}

#[tokio::test]
async fn rebuilds_a_composite_partition_key_pair_longest_first() {
    if base_conn().is_none() {
        skip("rebuilds_a_composite_partition_key_pair_longest_first");
        return;
    }
    let s = scratch().await;
    let ks = [
        key("h1", KeyType::Hash),
        key("h2", KeyType::Hash),
        key("sk", KeyType::Range),
    ];
    table(&s.db, "tc", &ks).await;
    // h1 = "a\x01" and h1 = "a\x01\x02", h2 = "b": the first one's rebuilt
    // text is the second one's legacy text.
    let legacy = ["2:a\u{1},1:b,", "3:a\u{1}\u{2},1:b,"];
    let rows: Vec<Vec<String>> = legacy.iter().map(|pk| row(&[pk, "s"])).collect();
    insert(&s.db, "tc", &["pk", "sk_s"], &rows).await;
    migrate(&s.db).await;
    let expected = rebuilt(&legacy);
    assert_eq!(
        expected,
        vec!["3:a\u{1}\u{2},1:b,", "4:a\u{1}\u{2}\u{2},1:b,"]
    );
    assert_eq!(column(&s.db, "tc", "pk").await, expected);
    s.cleanup().await;
}

#[tokio::test]
async fn rewrites_an_index_base_key_pair_longest_first() {
    if base_conn().is_none() {
        skip("rewrites_an_index_base_key_pair_longest_first");
        return;
    }
    let s = scratch().await;
    let base = [key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    table(&s.db, "tb", &base).await;
    index(&s.db, "tb", &base, "ib", &[key("g", KeyType::Hash)]).await;
    // Two base items with the same index key and base sort key. The base
    // table's own pair is the partition key case above; it is left empty
    // here so the index table is the one that is checked.
    let legacy = ["p\u{1}", "p\u{1}\u{2}"];
    let rows: Vec<Vec<String>> = legacy.iter().map(|p| row(&["g", p, "s"])).collect();
    insert(&s.db, "ib", &["pk", "base_pk", "base_sk_s"], &rows).await;
    migrate(&s.db).await;
    assert_eq!(column(&s.db, "ib", "base_pk").await, escaped(&legacy));
    s.cleanup().await;
}

/// More rows than one batch on both paths: a single-attribute column whose
/// one-statement rewrite fails, and a composite partition key column. The
/// pair of each table is three U+0001 long, so the rows between its two
/// lengths put the longer one in the first batch and the shorter one in the
/// second. Every final text is the escape of its legacy text; a row
/// rewritten twice would hold a longer one.
#[tokio::test]
async fn rewrites_each_row_once_across_batches() {
    if base_conn().is_none() {
        skip("rewrites_each_row_once_across_batches");
        return;
    }
    let fillers = 600;
    assert!(fillers > BATCH);
    let s = scratch().await;

    table(
        &s.db,
        "tf",
        &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
    )
    .await;
    let short = "a\u{1}\u{1}\u{1}";
    let long = "a\u{1}\u{2}\u{1}\u{2}\u{1}\u{2}";
    assert_eq!(escape_control(short), long);
    let mut legacy = vec![short.to_owned()];
    legacy.extend((0..fillers).map(|i| format!("k\u{1}{i:04}")));
    legacy.push(long.to_owned());
    let rows: Vec<Vec<String>> = legacy.iter().map(|pk| row(&[pk, "s"])).collect();
    insert(&s.db, "tf", &["pk", "sk_s"], &rows).await;

    let ks = [
        key("h1", KeyType::Hash),
        key("h2", KeyType::Hash),
        key("sk", KeyType::Range),
    ];
    table(&s.db, "tk", &ks).await;
    let short = "4:a\u{1}\u{1}\u{1},1:b,";
    let long = "7:a\u{1}\u{2}\u{1}\u{2}\u{1}\u{2},1:b,";
    assert_eq!(reescape_legacy_composite(short).as_deref(), Some(long));
    let mut composite = vec![short.to_owned()];
    composite.extend((0..fillers).map(|i| format!("5:k\u{1}{i:03},1:b,")));
    composite.push(long.to_owned());
    let rows: Vec<Vec<String>> = composite.iter().map(|pk| row(&[pk, "s"])).collect();
    insert(&s.db, "tk", &["pk", "sk_s"], &rows).await;

    migrate(&s.db).await;
    assert_eq!(column(&s.db, "tf", "pk").await, escaped(&legacy));
    assert_eq!(column(&s.db, "tk", "pk").await, rebuilt(&composite));
    s.cleanup().await;
}

/// A pair whose byte order is the reverse of its length order: the escape
/// of `a\x01b` is `a\x01\x02b`, which sorts below it, so an order by the
/// text itself, highest first, would rewrite the shorter key first.
#[tokio::test]
async fn rewrites_a_pair_whose_byte_order_is_shortest_first() {
    if base_conn().is_none() {
        skip("rewrites_a_pair_whose_byte_order_is_shortest_first");
        return;
    }
    let s = scratch().await;
    table(
        &s.db,
        "tv",
        &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
    )
    .await;
    let legacy = ["a\u{1}b", "a\u{1}\u{2}b"];
    let rows: Vec<Vec<String>> = legacy.iter().map(|pk| row(&[pk, "s"])).collect();
    insert(&s.db, "tv", &["pk", "sk_s"], &rows).await;
    migrate(&s.db).await;
    assert_eq!(column(&s.db, "tv", "pk").await, escaped(&legacy));
    s.cleanup().await;
}

/// An error from a rewrite names the DynamoDB table of the physical table:
/// here a two-HASH table whose `pk` is not a netstring.
#[tokio::test]
async fn an_error_names_the_dynamodb_table() {
    if base_conn().is_none() {
        skip("an_error_names_the_dynamodb_table");
        return;
    }
    let s = scratch().await;
    let ks = [
        key("h1", KeyType::Hash),
        key("h2", KeyType::Hash),
        key("sk", KeyType::Range),
    ];
    table(&s.db, "tn", &ks).await;
    insert(&s.db, "tn", &["pk", "sk_s"], &[row(&["x\u{1}", "s"])]).await;
    let err = run_pool(&s.db)
        .await
        .expect_err("a composite key that is not a netstring");
    let text = err.to_string();
    assert!(
        text.contains("_ddb_tn.pk (table \"tn\" of account 111122223333)"),
        "{text}"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn a_duplicate_that_remains_is_reported_with_its_key() {
    if base_conn().is_none() {
        skip("a_duplicate_that_remains_is_reported_with_its_key");
        return;
    }
    let s = scratch().await;
    sqlx::raw_sql(
        r#"CREATE TABLE "dup" (pk TEXT COLLATE "C" PRIMARY KEY);
        INSERT INTO "dup" VALUES (E'x\x01'), (E'y\x01');"#,
    )
    .execute(&s.db)
    .await
    .expect("seed");
    let mut tx = s.db.begin().await.expect("begin");
    // A rewrite that gives every row one text: the second update fails.
    let err = rewrite_longest_first(&mut tx, "\"dup\"", "pk", "TRUE", "dup.pk (t)", |_| {
        Ok("z\u{1}".to_owned())
    })
    .await
    .expect_err("a duplicate");
    let text = err.to_string();
    assert!(text.starts_with("re-encode dup.pk (t): "), "{text}");
    assert!(
        text.contains("(DETAIL: \"Key (pk)=(z\\u{1}) already exists.\")"),
        "{text}"
    );
    tx.rollback().await.expect("rollback");
    s.cleanup().await;
}
