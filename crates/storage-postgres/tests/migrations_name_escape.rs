// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Golden tests of migration 6, `attribute_name_escape`, on PostgreSQL: the
//! migration rewrites the raw names a release before it stored, and a
//! reconverge leaves the names written after it as they are. The rewritten
//! rows and the ledger notes are compared with
//! `tests/fixtures/golden/0006_attribute_name_escape.txt`;
//! `EXTENDDB_BLESS_EXPECTED=1` writes the file instead. See
//! `migrations_support` for the database these tests need.

mod migrations_support;

use std::fmt::Write as _;

use extenddb_core::types::{CreateTableInput, DescribeTableInput};
use extenddb_storage::migrations::{CatalogStatus, MigrationId, Runner};
use extenddb_storage::{BackupEngine, TableEngine};
use extenddb_storage_postgres::migrations::PostgresMigrations;
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use migrations_support::{ACCOUNT, LATEST, Scratch, base_conn, golden, notes, skip};
use serde_json::json;
use sqlx::PgPool;

/// Names a release before migration 6 stored raw: a U+0001 pair, which the
/// escape's decoder reads as U+0000, and U+0001 U+0002, which it reads as
/// U+0001.
const RAW_KEY: &str = "a\u{1}\u{1}z";
const RAW_GSI_KEY: &str = "g\u{1}\u{2}k";
const RAW_PROJECTED: &str = "n\u{1}\u{1}";
const RAW_VECTOR: &str = "v\u{1}\u{1}";
const RAW_HASH: &str = "t\u{1}\u{2}";
const RAW_TABLE_ID: &str = "00000000-0000-4000-8000-000000000006";
const RAW_INDEX_ID: &str = "00000000-0000-4000-8000-000000000016";

/// An engine on the scratch catalog.
async fn engine(s: &Scratch) -> PostgresEngine {
    PostgresEngine::new(
        &PostgresConfig {
            connection_string: s.catalog_url(),
            pool_size: 2,
            max_item_size_bytes: 400_000,
        },
        "us-east-1",
    )
    .await
    .expect("open an engine")
}

/// The raw documents of a table, its GSI, a backup, a vector index and a
/// queued index update, written by SQL as a release before migration 6
/// wrote them. A second GSI's name holds the six characters `\u0001`: the
/// rewrite finds its documents and leaves them as they are.
async fn seed_raw_names(catalog: &PgPool, data: &PgPool) {
    let ks = json!([{"AttributeName": RAW_KEY, "KeyType": "HASH"}]);
    let ad = json!([{"AttributeName": RAW_KEY, "AttributeType": "S"},
        {"AttributeName": RAW_GSI_KEY, "AttributeType": "S"}]);
    let gsi_ks = json!([{"AttributeName": RAW_GSI_KEY, "KeyType": "HASH"}]);
    let projection = json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": [RAW_PROJECTED]});
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
         table_status, table_arn, table_id) VALUES ($1, 'raw_tbl', $2, $3, 'ACTIVE', \
         'arn:raw_tbl', $4)",
    )
    .bind(ACCOUNT)
    .bind(&ks)
    .bind(&ad)
    .bind(RAW_TABLE_ID)
    .execute(catalog)
    .await
    .unwrap();
    for (index_id, name, key_schema, projection) in [
        (RAW_INDEX_ID, "raw_gsi", gsi_ks.clone(), projection.clone()),
        (
            "00000000-0000-4000-8000-000000000017",
            "text_gsi",
            json!([{"AttributeName": "lit\\u0001", "KeyType": "HASH"}]),
            json!({"ProjectionType": "KEYS_ONLY"}),
        ),
    ] {
        sqlx::query(
            "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, \
             projection) VALUES ($1, $2, $3, 'GSI', $4, $5)",
        )
        .bind(RAW_TABLE_ID)
        .bind(index_id)
        .bind(name)
        .bind(&key_schema)
        .bind(&projection)
        .execute(catalog)
        .await
        .unwrap();
    }
    let vector_attribute = json!({"AttributeName": RAW_VECTOR});
    let search_schema = json!([{"AttributeName": RAW_HASH, "SearchSchemaElementType": "HASH"}]);
    let all = json!({"ProjectionType": "ALL"});
    sqlx::query(
        "INSERT INTO vector_indexes (table_id, index_name, index_id, dimensions, \
         distance_function, vector_attribute, search_schema, projection, index_status, \
         backfilling) VALUES ($1, 'raw_vidx', '00000000-0000-4000-8000-000000000026', 2, \
         'COSINE', $2, $3, $4, 'ACTIVE', NULL)",
    )
    .bind(RAW_TABLE_ID)
    .bind(&vector_attribute)
    .bind(&search_schema)
    .bind(&all)
    .execute(catalog)
    .await
    .unwrap();
    let snapshot = json!({"Version": 1, "VectorIndexes": [{"IndexName": "raw_vidx",
        "Dimensions": 2, "DistanceFunction": "COSINE", "VectorAttribute": vector_attribute,
        "SearchSchema": search_schema, "Projection": all}]});
    sqlx::query(
        "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
         key_schema, attribute_definitions, vector_indexes) \
         VALUES ('arn:raw_backup', 'raw_backup', $1, 'raw_tbl', $2, $3, $4, $5)",
    )
    .bind(RAW_TABLE_ID)
    .bind(ACCOUNT)
    .bind(&ks)
    .bind(&ad)
    .bind(&snapshot)
    .execute(catalog)
    .await
    .unwrap();
    let definition = json!({"Version": 1, "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{"IndexName": "raw_gsi", "KeySchema": gsi_ks,
            "Projection": projection}],
        "LocalSecondaryIndexes": []});
    sqlx::query(
        "INSERT INTO backup_definitions (backup_arn, definition) VALUES ('arn:raw_backup', $1)",
    )
    .bind(&definition)
    .execute(catalog)
    .await
    .unwrap();
    // The item image is item data, which the migration leaves raw.
    let context = json!({"base_key_schema": ks, "attribute_definitions": ad,
        "index": {"index_id": RAW_INDEX_ID, "key_schema": gsi_ks, "projection": projection}});
    let new_item = json!({RAW_KEY: {"S": "x"}, RAW_GSI_KEY: {"S": "y"}});
    sqlx::query(
        "INSERT INTO gsi_pending (table_id, worker_partition, old_item, new_item, \
         index_context) VALUES ($1, 0, NULL, $2, $3)",
    )
    .bind(RAW_TABLE_ID)
    .bind(&new_item)
    .bind(&context)
    .execute(data)
    .await
    .unwrap();
}

/// Each row's documents as `jsonb::text`, under a label, sorted by byte
/// order.
async fn dump_documents(pool: &PgPool, sql: &str, out: &mut String) {
    let rows: Vec<(String, Vec<String>)> = sqlx::query_as(sql).fetch_all(pool).await.unwrap();
    for (label, columns) in rows {
        writeln!(out, "{label}").unwrap();
        for column in columns {
            writeln!(out, "  {column}").unwrap();
        }
    }
}

#[tokio::test]
async fn migration_6_rewrites_the_raw_names_of_every_document_that_carries_them() {
    let Some(base) = base_conn() else {
        return skip("golden 6");
    };
    let s = Scratch::new(&base, "pgm_g6").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::DropContinuousBackups).await;
    let (catalog, data) = (s.catalog().await, s.data().await);
    seed_raw_names(&catalog, &data).await;
    s.migrate_through(MigrationId::AttributeNameEscape).await;

    let mut out = String::new();
    for sql in [
        "SELECT 'tables ' || table_name, ARRAY['key_schema: ' || key_schema::text, \
         'attribute_definitions: ' || attribute_definitions::text] FROM tables \
         ORDER BY table_name COLLATE \"C\"",
        "SELECT 'indexes ' || index_name, ARRAY['key_schema: ' || key_schema::text, \
         'projection: ' || projection::text] FROM indexes ORDER BY index_name COLLATE \"C\"",
        "SELECT 'backups ' || backup_name, ARRAY['key_schema: ' || key_schema::text, \
         'attribute_definitions: ' || attribute_definitions::text, \
         'vector_indexes: ' || coalesce(vector_indexes::text, 'NULL')] FROM backups \
         ORDER BY backup_name COLLATE \"C\"",
        "SELECT 'backup_definitions ' || b.backup_name, ARRAY['definition: ' || \
         d.definition::text] FROM backup_definitions d JOIN backups b \
         ON b.backup_arn = d.backup_arn ORDER BY b.backup_name COLLATE \"C\"",
        "SELECT 'vector_indexes ' || index_name, ARRAY['vector_attribute: ' || \
         vector_attribute::text, 'search_schema: ' || coalesce(search_schema::text, 'NULL'), \
         'projection: ' || projection::text] FROM vector_indexes \
         ORDER BY index_name COLLATE \"C\"",
    ] {
        dump_documents(&catalog, sql, &mut out).await;
    }
    dump_documents(
        &data,
        "SELECT 'gsi_pending ' || table_id, ARRAY['index_context: ' || index_context::text, \
         'new_item: ' || coalesce(new_item::text, 'NULL')] FROM gsi_pending \
         ORDER BY table_id COLLATE \"C\", id",
        &mut out,
    )
    .await;
    writeln!(out, "catalog notes:\n{}", notes(&catalog, 6).await).unwrap();
    writeln!(out, "data notes:\n{}", notes(&data, 6).await).unwrap();
    golden("0006_attribute_name_escape.txt", &out);

    // This binary reads the rewritten names as the release wrote them.
    let engine = engine(&s).await;
    let desc = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "raw_tbl".to_owned(),
            },
        )
        .await
        .expect("DescribeTable");
    assert_eq!(desc.key_schema[0].attribute_name, RAW_KEY);
    let gsi = desc
        .global_secondary_indexes
        .iter()
        .flatten()
        .find(|g| g.index_name == "raw_gsi")
        .expect("raw_gsi");
    assert_eq!(gsi.key_schema[0].attribute_name, RAW_GSI_KEY);
    assert_eq!(
        gsi.projection.non_key_attributes.as_deref(),
        Some(&[RAW_PROJECTED.to_owned()][..])
    );
    let text = desc
        .global_secondary_indexes
        .iter()
        .flatten()
        .find(|g| g.index_name == "text_gsi")
        .expect("text_gsi");
    assert_eq!(text.key_schema[0].attribute_name, "lit\\u0001");
    let vector = &desc.vector_indexes.expect("vector indexes")[0];
    assert_eq!(vector.vector_attribute.attribute_name, RAW_VECTOR);
    let schema = vector.search_schema.as_deref().expect("a search schema");
    assert_eq!(schema[0].attribute_name, RAW_HASH);
    let source = engine
        .backup_restore_source(ACCOUNT, "arn:raw_backup")
        .await
        .expect("the restore source");
    assert_eq!(source.key_schema[0].attribute_name, RAW_KEY);
    let definition = source.definition.expect("the definition");
    assert_eq!(
        definition.global_secondary_indexes[0].key_schema[0].attribute_name,
        RAW_GSI_KEY
    );
    drop(engine);
    catalog.close().await;
    data.close().await;
}

/// Migration 6 is not convergent: a reconverge leaves the documents this
/// release wrote after it, through the escape, as they are, and the names
/// still read back as written.
#[tokio::test]
async fn a_reconverge_leaves_the_names_written_after_migration_6_as_they_are() {
    let Some(base) = base_conn() else {
        return skip("reconverge after migration 6");
    };
    let s = Scratch::new(&base, "pgm_g6rc").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = s.catalog().await;
    let engine = engine(&s).await;
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": "escaped_tbl",
        "KeySchema": [{"AttributeName": "a\u{0}z", "KeyType": "HASH"},
            {"AttributeName": RAW_KEY, "KeyType": "RANGE"}],
        "AttributeDefinitions": [{"AttributeName": "a\u{0}z", "AttributeType": "S"},
            {"AttributeName": RAW_KEY, "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    engine
        .create_table(ACCOUNT, input)
        .await
        .expect("CreateTable");
    let stored = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT key_schema::text || ' ' || attribute_definitions::text FROM tables \
             WHERE table_name = 'escaped_tbl'",
        )
        .fetch_one(&catalog)
        .await
        .unwrap()
    };
    let before = stored().await;
    assert!(before.contains("\"a\\u0001\\u0001z\""), "{before}");

    sqlx::query("INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'applied')")
        .execute(&catalog)
        .await
        .unwrap();
    assert_eq!(s.status().await.label(), "WrittenByOlderRelease");
    let m = PostgresMigrations::open_unlocked(&s.target())
        .await
        .unwrap();
    let report = Runner::new(&m, "test").reconverge().await;
    m.close().await;
    let report = report.expect("reconverge");
    assert!(report.applied.iter().all(|step| step.id != 6), "{report:?}");
    assert!(
        report
            .notes
            .iter()
            .any(|n| n.starts_with("Skipped 6 attribute_name_escape:")),
        "{report:?}"
    );
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    assert_eq!(stored().await, before);
    let desc = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "escaped_tbl".to_owned(),
            },
        )
        .await
        .expect("DescribeTable");
    let names: Vec<&str> = desc
        .key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();
    assert_eq!(names, ["a\u{0}z", RAW_KEY]);
    drop(engine);
    catalog.close().await;
}
