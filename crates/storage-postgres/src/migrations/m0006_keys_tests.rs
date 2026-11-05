// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live checks of migration 6's composite key rebuild and `worker_partition`
//! recompute, with the scratch database and the skip convention of
//! `m0006_tests`: without `EXTENDDB_TEST_PG_CONNECTION_STRING` every live test
//! reports a skip and passes. Each seeds rows the way a release without the
//! escape wrote them.

//! Live checks against a scratch database, with the helpers and the
//! skip convention of `migrations::live_reencode`: without
//! `EXTENDDB_TEST_PG_CONNECTION_STRING` every test reports a skip and
//! passes. Each seeds rows the way a build without the escape wrote them.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use serde_json::json;
use sqlx::PgPool;

use super::m0006_tests::{base_conn, run_pool, scratch, skip};
use crate::data::key_text::composite_pk_to_text;
use crate::gsi_queue::{
    GsiApplyContext, GsiIndexDef, PendingApplyContext, enqueue_gsi_pending, partition_for,
    worker_partition,
};

fn element(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn text(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

async fn exec(db: &PgPool, sql: &str, binds: &[serde_json::Value]) {
    let mut q = sqlx::query(sql);
    for b in binds {
        q = q.bind(b.clone());
    }
    q.execute(db).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn texts(db: &PgPool, sql: &str) -> Vec<String> {
    sqlx::query_scalar(sql)
        .fetch_all(db)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// A table with two HASH attributes (`t2`), a two-HASH GSI on a one-HASH
/// table (`i2` on `t1`), a one-HASH GSI on `t2` (`i3`), and a backup of
/// each table, each holding a legacy row with a raw U+0001 in a HASH part.
async fn seed_composite(db: &PgPool) {
    exec(
        db,
        "INSERT INTO accounts (account_id, account_name) VALUES ('111122223333', 'esc')",
        &[],
    )
    .await;
    let one = json!([{"AttributeName": "pk", "KeyType": "HASH"},
                     {"AttributeName": "sk", "KeyType": "RANGE"}]);
    let two = json!([{"AttributeName": "h1", "KeyType": "HASH"},
                     {"AttributeName": "h2", "KeyType": "HASH"},
                     {"AttributeName": "sk", "KeyType": "RANGE"}]);
    for (id, ks) in [("t1", &one), ("t2", &two)] {
        exec(
            db,
            &format!(
                "INSERT INTO tables (account_id, table_name, key_schema, \
                 attribute_definitions, table_status, table_arn, table_id) \
                 VALUES ('111122223333', '{id}', $1, '[]', 'ACTIVE', \
                 'arn:aws:dynamodb:us-east-1:111122223333:table/{id}', '{id}')"
            ),
            std::slice::from_ref(ks),
        )
        .await;
    }
    for (table, index, ks) in [
        (
            "t1",
            "i2",
            json!([{"AttributeName": "g1", "KeyType": "HASH"},
                   {"AttributeName": "g2", "KeyType": "HASH"}]),
        ),
        (
            "t2",
            "i3",
            json!([{"AttributeName": "g", "KeyType": "HASH"}]),
        ),
    ] {
        exec(
            db,
            &format!(
                "INSERT INTO indexes (table_id, index_id, index_name, index_type, \
                 key_schema, projection) VALUES ('{table}', '{index}', '{index}', 'GSI', \
                 $1, '{{\"ProjectionType\": \"ALL\"}}')"
            ),
            &[ks],
        )
        .await;
    }
    sqlx::raw_sql(
        r#"CREATE TABLE "_ddb_t2" (
            pk TEXT COLLATE "C" NOT NULL, sk_s TEXT COLLATE "C",
            item_data JSONB NOT NULL, PRIMARY KEY (pk, sk_s));
        CREATE TABLE "_ddb_i2" (
            pk TEXT COLLATE "C" NOT NULL, sk_s TEXT COLLATE "C",
            base_pk TEXT COLLATE "C" NOT NULL, base_sk_s TEXT COLLATE "C",
            item_data JSONB NOT NULL, PRIMARY KEY (pk, base_pk, base_sk_s));
        CREATE TABLE "_ddb_i3" (
            pk TEXT COLLATE "C" NOT NULL, sk_s TEXT COLLATE "C",
            base_pk TEXT COLLATE "C" NOT NULL, base_sk_s TEXT COLLATE "C",
            item_data JSONB NOT NULL, PRIMARY KEY (pk, base_pk, base_sk_s));
        INSERT INTO "_ddb_t2" VALUES
            (E'2:a\x01,1:b,', 's',
             '{"h1": {"S": "a\u0001"}, "h2": {"S": "b"}, "sk": {"S": "s"}}'),
            ('1:x,1:y,', 's', '{"h1": {"S": "x"}, "h2": {"S": "y"}, "sk": {"S": "s"}}');
        INSERT INTO "_ddb_i2" VALUES
            (E'2:g\x01,1:h,', NULL, E'p\x01', E's\x01',
             '{"g1": {"S": "g\u0001"}, "g2": {"S": "h"}, "pk": {"S": "p\u0001"}}');
        INSERT INTO "_ddb_i3" VALUES
            (E'g\x01', NULL, E'2:a\x01,1:b,', 's',
             '{"g": {"S": "g\u0001"}, "h1": {"S": "a\u0001"}, "h2": {"S": "b"}}');
        INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id,
                             key_schema, attribute_definitions) VALUES
            ('b1', 'b1', 't1', 't1', '111122223333',
             '[{"AttributeName": "pk", "KeyType": "HASH"}]', '[]'),
            ('b2', 'b2', 't2', 't2', '111122223333',
             '[{"AttributeName": "h1", "KeyType": "HASH"},
               {"AttributeName": "h2", "KeyType": "HASH"}]', '[]');
        INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES
            ('b1', E'p\x01', NULL, '{"pk": {"S": "p\u0001"}}'),
            ('b2', E'2:a\x01,1:b,', NULL, '{"h1": {"S": "a\u0001"}, "h2": {"S": "b"}}');"#,
    )
    .execute(db)
    .await
    .expect("seed the legacy rows");
}

#[tokio::test]
async fn rebuilds_composite_partition_keys_part_by_part() {
    if base_conn().is_none() {
        skip("rebuilds_composite_partition_keys_part_by_part");
        return;
    }
    let s = scratch().await;
    seed_composite(&s.db).await;
    run_pool(&s.db).await.expect("run the data code migrations");

    // What this build writes and looks up for the legacy key
    // h1 = "a\x01", h2 = "b": each part escaped, then joined.
    let mut item = Item::new();
    item.insert("h1".to_owned(), text("a\u{1}"));
    item.insert("h2".to_owned(), text("b"));
    let ks = [element("h1", KeyType::Hash), element("h2", KeyType::Hash)];
    let expected = composite_pk_to_text(&item, &ks).expect("composite key text");
    assert_eq!(expected, "3:a\u{1}\u{2},1:b,");

    // The table with two HASH attributes: the legacy row is rebuilt, the
    // plain row keeps its text.
    assert_eq!(
        texts(&s.db, "SELECT pk FROM \"_ddb_t2\" ORDER BY pk").await,
        vec!["1:x,1:y,".to_owned(), expected.clone()]
    );
    // A two-HASH GSI: pk rebuilt, the one-HASH base key replaced.
    let row: (String, String, String) =
        sqlx::query_as("SELECT pk, base_pk, base_sk_s FROM \"_ddb_i2\"")
            .fetch_one(&s.db)
            .await
            .expect("read _ddb_i2");
    assert_eq!(
        row,
        (
            "3:g\u{1}\u{2},1:h,".to_owned(),
            "p\u{1}\u{2}".to_owned(),
            "s\u{1}\u{2}".to_owned()
        )
    );
    // A one-HASH GSI on the two-HASH table: pk replaced, base_pk rebuilt.
    let row: (String, String) = sqlx::query_as("SELECT pk, base_pk FROM \"_ddb_i3\"")
        .fetch_one(&s.db)
        .await
        .expect("read _ddb_i3");
    assert_eq!(row, ("g\u{1}\u{2}".to_owned(), expected.clone()));
    // backup_items: the backup of the two-HASH table is rebuilt, the
    // other replaced.
    assert_eq!(
        texts(&s.db, "SELECT pk FROM backup_items ORDER BY backup_arn").await,
        vec!["p\u{1}\u{2}".to_owned(), expected]
    );
    s.cleanup().await;
}

fn context() -> PendingApplyContext {
    PendingApplyContext::Gsi(GsiApplyContext {
        base_key_schema: vec![element("pk", KeyType::Hash)],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        index: GsiIndexDef {
            index_id: "i1".to_owned(),
            key_schema: vec![element("g", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        },
    })
}

/// The partition a build without the escape gave a row for `item`: the
/// hash of the raw key text.
fn legacy_partition(item: &Item) -> i32 {
    let raw = extenddb_storage::util::composite_pk_to_text(item, &[element("pk", KeyType::Hash)])
        .expect("raw key text");
    partition_for(&raw)
}

#[tokio::test]
async fn recomputes_the_worker_partition_of_rows_queued_before_the_upgrade() {
    if base_conn().is_none() {
        skip("recomputes_the_worker_partition_of_rows_queued_before_the_upgrade");
        return;
    }
    let ctx = context();
    // A key holding U+0001 whose raw and escaped texts hash to different
    // workers, so the test sees the move.
    let item = [
        "a\u{1}b",
        "b\u{1}",
        "\u{1}c",
        "key\u{1}1",
        "key\u{1}2",
        "key\u{1}3",
    ]
    .into_iter()
    .map(|k| {
        let mut item = Item::new();
        item.insert("pk".to_owned(), text(k));
        item.insert("g".to_owned(), text("v"));
        item
    })
    .find(|item| {
        legacy_partition(item) != worker_partition(None, Some(item), &ctx).expect("partition")
    })
    .expect("a key whose partition moves");
    let current = worker_partition(None, Some(&item), &ctx).expect("partition");
    let legacy = legacy_partition(&item);

    // A key without U+0001 and a non-key string with it: same text both ways.
    let mut plain = Item::new();
    plain.insert("pk".to_owned(), text("plain"));
    plain.insert("note".to_owned(), text("n\u{1}"));
    let plain_partition = legacy_partition(&plain);

    let s = scratch().await;
    let raw_ctx = serde_json::to_value(&ctx).expect("context json");
    let raw = |i: &Item| serde_json::to_value(i).expect("item json");
    for (old_item, new_item, partition) in [
        (None, Some(raw(&item)), legacy),
        (Some(raw(&item)), None, legacy),
        (None, Some(raw(&plain)), plain_partition),
    ] {
        sqlx::query(
            "INSERT INTO gsi_pending (table_id, worker_partition, old_item, new_item, \
             index_context) VALUES ('t1', $1, $2, $3, $4)",
        )
        .bind(partition)
        .bind(old_item)
        .bind(new_item)
        .bind(&raw_ctx)
        .execute(&s.db)
        .await
        .expect("seed a queued row");
    }

    run_pool(&s.db).await.expect("run the data code migrations");

    let partitions: Vec<i32> =
        sqlx::query_scalar("SELECT worker_partition FROM gsi_pending ORDER BY id")
            .fetch_all(&s.db)
            .await
            .expect("read gsi_pending");
    assert_eq!(partitions, vec![current, current, plain_partition]);

    // The next write of the item is queued on the same worker.
    let mut tx = s.db.begin().await.expect("begin");
    enqueue_gsi_pending(&mut tx, "t1", None, Some(&item), 1, &ctx)
        .await
        .expect("enqueue the next write");
    tx.commit().await.expect("commit");
    let next: i32 =
        sqlx::query_scalar("SELECT worker_partition FROM gsi_pending ORDER BY id DESC LIMIT 1")
            .fetch_one(&s.db)
            .await
            .expect("read the new row");
    assert_eq!(next, current);
    s.cleanup().await;
}

mod unit {
    use super::super::m0006_escape_control_chars::has_composite_partition_key;
    use serde_json::json;

    #[test]
    fn only_a_key_schema_with_two_or_more_hash_attributes_is_composite() {
        let one = json!([
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]);
        let two = json!([
            {"AttributeName": "h1", "KeyType": "HASH"},
            {"AttributeName": "h2", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]);
        let ranges = json!([
            {"AttributeName": "h", "KeyType": "HASH"},
            {"AttributeName": "r1", "KeyType": "RANGE"},
            {"AttributeName": "r2", "KeyType": "RANGE"}
        ]);
        assert!(!has_composite_partition_key(&one));
        assert!(has_composite_partition_key(&two));
        assert!(!has_composite_partition_key(&ranges));
        assert!(!has_composite_partition_key(&json!({})));
    }
}
