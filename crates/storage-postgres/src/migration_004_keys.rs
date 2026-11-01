// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Two rewrites of `004_escape_control_chars` (`migrations.rs`) that cannot be
//! done by escaping a stored text where it lies, because the text is derived
//! from the key strings:
//!
//! - A partition key column of a key schema with more than one HASH attribute
//!   holds the netstring join of the parts (`<len>:<part>,` each), and the
//!   escape changes a part's length. [`rebuild_composite_rows`] splits each
//!   legacy value, escapes the parts and joins them again with the new
//!   lengths, through the function the write path uses
//!   (`key_text::join_escaped_parts`).
//! - `gsi_pending.worker_partition` is a hash of the stored base key text
//!   (`gsi_queue::partition_for`), so it changes with the key text.
//!   [`recompute_worker_partitions`] recomputes it with the function the
//!   enqueue uses (`gsi_queue::worker_partition`).
//!
//! Both run inside the per-table transaction of `escape_rows_in_table`, before
//! that table's progress marker commits.

use extenddb_storage::management_store::{OpError, OpResult};

use crate::data::key_text::reescape_legacy_composite;
use crate::gsi_queue::{PendingApplyContext, worker_partition};

/// The `backup_items` rows whose `pk` holds a composite key: the rows of a
/// backup whose table's key schema has more than one HASH attribute. A boolean
/// SQL expression over a `backup_items` row; `backups` is in the same catalog
/// database.
pub(crate) const BACKUP_ITEMS_COMPOSITE_ROWS: &str = "backup_arn IN (\
     SELECT b.backup_arn FROM backups b WHERE (\
         SELECT COUNT(*) FROM jsonb_array_elements(b.key_schema) e \
         WHERE e->>'KeyType' = 'HASH') > 1)";

/// Whether a catalog key schema (`[{"AttributeName", "KeyType"}, ...]`) has
/// more than one HASH attribute, so its partition key column holds a netstring
/// join.
pub(crate) fn has_composite_partition_key(key_schema: &serde_json::Value) -> bool {
    key_schema.as_array().is_some_and(|elements| {
        elements
            .iter()
            .filter(|e| e.get("KeyType").and_then(serde_json::Value::as_str) == Some("HASH"))
            .count()
            > 1
    })
}

/// Rebuild the composite key text in `col` of the rows that `rows` (a boolean
/// SQL expression over the row) selects and that hold U+0001, one row at a
/// time. A SQL `replace()` over the whole text would leave each length prefix
/// counting the raw part (`2:a\x01,1:b,` would become `2:a\x01\x02,1:b,`,
/// where this build writes and looks up `3:a\x01\x02,1:b,`). A value that is
/// not a netstring fails the migration, naming the table and column; the
/// table's transaction rolls back and nothing is marked.
pub(crate) async fn rebuild_composite_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    quoted_table: &str,
    col: &str,
    rows: &str,
) -> OpResult<u64> {
    let select = format!(
        "SELECT ctid::text, \"{col}\" FROM {quoted_table} \
         WHERE position(chr(1) IN \"{col}\") > 0 AND ({rows}) FOR UPDATE"
    );
    let found: Vec<(String, String)> = sqlx::query_as(&select)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| OpError::Internal(format!("Find {quoted_table}.{col} to rebuild: {e}")))?;
    let update = format!("UPDATE {quoted_table} SET \"{col}\" = $1 WHERE ctid = $2::tid");
    for (ctid, legacy) in &found {
        let rebuilt = reescape_legacy_composite(legacy).ok_or_else(|| {
            OpError::Internal(format!(
                "Re-encode {quoted_table}.{col}: the composite key {legacy:?} is not a netstring"
            ))
        })?;
        sqlx::query(&update)
            .bind(rebuilt)
            .bind(ctid)
            .execute(&mut **tx)
            .await
            .map_err(|e| OpError::Internal(format!("Rebuild {quoted_table}.{col}: {e}")))?;
    }
    Ok(found.len() as u64)
}

/// The partition a row queued with these columns gets from this build's
/// enqueue, or `None` when a column does not decode (a worker handles such a
/// row as a poison row, whatever partition holds it).
fn current_partition(
    old_item: Option<serde_json::Value>,
    new_item: Option<serde_json::Value>,
    index_context: serde_json::Value,
) -> Option<i32> {
    let old_item = old_item.map(crate::data::json_to_item).transpose().ok()?;
    let new_item = new_item.map(crate::data::json_to_item).transpose().ok()?;
    let context: PendingApplyContext = crate::data::stored_json_to(index_context).ok()?;
    worker_partition(old_item.as_ref(), new_item.as_ref(), &context).ok()
}

/// Recompute `worker_partition` of the queued rows whose item holds U+0001,
/// after their jsonb columns were re-encoded in the same transaction.
///
/// A build without the escape hashed the raw base key; this build hashes the
/// escaped one, so for a key holding U+0001 a row queued before the upgrade and
/// the next write of the same item would be claimed by different workers, in
/// either order, which breaks the queue's one item, one worker rule. Every row
/// of one item moves to the same new partition and keeps its `id` and
/// `ready_at`, so the item's rows are still claimed in write order, and a row
/// enqueued after the upgrade gets a `ready_at` no earlier than theirs from the
/// partition clamp in `enqueue_gsi_pending`.
pub(crate) async fn recompute_worker_partitions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> OpResult<u64> {
    type Row = (
        i64,
        i32,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        serde_json::Value,
    );
    // '\u0001' is how jsonb::text renders U+0001; chr(92) spells the backslash
    // as in `escape_jsonb_rows`. The base key is in new_item, or in old_item for
    // a delete, as `worker_partition` reads it.
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, worker_partition, old_item, new_item, index_context FROM gsi_pending \
         WHERE position((chr(92) || 'u0001') IN COALESCE(new_item, old_item)::text) > 0 \
         ORDER BY id FOR UPDATE",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| OpError::Internal(format!("Find gsi_pending rows to repartition: {e}")))?;
    let mut changed: u64 = 0;
    for (id, stored, old_item, new_item, index_context) in rows {
        let Some(partition) = current_partition(old_item, new_item, index_context) else {
            continue;
        };
        if partition == stored {
            continue;
        }
        sqlx::query("UPDATE gsi_pending SET worker_partition = $1 WHERE id = $2")
            .bind(partition)
            .bind(id)
            .execute(&mut **tx)
            .await
            .map_err(|e| OpError::Internal(format!("Repartition gsi_pending row {id}: {e}")))?;
        changed += 1;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::has_composite_partition_key;
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

#[cfg(test)]
mod live {
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

    use crate::data::key_text::composite_pk_to_text;
    use crate::gsi_queue::{
        GsiApplyContext, GsiIndexDef, PendingApplyContext, enqueue_gsi_pending, partition_for,
        worker_partition,
    };
    use crate::migrations::live_reencode::{base_conn, scratch, skip};
    use crate::migrations::run_data_code_migrations;

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
        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run the data code migrations");

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
        let raw =
            extenddb_storage::util::composite_pk_to_text(item, &[element("pk", KeyType::Hash)])
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

        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run the data code migrations");

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
}
