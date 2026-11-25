// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Table and queue helpers shared by the GSI queue storage-level tests
//! (`gsi_queue_fairness.rs`, `gsi_queue_many_tables.rs`), on the scratch
//! database of `tests/common/stream_disable_fixture.rs`.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind,
//! named `eddb_sdis_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it each test reports
//! a skip and passes. With `EXTENDDB_TEST_PG_REQUIRED` set to any value but `0`,
//! the fixture's `base_conn` fails each test instead.

#![allow(dead_code)]

// The fixture re-exports the types the stream disable tests need; these tests
// use its scratch database, its skip check and its table input only.
#[path = "../common/stream_disable_fixture.rs"]
#[allow(unused_imports)]
mod fixture;

use std::collections::BTreeMap;
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, GsiInput, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::{DataEngine, TableEngine};
use fixture::{ACCOUNT, table_input};
pub use fixture::{Scratch, base_conn};
use sqlx::PgPool;

/// The fixture's scratch database, with the engine's queue workers started.
pub async fn scratch() -> Scratch {
    let Scratch {
        engine,
        db,
        admin,
        db_name,
    } = fixture::scratch().await;
    Scratch {
        engine: engine.start_gsi_workers(),
        db,
        admin,
        db_name,
    }
}

pub async fn set_delay_ms(db: &PgPool, ms: u64) {
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(ms.to_string())
    .execute(db)
    .await
    .expect("set the propagation delay");
}

/// Create `name`, the fixture's table keyed on `pk`, with one GSI `gidx` keyed
/// on `g`.
pub async fn table_with_gsi(s: &Scratch, name: &str) -> TableKeyInfo {
    let mut input = table_input(name, false);
    input.attribute_definitions.push(AttributeDefinition {
        attribute_name: "g".to_owned(),
        attribute_type: ScalarAttributeType::S,
    });
    input.global_secondary_indexes = Some(vec![GsiInput {
        index_name: "gidx".to_owned(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "g".to_owned(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: None,
        warm_throughput: None,
    }]);
    s.engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("read the key info")
}

pub async fn put(s: &Scratch, key_info: &TableKeyInfo, pk: &str, g: &str) {
    let item = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("g".to_owned(), AttributeValue::S(g.to_owned())),
    ]);
    let maps = ExpressionMaps::default();
    s.engine
        .put_item(key_info, item, false, None, &maps, None)
        .await
        .expect("put the item");
}

pub async fn pending(db: &PgPool, table_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM gsi_pending WHERE table_id = $1")
        .bind(table_id)
        .fetch_one(db)
        .await
        .expect("count the table's pending rows")
}

/// Pending rows over several tables at once.
pub async fn pending_in(db: &PgPool, table_ids: &[String]) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM gsi_pending WHERE table_id = ANY($1)")
        .bind(table_ids)
        .fetch_one(db)
        .await
        .expect("count the tables' pending rows")
}

pub async fn index_rows(db: &PgPool, table_id: &str) -> i64 {
    let index_id: String = sqlx::query_scalar(
        "SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = 'gidx'",
    )
    .bind(table_id)
    .fetch_one(db)
    .await
    .expect("look up the index id");
    sqlx::query_scalar(&format!("SELECT count(*) FROM \"_ddb_{index_id}\""))
        .fetch_one(db)
        .await
        .expect("count the index rows")
}

/// The `(new_item, index_context)` of one row the engine enqueued for `table`,
/// read back and then removed from the queue: a payload the workers apply as they
/// would any write. The caller sets a propagation delay long enough (60 s) for the
/// row to still be there, and removes it so its `ready_at` does not floor rows
/// enqueued after it.
pub async fn template_row(
    s: &Scratch,
    table: &TableKeyInfo,
) -> (serde_json::Value, serde_json::Value) {
    put(s, table, "template", "g0").await;
    let row: (serde_json::Value, serde_json::Value) =
        sqlx::query_as("SELECT new_item, index_context FROM gsi_pending WHERE table_id = $1")
            .bind(&table.table_id)
            .fetch_one(&s.db)
            .await
            .expect("read the template row");
    sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
        .bind(&table.table_id)
        .execute(&s.db)
        .await
        .expect("remove the template row");
    row
}

/// Put `count` rows for `table` on the queue, each a distinct `pk`
/// (`<prefix><n>`), copied from `template`, ready `ready_in` from now (zero for
/// ready at once). `partition` pins every row to one worker; `None` spreads them
/// over the four.
///
/// The base items the rows describe are written to the table as well. A
/// secondary index row's apply writes the item as the base table holds it at
/// apply time, not the image the row carries, so a row with no base item behind
/// it applies as a delete and leaves no index row.
pub async fn enqueue_rows(
    s: &Scratch,
    table: &TableKeyInfo,
    template: &(serde_json::Value, serde_json::Value),
    prefix: &str,
    count: i64,
    partition: Option<i32>,
    ready_in: Duration,
) {
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{}\" (pk, item_data) \
         SELECT $2 || g::text, jsonb_set($1::jsonb, '{{pk,S}}', to_jsonb($2 || g::text)) \
         FROM generate_series(1, $3) g",
        table.table_id
    ))
    .bind(&template.0)
    .bind(prefix)
    .bind(count)
    .execute(&s.db)
    .await
    .expect("write the base items");
    sqlx::query(
        "INSERT INTO gsi_pending \
         (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
         SELECT $1, COALESCE($5, (g % 4)::int), NULL, \
                jsonb_set($2::jsonb, '{pk,S}', to_jsonb($6 || g::text)), $3::jsonb, \
                NOW() + make_interval(secs => $7) \
         FROM generate_series(1, $4) g",
    )
    .bind(&table.table_id)
    .bind(&template.0)
    .bind(&template.1)
    .bind(count)
    .bind(partition)
    .bind(prefix)
    .bind(ready_in.as_secs_f64())
    .execute(&s.db)
    .await
    .expect("enqueue the rows");
}
