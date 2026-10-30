// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helpers shared by the direct `DeleteTable` tests (`concurrent_delete_table`
//! and `delete_table_lease`): a one-key table, and reads of the catalog row,
//! the data table and the name-keyed rows that a delete removes.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, GsiInput,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType, TableDescription,
};
use extenddb_storage::{MetadataEngine, TableEngine};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::test_config;

/// How long the waits below poll before they fail the test.
const WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Runs `body` as its own task, then removes every catalog row the account
/// holds, whether `body` returned or panicked, and re-raises its panic.
///
/// The catalog keyspace is shared by every test in this binary. A test that
/// fails between a claim and its own clean-up would otherwise leave a
/// `DELETING` row there; once the test's account keyspace is dropped, the
/// control-plane sweep that next reaches the row fails on the missing
/// keyspace, inside whichever test ran it. Each row goes by its table id,
/// with the condition `DeleteTable` uses, together with its index rows, its
/// tags and the account's continuous-backup rows.
pub(crate) async fn removing_rows_after<F>(engine: &CassandraEngine, account_id: &str, body: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let outcome = tokio::spawn(body).await;
    remove_account_rows(engine, account_id).await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

/// Best effort: a failure here must not hide the panic being re-raised.
async fn remove_account_rows(engine: &CassandraEngine, account_id: &str) {
    use cdrs_tokio::types::IntoRustByName;
    let catalog = engine.catalog_keyspace();
    let select = format!(
        "SELECT table_name, table_id, table_arn FROM {catalog}.tables WHERE account_id = ?"
    );
    let rows = match engine
        .session()
        .query_with_values(&select, cdrs_tokio::query_values!(account_id))
        .await
        .ok()
        .and_then(|frame| frame.response_body().ok())
        .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
    {
        Some(rows) => rows,
        None => return,
    };
    for row in rows {
        let name: Result<String, _> = row.get_r_by_name("table_name");
        let table_id: Result<String, _> = row.get_r_by_name("table_id");
        let arn: Result<String, _> = row.get_r_by_name("table_arn");
        let (Ok(name), Ok(table_id), Ok(arn)) = (name, table_id, arn) else {
            continue;
        };
        let _ = engine
            .session()
            .query_with_values(
                &format!("DELETE FROM {catalog}.tags WHERE resource_arn = ?"),
                cdrs_tokio::query_values!(arn.as_str()),
            )
            .await;
        let _ = engine
            .session()
            .query_with_values(
                &format!("DELETE FROM {catalog}.indexes WHERE table_id = ?"),
                cdrs_tokio::query_values!(table_id.as_str()),
            )
            .await;
        let _ = extenddb_storage_cassandra::cassandra_util::query_lwt(
            engine.session(),
            &format!(
                "DELETE FROM {catalog}.tables WHERE account_id = ? AND table_name = ? \
                 IF table_id = ?"
            ),
            cdrs_tokio::query_values!(account_id, name.as_str(), table_id.as_str()),
        )
        .await;
    }
    let _ = engine
        .session()
        .query_with_values(
            &format!("DELETE FROM {catalog}.continuous_backups WHERE account_id = ?"),
            cdrs_tokio::query_values!(account_id),
        )
        .await;
}

/// Activates a `CREATING` row as the activation sweep does, scoped to this
/// row: `ACTIVE`, deadline cleared, `IF table_status = 'CREATING'`. In this
/// harness `create_table` writes `CREATING` with a 250 ms activation deadline
/// and nothing else activates it unless some test happens to run the sweep.
pub(crate) async fn activate(engine: &CassandraEngine, account_id: &str, name: &str) {
    let query = format!(
        "UPDATE {}.tables SET table_status = 'ACTIVE', status_transition_at = null \
         WHERE account_id = ? AND table_name = ? IF table_status = 'CREATING'",
        engine.catalog_keyspace()
    );
    extenddb_storage_cassandra::cassandra_util::query_lwt(
        engine.session(),
        &query,
        cdrs_tokio::query_values!(account_id, name),
    )
    .await
    .expect("activate the table");
    assert_eq!(
        row_status(engine, account_id, name).await.as_deref(),
        Some("ACTIVE"),
        "{name} is ACTIVE"
    );
}

/// Moves a claimed row's deadline one second into the past, the state a claim
/// reaches five minutes after its call stopped.
pub(crate) async fn move_deadline_into_the_past(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) {
    let past_ms = chrono::Utc::now().timestamp_millis() - 1_000;
    let query = format!(
        "UPDATE {}.tables SET status_transition_at = ? \
         WHERE account_id = ? AND table_name = ? IF EXISTS",
        engine.catalog_keyspace()
    );
    engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(past_ms, account_id, name))
        .await
        .expect("move the deadline into the past");
}

/// Polls until the name has no catalog row.
pub(crate) async fn wait_for_row_gone(engine: &CassandraEngine, account_id: &str, name: &str) {
    let started = std::time::Instant::now();
    while row_status(engine, account_id, name).await.is_some() {
        assert!(
            started.elapsed() < WAIT_LIMIT,
            "{name}: the row is still there"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Polls until the row reads `status`.
pub(crate) async fn wait_for_status(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
    status: &str,
) {
    let started = std::time::Instant::now();
    while row_status(engine, account_id, name).await.as_deref() != Some(status) {
        assert!(
            started.elapsed() < WAIT_LIMIT,
            "{name}: never read {status}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

/// Polls until the table's base data table has been dropped.
pub(crate) async fn wait_for_data_table_gone(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
) {
    let started = std::time::Instant::now();
    while data_table_exists(engine, account_id, table_id).await {
        assert!(
            started.elapsed() < WAIT_LIMIT,
            "data table of {table_id} still there"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// The tags stored for `arn`, as `(key, value)` pairs in key order.
pub(crate) async fn tags_of(engine: &CassandraEngine, arn: &str) -> Vec<(String, String)> {
    engine
        .list_tags(arn)
        .await
        .expect("read the tags")
        .into_iter()
        .map(|tag| (tag.key, tag.value))
        .collect()
}

/// [`create_input`] with `gsis` global secondary indexes, one CQL table each
/// for the delete to drop.
pub(crate) fn gsi_input(name: &str, gsis: usize) -> CreateTableInput {
    let mut input = create_input(name);
    let mut indexes = Vec::new();
    for i in 0..gsis {
        let attribute = format!("g{i}");
        input.attribute_definitions.push(AttributeDefinition {
            attribute_name: attribute.clone(),
            attribute_type: ScalarAttributeType::S,
        });
        indexes.push(GsiInput {
            index_name: format!("Idx{i}"),
            key_schema: vec![KeySchemaElement {
                attribute_name: attribute,
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
    }
    input.global_secondary_indexes = Some(indexes);
    input
}

/// A table with one string partition key, created through the engine so the
/// test has the ARN and the table id the catalog holds.
pub(crate) async fn create_one(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> TableDescription {
    engine
        .create_table(account_id, create_input(name))
        .await
        .expect("create the table")
}

pub(crate) fn create_input(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    }
}

pub(crate) fn delete_input(name: &str) -> DeleteTableInput {
    DeleteTableInput {
        table_name: name.to_owned(),
    }
}

/// True when the table's data table is still in the account keyspace.
pub(crate) async fn data_table_exists(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
) -> bool {
    let keyspace = format!("{}_account_{account_id}", test_config().keyspace_prefix);
    let data_table = format!("items_{}", table_id.replace('-', "_"));
    let rows = engine
        .session()
        .query_with_values(
            "SELECT table_name FROM system_schema.tables \
             WHERE keyspace_name = ? AND table_name = ?",
            cdrs_tokio::query_values!(keyspace.as_str(), data_table.as_str()),
        )
        .await
        .expect("read system_schema.tables")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default();
    !rows.is_empty()
}

pub(crate) async fn row_status(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> Option<String> {
    row_column(engine, account_id, name, "table_status").await
}

/// The table id the catalog row holds, which says which incarnation of the
/// name the row belongs to.
pub(crate) async fn row_table_id(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> Option<String> {
    row_column(engine, account_id, name, "table_id").await
}

pub(crate) async fn row_column(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
    column: &str,
) -> Option<String> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT {column} FROM {}.tables WHERE account_id = ? AND table_name = ?",
        engine.catalog_keyspace()
    );
    let rows = engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, name))
        .await
        .expect("read the row")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default();
    rows.first()
        .map(|row| row.get_r_by_name(column).expect("column"))
}

/// The PITR flag the `continuous_backups` row stores for the name, or `None`
/// when there is no row.
pub(crate) async fn pitr_row(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> Option<bool> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT pitr_enabled FROM {}.continuous_backups WHERE account_id = ? AND table_name = ?",
        engine.catalog_keyspace()
    );
    let rows = engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, name))
        .await
        .expect("read continuous_backups")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default();
    rows.first()
        .map(|row| row.get_r_by_name("pitr_enabled").expect("pitr_enabled"))
}

/// How many rows a catalog query returns.
pub(crate) async fn catalog_rows(
    engine: &CassandraEngine,
    query: &str,
    values: cdrs_tokio::query::QueryValues,
) -> usize {
    engine
        .session()
        .query_with_values(query, values)
        .await
        .expect("query the catalog")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default()
        .len()
}

/// The row's `status_transition_at` in milliseconds since the epoch.
pub(crate) async fn row_deadline_ms(
    engine: &CassandraEngine,
    account_id: &str,
    name: &str,
) -> Option<i64> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT toUnixTimestamp(status_transition_at) AS deadline_ms FROM {}.tables \
         WHERE account_id = ? AND table_name = ?",
        engine.catalog_keyspace()
    );
    let rows = engine
        .session()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, name))
        .await
        .expect("read the row")
        .response_body()
        .expect("response body")
        .into_rows()
        .unwrap_or_default();
    rows.first()
        .and_then(|row| row.get_by_name("deadline_ms").expect("deadline_ms"))
}
