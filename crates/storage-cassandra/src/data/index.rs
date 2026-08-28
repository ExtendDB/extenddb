// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI/LSI index operations for the `Cassandra` backend.
//!
//! Handles index metadata fetching, item projection for indexes, synchronous
//! index updates within batches, and async index enqueue for deferred
//! propagation.

use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::query_values;
use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, Projection, ProjectionType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    SortKeyValue, composite_pk_to_text, parse_sk, sk_column, sk_column_n,
};
use std::sync::Arc;

use super::ddl::{all_sort_key_info, index_table_name};
use crate::cassandra_util::{CassandraSession, get_column, query_rows};

/// Metadata for a single index, used during write-path GSI/LSI sync.
#[derive(Clone)]
pub struct IndexMeta {
    pub index_name: String,
    pub index_id: String,
    pub index_type: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    /// Per-GSI propagation delay in milliseconds. `None` means use system
    /// default. `Some(0)` means synchronous.
    pub propagation_delay_ms: Option<i32>,
}

/// Fetch all index metadata for a table from the catalog.
pub async fn fetch_indexes_for_table(
    table_id: &str,
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
) -> Result<Vec<IndexMeta>, StorageError> {
    let query = format!(
        "SELECT index_name, index_id, index_type, key_schema, projection, propagation_delay_ms \
         FROM {catalog_keyspace}.indexes WHERE table_id = ?"
    );

    let rows = query_rows(
        session,
        &query,
        query_values!(table_id),
        "fetch_indexes_for_table",
    )
    .await?;

    rows.into_iter()
        .map(|row| {
            let index_name: String = get_column(&row, "index_name", "fetch_indexes_for_table")?;
            let index_id: String = get_column(&row, "index_id", "fetch_indexes_for_table")?;
            let index_type: String = get_column(&row, "index_type", "fetch_indexes_for_table")?;
            let key_schema_text: String =
                get_column(&row, "key_schema", "fetch_indexes_for_table")?;
            let projection_text: String =
                get_column(&row, "projection", "fetch_indexes_for_table")?;
            let propagation_delay_ms: Option<i32> =
                row.get_by_name("propagation_delay_ms").ok().flatten();

            let key_schema: Vec<KeySchemaElement> = serde_json::from_str(&key_schema_text)
                .map_err(|e| {
                    tracing::error!("fetch_indexes_for_table deserialize key_schema: {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;

            let projection: Projection = serde_json::from_str(&projection_text).map_err(|e| {
                tracing::error!("fetch_indexes_for_table deserialize projection: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

            Ok(IndexMeta {
                index_name,
                index_id,
                index_type,
                key_schema,
                projection,
                propagation_delay_ms,
            })
        })
        .collect()
}

/// Fetch a single index by table_id and index_name (hot path for query routing).
///
/// Uses the PRIMARY KEY ((table_id), index_name) for efficient single-row lookup.
pub async fn fetch_index_by_name(
    table_id: &str,
    index_name: &str,
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
) -> Result<IndexMeta, StorageError> {
    let query = format!(
        "SELECT index_id, index_type, key_schema, projection, propagation_delay_ms \
         FROM {catalog_keyspace}.indexes WHERE table_id = ? AND index_name = ?"
    );

    let rows = query_rows(
        session,
        &query,
        query_values!(table_id, index_name),
        "fetch_index_by_name",
    )
    .await?;

    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::IndexNotFound(format!("Index {} not found", index_name)))?;

    let index_id: String = get_column(&row, "index_id", "fetch_index_by_name")?;
    let index_type: String = get_column(&row, "index_type", "fetch_index_by_name")?;
    let key_schema_text: String = get_column(&row, "key_schema", "fetch_index_by_name")?;
    let projection_text: String = get_column(&row, "projection", "fetch_index_by_name")?;
    let propagation_delay_ms: Option<i32> = row.get_by_name("propagation_delay_ms").ok().flatten();

    let key_schema: Vec<KeySchemaElement> =
        serde_json::from_str(&key_schema_text).map_err(|e| {
            tracing::error!("fetch_index_by_name deserialize key_schema: {e}");
            StorageError::Internal("Database error".to_owned())
        })?;

    let projection: Projection = serde_json::from_str(&projection_text).map_err(|e| {
        tracing::error!("fetch_index_by_name deserialize projection: {e}");
        StorageError::Internal("Database error".to_owned())
    })?;

    Ok(IndexMeta {
        index_name: index_name.to_owned(),
        index_id,
        index_type,
        key_schema,
        projection,
        propagation_delay_ms,
    })
}

/// Project an item according to an index's projection configuration.
///
/// Returns the projected item containing only the attributes that should be
/// stored in the index table's `item_data` column.
pub(crate) fn project_item_for_index(
    item: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    projection: &Projection,
) -> Item {
    match projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly => {
            let mut projected = Item::new();
            // Include base table keys + index keys
            for ks in base_ks.iter().chain(index_ks.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            projected
        }
        ProjectionType::Include => {
            // Base keys + index keys + non-key attributes
            let mut projected = Item::new();
            for ks in base_ks.iter().chain(index_ks.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            if let Some(ref attrs) = projection.non_key_attributes {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    }
}

/// Check if an item has all the key attributes required by an index.
pub(crate) fn item_has_index_keys(item: &Item, index_ks: &[KeySchemaElement]) -> bool {
    index_ks
        .iter()
        .all(|ks| item.contains_key(&ks.attribute_name))
}

/// Compute the effective propagation delay for an index.
///
/// Per-GSI setting overrides the system default. `Some(0)` = sync, `None` = use default.
pub(super) fn effective_delay(idx: &IndexMeta, system_default: u64) -> u64 {
    match idx.propagation_delay_ms {
        Some(0) => 0,
        Some(ms) if ms > 0 => ms as u64,
        Some(_) => system_default, // Negative values treated as "use system default".
        None => system_default,
    }
}

/// Synchronously update index tables for indexes with zero propagation delay.
///
/// Called within the same LOGGED BATCH as the base table write.
/// Only processes indexes where `effective_delay == 0`. Async indexes are
/// handled by async queue (Phase 2).
///
/// Adds parameterized statements to the batch builder.
#[allow(clippy::too_many_arguments)]
pub fn sync_indexes(
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    indexes: &[IndexMeta],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    system_default_delay: u64,
) -> Result<(), StorageError> {
    for idx in indexes {
        if idx.index_type != "LSI" && effective_delay(idx, system_default_delay) != 0 {
            continue; // Async — handled after commit. LSIs are always synchronous.
        }
        let idx_table = index_table_name(&idx.index_id);
        let idx_sks = all_sort_key_info(&idx.key_schema, attr_defs);
        let base_sks = all_sort_key_info(base_key_schema, attr_defs);

        // Delete old index row if the old item had index keys
        if let Some(old) = old_item {
            if item_has_index_keys(old, &idx.key_schema) {
                delete_index_row_multi(
                    batch,
                    account_keyspace,
                    &idx_table,
                    old,
                    &idx.key_schema,
                    base_key_schema,
                    &idx_sks,
                    &base_sks,
                )?;
            }
        }

        // Insert new index row if the new item has index keys
        if let Some(new) = new_item {
            if item_has_index_keys(new, &idx.key_schema) {
                let projected =
                    project_item_for_index(new, &idx.key_schema, base_key_schema, &idx.projection);
                insert_index_row_multi(
                    batch,
                    account_keyspace,
                    &idx_table,
                    new,
                    &projected,
                    &idx.key_schema,
                    base_key_schema,
                    &idx_sks,
                    &base_sks,
                )?;
            }
        }
    }
    Ok(())
}

/// Enqueue one `gsi_pending` row per async GSI into the in-progress logged
/// batch. Returns the number of rows enqueued.
///
/// Must be called before the batch is executed so the pending rows commit
/// atomically with the base write. LSIs and delay=0 GSIs are skipped (they are
/// handled synchronously by `sync_indexes`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enqueue_async_indexes(
    session: &std::sync::Arc<crate::cassandra_util::CassandraSession>,
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    key_info: &extenddb_core::types::TableKeyInfo,
    indexes: &[IndexMeta],
    old_item: Option<&extenddb_core::types::Item>,
    new_item: Option<&extenddb_core::types::Item>,
    system_default_delay: u64,
) -> Result<usize, StorageError> {
    use crate::gsi_queue::{GsiApplyContext, GsiIndexDef, jitter_delay_ms, partition_for};
    use extenddb_storage::util::composite_pk_to_text;

    let mut enqueued = 0usize;

    for idx in indexes {
        if idx.index_type == "LSI" {
            continue;
        }
        let delay = effective_delay(idx, system_default_delay);
        if delay == 0 {
            continue;
        }

        let context = GsiApplyContext {
            base_key_schema: key_info.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            index: GsiIndexDef {
                index_id: idx.index_id.clone(),
                key_schema: idx.key_schema.clone(),
                projection: idx.projection.clone(),
            },
        };

        let context_json =
            serde_json::to_string(&context).map_err(|e| StorageError::Internal(e.to_string()))?;
        let old_json = old_item
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let new_json = new_item
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let base_item = new_item.or(old_item);
        let worker_partition = match base_item {
            Some(item) => partition_for(&composite_pk_to_text(item, &key_info.key_schema)?),
            None => 0,
        };

        // Read last_ready_at for this partition (O(1) static column lookup).
        let last_ready_at = {
            use cdrs_tokio::query_values;
            use cdrs_tokio::types::IntoRustByName as _;
            let cql = format!(
                "SELECT last_ready_at FROM {account_keyspace}.gsi_pending \
                 WHERE worker_partition = ? LIMIT 1"
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                session,
                &cql,
                query_values!(worker_partition),
                "enqueue_async_indexes",
            )
            .await?;
            rows.into_iter()
                .next()
                .and_then(|row| row.get_by_name("last_ready_at").ok().flatten())
                .unwrap_or(0)
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let jittered_ms = now_ms + jitter_delay_ms(delay) as i64;
        // Clamp: ready_at must be >= last_ready_at + 1 to preserve causal ordering
        // across concurrent ExtendDB instances. last_ready_at is updated atomically
        // in the same logged batch via the INSERT (static columns can be set in INSERT).
        let ready_at_ms = jittered_ms.max(last_ready_at + 1);

        // Include last_ready_at in the INSERT — Cassandra allows setting static columns
        // directly in an INSERT, avoiding a separate UPDATE that would create a ghost row.
        let insert_cql = format!(
            "INSERT INTO {account_keyspace}.gsi_pending \
             (worker_partition, last_ready_at, ready_at, id, table_id, old_item, new_item, index_context) \
             VALUES (?, ?, ?, now(), ?, ?, ?, ?)"
        );
        let insert_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
            cdrs_tokio::types::value::Value::from(worker_partition),
            cdrs_tokio::types::value::Value::from(ready_at_ms),
            cdrs_tokio::types::value::Value::from(ready_at_ms),
            cdrs_tokio::types::value::Value::from(key_info.table_id.as_str()),
            match old_json {
                Some(ref s) => cdrs_tokio::types::value::Value::from(s.as_str()),
                None => cdrs_tokio::types::value::Value::NotSet,
            },
            match new_json {
                Some(ref s) => cdrs_tokio::types::value::Value::from(s.as_str()),
                None => cdrs_tokio::types::value::Value::NotSet,
            },
            cdrs_tokio::types::value::Value::from(context_json.as_str()),
        ]);

        let old = std::mem::replace(batch, BatchQueryBuilder::new());
        *batch = old.add_query(insert_cql, insert_qv);

        enqueued += 1;
    }

    Ok(enqueued)
}

/// Delete a row from an index table using index keys and base table keys.
///
/// Adds a parameterized DELETE statement to the batch.
/// Cassandra requires ALL PRIMARY KEY columns for DELETE.
pub(crate) fn delete_index_row_multi(
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    idx_table: &str,
    item: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    idx_sks: &[(&str, ScalarAttributeType)],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let idx_pk_text = composite_pk_to_text(item, index_ks)?;
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let mut where_cols = vec!["pk = ?".to_owned()];
    let mut values: Vec<cdrs_tokio::types::value::Value> =
        vec![cdrs_tokio::types::value::Value::from(idx_pk_text.as_str())];

    // Index sort keys (clustering)
    for (i, &(sk_name, sk_type)) in idx_sks.iter().enumerate() {
        let col = if i == 0 {
            sk_column(sk_type).to_owned()
        } else {
            sk_column_n(i, sk_type)
        };
        where_cols.push(format!("{col} = ?"));
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            values.push(sk_to_value(&sk));
        } else {
            values.push(cdrs_tokio::types::value::Value::NotSet);
        }
    }

    // Base partition key (clustering)
    where_cols.push("base_pk = ?".to_owned());
    values.push(cdrs_tokio::types::value::Value::from(base_pk_text.as_str()));

    // Base sort keys (clustering)
    for (i, &(sk_name, sk_type)) in base_sks.iter().enumerate() {
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        where_cols.push(format!("{col} = ?"));
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            values.push(sk_to_value(&sk));
        } else {
            values.push(cdrs_tokio::types::value::Value::NotSet);
        }
    }

    let cql = format!(
        "DELETE FROM {}.{} WHERE {}",
        account_keyspace,
        idx_table,
        where_cols.join(" AND ")
    );
    let qv = cdrs_tokio::query::QueryValues::SimpleValues(values);
    let old = std::mem::replace(batch, BatchQueryBuilder::new());
    *batch = old.add_query(cql, qv);
    Ok(())
}

/// Insert a row into an index table with multi-part key support.
///
/// Adds a parameterized INSERT statement to the batch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_index_row_multi(
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    idx_table: &str,
    item: &Item,
    projected: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    idx_sks: &[(&str, ScalarAttributeType)],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let idx_pk_text = composite_pk_to_text(item, index_ks)?;
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let item_json =
        serde_json::to_string(projected).map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut cols = vec!["pk".to_owned()];
    let mut values: Vec<cdrs_tokio::types::value::Value> =
        vec![cdrs_tokio::types::value::Value::from(idx_pk_text.as_str())];

    // Index SK values
    for (i, &(sk_name, sk_type)) in idx_sks.iter().enumerate() {
        cols.push(sk_column_n(i, sk_type));
        if let Some(sk_val) = item.get(sk_name) {
            values.push(sk_to_value(&parse_sk(sk_val, sk_type)?));
        } else {
            values.push(cdrs_tokio::types::value::Value::NotSet);
        }
    }

    // Base keys
    cols.push("base_pk".to_owned());
    values.push(cdrs_tokio::types::value::Value::from(base_pk_text.as_str()));

    for (i, &(sk_name, sk_type)) in base_sks.iter().enumerate() {
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        cols.push(col);
        if let Some(sk_val) = item.get(sk_name) {
            values.push(sk_to_value(&parse_sk(sk_val, sk_type)?));
        } else {
            values.push(cdrs_tokio::types::value::Value::NotSet);
        }
    }

    cols.push("item_data".to_owned());
    values.push(item_json.as_str().into());

    let placeholders = vec!["?"; cols.len()].join(", ");
    let cql = format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        account_keyspace,
        idx_table,
        cols.join(", "),
        placeholders
    );
    let qv = cdrs_tokio::query::QueryValues::SimpleValues(values);
    let old = std::mem::replace(batch, BatchQueryBuilder::new());
    *batch = old.add_query(cql, qv);
    Ok(())
}

/// Convert a `SortKeyValue` to a cdrs_tokio bound `Value`.
pub(crate) fn sk_to_value(sk: &SortKeyValue) -> cdrs_tokio::types::value::Value {
    match sk {
        SortKeyValue::S(s) => s.as_str().into(),
        SortKeyValue::N(n) => super::decimal_to_value(n),
        SortKeyValue::B(b) => {
            cdrs_tokio::types::value::Value::from(cdrs_tokio::types::blob::Blob::new(b.to_vec()))
        }
    }
}

/// Delete index metadata and drop index data tables for a given table.
///
/// Called by:
/// - `process_control_plane_transitions` when table is DELETING → deleted
/// - `update_table` when GSI is deleted via UpdateTable API
///
/// Returns list of index IDs that were deleted (for caller logging/tracking).
pub(crate) async fn delete_indexes_for_table(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
    account_keyspace: &str,
    table_id: &str,
    engine: &crate::CassandraEngine,
) -> Result<Vec<String>, StorageError> {
    // Collect index IDs
    let index_query = format!(
        "SELECT index_id FROM {}.indexes WHERE table_id = ?",
        catalog_keyspace
    );

    let rows = query_rows(
        session,
        &index_query,
        query_values!(table_id),
        "delete_indexes_for_table",
    )
    .await?;

    let mut index_ids = Vec::new();
    for row in rows {
        let index_id: String = get_column(&row, "index_id", "delete_indexes_for_table")?;
        index_ids.push(index_id);
    }

    // Delete index metadata from catalog
    let delete_query = format!(
        "DELETE FROM {}.indexes WHERE table_id = ?",
        catalog_keyspace
    );

    crate::cassandra_util::execute(
        session,
        &delete_query,
        query_values!(table_id),
        "delete_indexes_for_table",
    )
    .await?;

    // Drop index data tables
    for index_id in &index_ids {
        engine
            .drop_index_data_table(account_keyspace, index_id)
            .await?;
    }

    Ok(index_ids)
}

/// Delete a specific index by name for a table.
///
/// Called by `update_table` when a GSI is deleted via UpdateTable API.
///
/// Returns the index_id that was deleted.
pub(crate) async fn delete_index_by_name(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
    account_keyspace: &str,
    table_id: &str,
    index_name: &str,
    engine: &crate::CassandraEngine,
) -> Result<String, StorageError> {
    // Get index_id first
    let query = format!(
        "SELECT index_id FROM {}.indexes WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );

    let row = crate::cassandra_util::query_optional(
        session,
        &query,
        query_values!(table_id, index_name),
        "delete_index_by_name",
    )
    .await?
    .ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;

    let index_id: String = get_column(&row, "index_id", "delete_index_by_name")?;

    // Delete from catalog
    let delete_query = format!(
        "DELETE FROM {}.indexes WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );

    crate::cassandra_util::execute(
        session,
        &delete_query,
        query_values!(table_id, index_name),
        "delete_index_by_name",
    )
    .await?;

    // Drop index data table
    engine
        .drop_index_data_table(account_keyspace, &index_id)
        .await?;

    Ok(index_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, KeyType, ProjectionType};

    #[test]
    fn test_project_item_keys_only() {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("test".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N("123".to_owned()));
        item.insert("data".to_owned(), AttributeValue::S("value".to_owned()));

        let base_ks = vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ];

        let index_ks = vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        }];

        let projection = Projection {
            projection_type: ProjectionType::KeysOnly,
            non_key_attributes: None,
        };

        item.insert(
            "gsi_pk".to_owned(),
            AttributeValue::S("gsi_value".to_owned()),
        );

        let projected = project_item_for_index(&item, &index_ks, &base_ks, &projection);

        assert_eq!(projected.len(), 3);
        assert!(projected.contains_key("pk"));
        assert!(projected.contains_key("sk"));
        assert!(projected.contains_key("gsi_pk"));
        assert!(!projected.contains_key("data"));
    }

    #[test]
    fn test_item_has_index_keys() {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("test".to_owned()));
        item.insert("gsi_pk".to_owned(), AttributeValue::S("value".to_owned()));

        let index_ks = vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        }];

        assert!(item_has_index_keys(&item, &index_ks));

        let missing_ks = vec![KeySchemaElement {
            attribute_name: "missing".to_owned(),
            key_type: KeyType::Hash,
        }];

        assert!(!item_has_index_keys(&item, &missing_ks));
    }

    #[test]
    fn test_effective_delay() {
        let idx = IndexMeta {
            index_name: "test".to_owned(),
            index_id: "id".to_owned(),
            index_type: "GSI".to_owned(),
            key_schema: vec![],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            propagation_delay_ms: Some(0),
        };

        assert_eq!(effective_delay(&idx, 1000), 0);

        let idx_with_delay = IndexMeta {
            propagation_delay_ms: Some(500),
            ..idx.clone()
        };
        assert_eq!(effective_delay(&idx_with_delay, 1000), 500);

        let idx_default = IndexMeta {
            propagation_delay_ms: None,
            ..idx
        };
        assert_eq!(effective_delay(&idx_default, 1000), 1000);
    }
}
