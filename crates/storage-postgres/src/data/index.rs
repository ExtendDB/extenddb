// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI/LSI index operations for the `PostgreSQL` backend.
//!
//! Handles index metadata fetching, item projection for indexes, synchronous
//! index updates within transactions, and async index enqueue for deferred
//! propagation.

use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, Projection, ProjectionType, ScalarAttributeType,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column, sk_column_n};

use super::{all_sort_key_info, index_table_name};
use crate::gsi_queue::{GsiApplyContext, GsiIndexDef, enqueue_gsi_pending};

/// Map a `sqlx` error to `StorageError`, preserving the SQLSTATE code in the
/// message. The async GSI worker keys off the code (e.g. `42P01`,
/// undefined_table) to tell a dropped-index race apart from a real failure;
/// `sqlx`'s `Display` only carries the human text, so the code must be kept.
fn db_error(e: sqlx::Error) -> StorageError {
    match e.as_database_error().and_then(|d| d.code()) {
        Some(code) => StorageError::Internal(format!("SQLSTATE {code}: {e}")),
        None => StorageError::Internal(e.to_string()),
    }
}

/// Metadata for a single index, used during write-path GSI/LSI sync.
pub(crate) struct IndexMeta {
    pub(super) index_id: String,
    pub(super) index_name: String,
    pub(super) index_type: String,
    pub(super) key_schema: Vec<KeySchemaElement>,
    pub(super) projection: Projection,
    /// Per-GSI propagation delay in milliseconds. `None` means use system
    /// default. `Some(0)` means synchronous.
    pub(super) propagation_delay_ms: Option<i32>,
}

/// Fetch all index metadata for a table from the catalog.
pub(crate) async fn fetch_indexes_for_table(
    table_id: &str,
    pool: &sqlx::PgPool,
) -> Result<Vec<IndexMeta>, StorageError> {
    let rows: Vec<(String, String, String, serde_json::Value, serde_json::Value, Option<i32>)> = sqlx::query_as(
        "SELECT index_id, index_name, index_type, key_schema, projection, propagation_delay_ms FROM indexes WHERE table_id = $1",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    rows.into_iter()
        .map(|(id, index_name, idx_type, ks_json, proj_json, delay)| {
            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: Projection = serde_json::from_value(proj_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(IndexMeta {
                index_id: id,
                index_name,
                index_type: idx_type,
                key_schema,
                projection,
                propagation_delay_ms: delay,
            })
        })
        .collect()
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
/// Called within the same PG transaction as the base table write.
/// Only processes indexes where `effective_delay == 0`. Async indexes are
/// handled by the persistent `gsi_pending` queue after the transaction commits.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        if let Some(old) = old_item
            && item_has_index_keys(old, &idx.key_schema)
        {
            delete_index_row_multi(tx, &idx_table, old, base_key_schema, &base_sks).await?;
        }

        // Insert new index row if the new item has index keys
        if let Some(new) = new_item
            && item_has_index_keys(new, &idx.key_schema)
        {
            let projected =
                project_item_for_index(new, &idx.key_schema, base_key_schema, &idx.projection);
            insert_index_row_multi(
                tx,
                &idx_table,
                new,
                &projected,
                &idx.key_schema,
                base_key_schema,
                &idx_sks,
                &base_sks,
            )
            .await?;
        }
    }
    Ok(())
}

/// Enqueue async GSI propagation for a write, **one `gsi_pending` row per
/// async index**.
///
/// For every GSI (LSIs are always synchronous) whose effective propagation
/// delay is non-zero, a self-describing row is inserted carrying that single
/// index's definition and its own effective delay (see [`enqueue_gsi_pending`],
/// which applies per-index jitter and the partition-monotonic `ready_at`
/// clamp). Splitting per index — rather than bundling every index into one row
/// with one `ready_at` — is what lets each GSI honor its own
/// `propagation_delay_ms` and propagate independently.
///
/// Returns the number of rows enqueued, so callers can skip waking the workers
/// when nothing was queued. Must be called inside the base write's transaction
/// so the pending rows commit atomically with the item mutation.
pub(crate) async fn enqueue_async_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    indexes: &[IndexMeta],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    system_default_delay: u64,
) -> Result<usize, StorageError> {
    let mut enqueued = 0usize;
    for idx in indexes {
        if idx.index_type == "LSI" {
            continue; // LSIs are always synchronous.
        }
        let delay = effective_delay(idx, system_default_delay);
        if delay == 0 {
            continue; // Synchronous GSI — handled in-transaction by sync_indexes.
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
        enqueue_gsi_pending(
            tx,
            &key_info.table_id,
            old_item,
            new_item,
            delay,
            &crate::gsi_queue::PendingApplyContext::Gsi(context),
        )
        .await?;
        enqueued += 1;
    }
    Ok(enqueued)
}

/// Enqueue one propagation row for any index kind.
///
/// Thin wrapper so the vector maintenance path does not have to reach into the
/// queue module's naming, which still says "gsi" for historical reasons.
pub(crate) async fn enqueue_pending_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
    context: &crate::gsi_queue::PendingApplyContext,
) -> Result<(), StorageError> {
    enqueue_gsi_pending(tx, table_id, old_item, new_item, delay_ms, context).await
}

/// Delete a row from an index table using base table key columns.
pub(crate) async fn delete_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    idx_table: &str,
    item: &Item,
    base_ks: &[KeySchemaElement],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let mut where_parts = vec!["base_pk = $1".to_owned()];
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        let param_idx = (i as u32) + 2;
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        where_parts.push(format!("{col} = ${param_idx}"));
    }

    let sql = format!(
        "DELETE FROM {idx_table} WHERE {}",
        where_parts.join(" AND ")
    );
    let mut query = sqlx::query(&sql).bind(base_pk_text);

    for &(sk_name, sk_type) in base_sks {
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            query = match sk {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(n),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    query.execute(&mut **tx).await.map_err(db_error)?;
    Ok(())
}

/// Insert a row into an index table with multi-part key support.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        serde_json::to_value(projected).map_err(|e| StorageError::Internal(e.to_string()))?;

    // Build column list dynamically
    let mut cols = vec!["pk".to_owned()];
    for (i, &(_, sk_type)) in idx_sks.iter().enumerate() {
        cols.push(sk_column_n(i, sk_type));
    }
    cols.push("base_pk".to_owned());
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        cols.push(col);
    }
    cols.push("item_data".to_owned());

    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {idx_table} ({}) VALUES ({})",
        cols.join(", "),
        placeholders.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(idx_pk_text);

    // Bind index SK values
    for &(sk_name, sk_type) in idx_sks {
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            query = match sk {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(n),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    // Bind base PK
    query = query.bind(base_pk_text);

    // Bind base SK values
    for &(sk_name, sk_type) in base_sks {
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            query = match sk {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(n),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    // Bind item_data
    query = query.bind(item_json);

    query.execute(&mut **tx).await.map_err(db_error)?;
    Ok(())
}
