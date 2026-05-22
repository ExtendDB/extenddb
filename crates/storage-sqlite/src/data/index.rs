// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI/LSI index operations for the SQLite backend.

use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, Projection, ProjectionType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column, sk_column_n};

use super::{all_sort_key_info, index_table_name};

/// Metadata for a single index, used during write-path GSI/LSI sync.
pub(crate) struct IndexMeta {
    pub(super) index_id: String,
    pub(super) key_schema: Vec<KeySchemaElement>,
    pub(super) projection: Projection,
}

/// Fetch all index metadata for a table from the catalog.
pub(crate) async fn fetch_indexes_for_table(
    table_id: &str,
    pool: &sqlx::SqlitePool,
) -> Result<Vec<IndexMeta>, StorageError> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT index_id, index_type, key_schema, projection FROM indexes WHERE table_id = ?",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    rows.into_iter()
        .map(|(id, _idx_type, ks_str, proj_str)| {
            let key_schema: Vec<KeySchemaElement> = serde_json::from_str(&ks_str)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: Projection = serde_json::from_str(&proj_str)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(IndexMeta {
                index_id: id,
                key_schema,
                projection,
            })
        })
        .collect()
}

/// Project an item according to an index's projection configuration.
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
            for ks in base_ks.iter().chain(index_ks.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            projected
        }
        ProjectionType::Include => {
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

/// Synchronously update all index tables for all indexes.
///
/// In SQLite all indexes are always synchronous (no async propagation queue).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    indexes: &[IndexMeta],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    for idx in indexes {
        let idx_table = index_table_name(&idx.index_id);
        let idx_sks = all_sort_key_info(&idx.key_schema, attr_defs);
        let base_sks = all_sort_key_info(base_key_schema, attr_defs);

        if let Some(old) = old_item {
            if item_has_index_keys(old, &idx.key_schema) {
                delete_index_row_multi(tx, &idx_table, old, base_key_schema, attr_defs, &base_sks)
                    .await?;
            }
        }

        if let Some(new) = new_item {
            if item_has_index_keys(new, &idx.key_schema) {
                let projected =
                    project_item_for_index(new, &idx.key_schema, base_key_schema, &idx.projection);
                insert_index_row_multi(
                    tx,
                    &idx_table,
                    new,
                    &projected,
                    &idx.key_schema,
                    base_key_schema,
                    attr_defs,
                    &idx_sks,
                    &base_sks,
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Delete a row from an index table using base table key columns.
pub(crate) async fn delete_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    idx_table: &str,
    item: &Item,
    base_ks: &[KeySchemaElement],
    _attr_defs: &[AttributeDefinition],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let mut where_parts = vec!["base_pk = ?".to_owned()];
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        where_parts.push(format!("{col} = ?"));
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
                SortKeyValue::N(n) => query.bind(super::bigdecimal_to_f64(&n)),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    query
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Insert a row into an index table with multi-part key support.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    idx_table: &str,
    item: &Item,
    projected: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    _attr_defs: &[AttributeDefinition],
    idx_sks: &[(&str, ScalarAttributeType)],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let idx_pk_text = composite_pk_to_text(item, index_ks)?;
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let item_json =
        serde_json::to_value(projected).map_err(|e| StorageError::Internal(e.to_string()))?;

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

    let placeholders: Vec<&str> = cols.iter().map(|_| "?").collect();
    let sql = format!(
        "INSERT OR REPLACE INTO {idx_table} ({}) VALUES ({})",
        cols.join(", "),
        placeholders.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(idx_pk_text);

    for &(sk_name, sk_type) in idx_sks {
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            query = match sk {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(super::bigdecimal_to_f64(&n)),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    query = query.bind(base_pk_text);

    for &(sk_name, sk_type) in base_sks {
        if let Some(sk_val) = item.get(sk_name) {
            let sk = parse_sk(sk_val, sk_type)?;
            query = match sk {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(super::bigdecimal_to_f64(&n)),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }

    query = query.bind(item_json);

    query
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}
