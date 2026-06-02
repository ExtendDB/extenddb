// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` implementations for the `TiDB` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, StreamEventName, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, sk_column, sk_info};

use super::index::validate_item_secondary_index_key_constraints;
use super::query::{bind_sk_value, check_condition};
use super::tx_helpers::{
    StreamSequenceAllocator, finalize_stream_records_best_effort,
    put_prepared_item_without_old_item_in_tx, stream_capture_needs_old_item,
    write_stream_record_for_event_in_tx, write_stream_record_in_tx,
};
use super::{data_table_name, json_to_item, physical_pk_bytes};
use crate::TidbEngine;
use crate::tidb_util::is_unique_violation;

impl TidbEngine {
    /// Implementation of `DataEngine::put_item`.
    pub(crate) async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);

        let pk = physical_pk_bytes(&item, &key_info.key_schema)?;

        let item_json =
            serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;

        validate_item_secondary_index_key_constraints(
            &key_info.table_id,
            &item,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            &self.limits,
            &self.pool,
        )
        .await?;

        // When there's a condition, return_old, or stream capture, we need a transaction.
        let needs_tx = condition.is_some() || return_old || stream.is_some();

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if let Some(capture) =
                put_stream_capture_without_old_item(return_old, condition, stream)
            {
                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let event = put_prepared_item_without_old_item_in_tx(
                    &mut tx, key_info, &item, &pk, &item_json,
                )
                .await?;

                commit_put_stream_without_old_item(
                    &self.data_pool,
                    tx,
                    key_info,
                    capture,
                    event,
                    &item,
                )
                .await?;
                return Ok(None);
            }

            if needs_tx {
                let select_sql = format!(
                    "SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE"
                );

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> =
                    bind_sk_fetch_optional!(&select_sql, pk.as_slice(), &sk, &mut *tx)?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                    // Row exists, condition passed — update in place.
                    let update_sql = format!(
                        "UPDATE {ddb_table} SET item_data = ? WHERE pk = ? AND {sk_col} = ?"
                    );
                    bind_sk_update_execute!(&update_sql, &item_json, pk.as_slice(), &sk, &mut *tx)?;
                } else {
                    // No existing item — condition checks against empty item
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    // Condition passed against empty. In pessimistic mode,
                    // the preceding point SELECT FOR UPDATE locks this
                    // primary key even when absent. A duplicate here is still
                    // handled as the authoritative race outcome.
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?)"
                    );
                    let insert_result =
                        bind_sk_execute_raw!(&insert_sql, pk.as_slice(), &sk, &item_json, &mut *tx);
                    if let Err(err) = insert_result {
                        if is_unique_violation(&err) {
                            let winner: Option<(serde_json::Value,)> =
                                bind_sk_fetch_optional!(&select_sql, pk.as_slice(), &sk, &mut *tx)?;
                            let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                            return Err(StorageError::ConditionFailed(winner_item));
                        }
                        return Err(StorageError::Internal(err.to_string()));
                    }
                }

                // Write stream record atomically within the transaction.
                let mut sequence_allocator = StreamSequenceAllocator::default();
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                finalize_stream_records_best_effort(
                    &self.data_pool,
                    "put_item",
                    sequence_allocator.pending_records(),
                )
                .await;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
                     ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
                );
                bind_sk_execute!(&upsert_sql, pk.as_slice(), &sk, &item_json, &self.data_pool)?;
                Ok(None)
            }
        } else {
            // No sort key — PK-only table
            if let Some(capture) =
                put_stream_capture_without_old_item(return_old, condition, stream)
            {
                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let event = put_prepared_item_without_old_item_in_tx(
                    &mut tx, key_info, &item, &pk, &item_json,
                )
                .await?;

                commit_put_stream_without_old_item(
                    &self.data_pool,
                    tx,
                    key_info,
                    capture,
                    event,
                    &item,
                )
                .await?;
                return Ok(None);
            }

            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                    .bind(pk.as_slice())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                    // Row exists, condition passed — update in place.
                    let update_sql = format!("UPDATE {ddb_table} SET item_data = ? WHERE pk = ?");
                    sqlx::query(&update_sql)
                        .bind(&item_json)
                        .bind(pk.as_slice())
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                } else {
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    // Condition passed against empty. The preceding point
                    // SELECT FOR UPDATE locks the primary key in TiDB
                    // pessimistic mode; duplicate-key remains the final race
                    // signal if another writer already committed.
                    let insert_sql =
                        format!("INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?)");
                    let insert_result = sqlx::query(&insert_sql)
                        .bind(pk.as_slice())
                        .bind(&item_json)
                        .execute(&mut *tx)
                        .await;
                    if let Err(err) = insert_result {
                        if is_unique_violation(&err) {
                            let winner: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                                .bind(pk.as_slice())
                                .fetch_optional(&mut *tx)
                                .await
                                .map_err(|e| StorageError::Internal(e.to_string()))?;
                            let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                            return Err(StorageError::ConditionFailed(winner_item));
                        }
                        return Err(StorageError::Internal(err.to_string()));
                    }
                }

                // Write stream record atomically within the transaction.
                let mut sequence_allocator = StreamSequenceAllocator::default();
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                finalize_stream_records_best_effort(
                    &self.data_pool,
                    "put_item",
                    sequence_allocator.pending_records(),
                )
                .await;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
                     ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
                );
                sqlx::query(&upsert_sql)
                    .bind(pk.as_slice())
                    .bind(&item_json)
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }

    /// Implementation of `DataEngine::batch_get_items`.
    pub(crate) async fn batch_get_items_impl(
        &self,
        key_info: &TableKeyInfo,
        keys: &[Item],
        consistent_read: bool,
    ) -> Result<Vec<Item>, StorageError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let ddb_table = data_table_name(&key_info.table_id);
        let pool = self.data_read_pool(consistent_read);
        let rows: Vec<(serde_json::Value,)> = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_col = sk_column(sk_type);
            let sql = batch_get_pk_sk_sql(&ddb_table, sk_col, keys.len());
            let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
            for key in keys {
                query = query.bind(physical_pk_bytes(key, &key_info.key_schema)?);
                let sk_value = key.get(sk_name).ok_or_else(|| {
                    StorageError::Internal(format!("missing sort key attribute {sk_name}"))
                })?;
                let sk = parse_sk(sk_value, sk_type)?;
                query = bind_sk_value(query, &sk);
            }
            query
                .fetch_all(pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
        } else {
            let sql = batch_get_pk_sql(&ddb_table, keys.len());
            let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
            for key in keys {
                query = query.bind(physical_pk_bytes(key, &key_info.key_schema)?);
            }
            query
                .fetch_all(pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
        };

        rows.into_iter().map(|(json,)| json_to_item(json)).collect()
    }

    /// Implementation of `DataEngine::get_item`.
    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        consistent_read: bool,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let pk = physical_pk_bytes(key, &key_info.key_schema)?;
        let pool = self.data_read_pool(consistent_read);

        let json_opt = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk.as_slice(), &sk, pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk.as_slice())
                .fetch_optional(pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}

fn repeat_tuple_placeholders(count: usize, width: usize) -> String {
    let tuple = if width == 1 {
        "?".to_owned()
    } else {
        format!(
            "({})",
            std::iter::repeat_n("?", width)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    std::iter::repeat_n(tuple, count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn batch_get_pk_sql(table: &str, key_count: usize) -> String {
    format!(
        "SELECT item_data FROM {table} WHERE pk IN ({})",
        repeat_tuple_placeholders(key_count, 1)
    )
}

fn batch_get_pk_sk_sql(table: &str, sk_col: &str, key_count: usize) -> String {
    format!(
        "SELECT item_data FROM {table} WHERE (pk, {sk_col}) IN ({})",
        repeat_tuple_placeholders(key_count, 2)
    )
}

fn put_stream_capture_without_old_item<'a>(
    return_old: bool,
    condition: Option<&Expr>,
    stream: Option<&'a StreamCapture>,
) -> Option<&'a StreamCapture> {
    match stream {
        Some(capture)
            if !return_old && condition.is_none() && !stream_capture_needs_old_item(capture) =>
        {
            Some(capture)
        }
        _ => None,
    }
}

async fn commit_put_stream_without_old_item(
    pool: &sqlx::MySqlPool,
    mut tx: sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    event: StreamEventName,
    item: &Item,
) -> Result<(), StorageError> {
    let mut sequence_allocator = StreamSequenceAllocator::default();
    write_stream_record_for_event_in_tx(
        &mut tx,
        &mut sequence_allocator,
        key_info,
        capture,
        event,
        item,
        None,
        Some(item),
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    finalize_stream_records_best_effort(pool, "put_item", sequence_allocator.pending_records())
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_core::expression::Expr;
    use extenddb_core::types::StreamViewType;
    use extenddb_storage::StreamCapture;

    use super::{
        batch_get_pk_sk_sql, batch_get_pk_sql, put_stream_capture_without_old_item,
        repeat_tuple_placeholders,
    };

    fn capture(view_type: StreamViewType) -> StreamCapture {
        StreamCapture {
            view_type,
            user_identity: None,
            region: Arc::from("us-east-1"),
        }
    }

    fn condition() -> Expr {
        Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![Expr::Path(vec![
                extenddb_core::expression::PathElement::Attribute("pk".to_owned()),
            ])],
        }
    }

    #[test]
    fn stream_put_can_skip_old_item_for_key_or_new_image_views() {
        let keys_only = capture(StreamViewType::KeysOnly);
        let new_image = capture(StreamViewType::NewImage);

        assert!(put_stream_capture_without_old_item(false, None, Some(&keys_only)).is_some());
        assert!(put_stream_capture_without_old_item(false, None, Some(&new_image)).is_some());
    }

    #[test]
    fn stream_put_keeps_old_item_read_when_result_needs_old_state() {
        let condition = condition();
        let keys_only = capture(StreamViewType::KeysOnly);
        let old_image = capture(StreamViewType::OldImage);
        let both_images = capture(StreamViewType::NewAndOldImages);

        assert!(
            put_stream_capture_without_old_item(false, Some(&condition), Some(&keys_only))
                .is_none()
        );
        assert!(put_stream_capture_without_old_item(true, None, Some(&keys_only)).is_none());
        assert!(put_stream_capture_without_old_item(false, None, Some(&old_image)).is_none());
        assert!(put_stream_capture_without_old_item(false, None, Some(&both_images)).is_none());
    }

    #[test]
    fn batch_get_placeholders_use_native_primary_key_in_shape() {
        assert_eq!(repeat_tuple_placeholders(3, 1), "?, ?, ?");
        assert_eq!(repeat_tuple_placeholders(2, 2), "(?, ?), (?, ?)");
    }

    #[test]
    fn batch_get_sql_uses_primary_key_in_predicates() {
        assert_eq!(
            batch_get_pk_sql("`_ddb_table`", 3),
            "SELECT item_data FROM `_ddb_table` WHERE pk IN (?, ?, ?)"
        );
        assert_eq!(
            batch_get_pk_sk_sql("`_ddb_table`", "sk_n", 2),
            "SELECT item_data FROM `_ddb_table` WHERE (pk, sk_n) IN ((?, ?), (?, ?))"
        );
    }
}
