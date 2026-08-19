// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` implementations for the `PostgreSQL` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, pk_to_text, sk_column, sk_info};

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

impl PostgresEngine {
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

        let pk_text = composite_pk_to_text(&item, &key_info.key_schema)?;

        let item_json =
            serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch indexes for GSI/LSI updates (D-4: sync + async split).
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;

        // Index key attributes present in the item must match their declared
        // scalar type and be non-empty, matching real DynamoDB. This is up-front
        // input validation (a top-level ValidationException), so it runs before
        // any write work.
        if !indexes.is_empty() {
            let index_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = indexes
                .iter()
                .map(|idx| extenddb_core::validation::IndexKeyRef {
                    index_name: &idx.index_name,
                    key_schema: &idx.key_schema,
                })
                .collect();
            extenddb_core::validation::validate_index_keys(
                &item,
                &index_refs,
                &key_info.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        let sys_delay = if indexes.is_empty() {
            0
        } else {
            self.index_propagation_delay().await
        };

        // When there's a condition, return_old, indexes, or stream capture, we need a transaction
        let needs_tx = condition.is_some() || return_old || !indexes.is_empty() || stream.is_some();

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if needs_tx {
                let select_sql = format!(
                    "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 FOR UPDATE"
                );

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> =
                    bind_sk_fetch_optional!(&select_sql, pk_text.as_str(), &sk, &mut *tx)?;

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
                        "UPDATE {ddb_table} SET item_data = $3 WHERE pk = $1 AND {sk_col} = $2"
                    );
                    bind_sk_execute!(&update_sql, pk_text.as_str(), &sk, &item_json, &mut *tx)?;
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
                    // Condition passed against empty — atomic insert, fail if someone beat us.
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                         ON CONFLICT (pk, {sk_col}) DO NOTHING"
                    );
                    let result =
                        bind_sk_execute!(&insert_sql, pk_text.as_str(), &sk, &item_json, &mut *tx)?;
                    if result.rows_affected() == 0 {
                        // Another transaction inserted between our SELECT and INSERT.
                        // Fetch the winner to return with ConditionFailed.
                        let winner: Option<(serde_json::Value,)> =
                            bind_sk_fetch_optional!(&select_sql, pk_text.as_str(), &sk, &mut *tx)?;
                        let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                        return Err(StorageError::ConditionFailed(winner_item));
                    }
                }

                // Sync GSI/LSI update within transaction (D-4).
                let old_item_for_idx = if indexes.is_empty() {
                    None
                } else {
                    let oi = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        oi.as_ref(),
                        Some(&item),
                        sys_delay,
                    )
                    .await?;
                    oi
                };

                // Write stream record atomically within the transaction.
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }
                // Persist async GSI work inside the same transaction — one row
                // per async index, each honoring its own propagation delay.
                let async_enqueued = enqueue_async_indexes(
                    &mut tx,
                    key_info,
                    &indexes,
                    old_item_for_idx.as_ref(),
                    Some(&item),
                    sys_delay,
                )
                .await?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if async_enqueued > 0
                    && let Some(ref q) = self.gsi_queue
                {
                    q.notify_workers();
                }

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                     ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data"
                );
                bind_sk_execute!(
                    &upsert_sql,
                    pk_text.as_str(),
                    &sk,
                    &item_json,
                    &self.data_pool
                )?;
                Ok(None)
            }
        } else {
            // No sort key — PK-only table
            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                    .bind(pk_text.as_str())
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
                    let update_sql = format!("UPDATE {ddb_table} SET item_data = $2 WHERE pk = $1");
                    sqlx::query(&update_sql)
                        .bind(pk_text.as_str())
                        .bind(&item_json)
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
                    // Condition passed against empty — atomic insert, fail if someone beat us.
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                         ON CONFLICT (pk) DO NOTHING"
                    );
                    let result = sqlx::query(&insert_sql)
                        .bind(pk_text.as_str())
                        .bind(&item_json)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if result.rows_affected() == 0 {
                        // Another transaction inserted between our SELECT and INSERT.
                        let winner: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                            .bind(pk_text.as_str())
                            .fetch_optional(&mut *tx)
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                        return Err(StorageError::ConditionFailed(winner_item));
                    }
                }

                // Sync GSI/LSI update within transaction (D-4).
                let old_item_for_idx = if indexes.is_empty() {
                    None
                } else {
                    let oi = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        oi.as_ref(),
                        Some(&item),
                        sys_delay,
                    )
                    .await?;
                    oi
                };

                // Write stream record atomically within the transaction.
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }
                // Persist async GSI work inside the same transaction — one row
                // per async index, each honoring its own propagation delay.
                let async_enqueued = enqueue_async_indexes(
                    &mut tx,
                    key_info,
                    &indexes,
                    old_item_for_idx.as_ref(),
                    Some(&item),
                    sys_delay,
                )
                .await?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if async_enqueued > 0
                    && let Some(ref q) = self.gsi_queue
                {
                    q.notify_workers();
                }

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                     ON CONFLICT (pk) DO UPDATE SET item_data = EXCLUDED.item_data"
                );
                sqlx::query(&upsert_sql)
                    .bind(pk_text.as_str())
                    .bind(&item_json)
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }

    /// Implementation of `DataEngine::get_item`.
    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let json_opt = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &self.data_pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk_text.as_ref())
                .fetch_optional(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}
