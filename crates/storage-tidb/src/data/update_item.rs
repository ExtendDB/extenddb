// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the `TiDB` backend.

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, KeyType, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, sk_column, sk_info};

use super::index::validate_item_secondary_index_key_constraints;
use super::query::check_condition;
use super::tx_helpers::{
    StreamSequenceAllocator, finalize_stream_records_best_effort, write_stream_record_in_tx,
};
use super::{data_table_name, json_to_item, physical_pk_bytes};
use crate::TidbEngine;
use crate::tidb_util::is_unique_violation;

impl TidbEngine {
    /// Implementation of `DataEngine::update_item`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let pk = physical_pk_bytes(key, &key_info.key_schema)?;

        // UpdateItem always needs a transaction (read-modify-write)
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch existing item
        let old_json = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let select_sql = format!(
                "SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE"
            );
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&select_sql, pk.as_slice(), &sk, &mut *tx)?;
            row.map(|(v,)| v)
        } else {
            let select_sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                .bind(pk.as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        // Build the working item: existing or new with key attributes only (upsert)
        let mut item = if let Some(json) = old_json.clone() {
            json_to_item(json)?
        } else {
            key.clone()
        };
        let item_existed = old_json.is_some();

        // Save pre-mutation item for stream capture.
        let pre_mutation_item = if stream.is_some() && item_existed {
            Some(item.clone())
        } else {
            None
        };

        let old_item = if return_old && item_existed {
            Some(item.clone())
        } else {
            None
        };

        // Evaluate condition against the existing item (empty if non-existent).
        // DynamoDB treats a non-existent item as having no attributes at all.
        let condition_item = if item_existed {
            &item
        } else {
            &std::collections::BTreeMap::new()
        };
        match check_condition(condition, condition_item, maps) {
            Ok(()) => {}
            Err(StorageError::ConditionFailed(_)) => {
                if item_existed {
                    return Err(StorageError::ConditionFailed(Some(item)));
                }
                return Err(StorageError::ConditionFailed(None));
            }
            Err(e) => return Err(e),
        }

        // Apply update actions
        expression::apply_update(actions, &mut item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        validate_item_secondary_index_key_constraints(
            &key_info.table_id,
            &item,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            &self.limits,
            &self.pool,
        )
        .await?;

        // Validate post-update item size (400 KB limit)
        validation::validate_item_size(&item, self.limits.max_item_size_bytes)
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        let new_item = if return_new { Some(item.clone()) } else { None };

        // Write the updated item back
        let item_json =
            serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;

        if let Some((_, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            let sk_name_ref = key_info
                .key_schema
                .iter()
                .find(|ks| ks.key_type == KeyType::Range)
                .map(|ks| ks.attribute_name.as_str())
                .ok_or_else(|| StorageError::Internal("missing sort key schema".to_owned()))?;
            let sk_value = key
                .get(sk_name_ref)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            if item_existed {
                // Row existed — update in place.
                let update_sql =
                    format!("UPDATE {ddb_table} SET item_data = ? WHERE pk = ? AND {sk_col} = ?");
                bind_sk_update_execute!(&update_sql, &item_json, pk.as_slice(), &sk, &mut *tx)?;
            } else {
                // Row didn't exist. In TiDB pessimistic mode, the point
                // SELECT FOR UPDATE above locks this primary key even when
                // absent; duplicate-key remains the authoritative race signal.
                let insert_sql =
                    format!("INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?)");
                let insert_result =
                    bind_sk_execute_raw!(&insert_sql, pk.as_slice(), &sk, &item_json, &mut *tx);
                if let Err(err) = insert_result {
                    if is_unique_violation(&err) {
                        let winner_sql = format!(
                            "SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE"
                        );
                        let winner: Option<(serde_json::Value,)> =
                            bind_sk_fetch_optional!(&winner_sql, pk.as_slice(), &sk, &mut *tx)?;
                        let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                        return Err(StorageError::ConditionFailed(winner_item));
                    }
                    return Err(StorageError::Internal(err.to_string()));
                }
            }
        } else if item_existed {
            let update_sql = format!("UPDATE {ddb_table} SET item_data = ? WHERE pk = ?");
            sqlx::query(&update_sql)
                .bind(&item_json)
                .bind(pk.as_slice())
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            let insert_sql = format!("INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?)");
            let insert_result = sqlx::query(&insert_sql)
                .bind(pk.as_slice())
                .bind(&item_json)
                .execute(&mut *tx)
                .await;
            if let Err(err) = insert_result {
                if is_unique_violation(&err) {
                    let winner_sql =
                        format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
                    let winner: Option<(serde_json::Value,)> = sqlx::query_as(&winner_sql)
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
            write_stream_record_in_tx(
                &mut tx,
                &mut sequence_allocator,
                key_info,
                capture,
                pre_mutation_item.as_ref(),
                Some(&item),
            )
            .await?;
        }
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        finalize_stream_records_best_effort(
            &self.data_pool,
            "update_item",
            sequence_allocator.pending_records(),
        )
        .await;

        Ok((old_item, new_item))
    }
}
