// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the `PostgreSQL` backend.

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, KeyType, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column, sk_info};

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, pk_hash, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

impl PostgresEngine {
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

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        // UpdateItem always needs a transaction (read-modify-write)
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch indexes for GSI/LSI updates (D-4: sync + async split).
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        let sys_delay = if indexes.is_empty() {
            0
        } else {
            self.gsi_default_delay_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        };

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
                "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 FOR UPDATE"
            );
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&select_sql, pk_text.as_ref(), &sk, &mut *tx)?;
            row.map(|(v,)| v)
        } else {
            let select_sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                .bind(pk_text.as_ref())
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

        // Save pre-mutation item for index sync and stream capture.
        let pre_mutation_item = if (!indexes.is_empty() || stream.is_some()) && old_json.is_some() {
            Some(item.clone())
        } else {
            None
        };

        let old_item = if return_old { Some(item.clone()) } else { None };

        // Evaluate condition against the existing item (empty if non-existent).
        // DynamoDB treats a non-existent item as having no attributes at all.
        let condition_item = if old_json.is_some() {
            &item
        } else {
            &std::collections::BTreeMap::new()
        };
        match check_condition(condition, condition_item, maps) {
            Ok(()) => {}
            Err(StorageError::ConditionFailed(_)) => {
                if old_json.is_some() {
                    return Err(StorageError::ConditionFailed(Some(item)));
                }
                return Err(StorageError::ConditionFailed(None));
            }
            Err(e) => return Err(e),
        }

        // Apply update actions
        expression::apply_update(actions, &mut item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        // Validate post-update item size (400 KB limit)
        validation::validate_item_size(&item, self.max_item_size_bytes)
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
            if old_json.is_some() {
                // Row existed — update in place.
                let update_sql = format!(
                    "UPDATE {ddb_table} SET item_data = $3 WHERE pk = $1 AND {sk_col} = $2"
                );
                bind_sk_execute!(&update_sql, pk_text.as_ref(), &sk, &item_json, &mut *tx)?;
            } else {
                // Row didn't exist (upsert) — atomic insert, fail if someone beat us.
                let insert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                     ON CONFLICT (pk, {sk_col}) DO NOTHING"
                );
                let result =
                    bind_sk_execute!(&insert_sql, pk_text.as_ref(), &sk, &item_json, &mut *tx)?;
                if result.rows_affected() == 0 {
                    // Another transaction inserted between our SELECT and INSERT.
                    // Fetch the winner to return with ConditionFailed.
                    let winner_sql = format!(
                        "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2"
                    );
                    let winner: Option<(serde_json::Value,)> =
                        bind_sk_fetch_optional!(&winner_sql, pk_text.as_ref(), &sk, &mut *tx)?;
                    let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                    return Err(StorageError::ConditionFailed(winner_item));
                }
            }
        } else if old_json.is_some() {
            let update_sql = format!("UPDATE {ddb_table} SET item_data = $2 WHERE pk = $1");
            sqlx::query(&update_sql)
                .bind(pk_text.as_ref())
                .bind(&item_json)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            let insert_sql = format!(
                "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                 ON CONFLICT (pk) DO NOTHING"
            );
            let result = sqlx::query(&insert_sql)
                .bind(pk_text.as_ref())
                .bind(&item_json)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if result.rows_affected() == 0 {
                // Another transaction inserted between our SELECT and INSERT.
                // Fetch the winner to return with ConditionFailed.
                let winner_sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
                let winner: Option<(serde_json::Value,)> = sqlx::query_as(&winner_sql)
                    .bind(pk_text.as_ref())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                return Err(StorageError::ConditionFailed(winner_item));
            }
        }

        // Sync GSI/LSI update within transaction (D-4).
        if !indexes.is_empty() {
            sync_indexes(
                &mut tx,
                &key_info.table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                pre_mutation_item.as_ref(),
                Some(&item),
                sys_delay,
            )
            .await?;
        }

        // Write stream record atomically within the transaction.
        if let Some(capture) = stream {
            write_stream_record_in_tx(
                &mut tx,
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

        // Enqueue async GSI updates after commit (D-4).
        if let Some(ref q) = self.gsi_queue {
            enqueue_async_indexes(
                q,
                pk_hash(pk_text.as_ref()),
                &key_info.account_id,
                &key_info.table_name,
                &key_info.table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                pre_mutation_item.as_ref(),
                Some(&item),
                sys_delay,
            )
            .await;
        }

        Ok((old_item, new_item))
    }
}
