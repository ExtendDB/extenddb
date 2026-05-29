// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_item` implementation for the `TiDB` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};

use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::TidbEngine;

impl TidbEngine {
    /// Implementation of `DataEngine::delete_item`.
    pub(crate) async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let needs_tx = condition.is_some() || return_old || stream.is_some();

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if needs_tx {
                let select_sql = format!(
                    "SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE"
                );
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> =
                    bind_sk_fetch_optional!(&select_sql, pk_text.as_ref(), &sk, &mut *tx)?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
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
                    // Nothing to delete
                    return Ok(None);
                }

                // Delete the row
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(s.as_bytes().to_vec())
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(n)
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(b)
                            .execute(&mut *tx)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

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
                        None,
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(s.as_bytes().to_vec())
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(n)
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(b)
                            .execute(&self.data_pool)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        } else {
            // PK-only table
            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                    .bind(pk_text.as_ref())
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
                } else {
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    return Ok(None);
                }

                sqlx::query(&delete_sql)
                    .bind(pk_text.as_ref())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

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
                        None,
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");
                sqlx::query(&delete_sql)
                    .bind(pk_text.as_ref())
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }
}
