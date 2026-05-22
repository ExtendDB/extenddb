// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` implementations for the SQLite backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, pk_to_text, sk_column, sk_info};

use super::index::{fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::engine::SqliteEngine;

impl SqliteEngine {
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

        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        let needs_tx =
            condition.is_some() || return_old || !indexes.is_empty() || stream.is_some();

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");

                let mut tx = self
                    .pool
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
                    let update_sql = format!(
                        "UPDATE {ddb_table} SET item_data = ? WHERE pk = ? AND {sk_col} = ?"
                    );
                    match &sk {
                        extenddb_storage::util::SortKeyValue::S(s) => {
                            sqlx::query(&update_sql)
                                .bind(&item_json)
                                .bind(pk_text.as_str())
                                .bind(s)
                                .execute(&mut *tx)
                                .await
                        }
                        extenddb_storage::util::SortKeyValue::N(n) => {
                            sqlx::query(&update_sql)
                                .bind(&item_json)
                                .bind(pk_text.as_str())
                                .bind(super::bigdecimal_to_f64(n))
                                .execute(&mut *tx)
                                .await
                        }
                        extenddb_storage::util::SortKeyValue::B(b) => {
                            sqlx::query(&update_sql)
                                .bind(&item_json)
                                .bind(pk_text.as_str())
                                .bind(b)
                                .execute(&mut *tx)
                                .await
                        }
                    }
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
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
                         ON CONFLICT (pk, {sk_col}) DO NOTHING"
                    );
                    let result =
                        bind_sk_execute!(&insert_sql, pk_text.as_str(), &sk, &item_json, &mut *tx)?;
                    if result.rows_affected() == 0 {
                        let winner: Option<(serde_json::Value,)> =
                            bind_sk_fetch_optional!(&select_sql, pk_text.as_str(), &sk, &mut *tx)?;
                        let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                        return Err(StorageError::ConditionFailed(winner_item));
                    }
                }

                if !indexes.is_empty() {
                    let old_item = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }

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
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
                     ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data"
                );
                bind_sk_execute!(
                    &upsert_sql,
                    pk_text.as_str(),
                    &sk,
                    &item_json,
                    &self.pool
                )?;
                Ok(None)
            }
        } else {
            // PK-only table
            if needs_tx {
                let select_sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");

                let mut tx = self
                    .pool
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
                    let update_sql = format!("UPDATE {ddb_table} SET item_data = ? WHERE pk = ?");
                    sqlx::query(&update_sql)
                        .bind(&item_json)
                        .bind(pk_text.as_str())
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
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
                         ON CONFLICT (pk) DO NOTHING"
                    );
                    let result = sqlx::query(&insert_sql)
                        .bind(pk_text.as_str())
                        .bind(&item_json)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if result.rows_affected() == 0 {
                        let winner: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                            .bind(pk_text.as_str())
                            .fetch_optional(&mut *tx)
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let winner_item = winner.map(|(v,)| json_to_item(v)).transpose()?;
                        return Err(StorageError::ConditionFailed(winner_item));
                    }
                }

                if !indexes.is_empty() {
                    let old_item = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item.as_ref(),
                        Some(&item),
                    )
                    .await?;
                }

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
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let upsert_sql = format!(
                    "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
                     ON CONFLICT (pk) DO UPDATE SET item_data = EXCLUDED.item_data"
                );
                sqlx::query(&upsert_sql)
                    .bind(pk_text.as_str())
                    .bind(&item_json)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }

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
            let sql =
                format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &self.pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk_text.as_ref())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}
