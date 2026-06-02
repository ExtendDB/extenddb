// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native `BatchWriteItem` support for the `TiDB` backend.

use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, sk_column, sk_info};
use extenddb_storage::{BatchWriteOp, StreamCapture};

use super::index::{item_has_potential_secondary_index_key, validate_item_index_key_constraints};
use super::{data_table_name, physical_pk_bytes, repeat_tuple_placeholders};
use crate::TidbEngine;

pub(super) struct PreparedPut {
    pk: Vec<u8>,
    sk: Option<SortKeyValue>,
    item_json: serde_json::Value,
}

pub(super) struct PreparedDelete {
    pk: Vec<u8>,
    sk: Option<SortKeyValue>,
}

type WriteQuery<'q> = sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>;

impl TidbEngine {
    /// Implementation of `DataEngine::batch_write_items`.
    pub(crate) async fn batch_write_items_impl(
        &self,
        key_info: &TableKeyInfo,
        ops: &[BatchWriteOp<'_>],
        stream: Option<&StreamCapture>,
    ) -> Result<(), StorageError> {
        if ops.is_empty() {
            return Ok(());
        }

        if stream.is_some() {
            return self
                .batch_write_items_with_stream_loop(key_info, ops, stream)
                .await;
        }

        self.validate_batch_write_secondary_index_keys(key_info, ops)?;

        let ddb_table = data_table_name(&key_info.table_id);
        let sk = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for op in ops {
            match op {
                BatchWriteOp::Put(item) => {
                    puts.push(prepare_batch_put(key_info, item, sk)?);
                }
                BatchWriteOp::Delete(key) => {
                    deletes.push(prepare_batch_delete(key_info, key, sk)?);
                }
            }
        }

        if !puts.is_empty() {
            execute_batch_puts(&self.data_pool, &ddb_table, sk.map(|(_, ty)| ty), puts).await?;
        }
        if !deletes.is_empty() {
            execute_batch_deletes(&self.data_pool, &ddb_table, sk.map(|(_, ty)| ty), deletes)
                .await?;
        }

        Ok(())
    }

    async fn batch_write_items_with_stream_loop(
        &self,
        key_info: &TableKeyInfo,
        ops: &[BatchWriteOp<'_>],
        stream: Option<&StreamCapture>,
    ) -> Result<(), StorageError> {
        let maps = extenddb_core::expression::ExpressionMaps::default();
        for op in ops {
            match op {
                BatchWriteOp::Put(item) => {
                    self.put_item_impl(key_info, (*item).clone(), false, None, &maps, stream)
                        .await?;
                }
                BatchWriteOp::Delete(key) => {
                    self.delete_item_impl(key_info, key, false, None, &maps, stream)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn validate_batch_write_secondary_index_keys(
        &self,
        key_info: &TableKeyInfo,
        ops: &[BatchWriteOp<'_>],
    ) -> Result<(), StorageError> {
        if !ops.iter().any(|op| match op {
            BatchWriteOp::Put(item) => {
                item_has_potential_secondary_index_key(item, &key_info.secondary_index_key_schemas)
            }
            BatchWriteOp::Delete(_) => false,
        }) {
            return Ok(());
        }

        for op in ops {
            if let BatchWriteOp::Put(item) = op {
                validate_item_index_key_constraints(
                    item,
                    &key_info.secondary_index_key_schemas,
                    &key_info.attribute_definitions,
                    &self.limits,
                )?;
            }
        }
        Ok(())
    }
}

pub(super) fn prepare_batch_put(
    key_info: &TableKeyInfo,
    item: &Item,
    sk: Option<(&str, ScalarAttributeType)>,
) -> Result<PreparedPut, StorageError> {
    let pk = physical_pk_bytes(item, &key_info.key_schema)?;
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
    let sk = prepare_sort_key(item, sk)?;
    Ok(PreparedPut { pk, sk, item_json })
}

pub(super) fn prepare_batch_delete(
    key_info: &TableKeyInfo,
    key: &Item,
    sk: Option<(&str, ScalarAttributeType)>,
) -> Result<PreparedDelete, StorageError> {
    let pk = physical_pk_bytes(key, &key_info.key_schema)?;
    let sk = prepare_sort_key(key, sk)?;
    Ok(PreparedDelete { pk, sk })
}

fn prepare_sort_key(
    item: &Item,
    sk: Option<(&str, ScalarAttributeType)>,
) -> Result<Option<SortKeyValue>, StorageError> {
    let Some((sk_name, sk_type)) = sk else {
        return Ok(None);
    };
    let sk_value = item
        .get(sk_name)
        .ok_or_else(|| StorageError::Internal(format!("missing sort key attribute {sk_name}")))?;
    parse_sk(sk_value, sk_type).map(Some)
}

pub(super) async fn execute_batch_puts<'e, E>(
    executor: E,
    table: &str,
    sk_type: Option<ScalarAttributeType>,
    puts: Vec<PreparedPut>,
) -> Result<(), StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::MySql>,
{
    let sql = batch_put_sql(table, sk_type.map(sk_column), puts.len());
    let mut query = sqlx::query(&sql);
    for put in puts {
        query = query.bind(put.pk);
        if let Some(sk) = put.sk {
            query = bind_sort_key(query, sk);
        }
        query = query.bind(put.item_json);
    }
    query
        .execute(executor)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

pub(super) async fn execute_batch_deletes<'e, E>(
    executor: E,
    table: &str,
    sk_type: Option<ScalarAttributeType>,
    deletes: Vec<PreparedDelete>,
) -> Result<(), StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::MySql>,
{
    let sql = batch_delete_sql(table, sk_type.map(sk_column), deletes.len());
    let mut query = sqlx::query(&sql);
    for delete in deletes {
        query = query.bind(delete.pk);
        if let Some(sk) = delete.sk {
            query = bind_sort_key(query, sk);
        }
    }
    query
        .execute(executor)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

fn bind_sort_key<'q>(query: WriteQuery<'q>, sk: SortKeyValue) -> WriteQuery<'q> {
    match sk {
        SortKeyValue::S(s) => query.bind(s.into_bytes()),
        SortKeyValue::N(n) => query.bind(n),
        SortKeyValue::B(b) => query.bind(b),
    }
}

fn batch_put_sql(table: &str, sk_col: Option<&str>, row_count: usize) -> String {
    if let Some(sk_col) = sk_col {
        format!(
            "INSERT INTO {table} (pk, {sk_col}, item_data) VALUES {} \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)",
            repeat_tuple_placeholders(row_count, 3)
        )
    } else {
        format!(
            "INSERT INTO {table} (pk, item_data) VALUES {} \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)",
            repeat_tuple_placeholders(row_count, 2)
        )
    }
}

fn batch_delete_sql(table: &str, sk_col: Option<&str>, row_count: usize) -> String {
    if let Some(sk_col) = sk_col {
        format!(
            "DELETE FROM {table} WHERE (pk, {sk_col}) IN ({})",
            repeat_tuple_placeholders(row_count, 2)
        )
    } else {
        format!(
            "DELETE FROM {table} WHERE pk IN ({})",
            repeat_tuple_placeholders(row_count, 1)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{batch_delete_sql, batch_put_sql};

    #[test]
    fn batch_write_put_sql_uses_one_native_multi_row_upsert() {
        assert_eq!(
            batch_put_sql("`_ddb_table`", None, 2),
            "INSERT INTO `_ddb_table` (pk, item_data) VALUES (?, ?), (?, ?) \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
        assert_eq!(
            batch_put_sql("`_ddb_table`", Some("sk_s"), 2),
            "INSERT INTO `_ddb_table` (pk, sk_s, item_data) VALUES (?, ?, ?), (?, ?, ?) \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
    }

    #[test]
    fn batch_write_delete_sql_uses_native_primary_key_tuple_predicates() {
        assert_eq!(
            batch_delete_sql("`_ddb_table`", None, 3),
            "DELETE FROM `_ddb_table` WHERE pk IN (?, ?, ?)"
        );
        assert_eq!(
            batch_delete_sql("`_ddb_table`", Some("sk_b"), 2),
            "DELETE FROM `_ddb_table` WHERE (pk, sk_b) IN ((?, ?), (?, ?))"
        );
    }
}
