// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TableEngine` trait implementation for `SqliteEngine`.

use extenddb_core::types::{
    CreateTableInput, DeleteTableInput, DescribeTableInput, IndexInfo, ListTablesInput,
    ListTablesOutput, TableDescription, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::store::SqliteEngine;

impl TableEngine for SqliteEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.create_table_impl(&account_id, input).await })
    }

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.delete_table_impl(&account_id, input).await })
    }

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.build_table_description(&account_id, &input.table_name)
                .await
        })
    }

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let limit = i64::from(input.limit.unwrap_or(100));
            // SQLite's default BINARY collation matches DynamoDB's UTF-8 byte
            // order, so no COLLATE clause is needed. Fetch one extra to detect
            // a continuation.
            let rows: Vec<(String,)> = if let Some(start) = &input.exclusive_start_table_name {
                sqlx::query_as(
                    "SELECT table_name FROM tables WHERE account_id = ? AND table_name > ? \
                     ORDER BY table_name LIMIT ?",
                )
                .bind(&account_id)
                .bind(start)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query_as(
                    "SELECT table_name FROM tables WHERE account_id = ? \
                     ORDER BY table_name LIMIT ?",
                )
                .bind(&account_id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let names: Vec<String> = rows.into_iter().map(|(n,)| n).collect();
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let limit_usize = limit.max(0) as usize;
            if names.len() > limit_usize {
                Ok(ListTablesOutput {
                    last_evaluated_table_name: Some(names[limit_usize - 1].clone()),
                    table_names: names[..limit_usize].to_vec(),
                })
            } else {
                Ok(ListTablesOutput {
                    table_names: names,
                    last_evaluated_table_name: None,
                })
            }
        })
    }

    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.update_table_impl(&account_id, input).await })
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move { self.fetch_table_key_info(&account_id, &table_name).await })
    }

    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let index_name = index_name.to_owned();
        Box::pin(async move {
            self.fetch_index_info(&account_id, &table_name, &index_name)
                .await
        })
    }

    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let table_id = table_id.to_owned();
        let index_name = index_name.to_owned();
        Box::pin(async move {
            self.fetch_index_info_by_table_id(&table_id, &index_name)
                .await
        })
    }
}
