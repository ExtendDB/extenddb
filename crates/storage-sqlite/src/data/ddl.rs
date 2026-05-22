// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in SQLite.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use super::{all_sort_key_info, data_table_name, index_table_name};
use crate::engine::SqliteEngine;

impl SqliteEngine {
    /// Create the per-DynamoDB-table data table in SQLite.
    pub(crate) async fn create_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let sk_infos = all_sort_key_info(key_schema, attr_defs);

        let ddl = if sk_infos.is_empty() {
            format!(
                "CREATE TABLE {ddb_table} (\
                    pk TEXT NOT NULL PRIMARY KEY,\
                    item_data TEXT NOT NULL\
                )"
            )
        } else if sk_infos.len() == 1 {
            let sk_col = sk_column(sk_infos[0].1);
            format!(
                "CREATE TABLE {ddb_table} (\
                    pk TEXT NOT NULL,\
                    sk_s TEXT,\
                    sk_n REAL,\
                    sk_b BLOB,\
                    item_data TEXT NOT NULL,\
                    PRIMARY KEY (pk, {sk_col})\
                )"
            )
        } else {
            let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];
            let mut pk_cols = vec!["pk".to_owned()];
            for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
                let col = sk_column_n(i, sk_type);
                if i == 0 {
                    col_defs.push("sk_s TEXT".to_owned());
                    col_defs.push("sk_n REAL".to_owned());
                    col_defs.push("sk_b BLOB".to_owned());
                } else {
                    let n = i + 1;
                    col_defs.push(format!("sk{n}_s TEXT"));
                    col_defs.push(format!("sk{n}_n REAL"));
                    col_defs.push(format!("sk{n}_b BLOB"));
                }
                pk_cols.push(col);
            }
            col_defs.push("item_data TEXT NOT NULL".to_owned());
            format!(
                "CREATE TABLE {ddb_table} (\n    {},\n    PRIMARY KEY ({})\n)",
                col_defs.join(",\n    "),
                pk_cols.join(", ")
            )
        };

        sqlx::query(&ddl)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Drop the per-DynamoDB-table data table.
    pub(crate) async fn drop_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let ddl = format!("DROP TABLE IF EXISTS {ddb_table}");
        sqlx::query(&ddl)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Create a GSI/LSI data table in SQLite.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_index_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);

        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);
        let idx_sks = all_sort_key_info(index_key_schema, attr_defs);

        let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];

        for (i, &(_, _)) in idx_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("sk_s TEXT".to_owned());
                col_defs.push("sk_n REAL".to_owned());
                col_defs.push("sk_b BLOB".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("sk{n}_s TEXT"));
                col_defs.push(format!("sk{n}_n REAL"));
                col_defs.push(format!("sk{n}_b BLOB"));
            }
        }

        col_defs.push("base_pk TEXT NOT NULL".to_owned());
        for (i, &(_, _)) in base_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("base_sk_s TEXT".to_owned());
                col_defs.push("base_sk_n REAL".to_owned());
                col_defs.push("base_sk_b BLOB".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("base_sk{n}_s TEXT"));
                col_defs.push(format!("base_sk{n}_n REAL"));
                col_defs.push(format!("base_sk{n}_b BLOB"));
            }
        }

        col_defs.push("item_data TEXT NOT NULL".to_owned());

        let mut pk_cols = vec!["pk".to_owned(), "base_pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            let col = if i == 0 {
                format!("base_{}", sk_column(sk_type))
            } else {
                format!("base_{}", sk_column_n(i, sk_type))
            };
            pk_cols.push(col);
        }

        let ddl = format!(
            "CREATE TABLE {idx_table} (\n    {},\n    PRIMARY KEY ({})\n)",
            col_defs.join(",\n    "),
            pk_cols.join(", ")
        );

        sqlx::query(&ddl)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if !idx_sks.is_empty() {
            let mut order_cols = vec!["pk".to_owned()];
            for (i, &(_, sk_type)) in idx_sks.iter().enumerate() {
                order_cols.push(sk_column_n(i, sk_type));
            }
            order_cols.push("base_pk".to_owned());
            for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
                let col = if i == 0 {
                    format!("base_{}", sk_column(sk_type))
                } else {
                    format!("base_{}", sk_column_n(i, sk_type))
                };
                order_cols.push(col);
            }
            let order_idx = format!("CREATE INDEX ON {idx_table} ({})", order_cols.join(", "));
            sqlx::query(&order_idx)
                .execute(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Drop a GSI/LSI data table.
    pub(crate) async fn drop_index_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        index_id: &str,
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);
        let ddl = format!("DROP TABLE IF EXISTS {idx_table}");
        sqlx::query(&ddl)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Fetch key schema and attribute definitions for a table from the catalog.
    pub(crate) async fn fetch_table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let row: Option<(String, String, String, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions, table_status, table_id, \
             stream_specification, \
             (SELECT COUNT(*) FROM indexes WHERE table_id = tables.table_id AND index_type = 'LSI') \
             FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_str, ad_str, status, table_id, stream_spec_str, lsi_count) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }

        let key_schema: Vec<KeySchemaElement> =
            serde_json::from_str(&ks_str).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attribute_definitions: Vec<AttributeDefinition> =
            serde_json::from_str(&ad_str).map_err(|e| StorageError::Internal(e.to_string()))?;

        let stream_specification: Option<StreamSpecification> = stream_spec_str
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: account_id.to_owned(),
            table_id,
            key_schema,
            attribute_definitions,
            has_lsi: lsi_count > 0,
            stream_specification,
        })
    }

    /// Fetch metadata for a secondary index from the catalog.
    pub(crate) async fn fetch_index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_status FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (table_id, status) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }

        self.fetch_index_info_by_table_id(&table_id, index_name)
            .await
    }

    /// Fetch metadata for a secondary index using a known `table_id`.
    pub(crate) async fn fetch_index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        let idx_row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT index_type, index_id, key_schema, projection \
             FROM indexes WHERE table_id = ? AND index_name = ?",
        )
        .bind(table_id)
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (idx_type_str, idx_id, ks_str, proj_str) =
            idx_row.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;

        let index_type = match idx_type_str.as_str() {
            "GSI" => IndexType::Gsi,
            "LSI" => IndexType::Lsi,
            other => {
                return Err(StorageError::Internal(format!(
                    "unknown index type in database: {other}"
                )));
            }
        };

        let key_schema: Vec<KeySchemaElement> =
            serde_json::from_str(&ks_str).map_err(|e| StorageError::Internal(e.to_string()))?;
        let projection: Projection =
            serde_json::from_str(&proj_str).map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(IndexInfo {
            index_name: index_name.to_owned(),
            index_id: idx_id,
            index_type,
            key_schema,
            projection,
        })
    }
}
