// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL for per-DynamoDB-table and per-index data tables, plus catalog fetches
//! of `TableKeyInfo` / `IndexInfo`.
//!
//! Sort-key columns follow design decision D2: `sk_s`/`base_sk_s` are `TEXT`,
//! `sk_n`/`base_sk_n` are `TEXT` (order-preserving numeric encoding, never
//! `REAL`), `sk_b`/`base_sk_b` are `BLOB`.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::sk_column_n;

use super::{all_sort_key_info, data_table_name, index_table_name};
use crate::store::SqliteEngine;

/// SQLite column type for the Nth sort-key position and scalar type (D2).
fn sk_col_defs(index: usize) -> [String; 3] {
    if index == 0 {
        [
            "sk_s TEXT".to_owned(),
            "sk_n TEXT".to_owned(),
            "sk_b BLOB".to_owned(),
        ]
    } else {
        let n = index + 1;
        [
            format!("sk{n}_s TEXT"),
            format!("sk{n}_n TEXT"),
            format!("sk{n}_b BLOB"),
        ]
    }
}

/// SQLite column type for the Nth base-table sort-key position (D2).
fn base_sk_col_defs(index: usize) -> [String; 3] {
    if index == 0 {
        [
            "base_sk_s TEXT".to_owned(),
            "base_sk_n TEXT".to_owned(),
            "base_sk_b BLOB".to_owned(),
        ]
    } else {
        let n = index + 1;
        [
            format!("base_sk{n}_s TEXT"),
            format!("base_sk{n}_n TEXT"),
            format!("base_sk{n}_b BLOB"),
        ]
    }
}

impl SqliteEngine {
    /// Create the per-DynamoDB-table data table.
    ///
    /// # Safety (SQL injection)
    /// `table_id` is a server-generated UUID; column names are constants. No
    /// user input is interpolated into the DDL.
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
                "CREATE TABLE {ddb_table} (pk TEXT NOT NULL PRIMARY KEY, item_data TEXT NOT NULL)"
            )
        } else {
            let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];
            let mut pk_cols = vec!["pk".to_owned()];
            for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
                col_defs.extend(sk_col_defs(i));
                pk_cols.push(sk_column_n(i, sk_type));
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
        sqlx::query(&format!("DROP TABLE IF EXISTS {ddb_table}"))
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Create a GSI/LSI data table. GSI keys are not unique, so the primary key
    /// is `(pk, sk*, base_pk, base_sk*)` to keep one row per base item, and an
    /// ordering index covers `(pk, sk*, base_pk, base_sk*)`.
    pub(crate) async fn create_index_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);
        let idx_sks = all_sort_key_info(index_key_schema, attr_defs);
        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);

        let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];
        for i in 0..idx_sks.len() {
            col_defs.extend(sk_col_defs(i));
        }
        col_defs.push("base_pk TEXT NOT NULL".to_owned());
        for i in 0..base_sks.len() {
            col_defs.extend(base_sk_col_defs(i));
        }
        col_defs.push("item_data TEXT NOT NULL".to_owned());

        let mut pk_cols = vec!["pk".to_owned(), "base_pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            pk_cols.push(format!("base_{}", sk_column_n(i, sk_type)));
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
                order_cols.push(format!("base_{}", sk_column_n(i, sk_type)));
            }
            let order_idx = format!(
                "CREATE INDEX \"idx_order_{index_id}\" ON {idx_table} ({})",
                order_cols.join(", ")
            );
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
        sqlx::query(&format!("DROP TABLE IF EXISTS {idx_table}"))
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Fetch `TableKeyInfo` for an ACTIVE table from the catalog.
    pub(crate) async fn fetch_table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let row: Option<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions, table_status, table_id, \
             stream_specification \
             FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_str, ad_str, status, table_id, stream_spec_str) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        match status.as_str() {
            "ACTIVE" => {}
            // A table still being created, or being deleted, is not usable for
            // data-plane operations; DynamoDB reports it as not found (not
            // in-use). UPDATING keeps the not-active classification.
            "CREATING" | "DELETING" => {
                return Err(StorageError::TableNotFound(table_name.to_owned()));
            }
            _ => return Err(StorageError::TableNotActive(table_name.to_owned())),
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

        // Full secondary-index metadata rides on the (cached) TableKeyInfo so
        // per-index consumed-capacity accounting avoids a separate
        // describe_table round-trip on every write; it also supplies `has_lsi`.
        let (global_secondary_indexes, local_secondary_indexes) =
            self.fetch_all_index_info(&table_id).await?;
        let has_lsi = !local_secondary_indexes.is_empty();

        let key_info = TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: account_id.to_owned(),
            table_id,
            base_key_schema: key_schema.clone(),
            key_schema,
            attribute_definitions,
            has_lsi,
            global_secondary_indexes,
            local_secondary_indexes,
            stream_specification,
        };
        // Catalog metadata that cannot describe its own sort key would make the
        // keyed read paths fall back to a partition-only lookup and return the
        // wrong item, so refuse it here rather than serve a wrong answer (#259).
        key_info
            .validate_sort_key_definitions()
            .map_err(StorageError::Internal)?;
        Ok(key_info)
    }

    /// Fetch every secondary index defined on a table, split into
    /// `(global_secondary_indexes, local_secondary_indexes)`.
    async fn fetch_all_index_info(
        &self,
        table_id: &str,
    ) -> Result<(Vec<IndexInfo>, Vec<IndexInfo>), StorageError> {
        let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT index_name, index_type, index_id, key_schema, projection \
             FROM indexes WHERE table_id = ?",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut gsis = Vec::new();
        let mut lsis = Vec::new();
        for (index_name, idx_type_str, index_id, ks_json, proj_json) in rows {
            let index_type = match idx_type_str.as_str() {
                "GSI" => IndexType::Gsi,
                "LSI" => IndexType::Lsi,
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown index type in database: {other}"
                    )));
                }
            };
            let key_schema: Vec<KeySchemaElement> = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: Projection = serde_json::from_str(&proj_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let info = IndexInfo {
                index_name,
                index_id,
                index_type,
                key_schema,
                projection,
            };
            match index_type {
                IndexType::Gsi => gsis.push(info),
                IndexType::Lsi => lsis.push(info),
            }
        }
        Ok((gsis, lsis))
    }

    /// Fetch `IndexInfo` for a secondary index, validating the table is ACTIVE.
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
        match status.as_str() {
            "ACTIVE" => {}
            // A table still being created, or being deleted, is not usable for
            // data-plane operations; DynamoDB reports it as not found (not
            // in-use). UPDATING keeps the not-active classification.
            "CREATING" | "DELETING" => {
                return Err(StorageError::TableNotFound(table_name.to_owned()));
            }
            _ => return Err(StorageError::TableNotActive(table_name.to_owned())),
        }
        self.fetch_index_info_by_table_id(&table_id, index_name)
            .await
    }

    /// Fetch `IndexInfo` using a known `table_id`.
    pub(crate) async fn fetch_index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT index_type, index_id, key_schema, projection \
             FROM indexes WHERE table_id = ? AND index_name = ?",
        )
        .bind(table_id)
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (idx_type_str, index_id, ks_str, proj_str) =
            row.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;

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
            index_id,
            index_type,
            key_schema,
            projection,
        })
    }
}
