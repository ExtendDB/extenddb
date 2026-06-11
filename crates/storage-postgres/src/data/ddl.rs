// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in `PostgreSQL`.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use super::{all_sort_key_info, data_table_name, index_table_name};
use crate::PostgresEngine;

/// Row shape returned by the table-info query: (key_schema, attr_defs, status, table_id, stream_spec, has_lsi).
type TableInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
    Option<bool>,
);

impl PostgresEngine {
    /// Create the per-DynamoDB-table data table in `PostgreSQL`.
    ///
    /// Called within the `create_table` transaction. The DDL is dynamically
    /// generated based on the key schema — the primary key constraint uses
    /// the sort key column matching the sort key's scalar type.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] if the DDL execution fails.
    ///
    /// # Safety (SQL injection)
    ///
    /// Table names are validated at the engine layer to contain only `[a-zA-Z0-9_.-]`.
    /// Column names are compile-time constants. No user input is interpolated
    /// into the DDL beyond the validated table name.
    pub(crate) async fn create_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let sk_infos = all_sort_key_info(key_schema, attr_defs);

        let ddl = if sk_infos.is_empty() {
            format!(
                r"CREATE TABLE {ddb_table} (
                    pk TEXT NOT NULL PRIMARY KEY,
                    item_data JSONB NOT NULL
                )"
            )
        } else if sk_infos.len() == 1 {
            // Backward-compatible single SK path
            let sk_col = sk_column(sk_infos[0].1);
            format!(
                r"CREATE TABLE {ddb_table} (
                    pk TEXT NOT NULL,
                    sk_s TEXT,
                    sk_n NUMERIC,
                    sk_b BYTEA,
                    item_data JSONB NOT NULL,
                    PRIMARY KEY (pk, {sk_col})
                )"
            )
        } else {
            // Multi-part RANGE key: one typed column set per RANGE attribute
            let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];
            let mut pk_cols = vec!["pk".to_owned()];
            for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
                let col = sk_column_n(i, sk_type);
                // Add all three type columns for this SK position
                if i == 0 {
                    col_defs.push("sk_s TEXT".to_owned());
                    col_defs.push("sk_n NUMERIC".to_owned());
                    col_defs.push("sk_b BYTEA".to_owned());
                } else {
                    let n = i + 1;
                    col_defs.push(format!("sk{n}_s TEXT"));
                    col_defs.push(format!("sk{n}_n NUMERIC"));
                    col_defs.push(format!("sk{n}_b BYTEA"));
                }
                pk_cols.push(col);
            }
            col_defs.push("item_data JSONB NOT NULL".to_owned());
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
    ///
    /// Called when a table deletion transition completes.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] if the DDL execution fails.
    pub(crate) async fn drop_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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

    /// Create a GSI/LSI data table in PostgreSQL.
    ///
    /// GSI tables use the same `(pk, sk_*)` structure as base tables but add
    /// `base_pk` and `base_sk_*` columns for uniqueness (GSI keys are not unique).
    /// The primary key includes the base table key to ensure one row per base item.
    ///
    /// Multi-part keys: when the index has multiple HASH attributes, they are
    /// concatenated into the `pk` column. Multiple RANGE attributes get separate
    /// typed column sets (`sk_s/sk_n/sk_b`, `sk2_s/sk2_n/sk2_b`, etc.).
    // S2: Parameters mirror the SQL schema dimensions (account, table, index,
    // key schemas, attribute defs). A wrapper struct would obscure the call
    // site without adding clarity.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_index_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);

        // Determine base table sort key columns for the uniqueness constraint
        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);
        // Determine index sort keys
        let idx_sks = all_sort_key_info(index_key_schema, attr_defs);

        // Build column definitions
        let mut col_defs = vec!["pk TEXT NOT NULL".to_owned()];

        // Index SK columns
        for (i, &(_, _)) in idx_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("sk_s TEXT".to_owned());
                col_defs.push("sk_n NUMERIC".to_owned());
                col_defs.push("sk_b BYTEA".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("sk{n}_s TEXT"));
                col_defs.push(format!("sk{n}_n NUMERIC"));
                col_defs.push(format!("sk{n}_b BYTEA"));
            }
        }

        // Base table key columns for uniqueness
        col_defs.push("base_pk TEXT NOT NULL".to_owned());
        for (i, &(_, _)) in base_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("base_sk_s TEXT".to_owned());
                col_defs.push("base_sk_n NUMERIC".to_owned());
                col_defs.push("base_sk_b BYTEA".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("base_sk{n}_s TEXT"));
                col_defs.push(format!("base_sk{n}_n NUMERIC"));
                col_defs.push(format!("base_sk{n}_b BYTEA"));
            }
        }

        col_defs.push("item_data JSONB NOT NULL".to_owned());

        // Build PK constraint: (pk, base_pk[, base_sk_col...])
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

        // Create an index for sort key ordering within a partition
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
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
    ///
    /// Uses a single query that combines the table row with an LSI existence
    /// subquery, eliminating one catalog roundtrip per call (P118 optimization #1).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::TableNotFound`] if the table doesn't exist.
    /// Returns [`StorageError::TableNotActive`] if the table is not ACTIVE.
    /// Returns [`StorageError::Internal`] on query or deserialization failure.
    pub(crate) async fn fetch_table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let row: Option<TableInfoRow> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions, table_status, table_id, \
             stream_specification, \
             EXISTS(SELECT 1 FROM indexes WHERE table_id = tables.table_id AND index_type = 'LSI') AS has_lsi \
             FROM tables \
             WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_json, ad_json, status, table_id, stream_spec_json, has_lsi) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }

        let key_schema: Vec<KeySchemaElement> =
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attribute_definitions: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| StorageError::Internal(e.to_string()))?;

        let stream_specification: Option<StreamSpecification> = stream_spec_json
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: account_id.to_owned(),
            table_id,
            base_key_schema: key_schema.clone(),
            key_schema,
            attribute_definitions,
            has_lsi: has_lsi.unwrap_or(false),
            stream_specification,
        })
    }

    /// Fetch metadata for a secondary index from the catalog.
    ///
    /// This variant looks up `table_id` from the tables catalog. Prefer
    /// `fetch_index_info_by_table_id` when `TableKeyInfo` is already available
    /// (P118 optimization #4).
    pub(crate) async fn fetch_index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        // First get the table_id and verify the table is ACTIVE
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_status FROM tables WHERE account_id = $1 AND table_name = $2",
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
    ///
    /// Saves one catalog roundtrip vs `fetch_index_info` when the caller
    /// already has `TableKeyInfo` (P118 optimization #4).
    pub(crate) async fn fetch_index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        let idx_row: Option<(String, String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
            "SELECT index_type, index_id, key_schema, projection FROM indexes WHERE table_id = $1 AND index_name = $2",
        )
        .bind(table_id)
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (idx_type_str, idx_id, ks_json, proj_json) =
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
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let projection: Projection =
            serde_json::from_value(proj_json).map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(IndexInfo {
            index_name: index_name.to_owned(),
            index_id: idx_id,
            index_type,
            key_schema,
            projection,
        })
    }
}
