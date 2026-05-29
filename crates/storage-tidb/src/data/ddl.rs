// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in `TiDB`.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use super::index::{create_native_secondary_index, drop_native_secondary_index};
use super::{all_sort_key_info, data_table_name};
use crate::TidbEngine;

/// Row shape returned by the table-info query: (key_schema, attr_defs, status, table_id, stream_spec, has_lsi).
type TableInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
    Option<bool>,
);

fn table_accepts_data_plane(status: &str) -> bool {
    matches!(status, "ACTIVE" | "UPDATING")
}

fn data_table_ddl(
    table_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> String {
    let ddb_table = data_table_name(table_id);
    let sk_infos = all_sort_key_info(key_schema, attr_defs);

    if sk_infos.is_empty() {
        format!(
            r"CREATE TABLE IF NOT EXISTS {ddb_table} (
                    pk VARBINARY(2048) NOT NULL PRIMARY KEY CLUSTERED,
                    item_data JSON NOT NULL
                )"
        )
    } else if sk_infos.len() == 1 {
        let sk_col = sk_column(sk_infos[0].1);
        format!(
            r"CREATE TABLE IF NOT EXISTS {ddb_table} (
                    pk VARBINARY(2048) NOT NULL,
                    sk_s VARBINARY(1024),
                    sk_n DECIMAL(65, 30),
                    sk_b VARBINARY(1024),
                    item_data JSON NOT NULL,
                    PRIMARY KEY (pk, {sk_col}) CLUSTERED
                )"
        )
    } else {
        let mut col_defs = vec!["pk VARBINARY(2048) NOT NULL".to_owned()];
        let mut pk_cols = vec!["pk".to_owned()];
        for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
            let col = sk_column_n(i, sk_type);
            if i == 0 {
                col_defs.push("sk_s VARBINARY(1024)".to_owned());
                col_defs.push("sk_n DECIMAL(65, 30)".to_owned());
                col_defs.push("sk_b VARBINARY(1024)".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("sk{n}_s VARBINARY(1024)"));
                col_defs.push(format!("sk{n}_n DECIMAL(65, 30)"));
                col_defs.push(format!("sk{n}_b VARBINARY(1024)"));
            }
            pk_cols.push(col);
        }
        col_defs.push("item_data JSON NOT NULL".to_owned());
        format!(
            "CREATE TABLE IF NOT EXISTS {ddb_table} (\n    {},\n    PRIMARY KEY ({}) CLUSTERED\n)",
            col_defs.join(",\n    "),
            pk_cols.join(", ")
        )
    }
}

impl TidbEngine {
    /// Create the per-DynamoDB-table data table in `TiDB`.
    ///
    /// The DDL is dynamically
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
        pool: &sqlx::MySqlPool,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let ddl = data_table_ddl(table_id, key_schema, attr_defs);

        sqlx::query(&ddl)
            .execute(pool)
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
        pool: &sqlx::MySqlPool,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let ddl = format!("DROP TABLE IF EXISTS {ddb_table}");
        sqlx::query(&ddl)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Create a DynamoDB secondary index as native TiDB generated columns plus
    /// one native secondary index.
    ///
    /// TiDB has no separate local-index physical path; GSI versus LSI is
    /// DynamoDB API/catalog metadata.
    // S2: Parameters mirror the SQL schema dimensions (account, table, index,
    // key schemas, attribute defs). A wrapper struct would obscure the call
    // site without adding clarity.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_index_artifacts(
        pool: &sqlx::MySqlPool,
        table_id: &str,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        create_native_secondary_index(
            pool,
            table_id,
            index_id,
            index_key_schema,
            attr_defs,
            base_key_schema,
            base_attr_defs,
        )
        .await
    }

    /// Drop a native TiDB secondary index and its generated key columns.
    pub(crate) async fn drop_index_artifacts(
        pool: &sqlx::MySqlPool,
        table_id: &str,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        drop_native_secondary_index(pool, table_id, index_id, index_key_schema, attr_defs).await
    }

    /// Fetch key schema and attribute definitions for a table from the catalog.
    ///
    /// Uses a single query that combines the table row with an LSI API-metadata
    /// existence subquery for ItemCollectionMetrics. TiDB secondary indexes use
    /// the same native physical path for GSI and LSI definitions.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::TableNotFound`] if the table doesn't exist.
    /// Returns [`StorageError::TableNotActive`] if the table cannot serve data-plane requests.
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
             WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_json, ad_json, status, table_id, stream_spec_json, has_lsi) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        if !table_accepts_data_plane(&status) {
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
        // First get the table_id and verify the table can serve data-plane reads.
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

        if !table_accepts_data_plane(&status) {
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
        let idx_row: Option<(String, String, serde_json::Value, serde_json::Value)> =
            sqlx::query_as(
                "SELECT index_type, index_id, key_schema, projection \
             FROM indexes \
             WHERE table_id = ? AND index_name = ? AND index_status = 'ACTIVE'",
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

    pub(crate) async fn fetch_base_key_schema_by_table_id(
        &self,
        table_id: &str,
    ) -> Result<(Vec<KeySchemaElement>, Vec<AttributeDefinition>), StorageError> {
        let row: Option<(serde_json::Value, serde_json::Value, String)> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions, table_status \
             FROM tables WHERE table_id = ?",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_json, ad_json, status) =
            row.ok_or_else(|| StorageError::TableNotFound(table_id.to_owned()))?;
        if !table_accepts_data_plane(&status) {
            return Err(StorageError::TableNotActive(table_id.to_owned()));
        }

        let key_schema =
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs =
            serde_json::from_value(ad_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok((key_schema, attr_defs))
    }
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::data_table_ddl;

    fn attr(name: &str, ty: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ty,
        }
    }

    fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    #[test]
    fn hash_only_tables_use_explicit_clustered_primary_key() {
        let ddl = data_table_ddl(
            "tableid",
            &[key("pk", KeyType::Hash)],
            &[attr("pk", ScalarAttributeType::S)],
        );

        assert!(ddl.contains("PRIMARY KEY CLUSTERED"));
    }

    #[test]
    fn range_key_tables_use_explicit_clustered_primary_key() {
        let ddl = data_table_ddl(
            "tableid",
            &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
            &[
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::S),
            ],
        );

        assert!(ddl.contains("PRIMARY KEY (pk, sk_s) CLUSTERED"));
    }

    #[test]
    fn multipart_range_key_tables_use_explicit_clustered_primary_key() {
        let ddl = data_table_ddl(
            "tableid",
            &[
                key("pk", KeyType::Hash),
                key("sk", KeyType::Range),
                key("sk2", KeyType::Range),
            ],
            &[
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::S),
                attr("sk2", ScalarAttributeType::N),
            ],
        );

        assert!(ddl.contains("PRIMARY KEY (pk, sk_s, sk2_n) CLUSTERED"));
    }
}
