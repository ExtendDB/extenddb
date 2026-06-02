// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in `TiDB`.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo, TableReadInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use super::index::{
    NativeSecondaryIndex, create_native_secondary_indexes, drop_native_secondary_indexes,
    native_index_create_table_definitions, native_index_generated_column_definitions,
    native_index_name,
};
use super::{
    DATA_TABLE_PARTITIONS, DATA_TABLE_SPLIT_REGIONS, DECIMAL_SPLIT_LOWER, DECIMAL_SPLIT_UPPER,
    DYNAMODB_HASH_KEY_COLUMN_BYTES, DYNAMODB_HASH_KEY_COLUMN_TYPE, DYNAMODB_SORT_KEY_COLUMN_BYTES,
    DYNAMODB_SORT_KEY_COLUMN_TYPE, VARBINARY_SPLIT_LOWER, all_sort_key_info, data_table_name,
    validate_native_key_schema_shape, varbinary_split_upper,
};
use crate::TidbEngine;
use crate::tidb_util::{execute_tidb_create_table_ddl, execute_tidb_idempotent_ddl};

/// Row shape returned by the table-info query:
/// (key_schema, attr_defs, status, table_id, stream_spec, has_lsi).
type TableInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
    Option<bool>,
);

type TableWriteInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
    Option<bool>,
    serde_json::Value,
);

type TableReadInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
    Option<bool>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
);

fn table_accepts_data_plane(status: &str) -> bool {
    matches!(status, "ACTIVE" | "UPDATING")
}

fn data_table_ddl(
    table_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    indexes: &[NativeSecondaryIndex<'_>],
) -> Result<String, StorageError> {
    validate_native_key_schema_shape("table", key_schema)?;
    for index in indexes {
        validate_native_key_schema_shape("secondary index", index.key_schema)?;
    }

    let ddb_table = data_table_name(table_id);
    let sk_infos = all_sort_key_info(key_schema, attr_defs);

    let mut definitions = vec![format!("pk {DYNAMODB_HASH_KEY_COLUMN_TYPE} NOT NULL")];
    let mut pk_cols = vec!["pk".to_owned()];

    if sk_infos.len() == 1 {
        definitions.push(format!("sk_s {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
        definitions.push("sk_n DECIMAL(65, 30)".to_owned());
        definitions.push(format!("sk_b {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
        pk_cols.push(sk_column(sk_infos[0].1).to_owned());
    } else {
        for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
            let col = sk_column_n(i, sk_type);
            if i == 0 {
                definitions.push(format!("sk_s {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
                definitions.push("sk_n DECIMAL(65, 30)".to_owned());
                definitions.push(format!("sk_b {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
            } else {
                let n = i + 1;
                definitions.push(format!("sk{n}_s {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
                definitions.push(format!("sk{n}_n DECIMAL(65, 30)"));
                definitions.push(format!("sk{n}_b {DYNAMODB_SORT_KEY_COLUMN_TYPE}"));
            }
            pk_cols.push(col);
        }
    }

    definitions.push("item_data JSON NOT NULL".to_owned());
    definitions.extend(native_index_generated_column_definitions(
        indexes, attr_defs,
    )?);
    definitions.push(format!("PRIMARY KEY ({}) CLUSTERED", pk_cols.join(", ")));
    definitions.extend(native_index_create_table_definitions(
        indexes, attr_defs, true,
    ));

    Ok(format!(
        "CREATE TABLE {ddb_table} (\n    {}\n) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n\
         PARTITION BY KEY(pk) PARTITIONS {DATA_TABLE_PARTITIONS}",
        definitions.join(",\n    "),
    ))
}

fn split_bound_for_sort_key(
    scalar_type: extenddb_core::types::ScalarAttributeType,
    lower: bool,
) -> String {
    match scalar_type {
        extenddb_core::types::ScalarAttributeType::S
        | extenddb_core::types::ScalarAttributeType::B => {
            if lower {
                VARBINARY_SPLIT_LOWER.to_owned()
            } else {
                varbinary_split_upper(DYNAMODB_SORT_KEY_COLUMN_BYTES)
            }
        }
        extenddb_core::types::ScalarAttributeType::N => {
            if lower {
                DECIMAL_SPLIT_LOWER.to_owned()
            } else {
                DECIMAL_SPLIT_UPPER.to_owned()
            }
        }
    }
}

fn data_table_region_split_sql(
    table: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<String, StorageError> {
    validate_native_key_schema_shape("table", key_schema)?;
    let sk_infos = all_sort_key_info(key_schema, attr_defs);

    let mut lower = vec![VARBINARY_SPLIT_LOWER.to_owned()];
    let mut upper = vec![varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES)];
    for &(_, scalar_type) in &sk_infos {
        lower.push(split_bound_for_sort_key(scalar_type, true));
        upper.push(split_bound_for_sort_key(scalar_type, false));
    }

    Ok(format!(
        "SPLIT TABLE {table} BETWEEN ({}) AND ({}) REGIONS {DATA_TABLE_SPLIT_REGIONS}",
        lower.join(", "),
        upper.join(", ")
    ))
}

fn native_index_region_split_sql(table: &str, index_id: &str) -> String {
    let upper = varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES);
    format!(
        "SPLIT TABLE {table} INDEX `{}` BETWEEN ({VARBINARY_SPLIT_LOWER}) AND ({upper}) REGIONS {DATA_TABLE_SPLIT_REGIONS}",
        native_index_name(index_id)
    )
}

async fn split_native_secondary_index_regions(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    indexes: &[NativeSecondaryIndex<'_>],
) -> Result<(), StorageError> {
    let table = data_table_name(table_id);
    for index in indexes {
        let sql = native_index_region_split_sql(&table, index.index_id);
        execute_tidb_idempotent_ddl(pool, "split_native_secondary_index_regions", &sql).await?;
    }
    Ok(())
}

async fn split_data_table_regions(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    indexes: &[NativeSecondaryIndex<'_>],
) -> Result<(), StorageError> {
    let table = data_table_name(table_id);
    let table_split = data_table_region_split_sql(&table, key_schema, attr_defs)?;
    execute_tidb_idempotent_ddl(pool, "split_data_table_regions", &table_split).await?;
    split_native_secondary_index_regions(pool, table_id, indexes).await
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
        indexes: &[(&str, &[KeySchemaElement])],
    ) -> Result<(), StorageError> {
        let indexes = indexes
            .iter()
            .map(|(index_id, key_schema)| NativeSecondaryIndex {
                index_id,
                key_schema,
            })
            .collect::<Vec<_>>();
        let ddl = data_table_ddl(table_id, key_schema, attr_defs, &indexes)?;

        let created =
            execute_tidb_create_table_ddl(pool, "create_data_table_with_initial_indexes", &ddl)
                .await?;
        if !created {
            create_native_secondary_indexes(pool, table_id, &indexes, attr_defs).await?;
        }
        split_data_table_regions(pool, table_id, key_schema, attr_defs, &indexes).await?;

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
        execute_tidb_idempotent_ddl(pool, "drop_data_table", &ddl).await?;
        Ok(())
    }

    /// Create multiple DynamoDB secondary indexes, sharing the generated-column
    /// additions in one TiDB online `ALTER TABLE` for the table.
    pub(crate) async fn create_index_artifacts_batch(
        pool: &sqlx::MySqlPool,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let indexes = indexes
            .iter()
            .map(|(index_id, key_schema)| NativeSecondaryIndex {
                index_id,
                key_schema,
            })
            .collect::<Vec<_>>();
        create_native_secondary_indexes(pool, table_id, &indexes, attr_defs).await?;
        split_native_secondary_index_regions(pool, table_id, &indexes).await
    }

    /// Drop multiple native TiDB secondary indexes and remove their generated
    /// key columns with one online `ALTER TABLE` for the table.
    pub(crate) async fn drop_index_artifacts_batch(
        pool: &sqlx::MySqlPool,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let indexes = indexes
            .iter()
            .map(|(index_id, key_schema)| NativeSecondaryIndex {
                index_id,
                key_schema,
            })
            .collect::<Vec<_>>();
        drop_native_secondary_indexes(pool, table_id, &indexes, attr_defs).await
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
            secondary_index_key_schemas: Vec::new(),
            has_lsi: has_lsi.unwrap_or(false),
            stream_specification,
        })
    }

    pub(crate) async fn fetch_table_write_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let row: Option<TableWriteInfoRow> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions, table_status, table_id, \
             stream_specification, \
             EXISTS(SELECT 1 FROM indexes WHERE table_id = tables.table_id AND index_type = 'LSI') AS has_lsi, \
             COALESCE(( \
                 SELECT JSON_ARRAYAGG(key_schema) FROM indexes \
                 WHERE table_id = tables.table_id AND index_status IN ('ACTIVE', 'CREATING') \
             ), JSON_ARRAY()) AS secondary_index_key_schemas \
             FROM tables \
             WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (
            ks_json,
            ad_json,
            status,
            table_id,
            stream_spec_json,
            has_lsi,
            secondary_index_key_schemas_json,
        ) = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

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
        let secondary_index_key_schemas: Vec<Vec<KeySchemaElement>> =
            serde_json::from_value(secondary_index_key_schemas_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: account_id.to_owned(),
            table_id,
            key_schema,
            attribute_definitions,
            secondary_index_key_schemas,
            has_lsi: has_lsi.unwrap_or(false),
            stream_specification,
        })
    }

    pub(crate) async fn fetch_table_read_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: Option<&str>,
    ) -> Result<TableReadInfo, StorageError> {
        let Some(index_name) = index_name else {
            return Ok(TableReadInfo {
                table: self.fetch_table_key_info(account_id, table_name).await?,
                index: None,
            });
        };

        let row: Option<TableReadInfoRow> = sqlx::query_as(
            "SELECT t.key_schema, t.attribute_definitions, t.table_status, t.table_id, \
                    t.stream_specification, \
                    EXISTS(SELECT 1 FROM indexes WHERE table_id = t.table_id AND index_type = 'LSI') AS has_lsi, \
                    i.index_name, i.index_type, i.index_id, i.key_schema, i.projection \
             FROM tables t \
             LEFT JOIN indexes i \
               ON i.table_id = t.table_id \
              AND i.index_name = ? \
              AND i.index_status = 'ACTIVE' \
             WHERE t.account_id = ? AND t.table_name = ?",
        )
        .bind(index_name)
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (
            ks_json,
            ad_json,
            status,
            table_id,
            stream_spec_json,
            has_lsi,
            idx_name,
            idx_type,
            idx_id,
            idx_ks_json,
            idx_projection_json,
        ) = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

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

        let idx_name =
            idx_name.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;
        let idx_type =
            idx_type.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;
        let idx_id = idx_id.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;
        let idx_ks_json =
            idx_ks_json.ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;
        let idx_projection_json = idx_projection_json
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;

        let index_type = match idx_type.as_str() {
            "GSI" => IndexType::Gsi,
            "LSI" => IndexType::Lsi,
            other => {
                return Err(StorageError::Internal(format!(
                    "unknown index type in database: {other}"
                )));
            }
        };

        let index_key_schema: Vec<KeySchemaElement> = serde_json::from_value(idx_ks_json)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let projection: Projection = serde_json::from_value(idx_projection_json)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableReadInfo {
            table: TableKeyInfo {
                table_name: table_name.to_owned(),
                account_id: account_id.to_owned(),
                table_id,
                key_schema,
                attribute_definitions,
                secondary_index_key_schemas: Vec::new(),
                has_lsi: has_lsi.unwrap_or(false),
                stream_specification,
            },
            index: Some(IndexInfo {
                index_name: idx_name,
                index_id: idx_id,
                index_type,
                key_schema: index_key_schema,
                projection,
            }),
        })
    }

    /// Fetch metadata for a secondary index from the catalog.
    ///
    /// This variant looks up `table_id` from the tables catalog. Prefer
    /// `fetch_index_info_by_table_id` when `TableKeyInfo` is already available.
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
    /// already has `TableKeyInfo`.
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
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::{
        data_table_ddl, data_table_region_split_sql, native_index_region_split_sql,
        table_accepts_data_plane, varbinary_split_upper,
    };
    use crate::data::{
        DYNAMODB_HASH_KEY_COLUMN_BYTES, DYNAMODB_SORT_KEY_COLUMN_BYTES, index::NativeSecondaryIndex,
    };

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
            &[],
        )
        .expect("ddl");

        assert!(ddl.contains("PRIMARY KEY (pk) CLUSTERED"));
        assert!(ddl.contains("pk VARBINARY(2048) NOT NULL"));
        assert!(ddl.contains("DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"));
        assert!(ddl.ends_with("PARTITION BY KEY(pk) PARTITIONS 16"));
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
            &[],
        )
        .expect("ddl");

        assert!(ddl.contains("PRIMARY KEY (pk, sk_s) CLUSTERED"));
    }

    #[test]
    fn multipart_range_key_tables_are_rejected_before_tidb_ddl() {
        let err = data_table_ddl(
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
            &[],
        )
        .unwrap_err();

        assert!(err.to_string().contains("at most one RANGE key"));
        assert!(err.to_string().contains("3072-byte key limit"));
    }

    #[test]
    fn create_table_ddl_includes_initial_native_secondary_indexes() {
        let base_keys = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
        let index_keys = vec![key("gpk", KeyType::Hash), key("gsk", KeyType::Range)];
        let attrs = vec![
            attr("pk", ScalarAttributeType::S),
            attr("sk", ScalarAttributeType::N),
            attr("gpk", ScalarAttributeType::S),
            attr("gsk", ScalarAttributeType::B),
        ];
        let indexes = vec![NativeSecondaryIndex {
            index_id: "idx-1",
            key_schema: &index_keys,
        }];

        let ddl = data_table_ddl("tableid", &base_keys, &attrs, &indexes).expect("ddl");

        assert!(ddl.starts_with("CREATE TABLE `_ddb_tableid`"));
        assert!(ddl.contains("`edbidx_idx1_pk` VARBINARY(2048) AS"));
        assert!(ddl.contains("`edbidx_idx1_sk_b` VARBINARY(1024) AS"));
        assert!(ddl.contains("INDEX `idx_idx1` (edbidx_idx1_pk, edbidx_idx1_sk_b) GLOBAL"));
        assert!(!ddl.contains("IF NOT EXISTS"));
        assert!(!ddl.contains("ALTER TABLE"));
        assert!(ddl.contains("PARTITION BY KEY(pk) PARTITIONS 16"));
    }

    #[test]
    fn hash_only_tables_are_split_by_native_clustered_key_range() {
        let split = data_table_region_split_sql(
            "`_ddb_tableid`",
            &[key("pk", KeyType::Hash)],
            &[attr("pk", ScalarAttributeType::S)],
        )
        .expect("split sql");

        assert_eq!(
            split,
            format!(
                "SPLIT TABLE `_ddb_tableid` BETWEEN (X'') AND ({}) REGIONS 16",
                varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES)
            )
        );
    }

    #[test]
    fn range_key_tables_are_split_by_full_clustered_key_shape() {
        let split = data_table_region_split_sql(
            "`_ddb_tableid`",
            &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
            &[
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::N),
            ],
        )
        .expect("split sql");

        assert_eq!(
            split,
            format!(
                "SPLIT TABLE `_ddb_tableid` BETWEEN (X'', -99999999999999999999999999999999999.999999999999999999999999999999) AND ({}, 99999999999999999999999999999999999.999999999999999999999999999999) REGIONS 16",
                varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES)
            )
        );
    }

    #[test]
    fn native_secondary_indexes_are_split_by_generated_hash_key_prefix() {
        assert_eq!(
            native_index_region_split_sql("`_ddb_tableid`", "idx-1"),
            format!(
                "SPLIT TABLE `_ddb_tableid` INDEX `idx_idx1` BETWEEN (X'') AND ({}) REGIONS 16",
                varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES)
            )
        );
    }

    #[test]
    fn binary_sort_key_split_bound_matches_native_sort_key_width() {
        let split = data_table_region_split_sql(
            "`_ddb_tableid`",
            &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
            &[
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::B),
            ],
        )
        .expect("split sql");

        assert!(split.contains(&format!(
            "AND ({}, {})",
            varbinary_split_upper(DYNAMODB_HASH_KEY_COLUMN_BYTES),
            varbinary_split_upper(DYNAMODB_SORT_KEY_COLUMN_BYTES)
        )));
    }

    #[test]
    fn tidb_data_plane_remains_available_during_online_ddl() {
        assert!(table_accepts_data_plane("ACTIVE"));
        assert!(table_accepts_data_plane("UPDATING"));
        assert!(!table_accepts_data_plane("CREATING"));
        assert!(!table_accepts_data_plane("DELETING"));
    }
}
