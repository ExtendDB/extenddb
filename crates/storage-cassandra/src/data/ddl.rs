// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in Cassandra.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, ScalarAttributeType,
    StreamSpecification, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use crate::CassandraEngine;

/// CQL table name for a DynamoDB table in an account keyspace.
pub(crate) fn data_table_name(table_id: &str) -> String {
    format!("items_{}", table_id.replace('-', "_"))
}

/// CQL table name for a GSI/LSI data table.
pub fn index_table_name(index_id: &str) -> String {
    format!("index_{}", index_id.replace('-', "_"))
}

/// Look up all RANGE key attribute definitions from the key schema (preserving order).
pub(crate) fn all_sort_key_info<'a>(
    key_schema: &'a [KeySchemaElement],
    attr_defs: &'a [AttributeDefinition],
) -> Vec<(&'a str, ScalarAttributeType)> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == extenddb_core::types::KeyType::Range)
        .filter_map(|ks| {
            attr_defs
                .iter()
                .find(|ad| ad.attribute_name == ks.attribute_name)
                .map(|ad| (ks.attribute_name.as_str(), ad.attribute_type))
        })
        .collect()
}

impl CassandraEngine {
    /// Fetch lightweight table metadata required for data operations.
    ///
    /// Returns `TableKeyInfo` containing key schema, attribute definitions,
    /// table ID, and stream specification. Used by all `DataEngine` operations.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::TableNotFound` if the table doesn't exist.
    /// Returns `StorageError::TableNotActive` if the table status is not ACTIVE.
    pub(crate) async fn fetch_table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let catalog_keyspace = format!("{}_catalog", self.keyspace_prefix);

        let query = format!(
            "SELECT key_schema, attribute_definitions, table_status, table_id, stream_specification \
             FROM {catalog_keyspace}.tables WHERE account_id = ? AND table_name = ?"
        );

        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, table_name))
            .await
            .map_err(|e| StorageError::Internal(format!("Query table: {e}")))?;

        let body = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

        let rows = body
            .into_rows()
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let ks_text: String = row
            .get_r_by_name("key_schema")
            .map_err(|e| StorageError::Internal(format!("Parse key_schema: {e}")))?;
        let ad_text: String = row
            .get_r_by_name("attribute_definitions")
            .map_err(|e| StorageError::Internal(format!("Parse attribute_definitions: {e}")))?;
        let status: String = row
            .get_r_by_name("table_status")
            .map_err(|e| StorageError::Internal(format!("Parse table_status: {e}")))?;
        let table_id: String = row
            .get_r_by_name("table_id")
            .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?;
        let stream_spec_text: Option<String> =
            row.get_by_name("stream_specification").ok().flatten();

        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }

        let key_schema: Vec<KeySchemaElement> =
            serde_json::from_str(&ks_text).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attribute_definitions: Vec<AttributeDefinition> =
            serde_json::from_str(&ad_text).map_err(|e| StorageError::Internal(e.to_string()))?;

        let stream_specification: Option<StreamSpecification> = stream_spec_text
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch all secondary indexes and split into GSI/LSI.
        let all_indexes = crate::data::index::fetch_indexes_for_table(
            &table_id,
            &self.session_arc(),
            &catalog_keyspace,
        )
        .await
        .unwrap_or_default();

        let mut global_secondary_indexes: Vec<IndexInfo> = Vec::new();
        let mut local_secondary_indexes: Vec<IndexInfo> = Vec::new();
        for meta in all_indexes {
            let index_type = match meta.index_type.as_str() {
                "GSI" => IndexType::Gsi,
                "LSI" => IndexType::Lsi,
                other => {
                    tracing::warn!("fetch_table_key_info: unknown index type '{other}', skipping");
                    continue;
                }
            };
            let info = IndexInfo {
                index_name: meta.index_name,
                index_id: meta.index_id,
                index_type,
                key_schema: meta.key_schema,
                projection: meta.projection,
            };
            match info.index_type {
                IndexType::Gsi => global_secondary_indexes.push(info),
                IndexType::Lsi => local_secondary_indexes.push(info),
                IndexType::Vector => {} // Vector indexes not yet supported in Cassandra backend
            }
        }
        let has_lsi = !local_secondary_indexes.is_empty();

        Ok(TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: account_id.to_owned(),
            table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions,
            has_lsi,
            global_secondary_indexes,
            local_secondary_indexes,
            stream_specification,
            vector_indexes: Vec::new(),
        })
    }

    /// Create the per-DynamoDB-table data table in Cassandra.
    ///
    /// Called after catalog metadata is inserted. The DDL is dynamically
    /// generated based on the key schema.
    pub(crate) async fn create_data_table(
        &self,
        account_keyspace: &str,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let sk_infos = all_sort_key_info(key_schema, attr_defs);

        let ddl = if sk_infos.is_empty() {
            // Hash-only table (no clustering columns)
            // partition_max_delete_timestamp is a regular column (not STATIC) since there's only one row per partition
            format!(
                "CREATE TABLE {account_keyspace}.{ddb_table} (\
                    pk text PRIMARY KEY, \
                    partition_max_delete_timestamp bigint, \
                    item_data text, \
                    prepared_txn_id uuid, \
                    prepared_txn_timestamp bigint, \
                    last_committed_txn_timestamp bigint, \
                    created_to_prepare boolean\
                )"
            )
        } else if sk_infos.len() == 1 {
            // Single sort key - use typed columns
            let sk_col = sk_column(sk_infos[0].1);
            format!(
                "CREATE TABLE {account_keyspace}.{ddb_table} (\
                    pk text, \
                    partition_max_delete_timestamp bigint STATIC, \
                    sk_s text, \
                    sk_n decimal, \
                    sk_b blob, \
                    item_data text, \
                    prepared_txn_id uuid, \
                    prepared_txn_timestamp bigint, \
                    last_committed_txn_timestamp bigint, \
                    created_to_prepare boolean, \
                    PRIMARY KEY (pk, {sk_col})\
                )"
            )
        } else {
            // Multi-part RANGE key
            let mut col_defs = vec!["pk text".to_owned()];
            let mut pk_cols = vec!["pk".to_owned()];

            for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
                let col = sk_column_n(i, sk_type);
                // Add all three type columns for this SK position
                if i == 0 {
                    col_defs.push("sk_s text".to_owned());
                    col_defs.push("sk_n decimal".to_owned());
                    col_defs.push("sk_b blob".to_owned());
                } else {
                    let n = i + 1;
                    col_defs.push(format!("sk{n}_s text"));
                    col_defs.push(format!("sk{n}_n decimal"));
                    col_defs.push(format!("sk{n}_b blob"));
                }
                pk_cols.push(col);
            }
            col_defs.push("partition_max_delete_timestamp bigint STATIC".to_owned());
            col_defs.push("item_data text".to_owned());
            col_defs.push("prepared_txn_id uuid".to_owned());
            col_defs.push("prepared_txn_timestamp bigint".to_owned());
            col_defs.push("last_committed_txn_timestamp bigint".to_owned());
            col_defs.push("created_to_prepare boolean".to_owned());

            format!(
                "CREATE TABLE {}.{} ({}, PRIMARY KEY ({}))",
                account_keyspace,
                ddb_table,
                col_defs.join(", "),
                pk_cols.join(", ")
            )
        };

        self.session
            .query(&ddl)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to create data table: {e}")))?;

        Ok(())
    }

    /// Drop the per-DynamoDB-table data table.
    pub(crate) async fn drop_data_table(
        &self,
        account_keyspace: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let ddb_table = data_table_name(table_id);
        let ddl = format!("DROP TABLE IF EXISTS {account_keyspace}.{ddb_table}");

        self.session
            .query(&ddl)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to drop data table: {e}")))?;

        Ok(())
    }

    /// Drop a GSI/LSI data table.
    pub async fn drop_index_data_table(
        &self,
        account_keyspace: &str,
        index_id: &str,
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);
        let ddl = format!("DROP TABLE IF EXISTS {account_keyspace}.{idx_table}");

        self.session
            .query(&ddl)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to drop index data table: {e}")))?;

        Ok(())
    }

    /// Create a GSI/LSI data table in Cassandra.
    ///
    /// GSI tables use (pk, sk_*, base_pk, base_sk_*) structure where:
    /// - pk is the partition key (index PK)
    /// - sk_* are clustering keys for ordering (index SK)
    /// - base_pk, base_sk_* are clustering keys for uniqueness
    ///
    /// This differs from PostgreSQL where base keys come before index SK
    /// in the PRIMARY KEY constraint. Cassandra needs index SK as clustering
    /// keys to enable efficient range queries.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_data_table(
        &self,
        account_keyspace: &str,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let idx_table = index_table_name(index_id);

        // Determine base table sort key columns for uniqueness
        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);
        // Determine index sort keys
        let idx_sks = all_sort_key_info(index_key_schema, attr_defs);

        // Build column definitions
        let mut col_defs = vec!["pk text".to_owned()];

        // Index SK columns
        for (i, &(_, _)) in idx_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("sk_s text".to_owned());
                col_defs.push("sk_n decimal".to_owned());
                col_defs.push("sk_b blob".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("sk{n}_s text"));
                col_defs.push(format!("sk{n}_n decimal"));
                col_defs.push(format!("sk{n}_b blob"));
            }
        }

        // Base table key columns for uniqueness
        col_defs.push("base_pk text".to_owned());
        for (i, &(_, _)) in base_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push("base_sk_s text".to_owned());
                col_defs.push("base_sk_n decimal".to_owned());
                col_defs.push("base_sk_b blob".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("base_sk{n}_s text"));
                col_defs.push(format!("base_sk{n}_n decimal"));
                col_defs.push(format!("base_sk{n}_b blob"));
            }
        }

        col_defs.push("item_data text".to_owned());

        // Build PRIMARY KEY: ((pk), sk_*, base_pk, base_sk_*)
        // Partition key is (pk), clustering keys are index SK + base keys
        let mut clustering_cols = Vec::new();

        // Add index sort keys as clustering keys (for ordering)
        for (i, &(_, sk_type)) in idx_sks.iter().enumerate() {
            clustering_cols.push(sk_column_n(i, sk_type));
        }

        // Add base keys as clustering keys (for uniqueness)
        clustering_cols.push("base_pk".to_owned());
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            let col = if i == 0 {
                format!("base_{}", sk_column(sk_type))
            } else {
                format!("base_{}", sk_column_n(i, sk_type))
            };
            clustering_cols.push(col);
        }

        let primary_key = if clustering_cols.is_empty() {
            // Hash-only index with no base keys - just partition key
            "((pk))".to_owned()
        } else {
            // Partition key + clustering keys
            format!("((pk), {})", clustering_cols.join(", "))
        };

        let ddl = format!(
            "CREATE TABLE {}.{} ({}, PRIMARY KEY {})",
            account_keyspace,
            idx_table,
            col_defs.join(", "),
            primary_key
        );

        self.session
            .query(&ddl)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to create index table: {e}")))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{KeyType, ScalarAttributeType};

    #[test]
    fn test_data_table_name() {
        assert_eq!(data_table_name("abc123"), "items_abc123");
        // Test UUID normalization (hyphens replaced with underscores)
        assert_eq!(
            data_table_name("914da501-3f2e-4b1c-a5d6-1234567890ab"),
            "items_914da501_3f2e_4b1c_a5d6_1234567890ab"
        );
    }

    #[test]
    fn test_index_table_name() {
        assert_eq!(index_table_name("idx456"), "index_idx456");
        // Test UUID normalization (hyphens replaced with underscores)
        assert_eq!(
            index_table_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            "index_a1b2c3d4_e5f6_7890_abcd_ef1234567890"
        );
    }

    #[test]
    fn test_all_sort_key_info_no_range() {
        let key_schema = vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }];
        let attr_defs = vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }];

        let result = all_sort_key_info(&key_schema, &attr_defs);
        assert!(result.is_empty());
    }

    #[test]
    fn test_all_sort_key_info_single_range() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ];
        let attr_defs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: ScalarAttributeType::N,
            },
        ];

        let result = all_sort_key_info(&key_schema, &attr_defs);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "sk");
        assert_eq!(result[0].1, ScalarAttributeType::N);
    }

    #[test]
    fn test_all_sort_key_info_multi_range() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk1".to_string(),
                key_type: KeyType::Range,
            },
            KeySchemaElement {
                attribute_name: "sk2".to_string(),
                key_type: KeyType::Range,
            },
        ];
        let attr_defs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk1".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk2".to_string(),
                attribute_type: ScalarAttributeType::B,
            },
        ];

        let result = all_sort_key_info(&key_schema, &attr_defs);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "sk1");
        assert_eq!(result[0].1, ScalarAttributeType::S);
        assert_eq!(result[1].0, "sk2");
        assert_eq!(result[1].1, ScalarAttributeType::B);
    }
}
