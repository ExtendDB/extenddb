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

use super::{
    all_sort_key_info, data_table_name, index_table_name, vector_table_glob_pattern,
    vector_table_name,
};
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

    /// Create a vector-index data table: one row per indexed vector.
    ///
    /// `part` is the search-schema HASH value when one is declared, and a single
    /// constant otherwise, so an unscoped index is one partition rather than a
    /// separate code path. `nrm` is the vector's precomputed L2 norm, so cosine
    /// costs one dot product at query time instead of two passes.
    ///
    /// # Safety (SQL injection)
    /// `index_id` is a server-generated UUID and column names are constants, so
    /// no user input reaches the DDL. Vector attribute names are stored as data,
    /// never as identifiers.
    pub(crate) async fn create_vector_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
        index_id: &str,
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let vec_table = vector_table_name(table_id, index_id);
        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);

        let mut col_defs = vec![
            "part TEXT NOT NULL".to_owned(),
            "base_pk TEXT NOT NULL".to_owned(),
        ];
        for i in 0..base_sks.len() {
            col_defs.extend(base_sk_col_defs(i));
        }
        col_defs.push("vec BLOB NOT NULL".to_owned());
        col_defs.push("nrm REAL NOT NULL".to_owned());
        col_defs.push("item_data TEXT NOT NULL".to_owned());

        // Keyed by the base item, not by the partition, so one base item yields
        // at most one vector row and a re-put replaces rather than duplicates.
        let mut pk_cols = vec!["base_pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            pk_cols.push(format!("base_{}", sk_column_n(i, sk_type)));
        }

        let ddl = format!(
            "CREATE TABLE {vec_table} (\n    {},\n    PRIMARY KEY ({})\n)",
            col_defs.join(",\n    "),
            pk_cols.join(", ")
        );
        sqlx::query(&ddl)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // The scan is always partition-scoped, so this index is what keeps a
        // search off the full table when a HASH element is declared.
        let part_idx =
            format!("CREATE INDEX \"_vidx_part_{table_id}_{index_id}\" ON {vec_table} (part)");
        sqlx::query(&part_idx)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Drop every vector-index data table belonging to one DynamoDB table.
    ///
    /// Discovered from `sqlite_master` rather than from the catalog, because the
    /// caller runs this after the catalog rows have been cascade-deleted in the
    /// same transaction, so `vector_indexes` is already empty for this table.
    /// Without this, dropping a table would leave its vector data tables behind
    /// forever with nothing left pointing at them.
    /// Drop one vector index's data table.
    ///
    /// The table-drop path sweeps `sqlite_master` instead, because by the time it
    /// runs the catalog rows have already cascade-deleted and the index ids are
    /// unreadable. Here the id is known, so the name is derived directly rather
    /// than matched by pattern.
    pub(crate) async fn drop_vector_data_table_by_id(
        pool: &sqlx::SqlitePool,
        table_id: &str,
        index_id: &str,
    ) -> Result<(), StorageError> {
        let vec_table = vector_table_name(table_id, index_id);
        sqlx::query(&format!("DROP TABLE IF EXISTS {vec_table}"))
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn drop_all_vector_data_tables(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name GLOB ?",
        )
        .bind(vector_table_glob_pattern(table_id))
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for name in names {
            // Names come from sqlite_master, not from user input, and are quoted.
            sqlx::query(&format!("DROP TABLE IF EXISTS \"{name}\""))
                .execute(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
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
        // Vector data tables are keyed by index, not by table, so they are not
        // reached by dropping the item table or by the catalog cascade.
        Self::drop_all_vector_data_tables(tx, table_id).await?;
        Ok(())
    }

    /// Create a GSI/LSI data table. GSI keys are not unique, so the primary key
    /// is `(pk, base_pk, base_sk*)`: the base-table key is what makes a row
    /// unique, keeping one row per base item per index partition.
    ///
    /// Two secondary indexes are created:
    /// - `idx_order_*` on `(pk, sk*, base_pk, base_sk*)`, for sort-key ordering
    ///   within an index partition. Only when the index declares a sort key.
    /// - `idx_base_key_*` on `(base_pk, base_sk*)`, for the reverse lookup that
    ///   index propagation uses to delete an item's old index row. Always.
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

        // Index on the base-table key columns, for `delete_index_row_multi`.
        //
        // GSI/LSI propagation deletes the old index row by base-table key:
        // `DELETE FROM <idx> WHERE base_pk = ? AND base_sk* = ?`, with no `pk`
        // predicate. Both the PRIMARY KEY and the ordering index above lead
        // with `pk`, so neither can serve that filter and SQLite plans a full
        // `SCAN` of the index table on every propagating write. Unconditional
        // rather than gated on `idx_sks`, because the reverse lookup happens
        // whether or not the index declares a sort key.
        {
            let mut base_key_cols = vec!["base_pk".to_owned()];
            for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
                base_key_cols.push(format!("base_{}", sk_column_n(i, sk_type)));
            }
            let base_key_idx = format!(
                "CREATE INDEX \"idx_base_key_{index_id}\" ON {idx_table} ({})",
                base_key_cols.join(", ")
            );
            sqlx::query(&base_key_idx)
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
        // Vector indexes ride on the cached key info too, so the write path can
        // decide whether any maintenance is needed without a query, and so the
        // engine can validate vector attributes on writes.
        let vector_indexes = self.fetch_vector_index_key_info(&table_id).await?;

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
            // Every field is populated, with no `..Default::default()` spread: a
            // new core field should break this site and force a decision about
            // whether the write path needs it, rather than silently defaulting.
            vector_indexes,
        };
        // Catalog metadata that cannot describe its own sort key would make the
        // keyed read paths fall back to a partition-only lookup and return the
        // wrong item, so refuse it here rather than serve a wrong answer (#259).
        key_info
            .validate_sort_key_definitions()
            .map_err(StorageError::Internal)?;
        Ok(key_info)
    }

    /// Fetch the vector indexes of a table in the shape the engine caches.
    ///
    /// Selects the whole row rather than the five columns this shape needs, so
    /// that one row type and one decode path serve both this and the describe
    /// path. The extra columns are two short tokens and an integer, read only on
    /// a key-info cache miss.
    async fn fetch_vector_index_key_info(
        &self,
        table_id: &str,
    ) -> Result<Vec<extenddb_core::types::VectorIndexKeyInfo>, StorageError> {
        let rows: Vec<crate::table_helpers::VectorIndexRow> = sqlx::query_as(&format!(
            "SELECT {} FROM vector_indexes WHERE table_id = ? ORDER BY index_name",
            crate::table_helpers::VECTOR_INDEX_COLUMNS
        ))
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let catalog_rows = rows
            .into_iter()
            .map(crate::table_helpers::VectorIndexRow::into_catalog_row)
            .collect::<Result<Vec<_>, _>>()?;
        extenddb_storage::vector_catalog::vector_index_key_info(catalog_rows)
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

        let mut infos = Vec::new();
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
            infos.push(info);
        }
        // Grouped by core rather than matched here, so a new IndexType variant
        // does not break this backend. The string parse above already rejects
        // any kind this backend cannot have created.
        let grouped = extenddb_core::types::partition_indexes(infos);
        Ok((grouped.gsis, grouped.lsis))
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

#[cfg(test)]
mod index_data_table_tests {
    use crate::store::SqliteEngine;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    use sqlx::sqlite::SqlitePool;

    fn hash(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }
    }

    fn range(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Range,
        }
    }

    fn attr(name: &str, t: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: t,
        }
    }

    /// Create an index data table through the real DDL path.
    async fn make_index_table(
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        SqliteEngine::create_index_data_table(
            &mut tx,
            "probe",
            index_key_schema,
            attr_defs,
            base_key_schema,
            base_attr_defs,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        pool
    }

    /// The plan SQLite chooses for the reverse-lookup DELETE that index
    /// propagation issues. `SEARCH` means an index is used, `SCAN` means a full
    /// table scan.
    async fn delete_plan(pool: &SqlitePool, where_clause: &str) -> String {
        let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(&format!(
            "EXPLAIN QUERY PLAN DELETE FROM \"_ddb_probe\" WHERE {where_clause}"
        ))
        .fetch_all(pool)
        .await
        .unwrap();
        rows.into_iter()
            .map(|r| r.3)
            .collect::<Vec<_>>()
            .join(" | ")
    }

    #[tokio::test]
    async fn reverse_lookup_delete_uses_an_index_composite_base_key() {
        let pool = make_index_table(
            &[hash("gsi_pk"), range("gsi_sk")],
            &[
                attr("gsi_pk", ScalarAttributeType::S),
                attr("gsi_sk", ScalarAttributeType::S),
            ],
            &[hash("id"), range("ts")],
            &[
                attr("id", ScalarAttributeType::S),
                attr("ts", ScalarAttributeType::S),
            ],
        )
        .await;

        let plan = delete_plan(&pool, "base_pk = 'x' AND base_sk_s = 'y'").await;

        assert!(
            plan.contains("SEARCH"),
            "reverse-lookup DELETE must use an index, got plan: {plan}"
        );
        assert!(
            plan.contains("idx_base_key_probe"),
            "must use the base-key index specifically, got plan: {plan}"
        );
        assert!(
            !plan.contains("SCAN"),
            "must not fall back to a table scan, got plan: {plan}"
        );
    }

    #[tokio::test]
    async fn reverse_lookup_delete_uses_an_index_hash_only_base_key() {
        let pool = make_index_table(
            &[hash("gsi_pk"), range("gsi_sk")],
            &[
                attr("gsi_pk", ScalarAttributeType::S),
                attr("gsi_sk", ScalarAttributeType::S),
            ],
            &[hash("id")],
            &[attr("id", ScalarAttributeType::S)],
        )
        .await;

        let plan = delete_plan(&pool, "base_pk = 'x'").await;

        assert!(
            plan.contains("SEARCH") && plan.contains("idx_base_key_probe"),
            "hash-only base key must still index the reverse lookup, got plan: {plan}"
        );
        assert!(!plan.contains("SCAN"), "got plan: {plan}");
    }

    /// The ordering index is only created when the index declares a sort key, so
    /// a sort-key-less index is the case where the base-key index is the *only*
    /// secondary index. It must still be created, which is why that block is
    /// unconditional.
    #[tokio::test]
    async fn reverse_lookup_delete_uses_an_index_when_index_has_no_sort_key() {
        let pool = make_index_table(
            &[hash("gsi_pk")],
            &[attr("gsi_pk", ScalarAttributeType::S)],
            &[hash("id"), range("ts")],
            &[
                attr("id", ScalarAttributeType::S),
                attr("ts", ScalarAttributeType::S),
            ],
        )
        .await;

        let plan = delete_plan(&pool, "base_pk = 'x' AND base_sk_s = 'y'").await;

        assert!(
            plan.contains("SEARCH") && plan.contains("idx_base_key_probe"),
            "an index with no sort key still needs the base-key index, got plan: {plan}"
        );
        assert!(!plan.contains("SCAN"), "got plan: {plan}");
    }

    /// Guards the column order. An index on `(base_sk_s, base_pk)` would also
    /// satisfy the assertions above, but would not serve a `base_pk`-only
    /// lookup, so pin that the index leads with `base_pk`.
    #[tokio::test]
    async fn base_key_index_leads_with_base_pk() {
        let pool = make_index_table(
            &[hash("gsi_pk"), range("gsi_sk")],
            &[
                attr("gsi_pk", ScalarAttributeType::S),
                attr("gsi_sk", ScalarAttributeType::S),
            ],
            &[hash("id"), range("ts")],
            &[
                attr("id", ScalarAttributeType::S),
                attr("ts", ScalarAttributeType::S),
            ],
        )
        .await;

        let cols: Vec<(i64, i64, Option<String>)> =
            sqlx::query_as("PRAGMA index_info(\"idx_base_key_probe\")")
                .fetch_all(&pool)
                .await
                .unwrap();
        let names: Vec<String> = cols.into_iter().filter_map(|c| c.2).collect();

        assert_eq!(
            names,
            vec!["base_pk".to_owned(), "base_sk_s".to_owned()],
            "base-key index must lead with base_pk"
        );
    }
}
