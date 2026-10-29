// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DDL helpers for creating and dropping per-DynamoDB-table data tables in `PostgreSQL`.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, Projection, StreamSpecification,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n};

use super::{all_sort_key_info, data_table_name, index_table_name, vector_table_name};

/// Column type for every text key column (`pk`, `sk_s`, `base_pk`,
/// `base_sk_s`, and their numbered variants).
///
/// Amazon DynamoDB orders string keys by UTF-8 byte value (measured
/// 2026-09-16: `A < B < Z < _ < a < a_z < ab < b < z < é < 中`, with
/// `BETWEEN` and `begins_with` following the same comparison). The query
/// builders express that with `COLLATE "C"` on every string key predicate and
/// `ORDER BY`. Declaring the columns with the same collation makes the
/// primary key and the ordering indexes serve those predicates; a column left
/// on the database collation (`en_US.utf8` in the default setups) forces
/// PostgreSQL to seek on `pk` alone and filter the sort key afterwards,
/// which re-reads a partition from its start on every page.
///
/// Tables created before this constant existed keep the database collation
/// on these columns. Their results are still ordered correctly, because the
/// predicates carry the collation explicitly; only the index use differs.
const TEXT_KEY: &str = "TEXT COLLATE \"C\"";
use crate::PostgresEngine;

/// Row shape returned by the table-info query: (`key_schema`, `attr_defs`, status, `table_id`, `stream_spec`).
type TableInfoRow = (
    serde_json::Value,
    serde_json::Value,
    String,
    String,
    Option<serde_json::Value>,
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
                    pk {TEXT_KEY} NOT NULL PRIMARY KEY,
                    item_data JSONB NOT NULL
                )"
            )
        } else if sk_infos.len() == 1 {
            // Backward-compatible single SK path
            let sk_col = sk_column(sk_infos[0].1);
            format!(
                r"CREATE TABLE {ddb_table} (
                    pk {TEXT_KEY} NOT NULL,
                    sk_s {TEXT_KEY},
                    sk_n NUMERIC,
                    sk_b BYTEA,
                    item_data JSONB NOT NULL,
                    PRIMARY KEY (pk, {sk_col})
                )"
            )
        } else {
            // Multi-part RANGE key: one typed column set per RANGE attribute
            let mut col_defs = vec![format!("pk {TEXT_KEY} NOT NULL")];
            let mut pk_cols = vec!["pk".to_owned()];
            for (i, &(_, sk_type)) in sk_infos.iter().enumerate() {
                let col = sk_column_n(i, sk_type);
                // Add all three type columns for this SK position
                if i == 0 {
                    col_defs.push(format!("sk_s {TEXT_KEY}"));
                    col_defs.push("sk_n NUMERIC".to_owned());
                    col_defs.push("sk_b BYTEA".to_owned());
                } else {
                    let n = i + 1;
                    col_defs.push(format!("sk{n}_s {TEXT_KEY}"));
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

    /// Create a GSI/LSI data table in `PostgreSQL`.
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
        let mut col_defs = vec![format!("pk {TEXT_KEY} NOT NULL")];

        // Index SK columns
        for (i, &(_, _)) in idx_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push(format!("sk_s {TEXT_KEY}"));
                col_defs.push("sk_n NUMERIC".to_owned());
                col_defs.push("sk_b BYTEA".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("sk{n}_s {TEXT_KEY}"));
                col_defs.push(format!("sk{n}_n NUMERIC"));
                col_defs.push(format!("sk{n}_b BYTEA"));
            }
        }

        // Base table key columns for uniqueness
        col_defs.push(format!("base_pk {TEXT_KEY} NOT NULL"));
        for (i, &(_, _)) in base_sks.iter().enumerate() {
            if i == 0 {
                col_defs.push(format!("base_sk_s {TEXT_KEY}"));
                col_defs.push("base_sk_n NUMERIC".to_owned());
                col_defs.push("base_sk_b BYTEA".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("base_sk{n}_s {TEXT_KEY}"));
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

        // Index on base table key columns for delete_index_row_multi lookups.
        {
            let mut base_key_cols = vec!["base_pk".to_owned()];
            for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
                let col = if i == 0 {
                    format!("base_{}", sk_column(sk_type))
                } else {
                    format!("base_{}", sk_column_n(i, sk_type))
                };
                base_key_cols.push(col);
            }
            let idx_name = format!("_ddb_{index_id}_base_key_idx");
            let base_key_idx = format!(
                "CREATE INDEX \"{idx_name}\" ON {idx_table} ({})",
                base_key_cols.join(", ")
            );
            sqlx::query(&base_key_idx)
                .execute(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Create the data table for one vector index.
    ///
    /// Column set is the SQLite backend's, with PostgreSQL types: `embedding` is
    /// pgvector's own `vector(N)` rather than a blob, so distance, ordering and
    /// the row limit all evaluate in the server and the wire carries k rows
    /// instead of the whole candidate set.
    ///
    /// Keyed by the base item rather than by the partition, so one base item
    /// yields at most one vector row and a re-put replaces it. A partition move
    /// is therefore a delete followed by an insert, which is what the apply path
    /// does unconditionally.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Unsupported`] when the server has no pgvector, so
    /// a lost extension reports as a refusal rather than a fault.
    pub(crate) async fn create_vector_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        index_id: &str,
        dimensions: u32,
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let vec_table = vector_table_name(index_id);
        let base_sks = all_sort_key_info(base_key_schema, base_attr_defs);

        let mut col_defs = vec![
            // `pk_to_text` of the HASH element's value, or the unscoped sentinel.
            // Scoping happens on this column, never on the payload.
            //
            // BYTEA rather than TEXT, and not by preference: the shared unscoped
            // sentinel begins with a NUL byte, deliberately, because no partition
            // derived from item data can contain one. PostgreSQL cannot store a NUL
            // in a text column at all, so a text column here rejects every row of
            // every unscoped index. Bytes keep the shared value byte-identical
            // across backends, which is what makes a write and a search agree.
            "part BYTEA NOT NULL".to_owned(),
            format!("base_pk {TEXT_KEY} NOT NULL"),
        ];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            let _ = sk_type;
            if i == 0 {
                col_defs.push(format!("base_sk_s {TEXT_KEY}"));
                col_defs.push("base_sk_n NUMERIC".to_owned());
                col_defs.push("base_sk_b BYTEA".to_owned());
            } else {
                let n = i + 1;
                col_defs.push(format!("base_sk{n}_s {TEXT_KEY}"));
                col_defs.push(format!("base_sk{n}_n NUMERIC"));
                col_defs.push(format!("base_sk{n}_b BYTEA"));
            }
        }
        // NOT NULL because a non-indexable item is never inserted: the apply path
        // deletes instead, so there is no such thing as a row without a vector.
        col_defs.push(format!("embedding vector({dimensions}) NOT NULL"));
        // The L2 norm at write time, kept even though pgvector computes cosine
        // itself: it is what lets the cosine expression return the measured 1.0
        // for a zero vector, where pgvector's operator yields NaN.
        col_defs.push("nrm DOUBLE PRECISION NOT NULL".to_owned());
        col_defs.push("item_data JSONB NOT NULL".to_owned());

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
            .map_err(crate::vector::map_vector_sql_error)?;

        // Every search is partition-scoped, including an unscoped index's search
        // against the sentinel, so this index is what keeps one off a full scan.
        // Left unnamed, as the GSI tables' ordering index is: a name built from the
        // ids would be truncated at PostgreSQL's 63-byte identifier limit.
        let part_idx = format!("CREATE INDEX ON {vec_table} (part)");
        sqlx::query(&part_idx)
            .execute(&mut **tx)
            .await
            .map_err(crate::vector::map_vector_sql_error)?;

        // No ANN index yet: exact scan first, per ADR-0004 and the plan. Adding
        // HNSW later is a CREATE INDEX against this same column with no table
        // rewrite, which is why the column type is `vector(N)` from the start.
        Ok(())
    }

    /// Drop one vector index's data table.
    pub(crate) async fn drop_vector_data_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        index_id: &str,
    ) -> Result<(), StorageError> {
        let vec_table = vector_table_name(index_id);
        sqlx::query(&format!("DROP TABLE IF EXISTS {vec_table}"))
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
             stream_specification \
             FROM tables \
             WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ks_json, ad_json, status, table_id, stream_spec_json) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        match status.as_str() {
            // DynamoDB serves reads and writes on a DELETING table until the
            // table is gone (measured 2026-10-25); a write made in that window
            // goes away with the table. The data tables stay until the control
            // plane drops them, so the data plane reads a DELETING row as found;
            // a statement that reaches a dropped data table is answered as not
            // found by the `DataEngine` implementation.
            "ACTIVE" | "DELETING" => {}
            // A table that is still being created is not usable for data-plane
            // operations; DynamoDB reports it as not found (not as in-use).
            // UPDATING keeps the not-active classification.
            "CREATING" => {
                return Err(StorageError::TableNotFound(table_name.to_owned()));
            }
            _ => return Err(StorageError::TableNotActive(table_name.to_owned())),
        }

        let key_schema: Vec<KeySchemaElement> =
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attribute_definitions: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| StorageError::Internal(e.to_string()))?;

        let stream_specification: Option<StreamSpecification> = stream_spec_json
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch all secondary indexes once and carry their metadata on the
        // (cached) TableKeyInfo. This lets per-index consumed-capacity accounting
        // avoid a separate describe_table round-trip on every write, and also
        // supplies `has_lsi`.
        let (global_secondary_indexes, local_secondary_indexes) =
            self.fetch_all_index_info(&table_id).await?;
        let has_lsi = !local_secondary_indexes.is_empty();
        // Vector indexes ride on the cached key info too, so the engine can
        // validate vector attributes on writes and charge vector write capacity.
        // The update path already passes `key_info.vector_indexes` into the
        // expression evaluator, so populating this is what turns that validation
        // on: no call site changes.
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
    /// path. The extra columns are two short tokens and a boolean, read only on a
    /// key-info cache miss.
    async fn fetch_vector_index_key_info(
        &self,
        table_id: &str,
    ) -> Result<Vec<extenddb_core::types::VectorIndexKeyInfo>, StorageError> {
        let rows: Vec<crate::table_helpers::VectorIndexRow> = sqlx::query_as(&format!(
            "SELECT {} FROM vector_indexes WHERE table_id = $1 ORDER BY index_name",
            crate::table_helpers::VECTOR_INDEX_COLUMNS
        ))
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        extenddb_storage::vector_catalog::vector_index_key_info(
            rows.into_iter().map(Into::into).collect(),
        )
    }

    /// Fetch every secondary index defined on a table, split into
    /// `(global_secondary_indexes, local_secondary_indexes)`.
    async fn fetch_all_index_info(
        &self,
        table_id: &str,
    ) -> Result<(Vec<IndexInfo>, Vec<IndexInfo>), StorageError> {
        let rows: Vec<(String, String, String, serde_json::Value, serde_json::Value)> =
            sqlx::query_as(
                "SELECT index_name, index_type, index_id, key_schema, projection \
                 FROM indexes WHERE table_id = $1",
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
            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: Projection = serde_json::from_value(proj_json)
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

        match status.as_str() {
            // Same classification as `fetch_table_key_info`: a DELETING table
            // is served until it is gone, a CREATING one is not found.
            "ACTIVE" | "DELETING" => {}
            "CREATING" => {
                return Err(StorageError::TableNotFound(table_name.to_owned()));
            }
            _ => return Err(StorageError::TableNotActive(table_name.to_owned())),
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
        // A GSI still being built is not readable: its table is partial, or not
        // there yet. The service answers a Query or Scan naming one with "The
        // table does not have the specified index", the same as for no index at
        // all, which is what `IndexNotFound` becomes on the wire.
        let idx_row: Option<(String, String, serde_json::Value, serde_json::Value)> =
            sqlx::query_as(
                "SELECT index_type, index_id, key_schema, projection FROM indexes \
             WHERE table_id = $1 AND index_name = $2 AND index_status <> 'CREATING'",
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

impl PostgresEngine {
    /// The control plane's drop of a `DELETING` table's data tables and queue
    /// rows, in an order that cannot deadlock with the propagation queue.
    ///
    /// The worker claims a `gsi_pending` row, then takes `FOR SHARE` on the base
    /// row (the state-based apply). A drop that takes the base table first and
    /// then deletes the pending rows waits on the worker's claimed row while the
    /// worker waits on the table: a cycle, and when this side is the victim the
    /// catalog rows are already gone and the data tables stay behind.
    ///
    /// So the queue is stopped first. A `vector_index_holds` row for the table,
    /// committed on its own so the claim query can see it, keeps the worker from
    /// taking any more of the table's rows (the claim already honours holds, for
    /// vector builds). Then the pending rows are deleted, which waits for the
    /// claims already in flight to commit, each of which holds the base row only
    /// until then. Only after that are the tables dropped, with nothing left that
    /// could be waiting on them. The hold is removed in the same transaction as
    /// the drops, so a table id that no longer exists is never left held; a
    /// crash in between leaves a hold the recovery sweep clears by age (ten
    /// minutes for a hold of this kind).
    ///
    /// What this does not cover is a deadlock that does not go through the
    /// queue: the drop waits on a client transaction that waits, through another
    /// table, on a client transaction queued behind the drop. That is the same
    /// exposure any DDL has here and was there before; the server's deadlock
    /// check ends it, and [`lock_wait_failed`] counts a deadlock victim as a
    /// drop to retry.
    ///
    /// Every wait in the dropping transaction shares one `budget`: the
    /// pending-row delete (a claim in flight) runs under it as its lock
    /// timeout, and each `DROP` under what is left of it
    /// ([`drop_table_relations`]). Running out fails the call with SQLSTATE
    /// 55P03 ([`lock_wait_failed`]) and nothing dropped, and the hold stays
    /// until the next attempt or the recovery sweep. `DeleteTable` does not
    /// call this: it deletes the pending rows with no timeout and runs
    /// [`drop_table_relations`] in a transaction of its own
    /// (`delete_table.rs`).
    ///
    /// Errors are returned as `sqlx` errors so a caller can tell a lock wait
    /// from a failure that will repeat.
    pub(crate) async fn drop_table_storage(
        data: &sqlx::PgPool,
        table_id: &str,
        index_ids: &[String],
        vector_index_ids: &[String],
        budget: std::time::Duration,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO vector_index_holds (table_id, index_id) VALUES ($1, $1) \
             ON CONFLICT (table_id, index_id) DO NOTHING",
        )
        .bind(table_id)
        .execute(data)
        .await?;
        let deadline = std::time::Instant::now() + budget;
        let mut tx = data.begin().await?;
        set_lock_timeout_until(&mut tx, deadline).await?;
        delete_pending_queue_rows(&mut tx, table_id).await?;
        drop_table_relations(&mut tx, table_id, index_ids, vector_index_ids, deadline).await?;
        tx.commit().await
    }
}

/// The first statement of a table's storage drop, on `conn`'s open
/// transaction: delete the table's pending queue rows, which waits for the
/// claims in flight to commit ([`PostgresEngine::drop_table_storage`]).
pub(crate) async fn delete_pending_queue_rows(
    conn: &mut sqlx::PgConnection,
    table_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
        .bind(table_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The rest of a table's storage drop, on `conn`'s open transaction after
/// [`delete_pending_queue_rows`]: every relation
/// ([`table_storage_relations`], base table first), then the table's queue
/// holds, all by `deadline`.
///
/// One deadline for the group, not one lock timeout per statement: before
/// each statement `lock_timeout` is set to what is left until `deadline`
/// (at least 1 ms once it has passed, so a statement that would have to wait
/// fails with 55P03 at once and one that need not wait still runs). A lock
/// timeout per `DROP` let a table with many indexes wait up to the timeout
/// once per relation, holding the base table's `ACCESS EXCLUSIVE` (granted
/// first) for all of it. The transaction's `lock_timeout` is reset to its
/// default afterwards, so statements the caller runs next are not bounded
/// by what was left.
///
/// The caller owns the transaction and whatever stops the queue before the
/// drop starts ([`PostgresEngine::drop_table_storage`]).
pub(crate) async fn drop_table_relations(
    conn: &mut sqlx::PgConnection,
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
    deadline: std::time::Instant,
) -> Result<(), sqlx::Error> {
    for relation in table_storage_relations(table_id, index_ids, vector_index_ids) {
        set_lock_timeout_until(conn, deadline).await?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {relation}"))
            .execute(&mut *conn)
            .await?;
    }
    set_lock_timeout_until(conn, deadline).await?;
    sqlx::query("DELETE FROM vector_index_holds WHERE table_id = $1")
        .bind(table_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("SET LOCAL lock_timeout TO DEFAULT")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Set `conn`'s transaction-local `lock_timeout` to the time left until
/// `deadline`, and to 1 ms once it has passed (`0` would disable the
/// timeout).
async fn set_lock_timeout_until(
    conn: &mut sqlx::PgConnection,
    deadline: std::time::Instant,
) -> Result<(), sqlx::Error> {
    let left = deadline.saturating_duration_since(std::time::Instant::now());
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", left.as_millis().max(1)))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// SQLSTATE `lock_not_available` (55P03), which `lock_timeout` raises.
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";

/// Whether `e` is a lock wait that ran out its `lock_timeout` (55P03): a
/// session held a lock the statement needed, which a later attempt can get
/// once that session ends. Every other failure (a dependent object, a
/// missing privilege, a lost connection) is not one.
pub(crate) fn lock_not_available(e: &sqlx::Error) -> bool {
    sqlstate_is(e, &[SQLSTATE_LOCK_NOT_AVAILABLE])
}

/// The SQLSTATEs that end a drop's lock wait without anything being wrong
/// with the drop itself: `lock_not_available` (55P03, its `lock_timeout` ran
/// out), `deadlock_detected` (40P01, the server chose its transaction as the
/// victim of a wait cycle with another session) and `query_canceled` (57014,
/// a `statement_timeout` set for the role, the database or the session that
/// is shorter than the lock timeout, or an administrator's
/// `pg_cancel_backend`). Each rolls back the drop's transaction (or the
/// savepoint it ran under), and a later attempt succeeds once the other
/// session is done.
const LOCK_WAIT_FAILURES: [&str; 3] = [SQLSTATE_LOCK_NOT_AVAILABLE, "40P01", "57014"];

/// Whether a table storage drop failed in one of its lock waits
/// ([`LOCK_WAIT_FAILURES`]), so the table is still in use and the drop is
/// to be retried rather than reported: `DeleteTable` then takes its
/// asynchronous path, and the control plane counts the row as deferred.
/// The one classifier for both.
pub(crate) fn lock_wait_failed(e: &sqlx::Error) -> bool {
    sqlstate_is(e, &LOCK_WAIT_FAILURES)
}

fn sqlstate_is(e: &sqlx::Error, codes: &[&str]) -> bool {
    e.as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|c| codes.contains(&c.as_ref()))
}

/// Which sessions [`storage_in_use`] looks for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LockHolders {
    /// Sessions holding `ACCESS SHARE` (`AccessShareLock`): the long
    /// readers. `DeleteTable` probes for these only (`delete_table.rs`).
    Readers,
    /// Sessions holding any lock on the relations. The control plane probes
    /// for these before it retries a drop it already had to defer
    /// (`worker_store.rs`).
    Any,
}

/// Whether a session other than this connection's holds a granted lock of
/// the `holders` kind on one of `relations` ([`table_storage_relations`])
/// in this database. Runs on `executor`; `pg_locks` is cluster-wide, so the
/// database filter keeps a lock on the same OID in another database from
/// matching. `pid <> pg_backend_pid()` keeps the caller's own locks out.
pub(crate) async fn storage_in_use<'e, E>(
    executor: E,
    relations: Vec<String>,
    holders: LockHolders,
) -> Result<bool, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks \
         WHERE locktype = 'relation' AND granted \
         AND ($2 OR mode = 'AccessShareLock') \
         AND pid <> pg_backend_pid() \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         AND relation = ANY (SELECT to_regclass(name)::oid FROM unnest($1::text[]) AS name))",
    )
    .bind(relations)
    .bind(matches!(holders, LockHolders::Any))
    .fetch_one(executor)
    .await
}

/// Every data relation one table's storage consists of, in drop order: the
/// base data table, then each GSI/LSI data table, then each vector-index data
/// table. The one list of them: the drops ([`drop_table_relations`]) remove
/// exactly these, and `DeleteTable` and the control plane probe exactly these
/// for holders ([`storage_in_use`]), so a relation added to a table's storage
/// is added here and every path sees it.
///
/// The base table goes first. A client write holds the base table (`ROW
/// EXCLUSIVE`) before it writes an LSI or a synchronous GSI in the same
/// transaction, and a queue worker holds a base row `FOR SHARE` before it
/// writes the index table. A drop that holds the base waits for neither on
/// an index; a drop that took an index first would wait for such a session
/// on the base while it waits for the drop on the index, a wait cycle that
/// ends only at a lock timeout or the server's deadlock check.
pub(crate) fn table_storage_relations(
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
) -> Vec<String> {
    std::iter::once(data_table_name(table_id))
        .chain(index_ids.iter().map(|id| index_table_name(id)))
        .chain(vector_index_ids.iter().map(|id| vector_table_name(id)))
        .collect()
}
