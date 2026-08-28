// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan implementation for the Cassandra backend.
//!
//! Implements `DataEngine::scan` for both base tables and secondary indexes.
//!
//! # Design
//!
//! Unlike PostgreSQL — which can express a full table scan as `ORDER BY pk, sk`
//! with a row-value comparison (`(pk, sk) > (?, ?)`) for pagination — Cassandra
//! cannot order by or range-compare the *partition key* directly, because
//! partitions are distributed by token (hash). A full table scan in Cassandra
//! therefore walks the cluster in **token order** and pagination is expressed in
//! terms of `token(pk)`.
//!
//! ## Pagination (`ExclusiveStartKey`)
//!
//! Resuming after a key `(P, S)` requires two queries, mirroring the two-query
//! pattern already used by `query_impl` for index pagination (Cassandra has no
//! `OR`):
//!
//! 1. **Finish the current partition** (only when the table/index has a sort
//!    key): `WHERE pk = ? AND <clustering> > ?` — returns the remaining
//!    clustering rows in partition `P` after `S`.
//! 2. **Move to subsequent partitions**: `WHERE token(pk) > token(?)` — returns
//!    rows in all partitions that sort after `P` in the token ring.
//!
//! Results are concatenated (query 1 then query 2), which preserves the global
//! `(token(pk), clustering...)` ordering, then truncated to the requested limit.
//!
//! All values are passed as bound parameters (`?`), matching `query_impl` and
//! ADR-0002 — never interpolated as CQL literals.
//!
//! ## Parallel scan (`Segment` / `TotalSegments`)
//!
//! The token ring (`i64::MIN..=i64::MAX` for the Murmur3 partitioner) is split
//! into `TotalSegments` contiguous ranges. Each segment restricts the scan with
//! `token(pk) >= ? AND token(pk) <= ?` (bound `bigint` token bounds). This is the
//! idiomatic Cassandra token-range split and composes with pagination by
//! additionally bounding the "next partitions" query with the segment's upper
//! token bound.
//!
//! ## Known limitation
//!
//! Token-based pagination can, in theory, skip partition keys that hash to the
//! exact same 64-bit token as the resume key (a ~1-in-2^64 event per boundary).
//! This is an inherent trade-off of stateless token-range scanning and matches
//! the "token range pagination" approach approved in the workplan.

use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::value::Value;
use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, composite_pk_to_text, parse_sk, sk_column, sk_info};
use std::sync::Arc;

use super::ddl::{data_table_name, index_table_name};
use super::index::fetch_index_by_name;
use super::json_to_item;
use crate::cassandra_util::{self, CassandraSession};

/// Compute the inclusive `[lower, upper]` token bounds for a parallel-scan
/// segment, or `None` when the whole ring should be scanned.
///
/// Returns `None` when segment/total are absent or `total_segments == 1`
/// (single segment covers the entire ring, so no token predicate is needed).
fn segment_token_bounds(segment: Option<i64>, total_segments: Option<i64>) -> Option<(i64, i64)> {
    let (seg, total) = match (segment, total_segments) {
        (Some(s), Some(t)) if t > 1 && s >= 0 && s < t => (i128::from(s), i128::from(t)),
        _ => return None,
    };

    let min = i128::from(i64::MIN);
    let max = i128::from(i64::MAX);
    let ring = max - min + 1; // 2^64
    let span = ring / total;

    let lower = min + span * seg;
    let upper = if seg == total - 1 {
        max
    } else {
        min + span * (seg + 1) - 1
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    Some((lower as i64, upper as i64))
}

/// Convert a parsed sort-key value into a bound parameter `Value`, matching the
/// per-type binding used by `query_impl` so scan pagination binds values
/// consistently with the query path.
fn sk_to_value(sk: &SortKeyValue) -> Value {
    match sk {
        SortKeyValue::S(s) => Value::from(s.as_str()),
        SortKeyValue::N(n) => super::decimal_to_value(n),
        SortKeyValue::B(b) => Value::from(b.clone()),
    }
}

/// Execute a scan query that has no bind parameters and return the rows.
///
/// Used for the first page (no `ExclusiveStartKey`), where the only predicates
/// are literal token-range bounds.
async fn rows_no_values(
    session: &Arc<CassandraSession>,
    query: &str,
) -> Result<Vec<cdrs_tokio::types::rows::Row>, StorageError> {
    let result = session
        .query(query)
        .await
        .map_err(|e| StorageError::Internal(format!("scan query failed: {e}")))?;
    let body = result
        .response_body()
        .map_err(|e| StorageError::Internal(format!("scan parse failed: {e}")))?;
    Ok(body.into_rows().unwrap_or_default())
}

impl crate::CassandraEngine {
    /// Implementation of `DataEngine::scan`.
    ///
    /// Returns up to `limit` items (the engine layer applies any
    /// `FilterExpression`, `ProjectionExpression`, and the 1 MB cap) along with
    /// a `LastEvaluatedKey` when more items remain.
    pub(crate) async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let session = self.session_arc();

        // Resolve the Cassandra table to scan and the key schema that describes
        // its `pk`/`sk` columns (index key schema for index scans).
        let (table_name, scan_key_schema) = if let Some(idx_name) = index_name {
            let index = fetch_index_by_name(
                &key_info.table_id,
                idx_name,
                &session,
                &self.catalog_keyspace(),
            )
            .await?;
            (index_table_name(&index.index_id), index.key_schema)
        } else {
            (
                data_table_name(&key_info.table_id),
                key_info.key_schema.clone(),
            )
        };

        let account_keyspace = self.account_keyspace(&key_info.account_id);
        let select_prefix = format!("SELECT item_data FROM {account_keyspace}.{table_name}");

        // Sort-key info for the scanned table/index, and (for index scans) the
        // base table's sort key — both are clustering columns on the index table.
        let sk_info_opt = sk_info(&scan_key_schema, &key_info.attribute_definitions);
        let base_sk_info_opt = if index_name.is_some() {
            sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
        } else {
            None
        };

        let token_bounds = segment_token_bounds(segment, total_segments);
        let token_upper = token_bounds.map(|(_, hi)| hi);

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let fetch_limit = actual_limit + 1; // fetch one extra to detect more pages

        tracing::debug!(
            account_id = %key_info.account_id,
            table_id = %key_info.table_id,
            index = ?index_name,
            segment = ?segment,
            total_segments = ?total_segments,
            paginating = exclusive_start_key.is_some(),
            "scan: executing"
        );

        let rows = if let Some(start_key) = exclusive_start_key {
            self.scan_paginated(
                &session,
                &select_prefix,
                &scan_key_schema,
                key_info,
                sk_info_opt,
                base_sk_info_opt,
                index_name.is_some(),
                start_key,
                token_upper,
                fetch_limit,
            )
            .await?
        } else {
            // First page: full scan within the (optional) segment token range.
            let mut query = select_prefix.clone();
            let mut values: Vec<Value> = Vec::new();
            if let Some((lo, hi)) = token_bounds {
                query.push_str(" WHERE token(pk) >= ? AND token(pk) <= ?");
                values.push(Value::from(lo));
                values.push(Value::from(hi));
            }
            query.push_str(&format!(" LIMIT {fetch_limit}"));
            if values.is_empty() {
                // Unrestricted full scan — no bind parameters.
                rows_no_values(&session, &query).await?
            } else {
                cassandra_util::query_rows::<StorageError>(
                    &session,
                    &query,
                    QueryValues::SimpleValues(values),
                    "scan_first_page",
                )
                .await?
            }
        };

        // Parse item_data JSON into items.
        let items: Vec<Item> = rows
            .into_iter()
            .map(|row| {
                let json_str: String = cassandra_util::get_column(&row, "item_data", "scan parse")?;
                json_to_item(json_str)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Enforce the limit and derive the LastEvaluatedKey from the last item.
        let has_more = items.len() > actual_limit;
        let final_items: Vec<Item> = items.into_iter().take(actual_limit).collect();

        let last_key = if has_more {
            final_items
                .last()
                .map(|item| extenddb_core::types::extract_key(item, &scan_key_schema))
        } else {
            None
        };

        tracing::debug!(
            item_count = final_items.len(),
            has_more = has_more,
            "scan: fetched items"
        );

        Ok((final_items, last_key))
    }

    /// Token-based two-query pagination for `scan_impl`.
    #[allow(clippy::too_many_arguments)]
    async fn scan_paginated(
        &self,
        session: &Arc<CassandraSession>,
        select_prefix: &str,
        scan_key_schema: &[extenddb_core::types::KeySchemaElement],
        key_info: &TableKeyInfo,
        sk_info_opt: Option<(&str, ScalarAttributeType)>,
        base_sk_info_opt: Option<(&str, ScalarAttributeType)>,
        is_index: bool,
        start_key: &Item,
        token_upper: Option<i64>,
        fetch_limit: usize,
    ) -> Result<Vec<cdrs_tokio::types::rows::Row>, StorageError> {
        // Partition key text of the resume row (single or composite HASH key).
        let pk_text = composite_pk_to_text(start_key, scan_key_schema)?;

        let mut all_rows = Vec::new();

        // "Next partitions" upper token bound (bound parameter, if segmented).
        let token_clause: &str = if token_upper.is_some() {
            " AND token(pk) <= ?"
        } else {
            ""
        };

        // ── Query 1: finish the resume partition (only if there are clustering
        //    columns to advance past — i.e. the table/index has a sort key, or an
        //    index carries base-table key clustering columns). ──────────────────
        let q1_clustering = self.build_resume_clustering(
            key_info,
            sk_info_opt,
            base_sk_info_opt,
            is_index,
            start_key,
        )?;

        if let Some((clustering_predicate, clustering_binds)) = q1_clustering {
            let query1 = format!(
                "{select_prefix} WHERE pk = ? AND {clustering_predicate} LIMIT {fetch_limit}"
            );
            let mut values: Vec<Value> = Vec::with_capacity(1 + clustering_binds.len());
            values.push(Value::from(pk_text.as_str()));
            values.extend(clustering_binds);
            let rows1 = cassandra_util::query_rows::<StorageError>(
                session,
                &query1,
                QueryValues::SimpleValues(values),
                "scan_finish_partition",
            )
            .await?;
            all_rows.extend(rows1);
        }

        // ── Query 2: subsequent partitions in token order. ──────────────────────
        if all_rows.len() < fetch_limit {
            let remaining = fetch_limit - all_rows.len();
            let query2 = format!(
                "{select_prefix} WHERE token(pk) > token(?){token_clause} LIMIT {remaining}"
            );
            let mut values: Vec<Value> = vec![Value::from(pk_text.as_str())];
            if let Some(hi) = token_upper {
                values.push(Value::from(hi));
            }
            let rows2 = cassandra_util::query_rows::<StorageError>(
                session,
                &query2,
                QueryValues::SimpleValues(values),
                "scan_next_partitions",
            )
            .await?;
            all_rows.extend(rows2);
        }

        Ok(all_rows)
    }

    /// Build the clustering-comparison predicate for query 1 of pagination
    /// (the part after `pk = ? AND`), plus the bound values it references.
    ///
    /// Returns `None` when the partition has no clustering columns to advance
    /// past (hash-only base table), in which case query 1 is skipped.
    ///
    /// - Base table with sort key: `sk_col > ?`.
    /// - Index scan: a multi-column clustering tuple comparison
    ///   `(idx_sk?, base_pk, base_sk?) > (?, …)`.
    ///
    /// All comparison values are returned as bound parameters (never CQL
    /// literals), matching `query_impl`.
    fn build_resume_clustering(
        &self,
        key_info: &TableKeyInfo,
        sk_info_opt: Option<(&str, ScalarAttributeType)>,
        base_sk_info_opt: Option<(&str, ScalarAttributeType)>,
        is_index: bool,
        start_key: &Item,
    ) -> Result<Option<(String, Vec<Value>)>, StorageError> {
        if !is_index {
            // Base table: single clustering column (the sort key), if any.
            let Some((sk_name, sk_type)) = sk_info_opt else {
                return Ok(None); // hash-only base table → no query 1
            };
            let sk_av = start_key.get(sk_name).ok_or_else(|| {
                StorageError::Validation(
                    "The provided starting key is invalid: missing sort key".to_owned(),
                )
            })?;
            let sk_val = parse_sk(sk_av, sk_type)?;
            let col = sk_column(sk_type);
            return Ok(Some((format!("{col} > ?"), vec![sk_to_value(&sk_val)])));
        }

        // Index scan: clustering = (index sort key?, base_pk, base sort key?).
        let mut cols: Vec<String> = Vec::new();
        let mut binds: Vec<Value> = Vec::new();

        if let Some((idx_sk_name, idx_sk_type)) = sk_info_opt {
            let av = start_key.get(idx_sk_name).ok_or_else(|| {
                StorageError::Validation(
                    "The provided starting key is invalid: missing index sort key".to_owned(),
                )
            })?;
            cols.push(sk_column(idx_sk_type).to_owned());
            binds.push(sk_to_value(&parse_sk(av, idx_sk_type)?));
        }

        // base_pk clustering column (always present on index tables).
        let base_pk_text = composite_pk_to_text(start_key, &key_info.base_key_schema)?;
        cols.push("base_pk".to_owned());
        binds.push(Value::from(base_pk_text));

        if let Some((base_sk_name, base_sk_type)) = base_sk_info_opt
            && let Some(av) = start_key.get(base_sk_name) {
                cols.push(format!("base_{}", sk_column(base_sk_type)));
                binds.push(sk_to_value(&parse_sk(av, base_sk_type)?));
            }

        Ok(Some((format_clustering_comparison(&cols), binds)))
    }
}

/// Render a clustering "greater-than" predicate with bound-parameter
/// placeholders. Uses a plain `col > ?` comparison for a single column and the
/// multi-column tuple form `(c1, c2) > (?, ?)` for two or more columns.
fn format_clustering_comparison(cols: &[String]) -> String {
    if cols.len() == 1 {
        format!("{} > ?", cols[0])
    } else {
        let placeholders = vec!["?"; cols.len()].join(", ");
        format!("({}) > ({})", cols.join(", "), placeholders)
    }
}
