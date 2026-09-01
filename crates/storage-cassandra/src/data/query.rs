// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query implementation for Cassandra backend.

use extenddb_core::expression::{CompareOp, ExpressionMaps, KeyCondition, SortKeyCondition};
use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};

/// Extension trait for SortKeyValue to get scalar type.
trait SortKeyValueExt {
    fn scalar_type(&self) -> ScalarAttributeType;
}

impl SortKeyValueExt for SortKeyValue {
    fn scalar_type(&self) -> ScalarAttributeType {
        match self {
            SortKeyValue::S(_) => ScalarAttributeType::S,
            SortKeyValue::N(_) => ScalarAttributeType::N,
            SortKeyValue::B(_) => ScalarAttributeType::B,
        }
    }
}

use super::ddl::data_table_name;
use super::query_helpers::{
    query_with_pk_pk_sk, query_with_pk_sk, query_with_pk_sk_pk, query_with_pk_sk_pk_sk,
    query_with_pk_sk_sk, query_with_pk_sk_sk_sk,
};
use super::{json_to_item, resolve_expr_to_av};
use crate::CassandraEngine;
use crate::cassandra_util;

/// Extra pagination bind values for index queries.
///
/// Cassandra variant of PostgreSQL's PaginationBinds. Unlike PostgreSQL which uses
/// OR clauses, Cassandra requires splitting into two queries when paginating through
/// index results with base table key tie-breakers.
enum PaginationBinds {
    /// Base table query or index query with no extra pagination binds needed.
    None,
    /// Index query where base table has no SK — only `base_pk` as tie-breaker.
    BasePkOnly { pk_text: String },
    /// Index query where base table has a SK — `base_sk` as tie-breaker.
    BaseSkOnly { sk: SortKeyValue },
    /// Hash-only index where base table has a SK — both `base_pk` and `base_sk`.
    BasePkAndSk { pk_text: String, sk: SortKeyValue },
}

/// Compute upper bound for begins_with on strings.
/// Appends the maximum Unicode codepoint to create an exclusive upper bound.
fn string_upper_bound(prefix: &str) -> String {
    format!("{prefix}\u{10FFFF}")
}

/// Compute upper bound for begins_with on binary data.
/// Increments the last byte, extending if needed for overflow.
fn binary_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();

    // Try to increment the last byte
    for i in (0..upper.len()).rev() {
        if upper[i] < 255 {
            upper[i] += 1;
            return upper;
        }
        // This byte is 255, set to 0 and continue to next byte
        upper[i] = 0;
    }

    // All bytes were 255 - prepend a 1 byte
    let mut result = vec![1];
    result.extend_from_slice(&upper);
    result
}

/// Helper to determine base table sort key info for index queries.
///
/// Matches PostgreSQL pattern where base_sk_info is derived from base_key_schema.
/// Used for ORDER BY (sub-sort when index SKs equal) and pagination (compound keys).
fn base_sk_info(
    key_info: &TableKeyInfo,
    index_name: Option<&str>,
) -> Option<(String, ScalarAttributeType)> {
    if index_name.is_some() {
        sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
            .map(|(name, ty)| (name.to_owned(), ty))
    } else {
        None
    }
}

impl CassandraEngine {
    /// Implementation of DataEngine::query.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_impl(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        _maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        // Determine table to query (base or index)
        let (table_name, query_key_schema, is_lsi) = if let Some(idx_name) = index_name {
            let catalog_keyspace = self.catalog_keyspace();
            let index = super::index::fetch_index_by_name(
                &key_info.table_id,
                idx_name,
                &self.session_arc(),
                &catalog_keyspace,
            )
            .await?;
            let is_lsi = index.index_type == "LSI";
            (
                super::ddl::index_table_name(&index.index_id),
                index.key_schema,
                is_lsi,
            )
        } else {
            (
                data_table_name(&key_info.table_id),
                key_info.key_schema.clone(),
                false,
            )
        };

        // Step 1: Resolve partition key value
        let pk_av = resolve_expr_to_av(&key_condition.pk_value, _maps)?;
        let pk_text = pk_to_text(&pk_av)?.into_owned();

        // Step 2: Determine if there's a sort key condition
        let sk_info_opt = sk_info(&query_key_schema, &key_info.attribute_definitions);

        // Step 3: Build query
        let account_keyspace = self.account_keyspace(&key_info.account_id);

        let mut query =
            format!("SELECT item_data FROM {account_keyspace}.{table_name} WHERE pk = ?");

        // Step 4: Add sort key condition if present
        let sk_value_opt = if let (Some(sk_cond), Some((_, sk_type))) =
            (&key_condition.sk_condition, sk_info_opt)
        {
            use SortKeyCondition;
            use extenddb_storage::util::{parse_sk, sk_column};

            let sk_col = sk_column(sk_type);

            match sk_cond {
                SortKeyCondition::Compare { op, value, .. } => {
                    let op_str = match op {
                        CompareOp::Eq => "=",
                        CompareOp::Ne => "!=",
                        CompareOp::Lt => "<",
                        CompareOp::Le => "<=",
                        CompareOp::Gt => ">",
                        CompareOp::Ge => ">=",
                    };

                    query.push_str(&format!(" AND {sk_col} {op_str} ?"));

                    // Resolve and parse SK value
                    let sk_av = resolve_expr_to_av(value, _maps)?;
                    let sk_val = parse_sk(&sk_av, sk_type)?;
                    Some((sk_col, vec![sk_val]))
                }
                SortKeyCondition::Between { low, high, .. } => {
                    query.push_str(&format!(" AND {sk_col} >= ? AND {sk_col} <= ?"));

                    // Resolve and parse both bounds
                    let low_av = resolve_expr_to_av(low, _maps)?;
                    let high_av = resolve_expr_to_av(high, _maps)?;
                    let low_sk = parse_sk(&low_av, sk_type)?;
                    let high_sk = parse_sk(&high_av, sk_type)?;
                    Some((sk_col, vec![low_sk, high_sk]))
                }
                SortKeyCondition::BeginsWith { prefix, .. } => {
                    query.push_str(&format!(" AND {sk_col} >= ? AND {sk_col} < ?"));

                    // Resolve prefix
                    let prefix_av = resolve_expr_to_av(prefix, _maps)?;
                    let prefix_sk = parse_sk(&prefix_av, sk_type)?;

                    // Compute upper bound based on type
                    let upper_sk = match &prefix_sk {
                        SortKeyValue::S(s) => SortKeyValue::S(string_upper_bound(s)),
                        SortKeyValue::B(b) => SortKeyValue::B(binary_upper_bound(b)),
                        SortKeyValue::N(_) => {
                            return Err(StorageError::Validation(
                                "BeginsWith is not supported for numeric sort keys".to_owned(),
                            ));
                        }
                    };

                    Some((sk_col, vec![prefix_sk, upper_sk]))
                }
            }
        } else {
            None
        };

        // Step 4b: Determine pagination strategy
        // For index queries with base table key tie-breakers, we need two queries
        // since Cassandra doesn't support OR clauses like PostgreSQL.
        let base_sk_info_val = base_sk_info(key_info, index_name);

        // Need two-query pagination if it's an index query with pagination that has
        // compound keys requiring tie-breakers (index SK + base keys, or just base PK+SK)
        let needs_two_query_pagination = index_name.is_some()
            && exclusive_start_key.is_some()
            && (base_sk_info_val.is_some() || sk_info_opt.is_some());

        // Special case: hash-only index needs base_pk pagination but uses single query
        let is_hash_only_index = index_name.is_some() && sk_info_opt.is_none();

        let pagination_sk_opt = if let Some(start_key) = exclusive_start_key {
            if let Some((_, sk_type)) = sk_info_opt {
                // Table/index has sort key
                let sk_name = &query_key_schema[1].attribute_name;
                if let Some(start_sk_val) = start_key.get(sk_name) {
                    let sk_val = parse_sk(start_sk_val, sk_type)?;
                    if !needs_two_query_pagination {
                        // Simple pagination - add to query now
                        let sk_col = sk_column(sk_type);
                        let cmp = if forward { ">" } else { "<" };
                        query.push_str(&format!(" AND {sk_col} {cmp} ?"));
                    }
                    // else: two-query logic will handle it below
                    Some(sk_val)
                } else {
                    None
                }
            } else if is_hash_only_index {
                // Hash-only index: paginate on base_pk
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                if start_key.get(base_pk_attr).is_some() {
                    if !needs_two_query_pagination {
                        // Will be handled in Query 2 section for two-query path
                        query.push_str(" AND base_pk > ?");
                    }
                    None // Returning None, will use base_pk from start_key in execution
                } else {
                    None
                }
            } else {
                // PK-only base table with start key means no more items for this partition
                return Ok((Vec::new(), None));
            }
        } else {
            None
        };

        // Step 5: Add ORDER BY if table has sort key
        // (skipped for two-query pagination - each subquery adds its own ORDER BY/LIMIT)
        if !needs_two_query_pagination {
            if let Some((_, sk_type)) = sk_info_opt {
                let sk_col = sk_column(sk_type);
                let direction = if forward { "ASC" } else { "DESC" };
                query.push_str(&format!(" ORDER BY {sk_col} {direction}"));
            }

            // Step 6: Add LIMIT (fetch limit + 1 to detect pagination)
            // Default limit is 1,000,000 per DynamoDB behavior
            let fetch_limit = limit.map_or(1_000_001, |l| l.max(0) + 1);
            query.push_str(&format!(" LIMIT {fetch_limit}"));
        }

        tracing::debug!(
            account_id = %key_info.account_id,
            table_id = %key_info.table_id,
            pk = %pk_text,
            has_sk = sk_value_opt.is_some(),
            forward = forward,
            limit = limit,
            needs_two_queries = needs_two_query_pagination,
            "query: executing"
        );

        // Step 7: Execute query with appropriate parameters
        let rows = if needs_two_query_pagination {
            // Two-query pagination for indexes with base table key tie-breakers
            // Query 1: sk = start_sk AND base_key > start_base_key (finish current SK)
            // Query 2: sk > start_sk (move to next SK values)

            let start_key = exclusive_start_key.unwrap(); // Safe: needs_two_query_pagination requires this
            let start_sk = pagination_sk_opt.as_ref(); // May be None for hash-only index

            tracing::debug!(
                "Two-query pagination: sk_info={:?}, base_sk_info={:?}",
                sk_info_opt,
                base_sk_info_val
            );

            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
            let fetch_limit = actual_limit + 1;

            // Build pagination binds for base table keys
            let pagination_binds = if let Some((ref base_sk_name, base_sk_type)) = base_sk_info_val
            {
                // Index with base SK tie-breaker.
                // The index clustering order is (sk_*, base_pk, base_sk_*), so to
                // restrict base_sk_* we also need base_pk. Capture both.
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                let base_pk = start_key
                    .get(base_pk_attr.as_str())
                    .map(pk_to_text)
                    .transpose()?
                    .map(std::borrow::Cow::into_owned);
                if let Some(base_sk_val) = start_key.get(base_sk_name.as_str()) {
                    let sk = parse_sk(base_sk_val, base_sk_type)?;
                    if let Some(pk_text) = base_pk {
                        PaginationBinds::BasePkAndSk { pk_text, sk }
                    } else {
                        PaginationBinds::BaseSkOnly { sk }
                    }
                } else {
                    PaginationBinds::None
                }
            } else if sk_info_opt.is_some() {
                // Index with SK but no base SK — use base_pk
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                if let Some(pk_val) = start_key.get(base_pk_attr.as_str()) {
                    let pk_text = pk_to_text(pk_val)?.into_owned();
                    PaginationBinds::BasePkOnly { pk_text }
                } else {
                    PaginationBinds::None
                }
            } else {
                // Hash-only index
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                let base_pk = start_key
                    .get(base_pk_attr.as_str())
                    .map(pk_to_text)
                    .transpose()?
                    .map(std::borrow::Cow::into_owned);
                match (base_pk, &base_sk_info_val) {
                    (Some(pk_text), Some((sk_name, sk_type))) => {
                        if let Some(sk_val) = start_key.get(sk_name.as_str()) {
                            let sk = parse_sk(sk_val, *sk_type)?;
                            PaginationBinds::BasePkAndSk { pk_text, sk }
                        } else {
                            PaginationBinds::BasePkOnly { pk_text }
                        }
                    }
                    (Some(pk_text), None) => PaginationBinds::BasePkOnly { pk_text },
                    _ => PaginationBinds::None,
                }
            };

            // Build and execute queries based on pagination_binds
            let mut all_rows = Vec::new();

            // Query 1: finish current SK value (if we have base key binds)
            match (&pagination_binds, sk_info_opt) {
                (PaginationBinds::BaseSkOnly { sk: base_sk }, Some((_, sk_type)))
                    if start_sk.is_some() =>
                {
                    let start_sk = start_sk.unwrap(); // Safe: checked is_some()
                    let sk_col = sk_column(sk_type);
                    let base_sk_col = format!("base_{}", sk_column(base_sk.scalar_type()));
                    let base_cmp = if is_lsi && !forward { "<" } else { ">" };
                    let query1 = format!(
                        "{} AND {} = ? AND {} {} ? ORDER BY {} {}, {} {} LIMIT {}",
                        query,
                        sk_col,
                        base_sk_col,
                        base_cmp,
                        sk_col,
                        if forward { "ASC" } else { "DESC" },
                        base_sk_col,
                        if is_lsi && !forward { "DESC" } else { "ASC" },
                        fetch_limit
                    );

                    let rows1 = query_with_pk_sk_sk(
                        &self.session_arc(),
                        &query1,
                        &pk_text,
                        start_sk,
                        base_sk,
                        "same_sk",
                    )
                    .await?;
                    all_rows.extend(rows1);
                }
                (
                    PaginationBinds::BasePkAndSk {
                        pk_text: base_pk_text,
                        sk: base_sk,
                    },
                    Some((_, sk_type)),
                ) if start_sk.is_some() => {
                    // LSI pagination: clustering order is (sk_*, base_pk, base_sk_*).
                    // Must restrict sk_* = start_sk AND base_pk = base_pk AND base_sk_* > base_sk.
                    let start_sk = start_sk.unwrap();
                    let sk_col = sk_column(sk_type);
                    let base_sk_col = format!("base_{}", sk_column(base_sk.scalar_type()));
                    let base_cmp = if is_lsi && !forward { "<" } else { ">" };
                    let query1 = format!(
                        "{} AND {} = ? AND base_pk = ? AND {} {} ? ORDER BY {} {}, base_pk {}, {} {} LIMIT {}",
                        query,
                        sk_col,
                        base_sk_col,
                        base_cmp,
                        sk_col,
                        if forward { "ASC" } else { "DESC" },
                        if forward { "ASC" } else { "DESC" },
                        base_sk_col,
                        if forward { "ASC" } else { "DESC" },
                        fetch_limit
                    );

                    let rows1 = query_with_pk_sk_pk_sk(
                        &self.session_arc(),
                        &query1,
                        &pk_text,
                        start_sk,
                        base_pk_text,
                        base_sk,
                        "same_sk",
                    )
                    .await?;
                    all_rows.extend(rows1);
                }
                (
                    PaginationBinds::BasePkOnly {
                        pk_text: base_pk_text,
                    },
                    Some((_, sk_type)),
                ) if start_sk.is_some() => {
                    let start_sk = start_sk.unwrap();
                    let sk_col = sk_column(sk_type);
                    let cmp = if forward { ">" } else { "<" };
                    let dir = if forward { "ASC" } else { "DESC" };
                    let query1 = format!(
                        "{query} AND {sk_col} = ? AND base_pk {cmp} ? ORDER BY {sk_col} {dir}, base_pk {dir} LIMIT {fetch_limit}"
                    );

                    let rows1 = query_with_pk_sk_pk(
                        &self.session_arc(),
                        &query1,
                        &pk_text,
                        start_sk,
                        base_pk_text,
                        "same_sk",
                    )
                    .await?;
                    all_rows.extend(rows1);
                }
                (
                    PaginationBinds::BasePkOnly {
                        pk_text: base_pk_text,
                    },
                    None,
                )
                | (
                    PaginationBinds::BasePkAndSk {
                        pk_text: base_pk_text,
                        ..
                    },
                    None,
                ) => {
                    // Hash-only index with base_pk pagination
                    let query1 = if let PaginationBinds::BasePkAndSk { sk: base_sk, .. } =
                        &pagination_binds
                    {
                        let base_sk_col = format!("base_{}", sk_column(base_sk.scalar_type()));
                        format!(
                            "{} AND base_pk = ? AND {} > ? ORDER BY base_pk {}, {} {} LIMIT {}",
                            query,
                            base_sk_col,
                            if forward { "ASC" } else { "DESC" },
                            base_sk_col,
                            if forward { "ASC" } else { "DESC" },
                            fetch_limit
                        )
                    } else {
                        format!(
                            "{} AND base_pk = ? ORDER BY base_pk {} LIMIT {}",
                            query,
                            if forward { "ASC" } else { "DESC" },
                            fetch_limit
                        )
                    };

                    let rows1 = if let PaginationBinds::BasePkAndSk { sk: base_sk, .. } =
                        &pagination_binds
                    {
                        query_with_pk_pk_sk(
                            &self.session_arc(),
                            &query1,
                            &pk_text,
                            base_pk_text,
                            base_sk,
                            "same_sk",
                        )
                        .await?
                    } else {
                        cassandra_util::query_rows(
                            &self.session_arc(),
                            &query1,
                            cdrs_tokio::query_values!(pk_text.as_str(), base_pk_text.as_str()),
                            "same_sk",
                        )
                        .await?
                    };
                    all_rows.extend(rows1);
                }
                _ => {}
            }

            // Query 2: next SK values (only if we haven't reached limit yet)
            if all_rows.len() < fetch_limit && sk_info_opt.is_some() {
                if let Some(start_sk) = start_sk {
                    let remaining = fetch_limit - all_rows.len();
                    let query2 = if let Some((_, sk_type)) = sk_info_opt {
                        let sk_col = sk_column(sk_type);
                        let cmp = if forward { ">" } else { "<" };
                        let order_clause = if let Some((_, base_sk_type)) = &base_sk_info_val {
                            let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                            let dir = if forward { "ASC" } else { "DESC" };
                            format!(
                                " ORDER BY {sk_col} {dir}, base_pk {dir}, {base_sk_col} {dir} LIMIT {remaining}"
                            )
                        } else {
                            format!(
                                " ORDER BY {} {}, base_pk {} LIMIT {}",
                                sk_col,
                                if forward { "ASC" } else { "DESC" },
                                if forward { "ASC" } else { "DESC" },
                                remaining
                            )
                        };
                        format!("{query} AND {sk_col} {cmp} ?{order_clause}")
                    } else {
                        // Hash-only index
                        let order_clause = if let Some((_, base_sk_type)) = &base_sk_info_val {
                            let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                            format!(
                                " ORDER BY base_pk {}, {} {} LIMIT {}",
                                if forward { "ASC" } else { "DESC" },
                                base_sk_col,
                                if forward { "ASC" } else { "DESC" },
                                remaining
                            )
                        } else {
                            format!(
                                " ORDER BY base_pk {} LIMIT {}",
                                if forward { "ASC" } else { "DESC" },
                                remaining
                            )
                        };
                        format!("{query} AND base_pk > ?{order_clause}")
                    };

                    let rows2 = match &pagination_binds {
                        PaginationBinds::BaseSkOnly { .. }
                        | PaginationBinds::BasePkOnly { .. }
                        | PaginationBinds::BasePkAndSk { .. }
                            if sk_info_opt.is_some() =>
                        {
                            query_with_pk_sk(
                                &self.session_arc(),
                                &query2,
                                &pk_text,
                                start_sk,
                                "next_sk",
                            )
                            .await?
                        }
                        PaginationBinds::BasePkOnly {
                            pk_text: base_pk_text,
                        }
                        | PaginationBinds::BasePkAndSk {
                            pk_text: base_pk_text,
                            ..
                        } => {
                            cassandra_util::query_rows(
                                &self.session_arc(),
                                &query2,
                                cdrs_tokio::query_values!(pk_text.as_str(), base_pk_text.as_str()),
                                "next_sk",
                            )
                            .await?
                        }
                        _ => Vec::new(),
                    };
                    all_rows.extend(rows2);
                } // end if let Some(start_sk)
            }

            all_rows
        } else {
            // Single-query execution (existing logic)
            let rows_result = match (sk_value_opt, pagination_sk_opt) {
                (Some((_, sk_vals)), Some(page_sk)) => {
                    // Query with SK condition AND pagination
                    if sk_vals.len() == 1 {
                        // Compare condition + pagination
                        query_with_pk_sk_sk(
                            &self.session_arc(),
                            &query,
                            &pk_text,
                            &sk_vals[0],
                            &page_sk,
                            "query",
                        )
                        .await?
                    } else {
                        // Between condition (2 values) + pagination
                        query_with_pk_sk_sk_sk(
                            &self.session_arc(),
                            &query,
                            &pk_text,
                            &sk_vals[0],
                            &sk_vals[1],
                            &page_sk,
                            "query",
                        )
                        .await?
                    }
                }
                (Some((_, sk_vals)), None) => {
                    // Query with SK condition only (no pagination)
                    if sk_vals.len() == 1 {
                        // Compare condition
                        query_with_pk_sk(
                            &self.session_arc(),
                            &query,
                            &pk_text,
                            &sk_vals[0],
                            "query",
                        )
                        .await?
                    } else {
                        // Between condition (2 values)
                        query_with_pk_sk_sk(
                            &self.session_arc(),
                            &query,
                            &pk_text,
                            &sk_vals[0],
                            &sk_vals[1],
                            "query",
                        )
                        .await?
                    }
                }
                (None, Some(page_sk)) => {
                    // PK-only query with pagination
                    query_with_pk_sk(&self.session_arc(), &query, &pk_text, &page_sk, "query")
                        .await?
                }
                (None, None) => {
                    // Check if this is hash-only index with pagination
                    if is_hash_only_index {
                        if let Some(start_key) = exclusive_start_key {
                            let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                            if let Some(base_pk_val) = start_key.get(base_pk_attr) {
                                let base_pk_text = pk_to_text(base_pk_val)?.into_owned();
                                cassandra_util::query_rows(
                                    &self.session_arc(),
                                    &query,
                                    cdrs_tokio::query_values!(
                                        pk_text.as_str(),
                                        base_pk_text.as_str()
                                    ),
                                    "query",
                                )
                                .await?
                            } else {
                                Vec::new()
                            }
                        } else {
                            // PK-only query without pagination (original behavior)
                            cassandra_util::query_rows(
                                &self.session_arc(),
                                &query,
                                cdrs_tokio::query_values!(pk_text.as_str()),
                                "query",
                            )
                            .await?
                        }
                    } else {
                        // PK-only query without pagination (original behavior)
                        cassandra_util::query_rows(
                            &self.session_arc(),
                            &query,
                            cdrs_tokio::query_values!(pk_text.as_str()),
                            "query",
                        )
                        .await?
                    }
                } // end (None, None) arm
            }; // End of single-query match

            rows_result
        };

        // Step 8: Parse results
        let items: Vec<Item> = rows
            .into_iter()
            .map(|row| {
                let json_str: String =
                    cassandra_util::get_column(&row, "item_data", "query parse")?;
                json_to_item(json_str)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Step 9: Enforce limit and detect pagination
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let has_more = items.len() > actual_limit;

        let final_items: Vec<Item> = items.into_iter().take(actual_limit).collect();

        let last_key = if has_more {
            // Build LastEvaluatedKey from the last returned item
            final_items
                .last()
                .map(|item| extenddb_core::types::extract_key(item, &key_info.key_schema))
        } else {
            None
        };

        tracing::debug!(
            item_count = final_items.len(),
            has_more = has_more,
            "query: fetched items"
        );

        // Step 10: Return results with pagination key
        Ok((final_items, last_key))
    }
}
