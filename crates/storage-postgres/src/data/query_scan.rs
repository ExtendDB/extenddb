// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the `PostgreSQL` backend.

use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{encode_netstring_composite, sk_column, sk_column_n, sk_info};

use super::key_text::{parse_sk, pk_to_text};

use super::query::{
    PaginationBinds, build_key, build_sk_sql, execute_query_sql, execute_scan_sql,
    resolve_expr_to_av,
};
use super::{all_sort_key_info, data_table_name, index_table_name, json_to_item};
use crate::PostgresEngine;

/// Build the WHERE clause fragment for `ExclusiveStartKey` pagination.
///
/// Self-contained: takes `param_idx` (the next available placeholder number),
/// returns a complete SQL fragment. No mutable state leaks out.
///
/// The cursor columns are exactly the `ORDER BY` columns of the same query
/// (see [`order_by_columns`]), and every one of them runs in the direction of
/// `ScanIndexForward`, so the predicate is always a row comparison,
/// `(a, b) > ($1, $2)` or `(a, b) < ($1, $2)`. PostgreSQL turns that into one
/// index seek on the key index (the columns carry `COLLATE "C"`, and so do
/// the indexes on tables created since the columns were declared that way).
/// The equivalent `a > $1 OR (a = $1 AND b > $2)` is planned as a bitmap
/// union that collects every row past the cursor before the `LIMIT` applies,
/// or as an index scan filtered from the start of the partition.
///
/// The tie-breaker columns follow the direction too. DynamoDB returns a
/// reverse Query as the exact mirror of the forward one, including among
/// items that share an index sort key and among the items of a hash-only
/// index partition (measured 2026-09-17). Keeping the tie-breaker ascending
/// while the sort key descends is not a mirror, and where the tie-breaker was
/// the whole order (a hash-only index) an ascending cursor under a descending
/// `ORDER BY` repeated the first page's items and skipped the rest.
fn build_pagination_where(
    param_idx: u32,
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: &Option<(String, ScalarAttributeType)>,
    is_index: bool,
    is_lsi: bool,
    forward: bool,
) -> String {
    let cols = order_by_columns(sk_info_val, base_sk_info.as_ref(), is_index, is_lsi);
    if cols.is_empty() {
        return String::new();
    }
    let cmp = if forward { ">" } else { "<" };
    let last = param_idx + u32::try_from(cols.len()).expect("at most three cursor columns");
    let params = (param_idx..last)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>();
    if cols.len() == 1 {
        format!(" AND {} {cmp} {}", cols[0], params[0])
    } else {
        format!(" AND ({}) {cmp} ({})", cols.join(", "), params.join(", "))
    }
}

/// The columns, with their collations, that order a Query's rows and that its
/// page cursor compares, in order. Empty for a base-table Scan, whose rows are
/// ordered by the primary key clause the caller writes.
///
/// - Base table with a sort key: the sort key.
/// - LSI: the index sort key, then the base sort key (every row shares the
///   queried partition key, so the base sort key alone identifies a row).
/// - GSI with a sort key: the index sort key, then the FULL base primary key.
///   Rows in a GSI partition are unique on (index SK, base PK, base SK), not
///   on (index SK, base SK): many base partitions can project the same index
///   SK and the same base SK, and comparing base SK alone made a page-two
///   query return nothing whenever the rows sharing an index SK also shared a
///   base SK.
/// - Hash-only index: the full base primary key.
fn order_by_columns(
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: Option<&(String, ScalarAttributeType)>,
    is_index: bool,
    is_lsi: bool,
) -> Vec<String> {
    let collated = |col: &str, t: ScalarAttributeType| {
        if t == ScalarAttributeType::S {
            format!("{col} COLLATE \"C\"")
        } else {
            col.to_owned()
        }
    };
    let base_sk = base_sk_info.map(|(_, t)| collated(&format!("base_{}", sk_column(*t)), *t));
    let mut cols = Vec::with_capacity(3);
    if let Some((_, sk_type)) = sk_info_val {
        cols.push(collated(sk_column(sk_type), sk_type));
        if !is_index {
            return cols;
        }
        if !is_lsi {
            cols.push("base_pk COLLATE \"C\"".to_owned());
        }
        cols.extend(base_sk);
    } else if is_index {
        cols.push("base_pk COLLATE \"C\"".to_owned());
        cols.extend(base_sk);
    }
    cols
}

impl PostgresEngine {
    /// Implementation of `DataEngine::query`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_impl(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let (ddb_table, is_lsi) = if let Some(idx_name) = index_name {
            let idx_info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            let lsi = idx_info.index_type == extenddb_core::types::IndexType::Lsi;
            (index_table_name(&idx_info.index_id), lsi)
        } else {
            (data_table_name(&key_info.table_id), false)
        };

        // Resolve partition key value(s) — for multi-part keys, encode
        // all HASH attribute values into a single composite PK text using
        // netstring encoding (matching the write path in composite_pk_to_text).
        let pk_text = if key_condition.extra_pk_conditions.is_empty() {
            let pk_expr_val = resolve_expr_to_av(&key_condition.pk_value, maps)?;
            pk_to_text(&pk_expr_val)?.into_owned()
        } else {
            let mut parts = Vec::with_capacity(1 + key_condition.extra_pk_conditions.len());
            let first_val = resolve_expr_to_av(&key_condition.pk_value, maps)?;
            parts.push(pk_to_text(&first_val)?.into_owned());
            for (_, value) in &key_condition.extra_pk_conditions {
                let val = resolve_expr_to_av(value, maps)?;
                parts.push(pk_to_text(&val)?.into_owned());
            }
            encode_netstring_composite(&parts)
        };

        let sk_info_val = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let all_sks = all_sort_key_info(&key_info.key_schema, &key_info.attribute_definitions);

        // Build SQL query
        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
        let mut param_idx: u32 = 2;

        // Sort key condition SQL fragment (first RANGE key).
        let sk_sql_info = if let (Some(sk_cond), Some((_, sk_type))) =
            (&key_condition.sk_condition, sk_info_val)
        {
            Some(build_sk_sql(sk_cond, sk_column(sk_type), &mut param_idx))
        } else {
            None
        };

        if let Some(ref info) = sk_sql_info {
            sql.push_str(&info.fragment);
        }

        // Extra RANGE key equality conditions (multi-RANGE key schemas).
        // Each extra SK condition is an equality on an additional RANGE attribute.
        let mut extra_sk_col_indices: Vec<(usize, ScalarAttributeType)> = Vec::new();
        for (path, _value) in &key_condition.extra_sk_conditions {
            let attr_name = match path.first() {
                Some(extenddb_core::expression::PathElement::Attribute(name)) => {
                    if let Some(ref_name) = name.strip_prefix('#') {
                        if let Some(resolved) = maps.names.get(ref_name) {
                            resolved.clone()
                        } else {
                            tracing::warn!(name_ref = %ref_name, "unresolved expression attribute name in extra SK condition, skipping");
                            continue;
                        }
                    } else {
                        name.clone()
                    }
                }
                _ => continue,
            };
            // Find which RANGE key index this attribute corresponds to
            if let Some(pos) = all_sks
                .iter()
                .position(|(sk_name, _)| *sk_name == attr_name)
            {
                // Skip index 0 — that's the primary SK handled above
                if pos > 0 {
                    let (_, sk_type) = all_sks[pos];
                    let col = sk_column_n(pos, sk_type);
                    let _ = write!(sql, " AND {col} = ${param_idx}");
                    param_idx += 1;
                    extra_sk_col_indices.push((pos, sk_type));
                }
            }
        }

        // For index queries, derive the base table's sort key info from
        // base_key_schema. Needed for ORDER BY (sub-sort by base SK when index
        // SKs are equal) and pagination (compound ExclusiveStartKey condition).
        let base_sk_info: Option<(String, ScalarAttributeType)> = if index_name.is_some() {
            sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
                .map(|(name, ty)| (name.to_owned(), ty))
        } else {
            None
        };

        if exclusive_start_key.is_some() && sk_info_val.is_none() && index_name.is_none() {
            // PK-only base table with start key — no more items for this PK
            return Ok((Vec::new(), None));
        }

        if exclusive_start_key.is_some() {
            let pagination_sql = build_pagination_where(
                param_idx,
                sk_info_val,
                &base_sk_info,
                index_name.is_some(),
                is_lsi,
                forward,
            );
            sql.push_str(&pagination_sql);
        }

        // ORDER BY: the same columns the page cursor compares, every one in the
        // direction of ScanIndexForward, so a reverse Query is the exact mirror
        // of the forward one (as DynamoDB's is) and the cursor predicate above
        // is a row comparison the key index can seek on. String columns carry
        // COLLATE "C" for DynamoDB's UTF-8 byte order.
        let order_cols = order_by_columns(
            sk_info_val,
            base_sk_info.as_ref(),
            index_name.is_some(),
            is_lsi,
        );
        if !order_cols.is_empty() {
            let dir = if forward { "ASC" } else { "DESC" };
            let ordered = order_cols
                .iter()
                .map(|c| format!("{c} {dir}"))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(sql, " ORDER BY {ordered}");
        }

        // LIMIT — fetch one extra to detect pagination
        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Build pagination bind values in the same place as the SQL placeholders.
        // The enum variant determines exactly which values are bound and in what order,
        // preventing bind-order divergence between SQL generation and execution.
        let pagination_binds = if let Some(start_key) = exclusive_start_key {
            if sk_info_val.is_some()
                && let Some((ref base_sk_name, base_sk_type)) = base_sk_info
            {
                // Index that has its own SK, with a base-table tie-breaker. The
                // index SK is bound separately (see execute_query_sql); the binds
                // here supply the tie-breaker.
                //
                // An LSI needs the base SK alone, because every row shares the
                // queried partition key. A GSI needs the full base primary key,
                // because rows sharing an index SK can come from different base
                // partitions and can also share a base SK.
                let base_sk = start_key
                    .get(base_sk_name.as_str())
                    .map(|v| parse_sk(v, base_sk_type))
                    .transpose()?;
                if is_lsi {
                    match base_sk {
                        Some(sk) => PaginationBinds::BaseSkOnly { sk },
                        None => PaginationBinds::None,
                    }
                } else {
                    let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                    match (start_key.get(base_pk_attr.as_str()), base_sk) {
                        (Some(pk_val), Some(sk)) => PaginationBinds::BasePkAndSk {
                            pk_text: pk_to_text(pk_val)?.into_owned(),
                            sk,
                        },
                        _ => PaginationBinds::None,
                    }
                }
            } else if index_name.is_some() && sk_info_val.is_some() {
                // Index with SK but no base SK — SQL has $N for base_pk
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                match start_key.get(base_pk_attr.as_str()) {
                    Some(pk_val) => {
                        let pk_text = pk_to_text(pk_val)?.into_owned();
                        PaginationBinds::BasePkOnly { pk_text }
                    }
                    None => PaginationBinds::None,
                }
            } else if index_name.is_some() && sk_info_val.is_none() {
                // Hash-only index — SQL may have $N for base_pk and $N+1 for base_sk
                let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
                let base_pk = start_key
                    .get(base_pk_attr.as_str())
                    .map(pk_to_text)
                    .transpose()?
                    .map(std::borrow::Cow::into_owned);
                match (base_pk, &base_sk_info) {
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
            } else {
                PaginationBinds::None
            }
        } else {
            PaginationBinds::None
        };

        // Execute with dynamic bindings
        let rows = execute_query_sql(
            &sql,
            &pk_text,
            key_condition,
            maps,
            sk_info_val,
            &extra_sk_col_indices,
            exclusive_start_key,
            &pagination_binds,
            &self.data_pool,
        )
        .await?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let has_more = rows.len() > actual_limit;
        let items: Vec<Item> = rows
            .into_iter()
            .take(actual_limit)
            .map(json_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        let last_key = if has_more {
            items
                .last()
                .map(|item| build_key(item, &key_info.key_schema))
        } else {
            None
        };

        Ok((items, last_key))
    }

    /// Implementation of `DataEngine::scan`.
    pub(crate) async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let ddb_table = if let Some(idx_name) = index_name {
            let idx_info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            index_table_name(&idx_info.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };
        let sk_info_val = sk_info(&key_info.key_schema, &key_info.attribute_definitions);

        // For index scans, derive base table SK info for compound pagination.
        let base_sk_info: Option<(String, ScalarAttributeType)> = if index_name.is_some() {
            sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
                .map(|(name, ty)| (name.to_owned(), ty))
        } else {
            None
        };

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        let param_idx: u32 = 1;

        // Parallel scan: hash-based segment assignment.
        // CB-20 / SP-SCN-002: use bigint bitmask instead of abs() to avoid
        // SQL error 22003 on the one-in-4-billion hashtext() == i32::MIN case.
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            conditions.push(format!(
                "(hashtext(pk)::bigint & 2147483647) % {total} = {seg}"
            ));
        }

        // Pagination via exclusive start key
        if let Some(start_key) = exclusive_start_key {
            let pk_name = &key_info.key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            // Actual PK/SK binding happens in execute_scan_sql.

            if index_name.is_some() {
                // Index scan: use compound condition including base table key
                // to handle duplicate (pk, sk) pairs on GSIs.
                if let Some((_, sk_type)) = sk_info_val {
                    let sk_col = sk_column(sk_type);
                    let collate = if sk_type == ScalarAttributeType::S {
                        " COLLATE \"C\""
                    } else {
                        ""
                    };
                    if let Some((_, base_sk_type)) = &base_sk_info {
                        let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                        let base_collate = if *base_sk_type == ScalarAttributeType::S {
                            " COLLATE \"C\""
                        } else {
                            ""
                        };
                        conditions.push(format!(
                            "(pk, {sk_col}{collate}, base_pk COLLATE \"C\", {base_sk_col}{base_collate}) > \
                             (${p1}, ${p2}, ${p3}, ${p4})",
                            p1 = param_idx, p2 = param_idx + 1,
                            p3 = param_idx + 2, p4 = param_idx + 3
                        ));
                    } else {
                        conditions.push(format!(
                            "(pk, {sk_col}{collate}, base_pk COLLATE \"C\") > (${p1}, ${p2}, ${p3})",
                            p1 = param_idx, p2 = param_idx + 1, p3 = param_idx + 2
                        ));
                    }
                } else if let Some((_, base_sk_type)) = &base_sk_info {
                    // Hash-only index on a composite-key base table: rows that
                    // share the same index hash AND base partition key differ
                    // only by the base sort key, so it must be part of the
                    // pagination tie-breaker (matching the ORDER BY below).
                    let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                    let base_collate = if *base_sk_type == ScalarAttributeType::S {
                        " COLLATE \"C\""
                    } else {
                        ""
                    };
                    conditions.push(format!(
                        "(pk, base_pk COLLATE \"C\", {base_sk_col}{base_collate}) > (${p1}, ${p2}, ${p3})",
                        p1 = param_idx,
                        p2 = param_idx + 1,
                        p3 = param_idx + 2
                    ));
                } else {
                    // Hash-only index on a hash-only base table.
                    conditions.push(format!(
                        "(pk, base_pk COLLATE \"C\") > (${p1}, ${p2})",
                        p1 = param_idx,
                        p2 = param_idx + 1
                    ));
                }
            } else {
                // Base table scan: standard row-value comparison
                if let Some((_, sk_type)) = sk_info_val {
                    let sk_col = sk_column(sk_type);
                    let collate = if sk_type == ScalarAttributeType::S {
                        " COLLATE \"C\""
                    } else {
                        ""
                    };
                    conditions.push(format!(
                        "(pk, {sk_col}{collate}) > (${param_idx}, ${next})",
                        next = param_idx + 1
                    ));
                } else {
                    conditions.push(format!("pk > ${param_idx}"));
                }
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Deterministic ordering for pagination.
        if index_name.is_some() {
            // Index scan: include base table key columns for deterministic ordering.
            if let Some((_, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                let collate = if sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                if let Some((_, base_sk_type)) = &base_sk_info {
                    let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                    let base_collate = if *base_sk_type == ScalarAttributeType::S {
                        " COLLATE \"C\""
                    } else {
                        ""
                    };
                    let _ = write!(
                        sql,
                        " ORDER BY pk, {sk_col}{collate}, base_pk COLLATE \"C\", {base_sk_col}{base_collate}"
                    );
                } else {
                    let _ = write!(
                        sql,
                        " ORDER BY pk, {sk_col}{collate}, base_pk COLLATE \"C\""
                    );
                }
            } else if let Some((_, base_sk_type)) = &base_sk_info {
                let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                let base_collate = if *base_sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                let _ = write!(
                    sql,
                    " ORDER BY pk, base_pk COLLATE \"C\", {base_sk_col}{base_collate}"
                );
            } else {
                let _ = write!(sql, " ORDER BY pk, base_pk COLLATE \"C\"");
            }
        } else {
            // Base table scan: standard ordering.
            if let Some((_, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                let collate = if sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                let _ = write!(sql, " ORDER BY pk, {sk_col}{collate}");
            } else {
                sql.push_str(" ORDER BY pk");
            }
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Derive base table PK attribute name for index scan binding.
        let base_pk_attr_name: Option<&str> =
            if index_name.is_some() && exclusive_start_key.is_some() {
                Some(key_info.base_key_schema[0].attribute_name.as_str())
            } else {
                None
            };

        // Execute
        let rows = execute_scan_sql(
            &sql,
            exclusive_start_key,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            base_sk_info.as_ref(),
            base_pk_attr_name,
            &self.data_pool,
        )
        .await?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let has_more = rows.len() > actual_limit;
        let items: Vec<Item> = rows
            .into_iter()
            .take(actual_limit)
            .map(json_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        let last_key = if has_more {
            items
                .last()
                .map(|item| build_key(item, &key_info.key_schema))
        } else {
            None
        };

        Ok((items, last_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: ScalarAttributeType = ScalarAttributeType::S;
    const N: ScalarAttributeType = ScalarAttributeType::N;

    fn base_s() -> Option<(String, ScalarAttributeType)> {
        Some(("bsk".to_owned(), S))
    }

    /// The cursor fragments are pinned as strings because the row-comparison
    /// and OR forms return the same rows: only a plan can tell them apart, and
    /// a revert to the OR form would pass every wire test while losing the
    /// index seek.
    #[test]
    fn base_table_cursor_is_a_single_comparison() {
        assert_eq!(
            build_pagination_where(3, Some(("sk", S)), &None, false, false, true),
            " AND sk_s COLLATE \"C\" > $3"
        );
        assert_eq!(
            build_pagination_where(3, Some(("sk", N)), &None, false, false, false),
            " AND sk_n < $3"
        );
    }

    #[test]
    fn forward_gsi_cursor_is_a_row_comparison_over_the_full_base_key() {
        assert_eq!(
            build_pagination_where(3, Some(("gsk", S)), &base_s(), true, false, true),
            " AND (sk_s COLLATE \"C\", base_pk COLLATE \"C\", base_sk_s COLLATE \"C\") > ($3, $4, $5)"
        );
        // Numeric index sort key with a numeric base sort key: no collation on either.
        assert_eq!(
            build_pagination_where(
                3,
                Some(("gsk", N)),
                &Some(("bsk".to_owned(), N)),
                true,
                false,
                true
            ),
            " AND (sk_n, base_pk COLLATE \"C\", base_sk_n) > ($3, $4, $5)"
        );
    }

    #[test]
    fn reverse_gsi_cursor_is_the_mirror_row_comparison() {
        // The tie-breaker follows the direction: DynamoDB's reverse Query is
        // the exact mirror of its forward one, including among items sharing
        // an index sort key (measured 2026-09-17).
        assert_eq!(
            build_pagination_where(3, Some(("gsk", S)), &base_s(), true, false, false),
            " AND (sk_s COLLATE \"C\", base_pk COLLATE \"C\", base_sk_s COLLATE \"C\") < ($3, $4, $5)"
        );
    }

    #[test]
    fn gsi_on_a_hash_only_base_table_uses_base_pk_as_the_tie_breaker() {
        assert_eq!(
            build_pagination_where(3, Some(("gsk", S)), &None, true, false, true),
            " AND (sk_s COLLATE \"C\", base_pk COLLATE \"C\") > ($3, $4)"
        );
        assert_eq!(
            build_pagination_where(3, Some(("gsk", S)), &None, true, false, false),
            " AND (sk_s COLLATE \"C\", base_pk COLLATE \"C\") < ($3, $4)"
        );
    }

    #[test]
    fn lsi_cursor_is_a_row_comparison_in_both_directions() {
        assert_eq!(
            build_pagination_where(3, Some(("lsk", S)), &base_s(), true, true, true),
            " AND (sk_s COLLATE \"C\", base_sk_s COLLATE \"C\") > ($3, $4)"
        );
        assert_eq!(
            build_pagination_where(3, Some(("lsk", S)), &base_s(), true, true, false),
            " AND (sk_s COLLATE \"C\", base_sk_s COLLATE \"C\") < ($3, $4)"
        );
    }

    #[test]
    fn hash_only_index_cursor_is_a_row_comparison_over_the_base_key() {
        assert_eq!(
            build_pagination_where(3, None, &base_s(), true, false, true),
            " AND (base_pk COLLATE \"C\", base_sk_s COLLATE \"C\") > ($3, $4)"
        );
        assert_eq!(
            build_pagination_where(3, None, &None, true, false, true),
            " AND base_pk COLLATE \"C\" > $3"
        );
        assert_eq!(
            build_pagination_where(3, None, &None, false, false, true),
            ""
        );
    }

    #[test]
    fn hash_only_index_reverse_cursor_follows_the_direction() {
        // The defect: a reverse Query on a hash-only index ordered
        // `base_pk DESC` but its cursor read `base_pk > $3`, so page two
        // after (p5, p4) asked for keys above p4, returned p5 again, and
        // then ended with p0..p3 never delivered.
        assert_eq!(
            build_pagination_where(3, None, &None, true, false, false),
            " AND base_pk COLLATE \"C\" < $3"
        );
        assert_eq!(
            build_pagination_where(3, None, &base_s(), true, false, false),
            " AND (base_pk COLLATE \"C\", base_sk_s COLLATE \"C\") < ($3, $4)"
        );
        assert_eq!(
            build_pagination_where(3, None, &Some(("bsk".to_owned(), N)), true, false, false),
            " AND (base_pk COLLATE \"C\", base_sk_n) < ($3, $4)"
        );
    }

    #[test]
    fn order_by_columns_match_the_cursor_columns_for_every_shape() {
        // The two must agree column for column, or a cursor resumes from a
        // position the ORDER BY never produced.
        type Shape<'a> = (
            Option<(&'a str, ScalarAttributeType)>,
            Option<(String, ScalarAttributeType)>,
            bool,
            bool,
        );
        let shapes: [Shape<'_>; 7] = [
            (Some(("sk", S)), None, false, false),
            (Some(("gsk", S)), base_s(), true, false),
            (Some(("gsk", N)), Some(("bsk".to_owned(), N)), true, false),
            (Some(("gsk", S)), None, true, false),
            (Some(("lsk", S)), base_s(), true, true),
            (None, base_s(), true, false),
            (None, None, true, false),
        ];
        for (sk, base, is_index, is_lsi) in shapes {
            let cols = order_by_columns(sk, base.as_ref(), is_index, is_lsi);
            let fwd = build_pagination_where(1, sk, &base, is_index, is_lsi, true);
            let rev = build_pagination_where(1, sk, &base, is_index, is_lsi, false);
            let params = (1..=cols.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>();
            let (cols_sql, params_sql) = if cols.len() == 1 {
                (cols[0].clone(), params[0].clone())
            } else {
                (
                    format!("({})", cols.join(", ")),
                    format!("({})", params.join(", ")),
                )
            };
            assert_eq!(
                fwd,
                format!(" AND {cols_sql} > {params_sql}"),
                "forward {sk:?} {base:?}"
            );
            assert_eq!(
                rev,
                format!(" AND {cols_sql} < {params_sql}"),
                "reverse {sk:?} {base:?}"
            );
        }
        assert!(order_by_columns(None, None, false, false).is_empty());
    }
}
