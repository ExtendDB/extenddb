// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the `PostgreSQL` backend.

use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    encode_netstring_composite, parse_sk, pk_to_text, sk_column, sk_column_n, sk_info,
};

use super::query::{
    PaginationBinds, build_key, build_sk_sql, execute_query_sql, execute_scan_sql,
    resolve_expr_to_av,
};
use super::{all_sort_key_info, data_table_name, index_table_name, json_to_item};
use crate::PostgresEngine;

/// Build the WHERE clause fragment for ExclusiveStartKey pagination.
///
/// Self-contained: takes `param_idx` (the next available placeholder number),
/// returns a complete SQL fragment. No mutable state leaks out.
fn build_pagination_where(
    param_idx: u32,
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: &Option<(String, ScalarAttributeType)>,
    is_index: bool,
    is_lsi: bool,
    forward: bool,
) -> String {
    if let Some((_, sk_type)) = sk_info_val {
        let sk_col = sk_column(sk_type);
        let collate = if sk_type == ScalarAttributeType::S {
            " COLLATE \"C\""
        } else {
            ""
        };
        let cmp = if forward { ">" } else { "<" };

        if let Some((_, base_sk_type)) = base_sk_info {
            // Index with base SK tie-breaker
            let base_col = format!("base_{}", sk_column(*base_sk_type));
            let base_collate = if *base_sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            let base_cmp = if is_lsi { cmp } else { ">" };
            format!(
                " AND ({sk_col}{collate} {cmp} ${p1} OR \
                 ({sk_col}{collate} = ${p1} AND {base_col}{base_collate} {base_cmp} ${p2}))",
                p1 = param_idx,
                p2 = param_idx + 1
            )
        } else if is_index {
            // Index with no base SK — use base_pk as tie-breaker
            format!(
                " AND ({sk_col}{collate} {cmp} ${p1} OR \
                 ({sk_col}{collate} = ${p1} AND base_pk COLLATE \"C\" > ${p2}))",
                p1 = param_idx,
                p2 = param_idx + 1
            )
        } else {
            // Base table — simple comparison
            format!(" AND {sk_col}{collate} {cmp} ${param_idx}")
        }
    } else if is_index {
        // Hash-only index — paginate using base table PK
        if let Some((_, base_sk_type)) = base_sk_info {
            let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
            let base_sk_collate = if *base_sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            format!(
                " AND (base_pk COLLATE \"C\" > ${p1} OR \
                 (base_pk = ${p1} AND {base_sk_col}{base_sk_collate} > ${p2}))",
                p1 = param_idx,
                p2 = param_idx + 1
            )
        } else {
            format!(" AND base_pk COLLATE \"C\" > ${param_idx}")
        }
    } else {
        String::new()
    }
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
                        match maps.names.get(ref_name) {
                            Some(resolved) => resolved.clone(),
                            None => {
                                tracing::warn!(name_ref = %ref_name, "unresolved expression attribute name in extra SK condition, skipping");
                                continue;
                            }
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

        // ORDER BY — use COLLATE "C" for string sort keys to match DynamoDB
        // UTF-8 byte order.
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            let collate = if sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            let dir = if forward { "ASC" } else { "DESC" };
            if let Some((_, base_sk_type)) = &base_sk_info {
                // Index queries sub-sort by base table SK when index sort keys are equal.
                // LSI: base SK follows ScanIndexForward (same partition, composite sort).
                // GSI: base SK is always ASC (just a uniqueness tie-breaker).
                let base_col = format!("base_{}", sk_column(*base_sk_type));
                let base_collate = if *base_sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                let base_dir = if is_lsi { dir } else { "ASC" };
                let _ = write!(
                    sql,
                    " ORDER BY {sk_col}{collate} {dir}, {base_col}{base_collate} {base_dir}"
                );
            } else if index_name.is_some() {
                // Index with SK but no base SK: use base_pk as secondary sort
                let _ = write!(
                    sql,
                    " ORDER BY {sk_col}{collate} {dir}, base_pk COLLATE \"C\" ASC"
                );
            } else {
                let _ = write!(sql, " ORDER BY {sk_col}{collate} {dir}");
            }
        } else if index_name.is_some() {
            // Hash-only index: order by base table PK for deterministic pagination.
            let dir = if forward { "ASC" } else { "DESC" };
            if let Some((_, base_sk_type)) = &base_sk_info {
                let base_sk_col = format!("base_{}", sk_column(*base_sk_type));
                let base_sk_collate = if *base_sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                let _ = write!(
                    sql,
                    " ORDER BY base_pk COLLATE \"C\" {dir}, {base_sk_col}{base_sk_collate} {dir}"
                );
            } else {
                let _ = write!(sql, " ORDER BY base_pk COLLATE \"C\" {dir}");
            }
        }

        // LIMIT — fetch one extra to detect pagination
        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Build pagination bind values in the same place as the SQL placeholders.
        // The enum variant determines exactly which values are bound and in what order,
        // preventing bind-order divergence between SQL generation and execution.
        let pagination_binds = if let Some(start_key) = exclusive_start_key {
            if let Some((ref base_sk_name, base_sk_type)) = base_sk_info {
                // Index with base SK tie-breaker (SQL has $N for base_sk)
                if let Some(base_sk_val) = start_key.get(base_sk_name.as_str()) {
                    let sk = parse_sk(base_sk_val, base_sk_type)?;
                    PaginationBinds::BaseSkOnly { sk }
                } else {
                    PaginationBinds::None
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
                    .map(|v| pk_to_text(v))
                    .transpose()?
                    .map(|c| c.into_owned());
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
                } else {
                    // Hash-only index scan
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
