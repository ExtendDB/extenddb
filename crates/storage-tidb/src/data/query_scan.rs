// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the `TiDB` backend.

use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    encode_netstring_composite, pk_to_text, sk_column, sk_column_n, sk_info,
};

use super::index::{
    native_index_hash_column, native_index_key_tuple_columns, native_index_non_null_predicates,
    native_index_sort_columns,
};
use super::query::{
    build_key, build_sk_sql, execute_query_sql, execute_scan_sql, resolve_expr_to_av,
};
use super::{all_sort_key_info, data_table_name, json_to_item};
use crate::TidbEngine;

fn sort_key_columns(
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    all_sort_key_info(key_schema, attr_defs)
        .into_iter()
        .enumerate()
        .map(|(i, (_, sk_type))| sk_column_n(i, sk_type))
        .collect()
}

fn key_tuple_columns(
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    let mut columns = vec!["pk".to_owned()];
    columns.extend(sort_key_columns(key_schema, attr_defs));
    columns
}

fn tuple_comparison(columns: &[String], op: &str) -> String {
    debug_assert!(!columns.is_empty());
    if columns.len() == 1 {
        format!("{} {op} ?", columns[0])
    } else {
        format!(
            "({}) {op} ({})",
            columns.join(", "),
            std::iter::repeat_n("?", columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn push_order_by(sql: &mut String, columns: &[String], forward: bool) {
    if columns.is_empty() {
        return;
    }
    let dir = if forward { "ASC" } else { "DESC" };
    sql.push_str(" ORDER BY ");
    sql.push_str(
        &columns
            .iter()
            .map(|col| format!("{col} {dir}"))
            .collect::<Vec<_>>()
            .join(", "),
    );
}

impl TidbEngine {
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

        let index_info = if let Some(idx_name) = index_name {
            Some(
                self.fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                    .await?,
            )
        } else {
            None
        };

        let base_table_key = if index_info.is_some() {
            Some(
                self.fetch_base_key_schema_by_table_id(&key_info.table_id)
                    .await?,
            )
        } else {
            None
        };

        let ddb_table = data_table_name(&key_info.table_id);
        let pk_column = index_info.as_ref().map_or_else(
            || "pk".to_owned(),
            |idx| native_index_hash_column(&idx.index_id),
        );
        let sort_columns = index_info.as_ref().map_or_else(
            || sort_key_columns(&key_info.key_schema, &key_info.attribute_definitions),
            |idx| {
                native_index_sort_columns(
                    &idx.index_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
            },
        );

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
        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE {pk_column} = ?");
        let mut param_idx: u32 = 2;

        if let Some(idx) = &index_info {
            for predicate in native_index_non_null_predicates(
                &idx.index_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            ) {
                sql.push_str(" AND ");
                sql.push_str(&predicate);
            }
        }

        // Sort key condition SQL fragment (first RANGE key).
        let sk_sql_info = if let (Some(sk_cond), Some((_, sk_type))) =
            (&key_condition.sk_condition, sk_info_val)
        {
            let sk_col = index_info
                .as_ref()
                .and_then(|_| sort_columns.first().cloned())
                .unwrap_or_else(|| sk_column(sk_type).to_owned());
            Some(build_sk_sql(
                sk_cond,
                &sk_col,
                sk_type,
                maps,
                &mut param_idx,
            )?)
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
                    let col = index_info
                        .as_ref()
                        .and_then(|_| sort_columns.get(pos).cloned())
                        .unwrap_or_else(|| sk_column_n(pos, sk_type));
                    let _ = write!(sql, " AND {col} = ?");
                    param_idx += 1;
                    extra_sk_col_indices.push((pos, sk_type));
                }
            }
        }

        let cursor_columns = if let Some((base_key_schema, base_attr_defs)) = &base_table_key {
            let mut columns = sort_columns.clone();
            columns.extend(key_tuple_columns(base_key_schema, base_attr_defs));
            columns
        } else {
            sort_columns.clone()
        };

        if exclusive_start_key.is_some() {
            if cursor_columns.is_empty() {
                return Ok((Vec::new(), None));
            }
            let op = if forward { ">" } else { "<" };
            let _ = write!(sql, " AND {}", tuple_comparison(&cursor_columns, op));
        }

        push_order_by(&mut sql, &cursor_columns, forward);

        // LIMIT — fetch one extra to detect pagination
        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute with dynamic bindings
        let rows = execute_query_sql(
            &sql,
            &pk_text,
            key_condition,
            maps,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            sk_info_val,
            &extra_sk_col_indices,
            exclusive_start_key,
            base_table_key
                .as_ref()
                .map(|(key_schema, attr_defs)| (key_schema.as_slice(), attr_defs.as_slice())),
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

        let index_info = if let Some(idx_name) = index_name {
            Some(
                self.fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                    .await?,
            )
        } else {
            None
        };

        let base_table_key = if index_info.is_some() {
            Some(
                self.fetch_base_key_schema_by_table_id(&key_info.table_id)
                    .await?,
            )
        } else {
            None
        };

        let ddb_table = data_table_name(&key_info.table_id);

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        if let Some(idx) = &index_info {
            conditions.extend(native_index_non_null_predicates(
                &idx.index_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            ));
        }
        // Parallel scan: hash-based segment assignment.
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            let segment_column = index_info.as_ref().map_or_else(
                || "pk".to_owned(),
                |idx| native_index_hash_column(&idx.index_id),
            );
            conditions.push(format!("CRC32({segment_column}) % {total} = {seg}"));
        }

        let order_columns = if let Some((base_key_schema, base_attr_defs)) = &base_table_key {
            let idx = index_info
                .as_ref()
                .ok_or_else(|| StorageError::Internal("missing index metadata".to_owned()))?;
            let mut columns = native_index_key_tuple_columns(
                &idx.index_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            );
            columns.extend(key_tuple_columns(base_key_schema, base_attr_defs));
            columns
        } else {
            key_tuple_columns(&key_info.key_schema, &key_info.attribute_definitions)
        };

        if let Some(start_key) = exclusive_start_key {
            let pk_name = &key_info.key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            conditions.push(tuple_comparison(&order_columns, ">"));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let _ = write!(sql, " ORDER BY {}", order_columns.join(", "));

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute
        let rows = execute_scan_sql(
            &sql,
            exclusive_start_key,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            base_table_key
                .as_ref()
                .map(|(key_schema, attr_defs)| (key_schema.as_slice(), attr_defs.as_slice())),
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
    use super::tuple_comparison;

    #[test]
    fn tuple_comparison_uses_all_cursor_columns() {
        let cols = vec![
            "sk_s".to_owned(),
            "base_pk".to_owned(),
            "base_sk_n".to_owned(),
        ];
        assert_eq!(
            tuple_comparison(&cols, ">"),
            "(sk_s, base_pk, base_sk_n) > (?, ?, ?)"
        );
    }

    #[test]
    fn tuple_comparison_keeps_single_column_syntax_simple() {
        let cols = vec!["base_pk".to_owned()];
        assert_eq!(tuple_comparison(&cols, "<"), "base_pk < ?");
    }
}
