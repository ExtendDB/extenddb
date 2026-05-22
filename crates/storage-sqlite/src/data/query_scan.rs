// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the SQLite backend.

use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    encode_netstring_composite, parse_sk, pk_to_text, sk_column, sk_column_n, sk_info,
};

use super::query::{
    BoundValue, build_key, build_sk_sql, execute_dynamic_query, resolve_expr_to_av,
    sk_condition_bind_values, sk_to_bound,
};
use super::{all_sort_key_info, data_table_name, index_table_name, json_to_item};
use crate::engine::SqliteEngine;

impl SqliteEngine {
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

        let ddb_table = if let Some(idx_name) = index_name {
            let idx_info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            index_table_name(&idx_info.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };

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

        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
        let mut bind_values: Vec<BoundValue> = vec![BoundValue::Text(pk_text.clone())];

        // Sort key condition SQL fragment.
        if let (Some(sk_cond), Some((_, sk_type))) = (&key_condition.sk_condition, sk_info_val) {
            let sk_col = sk_column(sk_type);
            let info = build_sk_sql(sk_cond, sk_col);
            sql.push_str(&info.fragment);
            let vals = sk_condition_bind_values(sk_cond, sk_type, maps)?;
            for sk in &vals {
                bind_values.push(sk_to_bound(sk));
            }
        }

        // Extra RANGE key equality conditions.
        for (path, value) in &key_condition.extra_sk_conditions {
            let attr_name = match path.first() {
                Some(extenddb_core::expression::PathElement::Attribute(name)) => {
                    if let Some(ref_name) = name.strip_prefix('#') {
                        match maps.names.get(ref_name) {
                            Some(resolved) => resolved.clone(),
                            None => continue,
                        }
                    } else {
                        name.clone()
                    }
                }
                _ => continue,
            };
            if let Some(pos) = all_sks.iter().position(|(sk_name, _)| *sk_name == attr_name) {
                if pos > 0 {
                    let (_, sk_type) = all_sks[pos];
                    let col = sk_column_n(pos, sk_type);
                    let _ = write!(sql, " AND {col} = ?");
                    let av = resolve_expr_to_av(value, maps)?;
                    let sk = parse_sk(&av, sk_type)?;
                    bind_values.push(sk_to_bound(&sk));
                }
            }
        }

        // Pagination: exclusive start key
        if let (Some(start_key), Some((sk_name, sk_type))) = (exclusive_start_key, sk_info_val) {
            let sk_col = sk_column(sk_type);
            if forward {
                let _ = write!(sql, " AND {sk_col} > ?");
            } else {
                let _ = write!(sql, " AND {sk_col} < ?");
            }
            if let Some(sk_val) = start_key.get(sk_name) {
                let sk = parse_sk(sk_val, sk_type)?;
                bind_values.push(sk_to_bound(&sk));
            }
        } else if exclusive_start_key.is_some() && sk_info_val.is_none() {
            return Ok((Vec::new(), None));
        }

        // ORDER BY
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            let dir = if forward { "ASC" } else { "DESC" };
            let _ = write!(sql, " ORDER BY {sk_col} {dir}");
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        let rows = execute_dynamic_query(&sql, bind_values, &self.pool).await?;

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

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        let mut bind_values: Vec<BoundValue> = Vec::new();

        // Parallel scan: use rowid modulo for segment distribution in SQLite.
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            conditions.push(format!("(rowid % {total}) = {seg}"));
        }

        // Pagination via exclusive start key.
        if let Some(start_key) = exclusive_start_key {
            let pk_name = &key_info.key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            let pk_val = start_key.get(pk_name).unwrap();
            let pk_text = pk_to_text(pk_val)?;

            if let Some((sk_name, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                // SQLite doesn't support tuple comparison, so use explicit OR expansion.
                conditions.push(format!("(pk > ? OR (pk = ? AND {sk_col} > ?))"));
                bind_values.push(BoundValue::Text(pk_text.clone().into_owned()));
                bind_values.push(BoundValue::Text(pk_text.into_owned()));
                if let Some(sk_val) = start_key.get(sk_name) {
                    let sk = parse_sk(sk_val, sk_type)?;
                    bind_values.push(sk_to_bound(&sk));
                }
            } else {
                conditions.push("pk > ?".to_owned());
                bind_values.push(BoundValue::Text(pk_text.into_owned()));
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // ORDER BY
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            let _ = write!(sql, " ORDER BY pk, {sk_col}");
        } else {
            sql.push_str(" ORDER BY pk");
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        let rows = execute_dynamic_query(&sql, bind_values, &self.pool).await?;

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
