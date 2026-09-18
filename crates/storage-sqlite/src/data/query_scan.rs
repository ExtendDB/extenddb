// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` for the SQLite backend.
//!
//! Mirrors the PostgreSQL backend's index routing and pagination. For index
//! operations the engine passes `key_info.key_schema` = index key schema and
//! `key_info.base_key_schema` = base table key schema, so the index `sk_*`
//! columns are addressed by the index sort key and `base_*` columns provide
//! tie-breakers. SQLite differences: positional `?` placeholders, no `COLLATE`
//! (the default BINARY collation already matches DynamoDB byte order, and `N`
//! keys are stored as the order-preserving TEXT encoding per D2), and
//! `rowid % total_segments` for parallel scan.

use std::fmt::Write;

use extenddb_core::expression::{ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{Item, ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    encode_netstring_composite, parse_sk, pk_to_text, sk_column, sk_column_n, sk_info,
};

use super::query::{build_key, build_sk_sql_and_binds, execute_dynamic_query, resolve_expr_to_av};
use super::{
    BoundValue, all_sort_key_info, data_table_name, index_table_name, json_to_item, sk_bound,
};
use crate::store::SqliteEngine;

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
        let (ddb_table, is_lsi) = if let Some(idx_name) = index_name {
            let info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            (
                index_table_name(&info.index_id),
                info.index_type == extenddb_core::types::IndexType::Lsi,
            )
        } else {
            (data_table_name(&key_info.table_id), false)
        };

        // Partition key (composite via netstring when multi-HASH).
        let pk_text = if key_condition.extra_pk_conditions.is_empty() {
            pk_to_text(&resolve_expr_to_av(&key_condition.pk_value, maps)?)?.into_owned()
        } else {
            let mut parts =
                vec![pk_to_text(&resolve_expr_to_av(&key_condition.pk_value, maps)?)?.into_owned()];
            for (_, value) in &key_condition.extra_pk_conditions {
                parts.push(pk_to_text(&resolve_expr_to_av(value, maps)?)?.into_owned());
            }
            encode_netstring_composite(&parts)
        };

        let sk_info_val = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let all_sks = all_sort_key_info(&key_info.key_schema, &key_info.attribute_definitions);
        let base_sk_info: Option<(String, ScalarAttributeType)> = if index_name.is_some() {
            sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
                .map(|(n, t)| (n.to_owned(), t))
        } else {
            None
        };

        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
        let mut binds: Vec<BoundValue> = vec![BoundValue::Text(pk_text)];

        // Primary sort-key condition.
        if let (Some(sk_cond), Some((_, sk_type))) = (&key_condition.sk_condition, sk_info_val) {
            let (sk_sql, sk_binds) =
                build_sk_sql_and_binds(sk_cond, sk_column(sk_type), sk_type, maps)?;
            sql.push_str(&sk_sql);
            for v in &sk_binds {
                binds.push(sk_bound(v));
            }
        }

        // Extra RANGE-key equality conditions (multi-RANGE schemas).
        for (path, value) in &key_condition.extra_sk_conditions {
            let Some(attr_name) = resolve_attr_name(path, maps) else {
                continue;
            };
            if let Some(pos) = all_sks.iter().position(|(n, _)| *n == attr_name)
                && pos > 0
            {
                let (_, sk_type) = all_sks[pos];
                let _ = write!(sql, " AND {} = ?", sk_column_n(pos, sk_type));
                binds.push(sk_bound(&parse_sk(
                    &resolve_expr_to_av(value, maps)?,
                    sk_type,
                )?));
            }
        }

        // Pagination.
        if exclusive_start_key.is_some() && sk_info_val.is_none() && index_name.is_none() {
            return Ok((Vec::new(), None));
        }
        if let Some(start_key) = exclusive_start_key {
            append_query_pagination(
                &mut sql,
                &mut binds,
                start_key,
                sk_info_val,
                base_sk_info.as_ref(),
                key_info,
                index_name.is_some(),
                is_lsi,
                forward,
            )?;
        }

        // ORDER BY.
        let dir = if forward { "ASC" } else { "DESC" };
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            if let Some((_, base_type)) = &base_sk_info {
                let base_col = format!("base_{}", sk_column(*base_type));
                if is_lsi {
                    let _ = write!(sql, " ORDER BY {sk_col} {dir}, {base_col} {dir}");
                } else {
                    // GSI: order by the full base primary key after the index SK
                    // so the ordering matches the pagination tie-breaker exactly.
                    // Ordering by base SK alone leaves rows that share an index SK
                    // and a base SK in an arbitrary order, which no
                    // ExclusiveStartKey can resume from deterministically.
                    let _ = write!(sql, " ORDER BY {sk_col} {dir}, base_pk ASC, {base_col} ASC");
                }
            } else if index_name.is_some() {
                let _ = write!(sql, " ORDER BY {sk_col} {dir}, base_pk ASC");
            } else {
                let _ = write!(sql, " ORDER BY {sk_col} {dir}");
            }
        } else if index_name.is_some() {
            if let Some((_, base_type)) = &base_sk_info {
                let base_col = format!("base_{}", sk_column(*base_type));
                let _ = write!(sql, " ORDER BY base_pk {dir}, {base_col} {dir}");
            } else {
                let _ = write!(sql, " ORDER BY base_pk {dir}");
            }
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        let rows = execute_dynamic_query(&sql, binds, &self.pool).await?;
        finalize(rows, limit, &key_info.key_schema)
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
        let ddb_table = if let Some(idx_name) = index_name {
            let info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            index_table_name(&info.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };

        let sk_info_val = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let base_sk_info: Option<(String, ScalarAttributeType)> = if index_name.is_some() {
            sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
                .map(|(n, t)| (n.to_owned(), t))
        } else {
            None
        };

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        let mut binds: Vec<BoundValue> = Vec::new();

        // Parallel scan: disjoint rowid partitioning. seg/total are validated
        // non-negative integers at the engine layer, safe to interpolate.
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            conditions.push(format!("(rowid % {total}) = {seg}"));
        }

        if let Some(start_key) = exclusive_start_key {
            let pk_name = &key_info.key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            let pk_text = pk_to_text(start_key.get(pk_name).unwrap())?.into_owned();

            if index_name.is_some() {
                if let Some((sk_name, sk_type)) = sk_info_val {
                    let sk_col = sk_column(sk_type);
                    let sk_bv = start_key
                        .get(sk_name)
                        .map(|v| parse_sk(v, sk_type))
                        .transpose()?
                        .map(|s| sk_bound(&s));
                    let base_pk_text = base_pk_from_start_key(start_key, key_info)?;
                    if let Some((base_name, base_type)) = &base_sk_info {
                        let base_col = format!("base_{}", sk_column(*base_type));
                        conditions.push(format!(
                            "(pk, {sk_col}, base_pk, {base_col}) > (?, ?, ?, ?)"
                        ));
                        binds.push(BoundValue::Text(pk_text));
                        binds.push(sk_bv.unwrap_or(BoundValue::Text(String::new())));
                        binds.push(BoundValue::Text(base_pk_text));
                        if let Some(v) = start_key.get(base_name.as_str()) {
                            binds.push(sk_bound(&parse_sk(v, *base_type)?));
                        } else {
                            binds.push(BoundValue::Text(String::new()));
                        }
                    } else {
                        conditions.push(format!("(pk, {sk_col}, base_pk) > (?, ?, ?)"));
                        binds.push(BoundValue::Text(pk_text));
                        binds.push(sk_bv.unwrap_or(BoundValue::Text(String::new())));
                        binds.push(BoundValue::Text(base_pk_text));
                    }
                } else {
                    // Hash-only GSI. Include the base sort key in the
                    // pagination predicate when the base table has one: the
                    // index PRIMARY KEY is (pk, base_pk, base_sk*), so (pk,
                    // base_pk) alone is not a total order and would skip rows
                    // sharing a (pk, base_pk) across a page boundary.
                    let base_pk_text = base_pk_from_start_key(start_key, key_info)?;
                    if let Some((base_name, base_type)) = &base_sk_info {
                        let base_col = format!("base_{}", sk_column(*base_type));
                        conditions.push(format!("(pk, base_pk, {base_col}) > (?, ?, ?)"));
                        binds.push(BoundValue::Text(pk_text));
                        binds.push(BoundValue::Text(base_pk_text));
                        if let Some(v) = start_key.get(base_name.as_str()) {
                            binds.push(sk_bound(&parse_sk(v, *base_type)?));
                        } else {
                            binds.push(BoundValue::Text(String::new()));
                        }
                    } else {
                        conditions.push("(pk, base_pk) > (?, ?)".to_owned());
                        binds.push(BoundValue::Text(pk_text));
                        binds.push(BoundValue::Text(base_pk_text));
                    }
                }
            } else if let Some((sk_name, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                conditions.push(format!("(pk, {sk_col}) > (?, ?)"));
                binds.push(BoundValue::Text(pk_text));
                if let Some(v) = start_key.get(sk_name) {
                    binds.push(sk_bound(&parse_sk(v, sk_type)?));
                } else {
                    binds.push(BoundValue::Text(String::new()));
                }
            } else {
                conditions.push("pk > ?".to_owned());
                binds.push(BoundValue::Text(pk_text));
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Deterministic ordering for pagination.
        if index_name.is_some() {
            if let Some((_, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                if let Some((_, base_type)) = &base_sk_info {
                    let base_col = format!("base_{}", sk_column(*base_type));
                    let _ = write!(sql, " ORDER BY pk, {sk_col}, base_pk, {base_col}");
                } else {
                    let _ = write!(sql, " ORDER BY pk, {sk_col}, base_pk");
                }
            } else if let Some((_, base_type)) = &base_sk_info {
                // Hash-only GSI on a composite-key base table: order by the full
                // index key so it is a total order matching the pagination
                // predicate above.
                let base_col = format!("base_{}", sk_column(*base_type));
                let _ = write!(sql, " ORDER BY pk, base_pk, {base_col}");
            } else {
                let _ = write!(sql, " ORDER BY pk, base_pk");
            }
        } else if let Some((_, sk_type)) = sk_info_val {
            let _ = write!(sql, " ORDER BY pk, {}", sk_column(sk_type));
        } else {
            sql.push_str(" ORDER BY pk");
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        let rows = execute_dynamic_query(&sql, binds, &self.pool).await?;
        finalize(rows, limit, &key_info.key_schema)
    }
}

/// Resolve a key-condition path's attribute name, handling `#name` references.
fn resolve_attr_name(path: &[PathElement], maps: &ExpressionMaps) -> Option<String> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(reference) = name.strip_prefix('#') {
                maps.names.get(reference).cloned()
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    }
}

/// Extract the base-table partition key text from a (combined) start key.
impl SqliteEngine {
    /// Whether `key` belongs to `segment` under the rowid partitioning the
    /// scan predicate uses (`rowid % total_segments = segment`).
    ///
    /// rowid is storage identity, not key content, so membership is only
    /// decidable for a key whose row still exists. A key that resolves to no
    /// row returns `true` (indeterminate, permitted): DynamoDB accepts an
    /// `ExclusiveStartKey` that has been deleted since the previous page, and
    /// refusing it would reject legitimate resumptions. A row that does exist
    /// answers definitively, which is what makes the engine's cross-segment
    /// refusal discriminating on this backend.
    pub(crate) async fn scan_key_in_segment_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        segment: i64,
        total_segments: i64,
        index_name: Option<&str>,
    ) -> Result<bool, StorageError> {
        let ddb_table = if let Some(idx_name) = index_name {
            let info = self
                .fetch_index_info_by_table_id(&key_info.table_id, idx_name)
                .await?;
            index_table_name(&info.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };

        let pk_name = &key_info.key_schema[0].attribute_name;
        let Some(pk_value) = key.get(pk_name) else {
            // Schema validation upstream already rejects this shape.
            return Ok(false);
        };
        let pk_text = pk_to_text(pk_value)?.into_owned();

        let mut conditions: Vec<String> = vec!["pk = ?".to_owned()];
        let mut binds: Vec<BoundValue> = vec![BoundValue::Text(pk_text)];

        let sk_info_val = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        if let Some((sk_name, sk_type)) = sk_info_val
            && let Some(v) = key.get(sk_name)
        {
            conditions.push(format!("{} = ?", sk_column(sk_type)));
            binds.push(sk_bound(&parse_sk(v, sk_type)?));
        }

        if index_name.is_some() {
            conditions.push("base_pk = ?".to_owned());
            binds.push(BoundValue::Text(base_pk_from_start_key(key, key_info)?));
            if let Some((base_name, base_type)) =
                sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
                && let Some(v) = key.get(base_name)
            {
                conditions.push(format!("base_{} = ?", sk_column(base_type)));
                binds.push(sk_bound(&parse_sk(v, base_type)?));
            }
        }

        let sql = format!(
            "SELECT rowid FROM {ddb_table} WHERE {}",
            conditions.join(" AND ")
        );
        let mut query = sqlx::query_as::<_, (i64,)>(&sql);
        for bound in binds {
            query = super::bind_bound!(query, bound);
        }
        let row: Option<(i64,)> = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(match row {
            Some((rowid,)) => rowid.rem_euclid(total_segments) == segment,
            None => true,
        })
    }
}

fn base_pk_from_start_key(
    start_key: &Item,
    key_info: &TableKeyInfo,
) -> Result<String, StorageError> {
    let base_pk_attr = &key_info.base_key_schema[0].attribute_name;
    start_key
        .get(base_pk_attr)
        .map(pk_to_text)
        .transpose()?
        .map(|c| c.into_owned())
        .ok_or_else(|| {
            StorageError::Validation(
                "The provided starting key is invalid: missing base table partition key".to_owned(),
            )
        })
}

/// Append the query pagination predicate and its binds, mirroring the
/// PostgreSQL `build_pagination_where` cases.
#[allow(clippy::too_many_arguments)]
fn append_query_pagination(
    sql: &mut String,
    binds: &mut Vec<BoundValue>,
    start_key: &Item,
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: Option<&(String, ScalarAttributeType)>,
    key_info: &TableKeyInfo,
    is_index: bool,
    is_lsi: bool,
    forward: bool,
) -> Result<(), StorageError> {
    let cmp = if forward { ">" } else { "<" };

    if let Some((sk_name, sk_type)) = sk_info_val {
        let sk_col = sk_column(sk_type);
        let sk_bv = start_key
            .get(sk_name)
            .map(|v| parse_sk(v, sk_type))
            .transpose()?
            .map(|s| sk_bound(&s));

        if let Some((base_name, base_type)) = base_sk_info {
            let base_col = format!("base_{}", sk_column(*base_type));
            let sk_bv = sk_bv.unwrap_or(BoundValue::Text(String::new()));
            let base_sk_bv = if let Some(v) = start_key.get(base_name.as_str()) {
                sk_bound(&parse_sk(v, *base_type)?)
            } else {
                BoundValue::Text(String::new())
            };

            if is_lsi {
                // LSI: every row shares the queried partition key, so the base
                // table's sort key alone identifies a row uniquely, and it is a
                // user-visible sort dimension so it follows ScanIndexForward.
                let _ = write!(
                    sql,
                    " AND ({sk_col} {cmp} ? OR ({sk_col} = ? AND {base_col} {cmp} ?))"
                );
                binds.push(sk_bv.clone());
                binds.push(sk_bv);
                binds.push(base_sk_bv);
            } else {
                // GSI: the tie-breaker must be the FULL base primary key. Rows
                // in a GSI partition are unique on (index SK, base PK, base SK),
                // not on (index SK, base SK): many base partitions can project
                // the same index SK and the same base SK. Comparing base SK
                // alone made a page-two query return nothing whenever the rows
                // sharing an index SK also shared a base SK, so a paginating
                // client silently stopped after page one.
                let base_pk_bv = BoundValue::Text(base_pk_from_start_key(start_key, key_info)?);
                let _ = write!(
                    sql,
                    " AND ({sk_col} {cmp} ? OR ({sk_col} = ? AND (base_pk > ? \
                     OR (base_pk = ? AND {base_col} > ?))))"
                );
                binds.push(sk_bv.clone());
                binds.push(sk_bv);
                binds.push(base_pk_bv.clone());
                binds.push(base_pk_bv);
                binds.push(base_sk_bv);
            }
        } else if is_index {
            let _ = write!(
                sql,
                " AND ({sk_col} {cmp} ? OR ({sk_col} = ? AND base_pk > ?))"
            );
            let sk_bv = sk_bv.unwrap_or(BoundValue::Text(String::new()));
            binds.push(sk_bv.clone());
            binds.push(sk_bv);
            binds.push(BoundValue::Text(base_pk_from_start_key(
                start_key, key_info,
            )?));
        } else {
            let _ = write!(sql, " AND {sk_col} {cmp} ?");
            binds.push(sk_bv.unwrap_or(BoundValue::Text(String::new())));
        }
    } else if is_index {
        let base_pk_text = base_pk_from_start_key(start_key, key_info)?;
        if let Some((base_name, base_type)) = base_sk_info {
            let base_col = format!("base_{}", sk_column(*base_type));
            let _ = write!(
                sql,
                " AND (base_pk > ? OR (base_pk = ? AND {base_col} > ?))"
            );
            binds.push(BoundValue::Text(base_pk_text.clone()));
            binds.push(BoundValue::Text(base_pk_text));
            if let Some(v) = start_key.get(base_name.as_str()) {
                binds.push(sk_bound(&parse_sk(v, *base_type)?));
            } else {
                binds.push(BoundValue::Text(String::new()));
            }
        } else {
            let _ = write!(sql, " AND base_pk > ?");
            binds.push(BoundValue::Text(base_pk_text));
        }
    }
    Ok(())
}

/// Trim the over-fetched extra row, deserialize items, and derive the
/// `LastEvaluatedKey` (storage-side: the queried table's own key; the engine
/// enriches index LEKs with base-table key attributes).
fn finalize(
    rows: Vec<serde_json::Value>,
    limit: Option<i64>,
    key_schema: &[extenddb_core::types::KeySchemaElement],
) -> Result<(Vec<Item>, Option<Item>), StorageError> {
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
    let has_more = rows.len() > actual_limit;
    let items: Vec<Item> = rows
        .into_iter()
        .take(actual_limit)
        .map(json_to_item)
        .collect::<Result<Vec<_>, _>>()?;
    let last_key = if has_more {
        items.last().map(|item| build_key(item, key_schema))
    } else {
        None
    };
    Ok((items, last_key))
}
