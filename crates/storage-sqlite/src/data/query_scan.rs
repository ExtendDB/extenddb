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

        let Some((sql, binds)) = query_statement(
            &ddb_table,
            is_lsi,
            key_info,
            key_condition,
            maps,
            forward,
            limit,
            exclusive_start_key,
            index_name.is_some(),
        )?
        else {
            return Ok((Vec::new(), None));
        };
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

/// The statement and binds for one Query page on `ddb_table` (a data table or
/// an index table). `None` when the page is known to be empty without a read:
/// a start key on a hash-only base table, whose partition holds one item.
///
/// Separate from [`SqliteEngine::query_impl`] so the plan tests can run
/// `EXPLAIN QUERY PLAN` on exactly the statement a Query sends.
#[allow(clippy::too_many_arguments)]
fn query_statement(
    ddb_table: &str,
    is_lsi: bool,
    key_info: &TableKeyInfo,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    forward: bool,
    limit: Option<i64>,
    exclusive_start_key: Option<&Item>,
    is_index: bool,
) -> Result<Option<(String, Vec<BoundValue>)>, StorageError> {
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
    let base_sk_info: Option<(String, ScalarAttributeType)> = if is_index {
        sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
            .map(|(n, t)| (n.to_owned(), t))
    } else {
        None
    };

    let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
    let mut binds: Vec<BoundValue> = vec![BoundValue::Text(pk_text.clone())];

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
    if exclusive_start_key.is_some() && sk_info_val.is_none() && !is_index {
        return Ok(None);
    }
    if let Some(start_key) = exclusive_start_key {
        append_query_pagination(
            &mut sql,
            &mut binds,
            start_key,
            &pk_text,
            sk_info_val,
            base_sk_info.as_ref(),
            key_info,
            is_index,
            is_lsi,
            forward,
        )?;
    }

    // ORDER BY: the same columns the page cursor compares, every one in the
    // direction of ScanIndexForward, so a reverse Query is the exact mirror
    // of the forward one (as DynamoDB's is) and the cursor predicate is a
    // row comparison over the key index.
    let order_cols = query_order_columns(sk_info_val, base_sk_info.as_ref(), is_index);
    if !order_cols.is_empty() {
        let dir = if forward { "ASC" } else { "DESC" };
        let ordered = order_cols
            .iter()
            .map(|c| format!("{c} {dir}"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(sql, " ORDER BY {ordered}");
    }

    let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
    let _ = write!(sql, " LIMIT {fetch_limit}");
    Ok(Some((sql, binds)))
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

/// The columns that order a Query's rows and that its page cursor compares,
/// in order, mirroring the PostgreSQL `order_by_columns` cases. Empty for a
/// hash-only base table, which has no order to resume within a partition.
///
/// - Base table with a sort key: the sort key.
/// - Index with a sort key (GSI or LSI): the index sort key, then the FULL
///   base primary key. Rows in a GSI partition are unique on (index SK, base
///   PK, base SK), not on (index SK, base SK): many base partitions can
///   project the same index SK and the same base SK, and comparing base SK
///   alone made a page-two query return nothing whenever the rows sharing an
///   index SK also shared a base SK. In an LSI partition `base_pk` equals the
///   queried `pk` on every row, so it does not change the order, but it keeps
///   the `ORDER BY` identical to `idx_order_*` on `(pk, sk, base_pk,
///   base_sk)`. Without it SQLite reads the index in index sort key order
///   only and sorts the rest in a temporary B-tree.
/// - Hash-only index: the full base primary key.
fn query_order_columns(
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: Option<&(String, ScalarAttributeType)>,
    is_index: bool,
) -> Vec<String> {
    let base_sk = base_sk_info.map(|(_, t)| format!("base_{}", sk_column(*t)));
    let mut cols = Vec::with_capacity(3);
    if let Some((_, sk_type)) = sk_info_val {
        cols.push(sk_column(sk_type).to_owned());
        if !is_index {
            return cols;
        }
        cols.push("base_pk".to_owned());
        cols.extend(base_sk);
    } else if is_index {
        cols.push("base_pk".to_owned());
        cols.extend(base_sk);
    }
    cols
}

/// Append the query pagination predicate and its binds, mirroring the
/// PostgreSQL `build_pagination_where` cases.
///
/// The cursor columns are exactly the `ORDER BY` columns of the same query
/// (see [`query_order_columns`]) and every one of them runs in the direction
/// of `ScanIndexForward`, so the predicate is always a row comparison,
/// `(a, b) > (?, ?)` or `(a, b) < (?, ?)`, which SQLite serves with one seek
/// on the key index. The tie-breaker columns follow the direction too:
/// DynamoDB returns a reverse Query as the exact mirror of the forward one,
/// including among items that share an index sort key and among the items of
/// a hash-only index partition (measured 2026-09-17). Where the tie-breaker
/// was the whole order (a hash-only index) an ascending cursor under a
/// descending `ORDER BY` repeated the first page's items and skipped the rest.
///
/// `partition_key` is the queried partition key text. An LSI binds it as the
/// cursor's `base_pk`, because every row of the partition carries it there.
/// `base_pk` then compares equal on every row, and only the base sort key
/// separates a row from the cursor. The statement does not read the start
/// key's partition key: the bind keeps the page inside the queried partition.
/// DynamoDB refuses a start key that names another partition with a
/// ValidationException; that check belongs to request validation, not to this
/// statement. A GSI binds the start key's base partition key.
#[allow(clippy::too_many_arguments)]
fn append_query_pagination(
    sql: &mut String,
    binds: &mut Vec<BoundValue>,
    start_key: &Item,
    partition_key: &str,
    sk_info_val: Option<(&str, ScalarAttributeType)>,
    base_sk_info: Option<&(String, ScalarAttributeType)>,
    key_info: &TableKeyInfo,
    is_index: bool,
    is_lsi: bool,
    forward: bool,
) -> Result<(), StorageError> {
    let cols = query_order_columns(sk_info_val, base_sk_info, is_index);
    if cols.is_empty() {
        return Ok(());
    }
    // A missing sort-key attribute in the start key binds as the empty
    // string, which sorts before every other text value.
    let sk_bind = |name: &str, ty: ScalarAttributeType| -> Result<BoundValue, StorageError> {
        Ok(match start_key.get(name) {
            Some(v) => sk_bound(&parse_sk(v, ty)?),
            None => BoundValue::Text(String::new()),
        })
    };
    // Binds in the same order as the columns.
    let mut cursor: Vec<BoundValue> = Vec::with_capacity(cols.len());
    if let Some((sk_name, sk_type)) = sk_info_val {
        // The sort key first; a base table's cursor is the sort key alone.
        cursor.push(sk_bind(sk_name, sk_type)?);
        if is_index {
            cursor.push(BoundValue::Text(if is_lsi {
                partition_key.to_owned()
            } else {
                base_pk_from_start_key(start_key, key_info)?
            }));
            if let Some((base_name, base_type)) = base_sk_info {
                cursor.push(sk_bind(base_name, *base_type)?);
            }
        }
    } else {
        cursor.push(BoundValue::Text(base_pk_from_start_key(
            start_key, key_info,
        )?));
        if let Some((base_name, base_type)) = base_sk_info {
            cursor.push(sk_bind(base_name, *base_type)?);
        }
    }
    debug_assert_eq!(cursor.len(), cols.len());

    let cmp = if forward { ">" } else { "<" };
    let marks = vec!["?"; cols.len()].join(", ");
    if cols.len() == 1 {
        let _ = write!(sql, " AND {} {cmp} ?", cols[0]);
    } else {
        let _ = write!(sql, " AND ({}) {cmp} ({marks})", cols.join(", "));
    }
    binds.extend(cursor);
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

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeDefinition, AttributeValue, KeySchemaElement, KeyType};

    const S: ScalarAttributeType = ScalarAttributeType::S;
    const N: ScalarAttributeType = ScalarAttributeType::N;

    fn key_info(base_sk: Option<ScalarAttributeType>) -> TableKeyInfo {
        let mut base_key_schema = vec![KeySchemaElement {
            attribute_name: "bpk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let mut attribute_definitions = vec![
            AttributeDefinition {
                attribute_name: "bpk".to_owned(),
                attribute_type: S,
            },
            AttributeDefinition {
                attribute_name: "gsk".to_owned(),
                attribute_type: S,
            },
        ];
        if let Some(t) = base_sk {
            base_key_schema.push(KeySchemaElement {
                attribute_name: "bsk".to_owned(),
                key_type: KeyType::Range,
            });
            attribute_definitions.push(AttributeDefinition {
                attribute_name: "bsk".to_owned(),
                attribute_type: t,
            });
        }
        TableKeyInfo {
            table_name: "t".to_owned(),
            account_id: "a".to_owned(),
            table_id: "id".to_owned(),
            key_schema: base_key_schema.clone(),
            base_key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        }
    }

    fn start_key(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), AttributeValue::S((*v).to_owned())))
            .collect()
    }

    fn start_key_n(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), AttributeValue::N((*v).to_owned())))
            .collect()
    }

    fn texts(binds: &[BoundValue]) -> Vec<String> {
        binds
            .iter()
            .map(|b| match b {
                BoundValue::Text(t) => t.clone(),
                BoundValue::Blob(b) => format!("blob:{}", b.len()),
            })
            .collect()
    }

    fn paginate(
        sk: Option<(&str, ScalarAttributeType)>,
        base_sk: Option<ScalarAttributeType>,
        is_index: bool,
        is_lsi: bool,
        forward: bool,
        key: &Item,
    ) -> (String, Vec<String>) {
        paginate_in(sk, base_sk, is_index, is_lsi, forward, key, "p")
    }

    /// [`paginate`] with the queried partition key given explicitly.
    fn paginate_in(
        sk: Option<(&str, ScalarAttributeType)>,
        base_sk: Option<ScalarAttributeType>,
        is_index: bool,
        is_lsi: bool,
        forward: bool,
        key: &Item,
        partition_key: &str,
    ) -> (String, Vec<String>) {
        let ki = key_info(base_sk);
        let base_sk_info = base_sk.map(|t| ("bsk".to_owned(), t));
        let mut sql = String::new();
        let mut binds = Vec::new();
        append_query_pagination(
            &mut sql,
            &mut binds,
            key,
            partition_key,
            sk,
            base_sk_info.as_ref(),
            &ki,
            is_index,
            is_lsi,
            forward,
        )
        .expect("well-formed start key");
        (sql, texts(&binds))
    }

    /// The fragments are pinned as strings because the row-comparison and the
    /// expanded OR forms return the same rows for a matching ORDER BY; only
    /// the plan and the reverse tie order tell them apart.
    #[test]
    fn base_table_cursor_is_a_single_comparison_in_both_directions() {
        let key = start_key(&[("bpk", "p"), ("sk", "m")]);
        assert_eq!(
            paginate(Some(("sk", S)), None, false, false, true, &key),
            (" AND sk_s > ?".to_owned(), vec!["m".to_owned()])
        );
        assert_eq!(
            paginate(Some(("sk", S)), None, false, false, false, &key),
            (" AND sk_s < ?".to_owned(), vec!["m".to_owned()])
        );
    }

    #[test]
    fn gsi_cursor_is_a_row_comparison_over_the_full_base_key_in_both_directions() {
        let key = start_key(&[("gsk", "g"), ("bpk", "p"), ("bsk", "s")]);
        let want_binds = vec!["g".to_owned(), "p".to_owned(), "s".to_owned()];
        assert_eq!(
            paginate(Some(("gsk", S)), Some(S), true, false, true, &key),
            (
                " AND (sk_s, base_pk, base_sk_s) > (?, ?, ?)".to_owned(),
                want_binds.clone()
            )
        );
        // The tie-breaker follows the direction: DynamoDB's reverse Query is
        // the exact mirror of its forward one, including among items that
        // share an index sort key (measured 2026-09-17).
        assert_eq!(
            paginate(Some(("gsk", S)), Some(S), true, false, false, &key),
            (
                " AND (sk_s, base_pk, base_sk_s) < (?, ?, ?)".to_owned(),
                want_binds
            )
        );
    }

    #[test]
    fn gsi_on_a_hash_only_base_table_uses_base_pk_as_the_tie_breaker() {
        let key = start_key(&[("gsk", "g"), ("bpk", "p")]);
        assert_eq!(
            paginate(Some(("gsk", S)), None, true, false, true, &key),
            (
                " AND (sk_s, base_pk) > (?, ?)".to_owned(),
                vec!["g".to_owned(), "p".to_owned()]
            )
        );
        assert_eq!(
            paginate(Some(("gsk", S)), None, true, false, false, &key),
            (
                " AND (sk_s, base_pk) < (?, ?)".to_owned(),
                vec!["g".to_owned(), "p".to_owned()]
            )
        );
    }

    #[test]
    fn lsi_cursor_compares_the_ordering_index_columns() {
        // idx_order_* is (pk, sk, base_pk, base_sk). A cursor and ORDER BY
        // that skip base_pk leave SQLite an index that orders the index sort
        // key only, and it sorts the rest in a temporary B-tree.
        let key = start_key(&[("lsk", "l"), ("bpk", "p"), ("bsk", "s")]);
        let want_binds = vec!["l".to_owned(), "p".to_owned(), "s".to_owned()];
        assert_eq!(
            paginate(Some(("lsk", S)), Some(S), true, true, true, &key),
            (
                " AND (sk_s, base_pk, base_sk_s) > (?, ?, ?)".to_owned(),
                want_binds.clone()
            )
        );
        assert_eq!(
            paginate(Some(("lsk", S)), Some(S), true, true, false, &key),
            (
                " AND (sk_s, base_pk, base_sk_s) < (?, ?, ?)".to_owned(),
                want_binds
            )
        );
    }

    #[test]
    fn lsi_cursor_binds_the_queried_partition_key_as_base_pk() {
        // Every row of an LSI partition stores the queried partition key as
        // base_pk, and the cursor binds that value, not the start key's
        // partition key, so the page stays inside the queried partition. A
        // GSI binds the start key's base partition key.
        let key = start_key(&[("lsk", "l"), ("bpk", "other"), ("bsk", "s")]);
        assert_eq!(
            paginate_in(Some(("lsk", S)), Some(S), true, true, true, &key, "p").1,
            vec!["l".to_owned(), "p".to_owned(), "s".to_owned()]
        );
        let key = start_key(&[("gsk", "g"), ("bpk", "other"), ("bsk", "s")]);
        assert_eq!(
            paginate_in(Some(("gsk", S)), Some(S), true, false, true, &key, "g").1,
            vec!["g".to_owned(), "other".to_owned(), "s".to_owned()]
        );
    }

    #[test]
    fn hash_only_index_cursor_follows_the_direction() {
        // The defect: a reverse Query on a hash-only index ordered
        // `base_pk DESC` but its cursor read `base_pk > ?`, so page two after
        // (p5, p4) asked for keys above p4, returned p5 again, and then ended
        // with p0..p3 never delivered.
        let key = start_key(&[("bpk", "p4")]);
        assert_eq!(
            paginate(None, None, true, false, true, &key),
            (" AND base_pk > ?".to_owned(), vec!["p4".to_owned()])
        );
        assert_eq!(
            paginate(None, None, true, false, false, &key),
            (" AND base_pk < ?".to_owned(), vec!["p4".to_owned()])
        );
        let key = start_key(&[("bpk", "p4"), ("bsk", "s")]);
        assert_eq!(
            paginate(None, Some(S), true, false, false, &key),
            (
                " AND (base_pk, base_sk_s) < (?, ?)".to_owned(),
                vec!["p4".to_owned(), "s".to_owned()]
            )
        );
    }

    #[test]
    fn missing_sort_key_attributes_bind_as_the_empty_string() {
        // An index start key without the base sort key still produces one
        // bind per column, so the placeholder count always matches.
        let mut key = start_key_n(&[("gsk", "7")]);
        key.insert("bpk".to_owned(), AttributeValue::S("p".to_owned()));
        let (sql, binds) = paginate(Some(("gsk", N)), Some(N), true, false, true, &key);
        assert_eq!(sql, " AND (sk_n, base_pk, base_sk_n) > (?, ?, ?)");
        assert_eq!(binds.len(), 3);
        assert_eq!(binds[1], "p");
        assert_eq!(binds[2], "");
    }

    #[test]
    fn order_by_columns_match_the_cursor_columns_for_every_shape() {
        type Shape<'a> = (
            Option<(&'a str, ScalarAttributeType)>,
            Option<ScalarAttributeType>,
            bool,
            bool,
        );
        let shapes: [Shape<'_>; 7] = [
            (Some(("sk", S)), None, false, false),
            (Some(("gsk", S)), Some(S), true, false),
            (Some(("gsk", N)), Some(N), true, false),
            (Some(("gsk", S)), None, true, false),
            (Some(("lsk", S)), Some(S), true, true),
            (None, Some(S), true, false),
            (None, None, true, false),
        ];
        for (sk, base_sk, is_index, is_lsi) in shapes {
            // Value types follow the shape: N-typed keys take N values.
            let numeric = matches!(sk, Some((_, N)));
            let key = if numeric {
                let mut k = start_key_n(&[("gsk", "7"), ("bsk", "3")]);
                k.insert("bpk".to_owned(), AttributeValue::S("p".to_owned()));
                k
            } else {
                start_key(&[
                    ("sk", "x"),
                    ("gsk", "g"),
                    ("lsk", "l"),
                    ("bpk", "p"),
                    ("bsk", "s"),
                ])
            };
            let base_sk_info = base_sk.map(|t| ("bsk".to_owned(), t));
            let cols = query_order_columns(sk, base_sk_info.as_ref(), is_index);
            let marks = vec!["?"; cols.len()].join(", ");
            let (cols_sql, marks_sql) = if cols.len() == 1 {
                (cols[0].clone(), "?".to_owned())
            } else {
                (format!("({})", cols.join(", ")), format!("({marks})"))
            };
            let (fwd, fwd_binds) = paginate(sk, base_sk, is_index, is_lsi, true, &key);
            let (rev, rev_binds) = paginate(sk, base_sk, is_index, is_lsi, false, &key);
            assert_eq!(
                fwd,
                format!(" AND {cols_sql} > {marks_sql}"),
                "forward {sk:?} {base_sk:?}"
            );
            assert_eq!(
                rev,
                format!(" AND {cols_sql} < {marks_sql}"),
                "reverse {sk:?} {base_sk:?}"
            );
            assert_eq!(fwd_binds.len(), cols.len(), "bind count {sk:?} {base_sk:?}");
            assert_eq!(fwd_binds, rev_binds);
        }
        // A hash-only base table has nothing to resume within a partition.
        assert!(query_order_columns(None, None, false).is_empty());
        let (sql, binds) = paginate(None, None, false, false, true, &start_key(&[("bpk", "p")]));
        assert!(sql.is_empty());
        assert!(binds.is_empty());
    }
}

/// The plan SQLite chooses for the statement an index Query sends.
///
/// Each index table whose index has a sort key has an ordering index
/// `idx_order_*` on `(pk, sk*, base_pk, base_sk*)`. A Query whose `ORDER BY`
/// is that index minus the constant `pk` is a walk of the index; one that
/// skips a column of it makes SQLite sort the rows the walk reads, shown as
/// `USE TEMP B-TREE FOR ... ORDER BY` in `EXPLAIN QUERY PLAN`. The rows come
/// back in the same order either way, so these tests read the plan.
#[cfg(test)]
mod plan_tests {
    use super::*;
    use extenddb_core::expression::{parse_key_condition, tokenize};
    use extenddb_core::types::{AttributeValue, CreateTableInput, IndexType};
    use extenddb_storage::DataEngine;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

    const ACCOUNT: &str = "000000000000";
    const TABLE: &str = "lsi-plan";

    async fn engine() -> SqliteEngine {
        let engine = SqliteEngine::new(":memory:", 2, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query(
            "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default')",
        )
        .execute(&engine.pool)
        .await
        .expect("account");
        // Tables become ACTIVE at create time: no transition poller runs here.
        sqlx::query(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
        )
        .execute(&engine.pool)
        .await
        .expect("settings");
        engine
    }

    /// A (pk S, sk S) table with an LSI on (pk, lsk S) and a GSI on (gpk S,
    /// gsk S), and 300 items in partition `p` sharing 30 index sort key values.
    async fn table(engine: &SqliteEngine) -> TableKeyInfo {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": TABLE,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gpk", "KeyType": "HASH"},
                    {"AttributeName": "gsk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl(ACCOUNT, input, false)
            .await
            .expect("create table");
        let key_info = engine
            .fetch_table_key_info(ACCOUNT, TABLE)
            .await
            .expect("key info");
        let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
        for i in 0..300 {
            let s = |v: String| AttributeValue::S(v);
            let item: Item = BTreeMap::from([
                ("pk".to_owned(), s("p".to_owned())),
                ("sk".to_owned(), s(format!("s{i:04}"))),
                ("lsk".to_owned(), s(format!("l{:02}", i % 30))),
                ("gpk".to_owned(), s("g".to_owned())),
                ("gsk".to_owned(), s(format!("l{:02}", i % 30))),
            ]);
            engine
                .put_item(&key_info, item, false, None, &maps, None)
                .await
                .expect("put an item");
        }
        key_info
    }

    /// The table's key info as the engine passes it for a Query on `index`,
    /// with the index table name and whether the index is an LSI.
    async fn index_query_target(
        engine: &SqliteEngine,
        base: &TableKeyInfo,
        index: &str,
    ) -> (TableKeyInfo, String, bool) {
        let info = engine
            .fetch_index_info_by_table_id(&base.table_id, index)
            .await
            .expect("index info");
        let key_info = TableKeyInfo {
            key_schema: info.key_schema.clone(),
            base_key_schema: base.key_schema.clone(),
            ..base.clone()
        };
        (
            key_info,
            index_table_name(&info.index_id),
            info.index_type == IndexType::Lsi,
        )
    }

    fn partition(attr: &str, value: &str) -> (KeyCondition, ExpressionMaps) {
        condition(&format!("{attr} = :v"), &[("v", value)])
    }

    /// A key condition and its value map; `values` name the placeholders
    /// without the colon, as the engine passes them.
    fn condition(expr: &str, values: &[(&str, &str)]) -> (KeyCondition, ExpressionMaps) {
        let tokens = tokenize(expr).expect("tokenize");
        let cond = parse_key_condition(&tokens).expect("parse");
        let values = values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), AttributeValue::S((*value).to_owned())))
            .collect();
        (cond, ExpressionMaps::new(HashMap::new(), values))
    }

    fn s_key(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), AttributeValue::S((*v).to_owned())))
            .collect()
    }

    /// `EXPLAIN QUERY PLAN` detail lines for a statement and its binds.
    async fn plan(engine: &SqliteEngine, sql: &str, binds: Vec<BoundValue>) -> Vec<String> {
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(&explain);
        for bound in binds {
            query = crate::data::bind_bound!(query, bound);
        }
        query
            .fetch_all(&engine.pool)
            .await
            .expect("explain the statement")
            .into_iter()
            .map(|(_, _, _, detail)| detail)
            .collect()
    }

    /// Plan the Query statement for one page and assert it is a walk of the
    /// ordering index with no sort step.
    #[allow(clippy::too_many_arguments)]
    async fn assert_index_walk(
        engine: &SqliteEngine,
        label: &str,
        key_info: &TableKeyInfo,
        ddb_table: &str,
        is_lsi: bool,
        cond: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        start: Option<&Item>,
    ) {
        let (sql, binds) = query_statement(
            ddb_table,
            is_lsi,
            key_info,
            cond,
            maps,
            forward,
            Some(100),
            start,
            true,
        )
        .expect("build the statement")
        .expect("an index Query always reads");
        let plan = plan(engine, &sql, binds).await;
        let shown = format!("{sql}\n  {}", plan.join("\n  "));
        assert!(
            !plan.iter().any(|l| l.contains("TEMP B-TREE")),
            "{label}: the plan sorts in a temporary B-tree:\n{shown}"
        );
        assert!(
            plan.iter()
                .any(|l| l.contains("USING INDEX") && l.contains("idx_order_")),
            "{label}: the plan does not walk the ordering index:\n{shown}"
        );
    }

    #[tokio::test]
    async fn lsi_query_walks_the_ordering_index_without_a_sort() {
        let engine = engine().await;
        let base = table(&engine).await;
        let (lsi, lsi_table, is_lsi) = index_query_target(&engine, &base, "lsi1").await;
        assert!(is_lsi);
        // The partition alone, one index sort key value, and a range of them.
        let shapes = [
            ("pk only", partition("pk", "p")),
            (
                "lsk = :l",
                condition("pk = :v AND lsk = :l", &[("v", "p"), ("l", "l07")]),
            ),
            (
                "lsk BETWEEN",
                condition(
                    "pk = :v AND lsk BETWEEN :a AND :b",
                    &[("v", "p"), ("a", "l05"), ("b", "l10")],
                ),
            ),
        ];
        // A start key inside a group of rows sharing an index sort key value.
        let start = s_key(&[("pk", "p"), ("lsk", "l07"), ("sk", "s0127")]);
        for (shape, (cond, maps)) in &shapes {
            for forward in [true, false] {
                for start in [None, Some(&start)] {
                    let label =
                        format!("LSI {shape} forward={forward} resumed={}", start.is_some());
                    assert_index_walk(
                        &engine, &label, &lsi, &lsi_table, true, cond, maps, forward, start,
                    )
                    .await;
                }
            }
        }
    }

    #[tokio::test]
    async fn gsi_query_walks_the_ordering_index_without_a_sort() {
        let engine = engine().await;
        let base = table(&engine).await;
        let (gsi, gsi_table, is_lsi) = index_query_target(&engine, &base, "gsi1").await;
        assert!(!is_lsi);
        let (cond, maps) = partition("gpk", "g");
        let start = s_key(&[("gpk", "g"), ("gsk", "l07"), ("pk", "p"), ("sk", "s0127")]);
        for forward in [true, false] {
            for start in [None, Some(&start)] {
                let label = format!("GSI forward={forward} resumed={}", start.is_some());
                assert_index_walk(
                    &engine, &label, &gsi, &gsi_table, false, &cond, &maps, forward, start,
                )
                .await;
            }
        }
    }

    /// The pages of an LSI Query in both directions: ordered by the index sort
    /// key, then the base sort key, with each page resuming after the last row
    /// of the one before, including inside a group of equal index sort keys.
    #[tokio::test]
    async fn lsi_query_pages_follow_the_index_order_in_both_directions() {
        let engine = engine().await;
        let base = table(&engine).await;
        let (lsi, _, _) = index_query_target(&engine, &base, "lsi1").await;
        let (cond, maps) = partition("pk", "p");
        let mut want: Vec<(String, String)> = (0..300)
            .map(|i| (format!("l{:02}", i % 30), format!("s{i:04}")))
            .collect();
        want.sort();
        for forward in [true, false] {
            let mut got = Vec::new();
            let mut start: Option<Item> = None;
            loop {
                let (items, last) = engine
                    .query_impl(
                        &lsi,
                        &cond,
                        &maps,
                        forward,
                        Some(7),
                        start.as_ref(),
                        Some("lsi1"),
                    )
                    .await
                    .expect("query a page");
                let text = |item: &Item, attr: &str| match item.get(attr) {
                    Some(AttributeValue::S(v)) => v.clone(),
                    other => panic!("{attr}: {other:?}"),
                };
                got.extend(items.iter().map(|i| (text(i, "lsk"), text(i, "sk"))));
                if last.is_none() {
                    break;
                }
                // The engine adds the base sort key to an index page's key.
                let tail = items.last().expect("a page with a key has items");
                start = Some(s_key(&[
                    ("pk", "p"),
                    ("lsk", &text(tail, "lsk")),
                    ("sk", &text(tail, "sk")),
                ]));
            }
            let mut expected = want.clone();
            if !forward {
                expected.reverse();
            }
            assert_eq!(got, expected, "forward={forward}");
        }
    }
}
