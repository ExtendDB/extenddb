// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and scan SQL helpers for the `PostgreSQL` backend.
//!
//! Contains condition evaluation, sort-key SQL generation, and dynamic
//! parameter binding for `Query` and `Scan` operations.

use extenddb_core::expression::{self, Expr, ExpressionMaps, KeyCondition, SortKeyCondition};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, ScalarAttributeType, extract_key,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_info};

/// Extra pagination bind values for index queries.
///
/// Built in `query_scan.rs` in the same branch that generates the SQL placeholders,
/// then consumed here for binding. This ensures the bind order cannot diverge from
/// the SQL — the variant determines exactly which values are bound and in what order.
pub(crate) enum PaginationBinds {
    /// Base table query or index query with no extra pagination binds needed.
    None,
    /// Index query where base table has no SK — only base_pk as tie-breaker.
    BasePkOnly { pk_text: String },
    /// Index query where base table has a SK — base_sk as tie-breaker.
    BaseSkOnly { sk: SortKeyValue },
    /// Hash-only index where base table has a SK — both base_pk and base_sk.
    BasePkAndSk { pk_text: String, sk: SortKeyValue },
}

/// Evaluate a condition expression against an item inside a transaction.
///
/// Returns `Ok(())` if the condition passes or is `None`.
/// Returns `Err(StorageError::ConditionFailed)` if the condition fails.
pub(crate) fn check_condition(
    condition: Option<&Expr>,
    item: &std::collections::BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<(), StorageError> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        if !passed {
            return Err(StorageError::ConditionFailed(None));
        }
    }
    Ok(())
}

/// Resolve an expression (placeholder) to an `AttributeValue`.
pub(crate) fn resolve_expr_to_av(
    expr: &expression::Expr,
    maps: &ExpressionMaps,
) -> Result<AttributeValue, StorageError> {
    match expr {
        expression::Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Internal(
            "expected placeholder in key condition".to_owned(),
        )),
    }
}

/// SQL fragment for a sort key condition.
pub(crate) struct SkSqlInfo {
    pub(crate) fragment: String,
}

/// Build a SQL WHERE fragment for a sort key condition.
///
/// DynamoDB sorts strings by UTF-8 byte order, not by locale. We use
/// `COLLATE "C"` on string columns to match this behavior regardless of
/// the PostgreSQL database's `lc_collate` setting.
pub(crate) fn build_sk_sql(
    sk_cond: &SortKeyCondition,
    sk_col: &str,
    param_idx: &mut u32,
) -> SkSqlInfo {
    // Apply COLLATE "C" for string sort key columns to get byte-order comparison.
    let collate = if sk_col == "sk_s" || sk_col.ends_with("_s") {
        " COLLATE \"C\""
    } else {
        ""
    };
    match sk_cond {
        SortKeyCondition::Compare { op, .. } => {
            let sql_op = match op {
                expression::CompareOp::Eq => "=",
                expression::CompareOp::Ne => "<>",
                expression::CompareOp::Lt => "<",
                expression::CompareOp::Le => "<=",
                expression::CompareOp::Gt => ">",
                expression::CompareOp::Ge => ">=",
            };
            let frag = format!(" AND {sk_col}{collate} {sql_op} ${param_idx}");
            *param_idx += 1;
            SkSqlInfo { fragment: frag }
        }
        SortKeyCondition::Between { .. } => {
            let frag = format!(
                " AND {sk_col}{collate} BETWEEN ${lo} AND ${hi}",
                lo = *param_idx,
                hi = *param_idx + 1
            );
            *param_idx += 2;
            SkSqlInfo { fragment: frag }
        }
        SortKeyCondition::BeginsWith { .. } => {
            let is_binary = sk_col == "sk_b" || sk_col.ends_with("_b");
            if is_binary {
                // For binary sort keys, bind both prefix and incremented-prefix
                // as separate parameters. The upper bound is computed in Rust
                // (see bind_sk_condition) by incrementing the prefix bytes.
                let frag = format!(
                    " AND {sk_col} >= ${lo} AND {sk_col} < ${hi}",
                    lo = *param_idx,
                    hi = *param_idx + 1
                );
                *param_idx += 2;
                SkSqlInfo { fragment: frag }
            } else {
                // For string sort keys, append the max Unicode code point.
                // With COLLATE "C" (byte order), prefix || chr(1114111) is
                // strictly greater than any string starting with prefix.
                let frag = format!(
                    " AND {sk_col}{collate} >= ${p} AND {sk_col}{collate} < (${p} || chr(1114111))",
                    p = *param_idx
                );
                *param_idx += 1;
                SkSqlInfo { fragment: frag }
            }
        }
    }
}

/// Execute a query SQL statement with dynamic parameter binding.
///
/// # Parameter binding order:
/// 1. pk_text (partition key)
/// 2. SK condition params (from `build_sk_sql`)
/// 3. Extra SK equality params (multi-range schemas)
/// 4. ExclusiveStartKey index SK value (if paginating with sort key)
/// 5. Pagination extra binds (from `PaginationBinds` enum — built in `query_scan.rs`
///    in the same branch that generates the SQL, so order cannot diverge)
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_query_sql(
    sql: &str,
    pk_text: &str,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    sk_info: Option<(&str, ScalarAttributeType)>,
    extra_sk_col_indices: &[(usize, ScalarAttributeType)],
    exclusive_start_key: Option<&Item>,
    pagination_binds: &PaginationBinds,
    pool: &sqlx::PgPool,
) -> Result<Vec<serde_json::Value>, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);
    query = query.bind(pk_text.to_owned());

    // Bind sort key condition values
    if let (Some(sk_cond), Some((_, sk_type))) = (&key_condition.sk_condition, sk_info) {
        query = bind_sk_condition(query, sk_cond, sk_type, maps)?;
    }

    // Bind extra RANGE key equality values
    for (i, &(_pos, sk_type)) in extra_sk_col_indices.iter().enumerate() {
        if let Some((_, value)) = key_condition.extra_sk_conditions.get(i) {
            let av = resolve_expr_to_av(value, maps)?;
            let sk = parse_sk(&av, sk_type)?;
            query = bind_sk_value(query, &sk);
        }
    }

    // Bind exclusive start key SK value (index SK for the > / < comparison)
    if let (Some(start_key), Some((sk_name, sk_type))) = (exclusive_start_key, sk_info)
        && let Some(sk_val) = start_key.get(sk_name)
    {
        let sk = parse_sk(sk_val, sk_type)?;
        query = bind_sk_value(query, &sk);
    }

    // Bind pagination extra values — order is determined by the enum variant,
    // which is constructed in the same code branch that generated the SQL placeholders.
    match pagination_binds {
        PaginationBinds::None => {}
        PaginationBinds::BasePkOnly { pk_text } => {
            query = query.bind(pk_text.clone());
        }
        PaginationBinds::BaseSkOnly { sk } => {
            query = bind_sk_value(query, sk);
        }
        PaginationBinds::BasePkAndSk { pk_text, sk } => {
            query = query.bind(pk_text.clone());
            query = bind_sk_value(query, sk);
        }
    }

    let rows: Vec<(serde_json::Value,)> = query
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Bind sort key condition values to a query.
fn bind_sk_condition<'q>(
    query: sqlx::query::QueryAs<
        'q,
        sqlx::Postgres,
        (serde_json::Value,),
        sqlx::postgres::PgArguments,
    >,
    sk_cond: &SortKeyCondition,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<
    sqlx::query::QueryAs<'q, sqlx::Postgres, (serde_json::Value,), sqlx::postgres::PgArguments>,
    StorageError,
> {
    match sk_cond {
        SortKeyCondition::Compare { value, .. } => {
            let av = resolve_expr_to_av(value, maps)?;
            let sk = parse_sk(&av, sk_type)?;
            Ok(bind_sk_value(query, &sk))
        }
        SortKeyCondition::BeginsWith { prefix: value, .. } => {
            let av = resolve_expr_to_av(value, maps)?;
            let sk = parse_sk(&av, sk_type)?;
            if sk_type == ScalarAttributeType::B {
                // Binary begins_with: bind prefix and incremented prefix.
                let prefix_bytes = match &sk {
                    SortKeyValue::B(b) => b.clone(),
                    _ => unreachable!(),
                };
                let upper = increment_bytes(&prefix_bytes);
                let q = bind_sk_value(query, &sk);
                Ok(q.bind(upper))
            } else {
                Ok(bind_sk_value(query, &sk))
            }
        }
        SortKeyCondition::Between { low, high, .. } => {
            let lo_av = resolve_expr_to_av(low, maps)?;
            let hi_av = resolve_expr_to_av(high, maps)?;
            let lo_sk = parse_sk(&lo_av, sk_type)?;
            let hi_sk = parse_sk(&hi_av, sk_type)?;
            let q = bind_sk_value(query, &lo_sk);
            Ok(bind_sk_value(q, &hi_sk))
        }
    }
}

/// Compute the exclusive upper bound for a binary prefix range query.
///
/// Increments the last non-0xFF byte and truncates trailing 0xFF bytes.
/// If the prefix is all 0xFF, returns a 1025-byte all-0xFF vector (longer
/// than any valid DynamoDB sort key, so `< upper` is always true).
fn increment_bytes(prefix: &[u8]) -> Vec<u8> {
    let mut result = prefix.to_vec();
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            return result;
        }
        result.pop();
    }
    vec![0xFF; 1025]
}

/// Bind a single `SortKeyValue` to a query.
pub(crate) fn bind_sk_value<'q>(
    query: sqlx::query::QueryAs<
        'q,
        sqlx::Postgres,
        (serde_json::Value,),
        sqlx::postgres::PgArguments,
    >,
    sk: &SortKeyValue,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, (serde_json::Value,), sqlx::postgres::PgArguments> {
    match sk {
        SortKeyValue::S(s) => query.bind(s.clone()),
        SortKeyValue::N(n) => query.bind(n.clone()),
        SortKeyValue::B(b) => query.bind(b.clone()),
    }
}

/// Execute a scan SQL statement with dynamic parameter binding.
///
/// Bind order for index scans: pk, sk (if present), base_pk, base_sk (if present).
/// Bind order for base table scans: pk, sk (if present).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_scan_sql(
    sql: &str,
    exclusive_start_key: Option<&Item>,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_sk_info: Option<&(String, ScalarAttributeType)>,
    base_pk_attr_name: Option<&str>,
    pool: &sqlx::PgPool,
) -> Result<Vec<serde_json::Value>, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);

    if let Some(start_key) = exclusive_start_key {
        // Bind index/table PK
        let pk_name = &key_schema[0].attribute_name;
        let pk_val = start_key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing pk in start key".to_owned()))?;
        let pk_text = pk_to_text(pk_val)?;
        query = query.bind(pk_text.into_owned());

        // Bind index/table SK (if present)
        if let Some((sk_name, sk_type)) = sk_info(key_schema, attr_defs)
            && let Some(sk_val) = start_key.get(sk_name)
        {
            let sk = parse_sk(sk_val, sk_type)?;
            query = bind_sk_value(query, &sk);
        }

        // Bind base table PK for index scans
        if let Some(base_pk_name) = base_pk_attr_name
            && let Some(base_pk_val) = start_key.get(base_pk_name)
        {
            let base_pk_text = pk_to_text(base_pk_val)?;
            query = query.bind(base_pk_text.into_owned());
        }

        // Bind base table SK for index scans (if base table has a sort key)
        if let Some((base_sk_name, base_sk_type)) = base_sk_info
            && let Some(base_sk_val) = start_key.get(base_sk_name.as_str())
        {
            let sk = parse_sk(base_sk_val, *base_sk_type)?;
            query = bind_sk_value(query, &sk);
        }
    }

    let rows: Vec<(serde_json::Value,)> = query
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Build a `LastEvaluatedKey` from an item by extracting key attributes.
pub(crate) fn build_key(item: &Item, key_schema: &[KeySchemaElement]) -> Item {
    extract_key(item, key_schema)
}
