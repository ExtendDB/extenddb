// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and scan SQL helpers for the SQLite backend.

use extenddb_core::expression::{self, Expr, ExpressionMaps, SortKeyCondition};
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, extract_key};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use extenddb_storage::util::parse_sk;

/// Evaluate a condition expression against an item.
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
/// SQLite uses byte-order comparison for TEXT by default, which matches
/// DynamoDB's UTF-8 byte order for strings.
pub(crate) fn build_sk_sql(sk_cond: &SortKeyCondition, sk_col: &str) -> SkSqlInfo {
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
            SkSqlInfo {
                fragment: format!(" AND {sk_col} {sql_op} ?"),
            }
        }
        SortKeyCondition::Between { .. } => SkSqlInfo {
            fragment: format!(" AND {sk_col} BETWEEN ? AND ?"),
        },
        SortKeyCondition::BeginsWith { .. } => {
            let is_binary = sk_col == "sk_b" || sk_col.ends_with("_b");
            if is_binary {
                SkSqlInfo {
                    fragment: format!(" AND {sk_col} >= ? AND {sk_col} < ?"),
                }
            } else {
                // For string columns: prefix range using unicode char 1114111 as upper bound.
                SkSqlInfo {
                    fragment: format!(
                        " AND {sk_col} >= ? AND {sk_col} < (? || char(1114111))"
                    ),
                }
            }
        }
    }
}

/// Compute the exclusive upper bound for a binary prefix range query.
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

/// Bind sort key condition values to a query, returning the sk values to bind.
pub(crate) fn sk_condition_bind_values(
    sk_cond: &SortKeyCondition,
    sk_type: extenddb_core::types::ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<Vec<SortKeyValue>, StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { value, .. } => {
            let av = resolve_expr_to_av(value, maps)?;
            Ok(vec![parse_sk(&av, sk_type)?])
        }
        SortKeyCondition::BeginsWith { prefix: value, .. } => {
            let av = resolve_expr_to_av(value, maps)?;
            let sk = parse_sk(&av, sk_type)?;
            if sk_type == extenddb_core::types::ScalarAttributeType::B {
                let prefix_bytes = match &sk {
                    SortKeyValue::B(b) => b.clone(),
                    _ => unreachable!(),
                };
                let upper = increment_bytes(&prefix_bytes);
                Ok(vec![sk, SortKeyValue::B(upper)])
            } else {
                // For string: bind the prefix twice (>= prefix, < prefix || maxchar)
                let prefix_str = match &sk {
                    SortKeyValue::S(s) => s.clone(),
                    _ => unreachable!(),
                };
                Ok(vec![sk, SortKeyValue::S(prefix_str)])
            }
        }
        SortKeyCondition::Between { low, high, .. } => {
            let lo_av = resolve_expr_to_av(low, maps)?;
            let hi_av = resolve_expr_to_av(high, maps)?;
            Ok(vec![parse_sk(&lo_av, sk_type)?, parse_sk(&hi_av, sk_type)?])
        }
    }
}

/// Build a `LastEvaluatedKey` from an item by extracting key attributes.
pub(crate) fn build_key(item: &Item, key_schema: &[KeySchemaElement]) -> Item {
    extract_key(item, key_schema)
}

/// A bound value for dynamic query building.
pub(crate) enum BoundValue {
    Text(String),
    Real(f64),
    Blob(Vec<u8>),
}

/// Execute a dynamic query with collected bind values.
pub(crate) async fn execute_dynamic_query(
    sql: &str,
    values: Vec<BoundValue>,
    pool: &sqlx::SqlitePool,
) -> Result<Vec<serde_json::Value>, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);
    for v in values {
        query = match v {
            BoundValue::Text(s) => query.bind(s),
            BoundValue::Real(f) => query.bind(f),
            BoundValue::Blob(b) => query.bind(b),
        };
    }
    let rows: Vec<(serde_json::Value,)> = query
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Convert a `SortKeyValue` to a `BoundValue`.
pub(crate) fn sk_to_bound(sk: &SortKeyValue) -> BoundValue {
    match sk {
        SortKeyValue::S(s) => BoundValue::Text(s.clone()),
        SortKeyValue::N(n) => BoundValue::Real(super::bigdecimal_to_f64(n)),
        SortKeyValue::B(b) => BoundValue::Blob(b.clone()),
    }
}
