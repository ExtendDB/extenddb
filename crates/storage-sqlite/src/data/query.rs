// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared query helpers: condition evaluation, key-condition SQL fragments,
//! and dynamic query execution.
//!
//! Sort-key comparisons bind through [`super::sk_bound`], so `N` keys are
//! compared as their order-preserving TEXT encoding (D2): `>`, `<`, `BETWEEN`
//! all remain numerically correct. `begins_with` applies only to `S` (string
//! prefix via the maximal code point `char(1114111)`) and `B` (byte-range via
//! an incremented upper bound); DynamoDB rejects `begins_with` on `N`.

use extenddb_core::expression::{self, CompareOp, Expr, ExpressionMaps, SortKeyCondition};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, ScalarAttributeType, extract_key,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk};

use super::BoundValue;

/// Evaluate an optional condition expression against an item.
/// Returns `ConditionFailed(None)` when the condition evaluates to false.
pub(super) fn check_condition(
    condition: Option<&Expr>,
    item: &Item,
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

/// Resolve a key-condition placeholder expression to its `AttributeValue`.
pub(super) fn resolve_expr_to_av(
    expr: &Expr,
    maps: &ExpressionMaps,
) -> Result<AttributeValue, StorageError> {
    match expr {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Internal(
            "expected placeholder in key condition".to_owned(),
        )),
    }
}

/// SQL `WHERE` fragment for a sort-key condition on column `sk_col`.
pub(super) fn build_sk_sql(sk_cond: &SortKeyCondition, sk_col: &str) -> String {
    match sk_cond {
        SortKeyCondition::Compare { op, .. } => {
            let sql_op = match op {
                CompareOp::Eq => "=",
                CompareOp::Ne => "<>",
                CompareOp::Lt => "<",
                CompareOp::Le => "<=",
                CompareOp::Gt => ">",
                CompareOp::Ge => ">=",
            };
            format!(" AND {sk_col} {sql_op} ?")
        }
        SortKeyCondition::Between { .. } => format!(" AND {sk_col} BETWEEN ? AND ?"),
        SortKeyCondition::BeginsWith { .. } => {
            if sk_col.ends_with("_b") {
                // Binary prefix: [prefix, incremented-prefix).
                format!(" AND {sk_col} >= ? AND {sk_col} < ?")
            } else {
                // String prefix: [prefix, prefix || U+10FFFF).
                format!(" AND {sk_col} >= ? AND {sk_col} < (? || char(1114111))")
            }
        }
    }
}

/// The sort-key values to bind for a key condition, in placeholder order.
pub(super) fn sk_condition_bind_values(
    sk_cond: &SortKeyCondition,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<Vec<SortKeyValue>, StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { value, .. } => {
            Ok(vec![parse_sk(&resolve_expr_to_av(value, maps)?, sk_type)?])
        }
        SortKeyCondition::Between { low, high, .. } => Ok(vec![
            parse_sk(&resolve_expr_to_av(low, maps)?, sk_type)?,
            parse_sk(&resolve_expr_to_av(high, maps)?, sk_type)?,
        ]),
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let sk = parse_sk(&resolve_expr_to_av(prefix, maps)?, sk_type)?;
            match sk {
                SortKeyValue::B(b) => {
                    let upper = increment_bytes(&b);
                    Ok(vec![SortKeyValue::B(b), SortKeyValue::B(upper)])
                }
                // For strings the SQL upper bound is `? || char(1114111)`,
                // so the same prefix is bound twice.
                SortKeyValue::S(s) => Ok(vec![SortKeyValue::S(s.clone()), SortKeyValue::S(s)]),
                SortKeyValue::N(_) => Err(StorageError::Validation(
                    "begins_with is not supported on numeric sort keys".to_owned(),
                )),
            }
        }
    }
}

/// Exclusive upper bound for a binary prefix range: the smallest byte string
/// greater than every string having `prefix` as a prefix. Returns an empty
/// vector's successor convention when `prefix` is all `0xFF`.
fn increment_bytes(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return out;
        }
        out.pop();
    }
    // All 0xFF: no finite successor within the same length convention; use a
    // value that sorts after any prefixed string of practical length.
    vec![0xFF; prefix.len() + 1]
}

/// Build a `LastEvaluatedKey` from an item's key attributes.
pub(super) fn build_key(item: &Item, key_schema: &[KeySchemaElement]) -> Item {
    extract_key(item, key_schema)
}

/// Execute a dynamically-built query, binding `BoundValue`s positionally.
pub(super) async fn execute_dynamic_query(
    sql: &str,
    values: Vec<BoundValue>,
    pool: &sqlx::SqlitePool,
) -> Result<Vec<serde_json::Value>, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);
    for v in values {
        query = match v {
            BoundValue::Text(s) => query.bind(s),
            BoundValue::Blob(b) => query.bind(b),
        };
    }
    let rows: Vec<(serde_json::Value,)> = query
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(rows.into_iter().map(|(v,)| v).collect())
}
