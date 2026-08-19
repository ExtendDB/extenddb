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

/// SQL `WHERE` fragment for a sort-key condition on `sk_col`, together with the
/// values to bind for it.
///
/// The fragment and its bind list are returned from one function on purpose.
/// `begins_with` on a binary key sometimes has no upper bound at all (see
/// [`binary_prefix_upper_bound`]), so the placeholder count is not fixed by the
/// condition kind alone. Building the SQL in one place and the binds in another
/// meant the two could disagree about how many placeholders exist, which is a
/// bind-offset corruption rather than a visible error.
pub(super) fn build_sk_sql_and_binds(
    sk_cond: &SortKeyCondition,
    sk_col: &str,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<(String, Vec<SortKeyValue>), StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { op, value, .. } => {
            let sql_op = match op {
                CompareOp::Eq => "=",
                CompareOp::Ne => "<>",
                CompareOp::Lt => "<",
                CompareOp::Le => "<=",
                CompareOp::Gt => ">",
                CompareOp::Ge => ">=",
            };
            Ok((
                format!(" AND {sk_col} {sql_op} ?"),
                vec![parse_sk(&resolve_expr_to_av(value, maps)?, sk_type)?],
            ))
        }
        SortKeyCondition::Between { low, high, .. } => Ok((
            format!(" AND {sk_col} BETWEEN ? AND ?"),
            vec![
                parse_sk(&resolve_expr_to_av(low, maps)?, sk_type)?,
                parse_sk(&resolve_expr_to_av(high, maps)?, sk_type)?,
            ],
        )),
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let sk = parse_sk(&resolve_expr_to_av(prefix, maps)?, sk_type)?;
            match sk {
                SortKeyValue::B(b) => match binary_prefix_upper_bound(&b) {
                    Some(upper) => Ok((
                        format!(" AND {sk_col} >= ? AND {sk_col} < ?"),
                        vec![SortKeyValue::B(b), SortKeyValue::B(upper)],
                    )),
                    // Every byte string with this prefix is in range and there is
                    // no finite value above them all, so the only correct upper
                    // bound is none. Emitting one anyway is what silently dropped
                    // rows.
                    None => Ok((format!(" AND {sk_col} >= ?"), vec![SortKeyValue::B(b)])),
                },
                // For strings the SQL upper bound is `? || char(1114111)`,
                // so the same prefix is bound twice.
                SortKeyValue::S(s) => Ok((
                    format!(" AND {sk_col} >= ? AND {sk_col} < (? || char(1114111))"),
                    vec![SortKeyValue::S(s.clone()), SortKeyValue::S(s)],
                )),
                SortKeyValue::N(_) => Err(StorageError::Validation(
                    "begins_with is not supported on numeric sort keys".to_owned(),
                )),
            }
        }
    }
}

/// Exclusive upper bound for a binary prefix range, or `None` when the range is
/// unbounded above.
///
/// The bound is the smallest byte string greater than every string having
/// `prefix` as a prefix. Found by incrementing the last byte below `0xFF` and
/// discarding everything after it: `[1, 2]` yields `[1, 3]`, so `[1, 2, 9]` is
/// still included but `[1, 3]` is not.
///
/// `None` when every byte is `0xFF`, which includes the empty prefix. No finite
/// bound exists in that case, because a longer all-`0xFF` string always sorts
/// after any candidate: with `[0xFF]` the strings `[0xFF, 0xFF]`,
/// `[0xFF, 0xFF, 0xFF]` and so on continue without end. The previous code
/// returned `vec![0xFF; prefix.len() + 1]` here, which is a value inside the
/// matching set rather than above it, so rows were silently dropped: for an
/// empty prefix it produced `[0xFF]`, which excluded every key sorting at or
/// after it, and `begins_with([])` then returned part of the partition while
/// reporting success. Returning `None` and omitting the predicate is the only
/// correct answer that does not depend on a maximum key length, which is
/// operator-configurable via `max_sort_key_size_bytes`.
fn binary_prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
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

#[cfg(test)]
mod tests {
    use super::binary_prefix_upper_bound;

    /// Every byte string having `prefix` as a prefix must sort below the bound,
    /// and the bound itself must not. Asserted against explicit witnesses rather
    /// than trusting the arithmetic, since the defect this replaces was an
    /// off-by-domain error that looked arithmetically reasonable.
    #[test]
    fn the_bound_excludes_itself_and_includes_every_extension() {
        let upper = binary_prefix_upper_bound(&[1, 2]).expect("finite bound exists");
        assert_eq!(upper, vec![1, 3]);
        for witness in [
            vec![1, 2],
            vec![1, 2, 0],
            vec![1, 2, 0xFF],
            vec![1, 2, 9, 9],
        ] {
            assert!(witness < upper, "{witness:?} must be inside the range");
        }
        assert!(vec![1, 3] >= upper, "the bound must be exclusive");
    }

    /// A trailing `0xFF` carries: the last byte below `0xFF` is incremented and
    /// everything after it is discarded.
    #[test]
    fn a_trailing_all_ones_byte_carries_into_the_previous_byte() {
        assert_eq!(binary_prefix_upper_bound(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(binary_prefix_upper_bound(&[1, 0xFF, 0xFF]), Some(vec![2]));
        let upper = binary_prefix_upper_bound(&[1, 0xFF]).expect("finite bound exists");
        assert!(vec![1, 0xFF, 0xFF] < upper, "extension must be included");
    }

    /// The empty prefix matches everything, so no upper bound can exist. This is
    /// the case that silently dropped rows: the old code returned `[0xFF]`, which
    /// excluded every key sorting at or after it, so `begins_with([])` returned
    /// part of the partition and reported success.
    #[test]
    fn an_empty_prefix_has_no_upper_bound() {
        assert_eq!(binary_prefix_upper_bound(&[]), None);
    }

    /// Same defect one length up, and not only for the empty prefix: an all-`0xFF`
    /// prefix of any length has no finite bound, because a longer all-`0xFF`
    /// string always sorts after any candidate. The old code returned
    /// `[0xFF, 0xFF]` for `[0xFF]`, which wrongly excluded `[0xFF, 0xFF, 0xFF]`.
    #[test]
    fn an_all_ones_prefix_has_no_upper_bound_at_any_length() {
        for prefix in [vec![0xFF], vec![0xFF, 0xFF], vec![0xFF; 8]] {
            assert_eq!(
                binary_prefix_upper_bound(&prefix),
                None,
                "no finite bound exists above {prefix:?}"
            );
        }
    }
}
