// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and scan SQL helpers for the `TiDB` backend.
//!
//! Contains condition evaluation, sort-key SQL generation, and dynamic
//! parameter binding for `Query` and `Scan` operations.

use extenddb_core::expression::{self, Expr, ExpressionMaps, KeyCondition, SortKeyCondition};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, ScalarAttributeType, extract_key,
    item_size_bytes,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use extenddb_storage::util::parse_sk;
use futures::TryStreamExt;

use super::{all_sort_key_info, json_to_item, physical_pk_bytes};

pub(crate) type JsonRowsQuery<'q> =
    sqlx::query::QueryAs<'q, sqlx::MySql, (serde_json::Value,), sqlx::mysql::MySqlArguments>;

pub(crate) struct ReadPage {
    pub(crate) items: Vec<Item>,
    pub(crate) has_more: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct ReadPageLimits {
    pub(crate) item_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct KeyTupleBinding<'a> {
    pub(crate) key_schema: &'a [KeySchemaElement],
    pub(crate) attr_defs: &'a [AttributeDefinition],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ScanSegmentRange {
    pub(super) lower: Option<Vec<u8>>,
    pub(super) upper: Option<Vec<u8>>,
}

impl ScanSegmentRange {
    pub(super) fn predicates(&self, column: &str) -> Vec<String> {
        let mut predicates = Vec::with_capacity(2);
        if self.lower.is_some() {
            predicates.push(format!("{column} >= ?"));
        }
        if self.upper.is_some() {
            predicates.push(format!("{column} < ?"));
        }
        predicates
    }
}

pub(super) fn scan_segment_range(
    segment: i64,
    total_segments: i64,
) -> Result<ScanSegmentRange, StorageError> {
    if total_segments < 1 || segment < 0 || segment >= total_segments {
        return Err(StorageError::Validation(
            "invalid parallel scan segment".to_owned(),
        ));
    }

    let segment = u128::try_from(segment)
        .map_err(|_| StorageError::Validation("invalid parallel scan segment".to_owned()))?;
    let total = u128::try_from(total_segments)
        .map_err(|_| StorageError::Validation("invalid parallel scan segment".to_owned()))?;
    let keyspace = 1_u128 << 64;

    let lower = if segment == 0 {
        None
    } else {
        let value = segment * keyspace / total;
        Some(scan_segment_bound_bytes(value)?)
    };
    let upper = if segment + 1 == total {
        None
    } else {
        let value = (segment + 1) * keyspace / total;
        Some(scan_segment_bound_bytes(value)?)
    };

    Ok(ScanSegmentRange { lower, upper })
}

fn scan_segment_bound_bytes(value: u128) -> Result<Vec<u8>, StorageError> {
    let value = u64::try_from(value).map_err(|_| {
        StorageError::Internal(format!("parallel scan segment bound out of range: {value}"))
    })?;
    Ok(value.to_be_bytes().to_vec())
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

fn next_prefix_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] != u8::MAX {
            upper[i] += 1;
            upper.truncate(i + 1);
            return Some(upper);
        }
    }
    None
}

fn begins_with_prefix_bounds(
    value: &expression::Expr,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<(Vec<u8>, Option<Vec<u8>>), StorageError> {
    let av = resolve_expr_to_av(value, maps)?;
    let lower = match parse_sk(&av, sk_type)? {
        SortKeyValue::S(s) => s.into_bytes(),
        SortKeyValue::B(b) => b,
        SortKeyValue::N(_) => {
            return Err(StorageError::Validation(
                "begins_with is not supported for numeric sort keys".to_owned(),
            ));
        }
    };
    let upper = next_prefix_bytes(&lower);
    Ok((lower, upper))
}

/// Build a SQL WHERE fragment for a sort key condition.
///
/// DynamoDB sorts strings by UTF-8 byte order, not by locale. TiDB stores
/// string sort keys in `VARBINARY(1024)` columns to preserve byte ordering.
pub(crate) fn build_sk_sql(
    sk_cond: &SortKeyCondition,
    sk_col: &str,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
    param_idx: &mut u32,
) -> Result<SkSqlInfo, StorageError> {
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
            let frag = format!(" AND {sk_col} {sql_op} ?");
            *param_idx += 1;
            Ok(SkSqlInfo { fragment: frag })
        }
        SortKeyCondition::Between { .. } => {
            let frag = format!(" AND {sk_col} BETWEEN ? AND ?");
            *param_idx += 2;
            Ok(SkSqlInfo { fragment: frag })
        }
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let (_lower, upper) = begins_with_prefix_bounds(prefix, sk_type, maps)?;
            let frag = if upper.is_some() {
                *param_idx += 2;
                format!(" AND {sk_col} >= ? AND {sk_col} < ?")
            } else {
                *param_idx += 1;
                format!(" AND {sk_col} >= ?")
            };
            Ok(SkSqlInfo { fragment: frag })
        }
    }
}

/// Execute a query SQL statement with dynamic parameter binding.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_query_sql(
    sql: &str,
    pk: &[u8],
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    sk_info: Option<(&str, ScalarAttributeType)>,
    extra_sk_col_indices: &[(usize, ScalarAttributeType)],
    exclusive_start_key: Option<&Item>,
    base_table_key: Option<KeyTupleBinding<'_>>,
    page_limits: ReadPageLimits,
    pool: &sqlx::MySqlPool,
) -> Result<ReadPage, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);
    query = query.bind(pk.to_vec());

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

    if let Some(start_key) = exclusive_start_key {
        query = bind_sort_key_tuple(query, start_key, key_schema, attr_defs)?;
        if let Some(base) = base_table_key {
            query = bind_key_tuple(query, start_key, base.key_schema, base.attr_defs)?;
        }
    }

    collect_read_page(query, pool, page_limits).await
}

/// Bind sort key condition values to a query.
fn bind_sk_condition<'q>(
    query: JsonRowsQuery<'q>,
    sk_cond: &SortKeyCondition,
    sk_type: ScalarAttributeType,
    maps: &ExpressionMaps,
) -> Result<JsonRowsQuery<'q>, StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { value, .. } => {
            let av = resolve_expr_to_av(value, maps)?;
            let sk = parse_sk(&av, sk_type)?;
            Ok(bind_sk_value(query, &sk))
        }
        SortKeyCondition::BeginsWith { prefix: value, .. } => {
            let (lower, upper) = begins_with_prefix_bounds(value, sk_type, maps)?;
            let q = query.bind(lower);
            Ok(match upper {
                Some(upper) => q.bind(upper),
                None => q,
            })
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

/// Bind a single `SortKeyValue` to a query.
pub(crate) fn bind_sk_value<'q>(query: JsonRowsQuery<'q>, sk: &SortKeyValue) -> JsonRowsQuery<'q> {
    match sk {
        SortKeyValue::S(s) => query.bind(s.as_bytes().to_vec()),
        SortKeyValue::N(n) => query.bind(n.clone()),
        SortKeyValue::B(b) => query.bind(b.clone()),
    }
}

pub(crate) fn bind_key_tuple<'q>(
    query: JsonRowsQuery<'q>,
    key: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<JsonRowsQuery<'q>, StorageError> {
    let mut query = query.bind(physical_pk_bytes(key, key_schema)?);
    query = bind_sort_key_tuple(query, key, key_schema, attr_defs)?;
    Ok(query)
}

fn bind_sort_key_tuple<'q>(
    mut query: JsonRowsQuery<'q>,
    key: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<JsonRowsQuery<'q>, StorageError> {
    for (sk_name, sk_type) in all_sort_key_info(key_schema, attr_defs) {
        let sk_val = key.get(sk_name).ok_or_else(|| {
            StorageError::Internal(format!("missing sort key in start key: {sk_name}"))
        })?;
        let sk = parse_sk(sk_val, sk_type)?;
        query = bind_sk_value(query, &sk);
    }
    Ok(query)
}

/// Execute a scan SQL statement with dynamic parameter binding.
pub(crate) async fn execute_scan_sql(
    sql: &str,
    segment_range: Option<&ScanSegmentRange>,
    exclusive_start_key: Option<&Item>,
    key_binding: KeyTupleBinding<'_>,
    base_table_key: Option<KeyTupleBinding<'_>>,
    page_limits: ReadPageLimits,
    pool: &sqlx::MySqlPool,
) -> Result<ReadPage, StorageError> {
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql);

    if let Some(range) = segment_range {
        if let Some(lower) = &range.lower {
            query = query.bind(lower);
        }
        if let Some(upper) = &range.upper {
            query = query.bind(upper);
        }
    }

    if let Some(start_key) = exclusive_start_key {
        query = bind_key_tuple(
            query,
            start_key,
            key_binding.key_schema,
            key_binding.attr_defs,
        )?;
        if let Some(base) = base_table_key {
            query = bind_key_tuple(query, start_key, base.key_schema, base.attr_defs)?;
        }
    }

    collect_read_page(query, pool, page_limits).await
}

async fn collect_read_page(
    query: JsonRowsQuery<'_>,
    pool: &sqlx::MySqlPool,
    limits: ReadPageLimits,
) -> Result<ReadPage, StorageError> {
    let mut stream = query.fetch(pool);
    let mut items = Vec::new();
    let mut page_bytes = 0usize;

    while let Some((json,)) = stream
        .try_next()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
    {
        if items.len() >= limits.item_limit {
            return Ok(ReadPage {
                items,
                has_more: true,
            });
        }

        let item = json_to_item(json)?;
        let item_bytes = item_size_bytes(&item);
        if read_page_byte_limit_reached(
            !items.is_empty(),
            page_bytes,
            item_bytes,
            limits.byte_limit,
        ) {
            return Ok(ReadPage {
                items,
                has_more: true,
            });
        }

        page_bytes = page_bytes.saturating_add(item_bytes);
        items.push(item);
    }

    Ok(ReadPage {
        items,
        has_more: false,
    })
}

fn read_page_byte_limit_reached(
    has_items: bool,
    page_bytes: usize,
    next_item_bytes: usize,
    page_byte_limit: usize,
) -> bool {
    has_items && page_bytes.saturating_add(next_item_bytes) > page_byte_limit
}

/// Build a `LastEvaluatedKey` from an item by extracting key attributes.
pub(crate) fn build_key(item: &Item, key_schema: &[KeySchemaElement]) -> Item {
    extract_key(item, key_schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use extenddb_core::expression::{Expr, SortKeyCondition};
    use extenddb_core::types::{AttributeValue, ScalarAttributeType};

    use super::{
        ScanSegmentRange, build_sk_sql, next_prefix_bytes, read_page_byte_limit_reached,
        scan_segment_range,
    };

    #[test]
    fn prefix_upper_bound_uses_half_open_byte_range() {
        assert_eq!(next_prefix_bytes(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(next_prefix_bytes(&[0x61, 0xff]), Some(vec![0x62]));
        assert_eq!(next_prefix_bytes(&[0xff, 0xff]), None);
    }

    #[test]
    fn begins_with_builds_sargable_range() {
        let maps = extenddb_core::expression::ExpressionMaps::new(
            HashMap::new(),
            HashMap::from([(":p".to_owned(), AttributeValue::S("abc".to_owned()))]),
        );
        let mut param_idx = 2;
        let sql = build_sk_sql(
            &SortKeyCondition::BeginsWith {
                path: vec![],
                prefix: Expr::Placeholder(":p".to_owned()),
            },
            "sk_s",
            ScalarAttributeType::S,
            &maps,
            &mut param_idx,
        )
        .expect("begins_with should compile");

        assert_eq!(sql.fragment, " AND sk_s >= ? AND sk_s < ?");
        assert_eq!(param_idx, 4);
    }

    #[test]
    fn scan_segment_range_partitions_native_keyspace() {
        let first = scan_segment_range(0, 4).expect("first segment");
        let second = scan_segment_range(1, 4).expect("second segment");
        let last = scan_segment_range(3, 4).expect("last segment");

        assert_eq!(first.lower, None);
        assert_eq!(first.upper, Some(vec![0x40, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(second.lower, Some(vec![0x40, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(second.upper, Some(vec![0x80, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(last.lower, Some(vec![0xc0, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(last.upper, None);
    }

    #[test]
    fn scan_segment_predicates_are_native_range_seeks() {
        let range = ScanSegmentRange {
            lower: Some(vec![1]),
            upper: Some(vec![2]),
        };

        let predicates = range.predicates("edbidx_idx1_pk");

        assert_eq!(
            predicates,
            vec!["edbidx_idx1_pk >= ?", "edbidx_idx1_pk < ?"]
        );
        assert!(!predicates.join(" ").contains("CRC32"));
        assert!(!predicates.join(" ").contains('%'));
    }

    #[test]
    fn scan_segment_single_total_needs_no_range_predicates() {
        let range = scan_segment_range(0, 1).expect("single segment");

        assert_eq!(range.lower, None);
        assert_eq!(range.upper, None);
        assert!(range.predicates("pk").is_empty());
    }

    #[test]
    fn read_page_byte_cap_keeps_first_item_and_pages_before_overflow() {
        assert!(!read_page_byte_limit_reached(false, 0, 2_000_000, 1));
        assert!(!read_page_byte_limit_reached(true, 512, 512, 1024));
        assert!(read_page_byte_limit_reached(true, 900, 200, 1024));
    }

    #[test]
    fn scan_segment_range_rejects_invalid_segments() {
        assert!(scan_segment_range(0, 0).is_err());
        assert!(scan_segment_range(-1, 4).is_err());
        assert!(scan_segment_range(4, 4).is_err());
    }
}
