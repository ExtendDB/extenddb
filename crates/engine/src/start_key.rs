// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ExclusiveStartKey` validation for `Query` and `Scan`.

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
    evaluate_condition,
};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, IndexInfo, Item, KeySchemaElement, KeyType,
    ScalarAttributeType,
};

use crate::index_helpers::combined_lek_key_schema;

/// Validate `ExclusiveStartKey` against the (table or index) key schema for
/// `Scan`. Base-table scans use the long `DynamoDB` error message; index scans
/// use the short one.
///
/// # Errors
///
/// Returns `ValidationException` if the start key has missing keys, extras,
/// or scalar type mismatches.
pub fn validate_scan_exclusive_start_key(
    start_key: &Item,
    key_info: &extenddb_core::types::TableKeyInfo,
    index_info: Option<&IndexInfo>,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let required = combined_lek_key_schema(&key_info.key_schema, index_info);
    let message = scan_invalid_start_key_message(index_info);
    check_exclusive_start_key(
        start_key,
        &required,
        &key_info.attribute_definitions,
        message,
    )
}

/// Validate `ExclusiveStartKey` for `Query`. Same rules as Scan; uses the
/// short `DynamoDB` error message in all cases.
///
/// # Errors
///
/// Returns `ValidationException` if the start key has missing keys, extras,
/// or scalar type mismatches.
pub fn validate_query_exclusive_start_key(
    start_key: &Item,
    key_info: &extenddb_core::types::TableKeyInfo,
    index_info: Option<&IndexInfo>,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let required = combined_lek_key_schema(&key_info.key_schema, index_info);
    check_exclusive_start_key(
        start_key,
        &required,
        &key_info.attribute_definitions,
        QUERY_INVALID_START_KEY_MSG,
    )
}

const QUERY_INVALID_START_KEY_MSG: &str = "The provided starting key is invalid";
const QUERY_START_KEY_OUTSIDE_BOUNDARIES_MSG: &str =
    "The provided starting key is outside query boundaries based on provided conditions";
const QUERY_EXCLUSIVE_START_KEY_INVALID_MSG: &str = "The provided exclusive start key is invalid";
const QUERY_START_KEY_RANGE_PREDICATE_MSG: &str =
    "The provided starting key does not match the range key predicate";
const QUERY_AT_MOST_ONE_ROW_MSG: &str =
    "The query can return at most one row and cannot be restarted";

/// What a Query reads, for [`validate_query_start_key_bounds`].
#[derive(Debug, Clone, Copy)]
pub struct QueryReadShape<'a> {
    /// Key schema of what is read: the index key schema on an index Query,
    /// the table key schema otherwise.
    pub key_schema: &'a [KeySchemaElement],
    /// The Query names a GSI or LSI.
    pub on_index: bool,
    /// `ScanIndexForward`.
    pub forward: bool,
}

impl QueryReadShape<'_> {
    /// A base table Query on a table without a RANGE key: at most one row.
    fn is_hash_only_table(&self) -> bool {
        !self.on_index && !self.key_schema.iter().any(|k| k.key_type == KeyType::Range)
    }
}

/// Reject a Query `ExclusiveStartKey` that the `KeyConditionExpression` (or
/// legacy `KeyConditions`) can never reach.
///
/// Call after [`validate_query_exclusive_start_key`], which guarantees the
/// start key carries every key attribute with the declared scalar type. The
/// rule was measured on Amazon `DynamoDB` (issue #440, 2026-10-23 and
/// 2026-10-24; every row twice with identical results). The checks run in
/// this order:
///
/// 1. The sort key predicate, "a later key in the scan direction can still
///    satisfy the range condition", for either partition. A start key whose
///    sort value does not satisfy the condition gets "The provided starting
///    key does not match the range key predicate". Strict operators exclude
///    the bound (`sk > m` and `sk < m` with start key `m` are refused, `sk >=
///    m` is accepted).
/// 2. On a base table, a start key whose sort value satisfies the condition
///    but past which nothing later can match. Forward: `<=` and the
///    `BETWEEN` upper bound at the bound get the range sentence (any
///    partition). In reverse: `>=` and the `BETWEEN` lower bound at the
///    bound get the range sentence, while `<=` and the `BETWEEN` upper bound
///    are accepted. `sk = m` with the start key's sort value `m`, same
///    partition, either direction: "The provided exclusive start key is
///    invalid". `begins_with` in reverse with the start key's sort value
///    equal to the prefix, same partition: "The provided starting key is
///    outside query boundaries based on provided conditions" (forward at the
///    prefix is accepted). An index Query skips this step: index keys repeat,
///    so a repeat of the start key's sort value counts as a later key (GSI
///    and LSI measured for `<=`, reverse `>=`, `BETWEEN` at both ends,
///    reverse `begins_with` and `=` with the start key itself, all
///    accepted).
/// 3. A partition key value differs from the one the condition equates it to
///    (each HASH attribute of the queried schema; on a GSI only the index
///    partition, so a different base key in the same index partition
///    passes). With a sort key condition present, on a table, an LSI or a
///    GSI, in either direction and for every operator: "The query can return
///    at most one row and cannot be restarted". Without one: "The provided
///    starting key is outside query boundaries based on provided
///    conditions". A hash-only base table refuses every start key with "The
///    provided exclusive start key is invalid", the queried partition
///    included.
///
/// A bound or condition value whose type differs from the start key's, and
/// an undefined placeholder, are skipped: the existing handling of those
/// requests reports them.
///
/// # Errors
///
/// Returns `ValidationException` with one of the sentences above.
pub fn validate_query_start_key_bounds(
    start_key: &Item,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    shape: QueryReadShape<'_>,
) -> Result<(), DynamoDbError> {
    let evaluable_sk = key_condition
        .sk_condition
        .as_ref()
        .filter(|sk| sort_key_is_evaluable(sk, start_key, maps));
    if let Some(sk) = evaluable_sk
        && evaluate_condition(&sort_key_predicate(sk), start_key, maps).ok() == Some(false)
    {
        return Err(validation(QUERY_START_KEY_RANGE_PREDICATE_MSG));
    }

    let partition = partition_state(start_key, key_condition, maps);
    match far_end(start_key, key_condition, maps, shape) {
        Some(FarEnd::Bound) => return Err(validation(QUERY_START_KEY_RANGE_PREDICATE_MSG)),
        Some(FarEnd::Equality) if partition == Partition::Same => {
            return Err(validation(QUERY_EXCLUSIVE_START_KEY_INVALID_MSG));
        }
        Some(FarEnd::ReversePrefix) if partition == Partition::Same => {
            return Err(validation(QUERY_START_KEY_OUTSIDE_BOUNDARIES_MSG));
        }
        _ => {}
    }

    if shape.is_hash_only_table() && partition != Partition::Unknown {
        return Err(validation(QUERY_EXCLUSIVE_START_KEY_INVALID_MSG));
    }
    if partition == Partition::Differs {
        return Err(validation(if key_condition.sk_condition.is_some() {
            QUERY_AT_MOST_ONE_ROW_MSG
        } else {
            QUERY_START_KEY_OUTSIDE_BOUNDARIES_MSG
        }));
    }
    Ok(())
}

/// Whether a page that ends with `last_key` has nothing after it that can
/// match, so it carries no `LastEvaluatedKey`. Measured on a base table
/// (2026-10-24, ten runs each): `sk = m` with `Limit` 1, reverse `sk >= m`
/// with `Limit` 2 over `z, m`, and a walk with `Limit` 1 over `BETWEEN`,
/// `<=`, `=`, reverse `>=` and reverse `begins_with` all end without a key
/// and without a trailing empty page. [`validate_query_start_key_bounds`]
/// refuses such a key as a start key, so the handler drops it from the page.
/// A hash-only base table answers at most one row and refuses every start
/// key, so its page never carries a key either (not measured: the service's
/// page for that shape). Index pages are unchanged (a GSI over three
/// identical index keys with `Limit` 3 returns its key, ten of ten).
///
/// Not covered, and left as before: the service also ends a page without a
/// key when the next row exists but does not match (`sk < z` with `Limit` 7,
/// forward `begins_with(sk, a)` with `Limit` 3, ten of ten); ExtendDB keeps
/// the key there and the next request answers an empty page.
#[must_use]
pub fn page_ends_at_far_end(
    last_key: &Item,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    shape: QueryReadShape<'_>,
) -> bool {
    shape.is_hash_only_table() || far_end(last_key, key_condition, maps, shape).is_some()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Partition {
    Same,
    Differs,
    /// A condition value is undefined or of another type than the start key.
    Unknown,
}

/// Compare the start key's partition key values with the condition's.
fn partition_state(
    start_key: &Item,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
) -> Partition {
    let first = (&key_condition.pk_path, &key_condition.pk_value);
    let extra = key_condition
        .extra_pk_conditions
        .iter()
        .map(|(p, v)| (p, v));
    let mut unknown = false;
    for (path, value) in std::iter::once(first).chain(extra) {
        let wanted_and_supplied = condition_attribute_name(path, maps)
            .zip(match value {
                Expr::Placeholder(placeholder) => maps.resolve_value(placeholder).ok(),
                _ => None,
            })
            .and_then(|(name, wanted)| start_key.get(name).map(|supplied| (wanted, supplied)));
        match wanted_and_supplied {
            Some((wanted, supplied))
                if std::mem::discriminant(supplied) == std::mem::discriminant(wanted) =>
            {
                if supplied != wanted {
                    return Partition::Differs;
                }
            }
            _ => unknown = true,
        }
    }
    if unknown {
        Partition::Unknown
    } else {
        Partition::Same
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FarEnd {
    /// `sk = value` with the key equal to the value.
    Equality,
    /// `begins_with` in reverse with the key equal to the prefix.
    ReversePrefix,
    /// The inclusive bound at the end of iteration order.
    Bound,
}

/// Step 2 of [`validate_query_start_key_bounds`]: the base table only.
fn far_end(
    key: &Item,
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    shape: QueryReadShape<'_>,
) -> Option<FarEnd> {
    if shape.on_index {
        return None;
    }
    let sk = key_condition
        .sk_condition
        .as_ref()
        .filter(|sk| sort_key_is_evaluable(sk, key, maps))?;
    let (bound, kind) = far_end_bound(sk, shape.forward)?;
    let at_bound = Expr::Compare {
        left: Box::new(Expr::Path(sort_key_path(sk).to_vec())),
        op: CompareOp::Eq,
        right: Box::new(bound.clone()),
    };
    (evaluate_condition(&at_bound, key, maps).ok() == Some(true)).then_some(kind)
}

fn validation(message: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(message.to_owned())
}

/// The sort key condition as a condition expression over the sort key
/// attribute, so the evaluator's comparison rules (UTF-8 order for strings,
/// numeric order for numbers, unsigned bytes for binary) apply to the start
/// key.
fn sort_key_predicate(sk: &SortKeyCondition) -> Expr {
    match sk {
        SortKeyCondition::Compare { path, op, value } => Expr::Compare {
            left: Box::new(Expr::Path(path.clone())),
            op: *op,
            right: Box::new(value.clone()),
        },
        SortKeyCondition::Between { path, low, high } => Expr::Between {
            operand: Box::new(Expr::Path(path.clone())),
            low: Box::new(low.clone()),
            high: Box::new(high.clone()),
        },
        SortKeyCondition::BeginsWith { path, prefix } => Expr::Function {
            name: "begins_with".to_owned(),
            args: vec![Expr::Path(path.clone()), prefix.clone()],
        },
    }
}

/// The inclusive bound at the end of iteration order, past which no key can
/// match: the upper bound going forward, the lower bound going in reverse.
/// Exclusive bounds (`<`, `>`) need no entry: the start key equal to one
/// fails the predicate itself. `begins_with` has an end only in reverse, at
/// the prefix; forward at the prefix is accepted.
fn far_end_bound(sk: &SortKeyCondition, forward: bool) -> Option<(&Expr, FarEnd)> {
    match (sk, forward) {
        (
            SortKeyCondition::Compare {
                op: CompareOp::Eq,
                value,
                ..
            },
            _,
        ) => Some((value, FarEnd::Equality)),
        (
            SortKeyCondition::Compare {
                op: CompareOp::Le,
                value,
                ..
            },
            true,
        )
        | (
            SortKeyCondition::Compare {
                op: CompareOp::Ge,
                value,
                ..
            },
            false,
        ) => Some((value, FarEnd::Bound)),
        (SortKeyCondition::Between { high, .. }, true) => Some((high, FarEnd::Bound)),
        (SortKeyCondition::Between { low, .. }, false) => Some((low, FarEnd::Bound)),
        (SortKeyCondition::BeginsWith { prefix, .. }, false) => {
            Some((prefix, FarEnd::ReversePrefix))
        }
        _ => None,
    }
}

/// Whether the start key's sort key value and every bound of the condition
/// can be evaluated together: the sort key attribute resolves, the start key
/// carries it, and each bound is a defined placeholder of the same type.
fn sort_key_is_evaluable(sk: &SortKeyCondition, start_key: &Item, maps: &ExpressionMaps) -> bool {
    let Some(supplied) =
        condition_attribute_name(sort_key_path(sk), maps).and_then(|name| start_key.get(name))
    else {
        return false;
    };
    let bounds: Vec<&Expr> = match sk {
        SortKeyCondition::Compare { value, .. } => vec![value],
        SortKeyCondition::Between { low, high, .. } => vec![low, high],
        SortKeyCondition::BeginsWith { prefix, .. } => vec![prefix],
    };
    bounds.into_iter().all(|bound| match bound {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .is_ok_and(|v| std::mem::discriminant(v) == std::mem::discriminant(supplied)),
        _ => false,
    })
}

fn sort_key_path(sk: &SortKeyCondition) -> &[PathElement] {
    match sk {
        SortKeyCondition::Compare { path, .. }
        | SortKeyCondition::Between { path, .. }
        | SortKeyCondition::BeginsWith { path, .. } => path,
    }
}

/// Top-level attribute name a key condition path refers to, with a `#name`
/// reference resolved through `maps`.
fn condition_attribute_name<'a>(
    path: &'a [PathElement],
    maps: &'a ExpressionMaps,
) -> Option<&'a str> {
    match path.first() {
        Some(PathElement::Attribute(name)) => match name.strip_prefix('#') {
            Some(reference) => maps.names.get(reference).map(String::as_str),
            None => Some(name.as_str()),
        },
        _ => None,
    }
}

const SCAN_INVALID_START_KEY_MSG_BASE: &str = "The provided starting key is invalid: \
     The provided key element does not match the schema";
const SCAN_INVALID_START_KEY_MSG_INDEX: &str = "The provided starting key is invalid";

fn scan_invalid_start_key_message(index_info: Option<&IndexInfo>) -> &'static str {
    if index_info.is_some() {
        SCAN_INVALID_START_KEY_MSG_INDEX
    } else {
        SCAN_INVALID_START_KEY_MSG_BASE
    }
}

/// Three rules: required-keys-present, no-extras, scalar-type-match.
fn check_exclusive_start_key(
    start_key: &Item,
    required: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
    error_message: &str,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let invalid =
        || extenddb_core::error::DynamoDbError::ValidationException(error_message.to_owned());

    for ks in required {
        if !start_key.contains_key(&ks.attribute_name) {
            return Err(invalid());
        }
    }
    if start_key.len() != required.len() {
        return Err(invalid());
    }
    for ks in required {
        let declared = attribute_definitions
            .iter()
            .find(|ad| ad.attribute_name == ks.attribute_name)
            .ok_or_else(invalid)?;
        let supplied = start_key.get(&ks.attribute_name).ok_or_else(invalid)?;
        if !attr_value_matches_scalar(supplied, declared.attribute_type) {
            return Err(invalid());
        }
    }

    Ok(())
}

/// Whether an `AttributeValue` matches the declared scalar type (S / N / B).
fn attr_value_matches_scalar(value: &AttributeValue, scalar: ScalarAttributeType) -> bool {
    matches!(
        (value, scalar),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

#[cfg(test)]
#[path = "start_key_range_tests.rs"]
mod range_tests;
#[cfg(test)]
#[path = "start_key_tests.rs"]
mod tests;
