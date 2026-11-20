// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key value size limits.
//!
//! The write path checks the key values of an item against these limits (see
//! `validate_key_sizes`). `Query` applies the same limits to the values in its
//! key condition: a partition key value may not exceed
//! `max_partition_key_size_bytes` (2048), the partition key values of an index
//! with several HASH attributes may not exceed it together, and the values of
//! all range key attributes together may not exceed `max_sort_key_size_bytes`
//! (1024). All limits are inclusive. The three sentences are the service's
//! own; the single hash sentence has no space between `of` and the number.

use crate::error::DynamoDbError;
use crate::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, resolve_element_name,
};
use crate::limits::LimitsConfig;
use crate::types::{AttributeValue, KeySchemaElement, KeyType};
use crate::validation::number::validate_and_normalize_number;

/// Byte size of a key attribute value.
///
/// `S` is sized by its UTF-8 bytes, `B` by its decoded bytes and `N` by its
/// digit string. A number that passes the number rules is normalized when the
/// request is read; on the Query path a number that breaks them is refused
/// before it is sized (see `sized_clause`).
pub(super) fn key_value_byte_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => b.len(),
        _ => 0,
    }
}

/// The refusal for a partition key value over `max` bytes.
pub(super) fn hash_key_too_large(max: usize) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: \
         Size of hashkey has exceeded the maximum size limit of{max} bytes"
    ))
}

/// The refusal for partition key values that are each within `max` bytes but
/// add up to more than `max`. Measured 2026-10-23 on an index with HASH `h1`
/// and `h2`.
pub(super) fn hash_keys_too_large_in_total(max: usize) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: \
         Aggregated size of all hash keys has exceeded the size limit of {max} bytes"
    ))
}

/// The refusal for range key values over `max` bytes in total.
pub(super) fn range_keys_too_large(max: usize) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: \
         Aggregated size of all range keys has exceeded the size limit of {max} bytes"
    ))
}

/// One clause of a key condition: the attribute path and the value
/// expressions it carries (one for every form but `BETWEEN`, which has two).
type Clause<'a> = (&'a [PathElement], Vec<&'a Expr>);

/// Every clause of `kc`, whichever field the parser or the schema classifier
/// left it in. A clause is assigned to HASH or RANGE by its attribute name
/// against the key schema, not by its field, so the result does not depend on
/// how a multi-attribute key condition was arranged.
fn clauses(kc: &KeyCondition) -> Vec<Clause<'_>> {
    let mut clauses: Vec<Clause<'_>> = Vec::new();
    clauses.push((kc.pk_path.as_slice(), vec![&kc.pk_value]));
    for (path, value) in &kc.extra_pk_conditions {
        clauses.push((path.as_slice(), vec![value]));
    }
    match &kc.sk_condition {
        Some(SortKeyCondition::Compare { path, value, .. }) => {
            clauses.push((path.as_slice(), vec![value]));
        }
        Some(SortKeyCondition::Between { path, low, high }) => {
            clauses.push((path.as_slice(), vec![low, high]));
        }
        Some(SortKeyCondition::BeginsWith { path, prefix }) => {
            clauses.push((path.as_slice(), vec![prefix]));
        }
        None => {}
    }
    for (path, value) in &kc.extra_sk_conditions {
        clauses.push((path.as_slice(), vec![value]));
    }
    clauses
}

/// What one clause contributes to the size check.
enum ClauseSize {
    /// A `#name` or `:value` of the clause is not defined. The request keeps
    /// the undefined-reference refusal it gets downstream, ahead of any size.
    Unresolved,
    /// The clause names an attribute outside the key schema, or is not a
    /// placeholder, and adds nothing.
    Skipped,
    /// The key type of the clause's attribute and the bytes it contributes.
    Bytes(KeyType, usize),
}

/// Size one clause. A clause with two values (`BETWEEN`) contributes the
/// larger of the two. Measured 2026-10-23: bounds of 600 and 600 bytes pass (a
/// sum would be 1200), bounds of 1 and 1025 bytes are refused.
///
/// An `N` value is checked against the number rules first, and one that breaks
/// them is refused with the number's own sentence, never sized; one that
/// passes is sized by its normalized digit string. Measured 2026-11-20 in
/// us-east-1 on a GSI with HASH `n` (N): 2049 or 39 digits answer `Attempting
/// to store more than 38 significant digits in a Number` in
/// `KeyConditionExpression` and in `KeyConditions`, `1` followed by 2100 zeros
/// answers `Number overflow. ...`, and 2100 zeros followed by `1` is served.
///
/// # Errors
///
/// Returns the number rules' `ValidationException` for an `N` value that
/// breaks them.
fn sized_clause(
    clause: &Clause<'_>,
    key_schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
) -> Result<ClauseSize, DynamoDbError> {
    let (path, values) = clause;
    let Some(first) = path.first() else {
        return Ok(ClauseSize::Skipped);
    };
    let Ok(name) = resolve_element_name(first, maps) else {
        return Ok(ClauseSize::Unresolved);
    };
    let mut size = 0;
    let mut placeholders = true;
    for value in values {
        let Expr::Placeholder(placeholder) = value else {
            placeholders = false;
            continue;
        };
        let Ok(value) = maps.resolve_value(placeholder) else {
            return Ok(ClauseSize::Unresolved);
        };
        let bytes = match value {
            AttributeValue::N(n) => validate_and_normalize_number(n)?.len(),
            other => key_value_byte_size(other),
        };
        size = size.max(bytes);
    }
    let Some(key) = key_schema.iter().find(|ks| ks.attribute_name == name) else {
        return Ok(ClauseSize::Skipped);
    };
    if !placeholders {
        return Ok(ClauseSize::Skipped);
    }
    Ok(ClauseSize::Bytes(key.key_type, size))
}

/// Refuse a `Query` whose key condition carries a key value over the key size
/// limits.
///
/// - If any single HASH value is over `max_partition_key_size_bytes` (2048),
///   the single hash sentence. Otherwise, if the HASH values add up to more
///   than that, the aggregated hash sentence (only an index with several HASH
///   attributes can reach it).
/// - Every RANGE clause contributes the size of its value, whatever its
///   operator (`=`, `<`, `<=`, `>`, `>=`, `begins_with`); a `BETWEEN`
///   contributes the larger of its two bounds. The contributions are added
///   up, and a total over `max_sort_key_size_bytes` (1024) is refused with
///   the range sentence.
/// - The HASH checks run before the RANGE check, so the hash sentence wins when
///   both limits are exceeded.
/// - An `N` value that breaks the number rules is refused with the number's
///   sentence and is not sized (see `sized_clause`).
/// - If a `#name` or `:value` of any key clause is not defined, the check
///   returns `Ok`, because the service reports the undefined reference ahead
///   of the size. ExtendDB refuses most such requests later on. On a key
///   schema with one HASH and at most one RANGE attribute, an undefined
///   `#name` in a second clause (`pk = :h AND #u > :r`) is not refused yet and
///   the Query is served, where the service refuses it.
///
/// All limits are inclusive. `key_schema` is the key schema being queried: the
/// index's for an index query, the table's otherwise.
///
/// Measured on the service in us-east-1 2026-10-21 to 2026-10-23: the limits
/// and the sentences on a base table, an LSI and a GSI; string and binary
/// values sized by byte length; every operator above at 1025 bytes refused;
/// `BETWEEN` as described; the hash sentence when both limits are exceeded; on
/// an index with HASH `h1`, `h2`: `h1` of 2049 bytes and `h2` of 1 byte gives
/// the single sentence, 1025 and 1024 bytes (sum 2049) and 2048 and 2048 bytes
/// give the aggregated sentence; on an index with HASH `h` and RANGE `a`, `b`,
/// `c`, `d`: `a` of 600 and `c` of 600 bytes (with `b` of 1) refused, 512 and
/// 512 passes, 512 and 513 refused, `b > 1025` refused, `a` of 600 with `b
/// BETWEEN` 1 and 600 refused and with 1 and 424 passes. Measured 2026-11-20 on
/// the `h1`, `h2` index: 1024 and 1024 bytes (sum 2048) pass, and 1025 and 1024
/// bytes with a 1025-byte RANGE value give the aggregated hash sentence.
///
/// A clause that names an attribute outside the key schema, or that is not a
/// placeholder, adds nothing, so the refusal it already gets stays unchanged.
///
/// # Errors
///
/// Returns `ValidationException` with the service's sentence when a limit is
/// exceeded or an `N` value breaks the number rules.
pub fn validate_query_key_sizes(
    key_condition: &KeyCondition,
    key_schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    let mut hash_over = false;
    let mut hash_total: usize = 0;
    let mut range_total: usize = 0;
    for clause in clauses(key_condition) {
        match sized_clause(&clause, key_schema, maps)? {
            ClauseSize::Unresolved => return Ok(()),
            ClauseSize::Skipped => {}
            ClauseSize::Bytes(KeyType::Hash, size) => {
                hash_over |= size > limits.max_partition_key_size_bytes;
                hash_total = hash_total.saturating_add(size);
            }
            ClauseSize::Bytes(KeyType::Range, size) => {
                range_total = range_total.saturating_add(size);
            }
        }
    }
    if hash_over {
        return Err(hash_key_too_large(limits.max_partition_key_size_bytes));
    }
    if hash_total > limits.max_partition_key_size_bytes {
        return Err(hash_keys_too_large_in_total(
            limits.max_partition_key_size_bytes,
        ));
    }
    if range_total > limits.max_sort_key_size_bytes {
        return Err(range_keys_too_large(limits.max_sort_key_size_bytes));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
