// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition evaluation for IAM policy statements.
//!
//! Evaluates condition blocks against a `ConditionContext`. Supports all IAM
//! condition operators: String*, Numeric*, Date*, Bool, Null, Arn*, and the
//! set operators ForAllValues/ForAnyValue with optional `IfExists` suffix.

use super::context::ConditionContext;
use super::document::{Condition, ConditionOperator};
use super::matcher::wildcard_match;

/// Evaluate a single condition against a context.
///
/// Returns `true` if the condition is satisfied. The behavior depends on the
/// operator type:
/// - Base (bare) operators on a **single-valued** key: key must be present and
///   its value must match at least one policy value.
/// - Base (bare) operators on a **multivalued** key (`dynamodb:Attributes`,
///   `dynamodb:LeadingKeys`): never match. AWS IAM requires a
///   `ForAllValues:`/`ForAnyValue:` qualifier for multivalued keys; a bare
///   operator on such a key is a no-op regardless of value count (BR-7085).
/// - `IfExists`: passes if the key is absent; otherwise evaluates as the base
///   operator (and so never matches a present multivalued key).
/// - `Null`: checks key presence/absence.
/// - `ForAllValues`: every context value must match some policy value.
///   Absent key is vacuously true.
/// - `ForAnyValue`: at least one context value must match some policy value.
///   Absent key is false (unless wrapped in `IfExists`).
pub fn evaluate_condition(condition: &Condition, context: &impl ConditionContext) -> bool {
    let context_values = context.resolve_key(&condition.key);
    let is_multivalued = context.is_multivalued_key(&condition.key);

    // Expand policy variables (e.g. `${aws:PrincipalTag/Team}`) in condition values.
    let expanded_values: Vec<String> = condition
        .values
        .iter()
        .map(|v| expand_policy_variables(v, context))
        .collect();

    match &condition.operator {
        ConditionOperator::Null => {
            let key_absent = context_values.is_none();
            expanded_values
                .first()
                .is_some_and(|v| (v == "true" && key_absent) || (v == "false" && !key_absent))
        }
        ConditionOperator::ForAllValues(inner) => {
            let (_absent_passes, base_op) = unwrap_if_exists(inner);
            match context_values {
                None => true,
                Some(vals) => {
                    if is_negative_operator(base_op) {
                        // Negative operators: each context value must satisfy the
                        // negative comparison against ALL policy values.
                        // e.g. ForAllValues:StringNotEquals with ["admin","root"]
                        // means "every context value is neither admin nor root".
                        vals.iter().all(|cv| {
                            expanded_values
                                .iter()
                                .all(|pv| compare_single(base_op, cv, pv))
                        })
                    } else {
                        // Positive operators: each context value must match at
                        // least one policy value.
                        vals.iter().all(|cv| {
                            expanded_values
                                .iter()
                                .any(|pv| compare_single(base_op, cv, pv))
                        })
                    }
                }
            }
        }
        ConditionOperator::ForAnyValue(inner) => {
            let (absent_passes, base_op) = unwrap_if_exists(inner);
            match context_values {
                None => absent_passes,
                Some(vals) => {
                    if is_negative_operator(base_op) {
                        // Negative operators: at least one context value must
                        // satisfy the negative comparison against ALL policy values.
                        // e.g. ForAnyValue:StringNotEquals with ["admin","root"]
                        // means "at least one context value is neither admin nor root".
                        vals.iter().any(|cv| {
                            expanded_values
                                .iter()
                                .all(|pv| compare_single(base_op, cv, pv))
                        })
                    } else {
                        // Positive operators: at least one context value must
                        // match at least one policy value.
                        vals.iter().any(|cv| {
                            expanded_values
                                .iter()
                                .any(|pv| compare_single(base_op, cv, pv))
                        })
                    }
                }
            }
        }
        ConditionOperator::IfExists(inner) => match context_values {
            None => true,
            // A bare operator (even wrapped in IfExists) applied to a multivalued
            // key never matches in AWS IAM — only ForAllValues/ForAnyValue do.
            // Key present + multivalued → no match. (BR-7085)
            Some(_) if is_multivalued => false,
            Some(vals) => evaluate_single_value_condition(inner, &vals, &expanded_values),
        },
        other => match context_values {
            None => false,
            // Bare operator on a multivalued key never matches (BR-7085). This is
            // fail-safe: a bare Deny becomes a no-op (matching AWS, which forces the
            // author to use ForAnyValue:), and a bare Allow allowlist stops granting.
            Some(_) if is_multivalued => false,
            Some(vals) => evaluate_single_value_condition(other, &vals, &expanded_values),
        },
    }
}

/// Expand IAM policy variables in a string value.
///
/// Replaces `${variable}` patterns with their resolved values from the context.
/// Supported variables: `aws:PrincipalTag/*` and any key the context can
/// resolve. Unresolvable variables are left as-is (IAM behavior).
fn expand_policy_variables(value: &str, context: &impl ConditionContext) -> String {
    if !value.contains("${") {
        return value.to_owned();
    }

    let mut result = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        if let Some(end) = after_start.find('}') {
            let var_name = &after_start[..end];
            if let Some(vals) = context.resolve_key(var_name) {
                // Use the first value for single-valued expansion.
                if let Some(v) = vals.first() {
                    result.push_str(v);
                }
            } else {
                // Unresolvable — leave the variable literal.
                result.push_str(&rest[start..start + 3 + end]);
            }
            rest = &after_start[end + 1..];
        } else {
            // No closing brace — copy literally.
            result.push_str(&rest[start..]);
            rest = "";
        }
    }
    result.push_str(rest);
    result
}

/// Unwrap an `IfExists` wrapper if present.
///
/// Returns `(absent_passes, base_operator)` where `absent_passes` is true
/// if the original operator was `IfExists(base)`.
fn unwrap_if_exists(op: &ConditionOperator) -> (bool, &ConditionOperator) {
    match op {
        ConditionOperator::IfExists(base) => (true, base),
        other => (false, other),
    }
}

/// Evaluate a non-set, non-IfExists condition for a single-valued key.
///
/// The caller (`evaluate_condition`) only reaches this for single-valued keys;
/// bare operators on multivalued keys are rejected before this point (BR-7085).
/// A single-valued key resolves to exactly one context value in practice, so the
/// `all`/`any` combinators below collapse to that one value.
///
/// For positive operators (`StringEquals`, `NumericEquals`, etc.): the context
/// value must match at least one policy value (OR semantics across policy values
/// — "value in set").
///
/// For negative operators (`StringNotEquals`, `NumericNotEquals`, etc.): the
/// context value must satisfy the negative comparison against ALL policy values
/// (AND semantics — "value not in set"). This matches AWS IAM behavior where
/// `StringNotEquals` with `["a", "b"]` means "value is neither a nor b".
fn evaluate_single_value_condition(
    op: &ConditionOperator,
    context_values: &[&str],
    policy_values: &[String],
) -> bool {
    if is_negative_operator(op) {
        // Negative operators: context value must not match ANY policy value.
        // Equivalent to: for each context value, ALL policy values must fail
        // the positive comparison.
        context_values
            .iter()
            .all(|cv| policy_values.iter().all(|pv| compare_single(op, cv, pv)))
    } else {
        // Positive operators: context value must match at least one policy value.
        context_values
            .iter()
            .all(|cv| policy_values.iter().any(|pv| compare_single(op, cv, pv)))
    }
}

/// Returns true for negative/negating condition operators.
fn is_negative_operator(op: &ConditionOperator) -> bool {
    matches!(
        op,
        ConditionOperator::StringNotEquals
            | ConditionOperator::NumericNotEquals
            | ConditionOperator::DateNotEquals
            | ConditionOperator::StringNotLike
            | ConditionOperator::ArnNotEquals
            | ConditionOperator::ArnNotLike
    )
}

/// Compare a single context value against a single policy value.
///
/// Returns `true` if the comparison holds for the given operator.
///
/// # Panics
///
/// Does not panic. Returns `false` for set/wrapper operators that should
/// have been handled by the caller.
fn compare_single(op: &ConditionOperator, context_value: &str, policy_value: &str) -> bool {
    match op {
        ConditionOperator::StringEquals => context_value == policy_value,
        ConditionOperator::StringNotEquals => context_value != policy_value,
        ConditionOperator::StringEqualsIgnoreCase => {
            context_value.eq_ignore_ascii_case(policy_value)
        }
        ConditionOperator::StringLike => wildcard_match(policy_value, context_value),
        ConditionOperator::StringNotLike => !wildcard_match(policy_value, context_value),
        ConditionOperator::NumericEquals => parse_f64_cmp(context_value, policy_value, f64::eq),
        ConditionOperator::NumericNotEquals => {
            parse_f64_cmp(context_value, policy_value, |a, b| a != b)
        }
        ConditionOperator::NumericLessThan => {
            parse_f64_cmp(context_value, policy_value, |a, b| a < b)
        }
        ConditionOperator::NumericLessThanEquals => {
            parse_f64_cmp(context_value, policy_value, |a, b| a <= b)
        }
        ConditionOperator::NumericGreaterThan => {
            parse_f64_cmp(context_value, policy_value, |a, b| a > b)
        }
        ConditionOperator::NumericGreaterThanEquals => {
            parse_f64_cmp(context_value, policy_value, |a, b| a >= b)
        }
        ConditionOperator::DateEquals => compare_dates(context_value, policy_value, |a, b| a == b),
        ConditionOperator::DateNotEquals => {
            compare_dates(context_value, policy_value, |a, b| a != b)
        }
        ConditionOperator::DateLessThan => compare_dates(context_value, policy_value, |a, b| a < b),
        ConditionOperator::DateLessThanEquals => {
            compare_dates(context_value, policy_value, |a, b| a <= b)
        }
        ConditionOperator::DateGreaterThan => {
            compare_dates(context_value, policy_value, |a, b| a > b)
        }
        ConditionOperator::DateGreaterThanEquals => {
            compare_dates(context_value, policy_value, |a, b| a >= b)
        }
        ConditionOperator::Bool => context_value.eq_ignore_ascii_case(policy_value),
        ConditionOperator::ArnEquals => context_value == policy_value,
        ConditionOperator::ArnNotEquals => context_value != policy_value,
        ConditionOperator::ArnLike => super::matcher::arn_match(policy_value, context_value),
        ConditionOperator::ArnNotLike => !super::matcher::arn_match(policy_value, context_value),
        // Null, ForAllValues, ForAnyValue, IfExists handled by caller
        _ => false,
    }
}

/// Parse two strings as f64 and compare them.
/// Returns `false` if either value fails to parse.
fn parse_f64_cmp(a: &str, b: &str, cmp: impl FnOnce(&f64, &f64) -> bool) -> bool {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(va), Ok(vb)) => cmp(&va, &vb),
        _ => false,
    }
}

/// Parse two strings as ISO 8601 timestamps and compare them.
/// Returns `false` if either value fails to parse.
fn compare_dates(a: &str, b: &str, cmp: impl FnOnce(i128, i128) -> bool) -> bool {
    match (parse_epoch_millis(a), parse_epoch_millis(b)) {
        (Some(va), Some(vb)) => cmp(va, vb),
        _ => false,
    }
}

/// Parse an ISO 8601 date string to epoch milliseconds.
///
/// Supports formats: `YYYY-MM-DDThh:mm:ssZ` and `YYYY-MM-DDThh:mm:ss.sssZ`.
/// Also accepts epoch seconds as a plain number.
fn parse_epoch_millis(s: &str) -> Option<i128> {
    // Try epoch seconds first (plain number)
    if let Ok(n) = s.parse::<f64>()
        && !s.contains('T')
        && !s.contains('-')
    {
        // Reject NaN/Infinity — they are not valid epoch timestamps.
        if !n.is_finite() {
            return None;
        }
        // f64 → i128 via `as` is saturating (Rust ≥1.45). Epoch millis
        // for any realistic date fits in i128 with no precision loss.
        #[allow(clippy::cast_possible_truncation)]
        return Some((n * 1000.0) as i128);
    }

    // Try ISO 8601 with time crate
    let format = time::format_description::well_known::Rfc3339;
    time::OffsetDateTime::parse(s, &format)
        .ok()
        .map(|dt| dt.unix_timestamp_nanos() / 1_000_000)
}

#[cfg(test)]
mod tests;
