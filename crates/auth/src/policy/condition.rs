// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition evaluation for IAM policy statements.
//!
//! Evaluates condition blocks against a `ConditionContext`. Supports all IAM
//! condition operators: String*, Numeric*, Date*, Bool, Null, Arn*, and the
//! set operators ForAllValues/ForAnyValue with optional IfExists suffix.

use super::context::ConditionContext;
use super::document::{Condition, ConditionOperator};
use super::matcher::wildcard_match;

/// Evaluate a single condition against a context.
///
/// Returns `true` if the condition is satisfied. The behavior depends on the
/// operator type:
/// - Base operators: key must be present, all context values must match at
///   least one policy value.
/// - `IfExists`: passes if the key is absent; otherwise evaluates normally.
/// - `Null`: checks key presence/absence.
/// - `ForAllValues`: every context value must match some policy value.
///   Absent key is vacuously true.
/// - `ForAnyValue`: at least one context value must match some policy value.
///   Absent key is false (unless wrapped in `IfExists`).
pub fn evaluate_condition(condition: &Condition, context: &impl ConditionContext) -> bool {
    let context_values = context.resolve_key(&condition.key);

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
            Some(vals) => evaluate_single_value_condition(inner, &vals, &expanded_values),
        },
        other => match context_values {
            None => false,
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

/// Evaluate a non-set, non-IfExists condition.
///
/// For single-valued keys, `context_values` has one element.
/// For multi-valued keys (e.g., `dynamodb:LeadingKeys`), all context values
/// must satisfy the condition (implicit AND).
///
/// For positive operators (StringEquals, NumericEquals, etc.): each context
/// value must match at least one policy value (OR semantics — "value in set").
///
/// For negative operators (StringNotEquals, NumericNotEquals, etc.): each
/// context value must satisfy the negative comparison against ALL policy
/// values (AND semantics — "value not in set"). This matches AWS IAM behavior
/// where `StringNotEquals` with `["a", "b"]` means "value is neither a nor b".
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
mod tests {
    use super::*;
    use crate::policy::context::ConditionContext;
    use std::collections::HashMap;

    /// Simple test context that maps keys to values.
    struct TestContext(HashMap<String, Vec<String>>);

    impl TestContext {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn with(mut self, key: &str, values: Vec<&str>) -> Self {
            self.0.insert(
                key.to_owned(),
                values.into_iter().map(ToOwned::to_owned).collect(),
            );
            self
        }
    }

    impl ConditionContext for TestContext {
        fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
            self.0
                .get(key)
                .map(|v| v.iter().map(|s| s.as_str()).collect())
        }
    }

    fn cond(op: ConditionOperator, key: &str, values: Vec<&str>) -> Condition {
        Condition {
            operator: op,
            key: key.to_owned(),
            values: values.into_iter().map(ToOwned::to_owned).collect(),
        }
    }

    // --- StringEquals ---

    #[test]
    fn string_equals_match() {
        let ctx = TestContext::new().with("k", vec!["hello"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
            &ctx
        ));
    }

    #[test]
    fn string_equals_no_match() {
        let ctx = TestContext::new().with("k", vec!["hello"]);
        assert!(!evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["world"]),
            &ctx
        ));
    }

    #[test]
    fn string_equals_absent_key() {
        let ctx = TestContext::new();
        assert!(!evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
            &ctx
        ));
    }

    #[test]
    fn string_equals_multiple_policy_values() {
        let ctx = TestContext::new().with("k", vec!["b"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["a", "b", "c"]),
            &ctx
        ));
    }

    // --- StringLike ---

    #[test]
    fn string_like_wildcard() {
        let ctx = TestContext::new().with("k", vec!["hello-world"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringLike, "k", vec!["hello-*"]),
            &ctx
        ));
    }

    #[test]
    fn string_not_like() {
        let ctx = TestContext::new().with("k", vec!["hello"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringNotLike, "k", vec!["world*"]),
            &ctx
        ));
    }

    // --- StringEqualsIgnoreCase ---

    #[test]
    fn string_equals_ignore_case() {
        let ctx = TestContext::new().with("k", vec!["Hello"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::StringEqualsIgnoreCase,
                "k",
                vec!["hello"]
            ),
            &ctx
        ));
    }

    // --- Numeric ---

    #[test]
    fn numeric_equals() {
        let ctx = TestContext::new().with("k", vec!["42"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::NumericEquals, "k", vec!["42"]),
            &ctx
        ));
    }

    #[test]
    fn numeric_less_than() {
        let ctx = TestContext::new().with("k", vec!["5"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::NumericLessThan, "k", vec!["10"]),
            &ctx
        ));
    }

    #[test]
    fn numeric_greater_than_equals() {
        let ctx = TestContext::new().with("k", vec!["10"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::NumericGreaterThanEquals, "k", vec!["10"]),
            &ctx
        ));
    }

    #[test]
    fn numeric_invalid_parse() {
        let ctx = TestContext::new().with("k", vec!["abc"]);
        assert!(!evaluate_condition(
            &cond(ConditionOperator::NumericEquals, "k", vec!["42"]),
            &ctx
        ));
    }

    // --- Bool ---

    #[test]
    fn bool_true() {
        let ctx = TestContext::new().with("k", vec!["true"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::Bool, "k", vec!["true"]),
            &ctx
        ));
    }

    #[test]
    fn bool_case_insensitive() {
        let ctx = TestContext::new().with("k", vec!["True"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::Bool, "k", vec!["true"]),
            &ctx
        ));
    }

    // --- Null ---

    #[test]
    fn null_true_absent_key() {
        let ctx = TestContext::new();
        assert!(evaluate_condition(
            &cond(ConditionOperator::Null, "k", vec!["true"]),
            &ctx
        ));
    }

    #[test]
    fn null_true_present_key() {
        let ctx = TestContext::new().with("k", vec!["val"]);
        assert!(!evaluate_condition(
            &cond(ConditionOperator::Null, "k", vec!["true"]),
            &ctx
        ));
    }

    #[test]
    fn null_false_present_key() {
        let ctx = TestContext::new().with("k", vec!["val"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::Null, "k", vec!["false"]),
            &ctx
        ));
    }

    #[test]
    fn null_false_absent_key() {
        let ctx = TestContext::new();
        assert!(!evaluate_condition(
            &cond(ConditionOperator::Null, "k", vec!["false"]),
            &ctx
        ));
    }

    // --- ArnLike ---

    #[test]
    fn arn_like_match() {
        let ctx = TestContext::new().with("k", vec!["arn:aws:dynamodb:us-east-1:123:table/Users"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ArnLike,
                "k",
                vec!["arn:aws:dynamodb:*:*:table/User*"]
            ),
            &ctx
        ));
    }

    #[test]
    fn arn_not_like() {
        let ctx = TestContext::new().with("k", vec!["arn:aws:dynamodb:us-east-1:123:table/Orders"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ArnNotLike,
                "k",
                vec!["arn:aws:dynamodb:*:*:table/User*"]
            ),
            &ctx
        ));
    }

    // --- IfExists ---

    #[test]
    fn if_exists_absent_key_passes() {
        let ctx = TestContext::new();
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["val"]
            ),
            &ctx
        ));
    }

    #[test]
    fn if_exists_present_key_evaluates() {
        let ctx = TestContext::new().with("k", vec!["val"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["val"]
            ),
            &ctx
        ));
    }

    #[test]
    fn if_exists_present_key_fails() {
        let ctx = TestContext::new().with("k", vec!["other"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["val"]
            ),
            &ctx
        ));
    }

    // --- ForAllValues ---

    #[test]
    fn for_all_values_all_match() {
        let ctx = TestContext::new().with("k", vec!["a", "b"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a", "b", "c"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_all_values_one_missing() {
        let ctx = TestContext::new().with("k", vec!["a", "d"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a", "b", "c"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_all_values_absent_key_vacuously_true() {
        let ctx = TestContext::new();
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a"]
            ),
            &ctx
        ));
    }

    // --- ForAnyValue ---

    #[test]
    fn for_any_value_one_match() {
        let ctx = TestContext::new().with("k", vec!["x", "a"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_any_value_no_match() {
        let ctx = TestContext::new().with("k", vec!["x", "y"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_any_value_absent_key_false() {
        let ctx = TestContext::new();
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
                "k",
                vec!["a"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_any_value_if_exists_absent_key_true() {
        let ctx = TestContext::new();
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::IfExists(Box::new(
                    ConditionOperator::StringEquals
                )))),
                "k",
                vec!["a"]
            ),
            &ctx
        ));
    }

    // --- Date operators ---

    #[test]
    fn date_equals() {
        let ctx = TestContext::new().with("k", vec!["2026-01-01T00:00:00Z"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::DateEquals,
                "k",
                vec!["2026-01-01T00:00:00Z"]
            ),
            &ctx
        ));
    }

    #[test]
    fn date_less_than() {
        let ctx = TestContext::new().with("k", vec!["2025-01-01T00:00:00Z"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::DateLessThan,
                "k",
                vec!["2026-01-01T00:00:00Z"]
            ),
            &ctx
        ));
    }

    // --- Multi-value context with base operator ---

    #[test]
    fn multi_value_context_all_must_match() {
        // With a base operator (not ForAllValues), all context values must match
        // at least one policy value
        let ctx = TestContext::new().with("k", vec!["a", "b"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["a", "b"]),
            &ctx
        ));
    }

    #[test]
    fn multi_value_context_one_fails() {
        let ctx = TestContext::new().with("k", vec!["a", "c"]);
        assert!(!evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["a", "b"]),
            &ctx
        ));
    }

    // --- Negative operators with multiple policy values ---

    #[test]
    fn string_not_equals_single_policy_value() {
        let ctx = TestContext::new().with("k", vec!["admin"]);
        // "admin" == "admin", so StringNotEquals should be false
        assert!(!evaluate_condition(
            &cond(ConditionOperator::StringNotEquals, "k", vec!["admin"]),
            &ctx
        ));
    }

    #[test]
    fn string_not_equals_multiple_policy_values_match() {
        let ctx = TestContext::new().with("k", vec!["admin"]);
        // "admin" is in {"admin", "root"}, so StringNotEquals should be false
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::StringNotEquals,
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn string_not_equals_multiple_policy_values_no_match() {
        let ctx = TestContext::new().with("k", vec!["user"]);
        // "user" is NOT in {"admin", "root"}, so StringNotEquals should be true
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::StringNotEquals,
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn string_not_like_multiple_policy_values() {
        let ctx = TestContext::new().with("k", vec!["hello-world"]);
        // "hello-world" matches "hello-*", so StringNotLike should be false
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::StringNotLike,
                "k",
                vec!["hello-*", "foo-*"]
            ),
            &ctx
        ));
    }

    #[test]
    fn string_not_like_multiple_policy_values_no_match() {
        let ctx = TestContext::new().with("k", vec!["bar-baz"]);
        // "bar-baz" matches neither "hello-*" nor "foo-*"
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::StringNotLike,
                "k",
                vec!["hello-*", "foo-*"]
            ),
            &ctx
        ));
    }

    // --- Policy variable expansion ---

    #[test]
    fn policy_variable_expansion_principal_tag() {
        let ctx = TestContext::new()
            .with("dynamodb:ResourceTag/Team", vec!["Alpha"])
            .with("aws:PrincipalTag/Team", vec!["Alpha"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::StringEquals,
                "dynamodb:ResourceTag/Team",
                vec!["${aws:PrincipalTag/Team}"]
            ),
            &ctx
        ));
    }

    #[test]
    fn policy_variable_expansion_mismatch() {
        let ctx = TestContext::new()
            .with("dynamodb:ResourceTag/Team", vec!["Beta"])
            .with("aws:PrincipalTag/Team", vec!["Alpha"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::StringEquals,
                "dynamodb:ResourceTag/Team",
                vec!["${aws:PrincipalTag/Team}"]
            ),
            &ctx
        ));
    }

    #[test]
    fn policy_variable_no_expansion_needed() {
        let ctx = TestContext::new().with("k", vec!["hello"]);
        assert!(evaluate_condition(
            &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
            &ctx
        ));
    }

    #[test]
    fn policy_variable_unresolvable_left_literal() {
        let ctx = TestContext::new().with("k", vec!["${aws:PrincipalTag/Missing}"]);
        // Unresolvable variable stays literal — matches if context has the literal
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::StringEquals,
                "k",
                vec!["${aws:PrincipalTag/Missing}"]
            ),
            &ctx
        ));
    }

    // --- ForAllValues with negative operators ---

    #[test]
    fn for_all_values_string_not_equals_context_in_policy_set() {
        // ForAllValues:StringNotEquals with context ["admin"] and policy ["admin", "root"]
        // "admin" IS in the set, so the condition should be FALSE (deny applies).
        let ctx = TestContext::new().with("k", vec!["admin"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_all_values_string_not_equals_context_not_in_policy_set() {
        // ForAllValues:StringNotEquals with context ["user"] and policy ["admin", "root"]
        // "user" is NOT in the set, so the condition should be TRUE.
        let ctx = TestContext::new().with("k", vec!["user"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_all_values_string_not_equals_mixed_context() {
        // ForAllValues:StringNotEquals with context ["user", "admin"] and policy ["admin", "root"]
        // "admin" IS in the set, so the condition should be FALSE.
        let ctx = TestContext::new().with("k", vec!["user", "admin"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_any_value_string_not_equals_all_in_set() {
        // ForAnyValue:StringNotEquals with context ["admin", "root"] and policy ["admin", "root"]
        // Both context values are in the policy set, so none satisfy "not in set" → FALSE.
        let ctx = TestContext::new().with("k", vec!["admin", "root"]);
        assert!(!evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringNotEquals)),
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }

    #[test]
    fn for_any_value_string_not_equals_one_outside_set() {
        // ForAnyValue:StringNotEquals with context ["admin", "user"] and policy ["admin", "root"]
        // "user" is NOT in the set, so at least one satisfies "not in set" → TRUE.
        let ctx = TestContext::new().with("k", vec!["admin", "user"]);
        assert!(evaluate_condition(
            &cond(
                ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringNotEquals)),
                "k",
                vec!["admin", "root"]
            ),
            &ctx
        ));
    }
}
