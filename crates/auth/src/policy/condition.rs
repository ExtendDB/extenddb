// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition evaluation for IAM policy statements.
//!
//! Evaluates condition blocks against a `ConditionContext`. Supports all IAM
//! condition operators: String*, Numeric*, Date*, Bool, Null, Arn*, and the
//! set operators ForAllValues/ForAnyValue with optional `IfExists` suffix.

use super::context::ConditionContext;
use super::document::{Condition, ConditionOperator, base_operator};
use super::variables::{ConditionValue, arn_value_lacks_fields};

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
///
/// Policy variables in the values are expanded as in a `2012-10-17` document;
/// [`evaluate_condition_with`] takes the document's choice.
pub fn evaluate_condition(condition: &Condition, context: &impl ConditionContext) -> bool {
    evaluate_condition_with(condition, true, context)
}

/// [`evaluate_condition`] with `expand_variables` saying whether the document
/// expands policy variables in condition values
/// ([`super::document::PolicyDocument::expands_policy_variables`]). A
/// `2008-10-17` document, or one with no `Version`, compares each value as
/// written.
pub fn evaluate_condition_with(
    condition: &Condition,
    expand_variables: bool,
    context: &impl ConditionContext,
) -> bool {
    let context_values = context.resolve_key(&condition.key);
    let is_multivalued = context.is_multivalued_key(&condition.key);
    // Only the string and ARN operators expand policy variables (measured). A
    // numeric value written as a variable is compared as written, and a `Bool`
    // or `Null` value written as one is read as the boolean `false`
    // (`bool_matches`, `null_holds`).
    let expand_variables = expand_variables && operator_expands_variables(&condition.operator);

    // Expand policy variables (e.g. `${aws:PrincipalTag/Team}`) in condition values.
    let expanded_values: Vec<ConditionValue> = condition
        .values
        .iter()
        .map(|v| expand_policy_variables(v, expand_variables, context))
        .collect();

    match &condition.operator {
        ConditionOperator::Null => {
            let key_absent = context_values.is_none();
            expanded_values
                .first()
                .and_then(ConditionValue::text)
                .is_some_and(|v| null_holds(v, key_absent))
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
            // A negated operator on an absent single-valued key evaluates true
            // in AWS IAM: the key has no value, so it cannot equal the policy
            // value, so "not equals" holds. This makes a Deny gated on
            // StringNotEquals apply when the caller omits the key (the
            // confused-deputy guard). Measured on live IAM/STS and DynamoDB.
            // Bare operators on multivalued keys
            // never match, present or absent (BR-7085), so the rule is scoped
            // to single-valued keys.
            None => is_negative_operator(other) && !is_multivalued,
            // Bare operator on a multivalued key never matches (BR-7085). This is
            // fail-safe: a bare Deny becomes a no-op (matching AWS, which forces the
            // author to use ForAnyValue:), and a bare Allow allowlist stops granting.
            Some(_) if is_multivalued => false,
            Some(vals) => evaluate_single_value_condition(other, &vals, &expanded_values),
        },
    }
}

/// Three-valued outcome of evaluating a statement's condition block.
///
/// A condition key falls into one of three cases, and each is treated
/// differently:
/// 1. The context resolves the key to a value: evaluate normally.
/// 2. The context resolves the key as absent in the same way the service would
///    have it absent: IAM absent-key semantics apply (including the rule that a
///    negated operator on an absent single-valued key matches).
/// 3. The service would populate the key but this context cannot resolve it
///    (`is_unresolvable_key`): the statement fails closed whatever the operator.
///    A `Deny` that depends on it blocks; an `Allow` that depends on it does not
///    grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionMatch {
    /// All conditions are satisfied; the statement applies.
    Match,
    /// A condition is not satisfied; the statement does not apply.
    NoMatch,
    /// A condition depends on a key the service populates but this context
    /// cannot resolve, or has an `Arn*` value that contains `${` and cannot
    /// have the six fields of an ARN, which IAM refuses in a document of any
    /// `Version`. The statement fails closed.
    Unresolvable,
}

/// Evaluate one condition three-valued.
///
/// Returns [`ConditionMatch::Unresolvable`] when the key is unresolved and the
/// context reports it as service-populated (case 3). It also returns
/// `Unresolvable`, whatever `expand_variables` says, when a value of an `Arn*`
/// operator, with or without a set qualifier or `IfExists`, contains `${` and
/// has fewer than five `:` written outside its `${...}` references, such as
/// `${aws:ResourceTag/owner}`. Otherwise returns `Match` or `NoMatch` from the
/// ordinary evaluation, so cases 1 and 2 are unchanged.
pub fn evaluate_condition_match(
    condition: &Condition,
    expand_variables: bool,
    context: &impl ConditionContext,
) -> ConditionMatch {
    if context.resolve_key(&condition.key).is_none() && context.is_unresolvable_key(&condition.key)
    {
        return ConditionMatch::Unresolvable;
    }
    // The IAM policy simulator refuses a policy with such an ARN value
    // (`InvalidInput`) and IAM Access Analyzer reports `MISSING_ARN_FIELD`, so
    // IAM gives no decision to match. Failing closed makes a `Deny` apply and
    // keeps an `Allow` from granting. IAM checks the value's fields whatever
    // the document's `Version`, so the rule does not read `expand_variables`.
    if matches!(
        base_operator(&condition.operator),
        ConditionOperator::ArnEquals
            | ConditionOperator::ArnNotEquals
            | ConditionOperator::ArnLike
            | ConditionOperator::ArnNotLike
    ) && condition.values.iter().any(|v| arn_value_lacks_fields(v))
    {
        return ConditionMatch::Unresolvable;
    }
    if evaluate_condition_with(condition, expand_variables, context) {
        ConditionMatch::Match
    } else {
        ConditionMatch::NoMatch
    }
}

/// Evaluate a statement's whole condition block (all conditions are ANDed).
///
/// `NoMatch` dominates: if any condition is definitely not satisfied the
/// statement does not apply, even if another condition is unresolvable. Failing
/// that, any unresolvable condition makes the block `Unresolvable` (fail
/// closed). Otherwise every condition matched.
///
/// `expand_variables` is the document's
/// [`super::document::PolicyDocument::expands_policy_variables`].
pub fn evaluate_statement_conditions(
    conditions: &[Condition],
    expand_variables: bool,
    context: &impl ConditionContext,
) -> ConditionMatch {
    let mut any_unresolvable = false;
    for condition in conditions {
        match evaluate_condition_match(condition, expand_variables, context) {
            ConditionMatch::Match => {}
            ConditionMatch::NoMatch => return ConditionMatch::NoMatch,
            ConditionMatch::Unresolvable => any_unresolvable = true,
        }
    }
    if any_unresolvable {
        ConditionMatch::Unresolvable
    } else {
        ConditionMatch::Match
    }
}

/// Expand IAM policy variables in a string value.
///
/// With `expand_variables`, replaces `${variable}` and `${variable, 'default'}`
/// patterns with their resolved values from the context and `${*}`, `${?}` and
/// `${$}` with the literal character (see [`super::variables`]). Supported
/// variables: `aws:PrincipalTag/*` and any key the context can resolve. A
/// variable with no default whose key the request does not carry makes the
/// value match nothing, and so does a variable naming a key the context cannot
/// resolve, with or without a default; see
/// [`super::variables::expand_condition_value`]. Without `expand_variables`
/// the value is compared as written.
fn expand_policy_variables(
    value: &str,
    expand_variables: bool,
    context: &impl ConditionContext,
) -> ConditionValue {
    super::variables::expand_condition_value(value, expand_variables, context)
}

/// Whether `op` expands policy variables in its values: the string and ARN
/// operators, with or without a set qualifier or `IfExists`.
///
/// IAM's condition operator reference lists policy variables as supported for
/// the string, Boolean and ARN operators and not supported for the numeric,
/// date, binary and IP address operators, and says a variable cannot be used
/// with `Null`. The IAM policy simulator expands a variable for the string and
/// ARN operators only: it compares a `NumericEquals` value written as a
/// variable as written, and reads a `Bool` or `Null` one as the boolean `false`
/// (`bool_matches`, `null_holds`), although IAM Access Analyzer reports those
/// two as `INVALID_VARIABLE_FOR_OPERATOR`. The date and address operators are
/// treated as the numeric ones; that is not measured.
fn operator_expands_variables(op: &ConditionOperator) -> bool {
    match op {
        ConditionOperator::ForAllValues(inner)
        | ConditionOperator::ForAnyValue(inner)
        | ConditionOperator::IfExists(inner) => operator_expands_variables(inner),
        ConditionOperator::StringEquals
        | ConditionOperator::StringNotEquals
        | ConditionOperator::StringEqualsIgnoreCase
        | ConditionOperator::StringNotEqualsIgnoreCase
        | ConditionOperator::StringLike
        | ConditionOperator::StringNotLike
        | ConditionOperator::ArnEquals
        | ConditionOperator::ArnNotEquals
        | ConditionOperator::ArnLike
        | ConditionOperator::ArnNotLike => true,
        _ => false,
    }
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
    policy_values: &[ConditionValue],
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
            | ConditionOperator::StringNotEqualsIgnoreCase
            | ConditionOperator::NumericNotEquals
            | ConditionOperator::DateNotEquals
            | ConditionOperator::StringNotLike
            | ConditionOperator::ArnNotEquals
            | ConditionOperator::ArnNotLike
            | ConditionOperator::NotIpAddress
    )
}

/// Compare a single context value against a single policy value.
///
/// Returns `true` if the comparison holds for the given operator. An
/// unresolved policy value ([`ConditionValue::Unresolved`]) matches nothing:
/// a positive operator is false and a negative operator true, the answer an
/// absent key gets (measured for `StringEquals` and `StringNotEquals`).
///
/// # Panics
///
/// Does not panic. Returns `false` for set/wrapper operators that should
/// have been handled by the caller.
fn compare_single(op: &ConditionOperator, context_value: &str, value: &ConditionValue) -> bool {
    let Some(policy_value) = value.text() else {
        return is_negative_operator(op);
    };
    match op {
        ConditionOperator::StringEquals => context_value == policy_value,
        ConditionOperator::StringNotEquals => context_value != policy_value,
        ConditionOperator::StringEqualsIgnoreCase => {
            context_value.eq_ignore_ascii_case(policy_value)
        }
        ConditionOperator::StringNotEqualsIgnoreCase => {
            !context_value.eq_ignore_ascii_case(policy_value)
        }
        ConditionOperator::StringLike => value.wildcard_matches(context_value),
        ConditionOperator::StringNotLike => !value.wildcard_matches(context_value),
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
        ConditionOperator::Bool => bool_matches(context_value, policy_value),
        ConditionOperator::ArnEquals => context_value == policy_value,
        ConditionOperator::ArnNotEquals => context_value != policy_value,
        ConditionOperator::ArnLike => value.arn_matches(context_value),
        ConditionOperator::ArnNotLike => !value.arn_matches(context_value),
        // IpAddress matches only when the caller address and the policy range
        // are the same family and the address is in the range. NotIpAddress
        // matches only when they are the same family and the address is not in
        // the range. A family mismatch or a malformed value makes both false,
        // matching the live service.
        ConditionOperator::IpAddress => {
            super::matcher::ip_in_cidr(policy_value, context_value) == Some(true)
        }
        ConditionOperator::NotIpAddress => {
            super::matcher::ip_in_cidr(policy_value, context_value) == Some(false)
        }
        // Null, ForAllValues, ForAnyValue, IfExists handled by caller
        _ => false,
    }
}

/// Whether `Null` with the policy value `policy_value` holds: `true` means the
/// key must be absent and `false` that it must be present, and any other text
/// matches nothing. `Null` does not expand a policy variable. IAM reads a value
/// written as one as a boolean, `false` unless the text is `true` ignoring
/// case, so the key must be present whatever the variable's value or default
/// (measured with the IAM policy simulator, cells N1a to N1f). The cells are
/// `2012-10-17` documents; a `2008-10-17` or no-`Version` document is read the
/// same way, since IAM reads the text without expanding it whatever the
/// `Version` (not measured).
fn null_holds(policy_value: &str, key_absent: bool) -> bool {
    if policy_value.contains("${") {
        // A text that contains `${` is never `true`, so it reads as `false`:
        // the key must be present.
        return !key_absent;
    }
    (policy_value == "true" && key_absent) || (policy_value == "false" && !key_absent)
}

/// Whether `Bool` with the policy value `policy_value` matches the request
/// value: the two texts are equal ignoring case. `Bool` does not expand a
/// policy variable. IAM reads a value written as one as a boolean, `false`
/// unless the text is `true` ignoring case, so it matches a request value that
/// is not `true` whatever the variable's value (measured with the IAM policy
/// simulator, cells N2a to N2e). The cells are `2012-10-17` documents; a
/// `2008-10-17` or no-`Version` document is read the same way, since IAM reads
/// the text without expanding it whatever the `Version` (not measured).
fn bool_matches(context_value: &str, policy_value: &str) -> bool {
    if policy_value.contains("${") {
        // A text that contains `${` is never `true`, so it reads as `false`
        // and matches a request value that is not `true`.
        return !context_value.eq_ignore_ascii_case("true");
    }
    context_value.eq_ignore_ascii_case(policy_value)
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
