// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy values of the Arn condition operators.
//!
//! `ArnEquals`, `ArnLike`, `ArnNotEquals` and `ArnNotLike`, with their
//! `ForAllValues:`, `ForAnyValue:` and `IfExists` forms, compare a context ARN
//! with a policy value. This module holds how the live service matches such a
//! value against an ARN, and the checks it applies to the value when a policy
//! is written. Identity policies and role trust policies are checked
//! differently. Every rule here was measured on live IAM on 2026-10-30; the
//! rows are in the description of the pull request for issue #665.

use serde_json::Value;

use super::document::{Condition, ConditionOperator, PolicyParseError, Statement, base_operator};
use super::matcher::wildcard_match;

/// Code and message the live service returns when an identity, managed or
/// boundary policy holds an Arn condition value that
/// [`is_valid_identity_arn_value`] rejects.
pub const IDENTITY_ARN_VALUE_ERROR: &str =
    "MalformedPolicyDocument: The policy failed legacy parsing";

/// Whether `op` is an Arn condition operator, in any `ForAllValues:`,
/// `ForAnyValue:` or `IfExists` form.
#[must_use]
pub fn is_arn_condition_operator(op: &ConditionOperator) -> bool {
    matches!(
        base_operator(op),
        ConditionOperator::ArnEquals
            | ConditionOperator::ArnNotEquals
            | ConditionOperator::ArnLike
            | ConditionOperator::ArnNotLike
    )
}

/// Whether the operator name `name`, as written in a policy document, is an
/// Arn condition operator in any `ForAllValues:`, `ForAnyValue:` or `IfExists`
/// form.
#[must_use]
pub fn is_arn_operator_name(name: &str) -> bool {
    let base = name
        .strip_prefix("ForAllValues:")
        .or_else(|| name.strip_prefix("ForAnyValue:"))
        .unwrap_or(name);
    let base = base.strip_suffix("IfExists").unwrap_or(base);
    matches!(
        base,
        "ArnEquals" | "ArnNotEquals" | "ArnLike" | "ArnNotLike"
    )
}

/// Whether the Arn condition policy value `pattern` matches the context ARN
/// `arn`, as the live service matches them.
///
/// Both sides are split on `:` into at most six fields, so the resource field
/// keeps any further `:`. A six-field pattern matches when each of its fields
/// matches the same field of the ARN, with `*` and `?` wildcards in every
/// field and a case-sensitive comparison. A pattern of two to five fields
/// matches only when its first field is exactly `arn`; the fields it has are
/// compared the same way and the fields it lacks do not constrain the ARN, so
/// `arn:aws:iam:*` matches every ARN of the `iam` service in the `aws`
/// partition. A one-field pattern matches nothing, `*` included. The negative
/// operators negate this: `ArnNotEquals` with `*` holds for every ARN.
///
/// Measured twice per probe with a role session whose `aws:PrincipalArn` was
/// `arn:aws:iam::<account>:role/<name>`. Matched: `arn:aws:iam:*`,
/// `arn:aws:iam`, `arn:aws`, `arn:*`, `arn:aws:i*`, `arn:aws:iam:`,
/// `arn:aws:iam::<account>`, `*:*:*:*:*:*`, `arn:*:iam::<account>:role/<name>`.
/// Not matched: `*`, `a*`, `*:*`, `arn`, `arn:aws:ia`, `arn:aws:iam::9641` (a
/// prefix of the account), `arn:aws:iam:us-east-1` (the role ARN's region is
/// empty), `arn:aws:iam::<account>:` (empty resource), `<role ARN>:x` and
/// `arn:aws:dynamodb:*`.
#[must_use]
pub fn arn_condition_match(pattern: &str, arn: &str) -> bool {
    let p_fields: Vec<&str> = pattern.splitn(6, ':').collect();
    let v_fields: Vec<&str> = arn.splitn(6, ':').collect();
    if v_fields.len() != 6 {
        return false;
    }
    let present_fields_match = || {
        p_fields
            .iter()
            .zip(v_fields.iter())
            .all(|(p, v)| wildcard_match(p, v))
    };
    match p_fields.len() {
        1 => false,
        6 => present_fields_match(),
        _ => p_fields[0] == "arn" && present_fields_match(),
    }
}

/// Whether `value` passes the check the live service applies to an Arn
/// condition value when an identity, managed or boundary policy is written
/// (`CreatePolicy`, `PutRolePolicy`; the service calls it legacy parsing).
///
/// The value is split on `:` into at most six fields. It is rejected when the
/// first field does not match `arn` under `*` and `?` wildcards (case
/// sensitive), when the partition or service field is present and empty, or
/// when the service, region or account field holds `/` or the start of a
/// policy variable (`${`). A policy variable's own `:` splits it across
/// fields, so `arn:aws:iam::${aws:PrincipalAccount}:user/alice` puts `${aws`
/// in the account field; a variable in the resource field is accepted.
///
/// Everything else is accepted, including values that are not full ARNs:
/// `*`, `a*`, `*:*`, `arn`, `arn:*`, `arn:aws:iam`, `arn:aws:iam:*`, five
/// fields, an empty region or resource, and a resource holding `:`.
#[must_use]
pub fn is_valid_identity_arn_value(value: &str) -> bool {
    let fields: Vec<&str> = value.splitn(6, ':').collect();
    if !wildcard_match(fields[0], "arn") {
        return false;
    }
    if fields.get(1).is_some_and(|f| f.is_empty()) || fields.get(2).is_some_and(|f| f.is_empty()) {
        return false;
    }
    !fields
        .iter()
        .take(5)
        .skip(2)
        .any(|f| f.contains('/') || f.contains("${"))
}

/// Whether `value` passes the check the live service applies to an Arn
/// condition value in a role trust policy (`CreateRole`,
/// `UpdateAssumeRolePolicy`): it must split on `:` into six fields. What the
/// fields hold is not checked, so an empty partition, which an identity policy
/// rejects, is accepted here.
#[must_use]
pub fn is_valid_trust_arn_value(value: &str) -> bool {
    value.splitn(6, ':').count() == 6
}

/// The message the live service returns for a trust policy Arn condition
/// value that [`is_valid_trust_arn_value`] rejects. The service's text holds
/// `\n` and `\"` as two-character escapes, not as a newline and a bare quote;
/// this returns the same characters.
#[must_use]
pub fn trust_arn_value_message(value: &str) -> String {
    format!("value used for ARN condition is incorrect type: STRING\\nstring: \\\"{value}\\\"\\n")
}

/// Reject an Arn condition value that [`is_valid_identity_arn_value`] rejects.
///
/// `PolicyDocument::from_json` runs this on the identity-policy write path and
/// on the request path. At write the caller gets the service's code and
/// message. On the request path a stored policy holding such a value no longer
/// parses, so the request it governs is denied whether the statement is an
/// Allow or a Deny: the same fail-closed path as a malformed CIDR. Trust
/// policies do not use `from_json`; they follow [`is_valid_trust_arn_value`].
///
/// # Errors
///
/// Returns `PolicyParseError` naming the condition key and the value.
pub(super) fn validate_statement_arn_values(
    statements: &[Statement],
) -> Result<(), PolicyParseError> {
    for stmt in statements {
        for cond in &stmt.conditions {
            if !is_arn_condition_operator(&cond.operator) {
                continue;
            }
            if let Some(v) = cond.values.iter().find(|v| !is_valid_identity_arn_value(v)) {
                return Err(PolicyParseError(format!(
                    "{IDENTITY_ARN_VALUE_ERROR} (Arn condition on {} holds {v:?})",
                    cond.key
                )));
            }
        }
    }
    Ok(())
}

/// Whether `e` is the error `PolicyDocument::from_json` returns for an Arn
/// condition value that [`is_valid_identity_arn_value`] rejects.
#[must_use]
pub fn is_arn_value_error(e: &PolicyParseError) -> bool {
    e.0.starts_with(IDENTITY_ARN_VALUE_ERROR)
}

/// Whether a trust policy statement's parsed conditions hold an Arn condition
/// value that [`is_valid_trust_arn_value`] rejects. The trust evaluator treats
/// such a statement as unparseable, so a Deny blocks and an Allow does not
/// grant.
#[must_use]
pub fn holds_invalid_trust_arn_value(conditions: &[Condition]) -> bool {
    conditions.iter().any(|c| {
        is_arn_condition_operator(&c.operator)
            && c.values.iter().any(|v| !is_valid_trust_arn_value(v))
    })
}

/// The write error for a role trust policy holding an Arn condition value
/// that [`is_valid_trust_arn_value`] rejects, or `None`.
///
/// A number or boolean is read as its JSON text, which never splits into six
/// fields, so it is always rejected. The service rejected `123`, `true` and
/// `1.5` under `ArnEquals` at `UpdateAssumeRolePolicy` (measured twice each on
/// 2026-10-30), with that text in the message.
#[must_use]
pub fn trust_policy_arn_value_error(policy: &Value) -> Option<String> {
    invalid_trust_arn_value(policy)
        .map(|v| format!("MalformedPolicyDocument: {}", trust_arn_value_message(&v)))
}

/// The first Arn condition value in a role trust policy that
/// [`is_valid_trust_arn_value`] rejects, read the way the trust evaluator reads
/// it (a number or boolean as its JSON text), or `None`.
#[must_use]
pub fn invalid_trust_arn_value(policy: &Value) -> Option<String> {
    trust_arn_values(policy)
        .into_iter()
        .find(|v| !is_valid_trust_arn_value(v))
}

/// Every scalar value under an Arn condition operator in a trust policy
/// document, as text: a string as it is, a number or boolean as its JSON text.
fn trust_arn_values(policy: &Value) -> Vec<String> {
    let statements: Vec<&Value> = match &policy["Statement"] {
        Value::Array(a) => a.iter().collect(),
        v @ Value::Object(_) => vec![v],
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for stmt in statements {
        let Some(block) = stmt["Condition"].as_object() else {
            continue;
        };
        for (op, keys) in block {
            let Some(keys) = keys.as_object().filter(|_| is_arn_operator_name(op)) else {
                continue;
            };
            for v in keys.values() {
                let items: Vec<&Value> = match v {
                    Value::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for item in items {
                    match item {
                        Value::String(s) => out.push(s.clone()),
                        Value::Number(n) => out.push(n.to_string()),
                        Value::Bool(b) => out.push(b.to_string()),
                        _ => {}
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests;
