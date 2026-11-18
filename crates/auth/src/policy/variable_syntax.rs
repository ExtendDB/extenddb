// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Write-time check for a policy variable reference IAM refuses.
//!
//! IAM refuses at write a policy in which a `${...}` reference in a
//! `Resource` or `NotResource` pattern or a condition value is not closed or
//! is not well formed. The grammar is in `variables::reference`: a
//! `${` takes the next character that is not Java whitespace, then each `'`
//! opens or closes a quoted span and the first `}` outside one closes the
//! reference; the text is an escape, a key name, or a key name, a `,` and one
//! quoted default. So `a${}}`, `a${aws:username, '}x'}`,
//! `a${${aws:username}}` and `a${aws:username, 'x''y'}` are accepted, and
//! `a${`, `a${}`, `a$${`, `a${aws:username, '}'`, `a${'x'}`, `a${a,b}` and
//! `a${aws:username, 'x' 'y'}` are refused. Condition keys are not checked.
//! Measured on IAM on 2026-11-09, two samples each, on `PutUserPolicy`,
//! `PutGroupPolicy`, `PutRolePolicy`, `CreatePolicy` (`2012-10-17` only),
//! `CreateRole` and `UpdateAssumeRolePolicy`.
//!
//! The error is `MalformedPolicyDocument` with a message that depends on the
//! write path and the document `Version`:
//!
//! - Identity policy, `2012-10-17`: `Syntax errors in policy.`, for a
//!   `Resource` or `NotResource` pattern or a condition value under any
//!   operator. The check runs after the size limit and the JSON parse and
//!   before every other write check.
//! - Identity policy, `2008-10-17`: `The policy failed legacy parsing` when
//!   any value of the document fails IAM's legacy parsing (a numeric or date
//!   value, an Arn value whose first `$` lies before its resource field, a
//!   pattern whose first `$` lies in its service, region or account field),
//!   before the `Sid` check. Otherwise, after the `Sid` check, `Resource
//!   <pattern> must be in ARN format or "*".` for a pattern with a `${` that
//!   no later `}` closes, quoted or not, `Partition "<field>" is not valid
//!   for resource "<pattern>".` for a pattern whose first `$` lies in its
//!   partition field, and `Variable declaration is not well-formed.` for any
//!   other pattern or condition value. Every write path
//!   refuses an identity document with no `Version` before these checks.
//! - Trust policy, `2012-10-17`: `Policy contains one or more invalid
//!   variable declarations`, after the structural checks (`Effect`, operator
//!   names, a `Resource` field, a missing `Action` or `Principal`, `Action`
//!   with `NotAction`) and before the `Sid`, `NotPrincipal`, `NotResource`,
//!   principal value and action value checks.
//! - Trust policy, `2008-10-17` or no `Version`: an Arn operator value is
//!   refused with `value used for ARN condition is incorrect type`, after the
//!   structural, `NotPrincipal` and `Sid` checks and before the
//!   `NotResource`, principal value and action value checks. This check closes a reference at the
//!   first `}` whether or not it is quoted. Other values are stored and
//!   compared as written.
//!
//! The checks run at write only. A stored document keeps parsing and its
//! value is compared as written, as before.

use super::document::{
    ConditionOperator, PolicyDocument, PolicyParseError, ResourceMatch, base_operator,
};
use serde_json::Value;

/// IAM's message for an identity policy with `Version` `2012-10-17`.
pub const SYNTAX_ERROR: &str = "MalformedPolicyDocument: Syntax errors in policy.";

/// IAM's message for a condition value in an identity policy with `Version`
/// `2008-10-17`.
pub const VARIABLE_NOT_WELL_FORMED: &str =
    "MalformedPolicyDocument: Variable declaration is not well-formed.";

/// IAM's message for a value that fails legacy parsing in an identity policy
/// with `Version` `2008-10-17`.
pub const LEGACY_PARSING: &str = "MalformedPolicyDocument: The policy failed legacy parsing";

/// IAM's message for a trust policy with `Version` `2012-10-17`.
pub const TRUST_INVALID_VARIABLES: &str =
    "MalformedPolicyDocument: Policy contains one or more invalid variable declarations";

pub use super::variables::reference::has_invalid_reference;

/// Whether `value` holds a `${` that no later `}` closes, quoted or not.
fn has_unclosed_brace(value: &str) -> bool {
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return true;
        };
        rest = &after[end + 1..];
    }
    false
}

/// The string values of a JSON scalar or array.
fn strings(value: &Value) -> Vec<&str> {
    match value {
        Value::String(s) => vec![s.as_str()],
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// The statements of a raw document, as an object or an array.
fn raw_statements(raw: &Value) -> Vec<&Value> {
    match raw.get("Statement") {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(stmt @ Value::Object(_)) => vec![stmt],
        _ => Vec::new(),
    }
}

fn is_version_2012(raw: &Value) -> bool {
    raw.get("Version").and_then(Value::as_str) == Some("2012-10-17")
}

/// Every `(operator name, key, values)` entry of the raw statement's
/// `Condition` block.
fn raw_conditions(stmt: &Value) -> Vec<(&str, &str, &Value)> {
    let mut out = Vec::new();
    if let Some(Value::Object(ops)) = stmt.get("Condition") {
        for (op, keys) in ops {
            if let Value::Object(keys) = keys {
                for (key, values) in keys {
                    out.push((op.as_str(), key.as_str(), values));
                }
            }
        }
    }
    out
}

/// The identity check for `Version` `2012-10-17`, run on the raw document
/// before it is parsed so it wins over every check except the size limit and
/// the JSON parse.
pub(super) fn validate_identity_before_parse(
    json: &str,
    max_bytes: usize,
) -> Result<(), PolicyParseError> {
    if json.len() > max_bytes {
        return Ok(());
    }
    let Ok(raw) = serde_json::from_str::<Value>(json) else {
        return Ok(());
    };
    if !is_version_2012(&raw) {
        return Ok(());
    }
    for stmt in raw_statements(&raw) {
        for field in ["Resource", "NotResource"] {
            if let Some(v) = stmt
                .get(field)
                .map(strings)
                .and_then(|vs| vs.into_iter().find(|v| has_invalid_reference(v)))
            {
                return Err(PolicyParseError(format!(
                    "{SYNTAX_ERROR} ({field} holds {v:?})"
                )));
            }
        }
        for (_, key, values) in raw_conditions(stmt) {
            if let Some(v) = strings(values)
                .into_iter()
                .find(|v| has_invalid_reference(v))
            {
                return Err(PolicyParseError(format!(
                    "{SYNTAX_ERROR} (condition on {key} holds {v:?})"
                )));
            }
        }
    }
    Ok(())
}

/// The number of `:` in `value` before its first `$`, which is the index of
/// the ARN field that `$` lies in.
fn field_of_first_dollar(value: &str) -> usize {
    let end = value.find('$').unwrap_or(value.len());
    value[..end].matches(':').count()
}

fn is_numeric_or_date(op: &ConditionOperator) -> bool {
    use ConditionOperator as Op;
    matches!(
        op,
        Op::NumericEquals
            | Op::NumericNotEquals
            | Op::NumericLessThan
            | Op::NumericLessThanEquals
            | Op::NumericGreaterThan
            | Op::NumericGreaterThanEquals
            | Op::DateEquals
            | Op::DateNotEquals
            | Op::DateLessThan
            | Op::DateLessThanEquals
            | Op::DateGreaterThan
            | Op::DateGreaterThanEquals
    )
}

fn is_arn(op: &ConditionOperator) -> bool {
    use ConditionOperator as Op;
    matches!(
        op,
        Op::ArnEquals | Op::ArnNotEquals | Op::ArnLike | Op::ArnNotLike
    )
}

fn patterns(doc: &PolicyDocument) -> impl Iterator<Item = &String> {
    doc.statements.iter().flat_map(|stmt| {
        let (ResourceMatch::Resources(p) | ResourceMatch::NotResources(p)) = &stmt.resource_match;
        p.iter()
    })
}

/// The `2008-10-17` identity check for a value that fails IAM's legacy
/// parsing, run before the `Sid` check: anywhere in the document it decides
/// the message. A pattern is checked here only when its first `$` lies in the
/// service, region or account field (fields 2 to 4); a pattern whose first
/// `$` lies elsewhere gets the Resource message from
/// [`validate_identity_after_sid`].
pub(super) fn validate_identity_legacy(doc: &PolicyDocument) -> Result<(), PolicyParseError> {
    if doc.expands_policy_variables() {
        return Ok(());
    }
    for p in patterns(doc) {
        if has_invalid_reference(p) && (2..=4).contains(&field_of_first_dollar(p)) {
            return Err(PolicyParseError(format!(
                "{LEGACY_PARSING} (Resource holds {p:?})"
            )));
        }
    }
    for cond in doc.statements.iter().flat_map(|s| &s.conditions) {
        let op = base_operator(&cond.operator);
        for v in cond.values.iter().filter(|v| has_invalid_reference(v)) {
            if is_numeric_or_date(op) || (is_arn(op) && field_of_first_dollar(v) < 5) {
                return Err(PolicyParseError(format!(
                    "{LEGACY_PARSING} (condition on {} holds {v:?})",
                    cond.key
                )));
            }
        }
    }
    Ok(())
}

/// The `2008-10-17` identity check run after the `Sid` check, for the values
/// [`validate_identity_legacy`] leaves.
pub(super) fn validate_identity_after_sid(doc: &PolicyDocument) -> Result<(), PolicyParseError> {
    if doc.expands_policy_variables() {
        return Ok(());
    }
    for stmt in &doc.statements {
        let (field, p) = match &stmt.resource_match {
            ResourceMatch::Resources(p) => ("Resource", p),
            ResourceMatch::NotResources(p) => ("NotResource", p),
        };
        if let Some(p) = p.iter().find(|p| has_unclosed_brace(p)) {
            return Err(PolicyParseError(format!(
                "MalformedPolicyDocument: Resource {p} must be in ARN format or \"*\"."
            )));
        }
        if let Some(p) = p.iter().find(|p| has_invalid_reference(p)) {
            if field_of_first_dollar(p) == 1 {
                let partition = p.split(':').nth(1).unwrap_or_default();
                return Err(PolicyParseError(format!(
                    "MalformedPolicyDocument: Partition \"{partition}\" is not valid for resource \"{p}\"."
                )));
            }
            return Err(PolicyParseError(format!(
                "{VARIABLE_NOT_WELL_FORMED} ({field} holds {p:?})"
            )));
        }
        for cond in &stmt.conditions {
            if let Some(v) = cond.values.iter().find(|v| has_invalid_reference(v)) {
                return Err(PolicyParseError(format!(
                    "{VARIABLE_NOT_WELL_FORMED} (condition on {} holds {v:?})",
                    cond.key
                )));
            }
        }
    }
    Ok(())
}

/// The trust check for `Version` `2012-10-17`: the first condition value with
/// a reference IAM refuses, as IAM's message. `None` for another `Version`.
#[must_use]
pub fn trust_variable_error(trust_policy: &Value) -> Option<String> {
    if !is_version_2012(trust_policy) {
        return None;
    }
    raw_statements(trust_policy)
        .into_iter()
        .flat_map(raw_conditions)
        .find_map(|(_, key, values)| {
            strings(values)
                .into_iter()
                .find(|v| has_invalid_reference(v))
                .map(|v| format!("{TRUST_INVALID_VARIABLES} (condition on {key} holds {v:?})"))
        })
}

/// Whether `op_name` is an Arn operator, in any `ForAllValues:`,
/// `ForAnyValue:` or `IfExists` form.
fn is_arn_operator_name(op_name: &str) -> bool {
    let base = op_name
        .strip_prefix("ForAllValues:")
        .or_else(|| op_name.strip_prefix("ForAnyValue:"))
        .unwrap_or(op_name);
    let base = base.strip_suffix("IfExists").unwrap_or(base);
    matches!(
        base,
        "ArnEquals" | "ArnLike" | "ArnNotEquals" | "ArnNotLike"
    )
}

/// IAM's text for `value` inside the Arn type message. The value is escaped
/// twice. The first pass writes `\`, `"`, `'`, tab, newline and carriage
/// return with a backslash, and every other byte outside U+0020 to U+007E of
/// the UTF-8 text as a backslash and three octal digits. The second pass
/// writes `\`, `"`, `'` and `/` with a backslash, and a `${` as `\${`.
/// Measured on `UpdateAssumeRolePolicy` and `CreateRole` on 2026-11-09 with
/// `"`, `\`, `'`, tab, U+0001 and U+00E9 in the value, and with `a$${` and
/// `a$b${`; newline, carriage return and U+007F were not measured.
fn trust_arn_message(value: &str) -> String {
    let mut once = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'\\' => once.push_str("\\\\"),
            b'"' => once.push_str("\\\""),
            b'\'' => once.push_str("\\'"),
            b'\t' => once.push_str("\\t"),
            b'\n' => once.push_str("\\n"),
            b'\r' => once.push_str("\\r"),
            0x20..=0x7e => once.push(char::from(b)),
            _ => once.push_str(&format!("\\{b:03o}")),
        }
    }
    let mut twice = String::with_capacity(once.len() * 2);
    let mut rest = once.as_str();
    while let Some(c) = rest.chars().next() {
        if rest.starts_with("${") {
            twice.push_str("\\${");
            rest = &rest[2..];
            continue;
        }
        if matches!(c, '\\' | '"' | '\'' | '/') {
            twice.push('\\');
        }
        twice.push(c);
        rest = &rest[c.len_utf8()..];
    }
    format!(
        "MalformedPolicyDocument: value used for ARN condition is incorrect type: STRING\\nstring: \\\"{twice}\\\"\\n"
    )
}

/// The trust check for `Version` `2008-10-17` or none: the first Arn operator
/// value with a `${` that no later `}` closes, quoted or not, as IAM's
/// message. `None` for a `2012-10-17` document, which [`trust_variable_error`]
/// covers.
#[must_use]
pub fn trust_arn_value_error(trust_policy: &Value) -> Option<String> {
    if is_version_2012(trust_policy) {
        return None;
    }
    raw_statements(trust_policy)
        .into_iter()
        .flat_map(raw_conditions)
        .filter(|(op, _, _)| is_arn_operator_name(op))
        .find_map(|(_, _, values)| {
            strings(values)
                .into_iter()
                .find(|v| has_unclosed_brace(v))
                .map(trust_arn_message)
        })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod trust_tests;
