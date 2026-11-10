// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy values of the Arn condition operators.
//!
//! `ArnEquals`, `ArnLike`, `ArnNotEquals` and `ArnNotLike`, with their
//! `ForAllValues:`, `ForAnyValue:` and `IfExists` forms, compare a context ARN
//! with a policy value. This module holds how the live service matches such a
//! value against an ARN, and the checks it applies to the value when a policy
//! is written. Identity policies and role trust policies are checked
//! differently. The rules come from measurements on live IAM on 2026-10-30,
//! whose rows are in the description of the pull request for issue #665. One
//! evaluation question was not measured, and [`arn_condition_match_tokens`]
//! names it.

use serde_json::Value;

use super::document::{Condition, ConditionOperator, PolicyParseError, Statement, base_operator};
use super::matcher::wildcard_match;
use super::variables::{Token, match_tokens, split_fields};

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

/// The first field a pattern of two to five fields needs to match anything:
/// the text `arn`.
const ARN_FIELD: [Token; 3] = [Token::Unit(0x61), Token::Unit(0x72), Token::Unit(0x6E)];

/// Whether the Arn condition policy value `pattern`, as written, matches the
/// context ARN `arn`, as the live service matches them: every `*` and `?` in
/// `pattern` is a wildcard and every `:` a field separator. The rules are
/// those of [`arn_condition_match_tokens`].
#[must_use]
pub fn arn_condition_match(pattern: &str, arn: &str) -> bool {
    let tokens: Vec<Token> = pattern.encode_utf16().map(Token::written).collect();
    arn_condition_match_tokens(&tokens, arn)
}

/// Whether the Arn condition policy value `pattern`, in the tokens of
/// [`super::variables::ConditionValue`], matches the context ARN `arn`, as the
/// live service matches them.
///
/// Only a `*` or `?` the policy wrote is a wildcard, and only a `:` it wrote
/// separates fields, so text that came from a variable, a default or an
/// escape is matched as characters of its field (issue #534, PR #541). The
/// pattern is split at its first five written `:` and the ARN at its first
/// five `:`, so the resource field keeps any further `:`. A six-field pattern
/// matches when each of its fields matches the same field of the ARN, with
/// the wildcards matching in every field and a case-sensitive comparison. A
/// pattern of two to five fields matches only when its first field is
/// exactly `arn`; the fields it has are compared the same way and the fields
/// it lacks do not constrain the ARN, so `arn:aws:iam:*` matches every ARN of
/// the `iam` service in the `aws` partition. A one-field pattern matches
/// nothing, `*` included. The negative operators negate this: `ArnNotEquals`
/// with `*` holds for every ARN.
///
/// Measured twice per probe with a role session whose `aws:PrincipalArn` was
/// `arn:aws:iam::<account>:role/<name>`. Matched: `arn:aws:iam:*`,
/// `arn:aws:iam`, `arn:aws`, `arn:*`, `arn:aws:i*`, `arn:aws:iam:`,
/// `arn:aws:iam::<account>`, `*:*:*:*:*:*`, `arn:*:iam::<account>:role/<name>`.
/// Not matched: `*`, `a*`, `*:*`, `arn`, `arn:aws:ia`, `arn:aws:iam::9641` (a
/// prefix of the account), `arn:aws:iam:us-east-1` (the role ARN's region is
/// empty), `arn:aws:iam::<account>:` (empty resource), `<role ARN>:x` and
/// `arn:aws:dynamodb:*`.
///
/// On the request path a value with a variable reaches this only with six
/// fields: a value that holds `${` and has fewer than five written `:` fails
/// its condition closed before it is matched
/// ([`super::variables::arn_value_lacks_fields`]).
///
/// Not measured: how the service evaluates a pattern of two to five fields
/// whose first field holds a wildcard, such as `*:aws:iam:*` or `ar?:aws:iam`.
/// Identity policy writes accept such a pattern (measured), and this matcher
/// treats it as matching nothing. The one measured pattern of that kind, `*:*`,
/// matched nothing.
#[must_use]
pub fn arn_condition_match_tokens(pattern: &[Token], arn: &str) -> bool {
    let v_fields: Vec<&str> = arn.splitn(6, ':').collect();
    if v_fields.len() != 6 {
        return false;
    }
    let p_fields = split_fields(pattern);
    let present_fields_match = || {
        p_fields.iter().zip(&v_fields).all(|(p, v)| {
            let units: Vec<u16> = v.encode_utf16().collect();
            match_tokens(p, &units)
        })
    };
    match p_fields.len() {
        1 => false,
        6 => present_fields_match(),
        _ => p_fields[0] == &ARN_FIELD[..] && present_fields_match(),
    }
}

/// Why the check the live service applies to an Arn condition value when an
/// identity policy or a permissions boundary is written rejects `value`, or
/// `None` when it accepts it (measured with `CreatePolicy` and
/// `PutRolePolicy`). The service answers `MalformedPolicyDocument` with the
/// message "The policy failed legacy parsing"; the text returned here is
/// ExtendDB's own and names the field.
///
/// The value is split on `:` into at most six fields. It is rejected when the
/// first field does not match `arn` under `*` and `?` wildcards (case
/// sensitive), when the partition or service field is present and empty,
/// when the partition, service, region or account field holds `/` or the
/// start of a policy variable (`${`), or when the service field holds an
/// upper-case ASCII letter. A policy variable's own `:` splits it across
/// fields, so `arn:aws:iam::${aws:PrincipalAccount}:user/alice` puts `${aws`
/// in the account field; a variable in the resource field is accepted.
///
/// Everything else is accepted, including values that are not full ARNs:
/// `*`, `a*`, `*:*`, `arn`, `arn:*`, `arn:aws:iam`, `arn:aws:iam:*`, five
/// fields, an empty region or resource, and a resource holding `:`.
///
/// Each rejection above was measured, twice per value, among them `arn:`,
/// `arn::` and `arn:aws:` (an empty partition or service in a value of fewer
/// than six fields), `${` in the service and in the region field, and values
/// under the `ForAllValues:` operator forms. The partition and upper-case
/// rows were measured at `PutRolePolicy` on 2026-10-30 (the 21:13 round)
/// with `arn:aws/x:iam::<account>:root`, `arn:${aws:username}:iam::<account>:root`
/// and `arn:aws:IAM::<account>:root`; a service field with another mix of
/// cases was not sent. A whole-value variable such as `${aws:PrincipalArn}`
/// is rejected too, because its first field is `${aws`.
///
/// Every value that holds `${` and has fewer than five `:` written outside
/// its `${...}` references ([`super::variables::arn_value_lacks_fields`]) is
/// rejected here: an accepted value that holds `${` has five `:` before its
/// first `${`, and those are written ones. So on every value this accepts,
/// counting the fields on every `:` and counting them on the written `:`
/// give six fields or the same number.
#[must_use]
pub fn identity_arn_value_problem(value: &str) -> Option<String> {
    const NAMES: [&str; 5] = ["first", "partition", "service", "region", "account"];
    let fields: Vec<&str> = value.splitn(6, ':').collect();
    if !wildcard_match(fields[0], "arn") {
        return Some("its first field does not match arn".to_owned());
    }
    for i in [1, 2] {
        if fields.get(i).is_some_and(|f| f.is_empty()) {
            return Some(format!("its {} field is empty", NAMES[i]));
        }
    }
    for (name, field) in NAMES.iter().zip(&fields).skip(1) {
        if field.contains('/') {
            return Some(format!("its {name} field holds /"));
        }
        if field.contains("${") {
            return Some(format!("its {name} field holds a policy variable"));
        }
    }
    if fields
        .get(2)
        .is_some_and(|f| f.bytes().any(|b| b.is_ascii_uppercase()))
    {
        return Some("its service field holds an upper-case letter".to_owned());
    }
    None
}

/// Whether `value` passes the identity write check of the live service
/// ([`identity_arn_value_problem`]).
#[must_use]
pub fn is_valid_identity_arn_value(value: &str) -> bool {
    identity_arn_value_problem(value).is_none()
}

/// Whether `value` is a pattern of two to five `:`-separated fields whose
/// first field holds a `*` or `?`, other than `*:*`.
///
/// Identity policy writes on the live service accept such a pattern
/// (measured: `*:aws:iam:*`, `a*:aws:iam:*`, `ar?:aws:iam` and
/// `*:aws:iam::<account>`), but how the service evaluates one was not
/// measured. Neither answer is safe to assume: no match would let a `Deny` on
/// `ArnLike` that the service applies stop applying, and a match would let an
/// `Allow` on `ArnNotLike` that the service grants stop granting, or the
/// reverse for the other operator. So ExtendDB refuses such a value at write,
/// a deviation from the service, and a stored one fails its condition closed
/// ([`fails_closed_at_evaluation`]). `*:*` was measured to match nothing, and
/// it is accepted and evaluated so.
#[must_use]
pub fn is_unmeasured_short_pattern(value: &str) -> bool {
    let fields: Vec<&str> = value.splitn(6, ':').collect();
    (2..=5).contains(&fields.len()) && fields[0].contains(['*', '?']) && value != "*:*"
}

/// Whether the Arn condition value `value` makes its condition fail closed at
/// evaluation, whatever the request: a `Deny` holding it applies and an
/// `Allow` holding it does not grant.
///
/// That holds for a value that has no meaning as an ARN pattern, and for one
/// whose evaluation on the live service was not measured:
///
/// - a value that holds `${` and has fewer than five `:` written outside its
///   `${...}` references ([`super::variables::arn_value_lacks_fields`]);
/// - a value whose first field does not match `arn` under `*` and `?`
///   wildcards, `""` included, since no ARN starts that way;
/// - a value that [`is_unmeasured_short_pattern`] reports.
///
/// An identity policy write rejects each of these, so in an identity policy,
/// boundary or session policy one can only be stored from before the write
/// check. The other values that the write check rejects, such as
/// `arn:aws:iam::${aws:PrincipalAccount}:user/${aws:username}` (a variable in
/// the account field) or an empty partition, keep evaluating as written, so a
/// policy stored before the check loads and decides as it did. The first two
/// kinds can occur in a role trust policy too, whose write check accepts
/// every value of six fields.
#[must_use]
pub fn fails_closed_at_evaluation(value: &str) -> bool {
    super::variables::arn_value_lacks_fields(value)
        || !wildcard_match(value.split(':').next().unwrap_or_default(), "arn")
        || is_unmeasured_short_pattern(value)
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

/// Reject an Arn condition value that the identity write check of the live
/// service rejects ([`identity_arn_value_problem`]), or that ExtendDB does
/// not accept ([`is_unmeasured_short_pattern`]).
///
/// `PolicyDocument::from_json_for_write` runs this, so it applies when an
/// identity policy, a permissions boundary or a session policy is written and
/// not on the request path: a policy stored before the check keeps loading,
/// and [`fails_closed_at_evaluation`] decides which of its values fail closed.
/// Trust policies do not use this; they follow [`is_valid_trust_arn_value`].
///
/// # Errors
///
/// Returns `PolicyParseError` naming the condition key, the value and what is
/// wrong with it.
pub(super) fn validate_statement_arn_values(
    statements: &[Statement],
) -> Result<(), PolicyParseError> {
    for stmt in statements {
        for cond in &stmt.conditions {
            if !is_arn_condition_operator(&cond.operator) {
                continue;
            }
            for v in &cond.values {
                let problem = identity_arn_value_problem(v).or_else(|| {
                    is_unmeasured_short_pattern(v).then(|| {
                        "ExtendDB does not accept a value of two to five fields whose first \
                         field holds a wildcard"
                            .to_owned()
                    })
                });
                if let Some(problem) = problem {
                    return Err(PolicyParseError(format!(
                        "Arn condition on {} holds {v:?}: {problem}",
                        cond.key
                    )));
                }
            }
        }
    }
    Ok(())
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
