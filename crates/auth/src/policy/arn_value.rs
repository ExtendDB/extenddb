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
/// On the request path a value with a variable reaches this with six fields,
/// or, when it is expanded, with one field if its references hold every `:`
/// it has, as `${x, '}:::::'}` does. Its condition fails closed before it is
/// matched when it has fewer than five written `:`, each `${` ending at the
/// next `}` ([`super::variables::arn_value_lacks_fields`]), and, when it is
/// expanded, when its references as the evaluator reads them leave one to
/// four written `:` ([`super::variables::expanded_arn_value_is_short`]).
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
/// `Allow` holding it does not grant. `expanded` is whether the evaluator
/// expands the value's policy variables, which it does in a `2012-10-17`
/// document.
///
/// That holds for a value that has no meaning as an ARN pattern, and for one
/// whose evaluation on the live service was not measured:
///
/// - a value that holds `${` and has fewer than five `:` written outside its
///   `${...}` references ([`super::variables::arn_value_lacks_fields`]);
/// - when `expanded`, a value that holds `${` and whose expansion has two to
///   five fields ([`super::variables::expanded_arn_value_is_short`]), such
///   as `arn:aws:iam::${aws:PrincipalAccount, '}:'}`;
/// - a value whose first field holds no `${` and does not match `arn` under
///   `*` and `?` wildcards, `""` included, since no ARN starts that way;
/// - a value that [`is_unmeasured_short_pattern`] reports.
///
/// The second kind applies only when `expanded`: a value compared as written
/// splits on every `:`, and when the first kind does not report it, it has at
/// least five `:` and so six fields.
///
/// A first field that holds `${` is left to the matcher. `${x, '}:::::'}`
/// matches nothing in a document of either `Version`: expanded with `x`
/// absent it is the one field `}:::::`, and as written its first field is
/// `${x, '}`. So `ArnNotLike` with it holds, which is what the IAM policy
/// simulator answered for both `Version` values (issue #834).
///
/// An identity policy write rejects each of these, so in an identity policy,
/// boundary or session policy one can only be stored from before the write
/// check. The other values that the write check rejects, such as
/// `arn:aws:iam::${aws:PrincipalAccount}:user/${aws:username}` (a variable in
/// the account field), `${x, '}:::::'}` (a variable in the first field) or an
/// empty partition, keep evaluating as written, so a policy stored before the
/// check loads and decides as it did. The first three kinds can occur in a
/// role trust policy too, whose write check accepts every value of six
/// fields.
#[must_use]
pub fn fails_closed_at_evaluation(value: &str, expanded: bool) -> bool {
    let first = value.split(':').next().unwrap_or_default();
    super::variables::arn_value_lacks_fields(value)
        || (expanded && super::variables::expanded_arn_value_is_short(value))
        || (!first.contains("${") && !wildcard_match(first, "arn"))
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

/// The write error for an Arn condition value on `key` in a role trust
/// policy that [`is_valid_trust_arn_value`] rejects.
///
/// The live service answers `MalformedPolicyDocument` with the message
/// `value used for ARN condition is incorrect type: STRING\nstring:
/// \"<value>\"\n`, where `\n` and `\"` are two-character escapes because
/// the service embeds JSON in a string, and a number or boolean shows as its
/// JSON text. This text is ExtendDB's own; it names the key and the value.
#[must_use]
pub fn trust_arn_value_message(key: &str, value: &str) -> String {
    format!("Arn condition on {key} holds {value:?}, which does not have the six fields of an ARN")
}

/// Reject an Arn condition value that the identity write check of the live
/// service rejects ([`identity_arn_value_problem`]), or that ExtendDB does
/// not accept ([`is_unmeasured_short_pattern`]).
///
/// `PolicyDocument::from_json_for_write` runs this when an identity policy or
/// a permissions boundary is written, and
/// `PolicyDocument::from_json_for_session_policy` runs it when AssumeRole
/// receives a session policy. It does not run on the request path: a policy
/// stored before the check keeps loading, and [`fails_closed_at_evaluation`]
/// decides which of its values fail closed.
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

/// The first Arn condition value in a role trust policy statement's parsed
/// conditions that [`is_valid_trust_arn_value`] rejects, with its condition
/// key, or `None`.
///
/// The trust write check and the trust evaluator's guard call this, and the
/// AssumeRole warning calls [`fail_closed_trust_arn_value`], on conditions
/// read the same way: each number or boolean as its JSON text, then
/// `parse_conditions` (`assume_role::trust_conditions` in the server). The
/// service rejected
/// `123`, `true` and `1.5` under `ArnEquals` at `UpdateAssumeRolePolicy`
/// (measured twice each on 2026-10-30), so a number or boolean, whose text
/// never has six fields, is rejected too. The evaluator treats a statement
/// holding such a value as unparseable, so a Deny blocks and an Allow does
/// not grant.
#[must_use]
pub fn invalid_trust_arn_value(conditions: &[Condition]) -> Option<(&str, &str)> {
    first_arn_value(conditions, |v| !is_valid_trust_arn_value(v))
}

/// The first Arn condition value in a role trust policy statement's parsed
/// conditions that fails the statement closed at evaluation, with its
/// condition key, or `None`: a value [`invalid_trust_arn_value`] reports, or
/// one [`fails_closed_at_evaluation`] reports. `version_2012` is whether the
/// trust policy's `Version` is `2012-10-17`, where the evaluator expands the
/// values of the Arn operators.
///
/// The trust write check accepts a value of the second kind when it has six
/// fields, such as `ARN:aws:iam::123456789012:user/x`, as IAM accepted that
/// shape in a trust policy (2026-10-30), or
/// `arn:aws:iam::${aws:PrincipalAccount, '}:'}`. Such a value fails its
/// statement closed on every AssumeRole call, so the AssumeRole warning names
/// it too.
#[must_use]
pub fn fail_closed_trust_arn_value(
    conditions: &[Condition],
    version_2012: bool,
) -> Option<(&str, &str)> {
    first_arn_value(conditions, |v| {
        !is_valid_trust_arn_value(v) || fails_closed_at_evaluation(v, version_2012)
    })
}

/// The first value under an Arn condition operator in `conditions` for which
/// `pred` holds, with its condition key.
fn first_arn_value(conditions: &[Condition], pred: impl Fn(&str) -> bool) -> Option<(&str, &str)> {
    conditions
        .iter()
        .filter(|c| is_arn_condition_operator(&c.operator))
        .find_map(|c| {
            c.values
                .iter()
                .find(|v| pred(v))
                .map(|v| (c.key.as_str(), v.as_str()))
        })
}

#[cfg(test)]
mod tests;
