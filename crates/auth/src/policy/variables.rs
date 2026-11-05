// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy variable expansion.
//!
//! A policy may write `${key}` in a `Resource` or `NotResource` pattern or in
//! a condition value, and IAM replaces it with the request's value for `key`
//! before matching. The forms are:
//!
//! - `${key}`: the value of `key` in the request context. A multivalued key
//!   is never a variable: when the request carries it, with one value or
//!   several, the pattern matches nothing, default or not; a written default
//!   is used only when the request does not carry the key (measured with
//!   `dynamodb:LeadingKeys` holding one value and two, in `Resource` and in a
//!   condition value, PR #535 body table; and with `aws:TagKeys` present and
//!   absent against `${aws:TagKeys, 'x'}` on 2026-10-28).
//! - `${key, 'default'}`: the value of `key`, or `default` when the request
//!   does not carry `key`.
//! - `${*}`, `${?}`, `${$}`: a literal `*`, `?` or `$`.
//!
//! In a `Resource` or `NotResource` pattern the text written in the policy
//! keeps `*` and `?` as wildcards, while text that comes from a variable
//! value, a default or an escape is matched literally, so a tag value or an
//! escaped `*` cannot widen a pattern. A variable whose key the request does
//! not carry, with no default, makes the pattern match nothing. Both rules
//! were measured with the IAM policy simulator (issue #533). A variable whose
//! key the service populates but the context cannot resolve (a key listed in
//! `context::FAIL_CLOSED_GLOBAL_KEYS`) leaves the match unknown, and the
//! evaluator fails closed on it as it does on such a key in a condition.
//! Only a document whose `Version` is `2012-10-17` expands variables; the
//! caller decides that with [`PolicyDocument::expands_policy_variables`].
//!
//! ARN matching here follows the same field rule as
//! [`super::matcher::arn_match`]: the first five `:` separate fields, the
//! resource field keeps its own `:` and `/`, and both sides need the same
//! number of fields. Only a `:` written in the policy text separates fields;
//! a `:` inside a variable value is matched as a character of its field
//! (measured: a region-field value `us-east-1:123456789012` matches no table
//! ARN). `*` and `?` count UTF-16 code units, as in
//! [`super::matcher::wildcard_match`] (#581).

use super::context::ConditionContext;
use super::document::{PolicyDocument, PolicyParseError};
use super::matcher::arn_match;
use serde_json::Value;

impl PolicyDocument {
    /// Whether `${key}` policy variables in `Resource` and `NotResource`
    /// patterns are expanded.
    ///
    /// IAM expands them only in a `2012-10-17` document; a `2008-10-17`
    /// document, or one with no `Version`, matches the pattern as written.
    /// Measured with the IAM policy simulator (issue #533).
    #[must_use]
    pub fn expands_policy_variables(&self) -> bool {
        self.version.as_deref() == Some("2012-10-17")
    }
}

/// Refuse a `Version` that is not the string `2012-10-17` or `2008-10-17`,
/// the two values of IAM's policy grammar.
///
/// The value decides whether `Resource` and `NotResource` variables expand,
/// so a mistyped `Version` would otherwise turn that off for the whole
/// document with no error.
/// `PolicyDocument::from_json_for_write` calls this, so a stored document with
/// another value keeps parsing. A document with no `Version` passes here,
/// because each write path refuses it first. Whether IAM refuses such a
/// document at write, and with what message, was not measured.
pub(super) fn validate_version(json: &str) -> Result<(), PolicyParseError> {
    let raw: Value = serde_json::from_str(json).map_err(|e| PolicyParseError(e.to_string()))?;
    match raw.get("Version") {
        None => Ok(()),
        Some(Value::String(v)) if v == "2012-10-17" || v == "2008-10-17" => Ok(()),
        Some(other) => Err(PolicyParseError(format!(
            "invalid Version (must be the string 2012-10-17 or 2008-10-17): {other}"
        ))),
    }
}

/// `*` in UTF-16.
const STAR: u16 = 0x2A;
/// `?` in UTF-16.
const QUESTION: u16 = 0x3F;
/// `:` in UTF-16.
const COLON: u16 = 0x3A;

/// One unit of an expanded pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    /// `*` written in the policy text: matches zero or more UTF-16 code units.
    Star,
    /// `?` written in the policy text: matches exactly one UTF-16 code unit.
    Question,
    /// `:` written in the policy text: a field separator among the first five,
    /// a literal `:` in the resource field.
    Colon,
    /// One UTF-16 code unit matched literally.
    Unit(u16),
}

impl Token {
    /// The token for a UTF-16 code unit of the policy text, where `*`, `?`
    /// and `:` keep their meaning.
    fn written(unit: u16) -> Self {
        match unit {
            STAR => Self::Star,
            QUESTION => Self::Question,
            COLON => Self::Colon,
            other => Self::Unit(other),
        }
    }
}

/// A `${...}` reference as written in a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reference<'a> {
    /// `${key}` or `${key, 'default'}`.
    Key {
        name: &'a str,
        default: Option<&'a str>,
    },
    /// `${*}`, `${?}` or `${$}`.
    Escape(u8),
}

/// Trim the code points up to U+0020 from both ends of `text`, the set Java's
/// `String.trim` removes.
///
/// IAM trims this set around a variable's key name and its quoted default,
/// in both forms and in the escapes (measured with the IAM policy simulator:
/// space, tab, newline, U+000B, U+000C, U+000D, U+0001 and U+001F are
/// trimmed; U+0085, U+00A0 and U+3000 are not, so a key name written with
/// one of them names a key the request does not carry).
fn trim_up_to_space(text: &str) -> &str {
    text.trim_matches(|c: char| c <= ' ')
}

/// Parse the text between `${` and `}`.
///
/// The text is trimmed with [`trim_up_to_space`] first. Then `*`, `?` and
/// `$` are the escapes, and `name, 'default'` is the default form, where the
/// name and the quoted default are trimmed again and the text inside the
/// quotes is kept as written. Anything else, including a `,` whose right
/// side is not single-quoted, is taken as the key name.
fn parse_reference(inner: &str) -> Reference<'_> {
    let inner = trim_up_to_space(inner);
    match inner {
        "*" => return Reference::Escape(b'*'),
        "?" => return Reference::Escape(b'?'),
        "$" => return Reference::Escape(b'$'),
        _ => {}
    }
    if let Some((name, default)) = inner.split_once(',') {
        let default = trim_up_to_space(default);
        if default.len() >= 2 && default.starts_with('\'') && default.ends_with('\'') {
            return Reference::Key {
                name: trim_up_to_space(name),
                default: Some(&default[1..default.len() - 1]),
            };
        }
    }
    Reference::Key {
        name: inner,
        default: None,
    }
}

/// One piece of a policy string: plain text, or a `${...}` reference with its
/// raw text including the braces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part<'a> {
    Text(&'a str),
    Ref(&'a str, Reference<'a>),
}

/// Split `value` into plain text and `${...}` references. A `${` with no
/// closing `}` is plain text, as before.
///
/// With `iam_syntax` the text between the braces is read by
/// [`parse_reference`]. Without it the whole text, as written, is the key
/// name, which is how a condition value was read before the default form:
/// nothing is trimmed, and `${key, 'default'}` names the key
/// `key, 'default'`, which no request carries.
fn parts(value: &str, iam_syntax: bool) -> Vec<Part<'_>> {
    let mut out = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push(Part::Text(&rest[..start]));
        let after_start = &rest[start + 2..];
        match after_start.find('}') {
            Some(end) => {
                let raw = &rest[start..start + 3 + end];
                let inner = &after_start[..end];
                let reference = if iam_syntax {
                    parse_reference(inner)
                } else {
                    Reference::Key {
                        name: inner,
                        default: None,
                    }
                };
                out.push(Part::Ref(raw, reference));
                rest = &after_start[end + 1..];
            }
            None => {
                out.push(Part::Text(&rest[start..]));
                rest = "";
            }
        }
    }
    out.push(Part::Text(rest));
    out
}

/// Why a variable has no text to substitute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Missing {
    /// The request does not carry the key and no default was written.
    Absent,
    /// The service populates the key but the context cannot resolve it
    /// (`ConditionContext::is_unresolvable_key`). A default is not used,
    /// because the service would have a value.
    Unresolvable,
}

/// The request's value for `key`: the value of a present single-valued key,
/// or the default when the key is absent and a default was written. A present
/// key with no values expands to the empty string. A multivalued key is never
/// a value: when the request carries it the pattern matches nothing, default
/// or not, and when it is absent a written default is used (IAM policy
/// simulator, 2026-10-28, `aws:TagKeys` as `[x, y]`, `[x]` and absent against
/// `table/${aws:TagKeys, 'x'}`: implicitDeny, implicitDeny, allowed). A key
/// the request does not carry and the context reports unresolvable is
/// [`Missing::Unresolvable`] before either rule, multivalued or not, so its
/// default is not used.
fn resolve<'a>(
    name: &str,
    default: Option<&'a str>,
    context: &'a impl ConditionContext,
) -> Result<&'a str, Missing> {
    let values = context.resolve_key(name);
    if values.is_none() && context.is_unresolvable_key(name) {
        return Err(Missing::Unresolvable);
    }
    if context.is_multivalued_key(name) {
        return match values {
            Some(_) => Err(Missing::Absent),
            None => default.ok_or(Missing::Absent),
        };
    }
    match values {
        Some(values) => Ok(values.first().copied().unwrap_or("")),
        None => default.ok_or(Missing::Absent),
    }
}

/// Expand `pattern` into tokens. Fails with [`Missing::Absent`] when a
/// variable names a key the request does not carry and gives no default,
/// since that alone makes the pattern match nothing, and otherwise with
/// [`Missing::Unresolvable`] when a variable names a key the context cannot
/// resolve.
fn expand_tokens(pattern: &str, context: &impl ConditionContext) -> Result<Vec<Token>, Missing> {
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut unresolvable = false;
    for part in parts(pattern, true) {
        match part {
            Part::Text(text) => tokens.extend(text.encode_utf16().map(Token::written)),
            Part::Ref(_, Reference::Escape(byte)) => tokens.push(Token::Unit(u16::from(byte))),
            Part::Ref(_, Reference::Key { name, default }) => {
                match resolve(name, default, context) {
                    Ok(value) => tokens.extend(value.encode_utf16().map(Token::Unit)),
                    Err(Missing::Unresolvable) => unresolvable = true,
                    Err(Missing::Absent) => return Err(Missing::Absent),
                }
            }
        }
    }
    if unresolvable {
        Err(Missing::Unresolvable)
    } else {
        Ok(tokens)
    }
}

/// Whether the `Resource` or `NotResource` pattern `pattern` matches
/// `resource_arn`.
///
/// With `expand` false, or when `pattern` has no `${`, this is exactly
/// [`arn_match`]. Otherwise variables are expanded first, and a pattern with
/// an unresolved variable matches nothing, as does one that
/// [`resource_pattern_is_unresolvable`] reports.
#[must_use]
pub fn resource_pattern_matches(
    pattern: &str,
    resource_arn: &str,
    expand: bool,
    context: &impl ConditionContext,
) -> bool {
    if !expand || !pattern.contains("${") {
        return arn_match(pattern, resource_arn);
    }
    expand_tokens(pattern, context).is_ok_and(|tokens| arn_match_tokens(&tokens, resource_arn))
}

/// Whether expanding `pattern` stops on a variable whose key the service
/// populates but the context cannot resolve, so whether the pattern matches
/// is unknown. A pattern that also names an absent key with no default is not
/// reported: it matches nothing whatever the other value is.
///
/// The caller fails closed on such a pattern, as on a condition key the
/// context cannot resolve: a `Deny` applies and an `Allow` does not grant.
#[must_use]
pub fn resource_pattern_is_unresolvable(
    pattern: &str,
    expand: bool,
    context: &impl ConditionContext,
) -> bool {
    expand
        && pattern.contains("${")
        && expand_tokens(pattern, context) == Err(Missing::Unresolvable)
}

/// The key names the `${key}` and `${key, 'default'}` variables in `value`
/// read, in the order [`expand_condition_value`] reads them with the same
/// `iam_syntax`.
///
/// With `iam_syntax` the default is not part of the name, so a caller that
/// asks the context about each name sees the key behind
/// `${aws:RequestTag/env, 'x'}` as it sees the one behind
/// `${aws:RequestTag/env}`. Without it the name is the text between the
/// braces as written. The `${*}`, `${?}` and `${$}` escapes name no key, and
/// neither does a `${` with no closing brace, as both expand to themselves.
pub fn variable_key_names(value: &str, iam_syntax: bool) -> impl Iterator<Item = &str> {
    parts(value, iam_syntax)
        .into_iter()
        .filter_map(|part| match part {
            Part::Ref(_, Reference::Key { name, .. }) => Some(name),
            Part::Text(_) | Part::Ref(_, Reference::Escape(_)) => None,
        })
}

/// Expand variables in a condition value.
///
/// `${key}` is replaced by its value, and with `iam_syntax` so is
/// `${key, 'default'}`; a variable whose key the request does not carry,
/// with no default, is left as written, and so are the `${*}`, `${?}` and
/// `${$}` escapes, as before. A variable whose key the context reports
/// unresolvable is also left as written, with or without a default: the
/// service would have a value for the key, so the default is not used, and
/// the caller fails the condition closed instead of comparing the text
/// (`condition::evaluate_condition_match`, which finds the key through
/// [`variable_key_names`]).
///
/// The caller passes `iam_syntax` only where IAM reads the default form: in
/// a `2012-10-17` document and under a string or ARN operator. Elsewhere the
/// text between the braces is the key name as written ([`parts`]), so
/// `${key, 'default'}` stays as written, and `${key}` expands as it did
/// before the default form was read.
#[must_use]
pub fn expand_condition_value(
    value: &str,
    iam_syntax: bool,
    context: &impl ConditionContext,
) -> String {
    if !value.contains("${") {
        return value.to_owned();
    }
    let mut result = String::with_capacity(value.len());
    for part in parts(value, iam_syntax) {
        match part {
            Part::Text(text) | Part::Ref(text, Reference::Escape(_)) => result.push_str(text),
            Part::Ref(raw, Reference::Key { name, default }) => {
                result.push_str(resolve(name, default, context).unwrap_or(raw));
            }
        }
    }
    result
}

/// Match an expanded ARN pattern against an ARN value with the field rule of
/// [`arn_match`].
fn arn_match_tokens(pattern: &[Token], value: &str) -> bool {
    if pattern == [Token::Star] {
        return true;
    }
    let p_fields = split_fields(pattern);
    let v_fields: Vec<&str> = value.splitn(6, ':').collect();
    if p_fields.len() != v_fields.len() {
        return false;
    }
    p_fields.iter().zip(v_fields.iter()).all(|(p, v)| {
        let units: Vec<u16> = v.encode_utf16().collect();
        match_tokens(p, &units)
    })
}

/// Split at the first five `:` written in the policy text, so the resource
/// field keeps its own `:` and a `:` from a variable value stays in its field.
fn split_fields(pattern: &[Token]) -> Vec<&[Token]> {
    let mut fields = Vec::with_capacity(6);
    let mut rest = pattern;
    while fields.len() < 5 {
        match rest.iter().position(|t| *t == Token::Colon) {
            Some(i) => {
                fields.push(&rest[..i]);
                rest = &rest[i + 1..];
            }
            None => break,
        }
    }
    fields.push(rest);
    fields
}

/// Greedy wildcard match over tokens against the UTF-16 code units of a
/// value: the algorithm of `matcher::wildcard_match`, which counts what `?`
/// and `*` match in UTF-16 code units (#581), with `Star` and `Question` as
/// the wildcards.
fn match_tokens(p: &[Token], v: &[u16]) -> bool {
    let (plen, vlen) = (p.len(), v.len());
    let mut pi = 0;
    let mut vi = 0;
    let mut last_star = usize::MAX;
    let mut match_from = 0;

    while vi < vlen {
        if pi < plen && p[pi] == Token::Star {
            last_star = pi + 1;
            match_from = vi;
            pi += 1;
        } else if pi < plen
            && (p[pi] == Token::Question
                || p[pi] == Token::Unit(v[vi])
                || (p[pi] == Token::Colon && v[vi] == COLON))
        {
            pi += 1;
            vi += 1;
        } else if last_star != usize::MAX {
            match_from += 1;
            vi = match_from;
            pi = last_star;
        } else {
            return false;
        }
    }

    while pi < plen && p[pi] == Token::Star {
        pi += 1;
    }
    pi == plen
}

#[cfg(test)]
mod tests;
