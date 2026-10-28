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
//! In a `Resource` or `NotResource` pattern, and in a `StringLike` or
//! `ArnLike` condition value, the text written in the policy keeps `*` and
//! `?` as wildcards, while text that comes from a variable value, a default or
//! an escape is matched literally, so a tag value or an escaped `*` cannot
//! widen a pattern. A variable whose key the request does not carry, with no
//! default, makes the pattern or value match nothing. Both rules were measured
//! with the IAM policy simulator (issues #533 and #534). In a `Resource` or
//! `NotResource` pattern, a variable whose key the service populates but the
//! context cannot resolve (a key listed in `context::FAIL_CLOSED_GLOBAL_KEYS`)
//! leaves the match unknown, and the evaluator fails closed on it as it does
//! on such a key in a condition; in a condition value such a variable makes
//! the value match nothing. Only a document whose `Version` is `2012-10-17`
//! expands variables, in patterns and in condition values alike; a
//! `2008-10-17` document, or one with no `Version`, compares the text as
//! written. The caller decides that with
//! [`PolicyDocument::expands_policy_variables`].
//!
//! ARN matching here follows the same field rule as
//! [`super::matcher::arn_match`]: the first five `:` separate fields, the
//! resource field keeps its own `:` and `/`, and both sides need the same
//! number of fields. Only a `:` written in the policy text separates fields;
//! a `:` inside a variable value is matched as a byte of its field (measured:
//! a region-field value `us-east-1:123456789012` matches no table ARN).

use super::context::ConditionContext;
use super::document::{PolicyDocument, PolicyParseError};
use super::matcher::{arn_match, wildcard_match};
use serde_json::Value;

impl PolicyDocument {
    /// Whether `${key}` policy variables in `Resource` and `NotResource`
    /// patterns and in condition values are expanded.
    ///
    /// IAM expands them only in a `2012-10-17` document; a `2008-10-17`
    /// document, or one with no `Version`, matches the pattern or value as
    /// written. Measured with the IAM policy simulator (issues #533 and #534).
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

/// One unit of an expanded pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    /// `*` written in the policy text: matches zero or more bytes.
    Star,
    /// `?` written in the policy text: matches exactly one byte.
    Question,
    /// `:` written in the policy text: a field separator among the first five,
    /// a literal `:` in the resource field.
    Colon,
    /// One byte matched literally.
    Byte(u8),
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

/// Parse the text between `${` and `}`.
///
/// `*`, `?` and `$` are escapes. `name, 'default'` (whitespace around either
/// part ignored) is the default form. Anything else, including a `,` whose
/// right side is not single-quoted, is taken as the key name as written, which
/// is what the condition path did before the default form was read.
fn parse_reference(inner: &str) -> Reference<'_> {
    match inner {
        "*" => return Reference::Escape(b'*'),
        "?" => return Reference::Escape(b'?'),
        "$" => return Reference::Escape(b'$'),
        _ => {}
    }
    if let Some((name, default)) = inner.split_once(',') {
        let default = default.trim();
        if default.len() >= 2 && default.starts_with('\'') && default.ends_with('\'') {
            return Reference::Key {
                name: name.trim(),
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
fn parts(value: &str) -> Vec<Part<'_>> {
    let mut out = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push(Part::Text(&rest[..start]));
        let after_start = &rest[start + 2..];
        match after_start.find('}') {
            Some(end) => {
                let raw = &rest[start..start + 3 + end];
                out.push(Part::Ref(raw, parse_reference(&after_start[..end])));
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
/// `table/${aws:TagKeys, 'x'}`: implicitDeny, implicitDeny, allowed).
fn resolve<'a>(
    name: &str,
    default: Option<&'a str>,
    context: &'a impl ConditionContext,
) -> Result<&'a str, Missing> {
    if context.is_multivalued_key(name) {
        return match context.resolve_key(name) {
            Some(_) => Err(Missing::Absent),
            None => default.ok_or(Missing::Absent),
        };
    }
    match context.resolve_key(name) {
        Some(values) => Ok(values.first().copied().unwrap_or("")),
        None if context.is_unresolvable_key(name) => Err(Missing::Unresolvable),
        None => default.ok_or(Missing::Absent),
    }
}

/// The token for a byte written in the policy text.
fn written_token(byte: u8) -> Token {
    match byte {
        b'*' => Token::Star,
        b'?' => Token::Question,
        b':' => Token::Colon,
        other => Token::Byte(other),
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
    for part in parts(pattern) {
        match part {
            Part::Text(text) => tokens.extend(text.bytes().map(written_token)),
            Part::Ref(_, Reference::Escape(byte)) => tokens.push(Token::Byte(byte)),
            Part::Ref(_, Reference::Key { name, default }) => {
                match resolve(name, default, context) {
                    Ok(value) => tokens.extend(value.bytes().map(Token::Byte)),
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

/// A condition value after policy variable expansion for one request.
///
/// The `StringLike` and `ArnLike` operators read `*` and `?` as wildcards
/// only where the policy wrote them: a `*` or `?` that came from a variable
/// value, a default or an escape is matched literally, the same rule as a
/// `Resource` pattern. Measured with the IAM policy simulator (issue #534):
/// `StringLike` `a${*}` matches `a*` and not `abc`, `a${?}` matches `a?` and
/// not `ab`, and `ArnLike` `.../user/a${*}` matches `.../user/a*` and not
/// `.../user/abc`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionValue {
    /// The value as written: it has no `${`, or the document does not expand
    /// variables. Every `*` and `?` is a wildcard for the Like operators.
    Literal(String),
    /// The value with its variables, defaults and escapes expanded.
    Expanded {
        /// The expanded text, compared by the equality, numeric, date, boolean
        /// and address operators.
        text: String,
        /// The expanded text with the `*` and `?` the policy wrote marked as
        /// wildcards, matched by the Like operators.
        tokens: Vec<Token>,
    },
    /// A variable left nothing to compare: a `${key}` with no default named a
    /// key the request does not carry, or a variable named a multivalued key
    /// the request carries or a key the context cannot resolve, with or
    /// without a default. The value matches nothing: a positive operator is
    /// false against it and a negative operator true (measured: `StringEquals`
    /// `${aws:username}` with no `aws:username` does not match the literal
    /// text `${aws:username}`, and `StringNotEquals` with the same value
    /// matches `alice`).
    Unresolved,
}

impl ConditionValue {
    /// The text the value compares as, `None` for an unresolved variable.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Literal(text) | Self::Expanded { text, .. } => Some(text),
            Self::Unresolved => None,
        }
    }

    /// Whether the value, read as a `StringLike` pattern, matches `value`.
    #[must_use]
    pub fn wildcard_matches(&self, value: &str) -> bool {
        match self {
            Self::Literal(pattern) => wildcard_match(pattern, value),
            Self::Expanded { tokens, .. } => match_tokens(tokens, value.as_bytes()),
            Self::Unresolved => false,
        }
    }

    /// Whether the value, read as an `ArnLike` pattern, matches `value`.
    #[must_use]
    pub fn arn_matches(&self, value: &str) -> bool {
        match self {
            Self::Literal(pattern) => arn_match(pattern, value),
            Self::Expanded { tokens, .. } => arn_match_tokens(tokens, value),
            Self::Unresolved => false,
        }
    }
}

/// Expand variables in a condition value.
///
/// With `expand` false (a `2008-10-17` document, or one with no `Version`), or
/// when `value` has no `${`, the value is compared as written: `${aws:username}`
/// is the eleven-character text and `${*}` the four-character text (measured,
/// issue #534). Otherwise `${key}` and `${key, 'default'}` are replaced by
/// their value, `${*}`, `${?}` and `${$}` by `*`, `?` and `$` (the result is
/// not expanded again: `a${$}{x}` is the text `a${x}`), and a variable whose
/// key the request does not carry, with no default, makes the value
/// [`ConditionValue::Unresolved`]. So does a variable whose key the context
/// cannot resolve, with or without a default; a condition value does not fail
/// closed on it.
#[must_use]
pub fn expand_condition_value(
    value: &str,
    expand: bool,
    context: &impl ConditionContext,
) -> ConditionValue {
    if !expand || !value.contains("${") {
        return ConditionValue::Literal(value.to_owned());
    }
    let mut text = String::with_capacity(value.len());
    let mut tokens = Vec::with_capacity(value.len());
    for part in parts(value) {
        match part {
            Part::Text(written) => {
                text.push_str(written);
                tokens.extend(written.bytes().map(written_token));
            }
            Part::Ref(_, Reference::Escape(byte)) => {
                text.push(char::from(byte));
                tokens.push(Token::Byte(byte));
            }
            Part::Ref(_, Reference::Key { name, default }) => {
                // `Missing::Absent` and `Missing::Unresolvable` both leave
                // nothing to compare, so the value matches nothing.
                let Ok(resolved) = resolve(name, default, context) else {
                    return ConditionValue::Unresolved;
                };
                text.push_str(resolved);
                tokens.extend(resolved.bytes().map(Token::Byte));
            }
        }
    }
    ConditionValue::Expanded { text, tokens }
}

/// Whether the `Arn*` condition value `value` holds a `${...}` reference and
/// has fewer than five `:` written outside its references, as
/// `${aws:ResourceTag/owner}` has none. Only a written `:` separates ARN
/// fields here, so such a value cannot have the six fields of an ARN. The IAM
/// policy simulator refuses a policy with such a value (`InvalidInput`) and
/// IAM Access Analyzer reports `MISSING_ARN_FIELD` (measured 2026-10-28).
#[must_use]
pub(super) fn arn_value_lacks_fields(value: &str) -> bool {
    value.contains("${")
        && parts(value)
            .iter()
            .map(|part| match part {
                Part::Text(text) => text.matches(':').count(),
                Part::Ref(..) => 0,
            })
            .sum::<usize>()
            < 5
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
    p_fields
        .iter()
        .zip(v_fields.iter())
        .all(|(p, v)| match_tokens(p, v.as_bytes()))
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

/// Greedy wildcard match over tokens, the same algorithm as
/// `matcher::wildcard_match` with `Star` and `Question` as the wildcards.
fn match_tokens(p: &[Token], v: &[u8]) -> bool {
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
                || p[pi] == Token::Byte(v[vi])
                || (p[pi] == Token::Colon && v[vi] == b':'))
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
