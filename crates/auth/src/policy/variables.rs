// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy variable expansion.
//!
//! A policy may write `${key}` in a `Resource` or `NotResource` pattern or in
//! a condition value, and IAM replaces it with the request's value for `key`
//! before matching. The forms are:
//!
//! - `${key}`: the value of `key` in the request context. For a multivalued
//!   key the first value is used.
//! - `${key, 'default'}`: the value of `key`, or `default` when the request
//!   does not carry `key`.
//! - `${*}`, `${?}`, `${$}`: a literal `*`, `?` or `$`.
//!
//! In a `Resource` or `NotResource` pattern the text written in the policy
//! keeps `*` and `?` as wildcards, while text that comes from a variable
//! value, a default or an escape is matched literally, so a tag value or an
//! escaped `*` cannot widen a pattern. A variable whose key the request does
//! not carry, with no default, makes the pattern match nothing. Both rules
//! were measured with the IAM policy simulator (issue #533). Only a document
//! whose `Version` is `2012-10-17` expands variables; the caller decides that
//! with [`PolicyDocument::expands_policy_variables`].
//!
//! ARN matching here follows the same field rule as
//! [`super::matcher::arn_match`]: the first five `:` separate fields, the
//! resource field keeps its own `:` and `/`, and both sides need the same
//! number of fields.

use super::context::ConditionContext;
use super::document::PolicyDocument;
use super::matcher::arn_match;

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

/// One unit of an expanded pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    /// `*` written in the policy text: matches zero or more bytes.
    Star,
    /// `?` written in the policy text: matches exactly one byte.
    Question,
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

/// The request's value for `key`: the first value of a present key, the
/// default when the key is absent and a default was written, `None` when the
/// key is absent and no default was written. A present key with no values
/// expands to the empty string.
fn resolve<'a>(
    name: &str,
    default: Option<&'a str>,
    context: &'a impl ConditionContext,
) -> Option<&'a str> {
    match context.resolve_key(name) {
        Some(values) => Some(values.first().copied().unwrap_or("")),
        None => default,
    }
}

/// Expand `pattern` into tokens. Returns `None` when a variable names a key
/// the request does not carry and gives no default.
fn expand_tokens(pattern: &str, context: &impl ConditionContext) -> Option<Vec<Token>> {
    let mut tokens = Vec::with_capacity(pattern.len());
    for part in parts(pattern) {
        match part {
            Part::Text(text) => tokens.extend(text.bytes().map(|b| match b {
                b'*' => Token::Star,
                b'?' => Token::Question,
                other => Token::Byte(other),
            })),
            Part::Ref(_, Reference::Escape(byte)) => tokens.push(Token::Byte(byte)),
            Part::Ref(_, Reference::Key { name, default }) => {
                let value = resolve(name, default, context)?;
                tokens.extend(value.bytes().map(Token::Byte));
            }
        }
    }
    Some(tokens)
}

/// Whether the `Resource` or `NotResource` pattern `pattern` matches
/// `resource_arn`.
///
/// With `expand` false, or when `pattern` has no `${`, this is exactly
/// [`arn_match`]. Otherwise variables are expanded first and a pattern with an
/// unresolved variable matches nothing.
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
    match expand_tokens(pattern, context) {
        Some(tokens) => arn_match_tokens(&tokens, resource_arn),
        None => false,
    }
}

/// Expand variables in a condition value.
///
/// `${key}` and `${key, 'default'}` are replaced by their value; a variable
/// whose key the request does not carry, with no default, is left as written,
/// and so are the `${*}`, `${?}` and `${$}` escapes, as before.
#[must_use]
pub fn expand_condition_value(value: &str, context: &impl ConditionContext) -> String {
    if !value.contains("${") {
        return value.to_owned();
    }
    let mut result = String::with_capacity(value.len());
    for part in parts(value) {
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
    p_fields
        .iter()
        .zip(v_fields.iter())
        .all(|(p, v)| match_tokens(p, v.as_bytes()))
}

/// Split at the first five literal `:` bytes, so the resource field keeps its
/// own `:`.
fn split_fields(pattern: &[Token]) -> Vec<&[Token]> {
    let mut fields = Vec::with_capacity(6);
    let mut rest = pattern;
    while fields.len() < 5 {
        match rest.iter().position(|t| *t == Token::Byte(b':')) {
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
        } else if pi < plen && (p[pi] == Token::Question || p[pi] == Token::Byte(v[vi])) {
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
