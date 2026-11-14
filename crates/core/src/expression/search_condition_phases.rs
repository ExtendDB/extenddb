// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The two validation phases of a vector search filter expression, in the
//! order the service applies them.
//!
//! Measured against the live service on 2026-10-29 and 2026-10-30 in us-east-1,
//! each request shape sent twice with the same response both times:
//!
//! 1. The whole expression first. The syntax is checked, then each comparison
//!    in written order resolves its attribute name (an undefined `#alias` or a
//!    bare reserved word, in any element of the path) and then its value
//!    placeholder (an undefined `:value`). A failure here is reported wherever
//!    it sits, so an undefined value in the 20th or 21st comparison gets the
//!    undefined-value message, not the count message.
//! 2. Then each comparison in written order. The 20th comparison gets the
//!    count message. Before it, each comparison checks its comparator, then
//!    that its right side is a value placeholder, then that its attribute is
//!    not a nested path, then that its attribute is not a repeat. So `<`, a
//!    nested path, a repeat, or a right side without a colon gets its own
//!    message at position 2 and the count message at position 20.

use std::collections::HashMap;

use super::{
    MAX_INLINE_FILTER_CONDITIONS, MAX_PARTITION_KEY_CONDITIONS, MAX_SEARCH_CONDITIONS,
    SearchCondition, invalid,
};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// The message for a syntax error. The service names the token and its context
/// (`Syntax error; token: "<EOF>", near: "AND"`); ExtendDB does not yet.
const SYNTAX_ERROR: &str = "Invalid SearchConditionExpression";

/// One comparison after the whole-expression phase.
pub(super) struct Comparison<'a> {
    /// The left operand as written.
    lhs: &'a str,
    /// The comparator token.
    comparator: &'a str,
    /// The right operand as written.
    rhs: &'a str,
    /// The left operand with a single `#alias` resolved. Equal to `lhs` when
    /// `lhs` is a path of several elements.
    attribute_name: String,
    /// The resolved value, or `None` when the right operand is not a value
    /// placeholder.
    value: Option<AttributeValue>,
    /// The value placeholder as referenced, with its leading colon.
    pub(super) placeholder: Option<String>,
}

/// Phase 1: syntax, then name and value resolution of every comparison.
pub(super) fn whole_expression<'a>(
    expr: &'a str,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Vec<Comparison<'a>>, DynamoDbError> {
    // `!=` is a syntax error on the service at any position, measured at
    // positions 2 and 20. ExtendDB keeps its existing message for it.
    if expr.contains('!') {
        return Err(invalid(
            "Invalid SearchConditionExpression: Invalid comparator used in \
             SearchConditionExpression",
        ));
    }

    let tokens = tokenize(expr);

    // Only AND may join comparisons. Measured with OR between the 1st and 2nd
    // and between the 19th and 20th comparisons, and with NOT and IN in the
    // 20th: each gets the operator message.
    if tokens.iter().any(|tok| {
        let upper = tok.to_ascii_uppercase();
        matches!(upper.as_str(), "OR" | "NOT" | "BETWEEN" | "IN")
    }) {
        return Err(invalid(
            "Invalid operator used in SearchConditionExpression",
        ));
    }

    let mut comparisons = parse(&tokens)?;
    for comparison in &mut comparisons {
        comparison.attribute_name = resolve_name(comparison.lhs, names)?;
        if let Some((placeholder, value)) = resolve_value(comparison.rhs, values)? {
            comparison.placeholder = Some(placeholder);
            comparison.value = Some(value);
        }
    }
    Ok(comparisons)
}

/// Phase 2: the count, then the per-comparison checks, in written order.
pub(super) fn each_comparison(
    comparisons: Vec<Comparison<'_>>,
) -> Result<Vec<SearchCondition>, DynamoDbError> {
    let mut conditions: Vec<SearchCondition> = Vec::new();
    for (position, comparison) in comparisons.into_iter().enumerate() {
        if position >= MAX_SEARCH_CONDITIONS {
            return Err(invalid(format!(
                "Invalid SearchConditionExpression: SearchConditionExpression cannot have more than \
                 {MAX_PARTITION_KEY_CONDITIONS} partition key and more than \
                 {MAX_INLINE_FILTER_CONDITIONS} inline filter key comparisons"
            )));
        }

        // Only equality is supported.
        if comparison.comparator != "=" {
            return Err(invalid(
                "Invalid SearchConditionExpression: Invalid comparator used in \
                 SearchConditionExpression",
            ));
        }

        // The right side must be a value placeholder. The service's message
        // here is "Invalid condition in SearchConditionExpression: Multiple
        // attribute names used in one condition"; ExtendDB keeps its existing
        // one.
        let Some(value) = comparison.value else {
            return Err(invalid(SYNTAX_ERROR));
        };

        // Nested attribute access is not permitted, whether written or reached
        // through an alias.
        if is_nested(comparison.lhs) || is_nested(&comparison.attribute_name) {
            return Err(invalid(
                "SearchConditionExpression cannot have conditions on nested attributes",
            ));
        }

        // At most one condition per attribute, compared after alias resolution.
        if conditions
            .iter()
            .any(|c| c.attribute_name == comparison.attribute_name)
        {
            return Err(invalid(
                "Invalid SearchConditionExpression: SearchConditionExpression must only \
                 contain one condition per attribute",
            ));
        }
        conditions.push(SearchCondition {
            attribute_name: comparison.attribute_name,
            value,
        });
    }
    Ok(conditions)
}

/// Split an expression into operands, comparators and `AND`.
///
/// Whitespace separates tokens. `=`, `<`, `>`, `<=`, `>=` and `<>` are tokens
/// of their own whether or not spaces surround them.
fn tokenize(expr: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start: Option<usize> = None;
    let mut chars = expr.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if !(c.is_whitespace() || matches!(c, '=' | '<' | '>')) {
            start.get_or_insert(i);
            continue;
        }
        if let Some(s) = start.take() {
            tokens.push(&expr[s..i]);
        }
        if c.is_whitespace() {
            continue;
        }
        let pair = matches!(
            (c, chars.peek().map(|&(_, next)| next)),
            ('<', Some('=' | '>')) | ('>', Some('='))
        );
        let end = if pair {
            chars.next();
            i + 2
        } else {
            i + 1
        };
        tokens.push(&expr[i..end]);
    }
    if let Some(s) = start {
        tokens.push(&expr[s..]);
    }
    tokens
}

fn is_comparator(token: &str) -> bool {
    matches!(token, "=" | "<" | "<=" | ">" | ">=" | "<>")
}

fn operand<'a>(token: Option<&&'a str>) -> Result<&'a str, DynamoDbError> {
    match token {
        Some(tok) if !is_comparator(tok) && !tok.eq_ignore_ascii_case("AND") => Ok(tok),
        _ => Err(invalid(SYNTAX_ERROR)),
    }
}

/// Parse `operand comparator operand (AND operand comparator operand)*`.
fn parse<'a>(tokens: &[&'a str]) -> Result<Vec<Comparison<'a>>, DynamoDbError> {
    let mut comparisons = Vec::new();
    let mut index = 0;
    loop {
        let lhs = operand(tokens.get(index))?;
        let comparator = match tokens.get(index + 1) {
            Some(tok) if is_comparator(tok) => *tok,
            _ => return Err(invalid(SYNTAX_ERROR)),
        };
        let rhs = operand(tokens.get(index + 2))?;
        comparisons.push(Comparison {
            lhs,
            comparator,
            rhs,
            attribute_name: lhs.to_owned(),
            value: None,
            placeholder: None,
        });
        match tokens.get(index + 3) {
            None => return Ok(comparisons),
            Some(tok) if tok.eq_ignore_ascii_case("AND") => index += 4,
            Some(_) => return Err(invalid(SYNTAX_ERROR)),
        }
    }
}

/// Resolve the attribute name of a left operand.
///
/// Every element of the path is checked: an `#alias` must be defined and a
/// bare name must not be a reserved word. Measured with `a.bucket` and
/// `a.#nope` at positions 2 and 20, which get the reserved-keyword and the
/// undefined-name messages. A single `#alias` resolves to its attribute name;
/// a path of several elements is returned as written, for the nested-path
/// check of phase 2.
fn resolve_name(
    lhs: &str,
    names: Option<&HashMap<String, String>>,
) -> Result<String, DynamoDbError> {
    let mut resolved = None;
    for element in lhs.split('.') {
        let name = element.split('[').next().unwrap_or_default();
        if let Some(alias) = name.strip_prefix('#') {
            let target = names
                .and_then(|m| m.get(&format!("#{alias}")).or_else(|| m.get(alias)))
                .ok_or_else(|| {
                    invalid(format!(
                        "An expression attribute name used in the expression is not defined: #{alias}"
                    ))
                })?;
            resolved = Some(target.clone());
        } else if !name.is_empty() && crate::expression::reserved_words::is_reserved(name) {
            // Applied to the bare form only: the `#alias` branch exists so a
            // reserved word can be used by aliasing it.
            return Err(invalid(format!(
                "Invalid SearchConditionExpression: Attribute name is a reserved \
                 keyword; reserved keyword: {name}"
            )));
        }
    }
    if is_nested(lhs) {
        return Ok(lhs.to_owned());
    }
    Ok(resolved.unwrap_or_else(|| lhs.to_owned()))
}

/// Resolve a right operand that is a value placeholder (`:name`).
///
/// Returns `None` for a right operand without the colon; phase 2 refuses it.
fn resolve_value(
    rhs: &str,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Option<(String, AttributeValue)>, DynamoDbError> {
    let Some(placeholder) = rhs.strip_prefix(':') else {
        return Ok(None);
    };
    let value = values
        .and_then(|m| {
            m.get(&format!(":{placeholder}"))
                .or_else(|| m.get(placeholder))
        })
        .cloned()
        .ok_or_else(|| {
            invalid(format!(
                "Invalid SearchConditionExpression: An expression attribute value used in \
                 expression is not defined; attribute value: :{placeholder}"
            ))
        })?;
    Ok(Some((format!(":{placeholder}"), value)))
}

fn is_nested(path: &str) -> bool {
    path.contains('.') || path.contains('[')
}
