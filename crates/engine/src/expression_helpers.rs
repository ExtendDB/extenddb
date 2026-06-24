// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for building expression maps and parsing expressions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, KeyCondition, PathElement, Token, UpdateAction,
    parse_condition_with_depth_limit, parse_key_condition, parse_projection, parse_update_from,
    tokenize_for, tokenize_with_limit, validate_no_reserved_words,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, ConditionalOperator, ExpectedAttributeValue};

use crate::expected::desugar_expected;

/// Tokenize an expression and optionally validate reserved keywords.
pub fn tokenize_expression(
    input: &str,
    limits: &LimitsConfig,
) -> Result<Vec<Token>, DynamoDbError> {
    let tokens = tokenize_with_limit(input, limits.max_expression_tokens)?;
    if limits.enforce_reserved_keywords {
        validate_no_reserved_words(&tokens)?;
    }
    Ok(tokens)
}

/// Build `ExpressionMaps` from optional request fields.
///
/// Pre-parses all numeric placeholder values into `BigDecimal` so that
/// filter expressions comparing a placeholder against many items parse
/// the placeholder only once per request.
pub fn build_expression_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix('#').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        values
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix(':').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
    )
}

/// Reject an expression over the byte-size limit, measured on the raw text
/// before `#name` / `:value` substitution (Amazon `DynamoDB`: 4096). The error
/// carries the per-parameter prefix and, for Filter/Condition, the byte length.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when `expr` is over the limit.
pub fn check_expression_size(
    expr: &str,
    kind: ExpressionKind,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if expr.len() > limits.max_expression_length_bytes {
        let mut msg =
            format!("Invalid {kind}: Expression size has exceeded the maximum allowed size;");
        if kind.size_error_includes_length() {
            use std::fmt::Write as _;
            let _ = write!(msg, " expression size: {}", expr.len());
        }
        return Err(DynamoDbError::ValidationException(msg));
    }
    Ok(())
}

/// Tokenize, reserved-word check, and parse a required `ConditionExpression`.
///
/// Errors carry the `ConditionExpression` prefix, matching Amazon DynamoDB.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax or reserved-word errors.
pub fn parse_condition_expr(expr: &str, limits: &LimitsConfig) -> Result<Expr, DynamoDbError> {
    check_expression_size(expr, ExpressionKind::Condition, limits)?;
    tokenize_expression(expr, limits)
        .and_then(|tokens| parse_condition_with_depth_limit(&tokens, limits.max_expression_depth))
        .map_err(|e| prefix_expression_error(e, ExpressionKind::Condition))
}

/// Tokenize, reserved-word check, and parse an `UpdateExpression`.
///
/// Errors carry the `UpdateExpression` prefix, matching Amazon DynamoDB.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax or reserved-word errors.
pub fn parse_update_expr(
    update_expr: &str,
    limits: &LimitsConfig,
) -> Result<Vec<UpdateAction>, DynamoDbError> {
    check_expression_size(update_expr, ExpressionKind::Update, limits)?;
    tokenize_for(
        update_expr,
        limits.max_expression_tokens,
        ExpressionKind::Update,
    )
    .and_then(|update_tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&update_tokens)?;
        }
        parse_update_from(&update_tokens, update_expr)
    })
    .map_err(|e| prefix_expression_error(e, ExpressionKind::Update))
}

/// Parse an optional condition expression string into an AST.
///
/// Returns `None` if the input is `None` or empty.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax errors.
pub fn parse_optional_condition(
    expr: Option<&str>,
    limits: &LimitsConfig,
) -> Result<Option<Expr>, DynamoDbError> {
    match expr {
        Some(s) if !s.is_empty() => parse_condition_expr(s, limits).map(Some),
        _ => Ok(None),
    }
}

/// Parse an optional filter expression string into an AST.
///
/// `FilterExpression` uses the same grammar as `ConditionExpression`.
/// Returns `None` if the input is `None` or empty.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax errors.
pub fn parse_optional_filter(
    expr: Option<&str>,
    limits: &LimitsConfig,
) -> Result<Option<Expr>, DynamoDbError> {
    parse_optional_condition(expr, limits)
        .map_err(|e| prefix_expression_error(e, ExpressionKind::Filter))
}

/// Resolve a condition from either `ConditionExpression` or legacy `Expected`.
///
/// `DynamoDB` rejects requests that specify both. Returns the parsed condition
/// AST and the expression maps to use for evaluation.
///
/// # Errors
///
/// Returns `ValidationException` if both `ConditionExpression` and `Expected` are set,
/// or for any parsing/desugaring errors.
pub fn resolve_condition(
    condition_expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    expected: Option<&HashMap<String, ExpectedAttributeValue>>,
    conditional_operator: Option<ConditionalOperator>,
    limits: &LimitsConfig,
) -> Result<(Option<Expr>, ExpressionMaps), DynamoDbError> {
    let has_condition = condition_expression.is_some_and(|s| !s.is_empty());
    let has_expected = expected.is_some_and(|m| !m.is_empty());

    if has_condition && has_expected {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}"
                .to_owned(),
        ));
    }

    if let Some(exp) = expected.filter(|m| !m.is_empty()) {
        let (expr, mut maps) = desugar_expected(exp, conditional_operator.unwrap_or_default())?;
        // Merge request-level ExpressionAttributeNames/Values so UpdateExpression
        // placeholders still resolve when Expected is used for the condition.
        if let Some(n) = names {
            for (k, v) in n {
                maps.names
                    .entry(k.strip_prefix('#').unwrap_or(k).to_owned())
                    .or_insert_with(|| v.clone());
            }
        }
        if let Some(v) = values {
            for (k, val) in v {
                maps.values
                    .entry(k.strip_prefix(':').unwrap_or(k).to_owned())
                    .or_insert_with(|| val.clone());
            }
        }
        // Re-parse numerics after merging additional values.
        maps.pre_parse_numerics();
        return Ok((Some(expr), maps));
    }

    let maps = build_expression_maps(names, values);
    let condition = parse_optional_condition(condition_expression, limits)?;
    Ok((condition, maps))
}

/// Tokenize, reserved-word check, and parse a `ProjectionExpression`.
///
/// Errors carry the `ProjectionExpression` prefix, matching Amazon `DynamoDB`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax or reserved-word errors.
pub fn parse_projection_expr(
    proj_str: &str,
    limits: &LimitsConfig,
) -> Result<Vec<Vec<PathElement>>, DynamoDbError> {
    check_expression_size(proj_str, ExpressionKind::Projection, limits)?;
    let result = tokenize_for(
        proj_str,
        limits.max_expression_tokens,
        ExpressionKind::Projection,
    )
    .and_then(|tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&tokens)?;
        }
        parse_projection(&tokens)
    });
    result.map_err(|e| prefix_expression_error(e, ExpressionKind::Projection))
}

/// Size-check, tokenize, reserved-word check, and parse a `KeyConditionExpression`.
///
/// Errors carry the `KeyConditionExpression` prefix, matching Amazon `DynamoDB`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for over-size, syntax, or
/// reserved-word errors.
pub fn parse_key_condition_expr(
    expr: &str,
    limits: &LimitsConfig,
) -> Result<KeyCondition, DynamoDbError> {
    check_expression_size(expr, ExpressionKind::KeyCondition, limits)?;
    tokenize_for(
        expr,
        limits.max_expression_tokens,
        ExpressionKind::KeyCondition,
    )
    .and_then(|tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&tokens)?;
        }
        parse_key_condition(&tokens)
    })
    .map_err(|e| prefix_expression_error(e, ExpressionKind::KeyCondition))
}

/// Prefix an expression error with the expression type, matching `DynamoDB`'s format.
///
/// `FilterExpression` shares the condition parser, so its errors arrive labelled
/// `ConditionExpression`; those are relabelled to `expr_type`. Errors already
/// labelled with another expression type, or non-expression validation errors,
/// are returned unchanged.
pub fn prefix_expression_error(err: DynamoDbError, kind: ExpressionKind) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg) => {
            if let Some(rest) = msg.strip_prefix("Invalid ConditionExpression:") {
                DynamoDbError::ValidationException(format!("Invalid {kind}:{rest}"))
            } else if let Some(rest) = msg.strip_prefix("Invalid expression:") {
                DynamoDbError::ValidationException(format!("Invalid {kind}:{rest}"))
            } else if msg.starts_with("Invalid ") || msg.starts_with("1 validation") {
                DynamoDbError::ValidationException(msg)
            } else {
                DynamoDbError::ValidationException(format!("Invalid {kind}: {msg}"))
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::limits::LimitsConfig;

    const CONDITION_REDUNDANT: &str =
        "Invalid ConditionExpression: The expression has redundant parentheses;";

    #[test]
    fn condition_redundant_parens_rejected_with_canonical_message() {
        let limits = LimitsConfig::default();
        for expr in [
            "((a = :v))",
            "(((a = :v)))",
            "((a = :v AND b = :v2))",
            "((NOT (a = :v)))",
        ] {
            let err = parse_optional_condition(Some(expr), &limits).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == CONDITION_REDUNDANT),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn condition_valid_parens_accepted() {
        let limits = LimitsConfig::default();
        for expr in [
            "(a = :v)",
            "(a = :v) AND (b = :v2)",
            "((a = :v) AND (b = :v2))",
            "(NOT (a = :v))",
        ] {
            assert!(
                parse_optional_condition(Some(expr), &limits).is_ok(),
                "expr {expr} should parse"
            );
        }
    }

    #[test]
    fn filter_redundant_parens_rejected_with_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("((a = :v))"), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid FilterExpression: The expression has redundant parentheses;"),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_parser_errors_carry_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("a"), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("Invalid FilterExpression:")),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_over_size_limit_keeps_size_suffix_through_relabel() {
        // Filter relabels through Condition; the size suffix must survive.
        let limits = LimitsConfig::default();
        let path = "a".to_owned() + &"z".repeat(limits.max_expression_length_bytes);
        let expr = format!("{path} = :v");
        let err = parse_optional_filter(Some(&expr), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if *msg == format!(
                    "Invalid FilterExpression: Expression size has exceeded the \
                     maximum allowed size; expression size: {}", expr.len())),
            "got {err:?}"
        );
    }

    #[test]
    fn condition_over_size_limit_keeps_size_suffix() {
        let limits = LimitsConfig::default();
        let path = "a".to_owned() + &"z".repeat(limits.max_expression_length_bytes);
        let expr = format!("attribute_not_exists({path})");
        let err = parse_optional_condition(Some(&expr), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if *msg == format!(
                    "Invalid ConditionExpression: Expression size has exceeded the \
                     maximum allowed size; expression size: {}", expr.len())),
            "got {err:?}"
        );
    }

    #[test]
    fn projection_over_size_limit_omits_size_suffix() {
        let limits = LimitsConfig::default();
        let expr = "a".to_owned() + &"z".repeat(limits.max_expression_length_bytes);
        let err = parse_projection_expr(&expr, &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid ProjectionExpression: Expression size has exceeded \
                           the maximum allowed size;"),
            "got {err:?}"
        );
    }

    #[test]
    fn expression_size_limit_is_bytes_not_chars() {
        // Over the limit in bytes but not in chars: still rejected.
        let limits = LimitsConfig::default();
        let half = limits.max_expression_length_bytes / 2;
        let path = "\u{00e9}".repeat(half + 1); // 'é' = 2 bytes; (half+1)*2 > limit
        assert!(path.chars().count() <= limits.max_expression_length_bytes);
        assert!(path.len() > limits.max_expression_length_bytes);
        let err = parse_projection_expr(&path, &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.contains("Expression size has exceeded the maximum allowed size")),
            "got {err:?}"
        );
    }
}
