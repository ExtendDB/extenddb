// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for building expression maps and parsing expressions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, KeyCondition, PathElement, Token, UpdateAction,
    parse_condition_with_depth_limit, parse_key_condition, parse_projection, parse_update_sections,
    tokenize_for, tokenize_with_limit, validate_condition_semantics, validate_no_reserved_words,
    validate_update_semantics,
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
    check_expression_size_with(expr, kind, limits, kind.size_error_includes_length())
}

/// `check_expression_size` with the length suffix decided by the caller.
///
/// Amazon DynamoDB appends ` expression size: N` for a `ConditionExpression`
/// inside a transaction and for a Scan `FilterExpression`, and omits it for a
/// `ConditionExpression` on PutItem, UpdateItem, or DeleteItem and for a Query
/// `FilterExpression` (measured 2026-10-14). The suffix is therefore a property
/// of the operation, not of the expression kind alone.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when the expression is over
/// the configured byte limit.
pub fn check_expression_size_with(
    expr: &str,
    kind: ExpressionKind,
    limits: &LimitsConfig,
    include_length: bool,
) -> Result<(), DynamoDbError> {
    if expr.len() > limits.max_expression_length_bytes {
        let mut msg =
            format!("Invalid {kind}: Expression size has exceeded the maximum allowed size;");
        if include_length {
            use std::fmt::Write as _;
            let _ = write!(msg, " expression size: {}", expr.len());
        }
        return Err(DynamoDbError::ValidationException(msg));
    }
    Ok(())
}

/// Amazon DynamoDB wraps the expression-stage errors of the single-item write
/// APIs (PutItem, UpdateItem, DeleteItem) in `1 validation error detected: `:
/// every `Invalid ConditionExpression:` and `Invalid UpdateExpression:` message,
/// the unused-name and unused-value messages, and the names-or-values-without-
/// expression messages. The read APIs, the batch APIs, and the transactions
/// return the same messages bare. Apply this at the expression stage of those
/// three handlers only; other validation messages there ("Cannot update
/// attribute pk", key schema, item size) carry no prefix on the service.
pub fn single_item_write_prefix(err: DynamoDbError) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg)
            if !msg.starts_with("1 validation error detected")
                && (msg.starts_with("Invalid ConditionExpression:")
                    || msg.starts_with("Invalid UpdateExpression:")
                    || msg.starts_with("Value provided in ExpressionAttribute")
                    || msg.starts_with("ExpressionAttributeNames can only be specified")
                    || msg.starts_with("ExpressionAttributeValues can only be specified")) =>
        {
            DynamoDbError::ValidationException(format!("1 validation error detected: {msg}"))
        }
        other => other,
    }
}

/// Tokenize, reserved-word check, and parse a required `ConditionExpression`,
/// with the size-error length suffix decided by the caller (see
/// `check_expression_size_with`).
///
/// Errors carry the `ConditionExpression` prefix, matching Amazon DynamoDB.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for over-size, syntax, or
/// reserved-word errors.
pub fn parse_condition_expr_with(
    expr: &str,
    limits: &LimitsConfig,
    include_length: bool,
) -> Result<Expr, DynamoDbError> {
    check_expression_size_with(expr, ExpressionKind::Condition, limits, include_length)?;
    tokenize_expression(expr, limits)
        .and_then(|tokens| parse_condition_with_depth_limit(&tokens, limits.max_expression_depth))
        .map_err(|e| prefix_expression_error(e, ExpressionKind::Condition))
}

/// Size-check, tokenize, parse, and semantically validate an
/// `UpdateExpression` against the request's expression maps: every syntax
/// error first, then the service's single ordered walk (function names,
/// operand counts and types, undefined references, repeated sections), then
/// overlapping paths. See `extenddb_core::expression::validate_update_semantics`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the `Invalid
/// UpdateExpression:` prefix.
pub fn parse_update_expr_checked(
    update_expr: &str,
    maps: &ExpressionMaps,
    limits: &LimitsConfig,
) -> Result<Vec<UpdateAction>, DynamoDbError> {
    check_expression_size(update_expr, ExpressionKind::Update, limits)?;
    let parsed = tokenize_for(
        update_expr,
        limits.max_expression_tokens,
        ExpressionKind::Update,
    )
    .and_then(|update_tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&update_tokens)?;
        }
        parse_update_sections(&update_tokens, Some(update_expr))
    })
    .map_err(|e| prefix_expression_error(e, ExpressionKind::Update))?;
    validate_update_semantics(&parsed, maps)?;
    Ok(parsed.actions)
}

/// Size-check, tokenize, parse, and semantically validate a condition-grammar
/// expression (`ConditionExpression` or `FilterExpression`) against the
/// request's expression maps. Syntax errors come first, then the service's
/// ordered walk: undefined references, function names, operand counts and
/// types, and functions in the wrong position. See
/// `extenddb_core::expression::validate_condition_semantics`.
///
/// `kind` labels the messages; `include_length` decides the size-error suffix
/// (see [`check_expression_size_with`]).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the `Invalid {kind}:` prefix.
pub fn parse_condition_checked(
    expr: &str,
    maps: &ExpressionMaps,
    limits: &LimitsConfig,
    include_length: bool,
    kind: ExpressionKind,
) -> Result<Expr, DynamoDbError> {
    if expr.is_empty() {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid {kind}: The expression can not be empty;"
        )));
    }
    let parsed = parse_condition_expr_with(expr, limits, include_length)
        .map_err(|e| prefix_expression_error(e, kind))?;
    validate_condition_semantics(&parsed, maps, kind)?;
    Ok(parsed)
}

/// Parse an optional filter expression (grammar only, `FilterExpression`
/// prefix). The handlers run the semantic pass through
/// [`parse_condition_checked`]; this remains for the parser tests.
#[cfg(test)]
pub fn parse_optional_filter(
    expr: Option<&str>,
    limits: &LimitsConfig,
    include_length: bool,
) -> Result<Option<Expr>, DynamoDbError> {
    match expr {
        Some(s) if !s.is_empty() => parse_condition_expr_with(s, limits, include_length)
            .map(Some)
            .map_err(|e| prefix_expression_error(e, ExpressionKind::Filter)),
        _ => Ok(None),
    }
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

    extenddb_core::validation::validate_no_expression_param_mixing(
        &[("Expected", has_expected)],
        &[("ConditionExpression", has_condition)],
    )?;

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
    // The single-item write APIs report an over-size condition without the
    // length suffix; transactions keep it (see `check_expression_size_with`).
    // The whole condition, syntax and semantics, is checked here before the
    // caller looks at any other expression, as the service does.
    let condition = match condition_expression {
        Some(s) if !s.is_empty() => Some(parse_condition_checked(
            s,
            &maps,
            limits,
            false,
            ExpressionKind::Condition,
        )?),
        _ => None,
    };
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

/// Size-check, tokenize, reserved-word check, and parse a
/// `KeyConditionExpression`, then reject a `#name` or `:value` the request
/// does not define.
///
/// Errors carry the `KeyConditionExpression` prefix, matching Amazon
/// `DynamoDB`. The references are checked once the whole expression parses,
/// in the order they stand in the text, names and values alike (measured
/// 2026-10-27: `pk = :q AND #n = :v` names `:q`, `#b = :p AND #a > :q` names
/// `#b`). The check runs before the key schema is consulted, so an undefined
/// `#n` is reported as such rather than as a key schema mismatch (measured
/// 2026-10-14).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for over-size, syntax,
/// reserved-word, or undefined-reference errors.
pub fn parse_key_condition_expr(
    expr: &str,
    maps: &ExpressionMaps,
    limits: &LimitsConfig,
) -> Result<KeyCondition, DynamoDbError> {
    check_expression_size(expr, ExpressionKind::KeyCondition, limits)?;
    let (tokens, parsed) = tokenize_for(
        expr,
        limits.max_expression_tokens,
        ExpressionKind::KeyCondition,
    )
    .and_then(|tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&tokens)?;
        }
        let parsed = parse_key_condition(&tokens)?;
        Ok((tokens, parsed))
    })
    .map_err(|e| prefix_expression_error(e, ExpressionKind::KeyCondition))?;
    check_key_condition_refs(&tokens, maps)?;
    Ok(parsed)
}

/// The first `#name` or `:value` token, in text order, that `maps` does not
/// define.
fn check_key_condition_refs(tokens: &[Token], maps: &ExpressionMaps) -> Result<(), DynamoDbError> {
    for token in tokens {
        match token {
            Token::NameRef(name) if !maps.names.contains_key(name) => {
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #{name}"
                )));
            }
            Token::Placeholder(value) if !maps.values.contains_key(value) => {
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :{value}"
                )));
            }
            _ => {}
        }
    }
    Ok(())
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
            let err = parse_condition_expr_with(expr, &limits, true).unwrap_err();
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
                parse_condition_expr_with(expr, &limits, true).is_ok(),
                "expr {expr} should parse"
            );
        }
    }

    #[test]
    fn filter_redundant_parens_rejected_with_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("((a = :v))"), &limits, true).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid FilterExpression: The expression has redundant parentheses;"),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_parser_errors_carry_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("a"), &limits, true).unwrap_err();
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
        let err = parse_optional_filter(Some(&expr), &limits, true).unwrap_err();
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
        let err = parse_condition_expr_with(&expr, &limits, true).unwrap_err();
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
