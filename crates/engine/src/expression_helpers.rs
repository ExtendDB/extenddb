// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for building expression maps and parsing expressions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, KeyCondition, PathElement, Token, UpdateAction,
    is_syntax_error, parse_condition_from, parse_key_condition_from, parse_projection_from,
    parse_update_from, tokenize_for, tokenize_with_limit, validate_no_reserved_words,
    validate_operator_count, validate_placeholders_and_operands,
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
        .and_then(|tokens| parse_condition_from(&tokens, expr, limits.max_expression_depth))
        .map_err(|e| prefix_expression_error(e, ExpressionKind::Condition))
}

/// Check a parsed condition or filter in the service's order, after the parse
/// and before the unused placeholder check and the table lookup:
/// [`validate_placeholders_and_operands`], then [`validate_operator_count`]
/// against `limits.max_expression_operators`.
///
/// `TransactWriteItems` counts the same way on the service, but it does not
/// call this check (measured 2026-11-19: a Put of 151, 200 or 300 `#v = :v`
/// terms joined by OR answers 301, 399 or 599, a ConditionCheck of 151 terms
/// answers 301, and a Put of 150 terms is evaluated).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the `Invalid <kind>:`
/// prefix for the first fault.
pub fn check_parsed_condition(
    expr: &Expr,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_placeholders_and_operands(expr, maps).map_err(|e| prefix_expression_error(e, kind))?;
    validate_operator_count(expr, kind, limits.max_expression_operators)
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
        parse_update_from(&update_tokens, update_expr, limits.max_expression_depth)
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
    let condition = parse_optional_condition(condition_expression, limits)?;
    if let Some(ref expr) = condition {
        // Before the unused placeholder check and the table lookup.
        check_parsed_condition(expr, &maps, ExpressionKind::Condition, limits)?;
    }
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
        parse_projection_from(&tokens, proj_str)
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
        // The service reads a key condition with the condition grammar first.
        // A syntax error in that grammar comes before the operator count and
        // the key condition rules (measured 2026-11-19: `pk = :p OR` answers
        // token "<EOF>", near "OR"; `pk = :p =` token "=", near ":p ="; `pk =
        // :p sk = :s` token "sk", near ":p sk ="). The count comes next: `pk =
        // :v OR` + 150 terms answers the count, not the operator refusal, and
        // 151 AND-joined equalities answer the count ahead of the
        // one-condition-per-key check (measured 2026-10-25). When the
        // condition grammar refuses the text in another way, the key
        // condition parser reports the error in its own words.
        match parse_condition_from(&tokens, expr, limits.max_expression_depth) {
            Ok(condition) => {
                validate_operator_count(
                    &condition,
                    ExpressionKind::KeyCondition,
                    limits.max_expression_operators,
                )?;
            }
            Err(err) if is_syntax_error(&err) => return Err(err),
            Err(_) => {}
        }
        parse_key_condition_from(&tokens, expr)
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

    /// Size and token limits raised so only the depth limit refuses the
    /// expressions below.
    fn raised() -> LimitsConfig {
        LimitsConfig {
            max_expression_length_bytes: 4 << 20,
            max_expression_tokens: 1 << 20,
            ..LimitsConfig::default()
        }
    }

    /// Run `parse` on a thread with half a server worker's 2 MiB stack,
    /// failing the test after 10 seconds instead of hanging.
    fn on_small_stack(
        what: &'static str,
        parse: impl FnOnce() -> Result<(), DynamoDbError> + Send + 'static,
    ) -> Result<(), DynamoDbError> {
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .stack_size(1 << 20)
            .spawn(move || {
                let _ = tx.send(parse());
            })
            .expect("spawn the parse thread");
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(answer) => answer,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                std::panic::resume_unwind(worker.join().expect_err("no answer and no panic"))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!("{what} ran for 10 s"),
        }
    }

    #[test]
    fn update_expression_past_the_depth_limit_is_refused() {
        let limits = raised();
        let nested = |levels: usize| {
            format!(
                "SET qty = {}:v{}",
                "(qty + ".repeat(levels),
                ")".repeat(levels)
            )
        };
        let at_limit = nested(limits.max_expression_depth);
        assert!(parse_update_expr(&at_limit, &limits).is_ok());
        for levels in [limits.max_expression_depth + 1, 50_000] {
            let expr = nested(levels);
            let err = on_small_stack("deep update", move || {
                parse_update_expr(&expr, &raised()).map(drop)
            })
            .unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg)
                    if msg == "Invalid UpdateExpression: expression nesting depth exceeded"),
                "{levels} levels: got {err:?}"
            );
        }
    }

    #[test]
    fn update_expression_calls_past_the_depth_limit_are_refused() {
        let expr = format!(
            "SET qty = {}:v{}",
            "list_append(qty, ".repeat(50_000),
            ")".repeat(50_000)
        );
        let err = on_small_stack("deep update calls", move || {
            parse_update_expr(&expr, &raised()).map(drop)
        })
        .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid UpdateExpression: expression nesting depth exceeded"),
            "got {err:?}"
        );
    }

    #[test]
    fn condition_and_filter_size_calls_past_the_depth_limit_are_refused() {
        let expr = format!("{}qty{} > :v", "size(".repeat(50_000), ")".repeat(50_000));
        let condition = expr.clone();
        let err = on_small_stack("deep condition", move || {
            parse_condition_expr(&condition, &raised()).map(drop)
        })
        .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid ConditionExpression: expression nesting depth exceeded"),
            "got {err:?}"
        );
        let err = on_small_stack("deep filter", move || {
            parse_optional_filter(Some(&expr), &raised()).map(drop)
        })
        .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid FilterExpression: expression nesting depth exceeded"),
            "got {err:?}"
        );
    }
}

#[cfg(test)]
#[path = "expression_helpers_order_tests.rs"]
mod order_tests;
