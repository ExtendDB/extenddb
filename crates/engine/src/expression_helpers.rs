// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for building expression maps and parsing expressions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, KeyCondition, PathElement, Token, UpdateAction,
    parse_condition_from, parse_key_condition_from, parse_projection_from, parse_update_from,
    tokenize_for, tokenize_with_limit, validate_no_reserved_words, validate_operator_count,
    validate_placeholders_and_operands,
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

/// Refuse a condition or filter whose operator count exceeds
/// `limits.max_expression_operators`, the service's cap of 300 by default
/// (`Invalid <kind>: The expression contains too many operators; operator
/// count: <n>`).
///
/// Measured 2026-10-25 (us-east-1): the check runs after the syntax, reserved
/// word, undefined placeholder and operand type checks and before the unused
/// placeholder check and the table lookup. `TransactWriteItems` counts the
/// same way, but it does not call this check (measured 2026-11-19: a Put of
/// 151, 200 or 300 `#v = :v` terms joined by OR answers 301, 399 or 599, a
/// ConditionCheck of 151 terms answers 301, and a Put of 150 terms is
/// evaluated).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when the count is over the cap.
pub fn check_operator_count(
    expr: &Expr,
    kind: ExpressionKind,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_operator_count(expr, kind, limits.max_expression_operators)
}

/// Check a parsed condition or filter in the service's order: its placeholders
/// and operand types (see [`validate_placeholders_and_operands`]), then its
/// operator count. Measured 2026-11-19 (us-east-1) on Scan, Query, PutItem,
/// UpdateItem and DeleteItem: with 151 `#v = :v` terms joined by OR, an added
/// `OR #v = :zz` with `:zz` undefined answers the undefined value, `OR #x =
/// :v` the undefined name, `OR #v < :b` with `:b` a BOOL and `OR
/// begins_with(#v, :n)` with `:n` an N the operand type, and an unused `:u`
/// the count.
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
    check_operator_count(expr, kind, limits)
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
        // The service counts operators on the whole expression before it
        // applies the key condition grammar: `pk = :v OR` + 150 terms answers
        // the count, not the operator refusal, and 151 AND-joined equalities
        // answer the count ahead of the one-condition-per-key check (measured
        // 2026-10-25). The general condition parser supplies the count; when
        // it does not accept the text, the key condition parser reports the
        // error in its own words.
        if let Ok(expr) = parse_condition_from(&tokens, expr, limits.max_expression_depth) {
            check_operator_count(&expr, ExpressionKind::KeyCondition, limits)?;
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

    // Operator cap placement, measured 2026-10-25 (us-east-1).

    fn chain(n: usize) -> String {
        vec!["#v = :v"; n].join(" OR ")
    }

    /// Resolve `expr` with `#v` defined, `:v` an S, `:b` a BOOL and `:n` an N.
    fn resolve(expr: &str) -> Result<(Option<Expr>, ExpressionMaps), DynamoDbError> {
        let names = HashMap::from([("#v".to_owned(), "v".to_owned())]);
        let values = HashMap::from([
            (":v".to_owned(), AttributeValue::S("x".to_owned())),
            (":b".to_owned(), AttributeValue::Bool(true)),
            (":n".to_owned(), AttributeValue::N("1".to_owned())),
        ]);
        resolve_condition(
            Some(expr),
            Some(&names),
            Some(&values),
            None,
            None,
            &LimitsConfig::default(),
        )
    }

    fn resolve_err(expr: &str) -> String {
        match resolve(expr) {
            Err(DynamoDbError::ValidationException(msg)) => msg,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn count_over_the_cap_is_refused_with_the_full_count() {
        // 151 comparisons and 150 ORs: 301 [a30].
        assert_eq!(
            resolve_err(&chain(151)),
            "Invalid ConditionExpression: The expression contains too many operators; operator count: 301"
        );
        // 150 comparisons and 149 ORs: 299, accepted [a30].
        assert!(resolve(&chain(150)).is_ok());
    }

    #[test]
    fn placeholder_and_operand_faults_beat_the_count() {
        // The service's answers of 2026-11-19 (us-east-1) on PutItem; [p17] of
        // 2026-10-25 for the BOOL operand.
        let rows = [
            (
                "p01",
                format!("{} OR #v = :zz", chain(151)),
                "An expression attribute value used in expression is not defined; attribute value: :zz",
            ),
            (
                "p02",
                format!("{} OR #x = :v", chain(151)),
                "An expression attribute name used in the document path is not defined; attribute name: #x",
            ),
            (
                "p03",
                format!("{} OR begins_with(#v, :n)", chain(151)),
                "Incorrect operand type for operator or function; operator or function: begins_with, operand type: N",
            ),
            (
                "p17",
                format!("{} OR #v < :b", chain(150)),
                "Incorrect operand type for operator or function; operator or function: <, operand type: BOOL",
            ),
        ];
        for (case, expr, want) in rows {
            assert_eq!(
                resolve_err(&expr),
                format!("Invalid ConditionExpression: {want}"),
                "{case}"
            );
        }
    }

    #[test]
    fn an_undefined_value_in_a_branch_evaluation_skips_is_refused() {
        // [p04] measured 2026-11-19: `attribute_not_exists(pk)` holds for an
        // absent item, and the service still refuses `:zz`.
        assert_eq!(
            resolve_err("attribute_not_exists(pk) OR #v = :zz"),
            "Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :zz"
        );
    }

    #[test]
    fn syntax_error_beats_the_count() {
        // [e04] a 300-term chain followed by ` = =`.
        assert_eq!(
            resolve_err(&format!("{} = =", chain(300))),
            "Invalid ConditionExpression: Syntax error; token: \"=\", near: \":v = =\""
        );
    }

    #[test]
    fn key_condition_count_runs_before_the_key_grammar() {
        let limits = LimitsConfig::default();
        // [a62] `pk = :v OR` + 150 terms: 301, the count, not the OR refusal.
        let expr = format!("pk = :v OR {}", vec!["#v = :v"; 150].join(" AND "));
        let err = parse_key_condition_expr(&expr, &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "Invalid KeyConditionExpression: The expression contains too many operators; operator count: 301"),
            "got {err:?}"
        );
        // [a63] two conditions joined by OR under the cap: the key grammar answers.
        let err = parse_key_condition_expr("pk = :v OR sk = :s", &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "Invalid KeyConditionExpression: only AND is supported between key conditions"),
            "got {err:?}"
        );
        // [d23] a syntax error is reported by the key grammar in the service's words.
        let err = parse_key_condition_expr("pk = :v AND", &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "Invalid KeyConditionExpression: Syntax error; token: \"<EOF>\", near: \"AND\""),
            "got {err:?}"
        );
    }

    #[test]
    fn projection_syntax_error_quotes_the_source() {
        // [d27]
        let limits = LimitsConfig::default();
        let err = parse_projection_expr("#v, , #v", &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "Invalid ProjectionExpression: Syntax error; token: \",\", near: \", , #v\""),
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
