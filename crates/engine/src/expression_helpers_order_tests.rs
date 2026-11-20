// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order of the checks around the operator count and the syntax
//! error, as measured on the service.

use super::*;
use extenddb_core::limits::LimitsConfig;

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

/// `#v` is defined; `:v`, `:p` and `:s` are S values.
fn key_maps() -> ExpressionMaps {
    let names = HashMap::from([("#v".to_owned(), "v".to_owned())]);
    let values = HashMap::from([
        (":v".to_owned(), AttributeValue::S("x".to_owned())),
        (":p".to_owned(), AttributeValue::S("p1".to_owned())),
        (":s".to_owned(), AttributeValue::S("s1".to_owned())),
    ]);
    build_expression_maps(Some(&names), Some(&values))
}

#[test]
fn key_condition_count_runs_before_the_key_grammar() {
    let limits = LimitsConfig::default();
    let maps = key_maps();
    // [a62] `pk = :v OR` + 150 terms: 301, the count, not the OR refusal.
    let expr = format!("pk = :v OR {}", vec!["#v = :v"; 150].join(" AND "));
    let err = parse_key_condition_expr(&expr, &maps, &limits).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "Invalid KeyConditionExpression: The expression contains too many operators; operator count: 301"),
        "got {err:?}"
    );
    // [a63] two conditions joined by OR under the cap: the key grammar
    // answers (measured 2026-11-19 as x25).
    let err = parse_key_condition_expr("pk = :v OR sk = :s", &maps, &limits).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "Invalid KeyConditionExpression: Invalid operator used in KeyConditionExpression"),
        "got {err:?}"
    );
    // [d23] a syntax error is reported by the key grammar in the service's words.
    let err = parse_key_condition_expr("pk = :v AND", &maps, &limits).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "Invalid KeyConditionExpression: Syntax error; token: \"<EOF>\", near: \"AND\""),
        "got {err:?}"
    );
}

#[test]
fn key_condition_placeholder_fault_beats_the_count() {
    // [k21] measured 2026-11-20: `#x = :p AND` 150 terms, with `#x`
    // undefined, answers the undefined name, not the count of 301.
    let expr = format!("#x = :p AND {}", vec!["#v = :v"; 150].join(" AND "));
    let err = parse_key_condition_expr(&expr, &key_maps(), &LimitsConfig::default()).unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg)
        if msg == "Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"),
        "got {err:?}"
    );
}

#[test]
fn key_condition_syntax_errors_come_before_the_key_rules() {
    // Measured 2026-11-19 (us-east-1) on Query, probe cases in the first
    // column. A syntax error in the condition grammar is reported first;
    // an OR or a NOT that the grammar accepts is the key rules' refusal.
    let invalid = "Invalid operator used in KeyConditionExpression";
    let syntax =
        |token: &str, near: &str| format!("Syntax error; token: \"{token}\", near: \"{near}\"");
    let rows = [
        ("x20", "NOT pk = :p", invalid.to_owned()),
        ("x21", "pk = :p AND NOT sk = :s", invalid.to_owned()),
        ("k06", "NOT NOT pk = :p", invalid.to_owned()),
        ("k08", "(pk = :p) OR (sk = :s)", invalid.to_owned()),
        ("x22", "pk = :p AND NOT", syntax("<EOF>", "NOT")),
        ("x23", "pk = :p OR", syntax("<EOF>", "OR")),
        ("x24", "pk = :p =", syntax("=", ":p =")),
        ("k01", "pk = :p sk = :s", syntax("sk", ":p sk =")),
        ("k02", "pk = :p AND sk = :s sk", syntax("sk", ":s sk")),
        ("k05", "pk = :p OR OR sk = :s", syntax("OR", "OR OR sk")),
        ("k10", "NOT", syntax("<EOF>", "NOT")),
        ("k11", "pk = :p AND NOT NOT", syntax("<EOF>", "NOT")),
    ];
    let limits = LimitsConfig::default();
    let maps = key_maps();
    for (case, expr, want) in rows {
        let err = parse_key_condition_expr(expr, &maps, &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if *msg == format!("Invalid KeyConditionExpression: {want}")),
            "{case}: got {err:?}"
        );
    }
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
