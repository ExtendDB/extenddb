// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The grammar and the tree-building checks, before any search schema.
//!
//! Every expected answer was measured against the live service in
//! eu-north-1, each request twice with the same answer. The cases checked
//! through `outcome` were sent on 2026-11-23 to a missing table, with the
//! names and values each case uses; `Ok` stands for a request the service
//! passed to the table lookup. The combined size of substituted names was
//! measured on 2026-11-19 against a vector table, and the unused value
//! message on 2026-11-19 against a vector table and on 2026-11-23 against a
//! missing table, with a parse failure answering ahead of it.

use super::tests_support::{OK, and_chain, assert_outcomes, in_list, n, outcome, p};

const IN_LIST_ATTRIBUTE: &str = "The IN operator list must contain only expression attribute \
                                 values (:value), not attribute names";
const BETWEEN_BOUND_ATTRIBUTE: &str = "The BETWEEN operator bounds must be expression \
                                       attribute values (:value), not attribute names";
const NESTED: &str = "SearchConditionExpression cannot have conditions on nested attributes";
const MULTIPLE_NAMES: &str = "Invalid condition in SearchConditionExpression: Multiple \
                              attribute names used in one condition";
const NO_NAMES: &str =
    "Invalid condition in SearchConditionExpression: No attribute names used in a condition";

fn ordering(operator: &str, operand_type: &str) -> String {
    p(&format!(
        "Incorrect operand type for operator or function; operator or function: {operator}, \
         operand type: {operand_type}"
    ))
}

fn unsupported(function: &str) -> String {
    p(&format!(
        "The function is not allowed in a search condition; function: {function}"
    ))
}

#[test]
fn the_extended_grammar_accepts_every_documented_form() {
    let accepted = [
        "a = :s",
        ":s = a",
        "#x = :s",
        "#y = :s",
        "a = :s AND b = :n",
        "a = :s AND b = :n AND c = :b AND d = :t",
        "(a = :s) AND (b = :n)",
        "a = :s AND (b = :n AND c = :b)",
        // more than one condition on an attribute, a contradiction included
        "a > :n AND a < :n2",
        "a >= :n AND a <= :n2",
        "a = :s AND a = :s2",
        "a = :s AND a = :s",
        "a = :s AND a > :n",
        // the attribute type is unknown here: the schema check refuses these
        "a > :s",
        "a > :b",
        "a <> :t",
        "a = :s OR b > :n",
        "a = :s AND (b > :n OR c = :b)",
        "(a = :s AND b > :n) OR (a = :s2 AND b < :n2)",
        "NOT (a > :n)",
        "NOT a = :s",
        "NOT NOT (a = :s)",
        "a = :s AND NOT (b = :n OR c = :b)",
        "a IN (:s, :s2)",
        "a IN (:s)",
        "a IN (:s, :s)",
        "a IN (:s, :n)",
        "NOT a IN (:s, :s2)",
        "attribute_exists(a)",
        "attribute_not_exists(a)",
        "attribute_exists(a) AND NOT attribute_not_exists(b)",
        "a BETWEEN :n AND :n2",
        "a BETWEEN :n AND :n",
        "a = :s AND b BETWEEN :n AND :n2",
        "a > :n OR b BETWEEN :n AND :n2",
        "NOT a BETWEEN :n AND :n2",
        "a BETWEEN :s2 AND :s",
        // keywords in any case
        "a = :s and (b = :n or not c = :b)",
        "a = :s AnD b bEtWeEn :n aNd :n2",
    ];
    assert_outcomes(50, accepted.iter().map(|e| (*e, OK)));
}

#[test]
fn operands_of_a_condition_are_checked_in_written_order() {
    assert_outcomes(
        50,
        [
            ("a IN (b, :s)", p(IN_LIST_ATTRIBUTE)),
            ("a IN (:s, b)", p(IN_LIST_ATTRIBUTE)),
            // the list is checked before the attribute
            (":s IN (a)", p(IN_LIST_ATTRIBUTE)),
            (":s IN (:s2)", p(NO_NAMES)),
            ("a.b IN (:s)", p(NESTED)),
            ("attribute_exists(a.b)", p(NESTED)),
            ("a BETWEEN b AND :n", p(BETWEEN_BOUND_ATTRIBUTE)),
            ("a BETWEEN :n AND b", p(BETWEEN_BOUND_ATTRIBUTE)),
            (":n BETWEEN a AND :n2", p(BETWEEN_BOUND_ATTRIBUTE)),
            (":n BETWEEN :n AND :n2", p(NO_NAMES)),
            ("a.b BETWEEN :n AND :n2", p(NESTED)),
            // two rules broken: the first condition as written decides
            ("a BETWEEN b AND :n OR c IN (d)", p(BETWEEN_BOUND_ATTRIBUTE)),
            ("c IN (d) OR a BETWEEN b AND :n", p(IN_LIST_ATTRIBUTE)),
            ("a > b", p(MULTIPLE_NAMES)),
            ("a = b", p(MULTIPLE_NAMES)),
            (":n > :n2", p(NO_NAMES)),
            (":s = :s2", p(NO_NAMES)),
            ("a.b > :n", p(NESTED)),
            ("a.b = :s", p(NESTED)),
            ("a[0] < :n", p(NESTED)),
            ("a[0] = :s", p(NESTED)),
        ],
    );
}

#[test]
fn between_bounds_are_checked_while_the_tree_is_built() {
    assert_outcomes(
        50,
        [
            (
                "a BETWEEN :n2 AND :n",
                p(
                    "The BETWEEN operator requires upper bound to be greater than or equal to \
                   lower bound; lower bound operand: AttributeValue: {N:10}, upper bound \
                   operand: AttributeValue: {N:5}",
                ),
            ),
            (
                "a BETWEEN :n AND :s",
                p(
                    "The BETWEEN operator requires same data type for lower and upper bounds; \
                   lower bound operand: AttributeValue: {N:5}, upper bound operand: \
                   AttributeValue: {S:US}",
                ),
            ),
            (
                "a BETWEEN :b AND :s",
                p(
                    "The BETWEEN operator requires same data type for lower and upper bounds; \
                   lower bound operand: AttributeValue: {B:aGk=}, upper bound operand: \
                   AttributeValue: {S:US}",
                ),
            ),
            ("a BETWEEN :t AND :t", ordering("BETWEEN", "BOOL")),
            (
                "a BETWEEN a AND :n",
                p(
                    "The first operand must be distinct from the remaining operands for this \
                   operator or function; operator: BETWEEN, first operand: [a]",
                ),
            ),
        ],
    );
}

/// The bound check compares numbers exactly, while evaluation compares them
/// as binary64: each pair below is equal in binary64 and out of order exactly.
/// The requests sent `1.0000000000000001E+100` and `1E+100`, and
/// `1.0000000000000001E-10` and `1E-10`; the engine passes numbers here in
/// the plain decimal form the service shows. Measured on the live service on
/// 2026-11-23, two requests each.
#[test]
fn between_bounds_compare_numbers_exactly() {
    use std::collections::HashMap;

    use super::parse_search_filter;
    use crate::error::DynamoDbError;

    let out_of_order = |lo: &str, hi: &str| {
        p(&format!(
            "The BETWEEN operator requires upper bound to be greater than or equal to lower \
             bound; lower bound operand: AttributeValue: {{N:{lo}}}, upper bound operand: \
             AttributeValue: {{N:{hi}}}"
        ))
    };
    let e100 = format!("10000000000000001{}", "0".repeat(84));
    let e100_hi = format!("1{}", "0".repeat(100));
    let cases = [
        (
            "9007199254740993",
            "9007199254740992",
            out_of_order("9007199254740993", "9007199254740992"),
        ),
        (
            "99999999999999999999999999999999999999",
            "99999999999999999999999999999999999998",
            out_of_order(
                "99999999999999999999999999999999999999",
                "99999999999999999999999999999999999998",
            ),
        ),
        (
            e100.as_str(),
            e100_hi.as_str(),
            out_of_order(&e100, &e100_hi),
        ),
        (
            "0.10000000000000001",
            "0.1",
            out_of_order("0.10000000000000001", "0.1"),
        ),
        (
            "0.00000000010000000000000001",
            "0.0000000001",
            out_of_order("0.00000000010000000000000001", "0.0000000001"),
        ),
        (
            "-10000000000000000",
            "-10000000000000001",
            out_of_order("-10000000000000000", "-10000000000000001"),
        ),
        ("1.0", "1", OK.to_owned()),
        ("9007199254740992", "9007199254740993", OK.to_owned()),
    ];
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(lo, hi, expected)| {
            let values = HashMap::from([(":lo".to_owned(), n(lo)), (":hi".to_owned(), n(hi))]);
            let actual = match parse_search_filter(
                "Price BETWEEN :lo AND :hi",
                None,
                Some(&values),
                50,
                &crate::limits::LimitsConfig::default(),
            ) {
                Ok(_) => OK.to_owned(),
                Err(DynamoDbError::ValidationException(m)) => m,
                Err(other) => format!("{other:?}"),
            };
            (&actual != expected)
                .then(|| format!("{lo} / {hi}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn ordering_takes_only_string_number_and_binary_values() {
    assert_outcomes(
        50,
        [
            ("a > :t", ordering(">", "BOOL")),
            ("a > :null", ordering(">", "NULL")),
            ("a <= :t", ordering("<=", "BOOL")),
            (":t < a", ordering("<", "BOOL")),
            // checked while the tree is built, before the rules
            ("a > :t OR b = :s", ordering(">", "BOOL")),
        ],
    );
}

#[test]
fn names_and_values_resolve_before_any_rule() {
    assert_outcomes(
        50,
        [
            (
                "a = :missing",
                p(
                    "An expression attribute value used in expression is not defined; attribute \
                   value: :missing",
                ),
            ),
            (
                "a = #missing",
                p(
                    "An expression attribute name used in the document path is not defined; \
                   attribute name: #missing",
                ),
            ),
            (
                "year = :s",
                p("Attribute name is a reserved keyword; reserved keyword: year"),
            ),
            (
                "YEAR = :s",
                p("Attribute name is a reserved keyword; reserved keyword: YEAR"),
            ),
            // the left operand is checked before the comparison
            (
                "year > :t",
                p("Attribute name is a reserved keyword; reserved keyword: year"),
            ),
        ],
    );
}

#[test]
fn syntax_errors_name_the_token_and_the_text_near_it() {
    assert_outcomes(
        50,
        [
            ("", p("The expression can not be empty;")),
            ("a = ", p("Syntax error; token: \"<EOF>\", near: \"= \"")),
            ("a == :s", p("Syntax error; token: \"=\", near: \"== :s\"")),
            ("a = 10", p("Syntax error; token: \"10\", near: \"= 10\"")),
            ("((a = :s))", p("The expression has redundant parentheses;")),
            (
                "\u{feff}attribute_exists(a)",
                p("Syntax error; token: \"\u{feff}\", near: \"\u{feff}attribute_exists\""),
            ),
            (
                "\u{feff}a = :s",
                p("Syntax error; token: \"\u{feff}\", near: \"\u{feff}a\""),
            ),
        ],
    );
}

#[test]
fn only_the_two_existence_functions_are_conditions() {
    assert_outcomes(
        50,
        [
            ("begins_with(a, :s)", unsupported("begins_with")),
            ("contains(a, :s)", unsupported("contains")),
            ("attribute_type(a, :ty)", unsupported("attribute_type")),
            ("size(a) = :n", unsupported("size")),
            ("a = :s OR begins_with(b, :s)", unsupported("begins_with")),
            ("size(a) > :n", unsupported("size")),
            (":n < size(a)", unsupported("size")),
            ("size(a) IN (:n)", unsupported("size")),
            ("a IN (:n, size(b))", unsupported("size")),
            ("size(a) BETWEEN :n AND :n2", unsupported("size")),
            ("a BETWEEN :n AND size(b)", unsupported("size")),
            // the type name is checked while the tree is built
            (
                "attribute_type(a, :s)",
                p("Invalid attribute type name found; type: US, valid types: \
                   {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"),
            ),
        ],
    );
}

#[test]
fn the_shared_grammar_limits_apply() {
    assert_eq!(outcome(&in_list(100)), OK);
    assert_eq!(
        outcome(&in_list(101)),
        p("The IN operator is provided with too many operands; number of operands: 101")
    );
    // 151 comparisons and 150 ANDs: the operator limit answers before the
    // condition limit
    assert_eq!(
        outcome(&and_chain(151)),
        p("The expression contains too many operators; operator count: 301")
    );
    let sized = |len: usize| format!("{} = :s", "a".repeat(len - " = :s".len()));
    assert_eq!(outcome(&sized(4096)), OK);
    assert_eq!(
        outcome(&sized(4097)),
        p("Expression size has exceeded the maximum allowed size;")
    );
    let not_chain = format!("{}a = :s", "NOT ".repeat(1020));
    assert!(not_chain.len() <= 4096);
    assert_eq!(
        outcome(&not_chain),
        p("The expression contains too many operators; operator count: 1021")
    );
}

#[test]
fn not_counts_as_an_operator_and_not_as_a_condition() {
    let with_operators = |count: usize, operators: usize| {
        format!(
            "{}{}",
            "NOT ".repeat(operators - (2 * count - 1)),
            and_chain(count)
        )
    };
    assert_eq!(outcome(&with_operators(50, 300)), OK);
    assert_eq!(
        outcome(&with_operators(50, 301)),
        p("The expression contains too many operators; operator count: 301")
    );
    let not_chain = |count: usize| format!("{}a = :s", "NOT ".repeat(count));
    assert_eq!(outcome(&not_chain(299)), OK);
    assert_eq!(
        outcome(&not_chain(300)),
        p("The expression contains too many operators; operator count: 301")
    );
}

#[test]
fn the_combined_size_counts_each_substituted_name() {
    use std::collections::HashMap;
    let refs: Vec<String> = (0..17).map(|i| format!("#x{i}")).collect();
    let long: HashMap<String, String> = refs
        .iter()
        .map(|r| (r.clone(), format!("{r}{}", "x".repeat(65530))))
        .collect();
    let expr = refs
        .iter()
        .map(|r| format!("{r} = :s"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let values: HashMap<_, _> = [(":s".to_owned(), super::tests_support::s("US"))].into();
    let err = super::parse_search_filter(
        &expr,
        Some(&long),
        Some(&values),
        50,
        &crate::limits::LimitsConfig::default(),
    )
    .unwrap_err();
    assert_eq!(
        super::tests_support::message(err),
        p("Expression size has exceeded the maximum allowed size;")
    );
}

#[test]
fn an_unused_value_is_named_without_the_prefix() {
    use std::collections::HashMap;
    let values: HashMap<String, _> = [
        (":s".to_owned(), super::tests_support::s("US")),
        (":x".to_owned(), super::tests_support::s("unused")),
        (":w".to_owned(), super::tests_support::s("unused")),
    ]
    .into();
    let err = super::parse_search_filter(
        "a > :s",
        None,
        Some(&values),
        50,
        &crate::limits::LimitsConfig::default(),
    )
    .unwrap_err();
    assert_eq!(
        super::tests_support::message(err),
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:w, :x}"
    );
    // a parse failure answers before the unused value
    let err = super::parse_search_filter(
        "a > :s OR",
        None,
        Some(&values),
        50,
        &crate::limits::LimitsConfig::default(),
    )
    .unwrap_err();
    assert_eq!(
        super::tests_support::message(err),
        p("Syntax error; token: \"<EOF>\", near: \"OR\"")
    );
}

/// Measured on the live service on 2026-11-23, two requests each: a bare
/// name, a function name and a path element cannot start with `_`, and the
/// syntax error names the `_` alone. An underscore inside or at the end of a
/// name, and in a `#name` or `:value` reference, is accepted.
#[test]
fn a_name_cannot_start_with_an_underscore() {
    use std::collections::HashMap;

    use super::parse_search_filter;
    use super::tests_support::used_values;
    use crate::error::DynamoDbError;
    use crate::types::AttributeValue;

    let syntax = |token: &str, near: &str| {
        p(&format!(
            "Syntax error; token: \"{token}\", near: \"{near}\""
        ))
    };
    let cases = [
        ("Country = :us AND _Cat = :v", syntax("_", "AND _Cat")),
        ("_ = :v", syntax("_", "_ =")),
        ("_a = :v", syntax("_", "_a")),
        ("__a = :v", syntax("_", "__")),
        ("a._b = :v", syntax("_", "._b")),
        ("attribute_exists(_a)", syntax("_", "(_a")),
        ("_f(a)", syntax("_", "_f")),
        ("size(_a) = :v", syntax("_", "(_a")),
        ("a IN (_b)", syntax("_", "(_b")),
        ("a = _b", syntax("_", "= _b")),
        ("NOT _a = :v", syntax("_", "NOT _a")),
        ("(_a = :v)", syntax("_", "(_a")),
        ("a BETWEEN _b AND :v", syntax("_", "BETWEEN _b")),
        ("1a = :v", syntax("1", "1a")),
        ("a.1b = :v", syntax("1", ".1b")),
        ("a_ = :v", OK.to_owned()),
        ("a_b = :v", OK.to_owned()),
        ("a1 = :v", OK.to_owned()),
        ("#_x = :v", OK.to_owned()),
        ("#_ = :v", OK.to_owned()),
        ("a = :_v", OK.to_owned()),
    ];
    let names: HashMap<String, String> = [("#_x", "Category"), ("#_", "Category")]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let all: HashMap<String, AttributeValue> = [":v", ":us", ":_v"]
        .into_iter()
        .map(|k| (k.to_owned(), AttributeValue::S("x".to_owned())))
        .collect();
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(expr, expected)| {
            let values = used_values(expr, &all);
            let actual = match parse_search_filter(
                expr,
                Some(&names),
                Some(&values),
                50,
                &crate::limits::LimitsConfig::default(),
            ) {
                Ok(_) => OK.to_owned(),
                Err(DynamoDbError::ValidationException(m)) => m,
                Err(other) => format!("{other:?}"),
            };
            (&actual != expected)
                .then(|| format!("{expr:?}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// The expression size limit is `max_expression_length_bytes` of `[limits]`,
/// the key the other expressions read.
#[test]
fn the_expression_size_limit_follows_the_limits_key() {
    use super::parse_search_filter;
    use crate::limits::LimitsConfig;

    let values = super::tests_support::all_values();
    let expr = format!("{} = :s", "a".repeat(5_008 - " = :s".len()));
    let raised = LimitsConfig {
        max_expression_length_bytes: 8192,
        ..LimitsConfig::default()
    };
    let used: std::collections::HashMap<_, _> =
        values.into_iter().filter(|(k, _)| k == ":s").collect();
    assert!(parse_search_filter(&expr, None, Some(&used), 50, &raised).is_ok());
    let err = parse_search_filter(&expr, None, Some(&used), 50, &LimitsConfig::default())
        .err()
        .map(super::tests_support::message);
    assert_eq!(
        err.as_deref(),
        Some(p("Expression size has exceeded the maximum allowed size;").as_str())
    );
}
