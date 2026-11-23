// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The conditions a search condition records, and the condition limit.
//!
//! The recorded conditions are internal and have no answer of their own on
//! the wire. Every answer under the limit of 50 was measured against the live
//! service in eu-north-1, on an account with the extended grammar, on
//! 2026-11-23, each request twice with the same answer, against a missing
//! table; `Ok` stands for a request the service passed to the table lookup.
//! A limit of 1 or 3 exists only in these tests.

use super::rules::{Condition, Operator};
use super::tests_support::{
    OK, and_chain, assert_outcomes, in_list, n, outcome_with, p, parse_with, s,
};
use crate::types::AttributeValue;

fn cond(
    attribute: &str,
    operator: Operator,
    values: impl IntoIterator<Item = AttributeValue>,
    must_match: bool,
) -> Condition {
    Condition {
        attribute: attribute.to_owned(),
        operator,
        values: values.into_iter().collect(),
        must_match,
    }
}

fn too_many(count: usize, max: usize) -> String {
    p(&format!(
        "The expression contains too many conditions; number of conditions: {count}, \
         maximum: {max}"
    ))
}

#[test]
fn each_comparator_is_recorded_as_written_on_either_side() {
    use Operator::{Eq, Ge, Gt, Le, Lt, Ne};
    for (symbol, operator) in [
        ("=", Eq),
        ("<>", Ne),
        ("<", Lt),
        ("<=", Le),
        (">", Gt),
        (">=", Ge),
    ] {
        for expr in [format!("a {symbol} :n"), format!(":n {symbol} a")] {
            let filter = parse_with(&expr, 50).unwrap();
            assert_eq!(
                filter.conditions(),
                [cond("a", operator, [n("5")], true)],
                "{expr}"
            );
        }
    }
}

#[test]
fn each_condition_is_recorded_with_its_position() {
    use Operator::{Between, Eq, Exists, Gt, In, NotExists};
    let (us, ca, five, ten) = (s("US"), s("CA"), n("5"), n("10"));
    let cases: Vec<(&str, Vec<Condition>)> = vec![
        (
            "a = :s AND (b > :n OR c IN (:s, :s2))",
            vec![
                cond("a", Eq, [us.clone()], true),
                cond("b", Gt, [five.clone()], false),
                cond("c", In, [us.clone(), ca.clone()], false),
            ],
        ),
        (
            "a = :s AND b = :n AND c = :s",
            vec![
                cond("a", Eq, [us.clone()], true),
                cond("b", Eq, [five.clone()], true),
                cond("c", Eq, [us.clone()], true),
            ],
        ),
        // AND binds tighter than OR, so nothing here must match
        (
            "a = :s AND b = :n OR c = :s",
            vec![
                cond("a", Eq, [us.clone()], false),
                cond("b", Eq, [five.clone()], false),
                cond("c", Eq, [us.clone()], false),
            ],
        ),
        ("NOT a = :s", vec![cond("a", Eq, [us.clone()], false)]),
        ("NOT NOT (a = :s)", vec![cond("a", Eq, [us.clone()], false)]),
        ("a IN (:s)", vec![cond("a", In, [us.clone()], true)]),
        (
            "a BETWEEN :n AND :n2",
            vec![cond("a", Between, [five.clone(), ten.clone()], true)],
        ),
        (
            "a = :s AND NOT b BETWEEN :n AND :n2",
            vec![
                cond("a", Eq, [us.clone()], true),
                cond("b", Between, [five.clone(), ten.clone()], false),
            ],
        ),
        (
            "attribute_exists(a) AND NOT attribute_not_exists(b)",
            vec![
                cond("a", Exists, Vec::new(), true),
                cond("b", NotExists, Vec::new(), false),
            ],
        ),
        // a #name is recorded as the attribute it names
        (
            ":s = #x AND b = :n",
            vec![
                cond("a", Eq, [us.clone()], true),
                cond("b", Eq, [five.clone()], true),
            ],
        ),
    ];
    for (expr, expected) in cases {
        assert_eq!(
            parse_with(expr, 50).unwrap().conditions(),
            expected,
            "{expr}"
        );
    }
}

#[test]
fn the_condition_limit_counts_the_whole_expression() {
    assert_eq!(outcome_with(&and_chain(50), 50), OK);
    assert_eq!(outcome_with(&and_chain(51), 50), too_many(51, 50));
    assert_eq!(outcome_with(&and_chain(52), 50), too_many(52, 50));
    assert_eq!(outcome_with(&and_chain(60), 50), too_many(60, 50));
}

#[test]
fn comparisons_between_in_and_functions_count_one_each() {
    // conditions on one attribute count too
    assert_outcomes(
        3,
        [
            ("a > :n AND a < :n2 AND b = :s", OK.to_owned()),
            ("a > :n AND a < :n2 AND a <> :n AND b = :s", too_many(4, 3)),
            ("a BETWEEN :n AND :n2 AND b = :s AND c = :s", OK.to_owned()),
            (
                "a BETWEEN :n AND :n2 AND b = :s AND c = :s AND d = :s",
                too_many(4, 3),
            ),
            (
                "a IN (:s, :s2, :s) AND (attribute_exists(b) OR NOT c = :s)",
                OK.to_owned(),
            ),
            (
                "a IN (:s, :s2, :s) AND (attribute_exists(b) OR NOT c = :s) AND d = :s",
                too_many(4, 3),
            ),
            // conditions past the limit are counted, a BETWEEN among them
            (
                "a IN (:s, :s2, :s) AND (attribute_exists(b) OR NOT c = :s) AND d = :s AND \
                 (e IN (:s) OR NOT f BETWEEN :n AND :n2)",
                too_many(6, 3),
            ),
            // the condition that crosses the limit is checked first
            (
                "a > :n AND a < :n2 AND b = :s AND c = d",
                p(
                    "Invalid condition in SearchConditionExpression: Multiple attribute names \
                   used in one condition",
                ),
            ),
        ],
    );
    // a full IN list still counts one
    assert_eq!(outcome_with(&in_list(100), 1), OK);
}

/// 50 conditions, with a HASH condition first, in the shapes of a search over
/// 18 Number inline filters.
fn conditions_with(count: usize, at: usize, shape: &str) -> String {
    let mut parts = vec!["h = :s".to_owned()];
    for i in 1..count {
        let f = format!("f{:02}", i % 18);
        parts.push(if i + 1 == at {
            shape.replace("{f}", &f)
        } else {
            format!("{f} > :n")
        });
    }
    parts.join(" AND ")
}

#[test]
fn the_limit_with_a_hash_condition_and_mixed_shapes() {
    assert_eq!(outcome_with(&conditions_with(50, 0, ""), 50), OK);
    assert_eq!(
        outcome_with(&conditions_with(51, 0, ""), 50),
        too_many(51, 50)
    );
    // an OR group of 50 after the HASH condition is 51
    let ors = (0..50)
        .map(|i| format!("f{:02} > :n", i % 18))
        .collect::<Vec<_>>()
        .join(" OR ");
    assert_eq!(
        outcome_with(&format!("h = :s AND ({ors})"), 50),
        too_many(51, 50)
    );
    // one IN of 100 values among 50 conditions
    let with_in = conditions_with(50, 2, &format!("{{f}} IN ({})", vec![":n"; 100].join(", ")));
    assert_eq!(outcome_with(&with_in, 50), OK);
    // NOT adds no condition
    let nots = (1..50)
        .map(|_| "NOT f00 = :n".to_owned())
        .collect::<Vec<_>>()
        .join(" AND ");
    assert_eq!(outcome_with(&format!("h = :s AND {nots}"), 50), OK);
    // a BETWEEN counts one, at the 10th, the 50th or past the limit
    let between = "{f} BETWEEN :n AND :n2";
    assert_eq!(outcome_with(&conditions_with(50, 10, between), 50), OK);
    assert_eq!(outcome_with(&conditions_with(50, 50, between), 50), OK);
    assert_eq!(
        outcome_with(&conditions_with(51, 51, between), 50),
        too_many(51, 50)
    );
    // an unsupported function before the limit gets its own message
    let early = conditions_with(51, 2, "begins_with({f}, :s)");
    assert_eq!(
        outcome_with(&early, 50),
        p("The function is not allowed in a search condition; function: begins_with")
    );
}
