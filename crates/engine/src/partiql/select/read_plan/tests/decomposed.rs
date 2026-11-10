// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the refusal of a statement read by key that splits into
//! more than 50 disjuncts, and where it sits among the other refusals.

use super::*;
use extenddb_core::types::ScalarAttributeType::S;

const TOO_MANY: &str = "Too many decomposed read operations for a given query.";

/// `<prefix>000, <prefix>001, ...`, `n` string values.
fn values(prefix: &str, n: usize) -> String {
    let values: Vec<String> = (0..n).map(|i| format!("'{prefix}{i:03}'")).collect();
    values.join(", ")
}

/// `n` equalities of the partition key ORed.
fn ors(n: usize) -> String {
    let ors: Vec<String> = (0..n).map(|i| format!("pk = 'v{i:03}'")).collect();
    ors.join(" OR ")
}

/// `n` comparisons of other attributes ANDed.
fn comparisons(n: usize) -> String {
    let terms: Vec<String> = (1..=n).map(|i| format!("x{i:02} = 1")).collect();
    terms.join(" AND ")
}

#[test]
fn a_statement_read_by_key_splits_into_at_most_50_disjuncts() {
    let too_many = || Some(TOO_MANY.to_owned());
    let products =
        |a: usize, b: usize| format!("pk IN [{}] AND sk IN [{}]", values("v", a), values("s", b));
    for (condition, want) in [
        (format!("pk IN [{}]", values("v", 50)), None),
        (format!("pk IN [{}]", values("v", 51)), too_many()),
        (ors(50), None),
        (ors(51), too_many()),
        (products(25, 2), None),
        (products(5, 10), None),
        (products(17, 3), too_many()),
        (products(10, 10), too_many()),
        (products(100, 100), too_many()),
        (
            format!("pk IN [{}] AND (sk = 's' OR sk = 't')", values("v", 26)),
            too_many(),
        ),
        (
            format!(
                "(pk IN [{}] AND sk = 's') OR (pk IN [{}] AND sk = 't')",
                values("v", 26),
                values("w", 25)
            ),
            too_many(),
        ),
        (format!("pk = 'v000' AND sk IN [{}]", values("s", 50)), None),
        (
            format!("pk = 'v000' AND sk IN [{}]", values("s", 51)),
            too_many(),
        ),
        (
            format!("pk IN [{}] OR pk IN [{}]", values("v", 26), values("w", 25)),
            too_many(),
        ),
        (
            format!("pk IN [{}] AND sk > 'a'", values("v", 51)),
            too_many(),
        ),
        // The conditions of a filter are not counted.
        (
            format!("pk IN [{}] AND {}", values("v", 50), comparisons(99)),
            None,
        ),
        (
            format!("pk IN [{}] AND {}", values("v", 51), comparisons(1)),
            too_many(),
        ),
        // A statement that is a scan is not counted.
        (format!("pk IN [{}] OR x = 1", values("v", 51)), None),
        (format!("x = 1 OR pk IN [{}]", values("v", 51)), None),
        (format!("({}) OR x = 1", products(100, 100)), None),
    ] {
        let shown: String = condition.chars().take(60).collect();
        assert_eq!(answer(S, &condition, &[]), want, "{shown}");
    }
    let params: Vec<AttributeValue> = (0..51)
        .map(|i| AttributeValue::S(format!("v{i:03}")))
        .collect();
    let marks = |n: usize| vec!["?"; n].join(", ");
    assert_eq!(
        answer(S, &format!("pk IN [{}]", marks(51)), &params),
        too_many()
    );
    assert_eq!(
        answer(S, &format!("pk IN [{}]", marks(50)), &params[..50]),
        None
    );
}

#[test]
fn the_disjuncts_held_back_for_a_refusal_are_counted_before_it() {
    let big = "z".repeat(2049);
    let hash = "One or more parameter values were invalid: Size of hashkey has exceeded the \
                maximum size limit of2048 bytes";
    let filter = "Filter Expression can only contain non-primary key attributes: Primary key \
                  attribute: sk";
    for (condition, want) in [
        (
            format!("pk IN [{}, '{big}']", values("v", 50)),
            Some(TOO_MANY),
        ),
        (
            format!("pk IN ['{big}', {}]", values("v", 50)),
            Some(TOO_MANY),
        ),
        (format!("pk IN [{}, '{big}']", values("v", 49)), None),
        (
            format!("pk IN [{}] AND sk = a", values("v", 51)),
            Some(TOO_MANY),
        ),
        (
            format!("pk IN [{}] AND sk = a", values("v", 50)),
            Some(filter),
        ),
        (format!("pk IN ['{big}']"), Some(hash)),
    ] {
        let shown: String = condition.chars().take(60).collect();
        assert_eq!(answer(S, &condition, &[]).as_deref(), want, "{shown}");
    }
}

#[test]
fn key_values_and_the_overlap_refusal_come_before_the_count() {
    let scalar = "Key attributes must be scalars; list random access '[]' and map lookup '.' \
                  are not allowed: Key sk";
    let overlap = "Overlapping conditions with range keys are not supported in where clause";
    let repeated: Vec<String> = (0..51).map(|i| format!("'v{:03}'", i % 26)).collect();
    for (condition, want) in [
        (format!("pk IN [{}, 1]", values("v", 50)), key_type("pk")),
        (format!("pk IN [1, {}]", values("v", 50)), key_type("pk")),
        (
            format!("pk IN [{}] AND sk = 1", values("v", 51)),
            key_type("sk"),
        ),
        (
            format!("pk IN [{}] AND sk.x = 1", values("v", 51)),
            Some(scalar.to_owned()),
        ),
        (
            format!("pk IN [{}]", repeated.join(", ")),
            Some(overlap.to_owned()),
        ),
        (
            format!("pk IN [{}] AND (sk = 's' OR sk > 'a')", values("v", 26)),
            Some(overlap.to_owned()),
        ),
        (
            format!("pk IN [{}] OR pk IN [{}]", values("v", 26), values("v", 26)),
            Some(overlap.to_owned()),
        ),
    ] {
        let shown: String = condition.chars().take(60).collect();
        assert_eq!(answer(S, &condition, &[]), want, "{shown}");
    }
}
