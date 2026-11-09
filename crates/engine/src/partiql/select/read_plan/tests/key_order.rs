// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the order of the key checks and the ORDER BY refusals of
//! a SELECT on a table.

use super::*;
use extenddb_core::types::ScalarAttributeType::S;

const SCALAR: &str = "Key attributes must be scalars; list random access '[]' and map lookup \
                      '.' are not allowed: Key pk";
const TERM: &str = "Attribute n in ORDER BY clause must be part of the primary key";
const HASH_KEY: &str = "Must have hash key in ORDER BY clause when more than one hash key \
                        condition specified in WHERE clause.";
const OVERLAP: &str = "Overlapping conditions with range keys are not supported in where clause";
const NO_WHERE: &str = "Must have WHERE clause in the statement when using ORDER BY clause.";
const TOO_MANY: &str = "Too many decomposed read operations for a given query.";

/// The refusal `check_keys` gives `SELECT * FROM "t" <tail>` on the pk/sk
/// table with string keys, or `None`.
fn refusal(tail: &str) -> Option<String> {
    let s = select(&format!("SELECT * FROM \"t\" {tail}"));
    let info = table(S);
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = s
        .where_clause
        .as_ref()
        .map(|w| WhereAnalysis::new(w, KEYS, &value_of));
    check_keys(
        analysis.as_ref(),
        s.order_by.as_ref(),
        KEYS,
        &info,
        true,
        &[],
        &value_of,
    )
    .err()
    .map(|e| e.message().to_owned())
}

fn values(n: usize) -> String {
    let values: Vec<String> = (0..n).map(|i| format!("'v{i:03}'")).collect();
    values.join(", ")
}

#[test]
fn the_order_by_refusals_sit_among_the_key_checks() {
    // Measured 2026-11-03 for a permitted caller, each WHERE with ORDER BY n
    // and with ORDER BY sk: a path into a key, then the ORDER BY term, then
    // the key values, then more than one partition key value without the
    // partition key in the list, then overlapping disjuncts.
    let key_type = |name: &str| key_type(name).unwrap();
    let fifty_one = format!("pk IN [{}]", values(51));
    for (condition, by_n, by_sk) in [
        ("pk.x = 1", SCALAR.to_owned(), SCALAR.to_owned()),
        ("pk = 1", TERM.to_owned(), key_type("pk")),
        (
            "(pk = 'a' OR pk = 'a')",
            TERM.to_owned(),
            OVERLAP.to_owned(),
        ),
        (
            "(pk = 'a' OR pk = 'b' OR pk = 'a')",
            TERM.to_owned(),
            HASH_KEY.to_owned(),
        ),
        (&fifty_one, TERM.to_owned(), HASH_KEY.to_owned()),
        ("pk IN ['a', 1]", TERM.to_owned(), key_type("pk")),
        ("pk = 'a' AND sk = 1", TERM.to_owned(), key_type("sk")),
        (
            "pk = 'a' AND (sk = 'x' OR sk > 'a')",
            TERM.to_owned(),
            OVERLAP.to_owned(),
        ),
    ] {
        let shown: String = condition.chars().take(40).collect();
        assert_eq!(
            refusal(&format!("WHERE {condition} ORDER BY n")).as_deref(),
            Some(by_n.as_str()),
            "{shown} ORDER BY n"
        );
        assert_eq!(
            refusal(&format!("WHERE {condition} ORDER BY sk")).as_deref(),
            Some(by_sk.as_str()),
            "{shown} ORDER BY sk"
        );
    }
    assert_eq!(refusal("ORDER BY pk.x").as_deref(), Some(NO_WHERE));
    assert_eq!(refusal("WHERE n = 1 ORDER BY n").as_deref(), Some(TERM));
}

#[test]
fn the_key_checks_without_an_order_by_keep_their_order() {
    // Measured 2026-11-03: a key value of the wrong type ahead of overlap
    // and of the count, a path into a key ahead of a key value.
    let key_type = |name: &str| key_type(name).unwrap();
    for (condition, want) in [
        ("pk = 1 OR pk = 1".to_owned(), key_type("pk")),
        (format!("pk IN [{}, 1]", values(50)), key_type("pk")),
        ("pk.x = 1 AND pk = 1".to_owned(), SCALAR.to_owned()),
        (format!("pk IN [{}]", values(51)), TOO_MANY.to_owned()),
        (
            format!("pk IN [{}] ORDER BY pk", values(51)),
            TOO_MANY.to_owned(),
        ),
    ] {
        let shown: String = condition.chars().take(40).collect();
        assert_eq!(
            refusal(&format!("WHERE {condition}")).as_deref(),
            Some(want.as_str()),
            "{shown}"
        );
    }
    assert_eq!(refusal("WHERE pk IN ['a', 'b'] ORDER BY pk, sk"), None);
}
