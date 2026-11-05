// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the ORDER BY of a SELECT.

use super::super::bound_value;
use super::super::tests::{HASH_ONLY, KEYS, select};
use super::*;

/// The refusal an ORDER BY gets on a target keyed on `keys`: of its terms,
/// then of the WHERE it reads. A table and an index get the same checks.
fn refusal(sql: &str, keys: KeyNames<'_>) -> Option<String> {
    let s = select(sql);
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = s
        .where_clause
        .as_ref()
        .map(|w| WhereAnalysis::new(w, keys, &value_of));
    check_order_by_terms(s.order_by.as_ref(), analysis.is_some(), keys)
        .and_then(|()| {
            check_order_by_reads(s.order_by.as_ref(), analysis.as_ref(), keys, &value_of)
        })
        .err()
        .map(|e| e.message().to_owned())
}

/// The index of `an_order_by_names_key_attributes_of_what_is_read`.
const INDEX: KeyNames<'static> = KeyNames {
    partition: "g",
    sort: Some("gs"),
};

const ATTRIBUTE: &str = "in ORDER BY clause must be part of the primary key";
const NO_WHERE: &str = "Must have WHERE clause in the statement when using ORDER BY clause.";
const NOT_KEYED: &str = "Must have at least one non-optional hash key condition in WHERE clause \
                         when using ORDER BY clause.";
const HASH_KEY: &str = "Must have hash key in ORDER BY clause when more than one hash key \
                        condition specified in WHERE clause.";

#[test]
fn an_order_by_names_key_attributes_of_what_is_read() {
    let attribute = |name: &str| Some(format!("Attribute {name} {ATTRIBUTE}"));
    for (sql, want) in [
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY n",
            attribute("n"),
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY PK",
            attribute("PK"),
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY pk, n",
            attribute("n"),
        ),
        // Ahead of the refusal of a WHERE that does not read by key.
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' AND sk <> 5 ORDER BY n",
            attribute("n"),
        ),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY \"pk\"", None),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY sk, sk", None),
    ] {
        assert_eq!(refusal(sql, KEYS), want, "{sql}");
    }
    // On an index, its own keys; the table's partition key is not one.
    assert_eq!(
        refusal(
            "SELECT * FROM \"t\".\"gix\" WHERE g = 'a' ORDER BY pk",
            INDEX
        ),
        attribute("pk")
    );
    assert_eq!(
        refusal(
            "SELECT * FROM \"t\".\"gix\" WHERE g = 'a' ORDER BY g, gs",
            INDEX
        ),
        None
    );
}

#[test]
fn a_path_in_the_list_is_refused_at_its_first_token() {
    let path = |at: &str| {
        Some(format!(
            "Argument to ORDER BY must be of type VariableReference. Type: PATH at {at}"
        ))
    };
    for (sql, want) in [
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY m.x", path("1:43:1")),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY m[0]", path("1:43:1")),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY pk.x", path("1:43:2")),
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY \"m\".\"x\"",
            path("1:43:3"),
        ),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY sk, m.x", path("1:47:1")),
        ("SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY m.x, n", path("1:43:1")),
        // In list order with a name that is not a key attribute.
        (
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY n, m.x",
            Some(format!("Attribute n {ATTRIBUTE}")),
        ),
        // Ahead of the refusal of a WHERE that does not read by key.
        ("SELECT * FROM \"t\" WHERE pk > 'a' ORDER BY m.x", path("1:43:1")),
    ] {
        assert_eq!(refusal(sql, KEYS), want, "{sql}");
    }
}

#[test]
fn an_order_by_needs_a_where_read_by_key() {
    // With no WHERE, ahead of the list.
    for list in ["pk", "n", "m.x"] {
        let sql = format!("SELECT * FROM \"t\" ORDER BY {list}");
        assert_eq!(refusal(&sql, KEYS).as_deref(), Some(NO_WHERE), "{list}");
    }
    for condition in [
        "n = 1",
        "pk > 'a'",
        "pk = 'a' OR n = 1",
        "pk = 'a' AND sk <> 'x'",
        "pk = 'a' AND NOT sk = 'x'",
        "pk = 'a' AND contains(sk, 'x')",
        "pk = 'a' AND sk IS NOT MISSING",
        "pk = 'a' AND size(sk) = 1",
        "pk = 'a' AND sk = 'x' AND sk = 'y'",
        "pk = 'a' AND sk > 'a' AND sk < 'z'",
        "pk IN ['a', 'b'] AND sk <> 'x'",
    ] {
        let sql = format!("SELECT * FROM \"t\" WHERE {condition} ORDER BY sk");
        assert_eq!(
            refusal(&sql, KEYS).as_deref(),
            Some(NOT_KEYED),
            "{condition}"
        );
    }
    let sql = "SELECT * FROM \"t\" WHERE n = 1 ORDER BY pk";
    assert_eq!(refusal(sql, HASH_ONLY).as_deref(), Some(NOT_KEYED));
    for condition in [
        "pk = 'a' AND sk IN ['x', 'y']",
        "pk = 'a' AND (sk = 'x' OR sk = 'y')",
        "pk = 'a' AND m = 'x'",
        "pk = 'a' AND NOT m = 'y'",
    ] {
        let sql = format!("SELECT * FROM \"t\" WHERE {condition} ORDER BY sk");
        assert_eq!(refusal(&sql, KEYS), None, "{condition}");
    }
    // An index read gets the same refusals on its own keys (measured
    // 2026-11-05).
    for (condition, want) in [
        ("g > 'a' ORDER BY g", NOT_KEYED),
        ("g = 'a' OR n = 1 ORDER BY gs", NOT_KEYED),
        ("gs = 'x' ORDER BY gs", NOT_KEYED),
        ("g = 'a' AND gs <> 'x' ORDER BY gs", NOT_KEYED),
        ("g IN ['a', 'b'] ORDER BY gs", HASH_KEY),
    ] {
        let sql = format!("SELECT * FROM \"t\".\"gix\" WHERE {condition}");
        assert_eq!(refusal(&sql, INDEX).as_deref(), Some(want), "{condition}");
    }
    let sql = "SELECT * FROM \"t\".\"gix\" WHERE g IN ['a', 'b'] ORDER BY g, gs";
    assert_eq!(refusal(sql, INDEX), None);
}

#[test]
fn an_order_by_over_several_partitions_names_the_partition_key() {
    for (condition, want) in [
        ("pk IN ['a', 'b'] ORDER BY sk DESC", Some(HASH_KEY)),
        (
            "(pk = 'a' AND sk = 'x') OR pk = 'b' ORDER BY sk DESC",
            Some(HASH_KEY),
        ),
        (
            "(pk = 'a' AND sk = 'x') OR (pk = 'a' AND sk > 'y') ORDER BY sk DESC",
            None,
        ),
        ("pk = 'a' AND sk IN ['x', 'z'] ORDER BY sk DESC", None),
        ("pk IN ['a', 'b']", None),
        // The partition key may be anywhere in the list.
        ("pk IN ['a', 'b'] ORDER BY pk, sk", None),
        ("pk IN ['a', 'b'] ORDER BY sk, pk", None),
        ("(pk = 'a' AND sk = 'x') OR pk = 'b' ORDER BY pk, sk", None),
    ] {
        let sql = format!("SELECT * FROM \"t\" WHERE {condition}");
        assert_eq!(refusal(&sql, KEYS).as_deref(), want, "{condition}");
    }
}

#[test]
fn an_order_by_sets_the_direction_of_partitions_and_of_their_items() {
    let order = |list: &str, keys: KeyNames<'_>| {
        let s = select(&format!(
            "SELECT * FROM \"t\" WHERE pk = 'a' ORDER BY {list}"
        ));
        let d = direction(s.order_by.as_ref(), keys);
        (d.partitions_descending, d.items_descending)
    };
    for (list, partitions, items) in [
        ("pk", false, true),
        ("pk ASC", false, true),
        ("pk DESC", true, true),
        ("pk, pk", false, true),
        ("\"pk\"", false, true),
        ("pk, sk", false, false),
        ("sk", false, false),
        ("sk, pk", false, false),
        ("sk, sk", false, false),
        ("sk DESC", false, true),
        ("pk, sk DESC", false, true),
        ("sk DESC, pk", false, true),
        ("pk DESC, sk", true, false),
        ("pk DESC, sk DESC", true, true),
    ] {
        assert_eq!(order(list, KEYS), (partitions, items), "{list}");
    }
    assert!(order("pk DESC", HASH_ONLY).0);
    assert_eq!(direction(None, KEYS), Direction::default());
}
