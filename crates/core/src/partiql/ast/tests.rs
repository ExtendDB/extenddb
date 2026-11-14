// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the statement tree.

use super::{Statement, WhereExpr};
use crate::partiql::parse;

fn count(sql: &str) -> usize {
    parse(sql).unwrap().parameter_count()
}

#[test]
fn the_parameter_count_covers_every_position() {
    for (sql, expected) in [
        ("SELECT * FROM \"t\" WHERE pk = 'a'", 0),
        ("SELECT * FROM \"t\" WHERE pk = ? AND sk BETWEEN ? AND ?", 3),
        (
            "SELECT * FROM \"t\" WHERE pk IN [?, ?] OR begins_with(a, ?)",
            3,
        ),
        ("INSERT INTO \"t\" VALUE {'pk': ?, 'a': 'x', 'b': ?}", 2),
        (
            "UPDATE \"t\" SET a = ?, b = list_append(b, ?) SET ss = set_add(ss, ?) \
             WHERE pk = ? AND NOT c = ?",
            5,
        ),
        ("DELETE FROM \"t\" WHERE pk = ? AND x <> ?", 2),
        ("EXISTS(SELECT * FROM \"t\" WHERE pk = ? AND a = ?)", 2),
        // A `?` inside a string is text.
        ("UPDATE \"t\" SET a = '?' WHERE pk = 'k'", 0),
    ] {
        assert_eq!(count(sql), expected, "{sql}");
    }
}

#[test]
fn a_run_of_nots_comes_off_with_its_parity() {
    let condition = |sql: &str| match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        other => panic!("expected a select, got {other:?}"),
    };
    for (nots, negated) in [(1, true), (2, false), (3, true), (299, true)] {
        let sql = format!("SELECT * FROM \"t\" WHERE {}a = 1", "NOT ".repeat(nots));
        let expr = condition(&sql);
        let (odd, inner) = expr.without_nots();
        assert_eq!(odd, negated, "{nots}");
        assert!(matches!(inner, WhereExpr::Compare(..)), "{nots}");
    }
    let expr = condition("SELECT * FROM \"t\" WHERE a = 1");
    assert_eq!(expr.without_nots(), (false, &expr));
}
