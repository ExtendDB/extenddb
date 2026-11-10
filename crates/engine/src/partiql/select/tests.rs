// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the SELECT executor, and the fixtures the tests of its modules share.

use super::*;
use extenddb_core::partiql::ast::Statement;
use extenddb_core::partiql::parse;

pub(super) fn select(sql: &str) -> SelectStatement {
    match parse(sql).unwrap() {
        Statement::Select(s) => s,
        _ => panic!("not a select"),
    }
}

pub(super) const KEYS: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: Some("sk"),
};

pub(super) const HASH_ONLY: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: None,
};

fn projection_refusal(projection: &str) -> Option<String> {
    let sql = format!("SELECT {projection} FROM \"t\" WHERE pk = 'a' AND sk = 'b'");
    check_projection(&select(&sql)).err()
}

#[test]
fn a_projection_names_each_identifier_once() {
    for (projection, key) in [("a, a", "a"), ("m.k, k", "k"), ("a, \"a\"", "a")] {
        assert_eq!(
            projection_refusal(projection),
            Some(format!("Duplicate identifiers in select clause: {key}")),
            "{projection}"
        );
    }
}

#[test]
fn projected_paths_that_overlap_are_refused_later_path_first() {
    for (projection, one, two) in [
        ("m.k, m", "[m]", "[m, k]"),
        ("m, m.k", "[m, k]", "[m]"),
        ("l[1], l", "[l]", "[l, [1]]"),
    ] {
        assert_eq!(
            projection_refusal(projection),
            Some(format!(
                "Two document paths overlap with each other; must remove or rewrite one of \
                 these paths; path one: {one}, path two: {two}"
            )),
            "{projection}"
        );
    }
}

#[test]
fn distinct_projected_paths_pass() {
    for projection in ["*", "a, m.k", "m.k, m.j", "l[1], l[2]", "pk, sk"] {
        assert_eq!(projection_refusal(projection), None, "{projection}");
    }
}
