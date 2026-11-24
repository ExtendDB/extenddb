// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `?` and `*` in a policy pattern count UTF-16 code units, as IAM does.

use super::*;

/// Measured with the IAM policy simulator, in a Resource pattern: `?`
/// matched the one-unit `\u{e9}` and `\u{65e5}` and `??` did not;
/// against the two-unit `\u{1f600}`, `?` did not match, `??` did and
/// `????` did not. In a `StringLike` condition `p??/*` matched
/// `p\u{1f600}/x` and `p?/*` did not.
#[test]
fn question_mark_takes_one_utf16_unit() {
    assert!(wildcard_match("user-?", "user-\u{e9}"));
    assert!(!wildcard_match("user-??", "user-\u{e9}"));
    assert!(wildcard_match("?", "\u{65e5}"));
    assert!(!wildcard_match("??", "\u{65e5}"));
    assert!(!wildcard_match("???", "\u{65e5}"));
    assert!(wildcard_match("team/??", "team/\u{65e5}\u{672c}"));
    assert!(!wildcard_match("team/?", "team/\u{65e5}\u{672c}"));
    assert!(!wildcard_match("?", "\u{1f600}"));
    assert!(wildcard_match("??", "\u{1f600}"));
    assert!(!wildcard_match("????", "\u{1f600}"));
    assert!(wildcard_match("p??/*", "p\u{1f600}/x"));
    assert!(!wildcard_match("p?/*", "p\u{1f600}/x"));
    assert!(arn_match(
        "arn:aws:dynamodb:us-east-1:123456789012:table/?",
        "arn:aws:dynamodb:us-east-1:123456789012:table/\u{e9}"
    ));
    assert!(wildcard_match_ignore_case("A?C", "a\u{e9}c"));
    assert!(wildcard_match_ignore_case("A??C", "a\u{1f600}c"));
}

#[test]
fn star_backtracks_by_utf16_units() {
    assert!(wildcard_match("*?b", "\u{e9}b"));
    assert!(wildcard_match("a*\u{e9}", "a\u{e9}\u{e9}\u{e9}"));
    assert!(wildcard_match("*\u{e9}?", "x\u{e9}\u{e9}"));
    assert!(wildcard_match("*?", "\u{65e5}"));
    assert!(!wildcard_match("*??", "\u{65e5}"));
    assert!(!wildcard_match("*\u{e9}", "\u{e8}"));
    // Measured with the IAM policy simulator: `*??bc` does not match
    // `\u{65e5}bc`, one unit before `bc`, and matches `\u{1f600}bc`, two
    // units; `*?bc` matches `\u{1f600}bc`.
    assert!(!wildcard_match("*??bc", "\u{65e5}bc"));
    assert!(wildcard_match("*??bc", "\u{1f600}bc"));
    assert!(wildcard_match("*?bc", "\u{1f600}bc"));
}
