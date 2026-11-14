// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Sort key range tests for [`validate_query_start_key_bounds`] and
//! [`page_ends_at_far_end`] (issue #440). Shared helpers live in
//! `start_key_tests.rs`.

use extenddb_core::expression::CompareOp;

use super::tests::{
    AT_MOST_ONE_ROW, EXCLUSIVE_INVALID, OUTSIDE, RANGE_PREDICATE, attr, check, check_with, compare,
    condition, maps, n, ph, s, schema, start_key, with_sk,
};
use super::*;

// --- Range predicate: the start key must satisfy the condition and a later key must be able to ---

fn alice(sk: &str) -> Item {
    start_key(&[("pk", s("alice")), ("sk", s(sk))])
}

/// `pk = alice` plus `sk_condition`, with `:lo`, `:hi` and `:pre` available.
fn run(sk_condition: SortKeyCondition, sk: &str, forward: bool) -> Option<String> {
    let kc = with_sk(condition("pk", "p"), sk_condition);
    let m = maps(
        &[
            ("p", s("alice")),
            ("lo", s("aa")),
            ("hi", s("m")),
            ("v", s("m")),
            ("pre", s("ab")),
        ],
        &[],
    );
    let sch = schema("pk", Some("sk"));
    check_with(
        &alice(sk),
        &kc,
        &m,
        QueryReadShape {
            key_schema: &sch,
            on_index: false,
            forward,
        },
    )
}

fn between() -> SortKeyCondition {
    SortKeyCondition::Between {
        path: attr("sk"),
        low: ph("lo"),
        high: ph("hi"),
    }
}

fn begins(prefix: &str) -> SortKeyCondition {
    SortKeyCondition::BeginsWith {
        path: attr("sk"),
        prefix: ph(prefix),
    }
}

#[test]
fn lower_bound_accepts_the_bound_and_refuses_below_it() {
    // Measured: sk >= m, start key m is accepted; below the bound is refused.
    let ge = || compare("sk", CompareOp::Ge, "v");
    assert_eq!(run(ge(), "m", true), None);
    assert_eq!(run(ge(), "z", true), None);
    assert_eq!(run(ge(), "a", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(ge(), "a", false).as_deref(), Some(RANGE_PREDICATE));
}

#[test]
fn strict_bounds_refuse_the_bound_itself() {
    // Measured: sk > m and sk < m with start key m are both refused.
    let gt = compare("sk", CompareOp::Gt, "v");
    let lt = compare("sk", CompareOp::Lt, "v");
    assert_eq!(run(gt, "m", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(lt, "m", true).as_deref(), Some(RANGE_PREDICATE));
}

#[test]
fn upper_bound_refuses_above_and_at_the_bound() {
    // Measured: sk <= m with a start key above it is refused. At the bound
    // nothing later can match (BETWEEN measured; <= follows it).
    let le = || compare("sk", CompareOp::Le, "v");
    assert_eq!(run(le(), "z", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(le(), "m", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(le(), "a", true), None);
}

#[test]
fn between_refuses_outside_and_at_the_upper_bound_but_accepts_the_lower_bound() {
    // Measured with the range aa..m: a and z refused, aa and ab accepted,
    // m (the upper bound) refused.
    assert_eq!(run(between(), "a", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(between(), "z", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(between(), "aa", true), None);
    assert_eq!(run(between(), "ab", true), None);
    assert_eq!(run(between(), "m", true).as_deref(), Some(RANGE_PREDICATE));
}

#[test]
fn equality_refuses_another_value_with_the_range_sentence_and_the_value_itself_with_the_fourth() {
    // Measured: sk = m, start key a is the range sentence; start key m is
    // "The provided exclusive start key is invalid".
    let eq = || compare("sk", CompareOp::Eq, "v");
    assert_eq!(run(eq(), "a", true).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(eq(), "m", true).as_deref(), Some(EXCLUSIVE_INVALID));
}

#[test]
fn begins_with_needs_the_prefix_on_the_start_key() {
    // Measured: begins_with(sk, ab) with start key a is refused;
    // begins_with(sk, a) with start key a is accepted.
    assert_eq!(
        run(begins("pre"), "a", true).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(run(begins("pre"), "ab", true), None);
    assert_eq!(run(begins("pre"), "abz", true), None);
    let kc = with_sk(condition("pk", "p"), begins("pre"));
    let m = maps(&[("p", s("alice")), ("pre", s("a"))], &[]);
    let sch = schema("pk", Some("sk"));
    assert_eq!(check(&alice("a"), &kc, &m, &sch), None);
}

#[test]
fn reverse_far_end_follows_the_measured_rows() {
    // Measured 2026-10-24, ScanIndexForward false: the far end is the lower
    // bound. sk >= m at m and BETWEEN aa..m at aa get the range sentence;
    // sk <= m at m and BETWEEN at m are accepted.
    let ge = || compare("sk", CompareOp::Ge, "v");
    assert_eq!(run(ge(), "m", false).as_deref(), Some(RANGE_PREDICATE));
    assert_eq!(run(ge(), "z", false), None);
    assert_eq!(
        run(between(), "aa", false).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(run(between(), "m", false), None);
    let le = || compare("sk", CompareOp::Le, "v");
    assert_eq!(run(le(), "m", false), None);
    let gt = || compare("sk", CompareOp::Gt, "v");
    assert_eq!(run(gt(), "m", false).as_deref(), Some(RANGE_PREDICATE));
    // sk = m with the start key itself, either direction.
    let eq = || compare("sk", CompareOp::Eq, "v");
    assert_eq!(run(eq(), "m", false).as_deref(), Some(EXCLUSIVE_INVALID));
}

#[test]
fn reverse_begins_with_at_the_prefix_gets_the_outside_boundaries_sentence() {
    // Measured: begins_with(sk, ab) reverse with start key ab is "outside
    // query boundaries"; abz is accepted; forward at the prefix is accepted;
    // aa, ac, a and m are the range sentence in both directions.
    assert_eq!(run(begins("pre"), "ab", false).as_deref(), Some(OUTSIDE));
    assert_eq!(run(begins("pre"), "abz", false), None);
    assert_eq!(run(begins("pre"), "ab", true), None);
    for sk in ["aa", "ac", "a", "m"] {
        assert_eq!(
            run(begins("pre"), sk, true).as_deref(),
            Some(RANGE_PREDICATE)
        );
        assert_eq!(
            run(begins("pre"), sk, false).as_deref(),
            Some(RANGE_PREDICATE)
        );
    }
}

/// `g = one` plus `sk_condition` on `gs`, a GSI Query; the start key carries
/// the base key too. Bounds: `lo` = a, `hi` = m, `v` = m, `pre` = m.
fn run_index(
    sk_condition: SortKeyCondition,
    gs: &str,
    group: &str,
    forward: bool,
) -> Option<String> {
    let kc = with_sk(condition("g", "g"), sk_condition);
    let m = maps(
        &[
            ("g", s("one")),
            ("lo", s("a")),
            ("hi", s("m")),
            ("v", s("m")),
            ("pre", s("m")),
        ],
        &[],
    );
    let sch = schema("g", Some("gs"));
    check_with(
        &start_key(&[
            ("pk", s("alice")),
            ("sk", s("m")),
            ("g", s(group)),
            ("gs", s(gs)),
        ]),
        &kc,
        &m,
        QueryReadShape {
            key_schema: &sch,
            on_index: true,
            forward,
        },
    )
}

fn on(path: &str, op: CompareOp) -> SortKeyCondition {
    compare(path, op, "v")
}

#[test]
fn index_query_counts_a_repeat_of_the_sort_value_as_a_later_key() {
    // Measured 2026-10-24, GSI: gs <= m forward at m, gs >= m reverse at m,
    // gs BETWEEN a AND m forward at m and reverse at a, begins_with(gs, m)
    // reverse at m, gs = m with the start key itself: all accepted. An index
    // key repeats, so a repeat of the start key's sort value is a later key.
    let between = || SortKeyCondition::Between {
        path: attr("gs"),
        low: ph("lo"),
        high: ph("hi"),
    };
    let begins = SortKeyCondition::BeginsWith {
        path: attr("gs"),
        prefix: ph("pre"),
    };
    assert_eq!(run_index(on("gs", CompareOp::Le), "m", "one", true), None);
    assert_eq!(run_index(on("gs", CompareOp::Ge), "m", "one", false), None);
    assert_eq!(run_index(between(), "m", "one", true), None);
    assert_eq!(run_index(between(), "a", "one", false), None);
    assert_eq!(run_index(begins, "m", "one", false), None);
    assert_eq!(run_index(on("gs", CompareOp::Eq), "m", "one", true), None);
    assert_eq!(run_index(on("gs", CompareOp::Eq), "m", "one", false), None);
}

#[test]
fn index_query_still_refuses_strict_operators_at_the_bound() {
    // Measured: gs > m and gs < m with start key m are the range sentence.
    assert_eq!(
        run_index(on("gs", CompareOp::Gt), "m", "one", true).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(
        run_index(on("gs", CompareOp::Lt), "m", "one", true).as_deref(),
        Some(RANGE_PREDICATE)
    );
}

#[test]
fn index_query_in_another_partition_with_a_range_condition_gets_the_one_row_sentence() {
    // Measured: gs >= a, gs = m (both directions) and gs BETWEEN a AND z with
    // start key two/m on a GSI.
    assert_eq!(
        run_index(on("gs", CompareOp::Ge), "m", "two", true).as_deref(),
        Some(AT_MOST_ONE_ROW)
    );
    assert_eq!(
        run_index(on("gs", CompareOp::Eq), "m", "two", true).as_deref(),
        Some(AT_MOST_ONE_ROW)
    );
    assert_eq!(
        run_index(on("gs", CompareOp::Eq), "m", "two", false).as_deref(),
        Some(AT_MOST_ONE_ROW)
    );
}

#[test]
fn number_sort_keys_compare_numerically() {
    let kc = with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "lo"));
    let m = maps(&[("p", s("alice")), ("lo", n("2"))], &[]);
    let sch = schema("pk", Some("sk"));
    // Measured: sk >= 2 with start key 10 is accepted, with start key 1 refused.
    let ten = start_key(&[("pk", s("alice")), ("sk", n("10"))]);
    assert_eq!(check(&ten, &kc, &m, &sch), None);
    let one = start_key(&[("pk", s("alice")), ("sk", n("1"))]);
    assert_eq!(check(&one, &kc, &m, &sch).as_deref(), Some(RANGE_PREDICATE));
}

#[test]
fn binary_sort_keys_compare_as_unsigned_bytes() {
    let b = |v: &[u8]| AttributeValue::B(v.to_vec());
    let kc = with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "lo"));
    let m = maps(&[("p", s("alice")), ("lo", b(&[0x80]))], &[]);
    let sch = schema("pk", Some("sk"));
    let below = start_key(&[("pk", s("alice")), ("sk", b(&[0x7f]))]);
    assert_eq!(
        check(&below, &kc, &m, &sch).as_deref(),
        Some(RANGE_PREDICATE)
    );
    let above = start_key(&[("pk", s("alice")), ("sk", b(&[0xff]))]);
    assert_eq!(check(&above, &kc, &m, &sch), None);
}

#[test]
fn string_sort_keys_compare_in_utf8_byte_order() {
    let kc = |bound: &str| {
        (
            with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "lo")),
            maps(&[("p", s("alice")), ("lo", s(bound))], &[]),
        )
    };
    let sch = schema("pk", Some("sk"));
    // Measured: sk >= U+00E9 with start key z is refused; sk >= U+FF5E with
    // start key U+10000 is accepted (UTF-16 order would refuse it).
    let (k, m) = kc("\u{e9}");
    assert_eq!(
        check(&alice("z"), &k, &m, &sch).as_deref(),
        Some(RANGE_PREDICATE)
    );
    let (k, m) = kc("\u{ff5e}");
    assert_eq!(check(&alice("\u{10000}"), &k, &m, &sch), None);
}

#[test]
fn sort_key_named_through_an_expression_attribute_name_is_resolved() {
    let kc = with_sk(condition("pk", "p"), compare("#s", CompareOp::Ge, "lo"));
    let m = maps(&[("p", s("alice")), ("lo", s("m"))], &[("s", "sk")]);
    let sch = schema("pk", Some("sk"));
    assert_eq!(
        check(&alice("a"), &kc, &m, &sch).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(check(&alice("n"), &kc, &m, &sch), None);
}

// --- Order between the checks ---

#[test]
fn outside_the_range_wins_over_another_partition() {
    // Measured: sk >= m with start key {bob, a} is the range sentence.
    let kc = with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "lo"));
    let m = maps(&[("p", s("alice")), ("lo", s("m"))], &[]);
    let key = start_key(&[("pk", s("bob")), ("sk", s("a"))]);
    assert_eq!(
        check(&key, &kc, &m, &schema("pk", Some("sk"))).as_deref(),
        Some(RANGE_PREDICATE)
    );
}

/// `pk = alice` plus `sk_condition`, the start key in partition `bob`. Bounds:
/// `a`, `z`, `m`, and `pre` = `a`.
fn run_bob(sk_condition: SortKeyCondition, sk: &str, forward: bool) -> Option<String> {
    let kc = with_sk(condition("pk", "p"), sk_condition);
    let m = maps(
        &[
            ("p", s("alice")),
            ("a", s("a")),
            ("z", s("z")),
            ("m", s("m")),
            ("lo", s("aa")),
            ("hi", s("m")),
            ("pre", s("a")),
        ],
        &[],
    );
    let sch = schema("pk", Some("sk"));
    check_with(
        &start_key(&[("pk", s("bob")), ("sk", s(sk))]),
        &kc,
        &m,
        QueryReadShape {
            key_schema: &sch,
            on_index: false,
            forward,
        },
    )
}

#[test]
fn another_partition_inside_the_range_gets_the_one_row_sentence_for_every_operator() {
    // Measured 2026-10-24, forward: sk >= a, sk > a, sk <= z, sk < z,
    // begins_with(sk, a) with start key bob/aa, BETWEEN a AND z, sk = m with
    // start key bob/m. Reverse: sk >= a with bob/m, begins_with(sk, a) with
    // bob/ab.
    let cases = [
        (compare("sk", CompareOp::Ge, "a"), "m", true),
        (compare("sk", CompareOp::Gt, "a"), "m", true),
        (compare("sk", CompareOp::Le, "z"), "m", true),
        (compare("sk", CompareOp::Lt, "z"), "m", true),
        (begins("pre"), "aa", true),
        (
            SortKeyCondition::Between {
                path: attr("sk"),
                low: ph("a"),
                high: ph("z"),
            },
            "m",
            true,
        ),
        (compare("sk", CompareOp::Eq, "m"), "m", true),
        (compare("sk", CompareOp::Eq, "m"), "m", false),
        (compare("sk", CompareOp::Ge, "a"), "m", false),
        (begins("pre"), "ab", false),
    ];
    for (condition, sk, forward) in cases {
        assert_eq!(
            run_bob(condition, sk, forward).as_deref(),
            Some(AT_MOST_ONE_ROW)
        );
    }
}

#[test]
fn another_partition_at_the_far_end_or_outside_the_range_gets_the_range_sentence() {
    // Measured: BETWEEN aa..m and sk <= m with bob/m (the bound itself), sk >=
    // m reverse with bob/a, sk = m with bob/a and bob/z.
    assert_eq!(
        run_bob(between(), "m", true).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(
        run_bob(compare("sk", CompareOp::Le, "hi"), "m", true).as_deref(),
        Some(RANGE_PREDICATE)
    );
    assert_eq!(
        run_bob(compare("sk", CompareOp::Ge, "m"), "a", false).as_deref(),
        Some(RANGE_PREDICATE)
    );
    for sk in ["a", "z"] {
        assert_eq!(
            run_bob(compare("sk", CompareOp::Eq, "m"), sk, true).as_deref(),
            Some(RANGE_PREDICATE)
        );
    }
    // Reverse sk >= m with bob/z is inside the range: the one row sentence.
    assert_eq!(
        run_bob(compare("sk", CompareOp::Ge, "m"), "z", false).as_deref(),
        Some(AT_MOST_ONE_ROW)
    );
}

#[test]
fn another_partition_without_a_sort_key_condition_keeps_the_outside_boundaries_sentence() {
    let kc = condition("pk", "p");
    let m = maps(&[("p", s("alice"))], &[]);
    let key = start_key(&[("pk", s("bob")), ("sk", s("m"))]);
    for forward in [true, false] {
        let sch = schema("pk", Some("sk"));
        let shape = QueryReadShape {
            key_schema: &sch,
            on_index: false,
            forward,
        };
        assert_eq!(check_with(&key, &kc, &m, shape).as_deref(), Some(OUTSIDE));
    }
}

#[test]
fn a_sort_key_bound_of_another_type_or_undefined_is_left_to_the_existing_handling() {
    let sch = schema("pk", Some("sk"));
    let mistyped = with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "lo"));
    let m = maps(&[("p", s("alice")), ("lo", n("1"))], &[]);
    assert_eq!(check(&alice("a"), &mistyped, &m, &sch), None);
    let undefined = with_sk(condition("pk", "p"), compare("sk", CompareOp::Ge, "nope"));
    let m = maps(&[("p", s("alice"))], &[]);
    assert_eq!(check(&alice("a"), &undefined, &m, &sch), None);
}

// --- A page that ends at the far end carries no key ---

fn page_end(
    sk_condition: Option<SortKeyCondition>,
    sk: &str,
    forward: bool,
    on_index: bool,
) -> bool {
    let mut kc = condition("pk", "p");
    kc.sk_condition = sk_condition;
    let m = maps(
        &[
            ("p", s("alice")),
            ("lo", s("aa")),
            ("hi", s("m")),
            ("v", s("m")),
            ("pre", s("ab")),
        ],
        &[],
    );
    let sch = schema("pk", Some("sk"));
    page_ends_at_far_end(
        &alice(sk),
        &kc,
        &m,
        QueryReadShape {
            key_schema: &sch,
            on_index,
            forward,
        },
    )
}

#[test]
fn a_page_ending_at_the_upper_bound_going_forward_has_no_key() {
    // Measured: BETWEEN s1 AND s3 with Limit 3 over three matches returns no key.
    assert!(page_end(Some(between()), "m", true, false));
    assert!(page_end(
        Some(compare("sk", CompareOp::Le, "v")),
        "m",
        true,
        false
    ));
    assert!(page_end(
        Some(compare("sk", CompareOp::Eq, "v")),
        "m",
        true,
        false
    ));
    assert!(!page_end(Some(between()), "ab", true, false));
}

#[test]
fn a_page_ending_at_the_lower_bound_going_in_reverse_has_no_key() {
    assert!(page_end(Some(between()), "aa", false, false));
    assert!(page_end(
        Some(compare("sk", CompareOp::Ge, "v")),
        "m",
        false,
        false
    ));
    assert!(!page_end(Some(between()), "ab", false, false));
}

#[test]
fn a_page_going_the_other_way_or_without_an_end_keeps_its_key() {
    // The lower bound is the start of a forward walk, not its end.
    assert!(!page_end(Some(between()), "aa", true, false));
    assert!(!page_end(
        Some(compare("sk", CompareOp::Ge, "v")),
        "m",
        true,
        false
    ));
    assert!(!page_end(
        Some(compare("sk", CompareOp::Gt, "v")),
        "m",
        true,
        false
    ));
    assert!(!page_end(None, "m", true, false));
}

#[test]
fn an_index_page_keeps_its_key_at_the_bound() {
    assert!(!page_end(Some(between()), "m", true, true));
}

#[test]
fn a_reverse_page_ending_at_the_prefix_has_no_key() {
    // Measured: a reverse walk over begins_with(sk, a) with Limit 1 ends on
    // the item a without a trailing key.
    assert!(page_end(Some(begins("pre")), "ab", false, false));
    assert!(!page_end(Some(begins("pre")), "abz", false, false));
    assert!(!page_end(Some(begins("pre")), "ab", true, false));
}

#[test]
fn a_hash_only_table_page_never_carries_a_key() {
    let kc = condition("pk", "p");
    let m = maps(&[("p", s("alice"))], &[]);
    let sch = schema("pk", None);
    let shape = QueryReadShape {
        key_schema: &sch,
        on_index: false,
        forward: true,
    };
    assert!(page_ends_at_far_end(
        &start_key(&[("pk", s("alice"))]),
        &kc,
        &m,
        shape
    ));
    let on_index = QueryReadShape {
        on_index: true,
        ..shape
    };
    assert!(!page_ends_at_far_end(
        &start_key(&[("pk", s("alice"))]),
        &kc,
        &m,
        on_index
    ));
}
