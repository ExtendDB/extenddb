// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the visit order of a read of disjuncts one after another,
//! against the pages measured on the service (2026-11-03), written as the
//! positions of the items each page returns, in order.

use super::*;
use serde_json::json;

/// The pages of a read of `count` disjuncts with `limit`, the positions of
/// `held` held back for a refusal, and the Queries `queries` (position and
/// number of items); every other disjunct names one item that exists. Each
/// page is written as the positions of the items it returns, `p` for the
/// item a point read names and `pxn` for `n` items of the Query at `p`, and
/// the request the refusal ends is `R`. Each request takes the cursor
/// through its NextToken value. A Query reads from the key its last read
/// stopped at, and stops with a key when it reads `Limit` items, as the
/// classic Query does.
fn pages(count: usize, limit: Option<usize>, held: &[usize], queries: &[(usize, usize)]) -> String {
    let held_back: Vec<bool> = (0..count).map(|p| held.contains(&p)).collect();
    let mut out = Vec::new();
    let mut cursor = Some(Cursor::first(count));
    while let Some(sent) = cursor.take() {
        assert!(out.len() < 40, "more than 40 pages: {}", out.join(" | "));
        let token_value = sent.into_value();
        let mut walk = Walk::new(
            Cursor::from_value(&token_value, count).unwrap(),
            &held_back,
            limit,
        );
        let mut page = Vec::new();
        while let Some(visit) = walk.next_read() {
            let position = visit.position;
            let Some(&(_, items)) = queries.iter().find(|(q, _)| *q == position) else {
                page.push(position.to_string());
                walk.record(position, Read::Done(1));
                continue;
            };
            let from = visit
                .start
                .as_ref()
                .and_then(Value::as_u64)
                .map_or(0, |n| usize::try_from(n).unwrap());
            let units = walk.units();
            let take = units.map_or(items - from, |u| u.min(items - from));
            if take > 0 {
                page.push(format!("{position}x{take}"));
            }
            let read = if take > 0 && units == Some(take) {
                Read::Stopped(json!(from + take))
            } else {
                Read::Done(take)
            };
            walk.record(position, read);
        }
        match walk.finish() {
            Ending::Refused => {
                assert!(page.is_empty(), "a refused page read {page:?}");
                out.push("R".to_owned());
            }
            Ending::Last => out.push(format!("[{}]", page.join(" "))),
            Ending::More(next) => {
                out.push(format!("[{}]", page.join(" ")));
                cursor = Some(next);
            }
        }
    }
    out.join(" | ")
}

/// The positions `from` to `to`, written as a page writes them.
fn run(from: usize, to: usize) -> String {
    (from..=to)
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn a_list_is_visited_by_position_modulo_its_bucket_count() {
    // 12 positions fit 16 buckets, 13 take 32.
    let twelve: Vec<usize> = (10..22).collect();
    assert_eq!(
        visit_order(&twelve),
        [16, 17, 18, 19, 20, 21, 10, 11, 12, 13, 14, 15]
    );
    let thirteen: Vec<usize> = (10..23).collect();
    assert_eq!(visit_order(&thirteen), thirteen);
    // One bucket keeps the list's order.
    assert_eq!(visit_order(&[18, 2, 34, 1]), [1, 18, 2, 34]);
    assert_eq!(visit_order(&[2, 18, 1]), [1, 2, 18]);
    // The first page of at most 50 disjuncts takes them in read order.
    for count in [1, 12, 13, 24, 25, 48, 49, 50] {
        let all: Vec<usize> = (0..count).collect();
        assert_eq!(visit_order(&all), all, "{count}");
    }
}

#[test]
fn later_pages_of_point_reads_visit_in_the_measured_order() {
    let first = format!("[{}]", run(0, 9));
    for (count, later) in [
        (17, "[16 10 11 12 13 14 15]"),
        (18, "[16 17 10 11 12 13 14 15]"),
        (20, "[16 17 18 19 10 11 12 13 14 15]"),
        (21, "[16 17 18 19 20 10 11 12 13 14] | [15]"),
        (22, "[16 17 18 19 20 21 10 11 12 13] | [14 15]"),
        (23, "[10 11 12 13 14 15 16 17 18 19] | [20 21 22]"),
        (
            28,
            "[10 11 12 13 14 15 16 17 18 19] | [20 21 22 23 24 25 26 27]",
        ),
        (
            33,
            "[32 10 11 12 13 14 15 16 17 18] | [19 20 21 22 23 24 25 26 27 28] | [29 30 31]",
        ),
        (
            37,
            "[10 11 12 13 14 15 16 17 18 19] | [32 33 34 35 36 20 21 22 23 24] \
             | [25 26 27 28 29 30 31]",
        ),
        (
            41,
            "[10 11 12 13 14 15 16 17 18 19] | [32 33 34 35 36 37 38 39 40 20] \
             | [21 22 23 24 25 26 27 28 29 30] | [31]",
        ),
        (
            47,
            "[10 11 12 13 14 15 16 17 18 19] | [20 21 22 23 24 25 26 27 28 29] \
             | [32 33 34 35 36 37 38 39 40 41] | [42 43 44 45 46 30 31]",
        ),
        (
            49,
            "[10 11 12 13 14 15 16 17 18 19] | [20 21 22 23 24 25 26 27 28 29] \
             | [32 33 34 35 36 37 38 39 40 41] | [48 42 43 44 45 46 30 47 31]",
        ),
        (
            50,
            "[10 11 12 13 14 15 16 17 18 19] | [20 21 22 23 24 25 26 27 28 29] \
             | [32 33 34 35 36 37 38 39 40 41] | [48 49 42 43 44 45 46 30 47 31]",
        ),
    ] {
        let want = format!("{first} | {later}");
        assert_eq!(pages(count, None, &[], &[]), want, "{count}");
    }
}

#[test]
fn pages_with_a_limit_visit_in_the_measured_order() {
    for (count, limit, want) in [
        (
            21,
            3,
            "[0 1 2] | [3 4 5] | [6 7 8] | [16 17 18] | [19 20 9] | [10 11 12] | [13 14 15]",
        ),
        (
            33,
            4,
            "[0 1 2 3] | [4 5 6 7] | [8 9 10 11] | [32 12 13 14] | [15 16 17 18] \
             | [19 20 21 22] | [23 24 25 26] | [27 28 29 30] | [31]",
        ),
        (
            21,
            10,
            "[0 1 2 3 4 5 6 7 8 9] | [16 17 18 19 20 10 11 12 13 14] | [15]",
        ),
        (
            33,
            7,
            "[0 1 2 3 4 5 6] | [7 8 9 10 11 12 13] | [32 14 15 16 17 18 19] \
             | [20 21 22 23 24 25 26] | [27 28 29 30 31]",
        ),
    ] {
        assert_eq!(pages(count, Some(limit), &[], &[]), want, "{count} {limit}");
    }
}

#[test]
fn a_held_back_disjunct_takes_a_visit_on_every_page_that_reaches_it() {
    for (held, want) in [
        (
            0,
            "[1 2 3 4 5 6 7 8 9] | [32 10 11 12 13 14 15 16 17] \
             | [18 19 20 21 22 23 24 25 26] | [27 28 29 30 31] | R",
        ),
        (
            5,
            "[0 1 2 3 4 6 7 8 9] | [32 10 11 12 13 14 15 16 17] \
             | [18 19 20 21 22 23 24 25 26] | [27 28 29 30 31] | R",
        ),
        (
            12,
            "[0 1 2 3 4 5 6 7 8 9] | [32 10 11 13 14 15 16 17 18] \
             | [19 20 21 22 23 24 25 26 27] | [28 29 30 31] | R",
        ),
        (
            20,
            "[0 1 2 3 4 5 6 7 8 9] | [32 10 11 12 13 14 15 16 17 18] \
             | [19 21 22 23 24 25 26 27 28] | [29 30 31] | R",
        ),
        (
            32,
            "[0 1 2 3 4 5 6 7 8 9] | [10 11 12 13 14 15 16 17 18] \
             | [19 20 21 22 23 24 25 26 27] | [28 29 30 31] | R",
        ),
    ] {
        assert_eq!(pages(33, None, &[held], &[]), want, "{held}");
    }
    for (count, limit, held, want) in [
        (
            21,
            None,
            vec![20],
            "[0 1 2 3 4 5 6 7 8 9] | [16 17 18 19 10 11 12 13 14] | [15] | R",
        ),
        (
            21,
            None,
            vec![2],
            "[0 1 3 4 5 6 7 8 9] | [16 17 18 19 20 10 11 12 13] | [14 15] | R",
        ),
        (
            40,
            None,
            vec![39],
            "[0 1 2 3 4 5 6 7 8 9] | [10 11 12 13 14 15 16 17 18 19] \
             | [32 33 34 35 36 37 38 20 21] | [22 23 24 25 26 27 28 29 30] | [31] | R",
        ),
        (
            21,
            Some(5),
            vec![20],
            "[0 1 2 3 4] | [5 6 7 8 9] | [16 17 18 19 10] | [11 12 13 14 15] | R",
        ),
        (
            23,
            Some(4),
            vec![5, 17],
            "[0 1 2 3] | [4 6 7 8] | [9 10 11 12] | [16 18 19 20] | [21 22 13 14] \
             | [15] | R",
        ),
    ] {
        assert_eq!(pages(count, limit, &held, &[]), want, "{count} {held:?}");
    }
}

#[test]
fn a_query_stopped_at_the_limit_goes_on_at_its_place() {
    // Positions 0 to 18, a Query of 21 items at 10, `Limit` 6.
    assert_eq!(
        pages(19, Some(6), &[], &[(10, 21)]),
        "[0 1 2 3 4 5] | [6 7 8 9 10x2] | [16 17 18 10x3] | [10x6] | [10x6] \
         | [10x4 11 12] | [13 14 15]"
    );
    // A Query the page goes on inside is one of its 10 visits: a Query of
    // 13 items at 0 and `Limit` 12 give 12, 10, 10 and 1 items.
    assert_eq!(
        pages(21, Some(12), &[], &[(0, 13)]),
        "[0x12] | [0x1 1 2 3 4 5 6 7 8 9] | [16 17 18 19 20 10 11 12 13 14] | [15]"
    );
}

#[test]
fn several_queries_stopped_inside_each_go_on_from_their_own_key() {
    // A second Query, of 10 items at 16, that the page after the first
    // stopped stops in too, with and without position 17 held back.
    assert_eq!(
        pages(19, Some(6), &[], &[(10, 21), (16, 10)]),
        "[0 1 2 3 4 5] | [6 7 8 9 10x2] | [16x6] | [16x4 17 18] | [10x6] | [10x6] \
         | [10x6] | [10x1 11 12 13 14 15]"
    );
    assert_eq!(
        pages(19, Some(6), &[17], &[(10, 21), (16, 10)]),
        "[0 1 2 3 4 5] | [6 7 8 9 10x2] | [16x6] | [16x4 18 10x1] | [10x6] | [10x6] \
         | [10x6] | [11 12 13 14 15] | R"
    );
}

#[test]
fn a_request_whose_page_visits_only_held_back_disjuncts_is_refused() {
    // Ten held back that sort first: the first request is refused, with and
    // without `Limit` 3, though eleven disjuncts are left that read.
    let first: Vec<usize> = (0..10).collect();
    assert_eq!(pages(21, None, &first, &[]), "R");
    assert_eq!(pages(21, Some(3), &first, &[]), "R");
    // Ten held back at 10 to 19 of 25: the second page's ten visits are
    // theirs, so the second request is refused, though 20 to 24 are left.
    let middle: Vec<usize> = (10..20).collect();
    assert_eq!(pages(25, None, &middle, &[]), "[0 1 2 3 4 5 6 7 8 9] | R");
    // Nine held back and one read make a page.
    let nine: Vec<usize> = (0..9).collect();
    assert_eq!(
        pages(21, None, &nine, &[]),
        "[9] | [10] | [11] | [12] | [13] | [14] | [15] | [16] | [17] | [18 19 20] | R"
    );
    // A page that leaves only disjuncts held back leaves them for the next
    // request, which visits only them and is refused.
    let held_back: Vec<bool> = (0..3).map(|p| p == 2).collect();
    let mut walk = Walk::new(Cursor::first(3), &held_back, None);
    for position in [0, 1] {
        assert_eq!(walk.next_read().unwrap().position, position);
        walk.record(position, Read::Done(1));
    }
    assert!(walk.next_read().is_none());
    let Ending::More(next) = walk.finish() else {
        panic!("the page leaves position 2");
    };
    assert_eq!(next.left, [2]);
    let mut walk = Walk::new(next, &held_back, None);
    assert!(walk.next_read().is_none());
    assert!(matches!(walk.finish(), Ending::Refused));
}

#[test]
fn a_page_that_ends_before_an_item_leaves_it_first_of_those_not_visited() {
    let held_back = vec![false; 4];
    let mut walk = Walk::new(Cursor::first(4), &held_back, None);
    for position in [0, 1] {
        assert_eq!(walk.next_read().unwrap().position, position);
        walk.record(position, Read::Done(1));
    }
    assert_eq!(walk.next_read().unwrap().position, 2);
    walk.record(2, Read::Unread);
    assert!(walk.next_read().is_none());
    let Ending::More(next) = walk.finish() else {
        panic!("the page leaves 2 and 3");
    };
    assert_eq!(next.left, [2, 3]);
}

#[test]
fn a_cursor_round_trips_through_its_token_value() {
    let key = json!({ "pk": { "S": "a" }, "sk": { "S": "x" } });
    let cursor = Cursor {
        left: vec![16, 3, 10, 4],
        after: [(10, key.clone()), (16, json!({ "pk": { "S": "b" } }))]
            .into_iter()
            .collect(),
    };
    let value = cursor.clone().into_value();
    assert_eq!(
        value,
        json!({ "left": [16, 3, 10, 4], "after": [[10, key], [16, { "pk": { "S": "b" } }]] })
    );
    assert_eq!(Cursor::from_value(&value, 17).unwrap(), cursor);
    assert_eq!(
        Cursor::from_value(&Cursor::first(3).into_value(), 3).unwrap(),
        Cursor::first(3)
    );
}

#[test]
fn a_value_that_is_no_cursor_of_the_statement_is_refused() {
    for value in [
        json!({ "left": [0, 3], "after": [] }),
        json!({ "left": [0, 0], "after": [] }),
        json!({ "left": [0, -1], "after": [] }),
        json!({ "left": [0, "1"], "after": [] }),
        json!({ "left": [0, 1], "after": [[2, {}]] }),
        json!({ "left": [0, 1], "after": [[1, {}], [1, {}]] }),
        json!({ "left": [0, 1], "after": [[1]] }),
        json!({ "left": [0, 1] }),
        json!({ "after": [] }),
        json!({ "step": 1 }),
        json!({ "pk": { "S": "a" } }),
    ] {
        let err = Cursor::from_value(&value, 3).unwrap_err();
        assert_eq!(err.message(), "NextToken does not match request", "{value}");
    }
}
