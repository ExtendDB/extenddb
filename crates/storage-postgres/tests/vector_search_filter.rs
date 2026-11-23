// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A vector search with a search condition parsed by the extended grammar,
//! on PostgreSQL.
//!
//! The backend reads the rows in score order and keeps the first `top_k`
//! that satisfy the condition, so `top_k` counts matching items only. Equal
//! scores are ordered by the base key. The match semantics are the
//! FilterExpression ones. The two that the cases below turn on, a missing
//! attribute satisfying `<>` and `1E2` equal to `100`, were measured against
//! the live service in eu-north-1 on 2026-11-19, each request twice, with the
//! items of the wire tests.
//!
//! The fixture is in `common/vector_filter_fixture.rs`.

#![cfg(feature = "search-inequality-filters")]

#[path = "common/vector_filter_fixture.rs"]
mod fixture;

use std::sync::Arc;

use extenddb_core::expression::search_filter::parse_search_filter;
use extenddb_core::types::AttributeValue;
use fixture::{Fixture, Matcher, Row, ids, run, search, search_with, values};

/// Query `[1, 0]` under cosine: the distance grows with the second
/// component. `t2` has the vector of `n2`, so the two tie and `n2` ranks
/// first by its key. The `CA` partition holds the nearest vector and a
/// `Price` of 100, so a condition that leaked across partitions would return
/// it.
const ROWS: &[Row<'static>] = &[
    ("n1", "US", 1.0, 0.0, Some("5")),
    ("n2", "US", 1.0, 0.1, Some("10")),
    ("t2", "US", 1.0, 0.1, Some("10")),
    ("n3", "US", 1.0, 0.2, None),
    ("n4", "US", 1.0, 0.3, Some("1E2")),
    ("n5", "US", 1.0, 0.4, Some("15")),
    ("c1", "CA", 1.0, 0.05, Some("50")),
    ("c2", "CA", 1.0, 0.0, Some("100")),
];

async fn ids_for(fixture: &Fixture, expr: &str, top_k: i64) -> Vec<String> {
    let values = values(&[
        ("us", "S:US"),
        ("ca", "S:CA"),
        ("p5", "5"),
        ("p10", "10"),
        ("p100", "100"),
    ]);
    let hits = search(fixture, expr, &values, &[1.0, 0.0], top_k).await;
    ids(&hits).into_iter().map(str::to_owned).collect()
}

#[tokio::test]
async fn conditions_are_evaluated_before_the_top_k_cut() {
    run("COSINE", ROWS, async |f: &Fixture| {
        // unfiltered, the two nearest would be n1 and n2; the cut falls
        // between the tied n2 and t2 and the next match n4
        assert_eq!(
            ids_for(f, "Country = :us AND Price > :p5", 2).await,
            ["n2", "t2"]
        );
        assert_eq!(
            ids_for(f, "Country = :us AND Price >= :p5", 10).await,
            ["n1", "n2", "t2", "n4", "n5"]
        );
        // a missing Price satisfies <>, and 1E2 equals 100
        assert_eq!(
            ids_for(f, "Country = :us AND Price <> :p10", 10).await,
            ["n1", "n3", "n4", "n5"]
        );
        assert_eq!(
            ids_for(
                f,
                "Country = :us AND (attribute_not_exists(Price) OR Price = :p100)",
                10
            )
            .await,
            ["n3", "n4"]
        );
        assert_eq!(
            ids_for(f, "Country = :us AND NOT Price BETWEEN :p5 AND :p10", 10).await,
            ["n3", "n4", "n5"]
        );
    })
    .await;
}

#[tokio::test]
async fn equal_scores_are_cut_in_base_key_order() {
    run("COSINE", ROWS, async |f: &Fixture| {
        assert_eq!(
            ids_for(f, "Country = :us AND Price = :p10", 1).await,
            ["n2"]
        );
        assert_eq!(
            ids_for(f, "Country = :us AND Price = :p10", 2).await,
            ["n2", "t2"]
        );
    })
    .await;
}

#[tokio::test]
async fn the_hash_value_scopes_the_search() {
    run("COSINE", ROWS, async |f: &Fixture| {
        assert_eq!(
            ids_for(f, "Country = :ca AND Price >= :p5", 10).await,
            ["c2", "c1"]
        );
        assert_eq!(
            ids_for(f, "Country = :ca AND Price = :p100", 10).await,
            ["c2"]
        );
        // n4 holds 1E2, and c2 in the other partition holds 100
        assert_eq!(
            ids_for(f, "Country = :us AND Price = :p100", 10).await,
            ["n4"]
        );
    })
    .await;
}

#[tokio::test]
async fn the_partition_scopes_the_search_without_the_condition() {
    run("COSINE", ROWS, async |f: &Fixture| {
        // A condition with no HASH term, so only the partition keeps c2 out.
        let v = values(&[("p100", "100")]);
        let filter = parse_search_filter("Price = :p100", None, Some(&v), 50).expect("parse");
        let us = AttributeValue::S("US".to_owned());
        let hits = search_with(
            f,
            Some(("Country", &us)),
            Some(Arc::new(Matcher(filter))),
            &[1.0, 0.0],
            10,
        )
        .await;
        assert_eq!(ids(&hits), ["n4"]);
    })
    .await;
}
