// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A vector search with a search condition that the first page of the
//! ordering does not satisfy, on PostgreSQL.
//!
//! The backend reads the ordering in pages of growing size. These tests use a
//! partition large enough to need three pages, with groups of equal scores
//! that straddle each page boundary, and check the result against the
//! backend's own unfiltered ordering. The fixture is in
//! `common/vector_filter_fixture.rs`.

#![cfg(feature = "search-inequality-filters")]

#[path = "common/vector_filter_fixture.rs"]
mod fixture;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::VectorItemFilter;
use extenddb_storage::error::StorageError;
use fixture::{Fixture, Matcher, Row, ids, parse, run, search, search_with, values};
use sqlx::postgres::PgPoolOptions;

/// Rows in the `US` partition. With `top_k` 10 the first page holds 64 rows
/// and the second 512, so a scan to the end reads three pages.
const ROWS: usize = 600;
/// Rows that share one vector, and so one score. 64 and 576 are not
/// multiples of it, so a group straddles each page boundary.
const GROUP: usize = 7;

/// `iNNN` in the `US` partition at Euclidean distance `NNN / GROUP` from the
/// query, with `Price` `NNN`, and 20 `CA` rows nearer than all of them.
fn rows(ids: &[String]) -> Vec<Row<'_>> {
    let mut rows: Vec<Row<'_>> = ids[..ROWS]
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let price: &str = &id[1..];
            (id.as_str(), "US", (i / GROUP) as f64, 0.0, Some(price))
        })
        .collect();
    rows.extend(
        ids[ROWS..]
            .iter()
            .map(|id| (id.as_str(), "CA", 0.0, 0.0, Some(&id[1..]))),
    );
    rows
}

fn row_ids() -> Vec<String> {
    let mut ids: Vec<String> = (0..ROWS).map(|i| format!("i{i:03}")).collect();
    ids.extend((0..20).map(|i| format!("c{:03}", 590 + i)));
    ids
}

fn us() -> AttributeValue {
    AttributeValue::S("US".to_owned())
}

/// The whole `US` partition, unfiltered, in the backend's order.
async fn ranking(f: &Fixture) -> Vec<(String, f64)> {
    let us = us();
    search_with(f, Some(("Country", &us)), None, &[0.0, 0.0], 10_000).await
}

/// The first `top_k` rows of `ranking` whose `Price` satisfies `keep`.
fn expected(
    ranking: &[(String, f64)],
    keep: impl Fn(u32) -> bool,
    top_k: usize,
) -> Vec<(String, f64)> {
    ranking
        .iter()
        .filter(|(id, _)| keep(id[1..].parse().expect("a numeric id")))
        .take(top_k)
        .cloned()
        .collect()
}

#[tokio::test]
async fn the_cut_is_the_unfiltered_ordering_filtered_across_pages() {
    let names = row_ids();
    run("EUCLIDEAN", &rows(&names), async |f: &Fixture| {
        let all = ranking(f).await;
        // Equal scores in base key order, so the ordering is the ids sorted.
        assert_eq!(ids(&all), ids(&expected(&all, |_| true, ROWS)));
        let mut sorted = ids(&all);
        sorted.sort_unstable();
        assert_eq!(ids(&all), sorted);

        let v = values(&[
            ("us", "S:US"),
            ("a", "64"),
            ("b", "66"),
            ("c", "577"),
            ("p3", "3"),
            ("p590", "590"),
            ("p0", "0"),
            ("e1", "63"),
            ("e2", "575"),
        ]);
        let q = [0.0, 0.0];
        // 64 to 66 tie with 63, the last row of the first page, and 576 to
        // 580 tie with 575, the last row of the second.
        let straddling = "Country = :us AND (Price BETWEEN :a AND :b OR Price >= :c)";
        for top_k in [1, 3, 4, 10] {
            assert_eq!(
                search(f, straddling, &v, &q, top_k).await,
                expected(&all, |p| (64..=66).contains(&p) || p >= 577, top_k as usize),
                "top_k {top_k}"
            );
        }
        // 63 and 575 end the first and the second page, so a page that
        // repeated the row it starts after would keep it twice.
        let page_ends = "Country = :us AND (Price = :e1 OR Price = :e2 OR Price >= :p590)";
        assert_eq!(
            search(f, page_ends, &v, &q, 10).await,
            expected(&all, |p| p == 63 || p == 575 || p >= 590, 10)
        );
        // Rows from every page, and a scan to the end of the partition.
        assert_eq!(
            search(f, "Country = :us AND Price < :p3", &v, &q, 100).await,
            expected(&all, |p| p < 3, 100)
        );
        assert_eq!(
            search(f, "Country = :us AND Price >= :p590", &v, &q, 10).await,
            expected(&all, |p| p >= 590, 10)
        );
        assert_eq!(
            search(f, "Country = :us AND Price >= :p590", &v, &q, 100).await,
            expected(&all, |p| p >= 590, 100)
        );
        assert!(
            search(f, "Country = :us AND Price < :p0", &v, &q, 10)
                .await
                .is_empty()
        );
    })
    .await;
}

/// Deletes rows from the index's data table the first time it is asked to
/// match, then answers as `inner` does.
struct DeletesOnFirstCall {
    inner: Matcher,
    data_url: String,
    ids: Vec<&'static str>,
    done: AtomicBool,
}

impl VectorItemFilter for DeletesOnFirstCall {
    fn matches(&self, item: &Item) -> Result<bool, StorageError> {
        if !self.done.swap(true, Ordering::SeqCst) {
            let (url, ids) = (self.data_url.clone(), self.ids.clone());
            // A separate runtime on a separate thread, so the delete commits
            // while the search waits here between two of its statements.
            std::thread::spawn(move || {
                tokio::runtime::Runtime::new()
                    .expect("a runtime")
                    .block_on(delete_rows(&url, &ids));
            })
            .join()
            .expect("the delete thread");
        }
        self.inner.matches(item)
    }
}

async fn delete_rows(url: &str, ids: &[&str]) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect to the data database");
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::text FROM pg_tables WHERE tablename LIKE '\\_ddb\\_vec\\_%'",
    )
    .fetch_all(&pool)
    .await
    .expect("find the vector table");
    assert_eq!(tables.len(), 1, "{tables:?}");
    let deleted = sqlx::query(&format!(
        "DELETE FROM \"{}\" WHERE item_data -> 'id' ->> 'S' = ANY($1)",
        tables[0]
    ))
    .bind(ids)
    .execute(&pool)
    .await
    .expect("delete the rows");
    assert_eq!(deleted.rows_affected(), ids.len() as u64);
    pool.close().await;
}

#[tokio::test]
async fn every_page_reads_the_snapshot_of_the_first() {
    let names = row_ids();
    run("EUCLIDEAN", &rows(&names), async |f: &Fixture| {
        let v: HashMap<String, AttributeValue> =
            values(&[("a", "64"), ("b", "66"), ("us", "S:US")]);
        let (filter, _) = parse(f, "Country = :us AND Price BETWEEN :a AND :b", &v);
        // i063 ends the first page, whose key the second page starts after;
        // i064 to i066 are the second page's matches.
        let item_filter: Arc<dyn VectorItemFilter> = Arc::new(DeletesOnFirstCall {
            inner: Matcher(filter),
            data_url: f.data_url.clone(),
            ids: vec!["i063", "i064", "i065", "i066"],
            done: AtomicBool::new(false),
        });
        let us = us();
        let hits = search_with(
            f,
            Some(("Country", &us)),
            Some(item_filter),
            &[0.0, 0.0],
            10,
        )
        .await;
        assert_eq!(ids(&hits), ["i064", "i065", "i066"]);
        // A later search sees the delete.
        let hits = search(
            f,
            "Country = :us AND Price BETWEEN :a AND :b",
            &v,
            &[0.0, 0.0],
            10,
        )
        .await;
        assert!(hits.is_empty(), "{hits:?}");
    })
    .await;
}
