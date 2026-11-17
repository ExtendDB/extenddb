// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent check for [`MetricsStore`], run by each backend's own
//! test suite against a real catalog that has run every migration (#728).
//!
//! The check writes rows for two accounts that each have a table of the same
//! name, under the same metric, operation and minute, and one row of the
//! whole deployment. It then reads them back: two accounts' rows are two
//! rows, each with its own account and values, and the account filter
//! returns only that account's rows, never the deployment's.
//!
//! Every row is in one minute 23 hours back, inside the day the server keeps
//! metrics for, and the accounts and the deployment row's operation are made
//! up for the run, so rows another test or an earlier run writes to the same
//! catalog stay out of the reads. [`Expectations`] says whether the store adds
//! two rows of one key together and whether it prunes: the Cassandra store
//! writes a row of the same key over the previous one and leaves expiry to
//! its TTL, so it does neither.

use futures::future::BoxFuture;

use crate::management_store::{MetricsFilter, MetricsRow, MetricsStore};

/// The values the check reads back for a row: (account, table, metric, sum).
type Seen = (Option<String>, Option<String>, String, f64);

/// What a store does beyond keeping accounts apart.
#[derive(Debug, Clone, Copy)]
pub struct Expectations {
    /// Two inserts of one key read back as their sum, whether the store adds
    /// them in one row or keeps two rows.
    pub accumulates: bool,
    /// `prune_metrics` deletes the rows older than the retention it is given.
    pub prunes: bool,
}

/// Runs the check against `store`.
///
/// # Panics
///
/// When the store fails a call or returns rows other than the ones the
/// check expects.
pub async fn run(store: &dyn MetricsStore, expect: Expectations) {
    let bucket = minute_a_day_back();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
        % 1_000_000_000_000;
    let first = format!("{unique:012}");
    let second = format!("{:012}", (unique + 1) % 1_000_000_000_000);
    // The deployment row's key is the run's own, so a second run in the same
    // minute neither adds to it nor reads it.
    let deployment_op = format!("conformance-{unique}");
    let row = |account: Option<&str>, table: Option<&str>, metric: &str, sum: f64| MetricsRow {
        bucket,
        account_id: account.map(ToOwned::to_owned),
        metric: metric.to_owned(),
        table_name: table.map(ToOwned::to_owned),
        index_name: None,
        operation: Some(if account.is_some() {
            "PutItem".to_owned()
        } else {
            deployment_op.clone()
        }),
        sum,
        count: 1,
        min: sum,
        max: sum,
    };
    store
        .insert_metrics(&[
            row(
                Some(&first),
                Some("Orders"),
                "ConsumedWriteCapacityUnits",
                1.0,
            ),
            row(
                Some(&second),
                Some("Orders"),
                "ConsumedWriteCapacityUnits",
                5.0,
            ),
            row(Some(&first), None, "UserErrors", 2.0),
            // Rows that only the table filter or only the metric filter
            // leaves out of the narrow reads below.
            row(
                Some(&second),
                Some("Other"),
                "ConsumedWriteCapacityUnits",
                4.0,
            ),
            row(Some(&second), Some("Orders"), "UserErrors", 6.0),
            row(None, None, "WorkerCycleLatency", 7.0),
        ])
        .await
        .expect("insert_metrics");

    let ours = |rows: Vec<MetricsRow>| -> Vec<Seen> {
        let mut seen: Vec<Seen> = rows
            .into_iter()
            .filter(|r| {
                let mine = match r.account_id.as_deref() {
                    Some(a) => a == first || a == second,
                    None => r.operation.as_deref() == Some(deployment_op.as_str()),
                };
                mine && r.bucket == bucket
            })
            .map(|r| (r.account_id, r.table_name, r.metric, r.sum))
            .collect();
        seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        seen
    };
    let read = |filter: MetricsFilter| -> BoxFuture<'_, Vec<MetricsRow>> {
        Box::pin(async move {
            store
                .query_metrics(bucket, bucket, &filter)
                .await
                .expect("query_metrics")
        })
    };
    let seen = |account: &str, table: Option<&str>, metric: &str, sum: f64| -> Seen {
        (
            Some(account.to_owned()),
            table.map(ToOwned::to_owned),
            metric.to_owned(),
            sum,
        )
    };
    let deployment: Seen = (None, None, "WorkerCycleLatency".to_owned(), 7.0);

    // No filter: every account's rows and the deployment's, apart.
    let all = ours(read(MetricsFilter::default()).await);
    let mut want = vec![
        seen(&first, Some("Orders"), "ConsumedWriteCapacityUnits", 1.0),
        seen(&second, Some("Orders"), "ConsumedWriteCapacityUnits", 5.0),
        seen(&first, None, "UserErrors", 2.0),
        seen(&second, Some("Other"), "ConsumedWriteCapacityUnits", 4.0),
        seen(&second, Some("Orders"), "UserErrors", 6.0),
    ];
    let rows_of_deployment: Vec<&Seen> = all.iter().filter(|s| s.0.is_none()).collect();
    assert_eq!(
        rows_of_deployment,
        [&deployment],
        "the row of the deployment: {all:?}"
    );
    let accounts: Vec<Seen> = all.iter().filter(|s| s.0.is_some()).cloned().collect();
    want.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(accounts, want, "two accounts' rows of one table name");

    // The account filter: that account's rows only, no deployment row.
    let first_only = ours(
        read(MetricsFilter {
            account_id: Some(first.clone()),
            ..MetricsFilter::default()
        })
        .await,
    );
    let mut want_first = vec![
        seen(&first, Some("Orders"), "ConsumedWriteCapacityUnits", 1.0),
        seen(&first, None, "UserErrors", 2.0),
    ];
    want_first.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(first_only, want_first, "the first account's rows");

    // Every filter together.
    let narrow = ours(
        read(MetricsFilter {
            account_id: Some(second.clone()),
            table_name: Some("Orders".to_owned()),
            metric: Some("ConsumedWriteCapacityUnits".to_owned()),
        })
        .await,
    );
    assert_eq!(
        narrow,
        [seen(
            &second,
            Some("Orders"),
            "ConsumedWriteCapacityUnits",
            5.0
        )]
    );

    // The table filter alone, and the metric filter alone.
    let sorted = |mut v: Vec<Seen>| {
        v.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        v
    };
    let by_table = ours(
        read(MetricsFilter {
            table_name: Some("Orders".to_owned()),
            ..MetricsFilter::default()
        })
        .await,
    );
    assert_eq!(
        by_table,
        sorted(vec![
            seen(&first, Some("Orders"), "ConsumedWriteCapacityUnits", 1.0),
            seen(&second, Some("Orders"), "ConsumedWriteCapacityUnits", 5.0),
            seen(&second, Some("Orders"), "UserErrors", 6.0),
        ]),
        "the table filter"
    );
    let by_metric = ours(
        read(MetricsFilter {
            metric: Some("ConsumedWriteCapacityUnits".to_owned()),
            ..MetricsFilter::default()
        })
        .await,
    );
    assert_eq!(
        by_metric,
        sorted(vec![
            seen(&first, Some("Orders"), "ConsumedWriteCapacityUnits", 1.0),
            seen(&second, Some("Orders"), "ConsumedWriteCapacityUnits", 5.0),
            seen(&second, Some("Other"), "ConsumedWriteCapacityUnits", 4.0),
        ]),
        "the metric filter"
    );

    // An account with no rows reads nothing, and empty filters read as none.
    let unknown = read(MetricsFilter {
        account_id: Some(format!("{:012}", (unique + 2) % 1_000_000_000_000)),
        ..MetricsFilter::default()
    })
    .await;
    assert!(unknown.is_empty(), "{unknown:?}");
    let empty = ours(
        read(MetricsFilter {
            account_id: Some(String::new()),
            table_name: Some(String::new()),
            metric: Some(String::new()),
        })
        .await,
    );
    assert_eq!(empty, all, "empty filters read as no filter");

    // A second insert of one key reads back as the sum of the two.
    if expect.accumulates {
        store
            .insert_metrics(&[row(
                Some(&first),
                Some("Orders"),
                "ConsumedWriteCapacityUnits",
                2.0,
            )])
            .await
            .expect("insert_metrics again");
        let rows = read(MetricsFilter {
            account_id: Some(first.clone()),
            table_name: Some("Orders".to_owned()),
            metric: Some("ConsumedWriteCapacityUnits".to_owned()),
        })
        .await;
        let sum: f64 = rows.iter().map(|r| r.sum).sum();
        let count: i64 = rows.iter().map(|r| r.count).sum();
        assert_eq!((sum, count), (3.0, 2), "two inserts of one key: {rows:?}");
    }

    // The prune deletes a row older than the retention and keeps the rows
    // inside it.
    if expect.prunes {
        let old = bucket - time::Duration::hours(25);
        let mut stale = row(Some(&first), Some("Orders"), "UserErrors", 1.0);
        stale.bucket = old;
        store
            .insert_metrics(&[stale])
            .await
            .expect("insert an old row");
        let filter = MetricsFilter {
            account_id: Some(first.clone()),
            ..MetricsFilter::default()
        };
        let before = store
            .query_metrics(old, old, &filter)
            .await
            .expect("query the old row");
        assert_eq!(before.len(), 1, "{before:?}");
        store
            .prune_metrics(std::time::Duration::from_secs(36 * 3600))
            .await
            .expect("prune_metrics");
        let after = store
            .query_metrics(old, old, &filter)
            .await
            .expect("query after the prune");
        assert!(after.is_empty(), "the prune kept {after:?}");
        let kept = ours(read(MetricsFilter::default()).await);
        assert!(
            !kept.is_empty(),
            "the prune deleted rows inside the retention"
        );
    }
}

/// The start of the minute 23 hours back, in whole seconds: every store
/// keeps the bucket at second precision.
fn minute_a_day_back() -> time::OffsetDateTime {
    let t = time::OffsetDateTime::now_utc() - time::Duration::hours(23);
    let secs = t.unix_timestamp();
    time::OffsetDateTime::from_unix_timestamp(secs - secs.rem_euclid(60))
        .expect("a timestamp in range")
}
