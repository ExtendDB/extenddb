// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the admin-only console metrics dashboard and its data feed, and
//! for the navigation bar's links to the admin-only pages.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use axum::http::header::LOCATION;
use axum::response::Response;
use extenddb_core::metrics::{LatencySegments, MetricsCollector, MetricsQuery, TimeWindow};
use extenddb_storage::management_store::{MetricsFilter, MetricsRow, MetricsStore, OpResult};
use futures::future::BoxFuture;

use super::{metrics_json, render_metrics_page};
use crate::console::html;
use crate::console::pages::{SessionData, identity_label};
use crate::management::CallerIdentity;

/// A table in each of two accounts, as (account, table). The admin feed reads
/// every account's rows in one store read.
const TABLES: [(&str, &str); 2] = [
    ("111111111111", "orders-account-a"),
    ("222222222222", "orders-account-b"),
];

/// The index every fake row is recorded against, so a test can check that a
/// response carries no index name.
const INDEX: &str = "orders-by-status";

/// Metrics store holding one latency row per table in `TABLES`. It counts
/// reads, so a test can tell whether the handler reached the store.
#[derive(Default)]
struct FakeMetricsStore {
    reads: AtomicUsize,
}

impl MetricsStore for FakeMetricsStore {
    fn insert_metrics(&self, _rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn query_metrics(
        &self,
        _start: time::OffsetDateTime,
        _end: time::OffsetDateTime,
        _filter: &MetricsFilter,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let bucket = time::OffsetDateTime::now_utc() - time::Duration::minutes(2);
        let rows = TABLES
            .iter()
            .map(|(account, table)| MetricsRow {
                bucket,
                account_id: Some((*account).to_owned()),
                metric: "SuccessfulRequestLatency".to_owned(),
                table_name: Some((*table).to_owned()),
                index_name: Some(INDEX.to_owned()),
                operation: Some("GetItem".to_owned()),
                sum: 1_500.0,
                count: 3,
                min: 400.0,
                max: 600.0,
            })
            .collect();
        Box::pin(async move { Ok(rows) })
    }

    fn prune_metrics(&self, _retention: Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn admin() -> CallerIdentity {
    CallerIdentity::Admin("admin".to_owned())
}

fn iam_user() -> CallerIdentity {
    CallerIdentity::IamUser {
        account_id: "444455556666".to_owned(),
        user_name: "bob".to_owned(),
    }
}

fn session(identity: CallerIdentity) -> SessionData {
    SessionData {
        identity,
        csrf_token: "csrf-token".to_owned(),
    }
}

/// A collector holding one in-memory latency segment, for `GetItem`.
fn collector() -> Arc<MetricsCollector> {
    let collector = Arc::new(MetricsCollector::new());
    collector.record_segments(
        TABLES[0].0,
        "GetItem",
        Some(TABLES[0].1),
        LatencySegments {
            dispatch_us: 500.0,
            total_us: 500.0,
            ..LatencySegments::default()
        },
    );
    collector
}

fn all_time() -> MetricsQuery {
    MetricsQuery {
        window: Some(TimeWindow::AllTime),
        ..MetricsQuery::default()
    }
}

async fn body_text(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    String::from_utf8(bytes.to_vec()).expect("UTF-8 response body")
}

#[tokio::test]
async fn iam_user_gets_403_and_no_metrics_from_the_data_feed() {
    let store = FakeMetricsStore::default();

    let response = metrics_json(&iam_user(), &store, &collector(), &all_time()).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = body_text(response).await;
    assert!(!body.contains("TableName"), "403 body: {body}");
    for (account, table) in TABLES {
        assert!(!body.contains(table), "403 body names {table}: {body}");
        assert!(!body.contains(account), "403 body names an account: {body}");
    }
    assert!(!body.contains(INDEX), "403 body names an index: {body}");
    assert!(
        !body.contains("GetItem"),
        "403 body names an operation: {body}"
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        0,
        "the store was read for an IAM-user session"
    );
}

#[tokio::test]
async fn admin_gets_every_accounts_rows_from_the_data_feed() {
    let store = FakeMetricsStore::default();

    let response = metrics_json(&admin(), &store, &collector(), &all_time()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_str(&body_text(response).await).expect("JSON body");
    let tables: Vec<&str> = body["metrics"]
        .as_array()
        .expect("metrics array")
        .iter()
        .filter_map(|m| m["dimensions"].as_array())
        .flatten()
        .filter_map(|d| d["TableName"].as_str())
        .collect();
    let accounts: Vec<&str> = body["metrics"]
        .as_array()
        .expect("metrics array")
        .iter()
        .filter_map(|m| m["dimensions"].as_array())
        .flatten()
        .filter_map(|d| d["AccountId"].as_str())
        .collect();
    for (account, table) in TABLES {
        assert!(
            tables.contains(&table),
            "admin feed lacks {table}: {tables:?}"
        );
        assert!(
            accounts.contains(&account),
            "admin feed lacks the account of {table}"
        );
    }
    let indexes: Vec<&str> = body["metrics"]
        .as_array()
        .expect("metrics array")
        .iter()
        .filter_map(|m| m["dimensions"].as_array())
        .flatten()
        .filter_map(|d| d["GlobalSecondaryIndexName"].as_str())
        .collect();
    assert!(
        indexes.contains(&INDEX),
        "admin feed lacks {INDEX}: {indexes:?}"
    );
    assert_eq!(body["segments"][0]["operation"], "GetItem", "{body}");
}

#[test]
fn iam_user_is_redirected_from_the_metrics_page() {
    let response = render_metrics_page(&session(iam_user()), "test-version");

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|v| v.to_str().ok());
    assert_eq!(location, Some("/console"));
}

#[tokio::test]
async fn admin_gets_the_metrics_page() {
    let response = render_metrics_page(&session(admin()), "test-version");

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_text(response).await;
    assert!(body.contains("<h1>Metrics</h1>"), "{body}");
    assert!(body.contains("/console/metrics-data"), "{body}");
    assert!(!body.contains("accountFilter"), "{body}");
    // The raw metrics table names each row's account.
    assert!(body.contains("<th>Metric</th><th>Account</th><th>Table</th>"));
    assert!(body.contains("d.AccountId"));
}

#[test]
fn nav_bar_links_the_admin_only_pages_for_admins_only() {
    let accounts = r#"<a href="/console/accounts">Accounts</a>"#;
    let admin_links = [
        r#"<a href="/console/metrics">Metrics</a>"#,
        r#"<a href="/console/settings">Settings</a>"#,
        r#"<a href="/console/cache">Cache</a>"#,
    ];
    let spacer = r#"<span class="spacer"></span>"#;

    // An admin's bar lists the three links after Accounts, in this order,
    // and before the spacer.
    let admin_nav = html::nav_bar(&identity_label(&admin()));
    let admin_row = format!("{accounts}\n{}\n{spacer}", admin_links.join("\n"));
    assert!(admin_nav.contains(&admin_row), "{admin_nav}");

    // An IAM user's bar has no Accounts link either: Dashboard comes right
    // before the spacer.
    let dashboard = r#"<a href="/console">Dashboard</a>"#;
    let user_nav = html::nav_bar(&identity_label(&iam_user()));
    for link in admin_links {
        assert!(!user_nav.contains(link), "{user_nav}");
    }
    assert!(!user_nav.contains(accounts), "{user_nav}");
    assert!(
        user_nav.contains(&format!("{dashboard}\n{spacer}")),
        "{user_nav}"
    );
    assert!(user_nav.contains("444455556666/bob"), "{user_nav}");
}
