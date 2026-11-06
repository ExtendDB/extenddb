// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end behaviour tests for the bulk operation worker, written from
//! the specification: operations started the real way (through storage or
//! through `dispatch`) are claimed and driven to terminal states by the
//! worker entry points, against the same in-memory `SQLite` harness the
//! runner behaviour tests use. The worker's own unit tests pin the claim
//! loop's bookkeeping in isolation; these tests pin the wired path from a
//! started operation to items in the table, a rendered report, a purged
//! record, and the recorded metrics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use extenddb_auth::AuthCacheRegistry;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::{
    Dimension, MetricName, MetricSnapshot, MetricsCollector, MetricsQuery, TimeWindow,
};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::{
    BulkRowMarker, CancellationToken, FinishState, InputFile, NewBulkOperation, ReportKind,
    StartOutcome, StorageEngine, Terminal,
};

use super::runner::{FaultingStorage, Harness, file_of, plain_rows};
use crate::OperationContext;
use crate::bulk_put::arn;
use crate::bulk_put::store::BulkStore;
use crate::bulk_put::worker::{
    bulk_operation_retention_worker, bulk_operation_worker_with_interval,
};

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "123456789012";
const IN_BUCKET: &str = "loads";
/// Must match the bucket and prefix the harness's report readers assume.
const OUT_BUCKET: &str = "reports";
const OUT_PREFIX: &str = "run";
const TABLE: &str = "orders";

fn input_config(prefix: &str) -> Value {
    // An empty prefix is refused by the service (minimum of three
    // characters when present); the whole bucket is read when the member is
    // absent, so an empty argument omits it.
    let mut config = json!({
        "S3Bucket": IN_BUCKET,
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "NONE",
    });
    if !prefix.is_empty() {
        config["S3KeyPrefix"] = json!(prefix);
    }
    config
}

fn both_reports() -> Value {
    json!({
        "S3Bucket": OUT_BUCKET,
        "S3KeyPrefix": OUT_PREFIX,
        "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"],
    })
}

fn item(pk: &str, v: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S("s".to_owned())),
        ("v".to_owned(), AttributeValue::N(v.to_owned())),
    ])
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Start an operation on any table of the harness's account, the way
/// `Harness::start` does for its own table.
async fn start_on(
    storage: &Arc<dyn StorageEngine>,
    table_name: &str,
    table_id: &str,
    files: Vec<InputFile>,
    input: Value,
    output: Value,
) -> String {
    let operation_id = arn::generate_operation_id();
    let total: i64 = files.iter().map(|f| f.size).sum();
    let outcome = storage
        .start_bulk_operation(
            ACCOUNT,
            NewBulkOperation {
                operation_id: operation_id.clone(),
                table_id: table_id.to_owned(),
                table_name: table_name.to_owned(),
                table_arn: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{table_name}"),
                client_token: None,
                client_token_hash: None,
                request_fingerprint: format!("worker-e2e-{table_name}"),
                input_config: input,
                output_config: output,
                input_files: files,
                max_write_request_units: None,
                performance_tier: None,
                start_time_ms: now_ms(),
                total_input_bytes: Some(total),
                initial_failure: None,
                run_context: None,
            },
            50,
        )
        .await
        .expect("start bulk operation");
    assert!(
        matches!(outcome, StartOutcome::Created(_)),
        "the operation must be created"
    );
    operation_id
}

/// Spawn the worker over `storage` with a 100 ms tick, returning the token
/// that stops it and its join handle.
fn spawn_worker(
    h: &Harness,
    storage: Arc<dyn StorageEngine>,
    limits: LimitsConfig,
    metrics: Arc<MetricsCollector>,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_worker_with_interval(
        storage,
        Arc::new(limits),
        Arc::from(REGION),
        Some(Arc::new(BulkStore::filesystem(h.store.root()))),
        None,
        metrics,
        token.clone(),
        Duration::from_millis(100),
    ));
    (token, handle)
}

/// Poll the operation until it leaves the running statuses or the deadline
/// passes; answer its final status, or `None` on timeout.
async fn wait_terminal(h: &Harness, operation_id: &str, deadline: Duration) -> Option<String> {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        let record = h.record(operation_id).await;
        if record.status != "IN_PROGRESS" && record.status != "CANCELLING" {
            return Some(record.status);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

async fn stop_worker(token: CancellationToken, handle: tokio::task::JoinHandle<()>) {
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the worker stops at its next tick")
        .expect("the worker panicked");
}

/// The terminal-transition metrics are recorded after `finish` stores the
/// terminal status and the ledger rows are deleted, and the worker does not
/// await in-flight runs at shutdown, so a test that observed the status
/// flip waits here for the metric to land rather than reading it the same
/// instant.
async fn wait_for_metric(metrics: &MetricsCollector, name: MetricName, deadline: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        let snaps = metrics.query(&MetricsQuery {
            metric: Some(name),
            window: Some(TimeWindow::AllTime),
            ..MetricsQuery::default()
        });
        if !snaps.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{name} was not recorded within {deadline:?}");
}

fn one_snapshot(metrics: &MetricsCollector, name: MetricName) -> MetricSnapshot {
    let mut snaps = metrics.query(&MetricsQuery {
        metric: Some(name),
        window: Some(TimeWindow::AllTime),
        ..MetricsQuery::default()
    });
    assert_eq!(snaps.len(), 1, "exactly one {name} series, got {snaps:?}");
    snaps.remove(0)
}

/// A counter snapshot's sum as the integer it accumulated, so the
/// assertions compare exact integers rather than floats. The workers only
/// ever add whole counts far below 2^53, so the sum is exact and the cast
/// cannot truncate.
fn count(snapshot: &MetricSnapshot) -> u64 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let n = snapshot.sum as u64;
    n
}

/// Start an operation on `table`, claim it, write one Applied ledger row
/// for `(operation, 0, 0)` through a marked put, and finish it COMPLETED
/// with the chosen end time. The terminal transition clears the table's
/// guard, so successive calls claim unambiguously.
async fn start_claim_mark_finish(
    h: &Harness,
    owner: &str,
    table: &str,
    table_id: &str,
    pk: &str,
    end_ms: i64,
) -> String {
    let op = start_on(
        &h.storage,
        table,
        table_id,
        Vec::new(),
        input_config(""),
        both_reports(),
    )
    .await;
    let claimed = h
        .storage
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1, "exactly the new operation is runnable");
    assert_eq!(claimed[0].operation_id, op);
    let ki = h
        .storage
        .table_key_info(ACCOUNT, table)
        .await
        .expect("key info");
    let marker = BulkRowMarker {
        account_id: ACCOUNT.to_owned(),
        operation_id: op.clone(),
        file_index: 0,
        row_index: 0,
        owner: String::new(),
        keep_old: false,
        settled: (0, 0),
    };
    h.storage
        .put_item(
            &ki,
            item(pk, "1"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
            Some(&marker),
        )
        .await
        .expect("marked put");
    let state = h
        .storage
        .finish(
            ACCOUNT,
            &op,
            owner,
            Terminal {
                status: "COMPLETED".to_owned(),
                end_time_ms: end_ms,
                failure_code: None,
                failure_message: None,
                usage: json!({}),
            },
        )
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(_)));
    op
}

mod lifecycle;
mod retention;
