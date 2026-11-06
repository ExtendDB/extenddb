// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared test fixtures for the worker tests: the store root, the in
//! memory engine, the operation and ledger builders, the run probe, the
//! warning capture subscriber, and the unsupported store double.

mod cancel;
mod claim;
mod config;
mod retention;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::{AttributeValue, CreateTableInput, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage::{
    BoxedFuture as BoxFuture, BulkOperationListResult, BulkOperationRecord, BulkOperationStore,
    BulkRowMarker, CancelOutcome, CancellationToken, FinishState, InputFile, LeaseState,
    ListCursor, NewBulkOperation, Progress, ReportKind, ReportRecord, StartOutcome, StorageEngine,
    Terminal,
};

use crate::bulk_put::arn;
use crate::bulk_put::runner::RunnerError;

const REGION: &str = "us-east-1";
const ACCOUNT_A: &str = "111111111111";
const ACCOUNT_B: &str = "222222222222";
const IN_BUCKET: &str = "loads";

/// A fresh directory acting as a configured bulk store root, removed
/// when the test drops it.
struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("eb-bulk-worker-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir.canonicalize().unwrap())
    }

    fn root(&self) -> Arc<PathBuf> {
        Arc::new(self.0.clone())
    }

    fn file(&self, account: &str, bucket: &str, rel: &str, contents: &[u8]) {
        let path = self.0.join(account).join(bucket).join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// In-memory `SQLite` engine with both test accounts seeded, the same
/// arrangement the runner behaviour tests use.
async fn engine() -> Arc<dyn StorageEngine> {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        REGION,
        ServerComponentsOptions::default(),
    )
    .await
    .expect("server components");
    components
        .catalog_store
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("set control plane delay");
    for (id, name) in [(ACCOUNT_A, "worker-a"), (ACCOUNT_B, "worker-b")] {
        components
            .catalog_store
            .create_account(id, name)
            .await
            .expect("create account");
    }
    components.engine
}

async fn create_table(storage: &Arc<dyn StorageEngine>, account: &str, name: &str) -> String {
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("create table input");
    storage
        .create_table(account, input)
        .await
        .expect("create table")
        .table_id
}

fn input_config() -> Value {
    json!({
        "S3Bucket": IN_BUCKET,
        "S3KeyPrefix": "",
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "NONE",
    })
}

/// No report bucket, so the runner renders nothing beneath the store
/// root.
fn output_config() -> Value {
    json!({ "S3KeyPrefix": "run", "OutputType": ["ERROR"] })
}

#[allow(clippy::too_many_arguments)]
async fn start_op(
    storage: &Arc<dyn StorageEngine>,
    account: &str,
    table_name: &str,
    table_id: &str,
    start_time_ms: i64,
    files: Vec<InputFile>,
) -> String {
    let operation_id = arn::generate_operation_id();
    let total: i64 = files.iter().map(|f| f.size).sum();
    let outcome = storage
        .start_bulk_operation(
            account,
            NewBulkOperation {
                operation_id: operation_id.clone(),
                table_id: table_id.to_owned(),
                table_name: table_name.to_owned(),
                table_arn: format!("arn:aws:dynamodb:{REGION}:{account}:table/{table_name}"),
                client_token: None,
                client_token_hash: None,
                request_fingerprint: "worker-test".to_owned(),
                input_config: input_config(),
                output_config: output_config(),
                input_files: files,
                max_write_request_units: None,
                performance_tier: None,
                start_time_ms,
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

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

async fn key_info(storage: &Arc<dyn StorageEngine>, account: &str, table: &str) -> TableKeyInfo {
    storage
        .table_key_info(account, table)
        .await
        .expect("key info")
}

/// Write one item with an Applied ledger marker for `(operation, 0, 0)`,
/// the way a runner's marked condition write populates the ledger.
async fn write_ledger_row(
    storage: &Arc<dyn StorageEngine>,
    account: &str,
    table: &str,
    operation_id: &str,
) {
    let ki = key_info(storage, account, table).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    let item = Item::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]);
    let marker = BulkRowMarker {
        account_id: account.to_owned(),
        operation_id: operation_id.to_owned(),
        file_index: 0,
        row_index: 0,
        owner: String::new(),
        keep_old: false,
        settled: (0, 0),
    };
    storage
        .put_item(&ki, item, false, None, &maps, None, Some(&marker))
        .await
        .expect("marked put");
}

/// Poll `f` every 10 ms until it answers `true` or the deadline passes.
async fn wait_until<F, Fut>(deadline: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

/// Bookkeeping shared by the claim-loop tests: which operations have
/// started, and a semaphore whose permits release pended runs.
struct RunProbe {
    started: Mutex<Vec<(String, String)>>,
    release: tokio::sync::Semaphore,
}

impl RunProbe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Mutex::new(Vec::new()),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    fn started(&self) -> Vec<(String, String)> {
        self.started.lock().expect("lock").clone()
    }
}

/// A `run_one` whose runs record themselves and pend until released.
fn pending_run(
    probe: Arc<RunProbe>,
) -> impl Fn(BulkOperationRecord, String) -> BoxFuture<'static, Result<(), RunnerError>> + 'static {
    move |record: BulkOperationRecord, _owner: String| {
        let probe = Arc::clone(&probe);
        Box::pin(async move {
            probe
                .started
                .lock()
                .expect("lock")
                .push((record.account_id.clone(), record.operation_id.clone()));
            let permit = probe.release.acquire().await.expect("semaphore open");
            permit.forget();
            Ok(())
        })
    }
}

/// A minimal subscriber that keeps the formatted fields of every WARN
/// event, so a test can assert the clamp warning fired without a
/// subscriber crate.
#[derive(Clone)]
struct CaptureWarnings {
    lines: Arc<Mutex<Vec<String>>>,
}

struct FieldText<'a>(&'a mut Vec<String>);

impl tracing::field::Visit for FieldText<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push(format!("{}={value:?}", field.name()));
    }
}

impl tracing::Subscriber for CaptureWarnings {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut fields = Vec::new();
            event.record(&mut FieldText(&mut fields));
            if let Ok(mut lines) = self.lines.lock() {
                lines.push(fields.join(" "));
            }
        }
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// True when the metric carries a point in the last minute under the
/// given worker source dimension.
fn worker_metric_present(
    metrics: &MetricsCollector,
    metric: extenddb_core::metrics::MetricName,
    source: &str,
) -> bool {
    use extenddb_core::metrics::{Dimension, MetricsQuery, TimeWindow};
    let snapshots = metrics.query(&MetricsQuery {
        metric: Some(metric),
        window: Some(TimeWindow::LastMinute),
        ..MetricsQuery::default()
    });
    snapshots.iter().any(|snapshot| {
        snapshot.count > 0
            && snapshot
                .dimensions
                .iter()
                .any(|d| matches!(d, Dimension::Operation(op) if op == source))
    })
}

/// Refuses every method the way the backend without bulk operations
/// does, and counts the claims so a test can pin how many happened.
struct UnsupportedStore {
    claims: AtomicUsize,
    /// When true, `claim_runnable` answers a non-Validation error
    /// (`Internal`) instead of the unsupported Validation, so a test can
    /// exercise the claim loop's error path rather than its stop path.
    claim_internal: bool,
}

fn unsupported<T>() -> BoxFuture<'static, Result<T, StorageError>> {
    Box::pin(async {
        Err(StorageError::Validation(
            "Bulk operations are not supported on this storage backend".to_owned(),
        ))
    })
}

impl BulkOperationStore for UnsupportedStore {
    fn start_bulk_operation(
        &self,
        _account_id: &str,
        _req: NewBulkOperation,
        _account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>> {
        unsupported()
    }

    fn get_bulk_operation(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>> {
        unsupported()
    }

    fn count_non_terminal(&self, _account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>> {
        unsupported()
    }

    fn count_terminal_born_since(
        &self,
        _account_id: &str,
        _since_ms: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unsupported()
    }

    fn list_bulk_operations(
        &self,
        _account_id: &str,
        _limit: u32,
        _after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult> {
        unsupported()
    }

    fn request_cancel(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>> {
        unsupported()
    }

    fn claim_runnable(
        &self,
        _owner: &str,
        _lease: Duration,
        _max: usize,
        _exclude_accounts: &[String],
        _exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>> {
        self.claims.fetch_add(1, Ordering::SeqCst);
        if self.claim_internal {
            return Box::pin(async {
                Err(StorageError::Internal("injected claim failure".to_owned()))
            });
        }
        unsupported()
    }

    fn checkpoint(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _lease: Duration,
        _progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        unsupported()
    }

    fn report_records_page(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _kind: ReportKind,
        _after: Option<(i64, i64)>,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>> {
        unsupported()
    }

    fn finish(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>> {
        unsupported()
    }

    fn list_purgeable_before(
        &self,
        _cutoff: SystemTime,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unsupported()
    }

    fn purge_operations(
        &self,
        _pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unsupported()
    }
}
