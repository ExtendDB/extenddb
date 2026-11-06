// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::bulk_put::authorize::S3Action;

// A held output location fault is stored with the checkpoints. A claim that
// resumes after a crash, with no failing row left to read, ends the
// operation as the run without the crash does: status, FailureCode,
// FailureMessage, the counters, usage, and no report and no manifest.

/// Twenty rows; only row 1 fails, so every failing row is behind the crash.
fn rows_failing_at_one() -> Vec<String> {
    let mut rows = plain_rows(20);
    rows[1] = row_missing_sk(1);
    rows
}

fn runner_cfg(lease: Duration) -> RunnerConfig {
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = lease;
    cfg
}

/// What a client and the output bucket see of a finished operation, the
/// operation id in the message replaced by `{op}`.
#[derive(Debug, PartialEq)]
struct Outcome {
    status: String,
    code: Option<String>,
    message: Option<String>,
    processed: i64,
    failed: i64,
    bytes: i64,
    usage: Value,
    items: usize,
    report: bool,
    manifest: bool,
}

async fn outcome(h: &Harness, op: &str) -> Outcome {
    let record = h.record(op).await;
    Outcome {
        status: record.status,
        code: record.failure_code,
        message: record
            .failure_message
            .map(|message| message.replace(op, "{op}")),
        processed: record.processed_item_count,
        failed: record.failed_item_count,
        bytes: record.processed_input_bytes,
        usage: record.usage,
        items: h.count_items().await,
        report: h.read_report(op, ReportKind::Error).is_some(),
        manifest: h.read_manifest(op).is_some(),
    }
}

/// Start the operation over the twenty rows with `output`, recording the
/// test caller.
async fn start_rows(h: &Harness, output: Value) -> String {
    h.write_input("part-000.json", &file_of(&rows_failing_at_one()));
    let files = h.enumerate().await;
    h.start_with_run_context(
        files,
        input_config(false),
        output,
        Some(run_context_with_caller()),
    )
    .await
}

/// Deps for `owner` with `source`, the filesystem sink, `authorizer` and
/// `storage`.
fn deps_for(
    h: &Harness,
    owner: &str,
    cfg: RunnerConfig,
    source: Arc<dyn BulkSource>,
    authorizer: &Option<Arc<FakeAuthorizer>>,
    storage: Arc<dyn StorageEngine>,
) -> RunnerDeps {
    let mut deps = h.deps_with(owner, cfg, source, Arc::new(h.fs_sink()));
    deps.storage = storage;
    deps.authorizer = authorizer
        .as_ref()
        .map(|fake| Arc::clone(fake) as Arc<dyn BulkAuthorizer>);
    deps
}

/// The run without a crash.
async fn uninterrupted(output: Value, authorizer: Option<Arc<FakeAuthorizer>>) -> Outcome {
    let h = Harness::new().await;
    let op = start_rows(&h, output).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let deps = deps_for(
        &h,
        "runner-1",
        runner_cfg(Duration::from_secs(60)),
        Arc::new(h.fs_source()),
        &authorizer,
        Arc::clone(&h.storage),
    );
    runner::run(&deps, record).await.expect("runner run");
    outcome(&h, &op).await
}

/// The same run, stopped after the checkpoint at row 10 and resumed by a
/// second claim from the record.
async fn crashed_mid_input(output: Value, authorizer: Option<Arc<FakeAuthorizer>>) -> Outcome {
    let h = Harness::new().await;
    let op = start_rows(&h, output).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 12,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let deps = deps_for(
        &h,
        "runner-1",
        runner_cfg(Duration::from_millis(100)),
        source,
        &authorizer,
        Arc::clone(&h.storage),
    );
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    drop(deps);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10, "the crash follows a checkpoint");
    assert_eq!(mid.status, "IN_PROGRESS");

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let deps = deps_for(
        &h,
        "runner-2",
        runner_cfg(Duration::from_secs(60)),
        Arc::new(h.fs_source()),
        &authorizer,
        Arc::clone(&h.storage),
    );
    runner::run(&deps, record).await.expect("resumed run");
    outcome(&h, &op).await
}

/// The same run, stopped after the final checkpoint and before the terminal
/// transition, then resumed.
async fn crashed_before_finish(output: Value, authorizer: Option<Arc<FakeAuthorizer>>) -> Outcome {
    let h = Harness::new().await;
    let op = start_rows(&h, output).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.pend_finish = Some(Arc::clone(&notify));
    let deps = deps_for(
        &h,
        "runner-1",
        runner_cfg(Duration::from_millis(100)),
        Arc::new(h.fs_source()),
        &authorizer,
        Arc::new(faulting),
    );
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while finish pends"),
        () = notify.notified() => {}
    }
    drop(deps);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(h.record(&op).await.status, "IN_PROGRESS");

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let deps = deps_for(
        &h,
        "runner-2",
        runner_cfg(Duration::from_secs(60)),
        Arc::new(h.fs_source()),
        &authorizer,
        Arc::clone(&h.storage),
    );
    runner::run(&deps, record).await.expect("resumed run");
    outcome(&h, &op).await
}

fn assert_held_shape(outcome: &Outcome, code: &str) {
    assert_eq!(outcome.status, "FAILED");
    assert_eq!(outcome.code.as_deref(), Some(code));
    assert_eq!(outcome.processed, 19);
    assert_eq!(outcome.failed, 0);
    assert_eq!(outcome.bytes, 0);
    assert_eq!(outcome.usage["BulkPutBytes"], json!(0), "{}", outcome.usage);
    assert!(
        outcome.usage.get(held_member()).is_none(),
        "the terminal usage carries no held fault: {}",
        outcome.usage
    );
    assert_eq!(outcome.items, 19);
    assert!(!outcome.report && !outcome.manifest, "nothing is written");
}

fn held_member() -> &'static str {
    "HeldOutputLocationFault"
}

#[tokio::test]
async fn a_missing_output_bucket_held_before_a_crash_ends_as_without_the_crash() {
    let output = || output_config(Some("absent-reports"), &["ERROR"]);
    let expected = uninterrupted(output(), None).await;
    assert_held_shape(&expected, "S3NoSuchBucket");
    assert_eq!(
        expected.message.as_deref(),
        Some("The specified bucket does not exist")
    );
    assert_eq!(crashed_mid_input(output(), None).await, expected);
    assert_eq!(crashed_before_finish(output(), None).await, expected);
}

#[tokio::test]
async fn a_denied_report_write_held_before_a_crash_ends_as_without_the_crash() {
    let output = || output_config(Some(OUT_BUCKET), &["ERROR"]);
    let deny = || {
        Some(Arc::new(FakeAuthorizer::denying(
            S3Action::PutObject,
            "/output/errors/output-00000.json.gz",
        )))
    };
    let expected = uninterrupted(output(), deny()).await;
    assert_held_shape(&expected, "S3AccessDenied");
    assert!(
        expected
            .message
            .as_deref()
            .is_some_and(|message| message.contains("s3:PutObject")),
        "{:?}",
        expected.message
    );
    assert_eq!(crashed_mid_input(output(), deny()).await, expected);
    assert_eq!(crashed_before_finish(output(), deny()).await, expected);
}

#[tokio::test]
async fn the_checkpoints_store_the_held_fault_beside_the_usage() {
    let h = Harness::new().await;
    let op = start_rows(&h, output_config(Some("absent-reports"), &["ERROR"])).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 12,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let deps = h.deps_with(
        "runner-1",
        runner_cfg(Duration::from_millis(100)),
        source,
        Arc::new(h.fs_sink()),
    );
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!(
        mid.usage[held_member()],
        json!({
            "FailureCode": "S3NoSuchBucket",
            "FailureMessage": "The specified bucket does not exist",
        })
    );
    assert!(
        mid.usage["BulkPutBytes"].as_u64().unwrap() > 0,
        "the usage accumulates while the fault is held: {}",
        mid.usage
    );
    assert_eq!(mid.failure_code, None, "nothing is reported while it runs");
}
