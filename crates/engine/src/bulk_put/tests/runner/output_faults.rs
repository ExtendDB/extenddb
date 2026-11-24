// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::bulk_put::authorize::S3Action;
use crate::bulk_put::source::StoreFault;

// Output faults found after the output location check passed.

const NO_BUCKET: &str = "The specified bucket does not exist (Service: Amazon S3; Status \
     Code: 404; Error Code: NoSuchBucket; Request ID: R1; S3 Extended Request ID: X1; Proxy: \
     null)";

/// The filesystem sink, except that the manifest write answers `fault`, as
/// an S3 PutObject does for a bucket removed after its check.
struct ManifestWriteFails {
    inner: FsSink,
    fault: StoreFault,
}

impl BulkSink for ManifestWriteFails {
    fn check_location(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        self.inner.check_location(at, key, sse)
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        owner: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<Box<dyn ReportWriter>, SinkError>> {
        self.inner.open(at, prefix, operation_id, kind, owner, sse)
    }

    fn write_manifest(
        &self,
        _at: &BucketRef<'_>,
        _prefix: &str,
        _operation_id: &str,
        _owner: &str,
        _sse: &OutputSse,
        _lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        let fault = self.fault.clone();
        Box::pin(async move { Err(SinkError::Store(fault)) })
    }
}

#[tokio::test]
async fn a_manifest_write_refused_after_its_check_keeps_the_manifest_prefix() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let sink = Arc::new(ManifestWriteFails {
        inner: h.fs_sink(),
        fault: StoreFault {
            code: "S3NoSuchBucket",
            message: NO_BUCKET.to_owned(),
            after_first_byte: false,
        },
    });
    let deps = h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink);
    runner::run(&deps, record).await.expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(format!("Manifest is not available. {NO_BUCKET}").as_str())
    );
    assert_eq!(record.processed_item_count, 2);
}

/// A good row, a row with no sort key (the first report record, of the
/// error kind), a conditional row whose condition fails (the first record
/// of the condition kind) and a good row, run with checkpoints of
/// `batch_size` rows. Only the condition report's write is denied.
async fn run_denying_the_condition_report(batch_size: usize) -> BulkOperationRecord {
    let h = Harness::new().await;
    let rows = vec![
        row(0),
        row_missing_sk(1),
        condition_row("1", "attribute_exists(pk)"),
        row(3),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(run_context_with_caller()),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = batch_size;
    let mut deps = h.deps("runner-1", cfg);
    let condition_key = "/condition-check-failures/output-00000.json.gz";
    let fake = Arc::new(FakeAuthorizer::denying(S3Action::PutObject, condition_key));
    deps.authorizer = Some(Arc::clone(&fake) as Arc<dyn BulkAuthorizer>);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    let key = report::report_key(
        OUT_PREFIX,
        ACCOUNT,
        &op,
        ReportKind::ConditionCheckFailure,
        0,
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(denied("s3:PutObject", &format!("arn:aws:s3:::{OUT_BUCKET}/{key}")).as_str())
    );
    assert_eq!(
        (
            record.processed_item_count,
            record.failed_item_count,
            record.processed_input_bytes
        ),
        (3, 0, 0),
        "{record:?}"
    );
    assert!(h.read_report(&op, ReportKind::Error).is_none());
    assert!(
        h.read_report(&op, ReportKind::ConditionCheckFailure)
            .is_none()
    );
    assert!(h.read_manifest(&op).is_none(), "no manifest is written");
    record
}

#[tokio::test]
async fn a_denied_condition_report_is_found_at_its_own_first_record_and_held() {
    let record = run_denying_the_condition_report(25).await;
    assert_eq!(record.report_record_count, 0);
}

#[tokio::test]
async fn an_error_record_checkpointed_before_the_hold_is_not_rendered() {
    // With a checkpoint after every row the error record is persisted before
    // the condition kind is checked; the hold still renders nothing.
    let record = run_denying_the_condition_report(1).await;
    assert_eq!(
        record.report_record_count, 1,
        "the error record is persisted"
    );
}
