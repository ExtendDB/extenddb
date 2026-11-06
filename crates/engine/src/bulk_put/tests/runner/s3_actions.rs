// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::rotation::RollingSink;
use super::*;

use crate::bulk_put::authorize::S3Action;

// The S3 actions a run performs as the operation's caller, checked through
// the authorizer: s3:GetObject on an input object before it is opened and at
// every boundary while it is read, s3:PutObject on each report object and on
// the manifest (measured on the service for bucket policies denying each).

async fn run_checked(
    h: &Harness,
    files: Vec<InputFile>,
    output: Value,
    authorizer: Arc<FakeAuthorizer>,
    cfg: RunnerConfig,
) -> BulkOperationRecord {
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            output,
            Some(run_context_with_caller()),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut deps = h.deps("runner-1", cfg);
    deps.authorizer = Some(authorizer);
    runner::run(&deps, record).await.expect("runner run");
    h.record(&op).await
}

fn input_arn(path: &str) -> String {
    format!("arn:aws:s3:::{IN_BUCKET}/{path}")
}

fn status_of(line: &Value) -> &str {
    line["fileProcessingStatus"].as_str().expect("a status")
}

#[tokio::test]
async fn get_object_denied_at_open_fails_and_the_file_is_not_processed() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let total = files[0].size;
    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::GetObject,
        "part-000.json",
    ));
    let record = run_checked(&h, files, both_reports(), Arc::clone(&fake), config()).await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(denied("s3:GetObject", &input_arn("part-000.json")).as_str())
    );
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(record.total_input_bytes, Some(total));
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 0);
    assert_eq!(
        fake.checks()[0],
        (S3Action::GetObject, input_arn("part-000.json"), None),
        "the object is checked before the open"
    );
    assert_eq!(fake.count(S3Action::GetObject), 1);
    assert_eq!(
        fake.count(S3Action::PutObject),
        1,
        "the manifest write is checked"
    );
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("the manifest is written");
    assert_eq!(status_of(&manifest[0]), "NOT_PROCESSED");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json")));
}

#[tokio::test]
async fn an_explicit_deny_is_reported_in_its_own_form() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let mut fake = FakeAuthorizer::denying(S3Action::GetObject, "part-000.json");
    fake.rule.as_mut().unwrap().explicit = true;
    let record = run_checked(&h, files, both_reports(), Arc::new(fake), config()).await;

    let resource = input_arn("part-000.json");
    assert_eq!(
        record.failure_message.as_deref(),
        Some(
            format!(
                "User: arn:aws:iam::{ACCOUNT}:user/{CALLER} is not authorized to perform: \
                 s3:GetObject on resource: \"{resource}\" with an explicit deny in an \
                 identity-based policy"
            )
            .as_str()
        )
    );
}

#[tokio::test]
async fn get_object_denied_at_a_boundary_after_rows_were_written_is_not_available() {
    // The open is allowed; the check at the first boundary (row 5) is not.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let mut fake = FakeAuthorizer::denying(S3Action::GetObject, "part-000.json");
    fake.rule.as_mut().unwrap().allow_first = 1;
    let fake = Arc::new(fake);
    let mut cfg = config();
    cfg.batch_size = 5;
    let record = run_checked(&h, files, both_reports(), Arc::clone(&fake), cfg).await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.processed_item_count, 5, "the rows read before stand");
    assert_eq!(h.count_items().await, 5, "and stay written");
    assert_eq!(
        fake.count(S3Action::GetObject),
        2,
        "the open and one boundary"
    );
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("the manifest is written");
    assert_eq!(status_of(&manifest[0]), "NOT_AVAILABLE");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}

#[tokio::test]
async fn a_later_file_denied_at_its_open_is_not_processed() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    h.write_input("part-001.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::GetObject,
        "part-001.json",
    ));
    let record = run_checked(&h, files, both_reports(), Arc::clone(&fake), config()).await;

    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.processed_item_count, 3);
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("the manifest is written");
    assert_eq!(status_of(&manifest[0]), "FULLY_PROCESSED");
    assert_eq!(status_of(&manifest[1]), "NOT_PROCESSED");
    assert_eq!(
        fake.count(S3Action::GetObject),
        2,
        "the end-of-file boundary checks no file"
    );
}

#[tokio::test]
async fn put_object_denied_on_the_first_report_object_is_held_to_the_end_of_the_input() {
    // Five good rows, the faulting row, four good rows.
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows[5] = row_missing_sk(5);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::PutObject,
        "/output/errors/output-00000.json.gz",
    ));
    let record = run_checked(&h, files, both_reports(), Arc::clone(&fake), config()).await;
    let op = &record.operation_id;

    let key = report::report_key(OUT_PREFIX, ACCOUNT, op, ReportKind::Error, 0);
    let resource = format!("arn:aws:s3:::{OUT_BUCKET}/{key}");
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(denied("s3:PutObject", &resource).as_str()),
        "the message names the first error report object's full key"
    );
    assert_eq!(record.processed_item_count, 9);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(record.usage["BulkPutBytes"], json!(0));
    assert_eq!(h.count_items().await, 9);
    assert!(h.read_report(op, ReportKind::Error).is_none());
    assert!(h.read_manifest(op).is_none());
    assert_eq!(
        fake.count(S3Action::PutObject),
        1,
        "no later write is checked"
    );
}

#[tokio::test]
async fn put_object_denied_on_a_later_report_object_fails_the_render() {
    // 1,500 error records render as two pages; with the rotation threshold at
    // one byte the second page opens the second object.
    let h = Harness::new().await;
    let rows: Vec<String> = (0..1500).map(row_missing_sk).collect();
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
    let sink = Arc::new(RollingSink::new(h.fs_sink(), 1));
    let mut deps = h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink);
    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::PutObject,
        "/output/errors/output-00001.json.gz",
    ));
    deps.authorizer = Some(Arc::clone(&fake) as Arc<dyn BulkAuthorizer>);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    let key = report::report_key(OUT_PREFIX, ACCOUNT, &op, ReportKind::Error, 1);
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_message.as_deref(),
        Some(denied("s3:PutObject", &format!("arn:aws:s3:::{OUT_BUCKET}/{key}")).as_str())
    );
    assert_eq!(record.failed_item_count, 1500, "the run itself completed");
    assert!(
        h.report_objects(&op, ReportKind::Error).is_empty(),
        "no report object is installed"
    );
    let dir = h.report_path(&op, ReportKind::Error);
    let leftover: Vec<String> = std::fs::read_dir(dir.parent().unwrap())
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        leftover.is_empty(),
        "the dropped writer leaves nothing: {leftover:?}"
    );
    assert!(
        h.read_manifest(&op).is_some(),
        "the manifest follows as usual"
    );
}

#[tokio::test]
async fn put_object_denied_on_the_manifest_fails_with_the_manifest_prefix() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::PutObject,
        "/manifest-files.json",
    ));
    let record = run_checked(&h, files, both_reports(), fake, config()).await;
    let op = &record.operation_id;

    let key = report::manifest_key(OUT_PREFIX, ACCOUNT, op);
    let resource = format!("arn:aws:s3:::{OUT_BUCKET}/{key}");
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(
            format!(
                "Manifest is not available. {}",
                denied("s3:PutObject", &resource)
            )
            .as_str()
        )
    );
    assert_eq!(record.processed_item_count, 3);
    assert!(h.read_manifest(op).is_none());
}

#[tokio::test]
async fn a_record_with_no_caller_fails_closed() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(json!({"store": "filesystem"})),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let fake = Arc::new(FakeAuthorizer::default());
    let mut deps = h.deps("runner-1", config());
    deps.authorizer = Some(Arc::clone(&fake) as Arc<dyn BulkAuthorizer>);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert!(fake.checks().is_empty(), "there is no caller to evaluate");
    assert_eq!(h.count_items().await, 0);
}

#[tokio::test]
async fn with_no_authorizer_no_check_runs() {
    // Developer mode: the same operation the denying authorizer fails runs to
    // its end.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
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
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 3);
}

#[tokio::test]
async fn a_check_that_cannot_be_evaluated_fails_internal() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let fake = Arc::new(FakeAuthorizer {
        unavailable: true,
        ..FakeAuthorizer::default()
    });
    let record = run_checked(&h, files, both_reports(), fake, config()).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Internal server error")
    );
    assert_eq!(h.count_items().await, 0);
}

#[tokio::test]
async fn a_resumed_open_is_checked_and_a_denial_there_is_not_available() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(run_context_with_caller()),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 7,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let mut deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    deps.authorizer = Some(Arc::new(FakeAuthorizer::default()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(h.record(&op).await.cursor_row_index, 5);

    let fake = Arc::new(FakeAuthorizer::denying(
        S3Action::GetObject,
        "part-000.json",
    ));
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut deps = h.deps("runner-2", config());
    deps.authorizer = Some(Arc::clone(&fake) as Arc<dyn BulkAuthorizer>);
    runner::run(&deps, record).await.expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(
        fake.checks()[0],
        (S3Action::GetObject, input_arn("part-000.json"), None),
        "the resumed claim checks before it reopens the file"
    );
    assert_eq!(fake.count(S3Action::GetObject), 1);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(status_of(&manifest[0]), "NOT_AVAILABLE");
}
