// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The store an operation started on, recorded in its run context. A record
// started on another store than the runner's is neither read nor rendered:
// it ends FAILED with the internal failure. A record with no run context
// was started before the column existed, on the filesystem store.

fn counting(h: &Harness) -> (Arc<CountingSource>, Arc<AtomicU64>) {
    let source = Arc::new(CountingSource::new(h.fs_source()));
    let opens = Arc::clone(&source.opens);
    (source, opens)
}

#[tokio::test]
async fn a_record_started_on_another_store_fails_unread() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row(0), row_missing_sk(1)]));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(json!({"store": "s3"})),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let (source, opens) = counting(&h);
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Internal server error")
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0, "no input is opened");
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(h.count_items().await, 0);
    assert!(h.read_manifest(&op).is_none(), "nothing is rendered");
    assert!(h.read_report(&op, ReportKind::Error).is_none());
}

#[tokio::test]
async fn a_cancelling_record_started_on_another_store_is_cancelled_with_nothing_rendered() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(json!({"store": "s3"})),
        )
        .await;
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert!(
        h.read_manifest(&op).is_none(),
        "no manifest is written to a store the operation did not start on"
    );
}

#[tokio::test]
async fn a_record_naming_the_runners_store_or_none_is_read() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    for (run_context, owner) in [
        (Some(json!({"store": "filesystem"})), "runner-1"),
        (None, "runner-2"),
    ] {
        let op = h
            .start_with_run_context(
                files.clone(),
                input_config(false),
                both_reports(),
                run_context.clone(),
            )
            .await;
        let record = h.claim(owner, Duration::from_secs(60)).await;
        runner::run(&h.deps(owner, config()), record)
            .await
            .expect("runner run");
        let record = h.record(&op).await;
        assert_eq!(record.status, "COMPLETED", "{run_context:?}");
        assert_eq!(record.processed_item_count, 3, "{run_context:?}");
        assert!(h.read_manifest(&op).is_some(), "{run_context:?}");
    }
}

#[tokio::test]
async fn a_run_context_that_does_not_parse_fails_unread() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            both_reports(),
            Some(json!({"store": 7})),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let (source, opens) = counting(&h);
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert!(h.read_manifest(&op).is_none());
}
