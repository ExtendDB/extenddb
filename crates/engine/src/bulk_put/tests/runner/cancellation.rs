// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Cancellation.

#[tokio::test]
async fn a_cancel_between_batches_ends_cancelled_with_earlier_items_present() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(15)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    let storage = Arc::clone(&h.storage);
    let op_for_action = op.clone();
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        let op = op_for_action.clone();
        Box::pin(async move {
            storage
                .request_cancel(ACCOUNT, &op)
                .await
                .expect("request cancel");
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 7,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert!(record.cancel_requested);
    assert_eq!(
        record.processed_item_count, 10,
        "the batch in flight finishes; reading stops at its boundary"
    );
    assert!(h.get("k0").await.is_some(), "earlier items stay written");
    assert!(
        h.get("k9").await.is_some(),
        "the in-flight batch is written"
    );
    assert!(h.get("k10").await.is_none(), "no row past the boundary");
    // Measured on the service: a cancelled operation carries this pair, and
    // its input bytes are what was read before the stop, not zero; here the
    // reader had consumed the whole 15-row file by the boundary, so the
    // figure is the file's size.
    assert_eq!(record.failure_code.as_deref(), Some("RequestCancelled"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The request was cancelled")
    );
    let file_bytes: i64 = plain_rows(15).iter().map(|r| r.len() as i64 + 1).sum();
    assert_eq!(record.processed_input_bytes, file_bytes);
}

#[tokio::test]
async fn a_cancelling_operation_with_no_live_runner_is_finished_without_reading_input() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(5)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");

    // The claim predicate includes CANCELLING, so a worker picks it up.
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    assert_eq!(record.status, "CANCELLING");

    let source = Arc::new(CountingSource::new(h.fs_source()));
    let lists = Arc::clone(&source.lists);
    let opens = Arc::clone(&source.opens);
    let deps = h.deps_with("runner-2", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(
        record.processed_item_count, 0,
        "the persisted counters stand"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0, "no input file is opened");
    assert_eq!(lists.load(Ordering::SeqCst), 0, "no enumeration happens");
    assert_eq!(h.count_items().await, 0, "nothing is written");
}

// Cancellation, continued.

#[tokio::test]
async fn a_cancel_after_the_last_boundary_still_ends_cancelled() {
    let h = Harness::new().await;
    let mut rows = plain_rows(5);
    rows[2] = row_missing_sk(2);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // The cancel lands during rendering, after the runner passed its last
    // batch boundary; only the terminal transition can observe it.
    let sink = Arc::new(CancelOnOpenSink {
        inner: h.fs_sink(),
        storage: Arc::clone(&h.storage),
        operation_id: op.clone(),
        fired: Arc::new(AtomicBool::new(false)),
    });
    let deps = h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(
        record.status, "CANCELLED",
        "finish writes CANCELLED when the row is CANCELLING, whatever the \
         runner computed"
    );
    assert!(record.cancel_requested);
    assert_eq!(
        record.processed_item_count, 5,
        "every row was processed before the cancel landed"
    );
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 4);
}
