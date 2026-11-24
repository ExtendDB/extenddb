// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The stored terminal status in metrics, a render that outlives its
// lease, and the dead-runner temp sweep.

#[tokio::test]
async fn a_cancel_after_the_last_boundary_records_finished_with_cancelled() {
    let h = Harness::new().await;
    let mut rows = plain_rows(5);
    rows[2] = row_missing_sk(2);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // The cancel lands during rendering, after the runner passed its last
    // batch boundary, so the runner computes COMPLETED_WITH_ERRORS while
    // `finish` turns the CANCELLING row into CANCELLED.
    let metrics = Arc::new(MetricsCollector::new());
    let sink = Arc::new(CancelOnOpenSink {
        inner: h.fs_sink(),
        storage: Arc::clone(&h.storage),
        operation_id: op.clone(),
        fired: Arc::new(AtomicBool::new(false)),
    });
    let deps = RunnerDeps {
        storage: Arc::clone(&h.storage),
        source: Arc::new(h.fs_source()),
        sink,
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(REGION),
        owner: "runner-1".to_owned(),
        config: config(),
        metrics: Arc::clone(&metrics),
        pacer: Arc::new(WritePacer::unbounded()),
        store_kind: STORE_FILESYSTEM,
        authorizer: None,
        shutdown: CancellationToken::new(),
    };
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");

    // The status dimension is the status `finish` stored, not the status
    // the runner computed. Exactly one Finished series exists, so no
    // COMPLETED series was recorded beside the CANCELLED one.
    let mut snaps = metrics.query(&MetricsQuery {
        metric: Some(MetricName::BulkOperationsFinished),
        window: Some(TimeWindow::AllTime),
        ..MetricsQuery::default()
    });
    assert_eq!(snaps.len(), 1, "exactly one Finished series: {snaps:?}");
    let finished = snaps.remove(0);
    assert!(
        finished
            .dimensions
            .contains(&Dimension::Operation("CANCELLED".to_owned())),
        "the Finished dimension is the stored CANCELLED: {:?}",
        finished.dimensions
    );
    assert!(
        !finished
            .dimensions
            .contains(&Dimension::Operation("COMPLETED".to_owned())),
        "not the COMPLETED the runner computed: {:?}",
        finished.dimensions
    );
}

#[tokio::test]
async fn a_render_that_outlives_its_lease_does_not_replace_the_new_owners_report() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The first runner reaches rendering and pends inside `write_page`
    // with its temp file already created, and is then not polled, as a
    // paused process is: a render that outlives the lease. A runner that is
    // polled keeps its lease while a write waits. Its own lease is 60
    // seconds, so no renewal is due while the page waits and none is under
    // way when polling stops; a renewal stopped inside a storage call could
    // hold the engine's write lock, and the next claim would wait for it
    // without end. Its checkpoints ask the store for 100 ms, so the store's
    // lease runs out behind it.
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let sink = Arc::new(PendInWritePageSink {
        inner: h.fs_sink(),
        reached: Arc::clone(&reached),
        release: Arc::clone(&release),
    });
    let deps =
        h.with_short_store_lease(h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink));
    let stale = runner::run(&deps, record);
    tokio::pin!(stale);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut stale => panic!("the first runner must not finish before its render"),
            () = reached.notified() => {}
        }
    })
    .await
    .expect("the first runner must reach its render");

    // The lease expires; a new worker claims, re-renders, and finishes.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("the second run finishes");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("the second runner rendered the report");
    assert_eq!(report.len(), 1);

    // Distinct bytes stand in for the new owner's rendering: both runners
    // would encode the same records, so byte equality after the release is
    // what proves the stale runner replaced nothing.
    let final_path = h.report_path(&op, ReportKind::Error);
    let sentinel = b"the new owners rendering".to_vec();
    std::fs::write(&final_path, &sentinel).unwrap();

    // Released, the stale runner writes its page, re-reads the record,
    // observes the lease is no longer its own, drops its writer, and
    // stops cleanly with no rename.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), stale)
        .await
        .expect("the stale runner must stop once released")
        .expect("a lost lease is a clean stop, not an error");

    assert_eq!(
        std::fs::read(&final_path).unwrap(),
        sentinel,
        "the stale runner must not replace the new owner's report"
    );
    let leftovers: Vec<String> = std::fs::read_dir(final_path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".tmp-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "the stale runner's temp file is gone: {leftovers:?}"
    );
}

#[tokio::test]
async fn a_dead_runners_temp_file_is_removed_when_the_next_runner_opens_the_report_directory() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row(0), row_missing_sk(1)]));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    // A hard-killed runner's leftover: the temp name embeds the dead
    // owner's pid and a random suffix, so no successor ever computes the
    // same name and only the open sweep can remove it.
    let final_path = h.report_path(&op, ReportKind::Error);
    let report_dir = final_path.parent().unwrap();
    std::fs::create_dir_all(report_dir).unwrap();
    let dead = report_dir.join("output-00000.json.gz.tmp-dead-owner");
    std::fs::write(&dead, b"half-rendered leftovers").unwrap();

    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert!(
        !dead.exists(),
        "opening the report directory removes the dead owner's temp file"
    );
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([MISSING_KEY]));
}

#[tokio::test]
async fn a_read_error_at_the_render_lease_check_leaves_the_operation_in_progress() {
    // The lease re-read before a report rename is a storage read like the
    // final checkpoint's; when it fails nothing is known about ownership, so
    // the runner stops without a terminal transition and the next claim
    // renders again. Terminally failing the operation here would turn a
    // transient read error into a FAILED record whose report a retry would
    // have written.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row(0), row_missing_sk(1)]));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The runner reads the record twice before the first rename: the
    // boundary check at end of file, then the lease re-read. The second
    // read fails. The runner's own lease is 60 seconds, so no renewal
    // during an output call adds a read before the re-read; its
    // checkpoints ask the store for 100 ms, so the lease runs out for the
    // later claim.
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_get_on_call = Some(2);
    faulting.checkpoint_lease = Some(Duration::from_millis(100));
    let deps = h.deps_with_storage("runner-1", config(), Arc::new(faulting));
    let outcome = runner::run(&deps, record).await;
    assert!(
        matches!(outcome, Err(runner::RunnerError::Storage(_))),
        "the runner surfaces the read error instead of finishing: {outcome:?}"
    );

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "no terminal transition was made");
    assert!(
        h.read_report(&op, ReportKind::Error).is_none(),
        "no report was renamed into place"
    );

    // The lease expires; a later claim renders and finishes normally.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert!(h.read_report(&op, ReportKind::Error).is_some());
}
