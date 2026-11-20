// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use tokio::sync::Notify;

// The output check at the first report record, the report writes and the
// manifest write wait on the output store for up to its bound, longer than
// one lease. While one of them waits, the runner renews the lease without
// moving the cursor, so no other process claims the operation, and a cancel
// seen during the wait ends the run. These tests use a 2-second lease and
// calls held for over two lease periods. With a 300 ms lease and 5-second
// bounds they failed on a loaded host: the runner finished before the held
// call, or the run took longer than 5 seconds to end after it.

const LEASE: Duration = Duration::from_secs(2);
const HELD: Duration = Duration::from_millis(4500);
/// How long a step a test waits on may take before it counts as a hang.
const BOUND: Duration = Duration::from_secs(30);

/// Which output call a `HoldingSink` holds, once.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum HoldAt {
    /// The first location check, the one at the first report record.
    FirstCheck,
    /// The first report page write.
    ReportPage,
    /// The manifest write.
    Manifest,
}

/// The filesystem sink with one call held: it signals `reached`, then
/// answers only after `release`. A held location check also signals
/// `answered` once it has answered.
pub(super) struct HoldingSink {
    inner: FsSink,
    at: HoldAt,
    pub(super) reached: Arc<Notify>,
    pub(super) release: Arc<Notify>,
    answered: Arc<Notify>,
    fired: Arc<AtomicBool>,
}

struct HoldingWriter {
    inner: Box<dyn ReportWriter>,
    hold: Option<(Arc<Notify>, Arc<Notify>)>,
}

impl HoldingSink {
    pub(super) fn new(h: &Harness, at: HoldAt) -> Self {
        Self {
            inner: h.fs_sink(),
            at,
            reached: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            answered: Arc::new(Notify::new()),
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The hold for a call at `at`, the first time only.
    fn hold(&self, at: HoldAt) -> Option<(Arc<Notify>, Arc<Notify>)> {
        (self.at == at && !self.fired.swap(true, Ordering::SeqCst))
            .then(|| (Arc::clone(&self.reached), Arc::clone(&self.release)))
    }
}

async fn held(hold: Option<(Arc<Notify>, Arc<Notify>)>) {
    if let Some((reached, release)) = hold {
        reached.notify_one();
        release.notified().await;
    }
}

impl BulkSink for HoldingSink {
    fn check_location(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        let hold = self.hold(HoldAt::FirstCheck);
        let fut = self.inner.check_location(at, key, sse);
        Box::pin(async move {
            let holding = hold.is_some();
            held(hold).await;
            let answer = fut.await;
            if holding {
                self.answered.notify_one();
            }
            answer
        })
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
        let hold = self.hold(HoldAt::ReportPage);
        let fut = self.inner.open(at, prefix, operation_id, kind, owner, sse);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(HoldingWriter { inner, hold }) as Box<dyn ReportWriter>)
        })
    }

    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        owner: &str,
        sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        let hold = self.hold(HoldAt::Manifest);
        let fut = self
            .inner
            .write_manifest(at, prefix, operation_id, owner, sse, lines);
        Box::pin(async move {
            held(hold).await;
            fut.await
        })
    }
}

impl ReportWriter for HoldingWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            held(self.hold.take()).await;
            self.inner.write_page(records).await
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        let this = *self;
        this.inner.finish()
    }

    fn object_index(&self) -> u32 {
        self.inner.object_index()
    }
}

/// Five rows, the third without its sort key, so the run has one error
/// record, an error report and a manifest; a 300 ms lease.
async fn five_rows_one_error(h: &Harness) -> (String, BulkOperationRecord, RunnerConfig) {
    let mut rows = plain_rows(5);
    rows[2] = row_missing_sk(2);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", LEASE).await;
    let mut cfg = config();
    cfg.lease = LEASE;
    (op, record, cfg)
}

/// Hold the call at `at` for over two lease periods while the runner is
/// polled, check that no other process can claim the operation, that
/// runner-1 still holds it and that the renewals left the cursor where the
/// hold found it, then release the call and let the run finish.
async fn held_call_keeps_the_lease(at: HoldAt) -> (Harness, String) {
    let h = Harness::new().await;
    let (op, record, cfg) = five_rows_one_error(&h).await;
    let sink = Arc::new(HoldingSink::new(&h, at));
    let reached = Arc::clone(&sink.reached);
    let release = Arc::clone(&sink.release);
    let deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::time::timeout(BOUND, async {
        tokio::select! {
            ended = &mut run => panic!(
                "the runner must not finish before the call is held: {ended:?}, {:?}",
                h.record(&op).await
            ),
            () = reached.notified() => {}
        }
    })
    .await
    .expect("the runner reaches the held call");
    let at_hold = h.record(&op).await;

    // The runner is polled through the hold, so it renews its lease.
    let probe = async {
        tokio::time::sleep(HELD).await;
        let rivals = h
            .storage
            .claim_runnable("runner-2", Duration::from_secs(60), 10, &[], &[])
            .await
            .expect("claim");
        let mid = h.record(&op).await;
        (rivals.len(), mid)
    };
    let (rivals, mid) = tokio::select! {
        _ = &mut run => panic!("the runner must not finish while the call is held"),
        probed = probe => probed,
    };
    assert_eq!(
        rivals, 0,
        "no second owner claims the operation during the hold"
    );
    assert_eq!(mid.lease_owner.as_deref(), Some("runner-1"));
    assert_eq!(mid.status, "IN_PROGRESS");
    assert_eq!(
        (mid.cursor_file_index, mid.cursor_row_index),
        (at_hold.cursor_file_index, at_hold.cursor_row_index),
        "the renewals during the hold leave the cursor where the hold found it"
    );

    release.notify_one();
    tokio::time::timeout(BOUND, &mut run)
        .await
        .expect("the run ends once the call answers")
        .expect("runner run");
    (h, op)
}

#[tokio::test]
async fn a_report_write_held_past_two_leases_keeps_the_lease() {
    let (h, op) = held_call_keeps_the_lease(HoldAt::ReportPage).await;
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{record:?}");
    let report = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_eq!(report.len(), 1);
    assert!(h.read_manifest(&op).is_some(), "the manifest is written");
}

#[tokio::test]
async fn a_manifest_write_held_past_two_leases_keeps_the_lease() {
    let (h, op) = held_call_keeps_the_lease(HoldAt::Manifest).await;
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{record:?}");
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 1);
}

#[tokio::test]
async fn an_output_check_held_past_two_leases_keeps_the_lease() {
    let (h, op) = held_call_keeps_the_lease(HoldAt::FirstCheck).await;
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{record:?}");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 4);
}

/// A renewal that started while the output check waits runs to its end
/// when the check answers first: dropped inside its checkpoint, it could
/// leave the pooled connection inside the checkpoint's transaction. The
/// renewal's checkpoint is held until the check has answered.
#[tokio::test]
async fn a_renewal_started_during_a_held_check_runs_to_its_end() {
    let h = Harness::new().await;
    let (op, record, cfg) = five_rows_one_error(&h).await;
    let sink = Arc::new(HoldingSink::new(&h, HoldAt::FirstCheck));
    let reached = Arc::clone(&sink.reached);
    let release = Arc::clone(&sink.release);
    let entered = Arc::new(Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.hold_first_checkpoint = Some((Arc::clone(&entered), Arc::clone(&sink.answered)));
    let faulting = Arc::new(faulting);
    let mut deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
    deps.storage = Arc::clone(&faulting) as Arc<dyn StorageEngine>;
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    for (step, signal) in [
        ("the held check", &reached),
        ("the renewal's checkpoint", &entered),
    ] {
        tokio::time::timeout(BOUND, async {
            tokio::select! {
                ended = &mut run => panic!("the runner must not finish before {step}: {ended:?}"),
                () = signal.notified() => {}
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the runner reaches {step}"));
    }
    release.notify_one();
    tokio::time::timeout(BOUND, &mut run)
        .await
        .expect("the run ends once the check answers")
        .expect("runner run");

    assert_eq!(
        faulting.checkpoints_finished.load(Ordering::SeqCst),
        faulting.checkpoints_started.load(Ordering::SeqCst),
        "every checkpoint the run started ran to its end"
    );
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{record:?}");
}

/// A renewal that answers after the check it kept the lease for still
/// moves the local deadline. The renewal's checkpoint is held until a sixth
/// of a lease past the deadline the run started with, and a sixth of a
/// lease before the deadline the renewal grants (the renewal starts a third
/// of the lease after the claim). The next write batch, rows 3 and 4, then
/// finds the lease held by the runner's own clock and is written. With the
/// deadline left where the claim put it, that batch would find the lease
/// lapsed and stop the runner with the rows unwritten.
#[tokio::test]
async fn a_renewal_that_answers_after_the_check_moves_the_deadline() {
    let h = Harness::new().await;
    let (op, record, cfg) = five_rows_one_error(&h).await;
    let sink = Arc::new(HoldingSink::new(&h, HoldAt::FirstCheck));
    let reached = Arc::clone(&sink.reached);
    let release = Arc::clone(&sink.release);
    let entered = Arc::new(Notify::new());
    let unhold = Arc::new(Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.hold_first_checkpoint = Some((Arc::clone(&entered), Arc::clone(&unhold)));
    let faulting = Arc::new(faulting);
    let mut deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
    deps.storage = Arc::clone(&faulting) as Arc<dyn StorageEngine>;
    let started = std::time::Instant::now();
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    for (step, signal) in [
        ("the held check", &reached),
        ("the renewal's checkpoint", &entered),
    ] {
        tokio::time::timeout(BOUND, async {
            tokio::select! {
                ended = &mut run => panic!("the runner must not finish before {step}: {ended:?}"),
                () = signal.notified() => {}
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the runner reaches {step}"));
    }
    // The check answers while the renewal is inside its checkpoint; the
    // checkpoint answers once the deadline the run started with has passed.
    release.notify_one();
    let past_first_deadline = tokio::time::Instant::from_std(started + LEASE + LEASE / 6);
    tokio::select! {
        ended = &mut run => panic!("the runner must not finish while the renewal is held: {ended:?}"),
        () = tokio::time::sleep_until(past_first_deadline) => {}
    }
    unhold.notify_one();
    tokio::time::timeout(BOUND, &mut run)
        .await
        .expect("the run ends once the renewal answers")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{record:?}");
    assert_eq!(
        h.count_items().await,
        4,
        "the write batch after the renewal found the lease held"
    );
}

#[tokio::test]
async fn a_cancel_during_a_held_report_write_still_writes_the_outputs() {
    let h = Harness::new().await;
    let (op, record, cfg) = five_rows_one_error(&h).await;
    let sink = Arc::new(HoldingSink::new(&h, HoldAt::ReportPage));
    let reached = Arc::clone(&sink.reached);
    let release = Arc::clone(&sink.release);
    let deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::time::timeout(BOUND, async {
        tokio::select! {
            ended = &mut run => panic!(
                "the runner must not finish before the write is held: {ended:?}, {:?}",
                h.record(&op).await
            ),
            () = reached.notified() => {}
        }
    })
    .await
    .expect("the runner reaches the held report write");
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    // The renewals during the hold see the cancel and must not end the
    // write: the run read all its input before the cancel arrived.
    tokio::select! {
        _ = &mut run => panic!("a cancel must not end a held report write"),
        () = tokio::time::sleep(HELD) => {}
    }
    release.notify_one();
    tokio::time::timeout(BOUND, &mut run)
        .await
        .expect("the run ends once the write answers")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED", "{record:?}");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 4);
    let report = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_eq!(report.len(), 1);
    assert!(h.read_manifest(&op).is_some(), "the manifest is written");
}
