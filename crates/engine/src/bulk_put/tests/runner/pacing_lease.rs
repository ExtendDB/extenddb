// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use super::lease_keeper::{Wait, WaitSource};

// Write pacing. Each row's write units (1 KB rounded up, so one per test
// row) come out of the process bucket and the operation bucket right before
// the write. A bucket holds one second of capacity and starts full, so a
// run of 30 one-unit rows against a 20-per-second bucket pays 10 units of
// deficit: half a second of sleep. Lower bounds only; an upper bound on wall
// time is not asserted.

#[tokio::test]
async fn the_process_pacer_bounds_plain_row_writes() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    let pacer = Arc::new(WritePacer::new(20));
    let mut deps = h.deps("runner-1", config());
    deps.pacer = Arc::clone(&pacer);
    let before = std::time::Instant::now();
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 30);
    assert!(
        pacer.waited() >= Duration::from_millis(400),
        "30 units against a 20/s bucket wait about 500 ms, waited {:?}",
        pacer.waited()
    );
    assert!(before.elapsed() >= Duration::from_millis(400));
}

#[tokio::test]
async fn the_process_pacer_bounds_conditional_row_writes_too() {
    let h = Harness::new().await;
    // Thirty distinct keys, each with a passing condition, so every row
    // reaches the conditional write site rather than the plain one.
    let rows: Vec<String> = (0..30)
        .map(|i| {
            format!(
                "{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{i}\"}}}}, \
                 \"ConditionExpression\": \"attribute_not_exists(pk)\"}}"
            )
        })
        .collect();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    let pacer = Arc::new(WritePacer::new(20));
    let mut deps = h.deps("runner-1", config());
    deps.pacer = Arc::clone(&pacer);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 30);
    assert!(
        pacer.waited() >= Duration::from_millis(400),
        "conditional writes are paced like plain ones, waited {:?}",
        pacer.waited()
    );
}

#[tokio::test]
async fn max_write_request_units_paces_the_operation_on_its_own() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    let files = h.enumerate().await;
    let op = h
        .start_with_units(files, input_config(false), both_reports(), Some(20))
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // The process bucket is unbounded; only the request's ceiling applies.
    let before = std::time::Instant::now();
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 30);
    assert!(
        before.elapsed() >= Duration::from_millis(400),
        "30 units at MaxWriteRequestUnits 20 take about 500 ms of pacing, took {:?}",
        before.elapsed()
    );
}

/// One input row keyed `k{i}` whose `g` value `g` puts it in the `gsi1`
/// index of the `new_with_gsi` table.
fn gsi_row(i: usize, g: &str) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"sk\": {{\"S\": \"s\"}}, \"g\": {{\"S\": \"{g}\"}}}}}}"
    )
}

// `MaxWriteRequestUnits` bounds the write request units the operation
// consumes on the table and on all of its indexes (service documentation,
// "Running a bulk put items operation"). Each of these rows writes one unit
// to the table and one to the index, so 60 rows at a ceiling of 40 units per
// second pay 80 units of deficit after the full bucket, 2 s. Counting the
// table alone, they would pay 20 units, 0.5 s.
#[tokio::test]
async fn max_write_request_units_counts_the_index_writes() {
    let h = Harness::new_with_gsi().await;
    let rows: Vec<String> = (0..60).map(|i| gsi_row(i, &format!("g{i}"))).collect();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start_with_units(files, input_config(false), both_reports(), Some(40))
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 60;
    cfg.write_batch_rows = 60;
    let before = std::time::Instant::now();
    runner::run(&h.deps("runner-1", cfg), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 60);
    assert_eq!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(61_440),
        "every row was written to the index"
    );
    assert!(
        before.elapsed() >= Duration::from_millis(1600),
        "120 units at MaxWriteRequestUnits 40 take about 2 s of pacing, took {:?}",
        before.elapsed()
    );
}

// A write that moves an item's index key also consumes the units of the old
// projection, which the runner learns only from the item the write
// replaced. The operation bucket is settled after each batch, so the next
// batch waits for them. Each of these 60 rows moves its index key and
// consumes 3 units, against the 2 an insert would. In two batches of 30 at
// a ceiling of 30 units per second, the second batch waits for the 30 units
// the first one settled: about 4 s in all, against 3 s without the settle.
#[tokio::test]
async fn a_moved_index_key_is_charged_to_the_next_batch() {
    let h = Harness::new_with_gsi().await;
    for i in 0..60 {
        let mut seeded = item(&format!("k{i}"), "0");
        seeded.insert("g".to_owned(), AttributeValue::S(format!("old{i}")));
        h.put_direct(seeded).await;
    }
    let rows: Vec<String> = (0..60).map(|i| gsi_row(i, &format!("new{i}"))).collect();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start_with_units(files, input_config(false), both_reports(), Some(30))
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 60;
    cfg.write_batch_rows = 30;
    let before = std::time::Instant::now();
    runner::run(&h.deps("runner-1", cfg), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(122_880),
        "each row is a delete and an insert on the index"
    );
    assert!(
        before.elapsed() >= Duration::from_millis(3600),
        "two batches of 90 consumed units at MaxWriteRequestUnits 30 take about 4 s, took {:?}",
        before.elapsed()
    );
}

/// Runner-1's claim of one batch of `rows` one-unit rows against a 1 s lease,
/// with a 10-unit process bucket: the batch waits `rows / 10 - 1` seconds on
/// the bucket, which starts full.
async fn one_paced_batch(h: &Harness, rows: usize) -> (String, BulkOperationRecord, RunnerDeps) {
    h.write_input("part-000.json", &file_of(&plain_rows(rows)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let lease = Duration::from_secs(1);
    let record = h.claim("runner-1", lease).await;
    let mut cfg = config();
    cfg.batch_size = rows;
    cfg.write_batch_rows = rows;
    cfg.lease = lease;
    let mut deps = h.deps("runner-1", cfg);
    deps.pacer = Arc::new(WritePacer::new(10));
    (op, record, deps)
}

// The pacers can hold a write batch longer than the lease: at 150 units per
// second, 25 rows of 400 KB wait over a minute against a 60 s lease. The
// lease is renewed during the wait, so no other claim takes the operation
// over meanwhile. Here 40 one-unit rows wait 3 s on a 10-unit bucket against
// a 1 s lease, and a second claim tried 1.5 s into the wait finds nothing.
// By then at least three renewals have run, and the record's cursor is still
// before the staged rows.
async fn a_pacer_wait_longer_than_the_lease_keeps_the_lease_case(backend: Backend) {
    let h = Harness::on(backend).await;
    let (op, record, deps) = one_paced_batch(&h, 40).await;
    let second_claim = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let claimed = h
            .storage
            .claim_runnable("runner-2", Duration::from_secs(60), 10, &[], &[])
            .await
            .expect("claim");
        (claimed, h.record(&op).await)
    };
    let (ran, (claimed, mid)) = tokio::join!(runner::run(&deps, record), second_claim);
    ran.expect("runner-1's run");

    assert!(
        claimed.is_empty(),
        "the lease ran out while the batch waited on the pacer, and another claim took the operation"
    );
    assert_eq!(
        mid.cursor_row_index, 0,
        "a renewal during the wait moved the cursor past rows that were not written"
    );
    assert!(
        deps.pacer.waited() >= Duration::from_millis(2500),
        "40 units against a 10/s bucket wait about 3 s, waited {:?}",
        deps.pacer.waited()
    );
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 40);
    assert_eq!(h.count_items().await, 40);
    h.cleanup().await;
}

// A process that stops during the pacer wait, after its renewals ran, loses
// no row: the renewals left the cursor before the staged rows, so the next
// claim reads them again. Here runner-1 is dropped 1.5 s into a 3 s wait.
async fn a_crash_during_a_pacer_wait_loses_no_row_case(backend: Backend) {
    let h = Harness::on(backend).await;
    let (op, record, deps) = one_paced_batch(&h, 40).await;
    let stopped = tokio::time::timeout(Duration::from_millis(1500), runner::run(&deps, record));
    assert!(
        stopped.await.is_err(),
        "runner-1 must still wait on the pacer"
    );
    assert!(
        deps.pacer.settled_tokens() < 0.0,
        "runner-1 must reach the pacer wait"
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("runner-2's run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 40,
        "the next claim read every staged row again"
    );
    assert_eq!(h.count_items().await, 40);
    h.cleanup().await;
}

// The deadline is checked again after the pacer wait. A runner that is not
// polled through the wait, as a paused process is, runs no renewal, and its
// lease has run out when the wait ends: it drops the batch and stops. Here
// 20 one-unit rows wait 1 s on a 10-unit bucket, and the runner is not
// polled for 1.5 s against a 1 s lease.
async fn a_runner_stalled_through_a_pacer_wait_drops_its_batch_case(backend: Backend) {
    let h = Harness::on(backend).await;
    let (op, record, deps) = one_paced_batch(&h, 20).await;
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    // Poll the run until its batch has taken its units and waits on the
    // bucket. No renewal is due yet: the first comes a third of the lease
    // after the claim.
    while deps.pacer.settled_tokens() >= 0.0 {
        tokio::select! {
            _ = &mut run => panic!("runner-1 must reach the pacer wait"),
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    run.await
        .expect("a lapsed lease is a clean stop, not an error");

    assert_eq!(
        h.count_items().await,
        0,
        "the batch that outwaited the lease was dropped, not written"
    );
    let record = h.record(&op).await;
    assert_eq!(
        record.status, "IN_PROGRESS",
        "the runner stopped at the fence without a terminal transition"
    );
    assert_eq!(record.cursor_row_index, 0);
    h.cleanup().await;
}

on_both_backends!(
    a_pacer_wait_longer_than_the_lease_keeps_the_lease_case,
    a_pacer_wait_longer_than_the_lease_keeps_the_lease,
    a_pacer_wait_longer_than_the_lease_keeps_the_lease_on_postgres
);

on_both_backends!(
    a_crash_during_a_pacer_wait_loses_no_row_case,
    a_crash_during_a_pacer_wait_loses_no_row,
    a_crash_during_a_pacer_wait_loses_no_row_on_postgres
);

on_both_backends!(
    a_runner_stalled_through_a_pacer_wait_drops_its_batch_case,
    a_runner_stalled_through_a_pacer_wait_drops_its_batch,
    a_runner_stalled_through_a_pacer_wait_drops_its_batch_on_postgres
);

// A runner whose lease another owner has taken stops at its next boundary.
//
// The storage layer refuses a stale owner's checkpoint and finish (contract
// cases), so the stale runner can never persist anything; what this pins is
// the runner-side half: on `LeaseState::Lost` at a checkpoint it stops
// reading and writing instead of driving the rest of the input a second
// time behind the new owner.
//
// Runner-1 keeps a 60-second lease by its own clock, so the deadline check
// before a write batch never fires here, while its storage asks the store
// for 100 ms at every checkpoint, so the store's lease runs out. Runner-1
// (batch 5) checkpoints at rows 5 and 10 and is frozen whole before it is
// handed row 13, as a paused process is. The store's lease runs out and a
// second owner runs to completion. Runner-1 then reads rows 13 and 14,
// reaches the boundary at row 15, writes its staged rows (its own deadline
// has not passed), and its checkpoint answers `Lost`: it stops there, 15
// lines handed, not 26.
#[tokio::test]
async fn a_runner_that_lost_its_lease_stops_at_the_next_boundary() {
    let h = Harness::new().await;
    let mut rows = plain_rows(26);
    rows[2] = row_missing_sk(2);
    rows[12] = row_missing_sk(12);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start(files.clone(), input_config(false), both_reports())
        .await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    let (wait, frozen, resume) = Wait::freeze();
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 13, wait));
    let handed = Arc::clone(&source.handed);
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.checkpoint_lease = Some(Duration::from_millis(100));
    let batch_sizes = Arc::clone(&faulting.batch_sizes);
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_secs(60);
    let mut deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    deps.storage = Arc::new(faulting);
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach the freeze"),
        () = frozen.notified() => {}
    }

    // Runner-1 is not polled from here until it is resumed. Its last
    // checkpoint (row 10) renewed the store's lease for 100 ms; wait it
    // out, then a second owner claims and finishes.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps2 = h.deps_with(
        "runner-2",
        cfg,
        Arc::new(h.fs_source()),
        Arc::new(h.fs_sink()),
    );
    runner::run(&deps2, record)
        .await
        .expect("the new owner's run");
    let batches_before_resume = batch_sizes.lock().expect("batch size recorder").len();
    resume.notify_one();
    run.await
        .expect("a lost lease is a clean stop, not an error");

    assert_eq!(
        handed.load(Ordering::SeqCst),
        15,
        "runner-1 reads to the boundary after its lease was taken and no further"
    );
    // One more batch reached storage after the resume, the one at row 15,
    // so the deadline check before the write passed and the stop came from
    // the checkpoint's answer.
    assert_eq!(
        batch_sizes.lock().expect("batch size recorder").len(),
        batches_before_resume + 1,
        "runner-1 wrote its row 15 batch and stopped at the checkpoint after it"
    );
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 26);
    assert_eq!(record.failed_item_count, 2);
    assert_eq!(h.count_items().await, 24);
    assert_eq!(
        record.lease_owner, None,
        "finish cleared the new owner's lease"
    );
}

/// The lease was once checked only at the checkpoint,
/// so a runner stalled past its lease with rows staged wrote them on resume,
/// after a new owner had replayed the same rows and gone on, and a key
/// written twice in file order could end holding the earlier image. The
/// runner now compares its local lease deadline before every write batch.
///
/// Runner-1 checkpoints at row 10 (lease 1 s), stages rows 10 to 14 and
/// is frozen whole at line 13 with three of them staged, as a paused
/// process is. Runner-2 claims the expired lease, finishes the operation,
/// and then k12 to k14 are overwritten with a marker. When runner-1 resumes
/// its staged rows must be dropped: the markers survive and the table ends
/// in the state the new owner left.
///
/// The lease is long next to the time from the checkpoint to line 13, so
/// no renewal is due when the read of line 13 starts and none is under way
/// when polling stops. A renewal stopped inside a storage call could hold
/// the engine's write lock, and runner-2's claim would wait for it without
/// end.
#[tokio::test]
async fn a_runner_stalled_past_its_lease_drops_its_staged_rows_instead_of_writing_them() {
    let h = Harness::new().await;
    let rows = plain_rows(26);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start(files.clone(), input_config(false), both_reports())
        .await;
    let record = h.claim("runner-1", Duration::from_secs(1)).await;

    let (wait, frozen, resume) = Wait::freeze();
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 13, wait));
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.write_batch_rows = 5;
    cfg.lease = Duration::from_secs(1);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach the freeze"),
        () = frozen.notified() => {}
    }

    // Runner-1 is not polled from here until it is resumed.
    tokio::time::sleep(Duration::from_millis(1250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.write_batch_rows = 5;
    let deps2 = h.deps_with(
        "runner-2",
        cfg,
        Arc::new(h.fs_source()),
        Arc::new(h.fs_sink()),
    );
    runner::run(&deps2, record)
        .await
        .expect("the new owner's run");
    // A later write to the keys runner-1 still holds staged. The stale
    // runner must not overwrite it.
    let info = h.key_info().await;
    for i in 12..15 {
        h.storage
            .put_item(
                &info,
                item(&format!("k{i}"), "marker"),
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
                None,
            )
            .await
            .expect("marker put");
    }
    resume.notify_one();
    run.await
        .expect("a lapsed lease is a clean stop, not an error");

    for i in 12..15 {
        let got = h.get(&format!("k{i}")).await.expect("item present");
        assert_eq!(
            got.get("v"),
            Some(&AttributeValue::N("marker".to_owned())),
            "k{i}: the stale runner's staged write must not land after the new owner's"
        );
    }
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 26);
    assert_eq!(h.count_items().await, 26);
}

// An overwrite that moves an item to another GSI key is a delete plus an
// insert on the index (`capacity_helpers::index_write_units`), so the index
// usage for one sub-1 KB item is two write units, 2048 bytes. The figure
// depends on the runner asking storage for the old image on an indexed table
// (`return_old: has_indexes`); without the old image the write is charged as
// an insert, 1024 bytes, and the existing `> 0` assertion cannot tell the two
// apart.
#[tokio::test]
async fn a_gsi_key_change_on_overwrite_charges_delete_plus_insert() {
    let h = Harness::new_with_gsi().await;
    let mut seeded = Item::new();
    seeded.insert("pk".to_owned(), AttributeValue::S("i".to_owned()));
    seeded.insert("sk".to_owned(), AttributeValue::S("s".to_owned()));
    seeded.insert("g".to_owned(), AttributeValue::S("old".to_owned()));
    h.put_direct(seeded).await;
    h.write_input(
        "part-000.json",
        &file_of(&[json!({"Item": {
            "pk": {"S": "i"}, "sk": {"S": "s"}, "g": {"S": "new"}
        }})
        .to_string()]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(2048),
        "a changed index key is one delete plus one insert on the index"
    );
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(1024));
}

// The lease fence deadline is set from the instant taken
// before the checkpoint round trip plus one lease (`runner/write.rs`), so a
// runner stalled past one lease drops its staged rows. Crediting the round
// trip, or using two leases, would let a runner stalled between one and two
// leases still write its staged batch after a new owner had moved on. The
// existing drop test stalls 250 ms against a 100 ms lease, which a deadline
// of up to two leases (200 ms) also passes, so it does not pin the
// arithmetic. This one sits the stall between one lease and two: lease
// 200 ms, stall 300 ms. With the one-lease deadline the flush after the
// stall finds it passed and drops the staged rows; a two-lease deadline
// (400 ms) would not have passed and the rows would be written.
#[tokio::test]
async fn the_lease_fence_deadline_is_one_lease_not_two() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(26)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(200)).await;

    // Just before line 13 is handed (after the checkpoint at row 10 renewed
    // the lease and rows 10 onward are staging), freeze the whole runner for
    // 300 ms: longer than one lease, shorter than two.
    let (wait, frozen, resume) = Wait::freeze();
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 13, wait));
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.write_batch_rows = 5;
    cfg.lease = Duration::from_millis(200);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach the freeze"),
        () = frozen.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    resume.notify_one();
    run.await
        .expect("a lapsed lease is a clean stop, not an error");

    // Rows 0 to 9 were checkpointed and written; the batch staged past the
    // lease (rows 10 to 14) was dropped at the flush, not written.
    assert_eq!(
        h.count_items().await,
        10,
        "the batch staged past one lease was dropped, not written"
    );
    for i in 10..15 {
        assert!(
            h.get(&format!("k{i}")).await.is_none(),
            "k{i}: a batch staged more than one lease after the checkpoint must not be written"
        );
    }
    let record = h.record(&op).await;
    assert_eq!(
        record.status, "IN_PROGRESS",
        "the runner stopped at the fence without a terminal transition"
    );
}
