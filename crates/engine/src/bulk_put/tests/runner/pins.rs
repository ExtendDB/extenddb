// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Three runner properties, one test each: each test
// fails when its property is removed from the runner and passes on the tip.
//
// - no write units for rows found in the ledger on resume (`flush_staged`);
// - `output_checked` restored true on resume (`RunState::restore`);
// - `skip_render` on the held output location fault path
//   (`hold_location_fault`, honoured by `finalize`).

/// A conditional row keyed `k{i}` whose condition passes against an empty
/// table, so the row takes the marked write path and its outcome lands in
/// the row-outcome ledger. A distinct key per row keeps every row writable.
fn marked_row(i: usize) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{i}\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk)\"}}"
    )
}

/// Run one incarnation that dies after `after` lines were handed out, the
/// crash falling between the checkpoint at `checkpoint_rows` and the next
/// one. Each row is its own storage transaction, so rows read after the
/// checkpoint are committed (with their ledger markers) and not covered.
async fn crash_after(h: &Harness, after: u64, checkpoint_rows: usize) {
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = checkpoint_rows;
    cfg.write_batch_rows = 1;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    // The lease expires before a second owner can claim.
    tokio::time::sleep(Duration::from_millis(250)).await;
}

/// Rows an earlier incarnation already wrote are found in the ledger on
/// resume and are not sent again, so they take no write units.
/// Runner-1 checkpoints at row 10 and dies with rows 10 to 12 written under
/// their markers. The resume accounts for those three from the ledger and
/// writes rows 13 and 14, so a bucket of ten units ends with two spent:
/// eight left. Were the three charged, five would be left. Refill between
/// the acquires adds at most ten units per second of the resume's wall
/// time, so the balance is read as settled and the refill bound is taken
/// from the measured duration; a host too slow to keep the two outcomes
/// apart fails the precondition rather than passing by refill.
#[tokio::test]
async fn rows_found_in_the_ledger_on_resume_take_no_write_units() {
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows.extend((10..15).map(marked_row));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    crash_after(&h, 13, 5).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10, "the checkpoint before the crash");
    for row in 10..13 {
        assert_eq!(
            h.storage
                .bulk_row_outcome(ACCOUNT, &op, 0, row)
                .await
                .expect("ledger read"),
            Some(RowOutcome::Applied),
            "row {row} was written and marked before the crash"
        );
    }

    let rate = 10;
    let pacer = Arc::new(WritePacer::new(rate));
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.write_batch_rows = 1;
    let mut deps = h.deps("runner-2", cfg);
    deps.pacer = Arc::clone(&pacer);
    let started = std::time::Instant::now();
    runner::run(&deps, record).await.expect("resumed run");
    let elapsed = started.elapsed();

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 15);
    assert_eq!(h.count_items().await, 15);
    assert_eq!(
        pacer.waited(),
        Duration::ZERO,
        "five one-unit rows never put a ten-unit bucket in deficit"
    );
    // Refill over the whole resume bounds what the bucket could have gained
    // between its first and last acquire. Under 0.2 s that is under two
    // units, which keeps "eight plus refill" apart from "five plus refill".
    let refill_bound = elapsed.as_secs_f64() * rate as f64;
    assert!(
        refill_bound < 2.0,
        "the resume took {elapsed:?}; too slow to tell the refund apart from refill"
    );
    let left = pacer.settled_tokens();
    assert!(
        (8.0..8.0 + refill_bound).contains(&left) || left == 8.0,
        "the resume is charged for the two rows it wrote, not the three it \
         replayed: eight units of ten remain (plus at most {refill_bound:.2} of \
         refill), got {left}"
    );
}

/// A resumed run does not check the output location again when the
/// first incarnation already produced a report record: `output_checked`
/// is restored from `report_record_count > 0`. Runner-1 records two
/// faulting rows, checkpoints at row 10 and dies; the report bucket is
/// then removed. With the property, the resume reads to the end of the
/// input and fails only at render time (every row processed). Without it
/// the check runs again at the first resumed row and the operation fails
/// there with the first-record shape, `S3NoSuchBucket` and `The specified
/// bucket does not exist`, at the checkpoint's counters.
#[tokio::test]
async fn a_resume_after_a_recorded_fault_does_not_check_the_output_location_again() {
    let h = Harness::new().await;
    let mut rows = plain_rows(15);
    rows[2] = row_missing_sk(2);
    rows[7] = row_missing_sk(7);
    rows[12] = row_missing_sk(12);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    crash_after(&h, 12, 5).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10);
    assert_eq!(mid.processed_item_count, 10);
    assert_eq!(
        mid.report_record_count, 2,
        "the first incarnation checkpointed two error records"
    );

    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(OUT_BUCKET))
        .expect("remove the report bucket");

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(
        record.processed_item_count, 15,
        "the resume read every remaining row; it did not fail at its first row"
    );
    assert_eq!(record.failed_item_count, 3);
    assert_eq!(h.count_items().await, 12, "every clean row is written");
    // Measured on the service: with a report object already written,
    // the bucket removed at row 987 of 1,500, the hosted service finished
    // every row and failed at the manifest write with the location code and
    // the manifest prefix, not with an internal error and not with the
    // first-record text.
    assert_eq!(
        record.status, "FAILED",
        "the missing bucket fails the operation at the manifest write"
    );
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Manifest is not available. The specified bucket does not exist")
    );
}

/// An operation whose output location fault is held renders no report:
/// the records were never validated against the location. The hold drops the records waiting for a
/// checkpoint and keeps no later one. Here row 0's condition fails inside
/// the boundary's write batch (batch size 1), and the check runs on that
/// batch's record, before the checkpoint, and finds an output owner that is
/// not the caller. `skip_render` covering a record checkpointed before a
/// later kind's check held a fault is pinned in `output_faults`.
#[tokio::test]
async fn a_held_output_location_fault_renders_no_report_even_for_a_checkpointed_record() {
    let h = Harness::new().await;
    h.put_direct(item("c", "0")).await;
    h.write_input(
        "part-000.json",
        &file_of(&[
            condition_row("9", "attribute_not_exists(pk)"),
            row(1),
            row(2),
        ]),
    );
    let files = h.enumerate().await;
    let mut output = both_reports();
    output["S3BucketOwner"] = json!("999999999999");
    let mut cfg = config();
    cfg.batch_size = 1;
    let record = h.run_to_end(files, input_config(false), output, cfg).await;
    let op = record.operation_id.clone();

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(
        record.processed_item_count, 3,
        "processed minus failed: the condition row counts as processed"
    );
    assert_eq!(
        record.report_record_count, 0,
        "the condition record is checked before its checkpoint"
    );
    assert!(
        h.read_report(&op, ReportKind::ConditionCheckFailure)
            .is_none(),
        "no condition report for an unvalidated output configuration"
    );
    assert!(
        h.read_report(&op, ReportKind::Error).is_none(),
        "no error report either"
    );
    assert!(
        h.read_manifest(&op).is_none(),
        "no manifest on the held location fault path"
    );
}

///  (measured on the service): a cancel sent while the operation runs,
/// with report records already written and the report bucket removed in the
/// same moment, ends `CANCELLED` with the cancel pair and the input bytes
/// read before the stop; the missing bucket is not reported. Here the first incarnation records two faults and dies, the
/// bucket is removed, the cancel is requested, and the resume finds both.
#[tokio::test]
async fn a_cancel_outranks_a_missing_report_bucket() {
    let h = Harness::new().await;
    let mut rows = plain_rows(15);
    rows[2] = row_missing_sk(2);
    rows[7] = row_missing_sk(7);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    crash_after(&h, 12, 5).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.report_record_count, 2);
    assert!(mid.processed_input_bytes > 0);
    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(OUT_BUCKET))
        .expect("remove the report bucket");
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    assert_eq!(record.status, "CANCELLING");
    let mut cfg = config();
    cfg.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(record.failure_code.as_deref(), Some("RequestCancelled"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The request was cancelled")
    );
    // The bytes read before the stop stand (on the service, equal to the
    // total where every row had been processed; here the checkpoint's
    // figure).
    assert_eq!(record.processed_input_bytes, mid.processed_input_bytes);
}

// The index charge on an overwrite. Measurements on the service fix the rule for an ALL-projection GSI, on the
// shape they used: a single hash key `pk` and the index key `gsk`, so
// `{pk: "a", gsk: "g", v: <1,500 bytes>}` is 1,508 bytes and with a
// 2,000-byte `v` is 2,008.
fn v_of(n: usize) -> String {
    "x".repeat(n)
}
fn seed_item(gsk: &str, vbytes: usize) -> Item {
    let mut it = Item::new();
    it.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
    it.insert("gsk".to_owned(), AttributeValue::S(gsk.to_owned()));
    it.insert("v".to_owned(), AttributeValue::S(v_of(vbytes)));
    it
}
fn overwrite_row(gsk: &str, vbytes: usize) -> String {
    json!({"Item": {"pk": {"S": "a"}, "gsk": {"S": gsk}, "v": {"S": v_of(vbytes)}}}).to_string()
}
/// Seed one item, then bulk-write one row that overwrites key `a`, and
/// return the operation's `BulkOperationUsage`.
async fn overwrite_usage(seed_gsk: &str, seed_v: usize, new_gsk: &str, new_v: usize) -> Value {
    let h = Harness::new_with_gsi_hash_key().await;
    h.put_direct(seed_item(seed_gsk, seed_v)).await;
    h.write_input("part-000.json", &file_of(&[overwrite_row(new_gsk, new_v)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    record.usage
}

// A changed projection under the same index key is charged the larger
// of the old and new projected bytes. Seed v 1,500 B (1,508), overwrite
// v 2,000 B (2,008), same gsk: the index is charged max(1,508, 2,008) =
// 2,008, matching the service.
#[tokio::test]
async fn an_overwrite_changing_the_projection_under_the_same_index_key_charges_the_larger() {
    let usage = overwrite_usage("g", 1500, "g", 2000).await;
    assert_eq!(
        usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(2008),
        "a changed projection under the same key is charged max(old, new)"
    );
    assert_eq!(usage["Table"]["BulkPutBytes"], json!(2008));
    assert_eq!(usage["BulkPutBytes"], json!(4016));
}

// A moved index key is a delete of the old projected entry plus an
// insert of the new, so the index is charged old plus new. Seed
// {gsk: g, v 2,000 B} (2,008), overwrite {gsk: h, v 2,000 B} (2,008): the
// index is charged 2,008 + 2,008 = 4,016, matching the service.
#[tokio::test]
async fn an_overwrite_moving_the_index_key_charges_old_plus_new() {
    let usage = overwrite_usage("g", 2000, "h", 2000).await;
    assert_eq!(
        usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(4016),
        "a moved index key is charged old plus new"
    );
    assert_eq!(usage["Table"]["BulkPutBytes"], json!(2008));
    assert_eq!(usage["BulkPutBytes"], json!(6024));
}

// An identical overwrite is charged the index nothing and reports no
// index member; the table is charged the larger of old and new (equal
// here), 1,508. Matches the service. A plain row met again after
// a crash is charged the same way, so a crashed run undercharges the index
// for such rows against a crash-free run (see the resume batched test).
#[tokio::test]
async fn an_identical_overwrite_charges_the_index_nothing() {
    let usage = overwrite_usage("g", 1500, "g", 1500).await;
    assert!(
        usage["GlobalSecondaryIndexes"]["gsi1"].is_null(),
        "an unchanged projection charges the index nothing and reports no member"
    );
    assert_eq!(usage["Table"]["BulkPutBytes"], json!(1508));
    assert_eq!(usage["BulkPutBytes"], json!(1508));
}

// A shrinking overwrite under the same index key. Seed v 2,000 B
// (2,008), overwrite v 1,500 B (1,508): the index is charged the larger,
// 2,008, AND the table is charged the larger, 2,008 (total 4,016). Matches
// the service. The table larger-of rule is what distinguishes this from
// charging the new item alone (which would report table 1,508).
#[tokio::test]
async fn a_shrinking_overwrite_under_the_same_index_key_charges_the_larger() {
    let usage = overwrite_usage("g", 2000, "g", 1500).await;
    assert_eq!(
        usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(2008),
        "the index is charged max(old, new)"
    );
    assert_eq!(
        usage["Table"]["BulkPutBytes"],
        json!(2008),
        "the table is charged max(old, new) too"
    );
    assert_eq!(usage["BulkPutBytes"], json!(4016));
}

// A moved index key with unequal sizes. Seed {gsk g, v 1,500 B}
// (1,508), overwrite {gsk h, v 2,000 B} (2,008): the index is charged old
// plus new (1,508 + 2,008 = 3,516), the table the larger (2,008), total
// 5,524. Matches the service.
#[tokio::test]
async fn a_moved_index_key_with_unequal_sizes_charges_old_plus_new() {
    let usage = overwrite_usage("g", 1500, "h", 2000).await;
    assert_eq!(
        usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(3516),
        "a moved index key is charged old plus new"
    );
    assert_eq!(usage["Table"]["BulkPutBytes"], json!(2008));
    assert_eq!(usage["BulkPutBytes"], json!(5524));
}

// A KEYS_ONLY GSI whose projected entry is unchanged under the same
// index key is charged nothing and reports no member, even when a
// non-projected attribute changed. seed {gsk g, v 1,500 B}, overwrite
// {gsk g, v 2,000 B}: the index projects only pk and gsk, both unchanged,
// so no member; the table is the larger, 2,008. Matches the service, and
// reaches the same-key-unchanged-projection branch an ALL projection
// cannot (its projection changes whenever any attribute does).
#[tokio::test]
async fn an_overwrite_under_a_keys_only_index_with_an_unchanged_key_charges_nothing() {
    let h = Harness::new_with_gsi_keys_only_hash_key().await;
    h.put_direct(seed_item("g", 1500)).await;
    h.write_input("part-000.json", &file_of(&[overwrite_row("g", 2000)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"].is_null(),
        "an unchanged KEYS_ONLY projection charges the index nothing and reports no member"
    );
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(2008));
}

// The larger-of table rule applies to a table with no index too.
// Seed {pk a, sk row, v 2,000 B} (2,009), overwrite v 1,500 B (1,509): the
// table is charged max(2,009, 1,509) = 2,009. This needs the replaced
// item's size on an unindexed table, so the runner requests the old image
// on every write. Matches the service.
#[tokio::test]
async fn an_overwrite_on_a_plain_table_charges_the_table_the_larger() {
    let h = Harness::new().await;
    let mut seeded = Item::new();
    seeded.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
    seeded.insert("sk".to_owned(), AttributeValue::S("row".to_owned()));
    seeded.insert("v".to_owned(), AttributeValue::S(v_of(2000)));
    h.put_direct(seeded).await;
    h.write_input(
        "part-000.json",
        &file_of(&[json!({"Item": {
            "pk": {"S": "a"}, "sk": {"S": "row"}, "v": {"S": v_of(1500)}
        }})
        .to_string()]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.usage["Table"]["BulkPutBytes"],
        json!(2009),
        "the table is charged max(old, new) with no index present"
    );
    assert_eq!(record.usage["BulkPutBytes"], json!(2009));
    assert!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"].is_null(),
        "a plain table reports no index member"
    );
}
