// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A load of many one-row input files costs the same per file whatever the
//! file count: the checkpoint that moves the cursor past a file writes that
//! file's figures alone, and the read the runner makes at every batch
//! boundary reads the operation row without its file list.

use std::time::Instant;

use super::*;

/// The lease the loads run under, long enough that no case here sees it
/// run out.
const LOAD_LEASE: Duration = Duration::from_secs(600);

/// One checkpoint of a load of one-row files: the cursor moves past file
/// `index`, which finished with one row and a tag.
fn passed_file(index: usize) -> Progress {
    let cursor = i64::try_from(index + 1).unwrap_or(i64::MAX);
    Progress {
        cursor_file_index: cursor,
        cursor_row_index: 0,
        processed_item_count: cursor,
        failed_item_count: 0,
        processed_input_bytes: cursor.saturating_mul(10),
        report_record_count: 0,
        report_bytes: 0,
        usage: json!({"WriteRequestUnits": cursor}),
        records: Vec::new(),
        finished_files: vec![FinishedFile {
            index,
            rows: 1,
            etag: Some(format!("{index:032x}")),
        }],
        abandoned_files: Vec::new(),
    }
}

/// Runs the store calls a load of `files` one-row files makes and returns
/// how long the per-file part took: for every file, the checkpoint that
/// passes it and the boundary read that follows. The start, the claim and
/// the finish run outside the timed part.
async fn time_a_load<E: StorageEngine>(fx: &Fixture<E>, files: usize) -> Duration {
    let table = create_table(fx, &fx.account_a, &format!("many_files_{files}")).await;
    let owner = format!("many-files-{files}");
    let mut req = new_op(fx, &fx.account_a, &table, None, "fp-many-files", 1000);
    req.input_files = (0..files)
        .map(|i| InputFile {
            path: format!("data/part-{i:05}.json"),
            size: 10,
            ..InputFile::default()
        })
        .collect();
    let id = created(start(fx, &fx.account_a, req, 50).await).operation_id;
    let claimed = fx
        .engine
        .claim_runnable(&owner, LOAD_LEASE, 10, &[], &[])
        .await
        .expect("claim");
    let record = claimed
        .iter()
        .find(|r| r.operation_id == id)
        .expect("the load is claimed");
    assert_eq!(
        record.input_files.len(),
        files,
        "the claim reads every file"
    );

    let began = Instant::now();
    for index in 0..files {
        let state = fx
            .engine
            .checkpoint(&fx.account_a, &id, &owner, LOAD_LEASE, passed_file(index))
            .await
            .expect("checkpoint");
        assert!(matches!(state, LeaseState::Held), "{state:?}");
        let flags = fx
            .engine
            .get_bulk_operation_flags(&fx.account_a, &id)
            .await
            .expect("boundary read")
            .expect("the operation exists");
        assert!(!flags.cancel_requested);
        assert_eq!(flags.lease_owner.as_deref(), Some(owner.as_str()));
    }
    let elapsed = began.elapsed();

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.input_files.len(), files);
    assert_eq!(row.cursor_file_index, i64::try_from(files).unwrap_or(0));
    for (index, file) in row.input_files.iter().enumerate() {
        assert_eq!(file.path, format!("data/part-{index:05}.json"));
        assert_eq!(file.rows, Some(1), "{}", file.path);
        assert_eq!(
            file.etag.as_deref(),
            Some(format!("{index:032x}").as_str()),
            "{}",
            file.path
        );
    }
    let done = fx
        .engine
        .finish(
            &fx.account_a,
            &id,
            &owner,
            terminal("COMPLETED", 2000 + i64::try_from(files).unwrap_or(0)),
        )
        .await
        .expect("finish");
    assert!(matches!(done, FinishState::Held(_)));
    elapsed
}

/// A load of 5,000 one-row files costs no more per file than a load of
/// 500, and its per-file store calls finish inside two minutes. A
/// checkpoint that rewrote the whole file list, or a boundary read that
/// read it, makes each file cost in proportion to the file count, which
/// puts the larger load about ten times over the smaller one per file.
pub async fn a_many_file_load_costs_the_same_per_file_as_a_small_one<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    const SMALL: usize = 500;
    const LARGE: usize = 5_000;
    let small = time_a_load(fx, SMALL).await;
    let large = time_a_load(fx, LARGE).await;
    eprintln!(
        "store calls of a {SMALL}-file load took {small:?}, of a {LARGE}-file load {large:?}"
    );
    #[allow(clippy::cast_precision_loss)] // the file counts fit f64 exactly
    let (per_small, per_large) = (
        small.as_secs_f64() / SMALL as f64,
        large.as_secs_f64() / LARGE as f64,
    );
    assert!(
        large < Duration::from_secs(120),
        "the per-file store calls of a {LARGE}-file load took {large:?}"
    );
    assert!(
        per_large < per_small * 3.0,
        "a file of a {LARGE}-file load cost {:.3} ms against {:.3} ms in a {SMALL}-file load",
        per_large * 1000.0,
        per_small * 1000.0
    );
}

/// The boundary read answers the status, the cancel flag and the lease
/// holder as a full read does, through a claim and a cancel, and `None`
/// for an operation no row names.
pub async fn the_boundary_read_follows_the_cancel_and_the_lease<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "boundary_flags").await;
    let id = started_and_claimed(fx, &table, "flags-owner", 1000).await;
    let read = |id: String| async move {
        fx.engine
            .get_bulk_operation_flags(&fx.account_a, &id)
            .await
            .expect("boundary read")
    };
    let flags = read(id.clone()).await.expect("the operation exists");
    assert_eq!(flags.status, "IN_PROGRESS");
    assert!(!flags.cancel_requested);
    assert_eq!(flags.lease_owner.as_deref(), Some("flags-owner"));

    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, &id)
        .await
        .expect("cancel");
    assert!(matches!(cancel, CancelOutcome::Cancelling));
    let flags = read(id.clone()).await.expect("the operation exists");
    assert_eq!(flags.status, "CANCELLING");
    assert!(flags.cancel_requested);
    let full = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(flags, crate::OperationFlags::of(&full));

    assert!(
        read(op_id(1000)).await.is_none(),
        "an id no row names reads as absent"
    );
    assert!(
        fx.engine
            .get_bulk_operation_flags(&fx.account_b, &id)
            .await
            .expect("boundary read")
            .is_none(),
        "another account does not see the operation"
    );
}
