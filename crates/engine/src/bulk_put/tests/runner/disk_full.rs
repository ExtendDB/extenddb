// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// A write batch that fails because the database ran out of disk (SQLSTATE
// 53100, as the PostgreSQL storage layer renders it) rolled back, so the
// operation stays in progress and a later claim finishes it from the last
// checkpoint, the same path a connection loss takes.
//
// The storage double fails the first write of k3 with that internal error.
// The first claim's run returns it as a runner storage error with the
// operation still IN_PROGRESS and no terminal failure; a second claim after
// the lease expires finishes it with the same rows, counters and usage as
// an uninterrupted run.
#[tokio::test]
async fn a_batch_that_meets_a_full_disk_leaves_the_operation_to_resume() {
    let truth_h = Harness::new().await;
    truth_h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let truth_files = truth_h.enumerate().await;
    let truth = truth_h
        .run_to_end(truth_files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(truth.status, "COMPLETED");

    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_put_pk = Some("k3".to_owned());
    faulting.fail_put_pk_internal = Some(
        "SQLSTATE 53100: error returned from database: could not extend file: \
         No space left on device"
            .to_owned(),
    );
    let storage: Arc<dyn StorageEngine> = Arc::new(faulting);

    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let deps = h.deps_with_storage("runner-1", config(), Arc::clone(&storage));
    let result = runner::run(&deps, record).await;
    assert!(
        matches!(result, Err(runner::RunnerError::Storage(_))),
        "a full disk on a write batch is a runner storage error, not a FAILED operation"
    );
    let mid = h.record(&op).await;
    assert_eq!(
        mid.status, "IN_PROGRESS",
        "the operation stays non-terminal after a full disk"
    );
    assert!(
        mid.failure_code.is_none(),
        "no terminal failure was written"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let deps = h.deps_with_storage("runner-2", config(), Arc::clone(&storage));
    runner::run(&deps, record)
        .await
        .expect("the resume finishes");

    let final_record = h.record(&op).await;
    assert_eq!(final_record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 10);
    assert_eq!(
        final_record.processed_item_count,
        truth.processed_item_count
    );
    assert_eq!(final_record.failed_item_count, truth.failed_item_count);
    assert_eq!(final_record.usage, truth.usage);
}
