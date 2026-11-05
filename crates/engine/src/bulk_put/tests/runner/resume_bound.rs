// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The resume bound against the other ways an operation ends. The claim
//! that spends the bound reads no input; when it finds the table deleted,
//! the operation ends with the deleted-table failure, as every other claim
//! after the deletion would, and when it finds a cancel pending, the
//! operation ends `CANCELLED` with the cancel pair.

use super::*;

/// Checkpoints and write batches of five rows, the exactly-once cases'
/// window, so the first batch holds the faulted row.
fn window() -> RunnerConfig {
    RunnerConfig {
        batch_size: 5,
        write_batch_rows: 5,
        ..config()
    }
}

/// Run `claims` claims whose first batch fails transiently, so the
/// checkpoint never moves: the first claim and `claims - 1` resumes. Each
/// leaves the operation `IN_PROGRESS` for the next.
async fn stall(h: &Harness, op: &str, claims: usize) {
    for n in 0..claims {
        let owner = format!("runner-{n}");
        let record = h.claim(&owner, Duration::from_millis(100)).await;
        let mut faulting = FaultingStorage::new(&h.storage);
        faulting.fail_put_pk = Some("k0".to_owned());
        faulting.checkpoint_lease = Some(Duration::from_millis(100));
        let deps = h.deps_with_storage(&owner, window(), Arc::new(faulting));
        assert!(
            runner::run(&deps, record).await.is_err(),
            "claim {n} leaves the operation to resume"
        );
        assert_eq!(h.record(op).await.status, "IN_PROGRESS");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// The claim that spends the bound: the first claim and nine resumes ran,
/// so this is the tenth resume without progress.
async fn spend_the_bound(h: &Harness) {
    let record = h.claim("runner-last", Duration::from_millis(100)).await;
    let deps = h.deps("runner-last", window());
    runner::run(&deps, record)
        .await
        .expect("the run that ends the operation returns cleanly");
}

async fn stalled_operation(h: &Harness) -> String {
    h.write_input("part-000.json", &file_of(&plain_rows(6)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    stall(h, &op, 10).await;
    op
}

#[tokio::test]
async fn a_table_deleted_while_resumes_stall_ends_with_the_deleted_table_failure() {
    let h = Harness::new().await;
    let op = stalled_operation(&h).await;
    h.storage
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("delete table");
    spend_the_bound(&h).await;

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("DynamoDBResourceNotFoundException")
    );
}

#[tokio::test]
async fn a_cancel_while_resumes_stall_ends_cancelled() {
    let h = Harness::new().await;
    let op = stalled_operation(&h).await;
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    spend_the_bound(&h).await;

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(record.failure_code.as_deref(), Some("RequestCancelled"));
    assert_eq!(h.count_items().await, 0);
}
