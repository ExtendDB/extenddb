// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk write ledger and the write owner row during a bulk batch, run
//! against the `PostgreSQL` backend.

use super::*;

/// A ledger statement that fails names its SQLSTATE in the error, so the
/// bulk runner can tell a transient fault (a full disk, 53100) from a
/// terminal one. The ledger insert is made to fail with a check that no row
/// passes (23514), which the runner treats as terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ledger_statement_error_carries_its_sqlstate() {
    let test = "a_ledger_statement_error_carries_its_sqlstate";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let (key_info, _) = seeded_table(&s, "ledger_fault", 0).await;
    sqlx::query("ALTER TABLE bulk_row_outcomes ADD CONSTRAINT refuse_all CHECK (false) NOT VALID")
        .execute(&s.db)
        .await
        .expect("add the refusing check");
    s.engine
        .bulk_write_takeover(ACCOUNT, "op-ledger", "owner-1", (0, 0))
        .await
        .expect("record the write owner");
    let mut marked = row(item("k1", None));
    marked.marker = Some(extenddb_storage::BulkRowMarker {
        account_id: ACCOUNT.to_owned(),
        operation_id: "op-ledger".to_owned(),
        file_index: 0,
        row_index: 0,
        owner: "owner-1".to_owned(),
        keep_old: false,
        settled: (0, 0),
    });
    let error = s
        .engine
        .put_items_batch(&key_info, vec![marked], None)
        .await
        .expect_err("the ledger insert fails");
    match &error {
        StorageError::Internal(message) => assert!(
            message.starts_with("SQLSTATE 23514: "),
            "the error names its SQLSTATE: {message}"
        ),
        other => panic!("expected an internal error, got {other:?}"),
    }
    s.cleanup().await;
}

/// A full disk met by the ledger insert is a transient write fault: the
/// error carries SQLSTATE 53100, which the bulk runner resumes after, and
/// the batch wrote nothing. A trigger on the ledger table raises
/// `disk_full` for every insert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_disk_on_the_ledger_insert_is_a_transient_fault() {
    let test = "a_full_disk_on_the_ledger_insert_is_a_transient_fault";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let (key_info, _) = seeded_table(&s, "ledger_full", 0).await;
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_ledger_rows() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'could not extend file: No space left on device' \
         USING ERRCODE = 'disk_full'; END $$; \
         CREATE TRIGGER refuse_ledger_rows BEFORE INSERT ON bulk_row_outcomes \
         FOR EACH STATEMENT EXECUTE FUNCTION refuse_ledger_rows();",
    )
    .execute(&s.db)
    .await
    .expect("add the refusing trigger");
    s.engine
        .bulk_write_takeover(ACCOUNT, "op-full", "owner-1", (0, 0))
        .await
        .expect("record the write owner");
    let rows: Vec<BatchRow> = (0..3_i64)
        .map(|index| {
            let mut marked = row(item(&format!("k{index}"), None));
            marked.marker = Some(extenddb_storage::BulkRowMarker {
                account_id: ACCOUNT.to_owned(),
                operation_id: "op-full".to_owned(),
                file_index: 0,
                row_index: index,
                owner: "owner-1".to_owned(),
                keep_old: false,
                settled: (0, 0),
            });
            marked
        })
        .collect();
    let error = s
        .engine
        .put_items_batch(&key_info, rows, None)
        .await
        .expect_err("the ledger insert meets a full disk");
    assert!(
        error.is_transient_write_fault(),
        "a full disk on the ledger insert is transient: {error:?}"
    );
    let written: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM \"_ddb_{}\"",
        key_info.table_id
    ))
    .fetch_one(&s.db)
    .await
    .expect("count the items");
    assert_eq!(written, 0, "the batch wrote nothing");
    s.cleanup().await;
}

/// A process stopped inside a bulk batch transaction, after the batch has
/// locked the operation's write owner row, holds that row locked, and the
/// next claim's takeover waits on it. The data database ends a bulk batch
/// transaction left idle for the bound the store puts on its own
/// transactions, so the takeover returns within that bound and the stopped
/// batch's rows are rolled back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_stopped_inside_a_bulk_batch_releases_the_write_owner_row() {
    let test = "a_process_stopped_inside_a_bulk_batch_releases_the_write_owner_row";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let (key_info, _) = seeded_table(&s, "stopped_batch", 0).await;
    s.engine
        .bulk_write_takeover(ACCOUNT, "op-stopped", "owner-1", (0, 0))
        .await
        .expect("record the write owner");

    // A transaction of the test's own holds the ledger key the batch
    // inserts, so the batch stops at that insert with the owner row already
    // locked by its fence.
    let mut blocker = s.db.begin().await.expect("begin the blocker");
    sqlx::query(
        "INSERT INTO bulk_row_outcomes (account_id, operation_id, file_index, row_index, outcome) \
         VALUES ($1, 'op-stopped', 0, 0, 'applied')",
    )
    .bind(ACCOUNT)
    .execute(&mut *blocker)
    .await
    .expect("hold the ledger key");
    let mut marked = row(item("k1", None));
    marked.marker = Some(extenddb_storage::BulkRowMarker {
        account_id: ACCOUNT.to_owned(),
        operation_id: "op-stopped".to_owned(),
        file_index: 0,
        row_index: 0,
        owner: "owner-1".to_owned(),
        keep_old: false,
        settled: (0, 0),
    });
    let mut batch = Box::pin(s.engine.put_items_batch(&key_info, vec![marked], None));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), batch.as_mut())
            .await
            .is_err(),
        "the batch waits at its ledger insert"
    );
    // The insert now completes on the server and the batch is never polled
    // again: its connection sits idle inside the transaction, holding the
    // owner row, as a process stopped there leaves it.
    blocker.rollback().await.expect("release the ledger key");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let asked = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(40),
        s.engine
            .bulk_write_takeover(ACCOUNT, "op-stopped", "owner-2", (0, 0)),
    )
    .await
    .expect("the takeover returns while the stopped batch is open")
    .expect("takeover");
    let waited = asked.elapsed();
    assert!(
        waited < Duration::from_secs(25),
        "the takeover waited {waited:?} for the stopped batch"
    );
    let recorded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bulk_row_outcomes WHERE operation_id = 'op-stopped'",
    )
    .fetch_one(&s.db)
    .await
    .expect("count the ledger rows");
    assert_eq!(recorded, 0, "the stopped batch was rolled back");
    drop(batch);
    s.cleanup().await;
}
