// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `SQLite` `BulkOperationStore`: index definitions, the start
//! transaction and the stored `input_files` shape.

use super::*;
use crate::store::SqliteEngine;

pub(super) async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 2, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query(
        "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default')",
    )
    .execute(&engine.pool)
    .await
    .expect("account");
    // The start transaction refuses a table whose row is absent, so the
    // tables the cases start on exist.
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
             table_status, table_arn, table_id) \
         VALUES ('000000000000', 't', '[]', '[]', 'ACTIVE', \
             'arn:aws:dynamodb:us-east-1:000000000000:table/t', 'table-1'), \
             ('000000000000', 't2', '[]', '[]', 'ACTIVE', \
             'arn:aws:dynamodb:us-east-1:000000000000:table/t2', 'table-2'), \
             ('000000000000', 't3', '[]', '[]', 'ACTIVE', \
             'arn:aws:dynamodb:us-east-1:000000000000:table/t3', 'table-3')",
    )
    .execute(&engine.pool)
    .await
    .expect("tables");
    engine
}

pub(super) fn new_op(
    operation_id: &str,
    table_id: &str,
    token: Option<&str>,
    fingerprint: &str,
) -> NewBulkOperation {
    NewBulkOperation {
        operation_id: operation_id.to_owned(),
        table_id: table_id.to_owned(),
        table_name: "t".to_owned(),
        table_arn: format!("arn:aws:dynamodb:us-east-1:000000000000:table/{table_id}"),
        client_token: token.map(str::to_owned),
        // The engine hashes the token before calling storage; any distinct
        // string stands in for the hash here.
        client_token_hash: token.map(|t| format!("hash-{t}")),
        request_fingerprint: fingerprint.to_owned(),
        input_config: serde_json::json!({"S3Bucket": "b"}),
        output_config: serde_json::json!({}),
        input_files: vec![InputFile {
            path: "data/f0.json".to_owned(),
            size: 10,
            rows: None,
            etag: None,
            abandon: None,
        }],
        max_write_request_units: None,
        performance_tier: None,
        start_time_ms: 1_000,
        total_input_bytes: Some(10),
        initial_failure: None,
        run_context: None,
    }
}

/// Raw insert of a minimal row, bypassing the store, to exercise the
/// index definitions themselves.
async fn insert_raw(
    engine: &SqliteEngine,
    operation_id: &str,
    table_id: &str,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO bulk_operations ( \
             account_id, operation_id, table_id, table_name, table_arn, status, \
             request_fingerprint, input_config, output_config, input_files, \
             start_time_ms, total_input_bytes) \
         VALUES ('000000000000', ?, ?, 't', 'arn', ?, 'f', '{}', '{}', '[]', 0, 0)",
    )
    .bind(operation_id)
    .bind(table_id)
    .bind(status)
    .execute(&engine.pool)
    .await
    .map(|_| ())
}

/// The per-table guard is a partial unique index: two non-terminal rows
/// for one table conflict, and a terminal row on the same table does not
/// participate. The contract tests only observe the guard through
/// `start_bulk_operation`; this pins the index definition itself.
#[tokio::test]
async fn the_table_guard_index_is_partial_and_unique() {
    let engine = engine().await;
    insert_raw(&engine, "op-1", "table-1", "IN_PROGRESS")
        .await
        .expect("first non-terminal row");
    let conflict = insert_raw(&engine, "op-2", "table-1", "CANCELLING")
        .await
        .expect_err("second non-terminal row on the same table must conflict");
    assert!(
        conflict.to_string().contains("UNIQUE"),
        "expected a unique constraint violation, got: {conflict}"
    );
    insert_raw(&engine, "op-3", "table-1", "COMPLETED")
        .await
        .expect("a terminal row is outside the partial index");
}

/// Step 5 of the start transaction: the insert was ignored, the
/// transaction is still live, and the re-select of the token decides the
/// outcome. In-process, BEGIN IMMEDIATE plus the write lock mean a token
/// row committed by anyone else is already visible at step 2, so these
/// tests drive step 5 through the two conflicts that reach it: the
/// per-table guard and the primary key. The token-found branch defends
/// the same decision tree for a second process on one database file.
#[tokio::test]
async fn a_guard_conflict_with_a_fresh_token_answers_table_busy() {
    let engine = engine().await;
    let created = engine
        .start_bulk_operation("000000000000", new_op("op-1", "table-1", None, "f1"), 50)
        .await
        .expect("start");
    assert!(matches!(created, StartOutcome::Created(_)));
    // Same table, a token never seen: step 2 finds nothing, the insert is
    // ignored by the guard, and step 5's re-select finds nothing either.
    let busy = engine
        .start_bulk_operation(
            "000000000000",
            new_op("op-2", "table-1", Some("t2"), "f2"),
            50,
        )
        .await
        .expect("start");
    assert!(matches!(busy, StartOutcome::TableBusy));
}

#[tokio::test]
async fn a_primary_key_conflict_without_a_token_row_answers_table_busy() {
    let engine = engine().await;
    let created = engine
        .start_bulk_operation("000000000000", new_op("op-1", "table-1", None, "f1"), 50)
        .await
        .expect("start");
    assert!(matches!(created, StartOutcome::Created(_)));
    // Same operation id, different table: the guard passes, the primary
    // key conflicts, and step 5 classifies it through the token re-select.
    let busy = engine
        .start_bulk_operation(
            "000000000000",
            new_op("op-1", "table-2", Some("t3"), "f3"),
            50,
        )
        .await
        .expect("start");
    assert!(matches!(busy, StartOutcome::TableBusy));
}

#[tokio::test]
async fn a_seen_token_replays_or_mismatches_by_fingerprint() {
    let engine = engine().await;
    let created = engine
        .start_bulk_operation(
            "000000000000",
            new_op("op-1", "table-1", Some("tok"), "f1"),
            50,
        )
        .await
        .expect("start");
    assert!(matches!(created, StartOutcome::Created(_)));
    let replay = engine
        .start_bulk_operation(
            "000000000000",
            new_op("op-2", "table-2", Some("tok"), "f1"),
            50,
        )
        .await
        .expect("start");
    match replay {
        StartOutcome::IdempotentReplay(record) => assert_eq!(record.operation_id, "op-1"),
        _ => panic!("same token and fingerprint must replay"),
    }
    let mismatch = engine
        .start_bulk_operation(
            "000000000000",
            new_op("op-3", "table-3", Some("tok"), "different"),
            50,
        )
        .await
        .expect("start");
    assert!(matches!(mismatch, StartOutcome::IdempotentMismatch));
}

/// A failure computed by the runner that races a cancel must end CANCELLED
/// (CANCELLING always wins) AND must not carry the
/// runner-computed FailureCode/FailureMessage: no cancelled operation has
/// one on the wire. This composes the store primitives exactly as the
/// runner does on the failure-plus-cancel path. Regression test for the
/// finish CASE on the failure fields.
#[tokio::test]
async fn a_cancel_racing_a_failure_ends_cancelled_with_the_cancelled_failure_code() {
    let engine = engine().await;
    let created = engine
        .start_bulk_operation("000000000000", new_op("op-1", "table-1", None, "f1"), 50)
        .await
        .expect("start");
    assert!(matches!(created, StartOutcome::Created(_)));

    let claimed = engine
        .claim_runnable("owner-A", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);

    let cancel = engine
        .request_cancel("000000000000", "op-1")
        .await
        .expect("cancel");
    assert!(matches!(cancel, CancelOutcome::Cancelling));

    let state = engine
        .finish(
            "000000000000",
            "op-1",
            "owner-A",
            Terminal {
                status: "FAILED".to_owned(),
                end_time_ms: 2_000,
                failure_code: Some("DynamoDBResourceNotFoundException".to_owned()),
                failure_message: Some("Requested resource not found".to_owned()),
                usage: serde_json::json!({}),
            },
        )
        .await
        .expect("finish");

    match state {
        FinishState::Held(status) => assert_eq!(status, "CANCELLED"),
        FinishState::Lost => panic!("the owner still holds the lease"),
    }

    let row = engine
        .get_bulk_operation("000000000000", "op-1")
        .await
        .expect("get")
        .expect("row");
    assert_eq!(row.status, "CANCELLED");
    assert_eq!(
        row.failure_code.as_deref(),
        Some(extenddb_storage::CANCELLED_FAILURE_CODE),
        "a CANCELLED operation carries RequestCancelled, not the runner's failure code"
    );
    assert_eq!(
        row.failure_message.as_deref(),
        Some(extenddb_storage::CANCELLED_FAILURE_MESSAGE),
        "a CANCELLED operation carries the cancelled message, not the runner's"
    );
}

/// A `cancel_requested` that holds text or a blob is what makes the row
/// unreadable; it is not a cancel request. The claim ends such a row
/// `FAILED` with the internal pair, as any other unreadable row without a
/// cancel, and not `CANCELLED`.
#[tokio::test]
async fn a_malformed_cancel_request_ends_an_unreadable_row_failed() {
    let engine = engine().await;
    for (i, value) in ["'abc'", "''", "x'78'"].iter().enumerate() {
        let operation_id = format!("op-{i}");
        let created = engine
            .start_bulk_operation(
                "000000000000",
                new_op(&operation_id, "table-1", None, "f1"),
                50,
            )
            .await
            .expect("start");
        assert!(matches!(created, StartOutcome::Created(_)), "{value}");
        sqlx::query(&format!(
            "UPDATE bulk_operations SET cancel_requested = {value} WHERE operation_id = ?"
        ))
        .bind(&operation_id)
        .execute(&engine.pool)
        .await
        .expect("store a malformed cancel_requested");

        let claimed = engine
            .claim_runnable("owner-A", Duration::from_secs(60), 10, &[], &[])
            .await
            .expect("a claim over an unreadable row still succeeds");
        assert!(
            claimed.is_empty(),
            "{value}: the unreadable row is not returned"
        );

        let (status, code, message): (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT status, failure_code, failure_message FROM bulk_operations \
             WHERE operation_id = ?",
        )
        .bind(&operation_id)
        .fetch_one(&engine.pool)
        .await
        .expect("read the ended row");
        assert_eq!(status, "FAILED", "{value}");
        assert_eq!(
            code.as_deref(),
            Some(extenddb_storage::bulk_operation_store::UNREADABLE_FAILURE_CODE),
            "{value}"
        );
        assert_eq!(
            message.as_deref(),
            Some(extenddb_storage::bulk_operation_store::UNREADABLE_FAILURE_MESSAGE),
            "{value}"
        );
    }
}
