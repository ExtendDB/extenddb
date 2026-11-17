// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A disable and a `TransactWriteItems` lock the rows of a stream's shards in
//! one order, shard id order. A transaction that stores records on two shards
//! of a stream therefore cannot deadlock with the close of that stream.
//!
//! A trigger on `stream_pending` waits on an advisory lock, the gate. Each
//! test uses it to hold the transaction at its first stream record, after the
//! transaction locked that record's shard row. The test then starts the
//! disable. When the close waits for the transaction, the test opens the gate.
//! If the two lock in different orders, the transaction then waits for a row
//! that the close holds. PostgreSQL then cancels one of them after
//! `deadlock_timeout`.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`), and skips without it. Each test
//! builds its own scratch database and drops it when it passes. A failing
//! test leaves its database, named `eddb_sdis_*`, behind.

#[path = "common/stream_disable_fixture.rs"]
mod fixture;

use std::time::Duration;

use extenddb_core::types::{Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo};
use extenddb_storage::TransactWriteOp;
use fixture::*;
use sqlx::{Connection, PgConnection};

/// The advisory lock key of the gate.
const GATE: i64 = 0x4c6b_4f72;

/// What a raced transaction and disable returned.
type Raced = (
    Result<(), StorageError>,
    Result<TableDescription, StorageError>,
);

/// Open a connection to the scratch database outside its pools. Returns it and
/// its backend pid.
async fn connect(s: &Scratch) -> (PgConnection, i32) {
    let base = base_conn().expect("caller checks base_conn() first");
    let mut conn = PgConnection::connect(&format!("{base}/{}", s.db_name))
        .await
        .expect("connect to the scratch database");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .expect("read the backend pid");
    (conn, pid)
}

/// Make a write wait at the end of each stream record it stores while the gate
/// is closed. The write has locked the record's shard row by then.
async fn install_record_gate(s: &Scratch) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION hold_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           PERFORM pg_advisory_xact_lock_shared({GATE}); \
           RETURN NULL; \
         END $$; \
         CREATE TRIGGER hold_record AFTER INSERT ON stream_pending \
           FOR EACH ROW EXECUTE FUNCTION hold_record();"
    ))
    .execute(&s.db)
    .await
    .expect("install the gate trigger");
}

/// Close the gate on a session of its own. Returns the session and its pid.
async fn close_gate(s: &Scratch) -> (PgConnection, i32) {
    let (mut gate, pid) = connect(s).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GATE)
        .execute(&mut gate)
        .await
        .expect("close the gate");
    (gate, pid)
}

async fn open_gate(gate: &mut PgConnection) {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(GATE)
        .execute(gate)
        .await
        .expect("open the gate");
}

/// The pid of a backend that waits for a lock of kind `event` that the backend
/// `blocker` holds. Panics after 10 s.
async fn blocked_by(db: &PgPool, blocker: i32, event: &str) -> i32 {
    for _ in 0..1_000 {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND wait_event = $2 AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
        )
        .bind(blocker)
        .bind(event)
        .fetch_optional(db)
        .await
        .expect("read pg_stat_activity");
        if let Some(pid) = pid {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no backend waited on the {event} lock of backend {blocker}");
}

/// The open shards of the table, in byte order.
async fn shards_in_order(s: &Scratch, table_id: &str) -> Vec<String> {
    let mut ids = open_shard_ids(s, table_id).await;
    ids.sort();
    ids
}

/// A partition key whose records go to `shard` of table `name`.
async fn key_on(s: &Scratch, name: &str, shard: &str) -> String {
    for i in 0..1_000 {
        let pk = format!("k{i}");
        let assigned = s
            .engine
            .assign_shard(ACCOUNT, name, &pk)
            .await
            .expect("assign a shard");
        if assigned == shard {
            return pk;
        }
    }
    panic!("none of 1,000 keys goes to shard {shard}");
}

fn put_op<'a>(
    key_info: &'a TableKeyInfo,
    item: &'a Item,
    maps: &'a ExpressionMaps,
) -> TransactWriteOp<'a> {
    TransactWriteOp::Put {
        key_info,
        item,
        condition: None,
        maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(StreamCapture {
            view_type: StreamViewType::NewAndOldImages,
            user_identity: None,
            region: REGION.into(),
        }),
    }
}

/// Race a `TransactWriteItems` that puts the items `pks`, in that order,
/// against a disable of the table's stream. The gate holds the transaction at
/// its first stream record until the close waits for it.
async fn race_a_disable(s: &Scratch, name: &str, pks: [&str; 2]) -> Raced {
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("key info");
    install_record_gate(s).await;
    let (mut gate, gate_pid) = close_gate(s).await;

    let items: Vec<Item> = pks
        .iter()
        .map(|pk| serde_json::from_value(serde_json::json!({"pk": {"S": pk}})).expect("item"))
        .collect();
    let engine = s.engine.clone();
    let transaction = tokio::spawn(async move {
        let maps = ExpressionMaps::default();
        let ops: Vec<TransactWriteOp<'_>> = items
            .iter()
            .map(|item| put_op(&key_info, item, &maps))
            .collect();
        engine.transact_write_items(&ops, None).await
    });
    let writer = blocked_by(&s.db, gate_pid, "advisory").await;

    let input = serde_json::json!({
        "TableName": name,
        "StreamSpecification": {"StreamEnabled": false},
    });
    let engine = s.engine.clone();
    let disable = tokio::spawn(async move {
        engine
            .update_table(ACCOUNT, serde_json::from_value(input).expect("input"))
            .await
    });
    // The close waits for the shard row the transaction has locked.
    blocked_by(&s.db, writer, "transactionid").await;
    open_gate(&mut gate).await;

    let written = transaction.await.expect("the transaction task");
    let disabled = disable.await.expect("the disable task");
    activate(s, name).await;
    (written, disabled)
}

/// Both calls succeeded, the close numbered the transaction's two records, and
/// each is in the closed stream at or below its shard's ending number.
async fn assert_closed_with_both_records(s: &Scratch, raced: Raced, arn: &str, shards: [&str; 2]) {
    let (written, disabled) = raced;
    written.expect("the transaction commits");
    disabled.expect("the disable succeeds");
    // The close waited for the transaction to commit, so its numbering under
    // the lock numbered both records.
    assert_eq!(pending_count(s).await, 0);
    let closed = describe_stream(s, arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert!(
        closed
            .shards
            .iter()
            .all(|x| x.sequence_number_range.ending_sequence_number.is_some())
    );
    let past_the_end: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stream_records r JOIN stream_shards s ON s.shard_id = r.shard_id \
         WHERE s.ending_sequence_number IS NULL OR r.sequence_number > s.ending_sequence_number",
    )
    .fetch_one(&s.db)
    .await
    .expect("count records past an ending number");
    assert_eq!(past_the_end, 0);
    let mut recorded: Vec<String> = sqlx::query_scalar("SELECT shard_id FROM stream_records")
        .fetch_all(&s.db)
        .await
        .expect("the shards of the records");
    recorded.sort();
    let mut expected = shards.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(recorded, expected);
}

/// The items are in descending shard order. With the records in request
/// order, the transaction would hold the higher shard. The close would hold
/// the lower shard and wait for the higher one. The transaction would then
/// wait for the lower shard.
#[tokio::test]
async fn a_transaction_with_records_on_two_shards_in_descending_order_does_not_deadlock_a_disable()
{
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("lockdesc", true))
        .await
        .expect("create");
    activate(&s, "lockdesc").await;
    let table = describe_table(&s, "lockdesc").await;
    let arn = table.latest_stream_arn.clone().expect("stream arn");
    let shards = shards_in_order(&s, &table.table_id).await;
    let (low, high) = (shards[0].as_str(), shards[3].as_str());
    let (pk_low, pk_high) = (
        key_on(&s, "lockdesc", low).await,
        key_on(&s, "lockdesc", high).await,
    );

    let raced = race_a_disable(&s, "lockdesc", [&pk_high, &pk_low]).await;
    assert_closed_with_both_records(&s, raced, &arn, [low, high]).await;
    s.cleanup().await;
}

/// With index scans off, the close's statement reads the rows in the order
/// the table stores them. An update stores a new version of the lowest shard's
/// row after the other three, so the statement reads that row last. Without
/// its ORDER BY, the close would lock the other three rows and wait for the
/// lowest. The transaction would hold the lowest and wait for the second.
#[tokio::test]
async fn a_disable_locks_its_shard_rows_in_shard_id_order_whatever_order_they_are_stored_in() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let mut s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("lockscan", true))
        .await
        .expect("create");
    activate(&s, "lockscan").await;
    let table = describe_table(&s, "lockscan").await;
    let arn = table.latest_stream_arn.clone().expect("stream arn");
    let shards = shards_in_order(&s, &table.table_id).await;
    let (first, second) = (shards[0].as_str(), shards[1].as_str());
    let (pk_first, pk_second) = (
        key_on(&s, "lockscan", first).await,
        key_on(&s, "lockscan", second).await,
    );

    sqlx::query(
        "UPDATE stream_shards SET starting_sequence_number = starting_sequence_number \
         WHERE shard_id = $1",
    )
    .bind(first)
    .execute(&s.db)
    .await
    .expect("store a new version of the lowest shard's row");
    for setting in [
        "enable_indexscan",
        "enable_indexonlyscan",
        "enable_bitmapscan",
    ] {
        sqlx::query(&format!(
            "ALTER DATABASE \"{}\" SET {setting} = off",
            s.db_name
        ))
        .execute(&s.admin)
        .await
        .expect("turn index scans off");
    }
    // The engine's pooled sessions predate the settings. An engine opened now
    // takes them.
    s.engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{}/{}", base_conn().expect("checked above"), s.db_name),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine with index scans off");
    let (mut scan, _) = connect(&s).await;
    let read: Vec<String> = sqlx::query_scalar(
        "SELECT shard_id FROM stream_shards \
         WHERE shard_id = ANY($1) AND ending_sequence_number IS NULL",
    )
    .bind(&shards)
    .fetch_all(&mut scan)
    .await
    .expect("read the shard rows as the close's statement reads them");
    assert_eq!(read.last().map(String::as_str), Some(first), "{read:?}");

    // Request order is shard id order, the order the transaction stores its
    // records in.
    let raced = race_a_disable(&s, "lockscan", [&pk_first, &pk_second]).await;
    assert_closed_with_both_records(&s, raced, &arn, [first, second]).await;
    s.cleanup().await;
}
