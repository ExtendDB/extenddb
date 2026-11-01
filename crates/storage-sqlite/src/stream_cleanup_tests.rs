// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream record retention cleanup deletes in bounded batches, each under its
//! own hold of the engine write lock, so a write waits for one batch rather
//! than for the whole delete.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, CreateTableInput, Item, StreamViewType, TableKeyInfo};
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine};
use serde_json::json;

use crate::store::SqliteEngine;
use crate::stream::{DELETE_EXPIRED_STREAM_RECORDS_BATCH, STREAM_CLEANUP_BATCH_ROWS};

const ACCOUNT: &str = "000000000000";
/// The retention the stream cleanup worker passes.
const RETENTION_HOURS: i64 = 24;
/// The cutoff the batch tests pass to `delete_stream_records_before`.
const CUTOFF: &str = "2026-01-01T00:00:00.000Z";
/// A day before `CUTOFF`, and two hours after it, as SQLite time modifiers.
const BEFORE_CUTOFF: (&str, &str) = ("2026-01-01T00:00:00", "-1 days");
const AFTER_CUTOFF: (&str, &str) = ("2026-01-01T00:00:00", "+2 hours");
const RECORD_DATA: &str = r#"{"eventName":"INSERT","dynamodb":{"Keys":{"pk":{"S":"k"}}}}"#;

async fn engine_at(location: &str) -> SqliteEngine {
    let engine = SqliteEngine::new(location, 4, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::schema::apply(&engine.pool).await.expect("schema");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    // Tables become ACTIVE at create time; no transition poller runs here.
    sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
    )
    .execute(&engine.pool)
    .await
    .expect("settings");
    engine
}

/// A stream-enabled table `t` with its four shards.
async fn stream_table(engine: &SqliteEngine) -> TableKeyInfo {
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}
    }))
    .expect("input");
    let desc = engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    TableKeyInfo {
        table_name: "t".to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    }
}

/// Insert the records with sequence numbers `first..first + count` across the
/// table's four shards, in one statement. Record `i` is created at `base`
/// moved by `age`, less `i % 3600` seconds, so created-at order differs from
/// sequence order.
async fn insert_records(
    engine: &SqliteEngine,
    table_id: &str,
    first: i64,
    count: i64,
    (base, age): (&str, &str),
    record_data: &str,
) {
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT ?1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2) \
         INSERT INTO stream_records \
             (shard_id, sequence_number, table_id, event_name, record_data, created_at) \
         SELECT 'shardId-' || ?3 || '-' || printf('%016d', i % 4), printf('%021d', i), ?3, \
             'INSERT', ?4, \
             strftime('%Y-%m-%dT%H:%M:%fZ', ?5, ?6, '-' || (i % 3600) || ' seconds') \
         FROM n",
    )
    .bind(first)
    .bind(first + count - 1)
    .bind(table_id)
    .bind(record_data)
    .bind(base)
    .bind(age)
    .execute(&engine.pool)
    .await
    .expect("insert records");
}

async fn sequence_numbers(engine: &SqliteEngine) -> Vec<String> {
    sqlx::query_scalar("SELECT sequence_number FROM stream_records ORDER BY sequence_number")
        .fetch_all(&engine.pool)
        .await
        .expect("read records")
}

fn sequence_range(first: i64, count: i64) -> Vec<String> {
    (first..first + count).map(|i| format!("{i:021}")).collect()
}

fn item(pk: &str) -> Item {
    [("pk".to_owned(), AttributeValue::S(pk.to_owned()))]
        .into_iter()
        .collect()
}

/// Return once something holds the engine write lock. In these tests only the
/// cleanup takes it.
async fn wait_until_the_write_lock_is_held(engine: &SqliteEngine) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while engine.write_lock.try_lock().is_ok() {
        assert!(
            Instant::now() < deadline,
            "the cleanup never took the write lock"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Poll `task` once, so that it runs up to its first wait, and return it for
/// `tokio::spawn`. Fails if the task finishes instead of waiting.
async fn run_until_it_waits<F: Future>(task: F) -> Pin<Box<F>> {
    let mut task = Box::pin(task);
    let waiting = std::future::poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx).is_pending())).await;
    assert!(waiting, "the task finished without waiting");
    task
}

/// A PutItem sent while the cleanup deletes two hundred batches of expired
/// records completes while the cleanup is still running, in a small part of
/// the cleanup's time. With one statement for the whole delete it waits for
/// all of it.
#[tokio::test]
async fn a_write_during_the_cleanup_waits_for_one_batch_not_the_whole_delete() {
    const UNEXPIRED: i64 = 500;
    let engine = engine_at(":memory:").await;
    let key_info = stream_table(&engine).await;
    let expired = 200 * i64::from(STREAM_CLEANUP_BATCH_ROWS);
    let table_id = key_info.table_id.as_str();
    let now = ("now", "-48 hours");
    insert_records(&engine, table_id, 1, expired, now, RECORD_DATA).await;
    let recent = ("now", "-1 hours");
    insert_records(
        &engine,
        table_id,
        expired + 1,
        UNEXPIRED,
        recent,
        RECORD_DATA,
    )
    .await;

    let cleanup = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let deleted = engine
                .cleanup_expired_stream_records(RETENTION_HOURS)
                .await
                .expect("cleanup");
            (deleted, started.elapsed(), Instant::now())
        })
    };
    wait_until_the_write_lock_is_held(&engine).await;

    let write_started = Instant::now();
    engine
        .put_item(
            &key_info,
            item("written-during-cleanup"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put");
    let write_done = Instant::now();
    let write_took = write_done - write_started;

    let (deleted, cleanup_took, cleanup_done) = cleanup.await.expect("cleanup task");
    assert!(
        write_done < cleanup_done && write_took * 4 < cleanup_took,
        "the PutItem took {write_took:?} and the cleanup {cleanup_took:?}; \
         the PutItem must finish first, in under a quarter of the cleanup's time"
    );
    assert_eq!(deleted, u64::try_from(expired).expect("count"));
    assert_eq!(
        sequence_numbers(&engine).await,
        sequence_range(expired + 1, UNEXPIRED),
        "every expired record is deleted and every unexpired record remains"
    );
    let written = engine
        .get_item(&key_info, &item("written-during-cleanup"))
        .await
        .expect("get");
    assert!(written.is_some(), "the PutItem's item is stored");
}

/// The write lock is FIFO and the cleanup takes it once per batch, so a write
/// queued behind the cleanup runs after the cleanup's first batch. The test
/// holds the lock while the cleanup and then a PutItem queue for it, in that
/// order, and a temporary trigger records how many expired records were left
/// when the PutItem wrote its stream record.
#[tokio::test]
async fn a_write_queued_behind_the_cleanup_runs_after_one_batch() {
    const BATCHES: i64 = 20;
    let engine = engine_at(":memory:").await;
    let key_info = stream_table(&engine).await;
    let batch = i64::from(STREAM_CLEANUP_BATCH_ROWS);
    let expired = BATCHES * batch;
    let old = ("now", "-48 hours");
    insert_records(&engine, &key_info.table_id, 1, expired, old, RECORD_DATA).await;
    // The in-memory engine has one connection, so a temporary trigger sees
    // every statement. From here on only the PutItem inserts a stream record.
    sqlx::raw_sql(
        "CREATE TEMP TABLE expired_left_at_write (n INTEGER NOT NULL); \
         CREATE TEMP TRIGGER count_expired_left AFTER INSERT ON stream_records \
         BEGIN \
             INSERT INTO expired_left_at_write SELECT COUNT(*) FROM stream_records \
             WHERE created_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-24 hours'); \
         END",
    )
    .execute(&engine.pool)
    .await
    .expect("trigger");

    let lock = engine.write_lock.lock().await;
    // The cleanup reads nothing before it asks for the lock, so one poll makes
    // it the first waiter.
    let cleanup = run_until_it_waits({
        let engine = engine.clone();
        async move { engine.cleanup_expired_stream_records(RETENTION_HOURS).await }
    })
    .await;
    // The PutItem first reads the index propagation delay through the pool,
    // and one poll has it holding, or waiting for, the pool's one connection.
    let write = run_until_it_waits({
        let engine = engine.clone();
        let stream = StreamCapture {
            view_type: StreamViewType::NewImage,
            user_identity: None,
            region: Arc::from("us-east-1"),
        };
        async move {
            engine
                .put_item(
                    &key_info,
                    item("queued-behind-the-cleanup"),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    Some(&stream),
                )
                .await
        }
    })
    .await;
    let (cleanup, write) = (tokio::spawn(cleanup), tokio::spawn(write));
    // The pool hands out its connection in request order, so this returns
    // after the PutItem's read, which asks for the lock as soon as it ends.
    drop(engine.pool.acquire().await.expect("connection"));
    drop(lock);

    write.await.expect("write task").expect("put");
    let deleted = cleanup.await.expect("cleanup task").expect("cleanup");
    assert_eq!(deleted, u64::try_from(expired).expect("count"));
    let left: Vec<i64> = sqlx::query_scalar("SELECT n FROM expired_left_at_write")
        .fetch_all(&engine.pool)
        .await
        .expect("counts");
    let deleted_before_the_write: Vec<i64> = left.iter().map(|n| expired - n).collect();
    assert!(
        deleted_before_the_write == [batch],
        "the PutItem wrote its stream record after {deleted_before_the_write:?} of \
         {expired} expired records were deleted; one batch is {batch}"
    );
}

#[tokio::test]
async fn the_cleanup_stops_after_the_first_short_batch() {
    let engine = engine_at(":memory:").await;
    let table_id = stream_table(&engine).await.table_id;
    insert_records(&engine, &table_id, 1, 25, BEFORE_CUTOFF, RECORD_DATA).await;
    insert_records(&engine, &table_id, 26, 5, AFTER_CUTOFF, RECORD_DATA).await;

    let outcome = engine
        .delete_stream_records_before(CUTOFF, 10)
        .await
        .expect("cleanup");
    assert_eq!(outcome, (25, 3), "batches of 10, 10 and 5 records");
    assert_eq!(sequence_numbers(&engine).await, sequence_range(26, 5));
}

/// A statement that fails ends the cleanup with its error. The batches before
/// it stay deleted, and a warning reports how many records they removed.
#[tokio::test]
async fn a_failed_batch_returns_its_error_and_logs_the_records_already_deleted() {
    let engine = engine_at(":memory:").await;
    let table_id = stream_table(&engine).await.table_id;
    insert_records(&engine, &table_id, 1, 25, BEFORE_CUTOFF, RECORD_DATA).await;
    // Record 25 is the oldest and record 1 the newest, so in batches of 10
    // record 15 is in the second batch.
    sqlx::raw_sql(
        "CREATE TEMP TRIGGER fail_on_record_15 BEFORE DELETE ON stream_records \
         WHEN OLD.sequence_number = '000000000000000000015' \
         BEGIN SELECT RAISE(ABORT, 'injected failure'); END",
    )
    .execute(&engine.pool)
    .await
    .expect("trigger");

    let (log_guard, logs) = crate::schema::log_capture::capture();
    let outcome = engine.delete_stream_records_before(CUTOFF, 10).await;
    drop(log_guard);

    let err = outcome.expect_err("the second batch fails");
    assert!(err.to_string().contains("injected failure"), "{err}");
    assert_eq!(
        sequence_numbers(&engine).await,
        sequence_range(1, 15),
        "the first batch stays deleted and the failed batch deletes nothing"
    );
    let logs = logs.lock().expect("logs").clone();
    assert!(
        logs.iter().any(|l| l.starts_with("WARN")
            && l.contains("stopped after deleting 10 record(s) in 1 statement(s)")
            && l.contains("injected failure")),
        "{logs:#?}"
    );
}

#[tokio::test]
async fn a_whole_number_of_batches_ends_on_an_empty_statement() {
    let engine = engine_at(":memory:").await;
    let table_id = stream_table(&engine).await.table_id;
    insert_records(&engine, &table_id, 1, 30, BEFORE_CUTOFF, RECORD_DATA).await;

    let outcome = engine
        .delete_stream_records_before(CUTOFF, 10)
        .await
        .expect("cleanup");
    assert_eq!(outcome, (30, 4), "batches of 10, 10, 10 and 0 records");
    assert!(sequence_numbers(&engine).await.is_empty());
}

/// Nothing created before the cutoff: one statement, and every record stays,
/// including one created at the cutoff itself, since only records created
/// before it have expired.
#[tokio::test]
async fn a_cleanup_with_nothing_to_delete_runs_one_statement() {
    let engine = engine_at(":memory:").await;
    let table_id = stream_table(&engine).await.table_id;
    // `- 0 seconds` for record 3600 puts it at the cutoff exactly.
    let at_cutoff = ("2026-01-01T00:00:00", "+0 seconds");
    insert_records(&engine, &table_id, 3600, 1, at_cutoff, RECORD_DATA).await;
    insert_records(&engine, &table_id, 1, 4, AFTER_CUTOFF, RECORD_DATA).await;

    let outcome = engine
        .delete_stream_records_before(CUTOFF, 10)
        .await
        .expect("cleanup");
    assert_eq!(outcome, (0, 1));
    let mut expected = sequence_range(1, 4);
    expected.push(format!("{:021}", 3600));
    assert_eq!(sequence_numbers(&engine).await, expected);
}

/// A batch reads `idx_stream_records_created` in index order. Without it each
/// batch would sort every expired record to pick the oldest ones, and hold
/// the write lock for about as long as the old single statement.
#[tokio::test]
async fn a_batch_reads_the_created_at_index_in_order() {
    let engine = engine_at(":memory:").await;
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(&format!(
        "EXPLAIN QUERY PLAN {DELETE_EXPIRED_STREAM_RECORDS_BATCH}"
    ))
    .bind(CUTOFF)
    .bind(10_i64)
    .fetch_all(&engine.pool)
    .await
    .expect("plan");
    let steps: Vec<String> = plan.into_iter().map(|(.., detail)| detail).collect();
    assert!(
        steps
            .iter()
            .any(|s| s.contains("INDEX idx_stream_records_created (created_at<?)")),
        "{steps:?}"
    );
    assert!(
        !steps.iter().any(|s| s.contains("TEMP B-TREE")),
        "{steps:?}"
    );
}

/// Timing harness for `STREAM_CLEANUP_BATCH_ROWS`, not an assertion. For each
/// batch size it fills a file database with `SCB_ROWS` expired records of about
/// `SCB_PAYLOAD` bytes and deletes them twice: first alone, timing each
/// statement (the write lock is held for exactly that time), then with one
/// writer sending PutItem back to back, timing each PutItem. Batch size 0 runs
/// the single statement the cleanup used before it deleted in batches.
///
/// ```text
/// SCB_ROWS=280000 SCB_PAYLOAD=500 SCB_BATCHES=0,500,1000,2000,5000 \
///   cargo test -p extenddb-storage-sqlite --lib stream_cleanup_bench -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "timing harness, run explicitly"]
async fn stream_cleanup_bench() {
    let env = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.to_owned());
    let rows: i64 = env("SCB_ROWS", "280000").parse().expect("SCB_ROWS");
    let payload: usize = env("SCB_PAYLOAD", "500").parse().expect("SCB_PAYLOAD");
    let batches: Vec<u32> = env("SCB_BATCHES", "0,500,1000,2000,5000")
        .split(',')
        .map(|b| b.trim().parse().expect("SCB_BATCHES"))
        .collect();
    let record_data = json!({ "eventName": "INSERT", "pad": "x".repeat(payload) }).to_string();
    let dir = std::env::temp_dir().join(format!("stream-cleanup-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("bench dir");

    for (run, batch) in batches.into_iter().enumerate() {
        let open = |pass: &str| {
            let path = dir.join(format!("run{run}-{pass}.db"));
            let record_data = record_data.clone();
            async move {
                let engine = engine_at(path.to_str().expect("utf8 path")).await;
                let key_info = stream_table(&engine).await;
                let old = ("now", "-48 hours");
                insert_records(&engine, &key_info.table_id, 1, rows, old, &record_data).await;
                (engine, key_info)
            }
        };

        let (engine, _) = open("alone").await;
        let started = Instant::now();
        let holds = bench_cleanup(&engine, batch).await;
        let alone = started.elapsed();
        drop(engine);

        let (engine, key_info) = open("writer").await;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (engine, stop) = (engine.clone(), std::sync::Arc::clone(&stop));
            tokio::spawn(async move {
                let mut puts = Vec::new();
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let started = Instant::now();
                    let pk = format!("w{}", puts.len());
                    engine
                        .put_item(
                            &key_info,
                            item(&pk),
                            false,
                            None,
                            &ExpressionMaps::default(),
                            None,
                        )
                        .await
                        .expect("put");
                    puts.push(started.elapsed());
                }
                puts
            })
        };
        let started = Instant::now();
        bench_cleanup(&engine, batch).await;
        let with_writer = started.elapsed();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let puts = writer.await.expect("writer");
        drop(engine);

        println!(
            "batch {batch}: {} statements, hold ms min {} median {} p99 {} max {}, {:.1} s alone \
             ({:.0} rows/s); with a writer {:.1} s, {} PutItem, ms median {} p99 {} max {}",
            holds.len(),
            ms(&holds, 0.0),
            ms(&holds, 0.5),
            ms(&holds, 0.99),
            ms(&holds, 1.0),
            alone.as_secs_f64(),
            rows as f64 / alone.as_secs_f64(),
            with_writer.as_secs_f64(),
            puts.len(),
            ms(&puts, 0.5),
            ms(&puts, 0.99),
            ms(&puts, 1.0),
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Delete every expired record and return each statement's duration. Batch
/// size 0 is the single unbounded statement.
async fn bench_cleanup(engine: &SqliteEngine, batch: u32) -> Vec<Duration> {
    let cutoff = crate::sqlite_util::format_timestamp(
        time::OffsetDateTime::now_utc() - time::Duration::hours(RETENTION_HOURS),
    );
    let mut holds = Vec::new();
    if batch == 0 {
        let started = Instant::now();
        let _writer = engine.write_lock.lock().await;
        sqlx::query("DELETE FROM stream_records WHERE created_at < ?")
            .bind(&cutoff)
            .execute(&engine.pool)
            .await
            .expect("delete");
        holds.push(started.elapsed());
        return holds;
    }
    loop {
        let started = Instant::now();
        let deleted = engine
            .delete_stream_record_batch(&cutoff, batch)
            .await
            .expect("batch");
        holds.push(started.elapsed());
        if deleted < u64::from(batch) {
            return holds;
        }
    }
}

/// The `quantile` duration in `samples`, in whole milliseconds.
fn ms(samples: &[Duration], quantile: f64) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let Some(last) = sorted.len().checked_sub(1) else {
        return 0;
    };
    let index = (last as f64 * quantile).round() as usize;
    sorted[index.min(last)].as_millis()
}
