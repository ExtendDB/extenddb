// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Faults a bulk write batch meets on a row statement, against the
//! `PostgreSQL` backend, and the class the bulk runner reads from each with
//! `StorageError::is_transient_write_fault`: a backend terminated mid-batch
//! (the server sends SQLSTATE 57P01 before it closes) and a connection
//! closed mid-batch with no message from the server (an I/O error, no
//! SQLSTATE). Both are faults a resume clears, and neither batch leaves a
//! row written.

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::DataEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use super::{REGION, base_conn, contract, scratch};

/// Make the base table's second written row of each transaction run `act`
/// (a PL/pgSQL statement) before the write.
async fn act_at_the_second_row(pool: &PgPool, table_id: &str, act: &str) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION eddb_second_row() RETURNS trigger LANGUAGE plpgsql AS $$ \
         DECLARE n int := coalesce(nullif(current_setting('eddb.rows', true), ''), '0')::int + 1; \
         BEGIN \
             PERFORM set_config('eddb.rows', n::text, true); \
             IF n = 2 THEN {act} END IF; \
             RETURN NEW; \
         END $$; \
         CREATE TRIGGER eddb_second_row BEFORE INSERT OR UPDATE ON \"_ddb_{table_id}\" \
             FOR EACH ROW EXECUTE FUNCTION eddb_second_row();"
    ))
    .execute(pool)
    .await
    .expect("install the trigger");
}

async fn drop_the_trigger(pool: &PgPool, table_id: &str) {
    sqlx::raw_sql(&format!(
        "DROP TRIGGER eddb_second_row ON \"_ddb_{table_id}\"; DROP FUNCTION eddb_second_row();"
    ))
    .execute(pool)
    .await
    .expect("drop the trigger");
}

async fn rows_in(pool: &PgPool, table_id: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"_ddb_{table_id}\""))
        .fetch_one(pool)
        .await
        .expect("count the table's rows")
}

/// The batch's backend terminates itself at the batch's second row, the
/// way `pg_terminate_backend` from an administrator ends it: the server
/// sends FATAL 57P01 and closes. The error keeps the code, the runner's
/// classifier reads it as transient, and the batch wrote nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backend_terminated_mid_batch_is_a_transient_fault() {
    let Some(s) = scratch("a_backend_terminated_mid_batch_is_a_transient_fault").await else {
        return;
    };
    let (ki, rows) = contract::plain_batch_on_a_new_table(&s.fixture, "batch_terminated").await;
    act_at_the_second_row(
        &s.catalog,
        &ki.table_id,
        "PERFORM pg_terminate_backend(pg_backend_pid());",
    )
    .await;
    let error = s
        .fixture
        .engine
        .put_items_batch(&ki, rows, None)
        .await
        .expect_err("the batch's backend is terminated at its second row");
    assert!(
        matches!(&error, StorageError::Internal(m) if m.starts_with("SQLSTATE 57P01:")),
        "the error keeps the server's code, got {error:?}"
    );
    assert!(
        error.is_transient_write_fault(),
        "a terminated backend is a fault a resume clears, got {error:?}"
    );
    assert_eq!(
        rows_in(&s.catalog, &ki.table_id).await,
        0,
        "nothing committed"
    );

    // A resume writes the same rows once the fault is gone. The pool can
    // still hold the terminated connection, which the first attempt finds.
    drop_the_trigger(&s.catalog, &ki.table_id).await;
    let mut applied = false;
    for attempt in 0..3 {
        let (_, again) =
            contract::plain_batch_on_a_new_table(&s.fixture, &format!("batch_again_{attempt}"))
                .await;
        if s.fixture
            .engine
            .put_items_batch(&ki, again, None)
            .await
            .is_ok()
        {
            applied = true;
            break;
        }
    }
    assert!(applied, "the rows apply once the fault is gone");
    assert_eq!(rows_in(&s.catalog, &ki.table_id).await, 2);
    s.cleanup().await;
}

/// A TCP relay to the server whose open connections can all be closed at
/// once, the way a dropped network path ends them: no message from the
/// server reaches the client.
struct Relay {
    port: u16,
    close: Arc<Notify>,
    accept: tokio::task::JoinHandle<()>,
}

impl Relay {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the relay");
        let port = listener.local_addr().expect("the relay address").port();
        let close = Arc::new(Notify::new());
        let closing = Arc::clone(&close);
        let accept = tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let closing = Arc::clone(&closing);
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        () = closing.notified() => {}
                    }
                });
            }
        });
        Self {
            port,
            close,
            accept,
        }
    }

    fn close_all(&self) {
        self.close.notify_waiters();
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// The batch's data connection is closed at the batch's second row with no
/// message from the server, so the client sees an I/O error that carries
/// no SQLSTATE. The runner's classifier reads it as transient, and the
/// batch wrote nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_closed_mid_batch_is_a_transient_fault() {
    let Some(s) = scratch("a_connection_closed_mid_batch_is_a_transient_fault").await else {
        return;
    };
    let base = base_conn().expect("a base connection string");
    let (credentials, server) = base
        .rsplit_once('@')
        .expect("the base connection string names a user");
    let relay = Relay::start(server.to_owned()).await;
    // The engine opened here reaches its data database through the relay,
    // and its catalog directly.
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(format!(
        "{credentials}@127.0.0.1:{}/{}",
        relay.port, s.db_name
    ))
    .execute(&s.catalog)
    .await
    .expect("route the data database through the relay");
    let relayed = Arc::new(
        PostgresEngine::new(
            &PostgresConfig {
                connection_string: format!("{base}/{}", s.db_name),
                pool_size: 10,
                max_item_size_bytes: 400_000,
            },
            REGION,
        )
        .await
        .expect("open an engine whose data pool goes through the relay"),
    );

    let (ki, rows) = contract::plain_batch_on_a_new_table(&s.fixture, "batch_closed").await;
    act_at_the_second_row(&s.catalog, &ki.table_id, "PERFORM pg_sleep(3);").await;
    let batch = {
        let relayed = Arc::clone(&relayed);
        let ki = ki.clone();
        tokio::spawn(async move { relayed.put_items_batch(&ki, rows, None).await })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    relay.close_all();
    let error = batch
        .await
        .expect("the batch task")
        .expect_err("the batch's connection is closed at its second row");
    assert!(
        error.is_transient_write_fault(),
        "a connection closed under a row statement is a fault a resume clears, got {error:?}"
    );
    // The server sees the connection gone once the trigger's sleep ends,
    // and rolls the batch back.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        rows_in(&s.catalog, &ki.table_id).await,
        0,
        "nothing committed"
    );
    drop(relayed);
    drop(relay);
    s.cleanup().await;
}
