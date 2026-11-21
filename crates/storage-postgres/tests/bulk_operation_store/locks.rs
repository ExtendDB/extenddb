// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The lock-wait cases of the bulk operation store contract and the failing
//! index creation, run against the `PostgreSQL` backend: each holds a lock,
//! or installs a fault, from a connection of its own.

use sqlx::PgPool;

use super::{contract, scratch};

/// A `hold` for the lock-wait cases: a transaction on a connection of its
/// own runs `lock` with the table id bound, keeps what it took for the given
/// milliseconds, and rolls back.
fn hold_with(
    pool: PgPool,
    lock: &'static str,
) -> impl FnOnce(String, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    move |table_id, ms| {
        Box::pin(async move {
            let mut tx = pool.begin().await.expect("begin the holding transaction");
            let query = sqlx::query(lock);
            let query = if lock.contains("$1") {
                query.bind(table_id)
            } else {
                query
            };
            query.execute(&mut *tx).await.expect("take the lock");
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            tx.rollback().await.expect("release the lock");
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_waiting_on_a_held_row_does_not_delay_another_table() {
    let Some(s) = scratch("a_start_waiting_on_a_held_row_does_not_delay_another_table").await
    else {
        return;
    };
    contract::a_start_waiting_on_a_held_row_does_not_delay_another_table(
        &s.fixture,
        hold_with(
            s.catalog.clone(),
            "SELECT 1 FROM tables WHERE table_id = $1 FOR UPDATE",
        ),
    )
    .await;
    s.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_whose_index_read_waits_is_created() {
    let Some(s) = scratch("a_start_whose_index_read_waits_is_created").await else {
        return;
    };
    contract::a_start_whose_index_read_waits_is_created(
        &s.fixture,
        hold_with(
            s.catalog.clone(),
            "LOCK TABLE indexes IN ACCESS EXCLUSIVE MODE",
        ),
    )
    .await;
    s.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_called_during_a_failing_index_creation_is_created() {
    let Some(s) = scratch("a_start_called_during_a_failing_index_creation_is_created").await else {
        return;
    };
    // The scratch database is also the data database, so the trigger makes
    // the index's data table creation wait a second and fail.
    let pool = s.catalog.clone();
    contract::a_start_called_during_a_failing_index_creation_is_created(
        &s.fixture,
        || async move {
            sqlx::raw_sql(
                "CREATE FUNCTION refuse_table_creation() RETURNS event_trigger \
             LANGUAGE plpgsql AS $$ BEGIN \
                 PERFORM pg_sleep(1); \
                 RAISE EXCEPTION 'table creation refused by the test'; \
             END $$; \
             CREATE EVENT TRIGGER refuse_table_creation ON ddl_command_start \
                 WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION refuse_table_creation();",
            )
            .execute(&pool)
            .await
            .expect("install the event trigger");
        },
    )
    .await;
    s.cleanup().await;
}
