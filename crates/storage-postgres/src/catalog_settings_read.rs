// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bounded read of a setting, which the SearchVectors reader of the
//! `search_inequality_filters` setting sends.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::{Executor, PgPool, Row};

/// The server-side bound on a bounded settings read: how long it waits for
/// a lock on the `settings` table, and how long it runs in all.
const SETTING_READ_BOUND: &str = "SELECT set_config('lock_timeout', '1s', true), \
     set_config('statement_timeout', '1500ms', true)";

/// The read itself.
const READ: &str = "SELECT value FROM settings WHERE key = $1";

/// Whether the connection is inside a transaction block, sent as a simple
/// query. Outside one, the statement is its own transaction and starts
/// when it does; inside one, it starts after the `BEGIN` that opened the
/// block.
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

/// Read `key` from the `settings` table under [`SETTING_READ_BOUND`].
///
/// `set_config(.., true)` lasts until the end of the transaction, so the
/// read runs in one. On a connection already inside a transaction block,
/// which only its owner may end, the read runs under a savepoint and rolls
/// back to it, which also restores the two settings; otherwise it opens and
/// commits its own transaction.
pub(super) async fn get_setting_bounded(pool: PgPool, key: String) -> OpResult<Option<String>> {
    let failed = |e: sqlx::Error| {
        tracing::error!("get_setting_bounded: {e}");
        OpError::Internal("Database error".to_owned())
    };
    // The bound is set on the server, so a read that waits on a lock
    // ends there and gives its connection back even when the caller
    // has stopped waiting for it.
    let mut conn = pool.acquire().await.map_err(failed)?;
    // A text with no parameters goes as one simple query, in which a
    // statement outside a block starts when its transaction does. The
    // extended protocol sets the statement's start again at Execute.
    let in_block: bool = (&mut *conn)
        .fetch_one(IN_TRANSACTION_BLOCK)
        .await
        .and_then(|row| row.try_get(0))
        .map_err(failed)?;
    if in_block {
        sqlx::query("SAVEPOINT extenddb_bounded_setting_read")
            .execute(&mut *conn)
            .await
            .map_err(failed)?;
        let bound = sqlx::query(SETTING_READ_BOUND).execute(&mut *conn).await;
        let row = match bound {
            Ok(_) => {
                sqlx::query_scalar::<_, String>(READ)
                    .bind(&key)
                    .fetch_optional(&mut *conn)
                    .await
            }
            Err(e) => Err(e),
        };
        let rolled_back = sqlx::query("ROLLBACK TO SAVEPOINT extenddb_bounded_setting_read")
            .execute(&mut *conn)
            .await;
        let released = sqlx::query("RELEASE SAVEPOINT extenddb_bounded_setting_read")
            .execute(&mut *conn)
            .await;
        let row = row.map_err(failed)?;
        rolled_back.map_err(failed)?;
        released.map_err(failed)?;
        return Ok(row);
    }
    let mut tx = sqlx::Connection::begin(&mut *conn).await.map_err(failed)?;
    sqlx::query(SETTING_READ_BOUND)
        .execute(&mut *tx)
        .await
        .map_err(failed)?;
    let row = sqlx::query_scalar::<_, String>(READ)
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(failed)?;
    tx.commit().await.map_err(failed)?;
    Ok(row)
}
