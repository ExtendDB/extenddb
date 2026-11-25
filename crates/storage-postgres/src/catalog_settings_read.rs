// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bounded read of a setting, which the SearchVectors reader of the
//! `search_inequality_filters` setting sends.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

/// The server-side bound on a bounded settings read: how long it waits for
/// a lock on the `settings` table, and how long it runs in all.
const SETTING_READ_BOUND: &str = "SELECT set_config('lock_timeout', '1s', true), \
     set_config('statement_timeout', '1500ms', true)";

/// Read `key` from the `settings` table under [`SETTING_READ_BOUND`].
pub(super) async fn get_setting_bounded(pool: PgPool, key: String) -> OpResult<Option<String>> {
    let failed = |e: sqlx::Error| {
        tracing::error!("get_setting_bounded: {e}");
        OpError::Internal("Database error".to_owned())
    };
    // The bound is set on the server, so a read that waits on a lock
    // ends there and gives its connection back even when the caller
    // has stopped waiting for it.
    let mut tx = pool.begin().await.map_err(failed)?;
    sqlx::query(SETTING_READ_BOUND)
        .execute(&mut *tx)
        .await
        .map_err(failed)?;
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = $1")
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(failed)?;
    tx.commit().await.map_err(failed)?;
    Ok(row.map(|(v,)| v))
}
