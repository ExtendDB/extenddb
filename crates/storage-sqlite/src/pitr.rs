// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Point-in-time recovery item history on SQLite (`pitr_state`,
//! `item_history`, migration 10).
//!
//! [`record_item_history_in_tx`] runs inside a write's transaction and adds a
//! row only when the table has a `pitr_state` row, so the decision to capture
//! and the capture commit with the write. SQLite runs one writer at a time, so
//! `seq` (the row id) follows commit order.
//!
//! A reconstruct page selects the rows committed at or before the time that no
//! later row of the same key committed at or before it supersedes, by an
//! anti-join on `idx_item_history_key`, paged on `seq`. Each history row is
//! read once over all pages, where a window function over the history would
//! read the whole history again for every page.
//!
//! A page first pins its time in `pitr_state` and a prune lowers its cutoff
//! to a pin that holds; both run under the engine write lock, so a prune
//! either sees the pin or has already moved `pruned_to_us` past the time and
//! the page refuses.

use std::time::SystemTime;

use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::pitr::{
    DEFAULT_RECOVERY_PERIOD_DAYS, PitrCaptureStore, PitrState, RECONSTRUCT_PIN_LEASE,
    ReconstructPage, from_unix_micros, pinned_cutoff, reconstruct_held, stored_period, unix_micros,
};
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column, sk_info};
use futures::future::BoxFuture;

use crate::data::{BoundValue, data_table_name, json_to_item, sk_bound};
use crate::store::SqliteEngine;

/// Superseded: a later row of the same key committed at or before `?2`.
const SUPERSEDED: &str = "EXISTS (SELECT 1 FROM item_history n \
     WHERE n.table_id = h.table_id AND n.pk = h.pk AND n.sk IS h.sk \
     AND n.seq > h.seq AND n.commit_time_us <= ?2)";

/// An earlier row of the same key committed at or before `?2`.
const HAS_EARLIER: &str = "EXISTS (SELECT 1 FROM item_history e \
     WHERE e.table_id = h.table_id AND e.pk = h.pk AND e.sk IS h.sk \
     AND e.seq < h.seq AND e.commit_time_us <= ?2)";

/// One reconstruct page: `?1` table id, `?2` time, `?3` the cursor, `?4` the
/// limit.
fn reconstruct_sql() -> String {
    format!(
        "SELECT h.seq, h.item_data FROM item_history h \
         WHERE h.table_id = ?1 AND h.commit_time_us <= ?2 AND h.seq > ?3 \
         AND h.item_data IS NOT NULL AND NOT {SUPERSEDED} \
         ORDER BY h.seq LIMIT ?4"
    )
}

fn store_err(what: &str, e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("item history {what}: {e}"))
}

/// Pin `?2` on table `?1` until `?4`, at `?3` now: a pin that has expired is
/// replaced, one that holds keeps the earlier time and the later expiry.
const PIN_SQL: &str = "UPDATE pitr_state SET \
     pinned_at_us = CASE WHEN pin_expires_us IS NULL OR pin_expires_us < ?3 THEN ?2 \
     ELSE MIN(pinned_at_us, ?2) END, \
     pin_expires_us = CASE WHEN pin_expires_us IS NULL OR pin_expires_us < ?3 THEN ?4 \
     ELSE MAX(pin_expires_us, ?4) END \
     WHERE table_id = ?1";

/// Record one committed mutation of the table `key_info` names, in the
/// mutation's transaction. `key` is any image that carries the primary key;
/// `image` is the item after the mutation, `None` for a delete. Returns
/// whether a row was added, which is when the table's recovery is enabled.
pub(crate) async fn record_item_history_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    key: &Item,
    image: Option<&Item>,
    commit_time: SystemTime,
) -> Result<bool, StorageError> {
    let pk_name = &key_info.key_schema[0].attribute_name;
    let pk_value = key
        .get(pk_name)
        .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
    let pk_text = pk_to_text(pk_value)?;
    let sk = match sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
        Some((sk_name, sk_type)) => {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            Some(sk_bound(&parse_sk(sk_value, sk_type)?))
        }
        None => None,
    };
    let item_json = image
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let query = sqlx::query(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         SELECT ?1, ?2, ?3, ?4, ?5 WHERE EXISTS (SELECT 1 FROM pitr_state WHERE table_id = ?1)",
    )
    .bind(&key_info.table_id)
    .bind(pk_text.as_ref());
    let query = match sk {
        Some(BoundValue::Text(s)) => query.bind(s),
        Some(BoundValue::Blob(b)) => query.bind(b),
        None => query.bind(None::<String>),
    };
    let done = query
        .bind(unix_micros(commit_time)?)
        .bind(item_json)
        .execute(&mut **tx)
        .await
        .map_err(|e| store_err("record", e))?;
    Ok(done.rows_affected() == 1)
}

/// The write paths' hook: record a mutation from its before and after images,
/// in the mutation's transaction after its data write. A mutation with
/// neither image changed nothing (a delete of a missing item, a condition
/// check) and records nothing. Every mutation of one transaction passes the
/// same `commit_time`, so a reconstruct never splits a transaction.
pub(crate) async fn capture_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    old: Option<&Item>,
    new: Option<&Item>,
    commit_time: SystemTime,
) -> Result<(), StorageError> {
    let Some(key) = new.or(old) else {
        return Ok(());
    };
    record_item_history_in_tx(tx, key_info, key, new, commit_time).await?;
    Ok(())
}

/// The columns [`state_of`] reads, in its order.
const STATE_COLUMNS: &str = "recovery_period_days, enabled_at_us, pruned_to_us";

fn state_of(
    table_id: &str,
    (days, at, pruned): (i64, i64, i64),
) -> Result<PitrState, StorageError> {
    Ok(PitrState {
        pruned_to: from_unix_micros(pruned),
        ..PitrState::new(stored_period(table_id, days)?, from_unix_micros(at))
    })
}

async fn state_in(
    conn: &mut sqlx::SqliteConnection,
    table_id: &str,
) -> Result<Option<PitrState>, StorageError> {
    let row: Option<(i64, i64, i64)> = sqlx::query_as(&format!(
        "SELECT {STATE_COLUMNS} FROM pitr_state WHERE table_id = ?"
    ))
    .bind(table_id)
    .fetch_optional(conn)
    .await
    .map_err(|e| store_err("state", e))?;
    row.map(|r| state_of(table_id, r)).transpose()
}

/// Whether the catalog still has the table. A DeleteTable removes the row
/// and drops the data table in one transaction under the write lock.
async fn table_exists(
    conn: &mut sqlx::SqliteConnection,
    table_id: &str,
) -> Result<bool, StorageError> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tables WHERE table_id = ?)")
        .bind(table_id)
        .fetch_one(conn)
        .await
        .map_err(|e| store_err("table", e))
}

fn micros_from_now(d: std::time::Duration) -> Result<(i64, i64), StorageError> {
    let now = SystemTime::now();
    Ok((unix_micros(now)?, unix_micros(now + d)?))
}

impl PitrCaptureStore for SqliteEngine {
    fn pitr_state<'a>(
        &'a self,
        table_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<PitrState>, StorageError>> {
        Box::pin(async move {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| store_err("state", e))?;
            state_in(&mut conn, table_id).await
        })
    }

    /// Under the write lock, which every write takes before its transaction,
    /// so the baseline and the state commit with no write between them.
    fn enable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        period: Option<u8>,
        at: SystemTime,
    ) -> BoxFuture<'a, Result<PitrState, StorageError>> {
        Box::pin(async move {
            let table_id = key_info.table_id.as_str();
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| store_err("enable", e))?;
            if !table_exists(&mut tx, table_id).await? {
                return Err(StorageError::TableNotFound(key_info.table_name.clone()));
            }
            let state = match state_in(&mut tx, table_id).await? {
                Some(mut state) => {
                    if let Some(days) = period {
                        sqlx::query(
                            "UPDATE pitr_state SET recovery_period_days = ? WHERE table_id = ?",
                        )
                        .bind(i64::from(days))
                        .bind(table_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| store_err("enable", e))?;
                        state.recovery_period_days = days;
                    }
                    state
                }
                None => {
                    let state = PitrState::new(period.unwrap_or(DEFAULT_RECOVERY_PERIOD_DAYS), at);
                    sqlx::query(
                        "INSERT INTO pitr_state (table_id, recovery_period_days, enabled_at_us) \
                         VALUES (?, ?, ?)",
                    )
                    .bind(table_id)
                    .bind(i64::from(state.recovery_period_days))
                    .bind(unix_micros(at)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("enable", e))?;
                    // The data table's key columns are the history's: the
                    // same pk text and the same typed sort key value.
                    let sk = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
                        .map_or("NULL", |(_, t)| sk_column(t));
                    sqlx::query(&format!(
                        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
                         SELECT ?, pk, {sk}, ?, item_data FROM {}",
                        data_table_name(table_id)
                    ))
                    .bind(table_id)
                    .bind(unix_micros(at)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("baseline", e))?;
                    state
                }
            };
            tx.commit().await.map_err(|e| store_err("enable", e))?;
            Ok(state)
        })
    }

    fn pitr_tables(&self) -> BoxFuture<'_, Result<Vec<(String, PitrState)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(&format!(
                "SELECT table_id, {STATE_COLUMNS} FROM pitr_state ORDER BY table_id"
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| store_err("tables", e))?;
            rows.into_iter()
                .map(|(id, days, at, pruned)| {
                    let state = state_of(&id, (days, at, pruned))?;
                    Ok((id, state))
                })
                .collect()
        })
    }

    /// The catalog's foreign keys remove a table's history and setting with
    /// the table, so there is never an orphan here.
    fn remove_orphaned_history(&self, _limit: u32) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }

    fn disable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| store_err("disable", e))?;
            if !table_exists(&mut tx, &key_info.table_id).await? {
                return Err(StorageError::TableNotFound(key_info.table_name.clone()));
            }
            for sql in [
                "DELETE FROM item_history WHERE table_id = ?",
                "DELETE FROM pitr_state WHERE table_id = ?",
            ] {
                sqlx::query(sql)
                    .bind(&key_info.table_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("disable", e))?;
            }
            tx.commit().await.map_err(|e| store_err("disable", e))
        })
    }

    fn reconstruct_page<'a>(
        &'a self,
        table_id: &'a str,
        at: SystemTime,
        after: Option<i64>,
        limit: u32,
    ) -> BoxFuture<'a, Result<ReconstructPage, StorageError>> {
        Box::pin(async move {
            let at_us = unix_micros(at)?;
            {
                let (now_us, expires_us) = micros_from_now(RECONSTRUCT_PIN_LEASE)?;
                let _writer = self.write_lock.lock().await;
                let mut tx = self
                    .pool
                    .begin_with("BEGIN IMMEDIATE")
                    .await
                    .map_err(|e| store_err("pin", e))?;
                let pruned: Option<i64> =
                    sqlx::query_scalar("SELECT pruned_to_us FROM pitr_state WHERE table_id = ?")
                        .bind(table_id)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| store_err("pin", e))?;
                reconstruct_held(table_id, at_us, pruned)?;
                sqlx::query(PIN_SQL)
                    .bind(table_id)
                    .bind(at_us)
                    .bind(now_us)
                    .bind(expires_us)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("pin", e))?;
                tx.commit().await.map_err(|e| store_err("pin", e))?;
            }
            let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(&reconstruct_sql())
                .bind(table_id)
                .bind(at_us)
                .bind(after.unwrap_or(i64::MIN))
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
                .map_err(|e| store_err("reconstruct", e))?;
            let next = if rows.len() == limit as usize {
                rows.last().map(|(seq, _)| *seq)
            } else {
                None
            };
            let items = rows
                .into_iter()
                .map(|(_, v)| json_to_item(v))
                .collect::<Result<_, _>>()?;
            Ok(ReconstructPage { items, next })
        })
    }

    fn prune_before<'a>(
        &'a self,
        table_id: &'a str,
        cutoff: SystemTime,
        limit: u32,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            let (now_us, _) = micros_from_now(std::time::Duration::ZERO)?;
            // The write lock orders this with every write, enable, disable
            // and reconstruct pin of the engine.
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| store_err("prune", e))?;
            let pin: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
                "SELECT pinned_at_us, pin_expires_us FROM pitr_state WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| store_err("prune", e))?;
            let cutoff_us =
                pinned_cutoff(unix_micros(cutoff)?, pin.unwrap_or((None, None)), now_us);
            let superseded = format!(
                "DELETE FROM item_history WHERE seq IN (SELECT h.seq FROM item_history h \
                 WHERE h.table_id = ?1 AND h.commit_time_us <= ?2 AND {SUPERSEDED} \
                 ORDER BY h.seq LIMIT ?3)"
            );
            let mut removed = sqlx::query(&superseded)
                .bind(table_id)
                .bind(cutoff_us)
                .bind(i64::from(limit))
                .execute(&mut *tx)
                .await
                .map_err(|e| store_err("prune", e))?
                .rows_affected();
            // A delete is dropped only once it is the only row of its key at
            // or before the cutoff: with an older image still present, the
            // image would become the latest row and bring the item back.
            if removed < u64::from(limit) {
                let deletes = format!(
                    "DELETE FROM item_history WHERE seq IN (SELECT h.seq FROM item_history h \
                     WHERE h.table_id = ?1 AND h.commit_time_us <= ?2 \
                     AND h.item_data IS NULL AND NOT {SUPERSEDED} AND NOT {HAS_EARLIER} \
                     ORDER BY h.seq LIMIT ?3)"
                );
                removed += sqlx::query(&deletes)
                    .bind(table_id)
                    .bind(cutoff_us)
                    .bind(i64::try_from(u64::from(limit) - removed).unwrap_or(i64::MAX))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("prune", e))?
                    .rows_affected();
            }
            if removed > 0 {
                sqlx::query(
                    "UPDATE pitr_state SET pruned_to_us = MAX(pruned_to_us, ?) WHERE table_id = ?",
                )
                .bind(cutoff_us)
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| store_err("prune", e))?;
            }
            tx.commit().await.map_err(|e| store_err("prune", e))?;
            Ok(removed)
        })
    }
}

#[cfg(test)]
#[path = "pitr_tests.rs"]
mod tests;
