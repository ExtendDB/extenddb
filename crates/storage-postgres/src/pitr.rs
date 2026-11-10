// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Point-in-time recovery item history on PostgreSQL (`pitr_state`,
//! `item_history`, migration 10, both in the data database).
//!
//! [`record_item_history_in_tx`] runs inside a write's data transaction and
//! adds a row only when the table has a `pitr_state` row. A caller records
//! after its data write, so the row's `seq` is drawn under the item's row
//! lock and follows commit order for that key; the reconstruct and the prune
//! only compare `seq` within one key. The reconstruct is the anti-join the
//! SQLite store runs, paged on `seq`, over `idx_item_history_key`.
//!
//! A put or a delete that needs no transaction records in the same statement
//! as its data write ([`autocommit_put`], [`autocommit_delete`]): the
//! statement takes the data table's lock before it takes its snapshot, so a
//! write held by an enable reads the `pitr_state` row the enable committed.
//!
//! A row's commit time is read from the PostgreSQL server's clock
//! ([`DB_NOW_US`]) after the data write, so under the item's row lock: two
//! writes of one key take their times in the order they take their `seq`,
//! and every ExtendDB server of the database reads one clock.
//!
//! A page first pins its time in `pitr_state` and a prune lowers its cutoff
//! to a pin that holds; both lock the table's `pitr_state` row, so a prune
//! either sees the pin or has already moved `pruned_to_us` past the time and
//! the page refuses.

use std::time::SystemTime;

use extenddb_core::types::ScalarAttributeType;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::pitr::{
    DEFAULT_RECOVERY_PERIOD_DAYS, PitrCaptureStore, PitrState, RECONSTRUCT_PIN_LEASE,
    ReconstructPage, from_unix_micros, pinned_cutoff, reconstruct_held, stored_period, unix_micros,
};
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_info};
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::data::{data_table_name, json_to_item};

mod autocommit;
pub(crate) use autocommit::{autocommit_delete, autocommit_put};

/// The PostgreSQL server's clock in microseconds since the Unix epoch, read
/// when the expression is evaluated.
pub(crate) const DB_NOW_US: &str = "(extract(epoch FROM clock_timestamp()) * 1000000)::bigint";

/// Superseded: a later row of the same key committed at or before `$2`.
const SUPERSEDED: &str = "EXISTS (SELECT 1 FROM item_history n \
     WHERE n.table_id = h.table_id AND n.pk = h.pk AND n.sk = h.sk \
     AND n.seq > h.seq AND n.commit_time_us <= $2)";

/// An earlier row of the same key committed at or before `$2`.
const HAS_EARLIER: &str = "EXISTS (SELECT 1 FROM item_history e \
     WHERE e.table_id = h.table_id AND e.pk = h.pk AND e.sk = h.sk \
     AND e.seq < h.seq AND e.commit_time_us <= $2)";

/// One reconstruct page: `$1` table id, `$2` time, `$3` the cursor, `$4` the
/// limit.
pub(crate) fn reconstruct_sql() -> String {
    format!(
        "SELECT h.seq, h.item_data FROM item_history h \
         WHERE h.table_id = $1 AND h.commit_time_us <= $2 AND h.seq > $3 \
         AND h.item_data IS NOT NULL AND NOT {SUPERSEDED} \
         ORDER BY h.seq LIMIT $4"
    )
}

fn store_err(what: &str, e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("item history {what}: {e}"))
}

/// The sort key as `item_history.sk` holds it: equal for two values the data
/// table's typed column holds equal, and equal to what [`sk_sql`] computes
/// from that column, so a baseline row and a later write's row of one item
/// carry one key.
fn sk_text(sk: &SortKeyValue) -> String {
    match sk {
        SortKeyValue::S(s) => s.clone(),
        SortKeyValue::N(n) => n.normalized().to_plain_string(),
        SortKeyValue::B(b) => b.iter().map(|byte| format!("{byte:02x}")).collect(),
    }
}

/// [`sk_text`] computed from the data table's typed sort key column.
fn sk_sql(sk: Option<ScalarAttributeType>) -> &'static str {
    match sk {
        None => "''",
        Some(ScalarAttributeType::S) => "sk_s",
        Some(ScalarAttributeType::N) => "trim_scale(sk_n)::text",
        Some(ScalarAttributeType::B) => "encode(sk_b, 'hex')",
    }
}

/// Record one committed mutation of the table `key_info` names, in the
/// mutation's data transaction, after its data write. `key` is any image that
/// carries the primary key; `image` is the item after the mutation, `None`
/// for a delete. `commit_time` `None` reads [`DB_NOW_US`]. Returns the
/// recorded commit time when a row was added, which is when the table's
/// recovery is enabled.
pub(crate) async fn record_item_history_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    key: &Item,
    image: Option<&Item>,
    commit_time: Option<SystemTime>,
) -> Result<Option<i64>, StorageError> {
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
            sk_text(&parse_sk(sk_value, sk_type)?)
        }
        None => String::new(),
    };
    let item_json = image
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let commit_time = commit_time.map(unix_micros).transpose()?;
    sqlx::query_scalar(&format!(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         SELECT $1, $2, $3, coalesce($4, {DB_NOW_US}), $5 \
         WHERE EXISTS (SELECT 1 FROM pitr_state WHERE table_id = $1) RETURNING commit_time_us"
    ))
    .bind(&key_info.table_id)
    .bind(pk_text.as_ref())
    .bind(sk)
    .bind(commit_time)
    .bind(item_json)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| store_err("record", e))
}

/// The write paths' hook: record a mutation from its before and after images,
/// in the mutation's data transaction after its data write. A mutation with
/// neither image changed nothing (a delete of a missing item, a condition
/// check) and records nothing: a delete of a missing key holds no row lock, so
/// its row could take a higher `seq` than a concurrent first put of the key
/// that commits after it. The mutations of one transaction share
/// `commit_time`: the first one recorded reads [`DB_NOW_US`] and sets it, so
/// a reconstruct never splits a transaction.
pub(crate) async fn capture_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    old: Option<&Item>,
    new: Option<&Item>,
    commit_time: &mut Option<i64>,
) -> Result<(), StorageError> {
    let Some(key) = new.or(old) else {
        return Ok(());
    };
    let at = commit_time.map(from_unix_micros);
    if let Some(recorded) = record_item_history_in_tx(tx, key_info, key, new, at).await? {
        *commit_time = Some(recorded);
    }
    Ok(())
}

/// The columns [`state_of`] reads, in its order.
const STATE_COLUMNS: &str = "recovery_period_days, enabled_at_us, pruned_to_us";

fn state_of(
    table_id: &str,
    (days, at, pruned): (i32, i64, i64),
) -> Result<PitrState, StorageError> {
    Ok(PitrState {
        pruned_to: from_unix_micros(pruned),
        ..PitrState::new(
            stored_period(table_id, i64::from(days))?,
            from_unix_micros(at),
        )
    })
}

async fn state_in(
    conn: &mut sqlx::PgConnection,
    table_id: &str,
) -> Result<Option<PitrState>, StorageError> {
    let row: Option<(i32, i64, i64)> = sqlx::query_as(&format!(
        "SELECT {STATE_COLUMNS} FROM pitr_state WHERE table_id = $1"
    ))
    .bind(table_id)
    .fetch_optional(conn)
    .await
    .map_err(|e| store_err("state", e))?;
    row.map(|r| state_of(table_id, r)).transpose()
}

/// Exclude the table's writers, and every other enable and disable of it,
/// until the transaction ends. `SHARE ROW EXCLUSIVE` conflicts with the `ROW
/// EXCLUSIVE` every data write takes, so the lock waits for the writes in
/// flight to commit and holds new ones until this commits; a write held here
/// then reads the state as this transaction left it. It also conflicts with
/// itself, so a second enable reads the state the first committed. A data
/// table a DeleteTable dropped is [`StorageError::TableNotFound`].
async fn exclude_writers(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
) -> Result<(), StorageError> {
    sqlx::query(&format!(
        "LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE",
        data_table_name(&key_info.table_id)
    ))
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("42P01") {
            StorageError::TableNotFound(key_info.table_name.clone())
        } else {
            store_err("lock", e)
        }
    })?;
    Ok(())
}

fn micros_from_now(d: std::time::Duration) -> Result<(i64, i64), StorageError> {
    let now = SystemTime::now();
    Ok((unix_micros(now)?, unix_micros(now + d)?))
}

/// Pin `$2` on table `$1` until `$4`, at `$3` now: a pin that has expired is
/// replaced, one that holds keeps the earlier time and the later expiry.
const PIN_SQL: &str = "UPDATE pitr_state SET \
     pinned_at_us = CASE WHEN pin_expires_us IS NULL OR pin_expires_us < $3 THEN $2 \
     ELSE LEAST(pinned_at_us, $2) END, \
     pin_expires_us = CASE WHEN pin_expires_us IS NULL OR pin_expires_us < $3 THEN $4 \
     ELSE GREATEST(pin_expires_us, $4) END \
     WHERE table_id = $1";

impl PitrCaptureStore for PostgresEngine {
    fn pitr_state<'a>(
        &'a self,
        table_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<PitrState>, StorageError>> {
        Box::pin(async move {
            let mut conn = self
                .data_pool
                .acquire()
                .await
                .map_err(|e| store_err("state", e))?;
            state_in(&mut conn, table_id).await
        })
    }

    fn enable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        period: Option<u8>,
        at: SystemTime,
    ) -> BoxFuture<'a, Result<PitrState, StorageError>> {
        Box::pin(async move {
            let table_id = key_info.table_id.as_str();
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| store_err("enable", e))?;
            exclude_writers(&mut tx, key_info).await?;
            let state = match state_in(&mut tx, table_id).await? {
                Some(mut state) => {
                    if let Some(days) = period {
                        sqlx::query(
                            "UPDATE pitr_state SET recovery_period_days = $1 WHERE table_id = $2",
                        )
                        .bind(i32::from(days))
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
                         VALUES ($1, $2, $3)",
                    )
                    .bind(table_id)
                    .bind(i32::from(state.recovery_period_days))
                    .bind(unix_micros(at)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| store_err("enable", e))?;
                    let sk = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
                        .map(|(_, t)| t);
                    sqlx::query(&format!(
                        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
                         SELECT $1, pk, {}, $2, item_data FROM {}",
                        sk_sql(sk),
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
            let rows: Vec<(String, i32, i64, i64)> = sqlx::query_as(&format!(
                "SELECT table_id, {STATE_COLUMNS} FROM pitr_state ORDER BY table_id"
            ))
            .fetch_all(&self.data_pool)
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

    /// The catalog is another database, so DeleteTable leaves the data
    /// database's history and setting: tables with history or a setting that
    /// the catalog does not have are removed here, a batch of rows at a time,
    /// the setting once no row is left.
    fn remove_orphaned_history(&self, limit: u32) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let candidates: Vec<String> = sqlx::query_scalar(
                "SELECT table_id FROM pitr_state UNION \
                 SELECT DISTINCT table_id FROM item_history",
            )
            .fetch_all(&self.data_pool)
            .await
            .map_err(|e| store_err("orphans", e))?;
            if candidates.is_empty() {
                return Ok(0);
            }
            let live: Vec<String> =
                sqlx::query_scalar("SELECT table_id FROM tables WHERE table_id = ANY($1)")
                    .bind(&candidates)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| store_err("orphans", e))?;
            let gone: Vec<String> = candidates
                .into_iter()
                .filter(|id| !live.contains(id))
                .collect();
            if gone.is_empty() {
                return Ok(0);
            }
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| store_err("orphans", e))?;
            // The setting rows first, in one order, as a prune locks its
            // table's row before it deletes history.
            sqlx::query(
                "SELECT table_id FROM pitr_state WHERE table_id = ANY($1) \
                 ORDER BY table_id FOR UPDATE",
            )
            .bind(&gone)
            .execute(&mut *tx)
            .await
            .map_err(|e| store_err("orphans", e))?;
            let removed = sqlx::query(
                "DELETE FROM item_history WHERE seq IN (SELECT seq FROM item_history \
                 WHERE table_id = ANY($1) ORDER BY seq LIMIT $2)",
            )
            .bind(&gone)
            .bind(i64::from(limit))
            .execute(&mut *tx)
            .await
            .map_err(|e| store_err("orphans", e))?
            .rows_affected();
            sqlx::query(
                "DELETE FROM pitr_state s WHERE s.table_id = ANY($1) \
                 AND NOT EXISTS (SELECT 1 FROM item_history h WHERE h.table_id = s.table_id)",
            )
            .bind(&gone)
            .execute(&mut *tx)
            .await
            .map_err(|e| store_err("orphans", e))?;
            tx.commit().await.map_err(|e| store_err("orphans", e))?;
            Ok(removed)
        })
    }

    fn disable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| store_err("disable", e))?;
            exclude_writers(&mut tx, key_info).await?;
            // The setting row first, as a prune locks it before it deletes
            // history.
            for sql in [
                "DELETE FROM pitr_state WHERE table_id = $1",
                "DELETE FROM item_history WHERE table_id = $1",
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
            let (now_us, expires_us) = micros_from_now(RECONSTRUCT_PIN_LEASE)?;
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| store_err("pin", e))?;
            let pruned: Option<i64> = sqlx::query_scalar(
                "SELECT pruned_to_us FROM pitr_state WHERE table_id = $1 FOR UPDATE",
            )
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
            let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(&reconstruct_sql())
                .bind(table_id)
                .bind(at_us)
                .bind(after.unwrap_or(i64::MIN))
                .bind(i64::from(limit))
                .fetch_all(&self.data_pool)
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
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| store_err("prune", e))?;
            let pin: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
                "SELECT pinned_at_us, pin_expires_us FROM pitr_state WHERE table_id = $1 \
                 FOR UPDATE",
            )
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| store_err("prune", e))?;
            let cutoff_us =
                pinned_cutoff(unix_micros(cutoff)?, pin.unwrap_or((None, None)), now_us);
            let superseded = format!(
                "DELETE FROM item_history WHERE seq IN (SELECT h.seq FROM item_history h \
                 WHERE h.table_id = $1 AND h.commit_time_us <= $2 AND {SUPERSEDED} \
                 ORDER BY h.seq LIMIT $3)"
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
                     WHERE h.table_id = $1 AND h.commit_time_us <= $2 \
                     AND h.item_data IS NULL AND NOT {SUPERSEDED} AND NOT {HAS_EARLIER} \
                     ORDER BY h.seq LIMIT $3)"
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
                    "UPDATE pitr_state SET pruned_to_us = GREATEST(pruned_to_us, $1) \
                     WHERE table_id = $2",
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

#[cfg(feature = "test-hooks")]
impl PostgresEngine {
    /// [`record_item_history_in_tx`] in a data transaction of its own,
    /// committed or rolled back as `commit` says, for the shared checks of
    /// `extenddb_storage::pitr::conformance`.
    ///
    /// # Errors
    ///
    /// The record or the transaction fails.
    pub async fn record_item_history_for_tests(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        image: Option<&Item>,
        commit_time: SystemTime,
        commit: bool,
    ) -> Result<bool, StorageError> {
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| store_err("begin", e))?;
        let added = record_item_history_in_tx(&mut tx, key_info, key, image, Some(commit_time))
            .await?
            .is_some();
        if commit {
            tx.commit().await.map_err(|e| store_err("commit", e))?;
        } else {
            tx.rollback().await.map_err(|e| store_err("rollback", e))?;
        }
        Ok(added)
    }

    /// The reconstruct statement, for a test of its plan.
    #[must_use]
    pub fn item_history_reconstruct_sql_for_tests() -> String {
        reconstruct_sql()
    }
}
