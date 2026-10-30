// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The start of a GSI backfill, as the catalog records it.
//!
//! DescribeTable reports `Backfilling` for a `CREATING` GSI as whether its row
//! carries a `backfill_heartbeat`, and a Query or Scan naming it is refused by
//! the same test (see `extenddb_storage::gsi_backfill`). `UpdateTable` inserts
//! the row with no heartbeat. The recovery worker writes one when it takes the
//! index up, before it rebuilds the data table and starts the backfill, and the
//! backfill refreshes it after every page. Written before the backfill task
//! starts, it also tells a later scan of the recovery worker that the index is
//! taken, so the scan does not rebuild it a second time.

use std::time::Duration;

use cdrs_tokio::types::IntoRustByName as _;
use extenddb_storage::error::StorageError;

use crate::engine::CassandraEngine;

/// Record that the backfill of one index is starting, after the table's
/// `gsi_creating_phase_delay_ms` test hold, and return the hold so the
/// backfill task can wait it again once the start is recorded.
///
/// The recovery worker is one loop, so the first wait also delays the indexes
/// queued behind this one; it is zero outside tests.
///
/// Returns `None` when there is nothing to build: the row is no longer
/// `CREATING`, or it could not be read. The heartbeat is written only to a row
/// read as `CREATING` immediately before, because a CQL `UPDATE` of a deleted
/// row would recreate it in part.
pub(crate) async fn claim(
    engine: &CassandraEngine,
    table_id: &str,
    index_name: &str,
    table_name: &str,
) -> Option<Duration> {
    let catalog_ks = engine.catalog_keyspace();
    let session = engine.session_arc();
    let hold = creating_phase_delay(engine, table_name).await;
    tokio::time::sleep(hold).await;
    let row = crate::cassandra_util::query_optional::<StorageError>(
        &session,
        &format!(
            "SELECT index_status FROM {catalog_ks}.indexes WHERE table_id = ? AND index_name = ?"
        ),
        cdrs_tokio::query_values!(table_id, index_name),
        "gsi_backfill_claim_check",
    )
    .await;
    let status: Option<String> = match row {
        Ok(row) => row.and_then(|r| r.get_by_name("index_status").ok().flatten()),
        Err(e) => {
            tracing::warn!("gsi_recovery: re-read '{index_name}' before its backfill: {e}");
            return None;
        }
    };
    if status.as_deref() != Some("CREATING") {
        return None;
    }
    let claimed = crate::cassandra_util::execute::<StorageError>(
        &session,
        &format!(
            "UPDATE {catalog_ks}.indexes SET backfill_heartbeat = toTimestamp(now()) \
             WHERE table_id = ? AND index_name = ?"
        ),
        cdrs_tokio::query_values!(table_id, index_name),
        "gsi_backfill_claim",
    )
    .await;
    if let Err(e) = claimed {
        // The backfill still runs; its first page writes the heartbeat.
        tracing::warn!("gsi_recovery: record the backfill start of '{index_name}': {e}");
    }
    Some(hold)
}

/// The `gsi_creating_phase_delay_ms` test hold for one table; zero when unset.
async fn creating_phase_delay(engine: &CassandraEngine, table_name: &str) -> Duration {
    let catalog_ks = engine.catalog_keyspace();
    let key = extenddb_core::settings_keys::gsi_creating_phase_delay_key(table_name);
    let row = crate::cassandra_util::query_optional::<StorageError>(
        &engine.session_arc(),
        &format!("SELECT value FROM {catalog_ks}.settings WHERE key = ?"),
        cdrs_tokio::query_values!(key.as_str()),
        "gsi_creating_phase_delay",
    )
    .await
    .ok()
    .flatten();
    let value: Option<String> = row.and_then(|r| r.get_by_name("value").ok().flatten());
    extenddb_storage::gsi_backfill::phase_delay(value.as_deref())
}
