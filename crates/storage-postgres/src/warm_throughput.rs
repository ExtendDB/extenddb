// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stored `WarmThroughput` for `PostgresEngine`: the control-plane delay the
//! `UPDATING` window takes, and the `UpdateTable` merges.
//!
//! The record is written as sent and read back by DescribeTable; nothing
//! pre-warms or scales on it, and it has no effect on throughput, throttling
//! or storage. See `extenddb_core::types::WarmThroughputRecord`.

use extenddb_core::types::UpdateTableInput;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::warm_throughput_record_for_update;

use crate::PostgresEngine;

impl PostgresEngine {
    /// `control_plane_delay_seconds` from the settings table (0.25 when
    /// unset): the window a fresh record reports `UPDATING` for, the same
    /// one the CREATING to ACTIVE flip uses.
    ///
    /// The two windows are timed by different clocks: the CREATING to ACTIVE
    /// flip is scheduled from the database clock (`NOW()` in the create
    /// INSERT), the record's `UPDATING` deadline is the application clock
    /// (`epoch_seconds_now()`). On a database host whose clock differs from
    /// the server's, the member can read `UPDATING` after the table is
    /// `ACTIVE`, or `ACTIVE` before, by the difference.
    pub(crate) async fn control_plane_delay_secs(
        conn: &mut sqlx::PgConnection,
    ) -> Result<f64, StorageError> {
        sqlx::query_scalar(
            "SELECT COALESCE((SELECT value::FLOAT8 FROM settings \
             WHERE key = 'control_plane_delay_seconds'), 0.25)",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))
    }

    /// Store the request's table-level `WarmThroughput` and each
    /// `GlobalSecondaryIndexUpdates.Update.WarmThroughput`, merged over what
    /// is stored, inside the caller's catalog transaction. The engine has
    /// already validated the values against the current ones.
    pub(crate) async fn store_warm_throughput_updates(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        account_id: &str,
        table_id: &str,
        input: &UpdateTableInput,
    ) -> Result<(), StorageError> {
        let gsi_updates: Vec<_> = input
            .global_secondary_index_updates
            .iter()
            .flatten()
            .filter_map(|u| u.update.as_ref())
            .filter_map(|u| u.warm_throughput.as_ref().map(|wt| (&u.index_name, wt)))
            .collect();
        if input.warm_throughput.is_none() && gsi_updates.is_empty() {
            return Ok(());
        }
        let delay_secs = Self::control_plane_delay_secs(tx).await?;
        let to_json = |record| {
            serde_json::to_value(record).map_err(|e| StorageError::Internal(e.to_string()))
        };

        if let Some(spec) = &input.warm_throughput {
            let existing: Option<serde_json::Value> = sqlx::query_scalar(
                "SELECT warm_throughput FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let existing = existing.map(|v| v.to_string());
            let record = warm_throughput_record_for_update(existing.as_deref(), spec, delay_secs)?;
            sqlx::query(
                "UPDATE tables SET warm_throughput = $1 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(to_json(record)?)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        for (index_name, spec) in gsi_updates {
            let existing: Option<Option<serde_json::Value>> = sqlx::query_scalar(
                "SELECT warm_throughput FROM indexes \
                 WHERE table_id = $1 AND index_name = $2 AND index_type = 'GSI'",
            )
            .bind(table_id)
            .bind(index_name)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some(existing) = existing else {
                return Err(StorageError::IndexNotFound(index_name.clone()));
            };
            let existing = existing.map(|v| v.to_string());
            let record = warm_throughput_record_for_update(existing.as_deref(), spec, delay_secs)?;
            sqlx::query(
                "UPDATE indexes SET warm_throughput = $1 WHERE table_id = $2 AND index_name = $3",
            )
            .bind(to_json(record)?)
            .bind(table_id)
            .bind(index_name)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }
}
