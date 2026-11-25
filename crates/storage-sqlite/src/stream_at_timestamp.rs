// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AT_TIMESTAMP` seek: the first stream record in a shard created at or
//! after a point in time.

use extenddb_storage::error::StorageError;

use crate::SqliteEngine;
use crate::sqlite_util::format_epoch_millis;

impl SqliteEngine {
    /// `created_at` is stored as fixed-width millisecond text in the column
    /// default's shape, so a bound in the same shape compares chronologically.
    pub(crate) async fn first_sequence_number_at_or_after_impl(
        &self,
        shard_id: &str,
        at_ms: i64,
    ) -> Result<Option<String>, StorageError> {
        let bound = format_epoch_millis(at_ms);
        // MIN over no rows is one NULL row, so the column is read as Option.
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT MIN(sequence_number) FROM stream_records \
             WHERE shard_id = ? AND created_at >= ?",
        )
        .bind(shard_id)
        .bind(&bound)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(row.and_then(|(s,)| s))
    }
}
