// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AT_TIMESTAMP` seek: the first stream record in a shard created at or
//! after a point in time.

use extenddb_storage::error::StorageError;

use crate::PostgresEngine;

impl PostgresEngine {
    pub(crate) async fn first_sequence_number_at_or_after_impl(
        &self,
        shard_id: &str,
        at_ms: i64,
    ) -> Result<Option<String>, StorageError> {
        let bound = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(at_ms) * 1_000_000)
            .map_err(|e| StorageError::Validation(format!("Invalid Timestamp: {e}")))?;
        // MIN over no rows is one NULL row, so the column is read as Option.
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT MIN(sequence_number) FROM stream_records \
             WHERE shard_id = $1 AND created_at >= $2",
        )
        .bind(shard_id)
        .bind(bound)
        .fetch_optional(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(row.and_then(|(s,)| s))
    }
}
