// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AT_TIMESTAMP` seek: the first stream record in a shard created at or
//! after a point in time.
//!
//! `created_at` is not a clustering column, so the seek uses the sequence
//! number's millisecond prefix as a clustering-key lower bound and then
//! filters `created_at` within that single partition. For every record
//! `created_at_ms <= hlc_ms`: the `HybridClock` clamps its physical
//! component upward and never below the wall-clock read that `created_at`
//! stores. So `created_at_ms >= at_ms` implies `hlc_ms >= at_ms`, and every
//! qualifying record sits at or after the bound; the seek never skips one.
//! The filter removes the other case: a record written under a clamped clock
//! (after a backwards step) whose prefix clears the bound while its
//! `created_at` is before `at_ms`. The filter walks at most that clamp
//! window, which is zero under a monotonic clock.

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::error::StorageError;

use crate::engine::CassandraEngine;

/// The smallest sequence number whose millisecond prefix is `at_ms`.
#[must_use]
pub fn sequence_lower_bound(at_ms: i64) -> String {
    format!("{:013}{:06}{:04}", at_ms.max(0), 0, 0)
}

impl CassandraEngine {
    pub(crate) async fn first_sequence_number_at_or_after_impl(
        &self,
        shard_id: &str,
        at_ms: i64,
    ) -> Result<Option<String>, StorageError> {
        let keyspace = self.account_keyspace_for_shard(shard_id).await?;
        let bound = sequence_lower_bound(at_ms);
        // A single-partition clustering range with one non-key filter; the
        // rows come back in clustering (sequence) order, so the first is the
        // minimum qualifying sequence.
        let query = format!(
            "SELECT sequence_number FROM {keyspace}.stream_records \
             WHERE shard_id = ? AND sequence_number >= ? AND created_at >= ? \
             LIMIT 1 ALLOW FILTERING"
        );
        let result = self
            .session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(shard_id, bound.as_str(), at_ms),
            )
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let rows = result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .into_rows()
            .unwrap_or_default();
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.get_r_by_name("sequence_number").ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::sequence_lower_bound;

    #[test]
    fn lower_bound_is_the_first_sequence_in_that_millisecond() {
        let b = sequence_lower_bound(1_792_764_248_050);
        assert_eq!(b, "17927642480500000000000");
        assert_eq!(b.len(), 23);
        // Any real sequence in that millisecond sorts at or after the bound,
        // and every sequence from the previous millisecond sorts before it.
        assert!("17927642480500000010042" > b.as_str());
        assert!("17927642480499999990042" < b.as_str());
    }

    #[test]
    fn negative_timestamps_clamp_to_the_epoch() {
        assert_eq!(sequence_lower_bound(-5), "00000000000000000000000");
    }
}
