// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which of a table's shard rows are labelled, for the backends that check it
//! before serving a legacy shard (`util::shard_in_stream`).

use super::{ShardRow, ShardStream, parse_shard_id};

/// The byte-order range `[from, to)` that holds the labelled shard ids of
/// table `table_id` and none of its legacy ones, for the backends that write
/// 16-digit suffixes (SQLite and PostgreSQL). A labelled suffix starts with
/// the label's 13 milliseconds digits, not all zeros, so it sorts at or above
/// `0000000000001`; a legacy `{index:016}` suffix starts with 13 zeros and
/// sorts below. The upper end keeps the `shardId-{table_name}-{index:016}`
/// form out, which sorts anywhere relative to the table id.
#[must_use]
pub fn labelled_shard_id_range(table_id: &str) -> (String, String) {
    (
        format!("shardId-{table_id}-0000000000001"),
        format!("shardId-{table_id}."),
    )
}

/// A regular expression matching the labelled shard ids of table `table_id`
/// and none of its legacy ones, for MongoDB, whose legacy ids have 12-digit
/// suffixes that no byte-order bound separates from a labelled one. Table ids
/// are UUIDs, so the id needs no escaping.
#[must_use]
pub fn labelled_shard_id_pattern(table_id: &str) -> String {
    format!("^shardId-{table_id}-(?!0{{13}})[0-9]{{16}}$")
}

/// Whether any of `shards` is a labelled shard of table `table_id`.
#[must_use]
pub fn has_labelled_shard(table_id: &str, shards: &[ShardRow]) -> bool {
    shards.iter().any(|r| {
        r.table_id == table_id
            && matches!(
                parse_shard_id(table_id, &r.shard_id),
                Some((ShardStream::Labelled { .. }, _))
            )
    })
}
