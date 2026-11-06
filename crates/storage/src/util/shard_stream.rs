// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which stream of its table a shard belongs to.
//!
//! `GetRecords` carries a stream ARN in its iterator and reads one shard. Every
//! backend keys its catalog read on the caller's account and the table name
//! the ARN carries, keys its shard read on that table's id, and then asks the
//! one function here whether the shard row belongs to the stream the ARN's
//! label names. The rule lives in one place so a change to how streams are
//! identified touches one function and its test.

/// Whether shard `shard_id`, a row of table `table_id` whose current stream
/// label is `table_label` (`None` when the table never had a stream), belongs
/// to the stream labelled `arn_label`.
///
/// Today a table has one stream and every shard row of the table belongs to
/// it, so the shard is the stream's exactly when the ARN's label is the
/// table's current label. `table_id` and `shard_id` are part of the contract
/// for a scheme that names the stream inside the shard id; this rule does not
/// read them.
#[must_use]
pub fn shard_in_stream(
    table_id: &str,
    shard_id: &str,
    table_label: Option<&str>,
    arn_label: &str,
) -> bool {
    let _ = (table_id, shard_id);
    table_label == Some(arn_label)
}

#[cfg(test)]
mod tests {
    use super::shard_in_stream;

    const TABLE: &str = "0f3c1c1e-7b1d-4e3b-9a6e-4a2d3e5f6a7b";
    const LABEL: &str = "2026-01-01T00:00:00.000";
    const LATER: &str = "2026-02-02T00:00:00.000";

    fn shard(i: u32) -> String {
        format!("shardId-{TABLE}-{i:016}")
    }

    #[test]
    fn a_shard_belongs_to_its_tables_current_stream() {
        assert!(shard_in_stream(TABLE, &shard(0), Some(LABEL), LABEL));
        assert!(shard_in_stream(TABLE, &shard(3), Some(LABEL), LABEL));
    }

    #[test]
    fn an_earlier_label_on_the_same_shards_is_not_the_current_stream() {
        // The table took a new label while keeping its shard rows: the old
        // ARN no longer names a stream of the table.
        assert!(!shard_in_stream(TABLE, &shard(0), Some(LATER), LABEL));
        assert!(shard_in_stream(TABLE, &shard(0), Some(LATER), LATER));
    }

    #[test]
    fn a_table_without_a_stream_owns_no_stream_shard() {
        assert!(!shard_in_stream(TABLE, &shard(0), None, LABEL));
    }
}
