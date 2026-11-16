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

use std::future::Future;

use crate::stream_lineage::{ShardStream, parse_shard_id};

/// Whether shard `shard_id`, a row of table `table_id` whose current stream
/// label is `table_label` (`None` when the table never had a stream), belongs
/// to the stream labelled `arn_label`.
///
/// A table has a sequence of streams, one per enable, and each stream's
/// identity is in its shard ids ([`crate::stream_lineage::shard_id`]). A
/// labelled id belongs to the label it carries, so a closed stream's shards
/// stay readable under the closed stream's ARN after the table has a newer
/// stream, and an iterator pairing the newer ARN with one of them is refused.
/// An id from before streams were named in shard ids (the
/// `shardId-{table_id}-{index}` and `shardId-{table_name}-{index}` shapes)
/// belongs to the table's current stream while the table has no labelled
/// shard: it is the ARN's when the ARN's label is the table's current label
/// and `table_has_labelled_shard` answers `false`. From a table's first enable
/// under the labelled scheme every later stream is labelled, so legacy rows
/// beside a labelled row are an earlier, closed stream, which the newer ARN
/// does not name. The row is already this table's (the callers key the shard
/// read on the table id), so any id that does not parse is a legacy one.
///
/// `table_has_labelled_shard` reads the table's shard rows; it runs only for a
/// legacy id whose table label matches the ARN's, so a labelled id is decided
/// from the id alone.
///
/// # Errors
///
/// Whatever `table_has_labelled_shard` returns.
pub async fn shard_in_stream<F, E>(
    table_id: &str,
    shard_id: &str,
    table_label: Option<&str>,
    arn_label: &str,
    table_has_labelled_shard: impl FnOnce() -> F,
) -> Result<bool, E>
where
    F: Future<Output = Result<bool, E>>,
{
    match parse_shard_id(table_id, shard_id) {
        Some((ShardStream::Labelled { label, .. }, _)) => Ok(label == arn_label),
        Some((ShardStream::Legacy, _)) | None => {
            if table_label != Some(arn_label) {
                return Ok(false);
            }
            Ok(!table_has_labelled_shard().await?)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::convert::Infallible;

    use extenddb_core::types::StreamViewType;

    use super::shard_in_stream;
    use crate::stream_lineage::shard_id;

    const TABLE: &str = "0f3c1c1e-7b1d-4e3b-9a6e-4a2d3e5f6a7b";
    const LABEL: &str = "2026-01-01T00:00:00.000";
    const LATER: &str = "2026-02-02T00:00:00.000";

    fn legacy_shard(i: u32) -> String {
        format!("shardId-{TABLE}-{i:016}")
    }

    fn labelled_shard(label: &str, i: u32) -> String {
        shard_id(TABLE, label, StreamViewType::KeysOnly, i).expect("label shape")
    }

    /// `shard_in_stream` on a table whose shard rows include a labelled one
    /// when `has_labelled` is set.
    fn in_stream(
        shard: &str,
        table_label: Option<&str>,
        arn_label: &str,
        has_labelled: bool,
    ) -> bool {
        let answer = futures::executor::block_on(shard_in_stream(
            TABLE,
            shard,
            table_label,
            arn_label,
            || async move { Ok::<bool, Infallible>(has_labelled) },
        ));
        match answer {
            Ok(v) => v,
            Err(never) => match never {},
        }
    }

    #[test]
    fn a_labelled_shard_belongs_to_the_stream_its_id_names() {
        assert!(in_stream(
            &labelled_shard(LABEL, 0),
            Some(LABEL),
            LABEL,
            true
        ));
        assert!(in_stream(
            &labelled_shard(LABEL, 3),
            Some(LABEL),
            LABEL,
            true
        ));
    }

    #[test]
    fn a_closed_stream_stays_readable_after_the_table_has_a_newer_one() {
        // The table was disabled and enabled again: its label moved on to the
        // new stream, and the closed stream's shards still answer to the
        // closed stream's ARN.
        assert!(in_stream(
            &labelled_shard(LABEL, 0),
            Some(LATER),
            LABEL,
            true
        ));
        assert!(in_stream(
            &labelled_shard(LATER, 0),
            Some(LATER),
            LATER,
            true
        ));
    }

    #[test]
    fn the_newer_arn_with_an_older_streams_shard_id_is_not_its_stream() {
        // An iterator forged from the new ARN and an old shard id, or the
        // other way round, names a shard the stream does not have.
        assert!(!in_stream(
            &labelled_shard(LABEL, 0),
            Some(LATER),
            LATER,
            true
        ));
        assert!(!in_stream(
            &labelled_shard(LATER, 0),
            Some(LATER),
            LABEL,
            true
        ));
    }

    #[test]
    fn a_legacy_shard_belongs_to_its_tables_current_stream() {
        assert!(in_stream(&legacy_shard(0), Some(LABEL), LABEL, false));
        assert!(in_stream(&legacy_shard(3), Some(LABEL), LABEL, false));
        // The shape SQLite and PostgreSQL wrote before v0.1.14 names the table,
        // not its id; the row is the table's, so it is a legacy shard too.
        assert!(in_stream(
            "shardId-Orders-0000000000000002",
            Some(LABEL),
            LABEL,
            false
        ));
    }

    #[test]
    fn an_earlier_label_on_legacy_shards_is_not_the_current_stream() {
        // The table took a new label while keeping its legacy shard rows: the
        // old ARN no longer names a stream of the table.
        assert!(!in_stream(&legacy_shard(0), Some(LATER), LABEL, false));
        // The new label is a labelled stream of its own (an enable gave it
        // shards), so the legacy rows are the earlier, closed stream and the
        // newer ARN does not name them.
        assert!(!in_stream(&legacy_shard(0), Some(LATER), LATER, true));
    }

    #[test]
    fn a_labelled_shard_is_decided_without_reading_the_tables_shards() {
        let reads = Cell::new(0);
        let answer = futures::executor::block_on(shard_in_stream(
            TABLE,
            &labelled_shard(LABEL, 0),
            Some(LABEL),
            LABEL,
            || {
                reads.set(reads.get() + 1);
                async { Ok::<bool, Infallible>(true) }
            },
        ));
        assert!(matches!(answer, Ok(true)));
        assert_eq!(reads.get(), 0);
    }

    #[test]
    fn a_table_without_a_stream_owns_no_stream_shard() {
        assert!(!in_stream(&legacy_shard(0), None, LABEL, false));
        assert!(!in_stream(&labelled_shard(LABEL, 0), None, LATER, false));
    }
}
