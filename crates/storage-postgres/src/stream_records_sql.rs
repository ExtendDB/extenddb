// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The statements `get_stream_records` runs on the data database.
//!
//! Each statement starts from the shard row. A shard of the named table
//! returns one row even when it has no record to read (the record column is
//! NULL), and a shard that belongs to another table returns no row.
//!
//! The records come from a `LEFT JOIN LATERAL` subquery that carries its own
//! `ORDER BY sequence_number LIMIT n`. With the `ORDER BY` and the `LIMIT`
//! placed after a plain `LEFT JOIN`, the planner does not treat the join
//! output as ordered by the record's sequence number. It fetches every record
//! of the shard after the bound and sorts them before the `LIMIT` applies, so
//! the cost of a call grows with the unread backlog of the shard. Inside the
//! subquery the planner reads the primary key `(shard_id, sequence_number)` in
//! order and stops after `n` rows.
//!
//! The text is exported (hidden from the documentation) so the plan test in
//! `tests/stream_shard_ownership.rs` runs the statements the engine runs.

/// A page from the start of the shard, for an iterator that carries no
/// sequence number.
///
/// `$1` is the shard id, `$2` the id of the table the stream ARN names and `$3`
/// the page size.
pub const STREAM_RECORDS_FROM_START: &str = "\
    SELECT s.shard_id, r.record_data FROM stream_shards s \
    LEFT JOIN LATERAL ( \
        SELECT sequence_number, record_data FROM stream_records \
        WHERE shard_id = s.shard_id \
        ORDER BY sequence_number LIMIT $3 \
    ) r ON true \
    WHERE s.shard_id = $1 AND s.table_id = $2 \
    ORDER BY r.sequence_number";

/// The page after a sequence number.
///
/// `$1` is the shard id, `$2` the id of the table the stream ARN names, `$3`
/// the sequence number the page follows and `$4` the page size.
pub const STREAM_RECORDS_AFTER: &str = "\
    SELECT s.shard_id, r.record_data FROM stream_shards s \
    LEFT JOIN LATERAL ( \
        SELECT sequence_number, record_data FROM stream_records \
        WHERE shard_id = s.shard_id AND sequence_number > $3 \
        ORDER BY sequence_number LIMIT $4 \
    ) r ON true \
    WHERE s.shard_id = $1 AND s.table_id = $2 \
    ORDER BY r.sequence_number";
