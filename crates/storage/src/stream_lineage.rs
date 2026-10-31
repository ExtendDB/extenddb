// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The streams a table has had, and which of them a shard belongs to.
//!
//! Disabling a table's stream closes it: the stream reports `DISABLED`, every
//! shard gets an `EndingSequenceNumber`, and the stream stays describable and
//! listed. Enabling it again creates a new stream with a new label and new
//! shards. A table therefore has a sequence of streams, and only the newest
//! one can be enabled.
//!
//! Each stream's identity is carried in its shard ids rather than in a catalog
//! row, so no backend needs a schema change: a shard id is
//! `shardId-{table_id}-{label_millis:013}{view_type:1}{index:02}`, where
//! `label_millis` is the stream label as epoch milliseconds and `view_type`
//! one digit for the stream's `StreamViewType`. A shard id written before
//! this scheme (`shardId-{table_id}-{index}`, all zeros but the index)
//! belongs to the table's current stream, as it always did.
//!
//! Every backend reads its table row and shard rows and hands them to the
//! functions here, so the grouping, the status rule, the paging order and the
//! refusal messages are written once.

use extenddb_core::types::{
    KeySchemaElement, SequenceNumberRange, Shard, StreamDescription, StreamSpecification,
    StreamStatus, StreamViewType,
};

use crate::error::StorageError;
use crate::util::{
    format_stream_label, shard_does_not_exist, stream_does_not_exist, stream_not_found,
};

/// Fixed shards per stream (hash-based partition-key assignment).
pub const SHARDS_PER_STREAM: u32 = 4;

/// A `CreateTable` request with a disabled `StreamSpecification` dropped.
///
/// The service does not store `{StreamEnabled: false}`: `DescribeTable` on
/// such a table has no `StreamSpecification`, `LatestStreamArn` or
/// `LatestStreamLabel`, and the table answers `UpdateTable` as one created
/// without the member (measured 2026-10-24). Every backend passes its request
/// through here before storing it.
#[must_use]
pub fn without_disabled_specification(
    mut input: extenddb_core::types::CreateTableInput,
) -> extenddb_core::types::CreateTableInput {
    if input
        .stream_specification
        .as_ref()
        .is_some_and(|s| !s.stream_enabled)
    {
        input.stream_specification = None;
    }
    input
}

/// What an `UpdateTable` `StreamSpecification` asks of the table's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamChange {
    /// Create a new stream with this view type.
    Enable(StreamViewType),
    /// Close the enabled stream.
    Disable,
}

/// Decide a `StreamSpecification` change against the table's stored one.
///
/// # Errors
///
/// `Validation` with the service's sentence when the request enables a stream
/// on a table whose stream is enabled (whatever the view type), or disables
/// one on a table with no enabled stream (never had one, or already
/// disabled). Both measured 2026-10-24.
pub fn stream_change(
    table_name: &str,
    current: Option<&StreamSpecification>,
    requested: &StreamSpecification,
) -> Result<StreamChange, StorageError> {
    let enabled = current.is_some_and(|s| s.stream_enabled);
    match (requested.stream_enabled, enabled) {
        (true, true) => Err(StorageError::Validation(format!(
            "Table already has an enabled stream: TableName: {table_name}"
        ))),
        (false, false) => Err(StorageError::Validation(format!(
            "Table has no stream to disable: TableName: {table_name}"
        ))),
        // Unreachable through the engine, which refuses an enable without a
        // view type before storage with this same sentence. The service's
        // sentence for that case, measured 2026-10-24, is "One or more
        // parameter values were invalid: If stream is being enabled then
        // UpdateViewType is required", and it precedes the already-enabled
        // check; the engine's sentence pre-dates this change and is not
        // changed here.
        (true, false) => requested
            .stream_view_type
            .map(StreamChange::Enable)
            .ok_or_else(|| {
                StorageError::Validation(
                    "StreamViewType must be specified when StreamEnabled is true".to_owned(),
                )
            }),
        (false, true) => Ok(StreamChange::Disable),
    }
}

/// The stream a refused disable closes before it is answered: the table's
/// current label, when `requested` disables a table whose stored specification
/// is already disabled.
///
/// A release before shards were closed on disable left a disabled stream's
/// four shards open, and a close that fails after its catalog write leaves the
/// same state until the request is retried; either way `DescribeStream`
/// reports `DISABLED` while writes keep recording into the open shards. The
/// retry is refused with [`stream_change`]'s sentence, as on the service, and
/// the backend closes the current label's open shards first, which is what the
/// first disable would have done. `None` when the request enables, when the
/// table never had a stream (no stored specification), or when it has no
/// label.
#[must_use]
pub fn disabled_stream_to_close<'a>(
    current: Option<&StreamSpecification>,
    current_label: Option<&'a str>,
    requested: &StreamSpecification,
) -> Option<&'a str> {
    (!requested.stream_enabled && current.is_some_and(|c| !c.stream_enabled))
        .then_some(current_label)
        .flatten()
}

/// The specification to store once a stream change is decided: the request's
/// own on an enable, and on a disable `{StreamEnabled: false}` carrying the
/// closed stream's view type, so a legacy stream (whose view type lives only
/// here) still reports it once disabled. Responses omit a disabled
/// specification (`TableDescription::prepare_for_response`).
#[must_use]
pub fn stored_specification(
    change: StreamChange,
    current: Option<&StreamSpecification>,
    requested: &StreamSpecification,
) -> StreamSpecification {
    match change {
        StreamChange::Enable(_) => requested.clone(),
        StreamChange::Disable => StreamSpecification {
            stream_enabled: false,
            stream_view_type: current.and_then(|c| c.stream_view_type),
        },
    }
}

/// The label for a new stream: now, or one millisecond after the table's
/// previous label when the clock has not moved past it, so two streams of one
/// table never share an ARN.
#[must_use]
pub fn next_stream_label(previous: Option<&str>) -> String {
    let now = crate::util::new_stream_label();
    match previous {
        Some(prev) if now.as_str() <= prev => label_millis(prev)
            .and_then(|m| millis_label(m + 1))
            .unwrap_or(now),
        _ => now,
    }
}

/// The epoch milliseconds a stream label names, or `None` for a string that is
/// not in the label shape.
#[must_use]
pub fn label_millis(label: &str) -> Option<i64> {
    let b = label.as_bytes();
    let seps = [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'.'),
    ];
    if b.len() != 23 || seps.iter().any(|&(i, c)| b[i] != c) {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<u16> {
        let s = label.get(from..to)?;
        s.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| s.parse().ok())?
    };
    let month = time::Month::try_from(u8::try_from(num(5, 7)?).ok()?).ok()?;
    let date = time::Date::from_calendar_date(
        i32::from(num(0, 4)?),
        month,
        u8::try_from(num(8, 10)?).ok()?,
    )
    .ok()?;
    let at = time::Time::from_hms_milli(
        u8::try_from(num(11, 13)?).ok()?,
        u8::try_from(num(14, 16)?).ok()?,
        u8::try_from(num(17, 19)?).ok()?,
        num(20, 23)?,
    )
    .ok()?;
    i64::try_from(date.with_time(at).assume_utc().unix_timestamp_nanos() / 1_000_000).ok()
}

fn millis_label(millis: i64) -> Option<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .ok()
        .map(format_stream_label)
}

fn view_digit(view_type: StreamViewType) -> char {
    match view_type {
        StreamViewType::KeysOnly => '1',
        StreamViewType::NewImage => '2',
        StreamViewType::OldImage => '3',
        StreamViewType::NewAndOldImages => '4',
    }
}

fn digit_view(digit: u8) -> Option<StreamViewType> {
    match digit {
        b'1' => Some(StreamViewType::KeysOnly),
        b'2' => Some(StreamViewType::NewImage),
        b'3' => Some(StreamViewType::OldImage),
        b'4' => Some(StreamViewType::NewAndOldImages),
        _ => None,
    }
}

/// The id of shard `index` of the stream `label` on table `table_id`.
///
/// Derived from the table id, not its name: a name is shared by every account
/// that picks it and reused by delete-and-recreate, while a shard id must be
/// unique across a backend's shard store. 8 + 36 + 1 + 16 = 61 characters for
/// a UUID table id, inside the 65-character limit the AWS SDKs enforce on
/// `ShardId`.
///
/// # Errors
///
/// `Internal` when `label` is not in the label shape; every label comes from
/// [`next_stream_label`] or an earlier stored label.
pub fn shard_id(
    table_id: &str,
    label: &str,
    view_type: StreamViewType,
    index: u32,
) -> Result<String, StorageError> {
    let millis = label_millis(label)
        .ok_or_else(|| StorageError::Internal(format!("malformed stream label {label}")))?;
    Ok(format!(
        "shardId-{table_id}-{millis:013}{}{index:02}",
        view_digit(view_type)
    ))
}

/// Which stream of its table a shard id belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardStream {
    /// An id from before streams were named in shard ids: the table's current
    /// stream.
    Legacy,
    /// An id naming its stream's label and view type.
    Labelled {
        label: String,
        view_type: StreamViewType,
    },
}

/// Parse a shard id of table `table_id`: its stream and its index within the
/// stream. `None` when the id is not one of this table's.
#[must_use]
pub fn parse_shard_id(table_id: &str, shard_id: &str) -> Option<(ShardStream, u32)> {
    let suffix = shard_id
        .strip_prefix("shardId-")?
        .strip_prefix(table_id)?
        .strip_prefix('-')?;
    if suffix.is_empty() || !suffix.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if suffix.len() == 16 && !suffix.starts_with("0000000000000") {
        let millis: i64 = suffix[..13].parse().ok()?;
        let view_type = digit_view(suffix.as_bytes()[13])?;
        let index: u32 = suffix[14..].parse().ok()?;
        return Some((
            ShardStream::Labelled {
                label: millis_label(millis)?,
                view_type,
            },
            index,
        ));
    }
    Some((ShardStream::Legacy, suffix.parse().ok()?))
}

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

/// One shard row as a backend stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRow {
    pub table_id: String,
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub starting_sequence_number: String,
    pub ending_sequence_number: Option<String>,
}

/// The stream-relevant columns of one table row.
#[derive(Debug, Clone)]
pub struct TableStreamRow {
    pub table_name: String,
    pub table_id: String,
    pub table_status: String,
    /// `LatestStreamLabel`: the newest stream, enabled or not.
    pub stream_label: Option<String>,
    pub stream_specification: Option<StreamSpecification>,
}

/// One stream of a table and its shards, sorted by shard id.
#[derive(Debug, Clone)]
pub struct TableStream {
    pub label: String,
    pub view_type: StreamViewType,
    pub shards: Vec<ShardRow>,
}

/// Group a table's shards into its streams, oldest first.
///
/// Legacy shard ids (any id of this table that is not in the labelled shape)
/// take the table's current label and the view type the stored specification
/// carries, enabled or disabled (`KEYS_ONLY` when it carries none, which is
/// what DescribeStream reported for them before), unless a labelled stream
/// already holds that label.
#[must_use]
pub fn table_streams(table: &TableStreamRow, shards: &[ShardRow]) -> Vec<TableStream> {
    let mut streams: Vec<TableStream> = Vec::new();
    let mut legacy: Vec<ShardRow> = Vec::new();
    for row in shards.iter().filter(|r| r.table_id == table.table_id) {
        match parse_shard_id(&table.table_id, &row.shard_id) {
            Some((ShardStream::Labelled { label, view_type }, _)) => {
                match streams.iter_mut().find(|s| s.label == label) {
                    Some(s) => s.shards.push(row.clone()),
                    None => streams.push(TableStream {
                        label,
                        view_type,
                        shards: vec![row.clone()],
                    }),
                }
            }
            // Ids from before streams were named in shard ids: the
            // `shardId-{table_id}-{index}` form, and the
            // `shardId-{table_name}-{index}` form SQLite and PostgreSQL wrote
            // before v0.1.14. The rows are already this table's (filtered by
            // table id above), so any id that does not parse is a legacy one.
            Some((ShardStream::Legacy, _)) | None => legacy.push(row.clone()),
        }
    }
    if let Some(current) = table.stream_label.as_deref()
        && !legacy.is_empty()
        && !streams.iter().any(|s| s.label == current)
    {
        let view_type = table
            .stream_specification
            .as_ref()
            .and_then(|s| s.stream_view_type)
            .unwrap_or(StreamViewType::KeysOnly);
        streams.push(TableStream {
            label: current.to_owned(),
            view_type,
            shards: legacy,
        });
    }
    for s in &mut streams {
        s.shards.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
    }
    streams.sort_by(|a, b| a.label.cmp(&b.label));
    streams
}

/// The ids of the open shards of the stream `label` of table `table_id`: the
/// labelled rows carrying that label, or the legacy rows when no labelled row
/// carries it (a legacy stream's label is the table's label at the time it was
/// read). A disable closes exactly these, so a disable that applies after an
/// overlapping enable has committed does not close the stream the enable just
/// opened.
#[must_use]
pub fn open_shard_ids_of(table_id: &str, label: &str, shards: &[ShardRow]) -> Vec<String> {
    let rows: Vec<&ShardRow> = shards.iter().filter(|r| r.table_id == table_id).collect();
    let labelled_exists = rows.iter().any(|r| {
        matches!(
            parse_shard_id(table_id, &r.shard_id),
            Some((ShardStream::Labelled { label: l, .. }, _)) if l == label
        )
    });
    let mut ids: Vec<String> = rows
        .into_iter()
        .filter(|r| r.ending_sequence_number.is_none())
        .filter(|r| match parse_shard_id(table_id, &r.shard_id) {
            Some((ShardStream::Labelled { label: l, .. }, _)) => l == label,
            Some((ShardStream::Legacy, _)) | None => !labelled_exists,
        })
        .map(|r| r.shard_id.clone())
        .collect();
    ids.sort();
    ids
}

/// The ids of the open shards of every stream of table `table_id` older than
/// `label`: the labelled rows carrying an earlier label, and the legacy rows.
/// An enable creating stream `label` closes these, so a stream left open by a
/// close that failed after its catalog write is closed by the next enable,
/// whatever label that enable read.
#[must_use]
pub fn open_shard_ids_before(table_id: &str, label: &str, shards: &[ShardRow]) -> Vec<String> {
    let mut ids: Vec<String> = shards
        .iter()
        .filter(|r| r.table_id == table_id && r.ending_sequence_number.is_none())
        .filter(|r| match parse_shard_id(table_id, &r.shard_id) {
            Some((ShardStream::Labelled { label: l, .. }, _)) => l.as_str() < label,
            Some((ShardStream::Legacy, _)) | None => true,
        })
        .map(|r| r.shard_id.clone())
        .collect();
    ids.sort();
    ids
}

/// A stream's status: only the table's newest stream, while its specification
/// is enabled, is `ENABLED` (`DISABLING` once the table is being deleted);
/// every other stream of the table is `DISABLED`.
#[must_use]
pub fn stream_status(table: &TableStreamRow, label: &str) -> StreamStatus {
    let current = table.stream_label.as_deref() == Some(label)
        && table
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled);
    if !current {
        StreamStatus::Disabled
    } else if table.table_status == "DELETING" {
        StreamStatus::Disabling
    } else {
        StreamStatus::Enabled
    }
}

/// Whether the stream `label` an enable just created is still the table's
/// enabled stream, read from the catalog after its shards were inserted. On
/// the backends whose catalog and shards are two commits, a disable that reads
/// the row between the enable's commit and its shard inserts closes the shards
/// it finds at that instant, so the enable re-reads the row afterwards and
/// closes what it inserted when the answer is `false` (the label moved on, the
/// specification is no longer enabled, or the table is gone).
#[must_use]
pub fn enable_still_current(table: Option<&TableStreamRow>, label: &str) -> bool {
    table.is_some_and(|t| {
        t.stream_label.as_deref() == Some(label)
            && t.stream_specification
                .as_ref()
                .is_some_and(|s| s.stream_enabled)
    })
}

/// The shard ids of the table's open shards: the ones its enabled stream
/// writes to. Empty when the stream is disabled.
#[must_use]
pub fn open_shard_ids(shards: &[ShardRow]) -> Vec<String> {
    let mut ids: Vec<String> = shards
        .iter()
        .filter(|r| r.ending_sequence_number.is_none())
        .map(|r| r.shard_id.clone())
        .collect();
    ids.sort();
    ids
}

/// Build a `DescribeStream` answer from the table row and all its shard rows.
///
/// # Errors
///
/// `TableNotFound` with the stream-not-found message when the table has no
/// stream with this label.
#[allow(clippy::too_many_arguments)]
pub fn describe_stream(
    stream_arn: String,
    table: &TableStreamRow,
    key_schema: Vec<KeySchemaElement>,
    label: String,
    shards: &[ShardRow],
    limit: Option<i64>,
    exclusive_start_shard_id: Option<&str>,
) -> Result<StreamDescription, StorageError> {
    let stream = table_streams(table, shards)
        .into_iter()
        .find(|s| s.label == label)
        .ok_or_else(|| stream_not_found(&stream_arn))?;
    let limit = usize::try_from(limit.unwrap_or(100)).unwrap_or(0).max(1);
    let mut remaining: Vec<&ShardRow> = stream
        .shards
        .iter()
        .filter(|r| exclusive_start_shard_id.is_none_or(|s| r.shard_id.as_str() > s))
        .collect();
    let last_evaluated_shard_id =
        (remaining.len() > limit).then(|| remaining[limit - 1].shard_id.clone());
    remaining.truncate(limit);
    Ok(StreamDescription {
        stream_status: stream_status(table, &label),
        stream_view_type: stream.view_type,
        stream_arn,
        stream_label: label,
        table_name: table.table_name.clone(),
        key_schema,
        shards: remaining
            .into_iter()
            .map(|r| Shard {
                shard_id: r.shard_id.clone(),
                parent_shard_id: r.parent_shard_id.clone(),
                sequence_number_range: SequenceNumberRange {
                    starting_sequence_number: r.starting_sequence_number.clone(),
                    ending_sequence_number: r.ending_sequence_number.clone(),
                },
            })
            .collect(),
        last_evaluated_shard_id,
    })
}

/// Check that `shard_id` is a shard of the stream `label` of `table`.
///
/// # Errors
///
/// [`crate::util::STREAM_DOES_NOT_EXIST`] when the table has no such stream,
/// and [`crate::util::SHARD_DOES_NOT_EXIST`] when the stream exists but the
/// shard is not one of its own (measured 2026-10-24 for a shard of the table's
/// other stream and for a well-formed shard id the account never had).
pub fn validate_shard(
    table: Option<&TableStreamRow>,
    label: &str,
    shards: &[ShardRow],
    shard_id: &str,
) -> Result<(), StorageError> {
    let stream = table
        .and_then(|t| {
            table_streams(t, shards)
                .into_iter()
                .find(|s| s.label == label)
        })
        .ok_or_else(stream_does_not_exist)?;
    if stream.shards.iter().any(|r| r.shard_id == shard_id) {
        Ok(())
    } else {
        Err(shard_does_not_exist())
    }
}

/// The sentence for an `ExclusiveStartStreamArn` that names a stream of a
/// table other than `TableName` (measured 2026-10-24).
#[must_use]
pub fn invalid_exclusive_start_stream_arn(arn: &str) -> StorageError {
    StorageError::Validation(format!(
        "One or more parameter values were invalid: ExclusiveStartStreamArn provided is invalid: {arn}"
    ))
}

#[path = "stream_lineage_list.rs"]
mod list;
pub use list::list_streams;

#[cfg(test)]
#[path = "stream_lineage_tests.rs"]
mod tests;
