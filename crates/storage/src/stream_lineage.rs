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
    StreamStatus, StreamSummary, StreamViewType,
};

use crate::error::StorageError;
use crate::util::format_stream_label;

/// Fixed shards per stream (hash-based partition-key assignment).
pub const SHARDS_PER_STREAM: u32 = 4;

/// `GetShardIterator` naming a shard that is not part of the named stream.
/// Measured 2026-10-24 for a shard id of the table's other stream, on the old
/// and on the new stream ARN.
pub const SHARD_NOT_FOUND: &str = "Requested resource not found: Shard does not exist";

/// `GetShardIterator` naming a stream ARN the account does not have. Measured
/// 2026-10-24 with a real shard id and a label three digits off.
pub const STREAM_NOT_FOUND: &str = "Requested resource not found: Stream does not exist";

/// `DescribeStream` on a stream ARN that names no stream of the account.
/// Measured 2026-10-24 on an existing table with its label changed: no
/// trailing period.
#[must_use]
pub fn stream_not_found(stream_arn: &str) -> StorageError {
    StorageError::TableNotFound(format!(
        "Requested resource not found: Stream: {stream_arn} not found"
    ))
}

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
/// [`STREAM_NOT_FOUND`] when the table has no such stream, and
/// [`SHARD_NOT_FOUND`] when the stream exists but the shard is not one of its
/// own (measured for a shard of the table's other stream and for a well-formed
/// shard id the account never had).
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
        .ok_or_else(|| StorageError::TableNotFound(STREAM_NOT_FOUND.to_owned()))?;
    if stream.shards.iter().any(|r| r.shard_id == shard_id) {
        Ok(())
    } else {
        Err(StorageError::TableNotFound(SHARD_NOT_FOUND.to_owned()))
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

/// Build a `ListStreams` page from the account's tables and their shards.
///
/// Every stream of every table is listed, disabled ones included, in the
/// order the service answered on 2026-10-24: table names descending, and a
/// table's streams newest first. `exclusive_start` is the (table name, label)
/// of the previous page's last stream; `arn` formats a stream ARN.
/// `LastEvaluatedStreamArn` is present whenever `limit` cut the page, even
/// when no stream follows: with two streams and `Limit=1`, page two carries it
/// and page three is empty (measured 2026-10-24).
///
/// # Errors
///
/// `Validation` when `exclusive_start` names a table other than `table_name`.
pub fn list_streams(
    tables: &[TableStreamRow],
    shards: &[ShardRow],
    table_name: Option<&str>,
    limit: i64,
    exclusive_start: Option<(String, String)>,
    arn: impl Fn(&str, &str) -> String,
) -> Result<(Vec<StreamSummary>, Option<String>), StorageError> {
    if let (Some(name), Some((start_table, start_label))) = (table_name, &exclusive_start)
        && start_table != name
    {
        return Err(invalid_exclusive_start_stream_arn(&arn(
            start_table,
            start_label,
        )));
    }
    let mut listed: Vec<(String, String)> = tables
        .iter()
        .filter(|t| table_name.is_none_or(|n| t.table_name == n))
        .flat_map(|t| {
            table_streams(t, shards)
                .into_iter()
                .map(|s| (t.table_name.clone(), s.label))
        })
        .collect();
    listed.sort_by(|a, b| b.cmp(a));
    if let Some(start) = exclusive_start {
        listed.retain(|entry| *entry < start);
    }
    let limit = usize::try_from(limit).unwrap_or(0).max(1);
    let last = (listed.len() >= limit).then(|| {
        let (t, l) = &listed[limit - 1];
        arn(t, l)
    });
    listed.truncate(limit);
    let summaries = listed
        .into_iter()
        .map(|(table_name, label)| StreamSummary {
            stream_arn: arn(&table_name, &label),
            stream_label: label,
            table_name,
        })
        .collect();
    Ok((summaries, last))
}

#[cfg(test)]
#[path = "stream_lineage_tests.rs"]
mod tests;
