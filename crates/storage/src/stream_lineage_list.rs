// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ListStreams` page over a table's streams; see the parent module.

use extenddb_core::types::StreamSummary;

use super::{ShardRow, TableStreamRow, invalid_exclusive_start_stream_arn, table_streams};
use crate::error::StorageError;

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
