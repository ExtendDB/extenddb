// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data rows for the Cassandra Scan, Query and GSI backfill: the item a row
//! holds, reads that go on after the last row read, and reads that
//! Cassandra refuses.
//!
//! A partition whose items have all been deleted answers a read with one row
//! of its static columns. A TTL claim on an absent key, and a transaction
//! prepare that lands on an item deleted after the transaction read it, leave
//! a row with no `item_data`. The reads skip such rows. When every row of a
//! full read holds no item, the scan reads on after the last row read (its
//! partition key and, for a row with one, its sort key), with up to twice as
//! many rows each time and at most [`RUN_READ_ROWS`] (or the read's own
//! LIMIT, if larger). A Query reads on the same way inside its partition,
//! after the last row's sort key. Cassandra counts a tombstone for each
//! deleted row in the partitions a read passes, so every read of a Scan
//! call, the first one included, that Cassandra refuses with a read failure
//! (for example `READ_TOO_MANY_TOMBSTONES`) is tried again with half as many
//! rows, down to one, and the later reads in the same call stay at that
//! size. A Query's reads after its first read get the same retry.

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::ErrorType;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::decimal::Decimal;
use cdrs_tokio::types::value::Value;
use extenddb_core::types::{Item, ScalarAttributeType};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::sk_column;
use std::sync::Arc;

use super::json_to_item;
use crate::cassandra_util::{CassandraSession, PolicySession, Stmt};

/// The most rows one read takes while a read goes on through rows that hold
/// no item, unless the read's own LIMIT is larger.
pub(super) const RUN_READ_ROWS: usize = 1_000;

pub(super) type Row = cdrs_tokio::types::rows::Row;

/// A clustering predicate after `pk = ? AND`, and the values it binds.
pub(super) type Clustering = (String, Vec<Value>);

/// A position to read on after: a partition key text, and a clustering
/// predicate when the read goes on inside that partition.
pub(super) type Position = (String, Option<Clustering>);

/// The item `row` holds, or `None` for a row without `item_data`.
pub(super) fn row_item(row: &Row, context: &str) -> Result<Option<Item>, StorageError> {
    let item_data: Option<String> = row.get_by_name("item_data").map_err(|error| {
        StorageError::Internal(format!("{context} parse item_data failed: {error}"))
    })?;
    item_data.map(json_to_item).transpose()
}

/// The items `rows` hold, in order, without the rows that hold no item.
pub(super) fn row_items(rows: &[Row], context: &str) -> Result<Vec<Item>, StorageError> {
    rows.iter()
        .map(|row| row_item(row, context))
        .filter_map(Result::transpose)
        .collect()
}

/// The position of a base-table row, to read on after it: its partition key
/// text, and its sort key as a clustering predicate when the row has one. A
/// static-only row has no sort key, and neither does a hash-only table's row.
pub(super) fn row_position(
    row: &Row,
    sk_type: Option<ScalarAttributeType>,
) -> Result<Position, StorageError> {
    let pk: Option<String> = row
        .get_by_name("pk")
        .map_err(|error| StorageError::Internal(format!("scan parse row key failed: {error}")))?;
    let pk = pk.ok_or_else(|| StorageError::Internal("scan row without pk".to_owned()))?;
    let Some(sk_type) = sk_type else {
        return Ok((pk, None));
    };
    let col = sk_column(sk_type);
    let clustering = row_sort_key(row, sk_type)?.map(|v| (format!("{col} > ?"), vec![v]));
    Ok((pk, clustering))
}

/// The sort key of a base-table row of a table whose sort key has type
/// `sk_type`, as a bind value; `None` for a static-only row.
pub(super) fn row_sort_key(
    row: &Row,
    sk_type: ScalarAttributeType,
) -> Result<Option<Value>, StorageError> {
    let parse =
        |error: CdrsError| StorageError::Internal(format!("parse row sort key failed: {error}"));
    let col = sk_column(sk_type);
    Ok(match sk_type {
        ScalarAttributeType::S => IntoRustByName::<String>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
        ScalarAttributeType::N => IntoRustByName::<Decimal>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
        ScalarAttributeType::B => IntoRustByName::<Blob>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
    })
}

/// Runs one read at the consistency level of [`Stmt::Read`] and returns its
/// rows, keeping the driver's error.
pub(super) async fn rows_of(
    session: &Arc<CassandraSession>,
    query: &str,
    values: Vec<Value>,
    context: &'static str,
) -> Result<Vec<Row>, (&'static str, CdrsError)> {
    let result = session
        .run(Stmt::Read, query, QueryValues::SimpleValues(values))
        .await
        .map_err(|e| (context, e))?;
    let body = result.response_body().map_err(|e| (context, e))?;
    Ok(body.into_rows().unwrap_or_default())
}

/// Up to `limit` rows of a scan within the inclusive `token_bounds` (the
/// whole ring when `None`). With no `position`, the rows from the lower
/// bound. Else the rows after `position`: the rest of its partition past its
/// clustering predicate (query 1, when it has one), then the partitions after
/// it in token order up to the upper bound (query 2).
pub(super) async fn scan_rows(
    session: &Arc<CassandraSession>,
    select_prefix: &str,
    position: Option<&Position>,
    token_bounds: Option<(i64, i64)>,
    limit: usize,
) -> Result<Vec<Row>, (&'static str, CdrsError)> {
    let Some((pk_text, clustering)) = position else {
        let (bounds, values) = match token_bounds {
            Some((lo, hi)) => (
                " WHERE token(pk) >= ? AND token(pk) <= ?",
                vec![Value::from(lo), Value::from(hi)],
            ),
            None => ("", Vec::new()),
        };
        let query = format!("{select_prefix}{bounds} LIMIT {limit}");
        return rows_of(session, &query, values, "scan_first_page").await;
    };
    let mut all_rows = Vec::new();

    if let Some((clustering_predicate, clustering_binds)) = clustering {
        let query1 =
            format!("{select_prefix} WHERE pk = ? AND {clustering_predicate} LIMIT {limit}");
        let mut values: Vec<Value> = Vec::with_capacity(1 + clustering_binds.len());
        values.push(Value::from(pk_text.as_str()));
        values.extend(clustering_binds.iter().cloned());
        all_rows.extend(rows_of(session, &query1, values, "scan_finish_partition").await?);
    }

    if all_rows.len() < limit {
        let remaining = limit - all_rows.len();
        let token_clause = if token_bounds.is_some() {
            " AND token(pk) <= ?"
        } else {
            ""
        };
        let query2 =
            format!("{select_prefix} WHERE token(pk) > token(?){token_clause} LIMIT {remaining}");
        let mut values: Vec<Value> = vec![Value::from(pk_text.as_str())];
        if let Some((_, hi)) = token_bounds {
            values.push(Value::from(hi));
        }
        all_rows.extend(rows_of(session, &query2, values, "scan_next_partitions").await?);
    }

    Ok(all_rows)
}

/// Runs `read` with `*limit` rows. A read Cassandra refuses with a read
/// failure runs again with half the rows, down to one; `*limit` ends as the
/// size that was read and `*ceiling` is lowered to it, so the reads after it
/// do not grow back past it.
pub(super) async fn read_halving<F, Fut>(
    limit: &mut usize,
    ceiling: &mut usize,
    mut read: F,
) -> Result<Vec<Row>, StorageError>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<Vec<Row>, (&'static str, CdrsError)>>,
{
    loop {
        match read(*limit).await {
            Ok(rows) => return Ok(rows),
            Err((context, CdrsError::Server { body, .. }))
                if *limit > 1 && matches!(body.ty, ErrorType::ReadFailure(_)) =>
            {
                tracing::warn!(
                    rows = *limit,
                    "{context}: read failure ({}), reading again with half the rows",
                    body.message
                );
                *limit /= 2;
                *ceiling = *limit;
            }
            Err((context, error)) => {
                tracing::error!("{context} query failed: {error}");
                return Err(StorageError::Internal(format!("{context}: {error}")));
            }
        }
    }
}
