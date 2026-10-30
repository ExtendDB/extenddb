// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row handling for the Cassandra Scan: skipping rows that hold no item,
//! and reading on after the last row of a read.
//!
//! A partition whose items have all been deleted answers a read with one row
//! of its static columns. A TTL claim on an absent key, and a transaction
//! prepare that lands on an item deleted after the transaction read it, leave
//! a row with no `item_data`. The scan skips such rows. When every row of a
//! full read holds no item, the scan reads on after the last row read (its
//! partition key and, for a row with one, its sort key), with up to twice as
//! many rows each time and at most [`RUN_READ_ROWS`] (or the read's own
//! LIMIT, if larger), so no statement reads through the whole run. Cassandra
//! counts a tombstone for each deleted row in the partitions such a read
//! passes, so a read Cassandra refuses with a read failure (for example
//! `READ_TOO_MANY_TOMBSTONES`) is tried again with half as many rows, down to
//! one, and later reads in the same call stay at that size.

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
use crate::cassandra_util::CassandraSession;

/// The most rows one read takes while the scan reads on through rows that
/// hold no item, unless the read's own LIMIT is larger.
pub(super) const RUN_READ_ROWS: usize = 1_000;

pub(super) type Row = cdrs_tokio::types::rows::Row;

/// A clustering predicate after `pk = ? AND`, and the values it binds.
pub(super) type Clustering = (String, Vec<Value>);

/// Parses the item of every row that holds one, in order.
pub(super) fn rows_to_items(rows: Vec<Row>) -> Result<Vec<Item>, StorageError> {
    rows.into_iter()
        .map(|row| {
            let item_data: Option<String> = row.get_by_name("item_data").map_err(|error| {
                StorageError::Internal(format!("scan parse item_data failed: {error}"))
            })?;
            item_data.map(json_to_item).transpose()
        })
        .filter_map(Result::transpose)
        .collect()
}

/// The position of a base-table row, to read on after it: its partition key
/// text, and its sort key as a clustering predicate when the row has one. A
/// static-only row has no sort key, and neither does a hash-only table's row.
pub(super) fn row_position(
    row: &Row,
    sk_type: Option<ScalarAttributeType>,
) -> Result<(String, Option<Clustering>), StorageError> {
    let parse =
        |error: CdrsError| StorageError::Internal(format!("scan parse row key failed: {error}"));
    let pk: Option<String> = row.get_by_name("pk").map_err(parse)?;
    let pk = pk.ok_or_else(|| StorageError::Internal("scan row without pk".to_owned()))?;
    let Some(sk_type) = sk_type else {
        return Ok((pk, None));
    };
    let col = sk_column(sk_type);
    let value = match sk_type {
        ScalarAttributeType::S => IntoRustByName::<String>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
        ScalarAttributeType::N => IntoRustByName::<Decimal>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
        ScalarAttributeType::B => IntoRustByName::<Blob>::get_by_name(row, col)
            .map_err(parse)?
            .map(Value::from),
    };
    Ok((pk, value.map(|v| (format!("{col} > ?"), vec![v]))))
}

/// Runs one statement and returns its rows, keeping the driver's error.
async fn rows_of(
    session: &Arc<CassandraSession>,
    query: &str,
    values: Vec<Value>,
    context: &'static str,
) -> Result<Vec<Row>, (&'static str, CdrsError)> {
    let result = session
        .query_with_values(query, QueryValues::SimpleValues(values))
        .await
        .map_err(|e| (context, e))?;
    let body = result.response_body().map_err(|e| (context, e))?;
    Ok(body.into_rows().unwrap_or_default())
}

/// Rows after a position, `limit` in all: the rest of partition `pk_text`
/// past `clustering` (query 1, when given), then the partitions after it in
/// token order up to `token_upper` (query 2).
async fn rows_after_position(
    session: &Arc<CassandraSession>,
    select_prefix: &str,
    pk_text: &str,
    clustering: Option<&Clustering>,
    token_upper: Option<i64>,
    limit: usize,
) -> Result<Vec<Row>, (&'static str, CdrsError)> {
    let mut all_rows = Vec::new();

    if let Some((clustering_predicate, clustering_binds)) = clustering {
        let query1 =
            format!("{select_prefix} WHERE pk = ? AND {clustering_predicate} LIMIT {limit}");
        let mut values: Vec<Value> = Vec::with_capacity(1 + clustering_binds.len());
        values.push(Value::from(pk_text));
        values.extend(clustering_binds.iter().cloned());
        all_rows.extend(rows_of(session, &query1, values, "scan_finish_partition").await?);
    }

    if all_rows.len() < limit {
        let remaining = limit - all_rows.len();
        let token_clause = if token_upper.is_some() {
            " AND token(pk) <= ?"
        } else {
            ""
        };
        let query2 =
            format!("{select_prefix} WHERE token(pk) > token(?){token_clause} LIMIT {remaining}");
        let mut values: Vec<Value> = vec![Value::from(pk_text)];
        if let Some(hi) = token_upper {
            values.push(Value::from(hi));
        }
        all_rows.extend(rows_of(session, &query2, values, "scan_next_partitions").await?);
    }

    Ok(all_rows)
}

fn storage_error((context, error): (&'static str, CdrsError)) -> StorageError {
    tracing::error!("{context} query failed: {error}");
    StorageError::Internal(format!("{context}: {error}"))
}

/// [`rows_after_position`], with driver errors as storage errors.
pub(super) async fn read_after_position(
    session: &Arc<CassandraSession>,
    select_prefix: &str,
    pk_text: &str,
    clustering: Option<&Clustering>,
    token_upper: Option<i64>,
    limit: usize,
) -> Result<Vec<Row>, StorageError> {
    rows_after_position(
        session,
        select_prefix,
        pk_text,
        clustering,
        token_upper,
        limit,
    )
    .await
    .map_err(storage_error)
}

/// A continuation read of `*limit` rows after a position. A read Cassandra
/// refuses with a read failure is tried again with half the rows, down to
/// one; `*limit` ends as the size that was read and `*ceiling` is lowered to
/// it, so the reads after it do not grow back past it.
pub(super) async fn read_on_after_position(
    session: &Arc<CassandraSession>,
    select_prefix: &str,
    pk_text: &str,
    clustering: Option<&Clustering>,
    token_upper: Option<i64>,
    limit: &mut usize,
    ceiling: &mut usize,
) -> Result<Vec<Row>, StorageError> {
    loop {
        match rows_after_position(
            session,
            select_prefix,
            pk_text,
            clustering,
            token_upper,
            *limit,
        )
        .await
        {
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
            Err(error) => return Err(storage_error(error)),
        }
    }
}
