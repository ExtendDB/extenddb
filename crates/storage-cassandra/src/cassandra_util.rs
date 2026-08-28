// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra-specific utility functions.

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::rows::Row;
use std::sync::Arc;

pub type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Trait for errors that can be constructed from database operation failures.
/// Implemented by [`extenddb_storage::management_store::OpError`],
/// [`extenddb_storage::error::StorageError`], and
/// [`extenddb_core::error::DynamoDbError`].
pub trait FromDbError: Sized {
    fn db_error(msg: String) -> Self;
}

impl FromDbError for extenddb_storage::management_store::OpError {
    fn db_error(msg: String) -> Self {
        extenddb_storage::management_store::OpError::Internal(msg)
    }
}

impl FromDbError for extenddb_storage::error::StorageError {
    fn db_error(msg: String) -> Self {
        extenddb_storage::error::StorageError::Internal(msg)
    }
}

impl FromDbError for extenddb_core::error::DynamoDbError {
    fn db_error(msg: String) -> Self {
        extenddb_core::error::DynamoDbError::InternalServerError(msg)
    }
}

/// Check if an error is a unique constraint violation.
/// For Cassandra, this is always false in stub implementations.
#[must_use]
pub fn is_unique_violation(_err: &cdrs_tokio::error::Error) -> bool {
    false
}

/// Check if an error is a foreign key constraint violation.
/// For Cassandra, this is always false in stub implementations.
#[must_use]
pub fn is_fk_violation(_err: &cdrs_tokio::error::Error) -> bool {
    false
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 1: Core Query Helpers
// ══════════════════════════════════════════════════════════════════════════════

/// Execute a query and return all rows with standardized error handling.
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn query_rows<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    let result = session
        .query_with_values(query, values)
        .await
        .map_err(|e| {
            tracing::error!("{context} query failed: {e}");
            E::db_error(format!("{context}: {e}"))
        })?;

    let body = result.response_body().map_err(|e| {
        tracing::error!("{context} response_body failed: {e}");
        E::db_error(format!("{context} response_body: {e}"))
    })?;

    Ok(body.into_rows().unwrap_or_default())
}

/// Execute a query and return the first row, if any.
///
/// Similar to `sqlx::query().fetch_optional()`.
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn query_optional<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Option<Row>, E> {
    let mut rows = query_rows(session, query, values, context).await?;
    Ok(rows.drain(..).next())
}

/// Execute a non-query statement (INSERT/UPDATE/DELETE).
///
/// # Errors
/// Returns an error if the Cassandra statement fails.
pub async fn execute<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<(), E> {
    session
        .query_with_values(query, values)
        .await
        .map_err(|e| {
            tracing::error!("{context} execute failed: {e}");
            E::db_error("Database error".to_owned())
        })?;

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Type Conversion Helpers
// ══════════════════════════════════════════════════════════════════════════════

/// Extract a typed column value from a `Row` with standardized error handling.
///
/// # Errors
/// Returns an error if the column cannot be extracted or parsed as type `T`.
///
/// # Example
/// ```ignore
/// let name: String = get_column(&row, "user_name", "create_user")?;
/// let count: i64 = get_column(&row, "count", "list_items")?;
/// ```
pub fn get_column<T, E: FromDbError>(row: &Row, column: &str, context: &str) -> Result<T, E>
where
    cdrs_tokio::types::rows::Row: cdrs_tokio::types::IntoRustByName<T>,
{
    use cdrs_tokio::types::IntoRustByName;
    row.get_r_by_name(column).map_err(|e| {
        tracing::error!("{context} parse column '{column}': {e}");
        E::db_error("Database error".to_owned())
    })
}

/// Convert a Cassandra timestamp (milliseconds since Unix epoch) to [`time::OffsetDateTime`].
///
/// Cassandra stores timestamps as milliseconds; this divides by 1000 before converting.
///
/// # Errors
/// Returns an error if the timestamp value is out of range.
pub fn timestamp_to_datetime<E: FromDbError>(
    millis: i64,
    context: &str,
) -> Result<time::OffsetDateTime, E> {
    time::OffsetDateTime::from_unix_timestamp(millis / 1000).map_err(|e| {
        tracing::error!("{context} convert timestamp {millis}: {e}");
        E::db_error("Database error".to_owned())
    })
}

/// Extract a timestamp column and convert to [`time::OffsetDateTime`].
///
/// Convenience helper combining `get_column` + `timestamp_to_datetime`.
///
/// # Errors
/// Returns an error if the column cannot be extracted or the timestamp is out of range.
///
/// # Example
/// ```ignore
/// let created_at = get_timestamp(&row, "created_at", "list_users")?;
/// ```
pub fn get_timestamp<E: FromDbError>(
    row: &Row,
    column: &str,
    context: &str,
) -> Result<time::OffsetDateTime, E> {
    let millis: i64 = get_column(row, column, context)?;
    timestamp_to_datetime(millis, context)
}

/// Map rows to `Vec<T>` with a mapper function, collecting errors.
///
/// # Errors
/// Returns an error if any row fails to map.
///
/// # Example
/// ```ignore
/// let users = map_rows(rows, |row| {
///     Ok(get_column(&row, "user_name", "list_users")?)
/// }, "list_users")?;
/// ```
#[allow(clippy::needless_pass_by_value)] // Vec<Row> matches query_rows return type; &[Row] would require .as_slice() at 28 call sites
pub fn map_rows<T, F, E: FromDbError>(
    rows: Vec<Row>,
    mapper: F,
    _context: &str,
) -> Result<Vec<T>, E>
where
    F: Fn(&Row) -> Result<T, E>,
{
    rows.iter().map(mapper).collect()
}

/// Convert millisecond timestamp to seconds as `f64` (for `creation_date_time` fields).
#[allow(clippy::cast_precision_loss)]
pub fn millis_to_seconds_f64(timestamp_millis: i64) -> f64 {
    timestamp_millis as f64 / 1_000.0
}

/// Return the current time as milliseconds since Unix epoch as `i64`.
///
/// `SystemTime::as_millis()` returns `u128`; this cast is safe for all
/// timestamps within the range of `i64` (until year 292,277,026).
#[allow(clippy::cast_possible_truncation)]
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
///
/// Parses the `[applied]` column from the Cassandra LWT response.
/// Use for `INSERT ... IF NOT EXISTS` and `UPDATE ... IF ...` statements.
///
/// # Errors
/// Returns an error if the Cassandra statement fails or the response cannot be parsed.
pub async fn apply_lwt<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<bool, E> {
    use cdrs_tokio::types::IntoRustByName;

    let result = session
        .query_with_values(query, values)
        .await
        .map_err(|e| {
            tracing::error!("{context} lwt failed: {e}");
            E::db_error("Database error".to_owned())
        })?;

    let body = result.response_body().map_err(|e| {
        tracing::error!("{context} lwt response_body failed: {e}");
        E::db_error("Database error".to_owned())
    })?;

    Ok(body
        .into_rows()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.get_r_by_name("[applied]").ok())
        .unwrap_or(false))
}
