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
/// Implemented by OpError, StorageError, and DynamoDbError.
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
pub fn is_unique_violation(_err: &cdrs_tokio::error::Error) -> bool {
    false
}

/// Check if an error is a foreign key constraint violation.
/// For Cassandra, this is always false in stub implementations.
pub fn is_fk_violation(_err: &cdrs_tokio::error::Error) -> bool {
    false
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 1: Core Query Helpers
// ══════════════════════════════════════════════════════════════════════════════

/// Execute a query and return all rows with standardized error handling.
///
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
///
/// # Arguments
/// * `session` - Cassandra session
/// * `query` - CQL query string
/// * `values` - Query parameters
/// * `context` - Context string for error logging (e.g., "list_access_keys")
///
/// # Returns
/// Vec of rows, or error if query fails
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
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
/// Similar to sqlx::query().fetch_optional().
///
/// # Arguments
/// * `session` - Cassandra session
/// * `query` - CQL query string
/// * `values` - Query parameters
/// * `context` - Context string for error logging
///
/// # Returns
/// Option<Row>, or error if query fails
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
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
///
/// # Arguments
/// * `session` - Cassandra session
/// * `query` - CQL statement
/// * `values` - Query parameters
/// * `context` - Context string for error logging
///
/// # Returns
/// Ok(()) if successful, error if query fails
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

/// Extract a typed column value from a Row with standardized error handling.
///
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
/// The return type T must be specified at the call site.
///
/// # Arguments
/// * `row` - Row to extract from
/// * `column` - Column name
/// * `context` - Context string for error logging
///
/// # Returns
/// Typed value T, or error if parsing fails
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

/// Convert Cassandra timestamp (milliseconds) to OffsetDateTime.
///
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
/// Cassandra stores timestamps as milliseconds since Unix epoch.
/// This helper divides by 1000 and converts to OffsetDateTime.
///
/// # Arguments
/// * `millis` - Timestamp in milliseconds
/// * `context` - Context string for error logging
///
/// # Returns
/// OffsetDateTime, or error if conversion fails
pub fn timestamp_to_datetime<E: FromDbError>(
    millis: i64,
    context: &str,
) -> Result<time::OffsetDateTime, E> {
    time::OffsetDateTime::from_unix_timestamp(millis / 1000).map_err(|e| {
        tracing::error!("{context} convert timestamp {millis}: {e}");
        E::db_error("Database error".to_owned())
    })
}

/// Extract a timestamp column and convert to OffsetDateTime.
///
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
/// Convenience helper combining get_column + timestamp_to_datetime.
///
/// # Arguments
/// * `row` - Row to extract from
/// * `column` - Column name
/// * `context` - Context string for error logging
///
/// # Returns
/// OffsetDateTime, or error if extraction/conversion fails
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

/// Map rows to Vec<T> with a mapper function, collecting errors.
///
/// Generic over error type - works with OpError, StorageError, or DynamoDbError.
/// Reduces boilerplate when transforming Vec<Row> to Vec<SomeType>.
///
/// # Arguments
/// * `rows` - Rows to map
/// * `mapper` - Function to transform each row
/// * `context` - Context string for error logging
///
/// # Returns
/// Vec<T>, or error if any mapping fails
///
/// # Example
/// ```ignore
/// let users = map_rows(rows, |row| {
///     Ok((
///         get_column(&row, "user_name", "list_users")?,
///         get_column(&row, "email", "list_users")?,
///     ))
/// }, "list_users")?;
/// ```
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

/// Execute an LWT statement and return whether it was applied.
///
/// Parses the `[applied]` column from the Cassandra LWT response.
/// Use for `INSERT ... IF NOT EXISTS` and `UPDATE ... IF ...` statements.
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
