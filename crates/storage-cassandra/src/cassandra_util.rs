// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra-specific utility functions.

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::{StatementParams, StatementParamsBuilder};
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::rows::Row;
use std::sync::Arc;

pub type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

// ══════════════════════════════════════════════════════════════════════════════
// Statement consistency for the data path
// ══════════════════════════════════════════════════════════════════════════════
//
// Every statement under `data/` states its level through `read_params`,
// `write_params` or `lwt_params`; schema statements in `data/ddl.rs` are the
// one exception. `clippy.toml` disallows the driver calls that run at the
// driver default, `Consistency::One`, and `data/mod.rs` turns that lint on for
// the data path.

/// The level a data-path read runs at for a request's `ConsistentRead`.
///
/// Base-row writes and lightweight transactions are acknowledged at
/// `LOCAL_QUORUM` ([`write_params`], [`lwt_params`]). A `LOCAL_QUORUM` read
/// intersects that write set within the datacenter, because a quorum plus a
/// quorum exceeds the replication factor, so a `true` read reflects every
/// write acknowledged before it started. `LOCAL_ONE` reads one replica and
/// may miss recently acknowledged writes, which is the contract for `false`.
#[must_use]
pub const fn read_consistency(consistent_read: bool) -> Consistency {
    if consistent_read {
        Consistency::LocalQuorum
    } else {
        Consistency::LocalOne
    }
}

/// Parameters for a data-path read at [`read_consistency`].
#[must_use]
pub fn read_params(consistent_read: bool, values: QueryValues) -> StatementParams {
    StatementParamsBuilder::new()
        .with_consistency(read_consistency(consistent_read))
        .with_values(values)
        .build()
}

/// Parameters for a data-path write with no `IF` clause, acknowledged at
/// `LOCAL_QUORUM`.
#[must_use]
pub fn write_params(values: QueryValues) -> StatementParams {
    StatementParamsBuilder::new()
        .with_consistency(Consistency::LocalQuorum)
        .with_values(values)
        .build()
}

/// A batch of plain writes, acknowledged at `LOCAL_QUORUM`. `Session::batch`
/// sends the builder's own consistency, and `BatchQueryBuilder::new()` starts
/// at the driver default, so every batch of the data path is built here.
#[must_use]
#[allow(clippy::disallowed_methods)] // the one place a batch builder is created
pub fn batch_builder() -> cdrs_tokio::query::BatchQueryBuilder {
    cdrs_tokio::query::BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum)
}

/// Parameters for a lightweight transaction (a statement with an `IF`
/// clause): the Paxos round runs at `LOCAL_SERIAL` and the commit is
/// acknowledged at `LOCAL_QUORUM`.
#[must_use]
pub fn lwt_params(values: QueryValues) -> StatementParams {
    StatementParamsBuilder::new()
        .with_consistency(Consistency::LocalQuorum)
        .with_serial_consistency(Consistency::LocalSerial)
        .with_values(values)
        .build()
}

/// Execute one statement with parameters from [`read_params`],
/// [`write_params`] or [`lwt_params`] and return the response envelope, from
/// which a lightweight transaction's `[applied]` column is read with
/// [`lwt_applied`].
///
/// # Errors
/// Returns an error if Cassandra does not execute the statement at the stated
/// level.
pub async fn execute_at<E: FromDbError>(
    session: &CassandraSession,
    query: &str,
    params: StatementParams,
    context: &str,
) -> Result<Envelope, E> {
    session
        .query_with_params(query, params)
        .await
        .map_err(|e| E::db_error(format!("{context}: {e}")))
}

/// Execute a data-path read at [`read_consistency`] and return all rows.
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn query_rows_at<E: FromDbError>(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
    consistent_read: bool,
    context: &str,
) -> Result<Vec<Row>, E> {
    let result = session
        .query_with_params(query, read_params(consistent_read, values))
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

/// [`query_rows_at`], returning the first row, if any.
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn query_optional_at<E: FromDbError>(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
    consistent_read: bool,
    context: &str,
) -> Result<Option<Row>, E> {
    let rows = query_rows_at(session, query, values, consistent_read, context).await?;
    Ok(rows.into_iter().next())
}

/// Execute a lightweight transaction at [`lwt_params`]: a statement with an
/// `IF` clause runs with these parameters, through this helper or through
/// `query_with_params` and the `data/mod.rs` helpers.
pub async fn query_lwt(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
) -> Result<Envelope, extenddb_storage::error::StorageError> {
    session
        .query_with_params(query, lwt_params(values))
        .await
        .map_err(|error| {
            extenddb_storage::error::StorageError::Internal(format!("LWT query failed: {error}"))
        })
}

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

/// Execute a query at the driver's default consistency, `ONE`, and return all
/// rows with standardized error handling.
///
/// For catalog, metadata and backup reads outside the data path. A read under
/// `data/` states its level with [`query_rows_at`] instead.
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

/// Execute a query at `LOCAL_QUORUM` and return all rows: [`query_rows_at`]
/// with `consistent_read` set.
///
/// [`query_rows`] uses the driver's default consistency, which is `ONE`. Use
/// this instead wherever an *empty* result is treated as authoritative and acted
/// on destructively: at `ONE` a replica that has not yet received a write reads
/// as empty, and a decision made on that read cannot be undone by a timestamp
/// guard, because the write it missed is older than the guard.
pub async fn query_rows_quorum<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    query_rows_at(session, query, values, true, context).await
}

/// Execute a query and return the first row, if any.
///
/// Similar to `sqlx::query().fetch_optional()`. Runs at the driver's default
/// consistency like [`query_rows`]; under `data/` use [`query_optional_at`].
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

/// Execute a non-query statement (INSERT/UPDATE/DELETE) at the driver's
/// default consistency; under `data/` use [`execute_quorum`].
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

/// Execute a non-query statement at [`write_params`] (`LOCAL_QUORUM`).
///
/// A plain write of the data path runs with [`write_params`]; this helper
/// runs one that returns no rows. A batch runs through [`batch_builder`].
///
/// # Errors
/// Returns an error if Cassandra does not acknowledge the statement at quorum.
pub async fn execute_quorum<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<(), E> {
    session
        .query_with_params(query, write_params(values))
        .await
        .map_err(|error| {
            tracing::error!("{context} quorum execute failed: {error}");
            E::db_error(format!("{context}: {error}"))
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
#[must_use]
pub fn millis_to_seconds_f64(timestamp_millis: i64) -> f64 {
    timestamp_millis as f64 / 1_000.0
}

/// Return the current time as milliseconds since Unix epoch as `i64`.
///
/// `SystemTime::as_millis()` returns `u128`; this cast is safe for all
/// timestamps within the range of `i64` (until year 292,277,026).
#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
/// Parse the `[applied]` boolean from an already-executed LWT response envelope.
///
/// Returns `Ok(true)` if the condition was met and the write applied,
/// `Ok(false)` if not applied (lost race). A missing result row is treated as
/// applied (defensive: Cassandra always returns a row for LWT statements).
///
/// # Errors
/// Returns an error only if the response body cannot be parsed.
pub fn lwt_applied(
    result: &cdrs_tokio::frame::Envelope,
) -> Result<bool, extenddb_storage::error::StorageError> {
    use cdrs_tokio::types::IntoRustByName as _;
    let body = result.response_body().map_err(|e| {
        extenddb_storage::error::StorageError::Internal(format!("lwt_applied response_body: {e}"))
    })?;
    let Some(rows) = body.into_rows() else {
        return Ok(true);
    };
    let Some(row) = rows.into_iter().next() else {
        return Ok(true);
    };
    row.get_r_by_name("[applied]").map_err(|e| {
        extenddb_storage::error::StorageError::Internal(format!("lwt_applied parse [applied]: {e}"))
    })
}

/// Execute a lightweight transaction at the driver's default consistency and
/// return whether it applied.
///
/// For catalog and management writes outside the data path; a conditional
/// write under `data/` runs with [`lwt_params`] instead, through
/// [`query_lwt`], [`execute_at`] or `query_with_params`.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bound() -> QueryValues {
        cdrs_tokio::query_values!("pk", 7_i64)
    }

    #[test]
    fn consistent_read_selects_local_quorum_and_eventual_selects_local_one() {
        assert_eq!(read_consistency(true), Consistency::LocalQuorum);
        assert_eq!(read_consistency(false), Consistency::LocalOne);
    }

    #[test]
    fn read_params_carry_the_read_level_and_no_serial_level() {
        for (consistent_read, level) in [
            (true, Consistency::LocalQuorum),
            (false, Consistency::LocalOne),
        ] {
            let params = read_params(consistent_read, bound()).query_params;
            assert_eq!(params.consistency, level);
            assert_eq!(params.serial_consistency, None);
            assert_eq!(params.values, Some(bound()));
        }
    }

    #[test]
    fn write_params_are_local_quorum_with_no_serial_level() {
        let params = write_params(bound()).query_params;
        assert_eq!(params.consistency, Consistency::LocalQuorum);
        assert_eq!(params.serial_consistency, None);
        assert_eq!(params.values, Some(bound()));
    }

    #[test]
    fn lwt_params_commit_at_local_quorum_and_run_paxos_at_local_serial() {
        let params = lwt_params(bound()).query_params;
        assert_eq!(params.consistency, Consistency::LocalQuorum);
        assert_eq!(params.serial_consistency, Some(Consistency::LocalSerial));
        assert_eq!(params.values, Some(bound()));
    }
}
