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

// ══════════════════════════════════════════════════════════════════════════════
// Consistency policy (#770)
// ══════════════════════════════════════════════════════════════════════════════

/// The kind of a statement. [`params`] gives each kind its consistency level,
/// so this enum and [`params`] are the only place that sets the level of a
/// statement. [`batch`] does the same for batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stmt {
    /// A read: `LOCAL_QUORUM`.
    Read,
    /// A write without a condition: `LOCAL_QUORUM`.
    Write,
    /// A conditional write (LWT): Paxos at `LOCAL_SERIAL`, commit at
    /// `LOCAL_QUORUM`.
    Lwt,
    /// A read at `LOCAL_SERIAL`, which sees every committed Paxos round.
    SerialRead,
    /// `CREATE`, `ALTER`, `DROP`, `GRANT` or `REVOKE`: `ONE`. Cassandra does
    /// not use the client's consistency level for these statements.
    Ddl,
    /// A read of a node-local system table (`system.local`, `system.peers`,
    /// `system_schema`): `ONE`.
    SystemRead,
}

/// The statement parameters for a statement of `kind` with `values`.
#[must_use]
pub fn params(kind: Stmt, values: QueryValues) -> cdrs_tokio::statement::StatementParams {
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::statement::StatementParamsBuilder;

    let builder = StatementParamsBuilder::new().with_values(values);
    let builder = match kind {
        Stmt::Read | Stmt::Write => builder.with_consistency(Consistency::LocalQuorum),
        Stmt::Lwt => builder
            .with_consistency(Consistency::LocalQuorum)
            .with_serial_consistency(Consistency::LocalSerial),
        Stmt::SerialRead => builder.with_consistency(Consistency::LocalSerial),
        Stmt::Ddl | Stmt::SystemRead => builder.with_consistency(Consistency::One),
    };
    builder.build()
}

/// Run one statement at the consistency level of its kind. Every statement
/// outside this module and `migrations/` goes through [`PolicySession::run`]
/// or a helper that calls it.
pub trait PolicySession {
    /// Run `query` with `values` as a statement of `kind`, with the
    /// parameters [`params`] gives that kind. In a debug build, a statement
    /// whose CQL does not match `kind` ([`shape_error`]) panics.
    ///
    /// # Errors
    /// Returns the driver's error if the statement fails.
    fn run<Q: AsRef<str>>(
        &self,
        kind: Stmt,
        query: Q,
        values: QueryValues,
    ) -> impl std::future::Future<
        Output = Result<cdrs_tokio::frame::Envelope, cdrs_tokio::error::Error>,
    > + Send;

    /// Send `batch`. In a debug build, a batch whose levels are not the ones
    /// [`batch`] sets ([`batch_level_error`]) panics.
    ///
    /// # Errors
    /// Returns the driver's error if the batch fails.
    fn run_batch(
        &self,
        batch: cdrs_tokio::query::QueryBatch,
    ) -> impl std::future::Future<
        Output = Result<cdrs_tokio::frame::Envelope, cdrs_tokio::error::Error>,
    > + Send;
}

impl PolicySession for CassandraSession {
    fn run_batch(
        &self,
        batch: cdrs_tokio::query::QueryBatch,
    ) -> impl std::future::Future<
        Output = Result<cdrs_tokio::frame::Envelope, cdrs_tokio::error::Error>,
    > + Send {
        debug_assert!(
            batch_level_error(&batch).is_none(),
            "batch: {}",
            batch_level_error(&batch).unwrap_or_default()
        );
        self.batch(batch)
    }

    fn run<Q: AsRef<str>>(
        &self,
        kind: Stmt,
        query: Q,
        values: QueryValues,
    ) -> impl std::future::Future<
        Output = Result<cdrs_tokio::frame::Envelope, cdrs_tokio::error::Error>,
    > + Send {
        let query = query.as_ref().to_owned();
        debug_assert!(
            shape_error(kind, &query).is_none(),
            "{kind:?} statement: {}: {query}",
            shape_error(kind, &query).unwrap_or_default()
        );
        async move { self.query_with_params(query, params(kind, values)).await }
    }
}

/// What is wrong with `query` as a statement of `kind`, or `None` when its
/// CQL matches `kind`:
///
/// - [`Stmt::Read`] and [`Stmt::SerialRead`]: a `SELECT` that does not read
///   `system.*` or `system_schema.*`.
/// - [`Stmt::SystemRead`]: a `SELECT` from `system.*` or `system_schema.*`.
/// - [`Stmt::Write`]: an `INSERT`, `UPDATE`, `DELETE` or `BEGIN BATCH` with
///   no `IF` condition.
/// - [`Stmt::Lwt`]: an `INSERT`, `UPDATE` or `DELETE` with an `IF` condition.
/// - [`Stmt::Ddl`]: a `CREATE`, `ALTER`, `DROP`, `GRANT` or `REVOKE`.
///
/// Quoted literals and identifiers are removed before the check, so an `IF`
/// inside a string does not count.
#[must_use]
pub fn shape_error(kind: Stmt, query: &str) -> Option<&'static str> {
    let words = cql_words(query);
    let first = words.first().map_or("", String::as_str);
    let conditional = words.iter().any(|word| word == "IF");
    // Each node keeps its own copy of these keyspaces (LocalStrategy).
    let system = words
        .iter()
        .skip_while(|word| *word != "FROM")
        .nth(1)
        .is_some_and(|table| table.starts_with("SYSTEM.") || table.starts_with("SYSTEM_SCHEMA."));
    match kind {
        Stmt::Read | Stmt::SerialRead => {
            if first != "SELECT" {
                Some("a read must be a SELECT")
            } else if system {
                Some("a read of system.* or system_schema.* must use Stmt::SystemRead")
            } else {
                None
            }
        }
        Stmt::SystemRead => {
            if first != "SELECT" {
                Some("a system read must be a SELECT")
            } else if system {
                None
            } else {
                Some("a system read must read system.* or system_schema.*")
            }
        }
        Stmt::Write => {
            if !matches!(first, "INSERT" | "UPDATE" | "DELETE" | "BEGIN") {
                Some("a write must be an INSERT, UPDATE, DELETE or BEGIN BATCH")
            } else if conditional {
                Some("a write with an IF condition must use Stmt::Lwt")
            } else {
                None
            }
        }
        Stmt::Lwt => {
            if !matches!(first, "INSERT" | "UPDATE" | "DELETE") {
                Some("an LWT must be an INSERT, UPDATE or DELETE")
            } else if conditional {
                None
            } else {
                Some("an LWT must have an IF condition")
            }
        }
        Stmt::Ddl => (!matches!(first, "CREATE" | "ALTER" | "DROP" | "GRANT" | "REVOKE"))
            .then_some("DDL must be a CREATE, ALTER, DROP, GRANT or REVOKE"),
    }
}

/// What is wrong with the levels of `batch`, or `None` when they are the
/// ones [`batch`] sets: `LOCAL_QUORUM`, with `LOCAL_SERIAL` for a
/// conditional batch.
#[must_use]
pub fn batch_level_error(batch: &cdrs_tokio::query::QueryBatch) -> Option<&'static str> {
    use cdrs_tokio::consistency::Consistency;

    if batch.request.consistency != Consistency::LocalQuorum {
        Some("a batch must run at LOCAL_QUORUM; build it with cassandra_util::batch()")
    } else if batch.request.serial_consistency != Some(Consistency::LocalSerial) {
        Some("a batch must have LOCAL_SERIAL; build it with cassandra_util::batch()")
    } else {
        None
    }
}

/// The words of `query` in upper case, with quoted literals and quoted
/// identifiers removed. A word holds letters, digits, `_` and `.`.
fn cql_words(query: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    for c in query.chars() {
        if let Some(open) = quote {
            if c == open {
                quote = None;
            }
            continue;
        }
        if c == '\'' || c == '"' {
            quote = Some(c);
        } else if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
            word.push(c.to_ascii_uppercase());
            continue;
        }
        if !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

/// A batch builder at `LOCAL_QUORUM`, with `LOCAL_SERIAL` for a conditional
/// batch. Use it in place of `BatchQueryBuilder::new()`.
#[must_use]
pub fn batch() -> cdrs_tokio::query::BatchQueryBuilder {
    use cdrs_tokio::consistency::Consistency;

    cdrs_tokio::query::BatchQueryBuilder::new()
        .with_consistency(Consistency::LocalQuorum)
        .with_serial_consistency(Consistency::LocalSerial)
}

/// Run a `CREATE`, `ALTER` or `DROP` statement.
///
/// # Errors
/// Returns an error if the Cassandra statement fails.
pub async fn ddl<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    context: &str,
) -> Result<(), E> {
    session
        .run(Stmt::Ddl, query, QueryValues::SimpleValues(vec![]))
        .await
        .map_err(|e| {
            tracing::error!("{context} DDL failed: {e}");
            E::db_error(format!("{context}: {e}"))
        })?;
    Ok(())
}

/// Read rows from a node-local system table (`system.local`,
/// `system.peers`, `system_schema`).
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn system_read<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    rows_at(session, query, Stmt::SystemRead, values, context).await
}

/// Run `query` with `values` as a statement of `kind` and return its rows.
async fn rows_at<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    kind: Stmt,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    let result = session.run(kind, query, values).await.map_err(|e| {
        tracing::error!("{context} query failed: {e}");
        E::db_error(format!("{context}: {e}"))
    })?;

    let body = result.response_body().map_err(|e| {
        tracing::error!("{context} response_body failed: {e}");
        E::db_error(format!("{context} response_body: {e}"))
    })?;

    Ok(body.into_rows().unwrap_or_default())
}

/// Execute a lightweight transaction: Paxos at `LOCAL_SERIAL`, commit at
/// `LOCAL_QUORUM`. Returns the response, for callers that read more than
/// the `[applied]` flag.
pub async fn query_lwt(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
) -> Result<cdrs_tokio::frame::Envelope, extenddb_storage::error::StorageError> {
    session
        .run(Stmt::Lwt, query, values)
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

/// Execute a query at `LOCAL_QUORUM` and return all rows.
///
/// # Errors
/// Returns an error if the Cassandra query fails or the response cannot be parsed.
pub async fn query_rows<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    rows_at(session, query, Stmt::Read, values, context).await
}

/// Execute a query at `LOCAL_SERIAL` and return all rows: the read sees
/// every Paxos round that committed, so a row an LWT wrote (a claim taken,
/// a heartbeat refreshed) is read as written. For the reads whose answer
/// decides whether another process's claim is live.
pub async fn query_rows_serial<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<Vec<Row>, E> {
    rows_at(session, query, Stmt::SerialRead, values, context).await
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

/// Execute a non-query statement (INSERT/UPDATE/DELETE) at `LOCAL_QUORUM`.
///
/// # Errors
/// Returns an error if the Cassandra statement fails.
pub async fn execute<E: FromDbError>(
    session: &Arc<CassandraSession>,
    query: &str,
    values: QueryValues,
    context: &str,
) -> Result<(), E> {
    session.run(Stmt::Write, query, values).await.map_err(|e| {
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

/// Execute a lightweight transaction (Paxos at `LOCAL_SERIAL`, commit at
/// `LOCAL_QUORUM`) and return whether it applied.
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
    let result = session.run(Stmt::Lwt, query, values).await.map_err(|e| {
        tracing::error!("{context} lwt failed: {e}");
        E::db_error("Database error".to_owned())
    })?;
    lwt_applied_flag(&result, context)
}

/// The `[applied]` flag of an LWT response, `false` when the response has no row.
fn lwt_applied_flag<E: FromDbError>(
    result: &cdrs_tokio::frame::Envelope,
    context: &str,
) -> Result<bool, E> {
    use cdrs_tokio::types::IntoRustByName;

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
    use super::{Stmt, batch, batch_level_error, params, shape_error};

    fn one_insert(builder: cdrs_tokio::query::BatchQueryBuilder) -> cdrs_tokio::query::QueryBatch {
        builder
            .add_query(
                "INSERT INTO ks.t (id) VALUES (1)",
                QueryValues::SimpleValues(vec![]),
            )
            .build()
            .expect("a batch")
    }

    #[test]
    fn only_a_batch_from_batch_has_the_policy_levels() {
        assert_eq!(batch_level_error(&one_insert(batch())), None);
        assert!(
            batch_level_error(&one_insert(cdrs_tokio::query::BatchQueryBuilder::new())).is_some()
        );
        assert!(batch_level_error(&one_insert(Default::default())).is_some());
        assert!(
            batch_level_error(&one_insert(
                cdrs_tokio::query::BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
            ))
            .is_some(),
            "LOCAL_QUORUM without LOCAL_SERIAL"
        );
    }

    #[test]
    fn a_read_must_be_a_select() {
        assert_eq!(
            shape_error(Stmt::Read, "SELECT a FROM ks.t WHERE pk = ?"),
            None
        );
        assert_eq!(shape_error(Stmt::SerialRead, "select a from ks.t"), None);
        assert_eq!(
            shape_error(
                Stmt::Read,
                "SELECT role FROM system_auth.roles WHERE role = ?"
            ),
            None,
            "system_auth is replicated, so it is not a SystemRead"
        );
        assert!(shape_error(Stmt::Read, "SELECT token(?) AS t FROM system.local").is_some());
        assert!(
            shape_error(
                Stmt::SerialRead,
                "SELECT table_name FROM system_schema.tables"
            )
            .is_some()
        );
        assert!(shape_error(Stmt::Read, "UPDATE ks.t SET a = 1 WHERE pk = ?").is_some());
        assert!(shape_error(Stmt::SerialRead, "DELETE FROM ks.t WHERE pk = ?").is_some());
    }

    #[test]
    fn a_system_read_must_read_a_system_table() {
        assert_eq!(
            shape_error(
                Stmt::SystemRead,
                "SELECT keyspace_name FROM system_schema.keyspaces"
            ),
            None
        );
        assert_eq!(
            shape_error(Stmt::SystemRead, "SELECT data_center FROM system.local"),
            None
        );
        assert!(shape_error(Stmt::SystemRead, "SELECT role FROM system_auth.roles").is_some());
        assert!(shape_error(Stmt::SystemRead, "SELECT a FROM ks.t").is_some());
        assert!(shape_error(Stmt::SystemRead, "DELETE FROM system.local").is_some());
    }

    #[test]
    fn a_write_has_no_condition() {
        assert_eq!(
            shape_error(Stmt::Write, "INSERT INTO ks.t (a) VALUES (?)"),
            None
        );
        assert_eq!(
            shape_error(Stmt::Write, "UPDATE ks.t SET a = ? WHERE pk = ?"),
            None
        );
        assert_eq!(
            shape_error(Stmt::Write, "DELETE FROM ks.t WHERE pk = ?"),
            None
        );
        assert_eq!(
            shape_error(
                Stmt::Write,
                "BEGIN BATCH\nINSERT INTO ks.t (a) VALUES (1)\nAPPLY BATCH"
            ),
            None
        );
        assert!(
            shape_error(Stmt::Write, "INSERT INTO ks.t (a) VALUES (?) IF NOT EXISTS").is_some()
        );
        assert!(shape_error(Stmt::Write, "UPDATE ks.t SET a = ? WHERE pk = ? IF a = ?").is_some());
        assert!(shape_error(Stmt::Write, "DELETE FROM ks.t WHERE pk = ? IF EXISTS").is_some());
        assert!(shape_error(Stmt::Write, "SELECT a FROM ks.t").is_some());
    }

    #[test]
    fn an_lwt_has_a_condition() {
        assert_eq!(
            shape_error(Stmt::Lwt, "INSERT INTO ks.t (a) VALUES (?) IF NOT EXISTS"),
            None
        );
        assert_eq!(
            shape_error(Stmt::Lwt, "UPDATE ks.t SET a = ? WHERE pk = ? IF a = null"),
            None
        );
        assert!(shape_error(Stmt::Lwt, "UPDATE ks.t SET a = ? WHERE pk = ?").is_some());
        assert!(shape_error(Stmt::Lwt, "SELECT a FROM ks.t").is_some());
    }

    #[test]
    fn ddl_is_create_alter_drop_or_a_role_statement() {
        for cql in [
            "CREATE TABLE IF NOT EXISTS ks.t (a int PRIMARY KEY)",
            "ALTER TABLE ks.t ADD b int",
            "DROP KEYSPACE IF EXISTS ks",
            "CREATE ROLE IF NOT EXISTS 'app' WITH PASSWORD = 'x' AND LOGIN = true",
            "GRANT 'app' TO 'admin'",
            "REVOKE 'app' FROM 'admin'",
        ] {
            assert_eq!(shape_error(Stmt::Ddl, cql), None, "{cql}");
        }
        assert!(shape_error(Stmt::Ddl, "INSERT INTO ks.t (a) VALUES (1)").is_some());
    }

    #[test]
    fn a_quoted_if_is_not_a_condition() {
        assert_eq!(
            shape_error(Stmt::Write, "INSERT INTO ks.t (a) VALUES ('IF x = 1')"),
            None
        );
        assert_eq!(
            shape_error(
                Stmt::Write,
                "UPDATE ks.t SET \"IF\" = 'it''s IF' WHERE pk = ?"
            ),
            None
        );
        assert!(shape_error(Stmt::Lwt, "INSERT INTO ks.t (a) VALUES ('IF')").is_some());
    }
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::query::QueryValues;

    fn levels(kind: Stmt) -> (Consistency, Option<Consistency>) {
        let built = params(kind, QueryValues::SimpleValues(vec![]));
        (
            built.query_params.consistency,
            built.query_params.serial_consistency,
        )
    }

    #[test]
    fn reads_and_writes_run_at_local_quorum() {
        assert_eq!(levels(Stmt::Read), (Consistency::LocalQuorum, None));
        assert_eq!(levels(Stmt::Write), (Consistency::LocalQuorum, None));
    }

    #[test]
    fn an_lwt_runs_paxos_at_local_serial_and_commits_at_local_quorum() {
        assert_eq!(
            levels(Stmt::Lwt),
            (Consistency::LocalQuorum, Some(Consistency::LocalSerial))
        );
    }

    #[test]
    fn a_serial_read_runs_at_local_serial() {
        assert_eq!(levels(Stmt::SerialRead), (Consistency::LocalSerial, None));
    }

    #[test]
    fn ddl_and_system_reads_run_at_one() {
        assert_eq!(levels(Stmt::Ddl), (Consistency::One, None));
        assert_eq!(levels(Stmt::SystemRead), (Consistency::One, None));
    }

    #[test]
    fn params_keep_the_values() {
        let values = QueryValues::SimpleValues(vec![1_i32.into()]);
        let built = params(Stmt::Write, values.clone());
        assert_eq!(built.query_params.values, Some(values));
    }

    #[test]
    fn a_batch_runs_at_local_quorum_and_local_serial() {
        let built = batch()
            .add_query(
                "INSERT INTO ks.t (id) VALUES (1)",
                QueryValues::SimpleValues(vec![]),
            )
            .build()
            .expect("a batch");
        assert_eq!(built.request.consistency, Consistency::LocalQuorum);
        assert_eq!(
            built.request.serial_consistency,
            Some(Consistency::LocalSerial)
        );
    }
}
