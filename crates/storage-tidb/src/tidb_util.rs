// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared TiDB connection and error helpers.

use std::future::Future;
use std::time::Duration;

use extenddb_storage::error::StorageError;

const MAX_TIDB_OPERATION_RETRIES: usize = 3;
const TIDB_CONNECTION_MAX_LIFETIME: Duration = Duration::from_secs(1800);
const TIDB_RESOURCE_GROUP_NAME_MAX_CHARS: usize = 32;
pub(crate) const TIDB_REPLICA_READ_CLOSEST_ADAPTIVE: &str = "closest-adaptive";
const ACTIVE_TIDB_DDL_JOB_STATES: &[&str] = &[
    "none",
    "queueing",
    "running",
    "done",
    "rollingback",
    "cancelling",
    "pausing",
    "paused",
];

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct TidbActiveDdlJob {
    pub(crate) job_type: String,
    pub(crate) state: String,
}

#[derive(Clone, Default)]
struct TidbSessionInit {
    resource_group: Option<String>,
    replica_read: Option<&'static str>,
}

impl TidbSessionInit {
    fn new(
        resource_group: Option<&str>,
        replica_read: Option<&'static str>,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            resource_group: resource_group
                .map(quote_tidb_resource_group_name)
                .transpose()?,
            replica_read,
        })
    }

    fn init_statements(&self) -> Vec<String> {
        let mut statements = Vec::with_capacity(3);
        if let Some(resource_group) = &self.resource_group {
            statements.push(format!("SET RESOURCE GROUP {resource_group}"));
        }
        statements.push("SET SESSION tidb_txn_mode = 'pessimistic'".to_owned());
        if let Some(replica_read) = self.replica_read {
            statements.push(format!("SET SESSION tidb_replica_read = '{replica_read}'"));
        }
        statements
    }
}

/// Build a TiDB pool configuration with the session behavior ExtendDB relies on.
///
/// New TiDB clusters default to pessimistic transactions, but upgraded clusters
/// can retain the older optimistic mode. Set the session variable on every
/// pooled connection so `SELECT ... FOR UPDATE` and write transactions have the
/// same distributed locking semantics independent of cluster history.
pub(crate) fn tidb_pool_options(
    max_connections: u32,
    min_connections: u32,
) -> sqlx::mysql::MySqlPoolOptions {
    tidb_pool_options_with_session(max_connections, min_connections, TidbSessionInit::default())
}

/// Build a TiDB pool configuration bound to a Resource Control group.
pub(crate) fn tidb_pool_options_with_resource_group(
    max_connections: u32,
    min_connections: u32,
    resource_group: Option<&str>,
) -> Result<sqlx::mysql::MySqlPoolOptions, StorageError> {
    Ok(tidb_pool_options_with_session(
        max_connections,
        min_connections,
        TidbSessionInit::new(resource_group, None)?,
    ))
}

/// Build a read-only TiDB pool configuration.
///
/// `tidb_replica_read = 'closest-adaptive'` lets TiDB route larger read-only
/// statements to local replicas while keeping strong consistency through
/// follower read. Point reads stay on leaders when TiDB estimates that follower
/// read would add latency. When `resource_group` is set, every pooled session is
/// also bound to TiDB Resource Control before request traffic starts.
pub(crate) fn tidb_default_read_pool_options_with_resource_group(
    max_connections: u32,
    min_connections: u32,
    resource_group: Option<&str>,
) -> Result<sqlx::mysql::MySqlPoolOptions, StorageError> {
    Ok(tidb_pool_options_with_session(
        max_connections,
        min_connections,
        TidbSessionInit::new(resource_group, Some(TIDB_REPLICA_READ_CLOSEST_ADAPTIVE))?,
    ))
}

fn tidb_pool_options_with_session(
    max_connections: u32,
    min_connections: u32,
    session_init: TidbSessionInit,
) -> sqlx::mysql::MySqlPoolOptions {
    let init_statements = session_init.init_statements();
    sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .test_before_acquire(false)
        .max_lifetime(TIDB_CONNECTION_MAX_LIFETIME)
        .after_connect(move |conn, _meta| {
            let init_statements = init_statements.clone();
            Box::pin(async move {
                for sql in init_statements {
                    sqlx::query(&sql).execute(&mut *conn).await?;
                }
                Ok(())
            })
        })
}

fn quote_tidb_resource_group_name(value: &str) -> Result<String, StorageError> {
    if value.is_empty()
        || value.chars().count() > TIDB_RESOURCE_GROUP_NAME_MAX_CHARS
        || value.contains('`')
        || value.chars().any(char::is_control)
    {
        return Err(StorageError::Configuration(format!(
            "storage.tidb.resource_group must be a TiDB resource group identifier \
             between 1 and {TIDB_RESOURCE_GROUP_NAME_MAX_CHARS} characters with no backticks or control characters",
        )));
    }

    Ok(format!("`{value}`"))
}

/// Retry idempotent TiDB online DDL statements on transient schema/lock errors.
///
/// TiDB owns distributed DDL ordering and online backfill. ExtendDB only submits
/// idempotent `IF EXISTS` / `IF NOT EXISTS` DDL for the desired catalog state.
/// TiDB handles the already-exists/already-absent cases natively; this helper
/// only retries real transient conflicts such as schema-version races or lock
/// timeouts.
pub(crate) async fn execute_tidb_idempotent_ddl(
    pool: &sqlx::MySqlPool,
    operation: &'static str,
    sql: &str,
) -> Result<sqlx::mysql::MySqlQueryResult, StorageError> {
    let mut retries = 0;
    loop {
        match sqlx::query(sql).execute(pool).await {
            Ok(value) => return Ok(value),
            Err(error)
                if retries < MAX_TIDB_OPERATION_RETRIES && is_retryable_tidb_sqlx_error(&error) =>
            {
                retries += 1;
                let delay = transaction_retry_delay(retries);
                tracing::debug!(
                    operation,
                    retries,
                    delay_ms = delay.as_millis(),
                    "retrying TiDB idempotent DDL after retryable database error: {error}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(StorageError::Internal(error.to_string())),
        }
    }
}

/// Execute a physical table create and report whether this caller created it.
///
/// The clean path uses a plain `CREATE TABLE` statement with the complete
/// desired schema. A table-exists result is the native distributed race signal:
/// another frontend, an older server version, or a previous crashed attempt got
/// there first. Callers can then converge missing online-DDL artifacts with
/// `IF NOT EXISTS` DDL.
pub(crate) async fn execute_tidb_create_table_ddl(
    pool: &sqlx::MySqlPool,
    operation: &'static str,
    sql: &str,
) -> Result<bool, StorageError> {
    let mut retries = 0;
    loop {
        match sqlx::query(sql).execute(pool).await {
            Ok(_) => return Ok(true),
            Err(error) if is_table_exists_tidb_sqlx_error(&error) => return Ok(false),
            Err(error)
                if retries < MAX_TIDB_OPERATION_RETRIES && is_retryable_tidb_sqlx_error(&error) =>
            {
                retries += 1;
                let delay = transaction_retry_delay(retries);
                tracing::debug!(
                    operation,
                    retries,
                    delay_ms = delay.as_millis(),
                    "retrying TiDB CREATE TABLE after retryable database error: {error}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(StorageError::Internal(error.to_string())),
        }
    }
}

pub(crate) async fn retry_tidb_idempotent_operation<T, F, Fut>(
    operation: &'static str,
    mut op: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut retries = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error)
                if retries < MAX_TIDB_OPERATION_RETRIES
                    && is_retryable_tidb_storage_error(&error) =>
            {
                retries += 1;
                let delay = transaction_retry_delay(retries);
                tracing::debug!(
                    operation,
                    retries,
                    delay_ms = delay.as_millis(),
                    "retrying TiDB idempotent operation after retryable error: {error}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn active_tidb_ddl_job_for_table_sql() -> String {
    let states = ACTIVE_TIDB_DDL_JOB_STATES
        .iter()
        .map(|state| format!("'{state}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT JOB_TYPE, STATE \
         FROM information_schema.ddl_jobs \
         WHERE DB_NAME = DATABASE() \
           AND TABLE_NAME = ? \
           AND END_TIME IS NULL \
           AND LOWER(STATE) IN ({states}) \
         ORDER BY CREATE_TIME DESC, JOB_ID DESC \
         LIMIT 1"
    )
}

pub(crate) async fn active_tidb_ddl_job_for_table(
    pool: &sqlx::MySqlPool,
    physical_table_name: &str,
) -> Result<Option<TidbActiveDdlJob>, StorageError> {
    let sql = active_tidb_ddl_job_for_table_sql();
    let row: Option<(String, String)> = sqlx::query_as(&sql)
        .bind(physical_table_name)
        .fetch_optional(pool)
        .await
        .map_err(|e| StorageError::Internal(format!("read TiDB DDL jobs: {e}")))?;

    Ok(row.map(|(job_type, state)| TidbActiveDdlJob { job_type, state }))
}

pub(crate) async fn defer_if_table_has_active_ddl_job(
    pool: &sqlx::MySqlPool,
    operation: &'static str,
    physical_table_name: &str,
) -> Result<bool, StorageError> {
    let Some(job) = active_tidb_ddl_job_for_table(pool, physical_table_name).await? else {
        return Ok(false);
    };

    tracing::debug!(
        operation,
        physical_table_name,
        job_type = job.job_type,
        state = job.state,
        "deferring TiDB control-plane reconciliation while native DDL job is active"
    );
    Ok(true)
}

pub(crate) async fn current_tidb_tso(pool: &sqlx::MySqlPool) -> Result<i64, StorageError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

    let tso = current_tidb_transaction_tso(&mut tx).await?;

    tx.rollback()
        .await
        .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

    Ok(tso)
}

pub(crate) fn tidb_as_of_tso_clause(tso: i64) -> Result<String, StorageError> {
    if tso <= 0 {
        return Err(StorageError::Internal(format!(
            "invalid TiDB snapshot TSO: {tso}"
        )));
    }
    Ok(format!("AS OF TIMESTAMP TIDB_PARSE_TSO({tso})"))
}

pub(crate) fn tidb_as_of_epoch_clause(epoch_seconds: f64) -> Result<String, StorageError> {
    if !epoch_seconds.is_finite() || epoch_seconds < 0.0 {
        return Err(StorageError::Validation(
            "ExportTime must be a non-negative finite epoch timestamp".to_owned(),
        ));
    }
    Ok(format!("AS OF TIMESTAMP FROM_UNIXTIME({epoch_seconds:.6})"))
}

pub(crate) fn map_tidb_snapshot_read_sqlx_error(error: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(db_error) = &error {
        let message = db_error.message();
        if is_tidb_snapshot_read_error_text(message) {
            return StorageError::Validation(format!(
                "ExportTime is outside TiDB's historical read window: {message}"
            ));
        }
    }
    StorageError::Internal(error.to_string())
}

pub(crate) async fn current_tidb_transaction_tso(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<i64, StorageError> {
    // TiDB assigns `@@tidb_current_ts` to an active transaction. Reading TSO
    // helper functions outside a transaction can yield zero, which is not a
    // usable MVCC or BR snapshot timestamp.
    sqlx::query_scalar("SELECT @@tidb_current_ts")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(format!("Database error: {e}")))
}

fn transaction_retry_delay(retries: usize) -> Duration {
    let shift = u32::try_from(retries.saturating_sub(1)).unwrap_or(0);
    Duration::from_millis(10 * 2_u64.saturating_pow(shift))
}

fn is_retryable_tidb_storage_error(error: &StorageError) -> bool {
    match error {
        StorageError::Internal(message) => is_retryable_tidb_error_text(message),
        _ => false,
    }
}

fn is_retryable_tidb_sqlx_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };

    db_error
        .code()
        .is_some_and(|code| is_retryable_tidb_error_code(code.as_ref()))
        || is_retryable_tidb_error_text(db_error.message())
}

fn is_table_exists_tidb_sqlx_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };

    db_error.code().is_some_and(|code| code.as_ref() == "42S01")
        || is_table_exists_tidb_error_text(db_error.message())
}

pub(crate) fn is_table_not_found_tidb_sqlx_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };

    db_error.code().is_some_and(|code| code.as_ref() == "42S02")
        || is_table_not_found_tidb_error_text(db_error.message())
}

pub(crate) fn is_table_not_found_tidb_storage_error(error: &StorageError) -> bool {
    match error {
        StorageError::Internal(message) => is_table_not_found_tidb_error_text(message),
        _ => false,
    }
}

fn is_retryable_tidb_error_text(message: &str) -> bool {
    RETRYABLE_TIDB_ERROR_CODES
        .iter()
        .any(|code| message_contains_db_error_code(message, code))
        || message.contains("Information schema is changed")
        || message.contains("Write conflict")
        || message.contains("Resolve Lock Timeout")
        || message.contains("Lock wait timeout")
        || message.contains("Deadlock")
}

fn is_table_exists_tidb_error_text(message: &str) -> bool {
    message_contains_db_error_code(message, "1050")
        || (message.contains("Table") && message.contains("already exists"))
}

fn is_table_not_found_tidb_error_text(message: &str) -> bool {
    message_contains_db_error_code(message, "1146")
        || (message.contains("Table") && message.contains("doesn't exist"))
        || (message.contains("Table") && message.contains("does not exist"))
}

fn is_tidb_snapshot_read_error_text(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("invalid as of timestamp")
        || lower.contains("cannot set read timestamp to a future time")
        || lower.contains("gc safe point")
        || lower.contains("gc safepoint")
}

const RETRYABLE_TIDB_ERROR_CODES: &[&str] = &[
    "8028", // Schema changed during the transaction.
    "9004", // Resolve lock timeout.
    "9007", // Write conflict.
    "1205", // MySQL-compatible lock wait timeout.
    "1213", // MySQL-compatible deadlock.
];

fn is_retryable_tidb_error_code(code: &str) -> bool {
    RETRYABLE_TIDB_ERROR_CODES.contains(&code)
}

fn message_contains_db_error_code(message: &str, code: &str) -> bool {
    message.match_indices(code).any(|(idx, _)| {
        let before_is_digit = message[..idx]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_digit());
        let after_is_digit = message[idx + code.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit());
        !before_is_digit && !after_is_digit
    })
}

/// Check if a sqlx error is a unique constraint violation (MySQL/TiDB code 1062).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::UniqueViolation;
    }
    false
}

/// Check if a sqlx error is a foreign key violation (MySQL/TiDB code 1451/1452).
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::ForeignKeyViolation;
    }
    false
}

#[cfg(test)]
mod tests {
    use extenddb_storage::error::StorageError;
    use sqlx::error::DatabaseError;

    use super::{
        TIDB_REPLICA_READ_CLOSEST_ADAPTIVE, TidbSessionInit, active_tidb_ddl_job_for_table_sql,
        is_retryable_tidb_storage_error, is_table_exists_tidb_error_text,
        is_table_not_found_tidb_sqlx_error, is_table_not_found_tidb_storage_error,
        is_tidb_snapshot_read_error_text, quote_tidb_resource_group_name,
        retry_tidb_idempotent_operation, tidb_as_of_epoch_clause, tidb_as_of_tso_clause,
    };

    #[derive(Debug)]
    struct StubDbError {
        code: Option<&'static str>,
        message: &'static str,
    }

    impl std::fmt::Display for StubDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for StubDbError {}

    impl DatabaseError for StubDbError {
        fn message(&self) -> &str {
            self.message
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code.map(std::borrow::Cow::Borrowed)
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
    }

    #[test]
    fn retry_classifier_accepts_tidb_online_ddl_conflicts() {
        for message in [
            "ERROR 8028 (HY000): Information schema is changed. [try again later]",
            "ERROR 9007 (HY000): Write conflict",
            "ERROR 1213 (40001): Deadlock found when trying to get lock",
            "ERROR 1205 (HY000): Lock wait timeout exceeded",
            "ERROR 9004 (HY000): Resolve Lock Timeout",
        ] {
            assert!(is_retryable_tidb_storage_error(&StorageError::Internal(
                message.to_owned()
            )));
        }
    }

    #[test]
    fn retry_classifier_rejects_non_ddl_outcomes() {
        for error in [
            StorageError::ConditionFailed(None),
            StorageError::Validation("bad request".to_owned()),
            StorageError::Connection("lost connection during commit".to_owned()),
            StorageError::Internal("Duplicate entry for key".to_owned()),
        ] {
            assert!(!is_retryable_tidb_storage_error(&error));
        }
    }

    #[test]
    fn table_exists_classifier_accepts_tidb_create_table_races() {
        for message in [
            "ERROR 1050 (42S01): Table '_ddb_tableid' already exists",
            "Table 'extenddb_data._ddb_tableid' already exists",
        ] {
            assert!(is_table_exists_tidb_error_text(message));
        }
    }

    #[test]
    fn table_exists_classifier_rejects_other_duplicate_errors() {
        assert!(!is_table_exists_tidb_error_text(
            "ERROR 1062 (23000): Duplicate entry for key 'PRIMARY'"
        ));
    }

    #[test]
    fn table_not_found_classifier_accepts_tidb_missing_table_errors() {
        for message in [
            "ERROR 1146 (42S02): Table 'extenddb_data._ddb_tableid' doesn't exist",
            "Table 'extenddb_data._ddb_tableid' does not exist",
        ] {
            assert!(is_table_not_found_tidb_storage_error(
                &StorageError::Internal(message.to_owned())
            ));
        }
    }

    #[test]
    fn table_not_found_classifier_rejects_unrelated_errors() {
        assert!(!is_table_not_found_tidb_storage_error(
            &StorageError::Internal("Unknown column 'pk' in 'field list'".to_owned())
        ));
    }

    #[test]
    fn table_not_found_sqlx_classifier_accepts_tidb_error_code() {
        let error = sqlx::Error::Database(Box::new(StubDbError {
            code: Some("42S02"),
            message: "Table 'extenddb_catalog.settings' doesn't exist",
        }));

        assert!(is_table_not_found_tidb_sqlx_error(&error));
    }

    #[tokio::test]
    async fn idempotent_ddl_retry_reenters_whole_action() {
        let mut attempts = 0;
        let result = retry_tidb_idempotent_operation("test_idempotent_operation_retry", || {
            attempts += 1;
            let attempt = attempts;
            async move {
                if attempt == 1 {
                    Err(StorageError::Internal(
                        "ERROR 8028 (HY000): Information schema is changed. [try again later]"
                            .to_owned(),
                    ))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
    }

    #[tokio::test]
    async fn idempotent_operation_retry_rejects_condition_failures() {
        let mut attempts = 0;
        let result = retry_tidb_idempotent_operation("test_no_retry_condition_failure", || {
            attempts += 1;
            async move { Err::<(), _>(StorageError::ConditionFailed(None)) }
        })
        .await;

        assert!(matches!(result, Err(StorageError::ConditionFailed(None))));
        assert_eq!(attempts, 1);
    }

    #[test]
    fn active_ddl_job_query_uses_tidb_native_job_queue() {
        let sql = active_tidb_ddl_job_for_table_sql();

        assert!(sql.contains("information_schema.ddl_jobs"));
        assert!(sql.contains("DB_NAME = DATABASE()"));
        assert!(sql.contains("TABLE_NAME = ?"));
        assert!(sql.contains("END_TIME IS NULL"));
        assert!(sql.contains("LOWER(STATE) IN"));
        assert!(sql.contains("'queueing'"));
        assert!(sql.contains("'running'"));
        assert!(sql.contains("'done'"));
        assert!(sql.contains("ORDER BY CREATE_TIME DESC, JOB_ID DESC"));
    }

    #[test]
    fn session_init_binds_resource_group_before_other_session_settings() {
        let init = TidbSessionInit::new(
            Some("extenddb-api"),
            Some(TIDB_REPLICA_READ_CLOSEST_ADAPTIVE),
        )
        .expect("resource group should validate");

        assert_eq!(
            init.init_statements(),
            vec![
                "SET RESOURCE GROUP `extenddb-api`",
                "SET SESSION tidb_txn_mode = 'pessimistic'",
                "SET SESSION tidb_replica_read = 'closest-adaptive'",
            ]
        );
    }

    #[test]
    fn as_of_tso_clause_uses_numeric_literal() {
        assert_eq!(
            tidb_as_of_tso_clause(466_712_376_294_768_640).unwrap(),
            "AS OF TIMESTAMP TIDB_PARSE_TSO(466712376294768640)"
        );
    }

    #[test]
    fn as_of_tso_clause_rejects_invalid_tso() {
        assert!(matches!(
            tidb_as_of_tso_clause(0),
            Err(StorageError::Internal(_))
        ));
    }

    #[test]
    fn as_of_epoch_clause_uses_fixed_numeric_literal() {
        assert_eq!(
            tidb_as_of_epoch_clause(1_717_171_717.1234567).unwrap(),
            "AS OF TIMESTAMP FROM_UNIXTIME(1717171717.123457)"
        );
    }

    #[test]
    fn as_of_epoch_clause_rejects_non_finite_values() {
        for value in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(matches!(
                tidb_as_of_epoch_clause(value),
                Err(StorageError::Validation(_))
            ));
        }
    }

    #[test]
    fn snapshot_read_error_classifier_accepts_tidb_as_of_failures() {
        for message in [
            "invalid as of timestamp: as of timestamp cannot be NULL",
            "cannot set read timestamp to a future time, readTS: 2, currentTS: 1",
            "snapshot is older than GC safe point",
            "snapshot is older than GC safepoint",
        ] {
            assert!(is_tidb_snapshot_read_error_text(message), "{message}");
        }
    }

    #[test]
    fn snapshot_read_error_classifier_rejects_unrelated_errors() {
        assert!(!is_tidb_snapshot_read_error_text(
            "Table 'extenddb._ddb_missing' doesn't exist"
        ));
    }

    #[test]
    fn resource_group_identifier_rejects_sql_breakout() {
        for value in [
            "",
            "bad`name",
            "bad\nname",
            "abcdefghijklmnopqrstuvwxyz1234567",
        ] {
            assert!(quote_tidb_resource_group_name(value).is_err(), "{value}");
        }
    }
}
