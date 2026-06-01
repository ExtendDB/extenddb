// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared TiDB connection and error helpers.

use std::future::Future;
use std::time::Duration;

use extenddb_storage::error::StorageError;

const MAX_TIDB_OPERATION_RETRIES: usize = 3;
const TIDB_CONNECTION_MAX_LIFETIME: Duration = Duration::from_secs(1800);
pub(crate) const TIDB_REPLICA_READ_CLOSEST_ADAPTIVE: &str = "closest-adaptive";

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
    tidb_pool_options_with_replica_read(max_connections, min_connections, None)
}

/// Build a TiDB pool configuration for read-only data-plane traffic.
///
/// `tidb_replica_read = 'closest-adaptive'` lets TiDB route larger read-only
/// statements to local replicas while keeping strong consistency through
/// follower read. Point reads stay on leaders when TiDB estimates that follower
/// read would add latency.
pub(crate) fn tidb_default_read_pool_options(
    max_connections: u32,
    min_connections: u32,
) -> sqlx::mysql::MySqlPoolOptions {
    tidb_pool_options_with_replica_read(
        max_connections,
        min_connections,
        Some(TIDB_REPLICA_READ_CLOSEST_ADAPTIVE),
    )
}

fn tidb_pool_options_with_replica_read(
    max_connections: u32,
    min_connections: u32,
    replica_read: Option<&'static str>,
) -> sqlx::mysql::MySqlPoolOptions {
    sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .test_before_acquire(false)
        .max_lifetime(TIDB_CONNECTION_MAX_LIFETIME)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
                    .execute(&mut *conn)
                    .await?;
                if let Some(replica_read) = replica_read {
                    let sql = format!("SET SESSION tidb_replica_read = '{replica_read}'");
                    sqlx::query(&sql).execute(&mut *conn).await?;
                }
                Ok(())
            })
        })
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

    use super::{
        is_retryable_tidb_storage_error, is_table_exists_tidb_error_text,
        is_table_not_found_tidb_storage_error, retry_tidb_idempotent_operation,
    };

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
}
