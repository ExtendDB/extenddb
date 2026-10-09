// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared `PostgreSQL` error classification helpers.

/// Check if a sqlx error is a unique constraint violation (PG error code 23505).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.code().as_deref() == Some("23505");
    }
    false
}

/// Check if a sqlx error is a foreign key violation (PG error code 23503).
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.code().as_deref() == Some("23503");
    }
    false
}

/// Check if a mapped error is `PostgreSQL` aborting the transaction to break a
/// lock conflict: `deadlock_detected` (40P01) or `serialization_failure` (40001).
/// Matches the `SQLSTATE` prefix that `data::index::db_error` writes.
pub(crate) fn is_conflict_abort(e: &extenddb_storage::error::StorageError) -> bool {
    match e {
        extenddb_storage::error::StorageError::Internal(msg) => {
            msg.starts_with("SQLSTATE 40P01:") || msg.starts_with("SQLSTATE 40001:")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::fmt;

    use extenddb_storage::error::StorageError;

    use super::is_conflict_abort;
    use crate::data::index::db_error;

    /// A database error with a chosen SQLSTATE, standing in for the server.
    #[derive(Debug)]
    struct FakeDbError(&'static str, &'static str);

    impl fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.1)
        }
    }

    impl std::error::Error for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            self.1
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
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
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn mapped(code: &'static str, msg: &'static str) -> StorageError {
        db_error(sqlx::Error::Database(Box::new(FakeDbError(code, msg))))
    }

    #[test]
    fn deadlock_and_serialization_failure_are_conflict_aborts() {
        assert!(is_conflict_abort(&mapped("40P01", "deadlock detected")));
        assert!(is_conflict_abort(&mapped(
            "40001",
            "could not serialize access due to concurrent update"
        )));
    }

    #[test]
    fn other_errors_are_not_conflict_aborts() {
        // A unique violation, a lock timeout, and the same text without the
        // code all stay internal errors.
        assert!(!is_conflict_abort(&mapped("23505", "duplicate key value")));
        assert!(!is_conflict_abort(&mapped(
            "55P03",
            "could not obtain lock"
        )));
        assert!(!is_conflict_abort(&StorageError::Internal(
            "deadlock detected".to_owned()
        )));
        assert!(!is_conflict_abort(&db_error(sqlx::Error::PoolTimedOut)));
        assert!(!is_conflict_abort(&StorageError::TransactionConflict(
            "SQLSTATE 40P01: deadlock detected".to_owned()
        )));
    }
}
