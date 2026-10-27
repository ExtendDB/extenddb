// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Classification of driver errors raised inside a write transaction.
//!
//! A write to a table with an index or a stream runs as one MongoDB
//! transaction: the item, its index entries, the stream sequence counter and
//! the stream record. MongoDB aborts a transaction that touches a document
//! another transaction has written with `WriteConflict` (code 112) and the
//! `TransientTransactionError` label. The write paths in `data_engine` re-run
//! the whole transaction with backoff when the conflict is raised on the data
//! collection. The helpers that write the index entries, draw the sequence
//! number and insert the stream record return `StorageError`, and this module
//! gives them one mapping that keeps the conflict visible to those retry
//! loops instead of folding it into `StorageError::Internal`, which answers
//! HTTP 500.

use extenddb_storage::error::StorageError;

use crate::data_engine::is_transient_write_conflict;

/// Map an error the driver raised inside a write transaction to a storage
/// error.
///
/// A transient write conflict becomes [`StorageError::Transient`]; the
/// transaction bodies in `data_engine` convert that into a re-run of the
/// transaction with the same backoff and budget as a conflict on the data
/// collection. Every other error is [`StorageError::Internal`].
///
/// The `UnknownTransactionCommitResult` label is not transient: it is raised
/// by `commitTransaction`, which this function never classifies.
pub(crate) fn transaction_error(e: mongodb::error::Error) -> StorageError {
    if is_transient_write_conflict(&e) {
        StorageError::Transient(e.to_string())
    } else {
        StorageError::Internal(e.to_string())
    }
}

/// The error for a single-item write that could not commit in `attempts`
/// attempts, the last of which failed with `cause`.
///
/// Amazon DynamoDB applies concurrent writes to one item one after another
/// and answers each with 200; it answers `TransactionConflictException` to a
/// PutItem, DeleteItem or UpdateItem only while a TransactWriteItems holds
/// the item, which this backend cannot tell apart from a race against other
/// single-item writes. A write that ran out of attempts is a write the
/// backend could not apply now, so it is reported as
/// [`StorageError::Transient`], which the engine answers as HTTP 503
/// `ServiceUnavailable`. The DynamoDB error-handling documentation lists
/// `ServiceUnavailable` (HTTP 503) as "OK to retry? Yes", and the SDK retry
/// modes retry a 503 on their own, whereas a 400 `TransactionConflictException`
/// is handed to the application.
///
/// The attempts end on any error a re-run may clear, which is not always a
/// conflict with another writer (a primary step-down or a snapshot error ends
/// them as well), so the message carries the text of the last error that
/// ended an attempt instead of naming a cause. The engine logs it with the
/// warning it writes for a 503 and does not send it to the client.
///
/// Every single-item write path uses this one error, so the three operations
/// answer the same way to the same contention. `TransactWriteItems` keeps
/// `TransactionCanceledException` with a per-operation `TransactionConflict`
/// reason, which is what the service answers for a transaction.
pub(crate) fn retry_budget_exhausted(operation: &str, attempts: u32, cause: &str) -> StorageError {
    StorageError::Transient(format!(
        "{operation}: could not commit in {attempts} attempts: {cause}"
    ))
}

/// Note that attempt `attempt` of `operation` ended in an error that a re-run
/// may clear: one debug line, and the cause back to the caller, which keeps the
/// last one for [`retry_budget_exhausted`].
pub(crate) fn retrying(operation: &str, attempt: u32, cause: String) -> String {
    tracing::debug!(operation, attempt, error = %cause, "re-running a write transaction");
    cause
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_engine::synthetic_error_with_labels;
    use mongodb::bson::doc;

    /// A labelless server command error with the given code, the form
    /// `findAndModify` and `insert` return inside a transaction.
    fn command_error(code: i32) -> mongodb::error::Error {
        let command_error: mongodb::error::CommandError = bson::from_document(doc! {
            "code": code,
            "codeName": "SyntheticTestError",
            "errmsg": "synthetic command error",
        })
        .expect("static CommandError document deserializes");
        mongodb::error::ErrorKind::Command(command_error).into()
    }

    #[test]
    fn write_conflict_on_the_counter_is_transient() {
        // MongoDB 7.0 reports two transactions that $inc the same stream
        // sequence counter as code 112, WriteConflict.
        let e = command_error(112);
        assert!(matches!(transaction_error(e), StorageError::Transient(_)));
    }

    #[test]
    fn transient_label_is_transient() {
        // The label the server attaches to every error a transaction may be
        // re-run after, whatever the code.
        let e = synthetic_error_with_labels(
            "synthetic transient error",
            &[mongodb::error::TRANSIENT_TRANSACTION_ERROR],
        );
        assert!(matches!(transaction_error(e), StorageError::Transient(_)));
    }

    #[test]
    fn other_errors_are_internal() {
        for code in [11000, 2, 251] {
            let e = command_error(code);
            assert!(
                matches!(transaction_error(e), StorageError::Internal(_)),
                "code {code}"
            );
        }
    }

    #[test]
    fn unknown_commit_result_is_not_transient() {
        let e = synthetic_error_with_labels(
            "synthetic unknown commit",
            &[mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT],
        );
        assert!(matches!(transaction_error(e), StorageError::Internal(_)));
    }

    #[test]
    fn an_exhausted_budget_is_transient_and_names_the_operation_and_the_cause() {
        let cause = "Command failed: WriteConflict (code 112) on the stream counter";
        match retry_budget_exhausted("DeleteItem", 50, cause) {
            StorageError::Transient(msg) => assert_eq!(
                msg,
                format!("DeleteItem: could not commit in 50 attempts: {cause}")
            ),
            other => panic!("want Transient, got {other:?}"),
        }
    }

    #[test]
    fn a_retry_hands_its_cause_back_for_the_exhaustion_message() {
        assert_eq!(
            retrying("PutItem", 3, "snapshot too old".to_owned()),
            "snapshot too old"
        );
    }
}
