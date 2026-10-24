// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Classification of mongodb driver errors raised by catalog reads.
//!
//! A read that cannot reach the deployment is [`StorageError::Transient`],
//! which `storage_err_to_dynamo` answers as `ServiceUnavailable` (503,
//! retryable by the SDKs). Everything else stays [`StorageError::Internal`]
//! and answers as `InternalServerError` (500). This matches what the SQL
//! backends do for the same faults: `map_sqlx_err` in the SQLite backend and
//! `read_err` in the PostgreSQL backend both classify I/O errors, pool
//! trouble and the server's own connection-class replies as `Transient`.
//!
//! The driver has predicates for this (`is_network_error`, `is_read_retryable`)
//! but they are `pub(crate)` to the driver, so the classification is written
//! out here against `ErrorKind` and the driver's documented retryable-read
//! code set.

use extenddb_storage::error::StorageError;
use mongodb::error::{Error as DriverError, ErrorKind};

/// Server error codes that make a read retryable, from the driver's own
/// `RETRYABLE_READ_CODES` (mongodb 3.8.0 `src/error.rs`): HostUnreachable,
/// HostNotFound, NetworkTimeout, ShutdownInProgress, PrimarySteppedDown,
/// ExceededTimeLimit, SocketException, NotWritablePrimary,
/// InterruptedAtShutdown, InterruptedDueToReplStateChange,
/// NotPrimaryNoSecondaryOk, NotPrimaryOrSecondary, and
/// ReadConcernMajorityNotAvailableYet.
const RETRYABLE_READ_CODES: [i32; 13] = [
    6, 7, 89, 91, 134, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436,
];

/// Whether `e` means the read could not reach a server that could serve it.
///
/// `ServerSelection` is the one a stopped or unreachable deployment produces:
/// the driver marks the server Unknown and server selection then times out.
/// Its own label set is empty, because the `RetryableError` label sits on the
/// inner per-server error, so a label check does not see it; the kind does.
/// `Io` and `ConnectionPoolCleared` are what the driver itself calls a network
/// error. `DnsResolve` is a name lookup that did not answer.
///
/// The classification errs narrow, for the reason `map_sqlx_err` gives: a
/// fault wrongly called transient is retried by a client that cannot succeed,
/// and the GSI and TTL queue workers retry a `Transient` row instead of
/// dropping it, so a permanent fault called transient stalls a queue.
fn is_unreachable(e: &DriverError) -> bool {
    match &*e.kind {
        ErrorKind::Io(_)
        | ErrorKind::ConnectionPoolCleared { .. }
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::DnsResolve { .. } => true,
        ErrorKind::Command(command) => RETRYABLE_READ_CODES.contains(&command.code),
        _ => false,
    }
}

/// Map a driver error from a catalog read to a `StorageError`.
///
/// Used by the key-info and index-info lookups, whose result decides the wire
/// status on two paths: the server's authorization path reads key info before
/// dispatch (`key_info_for_authz`) and the engine reads it again for the
/// operation itself. Both run the result through `storage_err_to_dynamo`, so
/// this classification is what the client sees (#441).
pub(crate) fn catalog_read_err(e: &DriverError) -> StorageError {
    if is_unreachable(e) {
        StorageError::Transient(e.to_string())
    } else {
        StorageError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{RETRYABLE_READ_CODES, catalog_read_err};
    use extenddb_storage::error::StorageError;
    use mongodb::bson::doc;
    use mongodb::error::{Error as DriverError, ErrorKind};
    use std::sync::Arc;

    /// A labelless server command error with the given code. The struct
    /// variants of `ErrorKind` are `#[non_exhaustive]`, so a synthetic one is
    /// built through deserialization, as the data-engine tests do.
    fn command(code: i32) -> DriverError {
        let command_error: mongodb::error::CommandError = bson::from_document(doc! {
            "code": code,
            "codeName": "SyntheticTestError",
            "errmsg": "synthetic command error",
        })
        .expect("static CommandError document deserializes");
        ErrorKind::Command(command_error).into()
    }

    /// The error #441 is about, taken from the driver rather than built by
    /// hand: a client whose only server is not listening answers a catalog
    /// read with `ErrorKind::ServerSelection`. Port 1 on loopback refuses the
    /// connection, and `serverSelectionTimeoutMS` is short so the test costs
    /// milliseconds. `ErrorKind::ServerSelection` is `#[non_exhaustive]` and
    /// cannot be constructed outside the driver, so this is the only way to
    /// assert on the real thing.
    #[tokio::test]
    async fn a_deployment_that_is_not_listening_is_transient() {
        let client = mongodb::Client::with_uri_str(
            "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=50",
        )
        .await
        .expect("a syntactically valid connection string");
        let error = client
            .database("extenddb_catalog")
            .collection::<mongodb::bson::Document>("tables")
            .find_one(doc! { "table_id": "whatever" })
            .await
            .expect_err("no server is listening on port 1");
        assert!(
            matches!(*error.kind, ErrorKind::ServerSelection { .. }),
            "driver reported {:?}, the test's premise no longer holds",
            error.kind
        );
        assert!(
            matches!(catalog_read_err(&error), StorageError::Transient(_)),
            "a deployment that cannot be reached must be Transient, got {:?}",
            catalog_read_err(&error)
        );
    }

    #[test]
    fn a_dropped_socket_is_transient() {
        let e: DriverError = ErrorKind::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset by peer",
        )))
        .into();
        assert!(matches!(catalog_read_err(&e), StorageError::Transient(_)));
    }

    /// A node that is shutting down, stepping down or recovering answers the
    /// read with one of these codes. The deployment is reachable, so the
    /// driver does not call it a network error, but the read can succeed on
    /// retry once a new primary is elected.
    #[test]
    fn a_node_that_cannot_serve_the_read_is_transient() {
        for code in RETRYABLE_READ_CODES {
            assert!(
                matches!(catalog_read_err(&command(code)), StorageError::Transient(_)),
                "code {code} must be Transient"
            );
        }
    }

    /// Anything not positively identified as unreachable stays Internal, so a
    /// permanent fault is not retried forever by a client, and the GSI and TTL
    /// queue workers do not hold a row that can never be applied.
    #[test]
    fn other_faults_stay_internal() {
        // Unauthorized (13), NamespaceNotFound (26), BadValue (2),
        // WriteConflict (112).
        for code in [13, 26, 2, 112] {
            assert!(
                matches!(catalog_read_err(&command(code)), StorageError::Internal(_)),
                "code {code} must be Internal"
            );
        }
        for e in [
            DriverError::from(ErrorKind::SessionsNotSupported),
            DriverError::from(ErrorKind::MissingResumeToken),
            DriverError::from(ErrorKind::Shutdown),
        ] {
            let what = e.to_string();
            assert!(
                matches!(catalog_read_err(&e), StorageError::Internal(_)),
                "{what} must be Internal"
            );
        }
    }

    /// The driver's message is kept whichever way the error is classified, so
    /// the log line and the server's diagnostics do not lose it.
    #[test]
    fn the_driver_message_survives_classification() {
        let e: DriverError = ErrorKind::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        )))
        .into();
        let StorageError::Transient(message) = catalog_read_err(&e) else {
            panic!("expected Transient");
        };
        assert!(message.contains("connection refused"), "{message}");
    }

    /// #441 through the production lookups rather than the classifier alone.
    ///
    /// `MongoEngine::new` builds client options and a lazy client and performs
    /// no I/O, so an engine can be pointed at a port nothing listens on and
    /// the key-info lookups then fail the way an unreachable deployment makes
    /// them fail. `key_info_for_authz` in the server and the engine both run
    /// the result through `storage_err_to_dynamo`, which answers `Transient`
    /// as `ServiceUnavailable`; that mapping has its own test in
    /// `crates/server/src/request_helpers.rs`, so what is asserted here is the
    /// classification this backend produces.
    ///
    /// Needs no MongoDB, so it runs in `cargo test --workspace` on every job.
    #[tokio::test]
    async fn an_unreachable_deployment_makes_the_key_info_lookups_transient() {
        use extenddb_storage::TableEngine;

        let engine = crate::MongoEngine::new(
            "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=50",
            "us-east-1",
            None,
            mongodb::options::ReadConcern::local(),
        )
        .await
        .expect("building the engine performs no I/O");

        let by_name = engine
            .table_key_info("123456789012", "any-table")
            .await
            .expect_err("no server is listening on port 1");
        assert!(
            matches!(by_name, StorageError::Transient(_)),
            "table_key_info must report an unreachable deployment as Transient, got {by_name:?}"
        );

        let by_id = engine
            .table_key_info_by_table_id_impl("any-table-id")
            .await
            .expect_err("no server is listening on port 1");
        assert!(
            matches!(by_id, StorageError::Transient(_)),
            "table_key_info_by_table_id must report it as Transient, got {by_id:?}"
        );

        let index = engine
            .index_info_by_table_id("any-table-id", "any-index")
            .await
            .expect_err("no server is listening on port 1");
        assert!(
            matches!(index, StorageError::Transient(_)),
            "index_info_by_table_id must report it as Transient, got {index:?}"
        );
    }
}
