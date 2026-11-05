// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! A backend-classified client error reaches the client unchanged through the
//! backup mapping, keeping its variant and message. Control: a raw `Internal`
//! message is still redacted behind the fixed string.

use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;

use super::{
    create_backup_storage_err_to_dynamo, restore_storage_err_to_dynamo, storage_err_to_dynamo,
};

const INTERNAL_MSG: &str =
    "The server encountered an internal error trying to fulfill the request.";
const UNAVAILABLE_MSG: &str = "The database is temporarily unavailable, retry shortly.";
const CAPACITY_MSG: &str =
    "The level of configured provisioned throughput for the table was exceeded.";

#[test]
fn service_internal_server_error_passes_through() {
    let err = storage_err_to_dynamo(StorageError::Service(DynamoDbError::InternalServerError(
        INTERNAL_MSG.to_owned(),
    )));
    match err {
        DynamoDbError::InternalServerError(msg) => assert_eq!(msg, INTERNAL_MSG),
        other => panic!("expected InternalServerError, got {other:?}"),
    }
}

#[test]
fn service_service_unavailable_passes_through() {
    let err = storage_err_to_dynamo(StorageError::Service(DynamoDbError::ServiceUnavailable(
        UNAVAILABLE_MSG.to_owned(),
    )));
    match err {
        DynamoDbError::ServiceUnavailable(msg) => assert_eq!(msg, UNAVAILABLE_MSG),
        other => panic!("expected ServiceUnavailable, got {other:?}"),
    }
}

#[test]
fn service_capacity_error_passes_through() {
    let err = storage_err_to_dynamo(StorageError::Service(
        DynamoDbError::ProvisionedThroughputExceededException(CAPACITY_MSG.to_owned()),
    ));
    match err {
        DynamoDbError::ProvisionedThroughputExceededException(msg) => {
            assert_eq!(msg, CAPACITY_MSG);
        }
        other => panic!("expected ProvisionedThroughputExceededException, got {other:?}"),
    }
}

// The restore mapping delegates everything it does not name, so a Service
// error passes through it as well.
#[test]
fn service_error_passes_through_the_restore_mapping() {
    let err = restore_storage_err_to_dynamo(StorageError::Service(
        DynamoDbError::BackupNotFoundException("Backup not found: arn".to_owned()),
    ));
    match err {
        DynamoDbError::BackupNotFoundException(msg) => assert_eq!(msg, "Backup not found: arn"),
        other => panic!("expected BackupNotFoundException, got {other:?}"),
    }
}

// The create-backup mapping names two table-state variants and delegates the
// rest, so a Service error passes through it too.
#[test]
fn service_error_passes_through_the_create_backup_mapping() {
    let err = create_backup_storage_err_to_dynamo(
        StorageError::Service(DynamoDbError::LimitExceededException(
            "Too many backups in progress".to_owned(),
        )),
        "t",
    );
    match err {
        DynamoDbError::LimitExceededException(msg) => {
            assert_eq!(msg, "Too many backups in progress");
        }
        other => panic!("expected LimitExceededException, got {other:?}"),
    }
}

#[test]
fn internal_driver_text_is_still_redacted() {
    let err = storage_err_to_dynamo(StorageError::Internal("driver text".to_owned()));
    match err {
        DynamoDbError::InternalServerError(msg) => assert_eq!(msg, "Internal server error"),
        other => panic!("expected InternalServerError, got {other:?}"),
    }
}
