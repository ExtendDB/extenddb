// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The create-backup and restore mappings name a few table-state variants and
//! delegate the rest to `backup_requests::storage_err_to_dynamo`, so a
//! backend-classified error reaches the client through them as it does through
//! that mapping (`service_error_log_tests`).

use extenddb_core::error::error_document;
use extenddb_storage::error::StorageError;

use super::{create_backup_storage_err_to_dynamo, restore_storage_err_to_dynamo};

#[test]
fn a_service_error_reaches_the_client_as_classified_through_create_backup_and_restore() {
    for (error, body) in crate::service_error_log_tests::rows() {
        let create = create_backup_storage_err_to_dynamo(StorageError::Service(error.clone()), "t");
        assert_eq!(error_document(&create), body, "CreateBackup: {error:?}");
        let restore = restore_storage_err_to_dynamo(StorageError::Service(error.clone()));
        assert_eq!(error_document(&restore), body, "Restore: {error:?}");
    }
}
