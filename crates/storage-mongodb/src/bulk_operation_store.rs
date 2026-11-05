// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BulkOperationStore` for the `MongoDB` backend.
//!
//! Bulk operations are not implemented on this backend in this version.
//! Every method refuses with the unsupported-feature message, which the
//! engine surfaces to the client as a `ValidationException`, matching how
//! this backend reports other capabilities it does not carry.

use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;

use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BulkOperationListResult, BulkOperationRecord, BulkOperationStore, CancelOutcome, FinishState,
    LeaseState, ListCursor, NewBulkOperation, Progress, ReportKind, ReportRecord, StartOutcome,
    Terminal,
};

use crate::MongoEngine;

fn unsupported<T>() -> BoxFuture<'static, Result<T, StorageError>> {
    Box::pin(async {
        Err(StorageError::Validation(
            "Bulk operations are not supported on this storage backend".to_owned(),
        ))
    })
}

impl BulkOperationStore for MongoEngine {
    fn start_bulk_operation(
        &self,
        _account_id: &str,
        _req: NewBulkOperation,
        _account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>> {
        unsupported()
    }

    fn get_bulk_operation(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>> {
        unsupported()
    }

    fn count_non_terminal(&self, _account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>> {
        unsupported()
    }

    fn count_terminal_born_since(
        &self,
        _account_id: &str,
        _since_ms: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unsupported()
    }

    fn list_bulk_operations(
        &self,
        _account_id: &str,
        _limit: u32,
        _after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult> {
        unsupported()
    }

    fn request_cancel(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>> {
        unsupported()
    }

    fn claim_runnable(
        &self,
        _owner: &str,
        _lease: Duration,
        _max: usize,
        _exclude_accounts: &[String],
        _exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>> {
        unsupported()
    }

    fn checkpoint(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _lease: Duration,
        _progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        unsupported()
    }

    fn report_records_page(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _kind: ReportKind,
        _after: Option<(i64, i64)>,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>> {
        unsupported()
    }

    fn finish(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>> {
        unsupported()
    }

    fn list_purgeable_before(
        &self,
        _cutoff: SystemTime,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unsupported()
    }

    fn purge_operations(
        &self,
        _pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unsupported()
    }
}
