// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the bulk put runner reads of an operation at every batch boundary.
//! The two backends that run bulk operations answer it from the operation
//! row alone, without the input files a full read attaches, so the read
//! costs the same whatever the file count.

use futures::future::BoxFuture;

use crate::BulkOperationRecord;
use crate::error::StorageError;

/// The part of an operation row the runner reads at a batch boundary: its
/// status, whether a cancel was requested, and which claim holds its lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationFlags {
    pub status: String,
    pub cancel_requested: bool,
    pub lease_owner: Option<String>,
}

impl OperationFlags {
    /// The flags of a full record.
    #[must_use]
    pub fn of(record: &BulkOperationRecord) -> Self {
        Self {
            status: record.status.clone(),
            cancel_requested: record.cancel_requested,
            lease_owner: record.lease_owner.clone(),
        }
    }

    /// The flags of the record a full read answers, for a backend that
    /// keeps no cheaper read.
    pub(crate) fn of_read(
        read: BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>>,
    ) -> BoxFuture<'_, Result<Option<Self>, StorageError>> {
        Box::pin(async move { Ok(read.await?.as_ref().map(Self::of)) })
    }
}
