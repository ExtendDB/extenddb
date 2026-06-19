// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` implementation for the DynamoDB-at-home backend.
//!
//! DynamoDB drives its own CREATING→ACTIVE transitions internally; DescribeTable
//! reports the real status. ExtendDB's control-plane transition worker has
//! nothing to advance for this backend.

use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::DynamoEngine;

impl WorkerStore for DynamoEngine {
    /// DynamoDB manages its own table lifecycle transitions (CREATING→ACTIVE,
    /// DELETING→deleted). `DescribeTable` reflects the live status, so there
    /// are no pending transitions for ExtendDB to advance. Always returns an
    /// empty list.
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async { Ok(vec![]) })
    }
}
