// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` implementation for `MongoDB`.
//!
//! The MongoDB backend does not use transient control-plane states
//! (`CREATING`, `DELETING`) for tables: `create_table_impl` writes the
//! catalog row with `table_status: "ACTIVE"` synchronously and
//! `delete_table_impl` removes the row + collections in one call, so
//! there is never a table document waiting for a background transition.
//! Both paths run inline in the request handler because MongoDB's
//! collection create/drop is fast enough not to warrant asynchronous
//! promotion, and the alternative would require a background worker
//! whose only job is to catch up work the API call could have done
//! synchronously anyway.
//!
//! GSI create is the one control-plane operation that does need
//! async work — its background portion lives in
//! [`ttl_worker::gsi_backfill_worker`] rather than here because it
//! operates on the `indexes` catalog collection with a `CREATING`
//! index-status, not on the `tables` collection.
//!
//! The trait method returns an empty list so `WorkerStore` is
//! satisfied for the [`OperationsEngine`] supertrait bound without
//! introducing a background job that would only ever be a no-op.
//!
//! [`ttl_worker::gsi_backfill_worker`]: crate::ttl_worker::gsi_backfill_worker
//! [`OperationsEngine`]: extenddb_storage::OperationsEngine

use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;

impl WorkerStore for MongoEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }
}
