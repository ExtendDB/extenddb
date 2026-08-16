// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` implementation for `MongoDB`.
//!
//! Processes table control-plane transitions (`CREATING` → `ACTIVE`) as a
//! background job. `create_table_impl` and the restore path write the catalog
//! row as `CREATING` with a `status_transition_at` timestamp when
//! `control_plane_delay_seconds` > 0 (matching the Postgres backend and real
//! DynamoDB, which report `CREATING` before a table becomes usable); this
//! worker flips such rows to `ACTIVE` once their transition time has passed.
//! When `control_plane_delay_seconds` is 0 the create/restore paths write
//! `ACTIVE` directly and this worker has nothing to do.
//!
//! `DeleteTable` remains inline (the catalog row and collections are removed in
//! the request handler), so there is no `DELETING` transient state to reconcile
//! here. GSI create is handled separately by
//! [`ttl_worker::gsi_backfill_worker`] on the `indexes` catalog collection.
//!
//! [`ttl_worker::gsi_backfill_worker`]: crate::ttl_worker::gsi_backfill_worker

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;

/// Default control-plane delay (seconds) when the setting is absent or
/// unparseable. Matches the Postgres backend default.
const DEFAULT_CONTROL_PLANE_DELAY_SECS: f64 = 0.25;

impl MongoEngine {
    /// Read `control_plane_delay_seconds` from the settings collection,
    /// falling back to the default. A value <= 0 means "no CREATING window"
    /// (create/restore write `ACTIVE` synchronously).
    pub(crate) async fn control_plane_delay_seconds(&self) -> f64 {
        let coll = self.catalog_db.collection::<Document>("settings");
        coll.find_one(doc! { "_id": "control_plane_delay_seconds" })
            .await
            .ok()
            .flatten()
            .and_then(|d| d.get_str("value").ok().map(str::to_owned))
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v >= 0.0)
            .unwrap_or(DEFAULT_CONTROL_PLANE_DELAY_SECS)
    }
}

impl WorkerStore for MongoEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move {
            let mut transitions = Vec::new();
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let now = mongodb::bson::DateTime::now();

            // CREATING → ACTIVE: tables whose scheduled transition time has
            // passed. Each row is updated by its own compound `_id` (the mongo
            // catalog stores account_id/table_name inside `_id`, not at the top
            // level — the previous impl filtered on flat fields and matched
            // nothing).
            let filter = doc! {
                "table_status": "CREATING",
                "status_transition_at": { "$lte": now },
            };
            let mut cursor = tables_coll
                .find(filter)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            while let Some(table_doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let Some(id) = table_doc.get("_id").cloned() else {
                    continue;
                };
                let table_name = table_doc
                    .get_document("_id")
                    .ok()
                    .and_then(|d| d.get_str("table_name").ok())
                    .unwrap_or_default()
                    .to_owned();

                tables_coll
                    .update_one(
                        doc! { "_id": id, "table_status": "CREATING" },
                        doc! {
                            "$set": { "table_status": "ACTIVE" },
                            "$unset": { "status_transition_at": "" },
                        },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                transitions.push((table_name, "CREATING → active"));
            }

            Ok(transitions)
        })
    }
}
