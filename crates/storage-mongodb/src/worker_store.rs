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
//! `DeleteTable` runs inline: it marks the document `DELETING`, then drops the
//! collections and removes the document in the request. A `DeleteTable` that
//! stops in between (a step that fails, a process that dies) leaves the
//! document `DELETING`, and a second `DeleteTable` is refused as the service
//! refuses it, so this worker finishes such a delete once
//! [`DELETE_FINISH_AFTER`] has passed since the mark. GSI create is handled
//! separately by [`ttl_worker::gsi_backfill_worker`] on the `indexes` catalog
//! collection.
//!
//! [`ttl_worker::gsi_backfill_worker`]: crate::ttl_worker::gsi_backfill_worker

use std::time::Duration;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{Bson, Document, doc};

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;

/// Default control-plane delay (seconds) when the setting is absent or
/// unparseable: the workers and the console's default read this one value.
pub(crate) const DEFAULT_CONTROL_PLANE_DELAY_SECONDS: f64 = 0.25;

/// How long after a `DeleteTable` marks a document `DELETING` the control plane
/// finishes the delete itself, and how long it waits before it tries again
/// after a pass that could not finish one.
///
/// `DeleteTable` finishes the delete in the request, in milliseconds when
/// nothing holds the collections. The longest wait among its steps is a
/// collection drop behind a client transaction that holds the collection,
/// and MongoDB aborts a transaction once it is older than
/// `transactionLifetimeLimitSeconds`, 60 seconds by default. A document still
/// `DELETING` this long after its mark belongs to a `DeleteTable` that
/// stopped. A `DeleteTable` still running then runs the same idempotent steps
/// as the control plane ([`MongoEngine::finish_table_delete`]).
pub(crate) const DELETE_FINISH_AFTER: Duration = Duration::from_secs(60);

/// The `status_transition_at` a `DELETING` mark made now carries: the time
/// from which the control plane finishes the delete.
pub(crate) fn delete_finish_due() -> mongodb::bson::DateTime {
    let after = i64::try_from(DELETE_FINISH_AFTER.as_millis()).unwrap_or(i64::MAX);
    mongodb::bson::DateTime::from_millis(
        mongodb::bson::DateTime::now()
            .timestamp_millis()
            .saturating_add(after),
    )
}

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
            .unwrap_or(DEFAULT_CONTROL_PLANE_DELAY_SECONDS)
    }

    /// Finish every delete whose `DELETING` document is due
    /// ([`DELETE_FINISH_AFTER`] after its mark, or at once for a document
    /// with no transition time, which a build before this one wrote). Each
    /// document is first claimed by moving its transition time forward,
    /// conditioned on the value read, so that of several control-plane
    /// passes, in one process or several, one claims a document at a time; a
    /// `DeleteTable` whose own task is still running then runs the same steps
    /// alongside it. The claim also makes a step that keeps failing retry
    /// once per [`DELETE_FINISH_AFTER`] rather than at every poll. A failed
    /// step is logged and leaves the document `DELETING`; it does not stop
    /// the pass. Returns the names of the tables removed.
    async fn finish_due_deletes(&self) -> Result<Vec<String>, StorageError> {
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let due: Vec<Document> = tables_coll
            .find(doc! {
                "table_status": "DELETING",
                "$or": [
                    { "status_transition_at": { "$lte": mongodb::bson::DateTime::now() } },
                    { "status_transition_at": Bson::Null },
                ],
            })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut removed = Vec::new();
        for table_doc in due {
            let (Ok(id), Ok(table_id), Ok(table_arn)) = (
                table_doc.get_document("_id"),
                table_doc.get_str("table_id"),
                table_doc.get_str("table_arn"),
            ) else {
                tracing::warn!("a DELETING table document lacks _id, table_id or table_arn");
                continue;
            };
            let (Ok(account_id), Ok(table_name)) =
                (id.get_str("account_id"), id.get_str("table_name"))
            else {
                tracing::warn!("a DELETING table document's _id lacks account_id or table_name");
                continue;
            };
            let read_due = match table_doc.get("status_transition_at") {
                Some(Bson::DateTime(at)) => Bson::DateTime(*at),
                _ => Bson::Null,
            };
            let claimed = tables_coll
                .update_one(
                    doc! {
                        "_id": id.clone(),
                        "table_id": table_id,
                        "table_status": "DELETING",
                        "status_transition_at": read_due,
                    },
                    doc! { "$set": { "status_transition_at": delete_finish_due() } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if claimed.matched_count == 0 {
                // Another pass claimed it, or a DeleteTable finished it.
                continue;
            }
            match self
                .finish_table_delete(account_id, table_name, table_id, table_arn)
                .await
            {
                Ok(()) => removed.push(table_name.to_owned()),
                Err(e) => tracing::warn!(
                    "table '{table_name}' ({table_id}) stays DELETING; the control plane \
                     tries again in {} s: {e}",
                    DELETE_FINISH_AFTER.as_secs()
                ),
            }
        }
        Ok(removed)
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

            // DELETING → removed: deletes a DeleteTable left unfinished.
            for name in self.finish_due_deletes().await? {
                transitions.push((name, "DELETING → deleted"));
            }

            Ok(transitions)
        })
    }
}
