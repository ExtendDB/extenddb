// SPDX-License-Identifier: Apache-2.0

//! Startup repair of base-table documents stored without their key.
//!
//! Before issue #436 was fixed, an UpdateItem that created an item on the
//! native fast path upserted a document with a generated ObjectId `_id` and
//! an `item_data` without the key attributes. Such a document answers
//! GetItem without the key, and every PutItem on the key fails on the
//! immutable `_id`. `_id` cannot be changed in place, so the repair replaces
//! each such document with the one `item_to_document` writes: delete the old
//! document by its ObjectId, insert the rebuilt one. The scan is a bracket
//! query on the `_id` index (`{_id: {$type: "objectId"}}`), so a collection
//! with no such document costs one index probe.
//!
//! Up to [`REPAIR_BATCH`] documents are rebuilt in one transaction. A
//! transaction's commit is one majority-acknowledged round trip and dominates
//! the cost of a rebuild, so batching the rebuilds behind one commit is what
//! sets the rate; each batch is one in-transaction read, one delete and one
//! insert. The batch is atomic, so a process killed mid-repair leaves every
//! document either as it was or rebuilt, and the next start finishes the
//! remainder. A batch whose transaction cannot run is rebuilt one document
//! per transaction, so one document another writer holds, or one that cannot
//! be rebuilt at all, leaves the rest of the batch repaired.
//!
//! The documents are re-read inside the transaction rather than taken from
//! the scan, so a client that writes to a document between the scan and the
//! rebuild does not lose that write: the rebuild either sees it or hits a
//! write conflict and runs again.
//!
//! [`spawn_key_repair`] runs the whole pass as a background task, so `serve`
//! opens its listener without waiting for it (see `server_components_factory`
//! in `lib.rs`). Until the repair reaches a given document, requests on that
//! document's key answer as they did before this repair existed. Progress is
//! logged every [`PROGRESS_EVERY`] documents and once at the end. A failure
//! on one document is logged and skipped; the summary reports it.

use std::sync::Arc;

use bson::{Bson, Document, doc};
use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
use extenddb_storage::error::StorageError;
use futures::TryStreamExt;

use super::{CommitOutcome, TRANSIENT_RETRY_ATTEMPTS, backoff_sleep, is_transient_write_conflict};
use crate::MongoEngine;
use crate::data::data_collection_name;
use crate::data::key_fields::{is_stored_without_key, repaired_document};

/// Documents rebuilt in one transaction.
const REPAIR_BATCH: usize = 500;

/// Repaired documents between progress lines.
const PROGRESS_EVERY: u64 = 10_000;

/// How many times a single-document rebuild waits for a writer that holds its
/// document, and how long each wait is.
///
/// A process killed mid-repair leaves its last transaction holding that
/// batch's documents until the server expires it, which takes up to
/// `transactionLifetimeLimitSeconds` (60 s by default). The repair runs in the
/// background and nothing waits on it, so it waits the locks out instead of
/// counting those documents failed and leaving them for the next start.
const CONFLICT_WAITS: u32 = 40;
const CONFLICT_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// What a repair run did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct KeyRepairSummary {
    /// Tables whose data collection was scanned.
    pub tables_scanned: u64,
    /// Documents rewritten with their key.
    pub documents_repaired: u64,
    /// Documents left as they were because they could not be rebuilt or
    /// written; each is logged.
    pub documents_failed: u64,
}

impl KeyRepairSummary {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.documents_repaired == 0 && self.documents_failed == 0
    }
}

impl std::fmt::Display for KeyRepairSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} tables scanned, {} documents stored without their key repaired, {} failed",
            self.tables_scanned, self.documents_repaired, self.documents_failed
        )
    }
}

impl MongoEngine {
    /// Repair every base-table document stored without its key, across all
    /// tables in the catalog. See the module comment.
    pub async fn repair_items_stored_without_key(&self) -> Result<KeyRepairSummary, StorageError> {
        let tables = self.catalog_db.collection::<Document>("tables");
        let mut cursor = tables
            .find(doc! {})
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let mut summary = KeyRepairSummary::default();
        while let Some(table_doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            let (table_id, key_schema, attribute_definitions) = match table_key(&table_doc) {
                Ok(parsed) => parsed,
                Err(e) => {
                    tracing::warn!("key repair: skipping a catalog table row: {e}");
                    continue;
                }
            };
            summary.tables_scanned += 1;
            self.repair_table(&table_id, &key_schema, &attribute_definitions, &mut summary)
                .await?;
        }
        Ok(summary)
    }

    /// Repair the documents of one table's data collection.
    pub(crate) async fn repair_table(
        &self,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
        summary: &mut KeyRepairSummary,
    ) -> Result<(), StorageError> {
        let coll = self
            .data_db
            .collection::<Document>(&data_collection_name(table_id));
        let mut cursor = coll
            .find(doc! { "_id": { "$type": "objectId" } })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        // The rebuilt documents have the key text as their `_id`, outside the
        // ObjectId bracket this cursor reads, so inserting them cannot feed
        // the scan its own output.
        let mut ids: Vec<Bson> = Vec::with_capacity(REPAIR_BATCH);
        while let Some(bad) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            if !is_stored_without_key(&bad) {
                continue;
            }
            if let Some(id) = bad.get("_id") {
                ids.push(id.clone());
            }
            if ids.len() >= REPAIR_BATCH {
                self.repair_batch(
                    &coll,
                    table_id,
                    key_schema,
                    attribute_definitions,
                    &ids,
                    summary,
                )
                .await;
                ids.clear();
            }
        }
        if !ids.is_empty() {
            self.repair_batch(
                &coll,
                table_id,
                key_schema,
                attribute_definitions,
                &ids,
                summary,
            )
            .await;
        }
        Ok(())
    }

    /// Rebuild one batch, and when the batch transaction cannot run, rebuild
    /// its documents one transaction at a time so that one document that
    /// cannot be written leaves the rest of the batch repaired.
    async fn repair_batch(
        &self,
        coll: &mongodb::Collection<Document>,
        table_id: &str,
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
        ids: &[Bson],
        summary: &mut KeyRepairSummary,
    ) {
        let before = summary.documents_repaired;
        match self
            .replace_batch(coll, ids, key_schema, attribute_definitions)
            .await
        {
            Ok(outcome) => {
                summary.documents_repaired += outcome.repaired;
                summary.documents_failed += outcome.failed.len() as u64;
                for (id, reason) in outcome.failed {
                    tracing::error!(
                        "key repair: table {table_id}: cannot rebuild document {id:?}: {reason}"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "key repair: table {table_id}: a batch of {} documents could not be \
                     rewritten together ({e}); rewriting them one at a time",
                    ids.len()
                );
                for id in ids {
                    self.repair_one(
                        coll,
                        table_id,
                        id,
                        key_schema,
                        attribute_definitions,
                        summary,
                    )
                    .await;
                }
            }
        }
        log_progress(before, summary);
    }

    /// Rebuild the document at `id` in its own transaction, waiting out a
    /// writer that holds it rather than leaving it for the next start.
    async fn repair_one(
        &self,
        coll: &mongodb::Collection<Document>,
        table_id: &str,
        id: &Bson,
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
        summary: &mut KeyRepairSummary,
    ) {
        let one = std::slice::from_ref(id);
        for waited in 0..=CONFLICT_WAITS {
            match self
                .replace_batch(coll, one, key_schema, attribute_definitions)
                .await
            {
                Ok(outcome) => {
                    if waited > 0 && outcome.repaired > 0 {
                        tracing::info!(
                            "key repair: table {table_id}: document {id:?} rewritten after \
                             waiting {} s for the writer holding it",
                            waited * CONFLICT_WAIT.as_secs() as u32
                        );
                    }
                    summary.documents_repaired += outcome.repaired;
                    summary.documents_failed += outcome.failed.len() as u64;
                    for (failed_id, reason) in outcome.failed {
                        tracing::error!(
                            "key repair: table {table_id}: cannot rebuild document \
                             {failed_id:?}: {reason}"
                        );
                    }
                    return;
                }
                Err(StorageError::TransactionConflict(_)) if waited < CONFLICT_WAITS => {
                    tokio::time::sleep(CONFLICT_WAIT).await;
                }
                Err(e) => {
                    tracing::error!(
                        "key repair: table {table_id}: cannot rewrite document {id:?}: {e}"
                    );
                    summary.documents_failed += 1;
                    return;
                }
            }
        }
    }

    /// Rebuild the documents at `ids` in one transaction: read them in the
    /// transaction, delete them, insert the rebuilt documents. Ids whose
    /// document is gone or already carries its key are skipped. Ids whose
    /// document cannot be rebuilt are reported in `failed` and left in place.
    async fn replace_batch(
        &self,
        coll: &mongodb::Collection<Document>,
        ids: &[Bson],
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
    ) -> Result<BatchOutcome, StorageError> {
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let tx_options = self.write_transaction_options();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            match rebuild_in_session(coll, &mut session, ids, key_schema, attribute_definitions)
                .await
            {
                Ok(outcome) if outcome.repaired == 0 => {
                    let _ = session.abort_transaction().await;
                    return Ok(outcome);
                }
                Ok(outcome) => match self.commit_write_transaction(&mut session, None).await? {
                    CommitOutcome::Committed => return Ok(outcome),
                    CommitOutcome::RerunBody => backoff_sleep(attempt).await,
                },
                Err(BatchError::Mongo(e)) if is_transient_write_conflict(&e) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(e) => {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::Internal(e.to_string()));
                }
            }
        }
        Err(StorageError::TransactionConflict(
            "key repair: too many concurrent write conflicts, giving up".to_owned(),
        ))
    }
}

/// `table_id`, key schema and attribute definitions of a catalog `tables` row.
fn table_key(
    table_doc: &Document,
) -> Result<(String, Vec<KeySchemaElement>, Vec<AttributeDefinition>), StorageError> {
    let table_id = table_doc
        .get_str("table_id")
        .map_err(|_| StorageError::Internal("missing table_id".to_owned()))?
        .to_owned();
    let key_schema = table_doc
        .get("key_schema")
        .ok_or_else(|| StorageError::Internal(format!("table {table_id}: missing key_schema")))?;
    let key_schema: Vec<KeySchemaElement> = bson::from_bson(key_schema.clone())
        .map_err(|e| StorageError::Internal(format!("table {table_id}: key_schema: {e}")))?;
    let attribute_definitions = table_doc.get("attribute_definitions").ok_or_else(|| {
        StorageError::Internal(format!("table {table_id}: missing attribute_definitions"))
    })?;
    let attribute_definitions: Vec<AttributeDefinition> =
        bson::from_bson(attribute_definitions.clone()).map_err(|e| {
            StorageError::Internal(format!("table {table_id}: attribute_definitions: {e}"))
        })?;
    Ok((table_id, key_schema, attribute_definitions))
}

/// What one batch transaction did.
#[derive(Debug, Default)]
struct BatchOutcome {
    /// Documents the transaction rewrote.
    repaired: u64,
    /// Documents it left in place, with the reason.
    failed: Vec<(Bson, String)>,
}

/// Why a batch transaction could not run.
#[derive(Debug)]
enum BatchError {
    /// A MongoDB error, which may be a transient write conflict.
    Mongo(mongodb::error::Error),
    /// The delete did not match the documents the transaction had just read,
    /// so the batch cannot be inserted without risking a duplicate item. A
    /// transaction holds its snapshot, so this does not happen; the caller
    /// falls back to one transaction per document rather than trusting it.
    Counts { deleted: u64, read: usize },
}

impl From<mongodb::error::Error> for BatchError {
    fn from(e: mongodb::error::Error) -> Self {
        Self::Mongo(e)
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mongo(e) => write!(f, "{e}"),
            Self::Counts { deleted, read } => {
                write!(
                    f,
                    "deleted {deleted} of {read} documents read in the transaction"
                )
            }
        }
    }
}

/// Read the documents at `ids` in `session`'s transaction, delete them and
/// insert the rebuilt documents. The read is inside the transaction, so a
/// write that landed after the scan is either rebuilt with the document or
/// turns the delete into a write conflict.
async fn rebuild_in_session(
    coll: &mongodb::Collection<Document>,
    session: &mut mongodb::ClientSession,
    ids: &[Bson],
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<BatchOutcome, BatchError> {
    let mut outcome = BatchOutcome::default();
    let mut stale: Vec<Bson> = Vec::with_capacity(ids.len());
    let mut rebuilt: Vec<Document> = Vec::with_capacity(ids.len());
    let mut cursor = coll
        .find(doc! { "_id": { "$in": ids.to_vec() } })
        .session(&mut *session)
        .await?;
    while let Some(bad) = cursor.next(&mut *session).await {
        let bad = bad?;
        let Some(id) = bad.get("_id").cloned() else {
            continue;
        };
        if !is_stored_without_key(&bad) {
            continue;
        }
        match repaired_document(&bad, key_schema, attribute_definitions) {
            Ok(d) => {
                stale.push(id);
                rebuilt.push(d);
            }
            Err(e) => outcome.failed.push((id, e.to_string())),
        }
    }
    if rebuilt.is_empty() {
        return Ok(outcome);
    }
    let deleted = coll
        .delete_many(doc! { "_id": { "$in": stale } })
        .session(&mut *session)
        .await?;
    if deleted.deleted_count != rebuilt.len() as u64 {
        return Err(BatchError::Counts {
            deleted: deleted.deleted_count,
            read: rebuilt.len(),
        });
    }
    coll.insert_many(&rebuilt).session(session).await?;
    outcome.repaired = rebuilt.len() as u64;
    Ok(outcome)
}

/// Log a progress line when the repaired count has crossed a multiple of
/// `PROGRESS_EVERY`. A batch is smaller than that interval, so each crossing
/// is logged once.
fn log_progress(before: u64, summary: &KeyRepairSummary) {
    if before / PROGRESS_EVERY != summary.documents_repaired / PROGRESS_EVERY {
        tracing::info!("key repair in progress: {summary}");
    }
}

/// Run a whole repair pass in the background and log the outcome.
///
/// `serve` starts this instead of awaiting the pass, so the listener opens at
/// once however many documents need rebuilding. Requests on a key whose
/// document the pass has not reached yet answer as they did before the repair
/// existed, so a long pass delays the repair of a key rather than the start
/// of the server.
pub(crate) fn spawn_key_repair(engine: Arc<MongoEngine>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        match engine.repair_items_stored_without_key().await {
            Ok(repair) if repair.is_clean() => {
                tracing::debug!("startup key repair: {repair}");
            }
            Ok(repair) => {
                let seconds = started.elapsed().as_secs_f64();
                let rate = if seconds > 0.0 {
                    (repair.documents_repaired as f64) / seconds
                } else {
                    0.0
                };
                let line = format!(
                    "startup key repair finished: {repair}, {seconds:.2} s, \
                     {rate:.0} documents a second"
                );
                if repair.documents_failed == 0 {
                    tracing::info!("{line}");
                } else {
                    tracing::warn!("{line}");
                }
            }
            Err(e) => tracing::error!("Failed to repair items stored without their key: {e}"),
        }
    })
}
