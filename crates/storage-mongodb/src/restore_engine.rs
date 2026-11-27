// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane side of a store-backed restore on `MongoDB`
//! ([`RestoreEngine`]): the items of one verified data file written into the
//! CREATING target's data collection, the per-file record, the hand-over to
//! the index backfill worker and the count.
//!
//! The target's documents are built by [`item_to_document`], the write
//! path's function, so a restored table is laid out as a natively created
//! one and refuses what the write path refuses (an `N` sort key beyond
//! Decimal128's 34 significant digits is the write path's `Validation`
//! error, and fails the file). Base documents only: the target's indexes
//! are created CREATING with it and the GSI backfill worker fills them from
//! the base collection and flips the table ACTIVE once every index has
//! left CREATING, so `finish_restore` is the hand-over
//! ([`crate::MongoEngine::hand_over_to_backfill`]) and answers
//! [`Finished::Pending`].
//!
//! One file is `insert_many` calls of [`INSERT_MANY_DOCUMENTS`] documents,
//! each one session transaction with the record's progress write: the
//! transaction first writes the record (`find_one_and_update` on the
//! target's `_id`, `table_id`, `restore_job.status = RUNNING` and
//! `restore_job.owner_generation` equal to the claim's, setting
//! `restore_job.file_index`, `restore_job.items_applied` to the file's
//! running total and `heartbeat_at` to the server's clock), and ends the
//! file as a lost claim when that write matches nothing; then inserts the
//! batch in the same transaction and commits. The record write is the
//! fence: a takeover is a write on the same document, so a writer whose
//! record another instance took over while it stalled finds its transaction
//! aborted for the write conflict (re-run, it meets the other generation and
//! stops) or its record write matching nothing, and no batch of it lands
//! after the takeover, as PostgreSQL's and SQLite's loads condition their
//! writes. The progress is per batch and in the batch's transaction, so a
//! job that dies leaves no documents its record does not account for: a
//! resume starts after `items_applied`, and `BulkLoadFile::reapplying` is
//! not read (nothing of a batch lands without its record). A duplicate key
//! (E11000, the data collection's unique `_id`) is the backup's and fails
//! the file, as any other write error does. The record carries the
//! progress, so `read_restore_progress` and `sweep_restore_progress` keep
//! their defaults.

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement};
use extenddb_storage::backup_catalog::{
    JobClaim, JobStatus, RESTORE_MONGODB_AFTER_INSERT_MANY, RestoreTargetKey, restore_pause_point,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mongodb::bson::{self, Document, doc};

use crate::MongoEngine;
use crate::backup_catalog::{
    RESTORE_JOB_FILE_INDEX, RESTORE_JOB_GENERATION, RESTORE_JOB_HEARTBEAT,
    RESTORE_JOB_ITEMS_APPLIED, RESTORE_JOB_STATUS, generation_field, literal_set,
};
use crate::data::{data_collection_name, item_to_document};
use crate::data_engine::{CommitOutcome, TRANSIENT_RETRY_ATTEMPTS, TxErr, backoff_sleep};

/// Documents per `insert_many` call. Not a message bound: 1,000 documents at
/// the item size limit are about 390 MiB, and the driver splits one call by
/// the server's message and write-batch bounds on its own. It is the unit a
/// half-applied file is re-applied in, and the unit between two reads of the
/// record's claim.
pub(crate) const INSERT_MANY_DOCUMENTS: usize = 1000;

/// The server's duplicate-key error code.
const DUPLICATE_KEY: i32 = 11000;

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

/// The key shape of a CREATING restore target, read from its catalog
/// document.
struct TargetShape {
    key_schema: Vec<KeySchemaElement>,
    attribute_definitions: Vec<AttributeDefinition>,
}

impl MongoEngine {
    /// The target's catalog document, when `key` names a CREATING restore
    /// target with this table id.
    async fn restore_target_doc(&self, key: &RestoreTargetKey) -> Result<Document, StorageError> {
        self.catalog_db
            .collection::<Document>("tables")
            .find_one(doc! {
                "_id": { "account_id": &key.account_id, "table_name": &key.table_name },
                "table_id": &key.table_id,
                "table_status": "CREATING",
                "restore_source_backup_arn": { "$exists": true },
            })
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                StorageError::TableNotFound(format!(
                    "{} is not a CREATING restore target with table id {}",
                    key.table_name, key.table_id
                ))
            })
    }

    /// Write `batch` into the target's data collection in one transaction
    /// with the record's progress (module doc) and clear it; the documents
    /// the server took. `applied_total` is the file's running total once
    /// the batch lands (`start_at` plus every batch of this call). `Err` on
    /// a lost claim (the record is not RUNNING under `generation`, or a
    /// takeover's write conflicted with this transaction), on a duplicate
    /// key (the backup's), and on any other error.
    async fn insert_restore_batch(
        &self,
        collection: &mongodb::Collection<Document>,
        target: &RestoreTargetKey,
        generation: u64,
        file_index: u32,
        applied_total: u64,
        batch: &mut Vec<Document>,
    ) -> Result<u64, StorageError> {
        let tables = self.catalog_db.collection::<Document>("tables");
        let fence = doc! {
            "_id": { "account_id": &target.account_id, "table_name": &target.table_name },
            "table_id": &target.table_id,
            RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
            RESTORE_JOB_GENERATION: generation_field(generation),
        };
        let progress = vec![doc! { "$set": literal_set(doc! {
            RESTORE_JOB_FILE_INDEX: i64::from(file_index),
            RESTORE_JOB_ITEMS_APPLIED: i64::try_from(applied_total).unwrap_or(i64::MAX),
            RESTORE_JOB_HEARTBEAT: "$$NOW",
        }) }];
        let lost = || {
            StorageError::Internal(format!(
                "restore of table {}: the claim on the target was lost while a data file \
                 loaded (the record is not RUNNING under generation {generation}); the \
                 record's owner finishes it",
                target.table_name
            ))
        };
        let mut session = self.client.start_session().await.map_err(internal)?;
        let tx_options = self.write_transaction_options();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(internal)?;
            let body: Result<(), TxErr> = async {
                let fenced = tables
                    .find_one_and_update(fence.clone(), progress.clone())
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                if fenced.is_none() {
                    return Err(TxErr::Fatal(lost()));
                }
                if batch.is_empty() {
                    return Ok(());
                }
                collection
                    .insert_many(batch.iter().cloned())
                    .session(&mut session)
                    .await
                    .map_err(|e| {
                        if is_duplicate_key_anywhere(&e) {
                            // The engine prefixes the file's key, so the
                            // file is not named here.
                            TxErr::Fatal(StorageError::Validation(format!(
                                "holds a primary key the target already has (the backup holds \
                                 a key twice): {e}"
                            )))
                        } else {
                            TxErr::from(e)
                        }
                    })?;
                Ok(())
            }
            .await;
            match body {
                Ok(()) => match self.commit_write_transaction(&mut session, None).await? {
                    CommitOutcome::Committed => {
                        let sent = batch.len();
                        batch.clear();
                        return Ok(u64::try_from(sent).unwrap_or(u64::MAX));
                    }
                    CommitOutcome::RerunBody(_) => backoff_sleep(attempt).await,
                },
                Err(TxErr::Transient(_)) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }
        Err(StorageError::Internal(format!(
            "restore of table {}: a batch of data file {file_index} did not commit after \
             {TRANSIENT_RETRY_ATTEMPTS} conflicting attempts",
            target.table_name
        )))
    }
}

/// Whether `e` carries the server's duplicate-key code, as an insert's
/// write error, a bulk write's, or a command error inside a transaction.
fn is_duplicate_key_anywhere(e: &mongodb::error::Error) -> bool {
    match &*e.kind {
        mongodb::error::ErrorKind::InsertMany(failure) => failure
            .write_errors
            .as_ref()
            .is_some_and(|errors| errors.iter().any(|w| w.code == DUPLICATE_KEY)),
        _ => crate::data_engine::is_duplicate_key(e),
    }
}

impl RestoreEngine for MongoEngine {
    fn bulk_load<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        file: BulkLoadFile,
        mut items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            use futures::StreamExt;

            // The shape from the target's catalog document; each batch's
            // transaction writes the record under the claim before its
            // inserts (`insert_restore_batch`), which is the fence.
            let generation = claim.generation();
            let table_doc = self.restore_target_doc(target).await?;
            let shape = TargetShape {
                key_schema: bson::from_bson(
                    table_doc
                        .get("key_schema")
                        .cloned()
                        .ok_or_else(|| internal("restore target is missing key_schema"))?,
                )
                .map_err(|e| StorageError::Internal(format!("key schema: {e}")))?,
                attribute_definitions: bson::from_bson(
                    table_doc
                        .get("attribute_definitions")
                        .cloned()
                        .ok_or_else(|| {
                            internal("restore target is missing attribute_definitions")
                        })?,
                )
                .map_err(|e| StorageError::Internal(format!("attribute definitions: {e}")))?,
            };
            let collection = self
                .data_db
                .collection::<Document>(&data_collection_name(&target.table_id));
            // The test-hook gate the in-catalog copy also honours: it holds
            // the restore after the target's CREATING index metadata exists
            // and before the first insert of its first file, so a test can
            // run a backfill worker tick through that window and see that
            // the worker completes no index of a restore whose base load has
            // not started. Inert without the `test-hooks` feature and unless
            // a test armed it.
            if file.file_index == 0 && file.start_at == 0 {
                self.wait_for_gsi_backfill_test_gate(&target.table_name)
                    .await?;
            }

            let mut batch: Vec<Document> = Vec::with_capacity(INSERT_MANY_DOCUMENTS);
            let mut applied: u64 = 0;
            while let Some(item) = items.next().await {
                let item = item?;
                batch.push(item_to_document(
                    &item,
                    &shape.key_schema,
                    &shape.attribute_definitions,
                )?);
                if batch.len() == INSERT_MANY_DOCUMENTS {
                    let total = file.start_at + applied + batch.len() as u64;
                    applied += self
                        .insert_restore_batch(
                            &collection,
                            target,
                            generation,
                            file.file_index,
                            total,
                            &mut batch,
                        )
                        .await?;
                    restore_pause_point(RESTORE_MONGODB_AFTER_INSERT_MANY).await;
                }
            }
            // The file's last batch, or, for a file with no item left to
            // apply, the file's record alone (the fence write with the
            // running total and no insert).
            let total = file.start_at + applied + batch.len() as u64;
            applied += self
                .insert_restore_batch(
                    &collection,
                    target,
                    generation,
                    file.file_index,
                    total,
                    &mut batch,
                )
                .await?;
            Ok(applied)
        })
    }

    fn finish_restore<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        Box::pin(async move {
            let handed_over = self
                .hand_over_to_backfill(target, claim.generation(), item_count, Some(size_bytes))
                .await?;
            if handed_over {
                Ok(Finished::Pending)
            } else {
                Ok(Finished::NotMatched(format!(
                    "the job record of {} is not RUNNING under generation {}, or the table is \
                     not CREATING with table id {}",
                    target.table_name,
                    claim.generation(),
                    target.table_id
                )))
            }
        })
    }

    fn count_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // The id names the data collection; the key's account and name
            // are checked against the catalog so a bare id reads no other
            // account's table.
            self.restore_target_doc(key).await?;
            self.data_db
                .collection::<Document>(&data_collection_name(&key.table_id))
                .count_documents(doc! {})
                .await
                .map_err(internal)
        })
    }

    fn restore_target_ready<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The record's `ddl_done` marker, set after the last index
            // metadata document and data collection were created
            // (`create_table_impl_inner`); a record without it is one whose
            // owner died between the registration's commit and the end of
            // its DDL, and its collections and indexes are not the
            // registration's. The catalog document is read first, so a bare
            // id reads no other account's table.
            let table_doc = self.restore_target_doc(key).await?;
            Ok(table_doc
                .get_document(crate::backup_catalog::RESTORE_JOB)
                .ok()
                .and_then(|job| job.get_bool("ddl_done").ok())
                .unwrap_or(false))
        })
    }
}
