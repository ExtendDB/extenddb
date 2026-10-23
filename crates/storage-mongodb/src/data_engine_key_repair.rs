// SPDX-License-Identifier: Apache-2.0

//! Startup repair of base-table documents stored without their key.
//!
//! Before issue #436 was fixed, an UpdateItem that created an item on the
//! native fast path upserted a document with a generated ObjectId `_id` and
//! an `item_data` without the key attributes. Such a document answers
//! GetItem without the key, and every PutItem on the key fails on the
//! immutable `_id`. `_id` cannot be changed in place, so the repair replaces
//! each such document with the one `item_to_document` writes, in one
//! transaction: delete the old document by its ObjectId, insert the rebuilt
//! one. The scan is a bracket query on the `_id` index
//! (`{_id: {$type: "objectId"}}`), so a collection with no such document costs
//! one index probe.
//!
//! `serve` runs this once at startup, before the server accepts requests
//! (see `server_components_factory` in `lib.rs`). A failure on one document
//! is logged and skipped; the summary reports it.

use bson::{Document, doc};
use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
use extenddb_storage::error::StorageError;
use futures::TryStreamExt;

use super::{CommitOutcome, TRANSIENT_RETRY_ATTEMPTS, backoff_sleep, is_transient_write_conflict};
use crate::MongoEngine;
use crate::data::data_collection_name;
use crate::data::key_fields::{is_stored_without_key, repaired_document};

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
        while let Some(bad) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            if !is_stored_without_key(&bad) {
                continue;
            }
            let repaired = match repaired_document(&bad, key_schema, attribute_definitions) {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(
                        "key repair: table {table_id}: cannot rebuild document {:?}: {e}",
                        bad.get("_id")
                    );
                    summary.documents_failed += 1;
                    continue;
                }
            };
            match self.replace_document(&coll, &bad, repaired).await {
                Ok(true) => summary.documents_repaired += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        "key repair: table {table_id}: cannot rewrite document {:?}: {e}",
                        bad.get("_id")
                    );
                    summary.documents_failed += 1;
                }
            }
        }
        Ok(())
    }

    /// Delete `bad` by its `_id` and insert `repaired` in one transaction.
    /// Returns false when `bad` was gone already (another process repaired or
    /// deleted it), in which case nothing is inserted.
    async fn replace_document(
        &self,
        coll: &mongodb::Collection<Document>,
        bad: &Document,
        repaired: Document,
    ) -> Result<bool, StorageError> {
        let bad_id = bad
            .get("_id")
            .cloned()
            .ok_or_else(|| StorageError::Internal("document has no _id".to_owned()))?;
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
            let body = async {
                let deleted = coll
                    .delete_one(doc! { "_id": bad_id.clone() })
                    .session(&mut session)
                    .await?;
                if deleted.deleted_count == 0 {
                    return Ok::<bool, mongodb::error::Error>(false);
                }
                coll.insert_one(repaired.clone())
                    .session(&mut session)
                    .await?;
                Ok(true)
            }
            .await;
            match body {
                Ok(false) => {
                    let _ = session.abort_transaction().await;
                    return Ok(false);
                }
                Ok(true) => match self.commit_write_transaction(&mut session, None).await? {
                    CommitOutcome::Committed => return Ok(true),
                    CommitOutcome::RerunBody => backoff_sleep(attempt).await,
                },
                Err(e) if is_transient_write_conflict(&e) => {
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
