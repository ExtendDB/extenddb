// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream lifecycle on MongoDB: creating a table's stream, closing it on
//! `UpdateTable StreamEnabled=false`, and reading the table and shard
//! documents the shared rules in `extenddb_storage::stream_lineage` group into
//! streams.
//!
//! The table document is in the catalog database and the shards and records
//! in the data database. `UpdateTable` sets the specification and label on the
//! table document first and changes the shards after that write
//! ([`PendingStream`]), the order the PostgreSQL backend uses for the same
//! two-database split. A failure between the two leaves the table document's
//! answer in force. A disable whose close failed leaves a disabled stream whose
//! shards are still open: a retried disable is refused and closes them, and an
//! enable closes them too, since an enable closes the open shards of every
//! earlier stream of the table. An enable whose shard insert failed leaves an
//! enabled stream with no shards, which records nothing and is replaced by a
//! disable and an enable.

use futures::TryStreamExt;
use mongodb::bson::DateTime as BsonDateTime;
use mongodb::bson::{self, Bson, Document, doc};
use mongodb::options::FindOptions;

use extenddb_core::types::{StreamSpecification, StreamViewType};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{
    SHARDS_PER_STREAM, ShardRow, StreamChange, TableStreamRow, disabled_stream_to_close,
    enable_still_current, next_stream_label, open_shard_ids_before, open_shard_ids_of, shard_id,
    stored_specification, stream_change,
};

use crate::MongoEngine;

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn spec_of(doc: &Document) -> Option<StreamSpecification> {
    doc.get("stream_specification")
        .and_then(|b| bson::from_bson(b.clone()).ok())
}

fn table_row(table_name: &str, doc: &Document) -> TableStreamRow {
    TableStreamRow {
        table_name: table_name.to_owned(),
        table_id: doc.get_str("table_id").unwrap_or_default().to_owned(),
        table_status: doc.get_str("table_status").unwrap_or("ACTIVE").to_owned(),
        stream_label: doc
            .get_str("stream_label")
            .ok()
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
        stream_specification: spec_of(doc),
    }
}

/// The shard changes of an applied `UpdateTable` stream change: on a disable,
/// close the open shards of the stream the change read (`close`, the table's
/// label at that time); on an enable, close the open shards of every earlier
/// stream, then create the new stream's shards.
#[must_use]
pub(crate) struct PendingStream {
    table_id: String,
    close: Option<String>,
    open: Option<(String, StreamViewType)>,
}

impl PendingStream {
    /// Run the shard changes once the table document is written.
    ///
    /// The enable's shards are inserted after its table document is visible,
    /// so a disable that reads the document in between passes its check,
    /// writes the disabled specification and closes the shards it finds, which
    /// may be none or some of the four. The enable re-reads the table's stream
    /// fields after its inserts and, when the document no longer says enabled
    /// with this label, closes the shards it inserted. A disable written
    /// before the re-read is seen by it; one written after finds all four
    /// shards open and closes them itself; both closing is idempotent through
    /// the `ending_sequence_number: null` filter. A disable closes only the
    /// stream it read, so one applying after an overlapping enable leaves the
    /// new stream open.
    pub(crate) async fn apply(
        self,
        engine: &MongoEngine,
        account_id: &str,
        table_name: &str,
    ) -> Result<(), StorageError> {
        let Some((label, view_type)) = self.open else {
            if let Some(label) = &self.close {
                engine.close_stream_shards(&self.table_id, label).await?;
            }
            return Ok(());
        };
        engine.close_streams_before(&self.table_id, &label).await?;
        engine
            .init_stream_shards(&self.table_id, &label, view_type)
            .await?;
        let row = engine.stream_table_row(account_id, table_name).await?;
        if !enable_still_current(row.as_ref().map(|(t, _)| t), &label) {
            engine.close_stream_shards(&self.table_id, &label).await?;
        }
        Ok(())
    }
}

impl MongoEngine {
    /// Insert the four open shards of stream `label`. The caller stores the
    /// label on the table document.
    pub(crate) async fn init_stream_shards(
        &self,
        table_id: &str,
        label: &str,
        view_type: StreamViewType,
    ) -> Result<(), StorageError> {
        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        for i in 0..SHARDS_PER_STREAM {
            let insert_res = shards_coll
                .insert_one(doc! {
                    "shard_id": shard_id(table_id, label, view_type, i)?,
                    "table_id": table_id,
                    "starting_sequence_number": format!("{:021}", 0),
                    "created_at": BsonDateTime::now(),
                })
                .await;
            if let Err(e) = insert_res {
                // A duplicate `shard_id` (E11000 on the unique index the
                // bootstrapper creates) is a repeat of the same insert, from
                // a retried request: keep the shard that is there.
                let is_dup = matches!(
                    *e.kind,
                    mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                        mongodb::error::WriteError { code: 11000, .. }
                    ))
                );
                if !is_dup {
                    return Err(internal(e));
                }
            }
        }
        Ok(())
    }

    /// Close every open shard of the table: its `EndingSequenceNumber` becomes
    /// the sequence number of its last record, or its starting number when it
    /// holds none (both measured 2026-10-24).
    pub(crate) async fn close_stream_shards(
        &self,
        table_id: &str,
        label: &str,
    ) -> Result<(), StorageError> {
        self.close_selected_shards(table_id, label, open_shard_ids_of)
            .await
    }

    /// Close the open shards of every stream of the table older than `label`
    /// (`stream_lineage::open_shard_ids_before`), as [`Self::close_stream_shards`]
    /// closes one stream's.
    async fn close_streams_before(&self, table_id: &str, label: &str) -> Result<(), StorageError> {
        self.close_selected_shards(table_id, label, open_shard_ids_before)
            .await
    }

    async fn close_selected_shards(
        &self,
        table_id: &str,
        label: &str,
        select: fn(&str, &str, &[ShardRow]) -> Vec<String>,
    ) -> Result<(), StorageError> {
        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        let records_coll = self.data_db.collection::<Document>("stream_records");
        let rows = self
            .stream_shard_rows(std::slice::from_ref(&table_id.to_owned()))
            .await?;
        let ids = select(table_id, label, &rows);
        if ids.is_empty() {
            return Ok(());
        }
        let open: Vec<Document> = shards_coll
            .find(doc! { "shard_id": { "$in": &ids }, "ending_sequence_number": Bson::Null })
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        for shard in open {
            let id = shard.get_str("shard_id").map_err(internal)?;
            let last: Option<Document> = records_coll
                .find(doc! { "shard_id": id })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "sequence_number": -1 })
                        .limit(1)
                        .build(),
                )
                .await
                .map_err(internal)?
                .try_next()
                .await
                .map_err(internal)?;
            let ending = match last
                .as_ref()
                .and_then(|d| d.get_str("sequence_number").ok())
            {
                Some(seq) => seq.to_owned(),
                None => shard
                    .get_str("starting_sequence_number")
                    .map_err(internal)?
                    .to_owned(),
            };
            shards_coll
                .update_one(
                    doc! { "shard_id": id, "ending_sequence_number": Bson::Null },
                    doc! { "$set": { "ending_sequence_number": ending } },
                )
                .await
                .map_err(internal)?;
        }
        Ok(())
    }

    /// Decide an `UpdateTable` `StreamSpecification` against the table
    /// document and add its catalog fields to `update_doc`.
    ///
    /// Disabling stores `{StreamEnabled: false}` with the stream's view type,
    /// which DescribeTable omits, and keeps `stream_label`, so
    /// `LatestStreamArn` keeps naming the disabled stream. Enabling stores the specification and
    /// a new label. The returned shard changes run once `update_doc` is
    /// written. This backend has no transaction around UpdateTable, so two
    /// concurrent stream changes can both pass the check.
    pub(crate) fn prepare_stream_update(
        update_doc: &mut Document,
        table_doc: &Document,
        table_name: &str,
        requested: &StreamSpecification,
    ) -> Result<PendingStream, StorageError> {
        let table = table_row(table_name, table_doc);
        let change = stream_change(table_name, table.stream_specification.as_ref(), requested)?;
        update_doc.insert(
            "stream_specification",
            bson::to_bson(&stored_specification(
                change,
                table.stream_specification.as_ref(),
                requested,
            ))
            .map_err(internal)?,
        );
        let open = match change {
            StreamChange::Disable => None,
            StreamChange::Enable(view_type) => {
                let label = next_stream_label(table.stream_label.as_deref());
                update_doc.insert("stream_label", &label);
                Some((label, view_type))
            }
        };
        Ok(PendingStream {
            table_id: table.table_id,
            close: table.stream_label,
            open,
        })
    }

    /// [`Self::prepare_stream_update`], and for a disable refused because the
    /// stored specification is already disabled, the close of the current
    /// label's open shards first (left by a release that did not close them,
    /// or by a close that failed after the catalog write); the refusal is
    /// still the answer.
    pub(crate) async fn prepare_stream_update_or_close(
        &self,
        update_doc: &mut Document,
        table_doc: &Document,
        table_name: &str,
        requested: &StreamSpecification,
    ) -> Result<PendingStream, StorageError> {
        match Self::prepare_stream_update(update_doc, table_doc, table_name, requested) {
            Ok(pending) => Ok(pending),
            Err(refusal) => {
                let table = table_row(table_name, table_doc);
                if let Some(label) = disabled_stream_to_close(
                    table.stream_specification.as_ref(),
                    table.stream_label.as_deref(),
                    requested,
                ) {
                    self.close_stream_shards(&table.table_id, label).await?;
                }
                Err(refusal)
            }
        }
    }

    /// The table's stream fields and its document, by name.
    pub(crate) async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(TableStreamRow, Document)>, StorageError> {
        let doc = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .await
            .map_err(internal)?;
        Ok(doc.map(|d| (table_row(table_name, &d), d)))
    }

    /// Every table of the account that has had a stream.
    pub(crate) async fn stream_table_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<TableStreamRow>, StorageError> {
        let docs: Vec<Document> = self
            .catalog_db
            .collection::<Document>("tables")
            .find(doc! { "_id.account_id": account_id, "stream_label": { "$ne": Bson::Null } })
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        Ok(docs
            .iter()
            .filter_map(|d| {
                let name = d.get_document("_id").ok()?.get_str("table_name").ok()?;
                Some(table_row(name, d))
            })
            .collect())
    }

    /// The shard documents of the given tables.
    pub(crate) async fn stream_shard_rows(
        &self,
        table_ids: &[String],
    ) -> Result<Vec<ShardRow>, StorageError> {
        if table_ids.is_empty() {
            return Ok(Vec::new());
        }
        let docs: Vec<Document> = self
            .data_db
            .collection::<Document>("stream_shards")
            .find(doc! { "table_id": { "$in": table_ids } })
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        Ok(docs
            .iter()
            .filter_map(|d| {
                Some(ShardRow {
                    table_id: d.get_str("table_id").ok()?.to_owned(),
                    shard_id: d.get_str("shard_id").ok()?.to_owned(),
                    parent_shard_id: d.get_str("parent_shard_id").ok().map(str::to_owned),
                    starting_sequence_number: d
                        .get_str("starting_sequence_number")
                        .ok()?
                        .to_owned(),
                    ending_sequence_number: d
                        .get_str("ending_sequence_number")
                        .ok()
                        .map(str::to_owned),
                })
            })
            .collect())
    }
}

#[cfg(test)]
#[path = "stream_live_tests.rs"]
mod live_tests;
