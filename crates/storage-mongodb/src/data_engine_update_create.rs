// SPDX-License-Identifier: Apache-2.0

//! The native UpdateItem upsert, which creates the item when it does not
//! exist yet.
//!
//! `update_item_impl` runs an unconditional single-attribute update on a
//! table without indexes as one `findAndModify` with `upsert: true`, filtered
//! on the key fields `pk` and `sk_*`. When no document matches, MongoDB builds
//! the inserted document from those filter fields and the update operators.
//! Without more, that document gets a generated ObjectId `_id` and an
//! `item_data` without the key attributes, while PutItem writes `_id` as the
//! key text and the whole item (`item_to_document`). Every later PutItem on
//! the key then fails on the immutable `_id` (issue #436).
//! [`MongoEngine::upsert_native_update`] adds a `$setOnInsert` with the `_id`
//! and the key attributes, so the created document is the one PutItem would
//! have written, with `_v` 1 from the update's `$inc`.
//!
//! Two writers creating the same item at once each find no document and each
//! insert; the second insert fails with a duplicate-key error on `_id`. The
//! data collection also has a unique index on the key fields, and MongoDB
//! retries an upsert itself only when the error names an index whose fields
//! the filter matches by equality. The `_id` collision is reported first and
//! the filter does not name `_id`, so the server does not retry it and this
//! module does: the item now exists, so the next attempt updates it.

use bson::Document;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};

use super::{TRANSIENT_RETRY_ATTEMPTS, backoff_sleep};
use crate::MongoEngine;
use crate::data::key_fields::insert_only_key_fields;

/// Whether `e` is a duplicate-key error (E11000) as `findAndModify` reports
/// it: a command error, where `insert_one` reports a write error.
fn is_duplicate_key_command_error(e: &mongodb::error::Error) -> bool {
    matches!(*e.kind, mongodb::error::ErrorKind::Command(ref c) if c.code == 11000)
}

impl MongoEngine {
    /// Apply the native `update` (a `$set`/`$unset`/`$inc` operator document)
    /// to the item at `key_filter`, creating the item when it does not exist,
    /// and return the document after the update.
    ///
    /// `key` and `key_info` supply the `$setOnInsert` fields that make a
    /// created document match what PutItem writes. Attempts that lose the
    /// create race share the transient retry budget and back off between
    /// them; when every attempt loses, the error is the same
    /// `TransactionConflict` the transaction path returns.
    pub(crate) async fn upsert_native_update(
        &self,
        coll: &mongodb::Collection<Document>,
        key_filter: &Document,
        mut update: Document,
        key: &Item,
        key_info: &TableKeyInfo,
    ) -> Result<Option<Document>, StorageError> {
        update.insert(
            "$setOnInsert",
            insert_only_key_fields(key, &key_info.key_schema, &key_info.attribute_definitions)?,
        );
        let opts = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            match coll
                .find_one_and_update(key_filter.clone(), update.clone())
                .with_options(opts.clone())
                .await
            {
                Ok(doc) => return Ok(doc),
                Err(e) if is_duplicate_key_command_error(&e) => backoff_sleep(attempt).await,
                Err(e) => return Err(StorageError::Internal(e.to_string())),
            }
        }
        Err(StorageError::TransactionConflict(
            "UpdateItem: too many concurrent write conflicts, giving up".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn command_error(code: i32) -> mongodb::error::Error {
        let command_error: mongodb::error::CommandError = bson::from_document(doc! {
            "code": code,
            "codeName": "SyntheticTestError",
            "errmsg": "synthetic command error",
        })
        .expect("static CommandError document deserializes");
        mongodb::error::ErrorKind::Command(command_error).into()
    }

    fn write_error(code: i32) -> mongodb::error::Error {
        let write_error: mongodb::error::WriteError = bson::from_document(doc! {
            "code": code,
            "codeName": "SyntheticTestError",
            "errmsg": "synthetic write error",
        })
        .expect("static WriteError document deserializes");
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error))
            .into()
    }

    #[test]
    fn only_the_command_form_of_e11000_is_a_lost_create_race() {
        assert!(is_duplicate_key_command_error(&command_error(11000)));
        assert!(!is_duplicate_key_command_error(&command_error(112)));
        assert!(!is_duplicate_key_command_error(&write_error(11000)));
    }
}
