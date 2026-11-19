// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored-secret documents as the encryption-key commands read and write
//! them (RFC-0000 section 8.4), for `MongoCatalogStore`.
//!
//! A page is a `find` in `_id` order with the last `_id` as the page token,
//! and the `_id` hex is the row key the writes take back: nothing makes
//! `access_key_id` unique before migration 12, and a session's `_id` is not
//! its token. Each write is one `update_one` on the `_id` with no upsert,
//! under write concern `majority`, so a document the TTL index on
//! `iam_sessions.expires_at` deleted during the pass stays deleted.

use futures::TryStreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Binary, Document, doc};

use extenddb_storage::management_store::{
    OpError, OpResult, SecretPage, SecretWrite, StoredSecretRow,
};
use extenddb_storage::secret_keys::SecretTable;

use crate::catalog_store::MongoCatalogStore;
use crate::management_store::to_offset_dt;

/// The updates of one page in flight at once. Each waits for a majority of
/// the replica set, and the migration backend's store, which the offline
/// commands go through, bounds the whole page by its request timeout.
const WRITE_CONCURRENCY: usize = 8;

/// A driver error, logged with its context and answered as a database error.
fn database_error(context: &str, e: &dyn std::fmt::Display) -> OpError {
    tracing::error!("{context}: {e}");
    OpError::Internal("Database error".to_owned())
}

/// The `_id` of a stored-secret document as its row key: the hex of the
/// `ObjectId` the driver gave the document on insert. Refused for an `_id`
/// of another type, which no writer of these collections produces.
fn row_key(d: &Document, context: &str) -> OpResult<String> {
    d.get_object_id("_id").map(|id| id.to_hex()).map_err(|e| {
        database_error(
            context,
            &format!("the document _id is not an ObjectId: {e}"),
        )
    })
}

/// The `_id` a row key names.
fn object_id(key: &str, context: &str) -> OpResult<ObjectId> {
    ObjectId::parse_str(key)
        .map_err(|e| database_error(context, &format!("the row key is not an ObjectId: {e}")))
}

/// A required string field of a stored-secret document.
fn field(d: &Document, name: &str, context: &str) -> OpResult<String> {
    d.get_str(name)
        .map(str::to_owned)
        .map_err(|e| database_error(context, &format!("field {name}: {e}")))
}

/// One document of a page as a [`StoredSecretRow`].
fn stored_secret_row(table: SecretTable, d: &Document) -> OpResult<StoredSecretRow> {
    const WHAT: &str = "read_secret_page";
    let (principal_name, session_name, expires_at) = match table {
        SecretTable::AccessKeys => (field(d, "user_name", WHAT)?, None, None),
        SecretTable::IamSessions => (
            field(d, "role_name", WHAT)?,
            Some(field(d, "session_name", WHAT)?),
            d.get_datetime("expires_at")
                .ok()
                .map(|expires| to_offset_dt(*expires).unix_timestamp()),
        ),
    };
    let secret_key_encrypted = d
        .get_binary_generic("secret_key_encrypted")
        .map_err(|e| database_error(WHAT, &format!("field secret_key_encrypted: {e}")))?
        .clone();
    Ok(StoredSecretRow {
        table,
        key: row_key(d, WHAT)?,
        access_key_id: field(d, "access_key_id", WHAT)?,
        account_id: field(d, "account_id", WHAT)?,
        principal_name,
        session_name,
        expires_at,
        created_at: d
            .get_datetime("created_at")
            .ok()
            .map(|created| to_offset_dt(*created)),
        secret_key_encrypted,
        write_timestamp: None,
    })
}

impl MongoCatalogStore {
    /// The collection of `table` on the catalog database.
    fn secrets(&self, table: SecretTable) -> mongodb::Collection<Document> {
        self.catalog_db().collection::<Document>(table.name())
    }

    /// `w: majority` for the writes of the pass, keeping the client's
    /// `wtimeout` and `journal`.
    fn majority(&self) -> mongodb::options::WriteConcern {
        crate::majority_write_concern(self.catalog_db().write_concern())
    }

    pub(crate) async fn read_secret_page_impl(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> OpResult<SecretPage> {
        const WHAT: &str = "read_secret_page";
        let filter = match page {
            Some(last) => doc! { "_id": { "$gt": object_id(last, WHAT)? } },
            None => doc! {},
        };
        let docs: Vec<Document> = self
            .secrets(table)
            .find(filter)
            .sort(doc! { "_id": 1 })
            .limit(i64::try_from(page_size).unwrap_or(i64::MAX))
            .await
            .map_err(|e| database_error(WHAT, &e))?
            .try_collect()
            .await
            .map_err(|e| database_error(WHAT, &e))?;
        let rows = docs
            .iter()
            .map(|d| stored_secret_row(table, d))
            .collect::<OpResult<Vec<_>>>()?;
        // A full page may be the last one; the next read then finds nothing.
        let next = (rows.len() == page_size)
            .then(|| rows.last().map(|row| row.key.clone()))
            .flatten();
        Ok(SecretPage { rows, next })
    }

    pub(crate) async fn write_secrets_impl(
        &self,
        table: SecretTable,
        rows: &[SecretWrite],
    ) -> OpResult<()> {
        use futures::StreamExt as _;
        const WHAT: &str = "write_secrets";
        let coll = &self.secrets(table);
        let majority = &self.majority();
        let updates = rows
            .iter()
            .map(|row| Ok((object_id(&row.key, WHAT)?, row.secret_key_encrypted.clone())))
            .collect::<OpResult<Vec<_>>>()?;
        // A document that is gone matches nothing, which is not an error.
        let mut writes = futures::stream::iter(updates)
            .map(|(id, bytes)| async move {
                let ciphertext = Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes,
                };
                coll.update_one(
                    doc! { "_id": id },
                    doc! { "$set": { "secret_key_encrypted": ciphertext } },
                )
                .write_concern(majority.clone())
                .await
            })
            .buffer_unordered(WRITE_CONCURRENCY);
        while let Some(result) = writes.next().await {
            result.map_err(|e| database_error(WHAT, &e))?;
        }
        Ok(())
    }

    pub(crate) async fn delete_secrets_impl(
        &self,
        table: SecretTable,
        keys: &[String],
    ) -> OpResult<()> {
        const WHAT: &str = "delete_secrets";
        if keys.is_empty() {
            return Ok(());
        }
        let ids = keys
            .iter()
            .map(|key| object_id(key, WHAT))
            .collect::<OpResult<Vec<_>>>()?;
        self.secrets(table)
            .delete_many(doc! { "_id": { "$in": ids } })
            .write_concern(self.majority())
            .await
            .map_err(|e| database_error(WHAT, &e))?;
        Ok(())
    }

    pub(crate) async fn delete_setting_impl(&self, key: &str) -> OpResult<()> {
        self.catalog_db()
            .collection::<Document>("settings")
            .delete_one(doc! { "_id": key })
            .write_concern(self.majority())
            .await
            .map_err(|e| database_error("delete_setting", &e))?;
        Ok(())
    }

    /// One `update_one` whose filter carries `expected`, with no upsert.
    pub(crate) async fn replace_setting_impl(
        &self,
        key: &str,
        expected: &str,
        value: &str,
    ) -> OpResult<bool> {
        let done = self
            .catalog_db()
            .collection::<Document>("settings")
            .update_one(
                doc! { "_id": key, "value": expected },
                doc! { "$set": { "value": value } },
            )
            .write_concern(self.majority())
            .await
            .map_err(|e| database_error("replace_setting", &e))?;
        Ok(done.matched_count == 1)
    }
}
