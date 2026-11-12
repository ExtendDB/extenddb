// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 11, `unique_access_key_ids`, MongoDB: give the catalog's
//! `access_keys` collection a unique index on `access_key_id`.
//!
//! PostgreSQL, SQLite and Cassandra key `access_keys` by the ID. Migration 1
//! gives MongoDB's collection only a non-unique index on `account_id` and
//! `user_name`, so before this migration every import stored a document,
//! including one under an ID that another document held, and a credential
//! lookup, `find_one` on the ID, read whichever of them its query plan met
//! first.
//!
//! One aggregation scans the collection in natural order and groups the
//! documents by ID, keeping the IDs that more than one document holds and
//! the array IDs. `detect` refuses what the body cannot decide: an ID whose
//! documents belong to more than one user, two or more documents that have
//! no ID or share an ID that is not a string, and a document whose ID is an
//! array, which a lookup reads for each ID in it. `run` reads again and
//! refuses the same before it writes. Then, for each ID that one user holds
//! more than once, it finds the copy a lookup reads, logs the other copies,
//! deletes them and keeps that copy, so every key authenticates as it did
//! before. Last it creates the index, committed once a majority of the
//! replica set's data-bearing voting members have built it.
//!
//! A lookup reads the copy its query plan meets first. For each ID stored
//! more than once the body reads which of its listed copies still hold it,
//! with their record IDs, and sends `explain` for the lookup's command with
//! that ID. When the plan is a forward collection scan, as it is with no
//! index that can answer the lookup and no query settings that change its
//! plan, the lookup reads the copy with the lowest record ID. With any other
//! plan the body sends the lookup's own query.
//!
//! Once the index is built no ID can be stored twice, so a later read
//! groups only the documents whose ID is an array. `listIndexes` also lists
//! an index whose build is running, and until the server reports the build
//! finished the read groups every document. Every request is safe to send
//! again: a second run finds nothing to delete, and creating an index that
//! exists with the same key and options succeeds.
//!
//! The checksum test pins this file once the migration is released. The
//! module is excluded from rustfmt and clippy so neither can require an
//! edit.

use async_trait::async_trait;
use bson::{Bson, Document, doc};
use extenddb_storage::migrations::{CatalogStatus, CodeBody, Done, MigrationError};
use futures::TryStreamExt;
use mongodb::IndexModel;
use mongodb::options::{CommitQuorum, Hint, IndexOptions};

use super::MongoStores;

/// This module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0011_unique_access_key_ids.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0011_unique_access_key_ids/mongodb/1";

/// The catalog collection this migration indexes.
const COLLECTION: &str = "access_keys";

/// The name of the index this migration creates.
const INDEX: &str = "access_key_id_1";

/// NamespaceNotFound: `listIndexes` on a collection that does not exist.
const NAMESPACE_NOT_FOUND: i32 = 26;

/// Migration 11's catalog body.
pub(crate) struct UniqueAccessKeyIds;

/// The documents that hold one value of `access_key_id`, when more than one
/// does or the value is an array.
struct Group {
    /// The value, as `$group` returned it: null for documents without one.
    key: Bson,
    /// Whether the value is an array.
    array: bool,
    /// Every document that holds it, in natural order.
    copies: Vec<Stored>,
}

/// What the body reads of one document.
struct Stored {
    id: Bson,
    account_id: Option<String>,
    user_name: Option<String>,
    created_at: Option<String>,
}

/// An index of `access_keys`, as `listIndexes` lists it.
struct Listed {
    spec: Document,
    /// Whether the server reports the index built. It also lists an index
    /// whose build is running, with the spec of a built one.
    built: bool,
}

/// What the record IDs of an ID's copies tell about the copy a forward
/// collection scan reads.
#[derive(Debug, PartialEq)]
enum Scanned {
    /// The `_id` of the copy with the lowest record ID.
    First(Bson),
    /// No listed copy holds the ID any more.
    Gone,
    /// A record ID is not an Int64, so the copies cannot be ordered by it.
    Unordered,
}

impl Stored {
    fn describe(&self) -> String {
        let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "(none)".to_owned());
        let created = self
            .created_at
            .as_deref()
            .map(|t| format!(", created {t}"))
            .unwrap_or_default();
        format!(
            "_id {} (account {}, user {}{created})",
            self.id,
            field(&self.account_id),
            field(&self.user_name),
        )
    }
}

/// `_id A, _id B` for `copies`.
fn ids(copies: &[&Stored]) -> String {
    copies.iter().map(|c| format!("_id {}", c.id)).collect::<Vec<_>>().join(", ")
}

/// The command that deletes one document of the collection in mongosh.
fn delete_command(db: &str) -> String {
    format!("db.getSiblingDB(\"{db}\").{COLLECTION}.deleteOne({{ _id: <its _id> }})")
}

impl Group {
    /// The ID, when it is a string: the only kind a lookup asks for.
    fn access_key_id(&self) -> Option<&str> {
        match &self.key {
            Bson::String(id) if !self.array => Some(id),
            _ => None,
        }
    }

    /// Whether every copy names the same account and user.
    fn one_owner(&self) -> bool {
        self.copies.windows(2).all(|pair| {
            pair[0].account_id == pair[1].account_id && pair[0].user_name == pair[1].user_name
        })
    }

    /// Whether `run` may keep the copy a lookup reads and delete the others.
    fn resolvable(&self) -> bool {
        self.access_key_id().is_some() && self.one_owner()
    }

    /// What `detect` tells the operator about a group the body does not
    /// resolve, with the `_id` a lookup reads when it is one of the copies:
    /// the problem, the reason and the action, then the documents and the
    /// command that deletes one of them, last so that it copies on its own.
    /// The runner's advice adds the command to run after the fix.
    fn refusal(&self, db: &str, read: Option<&Bson>) -> String {
        let n = self.copies.len();
        let copies = self.copies.iter().map(Stored::describe).collect::<Vec<_>>().join(", ");
        let tail = format!(
            "{copies}. To delete a document, run this command in mongosh: {}",
            delete_command(db)
        );
        let key = &self.key;
        if self.array {
            return if n == 1 {
                format!(
                    "a document in {db}.{COLLECTION} has the access key ID {key}, which is an \
                     array. When a request uses an access key ID in the array, ExtendDB can \
                     read this document to authenticate the request. This migration cannot \
                     decide which access key ID the document keeps. In mongosh, delete the \
                     document or set its access_key_id to one string. The document is: {tail}"
                )
            } else {
                format!(
                    "{n} documents in {db}.{COLLECTION} have the access key ID {key}, which is an \
                     array. When a request uses an access key ID in the array, ExtendDB can \
                     read one of these documents to authenticate the request. This migration \
                     cannot decide which access key ID each document keeps. In mongosh, delete \
                     each document or set its access_key_id to one string. The documents are: \
                     {tail}"
                )
            };
        }
        match (key, self.access_key_id()) {
            (_, Some(id)) => {
                let read = read
                    .map(|r| {
                        format!(
                            " When a request uses this access key ID, ExtendDB reads _id {r} to \
                             authenticate the request."
                        )
                    })
                    .unwrap_or_default();
                format!(
                    "{n} documents in {db}.{COLLECTION} have the access key ID {id}, and these \
                     documents belong to more than one user. This migration cannot decide which \
                     user is the key owner.{read} Delete each document that does not belong to \
                     the key owner. The documents are: {tail}"
                )
            }
            (Bson::Null, None) => format!(
                "{n} documents in {db}.{COLLECTION} have no access key ID. ExtendDB never reads \
                 these documents to authenticate a request. The unique index on access_key_id \
                 allows at most one document with no access key ID. Delete all of these \
                 documents except one. The documents are: {tail}"
            ),
            (key, None) => format!(
                "{n} documents in {db}.{COLLECTION} have the access key ID {key}, which is not a \
                 string. ExtendDB never reads these documents to authenticate a request. The \
                 unique index on access_key_id allows at most one document with this access key \
                 ID. Delete all of these documents except one. The documents are: {tail}"
            ),
        }
    }
}

/// Whether `value` is true as an index option. False, null, a missing
/// value and a zero Int32, Int64 or Double are false, and any other value
/// is true.
fn true_value(value: Option<&Bson>) -> bool {
    match value {
        None | Some(Bson::Null) | Some(Bson::Undefined) => false,
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(n)) => *n != 0,
        Some(Bson::Int64(n)) => *n != 0,
        Some(Bson::Double(n)) => *n != 0.0,
        Some(_) => true,
    }
}

/// Whether `value` is the number 1 as an Int32, an Int64 or a Double. An
/// index whose key direction has another type, such as a Decimal128, is not
/// this migration's index here. The read beside it groups every document.
fn is_one(value: Option<&Bson>) -> bool {
    match value {
        Some(Bson::Int32(n)) => *n == 1,
        Some(Bson::Int64(n)) => *n == 1,
        Some(Bson::Double(n)) => *n == 1.0,
        _ => false,
    }
}

/// Whether `spec` has the key `{ access_key_id: 1 }`.
fn on_the_id(spec: &Document) -> bool {
    spec.get_document("key").map_or(false, |key| key.len() == 1 && is_one(key.get("access_key_id")))
}

/// The options of `spec` that [`is_ours`] compares: every field but `v`,
/// `key`, `name`, `ns`, `background` and `hidden`. `background` has no
/// effect, and a hidden index refuses a second copy of an ID as a visible
/// one does. `sparse` counts only when true, because false is its default.
fn options(spec: &Document) -> Document {
    let mut options = spec.clone();
    for field in ["v", "key", "name", "ns", "background", "hidden"] {
        options.remove(field);
    }
    if !true_value(options.get("sparse")) {
        options.remove("sparse");
    }
    options
}

/// Whether `spec` is the index this migration creates, hidden or not.
fn is_ours(spec: &Document) -> bool {
    let mut options = options(spec);
    spec.get_str("name") == Ok(INDEX)
        && on_the_id(spec)
        && true_value(options.remove("unique").as_ref())
        && options.is_empty()
}

/// Whether an `explain` reply reports a forward collection scan: `LIMIT`
/// over `COLLSCAN` with the direction forward. The plan is the winning plan
/// itself, or its `queryPlan` when the slot-based engine runs it. Any other
/// plan or reply is not one.
fn forward_scan(reply: &Document) -> bool {
    let Ok(winning) = reply.get_document("queryPlanner").and_then(|p| p.get_document("winningPlan")) else {
        return false;
    };
    let mut stage = winning.get_document("queryPlan").unwrap_or(winning);
    loop {
        match stage.get_str("stage") {
            Ok("LIMIT") => match stage.get_document("inputStage") {
                Ok(input) => stage = input,
                Err(_) => return false,
            },
            Ok("COLLSCAN") => return stage.get_str("direction") == Ok("forward"),
            _ => return false,
        }
    }
}

/// The copy a forward collection scan reads among `found`, an ID's listed
/// copies read with their record IDs: the copy with the lowest record ID.
/// A copy deleted and written again gets a new, higher record ID.
fn scanned_first(found: &[Document]) -> Scanned {
    let mut first: Option<(i64, &Bson)> = None;
    for copy in found {
        let (Ok(record), Some(id)) = (copy.get_i64("$recordId"), copy.get("_id")) else {
            return Scanned::Unordered;
        };
        if first.map_or(true, |(lowest, _)| record < lowest) {
            first = Some((record, id));
        }
    }
    first.map_or(Scanned::Gone, |(_, id)| Scanned::First(id.clone()))
}

/// Whether the read needs to group only the documents whose ID is an
/// array. It does when this migration's index is built, because that index
/// refuses a second copy of any other ID.
fn arrays_only(indexes: &[Listed]) -> bool {
    indexes.iter().any(|i| i.built && is_ours(&i.spec))
}

/// The server's error code of a failed command, if it reported one.
fn command_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => Some(command.code),
        _ => None,
    }
}

/// One entry of `listIndexes` with `includeIndexBuildInfo`: `{ spec }` for
/// a built index and `{ spec, indexBuildInfo }` for one whose build is
/// running. An entry without a `spec` is read as a spec whose build may be
/// running.
fn listed(entry: &Document) -> Listed {
    match entry.get_document("spec") {
        Ok(spec) => Listed { spec: spec.clone(), built: !entry.contains_key("indexBuildInfo") },
        Err(_) => Listed { spec: entry.clone(), built: false },
    }
}

/// The indexes of the collection, or none when the collection does not
/// exist.
async fn indexes(stores: &MongoStores) -> Result<Vec<Listed>, MigrationError> {
    let db = &stores.catalog;
    let command = doc! {
        "listIndexes": COLLECTION,
        "cursor": { "batchSize": 1000 },
        "includeIndexBuildInfo": true,
    };
    let listed_reply = stores
        .request(format!("list the indexes of {}.{COLLECTION}", db.name()), async {
            match db.run_command(command.clone()).await {
                Err(e) if command_code(&e) == Some(NAMESPACE_NOT_FOUND) => Ok(None),
                other => other.map(Some),
            }
        })
        .await?;
    let Some(reply) = listed_reply else {
        return Ok(Vec::new());
    };
    let cursor = reply.get_document("cursor").map_err(|e| {
        MigrationError::Store(format!("list the indexes of {}.{COLLECTION}: {e}", db.name()))
    })?;
    if cursor.get_i64("id").unwrap_or(0) != 0 {
        return Err(MigrationError::Store(format!(
            "list the indexes of {}.{COLLECTION}: the server returned them in more than one batch",
            db.name()
        )));
    }
    Ok(cursor
        .get_array("firstBatch")
        .map(|batch| batch.iter().filter_map(Bson::as_document).map(listed).collect())
        .unwrap_or_default())
}

/// Every value of `access_key_id` that more than one document holds or
/// that is an array, sorted, with its documents in natural order; with
/// `arrays_only`, the array values alone, which needs no grouping of the
/// other documents. The `$natural` hint makes the scan a collection scan in
/// natural order, and `$push` keeps the order in which it meets the
/// documents. `allowDiskUse` lets `$group` spill when its state passes the
/// server's memory limit.
async fn groups(stores: &MongoStores, arrays_only: bool) -> Result<Vec<Group>, MigrationError> {
    let db = &stores.catalog;
    let collection = db.collection::<Document>(COLLECTION);
    let mut pipeline = Vec::new();
    if arrays_only {
        pipeline.push(doc! { "$match": { "access_key_id": { "$type": "array" } } });
    }
    pipeline.push(doc! { "$group": {
        "_id": "$access_key_id",
        "count": { "$sum": 1 },
        "array": { "$max": { "$isArray": ["$access_key_id"] } },
        "copies": { "$push": {
            "_id": "$_id",
            "account_id": "$account_id",
            "user_name": "$user_name",
            "created_at": "$created_at",
        } },
    } });
    if !arrays_only {
        pipeline.push(doc! { "$match": { "$or": [{ "count": { "$gt": 1 } }, { "array": true }] } });
    }
    pipeline.push(doc! { "$sort": { "_id": 1 } });
    let read = stores
        .request(format!("group the documents of {}.{COLLECTION} by access key ID", db.name()), async {
            let cursor = collection
                .aggregate(pipeline.clone())
                .hint(Hint::Keys(doc! { "$natural": 1 }))
                .allow_disk_use(true)
                .await?;
            cursor.try_collect::<Vec<Document>>().await
        })
        .await?;
    Ok(read
        .into_iter()
        .map(|group| Group {
            key: group.get("_id").cloned().unwrap_or(Bson::Null),
            array: group.get_bool("array").unwrap_or(false),
            copies: group
                .get_array("copies")
                .map(|copies| {
                    copies
                        .iter()
                        .filter_map(Bson::as_document)
                        .map(|d| Stored {
                            id: d.get("_id").cloned().unwrap_or(Bson::Null),
                            account_id: d.get_str("account_id").ok().map(str::to_owned),
                            user_name: d.get_str("user_name").ok().map(str::to_owned),
                            created_at: d
                                .get_datetime("created_at")
                                .ok()
                                .and_then(|t| t.try_to_rfc3339_string().ok()),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
        .collect())
}

/// `explain` for a credential lookup's command with `id`: `find` on the ID
/// with `limit` 1 and `singleBatch`, the command the driver sends for the
/// lookup's `find_one`. On MongoDB 8 both fields are part of the query
/// shape that query settings name, so the command keeps them.
fn lookup_explain(id: &str) -> Document {
    doc! {
        "explain": { "find": COLLECTION, "filter": { "access_key_id": id }, "limit": 1, "singleBatch": true },
        "verbosity": "queryPlanner",
    }
}

/// The copy a credential lookup reads for `id`, with the `_id`s of `group`'s
/// copies that still hold the ID, among them the copy read; `None` when the
/// lookup reads none of them, because a copy was written or deleted since
/// the group was read. The body reads those copies with their record IDs and
/// sends `explain` for the lookup's command with this ID: a partial index
/// can give one ID another plan than the next. On a forward collection scan
/// the lookup reads the copy with the lowest record ID. With any other plan,
/// and when a record ID is not an Int64, the body sends the lookup's own
/// query, `find_one` on the ID with no sort, so the server picks the copy as
/// it does for the lookup. A copy that took the ID since the read is not
/// among those read; the index build fails on it.
async fn looked_up(stores: &MongoStores, id: &str, group: &Group) -> Result<Option<(Bson, Vec<Bson>)>, MigrationError> {
    let db = &stores.catalog;
    let collection = db.collection::<Document>(COLLECTION);
    let what = format!("read the copy of {id} a credential lookup reads in {}.{COLLECTION}", db.name());
    let listed = group.copies.iter().map(|c| c.id.clone()).collect::<Vec<_>>();
    let filter = doc! { "_id": { "$in": listed }, "access_key_id": id };
    let held = stores
        .request(&what, async {
            let cursor = collection
                .find(filter.clone())
                .projection(doc! { "_id": 1 })
                .show_record_id(true)
                .await?;
            cursor.try_collect::<Vec<Document>>().await
        })
        .await?;
    let held_ids = held.iter().filter_map(|d| d.get("_id").cloned()).collect::<Vec<_>>();
    let command = lookup_explain(id);
    let plan = stores
        .request(format!("explain the credential lookup of {id} in {}.{COLLECTION}", db.name()), async {
            db.run_command(command.clone()).await
        })
        .await?;
    if forward_scan(&plan) {
        match scanned_first(&held) {
            Scanned::First(read) => return Ok(Some((read, held_ids))),
            Scanned::Gone => return Ok(None),
            Scanned::Unordered => {}
        }
    }
    let found = stores
        .request(&what, async { collection.find_one(doc! { "access_key_id": id }).await })
        .await?;
    Ok(found
        .and_then(|d| d.get("_id").cloned())
        .filter(|read| held_ids.contains(read))
        .map(|read| (read, held_ids)))
}

/// What `detect` and `run` read.
async fn read(stores: &MongoStores) -> Result<Vec<Group>, MigrationError> {
    let indexes = indexes(stores).await?;
    groups(stores, arrays_only(&indexes)).await
}

/// The items `detect` and `run` refuse the migration for.
async fn refusals(stores: &MongoStores, groups: &[Group]) -> Result<Vec<String>, MigrationError> {
    let db = stores.catalog.name();
    let mut items = Vec::new();
    for group in groups.iter().filter(|g| !g.resolvable()) {
        let read = match group.access_key_id() {
            Some(id) => looked_up(stores, id, group).await?.map(|(read, _)| read),
            None => None,
        };
        items.push(group.refusal(db, read.as_ref()));
    }
    Ok(items)
}

#[async_trait]
impl CodeBody<MongoStores> for UniqueAccessKeyIds {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    async fn detect(&self, stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        let groups = read(stores).await?;
        refusals(stores, &groups).await
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.catalog;
        let collection = db.collection::<Document>(COLLECTION);
        let groups = read(stores).await?;
        // `detect` refused these before the run; what was written since is
        // refused the same way, before this body writes anything.
        let items = refusals(stores, &groups).await?;
        if !items.is_empty() {
            return Err(MigrationError::Status(CatalogStatus::NeedsAttention { items }));
        }
        let mut notes = Vec::new();
        for group in groups.iter().filter(|g| g.resolvable() && g.copies.len() > 1) {
            let Some(id) = group.access_key_id() else {
                continue;
            };
            // A lookup that reads none of the listed copies, because a copy
            // was written or deleted since the read: delete nothing for this
            // ID. The build below fails on a copy written since, and the next
            // run reads again.
            let Some((read, held)) = looked_up(stores, id, group).await? else {
                continue;
            };
            // Only the listed copies that still hold the ID: a copy deleted
            // or given another ID since the read is neither deleted nor
            // named.
            let (kept, others): (Vec<&Stored>, Vec<&Stored>) =
                group.copies.iter().filter(|c| held.contains(&c.id)).partition(|c| c.id == read);
            let [kept] = kept.as_slice() else {
                continue;
            };
            if others.is_empty() {
                continue;
            }
            let owner = format!(
                "account {}, user {}",
                kept.account_id.as_deref().unwrap_or("(none)"),
                kept.user_name.as_deref().unwrap_or("(none)"),
            );
            let total = others.len() + 1;
            let listed = ids(&others);
            let copies = match others.len() {
                1 => format!("the other document: {listed}"),
                _ => format!("the other documents: {listed}"),
            };
            let reads = format!(
                "When a request uses this access key ID, ExtendDB reads _id {} to authenticate \
                 the request.",
                kept.id
            );
            // The ledger records the notes after the body returns; this line
            // names the copies when a run stops before then.
            tracing::warn!(
                "migration 11 unique_access_key_ids: {total} documents in {}.{COLLECTION} have the \
                 access key ID {id}, and all of them belong to {owner}. {reads} This migration \
                 keeps _id {} and deletes {copies}.",
                db.name(),
                kept.id,
            );
            // Only those copies, and only while they hold the ID: a copy
            // written since is not deleted, and the index build below fails
            // on it.
            let filter = doc! {
                "_id": { "$in": others.iter().map(|c| c.id.clone()).collect::<Vec<_>>() },
                "access_key_id": id,
            };
            let result = stores
                .request(format!("delete the other copies of {id} in {}.{COLLECTION}", db.name()), async {
                    collection.delete_many(filter.clone()).await
                })
                .await?;
            let deleted = match usize::try_from(result.deleted_count) {
                Ok(n) if n == others.len() => copies,
                _ => format!("{} of the other documents: {listed}", result.deleted_count),
            };
            notes.push(format!(
                "{total} documents had the access key ID {id}, and all of them belonged to \
                 {owner}. {reads} This migration kept _id {} and deleted {deleted}.",
                kept.id,
            ));
        }
        // A copy written after the read above fails the build with a
        // duplicate key error, and the runner records nothing.
        let model = IndexModel::builder()
            .keys(doc! { "access_key_id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        stores
            .request(format!("create the unique index {INDEX} on {}.{COLLECTION}", db.name()), async {
                collection
                    .create_index(model)
                    .commit_quorum(CommitQuorum::Majority)
                    .await
                    .map(|_| ())
            })
            .await?;
        Ok(Done { notes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap_or_else(|e| panic!("{text}: {e:?}")))
    }

    fn ours() -> Document {
        doc! { "v": 2, "key": { "access_key_id": 1 }, "name": INDEX, "unique": true }
    }

    #[test]
    fn a_key_direction_is_one_only_as_the_number_1() {
        for value in [Bson::Int32(1), Bson::Int64(1), Bson::Double(1.0)] {
            assert!(is_one(Some(&value)), "{value}");
        }
        for value in [
            Bson::Int32(0),
            Bson::Int32(-1),
            Bson::Int32(2),
            Bson::Int64(0),
            Bson::Int64(-1),
            Bson::Int64(2),
            Bson::Double(0.0),
            Bson::Double(-1.0),
            Bson::Double(2.0),
            Bson::Double(1.5),
            Bson::Double(1.000_000_000_000_000_2),
            decimal("1"),
        ] {
            assert!(!is_one(Some(&value)), "{value}");
        }
        assert!(!is_one(None));
    }

    #[test]
    fn an_option_is_false_only_when_false_zero_null_or_missing() {
        for value in [
            Bson::Boolean(true),
            Bson::Int32(1),
            Bson::Int32(-1),
            Bson::Int64(1),
            Bson::Double(0.5),
        ] {
            assert!(true_value(Some(&value)), "{value}");
        }
        for value in [
            Bson::Boolean(false),
            Bson::Int32(0),
            Bson::Int64(0),
            Bson::Double(0.0),
            Bson::Double(-0.0),
            Bson::Null,
            Bson::Undefined,
        ] {
            assert!(!true_value(Some(&value)), "{value}");
        }
        assert!(!true_value(None));
    }

    #[test]
    fn list_indexes_tells_a_built_index_from_one_whose_build_runs() {
        let built = listed(&doc! { "spec": ours() });
        assert!(built.built);
        assert_eq!(built.spec, ours());
        let building = listed(&doc! {
            "indexBuildInfo": {
                "buildUUID": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![7; 16] },
                "method": "Hybrid",
                "phase": 1,
                "phaseStr": "collection scan",
                "replicationState": { "state": "In progress" },
            },
            "spec": ours(),
        });
        assert!(!building.built);
        assert_eq!(building.spec, ours());
        let unmarked = listed(&ours());
        assert!(!unmarked.built);
        assert_eq!(unmarked.spec, ours());
    }

    #[test]
    fn only_a_built_index_of_this_migration_limits_the_read_to_array_ids() {
        let hidden = doc! { "v": 2, "key": { "access_key_id": 1 }, "name": INDEX, "unique": true, "hidden": true };
        assert!(arrays_only(&[Listed { spec: ours(), built: true }]));
        assert!(arrays_only(&[Listed { spec: hidden.clone(), built: true }]));
        assert!(!arrays_only(&[Listed { spec: ours(), built: false }]));
        assert!(!arrays_only(&[Listed { spec: hidden, built: false }]));
        let mut plain = ours();
        plain.remove("unique");
        let mut renamed = ours();
        renamed.insert("name", "akid");
        assert!(!arrays_only(&[
            Listed { spec: plain, built: true },
            Listed { spec: renamed, built: true },
        ]));
        assert!(!arrays_only(&[]));
    }

    /// An `explain` reply around `winning`, the winning plan.
    fn explained(winning: Document) -> Document {
        doc! { "queryPlanner": { "namespace": "catalog.access_keys", "winningPlan": winning }, "ok": 1 }
    }

    fn scan(direction: &str) -> Document {
        doc! {
            "stage": "LIMIT",
            "limitAmount": 1,
            "inputStage": { "stage": "COLLSCAN", "filter": { "access_key_id": { "$eq": "X" } }, "direction": direction },
        }
    }

    #[test]
    fn explain_reports_a_forward_scan_only_for_a_collection_scan_in_natural_order() {
        // MongoDB 7.0.45 and 8.0.34 (which adds isCached), and 7.0.0, whose
        // slot-based engine puts the stages under queryPlan.
        assert!(forward_scan(&explained(scan("forward"))));
        let mut cached = scan("forward");
        cached.insert("isCached", false);
        assert!(forward_scan(&explained(cached)));
        assert!(forward_scan(&explained(doc! {
            "queryPlan": scan("forward"),
            "slotBasedPlan": { "slots": "", "stages": "" },
        })));
        // Query settings that allow only { $natural: -1 } on MongoDB 8.0.34.
        let mut backward = scan("backward");
        backward.insert("isCached", false);
        assert!(!forward_scan(&explained(backward)));
        assert!(!forward_scan(&explained(doc! { "queryPlan": scan("backward") })));
        // An index on access_key_id, on 7.0 and on 8.0.
        assert!(!forward_scan(&explained(doc! {
            "stage": "LIMIT",
            "limitAmount": 1,
            "inputStage": { "stage": "FETCH", "inputStage": { "stage": "IXSCAN", "indexName": "akid", "direction": "forward" } },
        })));
        assert!(!forward_scan(&explained(doc! {
            "isCached": false,
            "stage": "EXPRESS_IXSCAN",
            "keyPattern": "{ access_key_id: 1, created_at: -1 }",
            "indexName": "akid_newest_first",
        })));
        assert!(!forward_scan(&explained(doc! { "stage": "COLLSCAN" })));
        assert!(!forward_scan(&explained(doc! { "stage": "LIMIT" })));
        assert!(!forward_scan(&explained(doc! { "stage": "EOF" })));
        assert!(!forward_scan(&doc! { "ok": 1 }));
    }

    #[test]
    fn the_explained_command_has_the_shape_of_the_lookups_find_one() {
        // The server's lookup, as the profiler records it on 7.0.0, 7.0.45
        // and 8.0.34: { find, singleBatch: true, limit: 1, filter }. Query
        // settings on 8.0 name a shape that includes limit and singleBatch.
        assert_eq!(
            lookup_explain("AKIAEXAMPLE"),
            doc! {
                "explain": {
                    "find": "access_keys",
                    "filter": { "access_key_id": "AKIAEXAMPLE" },
                    "limit": 1,
                    "singleBatch": true,
                },
                "verbosity": "queryPlanner",
            }
        );
    }

    #[test]
    fn a_forward_scan_reads_the_copy_with_the_lowest_record_id() {
        let copy = |id: i32, record: Bson| doc! { "_id": id, "$recordId": record };
        // Copy 1 was deleted and written again after copy 2, so its record
        // ID is the higher one.
        assert_eq!(
            scanned_first(&[copy(1, Bson::Int64(4)), copy(2, Bson::Int64(2)), copy(3, Bson::Int64(3))]),
            Scanned::First(Bson::Int32(2))
        );
        assert_eq!(scanned_first(&[copy(5, Bson::Int64(9))]), Scanned::First(Bson::Int32(5)));
        assert_eq!(scanned_first(&[]), Scanned::Gone);
        assert_eq!(
            scanned_first(&[copy(1, Bson::Int64(1)), copy(2, Bson::String("k".to_owned()))]),
            Scanned::Unordered
        );
        assert_eq!(scanned_first(&[doc! { "$recordId": 1_i64 }]), Scanned::Unordered);
    }

    #[test]
    fn the_index_built_by_hand_is_ours_with_options_that_do_not_change_it() {
        // The upgrade manual builds the index by hand on a large collection,
        // and the read then groups the array IDs alone.
        let with = |field: &str, value: Bson| {
            let mut spec = ours();
            spec.insert(field, value);
            spec
        };
        let keyed = |direction: Bson| with("key", doc! { "access_key_id": direction }.into());
        for spec in [
            ours(),
            with("background", true.into()),
            with("sparse", false.into()),
            with("hidden", true.into()),
        ] {
            assert!(is_ours(&spec), "{spec}");
        }
        for spec in [
            with("unique", false.into()),
            with("sparse", true.into()),
            with("expireAfterSeconds", 3600.into()),
            with("partialFilterExpression", doc! { "access_key_id": { "$exists": true } }.into()),
            with("name", "akid".into()),
            with("key", doc! { "account_id": 1 }.into()),
            keyed(decimal("1")),
            keyed((-1).into()),
        ] {
            assert!(!is_ours(&spec), "{spec}");
        }
    }
}
