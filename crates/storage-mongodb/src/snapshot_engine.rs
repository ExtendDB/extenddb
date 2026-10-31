// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for `MongoDB`.
//!
//! One snapshot session covers catalog and data: a session started with
//! `snapshot: true` runs every read at the cluster time of its first read
//! (`MongoDB` 5.0+ snapshot reads outside a transaction). The `tables` document
//! is read first and pins the time; the index documents and the base
//! collection cursor then read at the same time, so the definition and the
//! items describe one instant without any lock.
//!
//! Every read sets `readConcern: snapshot` explicitly. The driver only
//! inserts that level when neither the operation nor its database or
//! collection carries a read concern, and the production catalog handle is
//! built with `readConcern: local` (`catalog_database`), as a connection
//! string with `readConcernLevel=` would build every handle. Without the
//! explicit level the reads fail, checked against a `MongoDB` 7 replica set
//! (`reads_without_the_snapshot_level_fail`): the first read runs at the
//! handle's level and the session reports no snapshot time, which the open
//! refuses, and a later read in a pinned session goes out with
//! `atClusterTime` and the handle's level, which the server refuses with
//! code 72 (`InvalidOptions`: "atClusterTime field can be set only if level
//! is equal to snapshot").
//!
//! A server that is not a replica set member (a standalone `mongod`) cannot
//! serve snapshot reads and refuses the first read with `NotAReplicaSet`
//! (code 123); the open maps that to [`StorageError::Unsupported`], with a
//! sentence that names that cause only (a first read that succeeded without
//! a snapshot time is an internal error with a sentence of its own). Checked
//! against a standalone `MongoDB` 7 by `a_standalone_server_is_unsupported`,
//! which CI's `mongodb-snapshot` job runs against a standalone container.
//!
//! Error messages are built from the server's code name, code and message
//! (`describe`), not the driver error's `Display`, which carries the raw BSON
//! reply as a hex dump.
//!
//! The server keeps at least `minSnapshotHistoryWindowInSeconds` (default
//! 300) of snapshot history. A stream that is still open past that window
//! may fail with `SnapshotTooOld` (code 239) once the server prunes the
//! snapshot, and a server that cannot serve the snapshot answers
//! `SnapshotUnavailable` (code 246); both surface as
//! [`StorageError::SnapshotExpired`] and the caller restarts the backup. At
//! typical cursor throughput the window bounds a single backup at tens of
//! millions of documents unless the server parameter is raised; the backup
//! design (#396) records that as an operator setting. The cursor is opened
//! without `noCursorTimeout`, so it has the server's default idle timeout
//! (`cursorTimeoutMillis`, 10 minutes): a stream not polled for that long
//! gets `CursorNotFound` (code 43) on its next batch, which surfaces as
//! [`StorageError::Internal`], and the caller likewise starts over.
//!
//! `key_ordered` is `false`. For a composite key `_id` is a netstring of the
//! key text (`<len>:<pk>,<len>:<sk>,`), which sorts by length before content
//! and is not the format's key order for any key type; a hash-only table's
//! `_id` is the hash key text and would be in order, but one rule for the
//! backend is simpler than a second ordered path no restore needs. The
//! stream reads in natural order.
//!
//! The stream does not record round trips into the shared paging counter:
//! the driver issues `getMore` inside `cursor.next` and does not expose the
//! batch boundary, so a count here could only echo the batch size the
//! stream asked for. The test fixture counts, through command monitoring,
//! the `find` commands the server receives on the data database at
//! `readConcern: snapshot` (only this stream's `find` on the table's base
//! collection reads at that level; the GSI backfill worker's `find` reads at
//! `majority` and is not counted) and the `getMore` commands on the cursor
//! ids those finds returned, so the conformance check observes the server's
//! paging of the snapshot stream and nothing else.
//!
//! Memory: a batch is at most `batch_size` documents
//! (`paging::DEFAULT_FETCH_BATCH`, 1000) and at most the server's 16 MiB
//! reply limit, whichever is reached first, so unlike the SQL backends'
//! row-bounded fetches one batch here is bounded in bytes too.

use extenddb_core::types::Item;
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, paging};
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mongodb::bson::{Document, doc};
use mongodb::options::ReadConcern;
use mongodb::{ClientSession, SessionCursor};

use crate::MongoEngine;
use crate::data::{data_collection_name, document_to_item};

/// Server error code for `SnapshotTooOld`: the read timestamp is older than
/// the oldest the server still holds.
const SNAPSHOT_TOO_OLD: i32 = 239;
/// Server error code for `SnapshotUnavailable`: the server cannot serve a
/// read at the requested snapshot and asks the client to retry. The remedy
/// for a backup is the same restart.
const SNAPSHOT_UNAVAILABLE: i32 = 246;
/// Server error code for `NotAReplicaSet`: a standalone server refusing a
/// snapshot read.
const NOT_A_REPLICA_SET: i32 = 123;

/// Why an open cannot proceed on a standalone server, which refuses the
/// first read with `NotAReplicaSet` ([`map_first_read_error`]).
const STANDALONE_SERVER: &str = "MongoDB refused the first read at readConcern snapshot: a \
     standalone server cannot serve snapshot reads, and table snapshots need a replica set \
     member";

/// Why an open cannot proceed when the first read succeeded but the session
/// reports no snapshot time.
const NO_SNAPSHOT_TIME: &str = "MongoDB served the first read without a snapshot time: a read \
     concern overrode the snapshot level";

impl SnapshotEngine for MongoEngine {
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            self.open_table_snapshot_impl(&account_id, &table_name)
                .await
        })
    }
}

impl MongoEngine {
    async fn open_table_snapshot_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableSnapshot, StorageError> {
        Self::validate_account_id(account_id)?;

        let mut session = self
            .client
            .start_session()
            .snapshot(true)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // First read pins the snapshot time for the whole session.
        let tables = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .read_concern(ReadConcern::snapshot())
            .session(&mut session)
            .await
            .map_err(|e| map_first_read_error(&e))?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let table_id = table_doc
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("missing table_id".to_owned()))?
            .to_owned();

        // The first read must have pinned a cluster time; without one the
        // server is not serving snapshot reads and the stream would not be
        // consistent. The cluster time is the marker: what a later change-log
        // archive (the oplog from this time on) anchors against. Nothing
        // reopens a snapshot from it.
        let marker = session
            .snapshot_time()
            .map(|t| format!("{}:{}", t.time, t.increment))
            .ok_or_else(|| StorageError::Internal(NO_SNAPSHOT_TIME.to_owned()))?;

        // After the `tables` document, before its index documents (the
        // stage's definition for this backend): an index read outside the
        // session would see what commits here.
        paging::open_pause(paging::OpenStage::AfterDefinition).await;
        let mut description = self
            .doc_to_table_description_in(&table_doc, Some(&mut session))
            .await?;
        // One rule for "is this table ACTIVE": the description's, as
        // DescribeTable reports it.
        if description.table_status != extenddb_core::types::TableStatus::Active {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }
        drop_deleting_indexes(&mut description);

        let collection = self
            .data_db
            .collection::<Document>(&data_collection_name(&table_id));
        let batch = paging::fetch_batch();
        paging::open_pause(paging::OpenStage::BeforeItems).await;
        let cursor = collection
            .find(doc! {})
            .batch_size(u32::try_from(batch).unwrap_or(u32::MAX))
            .read_concern(ReadConcern::snapshot())
            .session(&mut session)
            .await
            .map_err(|e| map_mongo_error(&e))?;

        Ok(TableSnapshot {
            description,
            info: SnapshotInfo {
                kind: SnapshotInfo::KIND_MONGODB_SNAPSHOT_READ_CONCERN.to_owned(),
                marker,
            },
            key_ordered: false,
            items: session_stream(session, cursor),
        })
    }
}

/// Stream items from a session cursor. The session and cursor live in the
/// stream's state; dropping the stream ends both. Round trips are counted
/// by the test fixture from the server's command stream (module doc).
fn session_stream(
    session: ClientSession,
    cursor: SessionCursor<Document>,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        session: ClientSession,
        cursor: SessionCursor<Document>,
        done: bool,
    }
    let state = State {
        session,
        cursor,
        done: false,
    };
    Box::pin(futures::stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        // The first error ends the stream, as on every backend: a document
        // that does not decode, like a failed getMore, means the stream is
        // not the whole table.
        match state.cursor.next(&mut state.session).await {
            Some(Ok(doc)) => {
                let item = document_to_item(&doc);
                state.done = item.is_err();
                Some((item, state))
            }
            Some(Err(e)) => {
                state.done = true;
                Some((Err(map_mongo_error(&e)), state))
            }
            None => None,
        }
    }))
}

pub(crate) fn map_mongo_error(e: &mongodb::error::Error) -> StorageError {
    // `find` and `getMore` failures arrive as command errors.
    let snapshot_gone = matches!(
        &*e.kind,
        mongodb::error::ErrorKind::Command(c)
            if c.code == SNAPSHOT_TOO_OLD || c.code == SNAPSHOT_UNAVAILABLE
    );
    if snapshot_gone {
        return StorageError::SnapshotExpired(describe(e));
    }
    StorageError::Internal(describe(e))
}

/// [`map_mongo_error`] for the open's first read, where a standalone server
/// answers `NotAReplicaSet`: that is a configuration the backend cannot take
/// a snapshot in, not an internal error.
fn map_first_read_error(e: &mongodb::error::Error) -> StorageError {
    if matches!(&*e.kind, mongodb::error::ErrorKind::Command(c) if c.code == NOT_A_REPLICA_SET) {
        return StorageError::Unsupported(format!("{STANDALONE_SERVER} ({})", describe(e)));
    }
    map_mongo_error(e)
}

/// A driver error as text: the server's code name, code and message for a
/// command error, the error kind otherwise. Not `Error`'s `Display`, which
/// appends the raw reply document as a hex dump.
fn describe(e: &mongodb::error::Error) -> String {
    match &*e.kind {
        mongodb::error::ErrorKind::Command(c) => {
            format!("{} (code {}): {}", c.code_name, c.code, c.message)
        }
        kind => kind.to_string(),
    }
}

#[cfg(test)]
mod error_mapping_tests {
    //! Pin the server error codes without a server: a wrong constant here
    //! turns the one retryable failure the module promises to classify into
    //! an internal error.

    use mongodb::error::{CommandError, Error, ErrorKind};

    use super::*;

    /// `CommandError` is non-exhaustive with a crate-private field, so it is
    /// built the way the driver builds it: from the server's reply document.
    fn command_error(code: i32, code_name: &str) -> Error {
        let reply = doc! { "code": code, "codeName": code_name, "errmsg": "synthetic" };
        let command: CommandError =
            mongodb::bson::from_document(reply).expect("command error document");
        Error::from(ErrorKind::Command(command))
    }

    #[test]
    fn snapshot_too_old_and_unavailable_are_snapshot_expired() {
        for (code, name) in [(239, "SnapshotTooOld"), (246, "SnapshotUnavailable")] {
            let mapped = map_mongo_error(&command_error(code, name));
            assert!(
                matches!(mapped, StorageError::SnapshotExpired(_)),
                "{name} ({code}): {mapped:?}"
            );
        }
    }

    #[test]
    fn not_a_replica_set_on_the_first_read_is_unsupported() {
        let StorageError::Unsupported(message) =
            map_first_read_error(&command_error(123, "NotAReplicaSet"))
        else {
            panic!("NotAReplicaSet on the first read is Unsupported");
        };
        assert!(message.starts_with(STANDALONE_SERVER), "{message}");
        assert!(message.contains("NotAReplicaSet (code 123)"), "{message}");
        // Only the first read maps it; later reads keep the general rule.
        assert!(matches!(
            map_mongo_error(&command_error(123, "NotAReplicaSet")),
            StorageError::Internal(_)
        ));
        // And the first read keeps the snapshot codes.
        assert!(matches!(
            map_first_read_error(&command_error(239, "SnapshotTooOld")),
            StorageError::SnapshotExpired(_)
        ));
    }

    #[test]
    fn other_command_errors_are_internal() {
        for (code, name) in [
            (50, "MaxTimeMSExpired"),
            (13, "Unauthorized"),
            (0, "unknown"),
        ] {
            let mapped = map_mongo_error(&command_error(code, name));
            assert!(
                matches!(mapped, StorageError::Internal(_)),
                "{name} ({code}): {mapped:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    //! Runs against a live replica set named by `EXTENDDB_TEST_MONGODB_URI`
    //! (for example `mongodb://127.0.0.1:27017/?directConnection=true`);
    //! skips otherwise. Each test builds an engine on its own
    //! `snap<uuid>_cat` / `snap<uuid>_data` databases, the way the backup
    //! live tests do, and drops them when the fixture drops, so a URI in the
    //! environment never makes `cargo test` touch another instance's
    //! databases. `devtools/run-mongodb-tests --mongo-only` starts a
    //! replica set.

    use std::sync::Arc;

    use extenddb_storage::snapshot_conformance::{self, PageSize};
    use extenddb_storage::{SnapshotEngine, TableEngine};

    use super::*;

    const ACCOUNT: &str = "123456789012";
    /// `MongoDB`: natural order, number keys stored as Decimal128 (34 digits);
    /// the open holds nothing an `UpdateTable` waits on.
    const EXPECT: snapshot_conformance::Expectations = snapshot_conformance::Expectations {
        key_ordered: false,
        numeric_key_digits: 34,
        update_table_waits_at: &[],
    };

    /// Every live test here opens a snapshot (or tries to) or sets the shared
    /// page size, so each holds `snapshot_conformance::hooks_lock` for its
    /// whole body: the knob, the round-trip counter and the open pause are
    /// process-global, and one lock covers all three.
    async fn hooks() -> snapshot_conformance::HooksGuard {
        snapshot_conformance::hooks_lock().await
    }

    /// The replica set URI, or `None` to skip. With
    /// `EXTENDDB_TEST_MONGODB_REQUIRED` set a missing URI is a failure, so CI
    /// cannot pass by skipping.
    fn uri() -> Option<String> {
        let uri = std::env::var("EXTENDDB_TEST_MONGODB_URI")
            .ok()
            .filter(|u| !u.trim().is_empty());
        assert!(
            uri.is_some() || std::env::var_os("EXTENDDB_TEST_MONGODB_REQUIRED").is_none(),
            "EXTENDDB_TEST_MONGODB_REQUIRED is set but EXTENDDB_TEST_MONGODB_URI is not; the \
             MongoDB snapshot tests must run, not skip"
        );
        uri
    }

    /// An engine on two scratch databases plus the GSI backfill worker that
    /// `serve` would run (the check table has a GSI, which only the worker
    /// flips to ACTIVE). Dropped with the fixture on success or panic (not
    /// when the test binary is killed): the worker is
    /// aborted, and the databases are dropped on a thread with its own
    /// runtime and a fresh client, since `Drop` cannot await on the test
    /// runtime and the test's client belongs to it.
    struct Fixture {
        engine: Option<MongoEngine>,
        worker: tokio::task::JoinHandle<()>,
        uri: String,
        names: [String; 2],
    }

    impl std::ops::Deref for Fixture {
        type Target = MongoEngine;
        fn deref(&self) -> &MongoEngine {
            self.engine.as_ref().expect("engine present until drop")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.worker.abort();
            drop(self.engine.take());
            let uri = self.uri.clone();
            let names = self.names.clone();
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("cleanup runtime");
                    rt.block_on(async move {
                        let Ok(client) = mongodb::Client::with_uri_str(&uri).await else {
                            eprintln!("scratch cleanup: could not connect; {names:?} left behind");
                            return;
                        };
                        for name in &names {
                            if let Err(e) = client.database(name).drop().await {
                                eprintln!("scratch cleanup: drop {name}: {e}");
                            }
                        }
                        client.shutdown().await;
                    });
                });
            });
        }
    }

    async fn fixture(uri: &str) -> Fixture {
        let stem = format!("snap{}", uuid::Uuid::new_v4().simple());
        let names = [format!("{stem}_cat"), format!("{stem}_data")];
        // The shared paging counter is fed from what the server receives,
        // counting only the snapshot stream's round trips: a `find` on the
        // data database at `readConcern: snapshot` (the stream's read of the
        // table's base collection; the GSI backfill worker also runs `find`
        // on base collections, at `majority`, and the engine's writes are
        // `findAndModify`, `update` and `delete`), and a `getMore` on a
        // cursor id one of those finds returned (read from its reply).
        let mut options = mongodb::options::ClientOptions::parse(uri)
            .await
            .expect("client options");
        options.command_event_handler = Some(snapshot_round_trip_counter(names[1].clone()));
        let client = mongodb::Client::with_options(options).expect("client");
        // The production option set (`readConcern: local` among them), so
        // the explicit snapshot read concern is tested against what `new`
        // builds.
        let catalog_db = crate::catalog_database_named(&client, &names[0]);
        catalog_db
            .collection::<Document>("accounts")
            .insert_one(doc! { "_id": ACCOUNT, "account_name": "snapshot-test" })
            .await
            .expect("account");
        catalog_db
            .collection::<Document>("settings")
            .insert_one(doc! { "_id": "control_plane_delay_seconds", "value": "0" })
            .await
            .expect("setting");
        let engine = MongoEngine {
            client: client.clone(),
            catalog_db,
            data_db: client.database(&names[1]),
            region: "us-east-1".to_owned(),
            gsi_cache: Arc::default(),
            tx_read_concern: ReadConcern::majority(),
            backup_liveness: crate::backup_engine::BackupLiveness::default(),
        };
        let worker = tokio::spawn(crate::ttl_worker::gsi_backfill_worker(Arc::new(
            engine.clone(),
        )));
        Fixture {
            engine: Some(engine),
            worker,
            uri: uri.to_owned(),
            names,
        }
    }

    /// Command monitor that records one paging round trip per `find` at
    /// `readConcern: snapshot` on `data_name` and per `getMore` on a cursor
    /// such a `find` opened.
    fn snapshot_round_trip_counter(
        data_name: String,
    ) -> mongodb::event::EventHandler<mongodb::event::command::CommandEvent> {
        use mongodb::event::command::CommandEvent;
        #[derive(Default)]
        struct Seen {
            /// Request ids of counted finds whose reply has not arrived.
            finds: std::collections::HashSet<i32>,
            /// Cursor ids those finds returned.
            cursors: std::collections::HashSet<i64>,
        }
        let seen = std::sync::Mutex::new(Seen::default());
        mongodb::event::EventHandler::callback(move |event: CommandEvent| {
            let mut seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match event {
                CommandEvent::Started(started) if started.db == data_name => {
                    match started.command_name.as_str() {
                        "find"
                            if started
                                .command
                                .get_document("readConcern")
                                .and_then(|rc| rc.get_str("level"))
                                == Ok("snapshot") =>
                        {
                            seen.finds.insert(started.request_id);
                            paging::record_fetch();
                        }
                        "getMore"
                            if started
                                .command
                                .get_i64("getMore")
                                .is_ok_and(|id| seen.cursors.contains(&id)) =>
                        {
                            paging::record_fetch();
                        }
                        _ => {}
                    }
                }
                CommandEvent::Succeeded(succeeded) if seen.finds.remove(&succeeded.request_id) => {
                    if let Ok(id) = succeeded
                        .reply
                        .get_document("cursor")
                        .and_then(|c| c.get_i64("id"))
                        && id != 0
                    {
                        seen.cursors.insert(id);
                    }
                }
                CommandEvent::Failed(failed) => {
                    seen.finds.remove(&failed.request_id);
                }
                _ => {}
            }
        })
    }

    async fn set_updating(engine: &MongoEngine, table: &str) {
        engine
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! { "_id": { "account_id": ACCOUNT, "table_name": table } },
                doc! { "$set": { "table_status": "UPDATING" } },
            )
            .await
            .expect("flip status");
    }

    #[tokio::test]
    async fn snapshot_is_consistent_under_concurrent_writes() {
        let Some(uri) = uri() else {
            eprintln!("SKIP snapshot_is_consistent_under_concurrent_writes: no MongoDB");
            return;
        };
        let hooks = hooks().await;
        let engine = fixture(&uri).await;
        snapshot_conformance::run(
            &hooks,
            &*engine,
            ACCOUNT,
            "snap_src",
            50,
            EXPECT,
            || async {
                let _ = extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine)
                    .await;
            },
        )
        .await;
        snapshot_conformance::run_refusals(
            &hooks,
            &*engine,
            ACCOUNT,
            "snap_refused",
            || async {
                let _ = extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine)
                    .await;
            },
            |table| {
                let engine = &*engine;
                async move { set_updating(engine, &table).await }
            },
        )
        .await;
    }

    /// A restored table's snapshot description carries the same
    /// `restore_summary` as `describe_table`'s (both read the restore fields
    /// of the `tables` document). The fields are set directly, as
    /// `RestoreTableFromBackup` leaves them.
    #[tokio::test]
    async fn a_restored_table_describes_its_restore() {
        let Some(uri) = uri() else {
            eprintln!("SKIP a_restored_table_describes_its_restore: no MongoDB");
            return;
        };
        let hooks = hooks().await;
        let engine = fixture(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("restored"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "restored", || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        })
        .await;
        snapshot_conformance::populate(&*engine, ACCOUNT, "restored", 5).await;
        engine
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! { "_id": { "account_id": ACCOUNT, "table_name": "restored" } },
                doc! { "$set": {
                    "restore_source_backup_arn":
                        "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01759320000250-0a1b2c3d",
                    "restore_source_table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/src",
                    "restore_date_time": mongodb::bson::DateTime::from_millis(1_759_320_000_250),
                } },
            )
            .await
            .expect("restore fields");
        snapshot_conformance::assert_snapshot_describes_the_table(
            &hooks, &*engine, ACCOUNT, "restored", true,
        )
        .await;
    }

    /// A document the decoder cannot read ends the stream at its error, as on
    /// the other backends: nothing after it, so a consumer cannot mistake the
    /// rest for the whole table.
    #[tokio::test]
    async fn an_undecodable_document_ends_the_stream() {
        let Some(uri) = uri() else {
            eprintln!("SKIP an_undecodable_document_ends_the_stream: no MongoDB");
            return;
        };
        let _hooks = hooks().await;
        let engine = fixture(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("bad"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "bad", || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        })
        .await;
        let table_id = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "bad".to_owned(),
                },
            )
            .await
            .expect("describe")
            .table_id;
        let collection = engine
            .data_db
            .collection::<Document>(&data_collection_name(&table_id));
        // Five items, the undecodable document, five more: natural order is
        // insertion order here, so the error falls mid-stream.
        snapshot_conformance::populate(&*engine, ACCOUNT, "bad", 5).await;
        collection
            .insert_one(doc! { "_id": "undecodable" })
            .await
            .expect("insert a document without item_data");
        let ki = extenddb_core::types::TableKeyInfo {
            table_name: "bad".to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id: table_id.clone(),
            key_schema: snapshot_conformance::table_input("bad").key_schema,
            base_key_schema: snapshot_conformance::table_input("bad").key_schema,
            attribute_definitions: snapshot_conformance::table_input("bad").attribute_definitions,
            ..Default::default()
        };
        for i in 100..105 {
            let item: Item = [
                (
                    "pk".to_owned(),
                    extenddb_core::types::AttributeValue::S("late".to_owned()),
                ),
                (
                    "sk".to_owned(),
                    extenddb_core::types::AttributeValue::N(i.to_string()),
                ),
            ]
            .into_iter()
            .collect();
            extenddb_storage::DataEngine::put_item(
                &*engine,
                &ki,
                item,
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
        }
        // Where the document falls in natural order, from a plain read: the
        // stream must yield exactly the items before it, then the error.
        let natural: Vec<Document> =
            futures::TryStreamExt::try_collect(collection.find(doc! {}).await.expect("find"))
                .await
                .expect("natural order");
        let position = natural
            .iter()
            .position(|d| d.get_str("_id") == Ok("undecodable"))
            .expect("the document is there");
        assert!(
            position + 1 < natural.len(),
            "precondition: the undecodable document is not last ({position} of {}), or the \
             test cannot tell a stream that ends from one that continues",
            natural.len()
        );
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "bad")
            .await
            .expect("open");
        let results: Vec<Result<Item, StorageError>> =
            futures::StreamExt::collect(snapshot.items).await;
        let errors = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(errors, 1, "one error for the one undecodable document");
        assert_eq!(
            results.len(),
            position + 1,
            "the {position} items before the document, then the error, then the end"
        );
        assert!(results.last().is_some_and(Result::is_err));
    }

    /// Pins the module doc's read-concern sentence and the error text: on a
    /// handle carrying `readConcern: local` (the production catalog handle)
    /// a snapshot session's first read without the explicit level runs at
    /// `local` and leaves no snapshot time, and a later read in a pinned
    /// session is refused with code 72. The mapped error names the server's
    /// code and message and carries no hex dump of the reply.
    #[tokio::test]
    async fn reads_without_the_snapshot_level_fail() {
        let Some(uri) = uri() else {
            eprintln!("SKIP reads_without_the_snapshot_level_fail: no MongoDB");
            return;
        };
        let _hooks = hooks().await;
        let engine = fixture(&uri).await;
        let accounts = engine.catalog_db.collection::<Document>("accounts");

        let mut session = engine
            .client
            .start_session()
            .snapshot(true)
            .await
            .expect("session");
        accounts
            .find_one(doc! { "_id": ACCOUNT })
            .session(&mut session)
            .await
            .expect("a read at the handle's level succeeds");
        assert!(
            session.snapshot_time().is_none(),
            "a read at readConcern local pins no snapshot time"
        );

        let mut session = engine
            .client
            .start_session()
            .snapshot(true)
            .await
            .expect("session");
        accounts
            .find_one(doc! { "_id": ACCOUNT })
            .read_concern(ReadConcern::snapshot())
            .session(&mut session)
            .await
            .expect("pinning read");
        assert!(session.snapshot_time().is_some(), "pinned");
        let err = accounts
            .find_one(doc! { "_id": ACCOUNT })
            .session(&mut session)
            .await
            .expect_err("the server refuses a non-snapshot level with atClusterTime");
        let raw = err.to_string();
        assert!(
            raw.contains("RawDocumentBuf"),
            "precondition: the driver's Display carries the reply dump: {raw}"
        );
        let StorageError::Internal(message) = map_mongo_error(&err) else {
            panic!("code 72 is an internal error");
        };
        assert!(
            message.starts_with("InvalidOptions (code 72): atClusterTime"),
            "{message}"
        );
        assert!(
            !message.contains("RawDocumentBuf") && !message.contains("server response"),
            "{message}"
        );
    }

    /// A standalone server refuses the open's first read with `NotAReplicaSet`;
    /// the open answers `Unsupported`, not an internal error. Runs only when
    /// `EXTENDDB_TEST_MONGODB_STANDALONE_URI` names a standalone `mongod`
    /// (for example one started with `docker run -p
    /// 127.0.0.1:27118:27017 mongo:7`, no `--replSet`); with
    /// `EXTENDDB_TEST_MONGODB_REQUIRED` set it fails rather than skips when
    /// that variable is missing, like every other live test here.
    #[tokio::test]
    async fn a_standalone_server_is_unsupported() {
        let uri = std::env::var("EXTENDDB_TEST_MONGODB_STANDALONE_URI")
            .ok()
            .filter(|u| !u.trim().is_empty());
        assert!(
            uri.is_some() || std::env::var_os("EXTENDDB_TEST_MONGODB_REQUIRED").is_none(),
            "EXTENDDB_TEST_MONGODB_REQUIRED is set but EXTENDDB_TEST_MONGODB_STANDALONE_URI is \
             not; the standalone test must run, not skip"
        );
        let Some(uri) = uri else {
            eprintln!(
                "SKIP a_standalone_server_is_unsupported: EXTENDDB_TEST_MONGODB_STANDALONE_URI \
                 is not set"
            );
            return;
        };
        let _hooks = hooks().await;
        let engine = fixture(&uri).await;
        let err = engine
            .open_table_snapshot(ACCOUNT, "anything")
            .await
            .expect_err("a standalone server cannot serve a snapshot");
        let StorageError::Unsupported(message) = err else {
            panic!("a standalone server is Unsupported");
        };
        eprintln!("standalone server: {message}");
        assert!(message.contains("NotAReplicaSet (code 123)"), "{message}");
    }

    #[tokio::test]
    async fn snapshot_marker_is_the_cluster_time() {
        let Some(uri) = uri() else {
            eprintln!("SKIP snapshot_marker_is_the_cluster_time: no MongoDB");
            return;
        };
        let _hooks = hooks().await;
        let engine = fixture(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("marker"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "marker", || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        })
        .await;
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "marker")
            .await
            .expect("open");
        assert_eq!(
            snapshot.info.kind,
            SnapshotInfo::KIND_MONGODB_SNAPSHOT_READ_CONCERN
        );
        assert!(!snapshot.key_ordered);
        assert!(!snapshot.info.marker.is_empty());
        let (secs, inc) = snapshot
            .info
            .marker
            .split_once(':')
            .expect("marker is <seconds>:<increment>");
        assert!(secs.parse::<u32>().unwrap() > 1_600_000_000);
        inc.parse::<u32>().unwrap();
    }

    /// A connection string that sets a read concern must not silently turn
    /// snapshot reads into ordinary reads: the explicit per-operation read
    /// concern wins, and the marker is still a cluster time.
    #[tokio::test]
    async fn uri_read_concern_does_not_defeat_the_snapshot() {
        let Some(uri) = uri() else {
            eprintln!("SKIP uri_read_concern_does_not_defeat_the_snapshot: no MongoDB");
            return;
        };
        let hooks = hooks().await;
        let sep = if uri.contains('?') { '&' } else { '?' };
        let uri = format!("{uri}{sep}readConcernLevel=majority");
        let engine = fixture(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("rc"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "rc", || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        })
        .await;
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "rc")
            .await
            .expect("open");
        assert!(
            !snapshot.info.marker.is_empty(),
            "snapshot time must be pinned"
        );
        // Release the session before the consistency run opens its own.
        drop(snapshot);
        snapshot_conformance::run(&hooks, &*engine, ACCOUNT, "rc_src", 50, EXPECT, || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        })
        .await;
    }

    /// The fixture's round-trip counter counts the snapshot stream and
    /// nothing else: a GSI backfill (which scans the base collection with
    /// `find`) leaves it where it was, and a stream at a page of three over
    /// twelve items moves it by at least four.
    #[tokio::test]
    async fn the_round_trip_counter_counts_only_the_snapshot_stream() {
        let Some(uri) = uri() else {
            eprintln!("SKIP the_round_trip_counter_counts_only_the_snapshot_stream: no MongoDB");
            return;
        };
        let _hooks = hooks().await;
        let engine = fixture(&uri).await;
        let pump = || async {
            let _ =
                extenddb_storage::WorkerStore::process_control_plane_transitions(&*engine).await;
        };
        let before = paging::fetches();
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("counted"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "counted", pump).await;
        snapshot_conformance::populate(&*engine, ACCOUNT, "counted", 12).await;
        let update: extenddb_core::types::UpdateTableInput =
            serde_json::from_value(serde_json::json!({
                "TableName": "counted",
                "AttributeDefinitions": [{"AttributeName": "u", "AttributeType": "S"}],
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "gu",
                    "KeySchema": [{"AttributeName": "u", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "KEYS_ONLY"}
                }}]
            }))
            .expect("UpdateTable input");
        engine
            .update_table(ACCOUNT, update)
            .await
            .expect("add a GSI");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "counted", pump).await;
        assert_eq!(
            paging::fetches(),
            before,
            "the backfill of the new GSI moved the snapshot round-trip counter"
        );
        let page = PageSize::set(3);
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "counted")
            .await
            .expect("open");
        let items: Vec<Result<Item, StorageError>> =
            futures::StreamExt::collect(snapshot.items).await;
        let items: Vec<Item> = items.into_iter().map(|r| r.expect("item")).collect();
        drop(page);
        assert_eq!(items.len(), 12);
        let counted = paging::fetches() - before;
        assert!(
            counted >= 4,
            "{counted} round trips for twelve items at a page of three"
        );
    }
}
