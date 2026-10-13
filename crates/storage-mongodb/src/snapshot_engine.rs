// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for MongoDB.
//!
//! One snapshot session covers catalog and data: a session started with
//! `snapshot: true` runs every read at the cluster time of its first read
//! (MongoDB 5.0+ snapshot reads outside a transaction). The `tables` document
//! is read first and pins the time; the index documents and the base
//! collection cursor then read at the same time, so the definition and the
//! items describe one instant without any lock.
//!
//! Every read sets `readConcern: snapshot` explicitly. The driver only
//! inserts that level when the operation carries no read concern of its own,
//! and a connection string with `readConcernLevel=` (which the engine
//! accepts) would otherwise turn the whole session into ordinary reads at
//! three different times with nothing failing. After the first read the
//! session must report a snapshot time; if it does not, the server did not
//! honour the snapshot and the open fails rather than returning an
//! unmarked, inconsistent stream.
//!
//! The server keeps snapshot history for `minSnapshotHistoryWindowInSeconds`
//! (default 300). A stream that is still open past that window fails with
//! `SnapshotTooOld` (code 239), and a server that cannot serve the snapshot
//! answers `SnapshotUnavailable` (code 246); both surface as
//! [`StorageError::SnapshotExpired`] and the caller restarts the backup. At
//! typical cursor throughput the window bounds a single backup at tens of
//! millions of documents unless the server parameter is raised; the backup
//! design (#396) records that as an operator setting.
//!
//! Items come back in `_id` order. `_id` is derived from the primary key but
//! its byte order is not the `DynamoDB` key order for number keys, so the
//! snapshot reports `key_ordered = false`.

use extenddb_core::types::Item;
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::drop_deleting_indexes;
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

/// Documents per `getMore`. Tests lower it through
/// `set_snapshot_fetch_batch_for_tests` so a small collection spans several
/// batches.
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Override for tests; 0 means the default. Compiled only with the
/// `test-hooks` feature, which the crate's own tests enable through a
/// dev-dependency on itself.
#[cfg(feature = "test-hooks")]
static BATCH_SIZE_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "test-hooks")]
fn batch_size() -> usize {
    match BATCH_SIZE_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => DEFAULT_BATCH_SIZE,
        n => n,
    }
}

#[cfg(not(feature = "test-hooks"))]
fn batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Set the documents per `getMore` for every snapshot opened afterwards; see
/// the PostgreSQL and SQLite equivalents.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn set_snapshot_fetch_batch_for_tests(docs: usize) {
    BATCH_SIZE_OVERRIDE.store(docs, std::sync::atomic::Ordering::Relaxed);
}

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
            .map_err(|e| map_mongo_error(&e))?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let status = table_doc.get_str("table_status").unwrap_or("");
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }
        let table_id = table_doc
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("missing table_id".to_owned()))?
            .to_owned();

        // The first read must have pinned a cluster time; without one the
        // server is not serving snapshot reads and the stream would not be
        // consistent.
        let marker = session
            .snapshot_time()
            .map(|t| format!("{}:{}", t.time, t.increment))
            .ok_or_else(|| {
                StorageError::Internal(
                    "MongoDB did not return a snapshot time; snapshot reads require a replica \
                     set on MongoDB 5.0 or later"
                        .to_owned(),
                )
            })?;

        let mut description = self
            .doc_to_table_description_in(&table_doc, Some(&mut session))
            .await?;
        drop_deleting_indexes(&mut description);

        let collection = self
            .data_db
            .collection::<Document>(&data_collection_name(&table_id));
        let cursor = collection
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .batch_size(u32::try_from(batch_size()).unwrap_or(u32::MAX))
            .read_concern(ReadConcern::snapshot())
            .session(&mut session)
            .await
            .map_err(|e| map_mongo_error(&e))?;

        Ok(TableSnapshot {
            description,
            info: SnapshotInfo {
                kind: "mongodb_snapshot_read_concern".to_owned(),
                marker,
            },
            key_ordered: false,
            items: session_stream(session, cursor),
        })
    }
}

/// Stream items from a session cursor. The session and cursor live in the
/// stream's state; dropping the stream ends both.
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
        match state.cursor.next(&mut state.session).await {
            Some(Ok(doc)) => Some((document_to_item(&doc), state)),
            Some(Err(e)) => {
                state.done = true;
                Some((Err(map_mongo_error(&e)), state))
            }
            None => None,
        }
    }))
}

fn map_mongo_error(e: &mongodb::error::Error) -> StorageError {
    // `find` and `getMore` failures arrive as command errors.
    let snapshot_gone = matches!(
        &*e.kind,
        mongodb::error::ErrorKind::Command(c)
            if c.code == SNAPSHOT_TOO_OLD || c.code == SNAPSHOT_UNAVAILABLE
    );
    if snapshot_gone {
        return StorageError::SnapshotExpired(e.to_string());
    }
    StorageError::Internal(e.to_string())
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
    //! skips otherwise. Uses the fixed `extenddb_catalog` / `extenddb_data`
    //! databases, which it drops first, so point it at a disposable server.
    //! `devtools/run-mongodb-tests --mongo-only` starts a suitable one.

    use extenddb_storage::bootstrapper::Bootstrapper;
    use extenddb_storage::snapshot_conformance;
    use extenddb_storage::{SnapshotEngine, TableEngine};

    use super::*;

    const ACCOUNT: &str = "123456789012";

    /// Every test drops and recreates the two fixed databases, so the tests
    /// in this module run one at a time whatever `--test-threads` says.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// The replica set URI, or `None` to skip. With
    /// `EXTENDDB_TEST_MONGODB_REQUIRED` set a missing URI is a failure, so CI
    /// cannot pass by skipping.
    fn uri() -> Option<String> {
        let uri = std::env::var("EXTENDDB_TEST_MONGODB_URI")
            .ok()
            .filter(|u| !u.trim().is_empty());
        if uri.is_none() && std::env::var_os("EXTENDDB_TEST_MONGODB_REQUIRED").is_some() {
            panic!(
                "EXTENDDB_TEST_MONGODB_REQUIRED is set but EXTENDDB_TEST_MONGODB_URI is not; \
                 the MongoDB snapshot tests must run, not skip"
            );
        }
        uri
    }

    /// Removes the scratch config directory whether or not bootstrap succeeds.
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn engine(uri: &str) -> MongoEngine {
        let client = mongodb::Client::with_uri_str(uri).await.expect("client");
        for db in ["extenddb_catalog", "extenddb_data"] {
            client.database(db).drop().await.expect("drop scratch db");
        }
        let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
            || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            std::path::PathBuf::from,
        );
        let dir = target.join(format!("eddb-snap-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("tempdir");
        let config = dir.join("extenddb.toml");
        // The URI may carry credentials: write the file owner-only and
        // remove it whether or not bootstrap succeeds.
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&config)
                .expect("config file");
            f.write_all(format!("[storage.mongodb]\nconnection_string = \"{uri}\"\n").as_bytes())
                .expect("config");
        }
        let _cleanup = Cleanup(dir.clone());
        let bootstrapper =
            crate::MongoBootstrapper::from_config(config.to_str().expect("UTF-8"), &[])
                .await
                .expect("bootstrapper");
        bootstrapper.create_catalog_db().await.expect("catalog db");
        bootstrapper.create_data_db().await.expect("data db");
        bootstrapper
            .run_catalog_migrations()
            .await
            .expect("catalog migrations");
        bootstrapper
            .run_data_migrations()
            .await
            .expect("data migrations");

        client
            .database("extenddb_catalog")
            .collection::<Document>("accounts")
            .insert_one(doc! { "_id": ACCOUNT, "account_name": "snapshot-test" })
            .await
            .expect("account");
        client
            .database("extenddb_catalog")
            .collection::<Document>("settings")
            .update_one(
                doc! { "_id": "control_plane_delay_seconds" },
                doc! { "$set": { "value": "0" } },
            )
            .upsert(true)
            .await
            .expect("setting");
        MongoEngine::new(
            uri,
            "us-east-1",
            None,
            mongodb::options::ReadConcern::majority(),
        )
        .await
        .expect("engine")
    }

    #[tokio::test]
    async fn snapshot_is_consistent_under_concurrent_writes() {
        let Some(uri) = uri() else {
            eprintln!("SKIP snapshot_is_consistent_under_concurrent_writes: no MongoDB");
            return;
        };
        let _serial = SERIAL.lock().await;
        let engine = engine(&uri).await;
        snapshot_conformance::run(
            &engine,
            ACCOUNT,
            "snap_src",
            50,
            set_snapshot_fetch_batch_for_tests,
            || async {
                let _ =
                    extenddb_storage::WorkerStore::process_control_plane_transitions(&engine).await;
            },
        )
        .await;
        snapshot_conformance::run_refusals(&engine, ACCOUNT).await;
    }

    #[tokio::test]
    async fn snapshot_marker_is_the_cluster_time() {
        let Some(uri) = uri() else {
            eprintln!("SKIP snapshot_marker_is_the_cluster_time: no MongoDB");
            return;
        };
        let _serial = SERIAL.lock().await;
        let engine = engine(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("marker"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&engine, ACCOUNT, "marker", || async {
            let _ = extenddb_storage::WorkerStore::process_control_plane_transitions(&engine).await;
        })
        .await;
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "marker")
            .await
            .expect("open");
        assert_eq!(snapshot.info.kind, "mongodb_snapshot_read_concern");
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
        let _serial = SERIAL.lock().await;
        let sep = if uri.contains('?') { '&' } else { '?' };
        let uri = format!("{uri}{sep}readConcernLevel=majority");
        let engine = engine(&uri).await;
        engine
            .create_table(ACCOUNT, snapshot_conformance::table_input("rc"))
            .await
            .expect("create");
        snapshot_conformance::wait_active(&engine, ACCOUNT, "rc", || async {
            let _ = extenddb_storage::WorkerStore::process_control_plane_transitions(&engine).await;
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
        snapshot_conformance::run(
            &engine,
            ACCOUNT,
            "rc_src",
            50,
            set_snapshot_fetch_batch_for_tests,
            || async {
                let _ =
                    extenddb_storage::WorkerStore::process_control_plane_transitions(&engine).await;
            },
        )
        .await;
    }
}
