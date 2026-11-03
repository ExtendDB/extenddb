// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Consistent table snapshots: the one obligation a backend has toward the
//! engine-owned backup path.
//!
//! A backup needs two things from a backend that nothing else needs: the
//! table's definition and every item in it, both as of one instant. The
//! definition lives in the catalog and the items in the data store, and on
//! `PostgreSQL` with a separate data database (the layout `extenddb init`
//! creates) those are different databases, so no single transaction covers
//! both. Each backend therefore has its own way of fixing the instant:
//! `PostgreSQL` with a separate data database holds the catalog row `FOR
//! SHARE` from the definition read until the data transaction's snapshot is
//! pinned; `PostgreSQL` on one database shared by catalog and data reads both
//! in one `REPEATABLE READ` transaction whose first read pins the snapshot,
//! like `SQLite`; `SQLite` reads catalog and data inside one deferred read
//! transaction whose first read pins the WAL snapshot, taking no lock;
//! `MongoDB` reads both through one session with snapshot read concern.
//! Cassandra has no read that fixes an instant across a table and returns
//! [`StorageError::Unsupported`]; its in-catalog logical backup (ADR-0019)
//! stays the backup path for that backend.
//!
//! What that fixes: on `PostgreSQL` with a separate data database,
//! `UpdateTable` and `DeleteTable` take the row `FOR UPDATE`, so neither
//! commits between the definition read and the data pin; background workers
//! that flip an index's status (a backfill finishing) do not take the row
//! lock, so an index may be reported in the status it had at the definition
//! read while the items are a few milliseconds newer. On `SQLite`, `MongoDB`
//! and `PostgreSQL` on one shared database the definition and the items come
//! from one snapshot, so no write of any kind sits between them; on that
//! `PostgreSQL` layout a `DeleteTable` that commits between the snapshot and
//! the open's lock on the data table makes the open report the table as not
//! found ([`paging::OpenStage::AfterDefinition`]).
//!
//! Indexes are reported in whatever lifecycle status they had, except that
//! an index being deleted is dropped from the description
//! ([`drop_deleting_indexes`]), and on `PostgreSQL` a GSI row whose data table
//! is missing (not an expected state: `UpdateTable` creates the data table
//! before it commits the row) is left out as the in-catalog backup leaves it
//! out. A restore recreates the indexes the
//! description lists and builds them from the items, so an index that was
//! still backfilling at the snapshot comes back complete; an index that is
//! not in the description does not come back at all, which is why a
//! `CREATING` index with a data table is kept.
//!
//! The returned [`TableSnapshot`] owns the open transaction, session, or
//! connection for as long as its `items` stream is alive. Dropping the stream
//! releases it. The stream must be dropped inside a Tokio runtime: on the
//! sqlx backends (`SQLite`, `PostgreSQL`) a stream that has not reached its end
//! (including one never polled) returns its pooled connection on drop by
//! spawning onto the current runtime, and dropping it outside one panics
//! ("this functionality requires a Tokio context"); a `MongoDB` stream drops
//! anywhere. A caller tested only on `MongoDB` will not see this, so test drops
//! inside a runtime. The caller must
//! drain or drop the stream promptly: a held `PostgreSQL` snapshot delays
//! vacuum on every table in the data database, a held `SQLite` read
//! transaction stops WAL checkpoints, and `MongoDB` keeps at least
//! `minSnapshotHistoryWindowInSeconds` (default 300 s) of snapshot history,
//! past which the server may discard the snapshot and the stream fails with
//! [`StorageError::SnapshotExpired`].
//!
//! The design overview (`docs/design/15-backup-restore.md`, #396) states the
//! rules the callers of this trait honour; the module docs of this file and
//! of each backend's `snapshot_engine.rs` are the record of what the
//! snapshots do.

use extenddb_core::backup_format::KeyOrder;
use extenddb_core::types::{IndexStatus, Item, TableDescription};
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::error::StorageError;

/// How a backend took its snapshot: the manifest's `snapshot` member, one
/// definition shared with the backup format. `kind` is one of the `KIND_*`
/// constants on the type; `marker` is `pg_current_snapshot()` text on
/// `PostgreSQL`, the cluster time `<seconds>:<increment>` on `MongoDB`, empty on
/// `SQLite` (a WAL read transaction has no addressable position). The marker
/// is recorded for a later change-log archive to anchor against (which
/// writes committed before the snapshot); it is not a resume point. No
/// backend can reopen a snapshot at its marker, so a stream that fails is
/// started over from a new open, and on `SQLite`, where the marker is empty,
/// a change-log archive has nothing to anchor on.
pub use extenddb_core::backup_format::SnapshotDescriptor as SnapshotInfo;

/// Page size of a snapshot stream and, under `test-hooks`, the knob and the
/// round-trip counter the conformance check uses. One knob for every backend,
/// so the check can set it and observe its effect without a per-backend
/// setter that a test could forget to call.
pub mod paging {
    /// Rows or documents a backend fetches per round trip. Measured on
    /// localhost `PostgreSQL` 16 with 318-byte items: 1000 and 10000 read a
    /// million rows in the same time; 1000 holds less memory. Over a 1 ms
    /// network link the larger fetch would save about a second per million
    /// rows, which is why the size is a constant and not a configuration knob.
    ///
    /// The bound is rows, not bytes. `PostgreSQL`'s `FETCH` and `SQLite`'s
    /// `LIMIT` each hold up to this many rows of item text in memory at
    /// once, and at the default 400 KB item limit one row's text can reach
    /// 27.7 MB (the measured worst case in #394's line-cap table), so one
    /// fetch is bounded by rows, not bytes. A byte budget (keyset paging with a byte cutoff) is
    /// required before the stream gets a caller; the backup design assigns
    /// it to the backup job PR. `MongoDB`'s batches are bounded by the
    /// server's 16 MiB reply limit as well as by this count.
    pub const DEFAULT_FETCH_BATCH: usize = 1000;

    #[cfg(feature = "test-hooks")]
    static OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    #[cfg(feature = "test-hooks")]
    static FETCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// The page size a stream is built with. Backends read it once when they
    /// open a stream, so a change to the knob during a stream cannot end it
    /// early or late.
    #[must_use]
    pub fn fetch_batch() -> usize {
        #[cfg(feature = "test-hooks")]
        {
            match OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
                0 => DEFAULT_FETCH_BATCH,
                n => n,
            }
        }
        #[cfg(not(feature = "test-hooks"))]
        {
            DEFAULT_FETCH_BATCH
        }
    }

    /// Record one round trip to the backend for a page of items. A no-op
    /// without `test-hooks`.
    ///
    /// The counter is trusted: the conformance check reads it to confirm
    /// the backend paged at the knob's size, so a backend must call this
    /// exactly once per round trip (one `FETCH`, one page query, one
    /// `find` or `getMore`), never per item. A backend that ignored the
    /// knob and called it per item would pass the paging assertions with
    /// the whole table read in one trip. `MongoDB`'s driver hides `getMore`
    /// from the stream, so its test fixture feeds the counter from command
    /// monitoring (what the server received) for that reason, and its
    /// engine does not call this.
    pub fn record_fetch() {
        #[cfg(feature = "test-hooks")]
        FETCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the page size for every stream opened afterwards; `0` restores the
    /// default.
    #[cfg(feature = "test-hooks")]
    pub fn set_fetch_batch(rows: usize) {
        OVERRIDE.store(rows, std::sync::atomic::Ordering::Relaxed);
    }

    /// Round trips recorded by every stream in this process so far.
    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn fetches() -> u64 {
        FETCHES.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The two points in `open_table_snapshot` at which every backend calls
    /// [`open_pause`]. Under `test-hooks` the conformance check holds an open
    /// at one of them while it commits writes and an `UpdateTable`, to check
    /// that the description and the items come from one instant.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum OpenStage {
        /// The definition has been read (on `MongoDB`, the `tables` document;
        /// its index documents are read after this point, in the same
        /// session). On `PostgreSQL` with a separate data database the data
        /// transaction has begun and holds `ACCESS SHARE` on the data table,
        /// the catalog barrier is still held, and the next statement is the
        /// `SELECT pg_current_snapshot()` that pins the data snapshot, so a
        /// barrier released any earlier would fall inside the pause. On
        /// `SQLite`, `MongoDB` and `PostgreSQL` with one shared database the first
        /// catalog read already pinned the snapshot; on the shared `PostgreSQL`
        /// layout the pause is before the `LOCK TABLE`, in the window where a
        /// `DeleteTable` can still drop the data table.
        AfterDefinition,
        /// The data snapshot is pinned; the item read (`PostgreSQL`'s `DECLARE
        /// ... CURSOR`, `MongoDB`'s `find`, `SQLite`'s first page) has not
        /// started.
        BeforeItems,
    }

    /// Hold the open at `stage` if a test armed a pause for it. Without
    /// `test-hooks` this is a ready future and the open never waits (it stays
    /// `async` so the backends call it the same way in both builds).
    #[cfg_attr(not(feature = "test-hooks"), allow(clippy::unused_async))]
    pub async fn open_pause(stage: OpenStage) {
        #[cfg(feature = "test-hooks")]
        PAUSE.reached(stage).await;
        #[cfg(not(feature = "test-hooks"))]
        let _ = stage;
    }

    #[cfg(feature = "test-hooks")]
    static PAUSE: crate::test_pause::PausePoint<OpenStage> = crate::test_pause::PausePoint::new();

    /// A pause armed by [`arm_open_pause`]. Dropping it disarms the pause and
    /// releases an open waiting at it, so a failing test cannot leave a pause
    /// armed for the next open in the process.
    #[cfg(feature = "test-hooks")]
    pub type OpenPause = crate::test_pause::Pause<OpenStage>;

    /// Arm a pause at `stage` for the next open that reaches it (one open,
    /// once), whatever its engine or table. Process-global like the knob and
    /// the counter: every test that opens a snapshot or touches these
    /// statics holds `snapshot_conformance::hooks_lock`, so no foreign open
    /// can take the pause.
    ///
    /// # Panics
    ///
    /// In debug builds, when a pause is already armed: a second arm would
    /// replace the first silently, and the first handle's drop would then
    /// disarm the second.
    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn arm_open_pause(stage: OpenStage) -> OpenPause {
        PAUSE.arm(stage)
    }
}

/// Drop indexes that were being deleted at the snapshot instant and keep
/// every other one, whatever its build state. One rule for every backend: a
/// restore recreates the indexes in the description and builds them from the
/// items, so a `CREATING` or `UPDATING` index comes back complete, and an
/// index the user had removed must not come back. No in-tree backend
/// currently reports a `DELETING` index (each removes the index row in the
/// operation that deletes it); the rule is here so one that does cannot leak
/// a deleted index into a backup.
pub fn drop_deleting_indexes(description: &mut TableDescription) {
    if let Some(gsis) = description.global_secondary_indexes.as_mut() {
        gsis.retain(|g| g.index_status != "DELETING");
    }
    if let Some(vectors) = description.vector_indexes.as_mut() {
        vectors.retain(|v| v.index_status != IndexStatus::Deleting);
    }
}

/// Whether a key schema has the order the backup format defines, by the
/// format's own rule (`KeyOrder::resolve`): one hash key listed first, at
/// most one range key with a name of its own, every key attribute defined
/// exactly once. A table that fails the
/// rule streams unordered and complete (multi-part keys; a range key whose
/// attribute definition is missing or duplicated, which would otherwise make
/// a backend page by the hash key alone and lose rows). Backends use this to
/// decide `key_ordered`, so a stream they mark ordered is one the manifest
/// parser accepts.
#[must_use]
pub fn key_schema_is_orderable(description: &TableDescription) -> bool {
    KeyOrder::resolve(&description.key_schema, &description.attribute_definitions).is_ok()
}

/// A consistent view of one table: its definition, how the snapshot was
/// taken, and every item as of the same instant.
pub struct TableSnapshot {
    /// The table as the storage layer's `describe_table` would have reported
    /// it at the snapshot instant: key schema, attribute definitions, indexes
    /// (minus any being deleted; see [`drop_deleting_indexes`]), billing,
    /// throughput, class, SSE, vector indexes, ARN, id and creation time.
    /// Streams, TTL and tags are not part of a backup and may be absent.
    ///
    /// This is the storage-level description, not the API response: the
    /// engine's `prepare_for_response` is not applied, so `restore_summary`
    /// may be present on an `ACTIVE` table that was restored (the API drops
    /// it once the table is not `CREATING`), `table_throughput_mode_summary`
    /// is unset (the API derives it from `billing_mode_summary`), and
    /// `item_count` and `table_size_bytes` are the catalog row's values, the
    /// stats pass's last figures and not the instant's: a caller that needs
    /// the count as of the snapshot counts the stream.
    pub description: TableDescription,
    /// Snapshot mechanism and position.
    pub info: SnapshotInfo,
    /// True when `items` yields rows in the backup format's key order
    /// (`extenddb_core::backup_format::key_order`): hash key by its text
    /// encoding, bytewise, so a number hash key sorts as text; range key `S`
    /// and `B` bytewise, `N` numeric. The SQL backends produce it from their
    /// primary key columns. On `PostgreSQL` the rule is the collation's name:
    /// the table is ordered only when its text key columns (`pk`, and `sk_s`
    /// for an `S` range key) have a collation named "C" or "POSIX". Any other
    /// collation, including "default" on a database whose own collation is
    /// C, streams unordered (complete, only not ordered), because ordering a
    /// column that does not already sort bytewise would sort the whole table
    /// before the first row. A backup records the flag so a restore into a
    /// backend that benefits from ordered input can rely on it; a restore
    /// must produce the same table either way.
    pub key_ordered: bool,
    /// Every item in the table as of the snapshot. Holds the snapshot's
    /// transaction or session until it ends or is dropped; drop it inside a
    /// Tokio runtime (the sqlx backends panic when a stream that has not
    /// reached its end is dropped outside one; see the module doc). The first
    /// `Err` ends the stream on every backend: nothing follows it, and since
    /// no backend can reopen a snapshot at its marker, any error means the
    /// caller starts over with a new open ([`StorageError::SnapshotExpired`]
    /// is the one worth retrying at once; the others name a fault).
    ///
    /// Items are yielded as stored, after the wire deserializer: there is no
    /// promise that every item has its key attributes with the declared
    /// types (the backends do not validate stored rows on the way out), and
    /// a valid `N` value arrives normalized (the deserializer rewrites it)
    /// while an invalid one stored by an earlier build passes through raw.
    /// Items whose TTL has expired but that the sweep has not yet removed
    /// are rows and are yielded like any other.
    pub items: BoxStream<'static, Result<Item, StorageError>>,
}

impl std::fmt::Debug for TableSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableSnapshot")
            .field("table", &self.description.table_name)
            .field("info", &self.info)
            .field("key_ordered", &self.key_ordered)
            .finish_non_exhaustive()
    }
}

/// Open a consistent snapshot of one table.
///
/// The one method has no default body on purpose: a backend that cannot
/// take a consistent snapshot must say so (the Cassandra implementation is
/// the refusal, `StorageError::Unsupported` before it resolves the table),
/// and a default that refused would let a new backend compile with the
/// backup path silently absent. Likewise `StorageError` is not
/// `#[non_exhaustive]` on purpose: the engine's exhaustive match over it
/// (`storage_err_to_dynamo`) is what gives [`StorageError::SnapshotExpired`]
/// its wire mapping, and a new variant must be mapped there, not fall into
/// a wildcard. Both are breaking changes for an out-of-tree backend, and
/// both are meant to be.
pub trait SnapshotEngine: Send + Sync {
    /// Open a snapshot of `table_name` in `account_id`.
    ///
    /// The table must be `ACTIVE` (a restore target is `CREATING` and is
    /// refused with [`StorageError::TableNotActive`]). The definition and the
    /// first item read are fixed at the same instant; writes committed after
    /// that instant are not visible in `items`, and items committed before it
    /// all are, exactly once.
    ///
    /// Resources, until the stream ends or is dropped: on `PostgreSQL` one
    /// connection and one snapshot permit (with a separate data database the
    /// open also holds a catalog connection until the data snapshot is
    /// pinned); on file-backed `SQLite` one pool connection and one backup
    /// slot; on in-memory `SQLite` one backup slot and no connection (the
    /// stream serves a copy read during the open); on `MongoDB` one session
    /// and one server cursor. The caller bounds how many snapshots are open
    /// at once; `PostgreSQL` and `SQLite` also refuse over their own bound
    /// (`LimitExceeded`) rather than queue, `MongoDB` has no bound of its own.
    ///
    /// The stream ends at its first `Err` on every backend; a caller that
    /// sees an error has not seen the whole table and must not treat what it
    /// read as complete. No snapshot can be reopened at its marker, so any
    /// error from the stream means a new open from the beginning; nothing
    /// read from the failed stream may be combined with the new one.
    /// Drop the stream inside a Tokio runtime: the sqlx
    /// backends return a connection on drop through the runtime and panic
    /// outside one when the stream has not reached its end, `MongoDB` does
    /// not, so a caller tested only on `MongoDB` will not see the panic.
    ///
    /// A stream left idle is ended by the server, not by this crate: on
    /// `PostgreSQL` an operator's `idle_in_transaction_session_timeout` ends
    /// the backend between two fetches and the stream reports `Internal`
    /// (not `SnapshotExpired`); on `MongoDB` the cursor has the server's
    /// default idle timeout (10 minutes; it is opened without
    /// `noCursorTimeout`), after which the next batch reports `Internal`
    /// likewise. A caller whose consumer can stall (an upload) must keep
    /// polling or expect to start over.
    ///
    /// # Errors
    ///
    /// [`StorageError::TableNotFound`] when the table does not exist in the
    /// account, [`StorageError::TableNotActive`] (or the backend's
    /// equivalent) when it is not `ACTIVE`, [`StorageError::Unsupported`]
    /// when the backend cannot take a consistent snapshot at all (Cassandra,
    /// which answers `Unsupported` before it resolves the table and so never
    /// returns `TableNotFound` or `TableNotActive`) or in this configuration
    /// (a `MongoDB` server that is not a replica set member, which cannot
    /// serve snapshot reads),
    /// [`StorageError::LimitExceeded`] when the backend's bound on open
    /// snapshots is reached (`PostgreSQL`, `SQLite`),
    /// [`StorageError::Validation`] when the account id is not one the
    /// backend accepts (`MongoDB` validates it before the catalog read), and
    /// [`StorageError::Internal`] for a database error during the open.
    ///
    /// From the stream: [`StorageError::SnapshotExpired`] when the backend
    /// discarded the snapshot before the stream was drained (retry with a
    /// new open), and [`StorageError::Internal`] for a database error during
    /// a fetch (a terminated backend, a timed-out cursor, a lost connection)
    /// or a stored row that does not parse as an item.
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::GsiDescription;

    #[test]
    fn deleting_indexes_are_dropped_and_others_kept() {
        let gsi = |name: &str, status: &str| -> GsiDescription {
            serde_json::from_value(serde_json::json!({
                "IndexName": name,
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "IndexStatus": status,
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "IndexArn": format!("arn:aws:dynamodb:us-east-1:123456789012:table/t/index/{name}")
            }))
            .expect("gsi description")
        };
        let mut description = TableDescription {
            global_secondary_indexes: Some(vec![
                gsi("kept_active", "ACTIVE"),
                gsi("kept_creating", "CREATING"),
                gsi("gone", "DELETING"),
            ]),
            ..TableDescription::default()
        };
        drop_deleting_indexes(&mut description);
        let names: Vec<_> = description
            .global_secondary_indexes
            .unwrap()
            .into_iter()
            .map(|g| g.index_name)
            .collect();
        assert_eq!(names, ["kept_active", "kept_creating"]);
    }

    #[test]
    fn deleting_vector_indexes_are_dropped_and_others_kept() {
        let vector = |name: &str, status: &str| -> extenddb_core::types::VectorIndexDescription {
            serde_json::from_value(serde_json::json!({
                "IndexName": name,
                "VectorAttribute": {"AttributeName": "emb"},
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "IndexStatus": status,
                "IndexSizeBytes": 0,
                "ItemCount": 0,
                "IndexArn": format!("arn:aws:dynamodb:us-east-1:123456789012:table/t/index/{name}")
            }))
            .expect("vector index description")
        };
        let mut description = TableDescription {
            vector_indexes: Some(vec![
                vector("kept_active", "ACTIVE"),
                vector("gone", "DELETING"),
                vector("kept_creating", "CREATING"),
            ]),
            ..TableDescription::default()
        };
        drop_deleting_indexes(&mut description);
        let names: Vec<_> = description
            .vector_indexes
            .unwrap()
            .into_iter()
            .map(|v| v.index_name)
            .collect();
        assert_eq!(names, ["kept_active", "kept_creating"]);
    }
}
