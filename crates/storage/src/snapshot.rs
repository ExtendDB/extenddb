// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Consistent table snapshots: the one obligation a backend has toward the
//! engine-owned backup path.
//!
//! A backup needs two things from a backend that nothing else needs: the
//! table's definition and every item in it, both as of one instant. The
//! definition lives in the catalog and the items in the data store, and on
//! PostgreSQL those are different databases, so no single transaction covers
//! both. Each backend therefore has its own way of fixing the instant:
//! PostgreSQL holds the catalog row `FOR SHARE` from the definition read
//! until the data transaction's snapshot is pinned; SQLite reads catalog and
//! data inside one deferred read transaction whose first read pins the WAL
//! snapshot, taking no lock; MongoDB reads both through one session with
//! snapshot read concern.
//!
//! What that fixes: on PostgreSQL, `UpdateTable` and `DeleteTable` take the
//! row `FOR UPDATE`, so neither commits between the definition read and the
//! data pin; background workers that flip an index's status (a backfill
//! finishing) do not take the row lock, so an index may be reported in the
//! status it had at the definition read while the items are a few
//! milliseconds newer. On SQLite and MongoDB the definition and the items
//! come from one snapshot, so no write of any kind sits between them.
//!
//! Indexes are reported in whatever lifecycle status they had, except that
//! an index being deleted is dropped from the description
//! ([`drop_deleting_indexes`]). A restore recreates the indexes the
//! description lists and builds them from the items, so an index that was
//! still backfilling at the snapshot comes back complete; an index that is
//! not in the description does not come back at all, which is why a
//! `CREATING` index is kept. This is the rule the in-catalog backup already
//! applies (it records every index row and none of its build state).
//!
//! The returned [`TableSnapshot`] owns the open transaction, session, or
//! connection for as long as its `items` stream is alive. Dropping the stream
//! releases it; drop it inside the Tokio runtime that opened it (the pooled
//! connection's drop schedules its return on the runtime). The caller must
//! drain or drop the stream promptly: a held PostgreSQL snapshot delays
//! vacuum on every table in the data database, a held SQLite read
//! transaction stops WAL checkpoints, and MongoDB bounds snapshot history by
//! `minSnapshotHistoryWindowInSeconds` (default 300 s), after which the
//! stream fails with [`StorageError::SnapshotExpired`].

use extenddb_core::types::{IndexStatus, Item, KeyType, TableDescription};
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::error::StorageError;

/// How a backend took its snapshot, recorded in the backup manifest. This is
/// the manifest's `snapshot` member (`extenddb_core::backup_format::
/// SnapshotDescriptor`) under the storage crate's name; the two are one
/// shape and the backup writer converts field for field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// The mechanism, named by the backend: `postgres_repeatable_read`,
    /// `sqlite_wal_read_transaction`, `mongodb_snapshot_read_concern`.
    pub kind: String,
    /// Backend-specific position: `pg_current_snapshot()` text on
    /// PostgreSQL, the cluster time `<seconds>:<increment>` on MongoDB,
    /// empty on SQLite (a WAL read transaction has no addressable position).
    /// Diagnostic today; a later change-log archive anchors against it.
    pub marker: String,
}

/// Drop indexes that were being deleted at the snapshot instant and keep
/// every other one, whatever its build state. One rule for every backend: a
/// restore recreates the indexes in the description and builds them from the
/// items, so a `CREATING` or `UPDATING` index comes back complete, and an
/// index the user had removed must not come back.
pub fn drop_deleting_indexes(description: &mut TableDescription) {
    if let Some(gsis) = description.global_secondary_indexes.as_mut() {
        gsis.retain(|g| g.index_status != "DELETING");
    }
    if let Some(vectors) = description.vector_indexes.as_mut() {
        vectors.retain(|v| v.index_status != IndexStatus::Deleting);
    }
}

/// Whether a key schema has the order the backup format defines: a hash key
/// with at most one range key. Multi-part keys stream unordered. Backends use
/// this to decide `key_ordered`; the comparator itself lives in
/// `extenddb_core::backup_format::key_order`.
#[must_use]
pub fn key_schema_is_orderable(description: &TableDescription) -> bool {
    let hashes = description
        .key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .count();
    let ranges = description
        .key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
        .count();
    hashes == 1 && ranges <= 1
}

/// A consistent view of one table: its definition, how the snapshot was
/// taken, and every item as of the same instant.
pub struct TableSnapshot {
    /// The table as `DescribeTable` would have reported it at the snapshot
    /// instant: key schema, attribute definitions, indexes (minus any being
    /// deleted; see [`drop_deleting_indexes`]), billing, throughput, class,
    /// SSE, vector indexes, counts, ARN, id and creation time. Streams, TTL
    /// and tags are not part of a backup and may be absent.
    pub description: TableDescription,
    /// Snapshot mechanism and position.
    pub info: SnapshotInfo,
    /// True when `items` yields rows in the backup format's key order
    /// (`extenddb_core::backup_format::key_order`): hash key by its text
    /// encoding, bytewise, so a number hash key sorts as text; range key `S`
    /// and `B` bytewise, `N` numeric. The SQL backends produce it from their
    /// primary key columns (PostgreSQL with `COLLATE "C"` on the text
    /// columns). A backup records it so a restore into a
    /// backend that benefits from ordered input can rely on it; a restore
    /// must produce the same table either way.
    pub key_ordered: bool,
    /// Every item in the table as of the snapshot. Holds the snapshot's
    /// transaction or session until dropped; drop it inside the runtime that
    /// opened it.
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
pub trait SnapshotEngine: Send + Sync {
    /// Open a snapshot of `table_name` in `account_id`.
    ///
    /// The table must be `ACTIVE`. The definition and the first item read
    /// are fixed at the same instant; writes committed after that instant
    /// are not visible in `items`, and items committed before it all are,
    /// exactly once.
    ///
    /// # Errors
    ///
    /// [`StorageError::TableNotFound`] when the table does not exist in the
    /// account, [`StorageError::TableNotActive`] (or the backend's
    /// equivalent) when it is not `ACTIVE`, [`StorageError::Unsupported`]
    /// when the backend configuration cannot hold a snapshot open alongside
    /// writers (SQLite in-memory databases, which have one connection), and
    /// [`StorageError::SnapshotExpired`] from the stream when the backend
    /// discarded the snapshot before the stream was drained.
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>>;
}
