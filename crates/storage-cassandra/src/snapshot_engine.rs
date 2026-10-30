// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for Cassandra: refused.
//!
//! The trait promises a view of one table as of one instant: writes committed
//! after the open are not visible, writes committed before it all are, and the
//! definition describes the same instant as the items. Cassandra has no
//! mechanism that gives that across a token-range scan. Reads see whatever
//! replicas hold at the consistency level of each query; a scan that pages
//! through `token(pk)` ranges observes each range at a different time, and
//! `LOCAL_SERIAL` reads serialize single partitions, not a table. The
//! in-catalog backup this backend ships (`backup_engine.rs`, ADR-0019) is a
//! logical copy that documents exactly this: "concurrent writes can race backup
//! creation ... not advertised as strict point-in-time recovery."
//!
//! Returning that copy under the `SnapshotEngine` contract would make the
//! backup manifest's `snapshot` member a lie, so this backend returns
//! [`StorageError::Unsupported`]. The in-catalog `BackupEngine` path is
//! unaffected. A token-range implementation
//! with a documented non-point-in-time `SnapshotInfo.kind` is a design change
//! the backup design (`docs/design/15-backup-restore.md`, section 3) leaves
//! open; it must not arrive as a silent widening of this method.

use extenddb_storage::error::StorageError;
use extenddb_storage::{SnapshotEngine, TableSnapshot};
use futures::future::BoxFuture;

use crate::CassandraEngine;

/// The reason every `open_table_snapshot` call returns.
pub const SNAPSHOT_UNSUPPORTED_REASON: &str = "table snapshots are not supported on the Cassandra \
     backend: Cassandra has no point-in-time read across a table, so a backup taken through a \
     snapshot could not promise the instant its manifest records";

impl SnapshotEngine for CassandraEngine {
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>> {
        // No query is issued: the answer does not depend on the table.
        let _ = (account_id, table_name);
        let result = Err(StorageError::Unsupported(
            SNAPSHOT_UNSUPPORTED_REASON.to_owned(),
        ));
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reason is one sentence an operator can act on: it names the
    /// backend and the cause (no point-in-time read across a table).
    #[test]
    fn reason_names_the_backend_and_the_cause() {
        assert!(SNAPSHOT_UNSUPPORTED_REASON.contains("Cassandra"));
        assert!(SNAPSHOT_UNSUPPORTED_REASON.contains("point-in-time"));
    }
}
