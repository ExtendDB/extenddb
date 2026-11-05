// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` on Cassandra refuses every table with `Unsupported`:
//! an existing ACTIVE table and a missing one alike, so an implementation
//! that someday consulted the catalog before refusing would still be held to
//! the refusal. That no query is issued is a property of the code (the impl
//! returns before touching the session), not asserted here.

#[cfg(test)]
mod tests {
    use extenddb_storage::SnapshotEngine;
    use extenddb_storage::error::StorageError;

    #[tokio::test]
    async fn open_table_snapshot_is_unsupported() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = crate::helpers::setup_engine().await;
        let table = crate::helpers::TestTable::new(&engine, "snap_refused", true).await;
        let account = table.key_info.account_id.clone();

        for name in [
            table.key_info.table_name.as_str(),
            "does_not_exist_for_snapshot",
        ] {
            let err = engine
                .open_table_snapshot(&account, name)
                .await
                .expect_err("refused");
            assert!(matches!(err, StorageError::Unsupported(_)), "{name}: {err}");
            assert!(err.to_string().contains("Cassandra"), "{err}");
        }
    }
}
