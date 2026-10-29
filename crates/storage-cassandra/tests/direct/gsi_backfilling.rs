// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `CREATING` GSI reports `Backfilling` and refuses reads by phase, as
//! measured on 2026-10-26 (see `extenddb_storage::gsi_backfill`).
//!
//! These direct tests run no recovery worker, so the catalog row is put in each
//! state by hand, the way the worker leaves it: `CREATING` with no
//! `backfill_heartbeat` as `UpdateTable` inserts it, `CREATING` with one once
//! the worker has taken the index up, then `ACTIVE` with the heartbeat left in
//! place.

#[cfg(test)]
mod tests {
    use extenddb_core::types::DescribeTableInput;
    use extenddb_storage::TableEngine;
    use extenddb_storage::error::StorageError;
    use extenddb_storage_cassandra::CassandraEngine;
    use extenddb_storage_cassandra::cassandra_util::execute;

    const BACKFILLING: &str = "Cannot read from backfilling global secondary index: gidx";

    async fn set_row(engine: &CassandraEngine, table_id: &str, assignments: &str) {
        let catalog_ks = engine.catalog_keyspace();
        execute::<StorageError>(
            &engine.session_arc(),
            &format!(
                "UPDATE {catalog_ks}.indexes SET {assignments} \
                 WHERE table_id = ? AND index_name = 'gidx'"
            ),
            cdrs_tokio::query_values!(table_id),
            "gsi_backfilling_test",
        )
        .await
        .expect("set the index row");
    }

    /// The serialised `IndexStatus` and `Backfilling` members of `gidx`.
    async fn described(
        engine: &CassandraEngine,
        account_id: &str,
        table_name: &str,
    ) -> (String, Option<serde_json::Value>) {
        let desc = engine
            .describe_table(
                account_id,
                DescribeTableInput {
                    table_name: table_name.to_owned(),
                },
            )
            .await
            .expect("describe the table");
        let gsi = desc
            .global_secondary_indexes
            .expect("GSIs")
            .into_iter()
            .find(|g| g.index_name == "gidx")
            .expect("gidx");
        let wire = serde_json::to_value(&gsi).expect("serialise the GSI description");
        (gsi.index_status, wire.get("Backfilling").cloned())
    }

    #[tokio::test]
    async fn a_creating_gsi_reports_each_phase_and_refuses_reads_by_phase() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = crate::helpers::setup_engine().await;
        let table = crate::helpers::TestTable::with_gsi(&engine, "GsiPhases", "gidx", "g").await;
        let ki = &table.key_info;

        set_row(
            &engine,
            &ki.table_id,
            "index_status = 'CREATING', backfill_heartbeat = null",
        )
        .await;
        assert_eq!(
            described(&engine, &ki.account_id, &ki.table_name).await,
            ("CREATING".to_owned(), Some(serde_json::Value::Bool(false)))
        );
        let read = engine.index_info_by_table_id(&ki.table_id, "gidx").await;
        assert!(
            matches!(read, Err(StorageError::IndexNotFound(_))),
            "before the backfill a read is told there is no such index: {read:?}"
        );

        set_row(
            &engine,
            &ki.table_id,
            "backfill_heartbeat = toTimestamp(now())",
        )
        .await;
        assert_eq!(
            described(&engine, &ki.account_id, &ki.table_name).await,
            ("CREATING".to_owned(), Some(serde_json::Value::Bool(true)))
        );
        let read = engine.index_info_by_table_id(&ki.table_id, "gidx").await;
        assert!(
            matches!(read, Err(StorageError::Validation(ref m)) if m == BACKFILLING),
            "during the backfill a read gets the backfilling sentence: {read:?}"
        );

        set_row(&engine, &ki.table_id, "index_status = 'ACTIVE'").await;
        assert_eq!(
            described(&engine, &ki.account_id, &ki.table_name).await,
            ("ACTIVE".to_owned(), None)
        );
        engine
            .index_info_by_table_id(&ki.table_id, "gidx")
            .await
            .expect("an ACTIVE index resolves");
    }
}
