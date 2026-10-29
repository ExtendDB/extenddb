// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `CREATING` GSI reports `Backfilling` and refuses reads by phase, as
//! measured on 2026-10-26 (see `extenddb_storage::gsi_backfill`).
//!
//! These direct tests run no recovery worker, so the first test puts the
//! catalog row in each state by hand, the way the worker leaves it: `CREATING`
//! with no `backfill_heartbeat` as `UpdateTable` inserts it, `CREATING` with
//! one once the worker has taken the index up, then `ACTIVE` with the heartbeat
//! left in place. The second drives single scans of the worker on its table.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use cdrs_tokio::query::QueryValues;
    use cdrs_tokio::types::IntoRustByName as _;
    use extenddb_core::types::{AttributeValue, DescribeTableInput};
    use extenddb_storage::error::StorageError;
    use extenddb_storage::{DataEngine, TableEngine};
    use extenddb_storage_cassandra::CassandraEngine;
    use extenddb_storage_cassandra::cassandra_util::{execute, query_optional};
    use extenddb_storage_cassandra::workers::recover_stale_gsi_backfills_of_table;
    use uuid::Uuid;

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

    /// One `i64` column, by its alias, from the first row of a query.
    async fn scalar(engine: &CassandraEngine, cql: &str, values: QueryValues) -> Option<i64> {
        query_optional::<StorageError>(&engine.session_arc(), cql, values, "gsi_claim_test")
            .await
            .expect("query")
            .and_then(|row| row.get_by_name("v").ok().flatten())
    }

    /// The id and the write time of the schema row of `gidx`'s data table.
    async fn data_table(engine: &CassandraEngine, account_ks: &str, table: &str) -> (Uuid, i64) {
        let row = query_optional::<StorageError>(
            &engine.session_arc(),
            "SELECT id, WRITETIME(comment) AS v FROM system_schema.tables \
             WHERE keyspace_name = ? AND table_name = ?",
            cdrs_tokio::query_values!(account_ks, table),
            "gsi_claim_test",
        )
        .await
        .expect("read the schema row")
        .expect("the data table exists");
        (
            row.get_by_name("id").ok().flatten().expect("table id"),
            row.get_by_name("v").ok().flatten().expect("write time"),
        )
    }

    /// The recovery worker's claim. `UpdateTable` leaves a new index with no
    /// heartbeat; the worker writes one before it rebuilds the data table and
    /// starts the backfill, which is the start of the second phase, and a
    /// later scan that finds the heartbeat fresh leaves the index to the build
    /// already running. The table's phase hold keeps that build waiting while
    /// the second scan runs.
    #[tokio::test]
    async fn the_recovery_claim_starts_the_second_phase_and_a_later_scan_leaves_the_build() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = Arc::new(crate::helpers::setup_engine().await);
        let table = crate::helpers::TestTable::with_gsi(&engine, "GsiClaim", "gidx", "g").await;
        let ki = &table.key_info;
        let catalog_ks = engine.catalog_keyspace();
        let account_ks = engine.account_keyspace(&ki.account_id);
        for n in 0..3 {
            let item = std::collections::BTreeMap::from([
                ("id".to_owned(), AttributeValue::S(format!("p{n}"))),
                ("g".to_owned(), AttributeValue::S("v".to_owned())),
            ]);
            engine
                .put_item(ki, item, false, None, &Default::default(), None)
                .await
                .expect("put");
        }
        let hold_key = extenddb_core::settings_keys::gsi_creating_phase_delay_key(&ki.table_name);
        execute::<StorageError>(
            &engine.session_arc(),
            &format!("INSERT INTO {catalog_ks}.settings (key, value) VALUES (?, ?)"),
            cdrs_tokio::query_values!(hold_key.as_str(), "3000"),
            "gsi_claim_test",
        )
        .await
        .expect("set the phase hold");
        let index_id: String = query_optional::<StorageError>(
            &engine.session_arc(),
            &format!(
                "SELECT index_id FROM {catalog_ks}.indexes WHERE table_id = ? AND index_name = 'gidx'"
            ),
            cdrs_tokio::query_values!(ki.table_id.as_str()),
            "gsi_claim_test",
        )
        .await
        .expect("read the index row")
        .and_then(|row| row.get_by_name("index_id").ok().flatten())
        .expect("index id");
        let data = extenddb_storage_cassandra::data::ddl::index_table_name(&index_id);

        // As UpdateTable leaves an index it adds.
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

        let stale_after = Duration::from_secs(120);
        recover_stale_gsi_backfills_of_table(&engine, &account_ks, &ki.table_id, stale_after)
            .await
            .expect("the first scan");
        assert_eq!(
            described(&engine, &ki.account_id, &ki.table_name).await,
            ("CREATING".to_owned(), Some(serde_json::Value::Bool(true))),
            "the claim records the start of the backfill"
        );
        let read = engine.index_info_by_table_id(&ki.table_id, "gidx").await;
        assert!(
            matches!(read, Err(StorageError::Validation(ref m)) if m == BACKFILLING),
            "after the claim a read gets the backfilling sentence: {read:?}"
        );
        let heartbeat_at = scalar(
            &engine,
            &format!(
                "SELECT WRITETIME(backfill_heartbeat) AS v FROM {catalog_ks}.indexes \
                 WHERE table_id = ? AND index_name = 'gidx'"
            ),
            cdrs_tokio::query_values!(ki.table_id.as_str()),
        )
        .await
        .expect("the heartbeat's write time");
        let (rebuilt, rebuilt_at) = data_table(&engine, &account_ks, &data).await;
        assert!(
            heartbeat_at < rebuilt_at,
            "the heartbeat ({heartbeat_at}) is written before the data table is rebuilt ({rebuilt_at})"
        );

        // The backfill the first scan started is waiting out the hold.
        recover_stale_gsi_backfills_of_table(&engine, &account_ks, &ki.table_id, stale_after)
            .await
            .expect("the second scan");
        assert_eq!(
            data_table(&engine, &account_ks, &data).await.0,
            rebuilt,
            "the second scan left the data table to the running build"
        );
        assert_eq!(
            described(&engine, &ki.account_id, &ki.table_name).await,
            ("CREATING".to_owned(), Some(serde_json::Value::Bool(true)))
        );

        let deadline = Instant::now() + Duration::from_secs(60);
        while described(&engine, &ki.account_id, &ki.table_name).await
            != ("ACTIVE".to_owned(), None)
        {
            assert!(
                Instant::now() < deadline,
                "the running build never published the index"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        engine
            .index_info_by_table_id(&ki.table_id, "gidx")
            .await
            .expect("an ACTIVE index resolves");
    }
}
