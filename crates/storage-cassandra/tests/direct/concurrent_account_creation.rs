// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Regression test for issue #408: concurrent account creation must not place
//! one account's data tables in another account's keyspace.
//!
//! `ensure_account_keyspace` runs the data migrations with a `USE <keyspace>`
//! on the process-wide cdrs-tokio `Session` followed by unqualified `CREATE
//! TABLE` DDL. Two concurrent creations can interleave their `USE`, so one
//! account's tables land in the other's keyspace while its own `schema_history`
//! still records the migration as applied — leaving the first keyspace missing
//! tables for good. This test creates two accounts concurrently and asserts
//! every expected data table exists in each account's keyspace.
//!
//! It is a race, so a single pass proves little: the test runs ROUNDS pairs and
//! fails if any keyspace is ever missing a table. On `main` it fails
//! (empirically within a few rounds); with the migration run serialized it
//! passes every round.

#[cfg(test)]
mod tests {
    use crate::helpers::{create_catalog_store, setup_engine, test_config, unique_test_account};
    use extenddb_storage::management_store::ManagementStore;
    use std::sync::Arc;

    /// Every data table a fresh account keyspace must contain, from
    /// migrations/account/0001_baseline.cql, migration 1's account body.
    const EXPECTED_DATA_TABLES: &[&str] = &[
        "schema_history",
        "stream_shards",
        "stream_records",
        "transaction_ledger",
        "index_propagation_holds",
        "gsi_pending",
        "ttl_expiration_buckets",
        "ttl_expirations",
        "ttl_reconcile_pending",
        "ttl_repair_inflight_partitions",
        "ttl_repair_inflight",
        "backup_items",
    ];

    /// Pairs of concurrent creations to run. A race that interleaves the `USE`
    /// does not fire every time; a handful of rounds makes it reliable without
    /// making the test slow.
    const ROUNDS: usize = 8;

    async fn table_exists(
        session: &Arc<extenddb_storage_cassandra::CassandraSession>,
        keyspace: &str,
        table: &str,
    ) -> bool {
        let q = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?";
        session
            .query_with_values(q, cdrs_tokio::query_values!(keyspace, table))
            .await
            .ok()
            .and_then(|f| f.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty())
    }

    #[tokio::test]
    async fn concurrent_account_creation_populates_every_keyspace() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let session = engine.session_arc();

        for round in 0..ROUNDS {
            let store_a = create_catalog_store(&engine, &config);
            let store_b = create_catalog_store(&engine, &config);
            let acct_a = unique_test_account();
            let acct_b = unique_test_account();
            let name_a = format!("A_{acct_a}");
            let name_b = format!("B_{acct_b}");

            // Create both accounts concurrently on the shared Session — the
            // interleave that moves one account's DDL into the other keyspace.
            let (ra, rb) = tokio::join!(
                store_a.create_account(&acct_a, &name_a),
                store_b.create_account(&acct_b, &name_b),
            );
            ra.expect("create_account A failed");
            rb.expect("create_account B failed");

            for (idx, (acct, label)) in [(&acct_a, "A"), (&acct_b, "B")].iter().enumerate() {
                let keyspace = engine.account_keyspace(acct);
                // The other account's keyspace, so a missing table can be
                // reported as "contaminated into the other keyspace" only when
                // that is actually what happened.
                let other_acct = if idx == 0 { &acct_b } else { &acct_a };
                let other_keyspace = engine.account_keyspace(other_acct);
                for table in EXPECTED_DATA_TABLES {
                    if table_exists(&session, &keyspace, table).await {
                        continue;
                    }
                    // Name the table and the account label only: keyspace names
                    // embed the account id, which must not reach the log.
                    let cause = if table_exists(&session, &other_keyspace, table).await {
                        "it exists in the other account's keyspace, so a concurrent \
                         migration's USE redirected the DDL"
                    } else {
                        "it is in neither keyspace, so the migration did not create it \
                         or a concurrent USE sent it to a third keyspace"
                    };
                    panic!(
                        "round {round}: account {label} is missing table `{table}`; {cause} \
                         (issue #408). To debug, list where the table was created."
                    );
                }
            }
        }
    }
}
