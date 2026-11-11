// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration test for MetricsStore on a catalog keyspace that has
//! run every migration: `extenddb_storage::metrics_conformance` (#728).

#[cfg(test)]
mod tests {
    use crate::helpers::{create_catalog_store, setup_engine, test_config};

    /// Two accounts' tables of the same name keep their own rows in
    /// `account_metrics`, and the account filter returns only that
    /// account's. The check's accounts are made up for the run, so the
    /// shared test catalog's other rows stay out of its reads.
    #[tokio::test]
    async fn the_metrics_store_keeps_two_accounts_apart() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let store = create_catalog_store(&engine, &test_config());
        extenddb_storage::metrics_conformance::run(
            &store,
            // Cassandra writes a row of the same key over the previous one,
            // and its TTL, not prune_metrics, expires the rows.
            extenddb_storage::metrics_conformance::Expectations {
                accumulates: false,
                prunes: false,
            },
        )
        .await;
    }
}
