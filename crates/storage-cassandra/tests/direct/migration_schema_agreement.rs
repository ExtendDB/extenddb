// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Regression test for the migration schema-agreement race (issue #403).
//!
//! The Cassandra backend opens a multi-connection pool (`max_connections`).
//! The driver round-robins queries across those connections, so a read issued
//! right after a DDL `CREATE TABLE` can land on a connection whose coordinator
//! view has not yet applied the change and fail with "table ... does not
//! exist". `run_data_migrations` creates `schema_history` and then immediately
//! queries it, so it hit this race once the pool held more than one connection.
//!
//! `run_data_migrations` now waits for cluster schema agreement after each DDL
//! statement. This test concentrates the race: it creates many fresh account
//! keyspaces concurrently, runs data migrations on each, and then reads
//! `schema_history` back from each. Before the fix this surfaced the race
//! intermittently; with the fix every keyspace migrates and reads back cleanly.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, unique_test_account};
    use extenddb_storage_cassandra::migrations::run_data_migrations;

    #[tokio::test]
    async fn migrations_survive_concurrent_keyspace_creation() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }

        // Enough concurrent keyspaces to exercise pool round-robin; each one is
        // an independent CREATE KEYSPACE + CREATE TABLE schema_history + read.
        const KEYSPACES: usize = 12;

        let engine = setup_engine().await;
        let session = engine.session_arc();

        let mut keyspaces = Vec::with_capacity(KEYSPACES);
        let mut handles = Vec::with_capacity(KEYSPACES);

        for _ in 0..KEYSPACES {
            let keyspace = format!("extenddb_sa_test_account_{}", unique_test_account());
            keyspaces.push(keyspace.clone());

            let engine = engine.clone();
            let session = session.clone();
            handles.push(tokio::spawn(async move {
                engine
                    .create_keyspace(&keyspace)
                    .await
                    .unwrap_or_else(|e| panic!("create_keyspace {keyspace}: {e:?}"));

                // The call under test. Internally: USE, then for the first
                // migration CREATE TABLE schema_history, then SELECT/INSERT
                // against it. Without the schema-agreement wait, the read can
                // race the create across pool connections.
                run_data_migrations(&session, &keyspace)
                    .await
                    .unwrap_or_else(|e| panic!("run_data_migrations {keyspace}: {e:?}"));

                // Read schema_history straight back, fully qualified, which is
                // routed through the pool and would fail if the table were not
                // yet visible on the chosen coordinator.
                let check = format!("SELECT version FROM {keyspace}.schema_history");
                session
                    .query(check)
                    .await
                    .unwrap_or_else(|e| panic!("read schema_history {keyspace}: {e:?}"));
            }));
        }

        for handle in handles {
            handle.await.expect("migration task panicked");
        }

        // Clean up the keyspaces this test created.
        for keyspace in keyspaces {
            let _ = session
                .query(format!("DROP KEYSPACE IF EXISTS {keyspace}"))
                .await;
        }
    }
}
