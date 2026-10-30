// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migration runners create their tables, indexes and added columns in
//! the keyspace they are given, whatever keyspace the session has selected
//! (#632).
//!
//! cdrs-tokio runs a `USE` on one connection and copies it to the session's
//! other connections, on every node, from a background task. On a session
//! with more than one connection, on one node or across nodes, a statement
//! that relies on the session keyspace can run in the keyspace selected
//! before it. These tests build their own session with `POOL_SIZE`
//! connections per node, so they do not depend on the engine's pool size.

use std::collections::BTreeSet;
use std::sync::Arc;

use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfigBuilder;
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use extenddb_storage::error::StorageError;
use extenddb_storage_cassandra::CassandraSession;
use extenddb_storage_cassandra::cassandra_util::{get_column, query_rows};
use extenddb_storage_cassandra::migrations::{run_catalog_migrations, run_data_migrations};

/// Connections per node, the `max_connections` default.
const POOL_SIZE: usize = 10;

/// Catalog keyspaces, and account keyspaces, the alternating `USE` test migrates.
const ROUNDS: usize = 5;

/// What one runner leaves in the keyspace it migrates.
struct Schema {
    tables: &'static [&'static str],
    indexes: &'static [&'static str],
    /// `(table, column)` pairs that `ALTER TABLE` statements add.
    added_columns: &'static [(&'static str, &'static str)],
}

/// Catalog migrations V001 to V005.
const CATALOG: Schema = Schema {
    tables: &[
        "access_keys",
        "accounts",
        "admin_users",
        "backups_by_account",
        "backups_by_arn",
        "backups_by_table",
        "continuous_backups",
        "iam_group_members",
        "iam_groups",
        "iam_permissions_boundaries",
        "iam_policies",
        "iam_role_tags",
        "iam_roles",
        "iam_sessions",
        "iam_user_tags",
        "iam_users",
        "idempotency_tokens_by_account",
        "indexes",
        "login_attempts",
        "metrics",
        "schema_history",
        "settings",
        "tables",
        "tags",
    ],
    indexes: &["tables_table_id_idx"],
    added_columns: &[
        ("backups_by_arn", "active_restores"),
        ("backups_by_arn", "definition"),
        ("tables", "restore_date_time"),
        ("tables", "restore_heartbeat"),
        ("tables", "restore_owner"),
        ("tables", "restore_source_backup_arn"),
        ("tables", "restore_source_table_arn"),
        ("tables", "ttl_backfill_cursor"),
    ],
};

/// Data migrations V001 and V002.
const DATA: Schema = Schema {
    tables: &[
        "backup_items",
        "gsi_pending",
        "index_propagation_holds",
        "schema_history",
        "stream_records",
        "stream_shards",
        "transaction_ledger",
        "ttl_expiration_buckets",
        "ttl_expirations",
        "ttl_reconcile_pending",
        "ttl_repair_inflight",
        "ttl_repair_inflight_partitions",
    ],
    indexes: &["idx_txn_started_at", "stream_shards_table_id_idx"],
    added_columns: &[],
};

/// A session built as the engine builds it, with `POOL_SIZE` connections per node.
async fn pooled_session() -> Arc<CassandraSession> {
    let config = crate::helpers::test_config();
    let mut node = NodeTcpConfigBuilder::new();
    for contact_point in &config.contact_points {
        node = node.with_contact_point(contact_point.clone().into());
    }
    let (Some(username), Some(password)) = (&config.username, &config.password) else {
        panic!("test_config sets no credentials");
    };
    let cluster = node
        .with_authenticator_provider(Arc::new(StaticPasswordAuthenticatorProvider::new(
            username, password,
        )))
        .build()
        .await
        .expect("cluster config");
    let pool = ConnectionPoolConfigBuilder::new()
        .with_local_size(POOL_SIZE)
        .with_remote_size(POOL_SIZE)
        .build();
    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster)
        .with_connection_pool_config(pool)
        .build()
        .await
        .expect("pooled session");
    Arc::new(session)
}

async fn create_keyspaces(session: &Arc<CassandraSession>, keyspaces: &[String]) {
    let datacenter = crate::helpers::test_config().datacenter;
    for keyspace in keyspaces {
        session
            .query(format!(
                "CREATE KEYSPACE {keyspace} WITH replication = \
                 {{'class': 'NetworkTopologyStrategy', '{datacenter}': 1}}"
            ))
            .await
            .expect("create keyspace");
    }
}

/// Drops `keyspaces` now. Each migrated keyspace holds 12 to 24 tables, so
/// leaving them for the harness's queued drops would grow the node's schema
/// by hundreds of tables per run.
async fn drop_keyspaces(session: &Arc<CassandraSession>, keyspaces: &[String]) {
    for keyspace in keyspaces {
        session
            .query(format!("DROP KEYSPACE IF EXISTS {keyspace}"))
            .await
            .expect("drop keyspace");
    }
}

/// The `system_schema` rows `query` returns for `keyspace`, as the values of
/// `columns` joined with a dot.
async fn schema_names(
    session: &Arc<CassandraSession>,
    query: &str,
    keyspace: &str,
    columns: &[&str],
) -> BTreeSet<String> {
    let rows = query_rows::<StorageError>(
        session,
        query,
        cdrs_tokio::query_values!(keyspace),
        "schema read",
    )
    .await
    .expect(query);
    rows.iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| get_column::<String, StorageError>(row, column, "schema read"))
                .collect::<Result<Vec<_>, _>>()
                .expect("schema column")
                .join(".")
        })
        .collect()
}

async fn tables_in(session: &Arc<CassandraSession>, keyspace: &str) -> BTreeSet<String> {
    let query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    schema_names(session, query, keyspace, &["table_name"]).await
}

/// One line for each table, index or added column of `schema` that `keyspace`
/// lacks, and for each table of `other` (the other runner's schema) it holds.
async fn problems_in(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    schema: &Schema,
    other: &Schema,
) -> Vec<String> {
    let tables = tables_in(session, keyspace).await;
    let index_query = "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = ?";
    let indexes = schema_names(session, index_query, keyspace, &["index_name"]).await;
    let column_query =
        "SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name = ?";
    let columns = schema_names(
        session,
        column_query,
        keyspace,
        &["table_name", "column_name"],
    )
    .await;

    let mut problems = Vec::new();
    for table in schema.tables.iter().filter(|t| !tables.contains(**t)) {
        problems.push(format!("{keyspace}: no table {table}"));
    }
    for index in schema.indexes.iter().filter(|i| !indexes.contains(**i)) {
        problems.push(format!("{keyspace}: no index {index}"));
    }
    for (table, column) in schema.added_columns {
        if !columns.contains(&format!("{table}.{column}")) {
            problems.push(format!("{keyspace}: no column {table}.{column}"));
        }
    }
    for table in other.tables.iter().filter(|t| !schema.tables.contains(t)) {
        if tables.contains(*table) {
            problems.push(format!(
                "{keyspace}: holds {table}, which the other runner creates"
            ));
        }
    }
    problems
}

/// Before every migration run the session selects another keyspace with
/// `USE`, as an earlier runner on the same session did before #632:
/// `extenddb init` and `extenddb migrate` run the catalog migrations and then
/// each account's data migrations on the engine's one session, and each
/// runner began with `USE` for its own keyspace. Each keyspace must end up
/// with its own schema and the other keyspace with none. Against runners
/// that rely on `USE`, some runs execute their statements in the other
/// keyspace, so the outcome varies from run to run, and `ROUNDS` runs of
/// each runner give it many chances to show.
#[tokio::test]
async fn migrations_ignore_the_keyspace_another_caller_selected() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let session = pooled_session().await;
    let prefix = format!("m632_{}", &crate::helpers::unique_test_id()[..8]);
    let decoy = format!("{prefix}_decoy");
    let rounds: Vec<[String; 2]> = (0..ROUNDS)
        .map(|i| {
            [
                format!("{prefix}_catalog_{i}"),
                format!("{prefix}_account_{i}"),
            ]
        })
        .collect();
    let _drop = crate::helpers::DropKeyspacesOnDrop(
        std::iter::once(decoy.clone())
            .chain(rounds.iter().flatten().cloned())
            .collect(),
    );
    create_keyspaces(&session, std::slice::from_ref(&decoy)).await;

    let use_decoy = format!("USE {decoy}");
    let mut problems = Vec::new();
    for round in &rounds {
        let [catalog, account] = round;
        create_keyspaces(&session, round).await;
        session.query(&use_decoy).await.expect("USE decoy");
        if let Err(e) = run_catalog_migrations(&session, catalog).await {
            problems.push(format!("{catalog}: run_catalog_migrations: {e:?}"));
        }
        session.query(&use_decoy).await.expect("USE decoy");
        if let Err(e) = run_data_migrations(&session, account).await {
            problems.push(format!("{account}: run_data_migrations: {e:?}"));
        }
        problems.extend(problems_in(&session, catalog, &CATALOG, &DATA).await);
        problems.extend(problems_in(&session, account, &DATA, &CATALOG).await);
        drop_keyspaces(&session, round).await;
    }
    for table in tables_in(&session, &decoy).await {
        problems.push(format!("{decoy}: holds {table}"));
    }
    drop_keyspaces(&session, std::slice::from_ref(&decoy)).await;

    let bad: BTreeSet<&str> = problems
        .iter()
        .filter_map(|p| p.split(':').next())
        .collect();
    assert!(
        problems.is_empty(),
        "{} of {} keyspaces wrong after {ROUNDS} catalog and {ROUNDS} account migrations \
         on a {POOL_SIZE}-connection session:\n{}",
        bad.len(),
        1 + 2 * ROUNDS,
        problems.join("\n")
    );
}

/// On a session that never selected a keyspace, any migration statement that
/// leaves its keyspace to the session fails ("No keyspace has been
/// specified"). The runners must also leave the session's keyspace unset, so
/// they move no other caller's statements.
#[tokio::test]
async fn migrations_run_on_a_session_with_no_keyspace_and_leave_none_selected() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let session = pooled_session().await;
    assert_eq!(
        session.current_keyspace(),
        None,
        "a new session has no keyspace"
    );
    let prefix = format!("m632_{}", &crate::helpers::unique_test_id()[..8]);
    let catalog = format!("{prefix}_catalog");
    let account = format!("{prefix}_account");
    let keyspaces = vec![catalog.clone(), account.clone()];
    let _drop = crate::helpers::DropKeyspacesOnDrop(keyspaces.clone());
    create_keyspaces(&session, &keyspaces).await;

    let catalog_result = run_catalog_migrations(&session, &catalog).await;
    let data_result = run_data_migrations(&session, &account).await;
    let mut problems = problems_in(&session, &catalog, &CATALOG, &DATA).await;
    problems.extend(problems_in(&session, &account, &DATA, &CATALOG).await);
    let selected = session.current_keyspace();
    drop_keyspaces(&session, &keyspaces).await;

    catalog_result.expect("catalog migrations on a session with no keyspace");
    data_result.expect("data migrations on a session with no keyspace");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert_eq!(
        selected, None,
        "the migration runners selected a keyspace on the session"
    );
}
