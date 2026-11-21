// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `destroy` waits for the nodes to agree on the schema (#645).
//!
//! `DROP KEYSPACE` returns once the node that took it has applied it, and the
//! other nodes follow later, so a read of `system_schema` on another node can
//! still list a keyspace that `destroy` dropped. `drop_databases` therefore
//! waits until every live node reports one schema version, on every run, also
//! when it found nothing to drop: a second run after a wait that timed out
//! finds nothing to drop and the nodes may still disagree.
//!
//! The effect of the wait is a race between two nodes, which the live tests in
//! `tests/migrations.rs` see on the `cassandra-two-nodes` job (the test that
//! reads each node, `destroy_is_applied_on_every_node_when_it_returns`, fails
//! when it loses that race). This test counts the waits that `drop_databases`
//! runs to the end instead, so it fails on every run, with one node or many, if
//! `drop_databases` stops running its wait. It does not show that the wait
//! brings the nodes to one schema: the two-node tests show that.

use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage_cassandra::CassandraBootstrapper;
use extenddb_storage_cassandra::test_hooks::destroy_waits;

use crate::helpers::{DropKeyspacesOnDrop, setup_engine, test_config, test_contact_point};

/// `init`'s bootstrapper for the deployment of `prefix`, connected as the
/// test node's superuser.
async fn bootstrapper(prefix: &str) -> CassandraBootstrapper {
    let mut config = test_config();
    config.keyspace_prefix = prefix.to_owned();
    config.rebuild_cached_connection_string();
    let (host, port) = test_contact_point()
        .split_once(':')
        .map(|(h, p)| (h.to_owned(), p.parse().expect("a port")))
        .expect("host:port");
    let user = config.username.clone().unwrap_or_default();
    let password = config.password.clone().unwrap_or_default();
    let bootstrap = BootstrapConfig {
        host,
        port,
        admin_user: user.clone(),
        admin_password: Some(password.clone()),
        app_user: user,
        app_password: password,
        catalog_db: format!("{prefix}_catalog"),
        data_db: String::new(),
    };
    CassandraBootstrapper::new(&config, bootstrap)
        .await
        .unwrap_or_else(|e| panic!("bootstrapper: {e:?}"))
}

#[tokio::test]
async fn test_destroy_waits_for_the_nodes_to_agree_on_every_run() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let prefix = format!("p2m_dw_{}", &crate::helpers::unique_test_id()[..8]);
    let catalog = format!("{prefix}_catalog");
    let _cleanup = DropKeyspacesOnDrop(vec![catalog.clone()]);
    engine
        .session()
        .query(format!(
            "CREATE KEYSPACE {catalog} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ))
        .await
        .expect("create the catalog keyspace");

    let bootstrapper = bootstrapper(&prefix).await;
    assert_eq!(destroy_waits(&prefix), 0);

    // A run that drops a keyspace waits once.
    bootstrapper.drop_databases("").await.expect("destroy");
    assert_eq!(destroy_waits(&prefix), 1, "the run that dropped a keyspace");

    // A second run finds nothing to drop. It still waits, because the first
    // run's wait may have timed out with the nodes still apart.
    bootstrapper
        .drop_databases("")
        .await
        .expect("destroy again");
    assert_eq!(
        destroy_waits(&prefix),
        2,
        "the run that found nothing to drop"
    );
}
