// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migration lock's lease, the serving registration and the bound on
//! a code body's requests under the lock.

use std::time::{Duration, Instant};

use extenddb_storage::migrations::{CatalogMigrations, CatalogStatus, MigrationError};
use extenddb_storage_cassandra::migrations::{
    CassandraCatalogMigrations, LeaseTimes, MigrationLock, ServingRegistration, register_server,
    schema_statement_bounds,
};

use super::{deployment, target_of};
use crate::helpers;
use crate::migrations_support::{self, TIMEOUT, cql, keyspace, new_prefix, session};

#[tokio::test]
async fn the_lock_holds_for_three_ttls_and_a_killed_holder_frees_it_within_one_ttl() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("lease");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    let times = LeaseTimes {
        ttl: Duration::from_secs(6),
        renew_every: Duration::from_millis(1500),
        margin: Duration::from_millis(2500),
        stall: Duration::from_secs(5),
    };
    let holder = MigrationLock::acquire(session.clone(), &catalog, times, TIMEOUT)
        .await
        .expect("take the lock");
    let other = self::session().await;
    let started = Instant::now();
    while started.elapsed() < times.ttl * 3 {
        holder.progress();
        let refused = MigrationLock::acquire(other.clone(), &catalog, times, Duration::ZERO).await;
        assert!(
            matches!(refused, Err(MigrationError::Locked(_))),
            "a second holder at {:?}: {refused:?}",
            started.elapsed()
        );
        holder.check().expect("the holder keeps its lease");
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    holder.abandon();
    let killed = Instant::now();
    let next = MigrationLock::acquire(other.clone(), &catalog, times, times.ttl * 3)
        .await
        .expect("the lock is free once the row expires");
    let waited = killed.elapsed();
    assert!(
        waited <= times.ttl + times.renew_every + Duration::from_secs(1),
        "waited {waited:?}"
    );
    next.release().await;
}

#[tokio::test]
async fn a_holder_that_makes_no_progress_loses_the_lock_and_writes_nothing() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("stall");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    let times = LeaseTimes {
        ttl: Duration::from_secs(5),
        renew_every: Duration::from_secs(1),
        margin: Duration::from_secs(2),
        stall: Duration::from_secs(2),
    };
    let holder = MigrationLock::acquire(session.clone(), &catalog, times, TIMEOUT)
        .await
        .expect("take the lock");
    // A body that hangs: the renewal stops after the stall, and the guard
    // ends the body once the deadline passes.
    let hung = holder
        .guard(async {
            futures::future::pending::<()>().await;
            Ok(())
        })
        .await;
    assert!(hung.is_err(), "{hung:?}");
    assert!(holder.check().is_err());
    // Another process takes the lock once the row expires.
    let next = MigrationLock::acquire(session.clone(), &catalog, times, times.ttl * 2)
        .await
        .expect("the lock is free");
    next.release().await;
}

/// A schema statement sent under the lock, or by `init`'s default-account
/// run under the lock's lease, is bounded by 20 s, inside the lease's 25 s
/// margin; CreateAccount's run, which holds neither, gets 60 s.
#[tokio::test]
async fn a_locked_or_leased_run_bounds_a_schema_statement_inside_the_margin() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("bound");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    let lock = MigrationLock::acquire(session.clone(), &catalog, LeaseTimes::DEFAULT, TIMEOUT)
        .await
        .expect("take the lock");
    let [locked, leased, unlocked] = schema_statement_bounds(&lock, &prefix);
    lock.release().await;
    assert_eq!(locked, Duration::from_secs(20));
    assert_eq!(leased, locked);
    assert!(locked < LeaseTimes::DEFAULT.margin, "{locked:?}");
    assert_eq!(unlocked, Duration::from_secs(60));
}

#[tokio::test]
async fn a_server_and_a_migration_refuse_each_other() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("serve");
    deployment(&session, &prefix).await;
    let catalog = format!("{prefix}_catalog");
    let registration = register_server(session.clone(), &prefix, "127.0.0.1:1", "test")
        .await
        .expect("register");
    // migrate --yes takes the lock, then waits for the server and exits 6
    // naming it.
    let migrations = CassandraCatalogMigrations;
    let refused = migrations
        .lock(&target_of(&prefix), Duration::from_secs(2))
        .await;
    match refused {
        Err(MigrationError::Locked(message)) => {
            assert!(
                message.contains("extenddb serve test on 127.0.0.1:1"),
                "{message}"
            );
        }
        other => panic!("{other:?}"),
    }
    registration.release().await;
    let lock = migrations
        .lock(&target_of(&prefix), Duration::from_secs(5))
        .await
        .expect("no server is registered");
    // A server that starts while a migration holds the lock exits 6.
    let refused = register_server(session.clone(), &prefix, "127.0.0.1:2", "test").await;
    assert!(
        matches!(&refused, Err(MigrationError::Locked(_))),
        "{:?}",
        refused.as_ref().err()
    );
    migrations.release(lock).await;
    // The status read and the preview take no lock beside a server.
    let registration: ServingRegistration =
        register_server(session.clone(), &prefix, "127.0.0.1:3", "test")
            .await
            .expect("register once the lock is free");
    assert!(matches!(
        migrations.status(&target_of(&prefix)).await,
        Ok(CatalogStatus::Current { .. })
    ));
    registration.release().await;
    let rows = session
        .query(format!("SELECT server_id FROM {catalog}.extenddb_servers"))
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    assert!(rows.is_empty(), "a released server leaves no row");
}

/// A code body's request to a node that stops answering fails at the request
/// timeout, through the store the body is handed, rather than when the lease
/// cuts the body 35 to 65 s later (SPEC.md A6: every request under the lock
/// is bounded by 10 s). The node is the test node behind a proxy that stops
/// passing bytes once the session is up, and the session sends every
/// request through the proxy, on one node or two.
#[tokio::test]
async fn a_bodys_request_to_a_node_that_stops_answering_fails_at_the_request_timeout() {
    use extenddb_storage::migrations::{HasManagementStore, MigrationBackend};
    use extenddb_storage_cassandra::migrations::CassandraMigrations;

    if helpers::skip_without_cassandra() {
        return;
    }
    let proxy =
        migrations_support::SilencingProxy::in_front_of(helpers::test_contact_point()).await;
    let session = proxy.session().await;
    let migrations = CassandraMigrations::for_status(session, "p2m_silent");
    let store = migrations.stores().management_store();
    // Through the proxy, a request is answered: the node reports that the
    // keyspace does not exist.
    let answered = format!(
        "{:?}",
        store.list_policies("000000000000", "user", "alice").await
    );
    assert!(answered.contains("does not exist"), "{answered}");
    proxy.go_silent();
    let started = Instant::now();
    let answer = tokio::time::timeout(
        Duration::from_secs(15),
        store.list_policies("000000000000", "user", "alice"),
    )
    .await
    .expect("an answer within the request timeout, not the lease");
    let elapsed = started.elapsed();
    let Err(error) = answer else {
        panic!("{answer:?}");
    };
    let message = format!("{error:?}");
    assert!(
        message.contains("no answer within 10 s"),
        "{message} after {elapsed:?}"
    );
    assert!(elapsed >= Duration::from_secs(10), "{elapsed:?}");
}

/// `init` runs the default account's migrations under the lease of the
/// migration lock it holds, so once that lease has lapsed the account run
/// writes nothing: no table in the account keyspace and no ledger row.
#[tokio::test]
async fn an_account_run_under_a_lapsed_lease_writes_nothing() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("lapsed");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    extenddb_storage_cassandra::migrations::migrate_catalog_keyspace(
        session.clone(),
        &prefix,
        "test",
        TIMEOUT,
    )
    .await
    .expect("migrate the catalog");
    let account = format!("{prefix}_account_111111111111");
    keyspace(&session, &account).await;

    let run = extenddb_storage_cassandra::migrations::migrate_account_under_lapsed_lease(
        session.clone(),
        &prefix,
        &account,
    )
    .await;
    let message = format!("{run:?}");
    assert!(
        run.is_err() && message.contains("was not renewed in time"),
        "the account run under a lapsed lease: {message}"
    );
    let tables = session
        .query_with_values(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            cdrs_tokio::query_values!(account.as_str()),
        )
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    assert!(
        tables.is_empty(),
        "{} tables written to {account} under a lapsed lease",
        tables.len()
    );
}
