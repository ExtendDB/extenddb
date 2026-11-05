// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The server's registration after the database ends its session, as a
//! restart, a failover or `pg_terminate_backend` does: the backend's side of
//! registering again. The loss is reported at the next renewal; an attempt
//! to register again takes the serving key on a new session when no
//! migration holds the catalog and its status is the one the server started
//! with, and is refused otherwise, leaving no key behind. See
//! `migrations_support` for the database these tests need.

mod migrations_support;

use std::time::Duration;

use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::migrations::{CatalogStatus, MigrationId, Runner};
use extenddb_storage_postgres::migrations::{
    MigrationLock, PostgresMigrations, ServerRegistration, migrate,
};
use migrations_support::{LATEST, Scratch, base_conn, pool, skip};
use sqlx::PgPool;

const RELEASE: &str = "1.0.0-test";

/// The renewal interval of these tests, so a loss is reported within 1 s.
const KEEPALIVE: Duration = Duration::from_secs(1);

/// A scratch pair with a fresh install, as `init` leaves it.
async fn installed(base: &str, prefix: &str) -> Scratch {
    let s = Scratch::new(base, prefix).await;
    s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    s
}

/// The sessions that hold the serving key on the scratch catalog, with
/// their `application_name`.
async fn serving_sessions(admin: &PgPool, s: &Scratch) -> Vec<(i32, String)> {
    sqlx::query_as(
        "SELECT l.pid, coalesce(a.application_name, '') FROM pg_locks l \
         JOIN pg_database d ON d.oid = l.database \
         LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.classid = 4539458 AND l.objid = 2 \
           AND l.objsubid = 2 AND l.mode = 'ShareLock' AND l.granted AND d.datname = $1 \
         ORDER BY l.pid",
    )
    .bind(&s.catalog_name)
    .fetch_all(admin)
    .await
    .unwrap()
}

/// End the one session that holds the serving key, and return its pid.
async fn end_the_serving_session(admin: &PgPool, s: &Scratch) -> i32 {
    let sessions = serving_sessions(admin, s).await;
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    let (pid, _) = sessions[0];
    let ended: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(admin)
        .await
        .unwrap();
    assert!(ended, "pid {pid} was not ended");
    pid
}

/// The reason [`ServingRegistrar::lost`] reports, within 10 s.
async fn lost(registration: &ServerRegistration) -> String {
    tokio::time::timeout(Duration::from_secs(10), registration.lost())
        .await
        .expect("the loss is reported within 10 s")
}

/// The registration is reported lost at the next renewal after the database
/// ends its session, and is taken again on a new session, which excludes a
/// migration again; a second loss is handled the same way.
#[tokio::test]
async fn a_registration_the_database_ended_is_taken_again_on_a_new_session() {
    let Some(base) = base_conn() else {
        return skip("registered again");
    };
    let s = installed(&base, "pgs_again").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let registration = ServerRegistration::register_with(&s.catalog_url(), KEEPALIVE)
        .await
        .expect("register");
    let mut ended = Vec::new();
    for round in 1..=2 {
        ended.push(end_the_serving_session(&admin, &s).await);
        let reason = lost(&registration).await;
        assert!(
            reason.contains("serving registration's connection was lost"),
            "round {round}: {reason}"
        );
        assert_eq!(
            serving_sessions(&admin, &s).await,
            [],
            "round {round}: the database kept the key"
        );
        assert_eq!(
            registration.register_again().await,
            Reregistration::Registered,
            "round {round}"
        );
        let sessions = serving_sessions(&admin, &s).await;
        assert_eq!(sessions.len(), 1, "round {round}: {sessions:?}");
        assert_eq!(sessions[0].1, "extenddb serve", "round {round}");
        assert!(
            !ended.contains(&sessions[0].0),
            "round {round}: the key is on an ended session {sessions:?}"
        );
        // The new registration excludes a migration, and is not reported
        // lost over two renewals.
        let err = migrate(&s.target(), RELEASE, Duration::from_secs(1))
            .await
            .expect_err("a migration while the server is registered again");
        assert_eq!(err.exit_code(), 6, "round {round}: {err}");
        assert!(
            err.to_string().contains("\"extenddb serve\""),
            "round {round}: {err}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(2500), registration.lost())
                .await
                .is_err(),
            "round {round}: the new registration was reported lost"
        );
    }
    admin.close().await;
}

/// A migration that holds the catalog when the server tries again refuses
/// the attempt, which leaves no key behind: one that holds both keys, and one
/// of a release before 1.0 that holds the migration key alone. Once it has
/// finished without a change, the server registers again.
#[tokio::test]
async fn a_migration_that_holds_the_catalog_refuses_the_registration() {
    let Some(base) = base_conn() else {
        return skip("refused by a migration");
    };
    let s = installed(&base, "pgs_held").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let registration = ServerRegistration::register_with(&s.catalog_url(), KEEPALIVE)
        .await
        .expect("register");
    end_the_serving_session(&admin, &s).await;
    lost(&registration).await;
    let lock = MigrationLock::acquire(&s.options(), Duration::from_secs(5))
        .await
        .expect("the migration lock, once the server's key is gone");
    match registration.register_again().await {
        Reregistration::Refused(why) => assert!(
            why.contains("a migration is running on this catalog") && why.contains("pid "),
            "{why}"
        ),
        other => panic!("registered beside a migration: {other:?}"),
    }
    assert_eq!(serving_sessions(&admin, &s).await, []);
    lock.release().await;

    let catalog = s.catalog().await;
    let mut legacy = catalog.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(4539458, 1)")
        .execute(&mut *legacy)
        .await
        .unwrap();
    match registration.register_again().await {
        Reregistration::Refused(why) => {
            assert!(why.contains("holds the migration lock"), "{why}");
        }
        other => panic!("registered beside a migrator: {other:?}"),
    }
    assert_eq!(serving_sessions(&admin, &s).await, []);
    legacy.rollback().await.unwrap();
    catalog.close().await;

    assert_eq!(
        registration.register_again().await,
        Reregistration::Registered
    );
    assert_eq!(serving_sessions(&admin, &s).await.len(), 1);
    admin.close().await;
}

/// A catalog whose status changed while the server held no registration
/// refuses the attempt, naming both statuses; the key it took is released.
#[tokio::test]
async fn a_status_that_changed_while_unregistered_refuses_the_registration() {
    let Some(base) = base_conn() else {
        return skip("refused on a changed status");
    };
    let s = installed(&base, "pgs_status").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let registration = ServerRegistration::register_with(&s.catalog_url(), KEEPALIVE)
        .await
        .expect("register");
    end_the_serving_session(&admin, &s).await;
    lost(&registration).await;
    // A migrator clears the latest migration in the meantime.
    let lock = MigrationLock::acquire(&s.options(), Duration::from_secs(5))
        .await
        .unwrap();
    let m = PostgresMigrations::open(&s.target(), &lock).await.unwrap();
    let forgot = Runner::new(&m, RELEASE).forget(LATEST).await;
    m.close().await;
    lock.release().await;
    forgot.expect("forget the latest migration");
    match registration.register_again().await {
        Reregistration::Refused(why) => assert!(
            why.contains("status changed")
                && why.contains(&format!("migration {LATEST}"))
                && why.contains("start the server again"),
            "{why}"
        ),
        other => panic!("registered on a changed catalog: {other:?}"),
    }
    assert_eq!(serving_sessions(&admin, &s).await, []);
    admin.close().await;
}

/// A database that accepts no connection makes the attempt unavailable,
/// not refused, so the server tries again; once it accepts them, the server
/// registers.
#[tokio::test]
async fn an_attempt_the_database_cannot_answer_is_unavailable() {
    let Some(base) = base_conn() else {
        return skip("unavailable attempt");
    };
    let s = installed(&base, "pgs_closed").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let registration = ServerRegistration::register_with(&s.catalog_url(), KEEPALIVE)
        .await
        .expect("register");
    let allow = |allowed: bool| {
        format!(
            "ALTER DATABASE \"{}\" WITH ALLOW_CONNECTIONS {allowed}",
            s.catalog_name
        )
    };
    sqlx::query(&allow(false)).execute(&admin).await.unwrap();
    end_the_serving_session(&admin, &s).await;
    lost(&registration).await;
    let attempt = registration.register_again().await;
    sqlx::query(&allow(true)).execute(&admin).await.unwrap();
    match attempt {
        Reregistration::Unavailable(why) => assert!(why.contains("connect"), "{why}"),
        other => panic!("an attempt the database cannot answer: {other:?}"),
    }
    assert_eq!(
        registration.register_again().await,
        Reregistration::Registered
    );
    admin.close().await;
}

/// The server drops an attempt at the bound of its window, and at
/// shutdown. An attempt dropped after it took the serving key, while its
/// status read waits on a lock, leaves no key held: a migration that then
/// asks for the catalog sees no server.
#[tokio::test]
async fn an_attempt_dropped_while_its_status_read_waits_leaves_no_key() {
    let Some(base) = base_conn() else {
        return skip("a dropped attempt");
    };
    let s = installed(&base, "pgm_drop").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let registration = ServerRegistration::register_with(&s.catalog_url(), KEEPALIVE)
        .await
        .expect("register");
    end_the_serving_session(&admin, &s).await;
    lost(&registration).await;
    // The status read of the next attempt waits on this lock.
    let catalog = s.catalog().await;
    let mut blocker = catalog.begin().await.expect("begin");
    sqlx::query("LOCK TABLE extenddb_migrations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("lock the ledger");
    let attempt = tokio::spawn(async move {
        let answer =
            tokio::time::timeout(Duration::from_secs(3), registration.register_again()).await;
        (answer.is_err(), registration)
    });
    let mut held_while_waiting = Vec::new();
    for _ in 0..20 {
        held_while_waiting = serving_sessions(&admin, &s).await;
        if !held_while_waiting.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (dropped, _registration) = attempt.await.expect("the attempt's task");
    let since = std::time::Instant::now();
    let mut held = serving_sessions(&admin, &s).await;
    while !held.is_empty() && since.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        held = serving_sessions(&admin, &s).await;
    }
    blocker.rollback().await.expect("release the ledger");
    catalog.close().await;
    assert!(
        dropped,
        "the attempt finished within 3 s, so it was not dropped"
    );
    assert!(
        !held_while_waiting.is_empty(),
        "the attempt never took the key, so its drop shows nothing"
    );
    assert_eq!(held, [], "a dropped attempt left the serving key held");
    admin.close().await;
}
