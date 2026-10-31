// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The backend's guards that no released body reaches: a code body that
//! commits the runner's transaction itself is stopped before its ledger
//! row, the serving session carries its keepalive settings, a registration
//! whose database stops answering is reported lost, an index build that
//! waits on a lock is bounded, and a role that does not own the tables is
//! told what to run. See `migrations_support` for the database these tests
//! need.

mod migrations_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extenddb_storage::migrations::{
    CodeBody, Done, HasManagementStore, Kind, LedgerRow, MigrationBackend, MigrationError,
    MigrationId, Outcome, RunKind, Runner, Step, StoreKind, StoreRef, Target, before_lock, exit,
};
use extenddb_storage_postgres::migrations::{
    MigrationLock, PostgresCatalogMigrations, PostgresMigrations, PostgresStores,
    ServingRegistration, migrate, status_notes,
};
use migrations_support::{ACCOUNT, Scratch, base_conn, count, note_steps, pool, skip};
use sqlx::postgres::PgConnectOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Creates a user through the management store, which commits a
/// transaction of its own on the runner's connection.
struct CommitsItself;

#[async_trait]
impl CodeBody<PostgresStores> for CommitsItself {
    fn fingerprint(&self) -> &'static str {
        "0002_resource_tag_condition_keys/test/1"
    }

    async fn detect(&self, _stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        stores
            .management_store()
            .create_user(ACCOUNT, "made_by_a_body", None)
            .await
            .map_err(|e| MigrationError::Store(format!("{e:?}")))?;
        Ok(Done::default())
    }
}

#[tokio::test]
async fn a_body_that_commits_the_runners_transaction_is_stopped_before_its_row() {
    let Some(base) = base_conn() else {
        return skip("commit guard");
    };
    let s = Scratch::new(&base, "pgm_guard").await;
    s.migrate_through(MigrationId::Baseline).await;
    let catalog = s.catalog().await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'a')")
        .bind(ACCOUNT)
        .execute(&catalog)
        .await
        .unwrap();
    let m = PostgresMigrations::open_unlocked(&s.target())
        .await
        .unwrap();
    let store = StoreRef {
        kind: StoreKind::Catalog,
        name: s.catalog_name.clone(),
    };
    let row = LedgerRow {
        id: 2,
        name: "resource_tag_condition_keys".to_owned(),
        kind: Kind::Breaking,
        checksum: "c".to_owned(),
        outcome: Outcome::Applied,
        notes: String::new(),
        applied_at: String::new(),
        applied_by: "test".to_owned(),
    };
    let err = m
        .apply(
            &store,
            MigrationId::ResourceTagConditionKeys,
            Step::Code(&CommitsItself),
            Some(row),
            Some(2),
        )
        .await
        .expect_err("stopped");
    assert!(
        err.to_string()
            .contains("committed the runner's transaction itself"),
        "{err}"
    );
    m.close().await;
    // The ledger has no row for the body, whatever the body committed.
    assert_eq!(
        count(
            &catalog,
            "SELECT count(*) FROM extenddb_migrations WHERE id = 2"
        )
        .await,
        0
    );
    catalog.close().await;
}

#[tokio::test]
async fn the_serving_session_sets_its_keepalives_and_name() {
    let Some(base) = base_conn() else {
        return skip("serving session");
    };
    let s = Scratch::new(&base, "pgm_ka").await;
    let serving = ServingRegistration::register(&s.options()).await.unwrap();
    let settings = serving.session_settings().await.unwrap();
    assert_eq!(
        settings,
        [
            ("application_name".to_owned(), "extenddb serve".to_owned()),
            ("tcp_keepalives_count".to_owned(), "4".to_owned()),
            ("tcp_keepalives_idle".to_owned(), "10".to_owned()),
            ("tcp_keepalives_interval".to_owned(), "5".to_owned()),
        ]
    );
    serving.release().await;
}

/// A TCP proxy to the test server that can stop forwarding while it keeps
/// both connections open: what a client sees when the database host is
/// paused or vanishes without closing the connection.
struct Proxy {
    port: u16,
    paused: Arc<AtomicBool>,
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    paused: Arc<AtomicBool>,
) {
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
        if paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        // A read that times out is dropped before it completes, which loses
        // nothing on a TcpStream, so the pause is noticed within 50 ms.
        match tokio::time::timeout(Duration::from_millis(50), from.read(&mut buf)).await {
            Err(_) => {}
            Ok(Ok(0) | Err(_)) => return,
            Ok(Ok(n)) => {
                if to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        }
    }
}

impl Proxy {
    async fn start(upstream: (String, u16)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let paused = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&paused);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect((upstream.0.as_str(), upstream.1)).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(pump(client_read, server_write, Arc::clone(&flag)));
                tokio::spawn(pump(server_read, client_write, Arc::clone(&flag)));
            }
        });
        Self { port, paused }
    }

    fn options(&self, s: &Scratch) -> PgConnectOptions {
        s.options().host("127.0.0.1").port(self.port)
    }
}

/// A database that stops answering without closing the connection: the
/// registration counts as lost within twice its keepalive interval, and the
/// migration lock's check fails instead of waiting.
#[tokio::test]
async fn a_registration_whose_database_stops_answering_is_lost() {
    let Some(base) = base_conn() else {
        return skip("renewal deadline");
    };
    let s = Scratch::new(&base, "pgm_pause").await;
    let options = s.options();
    let proxy = Proxy::start((options.get_host().to_owned(), options.get_port())).await;
    let serving = ServingRegistration::register_with(&proxy.options(&s), Duration::from_secs(1))
        .await
        .unwrap();
    let mut lost = serving.lost();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(lost.borrow().is_none(), "lost while the database answers");
    proxy.paused.store(true, Ordering::SeqCst);
    let paused_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(10), lost.changed())
        .await
        .expect("the registration is reported lost within 10 s of the pause")
        .unwrap();
    let reason = lost.borrow().clone().unwrap();
    assert!(reason.contains("could not be renewed"), "{reason}");
    assert!(paused_at.elapsed() >= Duration::from_secs(1), "{reason}");
    proxy.paused.store(false, Ordering::SeqCst);
    serving.release().await;

    let lock = MigrationLock::acquire_with(
        &proxy.options(&s),
        Duration::from_secs(5),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    proxy.paused.store(true, Ordering::SeqCst);
    let checked = tokio::time::timeout(Duration::from_secs(10), lock.check_alive())
        .await
        .expect("the check returns instead of waiting for the database");
    let err = checked.expect_err("an unanswered check fails");
    assert!(err.to_string().contains("did not answer"), "{err}");
    proxy.paused.store(false, Ordering::SeqCst);
    lock.release().await;
}

/// At a keepalive interval above the floors (3 s: a renewal waits 2 s, and
/// the deadline is 6 s), a registration whose database stops answering
/// right after it registered counts as lost 6 s after its last renewal, the
/// registration: the renewal at 3 s gets no answer by 5 s, and the next one
/// waits only for the 1 s left.
#[tokio::test]
async fn a_registration_is_lost_at_twice_its_interval_and_no_later() {
    let Some(base) = base_conn() else {
        return skip("renewal deadline at 3 s");
    };
    let s = Scratch::new(&base, "pgm_pause3").await;
    let options = s.options();
    let proxy = Proxy::start((options.get_host().to_owned(), options.get_port())).await;
    let serving = ServingRegistration::register_with(&proxy.options(&s), Duration::from_secs(3))
        .await
        .unwrap();
    let mut lost = serving.lost();
    proxy.paused.store(true, Ordering::SeqCst);
    let paused_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(15), lost.changed())
        .await
        .expect("the registration is reported lost within 15 s of the pause")
        .unwrap();
    let elapsed = paused_at.elapsed();
    let reason = lost.borrow().clone().unwrap();
    assert!(reason.contains("could not be renewed"), "{reason}");
    assert!(
        elapsed >= Duration::from_millis(5500) && elapsed < Duration::from_millis(6800),
        "lost {elapsed:?} after the pause: {reason}"
    );
    proxy.paused.store(false, Ordering::SeqCst);
    serving.release().await;
}

/// Migration 4's concurrent build that waits on a lock is cancelled after the
/// lock timeout and built again, and the run stops naming the holder once
/// every attempt waited; nothing is recorded, and the rerun builds it.
#[tokio::test]
async fn an_index_build_behind_a_held_lock_stops_naming_the_holder() {
    let Some(base) = base_conn() else {
        return skip("index lock wait");
    };
    let s = Scratch::new(&base, "pgm_cicwait").await;
    s.load("0.1.5").await;
    s.migrate_through(MigrationId::SortKeyDefinitions).await;
    let (catalog, data) = (s.catalog().await, s.data().await);
    let index_id: String =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE index_name = 'gsi1'")
            .fetch_one(&catalog)
            .await
            .unwrap();
    let mut blocker = data.begin().await.unwrap();
    sqlx::query(&format!(
        "LOCK TABLE \"_ddb_{index_id}\" IN ROW EXCLUSIVE MODE"
    ))
    .execute(&mut *blocker)
    .await
    .unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let started = Instant::now();
    let m = PostgresMigrations::open_unlocked(&s.target())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(60), Runner::new(&m, "test").migrate())
        .await
        .expect("the build's lock wait is bounded");
    m.close().await;
    let err = result.expect_err("every attempt waited on the held lock");
    let message = err.to_string();
    assert!(
        message.contains("could not be built") && message.contains(&format!("pid {blocker_pid}")),
        "{message}"
    );
    assert!(started.elapsed() >= Duration::from_secs(15), "{message}");
    blocker.rollback().await.unwrap();
    let ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM extenddb_migrations ORDER BY id")
        .fetch_all(&data)
        .await
        .unwrap();
    assert!(
        !ids.contains(&4),
        "migration 4 recorded in the data database: {ids:?}"
    );
    s.migrate_through(MigrationId::LATEST).await;
    for p in [catalog, data] {
        p.close().await;
    }
}

/// A released catalog whose tables a superuser owns: the application role
/// may read and write them but not alter them, and migration 1 stops naming
/// the owner and the statements that give the tables to the role.
#[tokio::test]
async fn a_role_that_does_not_own_the_tables_is_told_what_to_run() {
    let Some(base) = base_conn() else {
        return skip("table owners");
    };
    let s = Scratch::new(&base, "pgm_owner").await;
    s.load("0.1.14").await;
    let role = format!("{}_app", s.catalog_name);
    let admin = pool(&format!("{base}/postgres"), 1).await;
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" LOGIN PASSWORD 'owner-test'"
    ))
    .execute(&admin)
    .await
    .unwrap();
    for database in [&s.catalog_name, &s.data_name] {
        sqlx::query(&format!(
            "GRANT ALL ON DATABASE \"{database}\" TO \"{role}\""
        ))
        .execute(&admin)
        .await
        .unwrap();
        let p = pool(&format!("{base}/{database}"), 1).await;
        for grant in [
            "GRANT ALL ON SCHEMA public TO \"{role}\"",
            "GRANT ALL ON ALL TABLES IN SCHEMA public TO \"{role}\"",
            "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO \"{role}\"",
        ] {
            sqlx::query(&grant.replace("{role}", &role))
                .execute(&p)
                .await
                .unwrap();
        }
        p.close().await;
    }
    // A view the superuser owns: the note names it and the statement it
    // prints gives it to the role with the tables.
    let catalog_admin = pool(&format!("{base}/{}", s.catalog_name), 1).await;
    sqlx::query("CREATE VIEW owner_test_view AS SELECT 1 AS x")
        .execute(&catalog_admin)
        .await
        .unwrap();
    catalog_admin.close().await;
    let mut target = s.target();
    target.catalog = target.catalog.username(&role).password("owner-test");
    let host = base
        .rsplit('@')
        .next()
        .expect("a host in the connection string");
    // The server, and so the runner, reaches the data database as the role
    // too, through the catalog's data connection string row.
    let catalog_admin = pool(&format!("{base}/{}", s.catalog_name), 1).await;
    sqlx::query("UPDATE settings SET value = $1 WHERE key = 'data_database_connection_string'")
        .bind(format!(
            "postgresql://{role}:owner-test@{host}/{}",
            s.data_name
        ))
        .execute(&catalog_admin)
        .await
        .unwrap();
    catalog_admin.close().await;
    // Before any run: a note per database names the owner and the
    // statement that gives that database's objects to the role, and
    // migrate --yes refuses on them before it takes the lock.
    let notes = status_notes(&target).await.expect("status notes");
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert!(
        notes[0]
            .text
            .contains(&format!("catalog database {}:", s.catalog_name)),
        "{notes:?}"
    );
    assert!(
        notes[1]
            .text
            .contains(&format!("data database {}:", s.data_name)),
        "{notes:?}"
    );
    for note in &notes {
        assert!(note.refuses_runs, "{note:?}");
        assert!(
            note.text
                .contains(&format!("the application role {role} does not own"))
                && note.text.contains("owned by postgres")
                && note.text.contains("OWNER TO")
                && !note.text.contains("REASSIGN"),
            "{}",
            note.text
        );
    }
    let location = format!("postgresql://{role}:owner-test@{host}/{}", s.catalog_name);
    let refused = before_lock(
        &PostgresCatalogMigrations,
        &Target::at(location.as_str()),
        RunKind::Migrate,
    )
    .await
    .expect_err("migrate --yes refuses before the lock");
    assert_eq!(refused.exit_code(), exit::ERROR);
    assert!(
        refused.to_string().contains("no lock was taken"),
        "{refused}"
    );
    let err = migrate(&target, "test", Duration::from_secs(5))
        .await
        .expect_err("the role cannot alter tables it does not own");
    let message = err.to_string();
    assert!(
        message.contains("must own every table")
            && message.contains("owned by postgres")
            && message.contains("DO $$")
            && message.contains("OWNER TO")
            && !message.contains("REASSIGN"),
        "{message}"
    );
    let catalog = s.catalog().await;
    assert_eq!(
        count(
            &catalog,
            "SELECT count(*) FROM pg_tables WHERE tablename = 'extenddb_migrations'"
        )
        .await,
        0,
        "migration 1 committed something"
    );
    catalog.close().await;
    // The statements the note prints give the objects to the role; the note
    // is then gone and the run applies every migration. The objects are a
    // superuser's, so a superuser runs each of them.
    for (note, database) in notes.iter().zip([&s.catalog_name, &s.data_name]) {
        let steps = note_steps(&note.text);
        assert!(!steps.is_empty(), "{}", note.text);
        let p = pool(&format!("{base}/{database}"), 1).await;
        for (who, statement) in steps {
            assert_eq!(who, "a superuser", "{}", note.text);
            sqlx::raw_sql(&statement)
                .execute(&p)
                .await
                .unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        p.close().await;
    }
    assert_eq!(status_notes(&target).await.expect("status notes"), []);
    migrate(&target, "test", Duration::from_secs(5))
        .await
        .expect("the role owns every object now");
    for database in [&s.catalog_name, &s.data_name] {
        let p = pool(&format!("{base}/{database}"), 1).await;
        let _ = sqlx::query(&format!("DROP OWNED BY \"{role}\""))
            .execute(&p)
            .await;
        p.close().await;
    }
    drop(s);
    let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
        .execute(&admin)
        .await;
    admin.close().await;
}
