// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ownership note where no superuser runs its statements, as on a
//! managed service; the notes after a restore that left the application
//! role unable to read the catalog's settings, which never name a data
//! database the role did not give; and the lock wait's description of a
//! session whose details the role may not see. See `migrations_support`
//! for the database these tests need.

mod migrations_support;

use std::time::Duration;

use extenddb_storage::migrations::{CatalogMigrations, MigrationId, Target};
use extenddb_storage_postgres::migrations::{
    MigrationLock, PostgresCatalogMigrations, migrate, status_notes,
};
use migrations_support::{Scratch, base_conn, note_steps, pool, skip};
use sqlx::PgPool;

/// Run `sql` on `url`, panicking with it on an error.
async fn run(url: &str, sql: &str) {
    let p = pool(url, 1).await;
    sqlx::raw_sql(sql)
        .execute(&p)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    p.close().await;
}

/// `postgresql://<role>:<password>@<host of base>/<database>`.
fn as_role(base: &str, role: &str, password: &str, database: &str) -> String {
    let host = base.rsplit('@').next().expect("a host in the base URL");
    format!("postgresql://{role}:{password}@{host}/{database}")
}

async fn drop_roles(admin: &PgPool, roles: &[&str]) {
    for role in roles {
        let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
            .execute(admin)
            .await;
    }
}

/// The objects belong to an owner that is not a superuser, and the
/// application role was created by an administrator that is not one
/// either: each statement the notes print, run as printed by the role the
/// note names for it, gives the objects to the application role, and the
/// run then applies every migration.
#[tokio::test]
async fn an_owner_that_is_not_a_superuser_runs_the_printed_statements() {
    let Some(base) = base_conn() else {
        return skip("owner without a superuser");
    };
    let s = Scratch::new(&base, "pgm_nosuper").await;
    let (adm, app, dba) = (
        format!("{}_adm", s.catalog_name),
        format!("{}_app", s.catalog_name),
        format!("{}_dba", s.catalog_name),
    );
    let admin = pool(&format!("{base}/postgres"), 1).await;
    // An administrator gives only the attributes it has (PostgreSQL 16), so
    // it has CREATEDB to create an owner that has it.
    run(
        &format!("{base}/postgres"),
        &format!("CREATE ROLE \"{adm}\" LOGIN CREATEROLE CREATEDB PASSWORD 'adm-test'"),
    )
    .await;
    let adm_url = as_role(&base, &adm, "adm-test", "postgres");
    run(
        &adm_url,
        &format!(
            "CREATE ROLE \"{app}\" LOGIN PASSWORD 'owner-test'; \
             CREATE ROLE \"{dba}\" LOGIN CREATEDB PASSWORD 'dba-test'"
        ),
    )
    .await;
    // The owner owns both databases, as the administrator that restored
    // them does, and loads the released catalog itself.
    for database in [&s.catalog_name, &s.data_name] {
        run(
            &format!("{base}/postgres"),
            &format!("ALTER DATABASE \"{database}\" OWNER TO \"{dba}\""),
        )
        .await;
    }
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/released/0.1.14");
    for (file, database) in [("catalog.sql", &s.catalog_name), ("data.sql", &s.data_name)] {
        let sql = std::fs::read_to_string(dir.join(file)).expect("fixture");
        run(&as_role(&base, &dba, "dba-test", database), &sql).await;
        run(
            &as_role(&base, &dba, "dba-test", database),
            &format!(
                "GRANT ALL ON ALL TABLES IN SCHEMA public TO \"{app}\"; \
                 GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO \"{app}\""
            ),
        )
        .await;
    }
    let data_url = as_role(&base, &app, "owner-test", &s.data_name);
    run(
        &as_role(&base, &dba, "dba-test", &s.catalog_name),
        &format!(
            "UPDATE settings SET value = '{data_url}' WHERE key = \
             'data_database_connection_string'; UPDATE settings SET value = '{}' WHERE key = \
             'data_database_name'",
            s.data_name
        ),
    )
    .await;
    let mut target = s.target().with_data_connection_row(data_url);
    target.catalog = target.catalog.username(&app).password("owner-test");
    let notes = status_notes(&target).await.expect("status notes");
    assert_eq!(notes.len(), 2, "{notes:?}");
    for (note, database) in notes.iter().zip([&s.catalog_name, &s.data_name]) {
        let steps = note_steps(&note.text);
        let order: Vec<&str> = steps
            .iter()
            .map(|(_, statement)| statement.split(' ').next().unwrap_or(""))
            .collect();
        assert_eq!(order, ["GRANT", "GRANT", "DO", "REVOKE"], "{}", note.text);
        assert_eq!(steps[3].1, format!("REVOKE \"{app}\" FROM \"{dba}\";"));
        assert_eq!(steps[0].1, format!("GRANT \"{app}\" TO \"{dba}\";"));
        assert_eq!(
            steps[1].1,
            format!("GRANT CREATE ON SCHEMA public TO \"{app}\";")
        );
        for (who, statement) in &steps {
            let url = if who.starts_with("an administrator that may grant") {
                as_role(&base, &adm, "adm-test", database)
            } else if who.starts_with(&format!("{dba},")) {
                as_role(&base, &dba, "dba-test", database)
            } else {
                panic!(
                    "a statement for {who}, where no superuser runs them: {}",
                    note.text
                );
            };
            run(&url, statement).await;
        }
    }
    assert_eq!(status_notes(&target).await.expect("status notes"), []);
    migrate(&target, "test", Duration::from_secs(5))
        .await
        .expect("the role owns every object now");
    let owner: String = sqlx::query_scalar(
        "SELECT pg_get_userbyid(datdba)::text FROM pg_database WHERE datname = $1",
    )
    .bind(&s.catalog_name)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(owner, dba, "the databases keep their owner");
    let member: bool = sqlx::query_scalar("SELECT pg_has_role($1::name, $2::name, 'MEMBER')")
        .bind(&dba)
        .bind(&app)
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(!member, "the last step takes back the owner's membership");
    for database in [&s.catalog_name, &s.data_name] {
        run(
            &format!("{base}/{database}"),
            &format!("DROP OWNED BY \"{app}\""),
        )
        .await;
    }
    drop(s);
    drop_roles(&admin, &[&app, &dba, &adm]).await;
    admin.close().await;
}

/// A restore that left the application role unable to read the catalog's
/// settings, where the data database is not named after the catalog
/// database and a database of the name derived from the catalog's belongs
/// to another application: the notes name the catalog database and say how
/// to see the data database's note, and never name the other application's
/// database; with the data database given, both notes are printed.
#[tokio::test]
async fn a_restore_without_grants_names_no_guessed_data_database() {
    let Some(base) = base_conn() else {
        return skip("restore without grants");
    };
    let s = Scratch::with_names(&base, "pgm_nogrant", "_catalog", "_items", &[""]).await;
    s.load("0.1.14").await;
    let other = s.catalog_name.trim_end_matches("_catalog").to_owned();
    run(
        &format!("{base}/{other}"),
        "CREATE TABLE orders (id int PRIMARY KEY)",
    )
    .await;
    let app = format!("{other}_app");
    let admin = pool(&format!("{base}/postgres"), 1).await;
    run(
        &format!("{base}/postgres"),
        &format!("CREATE ROLE \"{app}\" LOGIN PASSWORD 'owner-test'"),
    )
    .await;
    let mut target = Target::at(as_role(&base, &app, "owner-test", &s.catalog_name).as_str());
    let denied = PostgresCatalogMigrations
        .status(&target)
        .await
        .expect_err("the role cannot read the settings");
    let text = denied.to_string();
    let notes: Vec<&str> = text.lines().filter(|l| l.starts_with("Note: ")).collect();
    assert!(
        text.contains("permission denied for table settings"),
        "{text}"
    );
    assert_eq!(notes.len(), 2, "{text}");
    assert!(
        notes[0].contains(&format!("catalog database {}:", s.catalog_name)),
        "{text}"
    );
    assert!(
        notes[1].contains("which the application role cannot read")
            && notes[1].contains("--data-db <name>"),
        "{text}"
    );
    assert!(
        !text.contains(&format!("database {other}:")) && !text.contains("orders"),
        "a note names the database of the derived name: {text}"
    );
    target.data_store = Some(s.data_name.clone());
    let denied = PostgresCatalogMigrations
        .status(&target)
        .await
        .expect_err("the role still cannot read the settings");
    let text = denied.to_string();
    let notes: Vec<&str> = text.lines().filter(|l| l.starts_with("Note: ")).collect();
    assert_eq!(notes.len(), 2, "{text}");
    assert!(
        notes[0].contains(&format!("catalog database {}:", s.catalog_name))
            && notes[1].contains(&format!("data database {}:", s.data_name)),
        "{text}"
    );
    assert!(
        !text.contains("--data-db <name>") && !text.contains("orders"),
        "{text}"
    );
    drop(s);
    drop_roles(&admin, &[&app]).await;
    admin.close().await;
}

/// A session of another role holds the migration lock: the wait names its
/// pid and role, and says its address and connection time are hidden from
/// this role, instead of printing them empty.
#[tokio::test]
async fn a_lock_held_by_another_role_names_that_role() {
    let Some(base) = base_conn() else {
        return skip("hidden lock holder");
    };
    let s = Scratch::new(&base, "pgm_hidden").await;
    s.migrate_through(MigrationId::LATEST).await;
    let (holder, app) = (
        format!("{}_holder", s.catalog_name),
        format!("{}_app", s.catalog_name),
    );
    let admin = pool(&format!("{base}/postgres"), 1).await;
    run(
        &format!("{base}/postgres"),
        &format!(
            "CREATE ROLE \"{holder}\" LOGIN PASSWORD 'holder-test'; \
             CREATE ROLE \"{app}\" LOGIN PASSWORD 'owner-test'"
        ),
    )
    .await;
    let held = pool(&as_role(&base, &holder, "holder-test", &s.catalog_name), 1).await;
    let mut tx = held.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(4539458, 1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let catalog = s.options().username(&app).password("owner-test");
    let err = match MigrationLock::acquire(&catalog, Duration::from_secs(1)).await {
        Ok(_) => panic!("the lock is held by another session"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains(&format!("pid {pid}, a session of the role {holder}"))
            && err.contains("cannot see")
            && !err.contains("connected )"),
        "{err}"
    );
    tx.rollback().await.unwrap();
    held.close().await;
    drop(s);
    drop_roles(&admin, &[&holder, &app]).await;
    admin.close().await;
}
