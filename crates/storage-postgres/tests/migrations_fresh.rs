// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Migration 1 on empty databases, the expected definitions, the status of
//! every state, and the post-check. See `migrations_support` for the
//! database these tests need.

mod migrations_support;

use extenddb_storage::migrations::{CatalogStatus, MigrationId, StoreKind};
use extenddb_storage_postgres::migrations::{
    Definitions, catalog_status, compare, describe, migrate,
};
use migrations_support::{
    LATEST, Scratch, base_conn, count, ids_in, ledger, legacy_rows, pool, settings, skip,
    step_count,
};

const CATALOG_BASELINE: &str = include_str!("../migrations/0001_baseline.sql");
const DATA_BASELINE: &str = include_str!("../data_migrations/0001_baseline.sql");
const RELEASE: &str = "1.0.0-test";

fn expected_path(dir: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(dir)
        .join("0001_baseline.expected.json")
}

/// The committed expected definitions are what migration 1's files create in
/// empty databases. `EXTENDDB_BLESS_EXPECTED=1` writes them instead.
#[tokio::test]
async fn the_expected_definitions_are_what_the_baseline_files_create() {
    let Some(base) = base_conn() else {
        return skip("expected definitions");
    };
    let s = Scratch::new(&base, "pgm_expect").await;
    for (url, sql, dir) in [
        (s.catalog_url(), CATALOG_BASELINE, "migrations"),
        (s.data_url(), DATA_BASELINE, "data_migrations"),
    ] {
        let p = pool(&url, 1).await;
        sqlx::raw_sql(sql)
            .execute(&p)
            .await
            .expect("apply the baseline file");
        let defs = describe(&p).await.expect("describe");
        p.close().await;
        let path = expected_path(dir);
        if migrations_support::bless() {
            let text = serde_json::to_string_pretty(&defs).expect("serialize") + "\n";
            std::fs::write(&path, text).expect("write the expected definitions");
            continue;
        }
        let committed: Definitions =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        let (differences, notes) = compare(&committed, &defs);
        assert!(
            differences.is_empty() && notes.is_empty(),
            "{dir}: {differences:?} {notes:?}"
        );
        assert_eq!(committed, defs, "{dir}");
    }
}

#[tokio::test]
async fn a_fresh_install_is_current_and_a_second_run_changes_nothing() {
    let Some(base) = base_conn() else {
        return skip("fresh install");
    };
    let s = Scratch::new(&base, "pgm_fresh").await;
    assert_eq!(s.status().await, CatalogStatus::Uninitialized);
    let report = migrate(&s.target(), RELEASE, std::time::Duration::from_secs(5))
        .await
        .expect("migrate");
    assert_eq!(report.from, None);
    assert_eq!(report.to, Some(LATEST));
    // Every id in order, the catalog first; the data database for the ids
    // with a data body.
    let order: Vec<(u32, String)> = report
        .applied
        .iter()
        .map(|a| (a.id, a.store_name.clone()))
        .collect();
    assert_eq!(order, s.all_steps());
    let (catalog, data) = (s.catalog().await, s.data().await);
    let ids = |rows: Vec<(i32, String, String)>| rows.into_iter().map(|r| r.0).collect::<Vec<_>>();
    assert_eq!(ids(ledger(&catalog).await), ids_in(StoreKind::Catalog));
    let data_ids = ids(ledger(&data).await);
    assert_eq!(data_ids, ids_in(StoreKind::Data));
    // Migrations 1 and 4 have data bodies.
    assert!(data_ids.starts_with(&[1, 4]), "{data_ids:?}");
    let settings_rows = settings(&catalog).await;
    assert_eq!(
        settings_rows.get("catalog_version"),
        Some(&LATEST.to_string())
    );
    assert!(!settings_rows.contains_key("resource_tag_migration"));
    assert_eq!(settings_rows.get("data_database_name"), Some(&s.data_name));
    assert_eq!(
        settings_rows.get("data_database_connection_string"),
        Some(&s.data_url())
    );
    // No baseline seeds a setting: the version and the connection rows only.
    assert_eq!(settings_rows.len(), 3, "{settings_rows:?}");
    assert_eq!(
        legacy_rows(&catalog).await,
        [
            "001_schema.sql",
            "002_vector_indexes.sql",
            "003_backup_definitions.sql"
        ]
    );
    assert_eq!(
        legacy_rows(&data).await,
        [
            "001_data_schema.sql",
            "002_gsi_pending.sql",
            "003_gsi_base_key_index",
            "003_idempotency_account_scope.sql",
            "004_vector_index_state.sql"
        ]
    );
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    let before = (
        describe(&catalog).await.unwrap(),
        describe(&data).await.unwrap(),
        ledger(&catalog).await,
    );
    let again = migrate(&s.target(), RELEASE, std::time::Duration::from_secs(5))
        .await
        .expect("rerun");
    assert!(again.applied.is_empty(), "{again:?}");
    let after = (
        describe(&catalog).await.unwrap(),
        describe(&data).await.unwrap(),
        ledger(&catalog).await,
    );
    assert_eq!(before, after);
}

#[tokio::test]
async fn the_status_names_each_state_and_writes_nothing() {
    let Some(base) = base_conn() else {
        return skip("status");
    };
    // A catalog database that does not exist.
    let missing = Scratch::new(&base, "pgm_missing").await;
    let target = extenddb_storage_postgres::migrations::PostgresTarget::new(
        format!("{base}/pgm_no_such_database").parse().unwrap(),
        &missing.data_name,
    );
    assert_eq!(
        catalog_status(&target).await.unwrap(),
        CatalogStatus::Uninitialized
    );

    let s = Scratch::new(&base, "pgm_status").await;
    s.load("0.1.14").await;
    let catalog = s.catalog().await;
    let snapshot = || async {
        let c = s.catalog().await;
        let d = s.data().await;
        let out = (
            describe(&c).await.unwrap(),
            describe(&d).await.unwrap(),
            settings(&c).await,
        );
        c.close().await;
        d.close().await;
        out
    };
    let before = snapshot().await;
    let CatalogStatus::Behind {
        version: None,
        pending,
    } = s.status().await
    else {
        panic!("a legacy catalog is behind at migration 1");
    };
    assert_eq!(pending.len(), step_count());
    assert_eq!(
        before,
        snapshot().await,
        "reading the status wrote to the catalog"
    );

    s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    // A release before 1.0 ran its `migrate`, which inserts the marker again.
    sqlx::query("INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'applied')")
        .execute(&catalog)
        .await
        .unwrap();
    assert!(matches!(
        s.status().await,
        CatalogStatus::WrittenByOlderRelease { .. }
    ));
    sqlx::query("DELETE FROM settings WHERE key = 'resource_tag_migration'")
        .execute(&catalog)
        .await
        .unwrap();
    // A recorded checksum that is not this binary's.
    let right: String = sqlx::query_scalar("SELECT checksum FROM extenddb_migrations WHERE id = 1")
        .fetch_one(&catalog)
        .await
        .unwrap();
    sqlx::query("UPDATE extenddb_migrations SET checksum = 'x' WHERE id = 1")
        .execute(&catalog)
        .await
        .unwrap();
    assert!(matches!(
        s.status().await,
        CatalogStatus::Changed { id: 1, .. }
    ));
    sqlx::query("UPDATE extenddb_migrations SET checksum = $1 WHERE id = 1")
        .bind(&right)
        .execute(&catalog)
        .await
        .unwrap();
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    // Ids this binary does not carry.
    let next = i32::try_from(LATEST + 1).unwrap();
    sqlx::query(
        "INSERT INTO extenddb_migrations (id, name, kind, checksum, outcome, applied_by) \
         VALUES ($1, 'later', 'additive', 'c', 'applied', 'later')",
    )
    .bind(next)
    .execute(&catalog)
    .await
    .unwrap();
    let status = s.status().await;
    assert!(
        matches!(status, CatalogStatus::AheadCompatible { .. }),
        "{status:?}"
    );
    sqlx::query("UPDATE extenddb_migrations SET kind = 'breaking' WHERE id = $1")
        .bind(next)
        .execute(&catalog)
        .await
        .unwrap();
    assert!(matches!(
        s.status().await,
        CatalogStatus::AheadIncompatible { .. }
    ));
    catalog.close().await;
}

/// Migration 1 never moves a stream sequence back and leaves the tokens of an
/// idempotency table that is already keyed per account.
#[tokio::test]
async fn migration_1_keeps_the_stream_sequences_and_the_idempotency_tokens() {
    let Some(base) = base_conn() else {
        return skip("sequences and tokens");
    };
    let s = Scratch::new(&base, "pgm_seq").await;
    s.load("0.1.14").await;
    let (catalog, data) = (s.catalog().await, s.data().await);
    // Ahead of the clock by about eleven days, as after a clock correction.
    let ahead: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT + 1000000000000")
            .fetch_one(&data)
            .await
            .unwrap();
    for p in [&catalog, &data] {
        sqlx::query("SELECT setval('stream_seq', $1)")
            .bind(ahead)
            .execute(p)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO idempotency_tokens (account_id, token, fingerprint) VALUES ('123456789012', 't1', 'f')")
        .execute(&data)
        .await
        .unwrap();
    migrate(&s.target(), RELEASE, std::time::Duration::from_secs(5))
        .await
        .expect("migrate");
    for p in [&catalog, &data] {
        let last: i64 = sqlx::query_scalar("SELECT last_value FROM stream_seq")
            .fetch_one(p)
            .await
            .unwrap();
        assert!(last >= ahead, "stream_seq went back from {ahead} to {last}");
    }
    assert_eq!(
        count(&data, "SELECT count(*) FROM idempotency_tokens").await,
        1
    );
    catalog.close().await;
    data.close().await;
}

/// A difference the post-check finds stops migration 1 in that store and
/// commits nothing there; a pre-release `table_restores` is accepted with a
/// note.
#[tokio::test]
async fn the_post_check_stops_a_different_shape_and_accepts_the_pre_release_one() {
    let Some(base) = base_conn() else {
        return skip("post-check");
    };
    let s = Scratch::new(&base, "pgm_post").await;
    s.load("0.1.14").await;
    let catalog = s.catalog().await;
    sqlx::query("ALTER TABLE accounts ADD COLUMN extra TEXT NOT NULL DEFAULT 'x'")
        .execute(&catalog)
        .await
        .unwrap();
    let err = migrate(&s.target(), RELEASE, std::time::Duration::from_secs(5))
        .await
        .expect_err("refused");
    assert!(
        err.to_string()
            .contains("column accounts.extra is not part of the v1.0 schema"),
        "{err}"
    );
    assert_eq!(
        count(
            &catalog,
            "SELECT count(*) FROM pg_class WHERE relname = 'extenddb_migrations'"
        )
        .await,
        0
    );
    let rows = settings(&catalog).await;
    assert_eq!(
        rows.get("catalog_version").map(String::as_str),
        Some("0.0.3")
    );
    assert!(rows.contains_key("resource_tag_migration"));
    assert_eq!(
        count(
            &catalog,
            "SELECT count(*) FROM pg_class WHERE relname = 'backup_definitions'"
        )
        .await,
        0
    );
    sqlx::query("ALTER TABLE accounts DROP COLUMN extra")
        .execute(&catalog)
        .await
        .unwrap();

    // The pre-release revision of legacy 003, then migrate.
    sqlx::raw_sql(include_str!("fixtures/003_backup_definitions_earlier.sql"))
        .execute(&catalog)
        .await
        .unwrap();
    migrate(&s.target(), RELEASE, std::time::Duration::from_secs(5))
        .await
        .expect("migrate");
    let note: String = sqlx::query_scalar("SELECT notes FROM extenddb_migrations WHERE id = 1")
        .fetch_one(&catalog)
        .await
        .unwrap();
    assert!(
        note.contains("table_restores.source_table_arn is nullable"),
        "{note}"
    );
    let default: Option<String> = sqlx::query_scalar(
        "SELECT column_default FROM information_schema.columns \
         WHERE table_name = 'table_restores' AND column_name = 'restore_date_time'",
    )
    .fetch_one(&catalog)
    .await
    .unwrap();
    assert_eq!(default, None);
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    catalog.close().await;
}
