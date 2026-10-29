// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Every released PostgreSQL state, loaded from its committed fixture,
//! upgraded by this binary, and compared with a fresh install seeded the same
//! way: the catalog and data objects, the per-table objects with ids mapped
//! to names, and the settings rows, against the accepted differences below.
//! The rows every table held are still there.
//!
//! Accepted differences, per state:
//! - key columns of tables created before v0.1.13 keep the database
//!   collation, where CreateTable now declares `COLLATE "C"` (filed);
//! - the `control_plane_delay_seconds` row and the propagation delay row the
//!   release seeded stay, under the legacy name `gsi_propagation_delay_ms`
//!   on catalogs created before v0.1.7;
//! - no `default_account_id` on catalogs created before v0.1.3.
//!
//! The vector states need a server with pgvector:
//! `EXTENDDB_TEST_PGVECTOR_CONNECTION_STRING`, or the main server when it has
//! the package. See `migrations_support` for the rest.

mod migrations_support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_storage::migrations::{CatalogStatus, MigrationId};
use extenddb_storage_postgres::migrations::{compare, migrate};
use migrations_support::{
    LATEST, Scratch, base_conn, describe_pool, has_pgvector, id_names, ledger, legacy_rows,
    per_table, seed_fresh, settings, shared, skip, skip_without_pgvector,
};
use sqlx::PgPool;

const RELEASE: &str = "1.0.0-test";

/// What a released state's fixture holds.
struct State {
    name: &'static str,
    /// Built from a tag's SQL files, with no table a binary created.
    catalog_only: bool,
    vector: bool,
    /// Created by a release before v0.1.13: key columns on the database
    /// collation.
    database_collation: bool,
    /// The release seeded `gsi_propagation_delay_ms` (before v0.1.7).
    legacy_delay_key: bool,
    /// `init` wrote `default_account_id` (v0.1.3 on).
    default_account: bool,
}

const fn image(name: &'static str, before_13: bool, before_7: bool, vector: bool) -> State {
    State {
        name,
        catalog_only: false,
        vector,
        database_collation: before_13,
        legacy_delay_key: before_7,
        default_account: true,
    }
}

async fn row_counts(s: &Scratch) -> BTreeMap<String, i64> {
    let (catalog, data) = (s.catalog().await, s.data().await);
    let mut out = BTreeMap::new();
    for table in [
        "accounts",
        "tables",
        "indexes",
        "iam_users",
        "iam_policies",
        "access_keys",
        "admin_users",
        "backups",
        "backup_items",
        "tags",
    ] {
        out.insert(
            format!("catalog {table}"),
            count_rows(&catalog, table).await,
        );
    }
    for table in ["stream_shards", "stream_records", "gsi_pending"] {
        out.insert(format!("data {table}"), count_rows(&data, table).await);
    }
    let names = id_names(&catalog).await;
    let per_table: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE '\\_ddb\\_%'",
    )
    .fetch_all(&data)
    .await
    .unwrap();
    for table in per_table {
        let mut label = table.clone();
        for (id, name) in &names {
            label = label.replace(id.as_str(), name);
        }
        out.insert(format!("items {label}"), count_rows(&data, &table).await);
    }
    catalog.close().await;
    data.close().await;
    out
}

async fn count_rows(p: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM \"{table}\""))
        .fetch_one(p)
        .await
        .unwrap_or(-1)
}

async fn upgrade_and_compare(state: &State) {
    let Some(mut base) = base_conn() else {
        return skip(state.name);
    };
    if state.vector && !has_pgvector(&base).await {
        match std::env::var("EXTENDDB_TEST_PGVECTOR_CONNECTION_STRING") {
            Ok(url) if !url.trim().is_empty() => base = url.trim_end_matches('/').to_owned(),
            _ => return skip_without_pgvector(state.name),
        }
    }
    let s = Scratch::new(&base, "pgm_rel").await;
    s.load(state.name).await;
    let rows_before = row_counts(&s).await;
    let CatalogStatus::Behind { version: None, .. } = s.status().await else {
        panic!(
            "{}: a released catalog is behind at migration 1",
            state.name
        );
    };
    let report = migrate(&s.target(), RELEASE, Duration::from_secs(5))
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", state.name));
    assert_eq!(report.to, Some(LATEST), "{}", state.name);
    assert_eq!(
        s.status().await,
        CatalogStatus::Current { version: LATEST },
        "{}",
        state.name
    );
    // A table the release did not have (-1) is created empty.
    let rows_after: BTreeMap<String, i64> = row_counts(&s)
        .await
        .into_iter()
        .filter(|(k, _)| rows_before.get(k).is_some_and(|n| *n >= 0))
        .collect();
    let rows_before: BTreeMap<String, i64> =
        rows_before.into_iter().filter(|(_, n)| *n >= 0).collect();
    assert_eq!(rows_after, rows_before, "{}: rows changed", state.name);

    let fresh = Scratch::new(&base, "pgm_relf").await;
    migrate(&fresh.target(), RELEASE, Duration::from_secs(5))
        .await
        .expect("fresh install");
    if !state.catalog_only {
        seed_fresh(&fresh, state.vector).await;
    }
    let (up_c, up_d, fr_c, fr_d) = (
        s.catalog().await,
        s.data().await,
        fresh.catalog().await,
        fresh.data().await,
    );
    let (up_cat, up_data) = (describe_pool(&up_c).await, describe_pool(&up_d).await);
    let (fr_cat, fr_data) = (describe_pool(&fr_c).await, describe_pool(&fr_d).await);
    for (what, up, fr) in [("catalog", &up_cat, &fr_cat), ("data", &up_data, &fr_data)] {
        let (a, notes) = compare(&shared(fr), &shared(up));
        let (b, _) = compare(&shared(up), &shared(fr));
        assert!(
            a.is_empty() && b.is_empty() && notes.is_empty(),
            "{} {what}: {a:?} {b:?} {notes:?}",
            state.name
        );
    }
    if !state.catalog_only {
        let up_tables = per_table(&up_data, &id_names(&up_c).await);
        let mut fr_tables = per_table(&fr_data, &id_names(&fr_c).await);
        if state.database_collation {
            fr_tables = fr_tables
                .into_iter()
                .map(|o| o.replace("collation: Some(\"C\")", "collation: None"))
                .collect();
        }
        let only_up: BTreeSet<_> = up_tables.difference(&fr_tables).collect();
        let only_fresh: BTreeSet<_> = fr_tables.difference(&up_tables).collect();
        assert!(
            only_up.is_empty() && only_fresh.is_empty(),
            "{}: per-table objects differ\nonly upgraded: {only_up:#?}\nonly fresh: {only_fresh:#?}",
            state.name
        );
    }

    let up_settings = settings(&up_c).await;
    let mut expected: BTreeSet<String> = settings(&fr_c).await.into_keys().collect();
    // Rows the test wrote to seed the fresh install, not the install.
    expected.remove("control_plane_delay_seconds");
    expected.remove("index_propagation_delay_ms");
    // Rows `init` writes after the schema, with values of its own.
    expected.insert("encryption_key".to_owned());
    if state.default_account {
        expected.insert("default_account_id".to_owned());
    }
    // The rows the release seeded, which stay.
    expected.insert("control_plane_delay_seconds".to_owned());
    let delay_key = if state.legacy_delay_key {
        "gsi_propagation_delay_ms"
    } else {
        "index_propagation_delay_ms"
    };
    expected.insert(delay_key.to_owned());
    let keys: BTreeSet<String> = up_settings.keys().cloned().collect();
    assert_eq!(keys, expected, "{}: settings keys", state.name);
    assert_eq!(
        up_settings["catalog_version"],
        LATEST.to_string(),
        "{}",
        state.name
    );
    assert_eq!(
        up_settings["control_plane_delay_seconds"], "0.25",
        "{}",
        state.name
    );
    assert_eq!(up_settings[delay_key], "10", "{}", state.name);
    assert_eq!(
        up_settings["data_database_name"], s.data_name,
        "{}",
        state.name
    );
    assert_eq!(legacy_rows(&up_c).await.len(), 3, "{}", state.name);
    assert_eq!(legacy_rows(&up_d).await.len(), 5, "{}", state.name);
    assert_eq!(
        ledger(&up_c).await.len(),
        MigrationId::ALL.len(),
        "{}",
        state.name
    );
    for p in [up_c, up_d, fr_c, fr_d] {
        p.close().await;
    }
}

#[tokio::test]
async fn released_0_1_5() {
    upgrade_and_compare(&image("0.1.5", true, true, false)).await;
}

#[tokio::test]
async fn released_0_1_6() {
    upgrade_and_compare(&image("0.1.6", true, true, false)).await;
}

#[tokio::test]
async fn released_0_1_8() {
    upgrade_and_compare(&image("0.1.8", true, false, false)).await;
}

#[tokio::test]
async fn released_0_1_10() {
    upgrade_and_compare(&image("0.1.10", true, false, false)).await;
}

#[tokio::test]
async fn released_0_1_11() {
    upgrade_and_compare(&image("0.1.11", true, false, false)).await;
}

#[tokio::test]
async fn released_0_1_12() {
    upgrade_and_compare(&image("0.1.12", true, false, false)).await;
}

#[tokio::test]
async fn released_0_1_13() {
    upgrade_and_compare(&image("0.1.13", false, false, false)).await;
}

#[tokio::test]
async fn released_0_1_14() {
    upgrade_and_compare(&image("0.1.14", false, false, false)).await;
}

#[tokio::test]
async fn released_pgvector_0_1_11() {
    upgrade_and_compare(&image("pv-0.1.11", true, false, true)).await;
}

#[tokio::test]
async fn released_pgvector_0_1_14() {
    upgrade_and_compare(&image("pv-0.1.14", false, false, true)).await;
}

/// v0.1.0 lacks the three `tables` columns CreateTable writes (D1) and keeps
/// no data ledger.
#[tokio::test]
async fn tag_v0_1_0_catalog_only() {
    upgrade_and_compare(&State {
        name: "v0.1.0",
        catalog_only: true,
        vector: false,
        database_collation: true,
        legacy_delay_key: true,
        default_account: false,
    })
    .await;
}

#[tokio::test]
async fn tag_v0_1_2_catalog_only() {
    upgrade_and_compare(&State {
        name: "v0.1.2",
        catalog_only: true,
        vector: false,
        database_collation: true,
        legacy_delay_key: true,
        default_account: false,
    })
    .await;
}
