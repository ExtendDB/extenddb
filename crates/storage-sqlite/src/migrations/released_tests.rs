// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Every released SQLite catalog, upgraded and compared with a fresh install
//! seeded the same way: catalog objects, per-table objects with ids mapped to
//! names, settings rows, the rows kept, and backup items row by row.

use std::collections::BTreeMap;
use std::path::Path;

use extenddb_storage::MetadataEngine;
use extenddb_storage::migrations::{CatalogStatus, MigrationId, Runner, exit};

use super::SqliteMigrations;
use super::postcheck;
use super::tests::{
    DEV_0_1_14, DEV_0_1_14_BACKUP, LATEST, RELEASE, count, exec, load_dump, next_second,
    plain_pool, reconverge, run, scalar, status, temp_db, version_row,
};

/// Settings rows a released baseline seeded with an equal default in code.
/// v1.0 seeds none of them and keeps the ones it finds, because a seeded
/// value cannot be told from one an operator set.
const DEV_IMAGE_SETTINGS: &[(&str, &str)] = &[
    ("control_plane_delay_seconds", "0.25"),
    ("index_propagation_delay_ms", "10"),
];
/// Before v0.1.7 the index delay key was `gsi_propagation_delay_ms`.
const V0_1_3_SETTINGS: &[(&str, &str)] = &[
    ("control_plane_delay_seconds", "0.25"),
    ("gsi_propagation_delay_ms", "10"),
];

/// A released state: its name, its dump, whether it holds the seeded table,
/// and the settings rows it is accepted to keep beyond a fresh install's.
struct State {
    name: &'static str,
    dump: &'static str,
    seeded: bool,
    settings: &'static [(&'static str, &'static str)],
}

macro_rules! dev_image {
    ($name:literal) => {
        State {
            name: $name,
            dump: include_str!(concat!("../../tests/fixtures/released/", $name, ".sql")),
            seeded: true,
            settings: DEV_IMAGE_SETTINGS,
        }
    };
}

const RELEASED_STATES: &[State] = &[
    State {
        name: "v0.1.3-to-v0.1.6",
        dump: include_str!("../../tests/fixtures/released/v0.1.3-to-v0.1.6.sql"),
        seeded: false,
        settings: V0_1_3_SETTINGS,
    },
    dev_image!("dev-image-0.1.7"),
    dev_image!("dev-image-0.1.8"),
    dev_image!("dev-image-0.1.10"),
    dev_image!("dev-image-0.1.11"),
    dev_image!("dev-image-0.1.12"),
    dev_image!("dev-image-0.1.13"),
    dev_image!("dev-image-0.1.14"),
    dev_image!("dev-image-0.1.14-backup"),
];

/// Rows only a bootstrap writes, absent from the catalog-only fixture.
const BOOTSTRAP_SETTINGS: &[&str] = &["default_account_id", "encryption_key"];

/// The database's objects with per-table ids replaced by `T_<table>` and
/// `I_T_<table>_<index>`.
async fn mapped_definitions(db: &Path) -> BTreeMap<String, Vec<String>> {
    let pool = plain_pool(db).await;
    let defs = postcheck::definitions(&pool).await.expect("definitions");
    let mut ids: Vec<(String, String)> = Vec::new();
    if count(
        db,
        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'tables'",
    )
    .await
        > 0
    {
        let tables: Vec<(String, String)> =
            sqlx::query_as("SELECT table_id, table_name FROM tables")
                .fetch_all(&pool)
                .await
                .expect("tables");
        let indexes: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT i.index_id, t.table_name, i.index_name FROM indexes i \
             JOIN tables t ON t.table_id = i.table_id",
        )
        .fetch_all(&pool)
        .await
        .expect("indexes");
        ids.extend(
            tables
                .into_iter()
                .map(|(id, name)| (id, format!("T_{name}"))),
        );
        ids.extend(
            indexes
                .into_iter()
                .map(|(id, t, i)| (id, format!("I_T_{t}_{i}"))),
        );
    }
    pool.close().await;
    let map = |text: &str| {
        ids.iter().fold(text.to_owned(), |acc, (id, name)| {
            acc.replace(id.as_str(), name)
        })
    };
    defs.into_values()
        .map(|o| {
            let mut definition: Vec<String> = o.definition.iter().map(|p| map(p)).collect();
            definition.sort_unstable();
            (format!("{} {}", o.kind, map(&o.name)), definition)
        })
        .collect()
}

async fn settings(db: &Path) -> BTreeMap<String, String> {
    let pool = plain_pool(db).await;
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT key, value FROM settings")
        .fetch_all(&pool)
        .await
        .expect("settings");
    pool.close().await;
    rows.into_iter().collect()
}

/// Row counts of every table but the ones a migration writes.
async fn row_counts(db: &Path) -> BTreeMap<String, i64> {
    let pool = plain_pool(db).await;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         AND name NOT IN ('settings', 'extenddb_migrations')",
    )
    .fetch_all(&pool)
    .await
    .expect("tables");
    let mut counts = BTreeMap::new();
    for table in tables {
        let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{table}\""))
            .fetch_one(&pool)
            .await
            .expect("count");
        counts.insert(table, n);
    }
    pool.close().await;
    counts
}

/// Every backup item's content, in the order of its rowid (its `id` once
/// migration 1 rebuilt the table).
async fn backup_items(db: &Path) -> Vec<(i64, String, String, Option<String>, String)> {
    let pool = plain_pool(db).await;
    let rows = sqlx::query_as(
        "SELECT rowid, backup_arn, pk, sk, item_data FROM backup_items ORDER BY rowid",
    )
    .fetch_all(&pool)
    .await
    .expect("backup items");
    pool.close().await;
    rows
}

/// A backup whose items have sort keys, in the columns every release wrote.
const BACKUP_WITH_SORT_KEYS: &str = "\
    INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, key_schema, \
        attribute_definitions) VALUES ('arn:with-sort-keys', 'b', 'x', 'cu_orders', '0', \
        '[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"},{\"AttributeName\":\"sk\",\"KeyType\":\"RANGE\"}]', \
        '[{\"AttributeName\":\"pk\",\"AttributeType\":\"S\"},{\"AttributeName\":\"sk\",\"AttributeType\":\"S\"}]'); \
    INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES \
        ('arn:with-sort-keys', 'p0', 's01', '{\"pk\":{\"S\":\"p0\"},\"sk\":{\"S\":\"s01\"}}'), \
        ('arn:with-sort-keys', 'p0', 's02', '{\"pk\":{\"S\":\"p0\"},\"sk\":{\"S\":\"s02\"}}');";

/// A v1.0 catalog seeded as the fixtures were: the bootstrap rows, then
/// CreateTable `cu_orders` with its GSI, LSI and stream, and its TTL index.
async fn fresh_seeded() -> BTreeMap<String, Vec<String>> {
    let db = temp_db("fresh-seeded");
    run(&db).await.expect("fresh install");
    exec(
        &db,
        "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default'); \
         INSERT INTO settings (key, value) VALUES ('default_account_id', '000000000000'); \
         INSERT INTO settings (key, value) VALUES ('encryption_key', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=');",
    )
    .await;
    let engine = crate::SqliteEngine::new(&db.to_string_lossy(), 2, "us-east-1", 400_000)
        .await
        .expect("engine");
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "cu_orders",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "gsk", "AttributeType": "N"},
            {"AttributeName": "lsk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "by_gpk",
            "KeySchema": [
                {"AttributeName": "gpk", "KeyType": "HASH"},
                {"AttributeName": "gsk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "by_lsk",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl("000000000000", input, false)
        .await
        .expect("create table");
    MetadataEngine::create_ttl_index(&engine, "000000000000", "cu_orders", "expires")
        .await
        .expect("ttl index");
    engine.pool.close().await;
    mapped_definitions(&db).await
}

#[tokio::test]
async fn every_released_catalog_upgrades_to_the_fresh_schema() {
    let fresh = fresh_seeded().await;
    assert!(
        fresh.contains_key("index idx_base_key_I_T_cu_orders_by_gpk"),
        "{fresh:#?}"
    );
    for state in RELEASED_STATES {
        let name = state.name;
        let db = temp_db(name);
        load_dump(&db, state.dump).await;
        exec(&db, BACKUP_WITH_SORT_KEYS).await;
        let before = row_counts(&db).await;
        let items_before = backup_items(&db).await;
        let settings_before = settings(&db).await;
        let status_before = status(&db).await;
        assert!(
            matches!(status_before, CatalogStatus::Behind { version: None, .. }),
            "{name}: {status_before:?}"
        );
        assert_eq!(status_before.exit_code(), exit::PENDING);

        let report = run(&db).await.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!((report.from, report.to), (None, Some(LATEST)), "{name}");
        assert_eq!(
            status(&db).await,
            CatalogStatus::Current { version: LATEST },
            "{name}"
        );
        assert!(run(&db).await.expect("again").applied.is_empty(), "{name}");

        // Rows: every table keeps its count, and every backup item its
        // content and its order, with its old rowid as its id.
        let after = row_counts(&db).await;
        for (table, n) in &before {
            assert_eq!(after.get(table), Some(n), "{name}: rows of {table}");
        }
        assert_eq!(
            backup_items(&db).await,
            items_before,
            "{name}: backup items"
        );

        // Objects, per-table ones mapped to names.
        let upgraded = mapped_definitions(&db).await;
        let expected: BTreeMap<&String, &Vec<String>> = fresh
            .iter()
            .filter(|(object, _)| state.seeded || !object.contains("T_cu_orders"))
            .collect();
        let actual: BTreeMap<&String, &Vec<String>> = upgraded.iter().collect();
        assert_eq!(
            actual, expected,
            "{name}: objects differ from a fresh install"
        );

        // Settings: a fresh install's version row, the bootstrap rows as the
        // release wrote them, and the state's accepted rows with its values.
        let mut expected_settings: BTreeMap<String, String> =
            [("catalog_version".to_owned(), LATEST.to_string())].into();
        if state.seeded {
            for key in BOOTSTRAP_SETTINGS {
                let value = settings_before
                    .get(*key)
                    .unwrap_or_else(|| panic!("{name}: {key}"));
                expected_settings.insert((*key).to_owned(), value.clone());
            }
        }
        for (key, value) in state.settings {
            expected_settings.insert((*key).to_owned(), (*value).to_owned());
        }
        // Migration 5 writes a random catalog id on every catalog.
        let mut actual_settings = settings(&db).await;
        let catalog_id = actual_settings
            .remove("catalog_id")
            .unwrap_or_else(|| panic!("{name}: no catalog_id"));
        assert!(
            uuid::Uuid::parse_str(&catalog_id).is_ok(),
            "{name}: {catalog_id}"
        );
        assert_eq!(actual_settings, expected_settings, "{name}: settings");

        // No post-check note: no released state has a pre-release shape.
        let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 1").await;
        let rebuilt = format!(
            "rebuilt backup_items with an id column, copying {} row(s); the old table's \
             pages stay free inside the database file until a VACUUM",
            items_before.len()
        );
        assert_eq!(notes, Some(rebuilt), "{name}");

        // A reconverge reruns migration 1 and leaves every item as it is.
        exec(
            &db,
            "INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'x')",
        )
        .await;
        reconverge(&db)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            backup_items(&db).await,
            items_before,
            "{name}: after a reconverge"
        );
    }
}

/// A release before 1.0 that starts on a v1.0 catalog rewrites
/// `catalog_version` (SQLite dev builds) or the marker (`migrate --yes`).
/// v1.0 then reports it, refuses to migrate, and `--reconverge` repairs it,
/// leaving the ledger rows as the first application wrote them.
#[tokio::test]
async fn a_catalog_an_older_release_wrote_to_is_reported_and_reconverged() {
    let db = temp_db("older-release");
    load_dump(&db, DEV_0_1_14).await;
    run(&db).await.expect("upgrade");
    let ledger = |db: &Path| {
        let db = db.to_path_buf();
        async move {
            let pool = plain_pool(&db).await;
            let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
                "SELECT id, checksum, notes, applied_at, applied_by FROM extenddb_migrations \
                 ORDER BY id",
            )
            .fetch_all(&pool)
            .await
            .expect("ledger");
            pool.close().await;
            rows
        }
    };
    let rows = ledger(&db).await;
    for trace in [
        "UPDATE settings SET value = '0.0.3' WHERE key = 'catalog_version'",
        "INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'applied')",
    ] {
        exec(&db, trace).await;
        let found = status(&db).await;
        assert_eq!(found.label(), "WrittenByOlderRelease", "{trace}");
        assert_eq!(found.exit_code(), exit::ATTENTION);
        let err = run(&db).await.expect_err("migrate refuses");
        assert_eq!(err.exit_code(), exit::ATTENTION);

        next_second().await;
        let report = reconverge(&db).await.expect("reconverge");
        assert_eq!(report.applied.len(), MigrationId::ALL.len());
        assert_eq!(
            status(&db).await,
            CatalogStatus::Current { version: LATEST },
            "{trace}"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM settings WHERE key = 'resource_tag_migration'"
            )
            .await,
            0
        );
        assert_eq!(ledger(&db).await, rows, "{trace}: the ledger rows are kept");
    }
}

/// Running migration 1 again on a catalog at a later migration keeps what a
/// later migration added to the tables migration 1 creates: a column of
/// `tables` and one of `backup_items`, with their values.
#[tokio::test]
async fn a_reconverge_keeps_what_a_later_migration_added() {
    let db = temp_db("later-columns");
    load_dump(&db, DEV_0_1_14_BACKUP).await;
    run(&db).await.expect("upgrade");
    exec(
        &db,
        "ALTER TABLE tables ADD COLUMN later_note TEXT; \
         UPDATE tables SET later_note = 'written by a later migration'; \
         ALTER TABLE backup_items ADD COLUMN later_note TEXT; \
         UPDATE backup_items SET later_note = 'item ' || id;",
    )
    .await;
    let dump = |db: &Path, table: &'static str| {
        let db = db.to_path_buf();
        async move {
            let pool = plain_pool(&db).await;
            let columns: Vec<String> =
                sqlx::query_scalar(&format!("SELECT name FROM pragma_table_info('{table}')"))
                    .fetch_all(&pool)
                    .await
                    .expect("columns");
            let rows: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {} FROM {table} ORDER BY rowid",
                columns
                    .iter()
                    .map(|c| format!("quote({c})"))
                    .collect::<Vec<_>>()
                    .join(" || '|' || ")
            ))
            .fetch_all(&pool)
            .await
            .expect("rows");
            pool.close().await;
            (columns, rows)
        }
    };
    let tables = dump(&db, "tables").await;
    let items = dump(&db, "backup_items").await;
    assert!(tables.0.contains(&"later_note".to_owned()));
    assert_eq!(items.1.len(), 10, "the backup's items");
    exec(
        &db,
        "INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'x')",
    )
    .await;
    reconverge(&db).await.expect("reconverge");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert_eq!(dump(&db, "tables").await, tables);
    assert_eq!(dump(&db, "backup_items").await, items);
}

/// `migrate_through` stops at an intermediate id; the rest applies later.
#[tokio::test]
async fn a_released_catalog_can_stop_at_an_intermediate_migration() {
    let db = temp_db("through");
    load_dump(&db, DEV_0_1_14).await;
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let report = Runner::new(&migrations, RELEASE)
        .migrate_through(MigrationId::ResourceTagConditionKeys)
        .await
        .expect("through 2");
    migrations.close().await;
    assert_eq!(report.to, Some(2));
    let CatalogStatus::Behind { version, pending } = status(&db).await else {
        panic!("expected Behind");
    };
    assert_eq!(version, Some(2));
    assert_eq!(
        pending.iter().map(|p| p.id).collect::<Vec<_>>(),
        (3..=LATEST).collect::<Vec<_>>()
    );
    run(&db).await.expect("the rest");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
}

/// The statuses a ledger alone decides, on a SQLite catalog: a changed
/// checksum, ids above this binary's, and `forget`.
#[tokio::test]
async fn changed_ahead_and_forgotten_ledgers() {
    let db = temp_db("ledger-states");
    run(&db).await.expect("fresh install");
    let future = LATEST + 1;

    // An id a newer binary recorded as additive: served, and migrate leaves it.
    exec(
        &db,
        &format!(
            "INSERT INTO extenddb_migrations (id, name, kind, checksum, outcome, applied_by) \
             VALUES ({future}, 'future', 'additive', 'x', 'applied', '1.1.0')"
        ),
    )
    .await;
    let found = status(&db).await;
    assert_eq!(found.label(), "AheadCompatible", "{found:?}");
    assert_eq!(found.exit_code(), exit::OK);
    assert!(run(&db).await.expect("migrate").applied.is_empty());

    // A breaking one: refused.
    exec(
        &db,
        &format!("UPDATE extenddb_migrations SET kind = 'breaking' WHERE id = {future}"),
    )
    .await;
    assert_eq!(status(&db).await.exit_code(), exit::AHEAD_INCOMPATIBLE);
    assert_eq!(
        run(&db).await.expect_err("refused").exit_code(),
        exit::AHEAD_INCOMPATIBLE
    );
    exec(
        &db,
        &format!("DELETE FROM extenddb_migrations WHERE id = {future}"),
    )
    .await;

    // A body whose recorded checksum differs from this binary's.
    let recorded = scalar(&db, "SELECT checksum FROM extenddb_migrations WHERE id = 1")
        .await
        .expect("row");
    exec(
        &db,
        "UPDATE extenddb_migrations SET checksum = 'edited' WHERE id = 1",
    )
    .await;
    let found = status(&db).await;
    let CatalogStatus::Changed {
        id,
        recorded: edited,
        ..
    } = &found
    else {
        panic!("{found:?}");
    };
    assert_eq!((*id, edited.as_str()), (1, "edited"));
    assert_eq!(found.exit_code(), exit::ATTENTION);
    exec(
        &db,
        &format!("UPDATE extenddb_migrations SET checksum = '{recorded}' WHERE id = 1"),
    )
    .await;

    // An unreleased id, migration 4, can be forgotten and runs again.
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let runner = Runner::new(&migrations, RELEASE);
    let four = MigrationId::IndexTableBaseKey.number();
    runner.forget(four).await.expect("forget 4");
    assert!(matches!(
        runner.status().await.expect("status"),
        CatalogStatus::Behind {
            version: Some(3),
            ..
        }
    ));
    let report = runner.migrate().await.expect("reapply");
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].id, four);
    migrations.close().await;
    assert_eq!(version_row(&db).await, Some(LATEST.to_string()));
}
