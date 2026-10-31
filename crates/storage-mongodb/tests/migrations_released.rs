// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Released MongoDB catalogs upgraded by the runner: the fixtures of the
//! 0.1.12, 0.1.13 and 0.1.14 images (0.1.12 stands for v0.1.4 to v0.1.11,
//! whose bootstrapper is the same), and `dotted-ttl`, a 0.1.12 catalog with
//! the TTL index releases before v0.1.12 created for a dotted attribute.
//! Each is compared after the upgrade with the definitions migration 1
//! creates, and its per-table objects with what they were before, and the
//! code bodies' output with committed golden files.

mod migrations_support;

use std::collections::BTreeMap;

use bson::{Document, doc};
use extenddb_storage::migrations::{CatalogStatus, MigrationBackend};
use extenddb_storage_mongodb::migrations::{Definitions, MongoMigrations, describe_store};
use migrations_support::{Deployment, fixture, server};
use serde_json::Value;

const CATALOG_EXPECTED: &str = include_str!("../migrations/catalog/0001_baseline.expected.json");
const DATA_EXPECTED: &str = include_str!("../migrations/data/0001_baseline.expected.json");
const STATES: [&str; 4] = ["0.1.12", "0.1.13", "0.1.14", "dotted-ttl"];

/// The per-table collections and their indexes, named after the catalog's
/// table and index names instead of their ids: `T_<table>` and
/// `I_<table>_<index>`.
async fn per_table(d: &Deployment) -> BTreeMap<String, Vec<(String, String)>> {
    let mut names = BTreeMap::new();
    for t in Deployment::documents(&d.catalog(), "tables").await {
        let table = t
            .get_document("_id")
            .unwrap()
            .get_str("table_name")
            .unwrap()
            .to_owned();
        names.insert(
            t.get_str("table_id").unwrap().to_owned(),
            format!("T_{table}"),
        );
    }
    for i in Deployment::documents(&d.catalog(), "indexes").await {
        let id = i.get_document("_id").unwrap();
        let table = names
            .get(id.get_str("table_id").unwrap())
            .cloned()
            .unwrap_or_default();
        names.insert(
            i.get_str("index_id").unwrap().to_owned(),
            format!("I_{}_{}", &table[2..], id.get_str("index_name").unwrap()),
        );
    }
    let mut objects = BTreeMap::new();
    for collection in Deployment::collections(&d.data()).await {
        let Some(id) = collection.strip_prefix("_ddb_") else {
            continue;
        };
        let name = names
            .get(id)
            .cloned()
            .unwrap_or_else(|| format!("unknown {id}"));
        let cursor = d
            .data()
            .collection::<Document>(&collection)
            .list_indexes()
            .await
            .unwrap();
        let mut indexes = index_texts(cursor).await;
        indexes.sort();
        objects.insert(name, indexes);
    }
    objects
}

/// The index definitions of a cursor, as (name, key and options).
async fn index_texts(cursor: mongodb::Cursor<mongodb::IndexModel>) -> Vec<(String, String)> {
    use futures::TryStreamExt;
    let models: Vec<mongodb::IndexModel> = cursor.try_collect().await.unwrap();
    models
        .iter()
        .map(|m| {
            let name = m
                .options
                .as_ref()
                .and_then(|o| o.name.clone())
                .unwrap_or_default();
            let options = m.options.as_ref().map(|o| {
                format!(
                    "unique={:?} sparse={:?} ttl={:?} partial={:?}",
                    o.unique, o.sparse, o.expire_after, o.partial_filter_expression
                )
            });
            (name, format!("{} {}", m.keys, options.unwrap_or_default()))
        })
        .collect()
}

async fn definitions(d: &Deployment) -> (Definitions, Definitions) {
    let migrations = MongoMigrations::open(&d.target, None).await.expect("open");
    let catalog = describe_store(migrations.stores(), &d.catalog())
        .await
        .expect("catalog");
    let data = describe_store(migrations.stores(), &d.data())
        .await
        .expect("data");
    migrations.close().await;
    (catalog, data)
}

async fn setting_keys(d: &Deployment) -> Vec<String> {
    Deployment::documents(&d.catalog(), "settings")
        .await
        .iter()
        .map(|s| s.get_str("_id").unwrap().to_owned())
        .collect()
}

/// Write `value` to `tests/fixtures/golden/<name>.txt` when
/// `EXTENDDB_BLESS_EXPECTED` is `1`, and compare it with the committed file.
fn golden(name: &str, value: &Value) {
    let path = format!(
        "{}/tests/fixtures/golden/{name}.txt",
        env!("CARGO_MANIFEST_DIR")
    );
    if std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1") {
        std::fs::write(&path, serde_json::to_string_pretty(value).unwrap() + "\n").unwrap();
    }
    let committed: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")),
    )
    .unwrap();
    assert_eq!(&committed, value, "{path}");
}

#[tokio::test(flavor = "multi_thread")]
async fn every_released_catalog_upgrades_to_the_definitions_of_a_fresh_install() {
    let Some(uri) = server() else { return };
    let expected_catalog: Definitions = serde_json::from_str(CATALOG_EXPECTED).unwrap();
    let expected_data: Definitions = serde_json::from_str(DATA_EXPECTED).unwrap();
    let fresh = Deployment::new(&uri, "freshkeys").await;
    fresh.create().await;
    let mut fresh_keys = setting_keys(&fresh).await;
    fresh.drop_databases().await;
    // `init` writes the encryption key and the data connection rows beside
    // the runner, so a fresh install made by `init` has these keys as well.
    fresh_keys.extend(
        [
            "data_connection_string",
            "data_database_name",
            "encryption_key",
        ]
        .map(str::to_owned),
    );
    fresh_keys.sort();

    for state in STATES {
        let d = Deployment::new(&uri, "released").await;
        fixture::load(&d, &fixture::read(state)).await;
        let before = per_table(&d).await;
        let CatalogStatus::Behind { version, pending } = d.status().await else {
            panic!("{state} is not behind");
        };
        assert_eq!(version, None, "{state} has no ledger");
        assert_eq!(pending.len(), 5, "{state}");
        let report = d.migrate().await.expect("migrate");
        assert_eq!((report.from, report.to), (None, Some(4)), "{state}");
        assert_eq!(
            d.status().await,
            CatalogStatus::Current { version: 4 },
            "{state}"
        );

        let (catalog, data) = definitions(&d).await;
        for (name, want) in &expected_catalog {
            assert_eq!(catalog.get(name), Some(want), "{state}: {name}");
        }
        for (name, want) in &expected_data {
            assert_eq!(data.get(name), Some(want), "{state}: {name}");
        }
        // Collections migration 1 does not create: released catalogs and a
        // fresh install have the same set. `counters` is created on first
        // use by streams in both.
        let extra_catalog: Vec<&String> = catalog
            .keys()
            .filter(|n| !expected_catalog.contains_key(*n))
            .collect();
        assert!(extra_catalog.is_empty(), "{state}: {extra_catalog:?}");
        let extra_data: Vec<&String> = data
            .keys()
            .filter(|n| !expected_data.contains_key(*n))
            .collect();
        assert_eq!(extra_data, ["counters"], "{state}");

        // No migration changes a per-table object on MongoDB.
        assert_eq!(per_table(&d).await, before, "{state}");
        if state == "dotted-ttl" {
            // The accepted difference of this state: the legacy index stays
            // until TTL is disabled on the table (inventory section 3.2,
            // MongoDB item 4), and v1.0 creates none for a dotted name.
            let dotted = &before["T_cu_dotted"];
            assert!(
                dotted.iter().any(|(n, _)| n == "idx_ttl_exp.at"),
                "{dotted:?}"
            );
        }
        let mut keys = setting_keys(&d).await;
        keys.sort();
        assert_eq!(keys, fresh_keys, "{state}");
        let history = Deployment::documents(&d.catalog(), "schema_history").await;
        assert_eq!(history.len(), 1, "{state}");
        d.drop_databases().await;
    }
}

/// Migration 2's body on the oldest released catalog: every stored policy
/// after the upgrade, which the committed golden file pins.
#[tokio::test(flavor = "multi_thread")]
async fn migration_2_rewrites_the_released_policies_as_the_golden_file_records() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "golden2").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    // A policy in the account a released `init` wrote without
    // `account_id`, which the store's own listing skips.
    let default = d
        .catalog()
        .collection::<Document>("accounts")
        .find_one(doc! { "account_name": "default" })
        .await
        .unwrap()
        .unwrap();
    let default_id = default.get_str("_id").unwrap().to_owned();
    d.catalog()
        .collection::<Document>("iam_users")
        .insert_one(doc! {
            "account_id": &default_id, "user_name": "ops",
            "user_arn": format!("arn:aws:iam::{default_id}:user/ops"),
            "created_at": bson::DateTime::from_millis(1_793_235_168_000),
        })
        .await
        .unwrap();
    d.catalog()
        .collection::<Document>("iam_policies")
        .insert_one(doc! {
            "account_id": &default_id, "principal_type": "user", "principal_name": "ops",
            "policy_name": "deny-prod-default",
            "policy_document": { "Version": "2012-10-17", "Statement": [{
                "Effect": "Deny", "Action": "dynamodb:*", "Resource": "*",
                "Condition": { "StringEquals": { "dynamodb:ResourceTag/env": "prod" } },
            }] },
            "created_at": bson::DateTime::from_millis(1_793_235_168_000),
        })
        .await
        .unwrap();
    d.migrate().await.expect("migrate");
    let policies: Vec<Value> = Deployment::documents(&d.catalog(), "iam_policies")
        .await
        .iter()
        .map(|p| {
            let account = if p.get_str("account_id") == Ok(default_id.as_str()) {
                "<default account>"
            } else {
                p.get_str("account_id").unwrap()
            };
            serde_json::json!({
                "account": account,
                "principal": format!("{} {}", p.get_str("principal_type").unwrap(), p.get_str("principal_name").unwrap()),
                "policy": p.get_str("policy_name").unwrap(),
                "document": serde_json::to_value(p.get_document("policy_document").unwrap()).unwrap(),
            })
        })
        .collect();
    let rows = d.catalog_ledger().await;
    let notes: Vec<String> = rows[1]
        .notes
        .lines()
        .map(|l| l.replace(&default_id, "<default account>"))
        .collect();
    golden(
        "0002_resource_tag_condition_keys",
        &serde_json::json!({ "policies": policies, "notes": notes }),
    );
    d.drop_databases().await;
}

/// Migration 1's bodies on the oldest released catalog: both databases'
/// definitions, the settings keys and the legacy ledger, which the
/// committed golden file pins.
#[tokio::test(flavor = "multi_thread")]
async fn migration_1_upgrades_the_oldest_release_as_the_golden_file_records() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "golden1").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    d.migrate_through(extenddb_storage::migrations::MigrationId::Baseline)
        .await
        .expect("migration 1");
    let (catalog, data) = definitions(&d).await;
    let history: Vec<String> = Deployment::documents(&d.catalog(), "schema_history")
        .await
        .iter()
        .map(|h| h.get_str("_id").unwrap().to_owned())
        .collect();
    golden(
        "0001_baseline",
        &serde_json::json!({
            "catalog": catalog,
            "data": data,
            "settings": setting_keys(&d).await,
            "schema_history": history,
        }),
    );
    d.drop_databases().await;
}
