// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Released MongoDB catalogs upgraded by the runner: the fixtures of the
//! 0.1.12, 0.1.13 and 0.1.14 images (0.1.12 stands for v0.1.4 to v0.1.11,
//! whose bootstrapper is the same), and `dotted-ttl`, a 0.1.12 catalog with
//! the TTL index releases before v0.1.12 created for a dotted attribute.
//! Each is compared after the upgrade with the definitions of a fresh
//! install, and its per-table objects with what they were before, and the
//! code bodies' output with committed golden files.

mod migrations_support;

use std::collections::BTreeMap;

use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use extenddb_storage::migrations::{CatalogStatus, MigrationBackend, MigrationId, exit};
use extenddb_storage_mongodb::migrations::{Definitions, MongoMigrations, describe_store};
use migrations_support::{ACCESS_KEY_ID_INDEX, Deployment, fixture, server};
use serde_json::Value;

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
    let expected_catalog = migrations_support::expected_catalog();
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
        assert_eq!(pending.len(), 13, "{state}");
        let report = d.migrate().await.expect("migrate");
        assert_eq!(
            (report.from, report.to),
            (None, Some(MigrationId::LATEST.number())),
            "{state}"
        );
        assert_eq!(
            d.status().await,
            CatalogStatus::Current {
                version: MigrationId::LATEST.number()
            },
            "{state}"
        );

        let (catalog, data) = definitions(&d).await;
        for (name, want) in &expected_catalog {
            assert_eq!(catalog.get(name), Some(want), "{state}: {name}");
        }
        for dropped in ["continuous_backups", "metrics"] {
            assert!(!catalog.contains_key(dropped), "{state}: {dropped}");
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

/// The indexes of the data database's `stream_shards`, by name, with their
/// keys.
async fn stream_shards_indexes(d: &Deployment) -> BTreeMap<String, Document> {
    use futures::TryStreamExt;
    let models: Vec<mongodb::IndexModel> = d
        .data()
        .collection::<Document>("stream_shards")
        .list_indexes()
        .await
        .expect("list the stream_shards indexes")
        .try_collect()
        .await
        .expect("read the stream_shards indexes");
    models
        .into_iter()
        .map(|m| (m.options.and_then(|o| o.name).unwrap_or_default(), m.keys))
        .collect()
}

/// The `{table_id, shard_id}` index on `stream_shards`, which no release
/// before 1.0 created: every released data database has only the unique
/// `shard_id` index, and the `migrate --yes` that adopts it adds the index
/// with migration 1's data step. A second run applies nothing.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_adds_the_stream_shards_table_index_to_every_released_data_database() {
    let Some(uri) = server() else { return };
    for state in STATES {
        let d = Deployment::new(&uri, "shardindex").await;
        fixture::load(&d, &fixture::read(state)).await;
        let before = stream_shards_indexes(&d).await;
        assert_eq!(
            before.keys().map(String::as_str).collect::<Vec<_>>(),
            ["_id_", "shard_id_1"],
            "{state}"
        );
        d.migrate().await.expect("migrate");
        let after = stream_shards_indexes(&d).await;
        assert_eq!(
            after.get("table_id_1_shard_id_1"),
            Some(&doc! { "table_id": 1, "shard_id": 1 }),
            "{state}: {after:?}"
        );
        let again = d.migrate().await.expect("second migrate");
        assert!(again.applied.is_empty(), "{state}: {again:?}");
        assert_eq!(stream_shards_indexes(&d).await, after, "{state}");
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

/// Migration 6's body on the oldest released catalog holding three restore
/// registrations: a finished restore (ACTIVE), one handed to the index
/// backfill worker at the stop (CREATING with `restore_backfill_pending`),
/// and one the stop interrupted (CREATING). Each gets its job record, the
/// first two DONE and completed at the table's creation, the third RUNNING
/// with the epoch heartbeat and generation 0, which the committed golden
/// file pins with the ledger row's notes. A reconverge runs the body again
/// and changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn migration_6_backfills_the_released_restore_registrations_as_the_golden_file_records() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "golden6").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    let tables = d.catalog().collection::<Document>("tables");
    let created = bson::DateTime::from_millis(1_700_000_000_000);
    for (name, status, handed) in [
        ("golden6_done", "ACTIVE", false),
        ("golden6_handed", "CREATING", true),
        ("golden6_interrupted", "CREATING", false),
    ] {
        let mut table = doc! {
            "_id": { "account_id": "123456789012", "table_name": name },
            "table_id": format!("golden6-{name}"),
            "table_status": status,
            "creation_date_time": created,
            "restore_source_backup_arn":
                "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01700000000000-0000000a",
            "restore_operation_id": "old-operation",
            "restore_heartbeat_at": created,
        };
        if handed {
            table.insert("restore_backfill_pending", true);
        }
        tables.insert_one(table).await.unwrap();
    }
    d.migrate().await.expect("migrate");
    let registrations = || async {
        let mut found: Vec<Value> = Vec::new();
        for t in Deployment::documents(&d.catalog(), "tables").await {
            if !t.contains_key("restore_source_backup_arn") {
                continue;
            }
            let job = t.get_document("restore_job").expect("a job record");
            let earlier: Vec<&str> = ["restore_operation_id", "restore_heartbeat_at"]
                .into_iter()
                .filter(|f| t.contains_key(*f))
                .collect();
            found.push(serde_json::json!({
                "table": t.get_document("_id").unwrap().get_str("table_name").unwrap(),
                "table_status": t.get_str("table_status").unwrap(),
                "status": job.get_str("status").unwrap(),
                "kind": job.get_str("kind").unwrap(),
                "owner_generation": job.get_i64("owner_generation").unwrap(),
                "heartbeat_at": job.get_datetime("heartbeat_at").unwrap().timestamp_millis(),
                "completed_at_creation": job
                    .get_datetime("completed_at")
                    .ok()
                    .map(|at| Some(at) == t.get_datetime("creation_date_time").ok()),
                "earlier_fields": earlier,
            }));
        }
        found.sort_by_key(|v| v["table"].as_str().unwrap().to_owned());
        found
    };
    let found = registrations().await;
    let catalog_id = d.setting("catalog_id").await.expect("a catalog id");
    assert!(uuid::Uuid::parse_str(&catalog_id).is_ok(), "{catalog_id}");
    let rows = d.catalog_ledger().await;
    let row = rows
        .iter()
        .find(|r| r.id == MigrationId::BackupCatalogIndex.number())
        .expect("migration 6's row");
    let notes: Vec<&str> = row.notes.lines().collect();
    golden(
        "0006_backup_catalog_index",
        &serde_json::json!({ "registrations": found, "notes": notes }),
    );

    let reconverged = d.reconverge().await.expect("reconverge");
    assert!(
        reconverged
            .applied
            .iter()
            .any(|step| step.id == row.id && step.notes.is_empty()),
        "{reconverged:?}"
    );
    assert_eq!(registrations().await, found);
    assert_eq!(
        d.setting("catalog_id").await,
        Some(catalog_id),
        "the catalog id is kept"
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

/// From 1.0 on a `dynamodb:ResourceTag` key written through the API has no
/// effect, as on the service. Migration 2 is not convergent, so a reconverge
/// leaves the policies written after it with the text they were written
/// with, one that names both forms of a key, which the rewrite refuses,
/// included, and names migration 2 as skipped.
#[tokio::test(flavor = "multi_thread")]
async fn a_reconverge_leaves_policies_written_after_migration_2_as_they_are() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "reconverge2").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    d.migrate().await.expect("migrate");
    let account = Deployment::documents(&d.catalog(), "accounts").await[0]
        .get_str("_id")
        .unwrap()
        .to_owned();
    let tagged = doc! { "Version": "2012-10-17", "Statement": [{
        "Effect": "Deny", "Action": "dynamodb:*", "Resource": "*",
        "Condition": { "StringEquals": { "dynamodb:ResourceTag/env": "prod" } },
    }] };
    let both_forms = doc! { "Version": "2012-10-17", "Statement": [{
        "Effect": "Deny", "Action": "dynamodb:*", "Resource": "*",
        "Condition": { "StringEquals": {
            "dynamodb:ResourceTag/env": "prod", "aws:ResourceTag/env": "dev",
        } },
    }] };
    d.catalog()
        .collection::<Document>("iam_users")
        .insert_one(doc! {
            "account_id": &account, "user_name": "after",
            "user_arn": format!("arn:aws:iam::{account}:user/after"),
            "created_at": bson::DateTime::from_millis(1_793_235_168_000),
        })
        .await
        .unwrap();
    for (name, document) in [("tagged-after", &tagged), ("both-forms-after", &both_forms)] {
        d.catalog()
            .collection::<Document>("iam_policies")
            .insert_one(doc! {
                "account_id": &account, "principal_type": "user", "principal_name": "after",
                "policy_name": name, "policy_document": document,
                "created_at": bson::DateTime::from_millis(1_793_235_168_000),
            })
            .await
            .unwrap();
    }
    let documents = |policies: Vec<Document>| -> Vec<String> {
        policies
            .iter()
            .map(|p| {
                format!(
                    "{} {}",
                    p.get_str("policy_name").unwrap(),
                    p.get_document("policy_document").unwrap()
                )
            })
            .collect()
    };
    let before = documents(Deployment::documents(&d.catalog(), "iam_policies").await);
    assert_eq!(
        before
            .iter()
            .filter(|p| p.contains("dynamodb:ResourceTag/env"))
            .count(),
        2,
        "{before:?}"
    );
    // What a release before 1.0 leaves when it writes to a migrated catalog.
    d.catalog()
        .collection::<Document>("settings")
        .insert_one(doc! { "_id": "resource_tag_migration", "value": "applied" })
        .await
        .unwrap();
    assert_eq!(d.status().await.label(), "WrittenByOlderRelease");

    let report = d.reconverge().await.expect("reconverge");
    assert!(report.applied.iter().all(|step| step.id != 2), "{report:?}");
    assert!(
        report
            .notes
            .iter()
            .any(|n| n.starts_with("Skipped 2 resource_tag_condition_keys:")),
        "{report:?}"
    );
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    assert_eq!(
        documents(Deployment::documents(&d.catalog(), "iam_policies").await),
        before
    );
    d.drop_databases().await;
}

/// The access key ID of AWS's documented example pair.
const EXAMPLE_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";

/// The released fixture's one access key: its `_id`, ID, account and
/// creation time.
async fn fixture_key(d: &Deployment) -> (Bson, String, String, i64) {
    let key = Deployment::documents(&d.catalog(), "access_keys")
        .await
        .pop()
        .expect("the fixture's access key");
    (
        key.get("_id").cloned().unwrap(),
        key.get_str("access_key_id").unwrap().to_owned(),
        key.get_str("account_id").unwrap().to_owned(),
        key.get_datetime("created_at").unwrap().timestamp_millis(),
    )
}

/// An access key document as a release before 1.0 stored it, with an `_id`
/// and a creation time the test chooses, and a secret that tells the copies
/// apart.
fn stored_key(n: u8, access_key_id: &str, account: &str, user: &str, created_ms: i64) -> Document {
    doc! {
        "_id": oid(n),
        "access_key_id": access_key_id,
        "account_id": account,
        "user_name": user,
        "secret_key_encrypted": Binary { subtype: BinarySubtype::Generic, bytes: vec![n; 60] },
        "is_active": true,
        "created_at": bson::DateTime::from_millis(created_ms),
    }
}

/// The `_id` of the document the server's credential lookup reads for
/// `access_key_id`, by the lookup's own query.
async fn looked_up(d: &Deployment, access_key_id: &str) -> Bson {
    d.catalog()
        .collection::<Document>("access_keys")
        .find_one(doc! { "access_key_id": access_key_id })
        .await
        .unwrap()
        .expect("a key")
        .get("_id")
        .cloned()
        .unwrap()
}

async fn access_key_indexes(d: &Deployment) -> Vec<String> {
    d.catalog()
        .collection::<Document>("access_keys")
        .list_index_names()
        .await
        .unwrap()
}

/// Migration 12's body on the oldest released catalog, after the fixture's
/// user imported its access key ID twice more and the user `ci` imported
/// the example pair three times: a release before 1.0 stored a document for
/// each import. Each ID keeps the copy a credential lookup read before the
/// upgrade: the first in natural order, which for the fixture's key is
/// neither the oldest copy nor the one with the lowest `_id`. The unique
/// index then refuses another copy. The committed golden file pins the keys
/// and the notes.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_keeps_the_copy_each_lookup_read_as_the_golden_file_records() {
    use futures::TryStreamExt;
    use mongodb::error::{ErrorKind, WriteFailure};

    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "golden12").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    let (fixture_doc, fixture_id, account, created) = fixture_key(&d).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    keys.insert_many([
        stored_key(1, &fixture_id, &account, "cu-tester", created - 60_000),
        stored_key(2, &fixture_id, &account, "cu-tester", created + 60_000),
        stored_key(11, EXAMPLE_KEY_ID, &account, "ci", created + 120_000),
        stored_key(12, EXAMPLE_KEY_ID, &account, "ci", created + 180_000),
        stored_key(13, EXAMPLE_KEY_ID, &account, "ci", created + 240_000),
    ])
    .await
    .unwrap();
    let ids = [fixture_id.as_str(), EXAMPLE_KEY_ID];
    let mut read_before = Vec::new();
    for id in ids {
        read_before.push(looked_up(&d, id).await);
    }
    assert_eq!(read_before[0], fixture_doc);

    // The profiler records the body's requests on the catalog: with only
    // migration 1's indexes the lookup is a forward scan, so the body reads
    // record IDs and sends no lookup query of its own.
    let catalog = d.catalog();
    catalog
        .run_command(doc! { "create": "system.profile", "capped": true, "size": 16 << 20 })
        .await
        .unwrap();
    catalog.run_command(doc! { "profile": 2 }).await.unwrap();
    d.migrate().await.expect("migrate");
    catalog.run_command(doc! { "profile": 0 }).await.unwrap();
    let profiled = catalog.collection::<Document>("system.profile");
    let count = |filter: Document| {
        let profiled = profiled.clone();
        async move { profiled.count_documents(filter).await.unwrap() }
    };
    assert_eq!(
        count(doc! {
            "command.find": "access_keys",
            "command.filter._id": { "$exists": false },
            "command.filter.access_key_id": { "$exists": true },
        })
        .await,
        0,
        "the body sent a lookup query of its own"
    );
    assert_eq!(
        count(doc! { "command.find": "access_keys", "command.showRecordId": true }).await,
        ids.len() as u64
    );
    assert_eq!(
        count(doc! { "command.explain.find": "access_keys" }).await,
        ids.len() as u64
    );
    catalog
        .collection::<Document>("system.profile")
        .drop()
        .await
        .unwrap();
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    for (id, kept) in ids.iter().zip(&read_before) {
        let copies: Vec<Document> = keys
            .find(doc! { "access_key_id": *id })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(copies.len(), 1, "{id}: {copies:?}");
        assert_eq!(copies[0].get("_id"), Some(kept), "{id}");
        assert_eq!(&looked_up(&d, id).await, kept, "{id}");
    }
    assert!(
        access_key_indexes(&d)
            .await
            .contains(&ACCESS_KEY_ID_INDEX.to_owned())
    );
    let refused = keys
        .insert_one(stored_key(3, &fixture_id, &account, "cu-tester", created))
        .await
        .expect_err("the index refuses another copy");
    assert!(
        matches!(*refused.kind, ErrorKind::Write(WriteFailure::WriteError(ref e)) if e.code == 11000),
        "{refused}"
    );

    let stored: Vec<Value> = Deployment::documents(&d.catalog(), "access_keys")
        .await
        .iter()
        .map(|k| {
            serde_json::json!({
                "_id": k.get_object_id("_id").unwrap().to_hex(),
                "access_key_id": k.get_str("access_key_id").unwrap(),
                "account_id": k.get_str("account_id").unwrap(),
                "user_name": k.get_str("user_name").unwrap(),
                "created_at": k.get_datetime("created_at").unwrap().try_to_rfc3339_string().unwrap(),
            })
        })
        .collect();
    let notes: Vec<String> = d
        .catalog_ledger()
        .await
        .iter()
        .find(|row| row.id == MigrationId::UniqueAccessKeyIds.number())
        .expect("migration 12's row")
        .notes
        .lines()
        .map(str::to_owned)
        .collect();
    golden(
        "0012_unique_access_key_ids",
        &serde_json::json!({ "access_keys": stored, "notes": notes }),
    );
    d.drop_databases().await;
}

/// Migration 12's `detect` refuses, before any migration writes, an ID that
/// another user of the account holds, an ID that users of one name in two
/// accounts hold, two documents without an ID, and a document whose ID is
/// an array: the status reads NeedsAttention, `migrate --yes` exits 5 and
/// names every copy, and every document is kept, the user `ci`'s two copies
/// of the example pair included, which the body itself would resolve. Once
/// the documents the refusal names are deleted, `migrate --yes` applies
/// every migration.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_refuses_copies_it_cannot_resolve_and_writes_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "refuse12").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let (owner, fixture_id, account, created) = fixture_key(&d).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    let without_id = |n: u8| {
        let mut key = stored_key(n, "", &account, "broken", created);
        key.remove("access_key_id");
        key
    };
    let mut array = stored_key(31, "", &account, "listed", created);
    array.insert("access_key_id", ARRAY_KEY_IDS.map(Bson::from).to_vec());
    keys.insert_many([
        stored_key(1, &fixture_id, &account, "importer", created + 60_000),
        stored_key(2, TWO_ACCOUNTS_KEY_ID, &account, "shared", created + 30_000),
        stored_key(
            3,
            TWO_ACCOUNTS_KEY_ID,
            OTHER_ACCOUNT,
            "shared",
            created + 90_000,
        ),
        stored_key(11, EXAMPLE_KEY_ID, &account, "ci", created + 120_000),
        stored_key(12, EXAMPLE_KEY_ID, &account, "ci", created + 180_000),
        without_id(21),
        without_id(22),
        array,
    ])
    .await
    .unwrap();
    let documents = Deployment::documents(&d.catalog(), "access_keys").await;
    let settings = Deployment::documents(&d.catalog(), "settings").await;

    let CatalogStatus::NeedsAttention { items } = d.status().await else {
        panic!("migration 12 is not refused");
    };
    assert_eq!(items.len(), 4, "{items:?}");
    let error = d.migrate().await.expect_err("refused");
    assert_eq!(error.exit_code(), exit::ATTENTION);
    let message = error.to_string();
    let catalog = &d.target.catalog;
    let owners = |id: &str, read: &Bson, copies: [String; 2]| {
        format!(
            "{REFUSED}2 documents in {catalog}.access_keys have the access key ID {id}, and \
             these documents belong to more than one user. This migration cannot decide which \
             user is the key owner. When a request uses this access key ID, ExtendDB reads _id \
             {read} to authenticate the request. Delete each document that does not belong to \
             the key owner. The documents are: {}. {}",
            copies.join(", "),
            delete_sentence(catalog)
        )
    };
    // A failure names the missing refusal and prints none of its text,
    // which names the account.
    for (refusal, item) in [
        (
            "the fixture's access key ID, which two users hold",
            owners(
                &fixture_id,
                &owner,
                [
                    described(&owner, &account, "cu-tester", created),
                    described(&oid(1).into(), &account, "importer", created + 60_000),
                ],
            ),
        ),
        (
            "the access key ID that users in two accounts hold",
            owners(
                TWO_ACCOUNTS_KEY_ID,
                &oid(2).into(),
                [
                    described(&oid(2).into(), &account, "shared", created + 30_000),
                    described(&oid(3).into(), OTHER_ACCOUNT, "shared", created + 90_000),
                ],
            ),
        ),
        (
            "the two documents without an access key ID",
            format!(
                "{REFUSED}2 documents in {catalog}.access_keys have no access key ID. ExtendDB \
                 never reads these documents to authenticate a request. The unique index on \
                 access_key_id allows at most one document with no access key ID. Delete all of \
                 these documents except one. The documents are: {}, {}. {}",
                described(&oid(21).into(), &account, "broken", created),
                described(&oid(22).into(), &account, "broken", created),
                delete_sentence(catalog)
            ),
        ),
        (
            "the document whose access key ID is an array",
            array_refusal(catalog, &account, 31, created),
        ),
    ] {
        assert!(
            items.contains(&item),
            "the status items lack the refusal of {refusal}"
        );
        assert!(
            message.contains(&item),
            "the error of migrate --yes lacks the refusal of {refusal}"
        );
    }
    assert!(!message.contains(EXAMPLE_KEY_ID), "{message}");
    assert_eq!(
        Deployment::documents(&d.catalog(), "access_keys").await,
        documents
    );
    assert_eq!(
        Deployment::documents(&d.catalog(), "settings").await,
        settings
    );
    assert!(
        Deployment::documents(&d.catalog(), "extenddb_migrations")
            .await
            .is_empty()
    );
    assert_eq!(unique_index(&d).await, None);

    // What the refusal asks for: the importer's copy, the copy of the other
    // account, one of the two documents without an ID, which the index
    // takes one of, and the array.
    for n in [1, 3, 22, 31] {
        keys.delete_one(doc! { "_id": oid(n) }).await.unwrap();
    }
    d.migrate().await.expect("migrate");
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    assert_eq!(looked_up(&d, &fixture_id).await, owner);
    for (filter, count) in [
        (doc! { "access_key_id": &fixture_id }, 1),
        (doc! { "access_key_id": TWO_ACCOUNTS_KEY_ID }, 1),
        (doc! { "access_key_id": EXAMPLE_KEY_ID }, 1),
        (doc! { "access_key_id": Bson::Null }, 1),
    ] {
        assert_eq!(
            keys.count_documents(filter.clone()).await.unwrap(),
            count,
            "{filter}"
        );
    }
    assert_eq!(unique_index(&d).await, Some(true));
    d.drop_databases().await;
}

/// The body's own `run` refuses what `detect` refuses, which the runner's
/// `detect` otherwise stops first: copies written after the runner read the
/// collection. It returns NeedsAttention and writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_run_refuses_copies_written_after_detect() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "run12").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    d.migrate_through(MigrationId::AttributeNameEscape)
        .await
        .expect("migrations 1 to 11");
    let (_, fixture_id, account, created) = fixture_key(&d).await;
    d.catalog()
        .collection::<Document>("access_keys")
        .insert_one(stored_key(
            1,
            &fixture_id,
            &account,
            "importer",
            created + 60_000,
        ))
        .await
        .unwrap();
    let documents = Deployment::documents(&d.catalog(), "access_keys").await;

    let items = run_body(&d).await;
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(
        items[0].starts_with(&format!(
            "2 documents in {}.access_keys have the access key ID {fixture_id}, and these \
             documents belong to more than one user. ",
            d.target.catalog
        )),
        "{items:?}"
    );
    assert_eq!(
        Deployment::documents(&d.catalog(), "access_keys").await,
        documents
    );
    assert_eq!(unique_index(&d).await, None);
    assert!(
        d.catalog_ledger()
            .await
            .iter()
            .all(|row| row.id != MigrationId::UniqueAccessKeyIds.number()),
        "migration 12 is not recorded"
    );
    d.drop_databases().await;
}

/// Migration 12's body `run`, called directly under the lock; the items it
/// refuses the migration for.
async fn run_body(d: &Deployment) -> Vec<String> {
    use extenddb_storage::migrations::{Body, MigrationError};
    use extenddb_storage_mongodb::migrations::{MigrationLock, Timing, migration};

    let held = MigrationLock::acquire(
        &d.target,
        std::time::Duration::from_secs(10),
        Timing::PRODUCTION,
    )
    .await
    .expect("lock");
    let migrations = MongoMigrations::open(&d.target, Some(&held))
        .await
        .expect("open");
    let entry = migration(MigrationId::UniqueAccessKeyIds);
    let Body::Code { body, .. } = &entry.bodies()[0].body else {
        panic!("migration 12 is a code body");
    };
    let result = body.run(migrations.stores()).await;
    migrations.close().await;
    held.release().await;
    match result {
        Err(MigrationError::Status(CatalogStatus::NeedsAttention { items })) => items,
        other => panic!("run did not refuse: {:?}", other.map(|done| done.notes)),
    }
}

/// An account other than the fixture's.
const OTHER_ACCOUNT: &str = "210987654321";

/// An access key ID that users of one name in two accounts hold.
const TWO_ACCOUNTS_KEY_ID: &str = "AKIATWOACCOUNTS00000";

/// The access key IDs of a document whose `access_key_id` is an array.
const ARRAY_KEY_IDS: [&str; 2] = ["AKIAARRAYEXAMPLE000A", "AKIAARRAYEXAMPLE000B"];

/// The `_id` [`stored_key`] gives document `n`.
fn oid(n: u8) -> ObjectId {
    ObjectId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, n])
}

/// What the runner writes before each of migration 12's refusals.
const REFUSED: &str = "migration 12 unique_access_key_ids: ";

/// How migration 12's refusals name a document of `user` in `account`,
/// created at `created_ms`.
fn described(id: &Bson, account: &str, user: &str, created_ms: i64) -> String {
    let created = bson::DateTime::from_millis(created_ms)
        .try_to_rfc3339_string()
        .unwrap();
    format!("_id {id} (account {account}, user {user}, created {created})")
}

/// The sentence that ends each of migration 12's refusals in `catalog`.
fn delete_sentence(catalog: &str) -> String {
    format!(
        "To delete a document, run this command in mongosh: \
         db.getSiblingDB(\"{catalog}\").access_keys.deleteOne({{ _id: <its _id> }})"
    )
}

/// Migration 12's refusal, as the runner writes it, of document `n` of the
/// user `listed` in `account`, created at `created_ms`, whose access key
/// ID is the array [`ARRAY_KEY_IDS`].
fn array_refusal(catalog: &str, account: &str, n: u8, created_ms: i64) -> String {
    format!(
        "{REFUSED}a document in {catalog}.access_keys has the access key ID [\"{}\", \"{}\"], \
         which is an array. When a request uses an access key ID in the array, ExtendDB can \
         read this document to authenticate the request. This migration cannot decide which \
         access key ID the document keeps. In mongosh, delete the document or set its \
         access_key_id to one string. The document is: {}. {}",
        ARRAY_KEY_IDS[0],
        ARRAY_KEY_IDS[1],
        described(&oid(n).into(), account, "listed", created_ms),
        delete_sentence(catalog)
    )
}

/// Whether `access_key_id_1` is unique, or `None` when it does not exist.
async fn unique_index(d: &Deployment) -> Option<bool> {
    use futures::TryStreamExt;
    let indexes: Vec<mongodb::IndexModel> = d
        .catalog()
        .collection::<Document>("access_keys")
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    indexes
        .iter()
        .filter_map(|m| m.options.as_ref())
        .find(|o| o.name.as_deref() == Some(ACCESS_KEY_ID_INDEX))
        .map(|o| o.unique == Some(true))
}

/// The items for which the status refuses migration 12.
async fn status_items(d: &Deployment) -> Vec<String> {
    match d.status().await {
        CatalogStatus::NeedsAttention { items } => items,
        status => panic!("migration 12 is not refused: {status}"),
    }
}

/// A catalog at migration 11 holds an ID of two users and an array ID. It
/// also holds a document without an ID and one whose ID is the number 42.
/// Beside a non-unique `access_key_id_1`, the status refuses the ID of two
/// users and the array. With the second user's copy deleted and that index
/// replaced by the unique index built by hand, the status refuses the array
/// alone. After the array is deleted, `migrate --yes` applies 12 and keeps
/// the document without an ID and the one with the number 42. Then
/// `--reconverge` refuses an array ID written later, and it refuses neither
/// of those two documents.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_refuses_array_ids_beside_its_own_index() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "arrays12").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    d.migrate_through(MigrationId::AttributeNameEscape)
        .await
        .expect("migrations 1 to 11");
    let (_, fixture_id, account, created) = fixture_key(&d).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    let mut without_id = stored_key(21, "", &account, "broken", created);
    without_id.remove("access_key_id");
    let mut numbered = stored_key(41, "", &account, "numbered", created);
    numbered.insert("access_key_id", 42);
    let array = |n: u8| {
        let mut key = stored_key(n, "", &account, "listed", created);
        key.insert("access_key_id", ARRAY_KEY_IDS.map(Bson::from).to_vec());
        key
    };
    keys.insert_many([
        stored_key(1, &fixture_id, &account, "importer", created + 60_000),
        without_id,
        numbered,
        array(31),
    ])
    .await
    .unwrap();
    keys.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "access_key_id": 1 })
            .build(),
    )
    .await
    .unwrap();
    let catalog = &d.target.catalog;
    let refused_array = |n: u8| array_refusal(catalog, &account, n, created);

    let items = status_items(&d).await;
    assert_eq!(items.len(), 2, "{items:?}");
    for start in [
        format!(
            "{REFUSED}2 documents in {catalog}.access_keys have the access key ID {fixture_id}, \
             and these documents belong to more than one user. "
        ),
        refused_array(31),
    ] {
        assert!(
            items.iter().any(|item| item.starts_with(&start)),
            "{start}\n{items:?}"
        );
    }

    // The importer's copy and the index go, and the manual's unique index
    // is built by hand.
    keys.delete_one(doc! { "_id": oid(1) }).await.unwrap();
    keys.drop_index(ACCESS_KEY_ID_INDEX).await.unwrap();
    d.catalog()
        .run_command(doc! { "createIndexes": "access_keys", "indexes": [
            { "key": { "access_key_id": 1 }, "name": ACCESS_KEY_ID_INDEX, "unique": true },
        ], "commitQuorum": "majority" })
        .await
        .unwrap();
    let items = status_items(&d).await;
    assert_eq!(items, [refused_array(31)]);

    keys.delete_one(doc! { "_id": oid(31) }).await.unwrap();
    d.migrate().await.expect("migrate");
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    for filter in [doc! { "_id": oid(21) }, doc! { "_id": oid(41) }] {
        assert_eq!(
            keys.count_documents(filter.clone()).await.unwrap(),
            1,
            "{filter}"
        );
    }

    keys.insert_one(array(32)).await.unwrap();
    let error = d.reconverge().await.expect_err("refused");
    assert_eq!(error.exit_code(), exit::ATTENTION);
    let message = error.to_string();
    assert!(message.contains(&refused_array(32)), "{message}");
    for absent in ["have no access key ID", "have the access key ID 42"] {
        assert!(!message.contains(absent), "{absent}\n{message}");
    }
    d.drop_databases().await;
}

/// With an operator's index on `{ access_key_id: 1, created_at: -1 }`, a
/// lookup reads an ID's newest copy, which is not its first in natural
/// order. The body keeps the copy the lookup's own query returns and
/// deletes the others, and the operator's index stays.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_keeps_the_copy_a_lookup_reads_through_an_operator_index() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "operator12").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let (first, fixture_id, account, created) = fixture_key(&d).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    keys.insert_many([
        stored_key(1, &fixture_id, &account, "cu-tester", created + 60_000),
        stored_key(2, &fixture_id, &account, "cu-tester", created - 60_000),
    ])
    .await
    .unwrap();
    keys.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "access_key_id": 1, "created_at": -1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .name("akid_newest_first".to_owned())
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    let newest = Bson::ObjectId(oid(1));
    assert_eq!(looked_up(&d, &fixture_id).await, newest);

    d.migrate().await.expect("migrate");
    let left: Vec<Bson> = Deployment::documents(&d.catalog(), "access_keys")
        .await
        .iter()
        .filter(|k| k.get_str("access_key_id") == Ok(fixture_id.as_str()))
        .map(|k| k.get("_id").cloned().unwrap())
        .collect();
    assert_eq!(left, std::slice::from_ref(&newest));
    let notes = d
        .catalog_ledger()
        .await
        .into_iter()
        .find(|row| row.id == MigrationId::UniqueAccessKeyIds.number())
        .expect("migration 12's row")
        .notes;
    assert_eq!(
        notes,
        format!(
            "3 documents had the access key ID {fixture_id}, and all of them belonged to account \
             {account}, user cu-tester. When a request uses this access key ID, ExtendDB reads \
             _id {newest} to authenticate the request. This migration kept _id {newest} and \
             deleted the other documents: _id {first}, _id {}.",
            Bson::ObjectId(oid(2))
        )
    );
    assert_eq!(unique_index(&d).await, Some(true));
    assert!(
        access_key_indexes(&d)
            .await
            .contains(&"akid_newest_first".to_owned())
    );
    d.drop_databases().await;
}

/// A partial index on `access_key_id` gives the lookup an index plan for
/// some IDs and not for others: here `{ access_key_id: 1, created_at: -1 }`
/// for IDs from AKIAM on. The lookup reads an ID below AKIAM by a forward
/// scan, its first copy in natural order, and an ID from AKIAM on through
/// the index, its newest copy. The body keeps, for each ID, the copy its
/// own lookup reads.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_keeps_the_copy_each_lookup_reads_beside_a_partial_index() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "partial12").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let (_, _, account, created) = fixture_key(&d).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    let (scanned, indexed) = ("AKIAAPARTIAL00000001", "AKIAZPARTIAL00000001");
    keys.insert_many([
        stored_key(1, scanned, &account, "low", created + 60_000),
        stored_key(2, indexed, &account, "high", created + 60_000),
        stored_key(3, scanned, &account, "low", created + 120_000),
        stored_key(4, indexed, &account, "high", created + 120_000),
    ])
    .await
    .unwrap();
    keys.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "access_key_id": 1, "created_at": -1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .name("akid_partial_newest_first".to_owned())
                    .partial_filter_expression(doc! { "access_key_id": { "$gte": "AKIAM" } })
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    let expected = [
        (scanned, Bson::ObjectId(oid(1))),
        (indexed, Bson::ObjectId(oid(4))),
    ];
    for (id, read) in &expected {
        assert_eq!(&looked_up(&d, id).await, read, "{id}");
    }

    d.migrate().await.expect("migrate");
    let documents = Deployment::documents(&d.catalog(), "access_keys").await;
    for (id, read) in &expected {
        let left: Vec<&Bson> = documents
            .iter()
            .filter(|k| k.get_str("access_key_id") == Ok(*id))
            .filter_map(|k| k.get("_id"))
            .collect();
        assert_eq!(left, [read], "{id}");
        assert_eq!(&looked_up(&d, id).await, read, "{id}");
    }
    assert_eq!(unique_index(&d).await, Some(true));
    d.drop_databases().await;
}

/// On MongoDB 8.0 and later, query settings can give the lookup's command
/// another plan with no index on `access_key_id`: here `allowedIndexes:
/// [{ $natural: -1 }]` for its shape, so a lookup reads an ID's last copy in
/// natural order. `explain` reports a backward scan, and the body keeps the
/// copy the lookup's own query returns. MongoDB 7.0 has no query settings,
/// and there the test stops after it loads the fixture, before it writes
/// the second copy.
#[tokio::test(flavor = "multi_thread")]
async fn migration_12_keeps_the_copy_a_lookup_reads_under_query_settings() {
    use futures::FutureExt;

    /// CommandNotFound.
    const NO_SUCH_COMMAND: i32 = 59;

    fn no_such_command(e: &mongodb::error::Error) -> bool {
        matches!(*e.kind, mongodb::error::ErrorKind::Command(ref c) if c.code == NO_SUCH_COMMAND)
    }

    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "settings12").await;
    let catalog = d.target.catalog.clone();
    let admin = d.client.database("admin");
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let (first, fixture_id, account, created) = fixture_key(&d).await;
    let set = admin
        .run_command(doc! {
            "setQuerySettings": {
                "find": "access_keys",
                "filter": { "access_key_id": &fixture_id },
                "limit": 1,
                "singleBatch": true,
                "$db": &catalog,
            },
            "settings": { "indexHints": {
                "ns": { "db": &catalog, "coll": "access_keys" },
                "allowedIndexes": [{ "$natural": -1 }],
            } },
        })
        .await;
    let hash = match set {
        Ok(reply) => reply.get_str("queryShapeHash").unwrap().to_owned(),
        Err(e) if no_such_command(&e) => {
            eprintln!("query settings need MongoDB 8.0 or later; this server has none");
            d.drop_databases().await;
            return;
        }
        Err(e) => panic!("setQuerySettings: {e}"),
    };
    let outcome = std::panic::AssertUnwindSafe(async {
        let keys = d.catalog().collection::<Document>("access_keys");
        keys.insert_one(stored_key(
            1,
            &fixture_id,
            &account,
            "cu-tester",
            created + 60_000,
        ))
        .await
        .unwrap();
        let last = Bson::ObjectId(oid(1));
        assert_eq!(looked_up(&d, &fixture_id).await, last);

        d.migrate().await.expect("migrate");
        let left: Vec<Bson> = Deployment::documents(&d.catalog(), "access_keys")
            .await
            .iter()
            .filter(|k| k.get_str("access_key_id") == Ok(fixture_id.as_str()))
            .map(|k| k.get("_id").cloned().unwrap())
            .collect();
        assert_eq!(left, std::slice::from_ref(&last));
        let notes = d
            .catalog_ledger()
            .await
            .into_iter()
            .find(|row| row.id == MigrationId::UniqueAccessKeyIds.number())
            .expect("migration 12's row")
            .notes;
        assert_eq!(
            notes,
            format!(
                "2 documents had the access key ID {fixture_id}, and all of them belonged to \
                 account {account}, user cu-tester. When a request uses this access key ID, \
                 ExtendDB reads _id {last} to authenticate the request. This migration kept _id \
                 {last} and deleted the other document: _id {first}."
            )
        );
    })
    .catch_unwind()
    .await;
    admin
        .run_command(doc! { "removeQuerySettings": &hash })
        .await
        .unwrap_or_else(|e| panic!("removeQuerySettings {hash}: {e}"));
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    d.drop_databases().await;
}
