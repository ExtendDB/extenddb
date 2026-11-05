// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 6's read of `access_keys` when its `$group` state is larger
//! than the server's memory limit for `$group`. The test lowers the limits
//! of the classic and the slot-based engine to 100 KB and sets them back at
//! the end. They are the server's, so the test is alone in its binary: the
//! tests of one binary run at once. A run killed before it sets them back
//! leaves them at 100 KB until mongod restarts, and the next run stops at
//! its start with the commands that set them back.

mod migrations_support;

use bson::{Bson, Document, doc};
use extenddb_storage::migrations::CatalogStatus;
use futures::{FutureExt, TryStreamExt};
use migrations_support::{Deployment, fixture, server};

/// The server parameters that bound the memory of `$group`, for the
/// classic engine and for the slot-based engine.
const LIMITS: [&str; 2] = [
    "internalDocumentSourceGroupMaxMemoryBytes",
    "internalQuerySlotBasedExecutionHashAggApproxMemoryUseInBytesBeforeSpill",
];

/// What the test sets the limits to while it runs.
const LOWERED: i64 = 100 * 1024;

/// The limits' default on MongoDB 7.0 and 8.0, 100 MB.
const DEFAULT: i64 = 100 * 1024 * 1024;

/// QueryExceededMemoryLimitNoDiskUseAllowed.
const NO_DISK_USE: i32 = 292;

const KEYS: u32 = 3000;
const SHARED: u32 = 20;

fn key_id(i: u32) -> String {
    format!("AKIASPILL{i:011}")
}

async fn set_limits(client: &mongodb::Client, values: &[Bson]) {
    for (name, value) in LIMITS.iter().zip(values) {
        client
            .database("admin")
            .run_command(doc! { "setParameter": 1, *name: value.clone() })
            .await
            .unwrap_or_else(|e| panic!("set {name}: {e}"));
    }
}

async fn limits(client: &mongodb::Client) -> Vec<Bson> {
    let mut values = Vec::new();
    for name in LIMITS {
        let reply = client
            .database("admin")
            .run_command(doc! { "getParameter": 1, name: 1 })
            .await
            .unwrap_or_else(|e| panic!("get {name}: {e}"));
        values.push(reply.get(name).cloned().expect(name));
    }
    values
}

/// Stops the test when a limit is at the value the test sets, which a run
/// killed before it set the limits back leaves: the test would otherwise
/// set back that value at its end. The server returns both limits as Int64.
fn check_not_left_lowered(values: &[Bson]) {
    let left: Vec<&str> = LIMITS
        .iter()
        .zip(values)
        .filter(|(_, value)| **value == Bson::Int64(LOWERED))
        .map(|(name, _)| *name)
        .collect();
    let (is, it) = if left.len() == 1 {
        ("is", "it")
    } else {
        ("are", "them")
    };
    assert!(
        left.is_empty(),
        "{} {is} at {LOWERED}, the value this test sets while it runs: an earlier run was \
         stopped before it set {it} back. Restart mongod, or set {it} back in mongosh with {}",
        left.join(" and "),
        left.iter()
            .map(|name| format!("db.adminCommand({{ setParameter: 1, {name}: {DEFAULT} }})"))
            .collect::<Vec<_>>()
            .join(" and ")
    );
}

/// 3,000 keys, and a second copy of 20 of them for the same user: migration
/// 6's `$group` holds more than 100 KB, and its read spills. The migration
/// applies, and each of the 20 IDs keeps the copy a lookup read.
#[tokio::test(flavor = "multi_thread")]
async fn migration_6_reads_a_collection_whose_group_spills_to_disk() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "spill6").await;
    let saved = limits(&d.client).await;
    check_not_left_lowered(&saved);
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let keys = d.catalog().collection::<Document>("access_keys");
    let account = Deployment::documents(&d.catalog(), "access_keys").await[0]
        .get_str("account_id")
        .unwrap()
        .to_owned();
    let key = |i: u32| {
        doc! {
            "access_key_id": key_id(i),
            "account_id": &account,
            "user_name": format!("u{}", i % 10),
            "is_active": true,
            "created_at": bson::DateTime::from_millis(1_793_235_168_000 + i64::from(i)),
        }
    };
    keys.insert_many((0..KEYS).map(key)).await.unwrap();
    keys.insert_many((0..SHARED).map(key)).await.unwrap();
    let mut read_before = Vec::new();
    for i in 0..SHARED {
        let first = keys
            .find_one(doc! { "access_key_id": key_id(i) })
            .await
            .unwrap()
            .unwrap();
        read_before.push(first.get("_id").cloned().unwrap());
    }

    let small = vec![Bson::Int64(LOWERED); LIMITS.len()];
    set_limits(&d.client, &small).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        // The state is past the limits: without disk use the same grouping
        // fails.
        let refused = keys
            .aggregate(vec![doc! { "$group": {
                "_id": "$access_key_id",
                "copies": { "$push": { "_id": "$_id", "user_name": "$user_name" } },
            } }])
            .allow_disk_use(false)
            .await;
        let code = match refused {
            Ok(cursor) => cursor.try_collect::<Vec<Document>>().await.err(),
            Err(e) => Some(e),
        }
        .and_then(|e| match *e.kind {
            mongodb::error::ErrorKind::Command(ref c) => Some(c.code),
            _ => None,
        });
        assert_eq!(code, Some(NO_DISK_USE), "the group fits in 100 KB");

        d.migrate().await.expect("migrate");
    })
    .catch_unwind()
    .await;
    set_limits(&d.client, &saved).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }

    assert_eq!(d.status().await, CatalogStatus::Current { version: 6 });
    for (i, read) in (0..SHARED).zip(&read_before) {
        let left: Vec<Document> = keys
            .find(doc! { "access_key_id": key_id(i) })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(left.len(), 1, "{}", key_id(i));
        assert_eq!(left[0].get("_id"), Some(read), "{}", key_id(i));
    }
    let notes = d
        .catalog_ledger()
        .await
        .into_iter()
        .find(|row| row.id == 6)
        .expect("migration 6's row")
        .notes;
    assert_eq!(notes.lines().count(), SHARED as usize, "{notes}");
    d.drop_databases().await;
}
