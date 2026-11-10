// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deleted principal or account leaves nothing behind (#693, #694), against
//! a MongoDB replica set named by `EXTENDDB_TEST_MONGODB_URI`.
//!
//! `cargo test` does not run these: `devtools/run-mongodb-tests --rust-live`
//! runs them with `--ignored`. Each test works in its own pair of databases,
//! created with every migration as `init` creates them, and drops them at the
//! end.

use std::time::Duration;

use extenddb_storage::management_store::{ManagementStore, PasswordHashStore};
use mongodb::bson::{Document, doc};

use crate::MongoCatalogStore;
use crate::migrations::{MongoTarget, Timing, lock, migrate_holding};

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27741/?directConnection=true",
    )
}

/// A catalog store on scratch databases that every migration created.
struct Scratch {
    client: mongodb::Client,
    catalog: String,
    data: String,
    store: MongoCatalogStore,
}

impl Scratch {
    async fn new() -> Self {
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let catalog = format!("t{}_cat", &tag[..12]);
        let data = format!("t{}_data", &tag[..12]);
        let target = MongoTarget::with_databases(uri(), catalog.clone(), data.clone());
        let (held, target) = lock(&target, true, Duration::from_secs(10), Timing::PRODUCTION)
            .await
            .expect("lock the scratch deployment");
        migrate_holding(&target, "test", &held)
            .await
            .expect("create the scratch catalog");
        held.release().await;
        let client = mongodb::Client::with_uri_str(uri()).await.expect("client");
        let store = MongoCatalogStore::on_database(client.clone(), client.database(&catalog));
        Self {
            client,
            catalog,
            data,
            store,
        }
    }

    async fn drop(self) {
        self.client
            .database(&self.catalog)
            .drop()
            .await
            .expect("drop catalog");
        self.client
            .database(&self.data)
            .drop()
            .await
            .expect("drop data");
    }
}

#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn deleted_principals_and_accounts_leave_nothing() {
    let s = Scratch::new().await;
    extenddb_storage::iam_leftovers_conformance::run(&s.store, "123456789012").await;
    s.drop().await;
}

/// Before account deletes removed them, a deleted account's users and roles
/// stayed. With only the `accounts` document removed, as such a delete left
/// it, the user cannot sign in to the console and the role cannot be assumed.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn a_user_or_role_of_a_deleted_account_grants_nothing() {
    const ACCOUNT: &str = "123456789012";
    let s = Scratch::new().await;
    let store = &s.store;
    store.create_account(ACCOUNT, "acct").await.unwrap();
    // Generated, not written in the source.
    let password = uuid::Uuid::new_v4().to_string();
    let hash = bcrypt::hash(&password, 4).unwrap();
    store
        .create_user(ACCOUNT, "alice", Some(&hash))
        .await
        .unwrap();
    let trust = serde_json::json!({"Version": "2012-10-17", "Statement": []});
    store.create_role(ACCOUNT, "app", &trust).await.unwrap();
    assert!(
        store
            .get_user_password_hash(ACCOUNT, "alice")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .verify_iam_user_password(ACCOUNT, "alice", &password)
            .await
            .unwrap()
    );
    assert!(
        store
            .get_role_trust_policy(ACCOUNT, "app")
            .await
            .unwrap()
            .is_some()
    );

    s.client
        .database(&s.catalog)
        .collection::<Document>("accounts")
        .delete_many(doc! { "account_id": ACCOUNT })
        .await
        .expect("remove the accounts document only");

    assert_eq!(
        store
            .get_user_password_hash(ACCOUNT, "alice")
            .await
            .unwrap(),
        None
    );
    assert!(
        !store
            .verify_iam_user_password(ACCOUNT, "alice", &password)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_role_trust_policy(ACCOUNT, "app").await.unwrap(),
        None
    );
    s.drop().await;
}
