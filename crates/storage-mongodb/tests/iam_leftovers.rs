// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deleted principal or account leaves nothing behind (#693, #694).
//!
//! Direct integration tests: they call the MongoDB catalog store's trait
//! methods against a live replica set named by `EXTENDDB_TEST_MONGODB_URI`,
//! with no ExtendDB server in between, as the Cassandra crate's
//! `tests/direct_integration.rs` does against a live node. Without the URI
//! they skip, unless `EXTENDDB_TEST_MONGODB_REQUIRED` is `1`, which
//! `devtools/run-mongodb-tests --rust-live` sets.
//!
//! Each test works in a deployment of its own, created with every migration
//! as `init` creates it, and drops it at the end.

mod migrations_support;

use bson::{Document, doc};
use extenddb_storage::management_store::{ManagementStore, PasswordHashStore};
use extenddb_storage_mongodb::MongoCatalogStore;
use migrations_support::{Deployment, server};

/// A catalog store on a new deployment, or `None` to skip.
async fn scratch(tag: &str) -> Option<(Deployment, MongoCatalogStore)> {
    let uri = server()?;
    let deployment = Deployment::new(&uri, tag).await;
    deployment.create().await;
    let store =
        MongoCatalogStore::on_database_for_tests(deployment.client.clone(), deployment.catalog());
    Some((deployment, store))
}

#[tokio::test]
async fn deleted_principals_and_accounts_leave_nothing() {
    let Some((deployment, store)) = scratch("leftovers").await else {
        return;
    };
    extenddb_storage::iam_leftovers_conformance::run(&store, "123456789012").await;
    deployment.drop_databases().await;
}

/// Before account deletes removed them, a deleted account's users and roles
/// stayed. With only the `accounts` document removed, as such a delete left
/// it, the user cannot sign in to the console and the role cannot be assumed.
#[tokio::test]
async fn a_user_or_role_of_a_deleted_account_grants_nothing() {
    const ACCOUNT: &str = "123456789012";
    let Some((deployment, store)) = scratch("deleted_acct").await else {
        return;
    };
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

    deployment
        .catalog()
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
    deployment.drop_databases().await;
}
