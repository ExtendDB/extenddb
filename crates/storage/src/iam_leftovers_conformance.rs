// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent check that a deleted principal or account leaves
//! nothing for one created later with its name or id (#693, #694), run by
//! each backend's own test suite against a real database.
//!
//! Inline policies and permissions boundaries are stored by principal name.
//! No backend links them to their principal, and MongoDB and Cassandra link
//! no IAM record to its account. The check:
//!
//! - deletes a user, a group and a role that hold policies and boundaries,
//!   and reads that none of them is left under its name;
//! - stores policies and boundaries under names that have no principal, as a
//!   delete before these fixes left them, creates principals with those
//!   names, and reads that each starts empty (a user with its
//!   `SelfServicePolicy` only);
//! - creates a principal that already exists, and reads that the refusal
//!   left its policies and boundary in place;
//! - deletes an account that holds a user, a group and a role with policies,
//!   creates an account with the same id, and reads that it has no
//!   principals and no policies under the old names.
//!
//! The store methods do not check that a principal exists before a put
//! (the management API does), which is what lets the check store rows under
//! a name with no principal.

use serde_json::{Value, json};

use crate::management_store::{ManagementStore, OpError};

fn allow(action: &str) -> Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": action, "Resource": "*"}],
    })
}

fn trust(account_id: &str) -> Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{account_id}:root")},
            "Action": "sts:AssumeRole",
        }],
    })
}

async fn policy_names(
    store: &dyn ManagementStore,
    account_id: &str,
    principal_type: &str,
    name: &str,
) -> Vec<String> {
    let mut names: Vec<String> = store
        .list_policies(account_id, principal_type, name)
        .await
        .unwrap_or_else(|e| panic!("list {principal_type} {name}'s policies: {e:?}"))
        .into_iter()
        .map(|(policy_name, _, _)| policy_name)
        .collect();
    names.sort();
    names
}

async fn boundary(
    store: &dyn ManagementStore,
    account_id: &str,
    principal_type: &str,
    name: &str,
) -> Option<Value> {
    match principal_type {
        "user" => store.get_user_boundary(account_id, name).await,
        "role" => store.get_role_boundary(account_id, name).await,
        other => unreachable!("principal type {other}"),
    }
    .unwrap_or_else(|e| panic!("get {principal_type} {name}'s boundary: {e:?}"))
}

async fn put(store: &dyn ManagementStore, account_id: &str, principal_type: &str, name: &str) {
    store
        .put_policy(
            account_id,
            principal_type,
            name,
            "old",
            &allow("dynamodb:ListTables"),
        )
        .await
        .unwrap_or_else(|e| panic!("put {principal_type} {name}'s policy: {e:?}"));
    let set = match principal_type {
        "user" => {
            store
                .set_user_boundary(account_id, name, &allow("dynamodb:*"))
                .await
        }
        "role" => {
            store
                .set_role_boundary(account_id, name, &allow("dynamodb:*"))
                .await
        }
        _ => Ok(()),
    };
    set.unwrap_or_else(|e| panic!("set {principal_type} {name}'s boundary: {e:?}"));
}

async fn create(store: &dyn ManagementStore, account_id: &str, principal_type: &str, name: &str) {
    let created = match principal_type {
        "user" => store.create_user(account_id, name, None).await,
        "group" => store.create_group(account_id, name).await,
        "role" => {
            store
                .create_role(account_id, name, &trust(account_id))
                .await
        }
        other => unreachable!("principal type {other}"),
    };
    created.unwrap_or_else(|e| panic!("create {principal_type} {name}: {e:?}"));
}

async fn delete(store: &dyn ManagementStore, account_id: &str, principal_type: &str, name: &str) {
    let deleted = match principal_type {
        "user" => store.delete_user(account_id, name).await,
        "group" => store.delete_group(account_id, name).await,
        "role" => store.delete_role(account_id, name).await,
        other => unreachable!("principal type {other}"),
    };
    deleted.unwrap_or_else(|e| panic!("delete {principal_type} {name}: {e:?}"));
}

/// What a principal of `principal_type` has when it starts empty.
fn fresh_policies(principal_type: &str) -> Vec<String> {
    if principal_type == "user" {
        vec!["SelfServicePolicy".to_owned()]
    } else {
        Vec::new()
    }
}

/// Assert that `name` has no policies (beyond a new user's) and no boundary.
async fn assert_empty(
    store: &dyn ManagementStore,
    account_id: &str,
    principal_type: &str,
    name: &str,
    expected_policies: &[String],
    when: &str,
) {
    assert_eq!(
        policy_names(store, account_id, principal_type, name).await,
        expected_policies,
        "{when}: {principal_type} {name}'s policies"
    );
    if principal_type != "group" {
        assert_eq!(
            boundary(store, account_id, principal_type, name).await,
            None,
            "{when}: {principal_type} {name}'s boundary"
        );
    }
}

/// Run the check in account `account_id`, which must not exist yet. The
/// account is deleted at the end; a failure leaves it for the caller's
/// cleanup.
///
/// # Panics
///
/// Panics on the first store error or wrong answer.
pub async fn run(store: &dyn ManagementStore, account_id: &str) {
    store
        .create_account(account_id, &format!("leftovers-{account_id}"))
        .await
        .expect("create the account");

    for principal_type in ["user", "group", "role"] {
        // A delete removes what the principal holds.
        let name = format!("deleted-{principal_type}");
        create(store, account_id, principal_type, &name).await;
        put(store, account_id, principal_type, &name).await;
        delete(store, account_id, principal_type, &name).await;
        assert_empty(
            store,
            account_id,
            principal_type,
            &name,
            &[],
            "after its delete",
        )
        .await;

        // A create clears rows a delete before these fixes left.
        let name = format!("left-{principal_type}");
        put(store, account_id, principal_type, &name).await;
        create(store, account_id, principal_type, &name).await;
        assert_empty(
            store,
            account_id,
            principal_type,
            &name,
            &fresh_policies(principal_type),
            "created over rows left under its name",
        )
        .await;

        // A create of a principal that exists clears nothing.
        let name = format!("live-{principal_type}");
        create(store, account_id, principal_type, &name).await;
        put(store, account_id, principal_type, &name).await;
        let again = match principal_type {
            "user" => store.create_user(account_id, &name, None).await,
            "group" => store.create_group(account_id, &name).await,
            _ => {
                store
                    .create_role(account_id, &name, &trust(account_id))
                    .await
            }
        };
        assert!(
            matches!(again, Err(OpError::AlreadyExists(_))),
            "create {principal_type} {name} again: {again:?}"
        );
        let mut kept = fresh_policies(principal_type);
        kept.insert(0, "old".to_owned());
        kept.sort();
        assert_eq!(
            policy_names(store, account_id, principal_type, &name).await,
            kept,
            "{principal_type} {name}'s policies after a refused create"
        );
        if principal_type != "group" {
            assert!(
                boundary(store, account_id, principal_type, &name)
                    .await
                    .is_some(),
                "{principal_type} {name}'s boundary after a refused create"
            );
        }
    }

    // An account's delete removes its principals and what they hold. Its
    // tables go first; the check creates none.
    for principal_type in ["user", "group", "role"] {
        let name = format!("in-account-{principal_type}");
        create(store, account_id, principal_type, &name).await;
        put(store, account_id, principal_type, &name).await;
    }
    for (principal_type, name) in [
        ("user", "left-user"),
        ("user", "live-user"),
        ("group", "left-group"),
        ("group", "live-group"),
        ("role", "left-role"),
        ("role", "live-role"),
    ] {
        delete(store, account_id, principal_type, name).await;
    }
    store
        .delete_account(account_id)
        .await
        .expect("delete the account");
    store
        .create_account(account_id, &format!("leftovers-again-{account_id}"))
        .await
        .expect("create the account again");
    assert!(
        store
            .list_users(account_id)
            .await
            .expect("list users")
            .is_empty(),
        "users of the account created again"
    );
    assert!(
        store
            .list_groups(account_id)
            .await
            .expect("list groups")
            .is_empty(),
        "groups of the account created again"
    );
    assert!(
        store
            .list_roles(account_id)
            .await
            .expect("list roles")
            .is_empty(),
        "roles of the account created again"
    );
    for principal_type in ["user", "group", "role"] {
        let name = format!("in-account-{principal_type}");
        assert_empty(
            store,
            account_id,
            principal_type,
            &name,
            &[],
            "in the account created again",
        )
        .await;
    }
    store
        .delete_account(account_id)
        .await
        .expect("delete the account again");
}
