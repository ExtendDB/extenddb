// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A stored IAM document that the authorization store cannot read fails the
//! load with `OpError::Internal`.
//!
//! Direct integration tests: they write documents into the catalog of a live
//! replica set named by `EXTENDDB_TEST_MONGODB_URI` and call the store's
//! `AuthorizationStore` methods, with no `ExtendDB` server in between. Without
//! the URI they skip, unless `EXTENDDB_TEST_MONGODB_REQUIRED` is `1`, which
//! `devtools/run-mongodb-tests --rust-live` sets.

mod migrations_support;

use bson::{Binary, Bson, Decimal128, Document, bson, doc, spec::BinarySubtype};
use extenddb_storage::authorization_store::AuthorizationStore;
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage_mongodb::MongoCatalogStore;
use migrations_support::scratch;

const ACCOUNT: &str = "444455556666";

/// The `AuthorizationStore` method a row calls.
#[derive(Clone, Copy)]
enum Load {
    UserPolicies,
    GroupPolicies,
    UserBoundary,
    RolePolicies,
    RoleBoundary,
    Session,
    UserTags,
    RoleTags,
    ResourceTags,
}

/// Call `load` for the principal (or the resource) `who`, and count what it
/// read: policies, tags, a boundary, or a session's policy and tags.
async fn load(store: &MongoCatalogStore, load: Load, who: &str) -> OpResult<usize> {
    Ok(match load {
        Load::UserPolicies => store.fetch_user_policies(ACCOUNT, who).await?.len(),
        Load::GroupPolicies => store.fetch_user_group_policies(ACCOUNT, who).await?.len(),
        Load::UserBoundary => usize::from(store.fetch_user_boundary(ACCOUNT, who).await?.is_some()),
        Load::RolePolicies => store.fetch_role_policies(ACCOUNT, who).await?.len(),
        Load::RoleBoundary => usize::from(store.fetch_role_boundary(ACCOUNT, who).await?.is_some()),
        Load::Session => store
            .fetch_session_data(ACCOUNT, who, "s")
            .await?
            .map_or(0, |s| {
                usize::from(s.session_policy.is_some()) + s.session_tags.len()
            }),
        Load::UserTags => store.fetch_user_tags(ACCOUNT, who).await?.len(),
        Load::RoleTags => store.fetch_role_tags(ACCOUNT, who).await?.len(),
        Load::ResourceTags => store.fetch_resource_tags(&table_arn(who)).await?.len(),
    })
}

fn table_arn(who: &str) -> String {
    format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{who}")
}

fn policy() -> Bson {
    bson!({
        "Version": "2012-10-17",
        "Statement": [{ "Effect": "Deny", "Action": "dynamodb:*", "Resource": "*" }],
    })
}

/// A value with no JSON form.
fn binary() -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: vec![1, 2, 3],
    })
}

fn boundary_doc(principal_type: &str, who: &str) -> Document {
    doc! {
        "account_id": ACCOUNT,
        "principal_type": principal_type,
        "principal_name": who,
        "policy_document": policy(),
    }
}

fn policy_doc(principal_type: &str, who: &str) -> Document {
    let mut d = boundary_doc(principal_type, who);
    d.insert("policy_name", "p");
    d
}

fn session_doc(who: &str) -> Document {
    let expires_at =
        bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 3_600_000);
    doc! {
        "session_token": format!("token-{who}"),
        "access_key_id": format!("ASIA{who}"),
        "account_id": ACCOUNT,
        "role_name": who,
        "session_name": "s",
        "expires_at": expires_at,
        "created_at": bson::DateTime::now(),
    }
}

/// A group membership of `who` and the group's one policy.
fn group_docs(who: &str) -> (Document, Document) {
    let group = format!("{who}-group");
    (
        doc! { "account_id": ACCOUNT, "group_name": &group, "user_name": who },
        policy_doc("group", &group),
    )
}

/// One stored document shape: what `docs` writes for `who`, and the load that
/// reads it. Undamaged, the documents hold the fields that the loads read, as
/// `ExtendDB` writes them. Damaged, one of those fields is missing or of
/// another type.
struct Row {
    name: &'static str,
    load: Load,
    docs: fn(who: &str, damaged: bool) -> Vec<(&'static str, Document)>,
}

const ROWS: &[Row] = &[
    Row {
        name: "a user policy without policy_document",
        load: Load::UserPolicies,
        docs: |who, damaged| {
            let mut d = policy_doc("user", who);
            if damaged {
                d.remove("policy_document");
            }
            vec![("iam_policies", d)]
        },
    },
    Row {
        name: "a role policy whose policy_document is binary data",
        load: Load::RolePolicies,
        docs: |who, damaged| {
            let mut d = policy_doc("role", who);
            if damaged {
                d.insert("policy_document", binary());
            }
            vec![("iam_policies", d)]
        },
    },
    Row {
        name: "a group membership whose group_name is a number",
        load: Load::GroupPolicies,
        docs: |who, damaged| {
            let (mut member, policy) = group_docs(who);
            if damaged {
                member.insert("group_name", 7);
            }
            vec![("iam_group_members", member), ("iam_policies", policy)]
        },
    },
    Row {
        name: "a group policy whose principal_name is an array",
        load: Load::GroupPolicies,
        docs: |who, damaged| {
            let (member, mut policy) = group_docs(who);
            if damaged {
                let group = policy.get_str("principal_name").unwrap().to_owned();
                policy.insert("principal_name", vec![group]);
            }
            vec![("iam_group_members", member), ("iam_policies", policy)]
        },
    },
    Row {
        name: "a group policy whose policy_document is a Decimal128",
        load: Load::GroupPolicies,
        docs: |who, damaged| {
            let (member, mut policy) = group_docs(who);
            if damaged {
                policy.insert("policy_document", "1.5".parse::<Decimal128>().unwrap());
            }
            vec![("iam_group_members", member), ("iam_policies", policy)]
        },
    },
    Row {
        name: "a user permissions boundary without policy_document",
        load: Load::UserBoundary,
        docs: |who, damaged| {
            let mut d = boundary_doc("user", who);
            if damaged {
                d.remove("policy_document");
            }
            vec![("iam_permissions_boundaries", d)]
        },
    },
    Row {
        name: "a role permissions boundary whose policy_document is binary data",
        load: Load::RoleBoundary,
        docs: |who, damaged| {
            let mut d = boundary_doc("role", who);
            if damaged {
                d.insert("policy_document", binary());
            }
            vec![("iam_permissions_boundaries", d)]
        },
    },
    Row {
        name: "a session policy that is binary data",
        load: Load::Session,
        docs: |who, damaged| {
            let mut d = session_doc(who);
            d.insert("session_policy", if damaged { binary() } else { policy() });
            vec![("iam_sessions", d)]
        },
    },
    Row {
        name: "session tags with binary data in a value",
        load: Load::Session,
        docs: |who, damaged| {
            let mut d = session_doc(who);
            let value = if damaged { binary() } else { Bson::from("a") };
            d.insert(
                "session_tags",
                vec![bson!({ "Key": "team", "Value": value })],
            );
            vec![("iam_sessions", d)]
        },
    },
    Row {
        name: "a user tag whose tag_value is a number",
        load: Load::UserTags,
        docs: |who, damaged| {
            let mut d = doc! { "account_id": ACCOUNT, "user_name": who, "tag_key": "team", "tag_value": "a" };
            if damaged {
                d.insert("tag_value", 7);
            }
            vec![("iam_user_tags", d)]
        },
    },
    Row {
        name: "a role tag without tag_key",
        load: Load::RoleTags,
        docs: |who, damaged| {
            let mut d = doc! { "account_id": ACCOUNT, "role_name": who, "tag_key": "team", "tag_value": "a" };
            if damaged {
                d.remove("tag_key");
            }
            vec![("iam_role_tags", d)]
        },
    },
    Row {
        name: "a resource tag whose tag_value is null",
        load: Load::ResourceTags,
        docs: |who, damaged| {
            let mut d =
                doc! { "resource_arn": table_arn(who), "tag_key": "team", "tag_value": "a" };
            if damaged {
                d.insert("tag_value", Bson::Null);
            }
            vec![("tags", d)]
        },
    },
];

/// Each row's undamaged documents load one entry. The damaged ones answer
/// `OpError::Internal`, the error a failed query gives, instead of loading
/// without the document.
#[tokio::test]
async fn a_stored_document_the_store_cannot_read_fails_the_load() {
    let Some((deployment, store)) = scratch("authz_unreadable").await else {
        return;
    };
    for (i, row) in ROWS.iter().enumerate() {
        for damaged in [false, true] {
            let who = format!("r{i}-{damaged}");
            for (collection, d) in (row.docs)(&who, damaged) {
                deployment
                    .catalog()
                    .collection::<Document>(collection)
                    .insert_one(d)
                    .await
                    .unwrap_or_else(|e| panic!("{}: write to {collection}: {e}", row.name));
            }
        }
        let stored = load(&store, row.load, &format!("r{i}-false")).await;
        assert_eq!(
            stored,
            Ok(1),
            "{}: undamaged, the load reads one entry",
            row.name
        );
        let changed = load(&store, row.load, &format!("r{i}-true")).await;
        assert_eq!(
            changed,
            Err(OpError::Internal(
                "Stored IAM document cannot be read".to_owned()
            )),
            "{}: damaged, the load fails",
            row.name
        );
    }
    deployment.drop_databases().await;
}
