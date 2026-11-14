// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `RestoreTableToPointInTime` authorizes the target table's item actions
//! before the restore action on the source (issue #446). The service checks
//! them in that order: a caller with no policy is denied on `dynamodb:Scan`
//! on the target table (measured 2026-10-22 with an index `SourceTableArn`).

use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{AuthzTarget, RESTORE_TARGET_ACTIONS, authz_targets};
use crate::authorization::check_authorization;
use crate::authz_cache::CachedAuthzStore;
use crate::authz_cache::tests::{CountingAuthzStore, fast_authz_cfg};

const R: &str = "us-east-1";
const A: &str = "444455556666";
const SOURCE: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/bb";
const TARGET: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/pitr";

/// The three ways a restore names its source: by name, by table ARN, and by
/// index ARN, the form the service was measured with.
fn inputs() -> [Value; 3] {
    [
        json!({ "SourceTableName": "bb", "TargetTableName": "pitr" }),
        json!({ "SourceTableArn": SOURCE, "TargetTableName": "pitr" }),
        json!({ "SourceTableArn": format!("{SOURCE}/index/gsi1"), "TargetTableName": "pitr" }),
    ]
}

/// The seven item actions on the target table come first, Scan leading, and
/// the restore action on the source table last, however the source is named.
#[test]
fn the_target_item_actions_are_authorized_before_the_restore_action() {
    let expected: Vec<AuthzTarget> = RESTORE_TARGET_ACTIONS
        .iter()
        .map(|a| AuthzTarget::new(a, TARGET.to_owned()))
        .collect();
    for input in inputs() {
        let targets = authz_targets("RestoreTableToPointInTime", &input, R, A);
        let (restore, items) = targets.split_last().expect("restore targets");
        assert_eq!(items, expected.as_slice(), "{input}");
        assert_eq!(items[0].action, "Scan", "{input}");
        assert_eq!(restore.action, "RestoreTableToPointInTime", "{input}");
        assert_eq!(restore.resource, SOURCE, "{input}");
        let named = input["SourceTableArn"].as_str().unwrap_or(SOURCE);
        assert_eq!(restore.supplied, named, "{input}");
    }
}

/// A caller with no policy is denied on `dynamodb:Scan` on the target table,
/// in the service's text, when the targets are checked in the order
/// `authorize_request` checks them, where the first denial is the answer.
#[tokio::test]
async fn a_caller_with_no_policy_is_denied_on_scan_on_the_target_table() {
    let cache = CachedAuthzStore::new(Arc::new(CountingAuthzStore::default()), fast_authz_cfg());
    let identity = AuthIdentity::User {
        account_id: A.to_owned(),
        user_name: "u".to_owned(),
    };
    let expected = format!(
        "User: arn:aws:iam::{A}:user/u is not authorized to perform: dynamodb:Scan on \
         resource: {TARGET} because no identity-based policy allows the dynamodb:Scan action"
    );
    for input in inputs() {
        let mut denial = None;
        for target in authz_targets("RestoreTableToPointInTime", &input, R, A) {
            let checked =
                check_authorization(&cache, &identity, &target, false, RequestParams::default())
                    .await;
            if let Err(e) = checked {
                denial = Some(e);
                break;
            }
        }
        match denial {
            Some(DynamoDbError::AccessDeniedException(m)) => assert_eq!(m, expected, "{input}"),
            other => panic!("expected AccessDeniedException for {input}, got {other:?}"),
        }
    }
}
