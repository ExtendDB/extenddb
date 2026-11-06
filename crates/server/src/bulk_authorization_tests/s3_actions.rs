// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The server's bulk operation authorizer: an S3 action evaluated for a
//! caller recorded at Start, against the caller's identity policies and
//! permissions boundary read through the cache, with a role session's policy,
//! tags and issue time taken from the recording.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_engine::bulk_put::authorize::{
    AuthorizeError, BulkAuthorizer, BulkIdentity, BulkPrincipal, S3Action, SessionSnapshot,
};
use extenddb_engine::bulk_put::store::BulkStore;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::OpResult;
use futures::future::BoxFuture;
use serde_json::{Value, json};

use super::{ACCOUNT, state_with_policies};
use crate::authz_cache::{AuthzCacheConfig, CachedAuthzStore};
use crate::bulk_authorizer::{ServerBulkAuthorizer, capture_principal, start_caller};

/// Serves one policy set for every principal: the identity policies, an
/// optional permissions boundary, the role's tags, and the session's current
/// data (which an evaluation of a recorded caller must not read).
#[derive(Default)]
struct Store {
    policies: Vec<String>,
    boundary: Option<String>,
    role_tags: Vec<(String, String)>,
    live_session: Option<SessionData>,
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, OpResult<T>> {
    Box::pin(async move { Ok(value) })
}

impl AuthorizationStore for Store {
    fn fetch_user_policies(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<String>>> {
        ready(self.policies.clone())
    }

    fn fetch_user_group_policies(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<String>>> {
        ready(Vec::new())
    }

    fn fetch_user_boundary(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        ready(self.boundary.clone())
    }

    fn fetch_role_policies(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<String>>> {
        ready(self.policies.clone())
    }

    fn fetch_role_boundary(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        ready(self.boundary.clone())
    }

    fn fetch_session_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        ready(self.live_session.clone())
    }

    fn fetch_user_tags(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        ready(Vec::new())
    }

    fn fetch_role_tags(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        ready(self.role_tags.clone())
    }

    fn fetch_resource_tags(&self, _: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        ready(Vec::new())
    }

    fn fetch_user_created_at(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        ready(None)
    }

    fn fetch_role_created_at(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        ready(None)
    }
}

fn authorizer(store: Store) -> ServerBulkAuthorizer {
    let cache = CachedAuthzStore::new(Arc::new(store), AuthzCacheConfig::default());
    ServerBulkAuthorizer::new(Arc::new(cache), Arc::from("us-east-1"))
}

fn policy(statements: &[Value]) -> String {
    json!({"Version": "2012-10-17", "Statement": statements}).to_string()
}

fn allow(action: &str, resource: &str) -> Value {
    json!({"Effect": "Allow", "Action": action, "Resource": resource})
}

fn user() -> BulkPrincipal {
    BulkPrincipal {
        identity: BulkIdentity::User {
            account_id: ACCOUNT.to_owned(),
            user_name: "u".to_owned(),
        },
        session: None,
        source_ip: None,
        user_agent: None,
        secure_transport: false,
    }
}

fn role(session: Option<SessionSnapshot>) -> BulkPrincipal {
    BulkPrincipal {
        identity: BulkIdentity::RoleSession {
            account_id: ACCOUNT.to_owned(),
            role_name: "r".to_owned(),
            session_name: "s".to_owned(),
        },
        session,
        source_ip: None,
        user_agent: None,
        secure_transport: false,
    }
}

async fn decide(
    authorizer: &ServerBulkAuthorizer,
    principal: &BulkPrincipal,
    action: S3Action,
    resource: &str,
) -> Result<(), AuthorizeError> {
    authorizer
        .authorize(principal, action, resource, None)
        .await
}

const IMPLICIT: Result<(), AuthorizeError> = Err(AuthorizeError::Denied { explicit: false });
const EXPLICIT: Result<(), AuthorizeError> = Err(AuthorizeError::Denied { explicit: true });

#[tokio::test]
async fn get_object_is_allowed_in_the_granted_bucket_only() {
    let a = authorizer(Store {
        policies: vec![policy(&[allow("s3:GetObject", "arn:aws:s3:::b/*")])],
        ..Store::default()
    });
    for who in [user(), role(None)] {
        let get = |resource: &'static str| decide(&a, &who, S3Action::GetObject, resource);
        assert_eq!(get("arn:aws:s3:::b/k").await, Ok(()), "{who:?}");
        assert_eq!(get("arn:aws:s3:::other/k").await, IMPLICIT, "{who:?}");
        assert_eq!(
            decide(&a, &who, S3Action::PutObject, "arn:aws:s3:::b/k").await,
            IMPLICIT,
            "{who:?}"
        );
    }
}

/// In a `2012-10-17` document a policy variable in `Resource` is expanded
/// from the recorded caller, as for the DynamoDB actions, so the statement
/// grants the user the folder named after it, report keys with a `:`
/// included. A role session has no `aws:username`, so it gets nothing. A
/// `2008-10-17` document matches the pattern as written.
#[tokio::test]
async fn a_resource_policy_variable_names_the_callers_own_folder() {
    async fn get(a: &ServerBulkAuthorizer, resource: &str) -> Result<(), AuthorizeError> {
        decide(a, &user(), S3Action::GetObject, resource).await
    }
    let statement = allow("s3:GetObject", "arn:aws:s3:::b/home/${aws:username}/*");
    let expanding = authorizer(Store {
        policies: vec![policy(&[
            statement.clone(),
            allow("s3:PutObject", "arn:aws:s3:::b/home/${aws:username}/*"),
        ])],
        ..Store::default()
    });
    assert_eq!(get(&expanding, "arn:aws:s3:::b/home/u/k").await, Ok(()));
    assert_eq!(
        get(&expanding, "arn:aws:s3:::b/home/other/k").await,
        IMPLICIT
    );
    assert_eq!(
        get(&expanding, "arn:aws:s3:::b/home/${aws:username}/k").await,
        IMPLICIT
    );
    assert_eq!(
        decide(
            &expanding,
            &role(None),
            S3Action::GetObject,
            "arn:aws:s3:::b/home/u/k"
        )
        .await,
        IMPLICIT
    );
    // A failed-row report key: its operation folder holds a `:`.
    let report = "AWSDynamoDB/BulkOperation/123456789012:op-0/output/errors/output-00000.json.gz";
    for (folder, expected) in [("u", Ok(())), ("other", IMPLICIT)] {
        let resource = format!("arn:aws:s3:::b/home/{folder}/{report}");
        assert_eq!(
            decide(&expanding, &user(), S3Action::PutObject, &resource).await,
            expected,
            "{resource}"
        );
    }

    let literal = authorizer(Store {
        policies: vec![json!({"Version": "2008-10-17", "Statement": [statement]}).to_string()],
        ..Store::default()
    });
    assert_eq!(get(&literal, "arn:aws:s3:::b/home/u/k").await, IMPLICIT);
    assert_eq!(
        get(&literal, "arn:aws:s3:::b/home/${aws:username}/k").await,
        Ok(())
    );
}

#[tokio::test]
async fn list_bucket_is_decided_on_the_s3_prefix() {
    let a = authorizer(Store {
        policies: vec![policy(&[json!({
            "Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::b",
            "Condition": {"StringLike": {"s3:prefix": "loads/*"}}
        })])],
        ..Store::default()
    });
    for who in [user(), role(None)] {
        let list = |prefix: &'static str| {
            a.authorize(&who, S3Action::ListBucket, "arn:aws:s3:::b", Some(prefix))
        };
        assert_eq!(list("loads/").await, Ok(()), "{who:?}");
        assert_eq!(list("other/").await, IMPLICIT, "{who:?}");
        assert_eq!(list("").await, IMPLICIT, "{who:?}");
    }
}

#[tokio::test]
async fn a_denial_reports_whether_a_deny_statement_matched() {
    let a = authorizer(Store {
        policies: vec![policy(&[
            allow("s3:*", "*"),
            json!({"Effect": "Deny", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::b/private/*"}),
        ])],
        ..Store::default()
    });
    for who in [user(), role(None)] {
        let put = |resource: &'static str| decide(&a, &who, S3Action::PutObject, resource);
        assert_eq!(put("arn:aws:s3:::b/private/k").await, EXPLICIT, "{who:?}");
        assert_eq!(put("arn:aws:s3:::b/public/k").await, Ok(()), "{who:?}");
    }
    let none = authorizer(Store::default());
    assert_eq!(
        decide(&none, &user(), S3Action::GetObject, "arn:aws:s3:::b/k").await,
        IMPLICIT
    );
}

#[tokio::test]
async fn a_recorded_session_policy_narrows_the_role() {
    let a = authorizer(Store {
        policies: vec![policy(&[allow("s3:*", "*")])],
        ..Store::default()
    });
    let narrowed = role(Some(SessionSnapshot {
        policy: Some(policy(&[allow("s3:GetObject", "arn:aws:s3:::b/*")])),
        ..SessionSnapshot::default()
    }));
    let get = decide(&a, &narrowed, S3Action::GetObject, "arn:aws:s3:::b/k").await;
    assert_eq!(get, Ok(()));
    let put = decide(&a, &narrowed, S3Action::PutObject, "arn:aws:s3:::b/k").await;
    assert_eq!(put, IMPLICIT);
    let other = decide(&a, &narrowed, S3Action::GetObject, "arn:aws:s3:::c/k").await;
    assert_eq!(other, IMPLICIT);
}

#[tokio::test]
async fn the_recorded_session_is_evaluated_in_place_of_the_current_one() {
    // The session's current data carries a policy that allows nothing; the
    // recording, made at Start, carries none. The recording decides.
    let a = authorizer(Store {
        policies: vec![policy(&[allow("s3:*", "*")])],
        live_session: Some(SessionData {
            session_policy: Some(policy(&[allow("dynamodb:GetItem", "*")])),
            session_tags: Vec::new(),
            created_at: None,
        }),
        ..Store::default()
    });
    let recorded = role(Some(SessionSnapshot::default()));
    let get = decide(&a, &recorded, S3Action::GetObject, "arn:aws:s3:::b/k").await;
    assert_eq!(
        get,
        Ok(()),
        "the recorded session has no policy to narrow it"
    );
    // A role recorded with no session data evaluates with none as well.
    assert_eq!(
        decide(&a, &role(None), S3Action::GetObject, "arn:aws:s3:::b/k").await,
        Ok(())
    );
}

#[tokio::test]
async fn recorded_session_tags_are_the_principal_tags() {
    let a = authorizer(Store {
        policies: vec![policy(&[json!({
            "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*",
            "Condition": {"StringEquals": {"aws:PrincipalTag/team": "a"}}
        })])],
        role_tags: vec![("team".to_owned(), "b".to_owned())],
        live_session: Some(SessionData {
            session_policy: None,
            session_tags: vec![("team".to_owned(), "b".to_owned())],
            created_at: None,
        }),
        ..Store::default()
    });
    let tagged = |team: &str| {
        role(Some(SessionSnapshot {
            tags: BTreeMap::from([("team".to_owned(), team.to_owned())]),
            ..SessionSnapshot::default()
        }))
    };
    let resource = "arn:aws:s3:::b/k";
    assert_eq!(
        decide(&a, &tagged("a"), S3Action::GetObject, resource).await,
        Ok(()),
        "the recorded session tag wins over the role's tag"
    );
    assert_eq!(
        decide(&a, &tagged("b"), S3Action::GetObject, resource).await,
        IMPLICIT
    );
    assert_eq!(
        decide(&a, &role(None), S3Action::GetObject, resource).await,
        IMPLICIT,
        "with no recorded tag the role's own tag applies"
    );
}

#[tokio::test]
async fn a_permissions_boundary_limits_the_caller() {
    let a = authorizer(Store {
        policies: vec![policy(&[allow("s3:*", "*")])],
        boundary: Some(policy(&[allow("s3:GetObject", "*")])),
        ..Store::default()
    });
    for who in [user(), role(None)] {
        let get = decide(&a, &who, S3Action::GetObject, "arn:aws:s3:::b/k").await;
        assert_eq!(get, Ok(()), "{who:?}");
        let put = decide(&a, &who, S3Action::PutObject, "arn:aws:s3:::b/k").await;
        assert_eq!(put, IMPLICIT, "{who:?}");
    }
}

#[tokio::test]
async fn a_recorded_session_policy_that_does_not_parse_denies() {
    let a = authorizer(Store {
        policies: vec![policy(&[allow("s3:*", "*")])],
        ..Store::default()
    });
    let broken = role(Some(SessionSnapshot {
        policy: Some("{not a policy".to_owned()),
        ..SessionSnapshot::default()
    }));
    let get = decide(&a, &broken, S3Action::GetObject, "arn:aws:s3:::b/k").await;
    assert_eq!(get, IMPLICIT);
}

#[tokio::test]
async fn request_level_keys_are_the_recorded_start_requests() {
    let a = authorizer(Store {
        policies: vec![policy(&[json!({
            "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*",
            "Condition": {
                "IpAddress": {"aws:SourceIp": "10.0.0.0/8"},
                "Bool": {"aws:SecureTransport": "true"},
                "StringEquals": {"aws:UserAgent": "loader/1"}
            }
        })])],
        ..Store::default()
    });
    let from = |ip: &str, secure: bool, agent: &str| BulkPrincipal {
        source_ip: Some(ip.to_owned()),
        user_agent: Some(agent.to_owned()),
        secure_transport: secure,
        ..user()
    };
    let resource = "arn:aws:s3:::b/k";
    for (principal, expected) in [
        (from("10.1.2.3", true, "loader/1"), Ok(())),
        (from("192.168.1.1", true, "loader/1"), IMPLICIT),
        (from("10.1.2.3", false, "loader/1"), IMPLICIT),
        (from("10.1.2.3", true, "other"), IMPLICIT),
    ] {
        let decision = decide(&a, &principal, S3Action::GetObject, resource).await;
        assert_eq!(decision, expected, "{principal:?}");
    }
}

#[tokio::test]
async fn start_records_a_user_with_the_requests_keys() {
    let (state, _) = state_with_policies(Vec::new(), false).await;
    let identity = AuthIdentity::User {
        account_id: ACCOUNT.to_owned(),
        user_name: "u".to_owned(),
    };
    let principal = capture_principal(&state, &identity, Some("10.1.2.3"), Some("ua"))
        .await
        .expect("capture");
    assert_eq!(
        principal,
        BulkPrincipal {
            source_ip: Some("10.1.2.3".to_owned()),
            user_agent: Some("ua".to_owned()),
            secure_transport: state.tls_enabled,
            ..user()
        }
    );
}

#[tokio::test]
async fn only_a_start_outside_developer_mode_carries_a_caller() {
    let identity = AuthIdentity::User {
        account_id: ACCOUNT.to_owned(),
        user_name: "u".to_owned(),
    };
    let (state, _) = state_with_policies(Vec::new(), false).await;
    let caller = start_caller(&state, &identity, "StartBulkPutItems", None, None)
        .await
        .expect("capture");
    assert!(caller.is_some_and(|(_, principal)| principal == user()));
    let other = start_caller(&state, &identity, "DescribeBulkOperation", None, None)
        .await
        .expect("capture");
    assert!(other.is_none(), "only Start records a caller");

    let (dev, _) = state_with_policies(Vec::new(), true).await;
    assert!(ServerBulkAuthorizer::for_state(&dev).is_none());
    let caller = start_caller(&dev, &identity, "StartBulkPutItems", None, None)
        .await
        .expect("capture");
    assert!(caller.is_none(), "developer mode runs no S3 check");
}

#[tokio::test]
async fn start_records_a_role_sessions_stored_policy_and_tags() {
    let (state, _) = state_with_policies(Vec::new(), false).await;
    let catalog = state.catalog_store.clone().expect("a catalog");
    catalog.create_account(ACCOUNT, "s3-actions").await.unwrap();
    catalog
        .create_role(
            ACCOUNT,
            "r",
            &json!({"Version": "2012-10-17", "Statement": []}),
        )
        .await
        .unwrap();
    let session_policy =
        json!({"Version": "2012-10-17", "Statement": [allow("s3:GetObject", "*")]});
    catalog
        .store_session(
            "token-s3-actions",
            "ASIAS3ACTIONSTEST001",
            b"secret",
            ACCOUNT,
            "r",
            "s",
            &Some(json!({"team": "a"})),
            &Some(session_policy.clone()),
            time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        )
        .await
        .unwrap();
    let identity = AuthIdentity::RoleSession {
        account_id: ACCOUNT.to_owned(),
        role_name: "r".to_owned(),
        session_name: "s".to_owned(),
    };
    let principal = capture_principal(&state, &identity, None, None)
        .await
        .expect("capture");
    let session = principal.session.expect("a role session records its data");
    let recorded: Value =
        serde_json::from_str(session.policy.as_deref().expect("a policy")).unwrap();
    assert_eq!(recorded, session_policy);
    assert_eq!(
        session.tags,
        BTreeMap::from([("team".to_owned(), "a".to_owned())])
    );
    assert!(session.token_issue_time.is_some());
}

/// The data perimeter Deny: any S3 action on a resource whose account is not
/// the caller's.
fn perimeter_policy() -> String {
    policy(&[
        allow("s3:*", "*"),
        json!({
            "Effect": "Deny",
            "Action": "s3:*",
            "Resource": "*",
            "Condition": {"StringNotEquals": {"aws:ResourceAccount": "${aws:PrincipalAccount}"}}
        }),
    ])
}

#[tokio::test]
async fn on_the_filesystem_store_the_resource_account_is_the_callers() {
    let fs = authorizer(Store {
        policies: vec![perimeter_policy()],
        ..Store::default()
    })
    .with_buckets_in_caller_account();
    for action in [
        S3Action::ListBucket,
        S3Action::GetObject,
        S3Action::PutObject,
    ] {
        assert_eq!(
            fs.authorize(&user(), action, "arn:aws:s3:::b/k", Some(""))
                .await,
            Ok(()),
            "{action:?}"
        );
    }
    // A Null check sees the key present.
    let present = authorizer(Store {
        policies: vec![policy(&[json!({
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*",
            "Condition": {"Null": {"aws:ResourceAccount": "false"}}
        })])],
        ..Store::default()
    })
    .with_buckets_in_caller_account();
    assert_eq!(
        present
            .authorize(&role(None), S3Action::GetObject, "arn:aws:s3:::b/k", None)
            .await,
        Ok(())
    );
}

#[tokio::test]
async fn on_the_s3_store_the_resource_account_stays_absent() {
    let s3 = authorizer(Store {
        policies: vec![perimeter_policy()],
        ..Store::default()
    });
    assert_eq!(
        s3.authorize(&user(), S3Action::GetObject, "arn:aws:s3:::b/k", None)
            .await,
        Err(AuthorizeError::Denied { explicit: true })
    );
}

/// On the S3 store `aws:ResourceAccount` is absent, so a `Resource` or
/// `NotResource` pattern that names it matches no resource: a Deny with it in
/// `Resource` never applies, an Allow never grants, and a Deny with it in
/// `NotResource` applies to every check. On the filesystem store the key is
/// the caller's account, and the same pattern names the caller's buckets.
#[tokio::test]
async fn a_resource_account_variable_in_a_resource_matches_nothing_on_the_s3_store() {
    let pattern = "arn:aws:s3:::${aws:ResourceAccount}-*";
    let deny_in = policy(&[
        allow("s3:*", "*"),
        json!({"Effect": "Deny", "Action": "s3:*", "Resource": pattern}),
    ]);
    let allow_in = policy(&[allow("s3:GetObject", pattern)]);
    let deny_not_in = policy(&[
        allow("s3:*", "*"),
        json!({"Effect": "Deny", "Action": "s3:*", "NotResource": pattern}),
    ]);
    let resource = format!("arn:aws:s3:::{ACCOUNT}-b/k");
    for (document, on_s3, on_filesystem) in [
        (deny_in, Ok(()), EXPLICIT),
        (allow_in, IMPLICIT, Ok(())),
        (deny_not_in, EXPLICIT, Ok(())),
    ] {
        let s3 = authorizer(Store {
            policies: vec![document.clone()],
            ..Store::default()
        });
        let filesystem = authorizer(Store {
            policies: vec![document.clone()],
            ..Store::default()
        })
        .with_buckets_in_caller_account();
        assert_eq!(
            decide(&s3, &user(), S3Action::GetObject, &resource).await,
            on_s3,
            "S3 store: {document}"
        );
        assert_eq!(
            decide(&filesystem, &user(), S3Action::GetObject, &resource).await,
            on_filesystem,
            "filesystem store: {document}"
        );
    }
}

#[tokio::test]
async fn the_server_sets_the_resource_account_only_for_the_filesystem_store() {
    let (mut state, _) = state_with_policies(vec![perimeter_policy()], false).await;
    let get = |authorizer: Arc<dyn BulkAuthorizer>| async move {
        authorizer
            .authorize(&user(), S3Action::GetObject, "arn:aws:s3:::b/k", None)
            .await
    };
    // A store with no directory tree (the S3 store has none) leaves the key
    // absent.
    let no_tree = ServerBulkAuthorizer::for_state(&state).expect("not developer mode");
    assert_eq!(
        get(no_tree).await,
        Err(AuthorizeError::Denied { explicit: true })
    );
    state.bulk = Some(Arc::new(BulkStore::filesystem(Arc::new(PathBuf::from(
        "/nonexistent-bulk-root",
    )))));
    let fs = ServerBulkAuthorizer::for_state(&state).expect("not developer mode");
    assert_eq!(get(fs).await, Ok(()));
}
