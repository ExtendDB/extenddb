// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Role sessions that share a session name, on the request path: each request
//! is judged by the session policy, session tags and issue time of the session
//! whose access key signed it.
//!
//! `AssumeRole` stores a new session for every call, so a role can hold
//! several sessions with one session name. These tests store such sessions in
//! an in-memory `SQLite` catalog and authorize requests through
//! [`check_authorization`] and the authorization cache over that catalog.

use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::secret_keys::SecretKeys;
use extenddb_storage_sqlite::SqliteCatalogStore;
use serde_json::{Value, json};
use sqlx::SqlitePool;

use super::check_authorization;
use crate::authz_cache::CachedAuthzStore;
use crate::authz_cache::tests::fast_authz_cfg;
use crate::authz_resource::AuthzTarget;

const ACCOUNT: &str = "123456789012";
const ROLE: &str = "app";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
/// The session name every session of a test is stored under.
const NAME: &str = "shared-name";

fn policy(statements: Value) -> Value {
    json!({"Version": "2012-10-17", "Statement": statements})
}

/// An in-memory catalog holding the role, with `role_policy` attached, and an
/// authorization cache over it.
struct Fixture {
    catalog: Arc<SqliteCatalogStore>,
    pool: SqlitePool,
    cache: CachedAuthzStore,
}

impl Fixture {
    async fn new(role_policy: Value) -> Self {
        // One connection keeps the in-memory database alive for the test.
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        let migrations = extenddb_storage_sqlite::migrations::SqliteMigrations::on_pool(
            pool.clone(),
            ":memory:",
        )
        .expect("one connection");
        extenddb_storage::migrations::Runner::new(&migrations, "test")
            .migrate()
            .await
            .expect("migrate the catalog");
        // The store writes no session without a key holder.
        let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key()
            .expect("a catalog key");
        let keys = SecretKeys::catalog_key(&key).expect("key holder");
        let catalog = Arc::new(SqliteCatalogStore::with_secret_keys(
            pool.clone(),
            Some(Arc::new(keys)),
            Arc::new(tokio::sync::Mutex::new(())),
        ));
        catalog
            .create_account(ACCOUNT, "sessions")
            .await
            .expect("create the account");
        let trust = policy(json!([{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{ACCOUNT}:root")},
            "Action": "sts:AssumeRole",
        }]));
        catalog
            .create_role(ACCOUNT, ROLE, &trust)
            .await
            .expect("create the role");
        catalog
            .put_policy(ACCOUNT, "role", ROLE, "p", &role_policy)
            .await
            .expect("attach the role policy");
        let cache = CachedAuthzStore::new(catalog.clone(), fast_authz_cfg());
        Self {
            catalog,
            pool,
            cache,
        }
    }

    /// Store a session of the role under [`NAME`] whose access key is `key`,
    /// ending `expires_in` from now.
    async fn session(
        &self,
        key: &str,
        expires_in: time::Duration,
        session_policy: Option<Value>,
        session_tags: Option<Value>,
    ) {
        let now = time::OffsetDateTime::now_utc();
        self.catalog
            .store_session(
                &format!("{key}-token"),
                key,
                b"placeholder for an encrypted secret",
                ACCOUNT,
                ROLE,
                NAME,
                &session_tags,
                &session_policy,
                now.replace_nanosecond(0).unwrap_or(now) + expires_in,
            )
            .await
            .expect("store the session");
    }

    /// Set the issue time the catalog holds for the session with `key`.
    async fn issued_at(&self, key: &str, created_at: &str) {
        sqlx::query("UPDATE iam_sessions SET created_at = ? WHERE access_key_id = ?")
            .bind(created_at)
            .bind(key)
            .execute(&self.pool)
            .await
            .expect("set the issue time");
    }

    /// `ALLOW` or `DENY` for `op` on the table, signed with `key`.
    async fn outcome(&self, key: &str, op: &str) -> &'static str {
        let identity = AuthIdentity::RoleSession {
            account_id: ACCOUNT.to_owned(),
            role_name: ROLE.to_owned(),
            session_name: NAME.to_owned(),
            access_key_id: key.to_owned(),
        };
        match check_authorization(
            &self.cache,
            &identity,
            &AuthzTarget::new(op, TABLE.to_owned()),
            false,
            RequestParams::default(),
        )
        .await
        {
            Ok(()) => "ALLOW",
            Err(DynamoDbError::AccessDeniedException(_)) => "DENY",
            Err(e) => panic!("{op} signed with {key}: {e:?}"),
        }
    }

    /// The outcomes of a `GetItem` and a `PutItem` signed with `key`.
    async fn outcomes(&self, key: &str) -> [&'static str; 2] {
        [
            self.outcome(key, "GetItem").await,
            self.outcome(key, "PutItem").await,
        ]
    }
}

fn allow_get_and_put() -> Value {
    policy(json!([{
        "Effect": "Allow",
        "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
        "Resource": "*",
    }]))
}

fn allow_only(action: &str) -> Value {
    policy(json!([{"Effect": "Allow", "Action": action, "Resource": "*"}]))
}

/// A session stored after an expired session of its name is judged by its own
/// session policy.
#[tokio::test]
async fn a_session_after_an_expired_one_of_its_name_has_its_own_policy() {
    let f = Fixture::new(allow_get_and_put()).await;
    f.session(
        "ASIAEXPIRED",
        time::Duration::hours(-1),
        Some(allow_only("dynamodb:PutItem")),
        None,
    )
    .await;
    f.session(
        "ASIALIVE",
        time::Duration::hours(1),
        Some(allow_only("dynamodb:GetItem")),
        None,
    )
    .await;
    assert_eq!(f.outcomes("ASIALIVE").await, ["ALLOW", "DENY"]);
}

/// Two live sessions of one name are each judged by their own session policy,
/// also once the first one's data is cached.
#[tokio::test]
async fn live_sessions_of_one_name_each_have_their_own_policy() {
    let f = Fixture::new(allow_get_and_put()).await;
    f.session(
        "ASIAREADER",
        time::Duration::hours(1),
        Some(allow_only("dynamodb:GetItem")),
        None,
    )
    .await;
    f.session(
        "ASIAWRITER",
        time::Duration::hours(1),
        Some(allow_only("dynamodb:PutItem")),
        None,
    )
    .await;
    assert_eq!(f.outcomes("ASIAREADER").await, ["ALLOW", "DENY"]);
    assert_eq!(f.outcomes("ASIAWRITER").await, ["DENY", "ALLOW"]);
}

/// Two live sessions of one name are each judged by their own session tags.
#[tokio::test]
async fn live_sessions_of_one_name_each_have_their_own_tags() {
    let tagged = |action: &str, team: &str| {
        json!({
            "Effect": "Allow",
            "Action": action,
            "Resource": "*",
            "Condition": {"StringEquals": {"aws:PrincipalTag/team": team}},
        })
    };
    let f = Fixture::new(policy(json!([
        tagged("dynamodb:GetItem", "readers"),
        tagged("dynamodb:PutItem", "writers"),
    ])))
    .await;
    f.session(
        "ASIAREADER",
        time::Duration::hours(1),
        None,
        Some(json!([{"Key": "team", "Value": "readers"}])),
    )
    .await;
    f.session(
        "ASIAWRITER",
        time::Duration::hours(1),
        None,
        Some(json!([{"Key": "team", "Value": "writers"}])),
    )
    .await;
    assert_eq!(f.outcomes("ASIAREADER").await, ["ALLOW", "DENY"]);
    assert_eq!(f.outcomes("ASIAWRITER").await, ["DENY", "ALLOW"]);
}

/// Two live sessions of one name are each judged by their own issue time
/// (`aws:TokenIssueTime`), here under a Deny of the sessions issued before a
/// cut-off between the two issue times.
#[tokio::test]
async fn live_sessions_of_one_name_each_have_their_own_issue_time() {
    let f = Fixture::new(policy(json!([
        {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem"], "Resource": "*"},
        {
            "Effect": "Deny",
            "Action": "*",
            "Resource": "*",
            "Condition": {"DateLessThan": {"aws:TokenIssueTime": "2026-03-01T00:00:00Z"}},
        },
    ])))
    .await;
    f.session("ASIAOLDER", time::Duration::hours(1), None, None)
        .await;
    f.session("ASIANEWER", time::Duration::hours(1), None, None)
        .await;
    f.issued_at("ASIAOLDER", "2026-01-01T00:00:00.000Z").await;
    f.issued_at("ASIANEWER", "2026-06-01T00:00:00.000Z").await;
    assert_eq!(f.outcomes("ASIAOLDER").await, ["DENY", "DENY"]);
    assert_eq!(f.outcomes("ASIANEWER").await, ["ALLOW", "ALLOW"]);
}
