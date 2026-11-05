// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Behaviour tests for the bulk operation authorization rules, written from
//! the specification's Authorization table.
//!
//! Every test drives `authorize_request` itself against a real `AppState`
//! (in-memory `SQLite` storage, a fresh authorization cache per test), so
//! the resource derivation, the pre-evaluation ARN checks, and the dev-mode
//! bypass are exercised through the same path a request takes. The
//! authorization store underneath the cache is a test double that serves
//! one canned policy set for the test principal and counts every fetch, so
//! "no policy was evaluated" is asserted as zero fetches against a cache
//! built fresh for the test.
//!
//! The cases live in `start` (StartBulkPutItems grants, table ARN spellings,
//! dev mode) and `arn` (Describe, Cancel and List against operation ARNs).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::HeaderMap;
use extenddb_auth::{AuthCacheRegistry, AuthIdentity, AuthProvider};
use extenddb_cache::SwrCacheConfig;
use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::throttle::ThrottleManager;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::OpResult;
use extenddb_storage::server_components::ServerComponentsOptions;
use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::AppState;
use crate::authz_cache::{AuthzCacheConfig, CachedAuthzStore};
use crate::key_info_cache::CachedTableKeyInfoStore;
use crate::request_helpers::authorize_request;

mod arn;
mod conditions;
mod s3_actions;
mod start;

pub(super) const REGION: &str = "us-east-1";

pub(super) const ACCOUNT: &str = "123456789012";

pub(super) const OTHER_ACCOUNT: &str = "999999999999";

pub(super) const USER: &str = "tester";

pub(super) const TABLE: &str = "orders";

pub(super) const OTHER_TABLE: &str = "invoices";

/// A well-formed operation id, so foreign-account ARNs pass layers 1 and 2.
pub(super) const OPERATION_ID: &str = "01789060589767-da9ddd4f";

/// Layer 1 of the operation ARN validation.
pub(super) const NOT_AN_ARN: &str = "Invalid Bulk operation ARN";
/// Layer 2 of the operation ARN validation.
pub(super) const BAD_ID: &str = "Invalid Request: Bulk operation ARN is invalid.";

pub(super) fn table_arn(account: &str, table: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:table/{table}")
}

pub(super) fn operation_arn(region: &str, account: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account}:bulk-operation/{OPERATION_ID}")
}

/// Authentication is upstream of everything these tests drive; the provider
/// only fills the `AppState` field.
pub(super) struct NeverAuth;

#[async_trait::async_trait]

impl AuthProvider for NeverAuth {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        unreachable!("authorization tests never authenticate")
    }
}

/// Serves one canned policy list for the test principal and counts every
/// fetch. The cache above it is built fresh per test, so a fetch count of
/// zero after a call means no policy evaluation path ran at all.
pub(super) struct CountingAuthzStore {
    policies: Vec<String>,
    fetches: Arc<AtomicU64>,
}

impl CountingAuthzStore {
    fn count(&self) -> u64 {
        self.fetches.fetch_add(1, Ordering::SeqCst)
    }
}

impl AuthorizationStore for CountingAuthzStore {
    fn fetch_user_policies(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        self.count();
        let policies = self.policies.clone();
        Box::pin(async move { Ok(policies) })
    }

    fn fetch_user_group_policies(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        self.count();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_user_boundary(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.count();
        Box::pin(async { Ok(None) })
    }

    fn fetch_role_policies(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        self.count();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_role_boundary(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.count();
        Box::pin(async { Ok(None) })
    }

    fn fetch_session_data(
        &self,
        _account_id: &str,
        _role_name: &str,
        _session_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        self.count();
        Box::pin(async { Ok(None) })
    }

    fn fetch_user_tags(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        self.count();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_role_tags(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        self.count();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_resource_tags(&self, _arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        self.count();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_user_created_at(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.count();
        Box::pin(async { Ok(None) })
    }

    fn fetch_role_created_at(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.count();
        Box::pin(async { Ok(None) })
    }
}

/// An `AppState` whose authorization cache serves exactly `policies` for the
/// test principal, plus the fetch counter that proves whether any policy
/// evaluation path ran.
pub(super) async fn state_with_policies(
    policies: Vec<String>,
    dev_mode: bool,
) -> (AppState, Arc<AtomicU64>) {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        REGION,
        ServerComponentsOptions::default(),
    )
    .await
    .expect("server components");
    let storage = components.engine;

    let fetches = Arc::new(AtomicU64::new(0));
    let counting = Arc::new(CountingAuthzStore {
        policies,
        fetches: Arc::clone(&fetches),
    });
    let authz_cache = Arc::new(CachedAuthzStore::new(counting, AuthzCacheConfig::default()));
    let table_key_info_cache = Arc::new(CachedTableKeyInfoStore::new(
        Arc::clone(&storage),
        SwrCacheConfig::default(),
    ));

    let limits = Arc::new(LimitsConfig::default());
    let state = AppState {
        storage,
        backup_runtime: Arc::new(extenddb_engine::BackupRuntime::new(4)),
        auth: Arc::new(NeverAuth),
        limits: Arc::clone(&limits),
        region: Arc::from(REGION),
        server_addr: "localhost:0".to_owned(),
        catalog_store: Some(components.catalog_store),
        version_info: Arc::from("test"),
        metrics: Arc::new(MetricsCollector::new()),
        tls_enabled: false,
        dev_mode,
        import_paths: Vec::new().into(),
        export_paths: Vec::new().into(),
        bulk: None,
        throttle: Arc::new(ThrottleManager::new(
            limits.per_account_max_rcu,
            limits.per_account_max_wcu,
            false,
        )),
        auth_cache: AuthCacheRegistry::empty(),
        authz_cache,
        table_key_info_cache,
        config_entries: Vec::new(),
        docs_store: None,
        admission: crate::registration::Admission::default(),
    };
    (state, fetches)
}

pub(super) fn identity() -> AuthIdentity {
    AuthIdentity::User {
        account_id: ACCOUNT.to_owned(),
        user_name: USER.to_owned(),
    }
}

pub(super) async fn authorize(
    state: &AppState,
    operation: &str,
    body: &Value,
) -> Result<(), DynamoDbError> {
    // The authorize_request future is around 20 KB; boxing it here keeps
    // every await in this file under clippy's large_futures bar.
    Box::pin(authorize_request(
        state,
        &identity(),
        body,
        operation,
        ACCOUNT,
        None,
        None,
    ))
    .await
    .map(|_key_info| ())
}

pub(super) fn allow(action: &str, resource: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": action, "Resource": resource}]
    })
    .to_string()
}

pub(super) fn allow_all_with_deny(deny_action: &str, deny_resource: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": deny_action, "Resource": deny_resource}
        ]
    })
    .to_string()
}

pub(super) fn start_body(table_name: &str) -> Value {
    json!({ "TableName": table_name })
}

pub(super) fn arn_body(operation_arn: &str) -> Value {
    json!({ "BulkOperationArn": operation_arn })
}

pub(super) fn assert_denied(result: Result<(), DynamoDbError>, what: &str) {
    match result {
        Err(DynamoDbError::AccessDeniedException(_)) => {}
        other => panic!("{what}: expected AccessDeniedException, got {other:?}"),
    }
}

pub(super) fn validation_message(result: Result<(), DynamoDbError>, what: &str) -> String {
    match result {
        Err(DynamoDbError::ValidationException(message)) => message,
        other => panic!("{what}: expected ValidationException, got {other:?}"),
    }
}

// StartBulkPutItems: two actions, both required. The account-scoped
// dynamodb:StartBulkOperation is evaluated against the bulk-operation
// wildcard, then the table-scoped dynamodb:BulkPutItems against the table
// ARN, with the own-ARN spelling of TableName resolved to the bare name
// before evaluation.

/// A policy carrying both Start grants: the control-plane action on the
/// account's bulk-operation wildcard and the data-plane action on `table`.
pub(super) fn start_grants(table: &str) -> Vec<String> {
    vec![
        allow("dynamodb:StartBulkOperation", &operation_wildcard()),
        allow("dynamodb:BulkPutItems", &table_arn(ACCOUNT, table)),
    ]
}

pub(super) fn operation_wildcard() -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*")
}

pub(super) fn denial_message(result: Result<(), DynamoDbError>, what: &str) -> String {
    match result {
        Err(DynamoDbError::AccessDeniedException(message)) => message,
        other => panic!("{what}: expected AccessDeniedException, got {other:?}"),
    }
}
