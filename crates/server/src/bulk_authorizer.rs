// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The server's side of the S3 permission checks of bulk operations
//! (`extenddb_engine::bulk_put::authorize`): the caller captured when a
//! `StartBulkPutItems` request is dispatched, and the evaluation of an S3
//! action for a recorded caller over the authorization cache.

use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_core::error::DynamoDbError;
use extenddb_engine::bulk_put::authorize::{
    AuthorizeError, BulkAuthorizer, BulkIdentity, BulkPrincipal, S3Action, SessionSnapshot,
};
use extenddb_storage::authorization_store::AuthorizationStore;
use futures::future::BoxFuture;

use crate::AppState;
use crate::authorization::full_action::{self, authorize_action, denial_is_explicit};
use crate::authz_cache::CachedAuthzStore;

/// Evaluates an S3 action for a recorded bulk operation caller. The
/// identity and group policies, the permissions boundary, the principal tags
/// and the creation times are read through the authorization cache when the
/// check runs, so a policy changed during a load takes effect at the next
/// check. A role session's session policy, session tags and issue time come
/// from the snapshot recorded at Start.
pub(crate) struct ServerBulkAuthorizer {
    cache: Arc<CachedAuthzStore>,
    region: Arc<str>,
    /// Whether every bucket a caller names is in the caller's account, as on
    /// the filesystem store (`{root}/{account}/{bucket}`): then
    /// `aws:ResourceAccount` is the caller's account. On the S3 store the
    /// bucket owner is not known, and the key stays absent.
    buckets_in_caller_account: bool,
}

impl ServerBulkAuthorizer {
    pub(crate) fn new(cache: Arc<CachedAuthzStore>, region: Arc<str>) -> Self {
        Self {
            cache,
            region,
            buckets_in_caller_account: false,
        }
    }

    /// This authorizer for a store whose buckets are all in the caller's
    /// account.
    pub(crate) fn with_buckets_in_caller_account(mut self) -> Self {
        self.buckets_in_caller_account = true;
        self
    }

    /// The authorizer for `state`; `None` in developer mode, where no S3
    /// permission check runs.
    pub(crate) fn for_state(state: &AppState) -> Option<Arc<dyn BulkAuthorizer>> {
        (!state.dev_mode).then(|| {
            let authorizer = Self::new(Arc::clone(&state.authz_cache), Arc::clone(&state.region));
            let filesystem = state
                .bulk
                .as_ref()
                .is_some_and(|store| store.filesystem_root().is_some());
            Arc::new(if filesystem {
                authorizer.with_buckets_in_caller_account()
            } else {
                authorizer
            }) as Arc<dyn BulkAuthorizer>
        })
    }
}

impl BulkAuthorizer for ServerBulkAuthorizer {
    fn authorize<'a>(
        &'a self,
        principal: &'a BulkPrincipal,
        action: S3Action,
        resource_arn: &'a str,
        s3_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AuthorizeError>> {
        Box::pin(async move {
            let identity = auth_identity(&principal.identity);
            let session = match &identity {
                AuthIdentity::RoleSession { .. } => {
                    Some(session_snapshot(principal.session.as_ref())?)
                }
                AuthIdentity::User { .. } => None,
            };
            // The request-level keys are the Start request's; `s3:prefix` is
            // the listing's; `aws:ResourceAccount` is the caller's account
            // where every bucket is in it. The other S3 keys stay absent.
            let params = RequestParams {
                region: Some(self.region.to_string()),
                secure_transport: principal.secure_transport,
                source_ip: principal.source_ip.clone(),
                user_agent: principal.user_agent.clone(),
                s3_prefix: s3_prefix.map(ToOwned::to_owned),
                resource_account: self
                    .buckets_in_caller_account
                    .then(|| account_of(&principal.identity).to_owned()),
                ..Default::default()
            };
            let action_name = action.as_str();
            let decision = Box::pin(authorize_action(
                &self.cache,
                &identity,
                session.as_ref(),
                action_name,
                resource_arn,
                params,
            ))
            .await;
            match decision {
                Ok(()) => Ok(()),
                Err(DynamoDbError::AccessDeniedException(message)) => Err(AuthorizeError::Denied {
                    explicit: denial_is_explicit(&identity, action_name, resource_arn, &message),
                }),
                Err(other) => Err(AuthorizeError::Unavailable(format!("{other:?}"))),
            }
        })
    }
}

/// The account a recorded caller belongs to.
fn account_of(identity: &BulkIdentity) -> &str {
    match identity {
        BulkIdentity::User { account_id, .. } | BulkIdentity::RoleSession { account_id, .. } => {
            account_id
        }
    }
}

fn auth_identity(identity: &BulkIdentity) -> AuthIdentity {
    match identity {
        BulkIdentity::User {
            account_id,
            user_name,
        } => AuthIdentity::User {
            account_id: account_id.clone(),
            user_name: user_name.clone(),
        },
        BulkIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => AuthIdentity::RoleSession {
            account_id: account_id.clone(),
            role_name: role_name.clone(),
            session_name: session_name.clone(),
        },
    }
}

/// The recorded session data in the evaluator's form. A role session
/// recorded without data evaluates with none, as the live path does for a
/// session whose data is gone. A recorded session policy that does not parse
/// denies, the rule the cache applies to a stored policy it cannot parse.
fn session_snapshot(
    recorded: Option<&SessionSnapshot>,
) -> Result<full_action::SessionSnapshot, AuthorizeError> {
    let Some(recorded) = recorded else {
        return Ok(full_action::SessionSnapshot::default());
    };
    let policy = match &recorded.policy {
        None => None,
        Some(text) => match PolicyDocument::from_json(text) {
            Ok(document) => Some(Arc::new(document)),
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "recorded session policy of a bulk operation does not parse; the check denies"
                );
                return Err(AuthorizeError::Denied { explicit: false });
            }
        },
    };
    Ok(full_action::SessionSnapshot {
        policy,
        tags: recorded
            .tags
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        token_issue_time: recorded.token_issue_time.clone(),
    })
}

/// The authorizer and the captured caller for a `StartBulkPutItems`
/// request; `None` for every other operation and in developer mode.
///
/// # Errors
///
/// `InternalServerError` when a role session's data cannot be read.
pub(crate) async fn start_caller(
    state: &AppState,
    identity: &AuthIdentity,
    operation: &str,
    source_ip: Option<&str>,
    user_agent: Option<&str>,
) -> Result<Option<(Arc<dyn BulkAuthorizer>, BulkPrincipal)>, DynamoDbError> {
    if operation != "StartBulkPutItems" {
        return Ok(None);
    }
    let Some(authorizer) = ServerBulkAuthorizer::for_state(state) else {
        return Ok(None);
    };
    let principal = capture_principal(state, identity, source_ip, user_agent).await?;
    Ok(Some((authorizer, principal)))
}

/// The caller as the operation records it: the identity, a role session's
/// session policy text, session tags and issue time as stored, and the
/// request's `aws:SourceIp`, `aws:UserAgent` and `aws:SecureTransport`.
pub(crate) async fn capture_principal(
    state: &AppState,
    identity: &AuthIdentity,
    source_ip: Option<&str>,
    user_agent: Option<&str>,
) -> Result<BulkPrincipal, DynamoDbError> {
    let (identity, session) = match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => (
            BulkIdentity::User {
                account_id: account_id.clone(),
                user_name: user_name.clone(),
            },
            None,
        ),
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => (
            BulkIdentity::RoleSession {
                account_id: account_id.clone(),
                role_name: role_name.clone(),
                session_name: session_name.clone(),
            },
            Some(read_session(state, account_id, role_name, session_name).await?),
        ),
    };
    Ok(BulkPrincipal {
        identity,
        session,
        source_ip: source_ip.map(ToOwned::to_owned),
        user_agent: user_agent.map(ToOwned::to_owned),
        secure_transport: state.tls_enabled,
    })
}

/// A role session's stored data, from the store the authorization cache
/// reads: the cache keeps the session policy parsed, and the operation
/// records the document text.
async fn read_session(
    state: &AppState,
    account_id: &str,
    role_name: &str,
    session_name: &str,
) -> Result<SessionSnapshot, DynamoDbError> {
    let internal = || DynamoDbError::InternalServerError("Internal server error".to_owned());
    let Some(catalog) = state.catalog_store.as_ref() else {
        tracing::error!("bulk operation caller capture: no catalog store");
        return Err(internal());
    };
    let store: &dyn AuthorizationStore = catalog.as_ref();
    match store
        .fetch_session_data(account_id, role_name, session_name)
        .await
    {
        Ok(Some(data)) => Ok(SessionSnapshot {
            policy: data.session_policy,
            tags: data.session_tags.into_iter().collect(),
            token_issue_time: data.created_at,
        }),
        Ok(None) => Ok(SessionSnapshot::default()),
        Err(error) => {
            tracing::error!(
                account_id = %account_id,
                role_name = %role_name,
                error = ?error,
                "bulk operation caller capture: the role session could not be read"
            );
            Err(internal())
        }
    }
}
