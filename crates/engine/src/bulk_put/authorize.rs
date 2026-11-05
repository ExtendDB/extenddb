// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 actions a bulk operation performs as its caller, evaluated against
//! the caller's ExtendDB policies: `s3:ListBucket` on the input bucket at
//! Start, `s3:GetObject` on each input object when it is opened and at every
//! checkpoint boundary while it is read, and `s3:PutObject` on each report
//! object and on the manifest. The service reports a denial of each as
//! `S3AccessDenied`; the evaluation runs on every store, so the filesystem
//! store answers the same denials.
//!
//! The caller is captured once, by the server at Start, as a
//! [`BulkPrincipal`], and recorded on the operation; the runner evaluates the
//! recorded caller. The server implements [`BulkAuthorizer`] over its policy
//! cache. In developer mode there is no authorizer and every check is
//! skipped, as authorization is for every other operation.

use std::collections::BTreeMap;

use extenddb_storage::BoxedFuture as BoxFuture;
use serde::{Deserialize, Serialize};

/// An S3 action a bulk operation performs as its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Action {
    ListBucket,
    GetObject,
    PutObject,
}

impl S3Action {
    /// The full action string a policy statement names.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ListBucket => "s3:ListBucket",
            Self::GetObject => "s3:GetObject",
            Self::PutObject => "s3:PutObject",
        }
    }
}

/// The authenticated identity of a bulk operation's caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BulkIdentity {
    User {
        account_id: String,
        user_name: String,
    },
    RoleSession {
        account_id: String,
        role_name: String,
        session_name: String,
    },
}

/// What a role session carried when Start ran: its inline session policy (the
/// stored document text), its session tags and its issue time. The runner
/// evaluates these instead of reading the session again, so a session that
/// expires or is removed during a long load neither widens the caller's
/// access nor fails the load.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_issue_time: Option<String>,
}

/// The caller of a `StartBulkPutItems` request as the server captured it,
/// with the request-level condition key values of that request
/// (`aws:SourceIp`, `aws:UserAgent`, `aws:SecureTransport`). Identity
/// policies, group policies, the permissions boundary and principal tags are
/// not part of it: they are read when each check runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkPrincipal {
    pub identity: BulkIdentity,
    /// Set for a role session, `None` for an IAM user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub secure_transport: bool,
}

impl BulkPrincipal {
    /// The principal ARN as S3 spells it in a denial: an IAM user's ARN, or
    /// the `sts` assumed-role ARN of a role session.
    #[must_use]
    pub fn arn(&self) -> String {
        match &self.identity {
            BulkIdentity::User {
                account_id,
                user_name,
            } => format!("arn:aws:iam::{account_id}:user/{user_name}"),
            BulkIdentity::RoleSession {
                account_id,
                role_name,
                session_name,
            } => format!("arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"),
        }
    }
}

/// Why an evaluation did not allow the action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizeError {
    /// The caller's policies do not allow it; `explicit` when a Deny
    /// statement matched.
    Denied { explicit: bool },
    /// The decision could not be made, for example because a policy could
    /// not be read. The detail is for the log.
    Unavailable(String),
}

/// Evaluates an S3 action for a recorded caller.
pub trait BulkAuthorizer: Send + Sync {
    /// Evaluate `action` on `resource_arn` for `principal`. `s3_prefix` is the
    /// `s3:prefix` condition key value of an `s3:ListBucket`, `None` for the
    /// other actions.
    fn authorize<'a>(
        &'a self,
        principal: &'a BulkPrincipal,
        action: S3Action,
        resource_arn: &'a str,
        s3_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AuthorizeError>>;
}

/// The ARN of an S3 bucket.
pub(crate) fn bucket_arn(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

/// The ARN of the object `key` in `bucket`.
pub(crate) fn object_arn(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

/// The `FailureMessage` of a denial, in S3's spelling with the resource
/// quoted. ExtendDB has no bucket policies, so only the identity-based forms
/// occur; no request was sent to S3, so there is no request-id tail.
#[must_use]
pub fn denial_message(
    principal: &BulkPrincipal,
    action: S3Action,
    resource_arn: &str,
    explicit: bool,
) -> String {
    let action = action.as_str();
    let head = format!(
        "User: {} is not authorized to perform: {action} on resource: \"{resource_arn}\"",
        principal.arn()
    );
    if explicit {
        format!("{head} with an explicit deny in an identity-based policy")
    } else {
        format!("{head} because no identity-based policy allows the {action} action")
    }
}

/// The outcome of one check, as the Start handler and the runner act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    Allowed,
    /// `S3AccessDenied` with this `FailureMessage`.
    Denied(String),
    /// The decision could not be made; the operation answers
    /// `InternalServerError`. Logged here.
    Unavailable,
}

/// The `FailureMessage` of a check that fails closed because the operation
/// records no caller.
pub(crate) const NO_CALLER_MESSAGE: &str = "Access Denied";

/// Run one check. With no authorizer (developer mode) every action is
/// allowed. With an authorizer and no recorded caller (an operation started
/// before callers were recorded) the check fails closed with a log line.
pub(crate) async fn decide(
    authorizer: Option<&dyn BulkAuthorizer>,
    principal: Option<&BulkPrincipal>,
    action: S3Action,
    resource_arn: &str,
    s3_prefix: Option<&str>,
    account_id: &str,
    operation_id: &str,
) -> Decision {
    let Some(authorizer) = authorizer else {
        return Decision::Allowed;
    };
    let Some(principal) = principal else {
        tracing::error!(
            account_id = %account_id,
            operation_id = %operation_id,
            action = action.as_str(),
            resource = %resource_arn,
            "bulk operation records no caller; the S3 permission check fails closed"
        );
        return Decision::Denied(NO_CALLER_MESSAGE.to_owned());
    };
    match authorizer
        .authorize(principal, action, resource_arn, s3_prefix)
        .await
    {
        Ok(()) => Decision::Allowed,
        Err(AuthorizeError::Denied { explicit }) => {
            tracing::info!(
                account_id = %account_id,
                operation_id = %operation_id,
                principal = %principal.arn(),
                action = action.as_str(),
                resource = %resource_arn,
                explicit,
                "bulk operation S3 permission check denied"
            );
            Decision::Denied(denial_message(principal, action, resource_arn, explicit))
        }
        Err(AuthorizeError::Unavailable(detail)) => {
            tracing::error!(
                account_id = %account_id,
                operation_id = %operation_id,
                action = action.as_str(),
                resource = %resource_arn,
                detail,
                "bulk operation S3 permission check could not be evaluated"
            );
            Decision::Unavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn role_session() -> BulkPrincipal {
        BulkPrincipal {
            identity: BulkIdentity::RoleSession {
                account_id: "123456789012".to_owned(),
                role_name: "loader".to_owned(),
                session_name: "night".to_owned(),
            },
            session: Some(SessionSnapshot {
                policy: Some("{}".to_owned()),
                tags: BTreeMap::from([("team".to_owned(), "a".to_owned())]),
                token_issue_time: Some("2026-10-25T16:00:00Z".to_owned()),
            }),
            source_ip: Some("10.0.0.1".to_owned()),
            user_agent: Some("ua".to_owned()),
            secure_transport: true,
        }
    }

    #[test]
    fn a_role_session_is_named_by_its_sts_assumed_role_arn() {
        assert_eq!(
            denial_message(
                &role_session(),
                S3Action::PutObject,
                "arn:aws:s3:::b/k",
                false
            ),
            "User: arn:aws:sts::123456789012:assumed-role/loader/night is not authorized to \
             perform: s3:PutObject on resource: \"arn:aws:s3:::b/k\" because no identity-based \
             policy allows the s3:PutObject action"
        );
    }

    #[test]
    fn the_recorded_caller_has_a_stable_stored_shape() {
        let value = serde_json::to_value(role_session()).unwrap();
        assert_eq!(
            value,
            json!({
                "identity": {"type": "RoleSession", "account_id": "123456789012",
                             "role_name": "loader", "session_name": "night"},
                "session": {"policy": "{}", "tags": {"team": "a"},
                            "token_issue_time": "2026-10-25T16:00:00Z"},
                "source_ip": "10.0.0.1",
                "user_agent": "ua",
                "secure_transport": true,
            })
        );
        let back: BulkPrincipal = serde_json::from_value(value).unwrap();
        assert_eq!(back, role_session());
    }
}
