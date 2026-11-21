// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;

use super::*;

use crate::bulk_put::authorize::{
    AuthorizeError, BulkAuthorizer, BulkIdentity, BulkPrincipal, S3Action,
};

// A stand-in for the server's authorizer: it allows every check except the
// one rule it carries, and records each check it was asked.

/// The caller the tests record on their operations.
pub(crate) const CALLER: &str = "loader";

/// Deny `action` on every resource ending with `resource_suffix`, after the
/// first `allow_first` such checks were allowed.
pub(crate) struct DenyRule {
    pub(crate) action: S3Action,
    pub(crate) resource_suffix: String,
    pub(crate) explicit: bool,
    pub(crate) allow_first: usize,
}

#[derive(Default)]
pub(crate) struct FakeAuthorizer {
    pub(crate) rule: Option<DenyRule>,
    /// Answer every check as one that cannot be evaluated.
    pub(crate) unavailable: bool,
    pub(crate) matched: Mutex<usize>,
    /// Every check asked: the action, the resource and `s3:prefix`.
    pub(crate) calls: Mutex<Vec<(S3Action, String, Option<String>)>>,
}

impl FakeAuthorizer {
    pub(crate) fn denying(action: S3Action, resource_suffix: &str) -> Self {
        Self {
            rule: Some(DenyRule {
                action,
                resource_suffix: resource_suffix.to_owned(),
                explicit: false,
                allow_first: 0,
            }),
            ..Self::default()
        }
    }

    pub(crate) fn checks(&self) -> Vec<(S3Action, String, Option<String>)> {
        self.calls.lock().unwrap().clone()
    }

    pub(crate) fn count(&self, action: S3Action) -> usize {
        self.checks()
            .iter()
            .filter(|(a, _, _)| *a == action)
            .count()
    }
}

impl BulkAuthorizer for FakeAuthorizer {
    fn authorize<'a>(
        &'a self,
        _principal: &'a BulkPrincipal,
        action: S3Action,
        resource_arn: &'a str,
        s3_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AuthorizeError>> {
        self.calls.lock().unwrap().push((
            action,
            resource_arn.to_owned(),
            s3_prefix.map(ToOwned::to_owned),
        ));
        let answer = if self.unavailable {
            Err(AuthorizeError::Unavailable("test".to_owned()))
        } else {
            match &self.rule {
                Some(rule)
                    if rule.action == action && resource_arn.ends_with(&rule.resource_suffix) =>
                {
                    let mut matched = self.matched.lock().unwrap();
                    *matched += 1;
                    if *matched > rule.allow_first {
                        Err(AuthorizeError::Denied {
                            explicit: rule.explicit,
                        })
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            }
        };
        Box::pin(async move { answer })
    }
}

/// The recorded caller: an IAM user in the test account.
pub(crate) fn caller() -> BulkPrincipal {
    BulkPrincipal {
        identity: BulkIdentity::User {
            account_id: ACCOUNT.to_owned(),
            user_name: CALLER.to_owned(),
        },
        session: None,
        source_ip: None,
        user_agent: None,
        secure_transport: false,
    }
}

/// The run context Start records for the test caller on the filesystem
/// store.
pub(crate) fn run_context_with_caller() -> Value {
    json!({
        "store": "filesystem",
        "principal": serde_json::to_value(caller()).unwrap(),
    })
}

/// The denial text for the test caller.
pub(crate) fn denied(action: &str, resource: &str) -> String {
    format!(
        "User: arn:aws:iam::{ACCOUNT}:user/{CALLER} is not authorized to perform: {action} on \
         resource: \"{resource}\" because no identity-based policy allows the {action} action"
    )
}
