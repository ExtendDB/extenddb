// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization storage trait for IAM policy lookups.
//!
//! [`AuthorizationStore`] is the read-only counterpart to
//! [`super::management_store::ManagementStore`]. It fetches IAM policies,
//! permissions boundaries, session data, and tags needed by the policy
//! evaluator on every `DynamoDB` request that requires authorization.

use futures::future::BoxFuture;

use super::management_store::OpResult;

/// Complete authorization metadata for an IAM user request.
pub struct UserAuthorizationData {
    /// Direct user policies plus inherited group policies.
    pub identity_policies: Vec<String>,
    /// User permissions boundary policy, if any.
    pub boundary: Option<String>,
    /// Principal tags for `aws:PrincipalTag/*` conditions.
    pub principal_tags: Vec<(String, String)>,
    /// Resource tags for `dynamodb:ResourceTag/*` conditions.
    pub resource_tags: Vec<(String, String)>,
}

/// Complete authorization metadata for an assumed-role request.
pub struct RoleAuthorizationData {
    /// Direct role policies.
    pub identity_policies: Vec<String>,
    /// Role permissions boundary policy, if any.
    pub boundary: Option<String>,
    /// Inline session policy, if any.
    pub session_policy: Option<String>,
    /// Role tags merged with session tags. Session tags win on key conflicts.
    pub principal_tags: Vec<(String, String)>,
    /// Resource tags for `dynamodb:ResourceTag/*` conditions.
    pub resource_tags: Vec<(String, String)>,
}

/// Policy lookups for authorization decisions.
///
/// These methods fetch IAM policies, permissions boundaries, session data,
/// and tags needed by the policy evaluator. They are read-only and called
/// on every `DynamoDB` request that requires authorization.
pub trait AuthorizationStore: Send + Sync {
    /// Fetch all user authorization metadata needed for one DynamoDB request.
    ///
    /// Backends with a native relational store should override this with a
    /// single set-oriented query. The default preserves the existing split
    /// lookup contract for simpler backends.
    fn fetch_user_authorization<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
        resource_arn: &'a str,
    ) -> BoxFuture<'a, OpResult<UserAuthorizationData>> {
        Box::pin(async move {
            let resource_tags = async {
                if resource_arn.ends_with("/*") {
                    Ok(Vec::new())
                } else {
                    self.fetch_resource_tags(resource_arn).await
                }
            };
            let (user_policies, group_policies, boundary, principal_tags, resource_tags) = futures::try_join!(
                self.fetch_user_policies(account_id, user_name),
                self.fetch_user_group_policies(account_id, user_name),
                self.fetch_user_boundary(account_id, user_name),
                self.fetch_user_tags(account_id, user_name),
                resource_tags,
            )?;

            let mut identity_policies = user_policies;
            identity_policies.extend(group_policies);

            Ok(UserAuthorizationData {
                identity_policies,
                boundary,
                principal_tags,
                resource_tags,
            })
        })
    }

    /// Fetch all assumed-role authorization metadata needed for one DynamoDB request.
    ///
    /// `access_key_id` identifies the exact temporary session row. Role session
    /// names are not unique, so backends must not resolve session policy or
    /// tags by `(account_id, role_name, session_name)` alone.
    ///
    /// Backends with a native relational store should override this with a
    /// single set-oriented query. The default preserves the existing split
    /// lookup contract for simpler backends.
    fn fetch_role_authorization<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
        session_name: &'a str,
        access_key_id: &'a str,
        resource_arn: &'a str,
    ) -> BoxFuture<'a, OpResult<RoleAuthorizationData>> {
        Box::pin(async move {
            let resource_tags = async {
                if resource_arn.ends_with("/*") {
                    Ok(Vec::new())
                } else {
                    self.fetch_resource_tags(resource_arn).await
                }
            };
            let (identity_policies, boundary, role_tags, session_data, resource_tags) = futures::try_join!(
                self.fetch_role_policies(account_id, role_name),
                self.fetch_role_boundary(account_id, role_name),
                self.fetch_role_tags(account_id, role_name),
                self.fetch_session_data(account_id, role_name, session_name, access_key_id),
                resource_tags,
            )?;

            let mut principal_tags = role_tags;
            let mut session_policy = None;
            if let Some(data) = session_data {
                session_policy = data.session_policy;
                merge_session_tags(&mut principal_tags, data.session_tags);
            }

            Ok(RoleAuthorizationData {
                identity_policies,
                boundary,
                session_policy,
                principal_tags,
                resource_tags,
            })
        })
    }

    /// Fetch all policy documents for a user (directly attached).
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>>;

    /// Fetch all policy documents from groups the user belongs to.
    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>>;

    /// Fetch the permissions boundary policy document for a user, if any.
    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>>;

    /// Fetch all policy documents for a role (directly attached).
    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>>;

    /// Fetch the permissions boundary policy document for a role, if any.
    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>>;

    /// Fetch session data (session policy, session tags) for a role session.
    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
        access_key_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>>;

    /// Fetch tags for a user (for condition key evaluation).
    fn fetch_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    /// Fetch tags for a role (for condition key evaluation in role sessions).
    fn fetch_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    /// Fetch tags for a resource ARN (for condition key evaluation).
    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
}

/// Session data returned by [`AuthorizationStore::fetch_session_data`].
#[derive(Debug, Clone)]
pub struct SessionData {
    /// The inline session policy document, if any.
    pub session_policy: Option<String>,
    /// Session tags as `(key, value)` pairs.
    pub session_tags: Vec<(String, String)>,
}

/// Merge session tags into principal tags, replacing role tag values with the
/// same key so session tags win as IAM requires.
pub fn merge_session_tags(
    principal_tags: &mut Vec<(String, String)>,
    session_tags: Vec<(String, String)>,
) {
    for (key, value) in session_tags {
        if let Some((_, existing_value)) = principal_tags
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            *existing_value = value;
        } else {
            principal_tags.push((key, value));
        }
    }
}
