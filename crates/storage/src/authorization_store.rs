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

/// Policy lookups for authorization decisions.
///
/// These methods fetch IAM policies, permissions boundaries, session data,
/// and tags needed by the policy evaluator. They are read-only and called
/// on every `DynamoDB` request that requires authorization.
pub trait AuthorizationStore: Send + Sync {
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
