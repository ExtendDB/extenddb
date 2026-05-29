// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Centralized handle for the auth/authz cache instances.
//!
//! `AuthCacheRegistry` is constructed during server bootstrap and threaded
//! through `ServerComponents` and `AppState` so management API handlers can
//! invalidate cache entries on IAM mutations (write-through invalidation).
//!
//! All cache instances are `Clone` and share their underlying state via
//! `Arc`. Cloning the registry is cheap.
//!
//! See `docs/design/12-auth-authz-cache.md`.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::CachedCredentialStore;

/// Invalidation hooks for the `TableKeyInfo` cache.
///
/// Implemented by `extenddb-server`'s `CachedTableKeyInfoStore` and held in
/// the registry as `Arc<dyn TableKeyInfoCacheInvalidator>` to avoid a
/// circular crate dependency.
pub trait TableKeyInfoCacheInvalidator: Send + Sync {
    fn invalidate<'a>(
        &'a self,
        account_id: &'a str,
        table_name: &'a str,
    ) -> futures::future::BoxFuture<'a, ()>;

    /// Drop every cached entry. Used by the manual `cache invalidate
    /// all` admin endpoint.
    fn invalidate_all(&self);
}

/// Invalidation hooks for the authorization cache.
///
/// Implemented by `extenddb-server`'s `CachedAuthzStore` (which can't be
/// referenced directly here without creating a circular dependency).
/// Implementations should be fast — they typically forward to `moka`'s
/// `invalidate` or `invalidate_entries_if`, which is fire-and-forget.
pub trait AuthzCacheInvalidator: Send + Sync {
    fn invalidate_user_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_user_group_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_users_group_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_names: &'a [String],
    ) -> BoxFuture<'a, ()>;

    fn invalidate_user_boundary<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_user_tags<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_role_policies<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_role_boundary<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_role_tags<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_session<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
        session_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    fn invalidate_resource_tags<'a>(&'a self, arn: &'a str) -> BoxFuture<'a, ()>;

    /// Invalidate every cached session-data entry for a given role.
    ///
    /// Used when a role is deleted so cached session policies and tags for
    /// any active session of that role are dropped immediately. Implemented
    /// via `invalidate_if` over the `(account, role, session)` cache key.
    fn invalidate_role_sessions<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()>;

    /// Invalidate every cached entry belonging to `account_id` across every
    /// authz subcache (policies, group policies, boundary, tags, sessions,
    /// resource tags). Used by `delete_account` to ensure cached state for
    /// the deleted account is dropped.
    fn invalidate_account<'a>(&'a self, account_id: &'a str) -> BoxFuture<'a, ()>;

    /// Drop every cached entry across every authz subcache. Used by the
    /// manual `cache invalidate all` admin endpoint.
    fn invalidate_all(&self);
}

/// Holds shared handles to every auth/authz cache instance.
///
/// Used by the management API to issue write-through invalidations after
/// admin mutations.
///
/// Construction note: the registry is built during server bootstrap. The
/// storage backend factory creates the credential cache (which lives in
/// this crate); the server crate creates the authorization cache (which
/// lives there) and registers it through `with_authz_invalidator`. This
/// lets the registry sit in `extenddb-auth` without requiring a circular
/// dependency on `extenddb-server`.
#[derive(Clone, Default)]
pub struct AuthCacheRegistry {
    pub credential: Option<Arc<CachedCredentialStore>>,
    pub authz: Option<Arc<dyn AuthzCacheInvalidator>>,
    pub table_key_info: Option<Arc<dyn TableKeyInfoCacheInvalidator>>,
}

impl AuthCacheRegistry {
    /// Construct an empty registry. All `invalidate_*` methods are no-ops in
    /// this state. Used for unit tests and for `enabled = false` mode.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Set the credential cache handle.
    #[must_use]
    pub fn with_credential(mut self, store: Arc<CachedCredentialStore>) -> Self {
        self.credential = Some(store);
        self
    }

    /// Set the authorization cache handle (typed-erased through
    /// [`AuthzCacheInvalidator`] to avoid a circular crate dependency).
    #[must_use]
    pub fn with_authz_invalidator(mut self, invalidator: Arc<dyn AuthzCacheInvalidator>) -> Self {
        self.authz = Some(invalidator);
        self
    }

    /// Set the table-key-info cache handle.
    #[must_use]
    pub fn with_table_key_info_invalidator(
        mut self,
        invalidator: Arc<dyn TableKeyInfoCacheInvalidator>,
    ) -> Self {
        self.table_key_info = Some(invalidator);
        self
    }

    /// Invalidate the cached `TableKeyInfo` for `(account_id, table_name)`.
    /// Called by control-plane handlers (`CreateTable`, `UpdateTable`,
    /// `DeleteTable`, etc.) after the catalog mutation succeeds.
    pub async fn invalidate_table_key_info(&self, account_id: &str, table_name: &str) {
        if let Some(c) = &self.table_key_info {
            c.invalidate(account_id, table_name).await;
        }
    }

    /// Invalidate the cached credential for `access_key_id`.
    ///
    /// Called by management endpoints after `CreateAccessKey`,
    /// `DeleteAccessKey`, `ImportAccessKey`, or any operation that changes the
    /// active state of the key. Safe to call when the cache is disabled
    /// (no-op).
    pub async fn invalidate_credential(&self, access_key_id: &str) {
        if let Some(c) = &self.credential {
            c.invalidate(access_key_id).await;
        }
    }

    pub async fn invalidate_user_policies(&self, account_id: &str, user_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_user_policies(account_id, user_name).await;
        }
    }

    pub async fn invalidate_user_group_policies(&self, account_id: &str, user_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_user_group_policies(account_id, user_name)
                .await;
        }
    }

    /// Fan out a group-membership-affecting event to the cached
    /// `user_group_policies` entry for each member. See
    /// [`AuthzCacheInvalidator::invalidate_users_group_policies`].
    pub async fn invalidate_users_group_policies(&self, account_id: &str, user_names: &[String]) {
        if let Some(c) = &self.authz {
            c.invalidate_users_group_policies(account_id, user_names)
                .await;
        }
    }

    pub async fn invalidate_user_boundary(&self, account_id: &str, user_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_user_boundary(account_id, user_name).await;
        }
    }

    pub async fn invalidate_user_tags(&self, account_id: &str, user_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_user_tags(account_id, user_name).await;
        }
    }

    pub async fn invalidate_role_policies(&self, account_id: &str, role_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_role_policies(account_id, role_name).await;
        }
    }

    pub async fn invalidate_role_boundary(&self, account_id: &str, role_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_role_boundary(account_id, role_name).await;
        }
    }

    pub async fn invalidate_role_tags(&self, account_id: &str, role_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_role_tags(account_id, role_name).await;
        }
    }

    pub async fn invalidate_session(&self, account_id: &str, role_name: &str, session_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_session(account_id, role_name, session_name)
                .await;
        }
    }

    pub async fn invalidate_resource_tags(&self, arn: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_resource_tags(arn).await;
        }
    }

    /// Invalidate every cached session-data entry for `role_name`. Called
    /// when a role is deleted. No-op when the cache is disabled.
    pub async fn invalidate_role_sessions(&self, account_id: &str, role_name: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_role_sessions(account_id, role_name).await;
        }
    }

    /// Invalidate every cached entry — across every authz subcache and the
    /// credential cache — that belongs to `account_id`. Called by
    /// `delete_account`. No-op when caches are disabled.
    pub async fn invalidate_account(&self, account_id: &str) {
        if let Some(c) = &self.authz {
            c.invalidate_account(account_id).await;
        }
        // Best-effort credential fanout; failures are logged-but-not-fatal
        // because the credential cache's negative TTL bounds the worst-case
        // exposure window if invalidation fails.
        if let Some(c) = &self.credential {
            if let Err(e) = c.invalidate_account(account_id) {
                tracing::warn!(
                    account_id = account_id,
                    error = ?e,
                    "credential cache invalidate_account failed; relying on TTL"
                );
            }
        }
    }

    /// Drop every cached entry across every cache (authz subcaches,
    /// credentials, table-key-info). Used by the manual `cache
    /// invalidate all` admin endpoint. No-op for any cache that is
    /// disabled. Callers MUST gate this on explicit confirmation; it
    /// causes a reload storm against the catalog as the next requests
    /// re-populate from cold.
    pub fn invalidate_all_caches(&self) {
        if let Some(c) = &self.authz {
            c.invalidate_all();
        }
        if let Some(c) = &self.table_key_info {
            c.invalidate_all();
        }
        if let Some(c) = &self.credential {
            c.invalidate_all();
        }
    }

    /// Invalidate every cached credential for `principal_name` in
    /// `account_id`. Called when a user or role is deleted. No-op when
    /// caches are disabled.
    pub async fn invalidate_principal_credentials(&self, account_id: &str, principal_name: &str) {
        if let Some(c) = &self.credential {
            if let Err(e) = c.invalidate_principal(account_id, principal_name) {
                tracing::warn!(
                    account_id = account_id,
                    principal = principal_name,
                    error = ?e,
                    "credential cache invalidate_principal failed; relying on TTL"
                );
            }
        }
    }
}
