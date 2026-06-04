// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Trait definitions for non-DynamoDB storage subsystems.
//!
//! These traits abstract the IAM management, settings, metrics, rate limiting,
//! admin user, and authorization storage that currently uses `sqlx::PgPool`
//! directly. A new storage backend must implement these traits alongside the
//! existing `TableEngine`, `DataEngine`, `MetadataEngine`, and `StreamEngine`
//! traits in `lib.rs`.
//!
//! # Design decisions
//!
//! - **RPITIT** (return-position `impl Trait` in traits) for async methods,
//!   matching the existing storage traits.
//! - **`OpError` / `OpResult`** live in the `types` submodule so both the
//!   storage implementation and the server crate can use them without circular
//!   dependencies.
//! - **Detail structs** (`AccountDetail`, `UserDetail`, etc.) live in `types`
//!   so the trait methods can return them without the server crate defining them.
//! - **Validation stays in the caller.** These traits are pure data access.
//!   Input validation (name format, password length, policy document parsing)
//!   remains in the engine/server layer. The storage layer trusts validated input.

mod types;

pub use types::{
    AccessKeyCreated, AccountDetail, AdminEntry, GroupDetail, MetricsRow, OpError, OpResult,
    RoleDetail, UserDetail,
};

use futures::future::BoxFuture;

// Type aliases for complex return types in management traits.
/// User info: `(account_id, user_name, user_arn, has_password, created_at)`.
pub type UserListEntry = (String, String, String, bool, time::OffsetDateTime);
/// Group info: `(account_id, group_name, group_arn, created_at)`.
pub type GroupListEntry = (String, String, String, time::OffsetDateTime);
/// Role info: `(account_id, role_name, role_arn, assume_role_policy, created_at)`.
pub type RoleListEntry = (
    String,
    String,
    String,
    serde_json::Value,
    time::OffsetDateTime,
);

// ── Settings store ─────────────────────────────────────────────────────

/// Runtime settings storage (e.g. `control_plane_delay_seconds`, `log_level`).
pub trait SettingsStore: Send + Sync {
    /// Get a single setting value. Returns `None` if the key does not exist.
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>>;

    /// Set a setting value (upsert).
    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List all settings as `(key, value)` pairs, ordered by key.
    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    /// P119: Get the cached encryption key if available. Returns `None` by
    /// default; backends that cache the key at startup override this.
    fn cached_encryption_key(&self) -> Option<String> {
        None
    }
}

// ── Metrics store ──────────────────────────────────────────────────────

/// Historical metrics persistence and query.
pub trait MetricsStore: Send + Sync {
    /// Insert a batch of metrics rows (periodic flush from in-memory collector).
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>>;

    /// Query metrics rows within a time range, with optional filters.
    fn query_metrics(
        &self,
        start: time::OffsetDateTime,
        end: time::OffsetDateTime,
        table_name: Option<&str>,
        metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>>;

    /// Delete metrics rows older than the retention period.
    fn prune_metrics(&self, retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>>;
}

// ── Rate limit store ───────────────────────────────────────────────────

/// Login rate limiting and account lockout storage.
pub trait RateLimitStore: Send + Sync {
    /// Count failed login attempts for a principal within the lookback window.
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>>;

    /// Count failed login attempts from a source IP within the lookback window.
    fn count_ip_failures(
        &self,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>>;

    /// Record a failed login attempt.
    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()>;

    /// Delete login attempt records older than `max_age_seconds`.
    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()>;
}

// ── Admin store ────────────────────────────────────────────────────────

/// Admin user management (separate from IAM users).
pub trait AdminStore: Send + Sync {
    /// Create an admin user with a pre-hashed password.
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List all admin users.
    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>>;

    /// Delete an admin user. Returns `NotFound` if the admin does not exist.
    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>>;

    /// Update an admin user's password hash. Returns `NotFound` if not found.
    fn change_admin_password(
        &self,
        admin_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Verify an admin password. Returns `None` if the admin does not exist,
    /// `Some(true)` if the password matches, `Some(false)` if it does not.
    fn verify_admin_password(
        &self,
        admin_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<Option<bool>>>;
}

// ── Management store (IAM) ─────────────────────────────────────────────

/// IAM entity CRUD: accounts, users, groups, roles, policies, access keys.
///
/// This is the largest trait because it covers the full IAM surface area.
/// Validation (name format, policy document parsing) is the caller's
/// responsibility — these methods are pure data access.
pub trait ManagementStore: Send + Sync {
    // ── Accounts ───────────────────────────────────────────────────

    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>>;

    /// Delete an account. Must fail with `HasDependents` if the account owns tables.
    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List all accounts as `(account_id, account_name)`.
    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    /// List all accounts with created_at as `(account_id, account_name, created_at)`.
    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>>;

    /// List accounts visible to a specific account.
    fn list_accounts_for(&self, account_id: &str)
    -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>>;

    /// Dashboard counts: `(account_count, admin_count)`.
    fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>>;

    // ── Users ──────────────────────────────────────────────────────

    fn create_user(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> BoxFuture<'_, OpResult<()>>;

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List users in an account as `(account_id, user_name, user_arn, has_password, created_at)`.
    fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>>;

    fn get_user_detail(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>>;

    /// Verify an IAM user's console password.
    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>>;

    /// Change an IAM user's console password (pre-hashed).
    fn change_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    // ── User tags ──────────────────────────────────────────────────

    /// Set tags on a user (upsert). All tags are applied atomically.
    fn tag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Remove tags from a user by key.
    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>>;

    /// List tags for a user as `(tag_key, tag_value)`.
    fn list_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    // ── Groups ─────────────────────────────────────────────────────

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List groups in an account as `(account_id, group_name, group_arn, created_at)`.
    fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>>;

    fn get_group_detail(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>>;

    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    fn remove_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    // ── Roles ──────────────────────────────────────────────────────

    fn create_role(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>>;

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>>;

    /// List roles in an account as `(account_id, role_name, role_arn, trust_policy, created_at)`.
    fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>>;

    fn get_role_detail(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>>;

    /// Fetch a role's trust policy. Returns `None` if the role does not exist.
    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;

    // ── Role tags ──────────────────────────────────────────────────

    /// Set tags on a role (upsert). All tags are applied atomically.
    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Remove tags from a role by key.
    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>>;

    /// List tags for a role as `(tag_key, tag_value)`.
    fn list_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;

    // ── Policies ───────────────────────────────────────────────────

    /// Put (create or replace) a policy attached to a principal.
    fn put_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>>;

    fn delete_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// List policies for a principal as `(policy_name, policy_document, created_at)`.
    fn list_policies(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>>;

    // ── Permissions boundaries ─────────────────────────────────────

    /// Set (upsert) a permissions boundary on a user.
    fn set_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Get the permissions boundary for a user. Returns `None` if not set.
    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;

    /// Delete the permissions boundary for a user.
    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Set (upsert) a permissions boundary on a role.
    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// Get the permissions boundary for a role. Returns `None` if not set.
    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;

    /// Delete the permissions boundary for a role.
    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    // ── Access keys ────────────────────────────────────────────────

    /// Create an access key for an IAM user. The implementation handles
    /// key generation and encryption internally.
    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>>;

    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    /// List access keys for a user as `(access_key_id, is_active, created_at)`.
    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>>;

    /// Import an externally-generated access key. The implementation handles
    /// encryption internally using the stored encryption key.
    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>>;

    // ── Sessions (`AssumeRole`) ────────────────────────────────────

    /// Store a temporary session credential from `AssumeRole`.
    #[allow(clippy::too_many_arguments)]
    fn store_session(
        &self,
        session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        session_tags: &Option<serde_json::Value>,
        session_policy: &Option<serde_json::Value>,
        expires_at: time::OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>>;

    // ── Caller tags (for trust policy evaluation) ──────────────────

    /// Fetch tags for a caller principal identified by ARN.
    /// Returns `(tag_key, tag_value)` pairs.
    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
}
