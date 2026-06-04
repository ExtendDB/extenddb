// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared types for management storage traits.

// ── Error types ────────────────────────────────────────────────────────

/// Error from a management or operational storage operation.
#[derive(Debug, Clone)]
pub enum OpError {
    /// Input validation failed (caller should have caught this).
    Validation(String),
    /// Entity already exists (unique constraint violation).
    AlreadyExists(String),
    /// Referenced entity not found.
    NotFound(String),
    /// Cannot delete due to dependent entities.
    HasDependents(String),
    /// Internal storage error.
    Internal(String),
}

/// Result alias for management operations.
pub type OpResult<T> = Result<T, OpError>;

// ── Detail structs ─────────────────────────────────────────────────────

/// Account detail returned by [`super::ManagementStore::get_account_detail`].
pub struct AccountDetail {
    pub account_name: String,
    pub users: Vec<String>,
    pub groups: Vec<String>,
    pub roles: Vec<String>,
}

/// User detail returned by [`super::ManagementStore::get_user_detail`].
pub struct UserDetail {
    /// `(access_key_id, is_active)`.
    pub keys: Vec<(String, bool)>,
    pub policies: Vec<String>,
    /// `(tag_key, tag_value)`.
    pub tags: Vec<(String, String)>,
    pub groups: Vec<String>,
}

/// Group detail returned by [`super::ManagementStore::get_group_detail`].
pub struct GroupDetail {
    pub members: Vec<String>,
    pub policies: Vec<String>,
    /// All users in the account (for add-member UI).
    pub all_users: Vec<String>,
}

/// Role detail returned by [`super::ManagementStore::get_role_detail`].
pub struct RoleDetail {
    pub trust_policy: serde_json::Value,
    pub policies: Vec<String>,
    /// `(tag_key, tag_value)`.
    pub tags: Vec<(String, String)>,
}

/// Result of creating an access key.
pub struct AccessKeyCreated {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// An admin user entry returned by [`super::AdminStore::list_admins`].
pub struct AdminEntry {
    pub admin_name: String,
    pub created_at: time::OffsetDateTime,
}

/// A single metrics row from persistent storage.
#[derive(Debug, Clone)]
pub struct MetricsRow {
    pub bucket: time::OffsetDateTime,
    pub metric: String,
    pub table_name: Option<String>,
    pub index_name: Option<String>,
    pub operation: Option<String>,
    pub sum: f64,
    pub count: i64,
    pub min: f64,
    pub max: f64,
}
