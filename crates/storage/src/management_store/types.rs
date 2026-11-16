// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared types for management storage traits.

// ── Error types ────────────────────────────────────────────────────────

/// Error from a management or operational storage operation.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// A database statement failed with this error code (PostgreSQL's
    /// SQLSTATE). `message` is the driver's text, for logs and the
    /// operator's commands; API responses never carry it.
    Database { message: String, sqlstate: String },
}

impl OpError {
    /// The database error code of the statement that failed, when the error
    /// carries one.
    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Database { sqlstate, .. } => Some(sqlstate),
            Self::Validation(_)
            | Self::AlreadyExists(_)
            | Self::NotFound(_)
            | Self::HasDependents(_)
            | Self::Internal(_) => None,
        }
    }

    /// The error's text, without the variant name that `{:?}` prints.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Validation(m)
            | Self::AlreadyExists(m)
            | Self::NotFound(m)
            | Self::HasDependents(m)
            | Self::Internal(m)
            | Self::Database { message: m, .. } => m,
        }
    }
}

/// Result alias for management operations.
pub type OpResult<T> = Result<T, OpError>;

/// A stored session's policy, returned by
/// [`super::ManagementStore::list_session_policies`] for the policy migration.
///
/// Only sessions that carry a session policy are returned. Carries the identity
/// needed to rewrite the policy and report it, keyed by `session_token`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPolicyEntry {
    pub session_token: String,
    pub account_id: String,
    pub role_name: String,
    pub session_name: String,
    pub policy: serde_json::Value,
}

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
    /// The account the row was recorded for; `None` for a metric of the
    /// whole deployment, stored as the empty string on SQLite, PostgreSQL
    /// and Cassandra and as a document without the field on MongoDB.
    pub account_id: Option<String>,
    pub metric: String,
    pub table_name: Option<String>,
    pub index_name: Option<String>,
    pub operation: Option<String>,
    pub sum: f64,
    pub count: i64,
    pub min: f64,
    pub max: f64,
}

/// The optional filters of [`super::MetricsStore::query_metrics`]. A `None`
/// field matches every row, and an empty string counts as `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsFilter {
    /// Only the rows recorded for this account.
    pub account_id: Option<String>,
    /// Only the rows of this table name.
    pub table_name: Option<String>,
    /// Only the rows of this metric.
    pub metric: Option<String>,
}

impl MetricsFilter {
    /// The account filter, with an empty string read as no filter.
    #[must_use]
    pub fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref().filter(|s| !s.is_empty())
    }

    /// The table name filter, with an empty string read as no filter.
    #[must_use]
    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref().filter(|s| !s.is_empty())
    }

    /// The metric filter, with an empty string read as no filter.
    #[must_use]
    pub fn metric(&self) -> Option<&str> {
        self.metric.as_deref().filter(|s| !s.is_empty())
    }
}
