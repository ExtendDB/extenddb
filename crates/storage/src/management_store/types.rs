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

/// One `access_keys` or `iam_sessions` row as the encryption-key commands
/// read it ([`super::ManagementStore::read_secret_page`]): the fields of its
/// AAD, its owner and creation time for the listings, and its ciphertext.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredSecretRow {
    /// The table that holds the row.
    pub table: crate::secret_keys::SecretTable,
    /// The row's primary key in the backend's own form, which
    /// [`super::ManagementStore::write_secrets`] and
    /// [`super::ManagementStore::delete_secrets`] take back. Opaque to the
    /// commands, and never printed: for a session it can hold the token.
    pub key: String,
    /// The access key ID.
    pub access_key_id: String,
    /// The account of the user or role.
    pub account_id: String,
    /// The user name of an access key, the role name of a session.
    pub principal_name: String,
    /// The session name, for a session.
    pub session_name: Option<String>,
    /// The session's `expires_at` in whole Unix seconds, for a session.
    pub expires_at: Option<i64>,
    /// When the row was created, when the backend stores it.
    pub created_at: Option<time::OffsetDateTime>,
    /// `nonce(12) || ciphertext || tag(16)`.
    pub secret_key_encrypted: Vec<u8>,
    /// Cassandra only: `WRITETIME(secret_key_encrypted)` in microseconds.
    pub write_timestamp: Option<i64>,
}

impl StoredSecretRow {
    /// The fields of this row's AAD.
    #[must_use]
    pub fn aad_row(&self) -> crate::secret_keys::SecretRow<'_> {
        crate::secret_keys::SecretRow {
            table: self.table,
            access_key_id: &self.access_key_id,
            account_id: &self.account_id,
            principal_name: &self.principal_name,
            session_name: self.session_name.as_deref(),
            expires_at: self.expires_at,
        }
    }
}

impl std::fmt::Debug for StoredSecretRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredSecretRow")
            .field("table", &self.table)
            .field("access_key_id", &self.access_key_id)
            .field("account_id", &self.account_id)
            .field("principal_name", &self.principal_name)
            .field("session_name", &self.session_name)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// One page of [`StoredSecretRow`]s.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretPage {
    /// The rows of the page.
    pub rows: Vec<StoredSecretRow>,
    /// The token of the next page, or `None` after the last page.
    pub next: Option<String>,
}

/// One ciphertext to write back by primary key
/// ([`super::ManagementStore::write_secrets`]).
#[derive(Clone, PartialEq, Eq)]
pub struct SecretWrite {
    /// [`StoredSecretRow::key`] of the row.
    pub key: String,
    /// The new `nonce(12) || ciphertext || tag(16)`.
    pub secret_key_encrypted: Vec<u8>,
    /// [`StoredSecretRow::write_timestamp`] of the read. Cassandra writes
    /// with `USING TIMESTAMP` above it.
    pub write_timestamp: Option<i64>,
}

impl std::fmt::Debug for SecretWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretWrite")
            .field("write_timestamp", &self.write_timestamp)
            .finish_non_exhaustive()
    }
}

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
