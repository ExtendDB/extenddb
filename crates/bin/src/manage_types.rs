// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! CLI argument types for `extenddb manage` subcommands.
//!
//! Extracted from `cmd_manage.rs` to keep all files under the 500-line limit.

use clap::{Args, Subcommand};

#[derive(Args)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
pub struct ManageArgs {
    /// Credentials: admin username, or account_id/user_name for IAM users
    #[arg(long)]
    pub user: String,
    /// Password (reads from EXTENDDB_PASSWORD env var if not provided)
    #[arg(long)]
    pub password: Option<String>,
    /// Server endpoint (default: from config file)
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    pub config: String,
    #[command(subcommand)]
    pub command: ManageCommand,
}

#[derive(Subcommand)]
pub enum ManageCommand {
    /// Create a new admin user
    CreateAdmin {
        #[arg(long)]
        admin_name: String,
        #[arg(long)]
        admin_password: String,
    },
    /// List admin users
    ListAdmins,
    /// Delete an admin user
    DeleteAdmin {
        #[arg(long)]
        admin_name: String,
    },
    /// Change an admin user's password
    ChangeAdminPassword {
        #[arg(long)]
        admin_name: String,
        #[arg(long)]
        new_password: String,
    },
    /// Create a new account
    CreateAccount {
        /// Account ID (12-digit numeric string). Auto-generated if omitted.
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        account_name: String,
    },
    /// List accounts
    ListAccounts,
    /// Delete an account
    DeleteAccount {
        #[arg(long)]
        account_id: String,
    },
    /// Create an IAM user
    CreateUser {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        /// Console password (optional)
        #[arg(long)]
        user_password: Option<String>,
    },
    /// List IAM users in an account
    ListUsers {
        #[arg(long)]
        account_id: String,
    },
    /// Delete an IAM user
    DeleteUser {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Create an access key (self-service or admin).
    /// When authenticating as an IAM user (--user `account_id/user_name`),
    /// --account-id and --user-name are inferred if omitted.
    CreateAccessKey {
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        user_name: Option<String>,
    },
    /// List access keys (self-service or admin)
    ListAccessKeys {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Delete an access key (self-service or admin)
    DeleteAccessKey {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        #[arg(long)]
        access_key_id: String,
    },
    /// Import an existing access key (e.g. real AWS credentials)
    ImportAccessKey {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        #[arg(long)]
        access_key_id: String,
        /// Secret access key (required).
        #[arg(long)]
        secret_access_key: String,
        /// Confirm import (required, no interactive prompt)
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Change an IAM user's password (self-service or admin)
    ChangeUserPassword {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        #[arg(long)]
        new_password: String,
    },
    /// Create an IAM group
    CreateGroup {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
    },
    /// List IAM groups in an account
    ListGroups {
        #[arg(long)]
        account_id: String,
    },
    /// Delete an IAM group
    DeleteGroup {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
    },
    /// Add a user to a group
    AddGroupMember {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
        #[arg(long)]
        user_name: String,
    },
    /// Remove a user from a group
    RemoveGroupMember {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
        #[arg(long)]
        user_name: String,
    },
    /// Put a user policy (creates or replaces)
    PutUserPolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        #[arg(long)]
        policy_name: String,
        /// Policy document JSON string
        #[arg(long)]
        policy_document: String,
    },
    /// List user policies
    ListUserPolicies {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Delete a user policy
    DeleteUserPolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        #[arg(long)]
        policy_name: String,
    },
    /// Put a group policy (creates or replaces)
    PutGroupPolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
        #[arg(long)]
        policy_name: String,
        /// Policy document JSON string
        #[arg(long)]
        policy_document: String,
    },
    /// List group policies
    ListGroupPolicies {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
    },
    /// Delete a group policy
    DeleteGroupPolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        group_name: String,
        #[arg(long)]
        policy_name: String,
    },
    /// Tag an IAM user
    TagUser {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        /// Tags as JSON: [{"key":"k","value":"v"},...]
        #[arg(long)]
        tags: String,
    },
    /// Untag an IAM user
    UntagUser {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        /// Comma-separated tag keys to remove
        #[arg(long)]
        tag_keys: String,
    },
    /// List tags for an IAM user
    ListUserTags {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Create an IAM role
    CreateRole {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        /// Trust policy JSON string
        #[arg(long)]
        trust_policy: String,
    },
    /// List IAM roles in an account
    ListRoles {
        #[arg(long)]
        account_id: String,
    },
    /// Delete an IAM role
    DeleteRole {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Tag an IAM role
    TagRole {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        /// Tags as JSON: [{"key":"k","value":"v"},...]
        #[arg(long)]
        tags: String,
    },
    /// Untag an IAM role
    UntagRole {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        /// Comma-separated tag keys to remove
        #[arg(long)]
        tag_keys: String,
    },
    /// List tags for an IAM role
    ListRoleTags {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Put a role policy (creates or replaces)
    PutRolePolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        #[arg(long)]
        policy_name: String,
        /// Policy document JSON string
        #[arg(long)]
        policy_document: String,
    },
    /// List role policies
    ListRolePolicies {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Delete a role policy
    DeleteRolePolicy {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        #[arg(long)]
        policy_name: String,
    },
    /// Assume an IAM role and get temporary credentials
    AssumeRole {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        /// ARN of the caller assuming the role
        #[arg(long)]
        caller_arn: String,
        /// Session name
        #[arg(long)]
        session_name: String,
        /// Session tags as JSON (optional)
        #[arg(long)]
        session_tags: Option<String>,
        /// Session policy JSON string (optional)
        #[arg(long)]
        session_policy: Option<String>,
        /// Duration in seconds (default 3600)
        #[arg(long, default_value = "3600")]
        duration_seconds: i64,
    },
    /// Set a user permissions boundary
    SetUserBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
        /// Permissions boundary policy document JSON string
        #[arg(long)]
        policy_document: String,
    },
    /// Get a user permissions boundary
    GetUserBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Delete a user permissions boundary
    DeleteUserBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Set a role permissions boundary
    SetRoleBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
        /// Permissions boundary policy document JSON string
        #[arg(long)]
        policy_document: String,
    },
    /// Get a role permissions boundary
    GetRoleBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Delete a role permissions boundary
    DeleteRoleBoundary {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Cache management (admin-only break-glass invalidation)
    Cache {
        #[command(subcommand)]
        action: CacheAction,
    },
}

#[derive(Subcommand)]
pub enum CacheAction {
    /// Invalidate cache entries. See `cache invalidate --help` for scopes.
    Invalidate {
        #[command(subcommand)]
        scope: CacheInvalidateScope,
    },
}

#[derive(Subcommand)]
pub enum CacheInvalidateScope {
    /// Invalidate every cached entry across every cache (requires --yes).
    All {
        /// Confirm the global flush. Refuses to run without it.
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Invalidate every cached entry for a specific account.
    Account {
        #[arg(long)]
        account_id: String,
    },
    /// Invalidate a single cached credential by access key.
    Credential {
        #[arg(long)]
        access_key_id: String,
    },
    /// Invalidate everything cached about an IAM user (policies, group
    /// policies, boundary, tags, credentials).
    User {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        user_name: String,
    },
    /// Invalidate everything cached about an IAM role (policies, boundary,
    /// tags, sessions, credentials).
    Role {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        role_name: String,
    },
    /// Invalidate the cached group_policies for a list of users (used
    /// after a group-membership change that the write-through hooks
    /// missed).
    GroupMembers {
        #[arg(long)]
        account_id: String,
        /// Comma-separated user names.
        #[arg(long)]
        user_names: String,
    },
    /// Invalidate the TableKeyInfo cache for a single table.
    TableKeyInfo {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        table_name: String,
    },
    /// Invalidate the resource_tags cache for a resource ARN.
    ResourceTags {
        #[arg(long)]
        arn: String,
    },
}
