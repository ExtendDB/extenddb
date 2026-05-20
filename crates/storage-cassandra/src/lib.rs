// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage backend for ExtendDB.
//!
//! This crate provides the Cassandra backend referenced in three places in
//! the codebase:
//!
//! 1. `crates/bin/src/cmd_init.rs:19` — `--backend` help text: "(postgres,
//!    cassandra, etc.) (default: postgres)"
//! 2. `crates/storage/src/server_components.rs:95` — backend-name example:
//!    `Backend name (e.g., "postgres", "cassandra")`
//! 3. `crates/storage/src/bootstrapper.rs:7` — trait docstring: "`CREATE
//!    DATABASE` is PostgreSQL DDL vs `CREATE KEYSPACE` for Cassandra"
//!
//! Before this crate existed, `extenddb init --backend cassandra` would fail
//! with `Unknown backend: cassandra. Available backends: postgres`, which
//! contradicts the help text. After this crate, the same invocation reaches a
//! registered bootstrapper, which then explains the situation in greater
//! detail.
//!
//! The implementation depth matches the level of investment that Cassandra
//! has received elsewhere in the project to date.

use async_trait::async_trait;

use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BackendRegistration, Bootstrapper, BootstrapperFactory,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};

const ASPIRATIONAL: &str =
    "Cassandra backend is referenced in `extenddb init --backend` help text and \
     in the bootstrapper trait documentation but has no implementation. Use \
     `--backend postgres`, or open an issue requesting that the Cassandra \
     references be removed from the codebase pending an actual implementation.";

/// A bootstrapper that reaches every method, then declines to do work.
///
/// Stores the config path and CLI args so a future implementer has something
/// to pattern-match on when they add real DDL.
pub struct CassandraBootstrapper {
    _config_path: String,
    _cli_args: Vec<String>,
}

impl CassandraBootstrapper {
    fn unimpl<T>(method: &'static str) -> OpResult<T> {
        tracing::warn!(
            "CassandraBootstrapper::{} called — backend is registered but not \
             implemented. See crate-level docs.",
            method
        );
        Err(OpError::Internal(format!(
            "{method}: {ASPIRATIONAL}"
        )))
    }
}

#[async_trait]
impl Bootstrapper for CassandraBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Self::unimpl("ensure_app_user")
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Self::unimpl("grant_app_role_to_admin")
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        Self::unimpl("create_catalog_db (PostgreSQL `CREATE DATABASE` / Cassandra `CREATE KEYSPACE`)")
    }

    async fn create_data_db(&self) -> OpResult<()> {
        Self::unimpl("create_data_db")
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        Self::unimpl("run_catalog_migrations")
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        Self::unimpl("run_data_migrations")
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        Self::unimpl("record_data_connection")
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        Self::unimpl("bootstrap_encryption_key")
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        Self::unimpl("bootstrap_default_account")
    }

    async fn bootstrap_admin_user(
        &self,
        _env_user: Option<&str>,
        _env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        Self::unimpl("bootstrap_admin_user")
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        Self::unimpl("is_catalog_initialized")
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        Self::unimpl("list_table_names")
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        Self::unimpl("get_data_db_name")
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        Self::unimpl("drop_databases")
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        Self::unimpl("read_catalog_version")
    }

    fn expected_catalog_version(&self) -> String {
        "0.0.0-not-implemented".to_string()
    }

    fn catalog_database_name(&self) -> String {
        "<no cassandra keyspace>".to_string()
    }

    fn endpoint_info(&self) -> String {
        "cassandra: not implemented (see --backend help text)".to_string()
    }

    fn catalog_connection_url(&self) -> String {
        // Returned for display in the generated config. Anyone copying this
        // out of a config file will notice.
        "cassandra://placeholder/please-implement".to_string()
    }
}

/// Factory: returns a registered bootstrapper that declines every method.
const FACTORY: BootstrapperFactory = |config_path, cli_args| {
    Box::pin(async move {
        Ok(Box::new(CassandraBootstrapper {
            _config_path: config_path,
            _cli_args: cli_args,
        }) as Box<dyn Bootstrapper>)
    })
};

inventory::submit! {
    BackendRegistration {
        name: "cassandra",
        factory: FACTORY,
    }
}

#[allow(dead_code)]
fn _surface_storage_error_in_link_graph(err: StorageError) -> String {
    // Keeps `extenddb_storage::error::StorageError` in the link graph for
    // future implementers; not currently used because the factory itself
    // does not fail.
    format!("{err:?}")
}
