// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The [`OperationContext`]s of the unit tests that need one, and item
//! authorizers that answer every access the same way. The catalog of
//! [`context`] is an empty in-memory database, and its snapshot holds the
//! table "t" as missing, so that nothing the PartiQL layer does for "t"
//! reads the catalog. [`catalog_context`] has a catalog that tables are
//! created in.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::{SqliteConfig, SqliteEngine};

use crate::{
    AuthorizedKeyInfo, BackupRuntime, ItemAccess, ItemAuthorization, ItemAuthorizer,
    OperationContext,
};

/// Run `future` to completion on a runtime of its own.
pub(super) fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// A context whose item authorizer is `authorizer`.
pub(super) async fn context(authorizer: Option<Arc<dyn ItemAuthorizer>>) -> OperationContext {
    let storage = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("an in-memory database opens");
    let mut authorized_key_info = AuthorizedKeyInfo::new();
    authorized_key_info.insert("t".to_owned(), None);
    OperationContext {
        storage: Arc::new(storage),
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from("000000000000"),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info,
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: authorizer,
    }
}

/// A context whose catalog is an in-memory database that holds the account
/// the backend bootstraps, in which a table is ACTIVE once it is created,
/// and whose item authorizer allows every access.
pub(super) async fn catalog_context() -> OperationContext {
    let config = SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 1,
    };
    let mut options = ServerComponentsOptions::default();
    options.release = "test";
    options.startup_recovery = false;
    let components =
        (extenddb_storage_sqlite::backend().server_components)(&config, "us-east-1", options)
            .await
            .unwrap_or_else(|e| panic!("server components: {e:?}"));
    let catalog = &components.catalog_store;
    let account_id = catalog
        .default_account_id()
        .await
        .unwrap()
        .expect("the bootstrapped account");
    catalog
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .unwrap();
    let allowing: Arc<dyn ItemAuthorizer> = Fixed::allowing();
    OperationContext {
        storage: components.engine,
        account_id: Arc::from(account_id),
        authorized_key_info: AuthorizedKeyInfo::new(),
        ..context(Some(allowing)).await
    }
}

/// Gives every access the same answer, and counts the accesses it is asked
/// about.
pub(super) struct Fixed {
    answer: Result<(), DynamoDbError>,
    asked: AtomicUsize,
}

impl Fixed {
    fn new(answer: Result<(), DynamoDbError>) -> Arc<Self> {
        Arc::new(Self {
            answer,
            asked: AtomicUsize::new(0),
        })
    }

    /// Allows every access.
    pub(super) fn allowing() -> Arc<Self> {
        Self::new(Ok(()))
    }

    /// Refuses every access.
    pub(super) fn refusing() -> Arc<Self> {
        Self::new(Err(DynamoDbError::AccessDeniedException(
            "refused".to_owned(),
        )))
    }

    /// Fails every access with a fault.
    pub(super) fn failing() -> Arc<Self> {
        Self::new(Err(DynamoDbError::InternalServerError(
            "Internal error during authorization".to_owned(),
        )))
    }

    /// The number of accesses asked about.
    pub(super) fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}

impl ItemAuthorizer for Fixed {
    fn authorize<'a>(&'a self, _access: &'a ItemAccess) -> ItemAuthorization<'a> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        let answer = self.answer.clone();
        Box::pin(async move { answer })
    }
}
