// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An [`OperationContext`] for the unit tests that need one, and item
//! authorizers that answer every access the same way. The context's catalog
//! is an empty in-memory database, and its snapshot holds the table "t" as
//! missing, so that nothing the PartiQL layer does for "t" reads the
//! catalog.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_storage_sqlite::SqliteEngine;

use crate::{AuthorizedKeyInfo, ItemAccess, ItemAuthorization, ItemAuthorizer, OperationContext};

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
