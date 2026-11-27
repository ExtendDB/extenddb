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

use crate::{ItemAccess, ItemAuthorization, ItemAuthorizer, OperationContext};

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
    let mut ctx =
        crate::test_context::context_on(crate::test_context::memory_engine().await, "000000000000");
    ctx.authorized_key_info.insert("t".to_owned(), None);
    ctx.item_authorizer = authorizer;
    ctx
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
