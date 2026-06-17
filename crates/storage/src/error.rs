// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use extenddb_core::types::{CancellationReason, Item};

#[derive(Debug, Clone, thiserror::Error)]
pub enum StorageError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("Table is not in ACTIVE state: {0}")]
    TableNotActive(String),
    #[error("Index not found: {0}")]
    IndexNotFound(String),
    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),
    #[error("Deletion protection enabled: {0}")]
    DeletionProtected(String),
    #[error("Condition check failed")]
    ConditionFailed(Option<Item>),
    #[error("Transaction canceled")]
    TransactionCanceled(Vec<CancellationReason>),
    #[error("Idempotent replay")]
    IdempotentReplay,
    #[error("Idempotent parameter mismatch")]
    IdempotentMismatch,
    #[error("No-op update: {0}")]
    NoOpUpdate(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error(
        "Catalog version mismatch: expected {expected}, found {found}. Run 'extenddb migrate' to update."
    )]
    CatalogVersionMismatch { expected: String, found: String },
    #[error("Catalog not initialized. Run 'extenddb init' to set up the catalog.")]
    CatalogNotInitialized,
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
