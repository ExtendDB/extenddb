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
    /// A single-item write raced an in-flight `TransactWriteItems` on
    /// the same item, and the backend was unable to serialize the two.
    /// Maps to `DynamoDbError::TransactionConflictException` at the
    /// engine boundary — DynamoDB's canonical error for this case
    /// (RFC-0003 §4.3).
    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),
    #[error("No-op update: {0}")]
    NoOpUpdate(String),
    #[error("Validation error: {0}")]
    Validation(String),
    /// A per-table or per-account limit was exceeded. Maps to
    /// `LimitExceededException`, which the service uses for the vector-index
    /// count limit on `UpdateTable` (a DIFFERENT class from the
    /// `ValidationException` `CreateTable` reports for the same limit;
    /// measured 2026-08-13).
    #[error("{0}")]
    LimitExceeded(String),
    /// A failure that is expected to succeed on retry: I/O errors, pool
    /// timeouts, SQLITE_BUSY / SQLITE_LOCKED. Exists so queue workers can tell
    /// "this row can never be applied" (drop it, or the whole queue stalls)
    /// from "the database hiccuped" (retry it, or the row's index write is
    /// silently lost). Before this distinction both collapsed to `Internal`
    /// and the worker dropped claimed rows on transient errors.
    #[error("{0}")]
    Transient(String),
    #[error(
        "Catalog version mismatch: expected {expected}, found {found}. Run 'extenddb migrate' to update."
    )]
    CatalogVersionMismatch { expected: String, found: String },
    #[error("Catalog not initialized. Run 'extenddb init' to set up the catalog.")]
    CatalogNotInitialized,
    #[error("Connection error: {0}")]
    Connection(String),
    /// The backend does not implement the requested feature. Distinct from
    /// `Internal`, which reports a fault: this reports a capability the backend
    /// never claimed, so it is not a bug and must not be logged as one.
    #[error("Not supported by this storage backend: {0}")]
    Unsupported(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
