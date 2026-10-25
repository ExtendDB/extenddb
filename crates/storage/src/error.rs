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
    /// A table name taken by a `CREATING` restore target. Raised by
    /// `RestoreTableFromBackup` into that name (the engine answers
    /// `TableInUseException`), distinct from [`Self::TableAlreadyExists`].
    #[error("Table: {table_name} is already being restored from backup: {backup_arn}")]
    TableBeingRestored {
        table_name: String,
        backup_arn: String,
    },
    #[error("Index not found: {0}")]
    IndexNotFound(String),
    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),
    /// A change refused because an index on the resource is mid-transition.
    /// Carries the whole client-facing message, because the state, and
    /// therefore the wording, is known only to the backend that holds it: a
    /// DeleteTable refused while indexes build carries the sentence AWS
    /// documents for that case, and a vector index deleted while its creation
    /// is still allocating resources carries the measured phase-dependent
    /// refusal (2026-08-19). Maps to `ResourceInUseException` verbatim.
    #[error("{0}")]
    IndexesInUse(String),
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
    /// A backup an in-progress restore is still reading. Maps to
    /// `BackupInUseException`.
    #[error("{0}")]
    BackupInUse(String),
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
    /// A client error the backend has already classified. The engine passes it
    /// to the client with its variant and message unchanged and does not log it
    /// as a storage fault. One exception: a `ConditionalCheckFailedException`
    /// carries its item to the client only when the request set
    /// `ReturnValuesOnConditionCheckFailure` to `ALL_OLD`, the rule the engine
    /// applies to its own `ConditionFailed`. Use this
    /// when the backend can map its own failure onto a specific service error
    /// with its own message, for example a hosted database reporting a
    /// documented unavailability or capacity condition.
    ///
    /// The message inside crosses the wire verbatim; the engine does not
    /// inspect or redact it. It must be the text the service documents for that
    /// error class, or text of the same shape, and must not contain driver
    /// output, host names, connection strings or credentials. A failure the
    /// backend cannot classify stays [`Self::Internal`], which the engine
    /// redacts behind a fixed string. Distinct from [`Self::Transient`], which
    /// the engine also redacts and which the queue workers read as the retry
    /// signal; `Service` is for request paths only.
    #[error(transparent)]
    Service(extenddb_core::error::DynamoDbError),
}
