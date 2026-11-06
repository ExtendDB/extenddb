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
    /// A catalog row that already exists, or whose claim a live owner holds
    /// (`BackupCatalog::claim_backup`).
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    /// A failure that is expected to succeed on retry: I/O errors, pool
    /// timeouts, SQLITE_BUSY / SQLITE_LOCKED. Exists so queue workers can tell
    /// "this row can never be applied" (drop it, or the whole queue stalls)
    /// from "the database hiccuped" (retry it, or the row's index write is
    /// silently lost). Before this distinction both collapsed to `Internal`
    /// and the worker dropped claimed rows on transient errors.
    #[error("{0}")]
    Transient(String),
    /// The backend discarded a consistent snapshot before the reader drained
    /// it (`MongoDB` `SnapshotTooOld`, a `PostgreSQL` snapshot conflict on a hot
    /// standby). The operation that held it must restart from the beginning;
    /// nothing read so far may be combined with a fresh snapshot.
    #[error("snapshot expired: {0}")]
    SnapshotExpired(String),
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

impl StorageError {
    /// Whether this error from a write is a transient fault a retry is
    /// expected to clear, so a bulk operation whose write batch met it is
    /// left non-terminal to resume rather than ended `FAILED`. The class is
    /// keyed on the variant and, for an internal error, on the SQLSTATE
    /// code the backends prefix, never on free message text, and is kept
    /// narrow: anything not positively a transient condition is not one.
    ///
    /// - `Transient` is the backends' own class for a retryable fault: a
    ///   pool timeout, an I/O error, and SQLite's BUSY and LOCKED result
    ///   codes.
    /// - `Connection` is a connection-level fault.
    /// - `Internal` carrying a transient SQLSTATE: a PostgreSQL statement
    ///   aborted by a connection or shutdown condition, a serialization or
    ///   deadlock abort, a lock or statement timeout, or an exhausted
    ///   resource such as a full disk surfaces as an internal error whose
    ///   message the backend renders with the `SQLSTATE <code>:` prefix; the
    ///   code there is the discriminant.
    #[must_use]
    pub fn is_transient_write_fault(&self) -> bool {
        match self {
            Self::Transient(_) | Self::Connection(_) => true,
            Self::Internal(message) => has_transient_sqlstate(message),
            _ => false,
        }
    }
}

/// Whether an internal storage error's message carries a PostgreSQL
/// SQLSTATE that names a transient condition, read from the fixed `SQLSTATE
/// <code>:` prefix the backend writes (never from the human text that
/// follows). The transient codes: the connection exception class (`08...`),
/// a full disk and too many connections (`53100`, `53300`), which clear once
/// the operator frees the resource, the admin and crash shutdown family
/// (`57P01`, `57P02`, `57P03`), a statement cancel or lock timeout (`57014`,
/// `55P03`), and the two serialization aborts (`40001`, `40P01`).
fn has_transient_sqlstate(message: &str) -> bool {
    let Some(rest) = message.strip_prefix("SQLSTATE ") else {
        return false;
    };
    let code = rest
        .split(|c: char| !c.is_ascii_alphanumeric())
        .next()
        .unwrap_or("");
    code.starts_with("08")
        || matches!(
            code,
            "53100" | "53300" | "57P01" | "57P02" | "57P03" | "57014" | "55P03" | "40001" | "40P01"
        )
}
