// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Store error type.
//!
//! Display messages carry operation context, key names, and service error
//! codes. They never carry credentials: S3 service errors are reduced to
//! their error code and service message before formatting, and transport
//! errors wrap the SDK error whose display contains no credential material.

/// Error returned by every [`crate::BackupStore`] operation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The requested object does not exist.
    #[error("object not found")]
    NotFound,

    /// The store refused the operation: filesystem permissions, a resolved
    /// path escaping the configured root, or an S3 `AccessDenied`. The message
    /// names what was refused.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// The key or prefix failed validation. The message names the rule that
    /// rejected it.
    #[error("invalid key: {0}")]
    InvalidKey(String),

    /// A local I/O failure. `context` names the operation and path.
    #[error("{context}: {source}")]
    Io {
        /// Operation and path the failure occurred on.
        context: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// A network or protocol failure talking to a remote store.
    #[error("transport error: {source}")]
    Transport {
        /// Underlying transport error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Any other failure. The message is self-contained.
    #[error("{0}")]
    Other(String),
}

impl StoreError {
    /// Wrap an I/O error with the operation and path it occurred on.
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}
