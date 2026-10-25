// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The log line for a storage fault the client sees as a redacted 500.
//!
//! `log_storage_fault!` expands at its call site, so the event keeps the
//! site's own `target`, `module_path`, `file` and `line`; an operator who
//! filters on `extenddb_engine::backup` still matches the backup mapper's
//! line. The macro adds `request_id` and `operation` from the request scope
//! the server sets around the request (see `extenddb_storage::request_scope`),
//! so the line can be matched to the `x-amzn-RequestId` the client received.
//! Outside a request both are `None`, and `tracing` records nothing for a
//! `None`, so the line carries neither field.

/// Write an error-level event for a storage fault.
///
/// `log_storage_fault!(error, "message")` records `internal_error` from
/// `error` (by `Display`) with the message literal. The message may carry
/// format arguments: `log_storage_fault!(error, "catalog version mismatch:
/// expected {expected}, found {found}")`.
macro_rules! log_storage_fault {
    ($error:expr, $($message:tt)+) => {{
        let (request_id, operation) = extenddb_storage::request_scope::log_fields();
        tracing::error!(
            internal_error = %$error,
            request_id = request_id.as_deref(),
            operation = operation.as_deref(),
            $($message)+
        );
    }};
}
