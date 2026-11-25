// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The log line for a storage fault the client sees as a redacted 500 or as
//! a retryable 503.
//!
//! `log_storage_fault!` expands at its call site, so the event keeps the
//! site's own `target`, `module_path`, `file` and `line`; an operator who
//! filters on `extenddb_engine::backup` still matches the backup mapper's
//! line. The macro adds `request_id` and `operation` from the request scope
//! the server sets around the request (see `extenddb_storage::request_scope`),
//! so the line can be matched to the `x-amzn-RequestId` the client received.
//! Outside a request both are `None`, and `tracing` records nothing for a
//! `None`, so the line carries neither field.

/// Write an event for a storage fault.
///
/// `log_storage_fault!(error, "message")` writes an error-level event that
/// records `internal_error` from `error` (by `Display`) with the message
/// literal. The message may carry format arguments:
/// `log_storage_fault!(error, "backup {arn_text}: reading {name} from the
/// store failed")`.
///
/// `log_storage_fault!(warn, field = error, "message")` writes the same
/// event at warn level, with the error recorded under `field`: the form for
/// a fault the client sees as a 503 it retries, such as a transient error.
macro_rules! log_storage_fault {
    (@event $level:expr, $field:ident, $error:expr, $($message:tt)+) => {{
        let (request_id, operation) = extenddb_storage::request_scope::log_fields();
        tracing::event!(
            $level,
            $field = %$error,
            request_id = request_id.as_deref(),
            operation = operation.as_deref(),
            $($message)+
        );
    }};
    (warn, $field:ident = $error:expr, $($message:tt)+) => {
        log_storage_fault!(@event tracing::Level::WARN, $field, $error, $($message)+)
    };
    ($error:expr, $($message:tt)+) => {
        log_storage_fault!(@event tracing::Level::ERROR, internal_error, $error, $($message)+)
    };
}
