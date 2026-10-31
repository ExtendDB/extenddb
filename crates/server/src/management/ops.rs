// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared management operation types and HTTP response mapping.
//!
//! The SQL functions that previously lived here have been migrated to the
//! `ManagementStore` trait in `extenddb-storage`. This module retains the
//! server-layer `OpError` type and the `op_err_to_response` helper used
//! by management API handlers to convert errors to HTTP responses.

/// Error from a management operation (server layer).
///
/// This mirrors `extenddb_storage::management_store::OpError` but lives in the
/// server crate so HTTP response mapping stays close to the handlers.
#[derive(Debug)]
pub enum OpError {
    /// Input validation failed.
    Validation(String),
    /// Entity already exists (unique constraint violation).
    AlreadyExists(String),
    /// Referenced entity not found (FK violation or missing row).
    NotFound(String),
    /// Cannot delete due to dependent entities.
    HasDependents(String),
    /// Internal database error (message intentionally not exposed in HTTP responses).
    Internal(#[allow(dead_code)] String),
}

/// Map an `OpError` to an HTTP response for the management API.
///
/// `Internal` errors return 500 with no body to avoid leaking details.
pub fn op_err_to_response(e: OpError) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    match e {
        OpError::Validation(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
        OpError::AlreadyExists(msg) => (StatusCode::CONFLICT, msg).into_response(),
        OpError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
        OpError::HasDependents(msg) => (StatusCode::CONFLICT, msg).into_response(),
        OpError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

impl OpError {
    /// Convert a storage-layer `OpError` into the server-layer `OpError`.
    pub fn from_storage(e: extenddb_storage::management_store::OpError) -> Self {
        use extenddb_storage::management_store::OpError as StorageOpError;
        match e {
            StorageOpError::Validation(msg) => Self::Validation(msg),
            StorageOpError::AlreadyExists(msg) => Self::AlreadyExists(msg),
            StorageOpError::NotFound(msg) => Self::NotFound(msg),
            StorageOpError::HasDependents(msg) => Self::HasDependents(msg),
            StorageOpError::Internal(msg) | StorageOpError::Database { message: msg, .. } => {
                Self::Internal(msg)
            }
        }
    }
}
