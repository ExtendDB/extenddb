// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The request checks the engine owns, which every entry point calls in one
//! order before it dispatches a request into the engine: the operation name,
//! then the refusal of point-in-time recovery, with the entry point's parse of
//! the body between them.
//!
//! An entry point calls [`check_operation`] with the operation name, parses
//! the raw request body, passes the parsed body to
//! [`KnownOperation::with_body`], and calls [`ParsedRequest::admit`] on the
//! result to get the body it passes to [`crate::dispatch`]. [`parse_request`]
//! is `check_operation` and a `serde_json` parse of the body in one call. The
//! WebAssembly adapter calls `parse_request(..)?.admit()`. The HTTP server
//! calls `check_operation` before it parses the body, parses the body with
//! its own parser (`crates/server/src/request_parse.rs`, which bounds the
//! nesting depth and checks the syntax of a deep body before
//! authentication), and calls `with_body(..).admit()` after it authenticates
//! the caller.
//!
//! A check that every request the engine dispatches must pass belongs in
//! `check_operation` or in `admit`. A check that an entry point makes itself,
//! its parse of the body included, reaches only the requests that come
//! through that entry point.
//!
//! The types fix the order of the steps: `admit` is a method of
//! [`ParsedRequest`], which only `with_body` constructs, and `with_body` is a
//! method of [`KnownOperation`], which only `check_operation` constructs. So
//! `admit` runs only on a request whose operation is known, with the body the
//! entry point parsed. The refusal `admit` applies,
//! [`crate::refuse_point_in_time_recovery`], is also public on its own and
//! checks whatever body it is given.

use extenddb_core::error::{DynamoDbError, malformed_body_error};
use serde_json::Value;

/// An operation name the engine dispatches.
///
/// Returned by [`check_operation`]; [`KnownOperation::with_body`] pairs it
/// with the parsed request body.
// Public for the HTTP server's `handle_request`, not for other users of the
// crate.
#[doc(hidden)]
#[derive(Debug)]
pub struct KnownOperation<'a> {
    operation: &'a str,
}

/// A request whose operation name is known and whose body parsed as JSON.
///
/// Returned by [`KnownOperation::with_body`] and [`parse_request`];
/// [`ParsedRequest::admit`] applies the refusals that follow and yields the
/// body to dispatch.
// Public for its two callers, the HTTP server's `handle_request` and the
// WebAssembly adapter's `Engine::dispatch`, not for other users of the crate.
#[doc(hidden)]
#[derive(Debug)]
pub struct ParsedRequest<'a> {
    operation: &'a str,
    body: Value,
}

/// The first step: check that `operation` is one the engine dispatches.
///
/// An unknown operation answers `UnknownOperationException` whatever the
/// body, so an entry point calls this before it parses the body. The check
/// does not read the caller's identity, so the server calls it before it
/// authenticates the request: real `DynamoDB` answers an unknown operation
/// before it checks authentication.
///
/// # Errors
///
/// `UnknownOperationException` for an operation the engine does not
/// dispatch.
// Public for the HTTP server's `handle_request`, not for other users of the
// crate.
#[doc(hidden)]
pub fn check_operation(operation: &str) -> Result<KnownOperation<'_>, DynamoDbError> {
    if crate::is_known_operation(operation) {
        Ok(KnownOperation { operation })
    } else {
        Err(DynamoDbError::UnknownOperationException(String::new()))
    }
}

/// [`check_operation`], then a parse of `body` as JSON with `serde_json`'s
/// defaults.
///
/// An unknown operation answers `UnknownOperationException` even when the
/// body is also malformed, and a body that does not parse answers the
/// `SerializationException` of [`malformed_body_error`].
///
/// # Errors
///
/// `UnknownOperationException` for an operation the engine does not
/// dispatch; `SerializationException` for a body that does not parse as JSON.
// Public for the WebAssembly adapter's `Engine::dispatch`, not for other users
// of the crate.
#[doc(hidden)]
pub fn parse_request<'a>(
    operation: &'a str,
    body: &[u8],
) -> Result<ParsedRequest<'a>, DynamoDbError> {
    let operation = check_operation(operation)?;
    let body = serde_json::from_slice(body).map_err(|e| malformed_body_error(&e))?;
    Ok(operation.with_body(body))
}

impl<'a> KnownOperation<'a> {
    /// Pair the operation with the request body the entry point parsed.
    #[must_use]
    pub fn with_body(self, body: Value) -> ParsedRequest<'a> {
        ParsedRequest {
            operation: self.operation,
            body,
        }
    }
}

impl ParsedRequest<'_> {
    /// The second step: refuse the requests no deployment serves, then return
    /// the body to dispatch.
    ///
    /// Today that is point-in-time recovery: every `RestoreTableToPointInTime`
    /// and an `UpdateContinuousBackups` that enables it are refused through
    /// [`crate::refuse_point_in_time_recovery`]. The refusal reads only the
    /// body, so the server applies it after authentication and before it
    /// authorizes the request or reads a table.
    ///
    /// # Errors
    ///
    /// The refusal's error for a point-in-time recovery request it refuses.
    pub fn admit(self) -> Result<Value, DynamoDbError> {
        match crate::refuse_point_in_time_recovery(self.operation, &self.body) {
            Some(refusal) => Err(refusal),
            None => Ok(self.body),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_type(err: &DynamoDbError) -> String {
        err.full_error_type()
    }

    /// An unknown operation is reported as unknown even when its body is not
    /// JSON: the name is checked before the body is parsed.
    #[test]
    fn an_unknown_operation_beats_a_malformed_body() {
        let err = parse_request("NoSuchOperation", b"{ not json").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::UnknownOperationException(_)),
            "{}",
            error_type(&err)
        );
    }

    /// A malformed point-in-time restore is answered as malformed: the body is
    /// parsed before the point-in-time refusal is applied.
    #[test]
    fn a_malformed_body_beats_the_point_in_time_refusal() {
        let err = parse_request("RestoreTableToPointInTime", b"{ not json").unwrap_err();
        assert!(
            matches!(err, DynamoDbError::SerializationException(_)),
            "{}",
            error_type(&err)
        );
    }

    /// The malformed-body answer is the shared helper's.
    #[test]
    fn a_malformed_body_gets_the_shared_serialization_error() {
        let parse_error = serde_json::from_slice::<Value>(b"").unwrap_err();
        let err = parse_request("GetItem", b"").unwrap_err();
        assert_eq!(err.message(), malformed_body_error(&parse_error).message());
    }

    /// A well-formed point-in-time restore and an enable pass the first step
    /// and are refused by the second.
    #[test]
    fn admit_refuses_point_in_time_recovery() {
        let restore = parse_request("RestoreTableToPointInTime", b"{}").expect("parses");
        let err = restore.admit().unwrap_err();
        assert!(
            matches!(
                err,
                DynamoDbError::PointInTimeRecoveryUnavailableException(_)
            ),
            "{}",
            error_type(&err)
        );

        let enable = parse_request(
            "UpdateContinuousBackups",
            br#"{"TableName":"T","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}"#,
        )
        .expect("parses");
        let err = enable.admit().unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ContinuousBackupsUnavailableException(_)),
            "{}",
            error_type(&err)
        );
    }

    /// Every other request is admitted with the body it was parsed from.
    #[test]
    fn admit_returns_the_parsed_body_otherwise() {
        let request = parse_request("GetItem", br#"{"TableName":"T"}"#).expect("parses");
        let body = request.admit().expect("admitted");
        assert_eq!(body, serde_json::json!({ "TableName": "T" }));
    }
}
