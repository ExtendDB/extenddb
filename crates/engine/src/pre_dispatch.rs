// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The request checks the engine owns, which every entry point calls in one
//! order before it dispatches a request into the engine: the operation name,
//! the body parse, and the refusal of point-in-time recovery.
//!
//! An entry point (the HTTP server, the WebAssembly adapter) calls
//! [`parse_request`] with the operation name and the raw request body, then
//! calls [`ParsedRequest::admit`] on the result to get the body it passes to
//! [`crate::dispatch`]. A check added to either step reaches both entry
//! points.
//!
//! They are the only checks both entry points make before dispatch. Every
//! other check the server makes before it dispatches a request is its own
//! (most of them are in its request handler, `handle_request`, and its
//! authorization, `authorize_request`), and apart from the IAM decision each
//! runs in developer mode too. The WebAssembly adapter makes none of them, so
//! a check that must reach both entry points belongs in one of the two steps
//! here.
//!
//! The types fix the order of the two steps: `admit` is a method of
//! [`ParsedRequest`], which only `parse_request` constructs, so `admit` runs
//! only on a request whose operation is known and whose body parsed. The
//! refusal `admit` applies, [`crate::refuse_point_in_time_recovery`], is also
//! public on its own and checks whatever body it is given.

use extenddb_core::error::{DynamoDbError, malformed_body_error};
use serde_json::Value;

/// A request whose operation name is known and whose body parsed as JSON.
///
/// Returned by [`parse_request`]; [`ParsedRequest::admit`] applies the
/// refusals that follow and yields the body to dispatch.
// Public for its two callers, the HTTP server's `handle_request` and the
// WebAssembly adapter's `Engine::dispatch`, not for other users of the crate.
#[doc(hidden)]
#[derive(Debug)]
pub struct ParsedRequest<'a> {
    operation: &'a str,
    body: Value,
}

/// The first step: check that `operation` is one the engine dispatches, then
/// parse `body` as JSON.
///
/// An unknown operation answers `UnknownOperationException` even when the
/// body is also malformed, and a body that does not parse answers the
/// `SerializationException` of [`malformed_body_error`]. Neither reads the
/// caller's identity, so the server calls this before it authenticates the
/// request: real `DynamoDB` answers both before it checks authentication.
///
/// # Errors
///
/// `UnknownOperationException` for an operation the engine does not
/// dispatch; `SerializationException` for a body that is not valid JSON.
// Public for its two callers, the HTTP server's `handle_request` and the
// WebAssembly adapter's `Engine::dispatch`, not for other users of the crate.
#[doc(hidden)]
pub fn parse_request<'a>(
    operation: &'a str,
    body: &[u8],
) -> Result<ParsedRequest<'a>, DynamoDbError> {
    if !crate::is_known_operation(operation) {
        return Err(DynamoDbError::UnknownOperationException(String::new()));
    }
    let body = serde_json::from_slice(body).map_err(|e| malformed_body_error(&e))?;
    Ok(ParsedRequest { operation, body })
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
