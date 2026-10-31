// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The message builders for the semantic rules. Each one carries the
//! service's text for one rule, with the parameter name `kind` supplies.

use super::super::kind::ExpressionKind;
use crate::error::DynamoDbError;

pub(super) fn validation(kind: ExpressionKind, msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("Invalid {kind}: {msg}"))
}

pub(super) fn with_kind(err: DynamoDbError, kind: ExpressionKind) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg) if !msg.starts_with("Invalid ") => {
            validation(kind, &msg)
        }
        other => other,
    }
}

pub(super) fn invalid_function_name(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(kind, &format!("Invalid function name; function: {name}"))
}

pub(super) fn not_allowed_this_way(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "The function is not allowed to be used this way in an expression; function: {name}"
        ),
    )
}

pub(super) fn operand_count_err(kind: ExpressionKind, name: &str, count: usize) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "Incorrect number of operands for operator or function; operator or function: {name}, number of operands: {count}"
        ),
    )
}

pub(super) fn requires_document_path(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(
        kind,
        &format!("Operator or function requires a document path; operator or function: {name}"),
    )
}

pub(super) fn operand_type_err(kind: ExpressionKind, name: &str, type_code: &str) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "Incorrect operand type for operator or function; operator or function: {name}, operand type: {type_code}"
        ),
    )
}
