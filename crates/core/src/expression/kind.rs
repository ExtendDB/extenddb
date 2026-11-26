// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression parameter kinds.

use std::fmt;

use crate::error::DynamoDbError;
use crate::limits::LimitsConfig;

/// A `DynamoDB` expression parameter.
///
/// The string form is the exact wire-format parameter name `DynamoDB` uses in
/// error messages (`Invalid <Kind>: ...`). Centralizing it here keeps those
/// labels typo-safe instead of repeating string literals at each call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionKind {
    Projection,
    Condition,
    Filter,
    KeyCondition,
    Update,
    /// The `SearchConditionExpression` of a `SearchVectors` request.
    SearchCondition,
}

impl ExpressionKind {
    /// The canonical `DynamoDB` parameter name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Projection => "ProjectionExpression",
            Self::Condition => "ConditionExpression",
            Self::Filter => "FilterExpression",
            Self::KeyCondition => "KeyConditionExpression",
            Self::Update => "UpdateExpression",
            Self::SearchCondition => "SearchConditionExpression",
        }
    }

    /// Whether the size-limit error appends `; expression size: N`. Amazon
    /// `DynamoDB` appends it for `Filter` and `Condition` only.
    ///
    /// `Condition` MUST stay in this set: `Filter` size errors are built as
    /// `Condition` then relabelled by `prefix_expression_error`, inheriting the
    /// suffix.
    #[must_use]
    pub fn size_error_includes_length(self) -> bool {
        matches!(self, Self::Filter | Self::Condition)
    }
}

impl fmt::Display for ExpressionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Reject an expression over the byte-size limit, measured on the raw text
/// before `#name` / `:value` substitution (Amazon `DynamoDB`: 4096). The error
/// carries the per-parameter prefix and, for Filter/Condition, the byte length.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when `expr` is over the limit.
pub fn check_expression_size(
    expr: &str,
    kind: ExpressionKind,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if expr.len() > limits.max_expression_length_bytes {
        let mut msg =
            format!("Invalid {kind}: Expression size has exceeded the maximum allowed size;");
        if kind.size_error_includes_length() {
            use std::fmt::Write as _;
            let _ = write!(msg, " expression size: {}", expr.len());
        }
        return Err(DynamoDbError::ValidationException(msg));
    }
    Ok(())
}
