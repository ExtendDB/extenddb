// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression parameter kinds.

use std::fmt;

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
