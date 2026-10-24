// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Semantic validation of parsed `UpdateExpression` and `ConditionExpression`
//! trees, run after the grammar pass and before any table is read.
//!
//! Amazon DynamoDB reports every syntax error first, then walks the parsed
//! tree once, in expression order, checking each node after its operands
//! (a post-order walk), and only then checks overlapping paths and unused
//! names and values. The order of the messages below was measured against
//! the service on 2026-10-14; each rule names the request that pinned it.
//!
//! Update expression, per node:
//! - a document path: every `#name` element is defined;
//! - a `:value` placeholder: defined;
//! - `+` and `-`: after both operands, a placeholder operand must be a
//!   number (`SET a = :s + :n` with a string `:s`);
//! - a function call: after its operands, `list_append` may not be an
//!   operand of a function or of `+`/`-` (`if_not_exists(list_append(:a,
//!   :b), :v)`); the name must be `if_not_exists` or `list_append` in that
//!   case (`IF_NOT_EXISTS(x, :v)` is refused); a condition function is
//!   refused by name (`SET x = size(x)`); both take two operands; the first
//!   operand of `if_not_exists` is a document path; a placeholder operand of
//!   `list_append` is a list;
//! - an `ADD` or `DELETE` action: after the path and the placeholder, the
//!   value is a number or a set for `ADD` and a set for `DELETE`;
//! - a section: after its actions, the keyword may not have appeared before
//!   (`SET a = :v SET b = :q` reports `:q`, `SET a = :v SET b = :v ADD n :s`
//!   reports the repeated `SET`).
//!
//! Condition expression, per node:
//! - operands of a comparison, `BETWEEN`, or `IN` are walked first; a
//!   function in operand position must be `size`, and `size` in condition
//!   position is refused (`attribute_exists(pk) = :v`, `size(pk)`);
//! - a function call: after its operands, the name must be one of the six
//!   condition functions as written (`ATTRIBUTE_EXISTS(pk)` is refused),
//!   `if_not_exists` and `list_append` are refused by name, the operand count
//!   is fixed per function, `attribute_exists` and `attribute_not_exists`
//!   take a document path (`attribute_type(:v, :t)` is accepted), the second operand of
//!   `begins_with` is a string or binary, and the second operand of
//!   `attribute_type` is a string naming a type;
//! - a comparison or `BETWEEN` with an ordering operator: a placeholder
//!   operand is a string, number, or binary.

const CONDITION_FUNCTIONS: [&str; 6] = [
    "attribute_exists",
    "attribute_not_exists",
    "attribute_type",
    "begins_with",
    "contains",
    "size",
];

const UPDATE_FUNCTIONS: [&str; 2] = ["if_not_exists", "list_append"];

mod condition_rules;
mod errors;
mod refs;
#[cfg(test)]
mod tests;
mod update_rules;

pub use condition_rules::validate_condition_semantics;
pub use update_rules::validate_update_semantics;
