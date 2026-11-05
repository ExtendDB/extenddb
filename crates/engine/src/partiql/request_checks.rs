// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The constraints the request model puts on the fields of the three PartiQL
//! requests, checked before anything else in a request is read.
//!
//! The service reports every failed constraint of a request in one
//! `ValidationException`: "1 validation error detected: <part>" for one and
//! "N validation errors detected: <part>; <part>" for more, each part worded
//! as it is on its own, at most ten of them. A part echoes the value as it
//! was sent, or `null` for a member the request does not carry. The parts
//! come in this order (measured 2026-11-02):
//!
//! - `ExecuteStatement`: `returnValuesOnConditionCheckFailure`, `nextToken`,
//!   `limit`, `statement`, `returnConsumedCapacity`, `parameters`.
//! - `BatchExecuteStatement`: `returnConsumedCapacity`, `statements` (the
//!   count), then each statement in order: its
//!   `returnValuesOnConditionCheckFailure`, `statement` and `parameters`.
//! - `ExecuteTransaction`: `clientRequestToken`, `transactStatements` (the
//!   count), each statement as in a batch, then `returnConsumedCapacity`.
//!
//! A statement holds 1 to 8192 code points, a `Parameters` list at least one
//! value, a `NextToken` 1 to 32768 code points, a `ClientRequestToken` 1 to
//! 36, a batch 1 to 25 statements and a transaction 1 to 100. The two enum
//! members take the values the service lists in its refusal. A statement
//! list past its limit is echoed as the service prints its request objects,
//! up to a fixed size (see [`echo`]).

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::statement_length_refusal;

use echo::{Echo, echo_list, write_batch_member, write_transaction_member};

mod echo;

/// The most statements a `BatchExecuteStatement` holds.
const MAX_BATCH_STATEMENTS: usize = 25;

/// The most statements an `ExecuteTransaction` holds.
const MAX_TRANSACTION_STATEMENTS: usize = 100;

/// The longest `ClientRequestToken`, in code points.
const MAX_CLIENT_REQUEST_TOKEN: usize = 36;

/// The longest `NextToken`, in code points.
const MAX_NEXT_TOKEN: usize = 32768;

/// The most failed constraints one refusal lists; its count then says 10 too
/// (measured 2026-11-02).
const MAX_PARTS: usize = 10;

/// The constraint an empty statement or list fails.
const AT_LEAST_ONE: &str = "Member must have length greater than or equal to 1";

/// The constraint a member the request does not carry fails.
const NOT_NULL: &str = "Member must not be null";

/// The values of `ReturnValuesOnConditionCheckFailure`, as the service lists
/// them.
const ON_FAILURE_VALUES: &[&str] = &["ALL_OLD", "NONE"];

/// The values of `ReturnConsumedCapacity`, as the service lists them.
const CONSUMED_CAPACITY_VALUES: &[&str] = &["INDEXES", "TOTAL", "NONE"];

/// The refusal a `BatchExecuteStatement` member gets for a statement that
/// holds only whitespace (see [`is_blank_statement`]).
pub(super) const EMPTY_BATCH_STATEMENT: &str = "The statement was empty.";

/// The refusal of an `ExecuteTransaction` with a member whose statement holds
/// only whitespace (see [`is_blank_statement`]). It names no member.
pub(super) const EMPTY_TRANSACTION_STATEMENT: &str =
    "ExecuteTransaction can not have empty statement.";

/// Whether a statement that passed the length constraint holds only
/// whitespace. `BatchExecuteStatement` refuses such a member and
/// `ExecuteTransaction` the whole request, each ahead of the member's
/// parameters and of the members after it, after the members before it;
/// `ExecuteStatement` reads it as a statement with nothing in it (measured
/// 2026-11-05). Measured as whitespace: space, tab, line feed, the vertical
/// tab, form feed, carriage return, U+001F, U+2028 and U+3000. Measured as
/// not: U+0000, U+0001, U+0085, U+00A0 and a comment. The other information
/// separators (U+001C to U+001E), the paragraph separator and the Unicode
/// spaces other than the no-break ones are taken as whitespace with them.
pub(super) fn is_blank_statement(statement: &str) -> bool {
    statement.chars().all(|c| {
        matches!(
            c,
            '\t'..='\r'
                | '\u{1c}'..='\u{20}'
                | '\u{1680}'
                | '\u{2000}'..='\u{2006}'
                | '\u{2008}'..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{205f}'
                | '\u{3000}'
        )
    })
}

/// Check an `ExecuteStatement` request.
///
/// # Errors
///
/// Returns the service's refusal naming every failed constraint.
pub(super) fn check_execute_statement(body: &Value) -> Result<(), DynamoDbError> {
    let mut found = Found::default();
    found.on_failure(body, "returnValuesOnConditionCheckFailure");
    found.length(body.get("NextToken"), "nextToken", MAX_NEXT_TOKEN);
    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        found.add(
            &limit.to_string(),
            "limit",
            "Member must have value greater than or equal to 1",
        );
    }
    found.statement(body, "statement");
    found.consumed_capacity(body);
    found.parameters(body, "parameters");
    found.into_result()
}

/// Check a `BatchExecuteStatement` request.
///
/// # Errors
///
/// Returns the service's refusal naming every failed constraint.
pub(super) fn check_batch_execute_statement(body: &Value) -> Result<(), DynamoDbError> {
    let mut found = Found::default();
    found.consumed_capacity(body);
    found.statements(
        body.get("Statements"),
        "statements",
        MAX_BATCH_STATEMENTS,
        write_batch_member,
    );
    found.into_result()
}

/// Check an `ExecuteTransaction` request.
///
/// # Errors
///
/// Returns the service's refusal naming every failed constraint.
pub(super) fn check_execute_transaction(body: &Value) -> Result<(), DynamoDbError> {
    let mut found = Found::default();
    found.length(
        body.get("ClientRequestToken"),
        "clientRequestToken",
        MAX_CLIENT_REQUEST_TOKEN,
    );
    found.statements(
        body.get("TransactStatements"),
        "transactStatements",
        MAX_TRANSACTION_STATEMENTS,
        write_transaction_member,
    );
    found.consumed_capacity(body);
    found.into_result()
}

/// The failed constraints of one request, each worded as the service words
/// it, in the order the service reports them.
#[derive(Default)]
struct Found(Vec<String>);

impl Found {
    /// A value that fails `constraint`, echoed as it was sent.
    fn add(&mut self, value: &str, path: &str, constraint: &str) {
        self.0.push(format!(
            "Value '{value}' at '{path}' failed to satisfy constraint: {constraint}"
        ));
    }

    /// A member the request does not carry.
    fn null(&mut self, path: &str) {
        self.0.push(format!(
            "Value null at '{path}' failed to satisfy constraint: {NOT_NULL}"
        ));
    }

    /// A token of 1 to `max` code points.
    fn length(&mut self, value: Option<&Value>, path: &str, max: usize) {
        let Some(Value::String(token)) = value else {
            return;
        };
        let length = token.chars().count();
        if length == 0 {
            self.add(token, path, AT_LEAST_ONE);
        } else if length > max {
            self.add(
                token,
                path,
                &format!("Member must have length less than or equal to {max}"),
            );
        }
    }

    /// An enum member's value, which must be one of `values`.
    fn one_of(&mut self, value: Option<&Value>, path: &str, values: &[&str]) {
        if let Some(Value::String(value)) = value
            && !values.contains(&value.as_str())
        {
            self.add(
                value,
                path,
                &format!(
                    "Member must satisfy enum value set: [{}]",
                    values.join(", ")
                ),
            );
        }
    }

    /// A request object's `ReturnValuesOnConditionCheckFailure`.
    fn on_failure(&mut self, request: &Value, path: &str) {
        self.one_of(
            request.get("ReturnValuesOnConditionCheckFailure"),
            path,
            ON_FAILURE_VALUES,
        );
    }

    /// The request's `ReturnConsumedCapacity`.
    fn consumed_capacity(&mut self, body: &Value) {
        self.one_of(
            body.get("ReturnConsumedCapacity"),
            "returnConsumedCapacity",
            CONSUMED_CAPACITY_VALUES,
        );
    }

    /// A request object's `Statement`: present, and 1 to 8192 code points.
    fn statement(&mut self, request: &Value, path: &str) {
        let statement = match request.get("Statement") {
            None | Some(Value::Null) => {
                self.null(path);
                return;
            }
            Some(Value::String(statement)) => statement,
            // A statement of another type is left to the executors.
            Some(_) => return,
        };
        let constraint = if statement.is_empty() {
            Some(AT_LEAST_ONE.to_owned())
        } else {
            statement_length_refusal(statement).map(|refusal| constraint_of(&refusal))
        };
        if let Some(constraint) = constraint {
            self.add(statement, path, &constraint);
        }
    }

    /// The length of a request object's `Parameters` list.
    fn parameters(&mut self, request: &Value, path: &str) {
        if let Some(Value::Array(values)) = request.get("Parameters")
            && values.is_empty()
        {
            self.add("[]", path, AT_LEAST_ONE);
        }
    }

    /// The count of a statement list, then each of its statements. A list
    /// past `max` is echoed with `member` writing each of its statements, at
    /// most [`echo::MAX_ECHO_BYTES`] of it.
    fn statements(
        &mut self,
        list: Option<&Value>,
        path: &str,
        max: usize,
        member: fn(&mut Echo, &Value),
    ) {
        let members = match list {
            None | Some(Value::Null) => {
                self.null(path);
                return;
            }
            Some(Value::Array(members)) => members,
            // A list of another type is left to the executors.
            Some(_) => return,
        };
        if members.is_empty() {
            self.add("[]", path, AT_LEAST_ONE);
        } else if members.len() > max {
            self.add(
                &echo_list(list, member),
                path,
                &format!("Member must have length less than or equal to {max}"),
            );
        }
        for (i, member) in members.iter().enumerate() {
            let at = format!("{path}.{}.member", i + 1);
            self.on_failure(member, &format!("{at}.returnValuesOnConditionCheckFailure"));
            self.statement(member, &format!("{at}.statement"));
            self.parameters(member, &format!("{at}.parameters"));
        }
    }

    /// The refusal, listing the first [`MAX_PARTS`] failed constraints.
    fn into_result(mut self) -> Result<(), DynamoDbError> {
        self.0.truncate(MAX_PARTS);
        let message = match self.0.as_slice() {
            [] => return Ok(()),
            [part] => format!("1 validation error detected: {part}"),
            parts => format!(
                "{} validation errors detected: {}",
                parts.len(),
                parts.join("; ")
            ),
        };
        Err(DynamoDbError::ValidationException(message))
    }
}

/// The constraint named at the end of a single-error refusal ("... failed to
/// satisfy constraint: <constraint>"), so the statement length rule keeps the
/// parser's limit.
fn constraint_of(refusal: &str) -> String {
    refusal
        .rsplit_once(" failed to satisfy constraint: ")
        .map_or_else(
            || refusal.to_owned(),
            |(_, constraint)| constraint.to_owned(),
        )
}

#[cfg(test)]
mod tests;
