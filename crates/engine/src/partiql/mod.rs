// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL surface: `ExecuteStatement`, `BatchExecuteStatement`,
//! `ExecuteTransaction`.
//!
//! This module is compiled only with the `partiql` feature and is the whole
//! feature on the engine side. It is a pure translation layer: it parses a
//! statement (via `extenddb_core::partiql`), then drives the operations the
//! engine already has (get/query/scan/put/update/delete and their batch and
//! transact forms). It adds no storage concept.
//!
//! Confinement: the [`PartiqlExecutor`] trait is implemented once, for
//! [`OperationContext`], and nothing else implements or requires it. Removing
//! PartiQL is deleting this module and `core::partiql`, dropping the three
//! `#[cfg(feature = "partiql")]` dispatch arms and `is_known_operation`
//! entries, removing the `DuplicateItemException` variant that only this path
//! uses from `core`'s error enum, and dropping the feature. With the feature
//! off the three operations are unknown and answered with
//! `UnknownOperationException`, exactly as before.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::partiql::number::check_literal_numbers;
use extenddb_core::partiql::operand_types::check_ordering_operands;
use extenddb_core::partiql::params::check_parameter_values;
use extenddb_core::partiql::parser::{MAX_STATEMENT_LENGTH, parse_for_execute_statement};
use extenddb_core::partiql::{Statement, table_name_refusal};
use extenddb_core::types::AttributeValue;

use crate::{DispatchResult, OperationContext};

mod base_join;
mod batch;
mod common;
mod expr_bridge;
mod index_read;
mod item_depth;
mod next_token;
mod request_checks;
mod returning;
mod select;
mod snapshot;
mod transaction;
mod write;

/// The confined PartiQL surface, implemented once for [`OperationContext`].
pub trait PartiqlExecutor {
    /// `ExecuteStatement`: one statement, response under `Items`.
    fn execute_statement(
        &self,
        body: Value,
    ) -> impl Future<Output = Result<DispatchResult, DynamoDbError>> + Send;

    /// `BatchExecuteStatement`: all-read or all-write, response under `Responses`.
    fn batch_execute_statement(
        &self,
        body: Value,
    ) -> impl Future<Output = Result<DispatchResult, DynamoDbError>> + Send;

    /// `ExecuteTransaction`: all-or-nothing, response under `Responses`.
    fn execute_transaction(
        &self,
        body: Value,
    ) -> impl Future<Output = Result<DispatchResult, DynamoDbError>> + Send;
}

impl PartiqlExecutor for OperationContext {
    async fn execute_statement(&self, body: Value) -> Result<DispatchResult, DynamoDbError> {
        let ctx = &translation_context(self);
        request_checks::check_execute_statement(&body)?;
        let req = SingleRequest::from_body(&body)?;
        check_parameters(body.get("Parameters")).map_err(DynamoDbError::ValidationException)?;
        // An EXISTS statement is refused by the parser, in its place among
        // the refusals of a statement that parses (measured 2026-11-02).
        let statement = parse_for_execute_statement(&req.statement, req.parameters.len())
            .map_err(|e| DynamoDbError::ValidationException(e.message))?;
        match &statement {
            // A projected call (`COUNT(*)`) is reported once the target is
            // resolved (see `select::run`), so only the table name rule comes
            // ahead of it (measured 2026-11-02).
            Statement::Select(select) if select.projection_error.is_some() => {
                check_table_name(&statement)
            }
            _ => check_statement(&statement, &req.parameters),
        }
        .map_err(DynamoDbError::ValidationException)?;
        match statement {
            Statement::Select(s) => select::run(ctx, s, &req).await,
            other => write::run(ctx, other, &req).await,
        }
    }

    async fn batch_execute_statement(&self, body: Value) -> Result<DispatchResult, DynamoDbError> {
        batch::run(&translation_context(self), &body).await
    }

    async fn execute_transaction(&self, body: Value) -> Result<DispatchResult, DynamoDbError> {
        transaction::run(&translation_context(self), &body).await
    }
}

/// The size, in bytes and in tokens, an expression written for a statement
/// may reach: 16 bytes for each character a statement may hold.
///
/// The text written for a statement is at most 5 bytes for each of its
/// characters, plus a fixed part under 64 bytes. A name or a value becomes a
/// placeholder of at most 6 bytes (`#n` or `:v` and an index of at most 4
/// digits, since a statement of 8192 characters holds fewer than 4096 names
/// or values); each is at least 1 character, and two of them have at least
/// one character between them; an operator or a separator gains at most a
/// space on each side. The densest form, `a=1`, becomes `#nNNNN = :vNNNN`,
/// 15 bytes for 3 characters. The fixed part is what a translation adds to
/// what the statement says, such as `attribute_exists(#n0) AND ` on a
/// write's key.
///
/// The filter of a read of an index by several disjuncts that reaches back
/// to the base table writes each key condition compared with false twice,
/// as the condition and its negation (see `plan::keyed_condition`), so its
/// text is at most twice as long: 10 bytes for each character and twice the
/// fixed part, 82,048 bytes. That is within 16 bytes for each of the 8192
/// characters, 131,072, and since a token is at least one byte, the same
/// number bounds its tokens.
const TRANSLATED_EXPRESSION_LIMIT: usize = 16 * MAX_STATEMENT_LENGTH;

/// The limits the requests a statement is translated into run with: `limits`
/// with the size and token limits of an expression raised to
/// [`TRANSLATED_EXPRESSION_LIMIT`]. The classic limits (4096 bytes and 4096
/// tokens) apply to the expressions a caller writes. The text written for a
/// statement is longer than the statement wherever a name or a value becomes
/// a placeholder, and the service runs a statement within its own limits
/// (8192 characters, 300 operators in its condition) whose text would be past
/// the classic ones (measured 2026-11-02: a condition of 150 `begins_with`
/// calls ANDed, 4275 bytes as classic text).
fn translation_limits(limits: &LimitsConfig) -> LimitsConfig {
    LimitsConfig {
        max_expression_length_bytes: TRANSLATED_EXPRESSION_LIMIT,
        max_expression_tokens: TRANSLATED_EXPRESSION_LIMIT,
        ..limits.clone()
    }
}

/// The context the requests a statement is translated into run in: `ctx`
/// with the limits of [`translation_limits`].
fn translation_context(ctx: &OperationContext) -> OperationContext {
    OperationContext {
        limits: Arc::new(translation_limits(&ctx.limits)),
        ..snapshot::context_with_key_info(ctx, ctx.authorized_key_info.clone())
    }
}

/// The parsed fields of a single-statement request, shared by the SELECT and
/// write executors.
pub(crate) struct SingleRequest {
    pub statement: String,
    pub parameters: Vec<AttributeValue>,
    pub consistent_read: bool,
    pub limit: Option<i64>,
    pub next_token: Option<String>,
    pub return_consumed_capacity: Option<String>,
    pub return_values_on_condition_check_failure: Option<String>,
}

impl SingleRequest {
    pub(super) fn from_body(body: &Value) -> Result<Self, DynamoDbError> {
        let statement = body
            .get("Statement")
            .and_then(Value::as_str)
            .ok_or_else(|| DynamoDbError::ValidationException("Statement is required".to_owned()))?
            .to_owned();
        let parameters = parse_parameters(body.get("Parameters"))?;
        Ok(Self {
            statement,
            parameters,
            consistent_read: body
                .get("ConsistentRead")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            limit: body.get("Limit").and_then(Value::as_i64),
            next_token: body
                .get("NextToken")
                .and_then(Value::as_str)
                .map(str::to_owned),
            return_consumed_capacity: body
                .get("ReturnConsumedCapacity")
                .and_then(Value::as_str)
                .map(str::to_owned),
            return_values_on_condition_check_failure: body
                .get("ReturnValuesOnConditionCheckFailure")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }
}

/// Deserialize a PartiQL `Parameters` array (wire-form AttributeValues).
pub(crate) fn parse_parameters(
    value: Option<&Value>,
) -> Result<Vec<AttributeValue>, DynamoDbError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let arr = value.as_array().ok_or_else(|| {
        DynamoDbError::ValidationException("Parameters must be a list".to_owned())
    })?;
    arr.iter()
        .map(|v| {
            serde_json::from_value::<AttributeValue>(v.clone())
                .map_err(|e| DynamoDbError::ValidationException(format!("invalid parameter: {e}")))
        })
        .collect()
}

/// Check a statement's `Parameters` (as the request carries them) for nesting
/// depth and number conversion. The service answers this ahead of the target's
/// key checks, and in `ExecuteTransaction` without a statement-position prefix
/// (measured 2026-11-02), so it runs before the statement is parsed.
///
/// The walk follows the parsed body: parameters and list elements in request
/// order, the members of a map in the order the parsed request holds them,
/// which is the request's when the build includes the MongoDB storage crate
/// (any build of the whole workspace) and name order otherwise. The service
/// walks map members in request order (measured 2026-11-02), so in name order,
/// when two members of one map both have a problem, the one reported here can
/// differ.
///
/// # Errors
///
/// Returns the service's message for the first problem the walk reaches.
pub(crate) fn check_parameters(parameters: Option<&Value>) -> Result<(), String> {
    let Some(Value::Array(items)) = parameters else {
        return Ok(());
    };
    check_parameter_values(items)
}

/// The checks a parsed statement gets before its target is resolved: the
/// table name rule, a projected call (`COUNT(*)`), number literals against
/// the item number rules, the operand types of ordering comparisons, then
/// update paths that overlap or conflict. The first four come in that order
/// on all three operations (measured 2026-11-02), except that
/// `ExecuteStatement` reports a projected call once the target is resolved.
/// Where the service checks overlapping or conflicting paths relative to the
/// others was not measured.
///
/// # Errors
///
/// Returns the service's message for the first failure.
pub(crate) fn check_statement(stmt: &Statement, params: &[AttributeValue]) -> Result<(), String> {
    check_table_name(stmt)?;
    if let Statement::Select(select) = stmt
        && let Some(message) = &select.projection_error
    {
        return Err(message.clone());
    }
    check_literal_numbers(stmt).map_err(|e| e.message)?;
    check_ordering_operands(stmt, params)?;
    match stmt {
        Statement::Update(update) => write::check_overlapping_paths(update),
        _ => Ok(()),
    }
}

/// The table name rule, the first check a parsed statement gets.
///
/// # Errors
///
/// Returns the service's refusal of the name.
pub(crate) fn check_table_name(stmt: &Statement) -> Result<(), String> {
    match table_name_refusal(stmt) {
        Some(refusal) => Err(refusal.to_owned()),
        None => Ok(()),
    }
}

/// Drop the expression-kind prefix (`Invalid FilterExpression: ` and the like)
/// from an error raised by a classic handler a statement was translated into.
/// The prefix names an expression the caller never wrote. Measured 2026-11-02:
/// the operand-type messages of a read's filter and of a write's condition
/// ("Incorrect operand type for operator or function; ...") come without it.
/// The messages the update evaluation raises ("The document path provided in
/// the update expression is invalid for update", "An operand in the update
/// expression has an incorrect data type") carry no prefix on either side, and
/// no other prefixed message was measured.
pub(crate) fn bare_expression_error(err: DynamoDbError) -> DynamoDbError {
    const PREFIXES: [&str; 5] = [
        "Invalid FilterExpression: ",
        "Invalid ConditionExpression: ",
        "Invalid KeyConditionExpression: ",
        "Invalid UpdateExpression: ",
        "Invalid ProjectionExpression: ",
    ];
    match err {
        DynamoDbError::ValidationException(message) => {
            let bare = PREFIXES
                .iter()
                .find_map(|prefix| message.strip_prefix(prefix))
                .map(str::to_owned);
            DynamoDbError::ValidationException(bare.unwrap_or(message))
        }
        other => other,
    }
}

/// Bind a positional parameter, returning the service's count-mismatch message
/// when the index is out of range. Supplying more parameters than placeholders
/// is not an error (the extras are ignored); supplying fewer is.
pub(crate) fn bind_parameter(
    params: &[AttributeValue],
    index: usize,
) -> Result<AttributeValue, DynamoDbError> {
    params.get(index).cloned().ok_or_else(|| {
        DynamoDbError::ValidationException(
            "Number of parameters in request and statement don't match.".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::parse;

    fn message(err: DynamoDbError) -> String {
        match err {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn a_translated_expression_may_reach_sixteen_bytes_a_statement_character() {
        let classic = LimitsConfig::default();
        let translated = translation_limits(&classic);
        assert_eq!(translated.max_expression_length_bytes, 131_072);
        assert_eq!(translated.max_expression_tokens, 131_072);
        // The other limits are the caller's.
        assert_eq!(
            translated.max_expression_depth,
            classic.max_expression_depth
        );
        assert_eq!(
            translated.max_attribute_name_bytes,
            classic.max_attribute_name_bytes
        );
    }

    #[test]
    fn the_expression_prefix_is_dropped() {
        let sentence = "Incorrect operand type for operator or function; operator or function: <, \
                        operand type: BOOL";
        for kind in [
            "FilterExpression",
            "ConditionExpression",
            "KeyConditionExpression",
            "UpdateExpression",
            "ProjectionExpression",
        ] {
            let err = DynamoDbError::ValidationException(format!("Invalid {kind}: {sentence}"));
            assert_eq!(message(bare_expression_error(err)), sentence, "{kind}");
        }
    }

    #[test]
    fn other_errors_pass_through_unchanged() {
        let plain = DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Missing the key pk in the item".to_owned(),
        );
        assert_eq!(
            message(bare_expression_error(plain)),
            "One or more parameter values were invalid: Missing the key pk in the item"
        );
        let other = bare_expression_error(DynamoDbError::ResourceNotFoundException(
            "Requested resource not found".to_owned(),
        ));
        assert!(matches!(other, DynamoDbError::ResourceNotFoundException(_)));
    }

    #[test]
    fn parameters_are_checked_as_the_request_carries_them() {
        assert_eq!(check_parameters(None), Ok(()));
        let body: Value = serde_json::json!({
            "Parameters": [{"S": "a"}, {"L": [{"N": "1"}, {"N": "1e99999999999"}]}]
        });
        assert_eq!(
            check_parameters(body.get("Parameters")),
            Err("The parameter cannot be converted to a numeric value: 1e99999999999".to_owned())
        );
        let body: Value = serde_json::json!({ "Parameters": [{"N": ""}] });
        assert_eq!(
            check_parameters(body.get("Parameters")),
            Err("The parameter cannot be converted to a numeric value".to_owned())
        );
    }

    #[test]
    fn statement_checks_run_the_table_name_rule_first() {
        // A number in a condition is refused by the parser; one in a SET
        // value is left to these checks.
        let stmt = parse("UPDATE \"t\" SET q = 1e126 WHERE pk = 'a'").unwrap();
        assert_eq!(
            check_statement(&stmt, &[]),
            Err(
                "Table name should be within the length [3, 255] and only contain 'a-z', \
                 'A-Z', '0-9', '-', '_', '.'."
                    .to_owned()
            )
        );
    }

    #[test]
    fn a_projected_call_is_checked_after_the_name_rule_and_before_literals() {
        const PROJECTED_CALL: &str = "Unexpected path component at 1:8:5";
        let stmt = parse("SELECT COUNT(*) FROM \"t\" WHERE pk = 'a'").unwrap();
        assert_eq!(
            check_statement(&stmt, &[]),
            Err(
                "Table name should be within the length [3, 255] and only contain 'a-z', \
                 'A-Z', '0-9', '-', '_', '.'."
                    .to_owned()
            )
        );
        for sql in [
            "SELECT COUNT(*) FROM \"tbl\" WHERE pk = 1e126",
            "SELECT COUNT(*) FROM \"tbl\" WHERE pk = 'a' AND v < true",
        ] {
            let stmt = parse(sql).unwrap();
            assert_eq!(
                check_statement(&stmt, &[]),
                Err(PROJECTED_CALL.to_owned()),
                "{sql}"
            );
        }
    }

    #[test]
    fn statement_checks_run_overlapping_update_paths_last() {
        let stmt = parse("UPDATE \"tbl\" SET q = 1, q = 2 WHERE pk = 'a'").unwrap();
        assert_eq!(
            check_statement(&stmt, &[]),
            Err(
                "Two document paths overlap with each other; must remove or rewrite one of \
                 these paths; path one: [q], path two: [q]"
                    .to_owned()
            )
        );
        let stmt = parse("UPDATE \"tbl\" SET q = 1e126, q = 2 WHERE pk = 'a'").unwrap();
        assert!(
            check_statement(&stmt, &[])
                .unwrap_err()
                .starts_with("Number overflow.")
        );
    }

    #[test]
    fn statement_checks_run_literals_before_operand_types() {
        let stmt = parse("UPDATE \"tbl\" SET q = 1e126 WHERE pk = 'a' AND val < ?").unwrap();
        let params = [AttributeValue::Bool(true)];
        assert_eq!(
            check_statement(&stmt, &params),
            Err(
                "Number overflow. Attempting to store a number with magnitude larger than \
                 supported range under root"
                    .to_owned()
            )
        );
        let stmt = parse("SELECT * FROM \"tbl\" WHERE pk = 1 AND val < ?").unwrap();
        assert_eq!(
            check_statement(&stmt, &params),
            Err(
                "Incorrect operand type for operator or function; operator or function: <, \
                 operand type: BOOL"
                    .to_owned()
            )
        );
    }
}
