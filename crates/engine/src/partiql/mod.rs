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
//!
//! The server does not authorize these operations before dispatch: each
//! statement is authorized here, through the request's
//! [`ItemAuthorizer`](crate::ItemAuthorizer), which is not specific to
//! PartiQL (see `authorize`).

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::partiql::operand_types::check_ordering_operands;
use extenddb_core::partiql::params::{
    check_parameter_types, check_parameter_values, modelled_value,
};
use extenddb_core::partiql::parser::{MAX_STATEMENT_LENGTH, parse_for_execute_statement};
use extenddb_core::partiql::values::{check_condition_values, check_written_values};
use extenddb_core::partiql::{Statement, table_name_refusal};
use extenddb_core::types::AttributeValue;

use crate::{DispatchResult, OperationContext};

use authorize::{Gate, Surface};

mod authorize;
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
#[cfg(test)]
mod test_context;
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
        check_request_parameter_types([body.get("Parameters")])?;
        request_checks::check_execute_statement(&body)?;
        check_parameters(body.get("Parameters")).map_err(DynamoDbError::ValidationException)?;
        let req = SingleRequest::from_body(&body)?;
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
            _ => check_statement(&statement, &req.parameters, &req.statement),
        }
        .map_err(DynamoDbError::ValidationException)?;
        // The statement is authorized against one read of its table, at its
        // place among the checks (see `authorize::gate`).
        let (ctx, key_info) =
            snapshot::pinned_context(ctx, common::statement_table(&statement)).await?;
        let mut gate = Gate::new(
            &ctx,
            &statement,
            key_info.as_ref(),
            &req,
            Surface::Statement,
        );
        let result = match &statement {
            Statement::Select(s) => select::run(&ctx, s, &req, &mut gate).await,
            other => write::run(&ctx, other, &req, &mut gate).await,
        };
        gate.settle(result).await?
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
    /// The text of each parameter that is a number, as the request sends
    /// it. `parameters` hold numbers normalized (`10.0` is `10`), and
    /// `dynamodb:LeadingKeys` keeps the text's scale (see `authorize`).
    pub parameter_numbers: Vec<Option<String>>,
    pub consistent_read: bool,
    pub limit: Option<i64>,
    pub next_token: Option<String>,
    pub return_consumed_capacity: Option<String>,
    pub return_values_on_condition_check_failure: Option<String>,
}

impl SingleRequest {
    /// The request of one statement, its parameters read. Its parameters
    /// pass [`check_request_parameter_types`] and [`check_parameters`]
    /// first, so they read: a failure here is a `Parameters` member that is
    /// not a list, which fails the request.
    pub(super) fn from_body(body: &Value) -> Result<Self, DynamoDbError> {
        let req = Self::without_parameters(body)?;
        Ok(Self {
            parameters: parse_parameters(body.get("Parameters"))?,
            ..req
        })
    }

    /// The request of one statement whose parameters are not read, for a
    /// statement refused before its parameters are.
    pub(super) fn without_parameters(body: &Value) -> Result<Self, DynamoDbError> {
        let statement = body
            .get("Statement")
            .and_then(Value::as_str)
            .ok_or_else(|| DynamoDbError::ValidationException("Statement is required".to_owned()))?
            .to_owned();
        let parameters = Vec::new();
        let parameter_numbers = body
            .get("Parameters")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .map(|value| value.get("N").and_then(Value::as_str).map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            statement,
            parameters,
            parameter_numbers,
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

/// Deserialize a PartiQL `Parameters` array (wire-form AttributeValues), each
/// value as the attribute value model reads it, without the members it
/// ignores (see `params::modelled_value`). [`check_request_parameter_types`]
/// and [`check_parameters`] have already refused the values the service
/// refuses, so a value the attribute value deserializer refuses here gets the
/// error a classic request gets for it (see `crate::deserialize_error`).
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
            serde_json::from_value::<AttributeValue>(modelled_value(v))
                .map_err(crate::deserialize_error)
        })
        .collect()
}

/// Check the JSON types of the parameters of a request, each item of
/// `lists` being one statement's `Parameters` (see
/// `params::check_parameter_types`). The service reads the JSON of the whole
/// request first: a value of the wrong JSON type in any statement fails the
/// request with a `SerializationException`, ahead of the checks of the
/// request and of every statement, and in `BatchExecuteStatement` no
/// statement runs (measured 2026-11-06). So this runs before anything else
/// in the request is checked.
///
/// # Errors
///
/// The `SerializationException`, worded as the attribute value
/// deserializer's refusals are (see `crate::deserialize_error`).
pub(crate) fn check_request_parameter_types<'a>(
    lists: impl IntoIterator<Item = Option<&'a Value>>,
) -> Result<(), DynamoDbError> {
    for list in lists {
        if let Some(Value::Array(parameters)) = list {
            check_parameter_types(parameters).map_err(|detail| {
                DynamoDbError::SerializationException(format!(
                    "Start of structure or map found where not expected: {detail}"
                ))
            })?;
        }
    }
    Ok(())
}

/// The `Parameters` of each member of a request's statement list (the
/// `Statements` of a batch or the `TransactStatements` of a transaction).
pub(crate) fn member_parameters(list: Option<&Value>) -> impl Iterator<Item = Option<&Value>> {
    list.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|member| member.get("Parameters"))
}

/// Check a statement's `Parameters` (as the request carries them): nesting
/// depth, number conversion, and the values the service refuses for their
/// content, such as an empty set (see `params::check_parameter_values`). The
/// JSON types of every parameter of the request are checked first (see
/// [`check_request_parameter_types`]). The service answers this ahead of the
/// target's key checks, and in `ExecuteTransaction` without a
/// statement-position prefix (measured 2026-11-02 and 2026-11-06), so it runs
/// before the statement is parsed.
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

/// The checks a parsed statement gets before its target is resolved, in
/// this order: the table name rule, a projected call (`COUNT(*)`), the
/// literals of the condition (number literals against the item number
/// rules, empty bags, number sets with two equal members), the operand types
/// of ordering comparisons, the values an INSERT or an UPDATE writes (the
/// same literal checks, and the calls of a SET value), then update paths that
/// overlap or conflict. `source` is the statement's text. The first three
/// come in that order on all three operations (measured 2026-11-02), except
/// that `ExecuteStatement` reports a projected call once the target is
/// resolved; the condition's literals come ahead of the operand types and
/// the written values after them, and the written values ahead of
/// overlapping paths (measured 2026-11-06, see `values`).
///
/// # Errors
///
/// Returns the service's message for the first failure.
pub(crate) fn check_statement(
    stmt: &Statement,
    params: &[AttributeValue],
    source: &str,
) -> Result<(), String> {
    check_table_name(stmt)?;
    if let Statement::Select(select) = stmt
        && let Some(message) = &select.projection_error
    {
        return Err(message.clone());
    }
    check_condition_values(stmt)?;
    check_ordering_operands(stmt, params)?;
    check_written_values(stmt, source)?;
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
    fn a_parameter_of_the_wrong_json_type_in_any_statement_fails_the_request() {
        // (measured 2026-11-06: a content problem in an earlier statement,
        // or in the same one, does not come first)
        let body = serde_json::json!({ "Statements": [
            { "Statement": "x", "Parameters": [{"SS": []}] },
            { "Statement": "y" },
            { "Statement": "z", "Parameters": [{"S": "a"}, {"SS": [1]}] },
        ]});
        match check_request_parameter_types(member_parameters(body.get("Statements"))) {
            Err(DynamoDbError::SerializationException(message)) => assert_eq!(
                message,
                "Start of structure or map found where not expected: SS member must be a string"
            ),
            other => panic!("expected a SerializationException, got {other:?}"),
        }
        let body = serde_json::json!({ "Parameters": [{"SS": []}, {"X": 1}, {"S": null}] });
        assert!(check_request_parameter_types([body.get("Parameters")]).is_ok());
        assert!(check_request_parameter_types(member_parameters(None)).is_ok());
    }

    #[test]
    fn a_parameter_is_read_without_the_members_the_model_ignores() {
        let body = serde_json::json!({
            "Statement": "x",
            "Parameters": [{"S": "a", "X": "1"}, {"N": "10", "B": null}],
        });
        let req = SingleRequest::from_body(&body).unwrap();
        assert_eq!(
            req.parameters,
            vec![
                AttributeValue::S("a".to_owned()),
                AttributeValue::N("10".to_owned())
            ]
        );
        assert_eq!(req.parameter_numbers, vec![None, Some("10".to_owned())]);
    }

    /// The statement checks of `sql` with the parameters `params`.
    fn checked(sql: &str, params: &[AttributeValue]) -> Result<(), String> {
        check_statement(&parse(sql).unwrap(), params, sql)
    }

    const OPERAND_TYPE: &str = "Incorrect operand type for operator or function; operator or function: <, operand type: \
         BOOL";

    #[test]
    fn statement_checks_run_the_table_name_rule_first() {
        // A number in a condition is refused by the parser; one in a SET
        // value is left to these checks.
        for sql in [
            "UPDATE \"t\" SET q = 1e126 WHERE pk = 'a'",
            "UPDATE \"t\" SET q = size(m) WHERE pk = 'a'",
        ] {
            assert_eq!(
                checked(sql, &[]),
                Err(
                    "Table name should be within the length [3, 255] and only contain 'a-z', \
                     'A-Z', '0-9', '-', '_', '.'."
                        .to_owned()
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn a_projected_call_is_checked_after_the_name_rule_and_before_literals() {
        const PROJECTED_CALL: &str = "Unexpected path component at 1:8:5";
        assert_eq!(
            checked("SELECT COUNT(*) FROM \"t\" WHERE pk = 'a'", &[]),
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
            assert_eq!(checked(sql, &[]), Err(PROJECTED_CALL.to_owned()), "{sql}");
        }
    }

    #[test]
    fn statement_checks_run_overlapping_update_paths_last() {
        assert_eq!(
            checked("UPDATE \"tbl\" SET q = 1, q = 2 WHERE pk = 'a'", &[]),
            Err(
                "Two document paths overlap with each other; must remove or rewrite one of \
                 these paths; path one: [q], path two: [q]"
                    .to_owned()
            )
        );
        assert!(
            checked("UPDATE \"tbl\" SET q = 1e126, q = 2 WHERE pk = 'a'", &[])
                .unwrap_err()
                .starts_with("Number overflow.")
        );
        // (measured 2026-11-06, in both orders)
        for sql in [
            "UPDATE \"tbl\" SET a = size(m), a = 1 WHERE pk = 'a'",
            "UPDATE \"tbl\" SET a = 1, a = size(m) WHERE pk = 'a'",
        ] {
            assert_eq!(
                checked(sql, &[]),
                Err(
                    "The function is not allowed in an update expression; function: size"
                        .to_owned()
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn the_written_values_are_checked_after_the_operand_types() {
        // (measured 2026-11-06, with a literal and with a parameter)
        let params = [AttributeValue::Bool(true)];
        for (sql, params) in [
            (
                "UPDATE \"tbl\" SET q = 1e126 WHERE pk = 'a' AND val < ?",
                &params[..],
            ),
            (
                "UPDATE \"tbl\" SET q = 1e126 WHERE pk = 'a' AND val < true",
                &[],
            ),
            (
                "UPDATE \"tbl\" SET q = size(m) WHERE pk = 'a' AND val < ?",
                &params[..],
            ),
            (
                "UPDATE \"tbl\" SET q = upper(m) WHERE pk = 'a' AND val < true",
                &[],
            ),
            (
                "UPDATE \"tbl\" SET q = <<>> WHERE pk = 'a' AND val < true",
                &[],
            ),
            (
                "SELECT * FROM \"tbl\" WHERE pk = 1 AND val < ?",
                &params[..],
            ),
        ] {
            assert_eq!(checked(sql, params), Err(OPERAND_TYPE.to_owned()), "{sql}");
        }
    }

    #[test]
    fn the_condition_literals_are_checked_before_the_operand_types() {
        // (measured 2026-11-06, in both orders)
        for sql in [
            "UPDATE \"tbl\" SET a = 1 WHERE pk = 'a' AND v = <<>> AND val < true",
            "UPDATE \"tbl\" SET a = 1 WHERE pk = 'a' AND val < true AND v = <<>>",
            "SELECT * FROM \"tbl\" WHERE pk = 'a' AND val < true AND v = <<>>",
            "UPDATE \"tbl\" SET a = size(m) WHERE pk = 'a' AND v = <<>>",
        ] {
            assert_eq!(
                checked(sql, &[]),
                Err("Empty bags are not supported".to_owned()),
                "{sql}"
            );
        }
    }
}
