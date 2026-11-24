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
    use extenddb_core::partiql::parser::{parse_for_request, parse_for_transaction};
    use extenddb_core::partiql::{ParseError, parse};

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

    /// The parser of one operation's statements.
    type Parse = fn(&str, usize) -> Result<Statement, ParseError>;

    /// What the operation that parses with `parse` answers `sql` ahead of
    /// its table lookup: the parser's refusal or the statement checks', or
    /// an empty string when the statement reaches the table.
    fn answer(parse: Parse, sql: &str) -> String {
        parse(sql, 0)
            .map_err(|e| e.message)
            .and_then(|statement| check_statement(&statement, &[], sql))
            .err()
            .unwrap_or_default()
    }

    #[test]
    fn calls_and_not_in_set_values_and_conditions_get_the_services_refusals() {
        // Measured 2026-11-23 in us-east-1 on this table, which does not
        // exist, each statement twice: with ExecuteStatement (`es`), and the
        // statements of their own also with BatchExecuteStatement (`batch`,
        // the Error of the response) and ExecuteTransaction (`tx`, after the
        // prefix "Validation failed in TransactStatements[0]: ", which the
        // transaction adds). A SET value starts at column 39, a condition
        // after the key at column 57 and an INSERT value after the key at
        // column 59. The service refuses a call or a NOT once the statement
        // parses, the first in written order. A quoted name is an aggregate
        // only when written in lower case, is never `nullif`, `coalesce` or
        // `cast`, and is any other function in any case. `SET a = size(m), x
        // = NOT a`, which the service refuses for `size`, is left out: the
        // parser's refusal of the NOT comes ahead of the checks made after
        // the parse (#886).
        let es: Parse = parse_for_execute_statement;
        let batch: Parse = parse_for_request;
        let tx: Parse = parse_for_transaction;
        const TABLE: &str = "\"ets-parityat-missing\"";
        let update = |value: &str| format!("UPDATE {TABLE} SET {value} WHERE pk = 'q'");
        let select =
            |condition: &str| format!("SELECT * FROM {TABLE} WHERE pk = 'q' AND {condition}");
        let insert = |value: &str| format!("INSERT INTO {TABLE} VALUE {{'pk': 'q', 'a': {value}}}");
        let wf =
            |reason: &str| format!("Statement wasn't well formed, can't be processed: {reason}");
        let unary = || wf("Aggregate functions are always unary");
        let no_right_side = || wf("Missing right-hand side expression of infix operator");
        let token =
            |token: &str, at: &str| format!("Unsupported token in expression: {token} at {at}");
        let unrecognized = |name: &str, at: &str| format!("Unrecognized function: {name} at {at}");
        let not_in_update = |name: &str| {
            format!("The function is not allowed in an update expression; function: {name}")
        };
        let not_in_condition = |name: &str| {
            format!("The function is not allowed in a condition expression; function: {name}")
        };
        let update_not = || "Unsupported operator in Update Expression. Operator: not".to_owned();
        let data_type =
            |name: &str, at: &str| format!("Unsupported data type: {name} under key root at {at}");
        let operation = |name: &str| format!("Unsupported operation: {name}");
        let exists_only =
            || "EXISTS can only be used in ExecuteTransaction write requests.".to_owned();
        let in_transaction = || "Unsupported operation in transaction.".to_owned();
        let table_lookup = String::new;
        #[rustfmt::skip]
        let rows = [
            // Calls in a SET value.
            ("F1", es, update("a = count()"), unary()),
            ("A1", es, update("a = COUNT()"), unary()),
            ("A2", es, update("a = avg()"), unary()),
            ("F5", es, update("a = count(m)"), token("count", "1:39:5")),
            ("F11", es, update("a = count(*)"), token("count", "1:39:5")),
            ("F12", es, update("a = count(m, 1)"), token("count", "1:39:5")),
            ("A3", es, update("a = count(DISTINCT m)"), token("count", "1:39:5")),
            ("F10", es, update("a = sum(m)"), token("sum", "1:39:3")),
            ("F16", es, update("a = \"count\"(m)"), token("count", "1:39:7")),
            ("F2", es, update("a = nullif(m)"), wf("Expected COMMA")),
            ("F15", es, update("a = nullif()"), wf("Unexpected term")),
            ("F7", es, update("a = nullif(m, 1)"), token("UNKNOWN", "1:39:6")),
            ("F14", es, update("a = coalesce()"), wf("Unexpected term")),
            ("F6", es, update("a = coalesce(m, 1)"), token("UNKNOWN", "1:47:1")),
            ("Q1", es, update("a = \"nullif\"(m, 1)"), unrecognized("nullif", "1:39:8")),
            ("Q2", es, update("a = \"coalesce\"(m, 1)"), unrecognized("coalesce", "1:39:10")),
            ("F3", es, update("a = cast(m AS INT)"), token("cast", "1:39:4")),
            ("C6", es, update("a = CAST(m AS VARCHAR(5))"), token("cast", "1:39:4")),
            ("F13", es, update("a = cast(m)"), wf("Expected AS")),
            ("C4", es, update("a = cast(m INT)"), wf("Expected AS")),
            ("C1", es, update("a = cast(m AS)"), wf("Expected type name")),
            ("C2", es, update("a = cast(m AS foo)"), wf("Expected type name")),
            ("C3", es, format!("UPDATE {TABLE} SET a = cast(m AS INT WHERE pk = 'q'"), wf("Expected RIGHT_PAREN")),
            ("C7", es, update("a = cast()"), wf("Unexpected term")),
            ("Q7", es, update("a = \"cast\"(m AS INT)"), wf("Expected RIGHT_PAREN")),
            ("F4", es, update("a = \"upper\"(m)"), unrecognized("upper", "1:39:7")),
            ("F8", es, update("a = upper(m)"), unrecognized("upper", "1:39:5")),
            ("F9", es, update("a = size(m)"), not_in_update("size")),
            ("F17", es, update("a = \"size\"(m)"), not_in_update("size")),
            ("F18", es, update("a = attribute_exists(m)"), not_in_update("attribute_exists")),
            ("F19", es, update("a = begins_with(m, 'x')"), not_in_update("begins_with")),
            ("Q5", es, update("a = \"if_not_exists\"(m, 1)"), table_lookup()),
            ("Q6", es, update("a = \"list_append\"(l, l)"), table_lookup()),
            ("S1", es, update("a = \"count\"()"), unary()),
            ("S13", es, update("a = \"avg\"()"), unary()),
            ("S6", es, update("a = \"COUNT\"(m)"), unrecognized("count", "1:39:7")),
            ("S2", es, update("a = count(*, 1)"), wf("Expected RIGHT_PAREN")),
            ("S3", es, update("a = count(ALL m)"), token("count", "1:39:5")),
            ("S4", es, update("a = count(DISTINCT)"), wf("Unexpected term")),
            ("V6", es, update("ss = \"set_add\"(ss, <<'b'>>)"), table_lookup()),
            ("V7", es, update("ss = \"set_delete\"(ss, <<'b'>>)"), table_lookup()),
            ("X1", es, update("a = sum(*)"), wf("sum(*) is not allowed")),
            ("X17", es, update("a = max(*)"), wf("max(*) is not allowed")),
            ("X10", es, update("a = count(DISTINCT m, 1)"), token("count", "1:39:5")),
            ("X28", es, update("a = \"sum\"(m)"), token("sum", "1:39:5")),
            ("X27", es, update("a = \"Sum\"(m)"), unrecognized("sum", "1:39:5")),
            ("X14", es, update("a = \"Count\"(m), b = count(m)"), unrecognized("count", "1:39:7")),
            ("X25", es, update("a = \"UPPER\"(m)"), unrecognized("upper", "1:39:7")),
            ("X37", es, update("a = \"NULLIF\"(m, 1)"), unrecognized("nullif", "1:39:8")),
            ("X38", es, update("a = \"nullif\"(m)"), unrecognized("nullif", "1:39:8")),
            ("X42", es, update("a = \"coalesce\"(m)"), unrecognized("coalesce", "1:39:10")),
            ("X21", es, update("a = \"SIZE\"(m)"), not_in_update("size")),
            ("X3", es, update("a = \"If_Not_Exists\"(m, 1)"), table_lookup()),
            ("X24", es, update("l = \"LIST_APPEND\"(l, [1])"), table_lookup()),
            ("X8", es, update("ss = \"SET_ADD\"(ss, <<'b'>>)"), table_lookup()),
            ("X40", es, update("ss = \"Set_Delete\"(ss, <<'b'>>)"), table_lookup()),
            // Calls in a WHERE condition.
            ("G6", es, select("count() = 1"), unary()),
            ("A4", es, select("max() = 1"), unary()),
            ("G1", es, select("count(m) = 1"), token("count", "1:57:5")),
            ("G11", es, select("count(*) = 1"), token("count", "1:57:5")),
            ("G10", es, select("sum(m) = 1"), token("sum", "1:57:3")),
            ("Q3", es, select("\"count\"(m) = 1"), token("count", "1:57:7")),
            ("G12", es, select("nullif(m) = 1"), wf("Expected COMMA")),
            ("G3", es, select("nullif(m, 1) = 1"), token("UNKNOWN", "1:57:6")),
            ("G2", es, select("coalesce(m, 1) = 1"), token("UNKNOWN", "1:65:1")),
            ("G13", es, select("coalesce(m) = 1"), token("UNKNOWN", "1:65:1")),
            ("G7", es, select("cast(m AS INT) = 1"), token("cast", "1:57:4")),
            ("C5", es, select("cast(m AS) = 1"), wf("Expected type name")),
            ("G4", es, select("if_not_exists(m, 1) = 1"), not_in_condition("if_not_exists")),
            ("Q4", es, select("\"if_not_exists\"(m, 1) = 1"), not_in_condition("if_not_exists")),
            ("G5", es, select("list_append(l, l) = 1"), not_in_condition("list_append")),
            ("G8", es, select("\"upper\"(m) = 1"), unrecognized("upper", "1:57:7")),
            ("G9", es, select("upper(m) = 1"), unrecognized("upper", "1:57:5")),
            ("G14", es, select("set_add(m, <<1>>) = 1"), unrecognized("set_add", "1:57:7")),
            ("G15", es, select("\"size\"(m) = 1"), table_lookup()),
            ("W1", es, select("\"count\"() = 1"), unary()),
            ("W5", es, select("\"COUNT\"(m) = 1"), unrecognized("count", "1:57:7")),
            ("W2", es, select("count(*, 1) = 1"), wf("Expected RIGHT_PAREN")),
            ("W3", es, select("count(ALL m) = 1"), token("count", "1:57:5")),
            ("X18", es, select("count(ALL) = 1"), wf("Unexpected term")),
            ("X22", es, select("\"If_Not_Exists\"(m, 1) = 1"), not_in_condition("if_not_exists")),
            ("X2", es, select("\"SIZE\"(m) = 1"), table_lookup()),
            ("X23", es, select("\"Attribute_Exists\"(m)"), table_lookup()),
            ("X39", es, select("\"Contains\"(m, 'a')"), table_lookup()),
            // NOT in a SET value, refused ahead of what its operand holds.
            ("N1", es, update("x = NOT a"), update_not()),
            ("N2", es, update("x = NOT NOT a"), update_not()),
            ("N3", es, update("x = NOT true"), update_not()),
            ("N4", es, update("x = 1 + NOT a"), update_not()),
            ("N36", es, update("x = NOT -0.0"), update_not()),
            ("N37", es, update("x = NOT {'k': 1, 'k': 2}"), update_not()),
            ("X6", es, update("x = NOT +b"), update_not()),
            ("X12", es, update("x = NOT <<1, 'a'>>"), update_not()),
            ("X20", es, update("a = size(NOT m), b = upper(m)"), update_not()),
            // A call as the statement: an aggregate, `nullif`, `coalesce`
            // and `cast` are named, also in parentheses. A quoted name that
            // is no form, and any of these with a sign before it, are calls
            // like any other: the EXISTS text outside a transaction, and in
            // one an operation a transaction does not run.
            ("B11", es, "\"count\"()".to_owned(), unary()),
            ("B16", es, "avg(*)".to_owned(), wf("avg(*) is not allowed")),
            ("B6", es, "nullif(1, 2)".to_owned(), operation("NullIf")),
            ("B6", batch, "nullif(1, 2)".to_owned(), operation("NullIf")),
            ("B6", tx, "nullif(1, 2)".to_owned(), operation("NullIf")),
            ("B8", es, "coalesce(1)".to_owned(), operation("Coalesce")),
            ("B8", batch, "coalesce(1)".to_owned(), operation("Coalesce")),
            ("B8", tx, "coalesce(1)".to_owned(), operation("Coalesce")),
            ("B10", es, "cast(1 AS INT)".to_owned(), operation("Cast")),
            ("B10", batch, "cast(1 AS INT)".to_owned(), operation("Cast")),
            ("B10", tx, "cast(1 AS INT)".to_owned(), operation("Cast")),
            ("X29", es, "(nullif(1, 2))".to_owned(), operation("NullIf")),
            ("X29", batch, "(nullif(1, 2))".to_owned(), operation("NullIf")),
            ("X29", tx, "(nullif(1, 2))".to_owned(), operation("NullIf")),
            ("X34", es, "(coalesce(1))".to_owned(), operation("Coalesce")),
            ("X4", es, "\"COUNT\"(1)".to_owned(), exists_only()),
            ("X4", batch, "\"COUNT\"(1)".to_owned(), exists_only()),
            ("X4", tx, "\"COUNT\"(1)".to_owned(), in_transaction()),
            ("X47", es, "(\"COUNT\"(1))".to_owned(), exists_only()),
            ("X13", es, "(\"nullif\"(1, 2))".to_owned(), exists_only()),
            ("X13", batch, "(\"nullif\"(1, 2))".to_owned(), exists_only()),
            ("X13", tx, "(\"nullif\"(1, 2))".to_owned(), in_transaction()),
            ("X33", es, "(\"coalesce\"(1))".to_owned(), exists_only()),
            ("X35", es, "(\"cast\"(1))".to_owned(), exists_only()),
            ("X32", es, "-\"nullif\"(1, 2)".to_owned(), exists_only()),
            ("X46", es, "-nullif(1, 2)".to_owned(), exists_only()),
            ("Y1", es, "-count(1)".to_owned(), exists_only()),
            ("Y1", batch, "-count(1)".to_owned(), exists_only()),
            ("Y2", es, "(-count(1))".to_owned(), exists_only()),
            ("Y3", es, "-\"count\"(1)".to_owned(), exists_only()),
            ("Y3", batch, "-\"count\"(1)".to_owned(), exists_only()),
            ("Y4", es, "+count(1)".to_owned(), exists_only()),
            // A call as an INSERT value, read as in a SET value and refused
            // by its name.
            ("C6", es, insert("count()"), unary()),
            ("C7", es, insert("cast(m AS INT)"), data_type("cast", "1:59:4")),
            ("C9", es, insert("upper(NOT b)"), data_type("upper", "1:59:5")),
            ("X5", es, insert("nullif(m)"), wf("Expected COMMA")),
            ("X9", es, insert("count(*)"), data_type("count", "1:59:5")),
            // A syntax error anywhere comes first, then the first refusal in
            // written order.
            ("O1", es, format!("UPDATE {TABLE} SET x = NOT a, y = WHERE pk = 'q'"), wf("Unexpected keyword")),
            ("O4", es, format!("UPDATE {TABLE} SET a = upper(m), b = WHERE pk = 'q'"), wf("Unexpected keyword")),
            ("O5", es, select("count(m) = 1 AND"), no_right_side()),
            ("O6", es, select("if_not_exists(m, 1) = 1 AND"), no_right_side()),
            ("O11", es, select("cast(m AS INT) = 1 AND"), no_right_side()),
            ("O7", es, select("upper(m) = 1 AND count(m) = 1"), unrecognized("upper", "1:57:5")),
            ("O8", es, select("count(m) = 1 AND upper(m) = 1"), token("count", "1:57:5")),
            ("O9", es, update("a = count(m), b = upper(m)"), token("count", "1:39:5")),
            ("O10", es, update("a = upper(m), b = count(m)"), unrecognized("upper", "1:39:5")),
            ("O3", es, update("x = NOT a, a = size(m)"), update_not()),
        ];
        let differing: Vec<String> = rows
            .iter()
            .filter_map(|(row, parse, sql, want)| {
                let got = answer(*parse, sql);
                (got != *want).then(|| format!("{row} {sql}\n  want {want:?}\n  got  {got:?}"))
            })
            .collect();
        assert!(differing.is_empty(), "{}", differing.join("\n"));
    }
}
