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

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::{Statement, parse};
use extenddb_core::types::AttributeValue;

use crate::{DispatchResult, OperationContext};

mod batch;
mod common;
mod expr_bridge;
mod select;
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
        let req = SingleRequest::from_body(&body)?;
        let statement =
            parse(&req.statement).map_err(|e| DynamoDbError::ValidationException(e.message))?;
        match statement {
            Statement::Select(s) => select::run(self, s, &req).await,
            other => write::run(self, other, &req).await,
        }
    }

    async fn batch_execute_statement(&self, body: Value) -> Result<DispatchResult, DynamoDbError> {
        batch::run(self, &body).await
    }

    async fn execute_transaction(&self, body: Value) -> Result<DispatchResult, DynamoDbError> {
        transaction::run(self, &body).await
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
