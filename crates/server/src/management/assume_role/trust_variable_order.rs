// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Where a trust policy write refuses a `${...}` reference IAM refuses among its other
//! checks (issue #834). Measured with `UpdateAssumeRolePolicy` and
//! `CreateRole` on 2026-11-09, two samples each.
//!
//! - `2012-10-17`: after the structural checks (`Effect`, a `Resource`
//!   field, a missing or doubled `Action`, a missing `Principal`, operator
//!   names, in any statement) and before the `Sid`, `NotPrincipal`,
//!   `NotResource`, principal value and action value checks.
//! - `2008-10-17` or no `Version`: an Arn value, after the structural checks
//!   on every statement and then the `NotPrincipal` and `Sid` checks, and
//!   before the `NotResource`, principal value and action value checks.

use serde_json::Value;

use super::{is_known_iam_condition_operator, non_null, validate_trust_effect};
use extenddb_auth::policy::variable_syntax::{trust_arn_value_error, trust_variable_error};

/// Refuse a statement `Sid` outside IAM's charset. Measured on live IAM
/// CreateRole: a Sid is accepted only when it is empty or entirely ASCII
/// letters and digits; a hyphen, underscore, space, dot or non-ASCII
/// character is rejected with MalformedPolicyDocument. Identity PutUserPolicy
/// applies the same rule (see validate_statement_sids).
pub(super) fn validate_trust_sids(statements: &[Value]) -> Result<(), String> {
    for stmt in statements {
        if let Some(sid) = stmt.get("Sid").and_then(Value::as_str)
            && !sid.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err("Trust policy Sid must be ASCII letters and digits only".to_owned());
        }
    }
    Ok(())
}

const RESOURCE_NOT_ALLOWED: &str = "Resource is not allowed in a trust policy";

pub(super) fn validate_trust_no_resource(stmt: &Value) -> Result<(), String> {
    if non_null(stmt.get("Resource")).is_some() || non_null(stmt.get("NotResource")).is_some() {
        return Err(RESOURCE_NOT_ALLOWED.to_owned());
    }
    Ok(())
}

/// The statement's `Action` or `NotAction`, refusing a statement with neither
/// or both.
pub(super) fn trust_action_field(stmt: &Value) -> Result<&Value, String> {
    match (
        non_null(stmt.get("Action")),
        non_null(stmt.get("NotAction")),
    ) {
        (None, None) => Err("statement is missing Action".to_owned()),
        (Some(_), Some(_)) => Err("statement has both Action and NotAction".to_owned()),
        (Some(a), None) | (None, Some(a)) => Ok(a),
    }
}

pub(super) const MISSING_PRINCIPAL: &str = "statement is missing Principal";
pub(super) const NOT_PRINCIPAL: &str = "NotPrincipal is not allowed in a trust policy";

/// The block and operator name checks of `validate_trust_condition`, without
/// the value checks.
fn validate_trust_condition_operators(cond: &Value) -> Result<(), String> {
    let obj = cond
        .as_object()
        .ok_or_else(|| "Condition must be an object".to_owned())?;
    for (op_name, keys) in obj {
        if !is_known_iam_condition_operator(op_name) {
            return Err(format!("invalid Condition operator: {op_name}"));
        }
        if !keys.is_object() {
            return Err(format!("Condition {op_name} must be an object"));
        }
    }
    Ok(())
}

/// The checks IAM runs before the variable check, on every statement.
fn validate_structure(stmt: &Value) -> Result<(), String> {
    validate_trust_effect(stmt)?;
    if non_null(stmt.get("Resource")).is_some() {
        return Err(RESOURCE_NOT_ALLOWED.to_owned());
    }
    trust_action_field(stmt)?;
    if non_null(stmt.get("Principal")).is_none() && non_null(stmt.get("NotPrincipal")).is_none() {
        return Err(MISSING_PRINCIPAL.to_owned());
    }
    if let Some(cond) = non_null(stmt.get("Condition")) {
        validate_trust_condition_operators(cond)?;
    }
    Ok(())
}

/// Refuse a value with a `${...}` reference IAM refuses in the place IAM refuses it,
/// running the checks IAM runs first. `Ok` when the document holds no such
/// value; the caller then runs its other checks.
pub(super) fn validate_trust_variables(
    trust_policy: &Value,
    statements: &[Value],
) -> Result<(), String> {
    if let Some(msg) = trust_variable_error(trust_policy) {
        for stmt in statements {
            validate_structure(stmt)?;
        }
        return Err(msg);
    }
    if let Some(msg) = trust_arn_value_error(trust_policy) {
        for stmt in statements {
            validate_structure(stmt)?;
        }
        if statements
            .iter()
            .any(|stmt| non_null(stmt.get("NotPrincipal")).is_some())
        {
            return Err(NOT_PRINCIPAL.to_owned());
        }
        validate_trust_sids(statements)?;
        return Err(msg);
    }
    Ok(())
}
