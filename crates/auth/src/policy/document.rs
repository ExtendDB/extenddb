// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy document types and JSON deserialization.
//!
//! Represents IAM policy documents as Rust types with compile-time guarantees:
//! `ActionMatch` and `ResourceMatch` enums prevent invalid states where both
//! `Action` and `NotAction` (or `Resource` and `NotResource`) are present.

use serde_json::Value;
use std::fmt;

/// A parsed IAM policy document.
#[derive(Debug, Clone)]
pub struct PolicyDocument {
    /// Policy version string (e.g., "2012-10-17").
    pub version: String,
    /// The policy statements.
    pub statements: Vec<Statement>,
}

/// A single policy statement.
#[derive(Debug, Clone)]
pub struct Statement {
    /// Optional statement ID.
    pub sid: Option<String>,
    /// Allow or Deny.
    pub effect: Effect,
    /// Action or `NotAction` matching.
    pub action_match: ActionMatch,
    /// Resource or `NotResource` matching.
    pub resource_match: ResourceMatch,
    /// Conditions that must all be true for the statement to apply.
    pub conditions: Vec<Condition>,
    /// Principal matching — used in trust policies only. `None` for identity-based policies.
    pub principal_match: Option<PrincipalMatch>,
}

/// Allow or Deny effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// Action matching: either Action (include list) or `NotAction` (exclude list).
/// A statement uses exactly one — never both.
#[derive(Debug, Clone)]
pub enum ActionMatch {
    /// Matches listed actions.
    Actions(Vec<String>),
    /// Matches everything except listed actions.
    NotActions(Vec<String>),
}

/// Resource matching: either Resource (include list) or `NotResource` (exclude list).
#[derive(Debug, Clone)]
pub enum ResourceMatch {
    /// Matches listed resources.
    Resources(Vec<String>),
    /// Matches everything except listed resources.
    NotResources(Vec<String>),
}

/// Principal matching for trust policies.
#[derive(Debug, Clone)]
pub enum PrincipalMatch {
    /// Matches listed principals.
    Principals(Vec<String>),
    /// Matches everything except listed principals.
    NotPrincipals(Vec<String>),
}

/// A condition block entry.
#[derive(Debug, Clone)]
pub struct Condition {
    /// The condition operator.
    pub operator: ConditionOperator,
    /// The condition key (e.g., "aws:PrincipalTag/Department").
    pub key: String,
    /// The values to compare against.
    pub values: Vec<String>,
}

/// All IAM condition operators relevant to `DynamoDB` access control.
///
/// Set operators (`ForAllValues`, `ForAnyValue`) and `IfExists` wrap a base
/// operator. Valid nestings: `ForAllValues(IfExists(base))`,
/// `ForAnyValue(IfExists(base))`, or any single wrapper around a base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionOperator {
    // String
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    // Numeric
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    // Date
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    // Boolean
    Bool,
    // Null check
    Null,
    // ARN
    ArnEquals,
    ArnNotEquals,
    ArnLike,
    ArnNotLike,
    // Set operators (prefix applied to any of the above)
    ForAllValues(Box<ConditionOperator>),
    ForAnyValue(Box<ConditionOperator>),
    // IfExists suffix (condition passes if key is absent)
    IfExists(Box<ConditionOperator>),
}

/// Error parsing a policy document.
#[derive(Debug, Clone)]
pub struct PolicyParseError(pub String);

impl fmt::Display for PolicyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "policy parse error: {}", self.0)
    }
}

impl std::error::Error for PolicyParseError {}

impl PolicyDocument {
    /// Parse a policy document from a JSON string.
    ///
    /// Enforces the default 6,144-byte size limit (matching AWS IAM inline
    /// policy limit). This applies to both write-path validation and read-path
    /// parsing of stored policies — a stored policy exceeding this limit will
    /// fail to parse, triggering fail-closed denial (defense in depth).
    ///
    /// # Errors
    ///
    /// Returns `PolicyParseError` if the JSON is malformed or contains
    /// invalid policy constructs (e.g., both Action and `NotAction`).
    pub fn from_json(json: &str) -> Result<Self, PolicyParseError> {
        Self::from_json_with_size_limit(json, 6_144)
    }

    /// Parse a policy document with an explicit size limit.
    ///
    /// # Errors
    ///
    /// Returns `PolicyParseError` if the document exceeds `max_bytes` or is malformed.
    pub fn from_json_with_size_limit(
        json: &str,
        max_bytes: usize,
    ) -> Result<Self, PolicyParseError> {
        if json.len() > max_bytes {
            return Err(PolicyParseError(format!(
                "policy document exceeds maximum size ({max_bytes} bytes)"
            )));
        }
        let raw: Value = serde_json::from_str(json).map_err(|e| PolicyParseError(e.to_string()))?;
        let version = raw["Version"].as_str().unwrap_or("2012-10-17").to_owned();
        let statements = parse_statements(&raw["Statement"])?;
        Ok(Self {
            version,
            statements,
        })
    }
}

/// Parse the Statement field — may be a single object or an array.
fn parse_statements(value: &Value) -> Result<Vec<Statement>, PolicyParseError> {
    match value {
        Value::Array(arr) => arr.iter().map(parse_statement).collect(),
        Value::Object(_) => Ok(vec![parse_statement(value)?]),
        _ => Err(PolicyParseError(
            "Statement must be an object or array".to_owned(),
        )),
    }
}

/// Parse a single statement object.
fn parse_statement(value: &Value) -> Result<Statement, PolicyParseError> {
    let sid = value["Sid"].as_str().map(ToOwned::to_owned);

    let effect = match value["Effect"].as_str() {
        Some(s) if s.eq_ignore_ascii_case("Allow") => Effect::Allow,
        Some(s) if s.eq_ignore_ascii_case("Deny") => Effect::Deny,
        Some(s) => return Err(PolicyParseError(format!("invalid Effect: {s}"))),
        None => return Err(PolicyParseError("missing Effect".to_owned())),
    };

    let action_match = parse_action_match(value)?;
    let resource_match = parse_resource_match(value)?;
    let conditions = parse_conditions(&value["Condition"])?;
    let principal_match = parse_principal_match(value)?;

    Ok(Statement {
        sid,
        effect,
        action_match,
        resource_match,
        conditions,
        principal_match,
    })
}

/// Parse Action or `NotAction` (mutually exclusive).
fn parse_action_match(value: &Value) -> Result<ActionMatch, PolicyParseError> {
    let has_action = !value["Action"].is_null();
    let has_not_action = !value["NotAction"].is_null();
    match (has_action, has_not_action) {
        (true, true) => Err(PolicyParseError(
            "statement has both Action and NotAction".to_owned(),
        )),
        (false, false) => Err(PolicyParseError(
            "statement has neither Action nor NotAction".to_owned(),
        )),
        (true, false) => Ok(ActionMatch::Actions(parse_string_or_array(
            &value["Action"],
        )?)),
        (false, true) => Ok(ActionMatch::NotActions(parse_string_or_array(
            &value["NotAction"],
        )?)),
    }
}

/// Parse Resource or `NotResource` (mutually exclusive).
fn parse_resource_match(value: &Value) -> Result<ResourceMatch, PolicyParseError> {
    let has_resource = !value["Resource"].is_null();
    let has_not_resource = !value["NotResource"].is_null();
    match (has_resource, has_not_resource) {
        (true, true) => Err(PolicyParseError(
            "statement has both Resource and NotResource".to_owned(),
        )),
        (false, false) => Err(PolicyParseError(
            "statement has neither Resource nor NotResource".to_owned(),
        )),
        (true, false) => Ok(ResourceMatch::Resources(parse_string_or_array(
            &value["Resource"],
        )?)),
        (false, true) => Ok(ResourceMatch::NotResources(parse_string_or_array(
            &value["NotResource"],
        )?)),
    }
}

/// Parse Principal or `NotPrincipal` (optional, for trust policies).
fn parse_principal_match(value: &Value) -> Result<Option<PrincipalMatch>, PolicyParseError> {
    let has_principal = !value["Principal"].is_null();
    let has_not_principal = !value["NotPrincipal"].is_null();
    match (has_principal, has_not_principal) {
        (true, true) => Err(PolicyParseError(
            "statement has both Principal and NotPrincipal".to_owned(),
        )),
        (false, false) => Ok(None),
        (true, false) => {
            // Principal can be "*" (string) or {"AWS": [...]}
            let principals = parse_principal_value(&value["Principal"])?;
            Ok(Some(PrincipalMatch::Principals(principals)))
        }
        (false, true) => {
            let principals = parse_principal_value(&value["NotPrincipal"])?;
            Ok(Some(PrincipalMatch::NotPrincipals(principals)))
        }
    }
}

/// Parse a Principal value — can be "*", a string ARN, or {"AWS": [...]}.
fn parse_principal_value(value: &Value) -> Result<Vec<String>, PolicyParseError> {
    match value {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Object(map) => {
            // {"AWS": "arn:..." } or {"AWS": ["arn:...", ...]}
            if let Some(aws) = map.get("AWS") {
                parse_string_or_array(aws)
            } else {
                Err(PolicyParseError(
                    "Principal object must have AWS key".to_owned(),
                ))
            }
        }
        _ => Err(PolicyParseError("invalid Principal value".to_owned())),
    }
}

/// Parse the Condition block: `{ "OperatorName": { "key": "value" | ["values"] } }`.
pub fn parse_conditions(value: &Value) -> Result<Vec<Condition>, PolicyParseError> {
    let obj = match value {
        Value::Null => return Ok(Vec::new()),
        Value::Object(o) => o,
        _ => return Err(PolicyParseError("Condition must be an object".to_owned())),
    };

    let mut conditions = Vec::new();
    for (op_name, keys_obj) in obj {
        let operator = parse_operator(op_name)?;
        let keys = keys_obj
            .as_object()
            .ok_or_else(|| PolicyParseError(format!("condition {op_name} must be an object")))?;
        for (key, vals) in keys {
            let values = parse_string_or_array(vals)?;
            conditions.push(Condition {
                operator: operator.clone(),
                key: key.clone(),
                values,
            });
        }
    }
    Ok(conditions)
}

/// Parse a condition operator name, handling `ForAllValues:`, `ForAnyValue:` prefixes
/// and `IfExists` suffix.
fn parse_operator(name: &str) -> Result<ConditionOperator, PolicyParseError> {
    let (set_prefix, rest) = if let Some(r) = name.strip_prefix("ForAllValues:") {
        (Some("ForAllValues"), r)
    } else if let Some(r) = name.strip_prefix("ForAnyValue:") {
        (Some("ForAnyValue"), r)
    } else {
        (None, name)
    };

    let (base_name, has_if_exists) = if let Some(b) = rest.strip_suffix("IfExists") {
        (b, true)
    } else {
        (rest, false)
    };

    let base = parse_base_operator(base_name)?;

    let with_if_exists = if has_if_exists {
        ConditionOperator::IfExists(Box::new(base))
    } else {
        base
    };

    match set_prefix {
        Some("ForAllValues") => Ok(ConditionOperator::ForAllValues(Box::new(with_if_exists))),
        Some("ForAnyValue") => Ok(ConditionOperator::ForAnyValue(Box::new(with_if_exists))),
        _ => Ok(with_if_exists),
    }
}

/// Parse a base operator name (no prefix/suffix).
fn parse_base_operator(name: &str) -> Result<ConditionOperator, PolicyParseError> {
    match name {
        "StringEquals" => Ok(ConditionOperator::StringEquals),
        "StringNotEquals" => Ok(ConditionOperator::StringNotEquals),
        "StringEqualsIgnoreCase" => Ok(ConditionOperator::StringEqualsIgnoreCase),
        "StringLike" => Ok(ConditionOperator::StringLike),
        "StringNotLike" => Ok(ConditionOperator::StringNotLike),
        "NumericEquals" => Ok(ConditionOperator::NumericEquals),
        "NumericNotEquals" => Ok(ConditionOperator::NumericNotEquals),
        "NumericLessThan" => Ok(ConditionOperator::NumericLessThan),
        "NumericLessThanEquals" => Ok(ConditionOperator::NumericLessThanEquals),
        "NumericGreaterThan" => Ok(ConditionOperator::NumericGreaterThan),
        "NumericGreaterThanEquals" => Ok(ConditionOperator::NumericGreaterThanEquals),
        "DateEquals" => Ok(ConditionOperator::DateEquals),
        "DateNotEquals" => Ok(ConditionOperator::DateNotEquals),
        "DateLessThan" => Ok(ConditionOperator::DateLessThan),
        "DateLessThanEquals" => Ok(ConditionOperator::DateLessThanEquals),
        "DateGreaterThan" => Ok(ConditionOperator::DateGreaterThan),
        "DateGreaterThanEquals" => Ok(ConditionOperator::DateGreaterThanEquals),
        "Bool" => Ok(ConditionOperator::Bool),
        "Null" => Ok(ConditionOperator::Null),
        "ArnEquals" => Ok(ConditionOperator::ArnEquals),
        "ArnNotEquals" => Ok(ConditionOperator::ArnNotEquals),
        "ArnLike" => Ok(ConditionOperator::ArnLike),
        "ArnNotLike" => Ok(ConditionOperator::ArnNotLike),
        _ => Err(PolicyParseError(format!("unknown operator: {name}"))),
    }
}

/// Parse a JSON value that is either a single string or an array of strings.
fn parse_string_or_array(value: &Value) -> Result<Vec<String>, PolicyParseError> {
    match value {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(arr) => arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| PolicyParseError("expected string in array".to_owned()))
            })
            .collect(),
        _ => Err(PolicyParseError("expected string or array".to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_allow_policy() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*"
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert_eq!(doc.version, "2012-10-17");
        assert_eq!(doc.statements.len(), 1);
        assert_eq!(doc.statements[0].effect, Effect::Allow);
        assert!(matches!(
            &doc.statements[0].action_match,
            ActionMatch::Actions(a) if a == &["dynamodb:*"]
        ));
    }

    #[test]
    fn parse_deny_with_not_action() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "NotAction": ["dynamodb:GetItem"],
                "Resource": "*"
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert_eq!(doc.statements[0].effect, Effect::Deny);
        assert!(matches!(
            &doc.statements[0].action_match,
            ActionMatch::NotActions(a) if a == &["dynamodb:GetItem"]
        ));
    }

    #[test]
    fn parse_condition_with_set_operator() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*",
                "Condition": {
                    "ForAllValues:StringEquals": {
                        "dynamodb:Attributes": ["id", "name"]
                    }
                }
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        let cond = &doc.statements[0].conditions[0];
        assert!(matches!(
            &cond.operator,
            ConditionOperator::ForAllValues(inner) if **inner == ConditionOperator::StringEquals
        ));
        assert_eq!(cond.key, "dynamodb:Attributes");
        assert_eq!(cond.values, vec!["id", "name"]);
    }

    #[test]
    fn parse_trust_policy_with_principal() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
                "Action": "sts:AssumeRole",
                "Resource": "*"
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert!(matches!(
            &doc.statements[0].principal_match,
            Some(PrincipalMatch::Principals(p)) if p == &["arn:aws:iam::123456789012:user/alice"]
        ));
    }

    #[test]
    fn parse_if_exists_suffix() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*",
                "Condition": {
                    "StringEqualsIfExists": {
                        "dynamodb:Select": ["ALL_ATTRIBUTES"]
                    }
                }
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert!(matches!(
            &doc.statements[0].conditions[0].operator,
            ConditionOperator::IfExists(inner) if **inner == ConditionOperator::StringEquals
        ));
    }

    #[test]
    fn reject_both_action_and_not_action() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "NotAction": "dynamodb:DeleteTable",
                "Resource": "*"
            }]
        }"#;
        assert!(PolicyDocument::from_json(json).is_err());
    }

    #[test]
    fn parse_single_statement_object() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*"
            }
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert_eq!(doc.statements.len(), 1);
    }

    #[test]
    fn parse_wildcard_principal() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "sts:AssumeRole",
                "Resource": "*"
            }]
        }"#;
        let doc = PolicyDocument::from_json(json).unwrap();
        assert!(matches!(
            &doc.statements[0].principal_match,
            Some(PrincipalMatch::Principals(p)) if p == &["*"]
        ));
    }
}
