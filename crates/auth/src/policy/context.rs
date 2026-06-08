// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition context trait and `DynamoDB` request context.
//!
//! `ConditionContext` is the shared trait for resolving condition keys during
//! policy evaluation. `RequestContext` implements it for `DynamoDB` operations;
//! `AssumeRoleContext` implements it for trust policy evaluation.

use std::collections::HashMap;

/// Trait for resolving condition keys during policy evaluation.
///
/// Implemented by `RequestContext` (`DynamoDB` operations) and
/// `AssumeRoleContext` (trust policy / `AssumeRole`).
pub trait ConditionContext {
    /// Resolve a condition key to its value(s).
    ///
    /// Returns `None` when the key is absent or not applicable to this context.
    /// Returns `Some(vec![])` when the key is present but has an empty value set.
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>>;
}

/// Request parameters extracted from a `DynamoDB` operation for condition evaluation.
#[derive(Debug, Default)]
pub struct RequestParams {
    /// Partition key values being accessed (for `dynamodb:LeadingKeys`).
    /// `None` for table-level operations (`CreateTable`, etc.).
    pub leading_keys: Option<Vec<String>>,
    /// Attribute names being read/written (for `dynamodb:Attributes`).
    /// `None` when not applicable.
    pub attributes: Option<Vec<String>>,
    /// The Select parameter value (for `dynamodb:Select`).
    pub select: Option<String>,
    /// The `ReturnValues` parameter value (for `dynamodb:ReturnValues`).
    pub return_values: Option<String>,
    /// The `ReturnConsumedCapacity` parameter value.
    pub return_consumed_capacity: Option<String>,
    /// The enclosing operation for batch/transact sub-operations.
    pub enclosing_operation: Option<String>,
}

/// Context for evaluating conditions on `DynamoDB` operations.
///
/// Built by the server middleware before policy evaluation. Contains all
/// condition keys that IAM policies can reference for `DynamoDB` access control.
#[derive(Debug)]
pub struct RequestContext {
    /// Tags on the authenticated principal (`aws:PrincipalTag/*`).
    pub principal_tags: HashMap<String, String>,
    /// Tags on the target resource (`dynamodb:ResourceTag/*`).
    pub resource_tags: HashMap<String, String>,
    /// Partition key values being accessed.
    pub leading_keys: Option<Vec<String>>,
    /// Attribute names being read/written.
    pub attributes: Option<Vec<String>>,
    /// The Select parameter value.
    pub select: Option<String>,
    /// The `ReturnValues` parameter value.
    pub return_values: Option<String>,
    /// The `ReturnConsumedCapacity` parameter value.
    pub return_consumed_capacity: Option<String>,
    /// Whether this is a Scan operation.
    pub full_table_scan: Option<bool>,
    /// The enclosing operation for batch/transact sub-operations.
    pub enclosing_operation: Option<String>,
}

impl RequestContext {
    /// Build context for a `DynamoDB` operation.
    ///
    /// `principal_tags` and `resource_tags` come from the identity and target
    /// table respectively. `is_scan` should be true for Scan operations.
    /// `params` carries operation-specific request parameters.
    #[must_use]
    pub fn build(
        principal_tags: HashMap<String, String>,
        resource_tags: HashMap<String, String>,
        is_scan: bool,
        params: RequestParams,
    ) -> Self {
        Self {
            principal_tags,
            resource_tags,
            leading_keys: params.leading_keys,
            attributes: params.attributes,
            select: params.select,
            return_values: params.return_values,
            return_consumed_capacity: params.return_consumed_capacity,
            full_table_scan: if is_scan { Some(true) } else { None },
            enclosing_operation: params.enclosing_operation,
        }
    }
}

impl ConditionContext for RequestContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            self.principal_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if let Some(tag_key) = key.strip_prefix("dynamodb:ResourceTag/") {
            self.resource_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else {
            match key {
                "dynamodb:LeadingKeys" => self
                    .leading_keys
                    .as_ref()
                    .map(|v| v.iter().map(std::string::String::as_str).collect()),
                "dynamodb:Attributes" => self
                    .attributes
                    .as_ref()
                    .map(|v| v.iter().map(std::string::String::as_str).collect()),
                "dynamodb:Select" => self.select.as_deref().map(|v| vec![v]),
                "dynamodb:ReturnValues" => self.return_values.as_deref().map(|v| vec![v]),
                "dynamodb:ReturnConsumedCapacity" => {
                    self.return_consumed_capacity.as_deref().map(|v| vec![v])
                }
                "dynamodb:FullTableScan" => self
                    .full_table_scan
                    .map(|v| vec![if v { "true" } else { "false" }]),
                "dynamodb:EnclosingOperation" => {
                    self.enclosing_operation.as_deref().map(|v| vec![v])
                }
                _ => None,
            }
        }
    }
}

/// Context for evaluating trust policy conditions during `AssumeRole`.
///
/// Trust policies can reference `aws:PrincipalTag/*` and `sts:ExternalId`.
/// DynamoDB-specific keys are not applicable.
#[derive(Debug)]
pub struct AssumeRoleContext {
    /// Tags on the calling principal.
    pub principal_tags: HashMap<String, String>,
    /// The external ID provided in the `AssumeRole` call (if any).
    pub external_id: Option<String>,
}

impl ConditionContext for AssumeRoleContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            self.principal_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if key == "sts:ExternalId" {
            self.external_id.as_deref().map(|v| vec![v])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_principal_tag() {
        let ctx = RequestContext::build(
            HashMap::from([("Department".to_owned(), "Eng".to_owned())]),
            HashMap::new(),
            false,
            RequestParams::default(),
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/Department"),
            Some(vec!["Eng"])
        );
        assert_eq!(ctx.resolve_key("aws:PrincipalTag/Missing"), None);
    }

    #[test]
    fn request_context_resource_tag() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::from([("Team".to_owned(), "Alpha".to_owned())]),
            false,
            RequestParams::default(),
        );
        assert_eq!(
            ctx.resolve_key("dynamodb:ResourceTag/Team"),
            Some(vec!["Alpha"])
        );
    }

    #[test]
    fn request_context_leading_keys() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                leading_keys: Some(vec!["pk1".to_owned(), "pk2".to_owned()]),
                ..Default::default()
            },
        );
        assert_eq!(
            ctx.resolve_key("dynamodb:LeadingKeys"),
            Some(vec!["pk1", "pk2"])
        );
    }

    #[test]
    fn request_context_absent_key() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams::default(),
        );
        assert_eq!(ctx.resolve_key("dynamodb:LeadingKeys"), None);
        assert_eq!(ctx.resolve_key("dynamodb:FullTableScan"), None);
    }

    #[test]
    fn request_context_full_table_scan() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            true,
            RequestParams::default(),
        );
        assert_eq!(
            ctx.resolve_key("dynamodb:FullTableScan"),
            Some(vec!["true"])
        );
    }

    #[test]
    fn assume_role_context_external_id() {
        let ctx = AssumeRoleContext {
            principal_tags: HashMap::new(),
            external_id: Some("ext-123".to_owned()),
        };
        assert_eq!(ctx.resolve_key("sts:ExternalId"), Some(vec!["ext-123"]));
        assert_eq!(ctx.resolve_key("dynamodb:LeadingKeys"), None);
    }
}
