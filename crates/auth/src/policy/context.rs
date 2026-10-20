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

    /// Returns `true` if `key` is a multivalued (set-valued) condition key in this
    /// context.
    ///
    /// Arity is a property of the *key*, not of how many values happen to be present
    /// on a given request. AWS IAM requires a `ForAllValues:`/`ForAnyValue:`
    /// set-operator qualifier for multivalued keys; a bare operator (no qualifier)
    /// applied to a multivalued key never matches, even when a single value is
    /// present. Verified against real AWS IAM (BR-7085). Returning `false` for an
    /// unknown key preserves the normal single-valued evaluation path.
    fn is_multivalued_key(&self, key: &str) -> bool;

    /// Whether `key` is one the service populates on a request of this kind but
    /// which this context cannot resolve.
    ///
    /// This distinguishes a key that is genuinely absent (the service would
    /// also have it absent, so IAM absent-key semantics apply) from a key the
    /// service populates that ExtendDB has no value for. A statement gated on
    /// an unresolvable key fails closed: a `Deny` blocks, an `Allow` does not
    /// grant. The default is `false` (no unresolvable keys), so a context that
    /// resolves every key it is asked about needs no override.
    fn is_unresolvable_key(&self, _key: &str) -> bool {
        false
    }
}

/// Global condition keys the AWS services always populate on a request. A
/// context that cannot resolve one of these treats it as unresolvable (fail
/// closed) rather than absent, so a negated operator on it cannot spuriously
/// match and an `Allow` gated on it cannot spuriously grant.
///
/// The set is the single-valued `aws:` global keys that STS always populates on
/// an `AssumeRole` call and that IAM always populates on a `DynamoDB`
/// data-plane request from an IAM principal. Keys that are only conditionally
/// present (for example `aws:MultiFactorAuthPresent`, which is absent when the
/// caller did not use MFA, or the VPC and federation keys) are deliberately not
/// listed: they are genuinely absent for ExtendDB's caller model, and IAM
/// absent-key semantics match the service there (measured, case e27 vs e28).
/// Tag keys (`aws:PrincipalTag/*`, `aws:RequestTag/*`, `aws:TagKeys`,
/// `dynamodb:ResourceTag/*`), `sts:ExternalId`, and the `dynamodb:` request
/// keys are resolved (or genuinely absent) per context and are not listed here.
#[must_use]
pub fn is_service_populated_global_key(key: &str) -> bool {
    matches!(
        key,
        "aws:PrincipalArn"
            | "aws:PrincipalAccount"
            | "aws:PrincipalOrgID"
            | "aws:PrincipalOrgPaths"
            | "aws:PrincipalType"
            | "aws:userid"
            | "aws:username"
            | "aws:SourceIp"
            | "aws:SecureTransport"
            | "aws:CurrentTime"
            | "aws:EpochTime"
            | "aws:RequestedRegion"
            | "aws:UserAgent"
    )
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

    fn is_multivalued_key(&self, key: &str) -> bool {
        // Per the AWS DynamoDB service authorization reference these keys are
        // `ArrayOfString` (multivalued); all other DynamoDB keys and tag keys are
        // single-valued. Verified against real AWS IAM (BR-7085).
        matches!(key, "dynamodb:LeadingKeys" | "dynamodb:Attributes")
    }
}

/// Context for evaluating trust policy conditions during `AssumeRole`.
///
/// Trust policies can reference `aws:PrincipalTag/*`, `sts:ExternalId`, the
/// caller identity keys `aws:PrincipalArn` / `aws:PrincipalAccount`, and the
/// session-tag keys `aws:RequestTag/*` / `aws:TagKeys`. Other global keys STS
/// populates on an `AssumeRole` call (for example `aws:PrincipalOrgID`,
/// `aws:SourceIp`, `aws:MultiFactorAuthPresent`) are not resolvable here and
/// are reported as unresolvable (fail closed). `DynamoDB`-specific keys are not
/// applicable.
#[derive(Debug)]
pub struct AssumeRoleContext {
    /// Tags on the calling principal.
    pub principal_tags: HashMap<String, String>,
    /// The external ID provided in the `AssumeRole` call (if any).
    pub external_id: Option<String>,
    /// The caller ARN (`aws:PrincipalArn`).
    pub principal_arn: String,
    /// The caller's account id (`aws:PrincipalAccount`), the account field of
    /// the caller ARN. `None` when the ARN has no account field.
    pub principal_account: Option<String>,
    /// Session tags passed on the call (`aws:RequestTag/*`, and their keys for
    /// `aws:TagKeys`).
    pub request_tags: HashMap<String, String>,
}

impl ConditionContext for AssumeRoleContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            self.principal_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if let Some(tag_key) = key.strip_prefix("aws:RequestTag/") {
            self.request_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else {
            match key {
                "sts:ExternalId" => self.external_id.as_deref().map(|v| vec![v]),
                "aws:PrincipalArn" => Some(vec![self.principal_arn.as_str()]),
                "aws:PrincipalAccount" => self.principal_account.as_deref().map(|v| vec![v]),
                "aws:TagKeys" => {
                    if self.request_tags.is_empty() {
                        None
                    } else {
                        Some(self.request_tags.keys().map(String::as_str).collect())
                    }
                }
                _ => None,
            }
        }
    }

    fn is_multivalued_key(&self, key: &str) -> bool {
        // `aws:TagKeys` is the set of session-tag keys (`ArrayOfString`); every
        // other trust-policy key (`aws:PrincipalTag/*`, `aws:RequestTag/*`,
        // `sts:ExternalId`, `aws:PrincipalArn`, `aws:PrincipalAccount`) is
        // single-valued.
        key == "aws:TagKeys"
    }

    fn is_unresolvable_key(&self, key: &str) -> bool {
        // Keys this context resolves (tags, external id, principal ARN and
        // account) are never reported here (the caller only consults this when
        // `resolve_key` returned `None`, i.e. the key is genuinely absent for a
        // tag or external id). Any other global key STS populates on an
        // `AssumeRole` call is unresolvable, so a Deny gated on it fails closed.
        is_service_populated_global_key(key)
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
            principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
            principal_account: Some("123456789012".to_owned()),
            request_tags: HashMap::new(),
        };
        assert_eq!(ctx.resolve_key("sts:ExternalId"), Some(vec!["ext-123"]));
        assert_eq!(ctx.resolve_key("dynamodb:LeadingKeys"), None);
    }

    #[test]
    fn assume_role_context_principal_arn_and_account() {
        let ctx = AssumeRoleContext {
            principal_tags: HashMap::new(),
            external_id: None,
            principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
            principal_account: Some("123456789012".to_owned()),
            request_tags: HashMap::new(),
        };
        assert_eq!(
            ctx.resolve_key("aws:PrincipalArn"),
            Some(vec!["arn:aws:iam::123456789012:user/alice"])
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalAccount"),
            Some(vec!["123456789012"])
        );
        // A global key STS populates but this context cannot resolve is
        // unresolvable (fail closed), not absent.
        assert_eq!(ctx.resolve_key("aws:PrincipalOrgID"), None);
        assert!(ctx.is_unresolvable_key("aws:PrincipalOrgID"));
        assert!(!ctx.is_unresolvable_key("sts:ExternalId"));
        assert!(!ctx.is_unresolvable_key("aws:PrincipalTag/team"));
    }

    #[test]
    fn assume_role_context_request_tags_and_tag_keys() {
        let ctx = AssumeRoleContext {
            principal_tags: HashMap::new(),
            external_id: None,
            principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
            principal_account: Some("123456789012".to_owned()),
            request_tags: HashMap::from([("env".to_owned(), "prod".to_owned())]),
        };
        assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec!["prod"]));
        assert_eq!(ctx.resolve_key("aws:RequestTag/missing"), None);
        assert_eq!(ctx.resolve_key("aws:TagKeys"), Some(vec!["env"]));
        assert!(ctx.is_multivalued_key("aws:TagKeys"));
        // With no session tags, aws:TagKeys is genuinely absent, not
        // unresolvable.
        let empty = AssumeRoleContext {
            principal_tags: HashMap::new(),
            external_id: None,
            principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
            principal_account: Some("123456789012".to_owned()),
            request_tags: HashMap::new(),
        };
        assert_eq!(empty.resolve_key("aws:TagKeys"), None);
        assert!(!empty.is_unresolvable_key("aws:TagKeys"));
    }
}
