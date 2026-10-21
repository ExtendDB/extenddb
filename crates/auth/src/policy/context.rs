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

/// Look up a tag map by key, case-insensitively on the key.
///
/// AWS resolves tag condition keys (`aws:PrincipalTag/<k>`, `aws:RequestTag/<k>`,
/// `aws:ResourceTag/<k>`, `dynamodb:ResourceTag/<k>`) without regard to the case
/// of `<k>`; a tag stored as `Team` is matched by a policy naming `team`. Tag
/// values stay case-sensitive. IAM forbids two tag keys that differ only in
/// case, so a case-insensitive match is unambiguous.
fn ci_tag_lookup<'a>(map: &'a HashMap<String, String>, tag_key: &str) -> Option<Vec<&'a str>> {
    map.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(tag_key))
        .map(|(_, v)| vec![v.as_str()])
}

/// Documented AWS global condition keys the live service populates for a
/// request from one of ExtendDB's callers but that this build cannot resolve to
/// the value the service would have. A statement using one fails closed: a
/// `Deny` blocks and an `Allow` does not grant, instead of the key being
/// treated as absent (where a negated operator on an absent single-valued key
/// would spuriously match).
///
/// This set is empty. ExtendDB resolves every global key the live service
/// populates for a signed IAM-user or assumed-role request, or an `AssumeRole`
/// call: the principal identity keys, `aws:userid`, the request time, region,
/// TLS state, source address, user agent, principal and resource account, and
/// the tag keys. Every other `aws:` global key is one the service leaves absent
/// for a standalone AWS account reached by a direct caller (the organization,
/// VPC-endpoint, federation, MFA, service-principal and cross-service keys), or
/// one the service does not recognize; the service treats both as absent.
/// ExtendDB does the same by not listing them here, so an unresolved key keeps
/// IAM absent-key semantics. The slice stays so a future global key the service
/// begins populating before ExtendDB resolves it can be made to fail closed by
/// listing it.
const FAIL_CLOSED_GLOBAL_KEYS: &[&str] = &[];

/// Whether an unresolved condition key must fail closed for the context that
/// could not resolve it. Consulted only after `resolve_key` returned `None`.
///
/// A key fails closed only when it is on [`FAIL_CLOSED_GLOBAL_KEYS`] (currently
/// empty). Every other unresolved key, `aws:` global, `dynamodb:`,
/// `sts:` or a tag sub-key, keeps absent-key semantics: a genuinely absent key
/// does not match, and a negated operator on an absent single-valued key
/// matches, which is what the live service does for a key it does not populate.
#[must_use]
pub fn aws_key_is_unresolvable(key: &str) -> bool {
    FAIL_CLOSED_GLOBAL_KEYS.contains(&key)
}

/// The request time as the pair (`aws:CurrentTime` as RFC 3339 UTC,
/// `aws:EpochTime` as whole seconds since the epoch).
#[must_use]
pub fn request_time_strings() -> (String, String) {
    let now = time::OffsetDateTime::now_utc();
    let iso = now
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    (iso, now.unix_timestamp().to_string())
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
    /// The caller ARN (`aws:PrincipalArn`), when known.
    pub principal_arn: Option<String>,
    /// The caller's account id (`aws:PrincipalAccount`), when known.
    pub principal_account: Option<String>,
    /// The server region (`aws:RequestedRegion`), when known.
    pub region: Option<String>,
    /// Whether the request arrived over TLS (`aws:SecureTransport`). The
    /// listener is entirely TLS or entirely plaintext, so this is a
    /// server-level value, not per-connection.
    pub secure_transport: bool,
    /// The caller principal type (`aws:PrincipalType`): `User` for an IAM user,
    /// `AssumedRole` for a role session.
    pub principal_type: Option<String>,
    /// The IAM user name (`aws:username`) for a user caller; `None` for a role
    /// session, where the service does not populate it.
    pub username: Option<String>,
    /// The caller's source IP (`aws:SourceIp`): the peer address of the
    /// connection the request arrived on. `None` when it could not be
    /// determined, where it keeps absent-key semantics.
    pub source_ip: Option<String>,
    /// The caller's `User-Agent` header (`aws:UserAgent`), when present.
    pub user_agent: Option<String>,
    /// The caller's principal unique id (`aws:userid`): an `AIDA...` id for a
    /// user, `<role-id>:<session-name>` for a role session.
    pub userid: Option<String>,
    /// The account that owns the target resource (`aws:ResourceAccount`). For
    /// ExtendDB this is the account in the resource ARN, which is the caller's
    /// account (ExtendDB resources are same-account).
    pub resource_account: Option<String>,
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
    /// The caller ARN (`aws:PrincipalArn`), when known.
    pub principal_arn: Option<String>,
    /// The caller's account id (`aws:PrincipalAccount`), when known.
    pub principal_account: Option<String>,
    /// The server region (`aws:RequestedRegion`), when known.
    pub region: Option<String>,
    /// Whether the request arrived over TLS (`aws:SecureTransport`).
    pub secure_transport: bool,
    /// The caller principal type (`aws:PrincipalType`).
    pub principal_type: Option<String>,
    /// The IAM user name (`aws:username`) for a user caller; `None` for a role
    /// session.
    pub username: Option<String>,
    /// The caller's source IP (`aws:SourceIp`).
    pub source_ip: Option<String>,
    /// The caller's `User-Agent` header (`aws:UserAgent`).
    pub user_agent: Option<String>,
    /// The caller's principal unique id (`aws:userid`).
    pub userid: Option<String>,
    /// The target resource owner account (`aws:ResourceAccount`).
    pub resource_account: Option<String>,
    /// The request time formatted for `aws:CurrentTime` (RFC 3339 UTC).
    pub current_time: String,
    /// The request time formatted for `aws:EpochTime` (whole seconds).
    pub epoch_time: String,
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
        let (current_time, epoch_time) = request_time_strings();
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
            principal_arn: params.principal_arn,
            principal_account: params.principal_account,
            region: params.region,
            secure_transport: params.secure_transport,
            principal_type: params.principal_type,
            username: params.username,
            source_ip: params.source_ip,
            user_agent: params.user_agent,
            userid: params.userid,
            resource_account: params.resource_account,
            current_time,
            epoch_time,
        }
    }
}

impl ConditionContext for RequestContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            ci_tag_lookup(&self.principal_tags, tag_key)
        } else if let Some(tag_key) = key.strip_prefix("aws:ResourceTag/") {
            // The service evaluates resource tags as aws:ResourceTag/*. ExtendDB
            // resolves them from the same resource-tag map it uses for
            // dynamodb:ResourceTag/*, case-insensitively on the key.
            ci_tag_lookup(&self.resource_tags, tag_key)
        } else if let Some(tag_key) = key.strip_prefix("dynamodb:ResourceTag/") {
            ci_tag_lookup(&self.resource_tags, tag_key)
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
                "aws:PrincipalArn" => self.principal_arn.as_deref().map(|v| vec![v]),
                "aws:PrincipalAccount" => self.principal_account.as_deref().map(|v| vec![v]),
                "aws:RequestedRegion" => self.region.as_deref().map(|v| vec![v]),
                "aws:SecureTransport" => Some(vec![if self.secure_transport {
                    "true"
                } else {
                    "false"
                }]),
                "aws:CurrentTime" => Some(vec![self.current_time.as_str()]),
                "aws:EpochTime" => Some(vec![self.epoch_time.as_str()]),
                "aws:PrincipalType" => self.principal_type.as_deref().map(|v| vec![v]),
                "aws:username" => self.username.as_deref().map(|v| vec![v]),
                "aws:SourceIp" => self.source_ip.as_deref().map(|v| vec![v]),
                "aws:UserAgent" => self.user_agent.as_deref().map(|v| vec![v]),
                "aws:userid" => self.userid.as_deref().map(|v| vec![v]),
                "aws:ResourceAccount" => self.resource_account.as_deref().map(|v| vec![v]),
                // ExtendDB is not an AWS service calling on a principal's behalf,
                // so these are always false, matching the service for a direct
                // caller.
                "aws:ViaAWSService" => Some(vec!["false"]),
                "aws:PrincipalIsAWSService" => Some(vec!["false"]),
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

    fn is_unresolvable_key(&self, key: &str) -> bool {
        // An unresolved aws: global key that is neither a tag sub-key nor on the
        // absent-ok allowlist is one the service populates but this context has
        // no value for (for example aws:PrincipalOrgID, aws:SourceIp): it fails
        // closed. Keys resolved above never reach here.
        aws_key_is_unresolvable(key)
    }
}

/// Context for evaluating trust policy conditions during `AssumeRole`.
///
/// Trust policies can reference `aws:PrincipalTag/*`, `sts:ExternalId`, the
/// caller identity keys `aws:PrincipalArn` / `aws:PrincipalAccount` /
/// `aws:PrincipalType` / `aws:username`, the session-tag keys
/// `aws:RequestTag/*` / `aws:TagKeys`, and the request-wide keys
/// `aws:SecureTransport`, `aws:RequestedRegion`, `aws:CurrentTime`,
/// `aws:EpochTime`, `aws:ViaAWSService` and `aws:PrincipalIsAWSService`, all of
/// which this context resolves. A global key STS populates that this context
/// cannot resolve (for example `aws:PrincipalOrgID` or `aws:SourceIp`, neither
/// on the absent-ok allowlist) is reported as unresolvable (fail closed). Keys
/// that are legitimately absent for this caller model keep absent-key
/// semantics. `DynamoDB`-specific keys are not applicable.
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
    /// The STS region (`aws:RequestedRegion`), when known.
    pub region: Option<String>,
    /// Whether the `AssumeRole` call arrived over TLS (`aws:SecureTransport`).
    pub secure_transport: bool,
    /// The calling principal's type (`aws:PrincipalType`): `User` for an IAM
    /// user, `AssumedRole` for a role session. `None` when it cannot be
    /// determined from the caller ARN (then it fails closed).
    pub principal_type: Option<String>,
    /// The calling IAM user's name (`aws:username`); `None` when the caller is
    /// not an IAM user (for example a role session), where it is absent.
    pub username: Option<String>,
    /// The caller's source IP (`aws:SourceIp`): the peer address of the
    /// `AssumeRole` connection, which is what STS records.
    pub source_ip: Option<String>,
    /// The caller's `User-Agent` header (`aws:UserAgent`) on the `AssumeRole`
    /// call.
    pub user_agent: Option<String>,
    /// The caller's principal unique id (`aws:userid`), derived from the caller
    /// ARN. `None` for a caller ARN that is neither a user nor an assumed-role
    /// session.
    pub userid: Option<String>,
    /// The account that owns the role being assumed (`aws:ResourceAccount`).
    pub resource_account: Option<String>,
    /// The request time formatted for `aws:CurrentTime` (RFC 3339 UTC).
    pub current_time: String,
    /// The request time formatted for `aws:EpochTime` (whole seconds).
    pub epoch_time: String,
}

impl ConditionContext for AssumeRoleContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            ci_tag_lookup(&self.principal_tags, tag_key)
        } else if let Some(tag_key) = key.strip_prefix("aws:RequestTag/") {
            ci_tag_lookup(&self.request_tags, tag_key)
        } else {
            match key {
                "sts:ExternalId" => self.external_id.as_deref().map(|v| vec![v]),
                "aws:PrincipalArn" => Some(vec![self.principal_arn.as_str()]),
                "aws:PrincipalAccount" => self.principal_account.as_deref().map(|v| vec![v]),
                "aws:RequestedRegion" => self.region.as_deref().map(|v| vec![v]),
                "aws:SecureTransport" => Some(vec![if self.secure_transport {
                    "true"
                } else {
                    "false"
                }]),
                "aws:CurrentTime" => Some(vec![self.current_time.as_str()]),
                "aws:EpochTime" => Some(vec![self.epoch_time.as_str()]),
                "aws:PrincipalType" => self.principal_type.as_deref().map(|v| vec![v]),
                "aws:username" => self.username.as_deref().map(|v| vec![v]),
                "aws:SourceIp" => self.source_ip.as_deref().map(|v| vec![v]),
                "aws:UserAgent" => self.user_agent.as_deref().map(|v| vec![v]),
                "aws:userid" => self.userid.as_deref().map(|v| vec![v]),
                "aws:ResourceAccount" => self.resource_account.as_deref().map(|v| vec![v]),
                "aws:ViaAWSService" => Some(vec!["false"]),
                "aws:PrincipalIsAWSService" => Some(vec!["false"]),
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
        // account, region, secure transport, time, principal type, user name)
        // never reach here. Any other unresolved aws: global key that is not on
        // the absent-ok allowlist is one STS populates but this context has no
        // value for, so a Deny gated on it fails closed and an Allow does not
        // grant.
        aws_key_is_unresolvable(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an `AssumeRoleContext` for an IAM user caller with the given tags,
    /// external id and session tags, and the request-wide keys resolved.
    fn ar_ctx(
        principal_tags: HashMap<String, String>,
        external_id: Option<String>,
        request_tags: HashMap<String, String>,
    ) -> AssumeRoleContext {
        AssumeRoleContext {
            principal_tags,
            external_id,
            principal_arn: "arn:aws:iam::123456789012:user/alice".to_owned(),
            principal_account: Some("123456789012".to_owned()),
            request_tags,
            region: Some("us-east-1".to_owned()),
            secure_transport: true,
            principal_type: Some("User".to_owned()),
            username: Some("alice".to_owned()),
            source_ip: Some("203.0.113.9".to_owned()),
            user_agent: Some("aws-sdk-rust".to_owned()),
            userid: Some("AIDAEXAMPLEUSERID1234".to_owned()),
            resource_account: Some("123456789012".to_owned()),
            current_time: "2026-01-01T00:00:00Z".to_owned(),
            epoch_time: "1767225600".to_owned(),
        }
    }

    #[test]
    fn request_context_resolves_new_round3_keys() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                source_ip: Some("203.0.113.9".to_owned()),
                user_agent: Some("Botocore/1.0".to_owned()),
                userid: Some("AIDAEXAMPLEUSERID1234".to_owned()),
                resource_account: Some("123456789012".to_owned()),
                ..Default::default()
            },
        );
        assert_eq!(ctx.resolve_key("aws:SourceIp"), Some(vec!["203.0.113.9"]));
        assert_eq!(ctx.resolve_key("aws:UserAgent"), Some(vec!["Botocore/1.0"]));
        assert_eq!(
            ctx.resolve_key("aws:userid"),
            Some(vec!["AIDAEXAMPLEUSERID1234"])
        );
        assert_eq!(
            ctx.resolve_key("aws:ResourceAccount"),
            Some(vec!["123456789012"])
        );
        // Organization keys are absent for a standalone account, not
        // unresolvable: they return None and do not fail closed.
        assert_eq!(ctx.resolve_key("aws:PrincipalOrgID"), None);
        assert!(!ctx.is_unresolvable_key("aws:PrincipalOrgID"));
        assert!(!ctx.is_unresolvable_key("aws:PrincipalOrgPaths"));
    }

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
    fn request_context_tag_key_is_case_insensitive() {
        // The service resolves the tag key case-insensitively; the value stays
        // case-sensitive. Stored `Team`, policy names `team`.
        let ctx = RequestContext::build(
            HashMap::from([("Team".to_owned(), "contractors".to_owned())]),
            HashMap::from([("Env".to_owned(), "Prod".to_owned())]),
            false,
            RequestParams::default(),
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/team"),
            Some(vec!["contractors"])
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/TEAM"),
            Some(vec!["contractors"])
        );
        assert_eq!(
            ctx.resolve_key("dynamodb:ResourceTag/env"),
            Some(vec!["Prod"])
        );
        // The value is not case-folded, only the key.
        assert_eq!(ctx.resolve_key("aws:PrincipalTag/missing"), None);
    }

    #[test]
    fn assume_role_context_tag_key_is_case_insensitive() {
        let ctx = ar_ctx(
            HashMap::from([("Team".to_owned(), "contractors".to_owned())]),
            None,
            HashMap::from([("Env".to_owned(), "prod".to_owned())]),
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/team"),
            Some(vec!["contractors"])
        );
        assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec!["prod"]));
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
        let ctx = ar_ctx(HashMap::new(), Some("ext-123".to_owned()), HashMap::new());
        assert_eq!(ctx.resolve_key("sts:ExternalId"), Some(vec!["ext-123"]));
        assert_eq!(ctx.resolve_key("dynamodb:LeadingKeys"), None);
    }

    #[test]
    fn assume_role_context_principal_arn_and_account() {
        let ctx = ar_ctx(HashMap::new(), None, HashMap::new());
        assert_eq!(
            ctx.resolve_key("aws:PrincipalArn"),
            Some(vec!["arn:aws:iam::123456789012:user/alice"])
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalAccount"),
            Some(vec!["123456789012"])
        );
        // An organization key is absent for a standalone account, not
        // unresolvable: it returns None and keeps absent-key semantics.
        assert_eq!(ctx.resolve_key("aws:PrincipalOrgID"), None);
        assert!(!ctx.is_unresolvable_key("aws:PrincipalOrgID"));
        assert!(!ctx.is_unresolvable_key("sts:ExternalId"));
        assert!(!ctx.is_unresolvable_key("aws:PrincipalTag/team"));
    }

    #[test]
    fn assume_role_context_request_tags_and_tag_keys() {
        let ctx = ar_ctx(
            HashMap::new(),
            None,
            HashMap::from([("env".to_owned(), "prod".to_owned())]),
        );
        assert_eq!(ctx.resolve_key("aws:RequestTag/env"), Some(vec!["prod"]));
        assert_eq!(ctx.resolve_key("aws:RequestTag/missing"), None);
        assert_eq!(ctx.resolve_key("aws:TagKeys"), Some(vec!["env"]));
        assert!(ctx.is_multivalued_key("aws:TagKeys"));
        // With no session tags, aws:TagKeys is genuinely absent, not
        // unresolvable.
        let empty = ar_ctx(HashMap::new(), None, HashMap::new());
        assert_eq!(empty.resolve_key("aws:TagKeys"), None);
        assert!(!empty.is_unresolvable_key("aws:TagKeys"));
    }

    fn user_ctx() -> RequestContext {
        RequestContext::build(
            HashMap::new(),
            HashMap::from([("env".to_owned(), "prod".to_owned())]),
            false,
            RequestParams {
                principal_arn: Some("arn:aws:iam::123456789012:user/alice".to_owned()),
                principal_account: Some("123456789012".to_owned()),
                region: Some("us-east-1".to_owned()),
                secure_transport: true,
                principal_type: Some("User".to_owned()),
                username: Some("alice".to_owned()),
                ..Default::default()
            },
        )
    }

    #[test]
    fn request_context_resolves_request_wide_keys() {
        let ctx = user_ctx();
        assert_eq!(
            ctx.resolve_key("aws:RequestedRegion"),
            Some(vec!["us-east-1"])
        );
        assert_eq!(ctx.resolve_key("aws:SecureTransport"), Some(vec!["true"]));
        assert_eq!(ctx.resolve_key("aws:PrincipalType"), Some(vec!["User"]));
        assert_eq!(ctx.resolve_key("aws:username"), Some(vec!["alice"]));
        assert_eq!(ctx.resolve_key("aws:ViaAWSService"), Some(vec!["false"]));
        assert_eq!(
            ctx.resolve_key("aws:PrincipalIsAWSService"),
            Some(vec!["false"])
        );
        // Time keys are populated (non-empty).
        assert!(
            ctx.resolve_key("aws:CurrentTime")
                .is_some_and(|v| !v[0].is_empty())
        );
        assert!(
            ctx.resolve_key("aws:EpochTime")
                .is_some_and(|v| !v[0].is_empty())
        );
        // aws:ResourceTag/* resolves from the resource-tag map, case-insensitive key.
        assert_eq!(ctx.resolve_key("aws:ResourceTag/env"), Some(vec!["prod"]));
        assert_eq!(ctx.resolve_key("aws:ResourceTag/ENV"), Some(vec!["prod"]));
        assert_eq!(ctx.resolve_key("aws:ResourceTag/missing"), None);
    }

    #[test]
    fn request_context_secure_transport_false_when_plaintext() {
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                secure_transport: false,
                ..Default::default()
            },
        );
        assert_eq!(ctx.resolve_key("aws:SecureTransport"), Some(vec!["false"]));
    }

    #[test]
    fn request_context_role_session_username_is_absent_not_unresolvable() {
        // A role session has no user name; the service leaves aws:username
        // absent, so it keeps absent-key semantics rather than failing closed.
        let ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                principal_type: Some("AssumedRole".to_owned()),
                username: None,
                ..Default::default()
            },
        );
        assert_eq!(
            ctx.resolve_key("aws:PrincipalType"),
            Some(vec!["AssumedRole"])
        );
        assert_eq!(ctx.resolve_key("aws:username"), None);
        assert!(!ctx.is_unresolvable_key("aws:username"));
    }

    #[test]
    fn no_documented_global_key_fails_closed_after_round3() {
        // The fail-closed set is empty. Every aws: global key is either resolved
        // by the context or kept absent (absent-key semantics), matching the
        // service; none is reported unresolvable. This includes the keys round
        // two failed closed on (the organization keys, aws:userid, aws:SourceIp,
        // aws:UserAgent), the keys the service leaves absent for a direct caller,
        // an unknown key, tag sub-keys and non-aws: keys.
        for k in [
            "aws:PrincipalOrgID",
            "aws:PrincipalOrgPaths",
            "aws:ResourceOrgID",
            "aws:ResourceOrgPaths",
            "aws:SourceOrgID",
            "aws:SourceOrgPaths",
            "aws:userid",
            "aws:SourceIp",
            "aws:UserAgent",
            "aws:ResourceAccount",
            "aws:MultiFactorAuthPresent",
            "aws:MultiFactorAuthAge",
            "aws:TokenIssueTime",
            "aws:SourceVpc",
            "aws:SourceVpce",
            "aws:VpcSourceIp",
            "aws:FederatedProvider",
            "aws:PrincipalServiceName",
            "aws:NoSuchGlobalKey",
            "aws:PrincipalTag/team",
            "aws:RequestTag/env",
            "aws:ResourceTag/env",
            "dynamodb:LeadingKeys",
            "sts:ExternalId",
        ] {
            assert!(!aws_key_is_unresolvable(k), "{k} must not fail closed");
        }
    }
}
