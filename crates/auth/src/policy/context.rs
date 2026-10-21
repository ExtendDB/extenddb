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
/// AWS resolves tag condition keys (`aws:PrincipalTag/<k>`, `aws:RequestTag/<k>`,
/// `aws:ResourceTag/<k>`, `dynamodb:ResourceTag/<k>`) without regard to the case
/// of `<k>`, under Unicode full case folding; a tag stored as `Team` is matched
/// by a policy naming `team`, and `cla\u{00df}` matches `class`. Tag values stay
/// case-sensitive. IAM forbids two tag keys that differ only in case, so a
/// case-insensitive match is unambiguous.
///
/// `folded_tag_key` is already case-folded by the caller; each stored key is
/// folded before comparison.
fn ci_tag_lookup<'a>(
    map: &'a HashMap<String, String>,
    folded_tag_key: &str,
) -> Option<Vec<&'a str>> {
    map.iter()
        .find(|(k, _)| super::casefold::case_fold(k) == folded_tag_key)
        .map(|(_, v)| vec![v.as_str()])
}

/// Strip `prefix` from the front of an already case-folded key, returning the
/// remainder (the folded tag sub-key) when it matches.
///
/// The live service resolves a condition key name without regard to case,
/// including the tag prefix (`aws:PrincipalTag/`, `aws:RequestTag/`,
/// `aws:ResourceTag/`, `dynamodb:ResourceTag/`), under Unicode full case
/// folding. The caller folds the whole key first, so `prefix` is given in its
/// folded (lower-case ASCII) form and a plain prefix match suffices. The
/// returned sub-key is the folded form, which `ci_tag_lookup` compares against
/// each folded stored key.
fn strip_folded_prefix<'a>(folded_key: &'a str, folded_prefix: &str) -> Option<&'a str> {
    folded_key.strip_prefix(folded_prefix)
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
    let folded = super::casefold::case_fold(key);
    FAIL_CLOSED_GLOBAL_KEYS
        .iter()
        .any(|k| super::casefold::case_fold(k) == folded)
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
    /// The session issue time (`aws:TokenIssueTime`), RFC 3339 UTC, set only for
    /// a role-session caller. `None` for an IAM user, where the service leaves
    /// the key absent.
    pub token_issue_time: Option<String>,
    /// Whether the caller authenticated with MFA (`aws:MultiFactorAuthPresent`).
    /// The service populates this for a role session (`false` for a session
    /// assumed without MFA) and leaves it absent for an IAM user with long-term
    /// keys. `Some(false)` for a role session, `None` for an IAM user.
    pub multi_factor_auth_present: Option<bool>,
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
    /// The session issue time (`aws:TokenIssueTime`) for a role-session caller;
    /// `None` for an IAM user (absent key).
    pub token_issue_time: Option<String>,
    /// Whether the caller authenticated with MFA (`aws:MultiFactorAuthPresent`):
    /// `Some(false)` for a role session assumed without MFA, `None` for an IAM
    /// user (absent key), matching the service.
    pub multi_factor_auth_present: Option<bool>,
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
            token_issue_time: params.token_issue_time,
            multi_factor_auth_present: params.multi_factor_auth_present,
            current_time,
            epoch_time,
        }
    }
}

impl ConditionContext for RequestContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        // Condition key names are resolved under Unicode full case folding,
        // matching the live service: `aws:securetransport`, `AWS:SecureTransport`
        // and `aws:SecureTran\u{017f}port` resolve the same, and
        // `aws:principaltag/team` the same as `aws:PrincipalTag/team`. Fold the
        // whole key once; the tag prefix is matched in its folded form and the
        // folded sub-key is compared against each folded stored tag key.
        let folded = super::casefold::case_fold(key);
        if let Some(tag_key) = strip_folded_prefix(&folded, "aws:principaltag/") {
            ci_tag_lookup(&self.principal_tags, tag_key)
        } else if let Some(tag_key) = strip_folded_prefix(&folded, "aws:resourcetag/") {
            // The service evaluates resource tags as aws:ResourceTag/*. ExtendDB
            // resolves them from the same resource-tag map it uses for
            // dynamodb:ResourceTag/*, case-folded on the key.
            ci_tag_lookup(&self.resource_tags, tag_key)
        } else if let Some(tag_key) = strip_folded_prefix(&folded, "dynamodb:resourcetag/") {
            ci_tag_lookup(&self.resource_tags, tag_key)
        } else {
            match folded.as_str() {
                "dynamodb:leadingkeys" => self
                    .leading_keys
                    .as_ref()
                    .map(|v| v.iter().map(std::string::String::as_str).collect()),
                "dynamodb:attributes" => self
                    .attributes
                    .as_ref()
                    .map(|v| v.iter().map(std::string::String::as_str).collect()),
                "dynamodb:select" => self.select.as_deref().map(|v| vec![v]),
                "dynamodb:returnvalues" => self.return_values.as_deref().map(|v| vec![v]),
                "dynamodb:returnconsumedcapacity" => {
                    self.return_consumed_capacity.as_deref().map(|v| vec![v])
                }
                "dynamodb:fulltablescan" => self
                    .full_table_scan
                    .map(|v| vec![if v { "true" } else { "false" }]),
                "dynamodb:enclosingoperation" => {
                    self.enclosing_operation.as_deref().map(|v| vec![v])
                }
                "aws:principalarn" => self.principal_arn.as_deref().map(|v| vec![v]),
                "aws:principalaccount" => self.principal_account.as_deref().map(|v| vec![v]),
                "aws:requestedregion" => self.region.as_deref().map(|v| vec![v]),
                "aws:securetransport" => Some(vec![if self.secure_transport {
                    "true"
                } else {
                    "false"
                }]),
                "aws:currenttime" => Some(vec![self.current_time.as_str()]),
                "aws:epochtime" => Some(vec![self.epoch_time.as_str()]),
                "aws:principaltype" => self.principal_type.as_deref().map(|v| vec![v]),
                "aws:username" => self.username.as_deref().map(|v| vec![v]),
                "aws:sourceip" => self.source_ip.as_deref().map(|v| vec![v]),
                "aws:useragent" => self.user_agent.as_deref().map(|v| vec![v]),
                "aws:userid" => self.userid.as_deref().map(|v| vec![v]),
                "aws:resourceaccount" => self.resource_account.as_deref().map(|v| vec![v]),
                "aws:tokenissuetime" => self.token_issue_time.as_deref().map(|v| vec![v]),
                // Populated for a role session (false for a session assumed
                // without MFA), absent for an IAM user, matching the service.
                "aws:multifactorauthpresent" => self
                    .multi_factor_auth_present
                    .map(|v| vec![if v { "true" } else { "false" }]),
                // ExtendDB is not an AWS service calling on a principal's behalf,
                // so these are always false, matching the service for a direct
                // caller.
                "aws:viaawsservice" => Some(vec!["false"]),
                "aws:principalisawsservice" => Some(vec!["false"]),
                _ => None,
            }
        }
    }

    fn is_multivalued_key(&self, key: &str) -> bool {
        // Per the AWS DynamoDB service authorization reference these keys are
        // `ArrayOfString` (multivalued); all other DynamoDB keys and tag keys are
        // single-valued. Verified against real AWS IAM (BR-7085). Key names are
        // matched under Unicode full case folding, matching the service.
        let folded = super::casefold::case_fold(key);
        matches!(
            folded.as_str(),
            "dynamodb:leadingkeys" | "dynamodb:attributes"
        )
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
        // Condition key names are resolved under Unicode full case folding,
        // matching the live service, including the tag prefixes.
        let folded = super::casefold::case_fold(key);
        if let Some(tag_key) = strip_folded_prefix(&folded, "aws:principaltag/") {
            ci_tag_lookup(&self.principal_tags, tag_key)
        } else if let Some(tag_key) = strip_folded_prefix(&folded, "aws:requesttag/") {
            ci_tag_lookup(&self.request_tags, tag_key)
        } else {
            match folded.as_str() {
                "sts:externalid" => self.external_id.as_deref().map(|v| vec![v]),
                "aws:principalarn" => Some(vec![self.principal_arn.as_str()]),
                "aws:principalaccount" => self.principal_account.as_deref().map(|v| vec![v]),
                "aws:requestedregion" => self.region.as_deref().map(|v| vec![v]),
                "aws:securetransport" => Some(vec![if self.secure_transport {
                    "true"
                } else {
                    "false"
                }]),
                "aws:currenttime" => Some(vec![self.current_time.as_str()]),
                "aws:epochtime" => Some(vec![self.epoch_time.as_str()]),
                "aws:principaltype" => self.principal_type.as_deref().map(|v| vec![v]),
                "aws:username" => self.username.as_deref().map(|v| vec![v]),
                "aws:sourceip" => self.source_ip.as_deref().map(|v| vec![v]),
                "aws:useragent" => self.user_agent.as_deref().map(|v| vec![v]),
                "aws:userid" => self.userid.as_deref().map(|v| vec![v]),
                "aws:resourceaccount" => self.resource_account.as_deref().map(|v| vec![v]),
                "aws:viaawsservice" => Some(vec!["false"]),
                "aws:principalisawsservice" => Some(vec!["false"]),
                "aws:tagkeys" => {
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
        // single-valued. Matched under Unicode full case folding, like the service.
        super::casefold::case_fold(key) == "aws:tagkeys"
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
    fn request_context_token_issue_time_present_for_role_session_absent_for_user() {
        // A role session carries aws:TokenIssueTime (the session issue time).
        let role_ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                token_issue_time: Some("2026-10-21T02:20:39Z".to_owned()),
                ..Default::default()
            },
        );
        assert_eq!(
            role_ctx.resolve_key("aws:TokenIssueTime"),
            Some(vec!["2026-10-21T02:20:39Z"])
        );
        // An IAM user leaves it absent: resolve_key returns None and the key is
        // not fail-closed (standard absent-key semantics apply).
        let user_ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams::default(),
        );
        assert_eq!(user_ctx.resolve_key("aws:TokenIssueTime"), None);
        assert!(!user_ctx.is_unresolvable_key("aws:TokenIssueTime"));
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
    fn request_context_mfa_present_for_role_session_absent_for_user() {
        // A role session carries aws:MultiFactorAuthPresent = false (assumed
        // without MFA); an IAM user leaves it absent (standard absent-key
        // semantics, not fail-closed).
        let role_ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams {
                multi_factor_auth_present: Some(false),
                ..Default::default()
            },
        );
        assert_eq!(
            role_ctx.resolve_key("aws:MultiFactorAuthPresent"),
            Some(vec!["false"])
        );
        // Case-insensitive on the key name too.
        assert_eq!(
            role_ctx.resolve_key("aws:multifactorauthpresent"),
            Some(vec!["false"])
        );
        let user_ctx = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams::default(),
        );
        assert_eq!(user_ctx.resolve_key("aws:MultiFactorAuthPresent"), None);
        assert!(!user_ctx.is_unresolvable_key("aws:MultiFactorAuthPresent"));
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
    fn request_context_key_name_is_case_insensitive() {
        // The service resolves a condition key NAME regardless of case,
        // including the `aws:` prefix and the tag prefix. A miscased key must
        // resolve to the same value as its canonical form.
        let ctx = RequestContext::build(
            HashMap::from([("Team".to_owned(), "contractors".to_owned())]),
            HashMap::from([("Env".to_owned(), "prod".to_owned())]),
            false,
            RequestParams {
                source_ip: Some("203.0.113.9".to_owned()),
                secure_transport: true,
                principal_type: Some("User".to_owned()),
                leading_keys: Some(vec!["k1".to_owned()]),
                attributes: Some(vec!["pk".to_owned()]),
                ..Default::default()
            },
        );
        // Global keys in lower case and with an upper-cased prefix.
        assert_eq!(ctx.resolve_key("aws:securetransport"), Some(vec!["true"]));
        assert_eq!(ctx.resolve_key("AWS:SecureTransport"), Some(vec!["true"]));
        assert_eq!(ctx.resolve_key("aws:principaltype"), Some(vec!["User"]));
        assert_eq!(ctx.resolve_key("aws:sourceip"), Some(vec!["203.0.113.9"]));
        // dynamodb: keys in lower case, single- and multi-valued.
        assert_eq!(ctx.resolve_key("dynamodb:leadingkeys"), Some(vec!["k1"]));
        assert_eq!(ctx.resolve_key("dynamodb:attributes"), Some(vec!["pk"]));
        assert!(ctx.is_multivalued_key("dynamodb:leadingkeys"));
        assert!(ctx.is_multivalued_key("DYNAMODB:Attributes"));
        // Tag prefixes in lower/upper case; the sub-key case is already folded.
        assert_eq!(
            ctx.resolve_key("aws:principaltag/team"),
            Some(vec!["contractors"])
        );
        assert_eq!(ctx.resolve_key("aws:resourcetag/ENV"), Some(vec!["prod"]));
        assert_eq!(
            ctx.resolve_key("dynamodb:resourcetag/env"),
            Some(vec!["prod"])
        );
    }

    #[test]
    fn request_context_key_name_unicode_case_folds() {
        // The service resolves a key name under Unicode full case folding.
        // Long s and Kelvin and the st ligature and sharp s fold to their ASCII
        // twins; the dotless and dotted i do NOT fold to i, matching the service
        // (measured).
        let ctx = RequestContext::build(
            HashMap::from([("class".to_owned(), "x".to_owned())]),
            HashMap::new(),
            false,
            RequestParams {
                secure_transport: true,
                principal_type: Some("User".to_owned()),
                region: Some("us-east-1".to_owned()),
                leading_keys: Some(vec!["k1".to_owned()]),
                ..Default::default()
            },
        );
        // long s (U+017F) in aws:SecureTran[s]port.
        assert_eq!(
            ctx.resolve_key("aws:SecureTran\u{017f}port"),
            Some(vec!["true"])
        );
        // st ligature (U+FB05) in aws:Reque[st]edRegion.
        assert_eq!(
            ctx.resolve_key("aws:Reque\u{fb05}edRegion"),
            Some(vec!["us-east-1"])
        );
        // Kelvin sign (U+212A) in dynamodb:Leading[k]eys.
        assert!(ctx.is_multivalued_key("dynamodb:Leading\u{212a}eys"));
        assert_eq!(
            ctx.resolve_key("dynamodb:Leading\u{212a}eys"),
            Some(vec!["k1"])
        );
        // sharp s (U+00DF) folds to ss in a tag key: cla[ß] matches stored class.
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/cla\u{00df}"),
            Some(vec!["x"])
        );
        // dotless i (U+0131) does NOT fold to i: the key reads absent.
        assert_eq!(ctx.resolve_key("aws:Pr\u{0131}ncipalType"), None);
        // dotted I (U+0130) does NOT fold to i either.
        assert_eq!(ctx.resolve_key("aws:Pr\u{0130}ncipalType"), None);
    }

    #[test]
    fn assume_role_context_key_name_unicode_case_folds() {
        let ctx = ar_ctx(
            HashMap::from([("class".to_owned(), "x".to_owned())]),
            Some("ext-123".to_owned()),
            HashMap::new(),
        );
        // sts:External[s->ſ]... long s in externalId: sts:External[ı]d would NOT
        // fold; use the st-ligature-free long s in a resolvable spot.
        assert_eq!(
            ctx.resolve_key("sts:External\u{0131}d"),
            None,
            "dotless i must not fold to i"
        );
        // Tag key folds: cla[ß] -> class.
        assert_eq!(
            ctx.resolve_key("aws:PrincipalTag/cla\u{00df}"),
            Some(vec!["x"])
        );
    }

    #[test]
    fn assume_role_context_key_name_is_case_insensitive() {
        let ctx = ar_ctx(
            HashMap::from([("Team".to_owned(), "contractors".to_owned())]),
            Some("ext-123".to_owned()),
            HashMap::from([("env".to_owned(), "prod".to_owned())]),
        );
        assert_eq!(ctx.resolve_key("sts:externalid"), Some(vec!["ext-123"]));
        assert_eq!(ctx.resolve_key("STS:ExternalId"), Some(vec!["ext-123"]));
        assert_eq!(
            ctx.resolve_key("aws:principalarn"),
            Some(vec!["arn:aws:iam::123456789012:user/alice"])
        );
        assert_eq!(ctx.resolve_key("aws:requesttag/env"), Some(vec!["prod"]));
        assert_eq!(ctx.resolve_key("aws:tagkeys"), Some(vec!["env"]));
        assert!(ctx.is_multivalued_key("aws:tagkeys"));
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
