// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the (IAM action, resource ARN) pairs that a `DynamoDB` request
//! is authorized against, matching live `DynamoDB`.
//!
//! The defect this fixes: `authorize_request` derived the
//! authorization resource only from a top-level `TableName`. Operations that
//! carry their resource in another field authorized against
//! `arn:aws:dynamodb:<region>:<account>:table/*`, so an explicit Deny on a
//! specific table, index, stream, or backup ARN never matched. In ABAC
//! deployments this let a caller change the tags that drive access decisions.
//!
//! The service evaluates these resources (verified against live DynamoDB):
//!
//! | Operation | IAM action | resource |
//! |---|---|---|
//! | `TagResource`/`UntagResource`/`ListTagsOfResource` | same | the `ResourceArn` (a table or stream ARN) |
//! | `DescribeStream`/`GetShardIterator` | same | the `StreamArn` |
//! | `ListStreams` (with `TableName`) | `ListStreams` | `table/<T>/stream/*` |
//! | `ListStreams` (no `TableName`) | `ListStreams` | `table/*/stream/*` |
//! | `DescribeBackup`/`DeleteBackup` | same | the `BackupArn` |
//! | `ListBackups` (with `TableName`) | `ListBackups` | `table/<T>/backup/*` |
//! | `ListBackups` (no `TableName`) | `ListBackups` | `table/*/backup/*` |
//! | `CreateBackup`/`DescribeContinuousBackups`/`UpdateContinuousBackups` | same | the table ARN |
//! | `ExportTableToPointInTime` | same | the `TableArn` |
//! | `ImportTable` | same | the new table ARN |
//! | `Query`/`Scan` (with `IndexName`) | same | `table/<T>/index/<I>` |
//! | `ListTables` | `ListTables` | `table/*` |
//! | item/table ops | same | the table ARN (from `TableName`, name or ARN) |
//!
//! Every resource ARN produced here, except a tag operation's stream ARN, is
//! CANONICAL for this deployment: the partition, service, region, and account
//! are fixed to this server's, and only the resource identity (table name and
//! optional sub-resource) is taken from the request. ExtendDB's stream and
//! backup resolvers ignore the partition, service, region, and account of a
//! supplied ARN and act within this deployment's account, so a caller who
//! respells those fields would otherwise evade a Deny written against the
//! canonical ARN. Canonicalizing here closes that bypass: a Deny on the real
//! resource matches every canonical ARN. The tag operations are the one
//! exception: their policies are matched against the `ResourceArn` as written,
//! as the service matches them, and a stream ARN there is kept as written,
//! because the engine tag handler refuses another partition, service or region
//! after the IAM decision, before any tag row is read or written (see
//! [`AuthzTarget`]). A resource in another account is refused before the IAM
//! decision for every operation ([`cross_account`]).
//!
//! A stream ARN is held to a stricter rule than canonicalization (issues #450
//! and #451; measured live 2026-10-23, 2026-10-28 and 2026-11-05, tabled in
//! the module docstring of `tests/test_stream_arn_scope.py`). On `StreamArn`
//! (DescribeStream, GetShardIterator) these checks run before the IAM
//! decision, in the service's order:
//!
//! 1. a value that is not a stream ARN (a table, index or backup ARN
//!    included), or has an empty partition, service or account field, is
//!    `Invalid StreamArn` in any account ([`validate_stream_arn_kind`]);
//! 2. a stream ARN in another account gets the cross-account answer
//!    ([`cross_account`]): "Access is denied" for an account the catalog does
//!    not hold, whatever its partition, service and region are, and for an
//!    account the catalog holds, step 3 and then the resource-policy denial;
//! 3. a partition other than `aws`, a service other than `dynamodb`, or a
//!    region other than this server's, an empty region included, is
//!    `Invalid StreamArn` ([`check_stream_arn_scope`]).
//!
//! A stream ARN in a tag operation's `ResourceArn` gets the cross-account
//! answer when it names another account. In the caller's account the IAM
//! decision runs on the ARN as sent, and the engine refuses another
//! partition, service or region afterwards with `Invalid StreamArn`. A
//! `GetRecords` iterator whose stream ARN is out of scope in any field, the
//! account included, is refused before the IAM decision with
//! ValidationException `Invalid ShardIterator` ([`getrecords_resource`]);
//! GetShardIterator issues no such iterator. So a respelled stream ARN is never
//! served as the caller's own stream, and a stream ARN that reaches the engine
//! spells the stream the engine reads.
//!
//! Fail-closed: a missing or unparseable resource field never falls back to
//! `table/*`. It yields the field value verbatim (often empty), which an
//! `Allow` on `*` still reaches so the engine returns its normal validation
//! error, while no Deny on a specific resource is silently skipped.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

pub(crate) mod cross_account;

/// Partition and service are fixed to this deployment's; see module docs.
const PARTITION: &str = "aws";
const SERVICE: &str = "dynamodb";

/// Scope checks for stream ARNs (issues #450 and #451).
mod stream_scope;
use stream_scope::{check_stream_arn_kind, check_stream_arn_scope, stream_arn_in_scope};

/// The item actions the service requires on the target table of a restore, in
/// addition to the restore action itself (measured live). A Deny on any of
/// these for the target table name blocks a restore into it.
/// `RestoreTableToPointInTime` checks them, Scan first, before the restore
/// action: a caller with no policy is denied on `dynamodb:Scan` on the target
/// table, as on the service (measured 2026-10-22, issue #446). The server
/// refuses every `RestoreTableToPointInTime` before this code runs.
const RESTORE_TARGET_ACTIONS: &[&str] = &[
    "Scan",
    "Query",
    "GetItem",
    "PutItem",
    "UpdateItem",
    "DeleteItem",
    "BatchWriteItem",
];

/// One (IAM action, resource) pair a request is authorized against.
///
/// `resource` is the canonical ARN for this deployment, except a tag
/// operation's stream ARN, which is kept as written: a stream carries no
/// resource tags for authorization ([`resource_tag_lookup_arn`]), and a tag
/// operation's policies are matched against `supplied`, which holds the same
/// ARN. Resource tags are read under `resource`, and the policies are matched
/// against it unless `match_supplied` is set, so a Deny on the real resource
/// matches however the caller spelled the partition, service or region.
/// `supplied` is the resource as the caller wrote it, and is what a denial
/// message names: the service echoes the caller's ARN with its partition,
/// service and region intact (measured 2026-10-20 with TagResource on an
/// `aws-cn` partition, an `s3` service and a `us-west-2` region). It equals
/// `resource` when the request named the resource by a bare name or by its
/// canonical ARN.
///
/// `match_supplied` is set on the tag operations only. The service matches
/// their policies against the `ResourceArn` as written: a caller allowed
/// `dynamodb:*` on the canonical table ARN alone is denied ListTagsOfResource
/// on the `aws-cn`, `s3` and `us-west-2` spellings of it (measured
/// 2026-11-03). The engine tag handler refuses those spellings after the IAM
/// decision, so no tag is read or written for a resource other than the one
/// the policies matched. The other ARN fields keep the canonical match: the
/// engine resolves an export `TableArn` or a restore `SourceTableArn` with
/// another partition, service or region, and a `BackupArn` or `CreateBackup`
/// table ARN in another region, to this deployment's resource, so a Deny on
/// that resource has to match the respelled ARN. A `StreamArn` with another
/// partition, service or region is refused before the IAM decision
/// ([`validate_operation_arns`]), so its policy match only sees the canonical
/// spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzTarget {
    /// The bare operation form; `check_authorization` prepends `dynamodb:`.
    pub action: String,
    pub resource: String,
    pub supplied: String,
    pub match_supplied: bool,
}

impl AuthzTarget {
    /// A target named by a bare name or built by the server: the denial names
    /// the canonical ARN.
    pub(crate) fn new(action: &str, resource: String) -> Self {
        Self {
            action: action.to_owned(),
            supplied: resource.clone(),
            resource,
            match_supplied: false,
        }
    }

    /// A target derived from request field `field`: when the field parses as
    /// a `DynamoDB` resource ARN, the denial names it as written; otherwise
    /// it names `resource`.
    fn from_field(action: &str, resource: String, field: &str) -> Self {
        let supplied = if extenddb_core::arn::parse(field).is_some() {
            field.to_owned()
        } else {
            resource.clone()
        };
        Self {
            action: action.to_owned(),
            resource,
            supplied,
            match_supplied: false,
        }
    }

    /// The ARN the policies are matched against.
    pub(crate) fn policy_resource(&self) -> &str {
        if self.match_supplied {
            &self.supplied
        } else {
            &self.resource
        }
    }
}

/// A `DynamoDB` resource identity: a table and an optional sub-resource.
#[derive(Debug, PartialEq, Eq)]
enum Sub {
    Index(String),
    Stream(String),
    Backup(String),
}

#[derive(Debug, PartialEq, Eq)]
struct ResourceId {
    table: String,
    sub: Option<Sub>,
}

/// Extract the resource identity (table name and optional sub-resource) from a
/// `DynamoDB` ARN, through the one shared parser ([`extenddb_core::arn::parse`]).
/// Returns `None` for anything that is not a `table/...` resource. The
/// partition, service, region, and account are intentionally ignored here: only
/// the resource identity is trusted from the input, and authorization
/// canonicalizes those fields to this deployment.
fn parse_resource_identity(arn: &str) -> Option<ResourceId> {
    let r = extenddb_core::arn::parse(arn)?;
    let sub = r.sub.map(|s| match s {
        extenddb_core::arn::SubResource::Index(i) => Sub::Index(i),
        extenddb_core::arn::SubResource::Stream(s) => Sub::Stream(s),
        extenddb_core::arn::SubResource::Backup(b) => Sub::Backup(b),
    });
    Some(ResourceId {
        table: r.table,
        sub,
    })
}

/// Split an ARN into its five fields through the one shared parser. Returns
/// `None` when the string is not shaped like an ARN with five colon-separated
/// fields after the `arn:` prefix.
fn arn_fields(arn: &str) -> Option<extenddb_core::arn::ArnFields<'_>> {
    extenddb_core::arn::fields(arn)
}

/// Build a canonical ARN for this deployment from a resource identity.
fn canonical(region: &str, account_id: &str, rid: &ResourceId) -> String {
    let base = format!(
        "arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/{}",
        rid.table
    );
    match &rid.sub {
        None => base,
        Some(Sub::Index(i)) => format!("{base}/index/{i}"),
        Some(Sub::Stream(s)) => format!("{base}/stream/{s}"),
        Some(Sub::Backup(b)) => extenddb_core::backup_format::format_backup_arn(
            PARTITION, region, account_id, &rid.table, b,
        ),
    }
}

/// The canonical ARN for a resource field that [`validate_operation_arns`] has
/// already confirmed parses to the expected kind before authorization. The
/// `unwrap_or_else` raw branch is a fail-closed default that the
/// pre-authorization validation makes unreachable in request handling: it never
/// produces a `table/*` wildcard a specific Deny would miss, and an input that
/// does not parse is refused with ValidationException before this is reached.
fn canonical_resource(arn: &str, region: &str, account_id: &str) -> String {
    match parse_resource_identity(arn) {
        Some(rid) => canonical(region, account_id, &rid),
        None => arn.to_owned(),
    }
}

/// A `BackupArn` with the surrounding ASCII whitespace the engine trims
/// before it resolves the backup (`check_backup_arn`) trimmed here too, so
/// authorization evaluates the backup the engine acts on. Without this, a Deny
/// on a backup ARN would be bypassed by sending the ARN with a trailing
/// newline: authorization would evaluate `.../backup/<id>\n` and the engine
/// act on `.../backup/<id>`.
fn backup_arn_field(input: &Value) -> String {
    str_field(input, "BackupArn")
        .map(|arn| extenddb_core::backup_format::trim_backup_arn(&arn).to_owned())
        .unwrap_or_default()
}

/// The resource ARN a tag operation authorizes against.
///
/// The cross-account refusal and `validate_tag_resource_arn_shape` have
/// already run: the value is a table or stream ARN in the caller's account, or
/// a foreign-service ARN (a resource in another account, an index, a backup or
/// a non-table `DynamoDB` ARN is refused before authorization). A table ARN is
/// canonicalized to this deployment, where its resource tags are read. A
/// stream ARN is kept as sent: it carries no resource tags for authorization,
/// and the policies are matched against the ARN as sent (`match_supplied`), as
/// the service evaluates the spelling the caller gave (a no-permissions
/// principal's denial names `arn:aws-cn:...` as sent, and a principal allowed
/// the exact canonical ARN is denied the `aws-cn` spelling; measured
/// 2026-10-23, module docstring of `tests/test_stream_arn_scope.py`). The
/// engine refuses another partition, service or region after authorization
/// with `Invalid StreamArn`. A foreign-service ARN is left verbatim so
/// authorization denies it, matching the service (measured, arn-1 table).
fn tag_resource_arn(arn: &str, region: &str, account_id: &str) -> String {
    match parse_resource_identity(arn) {
        Some(rid @ ResourceId { sub: None, .. }) => canonical(region, account_id, &rid),
        // A stream ARN (authorized as sent), a foreign-service ARN, or,
        // defensively, an index/backup the shape check already rejected.
        _ => arn.to_owned(),
    }
}

/// Messages DynamoDB returns, before the IAM decision, for a request field
/// whose ARN is not a valid resource ARN of the kind the field requires
/// (measured live, `live-r2/arn-kinds-live.json`).
const INVALID_STREAM_ARN_MESSAGE: &str = "Invalid StreamArn";
const INVALID_BACKUP_ARN_MESSAGE: &str = "Invalid Backup ARN";
const INVALID_EXPORT_TABLE_ARN_MESSAGE: &str =
    "One or more parameter values were invalid: tableArn is not a valid ARN";

/// True when `arn` is a well-formed DynamoDB backup ARN: it parses to a backup
/// and its partition, service, region, and account are all non-empty. A
/// `StreamArn` takes [`check_stream_arn_scope`] instead.
fn arn_is_backup(arn: &str) -> bool {
    let Some(f) = extenddb_core::arn::fields(arn) else {
        return false;
    };
    if f.partition.is_empty() || f.service.is_empty() || f.region.is_empty() || f.account.is_empty()
    {
        return false;
    }
    matches!(
        parse_resource_identity(arn),
        Some(ResourceId {
            sub: Some(Sub::Backup(_)),
            ..
        })
    )
}

/// Validate ARN-bearing fields before the IAM decision, matching the service
/// (measured live, `live-r2/arn-kinds-live.json`; for `StreamArn`, the tables
/// in the module docstring of `tests/test_stream_arn_scope.py`, measured
/// 2026-10-23, 2026-10-28 and 2026-11-05).
///
/// The two field families differ, as the service does:
///
/// - `StreamArn` (DescribeStream, GetShardIterator) and `BackupArn`
///   (DescribeBackup, DeleteBackup) are validated strictly: a value that is not
///   a well-formed stream (resp. backup) ARN of the correct kind, including a
///   table ARN, an `xrn:` ARN, or an empty partition, is refused before
///   authorization with `Invalid StreamArn` (resp. `Invalid Backup ARN`). A
///   stream ARN is checked by [`check_stream_arn_scope`]: first its kind and
///   its non-empty partition, service and account fields (the check that
///   [`validate_stream_arn_kind`] also runs before the cross-account
///   refusal), then the partition, service and region values (another value,
///   or an empty region, is `Invalid StreamArn`). The account is not
///   compared: a stream ARN in another account is answered by
///   [`cross_account`], which runs this check for an account the catalog
///   holds.
/// - An export `TableArn` is validated loosely: only an UNPARSEABLE value (a
///   repeated `table/` segment such as `.../table/a/stream/table/b`, a bare
///   `table/x`, or an `xrn:` ARN) is refused before authorization, with
///   `tableArn is not a valid ARN`. A well-formed DynamoDB ARN of the wrong
///   kind (a real stream or index ARN) is NOT refused here: it is authorized
///   against its canonical form (preserving its kind, so authorization and the
///   engine agree on the resource) and the export handler then refuses it with
///   `Invalid Request: Table ARN is invalid.`
///
/// `RestoreTableFromBackup` and `RestoreTableToPointInTime` carry no pre-authz
/// ARN check: a wrong-kind source is authorized first (so a no-permissions
/// principal is denied) and the engine refuses it afterwards. The server
/// refuses every `RestoreTableToPointInTime` before this code runs. Tag operations
/// keep their own measured shape check (`validate_tag_resource_arn_shape`);
/// `GetRecords` is handled through its iterator by [`getrecords_resource`].
pub(crate) fn validate_operation_arns(
    operation: &str,
    input: &Value,
    region: &str,
) -> Result<(), DynamoDbError> {
    let present = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    match operation {
        "DescribeStream" | "GetShardIterator" => {
            if let Some(arn) = present("StreamArn") {
                check_stream_arn_scope(arn, region)?;
            }
        }
        "DescribeBackup" | "DeleteBackup" => {
            if let Some(arn) = present("BackupArn")
                && !arn_is_backup(extenddb_core::backup_format::trim_backup_arn(arn))
            {
                return Err(DynamoDbError::ValidationException(
                    INVALID_BACKUP_ARN_MESSAGE.to_owned(),
                ));
            }
        }
        // Export: refuse only an unparseable TableArn here. A well-formed ARN of
        // the wrong kind goes through authorization against its canonical form
        // and is refused by the export handler, so authorization never runs
        // against a different table than the engine would read.
        "ExportTableToPointInTime" => {
            if let Some(arn) = present("TableArn")
                && extenddb_core::arn::parse(arn).is_none()
            {
                return Err(DynamoDbError::ValidationException(
                    INVALID_EXPORT_TABLE_ARN_MESSAGE.to_owned(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

/// Refuse a `StreamArn` (DescribeStream, GetShardIterator) that is not a
/// stream ARN, a table, index or backup ARN included, or that has an empty
/// partition, service or account field, with ValidationException
/// `Invalid StreamArn` ([`check_stream_arn_kind`]).
///
/// The service gives this answer before it looks at the account, to every
/// principal, in the caller's account, in an account that exists and in one
/// that does not (measured 2026-11-05 with a table, index and backup ARN, and
/// 2026-10-28 and 2026-11-05 with an empty partition or service field). So
/// `authorize_request` runs this before the cross-account refusal
/// ([`cross_account`]). An absent or empty field is left to the normal path.
pub(crate) fn validate_stream_arn_kind(
    operation: &str,
    input: &Value,
) -> Result<(), DynamoDbError> {
    if !matches!(operation, "DescribeStream" | "GetShardIterator") {
        return Ok(());
    }
    match input.get("StreamArn").and_then(Value::as_str) {
        Some(arn) if !arn.is_empty() => check_stream_arn_kind(arn),
        _ => Ok(()),
    }
}

/// The ARN to look resource tags up under for authorization, or `None` when
/// the authorized resource carries no tags, so `aws:ResourceTag/*` is absent.
///
/// Measured against DynamoDB:
///
/// | Authorized resource | Tags the request sees |
/// |---|---|
/// | Table ARN | the table's tags |
/// | Index ARN | the base table's tags (indexes inherit them) |
/// | Stream ARN | none, even when the stream or its table is tagged |
/// | Backup ARN | none, even when the source table is tagged |
///
/// A stream holds a tag set of its own (`TagResource` on the stream ARN), but
/// the service does not evaluate it, or the table's, for an operation on the
/// stream: `StringEquals aws:ResourceTag/k` does not match and `Null` is true.
/// Every other resource (an export, a foreign-service ARN, `*`) also gets no
/// tags; nothing taggable is stored under one. A table wildcard such as
/// `table/*` (ListTables) returns `Some`, and reads no tags because
/// `CachedAuthzStore::fetch_resource_tags` short-circuits a `/*` ARN.
pub(crate) fn resource_tag_lookup_arn(resource_arn: &str) -> Option<String> {
    let f = extenddb_core::arn::fields(resource_arn)?;
    match parse_resource_identity(resource_arn)? {
        ResourceId { sub: None, .. } => Some(resource_arn.to_owned()),
        ResourceId {
            table,
            sub: Some(Sub::Index(_)),
        } => Some(format!(
            "arn:{}:{}:{}:{}:table/{table}",
            f.partition, f.service, f.region, f.account
        )),
        ResourceId {
            sub: Some(Sub::Stream(_) | Sub::Backup(_)),
            ..
        } => None,
    }
}

/// How `GetRecords` must answer its shard iterator, with the stream resource
/// already canonicalized for authorization.
pub(crate) enum GetRecordsResource {
    /// Authorize against this canonical stream ARN.
    Stream(String),
    /// A pre-upgrade iterator: answer ExpiredIteratorException before authz.
    Expired,
    /// Answer "Invalid ShardIterator" ValidationException before authz.
    Invalid,
}

/// Resolve the stream resource a `GetRecords` request authorizes against from
/// its shard iterator, classifying a pre-upgrade or undecodable token.
///
/// The iterator is decoded once by the engine
/// ([`extenddb_engine::classify_shard_iterator`]). A current token's stream ARN
/// must parse to a stream and name this deployment's partition, service and
/// region and the caller's account, or the token is refused as Invalid rather
/// than canonicalized and served as the caller's own stream. GetShardIterator
/// encodes the `StreamArn` it is sent and refuses one outside that scope, so
/// every iterator issued from the canonical stream ARN passes. An iterator an
/// earlier build issued from a respelled `StreamArn` (`aws-cn`, `US-EAST-1`,
/// `DYNAMODB`), every iterator chained from it, and every iterator issued
/// before a change of `server.region` are refused as Invalid. The service
/// answers a token it did not issue with `Invalid ShardIterator` (measured
/// 2026-10-22, `live-r2/iter-live.json`).
pub(crate) fn getrecords_resource(
    input: &Value,
    region: &str,
    account_id: &str,
) -> GetRecordsResource {
    let token = input
        .get("ShardIterator")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match extenddb_engine::classify_shard_iterator(token) {
        extenddb_engine::ShardIteratorClass::Stream(arn) => match parse_resource_identity(&arn) {
            Some(
                rid @ ResourceId {
                    sub: Some(Sub::Stream(_)),
                    ..
                },
            ) if stream_arn_in_scope(&arn, region, account_id) => {
                GetRecordsResource::Stream(canonical(region, account_id, &rid))
            }
            _ => GetRecordsResource::Invalid,
        },
        extenddb_engine::ShardIteratorClass::Expired => GetRecordsResource::Expired,
        extenddb_engine::ShardIteratorClass::Invalid => GetRecordsResource::Invalid,
    }
}

/// The canonical table ARN for a `TableName` field that may be a bare name or a
/// table ARN. Any sub-resource on a supplied ARN is dropped: a `TableName` names
/// a table. Callers that need an index ARN append `/index/<name>` themselves.
pub(crate) fn table_resource_arn(value: &str, region: &str, account_id: &str) -> String {
    if value.starts_with("arn:") {
        if let Some(rid) = parse_resource_identity(value) {
            return canonical(
                region,
                account_id,
                &ResourceId {
                    table: rid.table,
                    sub: None,
                },
            );
        }
        return value.to_owned();
    }
    format!("arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/{value}")
}

/// The target for `action` on a table named by `value`, a bare name or a table
/// ARN (a `RequestItems` key, a `TransactItems` table): the canonical table
/// ARN for the policy match, the value as written for the denial.
pub(crate) fn table_target(
    action: &str,
    value: &str,
    region: &str,
    account_id: &str,
) -> AuthzTarget {
    AuthzTarget::from_field(action, table_resource_arn(value, region, account_id), value)
}

/// Account-wide table wildcard, for operations with no specific resource.
pub(crate) fn table_wildcard(region: &str, account_id: &str) -> String {
    format!("arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/*")
}

fn stream_wildcard(region: &str, account_id: &str) -> String {
    format!("arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/*/stream/*")
}

fn backup_wildcard(region: &str, account_id: &str) -> String {
    format!("arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/*/backup/*")
}

/// Validate the shape of a tag operation's `ResourceArn` before authorization,
/// as the service does (measured live). The service returns ValidationException
/// to every principal, before the IAM decision, for an ARN that does not start
/// with `arn:`, a DynamoDB ARN whose resource is not a table or stream, or an
/// index ARN (tags are accepted only on a table or stream). A table or stream
/// ARN passes; so does a foreign-service ARN (for example an S3 bucket), which
/// authorization then denies. The partition, service, region, and existence of
/// a table or stream ARN are checked after authorization by the engine
/// handler, so for those a no-permissions principal is denied first and only an
/// authorized principal sees the shape error: the service runs the IAM
/// decision on a stream ARN as sent and refuses `aws-cn`, `DYNAMODB` or
/// `us-west-2` afterwards with `Invalid StreamArn` (measured 2026-10-23, module
/// docstring of `tests/test_stream_arn_scope.py`). A resource in another
/// account gets the cross-account answer instead ([`cross_account`]), which
/// runs this check first for an account the catalog holds.
pub(crate) fn validate_tag_resource_arn_shape(arn: &str) -> Result<(), DynamoDbError> {
    if !arn.starts_with("arn:") {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: ARNs must start with 'arn:': {arn}"
        )));
    }
    match parse_resource_identity(arn) {
        Some(ResourceId {
            sub: None | Some(Sub::Stream(_)),
            ..
        }) => Ok(()),
        // A backup ARN is not a taggable resource kind (measured live, with a
        // real backup ARN and a short backup id).
        Some(ResourceId {
            sub: Some(Sub::Backup(_)),
            ..
        }) => Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Provided Arn is not a \
             DynamoDB resource arn: {arn}"
        ))),
        Some(_) => Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Invalid resource arn \
             provided, only table or stream is accepted. Provided resource arn: {arn}"
        ))),
        None => {
            // Not a table/... resource. A DynamoDB-service ARN is reported as
            // not a DynamoDB resource; a foreign-service ARN is left to
            // authorization, which denies it (measured live).
            if arn_fields(arn).map(|f| f.service) == Some(SERVICE) {
                Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Provided Arn is not a \
                     DynamoDB resource arn: {arn}"
                )))
            } else {
                Ok(())
            }
        }
    }
}

/// A string request field, owned, or `None` when absent or not a string.
fn str_field(input: &Value, key: &str) -> Option<String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

/// Derive the [`AuthzTarget`]s to authorize for a non-batch, non-transact
/// `DynamoDB` operation. Most operations map to a single target;
/// multi-permission operations return more than one.
///
/// This function never returns an empty list: the generic single-table path is
/// the fallback for operations not named explicitly.
pub(crate) fn authz_targets(
    operation: &str,
    input: &Value,
    region: &str,
    account_id: &str,
) -> Vec<AuthzTarget> {
    // A target built by the server (a wildcard, a table named by a bare name
    // or a sub-resource of one): the denial names the canonical ARN.
    let one = |resource: String| vec![AuthzTarget::new(operation, resource)];
    // A target derived from a request field that may hold an ARN: the denial
    // names the field as written.
    let from =
        |resource: String, field: &str| vec![AuthzTarget::from_field(operation, resource, field)];
    let table_name = str_field(input, "TableName").unwrap_or_default();

    match operation {
        // Resource lives in ResourceArn (a table or stream ARN; a foreign ARN
        // passes the shape check and is denied by authorization). The policies
        // are matched against the ARN as written and the resource tags are
        // read from the canonical table (see `AuthzTarget`).
        "TagResource" | "UntagResource" | "ListTagsOfResource" => {
            let arn = str_field(input, "ResourceArn").unwrap_or_default();
            let mut target = AuthzTarget::from_field(
                operation,
                tag_resource_arn(&arn, region, account_id),
                &arn,
            );
            target.match_supplied = true;
            vec![target]
        }
        // Resource lives in StreamArn, validated to a stream before authz.
        "DescribeStream" | "GetShardIterator" => {
            let arn = str_field(input, "StreamArn").unwrap_or_default();
            from(canonical_resource(&arn, region, account_id), &arn)
        }
        // GetRecords carries only an opaque shard iterator, not the stream in a
        // body field, so it is handled in `authorize_request`: the iterator is
        // decoded for the stream ARN, and an iterator that names no stream gets
        // ValidationException "Invalid ShardIterator". `authz_targets` is never
        // called for it; the account stream space is mapped here only so the
        // table covers every operation.
        "GetRecords" => one(stream_wildcard(region, account_id)),
        "ListStreams" => {
            let res = match str_field(input, "TableName") {
                Some(t) => {
                    // TableName here is a bare name; build table/<T>/stream/*.
                    let rid = ResourceId {
                        table: table_name_only(&t),
                        sub: Some(Sub::Stream("*".to_owned())),
                    };
                    canonical(region, account_id, &rid)
                }
                None => stream_wildcard(region, account_id),
            };
            one(res)
        }
        // Resource lives in BackupArn, validated to a backup before authz.
        "DescribeBackup" | "DeleteBackup" => {
            let arn = backup_arn_field(input);
            from(canonical_resource(&arn, region, account_id), &arn)
        }
        // Resource is the table named by TableName.
        "CreateBackup" | "DescribeContinuousBackups" | "UpdateContinuousBackups" => from(
            table_resource_arn(&table_name, region, account_id),
            &table_name,
        ),
        // ListBackups is scoped to a table's backups when TableName is given,
        // else to every table's backups. The service evaluates
        // table/<T>/backup/* and table/*/backup/* (measured live), the same
        // sub-resource shape as ListStreams, not the bare table or table/*.
        "ListBackups" => {
            let res = match str_field(input, "TableName") {
                Some(t) => {
                    let rid = ResourceId {
                        table: table_name_only(&t),
                        sub: Some(Sub::Backup("*".to_owned())),
                    };
                    canonical(region, account_id, &rid)
                }
                None => backup_wildcard(region, account_id),
            };
            one(res)
        }
        // Export names its source in TableArn. Authorize against the canonical
        // form of the ARN as given, preserving its kind: a well-formed stream or
        // index ARN is authorized against that stream/index ARN (the service
        // does the same, measured live), and the export handler then refuses a
        // non-table ARN. Unparseable TableArns were already refused before
        // authorization by `validate_operation_arns`.
        "ExportTableToPointInTime" => {
            let arn = str_field(input, "TableArn").unwrap_or_default();
            from(canonical_resource(&arn, region, account_id), &arn)
        }
        // Import creates a table; the resource is the new table. The service
        // reads `TableCreationParameters.TableName` as a name even when it is
        // an ARN, and authorizes `table/<value as written>` in the caller's
        // account (measured 2026-11-03); the engine creates the table under
        // the value as written too.
        "ImportTable" => {
            let t = input
                .get("TableCreationParameters")
                .and_then(|p| p.get("TableName"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            one(format!(
                "arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/{t}"
            ))
        }
        // RestoreTableFromBackup requires dynamodb:RestoreTableFromBackup on the
        // source backup ARN and the item actions on the target table it writes
        // (measured live). A Deny on the backup ARN, or on any item action for
        // the target table, blocks the restore.
        "RestoreTableFromBackup" => {
            let arn = backup_arn_field(input);
            let mut v = from(canonical_resource(&arn, region, account_id), &arn);
            v.extend(restore_target_actions(input, region, account_id));
            v
        }
        // RestoreTableToPointInTime requires the item actions on the target
        // table it writes and dynamodb:RestoreTableToPointInTime on the source
        // table (measured live). The service checks the target's item actions
        // first: a caller with no policy is denied on dynamodb:Scan on the
        // target table, not on the restore action on the source (measured
        // 2026-10-22 with an index SourceTableArn). The server refuses every
        // RestoreTableToPointInTime before this code runs.
        "RestoreTableToPointInTime" => {
            let source = str_field(input, "SourceTableArn")
                .or_else(|| str_field(input, "SourceTableName"))
                .unwrap_or_default();
            let restore = from(table_resource_arn(&source, region, account_id), &source);
            let mut v = restore_target_actions(input, region, account_id);
            v.extend(restore);
            v
        }
        // Query and Scan on an index authorize the index ARN. With a table ARN
        // in TableName the denial names that ARN with the index appended; any
        // other ARN in TableName (an index ARN, a malformed resource) names
        // the canonical index ARN, so the denial never names a resource the
        // caller did not write.
        "Query" | "Scan" => {
            let base = table_resource_arn(&table_name, region, account_id);
            match str_field(input, "IndexName") {
                Some(idx) if !idx.is_empty() => {
                    let resource = format!("{base}/index/{idx}");
                    let names_a_table =
                        extenddb_core::arn::parse(&table_name).is_some_and(|r| r.sub.is_none());
                    let supplied = if names_a_table {
                        format!("{table_name}/index/{idx}")
                    } else {
                        resource.clone()
                    };
                    vec![AuthzTarget {
                        action: operation.to_owned(),
                        resource,
                        supplied,
                        match_supplied: false,
                    }]
                }
                _ => from(base, &table_name),
            }
        }
        // Account-level operations. ListTables is evaluated against the table
        // wildcard and DescribeLimits against "*" (measured live).
        // DescribeEndpoints is not authorized at all (see authorize_request);
        // its "*" entry is kept so the resource map covers every operation.
        "ListTables" => one(table_wildcard(region, account_id)),
        "DescribeLimits" | "DescribeEndpoints" => one("*".to_owned()),
        // Generic single-table operations keyed by TableName (name or ARN).
        _ => match str_field(input, "TableName") {
            Some(t) => from(table_resource_arn(&t, region, account_id), &t),
            None => one(table_wildcard(region, account_id)),
        },
    }
}

/// The item actions a restore requires on its `TargetTableName`, one target
/// per action. The target is a new table named by a bare name, so each denial
/// names the canonical ARN.
fn restore_target_actions(input: &Value, region: &str, account_id: &str) -> Vec<AuthzTarget> {
    let target = table_resource_arn(
        &str_field(input, "TargetTableName").unwrap_or_default(),
        region,
        account_id,
    );
    RESTORE_TARGET_ACTIONS
        .iter()
        .map(|a| AuthzTarget::new(a, target.clone()))
        .collect()
}

/// The bare table name from a value that may be a name or a table ARN.
fn table_name_only(value: &str) -> String {
    if value.starts_with("arn:")
        && let Some(rid) = parse_resource_identity(value)
    {
        return rid.table;
    }
    value.to_owned()
}

/// The table name to look up key info under when a request names its table by
/// `value`, which may be a bare table name or a table ARN.
///
/// A table ARN is reduced to its table name only when it names a table in this
/// deployment's partition, service, region and the caller's account. Any other
/// ARN is returned unchanged, so the lookup finds no table and the engine
/// handles the request exactly as it would without this helper.
pub(crate) fn key_lookup_name(value: &str, region: &str, account_id: &str) -> String {
    if let Some(f) = arn_fields(value)
        && f.partition == PARTITION
        && f.service == SERVICE
        && f.region == region
        && f.account == account_id
        && let Some(rid) = parse_resource_identity(value)
        && rid.sub.is_none()
    {
        return rid.table;
    }
    value.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const R: &str = "us-east-1";
    const A: &str = "444455556666";

    fn only(op: &str, input: Value) -> String {
        let t = authz_targets(op, &input, R, A);
        assert_eq!(t.len(), 1, "expected one target for {op}: {t:?}");
        assert_eq!(t[0].action, op);
        t[0].resource.clone()
    }

    /// The engine trims surrounding ASCII whitespace from a `BackupArn`
    /// before it resolves the backup, so authorization evaluates the trimmed
    /// backup and a Deny on it cannot be bypassed with a trailing newline.
    #[test]
    fn backup_arn_with_surrounding_whitespace_authorizes_the_trimmed_backup() {
        let arn = format!("arn:aws:dynamodb:{R}:{A}:table/T/backup/01791976431438-73d8d5bc");
        for spelled in [
            format!("{arn}\n"),
            format!(" {arn}"),
            format!("\t{arn}\r\n"),
        ] {
            for op in ["DescribeBackup", "DeleteBackup"] {
                assert!(validate_operation_arns(op, &json!({ "BackupArn": spelled }), R).is_ok());
                assert_eq!(
                    only(op, json!({ "BackupArn": spelled })),
                    arn,
                    "{spelled:?}"
                );
            }
            let restore = authz_targets(
                "RestoreTableFromBackup",
                &json!({ "BackupArn": spelled, "TargetTableName": "X" }),
                R,
                A,
            );
            assert_eq!(
                (restore[0].action.as_str(), restore[0].resource.as_str()),
                ("RestoreTableFromBackup", arn.as_str())
            );
        }
    }

    // -- parser --

    #[test]
    fn parses_table() {
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:table/T"),
            Some(ResourceId {
                table: "T".into(),
                sub: None
            })
        );
    }

    #[test]
    fn parses_stream_label_with_colons() {
        assert_eq!(
            parse_resource_identity(
                "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188"
            ),
            Some(ResourceId {
                table: "T".into(),
                sub: Some(Sub::Stream("2026-10-20T16:17:40.188".into())),
            })
        );
    }

    #[test]
    fn parses_index_and_backup() {
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:table/T/index/gsi1"),
            Some(ResourceId {
                table: "T".into(),
                sub: Some(Sub::Index("gsi1".into()))
            })
        );
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:table/T/backup/01-ab"),
            Some(ResourceId {
                table: "T".into(),
                sub: Some(Sub::Backup("01-ab".into()))
            })
        );
    }

    #[test]
    fn rejects_non_table_and_malformed() {
        assert_eq!(parse_resource_identity("banana"), None);
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:banana/x"),
            None
        );
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:table/"),
            None
        );
        assert_eq!(
            parse_resource_identity("arn:aws:dynamodb:us-east-1:1:table/T/weird/x"),
            None
        );
    }

    // -- per-operation resource derivation (matches live DynamoDB) --

    #[test]
    fn tag_ops_use_resource_arn() {
        let table_arn = "arn:aws:dynamodb:us-east-1:444455556666:table/bb";
        for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
            assert_eq!(only(op, json!({ "ResourceArn": table_arn })), table_arn);
        }
        // a stream ARN as the tag target is preserved
        let stream =
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/stream/2026-10-20T16:17:40.188";
        assert_eq!(
            only("TagResource", json!({ "ResourceArn": stream })),
            stream
        );
    }

    #[test]
    fn stream_reads_use_stream_arn() {
        let s = "arn:aws:dynamodb:us-east-1:444455556666:table/bb/stream/2026-10-20T16:17:40.188";
        assert_eq!(only("DescribeStream", json!({ "StreamArn": s })), s);
        assert_eq!(only("GetShardIterator", json!({ "StreamArn": s })), s);
    }

    #[test]
    fn list_streams_scopes_to_table_then_account() {
        assert_eq!(
            only("ListStreams", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/stream/*"
        );
        assert_eq!(
            only("ListStreams", json!({})),
            "arn:aws:dynamodb:us-east-1:444455556666:table/*/stream/*"
        );
    }

    #[test]
    fn list_backups_scopes_to_table_backups_then_account() {
        // The service evaluates ListBackups against table/<T>/backup/* with a
        // TableName and table/*/backup/* without (measured live), not the bare
        // table or table/*.
        assert_eq!(
            only("ListBackups", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/*"
        );
        assert_eq!(
            only("ListBackups", json!({})),
            "arn:aws:dynamodb:us-east-1:444455556666:table/*/backup/*"
        );
        // A table ARN in TableName is reduced to its table name first.
        assert_eq!(
            only(
                "ListBackups",
                json!({ "TableName": "arn:aws:dynamodb:us-east-1:444455556666:table/bb" })
            ),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/*"
        );
    }

    #[test]
    fn backup_ops_use_backup_arn_or_table() {
        let b = "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/01-ab";
        assert_eq!(only("DescribeBackup", json!({ "BackupArn": b })), b);
        assert_eq!(only("DeleteBackup", json!({ "BackupArn": b })), b);
        assert_eq!(
            only(
                "CreateBackup",
                json!({ "TableName": "bb", "BackupName": "x" })
            ),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb"
        );
    }

    #[test]
    fn query_scan_on_index_use_index_arn() {
        assert_eq!(
            only("Query", json!({ "TableName": "bb", "IndexName": "gsi1" })),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/index/gsi1"
        );
        assert_eq!(
            only("Scan", json!({ "TableName": "bb", "IndexName": "lsi1" })),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/index/lsi1"
        );
        assert_eq!(
            only("Query", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb"
        );
    }

    #[test]
    fn table_name_may_be_an_arn() {
        let arn = "arn:aws:dynamodb:us-east-1:444455556666:table/bb";
        assert_eq!(only("GetItem", json!({ "TableName": arn })), arn);
        // a respelled table ARN in TableName canonicalizes
        assert_eq!(
            only(
                "GetItem",
                json!({ "TableName": "arn:aws-cn:dynamodb:us-west-2:111122223333:table/bb" })
            ),
            arn
        );
    }

    #[test]
    fn account_level_ops_use_table_wildcard() {
        assert_eq!(
            only("ListTables", json!({})),
            "arn:aws:dynamodb:us-east-1:444455556666:table/*"
        );
        assert_eq!(only("DescribeLimits", json!({})), "*");
    }

    #[test]
    fn restore_export_import_use_measured_resource() {
        let backup = "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/01-ab";
        // RestoreTableFromBackup: the restore action on the backup ARN plus the
        // seven item actions on the target table.
        let rfb = authz_targets(
            "RestoreTableFromBackup",
            &json!({ "TargetTableName": "restored", "BackupArn": backup }),
            R,
            A,
        );
        assert!(rfb.contains(&AuthzTarget::new(
            "RestoreTableFromBackup",
            backup.to_owned()
        )));
        let target = "arn:aws:dynamodb:us-east-1:444455556666:table/restored".to_owned();
        for a in RESTORE_TARGET_ACTIONS {
            assert!(
                rfb.contains(&AuthzTarget::new(a, target.clone())),
                "RestoreTableFromBackup missing {a} on target"
            );
        }
        assert_eq!(rfb.len(), 1 + RESTORE_TARGET_ACTIONS.len());

        // RestoreTableToPointInTime: the restore action on the source table
        // plus the seven item actions on the target table.
        let pitr = authz_targets(
            "RestoreTableToPointInTime",
            &json!({ "SourceTableName": "bb", "TargetTableName": "pitr" }),
            R,
            A,
        );
        assert!(pitr.contains(&AuthzTarget::new(
            "RestoreTableToPointInTime",
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb".to_owned()
        )));
        let ptarget = "arn:aws:dynamodb:us-east-1:444455556666:table/pitr".to_owned();
        for a in RESTORE_TARGET_ACTIONS {
            assert!(
                pitr.contains(&AuthzTarget::new(a, ptarget.clone())),
                "RestoreTableToPointInTime missing {a} on target"
            );
        }
        assert_eq!(pitr.len(), 1 + RESTORE_TARGET_ACTIONS.len());

        // ExportTableToPointInTime: only the export action on the source table.
        assert_eq!(
            only(
                "ExportTableToPointInTime",
                json!({ "TableArn": "arn:aws:dynamodb:us-east-1:444455556666:table/bb", "S3Bucket": "x" })
            ),
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb"
        );
        // ImportTable: only the import action on the new table.
        assert_eq!(
            only(
                "ImportTable",
                json!({ "TableCreationParameters": { "TableName": "imp" } })
            ),
            "arn:aws:dynamodb:us-east-1:444455556666:table/imp"
        );
    }

    #[test]
    fn missing_resource_field_never_falls_back_to_table_wildcard() {
        // An absent ResourceArn yields the empty string, not table/*, so no
        // specific Deny is silently skipped.
        assert_eq!(only("TagResource", json!({})), "");
        assert_eq!(only("DescribeStream", json!({})), "");
    }

    #[test]
    fn tag_resource_arn_shape_rejects_index_and_backup() {
        // Tags are accepted only on a table or a stream. An index is rejected
        // as not a table or stream; a backup is not a DynamoDB resource arn.
        // Both measured live.
        let index = "arn:aws:dynamodb:us-east-1:444455556666:table/bb/index/gsi1";
        assert!(matches!(
            validate_tag_resource_arn_shape(index),
            Err(DynamoDbError::ValidationException(ref m))
                if m.contains("only table or stream is accepted") && m.contains(index)
        ));
        for backup in [
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/01792513080555-82c5bd04",
            "arn:aws:dynamodb:us-east-1:444455556666:table/bb/backup/01-ab",
        ] {
            assert!(
                matches!(
                    validate_tag_resource_arn_shape(backup),
                    Err(DynamoDbError::ValidationException(ref m))
                        if m == &format!(
                            "One or more parameter values were invalid: Provided Arn is not a \
                             DynamoDB resource arn: {backup}"
                        )
                ),
                "expected not-a-DynamoDB-resource for {backup}"
            );
        }
        // A table ARN and a stream ARN pass the shape check.
        assert!(
            validate_tag_resource_arn_shape("arn:aws:dynamodb:us-east-1:444455556666:table/bb")
                .is_ok()
        );
        assert!(
            validate_tag_resource_arn_shape(
                "arn:aws:dynamodb:us-east-1:444455556666:table/bb/stream/2026-10-20T16:17:40.188"
            )
            .is_ok()
        );
    }

    #[test]
    fn tag_resource_arn_shape_matches_service_ordering() {
        // Measured live. ValidationException before authorization, every
        // principal:
        let not_arn = validate_tag_resource_arn_shape("banana");
        assert!(matches!(
            not_arn,
            Err(DynamoDbError::ValidationException(ref m))
                if m.contains("ARNs must start with 'arn:': banana")
        ));
        let bad_type =
            validate_tag_resource_arn_shape("arn:aws:dynamodb:us-east-1:444455556666:banana/x");
        assert!(matches!(
            bad_type,
            Err(DynamoDbError::ValidationException(ref m))
                if m.contains("Provided Arn is not a DynamoDB resource arn")
        ));
        // A foreign-service ARN whose resource is not a table passes the shape
        // check and is left to authorization (which denies it).
        assert!(validate_tag_resource_arn_shape("arn:aws:s3:::mybucket").is_ok());
        // A table ARN whose partition, service, or region is this deployment's
        // or not passes the shape check; the engine handler validates those
        // fields after authorization.
        assert!(
            validate_tag_resource_arn_shape("arn:aws-cn:dynamodb:us-east-1:444455556666:table/bb")
                .is_ok()
        );
        assert!(
            validate_tag_resource_arn_shape("arn:aws:s3:us-east-1:444455556666:table/bb").is_ok()
        );
    }

    // One differential corpus for every entry point that turns an ARN into a
    // resource identity. All of them now route through the single shared parser
    // `extenddb_core::arn`: authorization (`parse_resource_identity`), the
    // export table resolver and both tagging parsers (`extenddb_core::arn`
    // directly), the backup account split (`extenddb_core::arn::fields`), and
    // the engine stream resolver (`extenddb_storage::util::parse_stream_arn`).
    // The corpus asserts they agree on (table, sub-resource) or all refuse, so
    // no respelling exists that one layer resolves and another does not. This is
    // the property whose absence let the A2 bypass (`xrn:` authorized as the raw
    // string, served as the real stream) and the A1 bypass (export reading a
    // different table than authorization checked) happen.
    #[test]
    fn every_arn_entry_point_agrees_or_all_refuse() {
        use extenddb_core::arn::{self, SubResource};
        use extenddb_storage::util::parse_stream_arn;

        // Inputs spanning: non-`arn` prefixes, empty fields, repeated `table/`
        // segments, appended sub-resources on every kind, unknown kinds,
        // trailing slashes, empty ids, foreign partition/service/region/account,
        // colons in stream labels, and `/` inside ids.
        let corpus = [
            "banana",
            "xrn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188",
            ":aws:dynamodb:us-east-1:1:table/T",
            "arn:aws::us-east-1:1:table/T",
            "arn:aws:dynamodb::1:table/T",
            "arn:aws:dynamodb:us-east-1::table/T",
            "arn:aws:dynamodb:us-east-1:1:table/T",
            "arn:aws:dynamodb:us-east-1:1:table/allowed/stream/table/secret",
            "arn:aws:dynamodb:us-east-1:1:table/a/table/b",
            "arn:aws:dynamodb:us-east-1:1:table/T/index/gsi1",
            "arn:aws:dynamodb:us-east-1:1:table/T/index/gsi1/extra",
            "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:1:table/T/stream/a/b",
            "arn:aws:dynamodb:us-east-1:1:table/T/backup/01-ab",
            "arn:aws:dynamodb:us-east-1:1:table/T/weird/x",
            "arn:aws:dynamodb:us-east-1:1:table/",
            "arn:aws:dynamodb:us-east-1:1:table/T/",
            "arn:aws:dynamodb:us-east-1:1:table/T/stream/",
            "arn:aws-cn:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:s3:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:eu-west-1:1:table/T/backup/01-ab",
            "arn:aws:dynamodb:us-east-1:999999999999:table/T",
        ];

        for input in corpus {
            let core = arn::parse(input);

            // Authorization's parser wraps the shared parser: they agree on
            // (table, sub-resource) for every input.
            let authz = parse_resource_identity(input);
            match (&core, &authz) {
                (None, None) => {}
                (Some(c), Some(a)) => {
                    assert_eq!(a.table, c.table, "authz table mismatch for {input}");
                    let want_sub = c.sub.clone().map(|s| match s {
                        SubResource::Index(i) => Sub::Index(i),
                        SubResource::Stream(s) => Sub::Stream(s),
                        SubResource::Backup(b) => Sub::Backup(b),
                    });
                    assert_eq!(a.sub, want_sub, "authz sub mismatch for {input}");
                }
                _ => panic!("authz and core parser disagree on {input}"),
            }

            // The engine stream resolver accepts an input iff the shared parser
            // yields a stream, with the same table and label.
            match (&core, parse_stream_arn(input)) {
                (
                    Some(arn::TableResource {
                        table,
                        sub: Some(SubResource::Stream(label)),
                        ..
                    }),
                    Ok((t, l)),
                ) => {
                    assert_eq!(&t, table, "stream table mismatch for {input}");
                    assert_eq!(&l, label, "stream label mismatch for {input}");
                }
                (
                    Some(arn::TableResource {
                        sub: Some(SubResource::Stream(_)),
                        ..
                    }),
                    Err(_),
                ) => {
                    panic!("shared parser accepted stream but engine rejected it: {input}")
                }
                (_, Ok(_)) => panic!("engine resolved a non-stream input as a stream: {input}"),
                (_, Err(_)) => {}
            }

            // The export table resolver accepts an input iff the shared parser
            // yields a bare table (no sub-resource); a repeated `table/` segment
            // or any sub-resource is refused, so it cannot read a different
            // table than authorization canonicalized.
            let export_ok = matches!(core, Some(arn::TableResource { sub: None, .. }));
            if input == "arn:aws:dynamodb:us-east-1:1:table/allowed/stream/table/secret" {
                assert!(!export_ok, "export must refuse the A1 bypass ARN");
            }

            // Canonicalization closes the respelling bypass: every parseable
            // resource maps to this deployment's partition, service, region, and
            // account, so a Deny on the real resource is always evaluated
            // against the real resource.
            if let Some(rid) = &authz {
                let c = canonical("us-east-1", "444455556666", rid);
                assert!(
                    c.starts_with("arn:aws:dynamodb:us-east-1:444455556666:table/"),
                    "canonical did not pin deployment fields for {input}"
                );
            }
        }

        // The two bypass inputs that motivated this round, stated explicitly.
        // `xrn:` is refused everywhere (A2): authorization rejects it and the
        // engine stream resolver rejects it, so it is never authorized as the
        // raw string nor served.
        assert!(parse_resource_identity("xrn:aws:dynamodb:us-east-1:1:table/T/stream/x").is_none());
        assert!(parse_stream_arn("xrn:aws:dynamodb:us-east-1:1:table/T/stream/x").is_err());

        // The export A1 bypass input `.../table/allowed/stream/table/secret` is
        // a repeated `table/` segment, so it does not parse (the id would
        // contain `/`). Under the old code authorization canonicalized it to
        // table `allowed` while the engine's `rsplit_once("table/")` exported
        // `secret`. It is now unparseable, so export refuses it before
        // authorization with the service's "not a valid ARN" message.
        let a1 = "arn:aws:dynamodb:us-east-1:1:table/allowed/stream/table/secret";
        assert!(arn::parse(a1).is_none());
        assert!(matches!(
            validate_operation_arns("ExportTableToPointInTime", &json!({ "TableArn": a1 }), R),
            Err(DynamoDbError::ValidationException(ref m)) if m == INVALID_EXPORT_TABLE_ARN_MESSAGE
        ));
    }

    #[test]
    fn validate_operation_arns_matches_measured_live_behaviour() {
        let err_msg = |op: &str, input: Value| match validate_operation_arns(op, &input, R) {
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Ok(()) => None,
            Err(_) => panic!("unexpected non-validation error for {op}"),
        };

        // Export: only an UNPARSEABLE TableArn is refused before authz, with the
        // "tableArn is not a valid ARN" message (repeated table/ segments, a
        // bare name, an xrn: ARN). A well-formed stream or index ARN is NOT
        // refused here: it is authorized against its canonical form and the
        // export handler refuses it afterwards.
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/a/stream/table/b",
            "arn:aws:dynamodb:us-east-1:1:table/a/index/table/b",
            "table/x",
            "xrn:aws:dynamodb:us-east-1:1:table/t",
        ] {
            assert_eq!(
                err_msg("ExportTableToPointInTime", json!({ "TableArn": arn })).as_deref(),
                Some(INVALID_EXPORT_TABLE_ARN_MESSAGE),
                "export should refuse unparseable {arn} before authz"
            );
        }
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/t",
            "arn:aws:dynamodb:us-east-1:1:table/t/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:1:table/t/index/g",
        ] {
            assert_eq!(
                err_msg("ExportTableToPointInTime", json!({ "TableArn": arn })),
                None,
                "export should NOT refuse well-formed {arn} before authz"
            );
        }

        // Stream reads: a table ARN, an xrn: ARN, and an empty partition are all
        // refused before authz with exactly "Invalid StreamArn"; a stream ARN
        // passes.
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/t",
            "xrn:aws:dynamodb:us-east-1:1:table/t/stream/x",
            "arn::dynamodb:us-east-1:1:table/t/stream/x",
        ] {
            for op in ["DescribeStream", "GetShardIterator"] {
                assert_eq!(
                    err_msg(op, json!({ "StreamArn": arn })).as_deref(),
                    Some(INVALID_STREAM_ARN_MESSAGE),
                    "{op} should refuse {arn}"
                );
            }
        }
        // A stream ARN in this deployment's partition, service and region
        // passes.
        assert_eq!(
            err_msg(
                "DescribeStream",
                json!({ "StreamArn": format!("arn:aws:dynamodb:{R}:{A}:table/t/stream/2026-10-20T16:17:40.188") })
            ),
            None
        );

        // Backup ops: a table ARN and an xrn: ARN are refused before authz with
        // exactly "Invalid Backup ARN"; a backup ARN passes.
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/t",
            "xrn:aws:dynamodb:us-east-1:1:table/t/backup/01-ab",
        ] {
            for op in ["DescribeBackup", "DeleteBackup"] {
                assert_eq!(
                    err_msg(op, json!({ "BackupArn": arn })).as_deref(),
                    Some(INVALID_BACKUP_ARN_MESSAGE),
                    "{op} should refuse {arn}"
                );
            }
        }
        assert_eq!(
            err_msg(
                "DeleteBackup",
                json!({ "BackupArn": "arn:aws:dynamodb:us-east-1:1:table/t/backup/01-ab" })
            ),
            None
        );

        // An absent or empty field is left to the normal path.
        assert_eq!(err_msg("ExportTableToPointInTime", json!({})), None);
        assert_eq!(err_msg("DescribeStream", json!({ "StreamArn": "" })), None);
    }

    #[test]
    fn resource_tag_lookup_arn_reads_tags_only_for_tables_and_indexes() {
        // A table ARN is looked up under itself.
        assert_eq!(
            resource_tag_lookup_arn("arn:aws:dynamodb:us-east-1:1:table/bb").as_deref(),
            Some("arn:aws:dynamodb:us-east-1:1:table/bb")
        );
        // An index ARN resolves its tags under the base table ARN.
        assert_eq!(
            resource_tag_lookup_arn("arn:aws:dynamodb:us-east-1:1:table/bb/index/gsi1").as_deref(),
            Some("arn:aws:dynamodb:us-east-1:1:table/bb")
        );
        // A table wildcard is looked up as given; the cache returns no tags.
        assert_eq!(
            resource_tag_lookup_arn("arn:aws:dynamodb:us-east-1:1:table/*").as_deref(),
            Some("arn:aws:dynamodb:us-east-1:1:table/*")
        );
        // A stream, backup, export, foreign-service or wildcard resource has no
        // tags for authorization (None), even though a stream can be tagged.
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/bb/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:1:table/bb/backup/01792513080555-82c5bd04",
            "arn:aws:dynamodb:us-east-1:1:table/bb/export/01761100000000-abcd1234",
            "arn:aws:s3:::bucket",
            "*",
        ] {
            assert_eq!(resource_tag_lookup_arn(arn), None, "{arn}");
        }
    }

    #[test]
    fn getrecords_resource_classifies_and_canonicalizes() {
        // A current token issued by this server authorizes against the stream
        // ARN it carries (a token naming any other account, partition, service
        // or region is Invalid; see
        // `getrecords_iterator_with_respelled_stream_arn_is_invalid`).
        let stream =
            "arn:aws:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188";
        let tok = extenddb_engine::encode_shard_iterator(
            "shardId-1",
            "AFTER_SEQUENCE_NUMBER",
            "42",
            1000,
            stream,
        );
        match getrecords_resource(&json!({ "ShardIterator": tok }), R, A) {
            GetRecordsResource::Stream(arn) => assert_eq!(arn, stream),
            _ => panic!("expected Stream"),
        }
        // A token that carries no stream ARN but a parseable timestamp is the
        // pre-upgrade shape; it is Expired.
        let old = extenddb_engine::encode_shard_iterator(
            "shardId-2",
            "AFTER_SEQUENCE_NUMBER",
            "7",
            1000,
            "",
        );
        assert!(matches!(
            getrecords_resource(&json!({ "ShardIterator": old }), R, A),
            GetRecordsResource::Expired
        ));
        // Garbage and an absent iterator are Invalid.
        assert!(matches!(
            getrecords_resource(&json!({ "ShardIterator": "!!!" }), R, A),
            GetRecordsResource::Invalid
        ));
        assert!(matches!(
            getrecords_resource(&json!({}), R, A),
            GetRecordsResource::Invalid
        ));
    }
}

#[cfg(test)]
mod key_lookup_name_tests {
    use super::key_lookup_name;

    #[test]
    fn table_arn_for_this_account_and_region_resolves_to_the_table_name() {
        let r = "us-east-1";
        let a = "444455556666";
        assert_eq!(key_lookup_name("orders", r, a), "orders");
        assert_eq!(
            key_lookup_name("arn:aws:dynamodb:us-east-1:444455556666:table/orders", r, a),
            "orders"
        );
        for other in [
            "arn:aws:dynamodb:us-east-1:111111111111:table/orders",
            "arn:aws:dynamodb:eu-west-1:444455556666:table/orders",
            "arn:aws-cn:dynamodb:us-east-1:444455556666:table/orders",
            "arn:aws:s3:us-east-1:444455556666:table/orders",
            "arn:aws:dynamodb:us-east-1:444455556666:table/orders/index/g1",
            "arn:aws:dynamodb:us-east-1:444455556666:backup/orders",
            "arn:aws:dynamodb",
        ] {
            assert_eq!(key_lookup_name(other, r, a), other, "{other}");
        }
    }
}

/// The denial names the resource as the caller supplied it (issue #447).
#[cfg(test)]
mod supplied_resource_tests;

/// A point-in-time restore authorizes its target before its source (issue #446).
#[cfg(test)]
mod restore_order_tests;
