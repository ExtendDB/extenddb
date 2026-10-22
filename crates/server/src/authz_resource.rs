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
//! Every resource ARN produced here is CANONICAL for this deployment: the
//! partition, service, region, and account are fixed to this server's, and only
//! the resource identity (table name and optional sub-resource) is taken from
//! the request. ExtendDB's stream and backup resolvers ignore the partition,
//! service, region, and account of a supplied ARN and act within this
//! deployment's account, so a caller who respells those fields would otherwise
//! evade a Deny written against the canonical ARN. Canonicalizing here closes
//! that bypass: a Deny on the real resource always matches.
//!
//! Fail-closed: a missing or unparseable resource field never falls back to
//! `table/*`. It yields the field value verbatim (often empty), which an
//! `Allow` on `*` still reaches so the engine returns its normal validation
//! error, while no Deny on a specific resource is silently skipped.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

/// Partition and service are fixed to this deployment's; see module docs.
const PARTITION: &str = "aws";
const SERVICE: &str = "dynamodb";

/// The item actions the service requires on the target table of a restore, in
/// addition to the restore action itself (measured live). A Deny on any of
/// these for the target table name blocks a restore into it.
const RESTORE_TARGET_ACTIONS: &[&str] = &[
    "Scan",
    "Query",
    "GetItem",
    "PutItem",
    "UpdateItem",
    "DeleteItem",
    "BatchWriteItem",
];

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
        Some(Sub::Backup(b)) => format!("{base}/backup/{b}"),
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

/// The resource ARN a tag operation authorizes against.
///
/// `validate_tag_resource_arn_shape` has already run: the value is a table ARN,
/// a stream ARN, or a foreign-service ARN (an index, a backup, or a non-table
/// `DynamoDB` ARN is rejected before authorization). A table or stream ARN is
/// canonicalized to this deployment; a foreign-service ARN is left verbatim so
/// authorization denies it, matching the service (measured, arn-1 table).
fn tag_resource_arn(arn: &str, region: &str, account_id: &str) -> String {
    match parse_resource_identity(arn) {
        Some(rid @ ResourceId { sub: None, .. })
        | Some(
            rid @ ResourceId {
                sub: Some(Sub::Stream(_)),
                ..
            },
        ) => canonical(region, account_id, &rid),
        // A foreign-service ARN (or, defensively, an index/backup the shape
        // check already rejected): authorization denies it.
        _ => arn.to_owned(),
    }
}

/// The resource kind a strictly validated ARN-bearing request field must name.
/// Only stream and backup fields are validated before authorization; an export
/// `TableArn` is checked only for parseability (see `validate_operation_arns`).
#[derive(Clone, Copy)]
enum ArnKind {
    Stream,
    Backup,
}

/// Messages DynamoDB returns, before the IAM decision, for a request field
/// whose ARN is not a valid resource ARN of the kind the field requires
/// (measured live, `live-r2/arn-kinds-live.json`).
const INVALID_STREAM_ARN_MESSAGE: &str = "Invalid StreamArn";
const INVALID_BACKUP_ARN_MESSAGE: &str = "Invalid Backup ARN";
const INVALID_EXPORT_TABLE_ARN_MESSAGE: &str =
    "One or more parameter values were invalid: tableArn is not a valid ARN";

/// True when `arn` is a well-formed DynamoDB resource ARN of `kind`: it parses
/// to that kind and its partition, service, region, and account are all
/// non-empty. The field checks catch an empty-partition ARN
/// (`arn::dynamodb:...`), which the service rejects with `Invalid StreamArn`.
fn arn_is_kind(arn: &str, kind: ArnKind) -> bool {
    let Some(f) = extenddb_core::arn::fields(arn) else {
        return false;
    };
    if f.partition.is_empty() || f.service.is_empty() || f.region.is_empty() || f.account.is_empty()
    {
        return false;
    }
    matches!(
        (parse_resource_identity(arn), kind),
        (
            Some(ResourceId {
                sub: Some(Sub::Stream(_)),
                ..
            }),
            ArnKind::Stream
        ) | (
            Some(ResourceId {
                sub: Some(Sub::Backup(_)),
                ..
            }),
            ArnKind::Backup
        )
    )
}

/// Validate ARN-bearing fields before the IAM decision, matching the service
/// (measured live, `live-r2/arn-kinds-live.json`).
///
/// The two field families differ, as the service does:
///
/// - `StreamArn` (DescribeStream, GetShardIterator) and `BackupArn`
///   (DescribeBackup, DeleteBackup) are validated strictly: a value that is not
///   a well-formed stream (resp. backup) ARN of the correct kind, including a
///   table ARN, an `xrn:` ARN, or an empty partition, is refused before
///   authorization with `Invalid StreamArn` (resp. `Invalid Backup ARN`).
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
/// principal is denied) and the engine refuses it afterwards. Tag operations
/// keep their own measured shape check (`validate_tag_resource_arn_shape`);
/// `GetRecords` is handled through its iterator by [`getrecords_resource`].
pub(crate) fn validate_operation_arns(operation: &str, input: &Value) -> Result<(), DynamoDbError> {
    let present = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    match operation {
        "DescribeStream" | "GetShardIterator" => {
            if let Some(arn) = present("StreamArn")
                && !arn_is_kind(arn, ArnKind::Stream)
            {
                return Err(DynamoDbError::ValidationException(
                    INVALID_STREAM_ARN_MESSAGE.to_owned(),
                ));
            }
        }
        "DescribeBackup" | "DeleteBackup" => {
            if let Some(arn) = present("BackupArn")
                && !arn_is_kind(arn, ArnKind::Backup)
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
/// is canonicalized through the shared parser; a decoded stream ARN that does
/// not parse to a stream is refused as Invalid rather than authorized verbatim,
/// so no respelled or forged stream ARN is served.
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
            ) => GetRecordsResource::Stream(canonical(region, account_id, &rid)),
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

/// Account-wide table wildcard, for operations with no specific resource.
fn table_wildcard(region: &str, account_id: &str) -> String {
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
/// authorization then denies. The partition, service, region, account, and
/// existence of a table ARN are checked after authorization by the engine
/// handler, so for those a no-permissions principal is denied first and only an
/// authorized principal sees the shape error.
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

/// Derive the `(action, resource ARN)` pairs to authorize for a non-batch,
/// non-transact `DynamoDB` operation. `action` is the bare operation form
/// (`check_authorization` prepends `dynamodb:`). Most operations map to a
/// single pair; multi-permission operations return more than one.
///
/// This function never returns an empty list: the generic single-table path is
/// the fallback for operations not named explicitly.
pub(crate) fn authz_targets(
    operation: &str,
    input: &Value,
    region: &str,
    account_id: &str,
) -> Vec<(String, String)> {
    let one = |resource: String| vec![(operation.to_owned(), resource)];

    match operation {
        // Resource lives in ResourceArn (a table or stream ARN; a foreign ARN
        // passes the shape check and is denied by authorization).
        "TagResource" | "UntagResource" | "ListTagsOfResource" => one(tag_resource_arn(
            &str_field(input, "ResourceArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // Resource lives in StreamArn, validated to a stream before authz.
        "DescribeStream" | "GetShardIterator" => one(canonical_resource(
            &str_field(input, "StreamArn").unwrap_or_default(),
            region,
            account_id,
        )),
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
        "DescribeBackup" | "DeleteBackup" => one(canonical_resource(
            &str_field(input, "BackupArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // Resource is the table named by TableName.
        "CreateBackup" | "DescribeContinuousBackups" | "UpdateContinuousBackups" => {
            one(table_resource_arn(
                &str_field(input, "TableName").unwrap_or_default(),
                region,
                account_id,
            ))
        }
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
        "ExportTableToPointInTime" => one(canonical_resource(
            &str_field(input, "TableArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // Import creates a table; the resource is the new table.
        "ImportTable" => {
            let t = input
                .get("TableCreationParameters")
                .and_then(|p| p.get("TableName"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            one(table_resource_arn(t, region, account_id))
        }
        // RestoreTableFromBackup requires dynamodb:RestoreTableFromBackup on the
        // source backup ARN and the item actions on the target table it writes
        // (measured live). A Deny on the backup ARN, or on any item action for
        // the target table, blocks the restore.
        "RestoreTableFromBackup" => {
            let mut v = vec![(
                operation.to_owned(),
                canonical_resource(
                    &str_field(input, "BackupArn").unwrap_or_default(),
                    region,
                    account_id,
                ),
            )];
            let target = table_resource_arn(
                &str_field(input, "TargetTableName").unwrap_or_default(),
                region,
                account_id,
            );
            for a in RESTORE_TARGET_ACTIONS {
                v.push(((*a).to_owned(), target.clone()));
            }
            v
        }
        // RestoreTableToPointInTime requires dynamodb:RestoreTableToPointInTime
        // on the source table and the item actions on the target table it
        // writes (measured live).
        "RestoreTableToPointInTime" => {
            let source = str_field(input, "SourceTableArn")
                .or_else(|| str_field(input, "SourceTableName"))
                .unwrap_or_default();
            let mut v = vec![(
                operation.to_owned(),
                table_resource_arn(&source, region, account_id),
            )];
            let target = table_resource_arn(
                &str_field(input, "TargetTableName").unwrap_or_default(),
                region,
                account_id,
            );
            for a in RESTORE_TARGET_ACTIONS {
                v.push(((*a).to_owned(), target.clone()));
            }
            v
        }
        // Query and Scan on an index authorize the index ARN.
        "Query" | "Scan" => {
            let base = table_resource_arn(
                &str_field(input, "TableName").unwrap_or_default(),
                region,
                account_id,
            );
            match str_field(input, "IndexName") {
                Some(idx) if !idx.is_empty() => one(format!("{base}/index/{idx}")),
                _ => one(base),
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
            Some(t) => one(table_resource_arn(&t, region, account_id)),
            None => one(table_wildcard(region, account_id)),
        },
    }
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
    const A: &str = "964157134968";

    fn only(op: &str, input: Value) -> String {
        let t = authz_targets(op, &input, R, A);
        assert_eq!(t.len(), 1, "expected one target for {op}: {t:?}");
        assert_eq!(t[0].0, op);
        t[0].1.clone()
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

    // -- canonicalization closes the respelling bypass --

    #[test]
    fn respelled_partition_service_region_account_canonicalize_to_same_arn() {
        let canonical =
            "arn:aws:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188";
        for respelled in [
            "arn:aws-cn:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:s3:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-west-2:964157134968:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:111122223333:table/T/stream/2026-10-20T16:17:40.188",
        ] {
            assert_eq!(
                only("DescribeStream", json!({ "StreamArn": respelled })),
                canonical,
                "respelled {respelled} must canonicalize"
            );
        }
    }

    // -- per-operation resource derivation (matches live DynamoDB) --

    #[test]
    fn tag_ops_use_resource_arn() {
        let table_arn = "arn:aws:dynamodb:us-east-1:964157134968:table/bb";
        for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
            assert_eq!(only(op, json!({ "ResourceArn": table_arn })), table_arn);
        }
        // a stream ARN as the tag target is preserved
        let stream =
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/stream/2026-10-20T16:17:40.188";
        assert_eq!(
            only("TagResource", json!({ "ResourceArn": stream })),
            stream
        );
    }

    #[test]
    fn stream_reads_use_stream_arn() {
        let s = "arn:aws:dynamodb:us-east-1:964157134968:table/bb/stream/2026-10-20T16:17:40.188";
        assert_eq!(only("DescribeStream", json!({ "StreamArn": s })), s);
        assert_eq!(only("GetShardIterator", json!({ "StreamArn": s })), s);
    }

    #[test]
    fn list_streams_scopes_to_table_then_account() {
        assert_eq!(
            only("ListStreams", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/stream/*"
        );
        assert_eq!(
            only("ListStreams", json!({})),
            "arn:aws:dynamodb:us-east-1:964157134968:table/*/stream/*"
        );
    }

    #[test]
    fn list_backups_scopes_to_table_backups_then_account() {
        // The service evaluates ListBackups against table/<T>/backup/* with a
        // TableName and table/*/backup/* without (measured live), not the bare
        // table or table/*.
        assert_eq!(
            only("ListBackups", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/*"
        );
        assert_eq!(
            only("ListBackups", json!({})),
            "arn:aws:dynamodb:us-east-1:964157134968:table/*/backup/*"
        );
        // A table ARN in TableName is reduced to its table name first.
        assert_eq!(
            only(
                "ListBackups",
                json!({ "TableName": "arn:aws:dynamodb:us-east-1:964157134968:table/bb" })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/*"
        );
    }

    #[test]
    fn backup_ops_use_backup_arn_or_table() {
        let b = "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/01-ab";
        assert_eq!(only("DescribeBackup", json!({ "BackupArn": b })), b);
        assert_eq!(only("DeleteBackup", json!({ "BackupArn": b })), b);
        assert_eq!(
            only(
                "CreateBackup",
                json!({ "TableName": "bb", "BackupName": "x" })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb"
        );
    }

    #[test]
    fn query_scan_on_index_use_index_arn() {
        assert_eq!(
            only("Query", json!({ "TableName": "bb", "IndexName": "gsi1" })),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/index/gsi1"
        );
        assert_eq!(
            only("Scan", json!({ "TableName": "bb", "IndexName": "lsi1" })),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/index/lsi1"
        );
        assert_eq!(
            only("Query", json!({ "TableName": "bb" })),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb"
        );
    }

    #[test]
    fn table_name_may_be_an_arn() {
        let arn = "arn:aws:dynamodb:us-east-1:964157134968:table/bb";
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
            "arn:aws:dynamodb:us-east-1:964157134968:table/*"
        );
        assert_eq!(only("DescribeLimits", json!({})), "*");
    }

    #[test]
    fn restore_export_import_use_measured_resource() {
        let backup = "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/01-ab";
        // RestoreTableFromBackup: the restore action on the backup ARN plus the
        // seven item actions on the target table.
        let rfb = authz_targets(
            "RestoreTableFromBackup",
            &json!({ "TargetTableName": "restored", "BackupArn": backup }),
            R,
            A,
        );
        assert!(rfb.contains(&("RestoreTableFromBackup".to_owned(), backup.to_owned())));
        let target = "arn:aws:dynamodb:us-east-1:964157134968:table/restored".to_owned();
        for a in RESTORE_TARGET_ACTIONS {
            assert!(
                rfb.contains(&((*a).to_owned(), target.clone())),
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
        assert!(pitr.contains(&(
            "RestoreTableToPointInTime".to_owned(),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb".to_owned()
        )));
        let ptarget = "arn:aws:dynamodb:us-east-1:964157134968:table/pitr".to_owned();
        for a in RESTORE_TARGET_ACTIONS {
            assert!(
                pitr.contains(&((*a).to_owned(), ptarget.clone())),
                "RestoreTableToPointInTime missing {a} on target"
            );
        }
        assert_eq!(pitr.len(), 1 + RESTORE_TARGET_ACTIONS.len());

        // ExportTableToPointInTime: only the export action on the source table.
        assert_eq!(
            only(
                "ExportTableToPointInTime",
                json!({ "TableArn": "arn:aws:dynamodb:us-east-1:964157134968:table/bb", "S3Bucket": "x" })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb"
        );
        // ImportTable: only the import action on the new table.
        assert_eq!(
            only(
                "ImportTable",
                json!({ "TableCreationParameters": { "TableName": "imp" } })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/imp"
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
        let index = "arn:aws:dynamodb:us-east-1:964157134968:table/bb/index/gsi1";
        assert!(matches!(
            validate_tag_resource_arn_shape(index),
            Err(DynamoDbError::ValidationException(ref m))
                if m.contains("only table or stream is accepted") && m.contains(index)
        ));
        for backup in [
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/01792513080555-82c5bd04",
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb/backup/01-ab",
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
            validate_tag_resource_arn_shape("arn:aws:dynamodb:us-east-1:964157134968:table/bb")
                .is_ok()
        );
        assert!(
            validate_tag_resource_arn_shape(
                "arn:aws:dynamodb:us-east-1:964157134968:table/bb/stream/2026-10-20T16:17:40.188"
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
            validate_tag_resource_arn_shape("arn:aws:dynamodb:us-east-1:964157134968:banana/x");
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
            validate_tag_resource_arn_shape("arn:aws-cn:dynamodb:us-east-1:964157134968:table/bb")
                .is_ok()
        );
        assert!(
            validate_tag_resource_arn_shape("arn:aws:s3:us-east-1:964157134968:table/bb").is_ok()
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
                let c = canonical("us-east-1", "964157134968", rid);
                assert!(
                    c.starts_with("arn:aws:dynamodb:us-east-1:964157134968:table/"),
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
            validate_operation_arns("ExportTableToPointInTime", &json!({ "TableArn": a1 })),
            Err(DynamoDbError::ValidationException(ref m)) if m == INVALID_EXPORT_TABLE_ARN_MESSAGE
        ));
    }

    #[test]
    fn validate_operation_arns_matches_measured_live_behaviour() {
        let err_msg = |op: &str, input: Value| match validate_operation_arns(op, &input) {
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Ok(()) => None,
            other => panic!("unexpected {other:?}"),
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
        assert_eq!(
            err_msg(
                "DescribeStream",
                json!({ "StreamArn": "arn:aws:dynamodb:us-east-1:1:table/t/stream/2026-10-20T16:17:40.188" })
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
        // A current token authorizes against its canonical stream ARN.
        let stream = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188";
        let tok = extenddb_engine::encode_shard_iterator(
            "shardId-1",
            "AFTER_SEQUENCE_NUMBER",
            "42",
            1000,
            stream,
        );
        match getrecords_resource(&json!({ "ShardIterator": tok }), R, A) {
            GetRecordsResource::Stream(arn) => assert_eq!(
                arn,
                "arn:aws:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188"
            ),
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
        let a = "964157134968";
        assert_eq!(key_lookup_name("orders", r, a), "orders");
        assert_eq!(
            key_lookup_name("arn:aws:dynamodb:us-east-1:964157134968:table/orders", r, a),
            "orders"
        );
        for other in [
            "arn:aws:dynamodb:us-east-1:111111111111:table/orders",
            "arn:aws:dynamodb:eu-west-1:964157134968:table/orders",
            "arn:aws-cn:dynamodb:us-east-1:964157134968:table/orders",
            "arn:aws:s3:us-east-1:964157134968:table/orders",
            "arn:aws:dynamodb:us-east-1:964157134968:table/orders/index/g1",
            "arn:aws:dynamodb:us-east-1:964157134968:backup/orders",
            "arn:aws:dynamodb",
        ] {
            assert_eq!(key_lookup_name(other, r, a), other, "{other}");
        }
    }
}
