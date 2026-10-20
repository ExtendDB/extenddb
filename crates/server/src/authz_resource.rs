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
//! The service evaluates these resources (measured live, 2026-10-20, account
//! 964157134968):
//!
//! | Operation | IAM action | resource |
//! |---|---|---|
//! | `TagResource`/`UntagResource`/`ListTagsOfResource` | same | the `ResourceArn` (a table or stream ARN) |
//! | `DescribeStream`/`GetShardIterator` | same | the `StreamArn` |
//! | `ListStreams` (with `TableName`) | `ListStreams` | `table/<T>/stream/*` |
//! | `ListStreams` (no `TableName`) | `ListStreams` | `table/*/stream/*` |
//! | `DescribeBackup`/`DeleteBackup` | same | the `BackupArn` |
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

use serde_json::Value;

/// Partition and service are fixed to this deployment's; see module docs.
const PARTITION: &str = "aws";
const SERVICE: &str = "dynamodb";

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
/// `DynamoDB` ARN. Returns `None` for anything that is not a `table/...`
/// resource. The partition, service, region, and account are intentionally
/// ignored: only the resource identity is trusted from the input.
///
/// Stream labels contain `:` (e.g. `.../stream/2026-10-20T16:17:40.188`), so the
/// resource portion is taken as everything after the fifth `:`.
fn parse_resource_identity(arn: &str) -> Option<ResourceId> {
    let rest = arn.strip_prefix("arn:")?;
    // partition : service : region : account : resource(may contain ':')
    let mut it = rest.splitn(5, ':');
    let _partition = it.next()?;
    let _service = it.next()?;
    let _region = it.next()?;
    let _account = it.next()?;
    let resource = it.next()?;

    let tail = resource.strip_prefix("table/")?;
    let (table, after) = match tail.split_once('/') {
        Some((t, a)) => (t, Some(a)),
        None => (tail, None),
    };
    if table.is_empty() {
        return None;
    }
    let sub = match after {
        None => None,
        Some(a) => {
            // `a` is `<kind>/<id>`, e.g. `index/gsi1`, `stream/<label>` (the
            // label may contain ':'), or `backup/<id>`. A shape with no id, or
            // an unrecognized kind, is not a nameable resource.
            let (kind, id) = a.split_once('/')?;
            if id.is_empty() {
                return None;
            }
            match kind {
                "index" => Some(Sub::Index(id.to_owned())),
                "stream" => Some(Sub::Stream(id.to_owned())),
                "backup" => Some(Sub::Backup(id.to_owned())),
                _ => return None,
            }
        }
    };
    Some(ResourceId {
        table: table.to_owned(),
        sub,
    })
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

/// The canonical ARN for the resource a client-supplied ARN names, or the raw
/// string when it does not name a `DynamoDB` `table/...` resource. The raw
/// string lets an `Allow` on `*` reach the engine's own validation while never
/// producing a `table/*` fallback that a specific Deny would miss.
fn canonical_or_raw(arn: &str, region: &str, account_id: &str) -> String {
    match parse_resource_identity(arn) {
        Some(rid) => canonical(region, account_id, &rid),
        None => arn.to_owned(),
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
        // Resource lives in ResourceArn (a table or stream ARN).
        "TagResource" | "UntagResource" | "ListTagsOfResource" => one(canonical_or_raw(
            &str_field(input, "ResourceArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // Resource lives in StreamArn.
        "DescribeStream" | "GetShardIterator" => one(canonical_or_raw(
            &str_field(input, "StreamArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // GetRecords carries only an opaque shard iterator; the stream identity
        // is not in the request body (see report, Divergences kept). Authorize
        // against the account's stream space so an account-wide or action-wide
        // Deny is enforced.
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
        // Resource lives in BackupArn.
        "DescribeBackup" | "DeleteBackup" => one(canonical_or_raw(
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
        // ListBackups is scoped to a table when TableName is given, else account.
        "ListBackups" => match str_field(input, "TableName") {
            Some(t) => one(table_resource_arn(&t, region, account_id)),
            None => one(table_wildcard(region, account_id)),
        },
        // Export names the source table via TableArn.
        "ExportTableToPointInTime" => one(table_resource_arn(
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
        // RestoreTableFromBackup's primary action is evaluated on the source
        // backup ARN (measured live: the first permission the service requires
        // is dynamodb:RestoreTableFromBackup on the BackupArn). So a Deny on the
        // backup ARN blocks the restore.
        "RestoreTableFromBackup" => one(canonical_or_raw(
            &str_field(input, "BackupArn").unwrap_or_default(),
            region,
            account_id,
        )),
        // RestoreTableToPointInTime's primary action is evaluated on the source
        // table (SourceTableArn or SourceTableName).
        "RestoreTableToPointInTime" => {
            let source = str_field(input, "SourceTableArn")
                .or_else(|| str_field(input, "SourceTableName"))
                .unwrap_or_default();
            one(table_resource_arn(&source, region, account_id))
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
        // wildcard; DescribeLimits and DescribeEndpoints have no resource and
        // are evaluated against "*" (measured live).
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
        // RestoreTableFromBackup: primary action on the backup ARN.
        assert_eq!(
            only(
                "RestoreTableFromBackup",
                json!({ "TargetTableName": "restored", "BackupArn": backup })
            ),
            backup
        );
        // RestoreTableToPointInTime: on the source table.
        assert_eq!(
            only(
                "RestoreTableToPointInTime",
                json!({ "SourceTableName": "bb", "TargetTableName": "pitr" })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb"
        );
        // ExportTableToPointInTime: on the source table (TableArn field).
        assert_eq!(
            only(
                "ExportTableToPointInTime",
                json!({ "TableArn": "arn:aws:dynamodb:us-east-1:964157134968:table/bb", "S3Bucket": "x" })
            ),
            "arn:aws:dynamodb:us-east-1:964157134968:table/bb"
        );
        // ImportTable: on the new table.
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
}
