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
    // `tail` is `<table>[/<kind>/<id...>]`. Table names contain no `/`. A
    // trailing slash with nothing after it (`table/T/`) is treated as the bare
    // table, matching the engine tag handler. An empty sub-resource id
    // (`table/T/stream/`) is kept as an empty id, matching the engine stream
    // parser, so an input the engine accepts is never authorized verbatim here.
    let mut segs = tail.split('/');
    let table = segs.next()?;
    if table.is_empty() {
        return None;
    }
    let sub = match segs.next() {
        None | Some("") => None,
        Some(kind) => {
            let id = segs.collect::<Vec<&str>>().join("/");
            match kind {
                "index" => Some(Sub::Index(id)),
                "stream" => Some(Sub::Stream(id)),
                "backup" => Some(Sub::Backup(id)),
                // An unrecognized sub-resource kind is not a nameable resource.
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

fn backup_wildcard(region: &str, account_id: &str) -> String {
    format!("arn:{PARTITION}:{SERVICE}:{region}:{account_id}:table/*/backup/*")
}

/// The canonical stream ARN a GetRecords request reads, decoded from its shard
/// iterator token. Returns `None` when the iterator field is absent, does not
/// decode, or carries no stream ARN (a token issued by a build before the
/// stream ARN was carried, or a forged token), so `authorize_request` fails
/// closed rather than authorizing against a wildcard a specific-stream Deny
/// would miss.
pub(crate) fn getrecords_stream_resource(
    input: &Value,
    region: &str,
    account_id: &str,
) -> Option<String> {
    let token = input.get("ShardIterator").and_then(Value::as_str)?;
    let decoded = extenddb_engine::decode_shard_iterator(token)?;
    let stream_arn = decoded.stream_arn?;
    Some(canonical_or_raw(&stream_arn, region, account_id))
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
        // GetRecords carries only an opaque shard iterator, not the stream in a
        // body field, so it is handled in `authorize_request` by decoding the
        // iterator for the stream ARN and failing closed when it is absent.
        // `authz_targets` is never called for it; map it to the account stream
        // space as a defensive non-bypass resource should it ever be reached.
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
        // RestoreTableFromBackup requires dynamodb:RestoreTableFromBackup on the
        // source backup ARN and the item actions on the target table it writes
        // (measured live). A Deny on the backup ARN, or on any item action for
        // the target table, blocks the restore.
        "RestoreTableFromBackup" => {
            let mut v = vec![(
                operation.to_owned(),
                canonical_or_raw(
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

    // Differential corpus: every stream ARN the engine's parser accepts must be
    // canonicalized by the authorization parser to the same table and label.
    // If the engine accepted an input the authz parser rejected,
    // `canonical_or_raw` would authorize the raw string and a Deny on the real
    // resource would be skipped. The engine stream parser is the storage crate's
    // `parse_stream_arn`.
    #[test]
    fn authz_parser_canonicalizes_every_engine_accepted_stream_arn() {
        use extenddb_storage::util::parse_stream_arn;
        let corpus = [
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188",
            // respelled partition, service, region, account
            "arn:aws-cn:dynamodb:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:s3:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:eu-west-1:123:table/T/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:999999999999:table/T/stream/2026-10-20T16:17:40.188",
            // extra path segments and a label containing '/'
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/a/b",
            // trailing slash, empty label
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/",
            // uppercase resource type and leading whitespace (engine rejects)
            "arn:aws:dynamodb:us-east-1:123:Table/T/stream/x",
            "arn:aws:dynamodb:us-east-1:123: table/T/stream/x",
            // not a stream at all
            "arn:aws:dynamodb:us-east-1:123:table/T",
            "banana",
        ];
        for input in corpus {
            if let Ok((table, label)) = parse_stream_arn(input) {
                let rid = parse_resource_identity(input).unwrap_or_else(|| {
                    panic!("engine accepted {input} but authz parser rejected it")
                });
                assert_eq!(rid.table, table, "table mismatch for {input}");
                assert_eq!(
                    rid.sub,
                    Some(Sub::Stream(label.clone())),
                    "label mismatch for {input}"
                );
                assert_eq!(
                    canonical("us-east-1", "964157134968", &rid),
                    format!("arn:aws:dynamodb:us-east-1:964157134968:table/{table}/stream/{label}"),
                    "canonical mismatch for {input}"
                );
            }
        }
    }
}
