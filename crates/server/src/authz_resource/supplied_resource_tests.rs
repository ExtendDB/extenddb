// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The denial names the resource as the caller supplied it (issue #447).

use super::{AuthzTarget, authz_targets, check_resource_account, table_target};
use extenddb_core::error::DynamoDbError;
use serde_json::json;

const R: &str = "us-east-1";
const A: &str = "964157134968";
const CANON: &str = "arn:aws:dynamodb:us-east-1:964157134968:table/bb-arn-main";

fn single(op: &str, input: serde_json::Value) -> AuthzTarget {
    let mut t = authz_targets(op, &input, R, A);
    assert_eq!(t.len(), 1, "{op}: {t:?}");
    t.remove(0)
}

/// The three TagResource spellings the service was measured with
/// (2026-10-20, no-policy caller): the policy match uses the canonical ARN,
/// the denial names the ARN as written.
#[test]
fn tag_resource_arn_respelled_fields_are_matched_canonically_and_named_as_written() {
    for supplied in [
        "arn:aws-cn:dynamodb:us-east-1:964157134968:table/bb-arn-main",
        "arn:aws:s3:us-east-1:964157134968:table/bb-arn-main",
        "arn:aws:dynamodb:us-west-2:964157134968:table/bb-arn-main",
    ] {
        for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
            let t = single(op, json!({ "ResourceArn": supplied }));
            assert_eq!(t.resource, CANON, "{op} {supplied}");
            assert_eq!(t.supplied, supplied, "{op} {supplied}");
        }
    }
}

/// A canonical ARN or a bare name names the canonical ARN in both roles.
#[test]
fn canonical_arn_and_bare_name_name_the_canonical_arn() {
    let t = single("TagResource", json!({ "ResourceArn": CANON }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    let t = single("GetItem", json!({ "TableName": "bb-arn-main" }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    let t = single("GetItem", json!({ "TableName": CANON }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    // A foreign-service ARN that is not a table resource is left verbatim
    // in both roles, so authorization denies it and names it as written.
    let t = single(
        "TagResource",
        json!({ "ResourceArn": "arn:aws:s3:::bucket" }),
    );
    assert_eq!(t.resource, "arn:aws:s3:::bucket");
    assert_eq!(t.supplied, "arn:aws:s3:::bucket");
}

/// Every other ARN-bearing field keeps the caller's spelling for the
/// denial while matching the canonical resource.
#[test]
fn other_arn_fields_keep_the_supplied_spelling() {
    let respelled_table = "arn:aws:dynamodb:us-west-2:964157134968:table/bb-arn-main";
    for op in [
        "GetItem",
        "PutItem",
        "Query",
        "Scan",
        "DescribeTable",
        "CreateBackup",
        "DescribeContinuousBackups",
    ] {
        let t = single(op, json!({ "TableName": respelled_table }));
        assert_eq!(t.resource, CANON, "{op}");
        assert_eq!(t.supplied, respelled_table, "{op}");
    }
    let t = single(
        "Query",
        json!({ "TableName": respelled_table, "IndexName": "gsi1" }),
    );
    assert_eq!(t.resource, format!("{CANON}/index/gsi1"));
    assert_eq!(t.supplied, format!("{respelled_table}/index/gsi1"));

    let stream =
        "arn:aws-cn:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188";
    let t = single("DescribeStream", json!({ "StreamArn": stream }));
    assert_eq!(
        t.resource,
        "arn:aws:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188"
    );
    assert_eq!(t.supplied, stream);

    let backup = "arn:aws:dynamodb:eu-west-1:964157134968:table/T/backup/01-ab";
    let t = single(
        "DescribeBackup",
        json!({ "BackupArn": format!("{backup}\n") }),
    );
    assert_eq!(
        t.resource,
        "arn:aws:dynamodb:us-east-1:964157134968:table/T/backup/01-ab"
    );
    assert_eq!(
        t.supplied, backup,
        "the trimmed ARN, as the engine resolves it"
    );

    let t = single(
        "ExportTableToPointInTime",
        json!({ "TableArn": respelled_table, "S3Bucket": "x" }),
    );
    assert_eq!(t.resource, CANON);
    assert_eq!(t.supplied, respelled_table);

    let v = authz_targets(
        "RestoreTableToPointInTime",
        &json!({ "SourceTableArn": respelled_table, "TargetTableName": "pitr" }),
        R,
        A,
    );
    assert_eq!(v[0].resource, CANON);
    assert_eq!(v[0].supplied, respelled_table);
    let ptarget = "arn:aws:dynamodb:us-east-1:964157134968:table/pitr";
    assert!(
        v[1..]
            .iter()
            .all(|t| t.resource == ptarget && t.supplied == ptarget)
    );

    // Batch and transaction tables go through `table_target`.
    let t = table_target("BatchGetItem", respelled_table, R, A);
    assert_eq!(t.action, "BatchGetItem");
    assert_eq!(t.resource, CANON);
    assert_eq!(t.supplied, respelled_table);
    let t = table_target("GetItem", "bb-arn-main", R, A);
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
}

/// An ARN in TableName that is not a table resource ARN (an index ARN, a
/// malformed resource) names the canonical index ARN on an index query, so
/// the denial never names a resource the caller did not write.
#[test]
fn index_query_with_a_non_table_arn_names_the_canonical_index_arn() {
    for table_name in [
        "arn:aws:dynamodb:us-east-1:964157134968:table/bb-arn-main/index/gsi1",
        "arn:aws:dynamodb:us-east-1:964157134968:TABLE/bb-arn-main",
        "arn:aws:dynamodb:us-east-1:964157134968:table/x/table/bb-arn-main",
        "arn:aws:dynamodb:us-east-1:964157134968:banana/x",
    ] {
        for op in ["Query", "Scan"] {
            let t = single(op, json!({ "TableName": table_name, "IndexName": "gsi1" }));
            assert!(
                !t.supplied.contains("/index/gsi1/index/"),
                "{op} {table_name}: {}",
                t.supplied
            );
            assert_eq!(t.supplied, t.resource, "{op} {table_name}");
        }
    }
}

/// A table or stream ARN in another account is "Access is denied" for every
/// principal (measured 2026-10-20 with an admin and a no-policy caller);
/// anything else passes to authorization.
#[test]
fn other_account_resource_is_access_denied_before_authorization() {
    for arn in [
        "arn:aws:dynamodb:us-east-1:111122223333:table/bb-arn-main",
        "arn:aws:dynamodb:us-east-1:111122223333:table/bb-arn-main/stream/2026-10-20T16:17:40.188",
        "arn:aws-cn:dynamodb:us-west-2:111122223333:table/bb-arn-main",
    ] {
        let err = check_resource_account(arn, A).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::AccessDeniedException(m) if m == "Access is denied"),
            "{arn}: {err:?}"
        );
    }
    for value in [
        CANON,
        "arn:aws-cn:dynamodb:us-west-2:964157134968:table/bb-arn-main",
        "bb-arn-main",
        "arn:aws:s3:::bucket",
        "arn:aws:dynamodb:us-east-1:111122223333:banana/x",
        "banana",
    ] {
        assert!(check_resource_account(value, A).is_ok(), "{value}");
    }
}
