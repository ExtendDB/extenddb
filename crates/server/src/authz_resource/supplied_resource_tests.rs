// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The denial names the resource as the caller supplied it (issue #447).

use super::{AuthzTarget, authz_targets, table_target};
use serde_json::json;

const R: &str = "us-east-1";
const A: &str = "444455556666";
const CANON: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/bb-arn-main";

fn single(op: &str, input: serde_json::Value) -> AuthzTarget {
    let mut t = authz_targets(op, &input, R, A);
    assert_eq!(t.len(), 1, "{op}: {t:?}");
    t.remove(0)
}

/// The three TagResource spellings the service was measured with: the
/// policies are matched against the ARN as written (measured 2026-11-03 with
/// a caller allowed only on the canonical ARN), the resource tags are read
/// under the canonical ARN, and the denial names the ARN as written
/// (measured 2026-10-20, no-policy caller).
#[test]
fn tag_resource_arn_respelled_fields_are_matched_and_named_as_written() {
    for supplied in [
        "arn:aws-cn:dynamodb:us-east-1:444455556666:table/bb-arn-main",
        "arn:aws:s3:us-east-1:444455556666:table/bb-arn-main",
        "arn:aws:dynamodb:us-west-2:444455556666:table/bb-arn-main",
    ] {
        for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
            let t = single(op, json!({ "ResourceArn": supplied }));
            assert_eq!(t.resource, CANON, "{op} {supplied}");
            assert_eq!(t.supplied, supplied, "{op} {supplied}");
            assert_eq!(t.policy_resource(), supplied, "{op} {supplied}");
        }
    }
    let stream =
        "arn:aws-cn:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188";
    let t = single("TagResource", json!({ "ResourceArn": stream }));
    assert_eq!(
        t.resource,
        "arn:aws:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188"
    );
    assert_eq!(t.policy_resource(), stream);
}

/// A canonical ARN or a bare name names the canonical ARN in every role.
#[test]
fn canonical_arn_and_bare_name_name_the_canonical_arn() {
    let t = single("TagResource", json!({ "ResourceArn": CANON }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    assert_eq!(t.policy_resource(), CANON);
    let t = single("GetItem", json!({ "TableName": "bb-arn-main" }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    let t = single("GetItem", json!({ "TableName": CANON }));
    assert_eq!((t.resource.as_str(), t.supplied.as_str()), (CANON, CANON));
    // A foreign-service ARN that is not a table resource is left verbatim
    // in every role, so authorization denies it and names it as written.
    let t = single(
        "TagResource",
        json!({ "ResourceArn": "arn:aws:s3:::bucket" }),
    );
    assert_eq!(t.resource, "arn:aws:s3:::bucket");
    assert_eq!(t.supplied, "arn:aws:s3:::bucket");
    assert_eq!(t.policy_resource(), "arn:aws:s3:::bucket");
}

/// Every other ARN-bearing field keeps the caller's spelling for the
/// denial while matching the canonical resource.
#[test]
fn other_arn_fields_keep_the_supplied_spelling() {
    let respelled_table = "arn:aws:dynamodb:us-west-2:444455556666:table/bb-arn-main";
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
        assert_eq!(t.policy_resource(), CANON, "{op}");
    }
    let t = single(
        "Query",
        json!({ "TableName": respelled_table, "IndexName": "gsi1" }),
    );
    assert_eq!(t.resource, format!("{CANON}/index/gsi1"));
    assert_eq!(t.supplied, format!("{respelled_table}/index/gsi1"));
    assert_eq!(t.policy_resource(), format!("{CANON}/index/gsi1"));

    let stream =
        "arn:aws-cn:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188";
    let t = single("DescribeStream", json!({ "StreamArn": stream }));
    assert_eq!(
        t.resource,
        "arn:aws:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188"
    );
    assert_eq!(t.supplied, stream);
    assert_eq!(t.policy_resource(), t.resource);

    let backup = "arn:aws:dynamodb:eu-west-1:444455556666:table/T/backup/01-ab";
    let t = single(
        "DescribeBackup",
        json!({ "BackupArn": format!("{backup}\n") }),
    );
    assert_eq!(
        t.resource,
        "arn:aws:dynamodb:us-east-1:444455556666:table/T/backup/01-ab"
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
    // The item actions on the target come first and the restore action on
    // the source last (issue #446).
    let (source, target) = v.split_last().expect("restore targets");
    assert_eq!(source.resource, CANON);
    assert_eq!(source.supplied, respelled_table);
    let ptarget = "arn:aws:dynamodb:us-east-1:444455556666:table/pitr";
    assert!(
        target
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
        "arn:aws:dynamodb:us-east-1:444455556666:table/bb-arn-main/index/gsi1",
        "arn:aws:dynamodb:us-east-1:444455556666:TABLE/bb-arn-main",
        "arn:aws:dynamodb:us-east-1:444455556666:table/x/table/bb-arn-main",
        "arn:aws:dynamodb:us-east-1:444455556666:banana/x",
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

/// The service reads ImportTable's new table name as a name even when it is
/// an ARN, and authorizes `table/<value as written>` in the caller's account
/// (measured 2026-11-03 with another account's table ARN as the name).
#[test]
fn import_table_name_is_a_name_even_when_it_is_an_arn() {
    let value = "arn:aws:dynamodb:us-east-1:111122223333:table/x";
    let t = single(
        "ImportTable",
        json!({ "TableCreationParameters": { "TableName": value } }),
    );
    let expected = format!("arn:aws:dynamodb:us-east-1:444455556666:table/{value}");
    assert_eq!(
        (t.resource.as_str(), t.supplied.as_str()),
        (expected.as_str(), expected.as_str())
    );
    assert_eq!(t.policy_resource(), expected);
}
