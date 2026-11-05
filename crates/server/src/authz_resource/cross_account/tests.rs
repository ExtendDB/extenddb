// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which resources name another account, and the answer for each.

use super::{
    MIXED_ACCOUNTS, OWNER_ONLY, OtherAccount, answer, find, principal_arn, refusal,
    resource_policy_denial,
};
use extenddb_auth::AuthIdentity;
use extenddb_core::error::DynamoDbError;
use serde_json::json;

const A: &str = "444455556666";
const OTHER: &str = "111122223333";
const OWN: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/t";
const FOREIGN: &str = "arn:aws:dynamodb:us-east-1:111122223333:table/t";
const STREAM: &str =
    "arn:aws:dynamodb:us-east-1:111122223333:table/t/stream/2026-10-20T16:17:40.188";
const BACKUP: &str =
    "arn:aws:dynamodb:us-east-1:111122223333:table/t/backup/01791976431438-73d8d5bc";

fn other(action: &str, resource: &str) -> Option<OtherAccount> {
    Some(OtherAccount::Resource {
        action: action.to_owned(),
        resource: resource.to_owned(),
        account: OTHER.to_owned(),
    })
}

#[test]
fn every_resource_arn_field_is_read() {
    for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
        assert_eq!(
            find(op, &json!({ "ResourceArn": FOREIGN }), A),
            other(op, FOREIGN)
        );
        assert_eq!(
            find(op, &json!({ "ResourceArn": STREAM }), A),
            other(op, STREAM)
        );
    }
    for op in ["DescribeStream", "GetShardIterator"] {
        assert_eq!(
            find(op, &json!({ "StreamArn": STREAM }), A),
            other(op, STREAM)
        );
    }
    for op in ["DescribeBackup", "DeleteBackup", "RestoreTableFromBackup"] {
        let input = json!({ "BackupArn": format!(" {BACKUP}\n"), "TargetTableName": "r" });
        assert_eq!(
            find(op, &input, A),
            other(op, BACKUP),
            "{op}: the trimmed ARN"
        );
    }
    let input = json!({ "TableArn": FOREIGN, "S3Bucket": "b" });
    assert_eq!(
        find("ExportTableToPointInTime", &input, A),
        other("ExportTableToPointInTime", FOREIGN)
    );
    let input = json!({ "SourceTableArn": FOREIGN, "TargetTableName": "r" });
    assert_eq!(
        find("RestoreTableToPointInTime", &input, A),
        other("RestoreTableToPointInTime", FOREIGN)
    );
    for op in [
        "GetItem",
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "DescribeTable",
        "UpdateTable",
        "DeleteTable",
        "CreateTable",
        "DescribeTimeToLive",
        "UpdateTimeToLive",
        "DescribeContinuousBackups",
        "UpdateContinuousBackups",
        "CreateBackup",
        "ListBackups",
        "Query",
        "Scan",
    ] {
        assert_eq!(
            find(op, &json!({ "TableName": FOREIGN }), A),
            other(op, FOREIGN),
            "{op}"
        );
    }
}

/// An index query names the index under the table ARN as written (measured:
/// `.../table/<T>/index/<I>` in the denial).
#[test]
fn an_index_query_names_the_index_arn() {
    for op in ["Query", "Scan"] {
        let input = json!({ "TableName": FOREIGN, "IndexName": "gsi1" });
        assert_eq!(
            find(op, &input, A),
            other(op, &format!("{FOREIGN}/index/gsi1"))
        );
    }
}

/// The caller's own resources, bare names, ARNs that are not a table resource
/// and fields the service reads as a name are left to authorization.
#[test]
fn own_account_names_and_name_fields_are_not_refused() {
    for value in [
        OWN,
        "arn:aws-cn:dynamodb:us-west-2:444455556666:table/t",
        "t",
        "arn:aws:s3:::bucket",
        "arn:aws:dynamodb:us-east-1:111122223333:banana/x",
        "",
    ] {
        assert_eq!(
            find("GetItem", &json!({ "TableName": value }), A),
            None,
            "{value}"
        );
        assert_eq!(
            find("TagResource", &json!({ "ResourceArn": value }), A),
            None,
            "{value}"
        );
    }
    assert_eq!(
        find("ListStreams", &json!({ "TableName": FOREIGN }), A),
        None
    );
    let input = json!({ "SourceTableName": FOREIGN, "TargetTableName": "r" });
    assert_eq!(find("RestoreTableToPointInTime", &input, A), None);
    let input = json!({ "TableCreationParameters": { "TableName": FOREIGN } });
    assert_eq!(find("ImportTable", &input, A), None);
    assert_eq!(find("ListTables", &json!({}), A), None);
}

/// Tables in more than one account, the caller's bare name included, are
/// `Mixed` in either order (measured 2026-11-03); tables in one other account
/// name the first, with the action each item is authorized for.
#[test]
fn batch_and_transaction_tables_across_accounts_are_mixed() {
    let key = json!({ "pk": { "S": "a" } });
    let batch = json!({ "RequestItems": { "t": { "Keys": [key] }, FOREIGN: { "Keys": [key] } } });
    assert_eq!(find("BatchGetItem", &batch, A), Some(OtherAccount::Mixed));
    for items in [
        json!([{ "Get": { "TableName": OWN, "Key": key } }, { "Get": { "TableName": FOREIGN, "Key": key } }]),
        json!([{ "Get": { "TableName": FOREIGN, "Key": key } }, { "Get": { "TableName": "t", "Key": key } }]),
    ] {
        let input = json!({ "TransactItems": items });
        assert_eq!(
            find("TransactGetItems", &input, A),
            Some(OtherAccount::Mixed)
        );
    }
    let third = "arn:aws:dynamodb:us-east-1:777788889999:table/t";
    let batch = json!({ "RequestItems": { FOREIGN: [{ "PutRequest": { "Item": key } }], third: [{ "PutRequest": { "Item": key } }] } });
    assert_eq!(find("BatchWriteItem", &batch, A), Some(OtherAccount::Mixed));

    let batch = json!({ "RequestItems": { FOREIGN: [{ "PutRequest": { "Item": key } }] } });
    assert_eq!(
        find("BatchWriteItem", &batch, A),
        other("BatchWriteItem", FOREIGN)
    );
    let input = json!({ "TransactItems": [
        { "ConditionCheck": { "TableName": FOREIGN, "Key": key, "ConditionExpression": "attribute_exists(pk)" } },
        { "Put": { "TableName": FOREIGN, "Item": key } },
    ] });
    assert_eq!(
        find("TransactWriteItems", &input, A),
        other("ConditionCheckItem", FOREIGN)
    );
    let input = json!({ "TransactItems": [{ "Update": { "TableName": FOREIGN, "Key": key } }] });
    assert_eq!(
        find("TransactWriteItems", &input, A),
        other("UpdateItem", FOREIGN)
    );
    let batch = json!({ "RequestItems": { "t": { "Keys": [key] }, OWN: { "Keys": [key] } } });
    assert_eq!(find("BatchGetItem", &batch, A), None);
}

fn message(err: &DynamoDbError) -> (&'static str, String) {
    match err {
        DynamoDbError::AccessDeniedException(m) => ("AccessDeniedException", m.clone()),
        DynamoDbError::ValidationException(m) => ("ValidationException", m.clone()),
        other => panic!("unexpected {other:?}"),
    }
}

/// The answer for each operation family, for an account the catalog holds
/// and for one it does not (measured 2026-11-03, admin and no-policy caller).
#[test]
fn the_answer_depends_on_the_operation_and_the_account() {
    let p = "arn:aws:iam::444455556666:user/u";
    let long = |action: &str, resource: &str| {
        (
            "AccessDeniedException",
            format!(
                "User: {p} is not authorized to perform: dynamodb:{action} on resource: \
                 {resource} because no resource-based policy allows the dynamodb:{action} action"
            ),
        )
    };
    let access = ("AccessDeniedException", "Access is denied".to_owned());
    let caller = (
        "AccessDeniedException",
        "The caller is not authorized to access the specified resource.".to_owned(),
    );
    let owner = ("ValidationException", OWNER_ONLY.to_owned());
    for op in [
        "TagResource",
        "UntagResource",
        "ListTagsOfResource",
        "DescribeTable",
        "UpdateTable",
        "DeleteTable",
        "BatchWriteItem",
        "TransactGetItems",
        "TransactWriteItems",
        "DescribeStream",
        "GetShardIterator",
        "SearchVectors",
    ] {
        assert_eq!(
            message(&refusal(op, op, FOREIGN, true, p)),
            long(op, FOREIGN),
            "{op}"
        );
        assert_eq!(message(&refusal(op, op, FOREIGN, false, p)), access, "{op}");
    }
    for op in [
        "GetItem",
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "Query",
        "Scan",
        "BatchGetItem",
    ] {
        assert_eq!(
            message(&refusal(op, op, FOREIGN, true, p)),
            long(op, FOREIGN),
            "{op}"
        );
        assert_eq!(message(&refusal(op, op, FOREIGN, false, p)), caller, "{op}");
    }
    for op in [
        "CreateTable",
        "DescribeTimeToLive",
        "UpdateTimeToLive",
        "DescribeContinuousBackups",
        "UpdateContinuousBackups",
        "CreateBackup",
        "ListBackups",
        "DescribeBackup",
        "DeleteBackup",
        "RestoreTableFromBackup",
        "ExportTableToPointInTime",
    ] {
        assert_eq!(message(&refusal(op, op, FOREIGN, true, p)), owner, "{op}");
        assert_eq!(message(&refusal(op, op, FOREIGN, false, p)), access, "{op}");
    }
    for known in [true, false] {
        let err = refusal(
            "RestoreTableToPointInTime",
            "RestoreTableToPointInTime",
            FOREIGN,
            known,
            p,
        );
        assert_eq!(message(&err), owner);
    }
    // A transaction item names its own action.
    assert_eq!(
        message(&refusal(
            "TransactWriteItems",
            "ConditionCheckItem",
            FOREIGN,
            true,
            p
        )),
        long("ConditionCheckItem", FOREIGN)
    );
}

/// An account the catalog holds gets the ARN shape checks before the refusal;
/// one it does not hold is refused first (measured 2026-11-03 with an index
/// ARN in `ResourceArn` and a `StreamArn` in another partition).
#[test]
fn shape_checks_come_first_only_for_a_known_account() {
    let p = "arn:aws:iam::444455556666:user/u";
    let access = ("AccessDeniedException", "Access is denied".to_owned());
    // A table ARN where a stream ARN belongs.
    let input = json!({ "StreamArn": FOREIGN });
    let unknown = answer(
        "DescribeStream",
        &input,
        "DescribeStream",
        FOREIGN,
        false,
        p,
    );
    assert_eq!(message(&unknown), access);
    let known = answer("DescribeStream", &input, "DescribeStream", FOREIGN, true, p);
    assert_eq!(
        message(&known),
        ("ValidationException", "Invalid StreamArn".to_owned())
    );
    let index = format!("{FOREIGN}/index/gsi1");
    let input = json!({ "ResourceArn": index });
    let unknown = answer(
        "ListTagsOfResource",
        &input,
        "ListTagsOfResource",
        &index,
        false,
        p,
    );
    assert_eq!(message(&unknown), access);
    let (code, msg) = message(&answer(
        "ListTagsOfResource",
        &input,
        "ListTagsOfResource",
        &index,
        true,
        p,
    ));
    assert_eq!(code, "ValidationException");
    assert!(msg.contains("only table or stream is accepted"), "{msg}");
    // A well-formed stream ARN passes the shape check and gets the denial.
    let input = json!({ "StreamArn": STREAM });
    let known = answer("DescribeStream", &input, "DescribeStream", STREAM, true, p);
    assert_eq!(
        message(&known).1,
        resource_policy_denial(p, "DescribeStream", STREAM)
    );
}

/// The service writes the apostrophe in "owner's" as U+2019, and the mixed
/// account message as measured.
#[test]
fn messages_are_the_service_text() {
    assert_eq!(
        OWNER_ONLY,
        "This action is only supported by accounts that match the resource owner\u{2019}s account."
    );
    assert_eq!(
        MIXED_ACCOUNTS,
        "Invalid request! All tables in the request must be from a single account."
    );
    assert_eq!(
        resource_policy_denial("P", "GetItem", FOREIGN),
        format!(
            "User: P is not authorized to perform: dynamodb:GetItem on resource: {FOREIGN} \
             because no resource-based policy allows the dynamodb:GetItem action"
        )
    );
}

/// A role session is named by its STS ARN, as the service names it.
#[test]
fn principal_is_named_as_the_service_names_it() {
    let user = AuthIdentity::User {
        account_id: A.to_owned(),
        user_name: "u".to_owned(),
    };
    assert_eq!(principal_arn(&user), "arn:aws:iam::444455556666:user/u");
    let role = AuthIdentity::RoleSession {
        account_id: A.to_owned(),
        role_name: "r".to_owned(),
        session_name: "s".to_owned(),
    };
    assert_eq!(
        principal_arn(&role),
        "arn:aws:sts::444455556666:assumed-role/r/s"
    );
}
