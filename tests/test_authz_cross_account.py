# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A resource ARN in another account, and the tag operations' policy match.

ExtendDB has no resource-based policies, so a request that names a resource in
another account by ARN is refused before the IAM decision. The answer matches
DynamoDB, which gives a caller allowed dynamodb:* on * and a caller with no
policy the same answer, chosen by the operation and by whether the account
exists (measured 2026-11-03 in us-east-1 with an account that exists and with
999999999999, which does not). Here an account exists when the deployment's
catalog holds it: the tests create one and use another that is never created.

The tag operations match their policies against the ResourceArn as written,
as the service does: a caller allowed only on the canonical table ARN is denied
the aws-cn, s3 and us-west-2 spellings of it (measured 2026-11-03).

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}
NO_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}],
}
OWNER_ONLY = (
    "ValidationException",
    "This action is only supported by accounts that match the resource owner\u2019s account.",
)
ACCESS_IS_DENIED = ("AccessDeniedException", "Access is denied")
CALLER_NOT_AUTHORIZED = (
    "AccessDeniedException",
    "The caller is not authorized to access the specified resource.",
)
MIXED_ACCOUNTS = (
    "ValidationException",
    "Invalid request! All tables in the request must be from a single account.",
)
KEY = {"pk": {"S": "p1"}}


def _client(endpoint: str, ak: str, sk: str, region: str, service: str = "dynamodb") -> Any:
    kwargs: dict = dict(
        service_name=service,
        endpoint_url=endpoint,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _account(mgmt: ManagementClient, label: str) -> str:
    acct = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct, f"{label}-{acct}")
    assert resp.status_code == 201, resp.text
    return acct


@pytest.fixture(scope="module")
def env():
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: cross-account authz tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    caller = _account(mgmt, "xacct-caller")
    known = _account(mgmt, "xacct-known")
    # Never created, so the catalog does not hold it.
    unknown = f"{uuid.uuid4().int % 10**12:012d}"
    yield {"endpoint": endpoint, "mgmt": mgmt, "region": region,
           "caller": caller, "known": known, "unknown": unknown}
    mgmt.delete_account(known)
    mgmt.delete_account(caller)


def _principal(env: dict, policy: dict, service: str = "dynamodb") -> Any:
    mgmt, caller = env["mgmt"], env["caller"]
    user = f"u{uuid.uuid4().hex[:10]}"
    assert mgmt.create_user(caller, user, "TestPass123!").status_code == 201
    assert mgmt.put_user_policy(caller, user, "p", policy).status_code == 204
    resp = mgmt.create_access_key(caller, user)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return {
        name: _client(env["endpoint"], creds["access_key_id"],
                      creds["secret_access_key"], env["region"], name)
        for name in ("dynamodb", "dynamodbstreams")
    }


@pytest.fixture(scope="module")
def table(env):
    """A table in the caller's account with a GSI, a stream and a backup."""
    admin = _principal(env, ALLOW_ALL)["dynamodb"]
    name = f"t{uuid.uuid4().hex[:8]}"
    admin.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g1pk", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "g1pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    wait_for_active(admin, name)
    desc = admin.describe_table(TableName=name)["Table"]
    backup_arn = admin.create_backup(TableName=name, BackupName="xbk")["BackupDetails"]["BackupArn"]
    return {
        "name": name,
        "table_arn": desc["TableArn"],
        "stream_label": desc["LatestStreamArn"].split("/stream/", 1)[1],
        "backup_id": backup_arn.split("/backup/", 1)[1],
    }


def _error(fn) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc:
        fn()
    err = exc.value.response["Error"]
    return err["Code"], err.get("Message", "")


def _requests(t: str, table: dict) -> list[tuple[str, str, str, str, Any]]:
    """(label, family, action, resource the denial names, call) per request.

    Families: "table" answers the resource-based-policy denial for an account
    that exists and "Access is denied" for one that does not; "item" answers
    the same denial and "The caller is not authorized ..."; "owner" answers
    the owner-only ValidationException and "Access is denied"; "pitr" answers
    the owner-only ValidationException for both.
    """
    stream = f"{t}/stream/{table['stream_label']}"
    backup = f"{t}/backup/{table['backup_id']}"
    put = {"Item": {"pk": {"S": "p1"}}}
    schema = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    return [
        ("TagResource", "table", "TagResource", t,
         lambda c: c["dynamodb"].tag_resource(ResourceArn=t, Tags=[{"Key": "k", "Value": "v"}])),
        ("UntagResource", "table", "UntagResource", t,
         lambda c: c["dynamodb"].untag_resource(ResourceArn=t, TagKeys=["k"])),
        ("ListTagsOfResource", "table", "ListTagsOfResource", t,
         lambda c: c["dynamodb"].list_tags_of_resource(ResourceArn=t)),
        ("TagResource stream", "table", "TagResource", stream,
         lambda c: c["dynamodb"].tag_resource(ResourceArn=stream, Tags=[{"Key": "k", "Value": "v"}])),
        ("GetItem", "item", "GetItem", t,
         lambda c: c["dynamodb"].get_item(TableName=t, Key=KEY)),
        ("PutItem", "item", "PutItem", t,
         lambda c: c["dynamodb"].put_item(TableName=t, **put)),
        ("UpdateItem", "item", "UpdateItem", t,
         lambda c: c["dynamodb"].update_item(
             TableName=t, Key=KEY, UpdateExpression="SET v = :v",
             ExpressionAttributeValues={":v": {"S": "x"}})),
        ("Query", "item", "Query", t,
         lambda c: c["dynamodb"].query(
             TableName=t, KeyConditionExpression="pk = :p",
             ExpressionAttributeValues={":p": {"S": "p1"}})),
        ("Query index", "item", "Query", f"{t}/index/gsi1",
         lambda c: c["dynamodb"].query(
             TableName=t, IndexName="gsi1", KeyConditionExpression="g1pk = :p",
             ExpressionAttributeValues={":p": {"S": "g1"}})),
        ("Scan", "item", "Scan", t, lambda c: c["dynamodb"].scan(TableName=t)),
        ("BatchGetItem", "item", "BatchGetItem", t,
         lambda c: c["dynamodb"].batch_get_item(RequestItems={t: {"Keys": [KEY]}})),
        ("DescribeTable", "table", "DescribeTable", t,
         lambda c: c["dynamodb"].describe_table(TableName=t)),
        ("BatchWriteItem", "table", "BatchWriteItem", t,
         lambda c: c["dynamodb"].batch_write_item(RequestItems={t: [{"PutRequest": put}]})),
        ("TransactGetItems", "table", "GetItem", t,
         lambda c: c["dynamodb"].transact_get_items(
             TransactItems=[{"Get": {"TableName": t, "Key": KEY}}])),
        ("TransactWriteItems", "table", "PutItem", t,
         lambda c: c["dynamodb"].transact_write_items(
             TransactItems=[{"Put": {"TableName": t, **put}}])),
        ("DescribeStream", "table", "DescribeStream", stream,
         lambda c: c["dynamodbstreams"].describe_stream(StreamArn=stream)),
        ("GetShardIterator", "table", "GetShardIterator", stream,
         lambda c: c["dynamodbstreams"].get_shard_iterator(
             StreamArn=stream, ShardId="shardId-00000001700000000000-00000000",
             ShardIteratorType="TRIM_HORIZON")),
        ("DescribeBackup", "owner", "", "",
         lambda c: c["dynamodb"].describe_backup(BackupArn=backup)),
        ("RestoreTableFromBackup", "owner", "", "",
         lambda c: c["dynamodb"].restore_table_from_backup(
             BackupArn=backup, TargetTableName=f"r{uuid.uuid4().hex[:8]}")),
        ("CreateBackup", "owner", "", "",
         lambda c: c["dynamodb"].create_backup(TableName=t, BackupName="xbk")),
        ("ListBackups", "owner", "", "",
         lambda c: c["dynamodb"].list_backups(TableName=t)),
        ("DescribeContinuousBackups", "owner", "", "",
         lambda c: c["dynamodb"].describe_continuous_backups(TableName=t)),
        ("DescribeTimeToLive", "owner", "", "",
         lambda c: c["dynamodb"].describe_time_to_live(TableName=t)),
        ("CreateTable", "owner", "", "",
         lambda c: c["dynamodb"].create_table(TableName=t, **schema)),
        ("ExportTableToPointInTime", "owner", "", "",
         lambda c: c["dynamodb"].export_table_to_point_in_time(TableArn=t, S3Bucket="b")),
        ("RestoreTableToPointInTime", "pitr", "", "",
         lambda c: c["dynamodb"].restore_table_to_point_in_time(
             SourceTableArn=t, TargetTableName=f"p{uuid.uuid4().hex[:8]}")),
    ]


@pytest.mark.parametrize("policy", [NO_POLICY, ALLOW_ALL], ids=["no-policy", "admin"])
def test_other_account_answer_depends_on_operation_and_account(env, table, policy):
    clients = _principal(env, policy)
    region, caller = env["region"], env["caller"]
    for known, account in ((True, env["known"]), (False, env["unknown"])):
        t = f"arn:aws:dynamodb:{region}:{account}:table/{table['name']}"
        for label, family, action, resource, call in _requests(t, table):
            code, msg = _error(lambda: call(clients))
            where = (label, "known" if known else "unknown", code, msg)
            if family == "pitr" or (family == "owner" and known):
                assert (code, msg) == OWNER_ONLY, where
            elif known:
                assert code == "AccessDeniedException", where
                assert msg.startswith(f"User: arn:aws:iam::{caller}:user/"), where
                assert msg.endswith(
                    f" is not authorized to perform: dynamodb:{action} on resource: "
                    f"{resource} because no resource-based policy allows the "
                    f"dynamodb:{action} action"
                ), where
            elif family == "item":
                assert (code, msg) == CALLER_NOT_AUTHORIZED, where
            else:
                assert (code, msg) == ACCESS_IS_DENIED, where


@pytest.mark.parametrize("policy", [NO_POLICY, ALLOW_ALL], ids=["no-policy", "admin"])
def test_tables_in_more_than_one_account_are_refused(env, table, policy):
    d = _principal(env, policy)["dynamodb"]
    region = env["region"]
    for account in (env["known"], env["unknown"]):
        t = f"arn:aws:dynamodb:{region}:{account}:table/{table['name']}"
        own = table["name"]
        calls = [
            lambda: d.batch_get_item(RequestItems={own: {"Keys": [KEY]}, t: {"Keys": [KEY]}}),
            lambda: d.transact_get_items(TransactItems=[
                {"Get": {"TableName": table["table_arn"], "Key": KEY}},
                {"Get": {"TableName": t, "Key": KEY}}]),
            lambda: d.transact_get_items(TransactItems=[
                {"Get": {"TableName": t, "Key": KEY}},
                {"Get": {"TableName": own, "Key": KEY}}]),
        ]
        for call in calls:
            assert _error(call) == MIXED_ACCOUNTS, account


def test_shape_checks_run_before_a_known_accounts_denial(env, table):
    # Measured 2026-11-03: an index or backup ARN in ResourceArn gets the
    # shape ValidationException in an account that exists and "Access is
    # denied" in one that does not. The service answers a StreamArn in another
    # partition or region the same way; ExtendDB does not check that yet
    # (#451), so the StreamArn case below uses a table ARN, which its
    # StreamArn check refuses. That input was not sent to the service with
    # another account's ARN.
    clients = _principal(env, NO_POLICY)
    d, streams = clients["dynamodb"], clients["dynamodbstreams"]
    region = env["region"]
    known = f"arn:aws:dynamodb:{region}:{env['known']}:table/{table['name']}"
    unknown = f"arn:aws:dynamodb:{region}:{env['unknown']}:table/{table['name']}"
    code, msg = _error(lambda: d.list_tags_of_resource(ResourceArn=f"{known}/index/gsi1"))
    assert code == "ValidationException" and "only table or stream is accepted" in msg, msg
    backup = f"{known}/backup/{table['backup_id']}"
    code, msg = _error(lambda: d.list_tags_of_resource(ResourceArn=backup))
    assert code == "ValidationException" and "Provided Arn is not a DynamoDB resource arn" in msg, msg
    for arn in (f"{unknown}/index/gsi1", f"{unknown}/backup/{table['backup_id']}"):
        assert _error(lambda a=arn: d.list_tags_of_resource(ResourceArn=a)) == ACCESS_IS_DENIED, arn
    # A table ARN where a stream ARN belongs.
    assert _error(lambda: streams.describe_stream(StreamArn=known)) == (
        "ValidationException", "Invalid StreamArn")
    assert _error(lambda: streams.describe_stream(StreamArn=unknown)) == ACCESS_IS_DENIED


def test_tag_operations_match_the_arn_as_written(env, table):
    # Measured 2026-11-03 with ListTagsOfResource for a caller allowed
    # dynamodb:* on the canonical table ARN only: the canonical ARN is
    # allowed, and each respelling is denied naming the ARN as written.
    canonical = table["table_arn"]
    d = _principal(env, {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "dynamodb:*", "Resource": canonical}]})["dynamodb"]
    d.list_tags_of_resource(ResourceArn=canonical)
    region = canonical.split(":")[3]
    other_region = "us-west-2" if region != "us-west-2" else "us-east-1"
    for spelled in (
        canonical.replace("arn:aws:", "arn:aws-cn:", 1),
        canonical.replace(":dynamodb:", ":s3:", 1),
        canonical.replace(f":{region}:", f":{other_region}:", 1),
    ):
        for op, call in (
            ("ListTagsOfResource", lambda a=spelled: d.list_tags_of_resource(ResourceArn=a)),
            ("TagResource", lambda a=spelled: d.tag_resource(
                ResourceArn=a, Tags=[{"Key": "k", "Value": "v"}])),
        ):
            code, msg = _error(call)
            assert code == "AccessDeniedException", (op, spelled, code, msg)
            assert msg.endswith(
                f"dynamodb:{op} on resource: {spelled} because no identity-based "
                f"policy allows the dynamodb:{op} action"
            ), (op, spelled, msg)


def test_list_streams_refuses_a_table_arn_as_its_table_name(env, table):
    # Measured 2026-11-03 (#796): ListStreams answers a table ARN in TableName
    # with the table name length text, before the IAM decision. These are the
    # measured rows: an admin with another account that exists or one that does
    # not, and a caller with no policy with either of those or its own account.
    message = "TableName must be at least 3 characters long and at most 255 characters long"
    rows = [
        (ALLOW_ALL, env["known"]),
        (ALLOW_ALL, env["unknown"]),
        (NO_POLICY, env["known"]),
        (NO_POLICY, env["unknown"]),
        (NO_POLICY, env["caller"]),
    ]
    for policy, account in rows:
        streams = _principal(env, policy)["dynamodbstreams"]
        value = f"arn:aws:dynamodb:{env['region']}:{account}:table/{table['name']}"
        code, msg = _error(lambda: streams.list_streams(TableName=value))
        assert (code, msg) == ("ValidationException", message), (policy, account, code, msg)


def test_import_table_name_is_read_as_a_name(env, table):
    # Measured 2026-11-03: the service authorizes ImportTable on
    # table/<value as written> in the caller's account, also for an ARN.
    d = _principal(env, NO_POLICY)["dynamodb"]
    value = f"arn:aws:dynamodb:{env['region']}:{env['known']}:table/{table['name']}"
    code, msg = _error(lambda: d.import_table(
        S3BucketSource={"S3Bucket": "b"},
        InputFormat="DYNAMODB_JSON",
        TableCreationParameters={
            "TableName": value,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        },
    ))
    expected = f"arn:aws:dynamodb:{env['region']}:{env['caller']}:table/{value}"
    assert code == "AccessDeniedException", (code, msg)
    assert f"dynamodb:ImportTable on resource: {expected}" in msg, msg
