# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Explicit-Deny enforcement on resource-ARN operations.

Operations that carry their resource in a field other than a top-level
TableName (ResourceArn on the tag operations, StreamArn on stream operations,
BackupArn on backup operations, IndexName on Query and Scan, and a table ARN in
TableName) must authorize against that specific resource. A policy that allows
dynamodb:* on * but explicitly Denies one action on one specific ARN must block
that action, matching live DynamoDB: every case below returns
AccessDeniedException while an allowed control on the same principal succeeds.

These tests provision builtin-IAM principals through the management API, so they
run against ExtendDB on every storage backend in CI. The equivalent behavior on
live DynamoDB was measured with real IAM principals and recorded in the builder
report's Service measurements section.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: resource-ARN authz tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


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


ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}


def _allow_all_deny(action: str, resource: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": f"dynamodb:{action}", "Resource": resource},
        ],
    }


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct, f"arnauthz-{acct}")
    assert resp.status_code == 201, resp.text
    yield acct
    mgmt.delete_account(acct)


def _user_with_policy(mgmt: ManagementClient, account_id: str, policy: dict) -> tuple[str, str]:
    """Create a fresh IAM user with the given policy and return (ak, sk)."""
    user = f"u{uuid.uuid4().hex[:10]}"
    assert mgmt.create_user(account_id, user, "TestPass123!").status_code == 201
    assert mgmt.put_user_policy(account_id, user, "p", policy).status_code == 204
    resp = mgmt.create_access_key(account_id, user)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return creds["access_key_id"], creds["secret_access_key"]


@pytest.fixture(scope="module")
def table(auth_env, mgmt, account_id, region):
    """A table with a GSI, a stream, one item, and a backup. Returns the
    canonical table, index, stream, and backup ARNs."""
    endpoint = auth_env[0]
    ak, sk = _user_with_policy(mgmt, account_id, ALLOW_ALL)
    admin = _client(endpoint, ak, sk, region)
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
    admin.put_item(TableName=name, Item={"pk": {"S": "p1"}, "g1pk": {"S": "g1"}})
    desc = admin.describe_table(TableName=name)["Table"]
    table_arn = desc["TableArn"]
    backup_arn = admin.create_backup(TableName=name, BackupName="arnbk")["BackupDetails"]["BackupArn"]
    return {
        "name": name,
        "table_arn": table_arn,
        "index_arn": table_arn + "/index/gsi1",
        "stream_arn": desc.get("LatestStreamArn"),
        "backup_arn": backup_arn,
    }


class TestExplicitDenyOnResourceArn:
    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _ddb(self, policy: dict, service: str = "dynamodb"):
        ak, sk = _user_with_policy(self.mgmt, self.account_id, policy)
        return _client(self.endpoint, ak, sk, self.region, service)

    def _assert_denied(self, fn):
        with pytest.raises(ClientError) as exc:
            fn()
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException", (
            exc.value.response["Error"]
        )

    def _assert_validation(self, fn, substr: str):
        with pytest.raises(ClientError) as exc:
            fn()
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert substr in err.get("Message", ""), err

    # -- tag operations: the core defect, explicit Deny on a tagged resource --

    def test_tag_resource_deny_blocks(self, table):
        d = self._ddb(_allow_all_deny("TagResource", table["table_arn"]))
        self._assert_denied(lambda: d.tag_resource(
            ResourceArn=table["table_arn"], Tags=[{"Key": "k", "Value": "v"}]))

    def test_untag_resource_deny_blocks(self, table):
        d = self._ddb(_allow_all_deny("UntagResource", table["table_arn"]))
        self._assert_denied(lambda: d.untag_resource(
            ResourceArn=table["table_arn"], TagKeys=["k"]))

    def test_list_tags_deny_blocks(self, table):
        d = self._ddb(_allow_all_deny("ListTagsOfResource", table["table_arn"]))
        self._assert_denied(lambda: d.list_tags_of_resource(ResourceArn=table["table_arn"]))

    # -- stream, backup, index operations --

    def test_describe_stream_deny_blocks(self, table):
        s = self._ddb(_allow_all_deny("DescribeStream", table["stream_arn"]), "dynamodbstreams")
        self._assert_denied(lambda: s.describe_stream(StreamArn=table["stream_arn"]))

    def test_describe_stream_wildcard_label_deny_blocks(self, table):
        # A stream label contains ':'. A Deny on table/<T>/stream/* must match
        # the concrete stream ARN, so the resource field is matched as a whole
        # with the wildcard spanning the colons in the label.
        wild = table["table_arn"] + "/stream/*"
        s = self._ddb(_allow_all_deny("DescribeStream", wild), "dynamodbstreams")
        self._assert_denied(lambda: s.describe_stream(StreamArn=table["stream_arn"]))

    def test_describe_stream_wildcard_label_allow_permits(self, table):
        # An Allow that grants only DescribeStream on table/<T>/stream/* must
        # match the concrete stream ARN and permit the call.
        wild = table["table_arn"] + "/stream/*"
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:DescribeStream", "Resource": wild}]}
        s = self._ddb(policy, "dynamodbstreams")
        s.describe_stream(StreamArn=table["stream_arn"])  # must not raise

    def test_getrecords_exact_stream_deny_blocks(self, table):
        # GetShardIterator stays allowed; GetRecords against the same stream is
        # denied, because GetRecords is authorized against the stream the
        # iterator reads, not a table/*/stream/* wildcard.
        s = self._ddb(_allow_all_deny("GetRecords", table["stream_arn"]), "dynamodbstreams")
        desc = s.describe_stream(StreamArn=table["stream_arn"])["StreamDescription"]
        shard_id = desc["Shards"][0]["ShardId"]
        it = s.get_shard_iterator(
            StreamArn=table["stream_arn"], ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        self._assert_denied(lambda: s.get_records(ShardIterator=it))

    def test_getrecords_iterator_naming_no_stream_is_invalid(self, table):
        # DynamoDB answers a shard iterator it did not issue with
        # ValidationException "Invalid ShardIterator" before authorization,
        # whatever the caller's policy (measured 2026-10-20 for an allow-all
        # principal, a principal allowed only ListTables, and one with an
        # explicit Deny on GetRecords for every stream). An iterator that names
        # no stream (a token from a build before the stream ARN was carried, a
        # random base64 string, or plain text) gets the same answer.
        now = int(time.time())
        tokens = [
            base64.standard_b64encode(
                f"shardId-00000000000000000000-00000001|TRIM_HORIZON||{now}".encode()).decode(),
            base64.standard_b64encode(uuid.uuid4().bytes * 8).decode(),
            "not-a-real-iterator-" + "x" * 40,
        ]
        stream_wildcard = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/*/stream/*"
        policies = [
            ALLOW_ALL,
            {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}]},
            _allow_all_deny("GetRecords", stream_wildcard),
        ]
        for policy in policies:
            s = self._ddb(policy, "dynamodbstreams")
            for token in tokens:
                with pytest.raises(ClientError) as exc:
                    s.get_records(ShardIterator=token)
                err = exc.value.response["Error"]
                assert err["Code"] == "ValidationException", (policy, token, err)
                assert err.get("Message") == "Invalid ShardIterator", (policy, token, err)

    def test_getrecords_forged_shard_reads_nothing(self, table):
        # A token that names the allowed stream ARN but a shard that does not
        # belong to it must not read: the engine confirms the shard belongs to
        # the stream before reading. The real shard reads; the forged one errors.
        s = self._ddb({"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]}, "dynamodbstreams")
        desc = s.describe_stream(StreamArn=table["stream_arn"])["StreamDescription"]
        shard_id = desc["Shards"][0]["ShardId"]
        real_it = s.get_shard_iterator(
            StreamArn=table["stream_arn"], ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        s.get_records(ShardIterator=real_it)  # control: real shard reads
        now = int(time.time())
        forged_raw = f"shardId-forged-0000000000000000|AFTER_SEQUENCE_NUMBER||{now}|{table['stream_arn']}"
        forged = base64.standard_b64encode(forged_raw.encode()).decode()
        with pytest.raises(ClientError):
            s.get_records(ShardIterator=forged)

    def test_delete_backup_deny_blocks(self, table):
        d = self._ddb(_allow_all_deny("DeleteBackup", table["backup_arn"]))
        self._assert_denied(lambda: d.delete_backup(BackupArn=table["backup_arn"]))

    # -- ListBackups authorizes the backup sub-resource, not the table --
    # The service evaluates table/<T>/backup/* (with TableName) and
    # table/*/backup/* (without), measured live. A Deny on that resource must
    # block ListBackups, and a least-privilege Allow on it must permit it; a
    # policy written against the bare table ARN does not affect ListBackups.

    def _backup_wild_table(self, table) -> str:
        return table["table_arn"] + "/backup/*"

    def _backup_wild_all(self) -> str:
        return f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/*/backup/*"

    def test_list_backups_deny_backup_wildcard_with_table_blocks(self, table):
        d = self._ddb(_allow_all_deny("ListBackups", self._backup_wild_table(table)))
        self._assert_denied(lambda: d.list_backups(TableName=table["name"]))

    def test_list_backups_deny_backup_wildcard_no_table_blocks(self, table):
        d = self._ddb(_allow_all_deny("ListBackups", self._backup_wild_all()))
        self._assert_denied(lambda: d.list_backups())

    def test_list_backups_allow_backup_wildcard_with_table_permits(self, table):
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:ListBackups",
             "Resource": self._backup_wild_table(table)}]}
        d = self._ddb(policy)
        d.list_backups(TableName=table["name"])  # must not raise

    def test_list_backups_allow_bare_table_denies(self, table):
        # An Allow written only on the bare table ARN does not grant ListBackups,
        # because the service evaluates the backup sub-resource.
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:ListBackups",
             "Resource": table["table_arn"]}]}
        d = self._ddb(policy)
        self._assert_denied(lambda: d.list_backups(TableName=table["name"]))

    def test_list_backups_deny_bare_table_permits(self, table):
        # A Deny written on the bare table ARN does not block ListBackups,
        # because that is not the resource the service evaluates.
        d = self._ddb(_allow_all_deny("ListBackups", table["table_arn"]))
        d.list_backups(TableName=table["name"])  # must not raise

    def test_query_on_index_deny_blocks(self, table):
        d = self._ddb(_allow_all_deny("Query", table["index_arn"]))
        self._assert_denied(lambda: d.query(
            TableName=table["name"], IndexName="gsi1",
            KeyConditionExpression="g1pk = :p",
            ExpressionAttributeValues={":p": {"S": "g1"}}))

    # -- tag operations on an index ARN: ValidationException for every principal
    # The service accepts tags only on a table or stream; an index ARN is a
    # ValidationException before the IAM decision, for a principal with full
    # access and for one with none.

    _INDEX_MSG = "only table or stream is accepted"

    def test_tag_ops_on_index_arn_rejected_admin(self, table):
        d = self._ddb(ALLOW_ALL)
        self._assert_validation(lambda: d.tag_resource(
            ResourceArn=table["index_arn"], Tags=[{"Key": "k", "Value": "v"}]), self._INDEX_MSG)
        self._assert_validation(lambda: d.untag_resource(
            ResourceArn=table["index_arn"], TagKeys=["k"]), self._INDEX_MSG)
        self._assert_validation(lambda: d.list_tags_of_resource(
            ResourceArn=table["index_arn"]), self._INDEX_MSG)

    def test_tag_ops_on_index_arn_rejected_noperm(self, table):
        noperm = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}]}
        d = self._ddb(noperm)
        self._assert_validation(lambda: d.tag_resource(
            ResourceArn=table["index_arn"], Tags=[{"Key": "k", "Value": "v"}]), self._INDEX_MSG)
        self._assert_validation(lambda: d.untag_resource(
            ResourceArn=table["index_arn"], TagKeys=["k"]), self._INDEX_MSG)
        self._assert_validation(lambda: d.list_tags_of_resource(
            ResourceArn=table["index_arn"]), self._INDEX_MSG)

    # -- TagResource ResourceArn error ordering, measured live, every row --
    # ValidationException before authorization (every principal) for a malformed
    # or non-table/stream ARN; for an admin, a table ARN with a wrong partition,
    # service, or region is ValidationException where a no-permissions principal
    # is denied by authorization. Every case is a refusal.

    def _ordering_variants(self, table) -> dict:
        parts = table["table_arn"].split(":")  # arn:aws:dynamodb:region:acct:table/name
        region, account = parts[3], parts[4]
        other_region = "us-west-2" if region != "us-west-2" else "us-east-1"
        return {
            "not_an_arn": "banana",
            "bad_resource_type": f"arn:aws:dynamodb:{region}:{account}:banana/x",
            "wrong_partition": table["table_arn"].replace("arn:aws:", "arn:aws-cn:", 1),
            "wrong_service": table["table_arn"].replace(":dynamodb:", ":s3:", 1),
            "cross_account": f"arn:aws:dynamodb:{region}:111122223333:table/{table['name']}",
            "cross_region": f"arn:aws:dynamodb:{other_region}:{account}:table/{table['name']}",
            "nonexistent": f"arn:aws:dynamodb:{region}:{account}:table/{table['name']}-nope-xyz",
            "index_arn_for_tag": table["index_arn"],
        }

    def _assert_code(self, fn, code: str, substr: str = ""):
        with pytest.raises(ClientError) as exc:
            fn()
        err = exc.value.response["Error"]
        assert err["Code"] == code, err
        if substr:
            assert substr in err.get("Message", ""), err

    def test_tag_ordering_noperm_matches_service(self, table):
        v = self._ordering_variants(table)
        noperm = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}]}
        expect = {
            "not_an_arn": ("ValidationException", "ARNs must start with 'arn:'"),
            "bad_resource_type": ("ValidationException", "Provided Arn is not a DynamoDB resource arn"),
            "wrong_partition": ("AccessDeniedException", ""),
            "wrong_service": ("AccessDeniedException", ""),
            "cross_account": ("AccessDeniedException", ""),
            "cross_region": ("AccessDeniedException", ""),
            "nonexistent": ("AccessDeniedException", ""),
            "index_arn_for_tag": ("ValidationException", "only table or stream is accepted"),
        }
        for label, arn in v.items():
            code, substr = expect[label]
            d = self._ddb(noperm)
            self._assert_code(
                lambda a=arn, dd=d: dd.tag_resource(ResourceArn=a, Tags=[{"Key": "k", "Value": "v"}]),
                code, substr)

    def test_tag_ordering_admin_matches_service(self, table):
        v = self._ordering_variants(table)
        expect = {
            "not_an_arn": ("ValidationException", "ARNs must start with 'arn:'"),
            "bad_resource_type": ("ValidationException", "Provided Arn is not a DynamoDB resource arn"),
            "wrong_partition": ("ValidationException", "Invalid TableArn"),
            "wrong_service": ("ValidationException", "Invalid TableArn"),
            "cross_account": ("AccessDeniedException", ""),
            "cross_region": ("ValidationException", "Invalid TableArn"),
            "nonexistent": ("ResourceNotFoundException", ""),
            "index_arn_for_tag": ("ValidationException", "only table or stream is accepted"),
        }
        for label, arn in v.items():
            code, substr = expect[label]
            d = self._ddb(ALLOW_ALL)
            self._assert_code(
                lambda a=arn, dd=d: dd.tag_resource(ResourceArn=a, Tags=[{"Key": "k", "Value": "v"}]),
                code, substr)

    # -- permission chains: a Deny anywhere in the chain blocks the operation --

    def test_restore_from_backup_deny_target_item_action_blocks(self, table):
        # A restore writes items into the target table, so the service (and now
        # ExtendDB) require the item actions on the target. A Deny on PutItem for
        # the target table name blocks the restore.
        target = table["table_arn"] + "-rfb"
        d = self._ddb(_allow_all_deny("PutItem", target))
        self._assert_denied(lambda: d.restore_table_from_backup(
            TargetTableName=table["name"] + "-rfb", BackupArn=table["backup_arn"]))

    def test_restore_to_point_in_time_deny_target_item_action_blocks(self, table):
        target = table["table_arn"] + "-pitr"
        d = self._ddb(_allow_all_deny("PutItem", target))
        self._assert_denied(lambda: d.restore_table_to_point_in_time(
            SourceTableName=table["name"], TargetTableName=table["name"] + "-pitr"))

    def test_export_deny_table_blocks(self, table):
        d = self._ddb(_allow_all_deny("ExportTableToPointInTime", table["table_arn"]))
        self._assert_denied(lambda: d.export_table_to_point_in_time(
            TableArn=table["table_arn"], S3Bucket="bb-arn-nobucket"))

    def test_import_deny_new_table_blocks(self, table):
        new_table = table["table_arn"] + "-imp"
        d = self._ddb(_allow_all_deny("ImportTable", new_table))
        self._assert_denied(lambda: d.import_table(
            S3BucketSource={"S3Bucket": "bb-arn-nobucket"},
            InputFormat="DYNAMODB_JSON",
            TableCreationParameters={
                "TableName": table["name"] + "-imp", "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}))

    # -- fail closed: a respelled ARN cannot evade a Deny on the canonical ARN --

    def test_respelled_stream_arn_cannot_bypass_deny(self, table):
        """A Deny on the canonical stream ARN must also block a request that
        respells the partition. ExtendDB resolves the stream in this account
        regardless of the ARN's partition, so authorizing the respelled string
        verbatim would bypass the Deny."""
        respelled = table["stream_arn"].replace("arn:aws:", "arn:aws-cn:")
        s = self._ddb(_allow_all_deny("DescribeStream", table["stream_arn"]), "dynamodbstreams")
        self._assert_denied(lambda: s.describe_stream(StreamArn=respelled))

    def test_table_arn_in_tablename_deny_blocks(self, table):
        """A Deny on the table ARN blocks GetItem whether TableName is the bare
        name or the table ARN."""
        d = self._ddb(_allow_all_deny("GetItem", table["table_arn"]))
        self._assert_denied(lambda: d.get_item(
            TableName=table["name"], Key={"pk": {"S": "p1"}}))
        d2 = self._ddb(_allow_all_deny("GetItem", table["table_arn"]))
        self._assert_denied(lambda: d2.get_item(
            TableName=table["table_arn"], Key={"pk": {"S": "p1"}}))

    def test_table_arn_in_tablename_keeps_leading_keys_fence(self, table):
        """A principal fenced to one partition with dynamodb:LeadingKeys is
        refused another partition when it names the table by ARN, as it is when
        it names the table by name. Measured on DynamoDB 2026-10-20: GetItem with
        the table ARN as TableName for a partition outside the fence returns
        AccessDeniedException."""
        d = self._ddb({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": table["table_arn"],
                "Condition": {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["p1"]}},
            }],
        })
        self._assert_denied(lambda: d.get_item(
            TableName=table["name"], Key={"pk": {"S": "other"}}))
        self._assert_denied(lambda: d.get_item(
            TableName=table["table_arn"], Key={"pk": {"S": "other"}}))

    # -- positive control: without the Deny, the operation is allowed --

    def test_allow_without_deny_permits_tagging(self, table):
        d = self._ddb(ALLOW_ALL)
        # Must not raise.
        d.tag_resource(ResourceArn=table["table_arn"], Tags=[{"Key": "ctrl", "Value": "1"}])
        time.sleep(0)

    # -- AccessDenied message form matches the service (both forms) --

    def test_explicit_deny_message_form(self, table):
        d = self._ddb(_allow_all_deny("TagResource", table["table_arn"]))
        with pytest.raises(ClientError) as exc:
            d.tag_resource(ResourceArn=table["table_arn"], Tags=[{"Key": "k", "Value": "v"}])
        msg = exc.value.response["Error"]["Message"]
        assert "with an explicit deny in an identity-based policy" in msg, msg

    def test_implicit_deny_message_form(self, table):
        # A principal allowed only ListTables is denied GetItem with the
        # implicit-deny message form.
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}]}
        d = self._ddb(policy)
        with pytest.raises(ClientError) as exc:
            d.get_item(TableName=table["name"], Key={"pk": {"S": "p1"}})
        msg = exc.value.response["Error"]["Message"]
        assert "because no identity-based policy allows the dynamodb:GetItem action" in msg, msg

    # -- DescribeEndpoints: signed request, no IAM permission ----------------

    def test_describe_endpoints_needs_no_iam_permission(self):
        # DynamoDB checks the signature on DescribeEndpoints but evaluates no
        # IAM policy: a user with no policy, one allowed only ListTables, and
        # ones with an explicit Deny on dynamodb:DescribeEndpoints or on
        # dynamodb:* all get the endpoint list (measured 2026-10-20).
        user = f"u{uuid.uuid4().hex[:10]}"
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        resp = self.mgmt.create_access_key(self.account_id, user)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        no_policy = _client(self.endpoint, creds["access_key_id"],
                            creds["secret_access_key"], self.region)
        clients = [
            no_policy,
            self._ddb({"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}]}),
            self._ddb(_allow_all_deny("DescribeEndpoints", "*")),
            self._ddb({"Version": "2012-10-17", "Statement": [
                {"Effect": "Deny", "Action": "dynamodb:*", "Resource": "*"}]}),
        ]
        for c in clients:
            endpoints = c.describe_endpoints()["Endpoints"]
            assert len(endpoints) == 1, endpoints
            assert endpoints[0]["Address"], endpoints
