# SPDX-License-Identifier: Apache-2.0

"""Stream ARN scope: account, partition, region and service (issues #450, #451).

A StreamArn (DescribeStream, GetShardIterator), a stream ARN in a tag
operation's ResourceArn, and the stream ARN inside a GetRecords iterator must
name the caller's account and this deployment's partition, service and region.
Before this fix ExtendDB resolved a stream by table name and label only, so a
StreamArn in another account, partition or region, or with an upper-cased
service or region, was served as the caller's own stream.

Every expected answer below was measured against live DynamoDB on 2026-10-23
(12:22 to 12:27 UTC, probes F2-1 and F2-2; UntagResource and the
GetShardIterator unknown-label recheck in probe W1-1) for four principals: an
admin (Allow dynamodb:* on *), a user with no stream permissions, a user
allowed exactly the canonical stream ARN, and a user with Allow * plus a Deny
on the canonical stream ARN.

StreamArn (DescribeStream, GetShardIterator), the same for every principal, so
before the IAM decision:

  | spelling                                          | answer                              |
  |---------------------------------------------------|-------------------------------------|
  | another account (111111111111, abc), alone or with | AccessDeniedException               |
  |   aws-cn, us-west-2 or an unknown label            |   "Access is denied"                |
  | empty account; aws-cn; aws-us-gov; DynamoDB;       | ValidationException                 |
  |   DYNAMODB; us-west-2; US-EAST-1; an extra field;  |   "Invalid StreamArn"               |
  |   a missing region field; a trailing slash         |                                     |
  | a label the table never had                        | IAM decision first (noperm, exact   |
  |                                                    |   denied); then ResourceNotFound    |

ResourceArn (ListTagsOfResource, TagResource, UntagResource):

  | spelling                                          | answer                              |
  |---------------------------------------------------|-------------------------------------|
  | another account, abc, empty account               | AccessDeniedException               |
  |                                                    |   "Access is denied", every principal |
  | an extra field; a trailing slash                   | ValidationException "One or more    |
  |                                                    |   parameter values were invalid:    |
  |                                                    |   Provided Arn is not a DynamoDB    |
  |                                                    |   resource arn: <arn>", every       |
  |                                                    |   principal                         |
  | aws-cn; aws-us-gov; DynamoDB; DYNAMODB; us-west-2; | IAM decision first, on the ARN as   |
  |   US-EAST-1                                        |   sent (noperm and exact denied,    |
  |                                                    |   naming that ARN); then            |
  |                                                    |   ValidationException               |
  |                                                    |   "Invalid StreamArn"               |
  | a label the table never had                        | IAM decision first; then            |
  |                                                    |   ResourceNotFoundException         |
  |                                                    |   "Requested resource not found:    |
  |                                                    |   ResourceArn: <arn> not found"     |

A GetRecords token the service did not issue is ValidationException "Invalid
ShardIterator" whatever stream ARN it carries.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any, Callable

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

INVALID_STREAM_ARN = "Invalid StreamArn"
ACCESS_IS_DENIED = "Access is denied"
OTHER_ACCOUNT = "111111111111"

STREAM_OPS = ("DescribeStream", "GetShardIterator")
TAG_OPS = ("ListTagsOfResource", "TagResource", "UntagResource")


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: stream ARN scope tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD."
        )
    return endpoint, admin_user, admin_pass


def _client(endpoint: str, ak: str, sk: str, region: str, service: str) -> Any:
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
NO_PERMISSIONS = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}],
}


def _allow_only(resource: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": resource}],
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
    resp = mgmt.create_account(acct, f"streamscope-{acct}")
    assert resp.status_code == 201, resp.text
    yield acct
    mgmt.delete_account(acct)


def _user_with_policy(mgmt: ManagementClient, account_id: str, policy: dict) -> tuple[str, str, str]:
    user = f"u{uuid.uuid4().hex[:10]}"
    assert mgmt.create_user(account_id, user, "TestPass123!").status_code == 201
    assert mgmt.put_user_policy(account_id, user, "p", policy).status_code == 204
    resp = mgmt.create_access_key(account_id, user)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return user, creds["access_key_id"], creds["secret_access_key"]


@pytest.fixture(scope="module")
def table(auth_env, mgmt, account_id, region):
    """A table with a stream and one item. Returns its ARNs and a shard id."""
    endpoint = auth_env[0]
    _, ak, sk = _user_with_policy(mgmt, account_id, ALLOW_ALL)
    ddb = _client(endpoint, ak, sk, region, "dynamodb")
    streams = _client(endpoint, ak, sk, region, "dynamodbstreams")
    name = f"s{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    wait_for_active(ddb, name)
    ddb.put_item(TableName=name, Item={"pk": {"S": "p1"}})
    desc = ddb.describe_table(TableName=name)["Table"]
    stream_arn = desc["LatestStreamArn"]
    shard_id = streams.describe_stream(StreamArn=stream_arn)["StreamDescription"]["Shards"][0]["ShardId"]
    return {
        "name": name,
        "table_arn": desc["TableArn"],
        "stream_arn": stream_arn,
        "shard_id": shard_id,
    }


def _variants(stream_arn: str, region: str, account_id: str) -> dict[str, str]:
    """The spellings measured in probes F2-1 and W1-1, keyed by their names."""
    other_region = "us-west-2" if region != "us-west-2" else "us-east-1"
    table_arn = stream_arn.split("/stream/")[0]
    v = {
        "account_other": stream_arn.replace(f":{account_id}:", f":{OTHER_ACCOUNT}:", 1),
        "account_empty": stream_arn.replace(f":{account_id}:", "::", 1),
        "account_nonnumeric": stream_arn.replace(f":{account_id}:", ":abc:", 1),
        "partition_aws_cn": stream_arn.replace("arn:aws:", "arn:aws-cn:", 1),
        "partition_aws_us_gov": stream_arn.replace("arn:aws:", "arn:aws-us-gov:", 1),
        "service_DynamoDB": stream_arn.replace(":dynamodb:", ":DynamoDB:", 1),
        "service_DYNAMODB": stream_arn.replace(":dynamodb:", ":DYNAMODB:", 1),
        "region_other": stream_arn.replace(f":{region}:", f":{other_region}:", 1),
        "region_upper": stream_arn.replace(f":{region}:", f":{region.upper()}:", 1),
        "field_extra": stream_arn.replace(f":{account_id}:", f":{account_id}:x:", 1),
        "field_missing_region": stream_arn.replace(f":{region}:", ":", 1),
        "trailing_slash": stream_arn + "/",
        "label_unknown": table_arn + "/stream/2000-01-01T00:00:00.000",
    }
    v["account_other_and_aws_cn"] = v["account_other"].replace("arn:aws:", "arn:aws-cn:", 1)
    v["account_other_and_label_unknown"] = v["label_unknown"].replace(
        f":{account_id}:", f":{OTHER_ACCOUNT}:", 1)
    v["account_other_and_region_other"] = v["account_other"].replace(
        f":{region}:", f":{other_region}:", 1)
    return v


ACCOUNT_VARIANTS = (
    "account_other", "account_nonnumeric", "account_other_and_aws_cn",
    "account_other_and_label_unknown", "account_other_and_region_other",
)
RESPELLINGS = (
    "partition_aws_cn", "partition_aws_us_gov", "service_DynamoDB",
    "service_DYNAMODB", "region_other", "region_upper",
)
MALFORMED = ("field_extra", "trailing_slash")


def _not_dynamodb_resource(arn: str) -> str:
    return (
        "One or more parameter values were invalid: "
        f"Provided Arn is not a DynamoDB resource arn: {arn}"
    )


def _iam_denied(account_id: str, user: str, op: str, arn: str) -> str:
    return (
        f"User: arn:aws:iam::{account_id}:user/{user} is not authorized to perform: "
        f"dynamodb:{op} on resource: {arn} because no identity-based policy allows "
        f"the dynamodb:{op} action"
    )


class _Principal:
    def __init__(self, endpoint: str, region: str, account_id: str, user: str, ak: str, sk: str):
        self.user = user
        self.account_id = account_id
        self.ddb = _client(endpoint, ak, sk, region, "dynamodb")
        self.streams = _client(endpoint, ak, sk, region, "dynamodbstreams")

    def call(self, op: str, arn: str, shard_id: str) -> Callable[[], Any]:
        return {
            "DescribeStream": lambda: self.streams.describe_stream(StreamArn=arn),
            "GetShardIterator": lambda: self.streams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"),
            "ListTagsOfResource": lambda: self.ddb.list_tags_of_resource(ResourceArn=arn),
            "TagResource": lambda: self.ddb.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "k", "Value": "v"}]),
            "UntagResource": lambda: self.ddb.untag_resource(ResourceArn=arn, TagKeys=["k"]),
        }[op]

    def error(self, op: str, arn: str, shard_id: str) -> dict:
        with pytest.raises(ClientError) as exc:
            self.call(op, arn, shard_id)()
        return exc.value.response["Error"]

    def iam_denied(self, op: str, arn: str) -> str:
        return _iam_denied(self.account_id, self.user, op, arn)


class TestStreamArnScope:
    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region, table):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self.table = table
        self.shard_id = table["shard_id"]
        self.variants = _variants(table["stream_arn"], region, account_id)

    def _principal(self, policy: dict) -> _Principal:
        user, ak, sk = _user_with_policy(self.mgmt, self.account_id, policy)
        return _Principal(self.endpoint, self.region, self.account_id, user, ak, sk)

    def _principals(self) -> dict[str, _Principal]:
        """The four principals every case was measured as."""
        canonical = self.table["stream_arn"]
        return {
            "admin": self._principal(ALLOW_ALL),
            "noperm": self._principal(NO_PERMISSIONS),
            "exact": self._principal(_allow_only(canonical)),
            "deny": self._principal(_allow_all_deny("*", canonical)),
        }

    @staticmethod
    def _assert_err(err: dict, code: str, message: str | None, ctx: tuple) -> None:
        assert err["Code"] == code, (*ctx, err)
        if message is not None:
            assert err.get("Message") == message, (*ctx, err)

    # -- #450: another account is AccessDeniedException ----------------------

    def test_stream_arn_in_another_account_is_denied(self):
        """Another 12-digit account, `abc`, and another account combined with
        aws-cn, us-west-2 or an unknown label: AccessDeniedException "Access is
        denied" on all five operations, for every principal, so before the IAM
        decision and before the partition, region or label is looked at."""
        for who, p in self._principals().items():
            for label in ACCOUNT_VARIANTS:
                arn = self.variants[label]
                for op in STREAM_OPS + TAG_OPS:
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(err, "AccessDeniedException", ACCESS_IS_DENIED, (who, label, op))

    def test_empty_account_and_missing_region(self):
        """An empty account field is "Invalid StreamArn" on StreamArn and
        "Access is denied" on a tag ResourceArn, for every principal. A missing
        region field is "Invalid StreamArn" on StreamArn for every principal.
        (On a tag ResourceArn the service answers a missing region field with
        "Access is denied"; ExtendDB reads the shifted fields as a malformed
        resource first. That ordering is pre-existing and outside this fix, so
        it is not asserted here.)"""
        principals = self._principals()
        for label in ("account_empty", "field_missing_region"):
            arn = self.variants[label]
            for who, p in principals.items():
                for op in STREAM_OPS:
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(err, "ValidationException", INVALID_STREAM_ARN, (who, label, op))
        arn = self.variants["account_empty"]
        for who, p in principals.items():
            for op in TAG_OPS:
                err = p.error(op, arn, self.shard_id)
                self._assert_err(err, "AccessDeniedException", ACCESS_IS_DENIED, (who, "account_empty", op))

    # -- #451: another partition, region or service ---------------------------

    def test_respelled_stream_arn_on_stream_operations_is_invalid(self):
        """aws-cn, aws-us-gov, DynamoDB, DYNAMODB, us-west-2 and US-EAST-1 in
        StreamArn: ValidationException "Invalid StreamArn" for every principal,
        so before the IAM decision."""
        for who, p in self._principals().items():
            for label in RESPELLINGS:
                arn = self.variants[label]
                for op in STREAM_OPS:
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(err, "ValidationException", INVALID_STREAM_ARN, (who, label, op))

    def test_respelled_stream_arn_on_tag_operations(self):
        """The same six spellings in a tag ResourceArn: the IAM decision runs
        first, on the ARN as sent. A no-permissions principal and a principal
        allowed only the canonical ARN are denied with a message naming the
        spelling they sent; the admin, and a principal with Allow * plus a Deny
        on the canonical ARN (which the respelled ARN does not match), get
        ValidationException "Invalid StreamArn"."""
        principals = self._principals()
        for label in RESPELLINGS:
            arn = self.variants[label]
            for op in TAG_OPS:
                for who in ("admin", "deny"):
                    err = principals[who].error(op, arn, self.shard_id)
                    self._assert_err(err, "ValidationException", INVALID_STREAM_ARN, (who, label, op))
                for who in ("noperm", "exact"):
                    p = principals[who]
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(err, "AccessDeniedException", p.iam_denied(op, arn), (who, label, op))

    def test_malformed_stream_arn(self):
        """An extra field and a trailing slash: "Invalid StreamArn" on
        StreamArn; "Provided Arn is not a DynamoDB resource arn: <arn>" on a tag
        ResourceArn. Every principal, so before the IAM decision."""
        for who, p in self._principals().items():
            for label in MALFORMED:
                arn = self.variants[label]
                for op in STREAM_OPS:
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(err, "ValidationException", INVALID_STREAM_ARN, (who, label, op))
                for op in TAG_OPS:
                    err = p.error(op, arn, self.shard_id)
                    self._assert_err(
                        err, "ValidationException", _not_dynamodb_resource(arn), (who, label, op))

    # -- positive control and the Deny on the canonical ARN -------------------

    def test_canonical_stream_arn(self):
        """The ARN the server issued: served to the admin and to the principal
        allowed exactly that ARN on all five operations; the no-permissions
        principal gets the IAM denial naming it; the Deny principal gets the
        explicit-deny message."""
        p = self._principals()
        arn = self.table["stream_arn"]
        for who in ("admin", "exact"):
            for op in STREAM_OPS + TAG_OPS:
                p[who].call(op, arn, self.shard_id)()
        for op in STREAM_OPS + TAG_OPS:
            err = p["noperm"].error(op, arn, self.shard_id)
            self._assert_err(err, "AccessDeniedException", p["noperm"].iam_denied(op, arn), ("noperm", op))
            err = p["deny"].error(op, arn, self.shard_id)
            expected = (
                f"User: arn:aws:iam::{self.account_id}:user/{p['deny'].user} is not authorized "
                f"to perform: dynamodb:{op} on resource: {arn} with an explicit deny in an "
                "identity-based policy"
            )
            self._assert_err(err, "AccessDeniedException", expected, ("deny", op))

    def test_deny_on_real_stream_arn_is_not_evaded_by_respelling(self):
        """A principal with Allow * and a per-operation Deny on the canonical
        stream ARN gets no stream back for any spelling: another account is
        "Access is denied", the six respellings are "Invalid StreamArn", on
        every operation."""
        canonical = self.table["stream_arn"]
        for op in STREAM_OPS + TAG_OPS:
            p = self._principal(_allow_all_deny(op, canonical))
            err = p.error(op, canonical, self.shard_id)
            self._assert_err(err, "AccessDeniedException", None, ("canonical", op))
            for label in ACCOUNT_VARIANTS:
                err = p.error(op, self.variants[label], self.shard_id)
                self._assert_err(err, "AccessDeniedException", ACCESS_IS_DENIED, (label, op))
            for label in RESPELLINGS:
                err = p.error(op, self.variants[label], self.shard_id)
                self._assert_err(err, "ValidationException", INVALID_STREAM_ARN, (label, op))

    # -- GetRecords: a token the server did not issue -------------------------

    def test_getrecords_iterator_with_respelled_stream_arn_is_invalid(self):
        """A shard iterator is base64 of `shard|type|seq|ts|stream_arn`. A token
        carrying a stream ARN the server did not issue (another account, aws-cn,
        us-west-2, DYNAMODB) was canonicalized and served before; it is now
        ValidationException "Invalid ShardIterator", the service's answer to a
        token it did not issue, for an admin and for a no-permissions principal
        alike."""
        now = int(time.time())

        def token(arn: str) -> str:
            return base64.standard_b64encode(
                f"{self.shard_id}|AFTER_SEQUENCE_NUMBER||{now}|{arn}".encode()
            ).decode()

        admin = self._principal(ALLOW_ALL)
        noperm = self._principal(NO_PERMISSIONS)
        # Control: the same token shape with the issued ARN reads the stream.
        admin.streams.get_records(ShardIterator=token(self.table["stream_arn"]), Limit=10)
        for label in ("account_other", "partition_aws_cn", "region_other", "service_DYNAMODB"):
            for who, p in (("admin", admin), ("noperm", noperm)):
                with pytest.raises(ClientError) as exc:
                    p.streams.get_records(ShardIterator=token(self.variants[label]), Limit=10)
                err = exc.value.response["Error"]
                assert err == {"Code": "ValidationException", "Message": "Invalid ShardIterator"}, (
                    who, label, err)

    # -- a label the table never had ------------------------------------------

    def test_unknown_label(self):
        """IAM decision first (noperm and exact are denied naming the ARN) on all
        five operations, then ResourceNotFoundException for the admin and the
        Deny principal (whose Deny names another label). DescribeStream:
        "Requested resource not found: Stream: <arn> not found" with no trailing
        period (#451). GetShardIterator: the service says "Requested resource
        not found: Stream does not exist"; ExtendDB answers the DescribeStream
        message, a pre-existing difference outside this fix, so only the code is
        asserted. (The tag operations answer "Requested resource not found:
        ResourceArn: <arn> not found" on the service; ExtendDB checks only that
        the table exists, which is pre-existing and outside this fix, so the
        authorized principals are not asserted on them here.)"""
        p = self._principals()
        arn = self.variants["label_unknown"]
        for op in STREAM_OPS + TAG_OPS:
            for who in ("noperm", "exact"):
                err = p[who].error(op, arn, self.shard_id)
                self._assert_err(err, "AccessDeniedException", p[who].iam_denied(op, arn), (who, op))
        for op in STREAM_OPS:
            for who in ("admin", "deny"):
                err = p[who].error(op, arn, self.shard_id)
                expected = (
                    f"Requested resource not found: Stream: {arn} not found"
                    if op == "DescribeStream" else None
                )
                self._assert_err(err, "ResourceNotFoundException", expected, (who, op))
