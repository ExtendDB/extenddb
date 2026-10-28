# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Developer-mode authorization tests.

`dev-mode` opens the IAM policy decision for an authenticated caller: SigV4 is
still verified, but no `dynamodb:*` policy is required. The seeded `dev` user
holds only `SelfServicePolicy` (four `iam:*` actions on its own ARN), so any
operation that still reaches the IAM evaluator is denied. That makes an
`AccessDeniedException` here proof that an operation is bypassing the dev-mode
gate, not a policy misconfiguration.

Batch and transaction operations regressed exactly this way: their per-table
authorization branch returned before the dev-mode check was reached, so
`BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`
failed while every single-item operation succeeded.

Dev mode skips only the IAM decision. The input checks the service applies to
every principal before that decision still run, so the stream ARN scope
refusals of `tests/test_stream_arn_scope.py` (#450, #451) are asserted here
against the dev-mode server too.

These require a server built and started in dev mode, so they are gated on
EXTENDDB_TEST_DEV_MODE and excluded from the backend-agnostic suite, which runs
against a production-mode server where these operations are correctly denied.
"""

from __future__ import annotations

import base64
import os
import time

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active

pytestmark = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_DEV_MODE", "").strip() != "1",
    reason="requires a dev-mode server (set EXTENDDB_TEST_DEV_MODE=1)",
)


def _assert_not_denied(op_name: str, call):
    """Run `call`, failing loudly if the operation was denied rather than served.

    Any other ClientError is re-raised: this test is about the authorization
    decision, so a genuine request error should not be swallowed into a pass.
    """
    try:
        return call()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("AccessDeniedException", "UnrecognizedClientException"):
            pytest.fail(
                f"{op_name} was denied in dev mode with {code}: "
                f"{exc.response['Error'].get('Message', '')}"
            )
        raise


@pytest.fixture()
def dev_table(dynamodb_client, unique_table_name):
    dynamodb_client.create_table(
        TableName=unique_table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, unique_table_name)
    yield unique_table_name
    try:
        dynamodb_client.delete_table(TableName=unique_table_name)
    except ClientError:
        pass


class TestDevModeOpensAuthorization:
    """Every data-plane operation is served in dev mode, not just single-item ones."""

    def test_single_item_operations_are_not_denied(self, dynamodb_client, dev_table):
        # These already worked. They are kept so a regression that closes
        # authorization altogether is distinguishable from one that affects only
        # the batch and transaction branch.
        _assert_not_denied(
            "PutItem",
            lambda: dynamodb_client.put_item(
                TableName=dev_table, Item={"pk": {"S": "a"}, "v": {"N": "1"}}
            ),
        )
        _assert_not_denied(
            "GetItem",
            lambda: dynamodb_client.get_item(TableName=dev_table, Key={"pk": {"S": "a"}}),
        )
        _assert_not_denied(
            "UpdateItem",
            lambda: dynamodb_client.update_item(
                TableName=dev_table,
                Key={"pk": {"S": "a"}},
                UpdateExpression="SET #v = :v",
                ExpressionAttributeNames={"#v": "v"},
                ExpressionAttributeValues={":v": {"N": "2"}},
            ),
        )
        _assert_not_denied(
            "Query",
            lambda: dynamodb_client.query(
                TableName=dev_table,
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "a"}},
            ),
        )
        _assert_not_denied("Scan", lambda: dynamodb_client.scan(TableName=dev_table))
        _assert_not_denied(
            "DeleteItem",
            lambda: dynamodb_client.delete_item(
                TableName=dev_table, Key={"pk": {"S": "a"}}
            ),
        )

    def test_batch_write_item_is_not_denied(self, dynamodb_client, dev_table):
        resp = _assert_not_denied(
            "BatchWriteItem",
            lambda: dynamodb_client.batch_write_item(
                RequestItems={
                    dev_table: [
                        {"PutRequest": {"Item": {"pk": {"S": "b1"}}}},
                        {"PutRequest": {"Item": {"pk": {"S": "b2"}}}},
                    ]
                }
            ),
        )
        assert not resp.get("UnprocessedItems", {}).get(dev_table)

    def test_batch_get_item_is_not_denied(self, dynamodb_client, dev_table):
        dynamodb_client.put_item(TableName=dev_table, Item={"pk": {"S": "g1"}})
        resp = _assert_not_denied(
            "BatchGetItem",
            lambda: dynamodb_client.batch_get_item(
                RequestItems={dev_table: {"Keys": [{"pk": {"S": "g1"}}]}}
            ),
        )
        assert len(resp["Responses"][dev_table]) == 1

    def test_transact_write_items_is_not_denied(self, dynamodb_client, dev_table):
        _assert_not_denied(
            "TransactWriteItems",
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": dev_table, "Item": {"pk": {"S": "t1"}}}}
                ]
            ),
        )
        got = dynamodb_client.get_item(TableName=dev_table, Key={"pk": {"S": "t1"}})
        assert "Item" in got

    def test_transact_get_items_is_not_denied(self, dynamodb_client, dev_table):
        dynamodb_client.put_item(TableName=dev_table, Item={"pk": {"S": "tg1"}})
        resp = _assert_not_denied(
            "TransactGetItems",
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    {"Get": {"TableName": dev_table, "Key": {"pk": {"S": "tg1"}}}}
                ]
            ),
        )
        assert resp["Responses"][0]["Item"]["pk"]["S"] == "tg1"


OTHER_ACCOUNT = "111111111111"


def _error(call, *args) -> dict:
    with pytest.raises(ClientError) as exc:
        call(*args)
    return exc.value.response["Error"]


@pytest.fixture()
def stream_table(dynamodb_client, endpoint_url, unique_table_name):
    """A table with a stream and one record. Returns the issued stream ARN, a
    shard id and a streams client with retries off."""
    dynamodb_client.create_table(
        TableName=unique_table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    wait_for_active(dynamodb_client, unique_table_name)
    dynamodb_client.put_item(TableName=unique_table_name, Item={"pk": {"S": "s1"}})
    streams = boto3.client(
        "dynamodbstreams",
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    arn = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]["LatestStreamArn"]
    shard_id = streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"][0]["ShardId"]
    yield arn, shard_id, streams
    try:
        dynamodb_client.delete_table(TableName=unique_table_name)
    except ClientError:
        pass


class TestDevModeKeepsStreamArnScope:
    """The stream ARN refusals run before the dev-mode return (#450, #451).
    The expected answers are the service's, measured 2026-10-23 and tabled in
    the module docstring of tests/test_stream_arn_scope.py."""

    def test_stream_arn_outside_scope_is_refused(self, stream_table):
        arn, shard_id, streams = stream_table
        account = arn.split(":")[4]
        cases = (
            (arn.replace("arn:aws:", "arn:aws-cn:", 1),
             "ValidationException", "Invalid StreamArn"),
            (arn.replace(f":{account}:", f":{OTHER_ACCOUNT}:", 1),
             "AccessDeniedException", "Access is denied"),
        )
        calls = {
            "DescribeStream": lambda a: streams.describe_stream(StreamArn=a),
            "GetShardIterator": lambda a: streams.get_shard_iterator(
                StreamArn=a, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"),
        }
        for sent, code, message in cases:
            for op, call in calls.items():
                err = _error(call, sent)
                assert (err["Code"], err.get("Message")) == (code, message), (op, sent, err)

    def test_tag_resource_arn_with_trailing_slash_is_refused(self, dynamodb_client, stream_table):
        """The issued stream ARN with a trailing slash, as a tag operation's
        ResourceArn, is not a DynamoDB resource ARN: the service answers this
        for every principal, so the shape check runs before the dev-mode
        return. Without it TagResource writes a tag under that ARN."""
        sent = stream_table[0] + "/"
        expected = {
            "Code": "ValidationException",
            "Message": (
                "One or more parameter values were invalid: "
                f"Provided Arn is not a DynamoDB resource arn: {sent}"
            ),
        }
        calls = {
            "TagResource": lambda: dynamodb_client.tag_resource(
                ResourceArn=sent, Tags=[{"Key": "k", "Value": "v"}]),
            "ListTagsOfResource": lambda: dynamodb_client.list_tags_of_resource(ResourceArn=sent),
        }
        for op, call in calls.items():
            err = _error(call)
            assert err == expected, (op, err)

    def test_getrecords_token_not_issued_here_is_refused(self, stream_table):
        """A token is base64 of `shard|type|seq|ts|stream_arn`. One naming
        another account is "Invalid ShardIterator"; GetShardIterator issues no
        such token. A token in the pre-upgrade shape (four fields, no stream
        ARN) is answered "Shard iterator has expired", as in every other build."""
        arn, shard_id, streams = stream_table
        account = arn.split(":")[4]
        now = int(time.time())

        def token(*fields: str) -> str:
            return base64.standard_b64encode("|".join(fields).encode()).decode()

        def read(t: str):
            return streams.get_records(ShardIterator=t, Limit=10)

        # Control: the same hand-built shape with the issued ARN is served, so
        # the refusals below come from the ARN and the shape, not the encoding.
        read(token(shard_id, "AFTER_SEQUENCE_NUMBER", "", str(now), arn))
        foreign = arn.replace(f":{account}:", f":{OTHER_ACCOUNT}:", 1)
        err = _error(read, token(shard_id, "AFTER_SEQUENCE_NUMBER", "", str(now), foreign))
        assert err == {"Code": "ValidationException", "Message": "Invalid ShardIterator"}, err
        err = _error(read, token(shard_id, "AFTER_SEQUENCE_NUMBER", "", str(now)))
        assert err == {
            "Code": "ExpiredIteratorException", "Message": "Shard iterator has expired"
        }, err
