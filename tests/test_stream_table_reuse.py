# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A stream-enabled table name can be reused.

Shard ids used to be derived from the table name, while the shard store is
keyed by shard id alone (and on PostgreSQL keeps a deleted table's shards).
A second stream-enabled table under the same name then failed CreateTable
with InternalServerError: a delete-and-recreate in one account on
PostgreSQL, or the same name chosen by a second account on PostgreSQL and
SQLite. A long table name also produced
a ShardId over the 65-character limit the AWS SDKs enforce.

ExtendDB only: Amazon DynamoDB serves streams on a separate endpoint, and the
two-account case needs the management API.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from test_idempotency_account_scope import two_account_clients  # noqa: F401 - fixture

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="streams are served on the same endpoint by ExtendDB only",
)

STREAM_SPEC = {"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"}
# The AWS SDK model bounds ShardId to 28..65 characters.
SHARD_ID_MIN, SHARD_ID_MAX = 28, 65


def _streams_client_for(ddb_client):
    """A dynamodbstreams client sharing the given client's endpoint and keys."""
    creds = ddb_client._request_signer._credentials  # noqa: SLF001 - test helper
    endpoint = ddb_client.meta.endpoint_url
    kwargs: dict = dict(
        service_name="dynamodbstreams",
        region_name=ddb_client.meta.region_name,
        endpoint_url=endpoint,
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
    )
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _create_stream_table(client, name: str) -> str:
    client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification=STREAM_SPEC,
    )
    wait_for_active(client, name)
    return client.describe_table(TableName=name)["Table"]["LatestStreamArn"]


def _drop(client, name: str) -> None:
    try:
        client.delete_table(TableName=name)
    except client.exceptions.ResourceNotFoundException:
        return
    wait_for_deleted(client, name)


def _stream_pks(streams, stream_arn: str) -> list[str]:
    """Partition keys of every record in the stream, all shards, from the start."""
    desc = streams.describe_stream(StreamArn=stream_arn)["StreamDescription"]
    pks: list[str] = []
    for shard in desc["Shards"]:
        it = streams.get_shard_iterator(
            StreamArn=stream_arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        for _ in range(20):
            resp = streams.get_records(ShardIterator=it)
            pks += [r["dynamodb"]["Keys"]["pk"]["S"] for r in resp["Records"]]
            it = resp.get("NextShardIterator")
            if not it or not resp["Records"]:
                break
    return pks


def test_recreate_stream_table_under_same_name(dynamodb_client):
    streams = _streams_client_for(dynamodb_client)
    name = f"stream-reuse-{uuid.uuid4().hex[:8]}"
    try:
        first_arn = _create_stream_table(dynamodb_client, name)
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "first"}})
        _drop(dynamodb_client, name)

        second_arn = _create_stream_table(dynamodb_client, name)
        assert second_arn != first_arn
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "second"}})

        # The new stream carries only the new table's records, and the old
        # ARN does not resolve to it.
        assert _stream_pks(streams, second_arn) == ["second"]
        with pytest.raises(ClientError) as exc:
            streams.describe_stream(StreamArn=first_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _drop(dynamodb_client, name)


def test_two_accounts_stream_tables_with_one_name(two_account_clients):  # noqa: F811
    client_a, client_b = two_account_clients
    name = f"stream-shared-{uuid.uuid4().hex[:8]}"
    try:
        arn_a = _create_stream_table(client_a, name)
        arn_b = _create_stream_table(client_b, name)
        client_a.put_item(TableName=name, Item={"pk": {"S": "from-a"}})
        client_b.put_item(TableName=name, Item={"pk": {"S": "from-b"}})

        assert _stream_pks(_streams_client_for(client_a), arn_a) == ["from-a"]
        assert _stream_pks(_streams_client_for(client_b), arn_b) == ["from-b"]
    finally:
        _drop(client_a, name)
        _drop(client_b, name)


def test_shard_ids_fit_sdk_bounds_for_a_long_table_name(dynamodb_client):
    streams = _streams_client_for(dynamodb_client)
    name = f"stream-long-{uuid.uuid4().hex[:8]}-" + "x" * 200
    try:
        arn = _create_stream_table(dynamodb_client, name)
        shards = streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"]
        assert shards
        for shard in shards:
            assert SHARD_ID_MIN <= len(shard["ShardId"]) <= SHARD_ID_MAX, shard["ShardId"]
        # boto3 validates ShardId length client-side, so this call is the
        # end-to-end check that the id is usable.
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k"}})
        assert _stream_pks(streams, arn) == ["k"]
    finally:
        _drop(dynamodb_client, name)


def test_stream_disable_and_reenable(dynamodb_client):
    streams = _streams_client_for(dynamodb_client)
    name = f"stream-toggle-{uuid.uuid4().hex[:8]}"
    try:
        _create_stream_table(dynamodb_client, name)
        dynamodb_client.update_table(
            TableName=name, StreamSpecification={"StreamEnabled": False}
        )
        wait_for_active(dynamodb_client, name)
        dynamodb_client.update_table(TableName=name, StreamSpecification=STREAM_SPEC)
        wait_for_active(dynamodb_client, name)
        arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "after"}})
        assert "after" in _stream_pks(streams, arn)
    finally:
        _drop(dynamodb_client, name)
