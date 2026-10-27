# SPDX-License-Identifier: Apache-2.0

"""GetRecords reads only from a shard of the stream its iterator names.

The shard iterator carries the stream ARN the request is authorized against.
The records read resolves the shard against that stream, so an iterator that
pairs an allowed stream ARN with another stream's shard reads nothing, and an
iterator reads only while its ARN is the table's current stream.

These tests require an ExtendDB endpoint (DYNAMODB_ENDPOINT): Amazon DynamoDB
serves streams on a separate endpoint and its iterator tokens are opaque, so
a token naming another stream's shard cannot be built against it.
"""

from __future__ import annotations

import base64
import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import STREAM_DOES_NOT_EXIST, unique_name, wait_for_active, wait_for_deleted

pytestmark = pytest.mark.skipif(
    not os.environ.get("DYNAMODB_ENDPOINT", "").strip(),
    reason="Streams tests require extenddb (DYNAMODB_ENDPOINT)",
)

STREAM_SPEC = {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}


def _streams_client(endpoint_url, parameter_validation: bool):
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": botocore.config.Config(
            retries={"max_attempts": 0}, parameter_validation=parameter_validation
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def streams_client(endpoint_url):
    """DynamoDB Streams client pointing at the extenddb endpoint."""
    return _streams_client(endpoint_url, parameter_validation=True)


@pytest.fixture(scope="module")
def raw_streams_client(endpoint_url):
    """The same client with the SDK's parameter validation off, so a request
    the SDK would refuse locally reaches the server."""
    return _streams_client(endpoint_url, parameter_validation=False)


def _create_stream_table(dynamodb_client) -> tuple[str, str]:
    name = unique_name("shardown")
    resp = dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification=STREAM_SPEC,
    )
    wait_for_active(dynamodb_client, name)
    return name, resp["TableDescription"]["LatestStreamArn"]


def _shards(streams_client, stream_arn: str) -> list[str]:
    desc = streams_client.describe_stream(StreamArn=stream_arn)
    return [s["ShardId"] for s in desc["StreamDescription"]["Shards"]]


def _first_shard(streams_client, stream_arn: str) -> str:
    return _shards(streams_client, stream_arn)[0]


def _trim_horizon(streams_client, stream_arn: str, shard_id: str) -> str:
    return streams_client.get_shard_iterator(
        StreamArn=stream_arn, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]


class TestShardBelongsToIteratorStream:
    def test_allowed_stream_arn_with_another_streams_shard_reads_nothing(
        self, dynamodb_client, streams_client
    ):
        """A token naming stream A's ARN and one of stream B's shards reads
        nothing: ResourceNotFoundException for stream A, the same answer as a
        shard that does not exist. Stream B's own iterator reads its records."""
        name_a, arn_a = _create_stream_table(dynamodb_client)
        name_b, arn_b = _create_stream_table(dynamodb_client)
        try:
            for i in range(8):
                dynamodb_client.put_item(TableName=name_b, Item={"pk": {"S": f"k{i}"}})
            shard_b = _first_shard(streams_client, arn_b)
            now = int(time.time())
            raw = f"{shard_b}|AFTER_SEQUENCE_NUMBER||{now}|{arn_a}"
            forged = base64.standard_b64encode(raw.encode()).decode()

            with pytest.raises(ClientError) as exc:
                streams_client.get_records(ShardIterator=forged)
            err = exc.value.response["Error"]
            assert err["Code"] == "ResourceNotFoundException", err
            assert err["Message"] == STREAM_DOES_NOT_EXIST, err

            raw = f"shardId-forged-0000000000000000|AFTER_SEQUENCE_NUMBER||{now}|{arn_a}"
            forged = base64.standard_b64encode(raw.encode()).decode()
            with pytest.raises(ClientError) as exc:
                streams_client.get_records(ShardIterator=forged)
            err = exc.value.response["Error"]
            assert err["Code"] == "ResourceNotFoundException", err
            assert err["Message"] == STREAM_DOES_NOT_EXIST, err

            # Control: the shard reads under its own stream's iterator, and
            # stream A's own shard reads (nothing was written to A).
            resp = streams_client.get_records(
                ShardIterator=_trim_horizon(streams_client, arn_b, shard_b)
            )
            assert resp["NextShardIterator"]
            resp = streams_client.get_records(
                ShardIterator=_trim_horizon(
                    streams_client, arn_a, _first_shard(streams_client, arn_a)
                )
            )
            assert resp["Records"] == []
            assert resp["NextShardIterator"]
        finally:
            for name in (name_a, name_b):
                dynamodb_client.delete_table(TableName=name)
            for name in (name_a, name_b):
                wait_for_deleted(dynamodb_client, name)

    def test_iterator_reads_only_while_its_arn_is_the_tables_stream(
        self, dynamodb_client, streams_client
    ):
        """An iterator issued before the stream was disabled and enabled again
        reads nothing that was written to the stream that replaced it.

        While the table keeps its stream ARN across the cycle, the old
        iterators read the same records a fresh iterator on each shard reads.
        When the re-enabled stream has a new ARN (as on the service, measured
        2026-10-25), the old stream is closed with its own shard ids: an old
        iterator answers 200, returns none of the keys written after the
        re-enable, and its shard id is not one of the new stream's."""
        name, old_arn = _create_stream_table(dynamodb_client)
        try:
            old_shards = _shards(streams_client, old_arn)
            old_its = {s: _trim_horizon(streams_client, old_arn, s) for s in old_shards}
            dynamodb_client.update_table(
                TableName=name, StreamSpecification={"StreamEnabled": False}
            )
            wait_for_active(dynamodb_client, name)
            dynamodb_client.update_table(TableName=name, StreamSpecification=STREAM_SPEC)
            wait_for_active(dynamodb_client, name)
            new_arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
            new_keys = {f"n{i}" for i in range(8)}
            for key in sorted(new_keys):
                dynamodb_client.put_item(TableName=name, Item={"pk": {"S": key}})
            time.sleep(1)

            new_shards = _shards(streams_client, new_arn)
            new_records = [
                r
                for s in new_shards
                for r in streams_client.get_records(
                    ShardIterator=_trim_horizon(streams_client, new_arn, s)
                )["Records"]
            ]
            assert {r["dynamodb"]["Keys"]["pk"]["S"] for r in new_records} == new_keys

            old_pages = {
                s: streams_client.get_records(ShardIterator=it) for s, it in old_its.items()
            }
            if new_arn == old_arn:
                # The table kept its stream: the old iterators read, across the
                # four shards, exactly what the fresh iterators read.
                old_seqs = sorted(
                    r["dynamodb"]["SequenceNumber"]
                    for p in old_pages.values()
                    for r in p["Records"]
                )
                assert old_seqs == sorted(r["dynamodb"]["SequenceNumber"] for r in new_records)
            else:
                # The old stream was replaced: its shards are not the new
                # stream's, and its iterators return none of the new keys.
                assert not set(old_shards) & set(new_shards)
                for page in old_pages.values():
                    old_keys = {r["dynamodb"]["Keys"]["pk"]["S"] for r in page["Records"]}
                    assert not old_keys & new_keys
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

class TestGetRecordsLimitBounds:
    def test_limit_outside_1_to_1000_is_refused_before_the_read(
        self, dynamodb_client, streams_client, raw_streams_client
    ):
        """Limit 0 and -1 get the model's constraint sentence and Limit 1001
        the parameter sentence (both measured 2026-10-25); 1 and 1000 read.
        The bounds are checked before storage runs, so a Limit of 0 is never
        mistaken for a shard that is not the stream's."""
        name, arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k0"}})
            shard = _first_shard(streams_client, arn)
            it = _trim_horizon(streams_client, arn, shard)
            for limit in (0, -1):
                with pytest.raises(ClientError) as exc:
                    raw_streams_client.get_records(ShardIterator=it, Limit=limit)
                err = exc.value.response["Error"]
                assert err["Code"] == "ValidationException", err
                assert err["Message"] == (
                    "1 validation error detected: Value at 'limit' failed to satisfy "
                    "constraint: Member must have value greater than or equal to 1"
                ), err
            with pytest.raises(ClientError) as exc:
                raw_streams_client.get_records(ShardIterator=it, Limit=1001)
            err = exc.value.response["Error"]
            assert err["Code"] == "ValidationException", err
            assert err["Message"] == (
                "One or more parameter values were invalid: "
                "Limit should be less than or equal to 1000"
            ), err
            for limit in (1, 1000):
                resp = raw_streams_client.get_records(ShardIterator=it, Limit=limit)
                assert resp["NextShardIterator"]
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)
