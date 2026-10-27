# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Stream records for TransactWriteItems.

A transacted Put, Update or Delete records like the equivalent single-item
write. A ConditionCheck records nothing, and a cancelled transaction records
nothing for any of its items.

Assertions are per key. Records for one key share a shard and carry ascending
sequence numbers there, while records for different keys land in different
shards, whose sequence numbers are not comparable.

These tests require an extenddb endpoint (DYNAMODB_ENDPOINT) because real
DynamoDB uses a separate Streams endpoint.
"""

from __future__ import annotations

import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

pytestmark = pytest.mark.skipif(
    not os.environ.get("DYNAMODB_ENDPOINT", "").strip(),
    reason="Streams tests require extenddb (DYNAMODB_ENDPOINT)",
)


@pytest.fixture(scope="module")
def streams_client(endpoint_url):
    """DynamoDB Streams client pointing at the extenddb endpoint."""
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": botocore.config.Config(retries={"max_attempts": 0}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


def _create_stream_table(
    dynamodb_client, view_type: str = "NEW_AND_OLD_IMAGES"
) -> tuple[str, str]:
    """Create a table with streams enabled, return (table_name, stream_arn)."""
    name = unique_name("txnstream")
    resp = dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": view_type,
        },
    )
    stream_arn = resp["TableDescription"]["LatestStreamArn"]
    wait_for_active(dynamodb_client, name)
    return name, stream_arn


def _read_all_shards(streams_client, stream_arn: str, max_polls: int = 10) -> list[dict]:
    """Read records from every shard of a stream, keeping each shard's order.

    extenddb spreads records over several shards by hashing the partition key,
    so every shard has to be read to collect a transaction's records.
    """
    desc = streams_client.describe_stream(StreamArn=stream_arn)
    records: list[dict] = []
    for shard in desc["StreamDescription"]["Shards"]:
        resp = streams_client.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )
        iterator = resp["ShardIterator"]
        for _ in range(max_polls):
            resp = streams_client.get_records(ShardIterator=iterator)
            records.extend(resp["Records"])
            iterator = resp.get("NextShardIterator")
            if not iterator or not resp["Records"]:
                break
    return records


def _for_key(records: list[dict], key: str) -> list[dict]:
    """The records for one partition key, in the order the shard returned them."""
    return [r for r in records if r["dynamodb"]["Keys"]["pk"]["S"] == key]


def _drop(dynamodb_client, name: str) -> None:
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


class TestTransactWriteStreamRecords:
    """TransactWriteItems records to the stream like the single-item writes."""

    def test_transacted_put_update_delete_each_record(
        self, dynamodb_client, streams_client
    ):
        """One record per item mutation, with the event names and images."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            # Seed the keys all three ops act on: the Put overwrites an item, so
            # its record carries the seeded value as OldImage.
            for key, val in (("put", "seeded"), ("upd", "before"), ("del", "doomed")):
                dynamodb_client.put_item(
                    TableName=name, Item={"pk": {"S": key}, "v": {"S": val}}
                )
            time.sleep(1)

            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": name,
                            "Item": {"pk": {"S": "put"}, "v": {"S": "created"}},
                        }
                    },
                    {
                        "Update": {
                            "TableName": name,
                            "Key": {"pk": {"S": "upd"}},
                            "UpdateExpression": "SET v = :after",
                            "ExpressionAttributeValues": {":after": {"S": "after"}},
                        }
                    },
                    {
                        "Delete": {
                            "TableName": name,
                            "Key": {"pk": {"S": "del"}},
                        }
                    },
                ]
            )
            time.sleep(1)

            records = _read_all_shards(streams_client, stream_arn)
            assert len(records) == 6, f"three seeded plus three transacted, got {records}"

            # The seeded INSERT and the transacted MODIFY share this key's shard,
            # so the shard returns them in that order.
            put_records = _for_key(records, "put")
            assert [r["eventName"] for r in put_records] == ["INSERT", "MODIFY"]
            overwritten = put_records[1]
            assert overwritten["dynamodb"]["OldImage"]["v"]["S"] == "seeded"
            assert overwritten["dynamodb"]["NewImage"]["v"]["S"] == "created"

            update_records = _for_key(records, "upd")
            assert [r["eventName"] for r in update_records] == ["INSERT", "MODIFY"]
            modified = update_records[1]
            assert modified["dynamodb"]["OldImage"]["v"]["S"] == "before"
            assert modified["dynamodb"]["NewImage"]["v"]["S"] == "after"
            assert (
                modified["dynamodb"]["SequenceNumber"]
                > update_records[0]["dynamodb"]["SequenceNumber"]
            )

            delete_records = _for_key(records, "del")
            assert [r["eventName"] for r in delete_records] == ["INSERT", "REMOVE"]
            removed = delete_records[1]
            assert removed["dynamodb"]["OldImage"]["v"]["S"] == "doomed"
            assert "NewImage" not in removed["dynamodb"]

            for record in records:
                assert record["eventSource"] == "aws:dynamodb"
                assert record["dynamodb"]["StreamViewType"] == "NEW_AND_OLD_IMAGES"
        finally:
            _drop(dynamodb_client, name)

    def test_two_mutations_each_record(self, dynamodb_client, streams_client):
        """Two mutations on one streamed table record once each."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "first"}}}},
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "second"}}}},
                ]
            )
            time.sleep(1)

            records = _read_all_shards(streams_client, stream_arn)
            assert len(records) == 2, f"got {records}"
            assert {r["dynamodb"]["Keys"]["pk"]["S"] for r in records} == {
                "first",
                "second",
            }
            assert [r["eventName"] for r in records] == ["INSERT", "INSERT"]
        finally:
            _drop(dynamodb_client, name)

    def test_condition_check_records_nothing(self, dynamodb_client, streams_client):
        """A ConditionCheck records nothing; the Put beside it records."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "checked"}})
            time.sleep(1)

            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": name,
                            "Key": {"pk": {"S": "checked"}},
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    },
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "written"}}}},
                ]
            )
            time.sleep(1)

            records = _read_all_shards(streams_client, stream_arn)
            checked = _for_key(records, "checked")
            assert len(checked) == 1, f"the ConditionCheck recorded: {checked}"
            assert checked[0]["eventName"] == "INSERT"
            written = _for_key(records, "written")
            assert len(written) == 1, f"got {written}"
            assert written[0]["eventName"] == "INSERT"
        finally:
            _drop(dynamodb_client, name)

    def test_cancelled_transaction_records_nothing(
        self, dynamodb_client, streams_client
    ):
        """A transaction cancelled by a condition records nothing at all."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "taken"}})
            time.sleep(1)

            with pytest.raises(ClientError) as excinfo:
                dynamodb_client.transact_write_items(
                    TransactItems=[
                        {"Put": {"TableName": name, "Item": {"pk": {"S": "free"}}}},
                        {
                            "Put": {
                                "TableName": name,
                                "Item": {"pk": {"S": "taken"}},
                                "ConditionExpression": "attribute_not_exists(pk)",
                            }
                        },
                    ]
                )
            assert (
                excinfo.value.response["Error"]["Code"]
                == "TransactionCanceledException"
            )
            time.sleep(1)

            records = _read_all_shards(streams_client, stream_arn)
            assert len(records) == 1, f"cancelled transaction recorded: {records}"
            assert records[0]["dynamodb"]["Keys"]["pk"]["S"] == "taken"
            assert "Item" not in dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "free"}}
            )
        finally:
            _drop(dynamodb_client, name)

    def test_keys_only_stream_omits_images(self, dynamodb_client, streams_client):
        """A KEYS_ONLY stream records the keys and neither image."""
        name, stream_arn = _create_stream_table(dynamodb_client, view_type="KEYS_ONLY")
        try:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": name,
                            "Item": {"pk": {"S": "k"}, "v": {"S": "hidden"}},
                        }
                    }
                ]
            )
            time.sleep(1)

            records = _read_all_shards(streams_client, stream_arn)
            assert len(records) == 1, f"got {records}"
            assert records[0]["dynamodb"]["Keys"]["pk"]["S"] == "k"
            assert "NewImage" not in records[0]["dynamodb"]
            assert "OldImage" not in records[0]["dynamodb"]
            assert records[0]["dynamodb"]["StreamViewType"] == "KEYS_ONLY"
        finally:
            _drop(dynamodb_client, name)
