# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB Streams tests.

Covers ListStreams, DescribeStream, GetShardIterator, GetRecords,
iterator types, event types, exactly-once delivery, and in-order
delivery per primary key.

These tests require a extenddb endpoint (DYNAMODB_ENDPOINT) because
real DynamoDB uses a separate Streams endpoint.
"""

from __future__ import annotations

import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

# Streams tests only run against extenddb — real DynamoDB uses a separate endpoint.
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


def _create_stream_table(dynamodb_client) -> tuple[str, str]:
    """Create a table with streams enabled, return (table_name, stream_arn)."""
    name = unique_name("stream")
    resp = dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )
    stream_arn = resp["TableDescription"]["LatestStreamArn"]
    wait_for_active(dynamodb_client, name)
    return name, stream_arn


def _read_all_shards(
    streams_client,
    stream_arn: str,
    iterator_type: str = "TRIM_HORIZON",
    max_polls: int = 10,
) -> list[dict]:
    """Read records from ALL shards of a stream.

    extenddb uses multiple shards with CRC32 hash-based routing.  Records are
    distributed across shards, so we must read every shard to collect all
    records.
    """
    desc = streams_client.describe_stream(StreamArn=stream_arn)
    shards = desc["StreamDescription"]["Shards"]
    all_records: list[dict] = []
    for shard in shards:
        resp = streams_client.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard["ShardId"],
            ShardIteratorType=iterator_type,
        )
        iterator = resp["ShardIterator"]
        for _ in range(max_polls):
            resp = streams_client.get_records(ShardIterator=iterator)
            all_records.extend(resp["Records"])
            iterator = resp.get("NextShardIterator")
            if not iterator or not resp["Records"]:
                break
    return all_records


class TestListStreams:
    """ListStreams API tests."""

    def test_list_streams_returns_stream(self, dynamodb_client, streams_client):
        """ListStreams returns the stream for a table with streams enabled."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            resp = streams_client.list_streams(TableName=name)
            arns = [s["StreamArn"] for s in resp["Streams"]]
            assert stream_arn in arns
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_list_streams_all(self, dynamodb_client, streams_client):
        """ListStreams without TableName returns streams."""
        name, _ = _create_stream_table(dynamodb_client)
        try:
            resp = streams_client.list_streams()
            assert len(resp["Streams"]) >= 1
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)


class TestDescribeStream:
    """DescribeStream API tests."""

    def test_describe_stream_basic(self, dynamodb_client, streams_client):
        """DescribeStream returns stream details with shards."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            resp = streams_client.describe_stream(StreamArn=stream_arn)
            desc = resp["StreamDescription"]
            assert desc["StreamArn"] == stream_arn
            assert desc["StreamStatus"] in ("ENABLED", "ENABLING")
            assert desc["StreamViewType"] == "NEW_AND_OLD_IMAGES"
            assert len(desc["Shards"]) >= 1
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_describe_stream_invalid_arn(self, streams_client):
        """DescribeStream with invalid ARN fails."""
        with pytest.raises(ClientError) as exc:
            streams_client.describe_stream(StreamArn="arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent/stream/2026-01-01T00:00:00.000")
        # Could be ResourceNotFoundException or ValidationException
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )


class TestStreamRecords:
    """GetShardIterator and GetRecords tests.

    All tests read from ALL shards because extenddb distributes records
    across multiple shards via CRC32 hash-based routing.
    """

    def test_insert_event(self, dynamodb_client, streams_client):
        """PutItem generates an INSERT stream record."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "hello"}}
            )
            time.sleep(1)
            records = _read_all_shards(streams_client, stream_arn)
            insert_records = [r for r in records if r["eventName"] == "INSERT"]
            assert len(insert_records) >= 1
            new_image = insert_records[0]["dynamodb"]["NewImage"]
            assert new_image["pk"]["S"] == "k1"
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_modify_event(self, dynamodb_client, streams_client):
        """UpdateItem generates a MODIFY stream record."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "old"}}
            )
            time.sleep(1)
            dynamodb_client.update_item(
                TableName=name,
                Key={"pk": {"S": "k1"}},
                UpdateExpression="SET v = :new",
                ExpressionAttributeValues={":new": {"S": "new"}},
            )
            time.sleep(1)
            records = _read_all_shards(streams_client, stream_arn)
            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_remove_event(self, dynamodb_client, streams_client):
        """DeleteItem generates a REMOVE stream record."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "x"}}
            )
            time.sleep(1)
            dynamodb_client.delete_item(TableName=name, Key={"pk": {"S": "k1"}})
            time.sleep(1)
            records = _read_all_shards(streams_client, stream_arn)
            remove_records = [r for r in records if r["eventName"] == "REMOVE"]
            assert len(remove_records) >= 1
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_trim_horizon_iterator(self, dynamodb_client, streams_client):
        """TRIM_HORIZON reads from the beginning of the stream."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            for i in range(3):
                dynamodb_client.put_item(
                    TableName=name, Item={"pk": {"S": f"k{i}"}}
                )
            time.sleep(1)
            records = _read_all_shards(streams_client, stream_arn, "TRIM_HORIZON")
            assert len(records) >= 3
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_latest_iterator(self, dynamodb_client, streams_client):
        """LATEST iterator only sees records written after the iterator was obtained."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            # Write before getting iterators
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "before"}})
            time.sleep(1)

            # Get LATEST iterators for all shards
            desc = streams_client.describe_stream(StreamArn=stream_arn)
            shards = desc["StreamDescription"]["Shards"]
            iterators = []
            for shard in shards:
                resp = streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard["ShardId"],
                    ShardIteratorType="LATEST",
                )
                iterators.append(resp["ShardIterator"])

            # Write after getting iterators
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "after"}})
            time.sleep(1)

            # Read from all shards using the LATEST iterators
            all_records: list[dict] = []
            for it in iterators:
                resp = streams_client.get_records(ShardIterator=it)
                all_records.extend(resp["Records"])

            pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in all_records]
            assert "after" in pks
            assert "before" not in pks
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_empty_poll(self, dynamodb_client, streams_client):
        """Polling with no new records returns empty list and a new iterator."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        try:
            desc = streams_client.describe_stream(StreamArn=stream_arn)
            shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
            resp = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            iterator = resp["ShardIterator"]
            resp = streams_client.get_records(ShardIterator=iterator)
            assert resp["Records"] == []
            assert resp.get("NextShardIterator") is not None
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)


class TestExactlyOnceDelivery:
    """Verify that every written item appears exactly once across all shards."""

    def test_exactly_once_many_items(self, dynamodb_client, streams_client):
        """Write 50 distinct items, read all shards, verify each appears exactly once."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        num_items = 50
        try:
            for i in range(num_items):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={"pk": {"S": f"item-{i:04d}"}, "seq": {"N": str(i)}},
                )
            time.sleep(2)

            records = _read_all_shards(streams_client, stream_arn)
            insert_records = [r for r in records if r["eventName"] == "INSERT"]

            # Collect primary keys from INSERT records
            seen_pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in insert_records]
            expected_pks = {f"item-{i:04d}" for i in range(num_items)}

            # Every item must appear
            assert expected_pks == set(seen_pks), (
                f"Missing: {expected_pks - set(seen_pks)}, "
                f"Extra: {set(seen_pks) - expected_pks}"
            )
            # No duplicates
            assert len(seen_pks) == len(set(seen_pks)), (
                f"Duplicate records found: {len(seen_pks)} total vs "
                f"{len(set(seen_pks))} unique"
            )
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_exactly_once_mixed_operations(self, dynamodb_client, streams_client):
        """Write, update, and delete items — verify each operation appears exactly once."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        num_items = 30
        try:
            # INSERT 30 items
            for i in range(num_items):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={"pk": {"S": f"mix-{i:04d}"}, "v": {"N": str(i)}},
                )
            # MODIFY 15 items
            for i in range(0, num_items, 2):
                dynamodb_client.update_item(
                    TableName=name,
                    Key={"pk": {"S": f"mix-{i:04d}"}},
                    UpdateExpression="SET v = :new",
                    ExpressionAttributeValues={":new": {"N": str(i + 1000)}},
                )
            # REMOVE 10 items
            for i in range(0, num_items, 3):
                dynamodb_client.delete_item(
                    TableName=name, Key={"pk": {"S": f"mix-{i:04d}"}}
                )
            time.sleep(2)

            records = _read_all_shards(streams_client, stream_arn)

            inserts = [r for r in records if r["eventName"] == "INSERT"]
            modifies = [r for r in records if r["eventName"] == "MODIFY"]
            removes = [r for r in records if r["eventName"] == "REMOVE"]

            assert len(inserts) == num_items
            assert len(modifies) == 15  # every other item: 0,2,4,...,28
            assert len(removes) == 10  # every third item: 0,3,6,...,27
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)


class TestInOrderDelivery:
    """Verify that stream records for the same primary key arrive in order."""

    def test_in_order_per_key_many_updates(self, dynamodb_client, streams_client):
        """Perform 20 sequential updates to the same item, verify sequence order."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        num_updates = 20
        try:
            # Initial insert
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "ordered-key"}, "counter": {"N": "0"}},
            )
            # Sequential updates
            for i in range(1, num_updates + 1):
                dynamodb_client.update_item(
                    TableName=name,
                    Key={"pk": {"S": "ordered-key"}},
                    UpdateExpression="SET #c = :c",
                    ExpressionAttributeNames={"#c": "counter"},
                    ExpressionAttributeValues={":c": {"N": str(i)}},
                )
            time.sleep(2)

            records = _read_all_shards(streams_client, stream_arn)
            # Filter to records for our key
            key_records = [
                r for r in records
                if r["dynamodb"]["Keys"]["pk"]["S"] == "ordered-key"
            ]

            # Should have 1 INSERT + num_updates MODIFYs
            assert len(key_records) == num_updates + 1

            # Verify sequence numbers are monotonically increasing
            seq_nums = [r["dynamodb"]["SequenceNumber"] for r in key_records]
            assert seq_nums == sorted(seq_nums), (
                f"Sequence numbers not in order: {seq_nums}"
            )

            # Verify the event types are in order: INSERT then MODIFYs
            assert key_records[0]["eventName"] == "INSERT"
            for r in key_records[1:]:
                assert r["eventName"] == "MODIFY"
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)

    def test_in_order_multiple_keys(self, dynamodb_client, streams_client):
        """Updates to multiple keys maintain per-key ordering."""
        name, stream_arn = _create_stream_table(dynamodb_client)
        num_keys = 10
        updates_per_key = 5
        try:
            # Insert all keys
            for k in range(num_keys):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={"pk": {"S": f"key-{k:03d}"}, "v": {"N": "0"}},
                )
            # Update each key multiple times in round-robin
            for u in range(1, updates_per_key + 1):
                for k in range(num_keys):
                    dynamodb_client.update_item(
                        TableName=name,
                        Key={"pk": {"S": f"key-{k:03d}"}},
                        UpdateExpression="SET v = :val",
                        ExpressionAttributeValues={":val": {"N": str(u)}},
                    )
            time.sleep(2)

            records = _read_all_shards(streams_client, stream_arn)

            # Group records by primary key
            by_key: dict[str, list[dict]] = {}
            for r in records:
                pk = r["dynamodb"]["Keys"]["pk"]["S"]
                by_key.setdefault(pk, []).append(r)

            # Verify each key has the right count and ordering
            for k in range(num_keys):
                pk = f"key-{k:03d}"
                key_recs = by_key.get(pk, [])
                assert len(key_recs) == updates_per_key + 1, (
                    f"{pk}: expected {updates_per_key + 1} records, got {len(key_recs)}"
                )
                seq_nums = [r["dynamodb"]["SequenceNumber"] for r in key_recs]
                assert seq_nums == sorted(seq_nums), (
                    f"{pk}: sequence numbers not in order: {seq_nums}"
                )
        finally:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)
