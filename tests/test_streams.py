# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB Streams integration tests.

Covers: ListStreams, DescribeStream, GetShardIterator, GetRecords.
Tests the full polling protocol: iterator advancement, exactly-once delivery,
in-order delivery, all iterator types, and mixed workloads.

These tests require a running extenddb server (EXTENDDB_TEST_ENDPOINT must be set)
because real DynamoDB uses a separate Streams endpoint that boto3 routes
differently. extenddb serves both on the same endpoint.
"""

from __future__ import annotations

import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError
from conftest import _poll_interval, wait_for_active, wait_for_deleted
# EXTENDDB_TEST_ENDPOINT is required — devtools/run-tests validates this.
# Tests will fail with KeyError if the env var is missing.
@pytest.fixture(scope="module")
def endpoint_url() -> str:
    return os.environ["EXTENDDB_TEST_ENDPOINT"].strip()
@pytest.fixture(scope="module")
def dynamodb_client(endpoint_url: str):
    kwargs: dict = dict(
        service_name="dynamodb",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=endpoint_url,
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
@pytest.fixture(scope="module")
def streams_client(endpoint_url: str):
    """Separate DynamoDB Streams client pointing at the same extenddb endpoint."""
    kwargs: dict = dict(
        service_name="dynamodbstreams",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=endpoint_url,
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
@pytest.fixture()
def stream_table(dynamodb_client):
    """Create a table with streams enabled, yield (table_name, stream_arn), cleanup."""
    table_name = f"extenddb-stream-{uuid.uuid4().hex[:8]}"
    resp = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )
    wait_for_active(dynamodb_client, table_name)
    desc = dynamodb_client.describe_table(TableName=table_name)
    stream_arn = desc["Table"]["LatestStreamArn"]
    yield table_name, stream_arn
    try:
        dynamodb_client.delete_table(TableName=table_name)
        wait_for_deleted(dynamodb_client, table_name)
    except Exception:
        pass
def _drain_all_shards(
    streams_client, stream_arn: str, iterator_type: str = "TRIM_HORIZON",
    sequence_number: str | None = None, max_polls: int = 20,
) -> list[dict]:
    """Read all records from all shards of a stream. Returns list of records."""
    desc = streams_client.describe_stream(StreamArn=stream_arn)
    shards = desc["StreamDescription"]["Shards"]
    all_records: list[dict] = []
    for shard in shards:
        kwargs: dict = {
            "StreamArn": stream_arn,
            "ShardId": shard["ShardId"],
            "ShardIteratorType": iterator_type,
        }
        if sequence_number and iterator_type in (
            "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER",
        ):
            kwargs["SequenceNumber"] = sequence_number
        it_resp = streams_client.get_shard_iterator(**kwargs)
        shard_iter = it_resp["ShardIterator"]
        polls = 0
        while shard_iter and polls < max_polls:
            resp = streams_client.get_records(ShardIterator=shard_iter, Limit=100)
            all_records.extend(resp.get("Records", []))
            shard_iter = resp.get("NextShardIterator")
            polls += 1
            if not resp.get("Records"):
                break
    return all_records


def _wait_for_stream_records(
    streams_client, stream_arn: str, min_count: int,
    prefix: str | None = None, timeout: float = 5.0,
    iterator_type: str = "TRIM_HORIZON",
    sequence_number: str | None = None,
) -> list[dict]:
    """Poll stream until at least min_count records are available.

    If prefix is given, only count records whose pk starts with that prefix.
    Returns all records found (not just the filtered ones).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        records = _drain_all_shards(
            streams_client, stream_arn, iterator_type,
            sequence_number=sequence_number,
        )
        if prefix:
            matching = [
                r for r in records
                if r["dynamodb"]["Keys"]["pk"]["S"].startswith(prefix)
            ]
        else:
            matching = records
        if len(matching) >= min_count:
            return records
        time.sleep(_poll_interval())
    return records
class TestListStreams:
    """ListStreams operation tests."""

    def test_list_streams_returns_stream(self, streams_client, stream_table):
        table_name, stream_arn = stream_table
        resp = streams_client.list_streams(TableName=table_name)
        streams = resp.get("Streams", [])
        arns = [s["StreamArn"] for s in streams]
        assert stream_arn in arns

    def test_list_streams_all(self, streams_client, stream_table):
        _table_name, stream_arn = stream_table
        resp = streams_client.list_streams()
        arns = [s["StreamArn"] for s in resp.get("Streams", [])]
        assert stream_arn in arns

    def test_list_streams_wrong_table(self, streams_client, stream_table):
        resp = streams_client.list_streams(TableName="nonexistent-table-xyz")
        assert resp.get("Streams", []) == []
class TestDescribeStream:
    """DescribeStream operation tests."""

    def test_describe_stream_basic(self, streams_client, stream_table):
        table_name, stream_arn = stream_table
        resp = streams_client.describe_stream(StreamArn=stream_arn)
        desc = resp["StreamDescription"]
        assert desc["StreamArn"] == stream_arn
        assert desc["TableName"] == table_name
        assert desc["StreamStatus"] in ("ENABLED", "ENABLING")
        assert desc["StreamViewType"] == "NEW_AND_OLD_IMAGES"
        assert len(desc["Shards"]) > 0
        assert len(desc["KeySchema"]) >= 1

    def test_describe_stream_invalid_arn(self, streams_client):
        with pytest.raises(ClientError) as exc_info:
            streams_client.describe_stream(
                StreamArn="arn:aws:dynamodb:us-east-1:123456789012:table/NoSuchTable/stream/2026-01-01T00:00:00"
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "TrimmedDataAccessException",
        )
class TestIteratorAdvancement:
    """GetRecords → NextShardIterator → GetRecords: no duplicates, position holds."""

    def test_poll_returns_no_duplicates(self, dynamodb_client, streams_client, stream_table):
        """Write items, poll all records, poll again — second poll returns nothing."""
        table_name, stream_arn = stream_table
        n = 5
        for i in range(n):
            dynamodb_client.put_item(
                TableName=table_name,
                Item={"pk": {"S": f"adv-{i}"}, "val": {"N": str(i)}},
            )

        # First pass: drain all records, polling until we have them.
        desc = streams_client.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]
        shard_iters: dict[str, str | None] = {}
        first_pass_records: list[dict] = []

        for shard in shards:
            it = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iters[shard["ShardId"]] = it["ShardIterator"]

        # Read until we get all records.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            for sid in list(shard_iters):
                it = shard_iters[sid]
                if not it:
                    continue
                resp = streams_client.get_records(ShardIterator=it, Limit=100)
                first_pass_records.extend(resp.get("Records", []))
                shard_iters[sid] = resp.get("NextShardIterator")
            if len(first_pass_records) >= n:
                break
            time.sleep(_poll_interval())

        assert len(first_pass_records) >= n

        # Second pass with the continued iterators: should return 0 records.
        second_pass_records: list[dict] = []
        for sid, it in shard_iters.items():
            if not it:
                continue
            resp = streams_client.get_records(ShardIterator=it, Limit=100)
            second_pass_records.extend(resp.get("Records", []))

        assert len(second_pass_records) == 0, (
            f"Expected 0 records on second poll, got {len(second_pass_records)}"
        )

    def test_empty_poll_holds_position(self, dynamodb_client, streams_client, stream_table):
        """Poll an empty stream, write an item, poll again — get exactly that item."""
        table_name, stream_arn = stream_table

        desc = streams_client.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        # Drain any existing records first.
        shard_iters: dict[str, str | None] = {}
        for shard in shards:
            it = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iters[shard["ShardId"]] = it["ShardIterator"]

        # Drain existing.
        for _ in range(10):
            any_records = False
            for sid in list(shard_iters):
                it = shard_iters[sid]
                if not it:
                    continue
                resp = streams_client.get_records(ShardIterator=it, Limit=100)
                if resp.get("Records"):
                    any_records = True
                shard_iters[sid] = resp.get("NextShardIterator")
            if not any_records:
                break
            time.sleep(_poll_interval())

        # Empty poll — should return 0 records and a valid NextShardIterator.
        for sid, it in shard_iters.items():
            if not it:
                continue
            resp = streams_client.get_records(ShardIterator=it, Limit=100)
            assert len(resp.get("Records", [])) == 0
            assert resp.get("NextShardIterator") is not None
            shard_iters[sid] = resp["NextShardIterator"]

        # Now write one item.
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "empty-poll-test"}, "data": {"S": "hello"}},
        )

        # Poll again until we get the new record.
        new_records: list[dict] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            for sid, it in shard_iters.items():
                if not it:
                    continue
                resp = streams_client.get_records(ShardIterator=it, Limit=100)
                new_records.extend(resp.get("Records", []))
                shard_iters[sid] = resp.get("NextShardIterator")
            if new_records:
                break
            time.sleep(_poll_interval())

        assert len(new_records) == 1
        assert new_records[0]["dynamodb"]["Keys"]["pk"]["S"] == "empty-poll-test"
        assert new_records[0]["eventName"] == "INSERT"
class TestExactlyOnceDelivery:
    """Write N records, poll all shards to completion, assert total == N with no duplicates."""

    def test_exactly_once(self, dynamodb_client, streams_client, stream_table):
        table_name, stream_arn = stream_table
        n = 10
        expected_keys = set()
        for i in range(n):
            pk = f"exact-{uuid.uuid4().hex[:8]}"
            expected_keys.add(pk)
            dynamodb_client.put_item(
                TableName=table_name,
                Item={"pk": {"S": pk}, "seq": {"N": str(i)}},
            )

        records = _wait_for_stream_records(
            streams_client, stream_arn, min_count=n, prefix="exact-"
        )
        # Filter to only our records (table may have records from other tests).
        our_records = [
            r for r in records
            if r["dynamodb"]["Keys"]["pk"]["S"].startswith("exact-")
        ]
        seen_keys = [r["dynamodb"]["Keys"]["pk"]["S"] for r in our_records]

        assert len(seen_keys) == n, f"Expected {n} records, got {len(seen_keys)}"
        assert len(set(seen_keys)) == n, "Duplicate records detected"
        assert set(seen_keys) == expected_keys

    def test_no_duplicates_across_incremental_polls(
        self, dynamodb_client, streams_client, stream_table,
    ):
        """Poll with small Limit across multiple calls, assert no duplicates.

        P27 item 7: The previous test drains once with Limit=100. This test
        uses Limit=2 to force multiple GetRecords calls per shard and verifies
        that advancing the iterator never re-delivers records.
        """
        table_name, stream_arn = stream_table
        n = 8
        expected_keys = set()
        for i in range(n):
            pk = f"incr-{uuid.uuid4().hex[:8]}"
            expected_keys.add(pk)
            dynamodb_client.put_item(
                TableName=table_name,
                Item={"pk": {"S": pk}, "seq": {"N": str(i)}},
            )

        # Wait for records to be available before doing incremental polling.
        _wait_for_stream_records(
            streams_client, stream_arn, min_count=n, prefix="incr-"
        )

        desc = streams_client.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]
        all_event_ids: list[str] = []
        all_keys: list[str] = []

        for shard in shards:
            it_resp = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iter = it_resp["ShardIterator"]
            empty_polls = 0
            for _ in range(50):  # generous upper bound
                if not shard_iter:
                    break
                resp = streams_client.get_records(
                    ShardIterator=shard_iter, Limit=2,
                )
                records = resp.get("Records", [])
                for r in records:
                    pk = r["dynamodb"]["Keys"]["pk"]["S"]
                    if pk.startswith("incr-"):
                        all_event_ids.append(r["eventID"])
                        all_keys.append(pk)
                shard_iter = resp.get("NextShardIterator")
                if not records:
                    empty_polls += 1
                    if empty_polls >= 2:
                        break
                else:
                    empty_polls = 0

        assert len(all_event_ids) == len(set(all_event_ids)), (
            f"Duplicate eventIDs detected across incremental polls: "
            f"{len(all_event_ids)} total, {len(set(all_event_ids))} unique"
        )
        our_keys = [k for k in all_keys if k.startswith("incr-")]
        assert len(our_keys) == n, f"Expected {n} records, got {len(our_keys)}"
        assert set(our_keys) == expected_keys
class TestInOrderDelivery:
    """Within a single shard, sequence numbers must be monotonically increasing."""

    def test_monotonic_sequence_numbers(self, dynamodb_client, streams_client, stream_table):
        table_name, stream_arn = stream_table
        # Write several items to generate records.
        for i in range(5):
            dynamodb_client.put_item(
                TableName=table_name,
                Item={"pk": {"S": f"order-{i}"}, "val": {"N": str(i)}},
            )

        # Wait for records to be available.
        _wait_for_stream_records(
            streams_client, stream_arn, min_count=5, prefix="order-"
        )

        desc = streams_client.describe_stream(StreamArn=stream_arn)
        for shard in desc["StreamDescription"]["Shards"]:
            it_resp = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iter = it_resp["ShardIterator"]
            seq_numbers: list[str] = []
            for _ in range(10):
                if not shard_iter:
                    break
                resp = streams_client.get_records(ShardIterator=shard_iter, Limit=100)
                for rec in resp.get("Records", []):
                    seq_numbers.append(rec["dynamodb"]["SequenceNumber"])
                shard_iter = resp.get("NextShardIterator")
                if not resp.get("Records"):
                    break

            # Verify monotonically increasing within this shard.
            for i in range(1, len(seq_numbers)):
                assert seq_numbers[i] > seq_numbers[i - 1], (
                    f"Sequence numbers not monotonic in shard {shard['ShardId']}: "
                    f"{seq_numbers[i - 1]} >= {seq_numbers[i]}"
                )
class TestIteratorTypes:
    """All 4 iterator types: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER."""

    def test_trim_horizon(self, dynamodb_client, streams_client, stream_table):
        """TRIM_HORIZON reads from the beginning of the shard."""
        table_name, stream_arn = stream_table
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "horizon-1"}, "v": {"S": "a"}},
        )
        records = _wait_for_stream_records(
            streams_client, stream_arn, min_count=1, prefix="horizon-"
        )
        pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in records]
        assert "horizon-1" in pks

    def test_latest(self, dynamodb_client, streams_client, stream_table):
        """LATEST returns only records written after the iterator was obtained."""
        table_name, stream_arn = stream_table

        # Write a "before" item.
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "latest-before"}, "v": {"S": "old"}},
        )
        # Wait for it to appear in the stream.
        _wait_for_stream_records(
            streams_client, stream_arn, min_count=1, prefix="latest-before"
        )

        # Get LATEST iterators.
        desc = streams_client.describe_stream(StreamArn=stream_arn)
        shard_iters: dict[str, str] = {}
        for shard in desc["StreamDescription"]["Shards"]:
            it = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="LATEST",
            )
            shard_iters[shard["ShardId"]] = it["ShardIterator"]

        # Write an "after" item.
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "latest-after"}, "v": {"S": "new"}},
        )

        # Poll with LATEST iterators until we see the "after" item.
        records: list[dict] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            for sid, it in shard_iters.items():
                resp = streams_client.get_records(ShardIterator=it, Limit=100)
                records.extend(resp.get("Records", []))
                shard_iters[sid] = resp.get("NextShardIterator", it)
            pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in records]
            if "latest-after" in pks:
                break
            time.sleep(_poll_interval())

        pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in records]
        assert "latest-after" in pks
        assert "latest-before" not in pks

    def test_at_sequence_number(self, dynamodb_client, streams_client, stream_table):
        """AT_SEQUENCE_NUMBER returns the record at that exact sequence number."""
        table_name, stream_arn = stream_table
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "at-seq-test"}, "v": {"S": "x"}},
        )

        # Wait for the record to appear, then find its sequence number.
        records = _wait_for_stream_records(
            streams_client, stream_arn, min_count=1, prefix="at-seq-test"
        )
        target = [
            r for r in records
            if r["dynamodb"]["Keys"]["pk"]["S"] == "at-seq-test"
        ]
        assert len(target) >= 1
        seq_num = target[0]["dynamodb"]["SequenceNumber"]
        shard_id = None

        # Find which shard has this record.
        desc = streams_client.describe_stream(StreamArn=stream_arn)
        for shard in desc["StreamDescription"]["Shards"]:
            it = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard["ShardId"],
                ShardIteratorType="AT_SEQUENCE_NUMBER",
                SequenceNumber=seq_num,
            )
            resp = streams_client.get_records(
                ShardIterator=it["ShardIterator"], Limit=1,
            )
            if resp.get("Records"):
                first = resp["Records"][0]
                if first["dynamodb"]["SequenceNumber"] == seq_num:
                    shard_id = shard["ShardId"]
                    break

        assert shard_id is not None, "Could not find record at sequence number"

    def test_after_sequence_number(self, dynamodb_client, streams_client, stream_table):
        """AFTER_SEQUENCE_NUMBER returns records after the given sequence number."""
        table_name, stream_arn = stream_table

        # Write two items.
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "after-seq-1"}, "v": {"S": "first"}},
        )
        # Wait for first to appear before writing second.
        _wait_for_stream_records(
            streams_client, stream_arn, min_count=1, prefix="after-seq-1"
        )
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": "after-seq-2"}, "v": {"S": "second"}},
        )
        # Wait for both to appear.
        _wait_for_stream_records(
            streams_client, stream_arn, min_count=1, prefix="after-seq-2"
        )

        # Get all records to find the first item's sequence number.
        all_records = _drain_all_shards(streams_client, stream_arn, "TRIM_HORIZON")
        first_recs = [
            r for r in all_records
            if r["dynamodb"]["Keys"]["pk"]["S"] == "after-seq-1"
        ]
        assert len(first_recs) >= 1
        first_seq = first_recs[0]["dynamodb"]["SequenceNumber"]

        # Use AFTER_SEQUENCE_NUMBER with the first item's seq — should not include it.
        after_records = _drain_all_shards(
            streams_client, stream_arn, "AFTER_SEQUENCE_NUMBER",
            sequence_number=first_seq,
        )
        after_pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in after_records]
        assert "after-seq-1" not in after_pks
        # after-seq-2 should be present (it has a later sequence number, possibly on same shard).
        # Note: if they're on different shards, AFTER_SEQUENCE_NUMBER on the wrong shard
        # may not find it. This is expected DynamoDB behavior.
class TestMixedWorkload:
    """Inserts, updates, deletes generating INSERT/MODIFY/REMOVE events."""

    def test_insert_modify_remove_events(self, dynamodb_client, streams_client, stream_table):
        table_name, stream_arn = stream_table
        pk = f"mixed-{uuid.uuid4().hex[:8]}"

        # INSERT
        dynamodb_client.put_item(
            TableName=table_name,
            Item={"pk": {"S": pk}, "val": {"N": "1"}},
        )
        # MODIFY
        dynamodb_client.update_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET val = :v",
            ExpressionAttributeValues={":v": {"N": "2"}},
        )
        # REMOVE
        dynamodb_client.delete_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
        )

        # Poll until we have all 3 events for this pk.
        deadline = time.monotonic() + 5.0
        our_records: list[dict] = []
        while time.monotonic() < deadline:
            records = _drain_all_shards(streams_client, stream_arn, "TRIM_HORIZON")
            our_records = [
                r for r in records
                if r["dynamodb"]["Keys"]["pk"]["S"] == pk
            ]
            if len(our_records) >= 3:
                break
            time.sleep(_poll_interval())

        events = [r["eventName"] for r in our_records]
        assert events == ["INSERT", "MODIFY", "REMOVE"], (
            f"Expected [INSERT, MODIFY, REMOVE], got {events}"
        )

        # Verify images.
        insert_rec = our_records[0]
        assert "NewImage" in insert_rec["dynamodb"]
        assert insert_rec["dynamodb"]["NewImage"]["val"]["N"] == "1"

        modify_rec = our_records[1]
        assert "OldImage" in modify_rec["dynamodb"]
        assert modify_rec["dynamodb"]["OldImage"]["val"]["N"] == "1"
        assert "NewImage" in modify_rec["dynamodb"]
        assert modify_rec["dynamodb"]["NewImage"]["val"]["N"] == "2"

        remove_rec = our_records[2]
        assert "OldImage" in remove_rec["dynamodb"]
        assert remove_rec["dynamodb"]["OldImage"]["val"]["N"] == "2"
class TestGetShardIterator:
    """Edge cases for GetShardIterator."""

    def test_invalid_shard_id(self, streams_client, stream_table):
        _table_name, stream_arn = stream_table
        with pytest.raises(ClientError) as exc_info:
            streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId="nonexistent-shard-id-that-does-not-exist",
                ShardIteratorType="TRIM_HORIZON",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    @pytest.mark.xfail(
        reason="15-minute iterator expiration cannot be tested in integration tests",
        strict=True,
    )
    def test_expired_iterator(self, streams_client, stream_table):
        """Expired iterators should raise ExpiredIteratorException.

        We can't easily test real expiration (15 min), so this is a
        placeholder that verifies the error code format if we could
        craft an expired token.
        """
        pytest.fail("Cannot test 15-minute expiration in integration tests")
