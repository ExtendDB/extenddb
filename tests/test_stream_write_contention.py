# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Concurrent writes to a table with a stream enabled.

Amazon DynamoDB applies the writes one after another. Every PutItem and
DeleteItem answers 200, and the stream holds one record per write that
changed the item. A BatchWriteItem under the same contention answers 200 and
lists any request it did not apply in UnprocessedItems, so a put that
committed is never reported as failed and a client that replays
UnprocessedItems writes each item once.

On the MongoDB backend the stream sequence counter is drawn inside the
write's transaction, so concurrent writers on one table conflict on the
counter even when they write different keys. These tests fail on a server
that reports that conflict as an error instead of retrying it.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

WRITERS = 8
OPS_PER_WRITER = 60
KEYS = 4
BATCH_WRITERS = 8
BATCH_ROUNDS = 30


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every failure is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=BATCH_WRITERS * 2,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def streams_client(endpoint_url):
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def stream_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    ) as name:
        yield name


def _run_writers(count: int, body) -> list:
    """Run `body(i)` on `count` threads that start together; return their results."""
    start = threading.Barrier(count, timeout=30)
    out: list = [None] * count
    errors: list[BaseException] = []

    def run(i: int):
        try:
            start.wait()
            out[i] = body(i)
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return out


def _error_code(e: ClientError) -> str:
    return f"{e.response['ResponseMetadata']['HTTPStatusCode']} {e.response['Error']['Code']}"


def _stream_record_count(dynamodb_client, streams_client, table: str, prefix: str) -> int:
    """Records in the stream whose key starts with `prefix`."""
    arn = dynamodb_client.describe_table(TableName=table)["Table"]["LatestStreamArn"]
    n = 0
    for shard in streams_client.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"]:
        it = streams_client.get_shard_iterator(
            StreamArn=arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        empty = 0
        while it and empty < 3:
            resp = streams_client.get_records(ShardIterator=it, Limit=1000)
            records = resp.get("Records", [])
            n += sum(1 for r in records if r["dynamodb"]["Keys"]["pk"]["S"].startswith(prefix))
            empty = 0 if records else empty + 1
            it = resp.get("NextShardIterator")
    return n


def _wait_for_record_count(dynamodb_client, streams_client, table, prefix, want) -> int:
    deadline = time.monotonic() + 30
    while True:
        got = _stream_record_count(dynamodb_client, streams_client, table, prefix)
        if got >= want or time.monotonic() > deadline:
            return got
        time.sleep(0.5)


def test_racing_puts_and_deletes_on_a_stream_table_all_succeed(
    dynamodb_client, raw_client, streams_client, stream_table
):
    """PutItem and DeleteItem churn over a few keys: every call answers 200
    and the stream holds one record per write that changed the item."""
    prefix = f"churn-{uuid.uuid4().hex[:8]}-"

    def body(i: int) -> tuple[list[str], int]:
        failures: list[str] = []
        records = 0
        for n in range(OPS_PER_WRITER):
            key = {"pk": {"S": f"{prefix}{(i + n) % KEYS}"}}
            try:
                if n % 2 == 0:
                    raw_client.put_item(TableName=stream_table, Item={**key, "w": {"S": f"w{i}-{n}"}})
                    records += 1
                else:
                    r = raw_client.delete_item(TableName=stream_table, Key=key, ReturnValues="ALL_OLD")
                    # A delete of an item that does not exist writes no record.
                    if r.get("Attributes"):
                        records += 1
            except ClientError as e:
                failures.append(_error_code(e))
        return failures, records

    out = _run_writers(WRITERS, body)
    failures = [f for fails, _ in out for f in fails]
    assert not failures, f"{len(failures)} of {WRITERS * OPS_PER_WRITER} writes failed: {sorted(set(failures))}"
    want = sum(records for _, records in out)
    got = _wait_for_record_count(dynamodb_client, streams_client, stream_table, prefix, want)
    assert got == want, f"stream records {got}, writes that changed an item {want}"


def test_batch_writes_racing_puts_on_a_stream_table_never_fail(
    dynamodb_client, raw_client, streams_client, stream_table
):
    """BatchWriteItem (the raced key plus a side key) racing PutItem on the
    raced key: no call fails, a batch that did not apply every request lists
    the rest in UnprocessedItems, and replaying those lands each side item
    once."""
    prefix = f"batch-{uuid.uuid4().hex[:8]}-"
    raced = {"pk": {"S": f"{prefix}raced"}}

    def body(i: int) -> tuple[list[str], int, int]:
        failures: list[str] = []
        records = 0
        unprocessed_rounds = 0
        for n in range(BATCH_ROUNDS):
            w = {"S": f"w{i}-{n}"}
            try:
                if i % 2 == 0:
                    side = {"pk": {"S": f"{prefix}side-{i}"}}
                    # The side put first, so that a raced put the server
                    # could not apply is reported in UnprocessedItems after a
                    # committed put rather than failing the batch.
                    pending = [
                        {"PutRequest": {"Item": {**side, "w": w}}},
                        {"PutRequest": {"Item": {**raced, "w": w}}},
                    ]
                    while pending:
                        r = raw_client.batch_write_item(RequestItems={stream_table: pending})
                        left = r.get("UnprocessedItems", {}).get(stream_table, [])
                        records += len(pending) - len(left)
                        if left:
                            unprocessed_rounds += 1
                        pending = left
                else:
                    raw_client.put_item(TableName=stream_table, Item={**raced, "w": w})
                    records += 1
            except ClientError as e:
                failures.append(_error_code(e))
        return failures, records, unprocessed_rounds

    out = _run_writers(BATCH_WRITERS, body)
    failures = [f for fails, _, _ in out for f in fails]
    assert not failures, f"{len(failures)} calls failed: {sorted(set(failures))}"
    # Every side item holds its writer's last value: a replayed request was
    # applied once, after the request that was reported as processed.
    for i in range(0, BATCH_WRITERS, 2):
        item = dynamodb_client.get_item(
            TableName=stream_table,
            Key={"pk": {"S": f"{prefix}side-{i}"}},
            ConsistentRead=True,
        )["Item"]
        assert item["w"]["S"] == f"w{i}-{BATCH_ROUNDS - 1}", item
    want = sum(records for _, records, _ in out)
    got = _wait_for_record_count(dynamodb_client, streams_client, stream_table, prefix, want)
    assert got == want, f"stream records {got}, writes applied {want}"
