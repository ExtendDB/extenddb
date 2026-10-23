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
BATCH_WRITERS = 16
BATCH_ROUNDS = 30
UPDATE_WRITERS = 16
UPDATE_OPS = 20


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every failure is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=UPDATE_WRITERS * 2,
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


@pytest.fixture(scope="module")
def stream_gsi_table(dynamodb_client):
    """A table with both a stream and a GSI: the shape the UpdateItem
    contention measurement was taken on. A write on this table takes the item,
    its index entry, the stream sequence counter and the stream record in one
    transaction, so concurrent writers on one key have four documents to
    conflict on."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "g-index",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
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
    raced key: no batch call fails, a batch that did not apply every request
    lists the rest in UnprocessedItems, and replaying those lands each side
    item once.

    Each batch round sends the raced put first or the side put first, so both
    orders are covered. A request the server could not apply now belongs in
    UnprocessedItems whether or not an earlier request in the same batch
    committed; a batch that fails as a whole because the contended request
    happened to come first is the failure this covers. The PutItem arm may
    answer 503 ServiceUnavailable once a put has spent its retry budget,
    which the SDKs retry.
    """
    prefix = f"batch-{uuid.uuid4().hex[:8]}-"
    raced = {"pk": {"S": f"{prefix}raced"}}

    def body(i: int) -> tuple[list[str], int, int]:
        failures: list[str] = []
        records = 0
        unprocessed_rounds = 0
        for n in range(BATCH_ROUNDS):
            w = {"S": f"w{i}-{n}"}
            raced_first = n % 2 == 0
            try:
                if i % 2 == 0:
                    side = {"pk": {"S": f"{prefix}side-{i}"}}
                    raced_req = {"PutRequest": {"Item": {**raced, "w": w}}}
                    side_req = {"PutRequest": {"Item": {**side, "w": w}}}
                    # Even rounds put the raced key first, so the request the
                    # server may not be able to apply is the first in the
                    # batch and nothing has committed when it fails. Odd
                    # rounds put the side key first. Both answer 200 with the
                    # rest of the batch in UnprocessedItems.
                    pending = (
                        [raced_req, side_req] if raced_first else [side_req, raced_req]
                    )
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
                arm = "BatchWriteItem" if i % 2 == 0 else "PutItem"
                order = "raced first" if raced_first else "side first"
                failures.append(f"{arm} {_error_code(e)} ({order})")
        return failures, records, unprocessed_rounds

    out = _run_writers(BATCH_WRITERS, body)
    failures = [f for fails, _, _ in out for f in fails]
    batch_failures = [f for f in failures if f.startswith("BatchWriteItem")]
    assert not batch_failures, (
        f"{len(batch_failures)} batch calls failed: {sorted(set(batch_failures))}; a request "
        "the server could not apply belongs in UnprocessedItems in either order"
    )
    # A single-item PutItem that spends its whole retry budget answers 503
    # ServiceUnavailable, which the SDKs retry. Any other failure, and any
    # failure at all from the batch arm, is a defect.
    put_failures = [
        f for f in failures if not f.startswith("BatchWriteItem") and "503 " not in f
    ]
    assert not put_failures, f"PutItem answers: {sorted(set(put_failures))}"
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


def test_update_item_contention_is_never_a_transaction_conflict(
    dynamodb_client, raw_client, stream_gsi_table
):
    """16 writers on one key: UpdateItem never answers 400
    TransactionConflictException.

    Amazon DynamoDB applies this shape and answers 200 to every call (measured
    on 2026-10-23 against the service, client retries off, on a table with a
    GSI and a stream: 16 writers x 20 `ADD c :1` and 16 writers x 20
    `SET l = list_append(l, :m)` on one key). It answers
    TransactionConflictException to a single-item write only while a
    TransactWriteItems holds the item, and no caller here sends one. A write
    this backend could not apply inside its retry budget answers 503
    ServiceUnavailable, which the SDKs retry, the same answer PutItem and
    DeleteItem give to the same contention.

    The writes that were accepted are checked too: the counter equals the
    number of calls that answered 200, and every marker a caller was told
    committed is stored once, so a retried update neither drops a write nor
    applies one twice.
    """
    run = uuid.uuid4().hex[:8]
    add_key = f"upd-add-{run}"
    list_key = f"upd-list-{run}"
    raw_client.put_item(
        TableName=stream_gsi_table,
        Item={"pk": {"S": list_key}, "g": {"S": f"{list_key}-g"}, "l": {"L": []}},
    )

    def add_body(i: int) -> tuple[list[str], list[int]]:
        outcomes: list[str] = []
        counters: list[int] = []
        for n in range(UPDATE_OPS):
            try:
                r = raw_client.update_item(
                    TableName=stream_gsi_table,
                    Key={"pk": {"S": add_key}},
                    UpdateExpression="ADD #c :one SET #g = :g",
                    ExpressionAttributeNames={"#c": "c", "#g": "g"},
                    ExpressionAttributeValues={
                        ":one": {"N": "1"},
                        ":g": {"S": f"{add_key}-{i}-{n}"},
                    },
                    ReturnValues="ALL_NEW",
                )
                outcomes.append("200")
                counters.append(int(r["Attributes"]["c"]["N"]))
            except ClientError as e:
                outcomes.append(_error_code(e))
        return outcomes, counters

    def list_body(i: int) -> tuple[list[str], list[str]]:
        outcomes: list[str] = []
        committed: list[str] = []
        for n in range(UPDATE_OPS):
            marker = f"m-{i}-{n}"
            try:
                raw_client.update_item(
                    TableName=stream_gsi_table,
                    Key={"pk": {"S": list_key}},
                    UpdateExpression="SET #l = list_append(#l, :m)",
                    ExpressionAttributeNames={"#l": "l"},
                    ExpressionAttributeValues={":m": {"L": [{"S": marker}]}},
                )
                outcomes.append("200")
                committed.append(marker)
            except ClientError as e:
                outcomes.append(_error_code(e))
        return outcomes, committed

    add_out = _run_writers(UPDATE_WRITERS, add_body)
    list_out = _run_writers(UPDATE_WRITERS, list_body)

    outcomes = [o for part, _ in add_out + list_out for o in part]
    conflicts = [o for o in outcomes if "TransactionConflict" in o]
    assert not conflicts, (
        f"{len(conflicts)} of {len(outcomes)} UpdateItem calls answered "
        f"{sorted(set(conflicts))}; the service answers 200 to this shape and "
        "a write this backend could not apply answers 503"
    )
    unexpected = sorted({o for o in outcomes if o not in ("200", "503 ServiceUnavailable")})
    assert not unexpected, f"unexpected answers: {unexpected}"

    applied = sum(1 for part, _ in add_out for o in part if o == "200")
    stored = int(
        dynamodb_client.get_item(
            TableName=stream_gsi_table, Key={"pk": {"S": add_key}}, ConsistentRead=True
        )["Item"]["c"]["N"]
    )
    assert stored == applied, f"counter {stored}, calls answered 200 {applied}"
    returned = [v for _, counters in add_out for v in counters]
    assert sorted(returned) == list(range(1, applied + 1)), (
        "each ALL_NEW counter value should be returned to exactly one caller, "
        f"got {len(set(returned))} distinct of {len(returned)}"
    )

    committed = {m for _, part in list_out for m in part}
    stored_list = [
        x["S"]
        for x in dynamodb_client.get_item(
            TableName=stream_gsi_table, Key={"pk": {"S": list_key}}, ConsistentRead=True
        )["Item"]["l"]["L"]
    ]
    assert len(stored_list) == len(set(stored_list)), "a marker was appended twice"
    assert committed == set(stored_list), (
        f"{len(committed - set(stored_list))} markers a caller was told committed are "
        f"not stored, {len(set(stored_list) - committed)} stored markers were never "
        "reported as committed"
    )
