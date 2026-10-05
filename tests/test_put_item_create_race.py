# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Concurrent PutItem calls that create the same new item.

Amazon DynamoDB applies the puts one after another. Each one succeeds unless
its own condition fails against the item before it: a put that finds the item
already created overwrites it, and with ReturnValues ALL_OLD it returns the
item it replaced. So the old images of one round form a single chain, from no
item to the final one. The tests cover the request shapes that make a put
read the current item first: a condition, ALL_OLD, a GSI, and a stream.
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
ROUNDS = 15


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every failure is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=WRITERS * 2,
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


def _s(name: str) -> dict:
    return {"AttributeName": name, "AttributeType": "S"}


def _k(name: str, kind: str) -> dict:
    return {"AttributeName": name, "KeyType": kind}


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def range_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[_s("pk"), _s("sk")],
        key_schema=[_k("pk", "HASH"), _k("sk", "RANGE")],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def gsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[_s("pk"), _s("gk")],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gk-index",
                "KeySchema": [_k("gk", "HASH")],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def stream_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    ) as name:
        yield name


def _key(table_kind: str, pk: str) -> dict:
    key = {"pk": {"S": pk}}
    if table_kind == "range":
        key["sk"] = {"S": "s"}
    return key


def _race(client, table: str, table_kind: str, extra: dict) -> tuple[str, list]:
    """WRITERS clients put the same new key at once, each with its own `w`.

    Returns the key and, per writer, the old `w` it replaced (None when it
    created the item), or the ClientError response when the put failed.
    """
    pk = f"k-{uuid.uuid4().hex}"
    start = threading.Barrier(WRITERS, timeout=30)
    out: list = [None] * WRITERS
    errors: list[BaseException] = []

    def run(i: int):
        item = {**_key(table_kind, pk), "w": {"S": f"w{i}"}, "gk": {"S": f"g{i}"}}
        try:
            start.wait()
            r = client.put_item(TableName=table, Item=item, **extra)
            out[i] = ("ok", r.get("Attributes", {}).get("w", {}).get("S"))
        except ClientError as e:
            out[i] = ("error", e.response["Error"]["Code"])
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(WRITERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return pk, out


def _final_w(client, table: str, table_kind: str, pk: str) -> str:
    item = client.get_item(TableName=table, Key=_key(table_kind, pk), ConsistentRead=True)
    return item["Item"]["w"]["S"]


def _assert_all_succeed(out: list):
    failed = [o for o in out if o[0] != "ok"]
    assert not failed, f"{len(failed)} of {WRITERS} puts failed: {failed}"


def _assert_one_chain(out: list, final: str):
    """The old images lead from the final item back to no item, once each."""
    prev = {f"w{i}": old for i, (_, old) in enumerate(out)}
    seen, cur = 0, final
    while cur is not None and seen <= WRITERS:
        seen += 1
        cur = prev[cur]
    assert seen == WRITERS, f"old images do not form one chain: {prev}, final {final}"


@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_racing_puts_with_all_old_all_succeed(
    request, dynamodb_client, raw_client, table_kind
):
    table = request.getfixturevalue(f"{table_kind}_table")
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, table, table_kind, {"ReturnValues": "ALL_OLD"})
        _assert_all_succeed(out)
        _assert_one_chain(out, _final_w(dynamodb_client, table, table_kind, pk))


def test_racing_puts_check_their_condition_against_the_winner(
    dynamodb_client, raw_client, range_table
):
    """A condition that every item satisfies never fails, whoever created the item."""
    extra = {"ConditionExpression": "attribute_not_exists(zz)", "ReturnValues": "ALL_OLD"}
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, range_table, "range", extra)
        _assert_all_succeed(out)
        _assert_one_chain(out, _final_w(dynamodb_client, range_table, "range", pk))


def test_racing_conditional_creates_have_one_winner(raw_client, hash_table):
    """Control: with attribute_not_exists(pk), exactly one put creates the item."""
    for _ in range(ROUNDS):
        _, out = _race(
            raw_client, hash_table, "hash", {"ConditionExpression": "attribute_not_exists(pk)"}
        )
        codes = sorted(o[1] if o[0] == "error" else "ok" for o in out)
        assert codes == ["ConditionalCheckFailedException"] * (WRITERS - 1) + ["ok"], codes


def test_racing_puts_on_a_gsi_table_all_succeed(dynamodb_client, raw_client, gsi_table):
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, gsi_table, "hash", {})
        _assert_all_succeed(out)
        final = dynamodb_client.get_item(
            TableName=gsi_table, Key=_key("hash", pk), ConsistentRead=True
        )["Item"]
        assert final["gk"]["S"] == "g" + final["w"]["S"][1:], final


def _stream_events(streams_client, stream_arn: str, pk: str, want: int) -> list[dict]:
    """Poll the stream until it holds `want` records for `pk` (or 30 s pass)."""
    deadline = time.monotonic() + 30
    records: list[dict] = []
    while time.monotonic() < deadline:
        records = []
        shards = streams_client.describe_stream(StreamArn=stream_arn)["StreamDescription"]["Shards"]
        for shard in shards:
            it = streams_client.get_shard_iterator(
                StreamArn=stream_arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
            )["ShardIterator"]
            for _ in range(20):
                resp = streams_client.get_records(ShardIterator=it, Limit=1000)
                records += [
                    r for r in resp.get("Records", []) if r["dynamodb"]["Keys"]["pk"]["S"] == pk
                ]
                it = resp.get("NextShardIterator")
                if not it or not resp.get("Records"):
                    break
        if len(records) >= want:
            break
        time.sleep(1)
    return records


def test_racing_puts_on_a_stream_table_all_succeed(
    dynamodb_client, raw_client, streams_client, stream_table
):
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, stream_table, "hash", {})
        _assert_all_succeed(out)
    # The last round's stream: one INSERT, then a MODIFY per later put, each
    # with the item it replaced as its old image.
    arn = dynamodb_client.describe_table(TableName=stream_table)["Table"]["LatestStreamArn"]
    records = _stream_events(streams_client, arn, pk, WRITERS)
    names = sorted(r["eventName"] for r in records)
    assert names == ["INSERT"] + ["MODIFY"] * (WRITERS - 1), names
    olds = [r["dynamodb"]["OldImage"]["w"]["S"] for r in records if r["eventName"] == "MODIFY"]
    news = [r["dynamodb"]["NewImage"]["w"]["S"] for r in records]
    assert len(set(olds)) == WRITERS - 1 and set(olds) <= set(news), (olds, news)
