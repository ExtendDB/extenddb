# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Concurrent PutItem calls that create the same new item.

Amazon DynamoDB applies the puts one after another. Each one succeeds unless
its own condition fails against the item before it: a put that finds the item
already created overwrites it, and with ReturnValues ALL_OLD it returns the
item it replaced. So the old images of one round form a single chain, from no
item to the final one. The tests cover the request shapes that make a put
read the current item first: a condition, ALL_OLD, ReturnConsumedCapacity, a
GSI, an LSI, and a stream, and BatchWriteItem puts to a stream table. On index
and stream tables they also check that only the final item stays in the index,
and that each stream record's old image is the record before it.
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


# TEMPORARY: on MongoDB, a put without a condition on a table with no index and
# no stream returns HTTP 500 when it loses the create race. The MongoDB
# write-race fix repairs it. That fix lands separately; remove this marker
# once it is on main. The MongoDB test runner sets EXTENDDB_TEST_MONGODB_CONTAINER.
XFAIL_UNTIL_MONGODB_FIX = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()),
    reason="MongoDB returns HTTP 500 for a lost create race, fixed by the MongoDB write-race fix",
    strict=False,
)


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
def lsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[_s("pk"), _s("sk"), _s("lk")],
        key_schema=[_k("pk", "HASH"), _k("sk", "RANGE")],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lk-index",
                "KeySchema": [_k("pk", "HASH"), _k("lk", "RANGE")],
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


def _race(
    client, table: str, table_kind: str, extra: dict, batch: bool = False
) -> tuple[str, list]:
    """WRITERS clients put the same new key at once, each with its own `w`.

    Each writer's item also carries index keys unique to it: `gk` across the
    whole table, and `lk` within the key. With `batch`, each writer sends a
    BatchWriteItem with one PutRequest instead of a PutItem.

    Returns the key and, per writer, the old `w` it replaced (None when it
    created the item or used BatchWriteItem), or the error when the put failed.
    """
    pk = f"k-{uuid.uuid4().hex}"
    start = threading.Barrier(WRITERS, timeout=30)
    out: list = [None] * WRITERS
    errors: list[BaseException] = []

    def run(i: int):
        item = {
            **_key(table_kind, pk),
            "w": {"S": f"w{i}"},
            "gk": {"S": f"g-{pk}-{i}"},
            "lk": {"S": f"l{i}"},
        }
        try:
            start.wait()
            if batch:
                r = client.batch_write_item(RequestItems={table: [{"PutRequest": {"Item": item}}]})
                left = r.get("UnprocessedItems")
                out[i] = ("unprocessed", left) if left else ("ok", None)
                return
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


def _final(client, table: str, table_kind: str, pk: str) -> dict:
    return client.get_item(TableName=table, Key=_key(table_kind, pk), ConsistentRead=True)[
        "Item"
    ]


def _final_w(client, table: str, table_kind: str, pk: str) -> str:
    return _final(client, table, table_kind, pk)["w"]["S"]


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


@XFAIL_UNTIL_MONGODB_FIX
@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_racing_puts_with_all_old_all_succeed(
    request, dynamodb_client, raw_client, table_kind
):
    table = request.getfixturevalue(f"{table_kind}_table")
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, table, table_kind, {"ReturnValues": "ALL_OLD"})
        _assert_all_succeed(out)
        _assert_one_chain(out, _final_w(dynamodb_client, table, table_kind, pk))


@XFAIL_UNTIL_MONGODB_FIX
def test_racing_puts_that_return_consumed_capacity_all_succeed(
    dynamodb_client, raw_client, hash_table
):
    extra = {"ReturnConsumedCapacity": "TOTAL"}
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, hash_table, "hash", extra)
        _assert_all_succeed(out)
        assert _final_w(dynamodb_client, hash_table, "hash", pk) in {
            f"w{i}" for i in range(WRITERS)
        }


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


def _gsi_keys(client, table: str) -> dict[str, list[str]]:
    """Every `gk` in the GSI, grouped by the base key it points at."""
    by_pk: dict[str, list[str]] = {}
    kwargs = {"TableName": table, "IndexName": "gk-index"}
    while True:
        resp = client.scan(**kwargs)
        for item in resp["Items"]:
            by_pk.setdefault(item["pk"]["S"], []).append(item["gk"]["S"])
        if "LastEvaluatedKey" not in resp:
            return {pk: sorted(gks) for pk, gks in by_pk.items()}
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def test_racing_puts_on_a_gsi_table_leave_only_the_final_entry(
    dynamodb_client, raw_client, gsi_table
):
    """The GSI holds the final item's entry and none of the items it replaced."""
    want: dict[str, list[str]] = {}
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, gsi_table, "hash", {})
        _assert_all_succeed(out)
        want[pk] = [_final(dynamodb_client, gsi_table, "hash", pk)["gk"]["S"]]
    # The GSI is eventually consistent on Amazon DynamoDB, so poll.
    deadline = time.monotonic() + 30
    while (got := _gsi_keys(dynamodb_client, gsi_table)) != want and time.monotonic() < deadline:
        time.sleep(0.5)
    wrong = {pk: (want[pk], got.get(pk)) for pk in want if got.get(pk) != want[pk]}
    assert not wrong and got.keys() == want.keys(), f"stale GSI entries (want, got): {wrong}"


def test_racing_puts_on_an_lsi_table_leave_only_the_final_entry(
    dynamodb_client, raw_client, lsi_table
):
    """A strongly consistent LSI query returns the final item's entry only."""
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, lsi_table, "range", {"ReturnValues": "ALL_OLD"})
        _assert_all_succeed(out)
        final = _final(dynamodb_client, lsi_table, "range", pk)
        _assert_one_chain(out, final["w"]["S"])
        items = dynamodb_client.query(
            TableName=lsi_table,
            IndexName="lk-index",
            ConsistentRead=True,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
        )["Items"]
        assert [i["lk"]["S"] for i in items] == [final["lk"]["S"]], (final, items)


def _stream_records(streams_client, stream_arn: str) -> dict[str, list[dict]]:
    """Every record in the stream, grouped by key, in sequence order."""
    by_pk: dict[str, list[dict]] = {}
    shards = streams_client.describe_stream(StreamArn=stream_arn)["StreamDescription"]["Shards"]
    for shard in shards:
        it = streams_client.get_shard_iterator(
            StreamArn=stream_arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        empty = 0
        for _ in range(50):
            resp = streams_client.get_records(ShardIterator=it, Limit=1000)
            for r in resp.get("Records", []):
                by_pk.setdefault(r["dynamodb"]["Keys"]["pk"]["S"], []).append(r)
            it = resp.get("NextShardIterator")
            empty = 0 if resp.get("Records") else empty + 1
            if not it or empty >= 3:
                break
    for records in by_pk.values():
        records.sort(key=lambda r: int(r["dynamodb"]["SequenceNumber"]))
    return by_pk


def _assert_stream_chains(dynamodb_client, streams_client, table: str, pks: list[str]):
    """Per key: one INSERT, then a MODIFY per later put, each with the item
    before it as its old image."""
    arn = dynamodb_client.describe_table(TableName=table)["Table"]["LatestStreamArn"]
    deadline = time.monotonic() + 30
    while True:
        by_pk = _stream_records(streams_client, arn)
        if all(len(by_pk.get(pk, [])) >= WRITERS for pk in pks) or time.monotonic() > deadline:
            break
        time.sleep(1)
    for pk in pks:
        records = by_pk.get(pk, [])
        names = [r["eventName"] for r in records]
        assert names == ["INSERT"] + ["MODIFY"] * (WRITERS - 1), (pk, names)
        images = [
            (r["dynamodb"].get("OldImage", {}).get("w"), r["dynamodb"]["NewImage"]["w"])
            for r in records
        ]
        for (_, before), (old, _) in zip(images, images[1:]):
            assert old == before, f"old image is not the record before it: {pk} {images}"


def test_racing_puts_on_a_stream_table_all_succeed(
    dynamodb_client, raw_client, streams_client, stream_table
):
    pks = []
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, stream_table, "hash", {})
        _assert_all_succeed(out)
        pks.append(pk)
    _assert_stream_chains(dynamodb_client, streams_client, stream_table, pks)


def test_racing_batch_puts_on_a_stream_table_all_succeed(
    dynamodb_client, raw_client, streams_client, stream_table
):
    """BatchWriteItem puts read the current item for the stream record too."""
    pks = []
    for _ in range(ROUNDS):
        pk, out = _race(raw_client, stream_table, "hash", {}, batch=True)
        _assert_all_succeed(out)
        pks.append(pk)
    _assert_stream_chains(dynamodb_client, streams_client, stream_table, pks)
