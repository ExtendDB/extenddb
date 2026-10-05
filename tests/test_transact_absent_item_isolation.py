# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Concurrent TransactWriteItems that read items that do not exist.

A ConditionCheck or a Delete on a missing item reads that the item is absent.
Amazon DynamoDB keeps that read valid until the transaction commits: a
concurrent transaction that creates the item conflicts with it. So when each
of two transactions reads an item the other one creates, they never both
commit as though neither saw the other (write skew).

Each test runs two such transactions at once, many times, on fresh keys. It
checks every outcome against the two serial orders. A failure must be a
TransactionCanceledException with ConditionalCheckFailed, TransactionConflict,
or None reasons. Amazon DynamoDB answers a few contended transactions with
InternalServerError, so a small fixed number of 5xx is tolerated.
"""

from __future__ import annotations

import os
import threading
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

ROUNDS = 40
# Same bound as the contended-write tests: Amazon DynamoDB returns a few
# InternalServerError per run of contended transactions.
MAX_SERVER_ERRORS = 5
NOT_EXISTS = "attribute_not_exists(pk)"
REASON_CODES = {"None", "ConditionalCheckFailed", "TransactionConflict"}


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every 5xx and every cancellation is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=4,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def range_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


# The sort key of every item in the range table: a number with a fraction.
SK = {"N": "1.50"}


def _key(pk: str, sk: dict | None = None) -> dict:
    key = {"pk": {"S": pk}}
    if sk is not None:
        key["sk"] = sk
    return key


def _put(table: str, pk: str, sk: dict | None = None) -> dict:
    return {"Put": {"TableName": table, "Item": _key(pk, sk)}}


def _check_absent(table: str, pk: str, sk: dict | None = None) -> dict:
    return {
        "ConditionCheck": {
            "TableName": table,
            "Key": _key(pk, sk),
            "ConditionExpression": NOT_EXISTS,
        }
    }


def _delete(table: str, pk: str, condition: str | None = None) -> dict:
    op: dict = {"TableName": table, "Key": _key(pk)}
    if condition:
        op["ConditionExpression"] = condition
    return {"Delete": op}


def _update_absent(table: str, pk: str) -> dict:
    return {
        "Update": {
            "TableName": table,
            "Key": _key(pk),
            "UpdateExpression": "SET o = :o",
            "ConditionExpression": NOT_EXISTS,
            "ExpressionAttributeValues": {":o": {"S": pk}},
        }
    }


def _attempt(client, items: list[dict]) -> dict | None:
    """Run one transaction. Returns None when it commits, else the failure."""
    try:
        client.transact_write_items(TransactItems=items)
        return None
    except ClientError as e:
        r = e.response
        return {
            "status": r["ResponseMetadata"]["HTTPStatusCode"],
            "code": r["Error"]["Code"],
            "reasons": r.get("CancellationReasons"),
        }


def _race(client, t1: list[dict], t2: list[dict]) -> list[dict | None]:
    """Start both transactions together and return both outcomes."""
    start = threading.Barrier(2, timeout=30)
    out: list[dict | None] = [None, None]
    errors: list[BaseException] = []

    def run(i: int, items: list[dict]):
        try:
            start.wait()
            out[i] = _attempt(client, items)
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i, t)) for i, t in enumerate((t1, t2))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return out


def _exists(client, table: str, pk: str, sk: dict | None = None) -> bool:
    return "Item" in client.get_item(TableName=table, Key=_key(pk, sk), ConsistentRead=True)


def _run_rounds(dynamodb_client, raw_client, table, build, sk=None) -> list[tuple]:
    """Race `build(x, y)` on fresh keys each round.

    Returns one record per round: both outcomes and whether x and y exist after.
    Every failure must be a cancellation, apart from a few 5xx.
    """
    rounds = []
    server_errors = []
    for _ in range(ROUNDS):
        x, y = f"x-{uuid.uuid4().hex}", f"y-{uuid.uuid4().hex}"
        t1, t2 = build(x, y)
        r1, r2 = _race(raw_client, t1, t2)
        for f in (r1, r2):
            if f is None:
                continue
            if f["status"] >= 500:
                assert (f["status"], f["code"]) == (500, "InternalServerError"), f
                server_errors.append(f)
                continue
            assert (f["status"], f["code"]) == (400, "TransactionCanceledException"), f
            codes = [r["Code"] for r in f["reasons"]]
            assert len(codes) == 2 and set(codes) <= REASON_CODES, f
        rounds.append(
            (r1, r2, _exists(dynamodb_client, table, x, sk), _exists(dynamodb_client, table, y, sk))
        )
    assert len(server_errors) <= MAX_SERVER_ERRORS, server_errors
    return rounds


def _both_committed(rounds) -> list[tuple]:
    return [r for r in rounds if r[0] is None and r[1] is None]


def test_condition_checks_on_missing_items_do_not_write_skew(
    dynamodb_client, raw_client, table
):
    """[check y absent, put x] vs [check x absent, put y]: one at most commits."""
    rounds = _run_rounds(
        dynamodb_client,
        raw_client,
        table,
        lambda x, y: (
            [_check_absent(table, y), _put(table, x)],
            [_check_absent(table, x), _put(table, y)],
        ),
    )
    skewed = _both_committed(rounds)
    assert not skewed, f"{len(skewed)} of {ROUNDS} rounds committed both: {skewed[0]}"


def test_condition_checks_on_missing_items_of_a_range_table_do_not_write_skew(
    dynamodb_client, raw_client, range_table
):
    """The same race on a hash and range table, keyed by a number sort key."""
    rounds = _run_rounds(
        dynamodb_client,
        raw_client,
        range_table,
        lambda x, y: (
            [_check_absent(range_table, y, SK), _put(range_table, x, SK)],
            [_check_absent(range_table, x, SK), _put(range_table, y, SK)],
        ),
        sk=SK,
    )
    skewed = _both_committed(rounds)
    assert not skewed, f"{len(skewed)} of {ROUNDS} rounds committed both: {skewed[0]}"


def test_conditional_deletes_of_missing_items_do_not_write_skew(
    dynamodb_client, raw_client, table
):
    """[delete y if absent, put x] vs [delete x if absent, put y]: one at most commits."""
    rounds = _run_rounds(
        dynamodb_client,
        raw_client,
        table,
        lambda x, y: (
            [_delete(table, y, NOT_EXISTS), _put(table, x)],
            [_delete(table, x, NOT_EXISTS), _put(table, y)],
        ),
    )
    skewed = _both_committed(rounds)
    assert not skewed, f"{len(skewed)} of {ROUNDS} rounds committed both: {skewed[0]}"


def test_deletes_of_missing_items_serialize(dynamodb_client, raw_client, table):
    """[delete y, put x] vs [delete x, put y]: when both commit, the later one
    deletes the earlier one's item, so exactly one of x and y remains."""
    rounds = _run_rounds(
        dynamodb_client,
        raw_client,
        table,
        lambda x, y: (
            [_delete(table, y), _put(table, x)],
            [_delete(table, x), _put(table, y)],
        ),
    )
    skewed = [r for r in _both_committed(rounds) if r[2] and r[3]]
    assert not skewed, f"{len(skewed)} of {ROUNDS} rounds kept both items: {skewed[0]}"


def test_conditional_updates_of_missing_items_serialize(
    dynamodb_client, raw_client, table
):
    """[update x if absent, put y] vs [update y if absent, put x]: an update
    creates its item, so one at most commits."""
    rounds = _run_rounds(
        dynamodb_client,
        raw_client,
        table,
        lambda x, y: (
            [_update_absent(table, x), _put(table, y)],
            [_update_absent(table, y), _put(table, x)],
        ),
    )
    skewed = _both_committed(rounds)
    assert not skewed, f"{len(skewed)} of {ROUNDS} rounds committed both: {skewed[0]}"
