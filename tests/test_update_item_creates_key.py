# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""An UpdateItem that creates an item stores the item with its key.

Amazon DynamoDB creates the item from the key and the update expression:
GetItem returns the key attributes with the updated ones, ALL_NEW on creation
returns the whole new item including the key, UPDATED_NEW returns only the
updated attributes, ALL_OLD and UPDATED_OLD on creation return no Attributes,
and a later PutItem, UpdateItem, Query, Scan and DeleteItem on the key treat
the item as any other. On the MongoDB backend an unconditional UpdateItem on a
table without an index used to store the created item without its key
attributes, after which GetItem returned the item without the key and every
PutItem on the key answered HTTP 500 (issue #436). The race tests run the
issue's shape: writers creating the same keys with UpdateItem and PutItem at
once.

Expected responses were measured against the service on 2026-10-23 (probe
W4-1 in the fix report).
"""

from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

WRITERS = 8
KEYS = 30
RETURN_VALUES = ["ALL_NEW", "UPDATED_NEW", "ALL_OLD", "UPDATED_OLD", "NONE"]


# TEMPORARY: on MongoDB, a put without a condition on a table with no index and
# no stream returns HTTP 500 when it loses the create race to another writer's
# insert. The MongoDB write-race fix repairs it and lands separately.
# TODO: remove this marker once the MongoDB write-race fix is on main.
# Only an assertion counts as the expected failure. The marker is not strict,
# because the race does not fire in every run. The MongoDB test runner sets
# EXTENDDB_TEST_MONGODB_CONTAINER.
XFAIL_UNTIL_MONGODB_FIX = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()),
    reason="MongoDB returns HTTP 500 for a lost create race, fixed by the MongoDB write-race fix",
    raises=AssertionError,
    strict=False,
)


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
def raw_client(endpoint_url):
    """A client that never retries, so every failure is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=WRITERS * 4,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


def _key(table_kind: str, pk: str) -> dict:
    key = {"pk": {"S": pk}}
    if table_kind == "range":
        key["sk"] = {"S": "s"}
    return key


def _get(client, table: str, key: dict) -> dict | None:
    return client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")


def _warm(client, table: str, table_kind: str) -> None:
    """Write another key first.

    The MongoDB backend takes its native UpdateItem path only once a write has
    recorded that the table has no index; the first write on a fresh table
    takes the transaction path, which did not have the defect. The issue
    reproduced it the same way.
    """
    client.put_item(TableName=table, Item={**_key(table_kind, f"warm-{uuid.uuid4().hex}")})


def _create(client, table: str, key: dict, **extra) -> dict:
    """UpdateItem SET w = upd on a key that does not exist yet."""
    resp = client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="SET w = :w",
        ExpressionAttributeValues={":w": {"S": "upd"}},
        **extra,
    )
    resp.pop("ResponseMetadata", None)
    return resp


def _put_then_get(client, table: str, key: dict) -> None:
    """PutItem on a key created by UpdateItem succeeds and replaces the item."""
    put = {**key, "w": {"S": "put"}}
    client.put_item(TableName=table, Item=put)
    assert _get(client, table, key) == put


@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_set_creates_the_item_with_its_key(request, dynamodb_client, table_kind):
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    key = _key(table_kind, f"set-{uuid.uuid4().hex}")
    assert _create(dynamodb_client, table, key) == {}
    assert _get(dynamodb_client, table, key) == {**key, "w": {"S": "upd"}}
    _put_then_get(dynamodb_client, table, key)


@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_add_number_creates_the_item_with_its_key(request, dynamodb_client, table_kind):
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    key = _key(table_kind, f"add-{uuid.uuid4().hex}")
    dynamodb_client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="ADD c :one",
        ExpressionAttributeValues={":one": {"N": "1"}},
    )
    assert _get(dynamodb_client, table, key) == {**key, "c": {"N": "1"}}
    _put_then_get(dynamodb_client, table, key)


def test_add_set_creates_the_item_with_its_key(dynamodb_client, hash_table):
    _warm(dynamodb_client, hash_table, "hash")
    key = _key("hash", f"add-set-{uuid.uuid4().hex}")
    dynamodb_client.update_item(
        TableName=hash_table,
        Key=key,
        UpdateExpression="ADD tags :t",
        ExpressionAttributeValues={":t": {"SS": ["a", "b"]}},
    )
    item = _get(dynamodb_client, hash_table, key)
    assert item is not None
    assert item["pk"] == key["pk"]
    assert sorted(item["tags"]["SS"]) == ["a", "b"]
    _put_then_get(dynamodb_client, hash_table, key)


@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_remove_only_on_a_missing_item_succeeds_and_the_key_stays_writable(
    request, dynamodb_client, table_kind
):
    """The service answers 200 and creates no item from a REMOVE-only update.

    ExtendDB creates an item holding the key (measured on SQLite and on
    MongoDB); that difference is older than this test and is not asserted
    either way here.
    On MongoDB the call itself answered 500 before the fix, because the
    created document had no item_data at all.
    """
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    key = _key(table_kind, f"remove-{uuid.uuid4().hex}")
    resp = dynamodb_client.update_item(TableName=table, Key=key, UpdateExpression="REMOVE w")
    assert "Attributes" not in resp
    assert _get(dynamodb_client, table, key) in (None, key)
    _put_then_get(dynamodb_client, table, key)


def test_attribute_not_exists_condition_creates_the_item_once(dynamodb_client, hash_table):
    _warm(dynamodb_client, hash_table, "hash")
    key = _key("hash", f"cond-{uuid.uuid4().hex}")
    resp = _create(dynamodb_client, hash_table, key, ConditionExpression="attribute_not_exists(pk)")
    assert resp == {}
    assert _get(dynamodb_client, hash_table, key) == {**key, "w": {"S": "upd"}}
    with pytest.raises(ClientError) as exc:
        _create(dynamodb_client, hash_table, key, ConditionExpression="attribute_not_exists(pk)")
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert exc.value.response["Error"]["Message"] == "The conditional request failed"
    _put_then_get(dynamodb_client, hash_table, key)


@pytest.mark.parametrize("return_values", RETURN_VALUES)
@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_return_values_on_creation(request, dynamodb_client, table_kind, return_values):
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    key = _key(table_kind, f"rv-{return_values.lower()}-{uuid.uuid4().hex}")
    resp = _create(dynamodb_client, table, key, ReturnValues=return_values)
    created = {**key, "w": {"S": "upd"}}
    if return_values == "ALL_NEW":
        assert resp == {"Attributes": created}
    elif return_values == "UPDATED_NEW":
        assert resp == {"Attributes": {"w": {"S": "upd"}}}
    else:
        assert resp == {}
    assert _get(dynamodb_client, table, key) == created
    _put_then_get(dynamodb_client, table, key)


@pytest.mark.parametrize("table_kind", ["hash", "range"])
def test_later_operations_on_a_key_created_by_update_item(request, dynamodb_client, table_kind):
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    key = _key(table_kind, f"later-{uuid.uuid4().hex}")
    created = {**key, "w": {"S": "upd"}}
    _create(dynamodb_client, table, key)
    assert _get(dynamodb_client, table, key) == created

    put = {**key, "w": {"S": "put"}}
    resp = dynamodb_client.put_item(TableName=table, Item=put, ReturnValues="ALL_OLD")
    assert resp["Attributes"] == created
    assert _get(dynamodb_client, table, key) == put

    resp = dynamodb_client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="SET x = :x",
        ExpressionAttributeValues={":x": {"N": "1"}},
        ReturnValues="ALL_NEW",
    )
    final = {**put, "x": {"N": "1"}}
    assert resp["Attributes"] == final

    resp = dynamodb_client.query(
        TableName=table,
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": key["pk"]},
        ConsistentRead=True,
    )
    assert resp["Count"] == 1
    assert resp["Items"] == [final]

    scanned = dynamodb_client.scan(TableName=table, ConsistentRead=True)["Items"]
    assert final in scanned
    assert all("pk" in item for item in scanned)

    resp = dynamodb_client.delete_item(TableName=table, Key=key, ReturnValues="ALL_OLD")
    assert resp["Attributes"] == final
    assert _get(dynamodb_client, table, key) is None

    resp = dynamodb_client.put_item(TableName=table, Item=put, ReturnValues="ALL_OLD")
    assert "Attributes" not in resp
    assert _get(dynamodb_client, table, key) == put


@dataclass
class RaceResult:
    table: str
    table_kind: str
    keys: list[dict]
    failures: list[tuple[str, str, str]] = field(default_factory=list)

    def failed(self, op: str) -> list[tuple[str, str, str]]:
        return [f for f in self.failures if f[0] == op]


@pytest.fixture(scope="module", params=["hash", "range"])
def race(request, dynamodb_client, raw_client) -> RaceResult:
    """WRITERS UpdateItem and WRITERS PutItem per key, KEYS keys, all at once.

    The issue measured 128 of 240 PutItem failing with HTTP 500 and 16 of 30
    final items without `pk` on the MongoDB backend.
    """
    table_kind = request.param
    table = request.getfixturevalue(f"{table_kind}_table")
    _warm(dynamodb_client, table, table_kind)
    result = RaceResult(
        table, table_kind, [_key(table_kind, f"race-{uuid.uuid4().hex}") for _ in range(KEYS)]
    )
    start = threading.Barrier(2 * WRITERS, timeout=60)
    errors: list[BaseException] = []
    lock = threading.Lock()

    def run(op: str, i: int):
        try:
            start.wait()
            for key in result.keys:
                try:
                    if op == "put":
                        raw_client.put_item(TableName=table, Item={**key, "w": {"S": f"put{i}"}})
                    else:
                        raw_client.update_item(
                            TableName=table,
                            Key=key,
                            UpdateExpression="SET w = :w",
                            ExpressionAttributeValues={":w": {"S": f"upd{i}"}},
                        )
                except ClientError as e:
                    with lock:
                        result.failures.append((op, key["pk"]["S"], e.response["Error"]["Code"]))
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    threads = [
        threading.Thread(target=run, args=(op, i))
        for op in ("put", "update")
        for i in range(WRITERS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return result


def test_racing_update_item_calls_all_succeed(race):
    failed = race.failed("update")
    assert not failed, f"{len(failed)} of {WRITERS * KEYS} UpdateItem failed: {failed[:5]}"


def test_racing_writers_leave_every_item_with_its_key(dynamodb_client, race):
    without_key = []
    for key in race.keys:
        item = _get(dynamodb_client, race.table, key)
        if item is None or any(item.get(name) != value for name, value in key.items()):
            without_key.append(key["pk"]["S"])
    assert not without_key, f"{len(without_key)} of {KEYS} items lack a key attribute"


def test_put_item_after_the_race_replaces_every_item(dynamodb_client, race):
    for key in race.keys:
        _put_then_get(dynamodb_client, race.table, key)


@XFAIL_UNTIL_MONGODB_FIX
def test_racing_put_item_calls_all_succeed(race):
    failed = race.failed("put")
    assert not failed, f"{len(failed)} of {WRITERS * KEYS} PutItem failed: {failed[:5]}"
