# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Conditions on nested map members and list elements, on UpdateItem and DeleteItem.

The item holds ``m = {k: "inner", n: [10, "b"], mm: {k: "deep"}}`` and
``l = [{k: "first"}, "s"]``. Every expected answer in HOLDS, FAILS and the
DeleteItem tests was measured against Amazon DynamoDB on 2026-10-25 at 20:51
and 20:52 UTC (us-east-1), each request twice: a holding condition applies the
write and a failing one answers ConditionalCheckFailedException.

The item also holds ``t``, a map whose members are named ``M``, ``L``, ``S``
and ``0``. The first three are type tags, the names of the fields a stored
value sits under, and the last reads as a list index in a MongoDB path. The
rows in DERIVED_HOLDS were not measured on the service; their expected
answers come from ExtendDB's in-Rust condition evaluator.

On MongoDB, a condition on a table with no index and no stream is compiled to
a MongoDB filter while the server's cache records that the table has no index.
That entry is written by a session-path write and lasts 60 seconds; each test
puts its own item immediately before the conditional write, and that put
renews the entry when it has expired.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import re
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NAMES = {
    "#m": "m",
    "#k": "k",
    "#n": "n",
    "#mm": "mm",
    "#l": "l",
    "#z": "z",
    "#u": "u",
    "#t": "t",
    "#M": "M",
    "#L": "L",
    "#S": "S",
    "#zero": "0",
}
VALUES = {
    ":x": {"S": "z"},
    ":inner": {"S": "inner"},
    ":deep": {"S": "deep"},
    ":first": {"S": "first"},
    ":b": {"S": "b"},
    ":in": {"S": "in"},
    ":ner": {"S": "ner"},
    ":S": {"S": "S"},
    ":M": {"S": "M"},
    ":other": {"S": "other"},
    ":s": {"S": "s"},
    ":tm": {"S": "tm"},
    ":tl": {"S": "tl"},
    ":ts": {"S": "ts"},
    ":t0": {"S": "t0"},
}

ITEM = {
    "v": {"S": "x"},
    "m": {
        "M": {
            "k": {"S": "inner"},
            "n": {"L": [{"N": "10"}, {"S": "b"}]},
            "mm": {"M": {"k": {"S": "deep"}}},
        }
    },
    "l": {"L": [{"M": {"k": {"S": "first"}}}, {"S": "s"}]},
    "t": {
        "M": {
            "M": {"S": "tm"},
            "L": {"S": "tl"},
            "S": {"S": "ts"},
            "0": {"S": "t0"},
        }
    },
}

HOLDS = [
    "attribute_exists(#m.#k)",
    "attribute_not_exists(#m.#z)",
    "#m.#k = :inner",
    "begins_with(#m.#k, :in)",
    "contains(#m.#k, :ner)",
    "attribute_type(#m.#k, :S)",
    "attribute_type(#m.#mm, :M)",
    "#m.#mm.#k = :deep",
    "#l[0].#k = :first",
    "#m.#n[1] = :b",
    "#l[1] = :s",
    "attribute_exists(#m.#k) AND #m.#mm.#k = :deep AND #l[0].#k = :first",
    "NOT attribute_exists(#m.#mm.#z)",
]

FAILS = [
    "attribute_exists(#m.#z)",
    "attribute_not_exists(#m.#k)",
    "#m.#k <> :inner",
    "attribute_exists(#l.#k)",
    "attribute_exists(#m[0])",
    "#m.#k = :other OR #m.#mm.#k = :other OR #l[0].#k = :other",
]

DERIVED_HOLDS = [
    "#t.#M = :tm",
    "#t.#L = :tl",
    "#t.#S = :ts",
    "#t.#zero = :t0",
]


def _maps(*exprs: str) -> dict:
    tokens = set(re.findall(r"[#:][A-Za-z0-9_]+", " ".join(exprs)))
    out = {"ExpressionAttributeNames": {k: v for k, v in NAMES.items() if k in tokens}}
    values = {k: v for k, v in VALUES.items() if k in tokens}
    if values:
        out["ExpressionAttributeValues"] = values
    return out


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


@pytest.fixture
def key(dynamodb_client, table):
    key = {"pk": {"S": f"nested-{uuid.uuid4().hex[:8]}"}, "sk": {"S": "item"}}
    dynamodb_client.put_item(TableName=table, Item={**key, **ITEM})
    return key


def _get(client, table, key):
    return client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")


def _update(client, table, key, condition):
    return client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="SET #u = :x",
        ConditionExpression=condition,
        **_maps("SET #u = :x", condition),
    )


@pytest.mark.parametrize("condition", HOLDS + DERIVED_HOLDS)
def test_update_with_holding_nested_condition_applies(dynamodb_client, table, key, condition):
    _update(dynamodb_client, table, key, condition)
    assert _get(dynamodb_client, table, key)["u"] == {"S": "z"}


@pytest.mark.parametrize("condition", FAILS)
def test_update_with_failing_nested_condition_is_refused(dynamodb_client, table, key, condition):
    with pytest.raises(ClientError) as exc_info:
        _update(dynamodb_client, table, key, condition)
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ConditionalCheckFailedException", err
    assert "u" not in _get(dynamodb_client, table, key)


def test_delete_with_holding_nested_condition_applies(dynamodb_client, table, key):
    condition = "#m.#mm.#k = :deep AND #l[0].#k = :first"
    dynamodb_client.delete_item(
        TableName=table, Key=key, ConditionExpression=condition, **_maps(condition)
    )
    assert _get(dynamodb_client, table, key) is None


def test_delete_with_failing_nested_condition_is_refused(dynamodb_client, table, key):
    condition = "#m.#mm.#k = :other"
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.delete_item(
            TableName=table, Key=key, ConditionExpression=condition, **_maps(condition)
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ConditionalCheckFailedException", err
    assert _get(dynamodb_client, table, key) is not None
