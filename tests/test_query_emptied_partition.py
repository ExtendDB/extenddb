# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Reads of a partition whose items have all been deleted.

Amazon DynamoDB answers a Query of such a partition with no items, whatever
its shape, on the table and on a global secondary index, and a GetItem of a
deleted item with no Item. A partition with one item left answers that item.
A global secondary index added by UpdateTable to a table holding such a
partition becomes ACTIVE and holds only the items that exist. Measured on
2026-10-26 at 06:48, 06:52, 06:59 and 07:09 UTC (us-east-1), each case twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import time

import pytest

from conftest import scoped_table


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "by-g",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        yield name


def _fill_and_empty(client, table, pk, keep=()):
    for sk in ("a", "b", "c"):
        client.put_item(TableName=table, Item={"pk": {"S": pk}, "sk": {"S": sk}, "g": {"S": f"g-{pk}"}})
    for sk in ("a", "b", "c"):
        if sk not in keep:
            client.delete_item(TableName=table, Key={"pk": {"S": pk}, "sk": {"S": sk}})


def _query(client, table, pk, **kw):
    return client.query(TableName=table, KeyConditionExpression="pk = :p",
                        ExpressionAttributeValues={":p": {"S": pk}}, **kw)


SHAPES = [
    ("plain", {}),
    ("consistent", {"ConsistentRead": True}),
    ("limit_1", {"Limit": 1}),
    ("descending", {"ScanIndexForward": False}),
    ("count", {"Select": "COUNT"}),
]


@pytest.mark.parametrize(("case", "kw"), SHAPES, ids=[c[0] for c in SHAPES])
def test_query_of_an_emptied_partition_answers_no_items(dynamodb_client, table, case, kw):
    pk = f"emptied-{case}"
    _fill_and_empty(dynamodb_client, table, pk)
    resp = _query(dynamodb_client, table, pk, **kw)
    assert resp["Count"] == 0
    assert resp.get("Items", []) == []


def test_query_with_a_sort_key_condition(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "emptied-sk")
    resp = dynamodb_client.query(
        TableName=table, KeyConditionExpression="pk = :p AND sk >= :a",
        ExpressionAttributeValues={":p": {"S": "emptied-sk"}, ":a": {"S": "a"}},
    )
    assert resp["Count"] == 0


def test_get_item_of_a_deleted_item(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "emptied-get")
    resp = dynamodb_client.get_item(TableName=table, Key={"pk": {"S": "emptied-get"}, "sk": {"S": "a"}},
                                    ConsistentRead=True)
    assert "Item" not in resp


def test_a_partition_with_one_item_left(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "one-left", keep=("c",))
    for kw in ({"ConsistentRead": True}, {"ConsistentRead": True, "Limit": 1}):
        resp = _query(dynamodb_client, table, "one-left", **kw)
        assert [i["sk"]["S"] for i in resp["Items"]] == ["c"], kw


def test_index_query_of_an_emptied_partition(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "emptied-gsi")
    query = {"TableName": table, "IndexName": "by-g", "KeyConditionExpression": "g = :g",
             "ExpressionAttributeValues": {":g": {"S": "g-emptied-gsi"}}}
    # The index is eventually consistent; wait for the deletes to reach it.
    for _ in range(40):
        resp = dynamodb_client.query(**query)
        if resp["Count"] == 0:
            break
        time.sleep(0.5)
    assert resp["Count"] == 0
    assert dynamodb_client.query(**query, ScanIndexForward=False)["Count"] == 0


def test_an_index_added_later_skips_an_emptied_partition(dynamodb_client):
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
        for sk in ("a", "b"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "emptied"}, "sk": {"S": sk}, "g": {"S": "x"}})
            dynamodb_client.delete_item(TableName=name, Key={"pk": {"S": "emptied"}, "sk": {"S": sk}})
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "live"}, "sk": {"S": "a"}, "g": {"S": "y"}})
        dynamodb_client.update_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "g")],
            GlobalSecondaryIndexUpdates=[{"Create": {
                "IndexName": "by-g",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }}],
        )
        # The service took 520 s and 1,011 s to build this index.
        deadline = time.monotonic() + 1500
        while time.monotonic() < deadline:
            desc = dynamodb_client.describe_table(TableName=name)["Table"]
            if desc["TableStatus"] == "ACTIVE" and all(
                i["IndexStatus"] == "ACTIVE" for i in desc.get("GlobalSecondaryIndexes", [])
            ):
                break
            time.sleep(1)
        else:
            raise AssertionError("the index did not become ACTIVE")

        def count(g):
            return dynamodb_client.query(
                TableName=name, IndexName="by-g", KeyConditionExpression="g = :g",
                ExpressionAttributeValues={":g": {"S": g}},
            )["Count"]

        assert (count("y"), count("x")) == (1, 0)
