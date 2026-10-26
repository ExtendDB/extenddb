# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Reads of a partition whose items have all been deleted.

Amazon DynamoDB answers a Query of such a partition with no items, whatever
its shape, on the table and on a global secondary index, and a GetItem of a
deleted item with no Item. The Query consumes the capacity of an empty read,
and answers the same when the items were deleted by TransactWriteItems or
BatchWriteItem, or when it starts from an ExclusiveStartKey. A partition with
one item left answers that item. A global secondary index added by
UpdateTable to a table holding such a partition becomes ACTIVE and holds only
the items that exist. Measured on 2026-10-26 at 06:48, 06:52, 06:59, 07:09,
08:00 and 14:45 UTC (us-east-1), each case twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import time

import pytest

from conftest import _is_real_dynamodb, scoped_table


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
    ("limit_1", {"Limit": 1, "ConsistentRead": True}),
    ("descending", {"ScanIndexForward": False, "ConsistentRead": True}),
    ("count", {"Select": "COUNT", "ConsistentRead": True}),
]


@pytest.mark.parametrize(("case", "kw"), SHAPES, ids=[c[0] for c in SHAPES])
def test_query_of_an_emptied_partition_answers_no_items(dynamodb_client, table, case, kw):
    pk = f"emptied-{case}"
    _fill_and_empty(dynamodb_client, table, pk)
    resp = _query(dynamodb_client, table, pk, **kw)
    assert resp["Count"] == 0
    assert resp.get("Items", []) == []


def test_capacity_of_a_query_of_an_emptied_partition(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "emptied-cc")
    cc = _query(dynamodb_client, table, "emptied-cc", ReturnConsumedCapacity="INDEXES")["ConsumedCapacity"]
    assert (cc["CapacityUnits"], cc["Table"]["CapacityUnits"]) == (0.5, 0.5)
    resp = _query(dynamodb_client, table, "emptied-cc", ReturnConsumedCapacity="TOTAL", ConsistentRead=True)
    assert resp["Count"] == 0
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 1.0


ESK_SHAPES = [
    ("inside", "a", True),
    ("before_all", "0", True),
    ("after_all_descending", "zzz", False),
]


@pytest.mark.parametrize(("case", "sk", "forward"), ESK_SHAPES, ids=[c[0] for c in ESK_SHAPES])
def test_query_from_an_exclusive_start_key_in_an_emptied_partition(dynamodb_client, table, case, sk, forward):
    pk = f"emptied-esk-{case}"
    _fill_and_empty(dynamodb_client, table, pk)
    resp = _query(dynamodb_client, table, pk, ConsistentRead=True, ScanIndexForward=forward,
                  ExclusiveStartKey={"pk": {"S": pk}, "sk": {"S": sk}})
    assert resp["Count"] == 0
    assert "LastEvaluatedKey" not in resp


def _delete_by_transaction(client, table, pk):
    client.transact_write_items(TransactItems=[
        {"Delete": {"TableName": table, "Key": {"pk": {"S": pk}, "sk": {"S": sk}}}} for sk in ("a", "b", "c")
    ])


def _delete_by_batch(client, table, pk):
    client.batch_write_item(RequestItems={table: [
        {"DeleteRequest": {"Key": {"pk": {"S": pk}, "sk": {"S": sk}}}} for sk in ("a", "b", "c")
    ]})


TX_BATCH_SHAPES = [
    ("transaction", _delete_by_transaction, ({"ConsistentRead": True}, {"ConsistentRead": True, "Limit": 1})),
    ("batch", _delete_by_batch, ({"ConsistentRead": True},)),
]


@pytest.mark.parametrize(("case", "delete", "shapes"), TX_BATCH_SHAPES, ids=[c[0] for c in TX_BATCH_SHAPES])
def test_query_of_a_partition_emptied_by_a_transaction_or_a_batch(dynamodb_client, table, case, delete, shapes):
    pk = f"emptied-by-{case}"
    for sk in ("a", "b", "c"):
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": pk}, "sk": {"S": sk}})
    delete(dynamodb_client, table, pk)
    for kw in shapes:
        resp = _query(dynamodb_client, table, pk, **kw)
        assert (resp["Count"], resp.get("Items", [])) == (0, []), kw


def test_query_with_a_sort_key_condition(dynamodb_client, table):
    _fill_and_empty(dynamodb_client, table, "emptied-sk")
    resp = dynamodb_client.query(
        TableName=table, KeyConditionExpression="pk = :p AND sk >= :a",
        ExpressionAttributeValues={":p": {"S": "emptied-sk"}, ":a": {"S": "a"}},
        ConsistentRead=True,
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


def _wait_for_index_count(client, query, count):
    # The index is eventually consistent.
    for _ in range(40):
        resp = client.query(**query)
        if resp["Count"] == count:
            return
        time.sleep(0.5)
    raise AssertionError(f"index Count {resp['Count']}, expected {count}")


def test_index_query_of_an_emptied_partition(dynamodb_client, table):
    query = {"TableName": table, "IndexName": "by-g", "KeyConditionExpression": "g = :g",
             "ExpressionAttributeValues": {":g": {"S": "g-emptied-gsi"}}}
    _fill_and_empty(dynamodb_client, table, "emptied-gsi", keep=("a", "b", "c"))
    _wait_for_index_count(dynamodb_client, query, 3)
    for sk in ("a", "b", "c"):
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "emptied-gsi"}, "sk": {"S": sk}})
    _wait_for_index_count(dynamodb_client, query, 0)
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
        # The service took 520 s and 1,011 s to build this index; extenddb
        # builds it in seconds.
        deadline = time.monotonic() + (1500 if _is_real_dynamodb() else 120)
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


def test_a_hash_only_table_whose_item_was_deleted(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k"}, "x": {"S": "x"}})
        dynamodb_client.delete_item(TableName=name, Key={"pk": {"S": "k"}})
        query = {"TableName": name, "KeyConditionExpression": "pk = :p",
                 "ExpressionAttributeValues": {":p": {"S": "k"}}}
        for kw in ({}, {"ConsistentRead": True}, {"Select": "COUNT", "ConsistentRead": True}):
            assert dynamodb_client.query(**query, **kw)["Count"] == 0, kw
        assert "Item" not in dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k"}}, ConsistentRead=True)
        assert dynamodb_client.scan(TableName=name, ConsistentRead=True)["Count"] == 0
